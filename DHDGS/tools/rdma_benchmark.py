"""Three-host test runner; load generator runs in a separate TCP-only process."""
from pathlib import Path
import os,sys,json,yaml,subprocess,shlex,time,copy,concurrent.futures,hashlib,threading
import msgpack
CODE=Path(__file__).resolve().parents[1]
P=Path(os.environ.get('DHDGS_PROJECT_ROOT',str(CODE)))
SETTINGS=Path(os.environ['DHDGS_LOAD_SETTINGS']);S=yaml.safe_load(SETTINGS.read_text())
ROOT=Path(S['experiment']['scratch']);SRC=CODE
R=Path(os.environ['DHDGS_RUN_DIRECTORY']);R.mkdir(parents=True,exist_ok=False)
sys.path[:0]=[str(SRC/'python'),str(P/'tools')]
from dhdgs.cpp_head import NativeEngine
from dhdgs.managed_cluster import ClusterHead,admin_rpc
from dhdgs.managed_node import revision
from dhdgs.network_api import api_options
from dhdgs.metrics import process_snapshot
from dhdgs.config import load_config
hosts=S['hosts'];py=sys.executable
BASE={k:v for k,v in load_config(SETTINGS.parent/S['runtime_config']).items() if not k.startswith('_')}
def merge(a,b):
 for k,v in b.items():
  if isinstance(v,dict):merge(a[k],v)
  else:a[k]=copy.deepcopy(v)
def save(p,x):p.write_text(json.dumps(x,indent=2))
def command(h,script,payload):
 cmd=[h['python'],'-c',script]
 if h['ssh']:cmd=['ssh','-o','BatchMode=yes','-o','ConnectTimeout=8',h['ssh'],shlex.join(cmd)]
 r=subprocess.run(cmd,input=json.dumps(payload),text=True,capture_output=True,timeout=240)
 if r.returncode:raise RuntimeError(r.stderr)
 return json.loads(r.stdout)
launch='''import sys,json,os,resource,time,socket,subprocess
from pathlib import Path
p=json.load(sys.stdin);root=Path(p['root']);root.mkdir(parents=True,exist_ok=False)
s=socket.socket();s.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1);s.bind(('0.0.0.0',p['port']));s.close()
a,b=resource.getrlimit(resource.RLIMIT_NOFILE);resource.setrlimit(resource.RLIMIT_NOFILE,(min(65536,b),b))
os.environ.update(LD_LIBRARY_PATH=p['rdma_library_directory']+':'+os.environ.get('LD_LIBRARY_PATH',''),OPENBLAS_NUM_THREADS='1',OMP_NUM_THREADS='1',PYTHONFAULTHANDLER='1')
resource.setrlimit(resource.RLIMIT_CORE,(0,0))
(root/'plan.yaml').write_text(p['plan'])
with (root/'data.log').open('w') as f:
 child=subprocess.Popen(p.get('prefix',[])+[p['python'],p['project']+'/tools/cluster_service.py','data','--plan',str(root/'plan.yaml'),'--node',str(p['id'])],stdin=subprocess.DEVNULL,stdout=f,stderr=f,start_new_session=True)
deadline=time.monotonic()+180
while True:
 if child.poll() is not None:raise RuntimeError((root/'data.log').read_text())
 if any('"role": "data"' in row and str(child.pid) in row for row in (root/'data.log').read_text().splitlines()):break
 if time.monotonic()>deadline:raise TimeoutError('Missing initialized-service marker')
 time.sleep(.05)
print(json.dumps({'id':p['id'],'pid':child.pid,'runtime_libraries':[line.split()[-1] for line in Path('/proc/'+str(child.pid)+'/maps').read_text().splitlines() if any(k in line for k in ['librdmacm','libibverbs','libmlx5'])]}))
'''
shutdown_check='''import json,sys,time,socket
from pathlib import Path
p=json.load(sys.stdin);deadline=time.monotonic()+30
while True:
 f=Path('/proc')/str(p['pid'])/'cmdline'
 if not f.exists() or not f.read_bytes():break
 if time.monotonic()>deadline:raise TimeoutError('Experiment PID still live')
 time.sleep(.1)
s=socket.socket();s.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1);s.bind(('0.0.0.0',p['port']));s.close();print(json.dumps({'stopped':True,'pid':p['pid']}))
'''
class Collector:
 def __init__(self,c,token):self.c=c;self.token=token
 def nodes(self):return self.c['cluster']['nodes']
 def rpc(self,node,op,**kw):return admin_rpc(node,self.token,op,timeout=90,limit=self.c['network']['max_message_bytes'],**kw)
 pull=ClusterHead.pull
 def idle(self):
  deadline=time.monotonic()+180
  while True:
   states=[self.rpc(n,'admin_status') for n in self.nodes()]
   if all(n['quiescent'] for n in states):return states
   if time.monotonic()>deadline:raise TimeoutError('Data did not drain')
   time.sleep(.05)

PLACEMENT = r"""import json,sys,os,collections,hashlib
from pathlib import Path
p=json.load(sys.stdin);root=Path('/proc')/str(p['pid']);pages=collections.Counter();anon=collections.Counter()
for line in (root/'numa_maps').read_text().splitlines():
 for word in line.split():
  if word.startswith('N') and '=' in word and word[1:word.index('=')].isdigit():
   key,val=word.split('=');pages[key]+=int(val)
   if 'anon=' in line:anon[key]+=int(val)
masks=collections.Counter(','.join(map(str,sorted(os.sched_getaffinity(int(t.name))))) for t in (root/'task').iterdir())
libs=set(line.split()[-1] for line in (root/'maps').read_text().splitlines() if any(k in line for k in ['_native.','librdmacm.so','libjemalloc.so']))
print(json.dumps({'all_mapped_pages':dict(pages),'anonymous_pages':dict(anon),'thread_cpu_masks':dict(masks),'loadavg':Path('/proc/loadavg').read_text().strip(),'libraries':{f:hashlib.sha256(Path(f).read_bytes()).hexdigest() for f in libs}}))
"""
SAMPLER = r"""import time,sys,json,threading,os,subprocess
from pathlib import Path
stop=threading.Event()
threading.Thread(target=lambda:(sys.stdin.read(),stop.set()),daemon=True).start()
pid=int(sys.argv[1]);root=Path('/proc')/str(pid)
noise={};noise_at=0.
ports=[p for p in Path('/sys/class/infiniband').glob('*/ports/*') if 'ACTIVE' in (p/'state').read_text()]
while not stop.is_set():
 if time.monotonic()-noise_at>=.5:
  noise_at=time.monotonic();ps=subprocess.run(['ps','-C','fio','-o','pid=,pcpu=,nlwp=,comm='],text=True,capture_output=True)
  noise={'background_fio':ps.stdout.strip().splitlines(),'cpu_freq_khz':{}}
  for c in range(0,64,8):
   f=Path('/sys/devices/system/cpu')/('cpu'+str(c))/'cpufreq/scaling_cur_freq'
   if f.exists():noise['cpu_freq_khz'][str(c)]=int(f.read_text())
  noise['host_cpu_ticks']=list(map(int,Path('/proc/stat').read_text().splitlines()[0].split()[1:]))
 row={'time_ns':time.monotonic_ns(),'ports':{},**noise}
 for p in ports:
  row['ports'][str(p)]={k:int((p/'counters'/k).read_text()) for k in ['port_xmit_data','port_rcv_data','port_xmit_packets','port_rcv_packets','port_xmit_wait','port_rcv_errors'] if (p/'counters'/k).exists()}
 try:
  st=(root/'stat').read_text().split();row.update(cpu_ticks=int(st[13])+int(st[14]),threads=int(st[19]),hz=os.sysconf('SC_CLK_TCK'))
 except FileNotFoundError:break
 print(json.dumps(row),flush=True);stop.wait(.05)
"""
def start_samplers(launched,dest):
 out=[]
 try:
  for info in launched+[{'id':'head','pid':os.getpid()}]:
   h=next((h for h in hosts if h['id']==info['id']),None)
   cmd=[h['python'] if h else py,'-u','-c',SAMPLER,str(info['pid'])]
   if h:cmd=['ssh',h['ssh'],shlex.join(cmd)]
   f=(dest/('port_samples_'+str(info['id'])+'.jsonl')).open('w')
   proc=subprocess.Popen(cmd,stdin=subprocess.PIPE,stdout=f,stderr=subprocess.STDOUT);out.append((proc,f))
  return out
 except BaseException:stop_samplers(out);raise
def stop_samplers(items):
 for proc,f in items:
  proc.stdin.close()
 for proc,f in items:
  try:proc.wait(timeout=10)
  finally:f.close()

def case(spec):
 concurrency=spec['concurrency'];name=spec['name'];case=ROOT/'cases'/name;dest=R/'cases'/name;dest.mkdir(parents=True,exist_ok=True)
 if (dest/'finished.json').exists():return
 case.mkdir(parents=True,exist_ok=False)
 effective_settings=copy.deepcopy(S)
 effective_settings['api'].update(spec.get('api_overrides',{}))
 effective_settings['client'].update(spec.get('client_overrides',{}))
 settings_path=case/'load_settings.yaml'
 settings_path.write_text(yaml.safe_dump(effective_settings))
 (dest/'load_settings.yaml').write_text(yaml.safe_dump(effective_settings))
 headc=copy.deepcopy(BASE);c=copy.deepcopy(BASE)
 merge(c['native_runtime'],S['data_runtime_overrides'])
 plan={'dhdgs':c,'control':copy.deepcopy(S['control']),'deployment':{'hosts':copy.deepcopy(hosts)}}
 for config in [c,headc]:
  config['peer']['worker_threads']=spec['threads'];config['native_runtime']['network_module']['transport']=spec['transport'];config['search']['max_expansions']=S['experiment']['search_budget']
  config['peer'].update(spec.get('peer_overrides',{}))
  config['native_runtime']['route_threads']=spec.get('route_threads',S['experiment']['route_threads']);config['native_runtime']['merge_threads']=spec.get('merge_threads',S['experiment']['merge_threads'])
  config['peer']['head_port']=S['experiment']['report_port']
  if config is headc and spec.get('head_entry_overrides'):config['native_runtime']['head_entry_network'].update(spec['head_entry_overrides'])
  for n,port in zip(config['cluster']['nodes'],S['experiment']['data_ports']):n['port']=port
 for config in [c,headc]:
  for key,value in spec.get('network_overrides',{}).items():config['native_runtime']['network_module'][key]=value
 if spec.get('head_compute_cpu_ids'):headc['native_runtime']['compute_cpu_ids']=spec['head_compute_cpu_ids']
 if spec.get('head_network_cpu_ids'):headc['native_runtime']['network_module']['cpu_ids']=spec['head_network_cpu_ids']
 plan['deployment']['hosts']=copy.deepcopy(hosts)
 for h in plan['deployment']['hosts']:h['root']=str(Path(h['root'])/name);h['state_file']=h['root']+'/state.json'
 for f in ['source_hashes.json']:
  if (R/f).exists():(dest/f).write_bytes((R/f).read_bytes())
 # Restore the original 2000-row validation selection; old scratch selection may be repeated.
 selection=json.loads(Path(plan['control']['query_selection']).read_text())['validation'];selection=list(dict.fromkeys(selection));save(case/'selection.json',{'validation':selection});plan['control']['query_selection']=str(case/'selection.json')
 (case/'plan.yaml').write_text(yaml.safe_dump(plan));(dest/'plan.yaml').write_text(yaml.safe_dump(plan));(dest/'head.yaml').write_text(yaml.safe_dump(headc))
 token=Path(plan['control']['token_file']).read_text().strip();collector=Collector(c,token);launched=[];engine=None
 print('START',name,flush=True)
 if spec.get('head_cpu_ids'):os.sched_setaffinity(0,spec['head_cpu_ids'])
 try:
  for h in plan['deployment']['hosts']:
   node_plan=copy.deepcopy(plan);nc=node_plan['dhdgs']['native_runtime'];sid=str(h['id'])
   if sid in spec.get('compute_cpu_ids',{}):nc['compute_cpu_ids']=spec['compute_cpu_ids'][sid]
   if sid in spec.get('network_cpu_ids',{}):nc['network_module']['cpu_ids']=spec['network_cpu_ids'][sid]
   (dest/('node_'+sid+'.yaml')).write_text(yaml.safe_dump(node_plan))
   info=command(h,launch,{**h,'port':c['cluster']['nodes'][h['id']]['port'],'plan':yaml.safe_dump(node_plan),'prefix':spec.get('node_prefixes',{}).get(sid,[])});launched.append(info);save(dest/'launch.json',launched)
  save(dest/'launch.json',launched)
  assert all(any('/rdma_libs/librdmacm.so.1' in x for x in v['runtime_libraries']) for v in launched),'RDMA runtime library not pinned'
  for n in collector.nodes():
   state=collector.rpc(n,'admin_status');assert state['pid']==next(x['pid'] for x in launched if x['id']==n['id']);collector.rpc(n,'admin_resume',revision=revision(c))
  headc['_cluster_revision']=revision(headc);assert revision(c)==revision(headc)
  engine=NativeEngine(headc,external_completion=True);engine.runtime.start_api(api_options(headc,effective_settings))
  def client(label,count):
   with (dest/(label+'.log')).open('w') as log:
    subprocess.run(spec.get('client_prefix',[])+[py,str(SRC/'tools/network_load.py'),'--plan',str(case/'plan.yaml'),'--settings',str(settings_path),'--output',str(case/label),'--count',str(count),'--concurrency',str(concurrency)],stdout=log,stderr=log,check=True,timeout=600)
  client('warmup',spec['warmup']);collector.idle()
  from dhdgs.network_api import records
  assert all(q['transport_ok'] and q['result'].get('status')=='ok' for q in records(case/'warmup/results')),'Warmup failed'
  before=collector.pull()
  def connecting(x):
   if isinstance(x,dict):return sum((v if k=='connecting' and isinstance(v,int) else connecting(v))for k,v in x.items())
   if isinstance(x,list):return sum(map(connecting,x))
   return 0
  until=time.monotonic()+60
  while connecting(before)+connecting(engine.runtime.metrics()):
   if time.monotonic()>until:raise TimeoutError('Connection warmup did not drain')
   time.sleep(.05);before=collector.pull()
  save(dest/'placement.json',[{'id':info['id'],**command(next(h for h in hosts if h['id']==info['id']),PLACEMENT,info)}for info in launched])
  save(dest/'head_placement.json',command({'python':py,'ssh':None},PLACEMENT,{'pid':os.getpid()}))
  if spec.get('quiet_start_seconds'):
   quiet_script='import subprocess,json; p=subprocess.run(["ps","-C","fio","-o","pid=,pcpu=,comm="],text=True,capture_output=True); print(json.dumps({"fio":p.stdout.strip().splitlines()}))'
   observations=[];quiet_since=None;deadline=time.monotonic()+spec.get('quiet_start_timeout_seconds',120)
   while True:
    with concurrent.futures.ThreadPoolExecutor(3)as pool:noise=list(pool.map(lambda h:command(h,quiet_script,{}),hosts))
    local=subprocess.run(['ps','-C','fio','-o','pid=,pcpu=,comm='],text=True,capture_output=True);noise.append({'fio':local.stdout.strip().splitlines()});now=time.monotonic();observations.append(dict(time_ns=time.monotonic_ns(),hosts=noise))
    quiet_since=(quiet_since or now)if all(not n['fio']for n in noise)else None
    if quiet_since and now-quiet_since>=spec['quiet_start_seconds']:break
    if now>deadline:
     save(dest/'quiet_start.json',observations)
     raise TimeoutError('No quiet startup window; no timed measurement made')
    time.sleep(.5)
   save(dest/'quiet_start.json',observations)
  def clock_bound(h):
   begin=time.monotonic_ns();v=command(h,'import time,json;print(json.dumps({"now":time.monotonic_ns()}))',{});end=time.monotonic_ns()
   return dict(id=h['id'],head_begin_ns=begin,head_end_ns=end,data_ns=v['now'],offset_min_ns=v['now']-end,offset_max_ns=v['now']-begin)
  with concurrent.futures.ThreadPoolExecutor(3)as pool:save(dest/'clock_bounds.json',list(pool.map(clock_bound,hosts)))
  head_before=process_snapshot();api_before=engine.runtime.api_metrics();samples=[];stop=threading.Event()
  def monitor():
   while not stop.is_set():samples.append(dict(time_ns=time.monotonic_ns(),head=engine.runtime.metrics(),api=engine.runtime.api_metrics()));stop.wait(.2)
  monitor_thread=threading.Thread(target=monitor);monitor_thread.start()
  samplers=start_samplers(launched,dest)
  try:client('measured',spec['count'])
  finally:
   stop.set();monitor_thread.join();stop_samplers(samplers)
  head_after=process_snapshot();state=collector.idle();after=collector.pull();head_final=engine.runtime.metrics();api_final=engine.runtime.api_metrics()
  (case/'nodes_before.msgpack').write_bytes(msgpack.packb(before,use_bin_type=True));(case/'nodes_after.msgpack').write_bytes(msgpack.packb(after,use_bin_type=True))
  save(dest/'head_samples.json',samples);save(dest/'final.json',dict(spec=spec,nodes=state,head=head_final,api=api_final,api_before=api_before,head_process_before=head_before,head_process_after=head_after))
  manifest=json.loads((case/'measured/manifest.json').read_text());save(dest/'manifest.json',manifest);save(dest/'finished.json',dict(directory=str(case),time_ns=time.monotonic_ns(),head_pid=os.getpid(),client_pid=manifest['process_after']['pid']))
  print('MEASURED',name,manifest['received']/((manifest['last_received_ns']-manifest['offered_ns'])/1e9),flush=True)
  if spec.get('idle_observation_seconds'):
   idle_rows=[];until=time.monotonic()+spec['idle_observation_seconds']
   while time.monotonic()<until:
    idle_rows.append(dict(time_ns=time.monotonic_ns(),head=engine.runtime.metrics(),nodes=[collector.rpc(n,'admin_status')for n in collector.nodes()]));time.sleep(1)
   save(dest/'idle_samples.json',idle_rows)
   (case/'nodes_idle.msgpack').write_bytes(msgpack.packb(collector.pull(),use_bin_type=True))
 finally:
  if engine:engine.close()
  for info in launched:
   n=c['cluster']['nodes'][info['id']]
   try:collector.rpc(n,'admin_shutdown')
   except Exception as e:print('shutdown',repr(e),flush=True)
  audits=[]
  for info in launched:
   h=next(h for h in hosts if h['id']==info['id']);audits.append(command(h,shutdown_check,{**info,'port':c['cluster']['nodes'][info['id']]['port']}))
  save(dest/'shutdown.json',audits)
# Each run has independent evidence; only the round summary enters TestLog.md.
sys.path.insert(0,str(P/'tools'))
from experiment_log import experiment
wanted=os.environ.get('DHDGS_CASES','').split(',')
for spec in S['cases']:
 if wanted==[''] and spec.get('smoke'):continue
 if wanted!=[''] and spec['name'] not in wanted:continue
 dest=R/'cases'/spec['name']
 if (dest/'finished.json').exists():continue
 metadata=dict(title='默认RDMA配置验证',purpose='验证持久化默认RDMA部署的完整查询流程',hypothesis='迁移路径不改变搜索及传输行为',expected='无失败、预算守恒、节点不重访、所有服务排空',configuration='持久化RDMA YAML',code_changes='默认配置与部署路径固化；搜索算法未修改',status='运行中')
 for key in ('started_at','ended_at','revision'):metadata.pop(key,None)
 metadata.update(id=R.name+'_'+spec['name'],title='四机传输对照 '+spec['name'],
                 configuration=json.dumps(spec,ensure_ascii=False)+'；固定其余设置见本轮 plan.yaml/head.yaml/settings.yaml。',
                 results='待分析；尚无本轮性能结论。',artifacts=['输出目录：'+str(dest)],
                 next_steps='离线核验召回/失败/排空与协议，生成指标、时序图及网络利用率。')
 with experiment(metadata,dest/'experiment_log.json',round_id=R.name):case(spec)
save(R/'measurements_complete.json',{'time_ns':time.monotonic_ns()})
