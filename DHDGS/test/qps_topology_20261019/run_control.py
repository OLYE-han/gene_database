"""Secondary-bottleneck ablation: only native result channel capacity 2 -> 16."""
from pathlib import Path
import copy,json,shlex,subprocess,sys,time
import yaml
R=Path(__file__).resolve().parent;P=R.parents[1]
sys.path[:0]=[str(P/'tools'),str(P/'python')]
from cluster_service import load_plan
from dhdgs.managed_cluster import admin_rpc
from dhdgs.managed_node import save_json
plan=load_plan(R/'remote.yaml');root=R/'control16';root.mkdir(exist_ok=True)
remote='/sharenvme/xulihan_dhdgs/qps_topology_20261019'
plan['experiment']={'native_control_connections':16};plan['dhdgs']['peer']['head_port']=21480
for n,h in zip(plan['dhdgs']['cluster']['nodes'],plan['deployment']['hosts']):
 i=n['id'];n['port']=21451+i
 h['state_file']=str(root/f'node{i}_state.json') if i==0 else remote+f'/node{i}_state.json'
path=root/'plan.yaml';path.write_text(yaml.safe_dump(plan,sort_keys=False))
ssh=['ssh','-T','-o','BatchMode=yes','-o','ConnectTimeout=8','-o','ServerAliveInterval=5','-o','ServerAliveCountMax=2','intel-30']
py=plan['deployment']['hosts'][1]['python']
release=json.loads((P/'test/managed_35_30_20261019/deployment_node1.json').read_text())['project']
script='from pathlib import Path;import sys,json;r=Path(sys.argv[1]);r.mkdir(parents=True,exist_ok=True);v=json.load(sys.stdin);[(r/k).write_text(s) for k,s in v.items()]'
subprocess.run(ssh+[shlex.join([py,'-c',script,remote])],input=json.dumps({'plan.yaml':path.read_text(),'serve_variant.py':(R/'serve_variant.py').read_text()}).encode(),check=True,timeout=30)
procs=[];logs=[];token=Path(plan['control']['token_file']).read_text().strip();cleanup=[]
try:
 for i in range(3):
  cmd=[sys.executable,str(R/'serve_variant.py'),str(P),str(path),str(i)] if i==0 else ssh+[shlex.join([py,remote+'/serve_variant.py',release,remote+'/plan.yaml',str(i)])]
  f=(root/f'node{i}.log').open('a');logs.append(f);procs.append(subprocess.Popen(cmd,stdout=f,stderr=subprocess.STDOUT,stdin=subprocess.DEVNULL))
 for n in plan['dhdgs']['cluster']['nodes']:
  deadline=time.monotonic()+120
  while True:
   if any(p.poll() is not None for p in procs):raise RuntimeError('Node exited')
   try:s=admin_rpc(n,token,'admin_status',timeout=1);break
   except OSError:
    if time.monotonic()>deadline:raise
    time.sleep(.2)
  admin_rpc(n,token,'admin_resume',revision=s['revision'])
 for repeat in range(3):
  case=R/'cases'/f'remote_p64_control16_c128_r{repeat}';case.mkdir(parents=True,exist_ok=False)
  save_json(case/'case.json',dict(topology='remote',head_connections=64,control_connections=16,concurrency=128,repeat=repeat,plan_file=str(path)))
  with (case/'execution.log').open('w') as log:subprocess.run([sys.executable,str(R/'run_matrix.py'),'--child',str(case)],stdout=log,stderr=subprocess.STDOUT,check=True,timeout=240)
  s=json.loads((case/'result.json').read_text());print(json.dumps({'case':case.name,'qps':s['qps'],'p99_ms':s['latency_ms']['p99'],'recall':s['recall_at_k_all_requests']}),flush=True)
finally:
 for n in plan['dhdgs']['cluster']['nodes']:
  try:admin_rpc(n,token,'admin_shutdown',timeout=5);cleanup.append({'node':n['id'],'stop':True})
  except Exception as exc:cleanup.append({'node':n['id'],'error':str(exc)})
 for p in procs:
  try:p.wait(timeout=15)
  except subprocess.TimeoutExpired:p.terminate();p.wait(timeout=10)
 for f in logs:f.close()
 save_json(root/'cleanup.json',cleanup)
