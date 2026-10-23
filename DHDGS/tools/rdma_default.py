"""Persistent, validated RDMA deployment entry point (no test-directory imports)."""
import argparse,concurrent.futures,copy,hashlib,json,os,shlex,subprocess,sys
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo
import yaml
CODE=Path(__file__).resolve().parents[1]
P=Path(os.environ.get('DHDGS_PROJECT_ROOT',str(CODE)))
sys.path[:0]=[str(CODE/'python'),str(P/'tools')]
from dhdgs.config import load_config,StrictLoader

def sha(p):return hashlib.sha256(Path(p).read_bytes()).hexdigest()
def remote(h,code,payload):
 cmd=['ssh','-T','-o','BatchMode=yes','-o','ConnectTimeout=8',h['ssh'],shlex.join([h['python'],'-c',code])]
 p=subprocess.run(cmd,input=json.dumps(payload),text=True,capture_output=True,timeout=300)
 if p.returncode:raise RuntimeError(h['ssh']+': '+p.stderr)
 return json.loads(p.stdout)
def profile(path):
 s=yaml.load(path.read_text(),Loader=StrictLoader)
 c=load_config(path.parent/s['runtime_config'])
 if c['native_runtime']['network_module']['transport']!='rdma':raise ValueError('Default runtime is not RDMA')
 if len(s['hosts'])!=len(c['cluster']['nodes']):raise ValueError('Host/node count mismatch')
 lib=Path(s['local_rdma_library_directory'])/'librdmacm.so.1'
 if sha(lib)!=s['rdmacm_sha256']:raise ValueError('Pinned rdmacm hash mismatch')
 return s,c

BOOTSTRAP=r'''import sys,json,shutil,hashlib,os
from pathlib import Path
p=json.load(sys.stdin);h=p['host'];root=Path(h['root']);src=Path(h['bootstrap_project']);dst=Path(h['project'])
# Verify native sources BEFORE reusing the host-specific binary. Never copy a
# binary built for Intel-35 onto a different CPU/Python environment.
for name,want in p['native_hashes'].items():
 if hashlib.sha256((src/name).read_bytes()).hexdigest()!=want:raise RuntimeError('Native source differs; clean rebuild required: '+name)
if not list((src/'python/dhdgs').glob('_native*.so')):raise RuntimeError('Missing host native binary')
root.mkdir(parents=True,exist_ok=True)
if not dst.exists():shutil.copytree(src,dst,symlinks=True,ignore=shutil.ignore_patterns('__pycache__'))
elif src.resolve()!=dst.resolve():
 for f in (src/'python/dhdgs').glob('_native*.so'):shutil.copy2(f,dst/'python/dhdgs'/f.name)
 for f in (src/'python/dhdgs/.native_libs').glob('*'):
  (dst/'python/dhdgs/.native_libs').mkdir(exist_ok=True);shutil.copy2(f,dst/'python/dhdgs/.native_libs'/f.name)
old=Path(h['bootstrap_token_file']);token=Path(h['token_file'])
if not token.exists():shutil.copy2(old,token);token.chmod(0o600)
elif token.read_bytes()!=old.read_bytes():raise RuntimeError('Existing deployment token differs')
print(json.dumps({'host':h['ssh'],'native_sources_match':True,'project':str(dst)}))
'''
CHECK=r'''import sys,json,hashlib,os
from pathlib import Path
p=json.load(sys.stdin);h=p['host'];lib=Path(h['rdma_library_directory'])/'librdmacm.so.1'
assert hashlib.sha256(lib.read_bytes()).hexdigest()==p['rdmacm_sha256'],'RDMA library mismatch'
assert Path(h['token_file']).is_file(),'Missing auth file'
assert Path(h['index']).is_dir(),'Missing index'
sys.path.insert(0,h['project']+'/python');from dhdgs import _native
assert _native.rdma_available(),'Native RDMA unavailable'
for name,want in p['native_hashes'].items():assert hashlib.sha256((Path(h['project'])/name).read_bytes()).hexdigest()==want,name
allowed=os.sched_getaffinity(0);requested=set(p['cpus']);assert requested<=allowed,sorted(requested-allowed)
ports=[str(q) for q in Path('/sys/class/infiniband').glob('*/ports/*') if 'ACTIVE' in (q/'state').read_text()]
assert ports,'No active IB port'
libs=sorted(set(x.split()[-1] for x in Path('/proc/self/maps').read_text().splitlines() if 'librdmacm.so' in x))
assert str(lib) in libs,'Pinned RDMA library not actually loaded'
print(json.dumps({'host':h['ssh'],'rdma_available':True,'library_sha256':p['rdmacm_sha256'],'active_ports':ports,'native_sources_match':True,'cpu_affinity_valid':True,'native_binaries':{str(f):hashlib.sha256(f.read_bytes()).hexdigest() for f in (Path(h['project'])/'python/dhdgs').glob('_native*.so')}}))
'''
def hashes(source):
 files=[source/'CMakeLists.txt',*sorted((source/'cpp').glob('*'))]
 return {str(f.relative_to(source)):sha(f) for f in files if f.is_file()}
def prepare(s):
 source=Path(s["runtime_source"])
 native_hashes=hashes(source)
 def one(h):
  result=remote(h,BOOTSTRAP,{'host':h,'native_hashes':native_hashes})
  for name in ('cpp','python','tools','configs'):
   subprocess.run(['rsync','-a','--exclude=__pycache__','--exclude=*.pyc','--exclude=*.so','--exclude=.native_libs',str(source/name)+'/',h['ssh']+':'+h['project']+'/'+name+'/'],check=True)
  subprocess.run(['rsync','-a',str(source/'CMakeLists.txt'),str(source/'pyproject.toml'),h['ssh']+':'+h['project']+'/'],check=True)
  subprocess.run(['ssh',h['ssh'],shlex.join(['mkdir','-p',h['rdma_library_directory']])],check=True)
  subprocess.run(['rsync','-a',s['local_rdma_library_directory']+'/',h['ssh']+':'+h['rdma_library_directory']+'/'],check=True)
  reference=h['root']+'/Reference_Code/RaBitQ-Library'
  subprocess.run(['ssh',h['ssh'],shlex.join(['mkdir','-p',reference])],check=True)
  subprocess.run(['rsync','-a','--exclude=.git',str(P.parent/'Reference_Code/RaBitQ-Library')+'/',h['ssh']+':'+reference+'/'],check=True)
  return result
 with concurrent.futures.ThreadPoolExecutor(3) as pool:return list(pool.map(one,s['hosts']))
def check(s):
 source=Path(s["runtime_source"])
 native_hashes=hashes(source);spec=s['profile']
 def one(h):
  cpus=spec['compute_cpu_ids'][str(h['id'])]+spec['network_cpu_ids'][str(h['id'])]
  # Loader environment must be present at Python startup.
  wrapped='import os;os.environ["LD_LIBRARY_PATH"]='+repr(h['rdma_library_directory'])+';os.execv('+repr(h['python'])+',['+repr(h['python'])+',"-c",'+repr(CHECK)+'])'
  return remote(h,wrapped,dict(host=h,rdmacm_sha256=s['rdmacm_sha256'],native_hashes=native_hashes,cpus=cpus))
 with concurrent.futures.ThreadPoolExecutor(3) as pool:return list(pool.map(one,s['hosts']))
def analyze_run(run):
 import numpy as np
 from dhdgs.network_api import records
 from full_truth import FullTruth
 from experiment_log import record
 dirs=list((run/'cases').glob('*'))
 if len(dirs)!=1:raise ValueError('Expected one case per default run')
 dest=dirs[0];raw=Path(json.loads((dest/'finished.json').read_text())['directory'])
 manifest=json.loads((dest/'manifest.json').read_text())
 plan=yaml.safe_load((dest/'plan.yaml').read_text());truth=FullTruth(plan['control']['truth_plan'])
 selection=json.loads((raw/'measured/selection.json').read_text())['rows']
 jobs=set();latencies=[];recalls=[];failures=budget_errors=revisits=incomplete=0;protocols=set()
 for q in records(raw/'measured/results'):
  job=q['job']
  if job in jobs or not 0<=job<len(selection):raise ValueError('Duplicate/invalid job')
  jobs.add(job);result=q['result'];stats=result.get('stats',{})
  failed=not q['transport_ok'] or result.get('status')!='ok';failures+=failed
  if failed:continue
  latencies.append((q['client_done_ns']-q['client_start_ns'])/1e6)
  k=plan['dhdgs']['search']['top_k'];expected=truth.ids[selection[job],:k]
  recalls.append(len(set(result['ids'])&set(map(int,expected)))/k)
  budget_errors+=stats['expanded']>plan['dhdgs']['search']['max_expansions']
  revisits+=stats['node_revisit_count']!=0
  incomplete+=not stats.get('metrics_complete',False);protocols.add(stats['transport'])
 if len(jobs)!=manifest['count'] or sum(x['consumed'] for x in manifest['lanes'])!=len(jobs):raise ValueError('Missing output records')
 stopped=json.loads((dest/'shutdown.json').read_text())
 success=not (failures or budget_errors or revisits) and protocols=={'rdma'} and all(x['stopped'] for x in stopped)
 summary=dict(count=len(jobs),failed=failures,recall_at_10=float(np.mean(recalls)) if recalls else None,
  qps=len(latencies)/((manifest['last_received_ns']-manifest['offered_ns'])/1e9),
  mean_ms=float(np.mean(latencies)) if latencies else None,p99_ms=float(np.percentile(latencies,99)) if latencies else None,
  budget_errors=budget_errors,node_revisits=revisits,metrics_incomplete=incomplete,transport=sorted(protocols),
  all_services_stopped=all(x['stopped'] for x in stopped),correctness_pass=success,
  final_output_tail_ms=(manifest['all_consumed_synced_ns']-manifest['last_received_ns'])/1e6,
  limitation='Finite warm-cache loopback-API run; not a new global optimum or sustained SSD throughput claim.')
 (run/'summary.json').write_text(json.dumps(summary,indent=2))
 report=run/'REPORT.md'
 report.write_text('# 默认RDMA部署验证\n\n'+ '\n'.join('- '+k+': '+str(v) for k,v in summary.items())+
  '\n\n本次用于持久化路径及查询正确性核验，未绘制新的性能比较图。默认选型依据 docs/RDMA_DEFAULT.md 中的既有20万请求对照。\n')
 m=json.loads((dest/'experiment_log.json').read_text());m.update(id=run.name,round_id=run.name,status='已完成' if success else '失败',
  results=json.dumps(summary,ensure_ascii=False),artifacts=['['+run.name+'/REPORT.md]('+str(report.relative_to(P))+')'],
  next_steps='继续使用已验收默认参数；长期/冷SSD性能需独立实验。')
 record(m)
 if not success:raise RuntimeError('RDMA correctness validation failed')
 return summary

def main():
 parser=argparse.ArgumentParser(description=__doc__)
 parser.add_argument('action',choices=['prepare','check','benchmark','analyze'])
 parser.add_argument('--config',type=Path,default=P/'configs/rdma_deployment.yaml')
 parser.add_argument('--directory',type=Path,help='Existing capture directory for offline analyze')
 parser.add_argument('--name',help='Unique experiment ID; otherwise generated')
 parser.add_argument('--count',type=int,help='Validation override; normal defaults remain in YAML')
 parser.add_argument('--warmup',type=int)
 args=parser.parse_args()
 if args.action=='analyze':
  if args.directory is None:parser.error('analyze requires --directory')
  print(json.dumps(analyze_run(args.directory.resolve()),indent=2));return
 path=args.config.resolve();s,c=profile(path)
 if args.action=='prepare':print(json.dumps(prepare(s),indent=2));return
 if args.action=='check':print(json.dumps(check(s),indent=2));return
 name=args.name or 'rdma_'+datetime.now(ZoneInfo('Asia/Shanghai')).strftime('%Y%m%d_%H%M%S')
 if not name.replace('_','').replace('-','').isalnum():raise ValueError('Invalid run name')
 run=P/'test'/name
 if run.exists():raise FileExistsError(run)
 spec=copy.deepcopy(s['profile']);spec['name']=name
 if args.count is not None:
  if args.count<=0:raise ValueError('count must be positive')
  spec['count']=args.count
 if args.warmup is not None:
  if args.warmup<0:raise ValueError('warmup must be nonnegative')
  spec['warmup']=args.warmup
 effective=copy.deepcopy(s);effective['cases']=[spec]
 effective['runtime_config']=str((path.parent/s['runtime_config']).resolve())
 prepared=P/'artifacts/rdma_default/configurations';prepared.mkdir(parents=True,exist_ok=True)
 settings=prepared/(name+'.yaml')
 with settings.open('x') as f:yaml.safe_dump(effective,f,sort_keys=False)
 env=dict(os.environ,OPENBLAS_NUM_THREADS='1',OMP_NUM_THREADS='1',LD_LIBRARY_PATH=s['local_rdma_library_directory'],DHDGS_LOAD_SETTINGS=str(settings),DHDGS_RUN_DIRECTORY=str(run),DHDGS_CASES=name,DHDGS_PROJECT_ROOT=str(P))
 subprocess.run(spec['head_prefix']+[sys.executable,str(Path(s['runtime_source'])/'tools/rdma_benchmark.py')],env=env,check=True)
 subprocess.run([sys.executable,str(Path(s['runtime_source'])/'tools/rdma_default.py'),'analyze','--directory',str(run)],env=env,check=True)
if __name__=='__main__':main()
