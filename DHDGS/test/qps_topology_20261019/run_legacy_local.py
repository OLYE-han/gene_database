from pathlib import Path
import subprocess,sys,time
R=Path(__file__).resolve().parent;P=R.parents[1]
sys.path[:0]=[str(P/'tools'),str(P/'python')]
from cluster_service import load_plan
from dhdgs.managed_cluster import admin_rpc
from dhdgs.managed_node import save_json
plan=load_plan(R/'local.yaml');token=Path(plan['control']['token_file']).read_text().strip()
procs=[];logs=[];cleanup=[]
try:
 for n in plan['dhdgs']['cluster']['nodes']:
  f=(R/'legacy20local'/f"node{n['id']}.log").open('a');logs.append(f)
  procs.append(subprocess.Popen([sys.executable,str(P/'tools/cluster_service.py'),'data','--plan',str(R/'local.yaml'),'--node',str(n['id'])],stdout=f,stderr=subprocess.STDOUT,stdin=subprocess.DEVNULL))
 for n in plan['dhdgs']['cluster']['nodes']:
  deadline=time.monotonic()+120
  while True:
   if any(p.poll() is not None for p in procs):raise RuntimeError('Node exited')
   try:s=admin_rpc(n,token,'admin_status',timeout=1);break
   except OSError:
    if time.monotonic()>deadline:raise
    time.sleep(.2)
  admin_rpc(n,token,'admin_resume',revision=s['revision'])
 subprocess.run([sys.executable,str(R/'legacy20local/run_load.py')],check=True,timeout=600)
finally:
 for n in plan['dhdgs']['cluster']['nodes']:
  try:admin_rpc(n,token,'admin_shutdown',timeout=5);cleanup.append({'node':n['id'],'stop':True})
  except Exception as exc:cleanup.append({'node':n['id'],'error':str(exc)})
 for p in procs:
  try:p.wait(timeout=15)
  except subprocess.TimeoutExpired:p.terminate();p.wait(timeout=10)
 for f in logs:f.close()
 save_json(R/'legacy20local/cleanup.json',cleanup)
