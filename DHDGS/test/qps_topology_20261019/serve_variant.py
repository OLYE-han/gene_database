"""Experiment-only native completion RPC capacity; query/peer pools unchanged."""
import os
for k in ('OPENBLAS_NUM_THREADS','MKL_NUM_THREADS','OMP_NUM_THREADS'):os.environ[k]='1'
import copy,json,sys
from pathlib import Path
project,plan_path,sid=sys.argv[1:4];sid=int(sid)
sys.path[:0]=[str(Path(project)/'tools'),str(Path(project)/'python')]
from cluster_service import load_plan,serve
from dhdgs import _native
from dhdgs.managed_node import create_managed_server
plan=load_plan(plan_path);limit=plan['experiment']['native_control_connections']
original=_native.NativeController

def controller(c):
    isolated=copy.deepcopy(c);isolated['peer']['communication']['connections_per_endpoint']=limit
    return original(isolated)
_native.NativeController=controller
host=next(h for h in plan['deployment']['hosts'] if h['id']==sid)
server=create_managed_server(plan['dhdgs'],sid,Path(host['token_file']).read_text().strip(),host['state_file'])
try:
    print(json.dumps({'pid':os.getpid(),'node':sid,'native_control_connections':limit,'peer_connections':plan['dhdgs']['peer']['communication']['connections_per_endpoint']}),flush=True)
    serve(server)
finally:server.server_close()
