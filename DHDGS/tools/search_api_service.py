"""Start a DHDGS head with a persistent asynchronous TCP search endpoint."""
import os
for k in ('OPENBLAS_NUM_THREADS','OMP_NUM_THREADS','MKL_NUM_THREADS'):os.environ[k]='1'
import argparse,json,sys,signal,threading
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'python'))
import yaml
from dhdgs.cpp_head import NativeEngine
from dhdgs.config import validate
from dhdgs.managed_node import revision
from dhdgs.network_api import api_options
p=argparse.ArgumentParser();p.add_argument('--plan',required=True);p.add_argument('--settings',required=True);a=p.parse_args()
plan=yaml.safe_load(Path(a.plan).read_text());c=plan['dhdgs'];settings=yaml.safe_load(Path(a.settings).read_text());validate(c)
c['_cluster_revision']=revision(c)
engine=NativeEngine(c,external_completion=True);done=threading.Event()
for sig in (signal.SIGINT,signal.SIGTERM):signal.signal(sig,lambda *_:done.set())
try:
    if c.get('load_balance',{}).get('enabled'):
        control=plan['control']
        token=Path(control['token_file']).read_text().strip()
        engine.start_load_balancer(token,Path(control['results_directory'])/'load_balance')
    port=engine.runtime.start_api(api_options(c,settings));print(json.dumps(dict(role='search_api',pid=os.getpid(),port=port)),flush=True);done.wait()
finally:engine.close()
