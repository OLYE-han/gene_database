"""Sequential, bounded local experiments; never overlap test data services."""
from pathlib import Path
import argparse,hashlib,json,os,subprocess,sys
R=Path(__file__).resolve().parent;P=R.parents[1]
parser=argparse.ArgumentParser()
parser.add_argument('--tests',action='store_true')
parser.add_argument('--groups',nargs='*',default=[])
parser.add_argument('--manifest',default='sources.json')
parser.add_argument('--test-log',default='regression.log')
args=parser.parse_args()
if args.tests:
    env=dict(os.environ,PYTHONPATH=str(P/'python'),DHDGS_TEST_URING='1')
    with (R/args.test_log).open('w') as f:
        code=subprocess.call([sys.executable,'-m','unittest','discover','-s',str(P/'tests'),'-v'],env=env,stdout=f,stderr=subprocess.STDOUT)
    print('regression exit',code,flush=True)
    if code:raise SystemExit(code)
for group in args.groups:
    d=R/group
    if d.parent!=R or not (d/'experiment.yaml').exists():raise ValueError(group)
    for name,digest in json.loads((R/args.manifest).read_text()).items():
        if hashlib.sha256((P/name).read_bytes()).hexdigest()!=digest:raise RuntimeError('Source/config drift: '+name)
    print('starting',group,flush=True)
    with (d/'execution.log').open('a') as f:
        code=subprocess.call([sys.executable,str(d/'run_load.py')],stdout=f,stderr=subprocess.STDOUT)
    if code:raise RuntimeError(f'{group} exit {code}; inspect execution.log')
    run=sorted((d/'runs').glob('*/summary.json'))[-1]
    result=json.loads(run.read_text())
    print(json.dumps({'group':group,'run':run.parent.name,**{k:result[k] for k in ('successful_queries','failed_queries','qps','latency_ms','recall_at_k_all_requests')}}),flush=True)
