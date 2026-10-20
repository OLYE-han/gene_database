"""Real HTTP verification for query-affine head processes, outside performance runs."""
import os
for key in ('OPENBLAS_NUM_THREADS','OMP_NUM_THREADS','MKL_NUM_THREADS'):os.environ[key]='1'
from pathlib import Path
import concurrent.futures,json,subprocess,sys,time
import yaml
R=Path(__file__).resolve().parent;P=R.parents[1]
sys.path[:0]=[str(P/'tools'),str(P/'python')]
from cluster_service import load_plan
from cluster_ctl import request
from dhdgs.managed_node import save_json

root=R/'http';root.mkdir(exist_ok=False)
plan=load_plan(R/'candidate.yaml')
plan['control'].update(port=28091,results_directory=str(root.resolve()))
plan['dhdgs']['peer']['head_port']=28010
plan['dhdgs']['peer']['head_lanes']['results_directory']=str(root/'worker_results')
path=root/'cluster.yaml';path.write_text(yaml.safe_dump(plan,sort_keys=False))
with (root/'head.log').open('w') as log:
    process=subprocess.Popen([sys.executable,str(P/'tools/cluster_service.py'),'head','--plan',str(path)],stdout=log,stderr=subprocess.STDOUT)
    try:
        deadline=time.monotonic()+180
        while True:
            if process.poll() is not None:raise RuntimeError('HTTP head exited during startup')
            try:status=request(plan,'status');break
            except OSError:
                if time.monotonic()>deadline:raise
                time.sleep(.2)
        request(plan,'recover')
        rows=json.loads(Path(plan['control']['query_selection']).read_text())['validation'][:32]
        start=time.monotonic()
        with concurrent.futures.ThreadPoolExecutor(16) as pool:
            results=list(pool.map(lambda row:request(plan,'query',{'row':int(row)}),rows))
        elapsed=time.monotonic()-start
        assert all(v['status']=='ok' for v in results)
        bench=request(plan,'benchmark',dict(queries=32,concurrency=16,warmup=2))
        assert bench['successful_queries']==32 and all(bench['audit'].values())
        status=request(plan,'status');workers=status['head_receivers']
        assert status['head_pipeline']['mode']=='lanes'
        assert len(workers)==4 and all(w['alive'] and w['role']=='head_lane' and not w['active_queries'] for w in workers)
        assert all(Path(w['source_file']).resolve()==(P/'python/dhdgs/head_lanes.py').resolve() for w in workers)
        save_json(root/'verification.json',dict(successful_http_queries=32,client_concurrency=16,
            elapsed_seconds=elapsed,benchmark_queries=32,head_pid=process.pid,status=status,benchmark=bench))
        request(plan,'collect');request(plan,'shutdown')
        process.wait(timeout=30)
        assert process.returncode==0
    finally:
        if process.poll() is None:process.terminate();process.wait(timeout=30)
print('HTTP 32 queries + management benchmark 32 queries passed; 4 query-affine head processes')
