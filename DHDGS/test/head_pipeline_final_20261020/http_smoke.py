"""Real HTTP functional test: bounded receive workers, no receiver subprocess."""
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

root=R/'http';root.mkdir(exist_ok=True)
selection=json.loads((R/'selection.json').read_text())
plan=load_plan(R/'cases'/selection['case']/'plan.yaml')
plan['control'].update(port=22391,results_directory=str(root.resolve()))
plan['dhdgs']['peer']['head_port']=22380
path=root/'cluster.yaml';path.write_text(yaml.safe_dump(plan,sort_keys=False))
with (root/'head.log').open('w') as log:
    process=subprocess.Popen([sys.executable,str(P/'tools/cluster_service.py'),'head','--plan',str(path)],stdout=log,stderr=subprocess.STDOUT)
    try:
        deadline=time.monotonic()+120
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
        # Exercise management-to-query submission without blocking an ingress worker.
        bench=request(plan,'benchmark',dict(queries=32,concurrency=16,warmup=2))
        assert bench['successful_queries']==32 and all(bench['audit'].values())
        status=request(plan,'status')
        assert status['head_receivers']==[] and status['head_pipeline']['peak_workers']<=4
        children=subprocess.run(['ps','--ppid',str(process.pid),'-o','pid=,args='],capture_output=True,text=True).stdout.strip()
        assert not children,children
        save_json(root/'verification.json',dict(successful_http_queries=32,client_concurrency=16,
            elapsed_seconds=elapsed,benchmark_queries=32,receiver_subprocesses=0,
            children=children,head_pid=process.pid,status=status,benchmark=bench))
        request(plan,'collect');request(plan,'shutdown')
        process.wait(timeout=30)
        assert process.returncode==0
    finally:
        if process.poll() is None:process.terminate();process.wait(timeout=30)
print('HTTP 32 queries + management benchmark 32 queries passed; no receiver subprocess')
