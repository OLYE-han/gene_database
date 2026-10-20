"""Direct wall/thread-CPU stage timers; no interpreter profiler hooks."""
import os
for k in ('OPENBLAS_NUM_THREADS','MKL_NUM_THREADS','OMP_NUM_THREADS'):os.environ[k]='1'
from pathlib import Path
import time,threading,json,gzip,csv
import run_scan
R=Path(__file__).resolve().parent

def main():
    from dhdgs.head_flow import QueryFlow
    from dhdgs.search import Engine
    import numpy as np
    original=QueryFlow._step;init=Engine.__init__;local=threading.local();records=[]
    def record(stage,cpu,wall,qid):
        records.append(dict(stage=stage,request_id=qid,cpu_ms=(time.thread_time_ns()-cpu)/1e6,wall_ms=(time.monotonic_ns()-wall)/1e6))
    def step(self,rec,operation):
        local.rec=rec;g=rec['generator'];qid=g.gi_frame.f_locals.get('request_id') if g.gi_frame else None
        cpu=time.thread_time_ns();wall=time.monotonic_ns()
        try:return original(self,rec,operation)
        finally:
            if not qid and g.gi_frame:qid=g.gi_frame.f_locals.get('request_id')
            record('route_stage' if operation is None else 'merge_stage',cpu,wall,qid)
    class Proxy:
        def __init__(self,obj,method):self.obj=obj;self.method=method
        def __getattr__(self,name):
            fn=getattr(self.obj,name)
            if name!=self.method:return fn
            def measured(*args,**kwargs):
                cpu=time.thread_time_ns();wall=time.monotonic_ns()
                try:return fn(*args,**kwargs)
                finally:
                    g=local.rec['generator'];record(name,cpu,wall,g.gi_frame.f_locals.get('request_id'))
            return measured
    def initialize(self,c):
        init(self,c);self.router=Proxy(self.router,'search');self.quantizer=Proxy(self.quantizer,'score')
    QueryFlow._step=step;Engine.__init__=initialize
    case=R/'stage_timers';case.mkdir(exist_ok=True)
    (case/'case.json').write_text(json.dumps(dict(mode=2,queries=2000,concurrency=128,warmup=32)))
    run_scan.child(case)
    result=json.loads((case/'result.json').read_text())
    with gzip.open(Path(result['directory'])/'queries.jsonl.gz','rt') as f:ids={json.loads(l)['request_id'] for l in f}
    rows=[r for r in records if r['request_id'] in ids]
    with (case/'timings.csv').open('w') as f:
        w=csv.DictWriter(f,rows[0]);w.writeheader();w.writerows(rows)
    summary={stage:{k:float(np.mean([r[k] for r in rows if r['stage']==stage])) for k in ('cpu_ms','wall_ms')} for stage in sorted({r['stage'] for r in rows})}
    (case/'timings.json').write_text(json.dumps(summary,indent=2));print(summary)
if __name__=='__main__':main()
