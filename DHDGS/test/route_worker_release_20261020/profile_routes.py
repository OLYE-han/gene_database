"""Diagnostic instrumentation only; no changes to search decisions or production code."""
import os
for k in ('OPENBLAS_NUM_THREADS','MKL_NUM_THREADS','OMP_NUM_THREADS'):os.environ[k]='1'
from pathlib import Path
import argparse,json,sys,threading,time
from concurrent.futures import Future,ThreadPoolExecutor,wait,FIRST_COMPLETED
import numpy as np
R=Path(__file__).resolve().parent;P=R.parents[1]
sys.path[:0]=[str(P/'tools'),str(P/'python')]
from cluster_service import load_plan
from cluster_ctl import request
from dhdgs.managed_cluster import ClusterHead
from dhdgs.managed_node import revision,save_json
from full_truth import FullTruth
import dhdgs.search as search

def dist(a):return dict(mean=float(np.mean(a)),p99=float(np.percentile(a,99)),max=float(max(a)))

class Probe:
    def __init__(self,engine):
        self.engine=engine;self.local=threading.local();self.records=[]
        self.router=engine.router;self.quantizer=engine.quantizer
        class Proxy:
            def __init__(proxy,obj,method,label):proxy.obj=obj;proxy.method=method;proxy.label=label
            def __getattr__(proxy,name):
                fn=getattr(proxy.obj,name)
                return self.wrap(fn,proxy.label) if name==proxy.method else fn
        engine.router=Proxy(self.router,'search','hnsw')
        engine.quantizer=Proxy(self.quantizer,'score','rabitq')
        self.signature=search.execution_signature;search.execution_signature=self.wrap(self.signature,'signature')
        self.prepare=search.prepare;search.prepare=self.wrap(self.prepare,'prepare')
        self.enqueue=engine.head_hub.enqueue;engine.head_hub.enqueue=self.wrap(self.enqueue,'enqueue')
        self.step=engine.head_flow._step
        def step(rec,op):
            if op is not None:return self.step(rec,op)
            return self.measure(lambda:self.step(rec,op),rec['timing'])
        engine.head_flow._step=step
    def wrap(self,fn,label):
        def call(*a,**kw):
            rec=getattr(self.local,'record',None)
            if rec is None:return fn(*a,**kw)
            w=time.monotonic_ns();c=time.thread_time_ns()
            try:return fn(*a,**kw)
            finally:rec[label]=dict(wall_ms=(time.monotonic_ns()-w)/1e6,cpu_ms=(time.thread_time_ns()-c)/1e6)
        return call
    def measure(self,fn,timing=None):
        rec=dict(thread=threading.get_native_id());self.local.record=rec
        w=time.monotonic_ns();c=time.thread_time_ns()
        try:return fn()
        finally:
            rec['total']=dict(wall_ms=(time.monotonic_ns()-w)/1e6,cpu_ms=(time.thread_time_ns()-c)/1e6)
            rec['other']={k:rec['total'][k]-sum(v[k] for name,v in rec.items() if name not in ('total','thread')) for k in ('wall_ms','cpu_ms')}
            if timing is not None:rec['queue_ms']=(timing['route_started_ns']-timing['route_enqueued_ns'])/1e6
            self.records.append(rec);self.local.record=None
    def summary(self):
        r=self.records
        keys=set.intersection(*[set(x)-{'thread','queue_ms'} for x in r])
        return dict(count=len(r),threads=len({x['thread'] for x in r}),
            stages={k:{clock:dist([x[k][clock] for x in r]) for clock in ('wall_ms','cpu_ms')} for k in sorted(keys)})
    def close(self):
        self.engine.router=self.router;self.engine.quantizer=self.quantizer
        search.execution_signature=self.signature;search.prepare=self.prepare
        self.engine.head_hub.enqueue=self.enqueue;self.engine.head_flow._step=self.step

def bounded(pool,fn,rows):
    rows=iter(rows);active=set()
    for _ in range(128):
        row=next(rows,None)
        if row is not None:active.add(pool.submit(fn,row))
    while active:
        done,active=wait(active,return_when=FIRST_COMPLETED)
        for f in done:
            f.result();row=next(rows,None)
            if row is not None:active.add(pool.submit(fn,row))

def main(n):
    plan=load_plan(R/'candidate.yaml');c=plan['dhdgs']
    c['peer']['head_port']=22592+(n==16);c['peer']['head_pipeline']['route_workers']=n
    case=R/f'profile_{n}';case.mkdir(exist_ok=False);plan['control']['results_directory']=str(case)
    truth=FullTruth(plan['control']['truth_plan'])
    rows=json.loads(Path(plan['control']['query_selection']).read_text())['validation'][:2000]
    head=ClusterHead(c,plan['control'],Path(plan['control']['token_file']).read_text().strip(),lambda row,k:list(map(int,truth.ids[row,:k])))
    probe=None
    try:
        assert all(s.get('quiescent') and s.get('accepting') and s.get('revision')==revision(head.c) for s in head.status()['nodes'])
        head.ready=True
        # Warm actual remote search before enabling the instrumentation.
        for row in rows[:32]:head.engine.search(truth.queries[row],int(truth.qids[row]))
        probe=Probe(head.engine)
        result=head.benchmark(dict(queries=2000,concurrency=128,warmup=0))
        save_json(case/'full_benchmark.json',result)
        assert result['failed_queries']==0 and all(result['audit'].values())
        save_json(case/'full_profile.json',dict(summary=probe.summary(),records=probe.records));probe.records=[]
        # Same exact route preparation, terminating at yield without dispatch.
        head.engine.head_hub.enqueue=probe.wrap(lambda fn:Future(),'enqueue')
        def isolated(row):
            g=head.engine._search_steps(truth.queries[row],int(truth.qids[row]),pipeline_timing={})
            try:probe.measure(lambda:next(g))
            finally:g.close()
        for row in rows[:32]:isolated(row)
        probe.records=[]
        with ThreadPoolExecutor(n) as pool:
            start=time.monotonic();bounded(pool,isolated,rows);elapsed=time.monotonic()-start
        save_json(case/'isolated_profile.json',dict(summary=probe.summary(),elapsed_s=elapsed,
                routes_per_second=len(rows)/elapsed,records=probe.records));probe.records=[]
        # HNSW kernel and Python binding only; no request construction/RaBitQ.
        def hnsw(row):
            probe.measure(lambda:head.engine.router.search(truth.queries[row],16,64))
        for row in rows[:32]:hnsw(row)
        probe.records=[]
        with ThreadPoolExecutor(n) as pool:
            start=time.monotonic();bounded(pool,hnsw,rows);elapsed=time.monotonic()-start
        save_json(case/'hnsw_profile.json',dict(summary=probe.summary(),elapsed_s=elapsed,
                routes_per_second=len(rows)/elapsed,records=probe.records))
        print(json.dumps(dict(threads=n,full_qps=result['qps'],hnsw_qps=len(rows)/elapsed)),flush=True)
    finally:
        if probe:probe.close()
        head.close()

if __name__=='__main__':
    a=argparse.ArgumentParser();a.add_argument('threads',type=int,choices=[1,16]);main(a.parse_args().threads)
