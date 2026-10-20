"""Test-only gates: prove a route worker never waits for dispatch/ACK/results."""
import os
for k in ('OPENBLAS_NUM_THREADS','MKL_NUM_THREADS','OMP_NUM_THREADS'):os.environ[k]='1'
from pathlib import Path
import copy,json,sys,threading,time
import numpy as np
import yaml
R=Path(__file__).resolve().parent;P=R.parents[1]
sys.path[:0]=[str(P/'tools'),str(P/'python')]
from cluster_service import load_plan
from cluster_ctl import request
from dhdgs.managed_cluster import ClusterHead
from dhdgs.managed_node import revision,save_json
from full_truth import FullTruth

def distribution(values):
    return dict(mean=float(np.mean(values)),p99=float(np.percentile(values,99)),max=float(max(values)))

def verify(head,truth,rows,kind):
    flow=head.engine.head_flow;hub=head.engine.head_hub
    gate=threading.Event();all_routed=threading.Event();tls=threading.local();lock=threading.Lock();records=[]
    original_step=flow._step;original_enqueue=hub.enqueue
    original_submit=hub.submit_query;original_exchange=hub.exchange;original_finish=hub._finish
    def step(rec,operation):
        if operation is not None:return original_step(rec,operation)
        stamp=dict(thread=threading.get_ident(),thread_name=threading.current_thread().name,
                   start_ns=time.monotonic_ns(),cpu_start_ns=time.thread_time_ns())
        tls.stamp=stamp
        try:return original_step(rec,operation)
        finally:
            stamp.update(return_ns=time.monotonic_ns(),cpu_end_ns=time.thread_time_ns(),timing=rec['timing'])
            with lock:
                records.append(stamp)
                if len(records)==len(rows):all_routed.set()
            tls.stamp=None
    def enqueue(fn):
        stamp=tls.stamp;stamp['enqueue_enter_ns']=time.monotonic_ns()
        f=original_enqueue(fn);stamp['enqueue_return_ns']=time.monotonic_ns();return f
    def hold():
        if not gate.wait(20):raise TimeoutError('Diagnostic gate exceeded safety limit')
    def submit(*args,**kwargs):
        if kind=='dispatch':hold()
        return original_submit(*args,**kwargs)
    def exchange(endpoint,message,*args,**kwargs):
        reply=original_exchange(endpoint,message,*args,**kwargs)
        if kind=='ack' and message.get('op')=='peer_submit':hold()
        return reply
    def finish(s,item):
        if kind=='completion' and not gate.is_set():return
        return original_finish(s,item)
    flow._step=step;hub.enqueue=enqueue;hub.submit_query=submit;hub.exchange=exchange;hub._finish=finish
    futures=[];started=time.monotonic_ns()
    try:
        futures=[head.engine.search_async(truth.queries[row],int(truth.qids[row])) for row in rows]
        assert all_routed.wait(10),'Route worker did not return for all requests'
        sentinel=flow.route.submit(lambda:dict(thread=threading.get_ident(),time_ns=time.monotonic_ns())).result(2)
        assert not any(f.done() for f in futures),'Query completed through closed gate'
        assert len({r['thread'] for r in records})==1
        assert sentinel['thread']==records[0]['thread']
        if kind=='completion':
            until=time.monotonic()+10
            while True:
                with hub.lock:items=[(hub.sessions[k],v) for k,v in hub.pending.items()]
                complete=0
                for s,item in items:
                    with s.delivery_lock:complete+=int(item['entry'] is not None and s.registry.complete())
                if complete==len(rows):break
                if time.monotonic()>until:raise TimeoutError('Full reports did not arrive')
                time.sleep(.005)
        else:complete=None
        state=dict(dispatcher=hub.dispatcher.metrics(),pending_outputs=sum(not f.done() for f in futures),
                   full_report_sets=complete,sentinel=sentinel,all_routes_returned_before_gate_open=True)
        opened=time.monotonic_ns();gate.set()
        if kind=='completion':
            with hub.lock:items=[(hub.sessions[k],v) for k,v in hub.pending.items()]
            for s,item in items:
                with s.delivery_lock:original_finish(s,item)
        results=[f.result(30) for f in futures]
        assert all(q['status']=='ok' for q in results)
        assert all(q['stats']['expanded']<=200 and q['stats'].get('node_revisit_count',0)==0 for q in results)
        for rec in records:
            assert rec['return_ns']<opened
            rec['after_enqueue_return_ms']=(rec['return_ns']-rec['enqueue_return_ns'])/1e6
            rec['route_worker_wall_ms']=(rec['return_ns']-rec['start_ns'])/1e6
            rec['route_worker_cpu_ms']=(rec['cpu_end_ns']-rec['cpu_start_ns'])/1e6
        summary=dict(gate=kind,queries=len(rows),route_workers=1,observed_route_threads=1,
                     all_routes_ms=(max(r['return_ns'] for r in records)-started)/1e6,
                     gate_hold_ms=(opened-started)/1e6,successes=len(results),
                     recall=float(np.mean([len(set(q['ids'])&set(truth.ids[row,:10]))/10 for q,row in zip(results,rows)])),
                     before_release=state,
                     measurements={k:distribution([r[k] for r in records]) for k in
                         ('after_enqueue_return_ms','route_worker_wall_ms','route_worker_cpu_ms')})
        save_json(R/f'release_{kind}.json',dict(summary=summary,records=records))
        print(json.dumps(summary),flush=True)
    finally:
        gate.set()
        flow._step=original_step;hub.enqueue=original_enqueue;hub.submit_query=original_submit
        hub.exchange=original_exchange;hub._finish=original_finish
        with hub.lock:items=[(hub.sessions[k],v) for k,v in hub.pending.items()]
        for s,item in items:
            with s.delivery_lock:original_finish(s,item)
        for f in futures:
            try:f.result(30)
            except Exception:pass
    head.quiescent()

def main():
    e=yaml.safe_load((R/'experiment.yaml').read_text());base=load_plan(e['base_plan'])
    old=request(base,'status');assert old['ready'] and not old['busy'] and not old['active_queries']
    plan=load_plan(R/'candidate.yaml');plan['dhdgs']['peer']['head_port']=22589
    plan['dhdgs']['peer']['head_pipeline']['route_workers']=1
    truth=FullTruth(plan['control']['truth_plan'])
    rows=json.loads(Path(plan['control']['query_selection']).read_text())['validation'][:128]
    head=ClusterHead(plan['dhdgs'],plan['control'],Path(plan['control']['token_file']).read_text().strip(),
                     lambda row,k:list(map(int,truth.ids[row,:k])))
    try:
        states=head.status()['nodes']
        assert all(s.get('quiescent') and s.get('accepting') and s.get('revision')==revision(head.c) for s in states)
        head.ready=True
        for row in rows[:32]:head.engine.search(truth.queries[row],int(truth.qids[row]))
        for kind in ('dispatch','ack','completion'):verify(head,truth,rows,kind)
        save_json(R/'release_final_status.json',head.status())
    finally:head.close()

if __name__=='__main__':main()
