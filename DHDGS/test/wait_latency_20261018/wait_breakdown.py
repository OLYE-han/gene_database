"""Observed scheduling waits, with interval union instead of branch-time addition.

This is not a CPU off-CPU profiler: durations include scheduler/GIL effects and
small operations between timestamps. Exposed means no same-query native search
or entry routing overlaps the recorded waiting interval. Unknown waits remain
unknown; never infer pure waiting as wall time minus thread CPU time.
"""
from pathlib import Path
import argparse,csv,gzip,json
import numpy as np

R=Path(__file__).resolve().parent
def distribution(v):
    return {"count":len(v),"mean":float(np.mean(v)),"p50":float(np.percentile(v,50)),
            "p99":float(np.percentile(v,99)),"max":float(max(v))} if v else None

def union(intervals):
    out=[]
    for a,b in sorted(intervals):
        if b<=a:continue
        if out and a<=out[-1][1]:out[-1]=(out[-1][0],max(out[-1][1],b))
        else:out.append((a,b))
    return out

def subtract(intervals,occupied):
    out=[]
    for a,b in union(intervals):
        cursor=a
        for c,d in union(occupied):
            if d<=cursor:continue
            if c>=b:break
            if c>cursor:out.append((cursor,min(c,b)))
            cursor=max(cursor,d)
        if cursor<b:out.append((cursor,b))
    return out

def analyze(source):
    with gzip.open(source,'rt') as f:r=json.load(f)
    ts={};events={}
    for node in r['node_process_load']:
        life=node['peer_lifecycle_after']
        for t in life['execution_timestamps']:ts[t['branch_id']]=t
        for e in life['control_events']:events.setdefault(e['branch_id'],[]).append(e)
    values={};rows=[]
    def value(name,v):values.setdefault(name,[]).append(v)
    for q in r['queries']:
        reports=q['branch_reports']
        if not all('native_loop_done_ns' in ts.get(b['branch_id'],{}) for b in reports):continue
        intervals=[];busy=[];start=q['client_start_ns'];end=q['client_end_ns']
        def interval(label,a,b):
            a=max(start,a);b=min(end,b)
            if b<a:return
            value(label,(b-a)/1e6);intervals.append((a,b))
        head=q['stats'].get('head_timestamps',{})
        if 'routing_done_ns' in head:busy.append((start,head['routing_done_ns']))
        rpc=q['stats']['entry_rpc']
        if 'rpc_start_ns' in rpc:interval('entry_pool_ms',rpc['rpc_start_ns'],rpc['pool_acquired_ns'])
        per_query={}
        for b in reports:
            t=ts[b['branch_id']]
            for name,a,z in [('admission_queue_ms','enqueued_ns','dequeued_ns'),
                              ('compute_queue_ms','executor_enqueue_ns','executor_dequeue_ns'),
                              ('worker_state_lock_ms','worker_enter_ns','worker_state_lock_acquired_ns'),
                              ('gil_reacquire_ms','native_loop_done_ns','native_gil_reacquired_ns'),
                              ('transfer_queue_ms','transfer_enqueue_return_ns','transfer_worker_enter_ns'),
                              ('accept_state_lock_ms','accept_state_lock_enter_ns','accept_state_lock_acquired_ns')]:
                if a in t and z in t:interval(name,t[a],t[z])
            value('accepted_to_worker_ms',(t['worker_enter_ns']-t['accepted_ns'])/1e6)
            if 'accept_thread_cpu_return_ns' in t:
                value('accept_cpu_ms',(t['accept_thread_cpu_return_ns']-t['accept_thread_cpu_enter_ns'])/1e6)
                value('accept_lock_held_wall_ms',(t['accept_state_lock_released_ns']-t['accept_state_lock_acquired_ns'])/1e6)
            busy.append((t['native_search_start_ns'],t['native_loop_done_ns']))
            value('native_loop_to_worker_free_ms',(t['worker_available_ns']-t['native_loop_done_ns'])/1e6)
            value('native_return_to_worker_free_ms',(t['worker_available_ns']-t['native_return_ns'])/1e6)
            # Finalization waits can overlap a child search or in-flight ACK.
            # They are reported separately and excluded from the queue union.
            value('native_return_to_report_ms',(t['report_ready_ns']-t['native_return_ns'])/1e6)
            e=min((e for e in events[b['branch_id']] if e['kind']=='result'),key=lambda e:e['message_ready_ns'])
            interval('report_ready_to_sender_ms',t['report_ready_ns'],max(t['report_ready_ns'],e['sender_enter_ns']))
            if 'local_done_consumed_ns' in t:
                interval('completion_queue_ms',t['local_search_done_ns'],t['local_done_consumed_ns'])
            if 'local_done_enqueued_ns' in t:
                value('completion_queue_all_branches_ms',(t['local_done_dequeued_ns']-t['local_done_enqueued_ns'])/1e6)
            elif t.get('completion_publish_path')=='try_lock':
                value('completion_queue_all_branches_ms',0.)
            for name,a,z in [('local_done_dequeue_ms','local_done_enqueued_ns','local_done_dequeued_ns'),
                              ('local_done_state_update_ms','local_done_dequeued_ns','local_done_consumed_ns'),
                              ('consumed_to_finalization_ms','local_done_consumed_ns','finalize_enter_ns'),
                              ('finalize_lock_ms','finalize_enter_ns','finalize_acquired_ns'),
                              ('finalize_publish_ms','finalize_acquired_ns','report_ready_ns')]:
                if a in t and z in t:value(name,(t[z]-t[a])/1e6)
        observed=union(intervals);exposed=subtract(observed,busy)
        row={'request_id':q['request_id'],'query_id':q['query_id'],'latency_ms':q['client_wall_ms'],
             'observed_queue_union_ms':sum(b-a for a,b in observed)/1e6,
             'exposed_queue_union_ms':sum(b-a for a,b in exposed)/1e6}
        assert 0<=row['exposed_queue_union_ms']<=row['observed_queue_union_ms']<=row['latency_ms']+1e-6
        rows.append(row)
    out=R/'analysis'/source.parents[2].name/source.parent.name;out.mkdir(parents=True,exist_ok=True)
    if rows:
        with (out/'query_wait_union.csv').open('w') as f:
            w=csv.DictWriter(f,list(rows[0]));w.writeheader();w.writerows(rows)
    result={'group':source.parents[2].name,'run':source.parent.name,'traced_queries':len(rows),
            'branch_stage_ms':{k:distribution(v) for k,v in values.items()},
            'query_observed_queue_union_ms':distribution([q['observed_queue_union_ms'] for q in rows]),
            'query_exposed_queue_union_ms':distribution([q['exposed_queue_union_ms'] for q in rows]),
            'exposed_queue_over_100ms':sum(q['exposed_queue_union_ms']>=100 for q in rows),
            'selected_query':next((q for q in rows if q['query_id']==734744),None)}
    (out/'wait_summary.json').write_text(json.dumps(result,indent=2))
    return result

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--groups',nargs='*');args=p.parse_args()
    rows=[]
    for s in sorted(R.glob('*/runs/*/report.json.gz')):
        if args.groups and s.parents[2].name not in args.groups:continue
        rows.append(analyze(s));print(json.dumps(rows[-1]),flush=True)
    (R/'wait_summaries.json').write_text(json.dumps(rows,indent=2))
