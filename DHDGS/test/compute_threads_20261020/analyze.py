"""Matched-case aggregate metrics and completion windows; analysis after tests."""
from pathlib import Path
from collections import defaultdict
import csv
import gzip
import json
import os
import statistics
import sys
import numpy as np

ROOT=Path(__file__).resolve().parent
os.environ.setdefault('MPLCONFIGDIR',str(ROOT/'.mplconfig'))
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt


def write_csv(path, rows):
    if not rows:return
    keys=list(dict.fromkeys(k for r in rows for k in r))
    with path.open('w') as f:
        w=csv.DictWriter(f,fieldnames=keys);w.writeheader();w.writerows(rows)


def main():
    rows=[];node_rows=[];window_rows=[]
    for path in sorted(ROOT.glob('*/*/result.json')):
        result=json.loads(path.read_text());run=Path(result['directory']);case=path.parent
        d=result['query_metric_distributions']
        r=dict(phase=result['phase'],case=case.name,threads=result['threads'],queries=result['query_count'],
               qps=result['qps'],mean_ms=result['latency_ms']['mean'],p99_ms=result['latency_ms']['p99'],
               recall=result['recall_at_k_all_requests'],failed=result['failed_queries'],audit=all(result['audit'].values()),
               ready_wait_ms=d['query_ready_queue_ms']['mean'],ready_p99_ms=d['query_ready_queue_ms']['p99'],
               search_cpu_ms=d['server_thread_cpu_ms']['mean'],pread_ms=d['pread_ms']['mean'],
               rabitq_ms=d['rabitq_ms']['mean'],exact_ms=d['exact_distance_ms']['mean'],
               rpc=d['all_protocol_rpc_count']['mean'],wire_bytes=d['all_protocol_bytes']['mean'],
               logical_read_bytes=d['file_read_bytes']['mean'],expanded=d['expanded']['mean'],
               head_cpu_cores=sum(v['mean_cpu_cores'] for v in result['head_process_load']))
        for n in result['node_process_load']:
            r[f"node{n['shard_id']}_cpu_cores"]=n['mean_cpu_cores']
        r['physical_read_bytes']=sum(n['process_deltas']['storage_read_bytes'] or 0 for n in result['node_process_load'])
        before=json.load(gzip.open(run/'nodes_before.json.gz','rt'))
        after=json.load(gzip.open(run/'nodes_after.json.gz','rt'))
        formal_ids=set();expected_traces=defaultdict(int)
        with (run/'queries.csv').open() as f:
            for query in csv.DictReader(f):
                formal_ids.add(query['request_id'])
                for sid in json.loads(query['visited_nodes']):expected_traces[str(sid)]+=1
        for sid,n in after.items():
            old=before[sid];traces=[t for t in n['peer_lifecycle']['execution_timestamps']
                                   if t.get('query_request_id') in formal_ids]
            nr=dict(phase=result['phase'],case=case.name,threads=result['threads'],node=sid,retained_traces=len(traces),
                    expected_traces=expected_traces[sid],trace_coverage=len(traces)/expected_traces[sid] if expected_traces[sid] else None,
                    control_errors=n['peer_lifecycle']['control_totals']['errors']-old['peer_lifecycle']['control_totals']['errors'])
            pool=n['peer_lifecycle'].get('compute_queue') or {}
            nr['actual_pool_workers']=pool.get('workers')
            nr['compute_peak_queued']=pool.get('peak_queued')
            nr['compute_capacity_rejections']=pool.get('capacity_rejections')
            nr['admission_peak_pending_start']=n['peer_lifecycle'].get('admission_peak_pending_start')
            nr['retained_branches_after']=n['peer_lifecycle'].get('retained_branches')
            nr['process_threads_after']=n['process'].get('threads')
            if nr['actual_pool_workers'] is not None:
                assert nr['actual_pool_workers']==result['threads']
            # Same-node interval overlap, without sampling or cross-host clocks.
            # Native wall intervals can include preemption and are not CPU-running time.
            for label,begin,end in [('native','native_search_start_ns','native_loop_done_ns'),
                                    ('worker','worker_enter_ns','worker_available_ns')]:
                events=sorted((stamp,delta) for t in traces if begin in t and end in t
                              for stamp,delta in [(t[begin],1),(t[end],-1)])
                active=peak=0;area=full=0;last=events[0][0] if events else 0
                for stamp,delta in events:
                    span=stamp-last;area+=span*active
                    if active>=result['threads']:full+=span
                    active+=delta;peak=max(active,peak);last=stamp
                duration=events[-1][0]-events[0][0] if events else 0
                nr[label+'_peak_concurrent']=peak
                nr[label+'_mean_concurrent_in_trace_span']=area/duration if duration else None
                nr[label+'_full_capacity_fraction_in_trace_span']=full/duration if duration else None
            for name,a,b in [('admission','enqueued_ns','dequeued_ns'),('executor','dequeued_ns','worker_enter_ns'),
                             ('state_lock_interval','worker_enter_ns','worker_state_lock_acquired_ns'),
                             ('native_search','native_search_start_ns','native_loop_done_ns'),
                             ('release_after_search','native_loop_done_ns','worker_available_ns')]:
                values=[(t[b]-t[a])/1e6 for t in traces if a in t and b in t]
                if values:
                    nr[name+'_mean_ms']=statistics.mean(values);nr[name+'_p99_ms']=float(np.percentile(values,99))
            gc=n.get('gc_profile') or {};old_gc=old.get('gc_profile') or {}
            nr['gc_count']=gc.get('count',0)-old_gc.get('count',0)
            nr['gc_wall_ms']=gc.get('wall_ms',0)-old_gc.get('wall_ms',0)
            nr['gc_cpu_ms']=gc.get('cpu_ms',0)-old_gc.get('cpu_ms',0)
            gc_events=[e for e in gc.get('events',[]) if old['process']['monotonic_ns']<=e['end_ns']<=n['process']['monotonic_ns']]
            nr['max_retained_gc_ms']=max((e['wall_ms'] for e in gc_events),default=0)
            r[f'node{sid}_gc_wall_ms']=nr['gc_wall_ms']
            node_rows.append(nr)
        rows.append(r)
        if result['phase']=='sustained':
            bins=defaultdict(list)
            with gzip.open(run/'queries.jsonl.gz','rt') as f:
                for line in f:
                    q=json.loads(line);seconds=(q['client_end_ns']-q['offered_ns'])/1e9
                    bins[int(seconds//10)].append((q['client_wall_ms'],q['stats'].get('query_ready_queue_ms'),q['status']))
            for i in range(int(result['measured_wall_seconds']//10)+1):
                values=bins[i];start=10*i;end=min(start+10,result['measured_wall_seconds'])
                if end<=start:continue
                window_rows.append(dict(case=case.name,threads=result['threads'],start_s=start,end_s=end,
                    partial=end-start<10,completed=len(values),qps=sum(v[2]=='ok' for v in values)/(end-start),
                    p99_ms=float(np.percentile([v[0] for v in values],99)) if values else None,
                    ready_ms=statistics.mean(v[1] for v in values) if values else None))
    write_csv(ROOT/'rounds.csv',rows);write_csv(ROOT/'nodes.csv',node_rows);write_csv(ROOT/'windows.csv',window_rows)
    groups=defaultdict(list)
    for r in rows:groups[(r['phase'],r['threads'])].append(r)
    aggregate=[]
    for (phase,threads),rs in sorted(groups.items()):
        a=dict(phase=phase,threads=threads,rounds=len(rs),queries=sum(r['queries'] for r in rs),
               qps_median=statistics.median(r['qps'] for r in rs),qps_min=min(r['qps'] for r in rs),qps_max=max(r['qps'] for r in rs),
               p99_median_ms=statistics.median(r['p99_ms'] for r in rs),failed=sum(r['failed'] for r in rs),
               audit=all(r['audit'] for r in rs))
        for key in ('mean_ms','recall','ready_wait_ms','ready_p99_ms','search_cpu_ms','pread_ms','rabitq_ms','exact_ms',
                    'rpc','wire_bytes','logical_read_bytes','expanded','head_cpu_cores','physical_read_bytes',
                    'node0_cpu_cores','node1_cpu_cores','node2_cpu_cores','node0_gc_wall_ms','node1_gc_wall_ms','node2_gc_wall_ms'):
            a[key]=statistics.mean(r[key] for r in rs)
        aggregate.append(a)
    write_csv(ROOT/'summary.csv',aggregate);(ROOT/'summary.json').write_text(json.dumps(aggregate,indent=2))
    phases=list(dict.fromkeys(r['phase'] for r in aggregate))
    fig,axes=plt.subplots(len(phases),3,figsize=(13,4*len(phases)),squeeze=False)
    for pi,phase in enumerate(phases):
        values=[r for r in aggregate if r['phase']==phase];x=np.arange(len(values))
        for ax,key,label in zip(axes[pi],('qps_median','p99_median_ms','ready_wait_ms'),('QPS (median of rounds)','p99 ms (median of rounds)','Cumulative ready wait / query (ms)')):
            bars=ax.bar(x,[r[key] for r in values],color='#4a82b7');ax.bar_label(bars,fmt='%.1f',fontsize=9)
            if key in ('qps_median','p99_median_ms'):
                rawkey='qps' if key=='qps_median' else 'p99_ms'
                for j,r in enumerate(values):
                    ax.scatter([j]*len(groups[(phase,r['threads'])]),[s[rawkey] for s in groups[(phase,r['threads'])]],s=18,color='#cd5746',zorder=4)
            ax.set_xticks(x,[str(r['threads']) for r in values]);ax.set_xlabel('Compute threads per data node')
            ax.set_title(phase+': '+label);ax.margins(y=.18)
    fig.suptitle('Fixed concurrency 256 / budget 200 / 3 data nodes / 4 head workers')
    fig.tight_layout();fig.savefig(ROOT/'comparison.png',dpi=160);plt.close(fig)
    if window_rows:
        fig,axes=plt.subplots(2,1,figsize=(12,7),sharex=True)
        for case in dict.fromkeys(r['case'] for r in window_rows):
            rs=[r for r in window_rows if r['case']==case];x=[(r['start_s']+r['end_s'])/2 for r in rs]
            axes[0].plot(x,[r['qps'] for r in rs],marker='.',label=case)
            axes[1].plot(x,[r['p99_ms'] for r in rs],marker='.',label=case)
        axes[0].set_ylabel('Completed queries / second');axes[1].set_ylabel('Completion-window p99 (ms)')
        axes[1].set_xlabel('Seconds since run start (10-second windows; last window may be partial)')
        for ax in axes:ax.grid(alpha=.25);ax.legend()
        fig.tight_layout();fig.savefig(ROOT/'sustained_windows.png',dpi=160);plt.close(fig)
    print(json.dumps(aggregate,indent=2))


if __name__=='__main__':main()
