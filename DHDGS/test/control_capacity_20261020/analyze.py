"""Aggregate complete-query metrics; never sum overlapping branch waits as latency."""
from pathlib import Path
import argparse
import collections
import csv
import gzip
import json
import statistics
import os
import numpy as np

ROOT=Path(__file__).resolve().parent
os.environ.setdefault('MPLCONFIGDIR',str(ROOT/'.mplconfig'))
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

def write_csv(path,rows):
    if not rows:return
    with path.open('w') as f:
        w=csv.DictWriter(f,fieldnames=list(rows[0]));w.writeheader();w.writerows(rows)

def timeline(q):
    s=q['stats'];l=s['head_lane'];p=s['head_pipeline'];r=s['head_receiver']
    entry_return=s['head_timestamps']['entry_submit_return_ns']
    # Reports can finish before the entry ACK: assign the overlap to entry RPC,
    # and wait for both before merge. Never sum two overlapping waits.
    reports_ready=max(entry_return,r['query_ready_ns'])
    stamps=[('Client submit',q['client_start_ns']),('Admission',l['submitted_ns']),
        ('Input queue + IPC',l['worker_received_ns']),('Worker preparation',p['receive_enqueued_ns']),
        ('Receive queue',p['receive_started_ns']),('Receive handoff',p['route_enqueued_ns']),
        ('Route queue',p['route_started_ns']),('HNSW + route preparation',p['dispatch_enqueued_ns']),
        ('Dispatch queue',p['dispatch_started_ns']),('Register + request preparation',r['entry_submit_enter_ns']),
        ('Entry RPC incl. ACK',entry_return),('Wait for complete reports',reports_ready),
        ('Merge queue',p['merge_started_ns']),('Merge + metrics',p['merge_finished_ns']),
        ('Cleanup + persistence',l['worker_finished_ns']),('Output IPC',l['parent_received_ns']),
        ('Client consumer',q['client_end_ns'])]
    rows=[dict(stage=name,start_ms=(stamps[i-1][1]-stamps[0][1])/1e6,end_ms=(t-stamps[0][1])/1e6,
               duration_ms=(t-stamps[i-1][1])/1e6) for i,(name,t) in enumerate(stamps) if i]
    assert min(x['duration_ms'] for x in rows)>=0,q['request_id']
    assert abs(sum(x['duration_ms'] for x in rows)-q['client_wall_ms'])<.001
    for v in rows:v['fraction']=v['duration_ms']/q['client_wall_ms']
    return rows

def analyze(directory,selected,additional=None):
    root=ROOT/directory;groups=collections.defaultdict(list);rounds=[];queries={};controls=[];stages=[]
    paths=sorted(root.glob('*/result.json'))
    if additional:paths+=sorted((ROOT/additional).glob('*/result.json'))
    for path in paths:
        s=json.loads(path.read_text());case=path.parent;mode=s['mode'];run=Path(s['directory'])
        q=[json.loads(l) for l in gzip.open(run/'queries.jsonl.gz','rt')]
        assert all(x['status']=='ok' for x in q)
        assert all(s['audit'].values())
        case_label=str(case.relative_to(ROOT))
        queries[case_label]=q;ids={v['request_id'] for v in q}
        row=dict(case=case_label,mode=mode,qps=s['qps'],p99_ms=s['latency_ms']['p99'],
            mean_ms=s['latency_ms']['mean'],recall=s['recall_at_k_all_requests'],failed=s['failed_queries'],
            head_cpu_cores=sum(x['mean_cpu_cores'] for x in s['head_process_load']),
            head_rss_gib=sum(x['process']['rss_bytes'] for x in s['head_process_load'])/2**30,
            **{f'node_{x["shard_id"]}_cpu_cores':x['mean_cpu_cores'] for x in s['node_process_load']})
        for k in ('query_ready_queue_ms','pread_ms','exact_distance_ms','rabitq_ms','server_thread_cpu_ms',
                  'expanded','max_expanded_depth','max_cross_shard_depth','peer_branch_rpc_count','all_protocol_rpc_count','all_protocol_bytes','file_read_bytes'):
            row[k]=statistics.mean(v['stats'][k] for v in q)
        row['physical_read_bytes']=sum(x['process_deltas']['storage_read_bytes'] or 0 for x in s['node_process_load'])
        rounds.append(row);groups[mode].append(row)
        if q and 'head_lane' in q[0]['stats']:
            sums=collections.Counter()
            for value in q:
                for part in timeline(value):sums[part['stage']]+=part['duration_ms']
            for stage,total in sums.items():stages.append(dict(case=case_label,mode=mode,stage=stage,
                mean_ms=total/len(q),fraction_of_total_wall=total/sum(v['client_wall_ms'] for v in q)))
        before=json.load(gzip.open(run/'nodes_before.json.gz','rt'));after=json.load(gzip.open(run/'nodes_after.json.gz','rt'))
        for sid,node in after.items():
            events=[e for e in node['peer_lifecycle']['control_events'] if e.get('query_request_id') in ids and e.get('kind')=='result']
            trace=[e for e in node['peer_lifecycle']['execution_timestamps'] if e.get('query_request_id') in ids]
            v=dict(case=case_label,mode=mode,node=sid,events=len(events),
                control_errors=node['peer_lifecycle']['control_totals']['errors']-before[sid]['peer_lifecycle']['control_totals']['errors'])
            for key in ('pool_wait_ms','response_wait_ms','wall_ms'):
                values=[e['rpc_counts'][key] for e in events if key in e.get('rpc_counts',{})];v[key]=statistics.mean(values) if values else None
            for name,a,b in [('admission','enqueued_ns','dequeued_ns'),('executor_submit','submit_enter_ns','submit_return_ns'),
                    ('native_prepare','worker_state_lock_acquired_ns','native_call_ns'),('native_search','native_search_start_ns','native_loop_done_ns')]:
                vals=[(t[b]-t[a])/1e6 for t in trace if a in t and b in t]
                v[name+'_ms']=statistics.mean(vals) if vals else None
            controls.append(v)
    aggregate={m:{'rounds':len(rows),'queries':sum(len(queries[x['case']]) for x in rows),
        **{k:statistics.median(x[k] for x in rows) if k in ('qps','p99_ms') else statistics.mean(x[k] for x in rows)
           for k in rounds[0] if k not in ('case','mode')}} for m,rows in groups.items()}
    (root/'analysis.json').write_text(json.dumps(aggregate,indent=2))
    write_csv(root/'rounds.csv',rounds);write_csv(root/'control_windows.csv',controls)
    write_csv(root/'stage_means.csv',stages)
    metrics=[dict(mode=m,**v) for m,v in aggregate.items()];write_csv(root/'metrics.csv',metrics)
    modes=list(aggregate);fig,axes=plt.subplots(1,2,figsize=(11,4))
    for ax,key,title in zip(axes,['qps','p99_ms'],['Complete-query QPS','Complete-query p99 (ms)']):
        bars=ax.bar(modes,[aggregate[m][key] for m in modes]);ax.set_title(title)
        ax.bar_label(bars,fmt='%.1f');ax.tick_params(axis='x',rotation=20)
    fig.tight_layout();fig.savefig(root/'comparison.png',dpi=160);plt.close(fig)
    case=next((r['case'] for r in rounds if r['mode']==selected),None)
    if case is not None and 'head_lane' in queries[case][0]['stats']:
        q=queries[case][int(np.random.default_rng(42).integers(len(queries[case])))];rows=timeline(q)
        write_csv(root/'selected_query_timeline.csv',rows)
        (root/'selected_query.json').write_text(json.dumps(q,indent=2))
        fig,ax=plt.subplots(figsize=(11,7));colors=plt.cm.tab20(np.linspace(0,1,len(rows)))
        ax.barh([x['stage'] for x in rows],[x['duration_ms'] for x in rows],left=[x['start_ms'] for x in rows],color=colors)
        for i,v in enumerate(rows):ax.text(v['end_ms']+.5,i,f"{v['duration_ms']:.3f} ms ({v['fraction']:.1%})",va='center',fontsize=8)
        ax.invert_yaxis();ax.set_xlim(0,q['client_wall_ms']*1.28);ax.set_xlabel('Milliseconds since client submit')
        ax.set_title(f"Query {q['query_id']} | {q['client_wall_ms']:.3f} ms | Recall@10 {q['recall']:.0%}")
        fig.tight_layout();fig.savefig(root/'query_timeline.png',dpi=160);plt.close(fig)
    print(json.dumps(aggregate,indent=2))

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('directory');p.add_argument('--selected',default='optimized');p.add_argument('--additional');a=p.parse_args();analyze(a.directory,a.selected,a.additional)
