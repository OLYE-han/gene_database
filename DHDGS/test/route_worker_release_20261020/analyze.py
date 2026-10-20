"""Reproduce route queue, CPU/wall profiles and control-return diagnosis."""
from pathlib import Path
import csv,gzip,json
import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
R=Path(__file__).resolve().parent
def read(p):return json.loads(p.read_text())
def gz(p):
    with gzip.open(p,'rt') as f:return json.load(f)
def dist(v):return dict(mean=float(np.mean(v)),p99=float(np.percentile(v,99)))
groups={};cases=[];control_rows=[]
for mode in ('q1','q16'):
    queries=[];control={k:[] for k in ('ready_to_sender_ms','rpc_ms','pool_ms','response_ms','done_to_sender_ms','done_to_select_ms')}
    summaries=[]
    for p in sorted(R.glob('cases/*_'+mode+'/result.json')):
        s=read(p);summaries.append(s);d=Path(s['directory'])
        with gzip.open(d/'queries.jsonl.gz','rt') as f:qs=[json.loads(x) for x in f]
        queries+=qs;ids={q['request_id'] for q in qs}
        before=gz(d/'nodes_before.json.gz');after=gz(d/'nodes_after.json.gz')
        cases.append(dict(case=p.parent.name,qps=s['qps'],p99_ms=s['latency_ms']['p99'],mean_ms=s['latency_ms']['mean'],recall=s['recall_at_k_all_requests'],failed=s['failed_queries']))
        assert s['failed_queries']==0 and all(s['audit'].values())
        for node,x in after.items():
            lifecycle=x['peer_lifecycle'];old=before[node]['peer_lifecycle']
            events=[e for e in lifecycle['control_events'] if e['query_request_id'] in ids and e['kind']=='result']
            ex={e['branch_id']:e for e in lifecycle['execution_timestamps'] if e['query_request_id'] in ids}
            failures=sum(not e['succeeded'] for e in events)
            errors=lifecycle['control_totals']['errors']-old['control_totals']['errors']
            assert errors==0 and failures==0
            control_rows.append(dict(case=p.parent.name,node=node,events=len(events),errors=errors,
                physical_read_bytes=x['process']['storage_read_bytes']-before[node]['process']['storage_read_bytes'],
                failed_result_attempts=failures,pid=x['process']['pid']))
            for e in events:
                counts=e['rpc_counts'];t=ex[e['branch_id']]
                for name,value in dict(ready_to_sender_ms=(e['sender_enter_ns']-e['queued_ns'])/1e6,
                    rpc_ms=(e['sender_rpc_return_ns']-e['sender_enter_ns'])/1e6,
                    pool_ms=counts['pool_wait_ms'],response_ms=counts['response_wait_ms'],
                    done_to_sender_ms=(e['sender_enter_ns']-t['native_report_ready_ns'])/1e6,
                    done_to_select_ms=(t['control_result_selected_ns']-t['native_report_ready_ns'])/1e6).items():control[name].append(value)
    stats={k:dist([q['stats'][k] for q in queries]) for k in ('head_receive_queue_ms','head_route_queue_ms','head_dispatch_queue_ms','head_merge_queue_ms',
        'query_ready_queue_ms','query_branch_wall_ms','coordinator_thread_cpu_ms','server_thread_cpu_ms','expanded','max_expanded_depth',
        'max_cross_shard_depth','all_protocol_bytes','all_protocol_rpc_count','file_read_bytes','pread_ms','rabitq_ms','exact_distance_ms')}
    parts={k:[] for k in ('receive_and_admit','route_queue','route_work','dispatch_queue','dispatch_to_ready','merge_queue','merge_and_return')}
    for q in queries:
        t=q['stats']['head_pipeline']
        values=[(t['route_enqueued_ns']-q['client_start_ns'])/1e6,
            (t['route_started_ns']-t['route_enqueued_ns'])/1e6,
            (t['dispatch_enqueued_ns']-t['route_started_ns'])/1e6,
            (t['dispatch_started_ns']-t['dispatch_enqueued_ns'])/1e6,
            (t['merge_enqueued_ns']-t['dispatch_started_ns'])/1e6,
            (t['merge_started_ns']-t['merge_enqueued_ns'])/1e6,
            (q['client_end_ns']-t['merge_started_ns'])/1e6]
        assert min(values)>=0 and abs(sum(values)-q['client_wall_ms'])<1e-6
        for k,v in zip(parts,values):parts[k].append(v)
    groups[mode]=dict(queries=len(queries),qps_median=float(np.median([s['qps'] for s in summaries])),
        p99_median=float(np.median([s['latency_ms']['p99'] for s in summaries])),mean_latency_ms=float(np.mean([q['client_wall_ms'] for q in queries])),
        recall=float(np.mean([q['recall'] for q in queries])),head_cpu_cores=float(np.mean([s['head_process_load'][0]['mean_cpu_cores'] for s in summaries])),
        metrics=stats,control={k:dist(v) for k,v in control.items()},stages={k:dist(v) for k,v in parts.items()})
for name,rows in [('cases',cases),('control_windows',control_rows)]:
    with (R/(name+'.csv')).open('w') as f:
        w=csv.DictWriter(f,fieldnames=list(rows[0]));w.writeheader();w.writerows(rows)
(R/'analysis.json').write_text(json.dumps(groups,indent=2))
fig,ax=plt.subplots(figsize=(11,4));left=np.zeros(2)
for label in groups['q1']['stages']:
    widths=[groups[m]['stages'][label]['mean'] for m in ('q1','q16')]
    ax.barh(['1 route worker','16 route workers'],widths,left=left,label=label.replace('_',' '));left+=widths
ax.set_xlabel('Mean complete API latency (ms), 6000 queries / group');ax.legend(loc='upper center',bbox_to_anchor=(.5,-.2),ncol=3,fontsize=8)
fig.tight_layout();fig.savefig(R/'queue_shift.png',dpi=160);plt.close(fig)
fig,axes=plt.subplots(1,2,figsize=(11,4))
for label in ('hnsw','isolated'):
    axes[0].plot([1,16],[read(R/f'profile_{n}/{label}_profile.json')['routes_per_second'] for n in (1,16)],'o-',label=label)
axes[0].set(xlabel='Route workers',ylabel='Routes / second',title='Diagnostic: HNSW vs complete route preparation');axes[0].legend()
labels=['1 / isolated','1 / full','16 / isolated','16 / full'];cpus=[];waits=[]
for n,kind in ((1,'isolated'),(1,'full'),(16,'isolated'),(16,'full')):
    total=read(R/f'profile_{n}/{kind}_profile.json')['summary']['stages']['total']
    cpus.append(total['cpu_ms']['mean']);waits.append(total['wall_ms']['mean']-total['cpu_ms']['mean'])
axes[1].bar(labels,cpus,label='Thread CPU');axes[1].bar(labels,waits,bottom=cpus,label='Wall minus CPU (not all GIL)')
axes[1].set(ylabel='Mean route worker residence (ms)',title='Test-only timing wrappers');axes[1].tick_params(axis='x',rotation=25);axes[1].legend(fontsize=8)
fig.tight_layout();fig.savefig(R/'route_profile.png',dpi=160);plt.close(fig)
print(json.dumps({k:{name:v[name] for name in ('qps_median','p99_median','mean_latency_ms','recall')} for k,v in groups.items()},indent=2))
