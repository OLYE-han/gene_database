"""Full-query measurements, node-local return traffic and deterministic query plot."""
from pathlib import Path
import csv,gzip,json
import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
R=Path(__file__).resolve().parent
def load(p):return json.loads(p.read_text())
def gz(p):
    with gzip.open(p,'rt') as f:return json.load(f)
def dist(v):return dict(count=len(v),mean=float(np.mean(v)),p99=float(np.percentile(v,99)),max=float(max(v)))
def parts(q,mode):
    s=q['stats']
    if mode=='direct':
        t=s['head_direct'];stamps=[q['client_start_ns'],t['distribution_started_ns'],t['distribution_send_done_ns'],
            t['result_enqueued_ns'],t['result_started_ns'],t['result_finished_ns'],q['client_end_ns']]
    else:
        t=s['head_pipeline'];stamps=[q['client_start_ns'],t['route_started_ns'],s['entry_rpc']['send_done_ns'],
            t['merge_enqueued_ns'],t['merge_started_ns'],t['merge_finished_ns'],q['client_end_ns']]
    values=np.diff(stamps)/1e6
    assert min(values)>=0 and abs(sum(values)-q['client_wall_ms'])<1e-6
    return dict(zip(('admission_queue','route_through_send','wait_after_send','result_queue','result_work','return_to_caller'),map(float,values)))
groups={};rounds=[];windows=[];metric_rows=[];sample=None
sample_job=int(np.random.default_rng(42).integers(2000))
for mode in ('q16','direct'):
    queries=[];summaries=[];control={k:[] for k in ('pool_ms','send_to_ack_ms','report_ready_to_selected_ms')}
    for p in sorted(R.glob('cases/*_'+mode+'/result.json')):
        s=load(p);summaries.append(s);path=Path(s['directory'])
        with gzip.open(path/'queries.jsonl.gz','rt') as f:qs=[json.loads(x) for x in f]
        queries.extend(qs);ids={q['request_id'] for q in qs}
        assert s['failed_queries']==0 and all(s['audit'].values())
        rounds.append(dict(case=p.parent.name,qps=s['qps'],p99_ms=s['latency_ms']['p99'],mean_ms=s['latency_ms']['mean'],
            recall=s['recall_at_k_all_requests'],failed=s['failed_queries'],
            head_cpu_cores=sum(x['mean_cpu_cores'] for x in s['head_process_load']),
            head_rss_bytes=sum(x['process']['rss_bytes'] for x in s['head_process_load'])))
        before=gz(path/'nodes_before.json.gz');after=gz(path/'nodes_after.json.gz')
        for sid,n in after.items():
            lc=n['peer_lifecycle'];old=before[sid]['peer_lifecycle']
            events=[e for e in lc['control_events'] if e['query_request_id'] in ids and e['kind']=='result']
            timestamps={e['branch_id']:e for e in lc['execution_timestamps'] if e['query_request_id'] in ids}
            errors=lc['control_totals']['errors']-old['control_totals']['errors'];failed=sum(not e['succeeded'] for e in events)
            assert errors==0 and failed==0
            windows.append(dict(case=p.parent.name,node=sid,events=len(events),errors=errors,failed_events=failed,
                physical_read_bytes=n['process']['storage_read_bytes']-before[sid]['process']['storage_read_bytes'],pid=n['process']['pid']))
            for e in events:
                control['pool_ms'].append(e['rpc_counts']['pool_wait_ms'])
                control['send_to_ack_ms'].append((e['sender_rpc_return_ns']-e['sender_enter_ns'])/1e6)
                t=timestamps[e['branch_id']]
                control['report_ready_to_selected_ms'].append((t['control_result_selected_ns']-t['native_report_ready_ns'])/1e6)
        if mode=='direct':
            assert len(s['pipeline_status']['result_processes'])==16
            assert {q['stats']['result_process_pid'] for q in qs}<=set(s['pipeline_status']['result_processes'])
            assert not any(q['stats']['entry_submit_retries'] for q in qs)
            stored={}
            for file in p.parent.glob('worker_results/results-*.jsonl'):
                for line in file.open():
                    q=json.loads(line);stored[q['request_id']]=q
            assert all(stored[q['request_id']]['ids']==q['ids'] for q in qs)
            if sample is None:sample=qs[sample_job];(R/'selected_query.json').write_text(json.dumps(sample,indent=2))
    keys=['head_route_queue_ms','head_distribution_queue_ms','head_result_queue_ms','result_return_ipc_ms','result_write_ms',
        'query_ready_queue_ms','query_branch_wall_ms','pread_ms','exact_distance_ms','rabitq_ms','coordinator_thread_cpu_ms',
        'server_thread_cpu_ms','expanded','max_expanded_depth','max_cross_shard_depth','all_protocol_bytes','all_protocol_rpc_count',
        'peer_branch_rpc_count','file_read_bytes']
    metrics={k:dist([q['stats'][k] for q in queries if k in q['stats']]) for k in keys if k in queries[0]['stats']}
    for k,v in metrics.items():metric_rows.append(dict(mode=mode,metric=k,**v))
    stages=[parts(q,mode) for q in queries]
    groups[mode]=dict(queries=len(queries),qps_median=float(np.median([s['qps'] for s in summaries])),
        p99_median_ms=float(np.median([s['latency_ms']['p99'] for s in summaries])),
        api_ms=dist([q['client_wall_ms'] for q in queries]),recall=float(np.mean([q['recall'] for q in queries])),
        stages={k:dist([t[k] for t in stages]) for k in stages[0]},metrics=metrics,
        control={k:dist(v) for k,v in control.items()},
        mean_head_cpu_cores=float(np.mean([sum(x['mean_cpu_cores'] for x in s['head_process_load']) for s in summaries])),
        mean_head_rss_bytes=float(np.mean([sum(x['process']['rss_bytes'] for x in s['head_process_load']) for s in summaries])))
for name,rows in [('rounds',rounds),('control_windows',windows),('metrics',metric_rows)]:
    with (R/(name+'.csv')).open('w') as f:
        w=csv.DictWriter(f,fieldnames=list(rows[0]));w.writeheader();w.writerows(rows)
(R/'analysis.json').write_text(json.dumps(groups,indent=2))
fig,ax=plt.subplots(figsize=(11,4));left=np.zeros(2)
for k in groups['direct']['stages']:
    width=[groups[m]['stages'][k]['mean'] for m in ('q16','direct')]
    ax.barh(['Previous pipeline (16 route)','Direct (16 threads / 2 confirm / 16 processes)'],width,left=left,label=k.replace('_',' '));left+=width
ax.set_xlabel('Mean complete API latency (ms), 6000 queries per group')
ax.legend(loc='upper center',bbox_to_anchor=(.5,-.2),ncol=3,fontsize=8);fig.tight_layout();fig.savefig(R/'comparison.png',dpi=160);plt.close(fig)
t=sample['stats']['head_direct'];base=sample['client_start_ns'];rec=sample['stats']['head_receiver']
timeline=[('API admission',base,t['submitted_ns']),('Distribution queue',t['submitted_ns'],t['distribution_started_ns']),
    ('Route + request send',t['distribution_started_ns'],t['distribution_send_done_ns']),
    ('Wait for confirmation + reports',t['distribution_send_done_ns'],t['result_enqueued_ns']),
    ('Result queue + input IPC',t['result_enqueued_ns'],t['result_started_ns']),
    ('Merge + metrics + persistence',t['result_started_ns'],t['result_finished_ns']),
    ('Output IPC + callback',t['result_finished_ns'],t['result_received_ns']),
    ('Benchmark consumer',t['result_received_ns'],sample['client_end_ns'])]
rows=[dict(stage=k,start_ms=(a-base)/1e6,end_ms=(b-base)/1e6,duration_ms=(b-a)/1e6) for k,a,b in timeline]
with (R/'selected_query_timeline.csv').open('w') as f:
    w=csv.DictWriter(f,fieldnames=list(rows[0]));w.writeheader();w.writerows(rows)
assert all(r['duration_ms']>=0 for r in rows)
fig,ax=plt.subplots(figsize=(11,5))
for i,row in enumerate(rows):
    ax.barh(i,row['duration_ms'],left=row['start_ms'],color=plt.cm.tab10(i));ax.text(row['end_ms']+.5,i,f"{row['duration_ms']:.3f} ms",va='center',fontsize=8)
ax.set_yticks(range(len(rows)),[r['stage'] for r in rows]);ax.invert_yaxis();ax.set_xlabel('Time since API submission (ms), head host clock only')
ax.set_title(f"Query {sample['query_id']} (seed 42, job {sample_job}), API {sample['client_wall_ms']:.3f} ms")
ax.set_xlim(0,sample['client_wall_ms']*1.18);fig.tight_layout();fig.savefig(R/'query_timeline.png',dpi=160);plt.close(fig)
print(json.dumps({k:{name:v[name] for name in ('qps_median','p99_median_ms','mean_head_cpu_cores','mean_head_rss_bytes')} for k,v in groups.items()},indent=2))
print('selected query',sample['query_id'],'job',sample_job,'API ms',sample['client_wall_ms'])
