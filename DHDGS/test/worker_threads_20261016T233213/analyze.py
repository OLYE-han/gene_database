"""Aggregate immutable benchmark reports; no search or product changes."""
from pathlib import Path
from collections import defaultdict
import csv,json,hashlib
import numpy as np
from dhdgs.metrics import summarize_query_timings,summarize_per_query_timings,export_timing_csvs
root=Path(__file__).resolve().parent
seed=20261016

def dump(name,rows):
    if not rows:return
    keys=list(dict.fromkeys(k for row in rows for k in row))
    with (root/name).open('w',newline='') as f:
        w=csv.DictWriter(f,fieldnames=keys);w.writeheader();w.writerows(rows)

def bootstrap(rows,field='latency_ms',quantile=99):
    groups=defaultdict(list)
    for q in rows:
        if field=='recall':groups[q['query_row']].append(q['recall'])
        elif q['status']=='ok':groups[q['query_row']].append(q['stats'][field])
    matrix=np.array([groups[k] for k in sorted(groups)])
    rng=np.random.default_rng(seed)
    idx=rng.integers(0,len(matrix),(2000,len(matrix)))
    samples=matrix[idx].reshape(2000,-1)
    values=samples.mean(axis=1) if field=='recall' else np.percentile(samples,quantile,axis=1)
    return [float(x) for x in np.percentile(values,[2.5,97.5])]

all_groups={};run_rows=[];query_rows=[];node_rows=[];points=[]
for stage in ('calibration','validation','confirmation'):
    summary=root/f'{stage}_results.json'
    if not summary.exists():continue
    for item in json.loads(summary.read_text()):
        r=json.loads(Path(item['report']).read_text());key=(stage,item['concurrency'],item['threads'])
        all_groups.setdefault(key,[]).append(r)
        run_rows.append(item)
        for q in r['queries']:
            s=q['stats'];qr={k:item[k] for k in ('stage','threads','concurrency','trial')}
            qr.update(query_row=q['query_row'],status=q['status'],recall=q['recall'],metrics_complete=s.get('metrics_complete'),control_errors=s.get('control_errors',0),rpc_retries=s.get('rpc_retries',0))
            for k in ('latency_ms','server_thread_cpu_ms','coordinator_thread_cpu_ms','pread_ms','rabitq_ms','exact_distance_ms',
                      'service_ms','pread_calls','file_read_bytes','all_protocol_bytes','peer_branch_rpc_count','expanded',
                      'duplicate_expansions','max_expanded_depth','node_revisit_count'):
                qr[k]=s.get(k)
            qr.update(s.get('latency_breakdown_ms',{}));query_rows.append(qr)
        for n in r['node_process_load']:
            nr={k:item[k] for k in ('stage','threads','concurrency','trial')}
            nr.update(shard_id=n['shard_id'],cpu_cores=n['mean_cpu_cores'],rss_bytes=n['process']['rss_bytes'],
                storage_read_bytes=n['process_deltas']['storage_read_bytes'],os_threads_after=n['process']['threads'],
                peak_expand_active=n['peak_active_since_start'],active_after=n['active_after'])
            node_rows.append(nr)
for (stage,c,w),reports in sorted(all_groups.items()):
    qs=[q for r in reports for q in r['queries']];ok=[q for q in qs if q['status']=='ok']
    lat=[q['stats']['latency_ms'] for q in ok]
    complete=[q for q in ok if q['stats'].get('metrics_complete') is True]
    n=len(qs);seconds=sum(r['measured_wall_seconds'] for r in reports)
    point={'stage':stage,'threads':w,'concurrency':c,'requests':n,'trials':len(reports),'failed':n-len(ok),
           'recall':float(np.mean([q['recall'] for q in qs])),'qps':len(ok)/seconds,
           'incomplete_metrics_requests':sum(q['stats'].get('metrics_complete') is not True for q in qs),
           'control_errors':sum(q['stats'].get('control_errors',0) for q in qs),
           'complete_resource_samples':len(complete),
           'mean_ms':float(np.mean(lat)), 'p50_ms':float(np.percentile(lat,50)),
           'p95_ms':float(np.percentile(lat,95)), 'p99_ms':float(np.percentile(lat,99)),
           'p999_ms':float(np.percentile(lat,99.9)), 'max_ms':float(max(lat)),
           'over_1000ms':sum(q['stats']['latency_ms']>1000 for q in ok),
           'first_wave_over_1000ms':sum(q['stats']['latency_ms']>1000 and q['job']<c for q in ok),
           'trial_p99_min':min(r['latency_ms']['p99'] for r in reports),'trial_p99_max':max(r['latency_ms']['p99'] for r in reports),
           'trial_qps_min':min(r['qps'] for r in reports),'trial_qps_max':max(r['qps'] for r in reports)}
    for k in ('server_thread_cpu_ms','pread_ms','rabitq_ms','exact_distance_ms','pread_calls','file_read_bytes',
              'all_protocol_bytes','peer_branch_rpc_count','expanded','duplicate_expansions'):
        values=[q['stats'][k] for q in complete if k in q['stats']]
        point['mean_'+k]=float(np.mean(values)) if values else None
    point['physical_read_bytes_total']=sum((node['process_deltas']['storage_read_bytes'] or 0) for r in reports for node in r['node_process_load'])
    point['node_revisits']=sum(q['stats'].get('node_revisit_count',0) for q in qs)
    point['trial_recall_min']=min(r['recall_at_k'] for r in reports)
    point['services_stopped']=all(s['stopped'] for r in reports for s in r['local_cluster']['services'])
    timings={'query_timing_summary':summarize_query_timings(qs),'per_query_timing_means':summarize_per_query_timings(qs)}
    directory=root/'aggregated'/stage/f'w{w}_c{c}';directory.mkdir(parents=True,exist_ok=True)
    
    if not (directory/'query_stage_means.csv').exists():
        export_timing_csvs(directory,timings['query_timing_summary'],timings['per_query_timing_means'])
    for name,value in timings['query_timing_summary']['wall_stages'].items():
        point['mean_'+name]=value['mean_ms']
    if stage!='calibration':
        lo,hi=bootstrap(qs);point.update(p99_ci_low=lo,p99_ci_high=hi)
        lo,hi=bootstrap(qs,'recall');point.update(recall_ci_low=lo,recall_ci_high=hi)
    points.append(point)
dump('runs_summary.csv',run_rows);dump('points_summary.csv',points);dump('all_queries.csv',query_rows);dump('all_nodes.csv',node_rows)
(root/'points_summary.json').write_text(json.dumps(points,indent=2))
# Paired query-cluster bootstrap: repeated requests stay in their query cluster.
comparisons=[]
selection=json.loads((root/'selection_before_validation.json').read_text())['selection']
for stage in ('validation','confirmation'):
    for item in selection:
        if item['status']!='selected':continue
        c=item['concurrency'];w=item['chosen']['threads']
        if (stage,c,w) not in all_groups or (stage,c,4) not in all_groups:continue
        chosen=next(p for p in points if (p['stage'],p['concurrency'],p['threads'])==(stage,c,w))
        baseline=next(p for p in points if (p['stage'],p['concurrency'],p['threads'])==(stage,c,4))
        arrays=[]
        for threads in (w,4):
            groups=defaultdict(list)
            for r in all_groups[stage,c,threads]:
                for q in r['queries']:
                    if q['status']=='ok':groups[q['query_row']].append(q['stats']['latency_ms'])
            arrays.append((sorted(groups),np.array([groups[k] for k in sorted(groups)])))
        assert arrays[0][0]==arrays[1][0]
        rng=np.random.default_rng(seed);idx=rng.integers(0,len(arrays[0][0]),(2000,len(arrays[0][0])))
        delta=np.percentile(arrays[0][1][idx].reshape(2000,-1),99,axis=1)-np.percentile(arrays[1][1][idx].reshape(2000,-1),99,axis=1)
        low,high=np.percentile(delta,[2.5,97.5])
        comparisons.append({'stage':stage,'concurrency':c,'chosen_threads':w,'baseline_threads':4,
            'chosen_p99_ms':chosen['p99_ms'],'baseline_p99_ms':baseline['p99_ms'],
            'p99_reduction_percent':100*(1-chosen['p99_ms']/baseline['p99_ms']),
            'p99_delta_ci_low':float(low),'p99_delta_ci_high':float(high),
            'chosen_qps':chosen['qps'],'baseline_qps':baseline['qps'],
            'chosen_recall':chosen['recall'],'chosen_recall_ci_low':chosen['recall_ci_low'],
            'failed':chosen['failed'],'descriptive_ci_supports_lower_p99':bool(high<0)})
dump('paired_comparisons.csv',comparisons)
(root/'paired_comparisons.json').write_text(json.dumps(comparisons,indent=2))
print(json.dumps({'points':len(points),'requests':len(query_rows),'comparisons':comparisons},indent=2))
