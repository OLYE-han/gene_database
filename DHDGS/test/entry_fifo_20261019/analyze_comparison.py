from pathlib import Path
import csv,gzip,json
import numpy as np
R=Path(__file__).resolve().parent
rows=[]
for case in sorted((R/'cases').iterdir()):
    if not (case/'result.json').exists():continue
    s=json.loads((case/'result.json').read_text())
    with gzip.open(Path(s['directory'])/'queries.jsonl.gz','rt') as f:qs=[json.loads(line) for line in f]
    row=dict(case=case.name,mode=s['scheduling'],qps=s['qps'],mean_ms=s['latency_ms']['mean'],
             p99_ms=s['latency_ms']['p99'],max_ms=s['latency_ms']['max'],recall=s['recall_at_k_all_requests'],failed=s['failed_queries'])
    data={k:[] for k in ('pool','release','release_lock','rpc','post_rpc','after_entry','data_ready','compute')}
    for q in qs:
        st=q['stats'];rpc=st['entry_rpc'];t=st['head_timestamps']
        values=dict(pool=rpc['pool_wait_ms'],release=rpc['pool_release_ms'],release_lock=rpc['pool_release_lock_wait_ms'],
            rpc=rpc['wall_ms']-rpc['pool_wait_ms'],
            post_rpc=(t['entry_submit_return_ns']-rpc['rpc_release_done_ns'])/1e6,
            after_entry=(t['all_reports_received_ns']-t['entry_submit_return_ns'])/1e6,
            data_ready=st['query_ready_queue_ms'],compute=st['query_compute_slice_wall_ms'])
        for k,v in values.items():data[k].append(v)
    for k,v in data.items():
        row[k+'_mean_ms']=float(np.mean(v));row[k+'_p99_ms']=float(np.percentile(v,99));row[k+'_max_ms']=float(max(v))
    row['pool_over100_count']=sum(v>100 for v in data['pool'])
    row['api_over100_count']=sum(q['client_wall_ms']>100 for q in qs)
    rows.append(row)
if rows:
    with (R/'comparison.csv').open('w') as f:
        w=csv.DictWriter(f,list(rows[0]));w.writeheader();w.writerows(rows)
groups={mode:{k:float(np.median([r[k] for r in rows if r['mode']==mode]))
              for k in rows[0] if type(rows[0][k]) in (int,float)} for mode in sorted({r['mode'] for r in rows})} if rows else {}
(R/'analysis.json').write_text(json.dumps(dict(runs=rows,group_medians=groups),indent=2))
print(json.dumps(groups,indent=2))
