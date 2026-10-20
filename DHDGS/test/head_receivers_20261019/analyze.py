from pathlib import Path
import csv,gzip,json
import numpy as np
R=Path(__file__).resolve().parent
rows=[];details=[]
for case in sorted((R/'cases').iterdir()):
    if not (case/'result.json').exists():continue
    result=json.loads((case/'result.json').read_text());d=Path(result['directory'])
    with gzip.open(d/'queries.jsonl.gz','rt') as f:qs=[json.loads(line) for line in f]
    vals={k:[] for k in ('pool','after_entry','registration','ipc_delivery','apply','resume','callback_completion','ready_queue','compute','rpc','wire','pread','expanded','cross_depth','report_ready_to_sender','report_sender_ack')}
    for q in qs:
        s=q['stats'];t=s['head_timestamps'];m=s.get('head_receiver',{})
        vals['pool'].append(s['entry_rpc']['pool_wait_ms'])
        vals['after_entry'].append((t['all_reports_received_ns']-t['entry_submit_return_ns'])/1e6)
        for name,a,b in [('ipc_delivery','terminal_publish_ns','terminal_parent_received_ns'),('apply','terminal_parent_received_ns','terminal_parent_applied_ns'),('callback_completion','last_callback_dispatch_ns','terminal_publish_ns')]:
            if a in m:vals[name].append((m[b]-m[a])/1e6)
        if m:
            vals['registration'].append(m['registration_ms'])
            vals['resume'].append((t['all_reports_received_ns']-max(m['terminal_parent_applied_ns'],t['entry_submit_return_ns']))/1e6)
        for name,key in [('ready_queue','query_ready_queue_ms'),('compute','server_thread_cpu_ms'),('rpc','all_protocol_rpc_count'),('wire','all_protocol_bytes'),('pread','pread_ms'),('expanded','expanded'),('cross_depth','max_cross_shard_depth')]:
            vals[name].append(s.get(key,0))
    with gzip.open(d/'nodes_after.json.gz','rt') as f:nodes=json.load(f)
    query_ids={q['request_id'] for q in qs}
    for node in nodes.values():
        lifecycle=node.get('peer_lifecycle',{})
        stamps={t['branch_id']:t for t in lifecycle.get('execution_timestamps',[]) if t['query_request_id'] in query_ids}
        for event in lifecycle.get('control_events',[]):
            if event['query_request_id'] not in query_ids or event['kind']!='result':continue
            t=stamps.get(event['branch_id'],{})
            if 'report_ready_ns' in t:
                vals['report_ready_to_sender'].append(max(0,event['sender_enter_ns']-t['report_ready_ns'])/1e6)
                vals['report_sender_ack'].append((event['sender_rpc_return_ns']-event['sender_enter_ns'])/1e6)
    row=dict(case=case.name,receivers=result['receivers'],qps=result['qps'],mean=result['latency_ms']['mean'],p50=result['latency_ms']['p50'],p99=result['latency_ms']['p99'],recall=result['recall_at_k_all_requests'],failures=result['failed_queries'],api_over100=sum(q['client_wall_ms']>100 for q in qs),**{k+'_mean':float(np.mean(v)) if v else None for k,v in vals.items()},**{k+'_p99':float(np.percentile(v,99)) if v else None for k,v in vals.items()})
    rows.append(row)
    details.append(dict(case=case.name,loads=result['node_process_load'],audits=result['audit']))
with (R/'comparison.csv').open('w') as f:
    w=csv.DictWriter(f,rows[0].keys());w.writeheader();w.writerows(rows)
medians=[]
for count in sorted({r['receivers'] for r in rows}):
    group=[r for r in rows if r['receivers']==count]
    medians.append(dict(receivers=count,runs=len(group),**{key:float(np.median([r[key] for r in group])) for key in group[0] if key not in ('case','receivers') and group[0][key] is not None}))
(R/'analysis.json').write_text(json.dumps(dict(medians=medians,rows=rows,details=details),indent=2))
print(json.dumps(medians,indent=2))
