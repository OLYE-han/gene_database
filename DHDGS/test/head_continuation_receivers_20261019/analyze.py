from pathlib import Path
import json,gzip,csv
import numpy as np
R=Path(__file__).resolve().parent
pairs=[('command_queue','register_parent_enqueued_ns','register_parent_dequeued_ns'),('command_transit','register_parent_dequeued_ns','register_receiver_received_ns'),('receiver_setup','register_receiver_received_ns','registered_ns'),('ack_transit','register_ack_publish_ns','register_ack_parent_received_ns'),('ack_apply','register_ack_parent_received_ns','register_ack_parent_applied_ns'),('register_resume','register_ack_parent_applied_ns','register_return_ns'),('terminal_ipc','terminal_publish_ns','terminal_parent_received_ns'),('terminal_apply','terminal_parent_received_ns','terminal_parent_applied_ns'),('entry_queue','entry_enqueued_ns','entry_start_ns')]
rows=[]
for p in sorted((R/'cases').glob('*/result.json')):
 s=json.loads(p.read_text());d=Path(s['directory'])
 with gzip.open(d/'queries.jsonl.gz','rt') as f:qs=[json.loads(l) for l in f]
 vals={k:[] for k,_,_ in pairs}
 for k in ('registration_ready','registration_block','head_stage_queue','head_stage_resume_queue','entry_pool','after_entry'):vals[k]=[]
 for q in qs:
  st=q['stats'];m=dict(st['head_receiver']);t=st['head_timestamps'];m['entry_start_ns']=t['entry_submit_enter_ns']
  for k,a,b in pairs:
   if a in m and b in m:vals[k].append((m[b]-m[a])/1e6)
  vals['registration_ready'].append((m['registered_ns']-m['register_enter_ns'])/1e6)
  vals['registration_block'].append(m['registration_ms'])
  for k in ('head_stage_queue','head_stage_resume_queue'):vals[k].append(st.get(k+'_ms',0))
  vals['entry_pool'].append(st['entry_rpc']['pool_wait_ms'])
  vals['after_entry'].append((t['all_reports_received_ns']-t['entry_submit_return_ns'])/1e6)
 row=dict(case=p.parent.name,mode=s.get('ipc_mode',s.get('head_workers',s.get('receiver_processes'))),qps=s['qps'],mean_ms=s['latency_ms']['mean'],p50_ms=s['latency_ms']['p50'],p99_ms=s['latency_ms']['p99'],recall=s['recall_at_k_all_requests'],failed=s['failed_queries'],api_over100=sum(q['client_wall_ms']>100 for q in qs),**{k+'_mean':float(np.mean(v)) if v else None for k,v in vals.items()},**{k+'_p99':float(np.percentile(v,99)) if v else None for k,v in vals.items()})
 rows.append(row)
 if not all(s['audit'].values()):raise RuntimeError('Failed audit')
with (R/'comparison.csv').open('w') as f:
 w=csv.DictWriter(f,rows[0]);w.writeheader();w.writerows(rows)
medians=[]
for mode in sorted({r['mode'] for r in rows}):
 group=[r for r in rows if r['mode']==mode]
 medians.append(dict(mode=mode,runs=len(group),**{k:float(np.median([r[k] for r in group])) for k in group[0] if k not in ('case','mode') and group[0][k] is not None}))
(R/'analysis.json').write_text(json.dumps(dict(rows=rows,medians=medians),indent=2))
print(json.dumps(medians,indent=2))
