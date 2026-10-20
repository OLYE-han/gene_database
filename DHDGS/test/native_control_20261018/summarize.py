"""Final frozen-build comparison; pooled work counts are not additive latency."""
from pathlib import Path
import csv,gzip,json
import numpy as np
R=Path(__file__).resolve().parent
rows=[]
for rel in json.loads((R/'final_runs.json').read_text()):
 p=R/rel
 with gzip.open(p,'rt') as f:r=json.load(f)
 s=json.loads((R/'analysis'/p.parents[2].name/p.parent.name/'wait_summary.json').read_text())
 row=dict(group=p.parents[2].name,run=p.parent.name,queries=r['query_count'],failed=r['failed_queries'],qps=r['qps'],mean_ms=r['latency_ms']['mean'],p99_ms=r['latency_ms']['p99'],recall=r['recall_at_k_all_requests'],actual_peak=r['actual_peak_inflight'])
 for name in ('all_protocol_rpc_count','all_protocol_bytes','file_read_bytes','pread_calls','pread_ms','rabitq_ms','exact_distance_ms','server_thread_cpu_ms','expanded','max_expanded_depth','peer_branch_rpc_count','peer_pool_wait_ms','peer_transfer_wall_ms'):
  row['mean_'+name]=float(np.mean([q['stats'].get(name,0) for q in r['queries']]))
 for name in ('admission_queue_ms','compute_queue_ms','completion_queue_all_branches_ms','report_ready_to_sender_ms','native_loop_to_worker_free_ms','loop_done_to_report_ms','gil_reacquire_ms'):
  v=s['branch_stage_ms'][name]
  for key in ('mean','p99','max'):row[name+'_'+key]=v[key]
 for key in ('mean','p99','max'):row['exposed_queue_union_ms_'+key]=s['query_exposed_queue_union_ms'][key]
 row['exposed_queue_over_100ms']=s['exposed_queue_over_100ms']
 row['physical_read_bytes']=sum(n['process_deltas']['storage_read_bytes'] for n in r['node_process_load'])
 row['node_cpu_cores']=[n['mean_cpu_cores'] for n in r['node_process_load']]
 rows.append(row)
(R/'comparison.json').write_text(json.dumps(rows,indent=2))
with (R/'comparison.csv').open('w') as f:
 w=csv.DictWriter(f,list(rows[0]));w.writeheader();w.writerows(rows)
for row in rows:
 print(row['group'],row['run'],{k:round(row[k],3) for k in ('qps','mean_ms','p99_ms','recall','mean_all_protocol_rpc_count','mean_file_read_bytes','mean_server_thread_cpu_ms','mean_peer_pool_wait_ms')})
