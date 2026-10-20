from pathlib import Path
import gzip,json,numpy as np
R=Path(__file__).resolve().parent
rows=[]
def dist(values):
 return dict(mean=float(np.mean(values)),p50=float(np.percentile(values,50)),p99=float(np.percentile(values,99)),max=float(max(values))) if values else None
for source in sorted(R.glob('*/runs/*/report.json.gz')):
 with gzip.open(source,'rt') as f:r=json.load(f)
 qids={q['request_id'] for q in r['queries']};qs=r['queries'];ts=[]
 assert len(qs)==len(qids)==2000
 for q in qs:
  assert q['status']=='ok' and q['stats']['metrics_complete'] and q['stats']['expanded']<=200
  assert q['recall']==len(set(q['ids'])&set(q['truth_ids']))/10
  assert q['stats']['node_revisit_count']==q['stats']['router_expand_rpc_count']==0
  for b in q['branch_reports']:
   assert b['compute_thread_migrations']==0
   assert b['work_grant']==b['work_spent']+b['work_unused']+sum(ch['work_grant'] for ch in b['children'])
 for n in r['node_process_load']:ts += [t for t in n['peer_lifecycle_after']['execution_timestamps'] if t['query_request_id'] in qids]
 row={'group':source.parents[2].name,'run':source.parent.name,'mean_ms':r['latency_ms']['mean'],'p99_ms':r['latency_ms']['p99'],'qps':r['qps'],'recall':r['recall_at_k_all_requests']}
 for label,a,b in [('native_return_to_worker_free','native_return_ns','worker_available_ns'),('native_loop_to_worker_free','native_loop_done_ns','worker_available_ns'),('native_loop_to_gil','native_loop_done_ns','native_gil_reacquired_ns'),('native_report_materialize','native_gil_reacquired_ns','native_report_ready_ns'),('accepted_to_worker','accepted_ns','worker_enter_ns'),('native_search_wall','native_search_start_ns','native_loop_done_ns')]:
  row[label]=dist([(t[b]-t[a])/1e6 for t in ts if a in t and b in t])
 row['worker_free_before_report_count']=sum(t.get('worker_available_ns',1e100)<t['report_ready_ns'] for t in ts);row['traced_branches']=len(ts);row['branches']=sum(len(q['branch_reports']) for q in qs)
 for name,a,b in [('receiver_accept_wall','receiver_accept_enter_ns','receiver_accept_return_ns'),('receiver_ack_send_wall','receiver_ack_send_enter_ns','receiver_ack_send_return_ns')]:
  row[name]=dist([(t[b]-t[a])/1e6 for t in ts if a in t and b in t])
 by_branch={t['branch_id']:t for t in ts};ack_to_received=[]
 for q in qs:
  root=next(b for b in q['branch_reports'] if b['parent_branch_id'] is None)
  t=by_branch.get(root['branch_id'],{});rpc=q['stats'].get('entry_rpc',{})
  if 'receiver_ack_send_enter_ns' in t and 'response_received_ns' in rpc:ack_to_received.append((rpc['response_received_ns']-t['receiver_ack_send_enter_ns'])/1e6)
 row['root_ack_enter_to_client_receive']=dist(ack_to_received)
 for key in ['pool_wait_ms','connect_ms','send_ms','response_wait_ms','wall_ms']:
  row['entry_'+key]=dist([q['stats']['entry_rpc'][key] for q in qs if key in q['stats'].get('entry_rpc',{})])
 for key in ['peer_transfer_queue_ms','peer_pool_wait_ms','peer_response_wait_ms','server_thread_cpu_ms','coordinator_thread_cpu_ms','query_transfer_finalize_wait_ms','query_compute_slice_wall_ms']:
  row[key]=dist([q['stats'][key] for q in qs])
 row['routing_wall_ms']=dist([q['stats']['latency_breakdown_ms']['routing_wall_ms'] for q in qs])
 before=json.loads((source.parent/'nodes_before.json').read_text())
 row['control_scheduler_nodes']=[]
 for n in r['node_process_load']:
  after=n['peer_lifecycle_after']['control_scheduler_totals'];prior=before[str(n['shard_id'])]['peer_lifecycle']['control_scheduler_totals']
  row['control_scheduler_nodes'].append({k:v-prior.get(k,0) for k,v in after.items()})
 rows.append(row)
 print(json.dumps(row),flush=True)
(R/'diagnostics.json').write_text(json.dumps(rows,indent=2))
