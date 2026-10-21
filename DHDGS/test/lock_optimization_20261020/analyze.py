import csv,gzip,json
from pathlib import Path
import numpy as np
ROOT=Path(__file__).resolve().parent
rows=[];nodes=[];connection_events=[]
for path in sorted(ROOT.glob('*/*/result.json')):
 r=json.loads(path.read_text());run=Path(r['directory']);d=r['query_metric_distributions'];row=dict(case=str(path.parent.relative_to(ROOT)),qps=r['qps'],mean_ms=r['latency_ms']['mean'],p99_ms=r['latency_ms']['p99'],recall=r['recall_at_k_all_requests'],failed=r['failed_queries'],audit=all(r['audit'].values()),ready_ms=d['query_ready_queue_ms']['mean'],search_cpu_ms=d['server_thread_cpu_ms']['mean']);rows.append(row)
 n=json.load(gzip.open(run/'nodes_after.json.gz','rt'));formal=set()
 with (run/'queries.csv').open() as f:
  for q in csv.DictReader(f):formal.add(q['request_id'])
 for sid,data in n.items():
  ts=[t for t in data['peer_lifecycle']['execution_timestamps'] if t['query_request_id'] in formal];nr=dict(case=str(path.parent.relative_to(ROOT)),node=sid,traces=len(ts));nodes.append(nr)
  for name,a,b in [('admission','enqueued_ns','dequeued_ns'),('queue_lock','admission_lock_enter_ns','admission_lock_acquired_ns'),('submit_lock','submit_state_lock_enter_ns','submit_state_lock_acquired_ns'),('submit','submit_enter_ns','submit_return_ns'),('executor','executor_enqueue_ns','executor_dequeue_ns'),('worker_lock','worker_enter_ns','worker_state_lock_acquired_ns'),('native','native_search_start_ns','native_loop_done_ns'),('accept_lock','accept_state_lock_enter_ns','accept_state_lock_acquired_ns'),('accept_hold','accept_state_lock_acquired_ns','accept_state_lock_released_ns'),('publish','native_loop_done_ns','control_result_selected_ns')]:
   vals=[(t[b]-t[a])/1e6 for t in ts if a in t and b in t]
   if vals:nr.update({name+'_mean':float(np.mean(vals)),name+'_p99':float(np.percentile(vals,99)),name+'_max':max(vals)})
  nr['consumers']=len(set(t.get('admission_thread_id') for t in ts));nr['empty_waits']=sum(t.get('admission_waited_empty',False) for t in ts)
  nr['adaptive']=data['peer_lifecycle'].get('adaptive_admission')
  conn=data['peer_lifecycle'].get('adaptive_connections')
  if conn:
   nr['connection_peak_limits']=str([(e['endpoint'],e['peak_limit']) for e in conn['endpoints']])
   connection_events.append(dict(case=str(path.parent.relative_to(ROOT)),node=sid,**conn))
  nr['actual_search_workers']=data['peer_lifecycle']['compute_queue']['workers']
  assert nr['actual_search_workers']==32
  gc=data.get('gc_profile',{});nr['gc_wall_ms']=gc.get('wall_ms')
(ROOT/'connection_events.json').write_text(json.dumps(connection_events,indent=2))
for name,rs in [('rounds',rows),('nodes',nodes)]:
 (ROOT/(name+'.json')).write_text(json.dumps(rs,indent=2));keys=list(dict.fromkeys(k for r in rs for k in r))
 with (ROOT/(name+'.csv')).open('w') as f:
  w=csv.DictWriter(f,fieldnames=keys);w.writeheader();w.writerows(rs)
print(json.dumps(rows,indent=2))
print(json.dumps([{k:v for k,v in r.items() if k in ['case','node','admission_mean','queue_lock_mean','queue_lock_max','submit_mean','executor_mean','worker_lock_mean','native_mean','consumers','empty_waits']} for r in nodes],indent=2))
