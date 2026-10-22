"""Offline A/B phases from measured request IDs, never warmup/retention history."""
from pathlib import Path
import json,gzip,collections,statistics,argparse
import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
ROOT=Path(__file__).resolve().parent
PHASES=[('receive','receiver_message_ready_ns','accepted_ns'),('admission','accepted_ns','enqueued_ns'),('runtime_lock','accept_state_lock_enter_ns','accept_state_lock_acquired_ns'),('compute_ready','enqueued_ns','native_search_start_ns'),('search','native_search_start_ns','native_loop_done_ns'),('report_worker','native_loop_done_ns','compute_released_ns'),('completion_queue','local_done_enqueued_ns','local_done_dequeued_ns'),('publication','local_done_dequeued_ns','report_ready_ns'),('register_command_queue','native_register_enqueued_ns','native_register_consumed_ns')]
def dist(x):
 return dict(count=len(x),mean=float(np.mean(x)),p50=float(np.percentile(x,50)),p95=float(np.percentile(x,95)),p99=float(np.percentile(x,99))) if x else dict(count=0)
parser=argparse.ArgumentParser();parser.add_argument('--progress',default='progress.json');parser.add_argument('--output',default='comparison');args=parser.parse_args()
rows=json.loads((ROOT/args.progress).read_text());out=[]
for row in rows:
 run=Path(row['directory']);case=ROOT/'cases'/row['name'];cache=case/'timings.json'
 if cache.exists():out.append(dict(**row,timings=json.loads(cache.read_text())));continue
 ids=set();head=collections.defaultdict(list);computes=[];net=[]
 with gzip.open(run/'queries.jsonl.gz','rt') as f:
  for l in f:
   q=json.loads(l);ids.add(q['request_id']);t=q['stats']['native_head_timestamps']
   for name,start,end in [('route_queue','search_enter_ns','route_started_ns'),('routing','route_started_ns','routing_done_ns'),('entry_send','routing_done_ns','entry_send_done_ns'),('data_and_control','entry_send_done_ns','last_report_received_ns'),('merge_queue','merge_enqueued_ns','merge_started_ns'),('merge','merge_started_ns','native_ready_ns')]:head[name].append((t[end]-t[start])/1e6)
 with gzip.open(run/'nodes_after.json.gz','rt') as f:nodes=json.load(f)
 pernode={};occupancy={};overall=collections.defaultdict(list)
 for sid,node in nodes.items():
  phases=collections.defaultdict(list);ts=[t for t in node['peer_lifecycle']['execution_timestamps'] if t['query_request_id'] in ids]
  for t in ts:
   for name,start,end in PHASES:
    if start in t and end in t:
     v=(t[end]-t[start])/1e6;assert v>=0,(name,v);phases[name].append(v)
   phases['controller_state_lock'].append(t['native_register_state_wait_ns']/1e6)
  ev=[e for e in node['peer_lifecycle']['control_events'] if e['query_request_id'] in ids and e['kind']=='result' and e['succeeded']]
  for e in ev:
   phases['report_send_queue'].append(e['rpc_counts']['pool_wait_ms']);phases['report_ack'].append(e['rpc_counts']['response_wait_ms'])
  pernode[sid]={k:dist(v) for k,v in phases.items()}
  span=(max(t['compute_released_ns'] for t in ts)-min(t['worker_enter_ns'] for t in ts))/1e6
  occupied=sum((t['compute_released_ns']-t['worker_enter_ns'])/1e6 for t in ts)
  occupancy[sid]=dict(window_ms=span,mean_busy_search_workers=occupied/span,worker_utilization=occupied/span/row['threads'],configured_workers=row['threads'])
  for k,v in phases.items():overall[k].extend(v)
 data=dict(head={k:dist(v) for k,v in head.items()},branches={k:dist(v) for k,v in overall.items()},nodes=pernode,occupancy=occupancy)
 cache.write_text(json.dumps(data,indent=2));out.append(dict(**row,timings=data))
groups={}
for mode in sorted({r['mode'] for r in out}):
 rs=[r for r in out if r['mode']==mode]
 groups[mode]=dict(runs=len(rs),qps_median=statistics.median(r['qps'] for r in rs),qps_range=[min(r['qps'] for r in rs),max(r['qps'] for r in rs)],p99_median=statistics.median(r['p99'] for r in rs),recall_range=[min(r['recall'] for r in rs),max(r['recall'] for r in rs)],branch_phase_mean_medians={k:statistics.median(r['timings']['branches'][k]['mean'] for r in rs) for k in rs[0]['timings']['branches']},head_phase_mean_medians={k:statistics.median(r['timings']['head'][k]['mean'] for r in rs) for k in rs[0]['timings']['head']})
(ROOT/(args.output+'.json')).write_text(json.dumps(dict(groups=groups,runs=out),indent=2))
fig,axs=plt.subplots(1,3,figsize=(14,4),layout='constrained')
for ax,key,label in zip(axs[:2],['qps','p99'],['QPS','End-to-end p99 (ms)']):
 for i,mode in enumerate(groups):
  values=[r[key] for r in rows if r['mode']==mode];ax.scatter([i]*len(values),values,s=45);ax.plot([i-.2,i+.2],[statistics.median(values)]*2,color='black')
 ax.set_xticks(range(len(groups)),list(groups));ax.set_ylabel(label);ax.grid(alpha=.2)
names=['receive','admission','compute_ready','search','completion_queue'];x=np.arange(len(names));width=.8/max(1,len(groups))
for j,(mode,g) in enumerate(groups.items()):axs[2].bar(x+j*width,[g['branch_phase_mean_medians'][k] for k in names],width=width,label=mode)
axs[2].set_xticks(x+width*(len(groups)-1)/2,names,rotation=20);axs[2].set_ylabel('Median of per-run branch means (ms)');axs[2].legend();axs[2].grid(axis='y',alpha=.2)
fig.suptitle('Three hosts | 32 vs 64 search workers/node | budget 200 | concurrency 256')
fig.savefig(ROOT/(args.output+'.png'),dpi=160);plt.close(fig)
print(json.dumps(groups,indent=2))
