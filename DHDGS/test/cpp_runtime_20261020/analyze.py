"""Summarize complete records; plots never subtract clocks on different hosts."""
import csv,gzip,json,random,statistics
from pathlib import Path
import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
ROOT=Path(__file__).resolve().parent

def dist(values):
 a=np.asarray(values,dtype=float)
 return dict(mean=float(a.mean()),p50=float(np.percentile(a,50)),p99=float(np.percentile(a,99)),max=float(a.max())) if len(a) else None

def read_lines(path):
 with gzip.open(path,'rt') as f:
  for line in f:yield json.loads(line)

def analyze(case):
 summary=json.loads((case/'result.json').read_text());run=Path(summary['directory']);qs=list(read_lines(run/'queries.jsonl.gz'))
 keys=['query_ready_queue_ms','query_admission_queue_ms','query_executor_start_wait_ms','query_transfer_finalize_wait_ms',
       'pread_ms','rabitq_ms','exact_distance_ms','server_thread_cpu_ms','native_latency_ms','api_result_delivery_ms',
       'head_route_cpu_ms','head_merge_cpu_ms','file_read_bytes','pread_calls','all_protocol_bytes','all_protocol_rpc_count',
       'peer_branch_rpc_count','expanded','max_expanded_depth','max_cross_shard_depth','branch_count']
 metrics={k:dist([q['stats'][k] for q in qs if k in q['stats']]) for k in keys}
 metrics['benchmark_consumer_wait_ms']=dist([q['benchmark_consumer_wait_ms'] for q in qs if 'benchmark_consumer_wait_ms' in q])
 parts=sorted({k for q in qs for k in q['stats']['latency_breakdown_ms']})
 metrics['stages']={k:dist([q['stats']['latency_breakdown_ms'].get(k,0) for q in qs]) for k in parts}
 metrics['client_minus_api_ms']=dist([q['client_wall_ms']-q['stats']['latency_ms'] for q in qs])
 with gzip.open(run/'nodes_after.json.gz','rt') as f:nodes=json.load(f)
 timings=[]
 for sid,node in nodes.items():
  for t in node['peer_lifecycle']['execution_timestamps']:
   for label,a,b in [('accept_lock','accept_state_lock_enter_ns','accept_state_lock_acquired_ns'),
                     ('accept_hold','accept_state_lock_acquired_ns','accept_state_lock_released_ns'),
                     ('accepted_to_compute','accepted_ns','worker_enter_ns'),
                     ('compute_release_after_loop','native_loop_done_ns','compute_released_ns')]:
    if a in t and b in t:timings.append(dict(node=int(sid),label=label,ms=(t[b]-t[a])/1e6))
 metrics['node_timings']={str(sid):{label:dist([t['ms'] for t in timings if t['node']==sid and t['label']==label])
      for label in sorted({t['label'] for t in timings})} for sid in range(3)}
 (case/'analysis.json').write_text(json.dumps(metrics,indent=2))
 return summary,metrics,qs,nodes

def plot(case,qs,nodes):
 # Uniform deterministic choice among cross-node queries; never choose by speed.
 candidates=[q for q in qs if len(q['branch_reports'])>1] or qs
 q=random.Random(20261020).choice(candidates)
 (case/'selected_query.json').write_text(json.dumps(q,indent=2))
 stats=q['stats'];t=stats['native_head_timestamps'];client=q['client_start_ns']
 stages=[('API submit',client,t['search_enter_ns']),('Route queue',t['search_enter_ns'],t['route_started_ns']),
         ('HNSW + RaBitQ routing',t['route_started_ns'],t['routing_done_ns']),
         ('Data search + control',t['routing_done_ns'],t['merge_enqueued_ns']),
         ('Merge queue',t['merge_enqueued_ns'],t['merge_started_ns']),
         ('Merge',t['merge_started_ns'],t['native_ready_ns']),
         ('Python result delivery',t['native_ready_ns'],stats['head_timestamps']['search_return_ns']),
         ('API completion' if 'api_future_done_ns' in q else 'API / consumer (unsplit)',stats['head_timestamps']['search_return_ns'],q.get('api_future_done_ns',q['client_end_ns'])),
         ('Benchmark consumer',q.get('api_future_done_ns',q['client_end_ns']),q['client_end_ns'])]
 figure,axes=plt.subplots(2,1,figsize=(13,6.5),gridspec_kw={'height_ratios':[1,1.7]},layout='constrained')
 colors=plt.get_cmap('tab10').colors
 for i,(label,a,b) in enumerate(stages):
  axes[0].barh(0,max(0,b-a)/1e6,left=(a-client)/1e6,label=f'{label}: {(b-a)/1e6:.3f} ms',color=colors[i])
 axes[0].set_yticks([0],['Head + client']);axes[0].set_xlabel('Milliseconds since client submission (Intel-35 clock)')
 axes[0].legend(loc='upper left',bbox_to_anchor=(1,1),fontsize=8);axes[0].grid(axis='x',alpha=.2)
 rows=[];branch_labels=set()
 for branch in q['branch_reports']:
  sid=branch['node_id'];life=nodes[str(sid)]['peer_lifecycle']['execution_timestamps']
  row=next((r for r in life if r['query_request_id']==q['request_id'] and r['branch_id']==branch['branch_id']),None)
  if row is None:continue
  start=row['accepted_ns'];work=row['worker_enter_ns'];entry=row['native_entry_ns'];loop=row['native_loop_done_ns'];release=row.get('compute_released_ns',row.get('native_return_ns',loop))
  seq=[('Ready queue',start,work),('Worker setup',work,entry),('Native search / reads',entry,loop),('Local completion',loop,release)]
  if row.get('report_ready_ns',release)>release:seq.append(('Control after compute release',release,row['report_ready_ns']))
  y=len(rows)
  for i,(label,a,b) in enumerate(seq):
   axes[1].barh(y,max(0,b-a)/1e6,left=(a-start)/1e6,color=colors[i],label=label if label not in branch_labels else None)
   branch_labels.add(label)
  axes[1].scatter([(release-start)/1e6],[y],marker='|',s=180,color='black',zorder=4)
  rows.append(f"Node {sid} (thread {branch['compute_thread_id']})")
 axes[1].set_yticks(range(len(rows)),rows);axes[1].invert_yaxis();axes[1].set_xlabel('Milliseconds since this branch was accepted on its own host')
 axes[1].legend(loc='upper left',bbox_to_anchor=(1,1),fontsize=8);axes[1].grid(axis='x',alpha=.2)
 figure.suptitle(f"Query {q['query_id']} / job {q['job']} | client {q['client_wall_ms']:.2f} ms | recall {q['recall']:.2f}\nBranch clocks have independent origins; black marks release of compute workers",fontsize=11)
 figure.savefig(case/'query_timeline.png',dpi=180);figure.savefig(case/'query_timeline.svg');plt.close(figure)
 with (case/'selected_head_stages.csv').open('w',newline='') as f:
  writer=csv.writer(f);writer.writerow(['stage','start_ns','end_ns','duration_ms'])
  writer.writerows((label,a,b,(b-a)/1e6) for label,a,b in stages)

if __name__=='__main__':
 import argparse
 p=argparse.ArgumentParser();p.add_argument('cases',nargs='+');args=p.parse_args();overall=[]
 for name in args.cases:
  case=ROOT/name;summary,metrics,qs,nodes=analyze(case)
  if json.loads((case/'spec.json').read_text())['backend']=='cpp':plot(case,qs,nodes)
  overall.append(dict(case=name,summary=summary,metrics=metrics))
 (ROOT/'comparison.json').write_text(json.dumps(overall,indent=2))
 print(json.dumps([{k:r['summary'][k] for k in ('qps','latency_ms','recall_at_k_all_requests')} for r in overall],indent=2))
