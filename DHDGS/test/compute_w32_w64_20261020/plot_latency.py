import json,gzip
from pathlib import Path
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
ROOT=Path(__file__).resolve().parent
comp=json.loads((ROOT/'comparison.json').read_text());rows=[r for r in comp['runs'] if r['threads']==64]
labels=['API submit','Route queue','HNSW routing','Data search + network/control','Merge queue','Merge','Result handoff']
colors=['#333333','#9a77b9','#4394c3','#e3a344','#d66b7b','#59a77b','#6b828e']
fig,ax=plt.subplots(figsize=(13,4.8),layout='constrained')
for j,r in enumerate(rows):
 left=0
 for i,(key,val) in enumerate(r['stage_mean_ms'].items()):
  ax.barh(j,val,left=left,color=colors[i],label=labels[i] if j==0 else None)
  if val>2:ax.text(left+val/2,j,f'{val:.2f} ms',ha='center',va='center',fontsize=10)
  left+=val
 ax.text(left+.3,j,f'{left:.2f} ms',va='center')
ax.set_yticks(range(2),['64 threads / run 1','64 threads / run 2']);ax.invert_yaxis();ax.set_xlabel('Mean latency per query (ms); 10,000 requests per run');ax.set_xlim(0, max(r['mean_ms'] for r in rows)*1.2);ax.legend(loc='upper center',bbox_to_anchor=(.5,-.19),ncol=3);ax.set_title('64-thread latency composition: data + control is not pure computation')
fig.savefig(ROOT/'latency_64_mean.png',dpi=180);fig.savefig(ROOT/'latency_64_mean.svg');plt.close(fig)
# Deterministically choose nearest-to-p99 query in first 64-thread run.
p=ROOT/'three/01_w64';d=json.loads((p/'result.json').read_text());best=None;delta=float('inf')
with gzip.open(Path(d['directory'])/'queries.jsonl.gz','rt') as f:
 for line in f:
  q=json.loads(line);diff=abs(q['client_wall_ms']-d['latency_ms']['p99'])
  if diff<delta:best=q;delta=diff
q=best;(ROOT/'selected_query_64.json').write_text(json.dumps(q,indent=2));t=q['stats']['native_head_timestamps'];start=q['client_start_ns']
points=[('API submit',start),('Route queue',t['search_enter_ns']),('HNSW routing',t['route_started_ns']),('Entry dispatch incl. send queue',t['routing_done_ns']),('Until last report received',t['entry_send_done_ns']),('Report processing / remaining confirmation',t['last_report_received_ns']),('Merge enqueue',t['all_reports_received_ns']),('Merge queue',t['merge_enqueued_ns']),('Merge',t['merge_started_ns']),('Result handoff',t['native_ready_ns']),('Complete',q['api_future_done_ns'])]
assert all(a[1]<=b[1] for a,b in zip(points,points[1:])),points
stages=[dict(stage=a[0],start_ms=(a[1]-start)/1e6,end_ms=(b[1]-start)/1e6,duration_ms=(b[1]-a[1])/1e6) for a,b in zip(points,points[1:])]
fig,axes=plt.subplots(3,1,figsize=(15,11),layout='constrained');palette=plt.get_cmap('tab20').colors
for i,s in enumerate(stages):axes[0].barh(0,s['duration_ms'],left=s['start_ms'],color=palette[i],label=f"{s['stage']}: {s['duration_ms']:.3f} ms")
axes[0].set_yticks([0],[f"job {q['job']}"]);axes[0].set_xlabel('Milliseconds since submission (Intel-35 clock)');axes[0].legend(loc='upper left',bbox_to_anchor=(1,1),fontsize=8);axes[0].set_title('Head timeline: intervals include overlapping remote work')
branches=[]
for j,b in enumerate(q['branch_reports']):
 s=b['stats'];wall=s['query_branch_wall_ms'];ready=s['query_ready_queue_ms'];compute=s['query_compute_slice_wall_ms'];final=s['query_transfer_finalize_wait_ms'];other=wall-ready-compute-final
 assert other>=-.001,(wall,ready,compute,final)
 parts=[('Compute ready queue',ready),('Search execution wall',compute),('Transfer finalization',final),('Other branch control',max(0,other))]
 left=0
 for i,(label,v) in enumerate(parts):
  axes[1].barh(j,v,left=left,color=palette[i*2],label=label if j==0 else None)
  if v>2:axes[1].text(left+v/2,j,f'{v:.2f}',ha='center',va='center')
  left+=v
 axes[1].text(left+.2,j,f'{wall:.2f} ms',va='center')
 measured=[('pread',s['pread_ms']),('RaBitQ',s['rabitq_ms']),('Exact distance',s['exact_distance_ms'])];residual=compute-sum(v for _,v in measured);assert residual>=-.001
 measured.append(('Other search incl. scheduling',max(0,residual)));left=0
 for i,(label,v) in enumerate(measured):
  axes[2].barh(j,v,left=left,color=palette[2*i+1],label=label if j==0 else None)
  if v>2:axes[2].text(left+v/2,j,f'{v:.2f}',ha='center',va='center')
  left+=v
 branches.append(dict(node_id=b['node_id'],branch_id=b['branch_id'],wall_ms=wall,parts_ms=dict(parts),search_parts_ms=dict(measured),thread_cpu_ms=s['server_thread_cpu_ms'],search_wall_minus_cpu_ms=compute-s['server_thread_cpu_ms']))
for a in axes[1:]:
 a.set_yticks(range(len(branches)),[f"node {b['node_id']}" for b in branches]);a.set_xlabel('Duration composition (ms); independent branch clocks, not aligned to head');a.legend(loc='upper left',bbox_to_anchor=(1,1),fontsize=8);a.set_xlim(0,a.get_xlim()[1]*1.1)
axes[1].set_title('Branch duration components: branches can overlap; do not add to end-to-end latency')
axes[2].set_title('Inside search execution: aggregated measured scopes, not chronological batch order')
for a in axes:a.grid(axis='x',alpha=.2)
fig.suptitle(f"64 threads/node | selected near-p99 query {q['query_id']} | {q['client_wall_ms']:.3f} ms | Recall@10 {q['recall']:.2f}",fontsize=14)
fig.savefig(ROOT/'latency_64_p99_query.png',dpi=180);fig.savefig(ROOT/'latency_64_p99_query.svg');plt.close(fig)
output=dict(entry_rpc=q['stats']['entry_rpc'],selection='Nearest p99 query, first 64-thread run',job=q['job'],query_id=q['query_id'],latency_ms=q['client_wall_ms'],stages=stages,branches=branches)
(ROOT/'latency_64_detail.json').write_text(json.dumps(output,indent=2));print(json.dumps(output,indent=2))
