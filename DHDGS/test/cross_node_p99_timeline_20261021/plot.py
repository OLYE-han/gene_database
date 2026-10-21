import json
from pathlib import Path
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
ROOT=Path(__file__).resolve().parent;d=json.loads((ROOT/'selected.json').read_text());q=d['query'];branches=sorted(d['branches'],key=lambda b:b['report']['parent_branch_id'] is not None);fig,axes=plt.subplots(3,1,figsize=(16,13),layout='constrained',gridspec_kw={'height_ratios':[1,2,2]});palette=plt.get_cmap('tab20').colors;records=[]
t=q['stats']['native_head_timestamps'];start=q['client_start_ns'];p=[('API submit',start),('Route queue',t['search_enter_ns']),('HNSW routing',t['route_started_ns']),('Entry dispatch incl. queue',t['routing_done_ns']),('Data / network / control',t['entry_send_done_ns']),('Report processing',t['last_report_received_ns']),('Merge queue',t['merge_enqueued_ns']),('Merge',t['merge_started_ns']),('Result handoff',t['native_ready_ns']),('Done',q['api_future_done_ns'])]
for i,((label,a),(_,z)) in enumerate(zip(p,p[1:])):
 assert z>=a;v=(z-a)/1e6;axes[0].barh(0,v,left=(a-start)/1e6,color=palette[i],label=f'{label}: {v:.3f} ms')
axes[0].set_yticks([0],['Top-level']);axes[0].set_title(f'Intel-35 head clock: query {q["query_id"]}, job {q["job"]}, end-to-end {q["client_wall_ms"]:.3f} ms');axes[0].set_xlabel('ms from client submission');axes[0].legend(loc='upper left',bbox_to_anchor=(1,1),fontsize=8)
for ax,b in zip(axes[1:],branches):
 t=b['trace'];origin=t['receiver_message_ready_ns'];ev=next(e for e in b['events'] if e['kind']=='result' and e['succeeded']);stages=[]
 def add(lane,label,a,z):
  assert z>=a,(label,a,z)
  stages.append(dict(lane=lane,label=label,start_ms=(a-origin)/1e6,ms=(z-a)/1e6))
 add(0,'Receive queue / validation',origin,t['accepted_ns'])
 add(0,'Admission + registration',t['accepted_ns'],t['enqueued_ns'])
 add(0,'Compute queue + setup',t['enqueued_ns'],t['native_search_start_ns'])
 add(1,'Search loop',t['native_search_start_ns'],t['native_loop_done_ns'])
 add(1,'Report build / worker release',t['native_loop_done_ns'],t['compute_released_ns'])
 add(2,'Completion command wait',t['local_done_enqueued_ns'],t['local_done_dequeued_ns'])
 add(2,'Finalize incl. pending transfer',t['local_done_dequeued_ns'],t['report_ready_ns'])
 add(2,'Schedule + prepare report',t['report_ready_ns'],ev['message_ready_ns'])
 add(2,'Result outbound queue / event loop',ev['message_ready_ns'],ev['sender_enter_ns'])
 add(2,'Result send + ACK',ev['sender_enter_ns'],ev['sender_rpc_return_ns'])
 if 'transfer_worker_enter_ns' in t:add(3,'Cross-node submit to ACK callback',t['transfer_worker_enter_ns'],t['transfer_worker_return_ns'])
 for i,s in enumerate(stages):
  ax.barh(s['lane'],s['ms'],left=s['start_ms'],height=.5,color=palette[i],label=f"{s['label']}: {s['ms']:.3f} ms")
  if s['ms']>4:ax.text(s['start_ms']+s['ms']/2,s['lane'],f"{s['ms']:.2f}",ha='center',va='center',fontsize=9)
 release=(t['compute_released_ns']-origin)/1e6;ax.axvline(release,color='black',ls='--',label=f'Worker released: {release:.3f} ms')
 ax.set_yticks([0,1,2,3],['Receive / ready','Compute worker','Completion / report','Cross-node RPC']);ax.invert_yaxis();ax.set_xlabel('ms from THIS node receiving a complete request frame; independent local origin')
 parent=b['report']['parent_branch_id'] is None;host={0:'Intel-35',1:'Intel-32',2:'Intel-30'}[b['report']['node_id']]
 ax.set_title(f'{host} {"PARENT (entry)" if parent else "CHILD"} branch — local clock; this x-axis is NOT aligned to other panels');ax.legend(loc='upper left',bbox_to_anchor=(1,1),fontsize=8)
 records.append(dict(node_id=b['report']['node_id'],role='parent' if parent else 'child',worker_release_ms=release,total_to_report_ack_ms=(ev['sender_rpc_return_ns']-origin)/1e6,stages=stages,peer_transfer=b['report']['peer_transfer']))
for ax in axes:ax.grid(axis='x',alpha=.2)
fig.suptitle(f'Cross-node p99 sample: Intel-32 → Intel-35 | 256 concurrency / 64 workers per node\nOverall p99 {d["target_p99_ms"]:.6f} ms; sample {q["client_wall_ms"]:.6f} ms; local panels must not be summed',fontsize=13)
fig.savefig(ROOT/'cross_node_p99.png',dpi=180);fig.savefig(ROOT/'cross_node_p99.svg');plt.close(fig);(ROOT/'stages.json').write_text(json.dumps(records,indent=2));print(json.dumps(records,indent=2))
