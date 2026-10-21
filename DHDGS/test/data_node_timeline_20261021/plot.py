import json,gzip
from pathlib import Path
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
ROOT=Path(__file__).resolve().parent;PROJECT=ROOT.parents[1]
summary=json.loads((PROJECT/'test/concurrency_scale_20261020/three/00_c256/result.json').read_text());run=Path(summary['directory'])
with gzip.open(run/'nodes_after.json.gz','rt') as f:nodes=json.load(f)
x=nodes['1']['peer_lifecycle'];traces={v['branch_id']:v for v in x['execution_timestamps']};events={v['branch_id']:v for v in x['control_events'] if v['kind']=='result' and v['succeeded']}
best=None;delta=float('inf')
with gzip.open(run/'queries.jsonl.gz','rt') as f:
 for line in f:
  q=json.loads(line)
  if len(q['branch_reports'])!=1:continue
  b=q['branch_reports'][0];bid=b['branch_id']
  if b['node_id']!=1 or bid not in traces or bid not in events:continue
  t=traces[bid]
  if 'compute_released_ns' not in t:continue
  diff=abs(q['client_wall_ms']-summary['latency_ms']['p50'])
  if diff<delta:best=(q,b,t,events[bid]);delta=diff
q,b,t,e=best;assert t['clock_domain']==b['native_timestamps']['clock_domain'];origin=t['receiver_message_ready_ns'];stages=[]
def add(lane,label,a,z):
 assert z>=a,(label,a,z)
 stages.append(dict(lane=lane,label=label,start_ns=a,end_ns=z,start_ms=(a-origin)/1e6,ms=(z-a)/1e6))
add(0,'Receive queue + validation',origin,t['accepted_ns'])
add(0,'Admission preparation',t['accepted_ns'],t['accept_state_lock_enter_ns'])
add(0,'Admission state lock wait',t['accept_state_lock_enter_ns'],t['accept_state_lock_acquired_ns'])
add(0,'Registration + task enqueue',t['accept_state_lock_acquired_ns'],t['enqueued_ns'])
add(1,'Compute ready queue',t['enqueued_ns'],t['worker_enter_ns'])
add(1,'Search setup',t['worker_enter_ns'],t['native_search_start_ns'])
add(1,'Search loop',t['native_search_start_ns'],t['native_loop_done_ns'])
add(1,'Build report + submit completion',t['native_loop_done_ns'],t['local_done_enqueued_ns'])
add(1,'Compute cleanup / release',t['local_done_enqueued_ns'],t['compute_released_ns'])
add(2,'Completion command wait',t['local_done_enqueued_ns'],t['local_done_dequeued_ns'])
add(2,'Finalize report',t['local_done_dequeued_ns'],t['report_ready_ns'])
add(2,'Report scheduling',t['report_ready_ns'],e['queued_ns'])
add(2,'Prepare result message',e['queued_ns'],e['message_ready_ns'])
add(3,'Outbound submit / pool / event loop',e['message_ready_ns'],e['sender_enter_ns'])
send_done=e['sender_enter_ns']+round(e['rpc_counts']['send_ms']*1e6)
add(3,'Send result (incl. connect)',e['sender_enter_ns'],send_done)
add(3,'Wait for report ACK',send_done,e['sender_rpc_return_ns'])
fig,axes=plt.subplots(2,1,figsize=(15,9),layout='constrained',gridspec_kw={'height_ratios':[3,1]});ax=axes[0];colors=plt.get_cmap('tab20').colors
for i,s in enumerate(stages):
 ax.barh(s['lane'],s['ms'],left=s['start_ms'],height=.5,color=colors[i],label=f"{s['label']}: {s['ms']:.3f} ms")
 if s['ms']>1.5:ax.text(s['start_ms']+s['ms']/2,s['lane'],f"{s['ms']:.2f}",ha='center',va='center',fontsize=9)
release=(t['compute_released_ns']-origin)/1e6;ax.axvline(release,color='black',ls='--',label=f'Compute thread released: {release:.3f} ms')
ax.set_yticks([0,1,2,3],['Receive / admission','Compute worker','Completion controller','Result network client']);ax.invert_yaxis();ax.set_xlabel('Milliseconds from complete request frame (Intel-32 monotonic clock)');ax.legend(loc='upper left',bbox_to_anchor=(1,1),fontsize=8);ax.grid(axis='x',alpha=.2)
s=b['stats'];loop=(t['native_loop_done_ns']-t['native_search_start_ns'])/1e6
parts=[('pread',s['pread_ms']),('RaBitQ prepare',s['rabitq_prepare_ms']),('RaBitQ scoring',s['rabitq_ms']),('Exact distance',s['exact_distance_ms'])];rest=loop-sum(v for _,v in parts);assert rest>=0;parts.append(('Other loop work / scheduling',rest));left=0
for i,(label,v) in enumerate(parts):
 axes[1].barh(0,v,left=left,color=colors[i*2],label=f'{label}: {v:.3f} ms');left+=v
axes[1].set_yticks([0],['Search loop breakdown']);axes[1].set_xlabel('Accumulated measured duration / ms; not chronological operation order');axes[1].legend(loc='upper left',bbox_to_anchor=(1,1),fontsize=8)
fig.suptitle(f"Intel-32 | query {q['query_id']} / job {q['job']} | concurrency 256 / 64 search workers\nSingle data branch; selected nearest overall median among complete Intel-32 traces",fontsize=13)
fig.savefig(ROOT/'data_node_timeline.png',dpi=180);fig.savefig(ROOT/'data_node_timeline.svg');plt.close(fig)
out=dict(source=str(run),selection='Single-branch Intel-32 query nearest overall p50 with complete trace',query_id=q['query_id'],job=q['job'],client_wall_ms=q['client_wall_ms'],recall=q['recall'],origin_ns=origin,node_total_to_report_ack_ms=(e['sender_rpc_return_ns']-origin)/1e6,compute_release_ms=release,stages=stages,search_parts_ms=dict(parts),stats=s,trace=t,event=e)
(ROOT/'detail.json').write_text(json.dumps(out,indent=2));(ROOT/'query.json').write_text(json.dumps(q,indent=2));print(json.dumps({k:v for k,v in out.items() if k not in ['stats','trace','event']},indent=2))
