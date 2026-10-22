import json,gzip,csv
from pathlib import Path
import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
O=Path(__file__).resolve().parent
S=O.parent/'receive_admission_20261021/p99/rx8_accept8/selected.json'
s=json.loads(S.read_text()); run=Path(s['source_run']); sample=s['query']['request_id']
with gzip.open(run/'queries.jsonl.gz','rt') as f: ids={json.loads(l)['request_id'] for l in f}
with gzip.open(run/'nodes_after.json.gz','rt') as f: nodes=json.load(f)
fig,axs=plt.subplots(3,2,figsize=(15,10),layout='constrained'); summary={}; allrows=[]; traces={}
for i,(key,node) in enumerate(sorted(nodes.items())):
 host=['intel-35','intel-32','intel-30'][int(key)]
 ts=sorted([t for t in node['peer_lifecycle']['execution_timestamps'] if t['query_request_id'] in ids],key=lambda t:t['receiver_message_ready_ns']);traces[key]=ts
 origin=ts[0]['receiver_message_ready_ns']; x=np.array([(t['receiver_message_ready_ns']-origin)/1e9 for t in ts]); w=np.array([(t['accepted_ns']-t['receiver_message_ready_ns'])/1e6 for t in ts]); a=np.array([(t['accepted_ns']-origin)/1e9 for t in ts]); ax,bx=axs[i]
 ax.scatter(x,w,s=3,alpha=.2,color='#2675b8',rasterized=True,label='Each measured branch')
 bins=np.arange(0,x.max()+.05,.05); med=[]; p95=[];cent=[]
 for b in bins:
  v=w[(x>=b)&(x<b+.05)]
  if len(v):cent.append(b+.025);med.append(np.median(v));p95.append(np.percentile(v,95))
 ax.plot(cent,med,color='black',lw=1.3,label='50 ms bins: median');ax.plot(cent,p95,color='#e48418',lw=1.3,label='50 ms bins: p95')
 events=sorted([(t,1) for t in x]+[(t,-1) for t in a]); et=np.array([v[0] for v in events]); depth=np.cumsum([v[1] for v in events]);bx.step(et,depth,where='post',color='#2675b8',lw=.9)
 st=[t for t in ts if t['query_request_id']==sample]
 for t in st:
  xx=(t['receiver_message_ready_ns']-origin)/1e9; ww=(t['accepted_ns']-t['receiver_message_ready_ns'])/1e6
  ax.scatter([xx],[ww],marker='*',s=150,color='crimson',zorder=5,label=f'Sample: {ww:.3f} ms');bx.axvspan(xx,xx+ww/1000,color='crimson',alpha=.25)
  bx.axvline(xx,color='crimson',lw=1)
 stats={k:float(v) for k,v in zip(['mean_ms','p50_ms','p95_ms','p99_ms','max_ms'],[w.mean(),*np.percentile(w,[50,95,99]),w.max()])}
 stats.update(host=host,branches=len(ts),pending_before_accept_peak=int(depth.max()),over_50ms=int(sum(w>50)),receiver_threads=len({t['receiver_thread_id'] for t in ts}),origin_ns=origin)
 if st:
  t=st[0];lo=t['receiver_message_ready_ns'];hi=t['accepted_ns'];tid=t['receiver_thread_id'];same=[v for v in ts if v['receiver_thread_id']==tid and v['accepted_ns']<hi and v['receiver_accept_return_ns']>lo]
  intervals=[(max(lo,v['accepted_ns']),min(hi,v['receiver_accept_return_ns'])) for v in same];covered=sum(max(0,b-a) for a,b in intervals)/1e6
  lock_overlap=sum(max(0,min(hi,v['accept_state_lock_acquired_ns'])-max(lo,v['accept_state_lock_enter_ns'])) for v in same)/1e6
  stats['sample']={'same_handler_state_lock_overlap_ms':lock_overlap,'wait_ms':(hi-lo)/1e6,'thread':tid,'earlier_same_handler_admissions':len(same),'same_handler_admission_overlap_ms':covered,'earlier_admissions':[dict(request_id=v['query_request_id'],start_relative_ms=(v['accepted_ns']-lo)/1e6,end_relative_ms=(v['receiver_accept_return_ns']-lo)/1e6,lock_wait_ms=(v['accept_state_lock_acquired_ns']-v['accept_state_lock_enter_ns'])/1e6,native_state_wait_ms=v.get('native_register_state_wait_ns',0)/1e6) for v in same]}
 summary[key]=stats
 for t,xx,ww in zip(ts,x,w):allrows.append(dict(node=key,host=host,request_id=t['query_request_id'],branch_id=t['branch_id'],receiver_thread_id=t['receiver_thread_id'],local_receive_s=xx,receive_to_accept_ms=ww))
 ax.set_title(f'{host} / data {int(key)+1}: n={len(ts)}, mean={w.mean():.2f}, p99={np.percentile(w,99):.2f} ms');ax.set_ylabel('Full frame received -> accepted (ms)');ax.set_ylim(0,125);ax.grid(alpha=.2);ax.legend(fontsize=7,loc='upper left')
 bx.set_title(f'{host}: received but not yet accepted (peak={depth.max()})');bx.set_ylabel('Measured branches (not exact worker queue)');bx.set_ylim(0,240);bx.grid(alpha=.2)
 for aa in [ax,bx]:aa.set_xlabel('Seconds since this HOST first measured frame (independent origins)')
fig.suptitle('8 RX + 8 admission handlers | same 20,000-query run | clocks NOT synchronized',fontsize=14)
fig.savefig(O/'receive_wait_timeseries.png',dpi=160);fig.savefig(O/'receive_wait_timeseries.svg');plt.close(fig)
(O/'summary.json').write_text(json.dumps(dict(source_run=str(run),sample_request_id=sample,nodes=summary),indent=2))
with (O/'branches.csv').open('w') as f:
 writer=csv.DictWriter(f,fieldnames=list(allrows[0]));writer.writeheader();writer.writerows(allrows)
# Intel-32 sample: zoom into same handler admissions and concurrent backlog.
ts=traces['1'];t=next(t for t in ts if t['query_request_id']==sample);lo=t['receiver_message_ready_ns'];hi=t['accepted_ns'];tid=t['receiver_thread_id']
fig,(ax,bx)=plt.subplots(2,1,figsize=(13,7),layout='constrained')
for v in ts:
 x=(v['receiver_message_ready_ns']-lo)/1e6;y=(v['accepted_ns']-v['receiver_message_ready_ns'])/1e6
 if -40<=x<=140:ax.scatter(x,y,s=18 if v['receiver_thread_id']==tid else 5,color='crimson' if v['receiver_thread_id']==tid else '#9eb9cf',alpha=.7)
ax.scatter(0,(hi-lo)/1e6,marker='*',s=200,color='black',label='Selected child branch');ax.legend();ax.set_ylabel('Receive -> accept (ms)');ax.grid(alpha=.2)
prior=[v for v in ts if v['receiver_thread_id']==tid and v['accepted_ns']<hi and v['receiver_accept_return_ns']>lo]
for j,v in enumerate(prior):
 start=(v['accepted_ns']-lo)/1e6;end=(v['receiver_accept_return_ns']-lo)/1e6
 bx.barh(j,end-start,left=start,color='#efb05b')
 l=(v['accept_state_lock_enter_ns']-lo)/1e6;r=(v['accept_state_lock_acquired_ns']-lo)/1e6;bx.barh(j,r-l,left=l,color='#a14285')
bx.axvspan(0,(hi-lo)/1e6,color='gray',alpha=.12);bx.set_yticks(list(range(0,len(prior),4))+[len(prior)-1],[f'Earlier task {j+1}' for j in list(range(0,len(prior),4))+[len(prior)-1]]);bx.set_xlabel('Local time relative to sample full-frame receipt (ms)');bx.set_xlim(-5,110);bx.grid(axis='x',alpha=.2)
bx.set_title('Same admission handler: orange=admission until return; purple=runtime state mutex wait')
fig.suptitle('intel-32 sample zoom: red dots=same handler; blue dots=other handlers')
fig.savefig(O/'intel32_sample_zoom.png',dpi=160);plt.close(fig)
print(json.dumps(summary,indent=2))
