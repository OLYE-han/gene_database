"""Streaming comparisons, preserving service and recorder boundaries separately."""
import gzip,json,random
from pathlib import Path
import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

ROOT=Path(__file__).resolve().parent
CASES=['three/00_legacy_capture','three/01_memory_capture','three/02_batched_memory','three/03_deferred_memory','local/04_overflow']

def dist(values):
 a=np.asarray(values,dtype=float)
 return dict(mean=float(a.mean()),p50=float(np.percentile(a,50)),p99=float(np.percentile(a,99)),max=float(a.max())) if len(a) else None

all_cases=[]
for name in CASES:
 case=ROOT/name;summary=json.loads((case/'result.json').read_text());run=Path(summary['directory'])
 values={};jobs=set();offered=None;last_done=0;min_encode=None;rng=random.Random(20261020);sample=None;seen=0
 def add(key,value):values.setdefault(key,[]).append(value)
 with gzip.open(run/'queries.jsonl.gz','rt') as f:
  for line in f:
   q=json.loads(line);s=q['stats'];jobs.add(q['job']);offered=q['offered_ns'];done=q['api_future_done_ns'];last_done=max(last_done,done)
   add('service_ms',(done-q['client_start_ns'])/1e6)
   add('observed_ms',(q.get('benchmark_observed_end_ns',q['client_end_ns'])-q['client_start_ns'])/1e6)
   add('capture_queue_ms',q['benchmark_consumer_wait_ms'])
   for key in ('native_latency_ms','api_result_delivery_ms','query_ready_queue_ms','result_encoding_ms',
               'server_thread_cpu_ms','pread_ms','rabitq_ms','exact_distance_ms','all_protocol_rpc_count',
               'all_protocol_bytes','peer_branch_rpc_count','max_expanded_depth','file_read_bytes'):
    if key in s:add(key,s[key])
   if 'result_encoding_timestamps' in q:
    start=q['result_encoding_timestamps']['start_ns'];min_encode=start if min_encode is None else min(start,min_encode)
   if len(q.get('branch_reports',[]))>1:
    seen+=1
    if rng.randrange(seen)==0:sample=q
 result=dict(case=name,summary=summary,metrics={k:dist(v) for k,v in values.items()},
             service_boundary_qps=len(jobs)/((last_done-offered)/1e9),
             min_encode_start_ns=min_encode,last_service_done_ns=last_done,
             all_jobs_present=jobs==set(range(summary['query_count'])))
 if name=='three/03_deferred_memory':
  assert min_encode>=last_done and summary['result_capture']['overflow_batches']==0
  result['all_encoding_after_all_queries']=True
  (case/'selected_query.json').write_text(json.dumps(sample,indent=2))
  selected=(case,run,sample,summary,last_done,offered)
 (case/'analysis.json').write_text(json.dumps(result,indent=2));all_cases.append(result)
(ROOT/'comparison.json').write_text(json.dumps(all_cases,indent=2))

case,run,q,summary,last_done,offered=selected
t=q['stats']['native_head_timestamps'];h=q['stats']['head_timestamps'];start=q['client_start_ns']
stages=[('API submit',start,t['search_enter_ns']),('Route queue',t['search_enter_ns'],t['route_started_ns']),
 ('HNSW + RaBitQ',t['route_started_ns'],t['routing_done_ns']),('Data + control',t['routing_done_ns'],t['merge_enqueued_ns']),
 ('Merge queue',t['merge_enqueued_ns'],t['merge_started_ns']),('Merge',t['merge_started_ns'],t['native_ready_ns']),
 ('Native result handoff',t['native_ready_ns'],h['search_return_ns']),('Future callback',h['search_return_ns'],q['api_future_done_ns']),
 ('Recorder notification wait',q['api_future_done_ns'],q['benchmark_observed_end_ns']),
 ('Retain native result',q['benchmark_observed_end_ns'],q['capture_stored_ns'])]
fig,axes=plt.subplots(2,1,figsize=(15,7),layout='constrained')
colors=plt.get_cmap('tab10').colors
for i,(label,a,b) in enumerate(stages):
 axes[0].barh(0,(b-a)/1e6,left=(a-start)/1e6,color=colors[i],label=f'{label}: {(b-a)/1e6:.3f} ms')
axes[0].axvline((q['api_future_done_ns']-start)/1e6,color='black',ls='--',label='Service completion')
axes[0].set_yticks([0],[f"Query {q['query_id']} / job {q['job']}"])
axes[0].set_xlabel('Milliseconds since query submission (Intel-35 clock)')
axes[0].legend(loc='upper left',bbox_to_anchor=(1,1),fontsize=8)
cap=summary['result_capture'];native=cap['native_drain_events'][0];raw=cap['drain_events'][0]
run_stages=[('All 10,000 queries',offered,last_done),('Drain / management snapshots',last_done,native['start_ns']),
 ('Deferred native encoding',native['start_ns'],raw['start_ns']),('Independent JSON conversion',raw['start_ns'],raw['end_ns']),
 ('Report startup / metadata',raw['end_ns'],summary['report_started_ns']),
 ('Independent statistics / exports',summary['report_started_ns'],summary['report_finished_ns'])]
for i,(label,a,b) in enumerate(run_stages):
 axes[1].barh(0,(b-a)/1e9,left=(a-offered)/1e9,color=colors[i],label=f'{label}: {(b-a)/1e9:.3f} s')
axes[1].axvline((last_done-offered)/1e9,color='black',ls='--')
axes[1].set_yticks([0],['Entire benchmark run'])
axes[1].set_xlabel('Seconds since load start; processing after dashed line is outside service QPS interval')
axes[1].legend(loc='upper left',bbox_to_anchor=(1,1),fontsize=8)
for ax in axes:ax.grid(axis='x',alpha=.2)
fig.suptitle(f"Deferred memory capture | service {q['client_wall_ms']:.2f} ms | recall {q['recall']:.2f}\nComplete timestamps retained; serialization and reporting run after queries",fontsize=12)
fig.savefig(case/'query_timeline.png',dpi=180);fig.savefig(case/'query_timeline.svg');plt.close(fig)
(case/'selected_stages.json').write_text(json.dumps(dict(query=[dict(stage=l,start_ns=a,end_ns=b,ms=(b-a)/1e6) for l,a,b in stages],run=[dict(stage=l,start_ns=a,end_ns=b,seconds=(b-a)/1e9) for l,a,b in run_stages]),indent=2))
print(json.dumps([dict(case=x['case'],qps=x['summary']['qps'],service_qps=x['service_boundary_qps'],service=x['metrics']['service_ms'],capture_queue=x['metrics']['capture_queue_ms']) for x in all_cases],indent=2))
