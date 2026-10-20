from pathlib import Path
import collections,csv,gzip,json,hashlib,os
import numpy as np
R=Path(__file__).resolve().parent
os.environ['MPLCONFIGDIR']=str(R/'.mplconfig')
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

records=json.loads((R/'analysis.json').read_text())
formal=[r for r in records if not r.get('profile')]
# The extra instrumentation runs are diagnostic; never merge them into repeats.
formal=[r for r in formal if r['repeat']!='profile']
groups=collections.defaultdict(list)
for r in formal:groups[(r['topology'],1,r['head_connections'],r.get('control_connections',2))].append(r)
legacy=[];allrecalls=[];total=0;failures=0;physical=[];audit=[]
for topology,dirname in [('remote','legacy20'),('local','legacy20local')]:
 for file in sorted((R/dirname/'runs').glob('*/summary.json')):
  s=json.loads(file.read_text());d=file.parent
  with gzip.open(d/'queries.jsonl.gz','rt') as f:qs=[json.loads(line) for line in f]
  detail={'topology':topology,'head_processes':20,'head_connections':2,'control_connections':2,'qps':s['qps'],'p99_ms':s['latency_ms']['p99'],'mean_ms':s['latency_ms']['mean'],'recall':s['recall_at_k_all_requests'],'failed':s['failed_queries'],'source':str(d)}
  stages=collections.defaultdict(list)
  for q in qs:
   st=q['stats'];t=st['head_timestamps'];rpc=st['entry_rpc']
   stages['routing'].append((t['routing_done_ns']-t['search_enter_ns'])/1e6)
   stages['entry_pool'].append(rpc['pool_wait_ms']);stages['entry_socket'].append(rpc['wall_ms']-rpc['pool_wait_ms'])
   stages['report_after_entry_ack'].append((t['all_reports_received_ns']-t['entry_submit_return_ns'])/1e6)
   stages['head_merge_return'].append((t['search_return_ns']-t['all_reports_received_ns'])/1e6)
   stages['head_other'].append(q['client_wall_ms']-sum(stages[k][-1] for k in ['routing','entry_pool','entry_socket','report_after_entry_ack','head_merge_return']))
   for k in ['query_ready_queue_ms','query_admission_queue_ms','query_compute_slice_wall_ms','coordinator_thread_cpu_ms']:stages[k].append(st[k])
  detail['stages_ms']={k:{'mean':float(np.mean(v)),'p99':float(np.percentile(v,99))} for k,v in stages.items()}
  detail['per_node']={}
  for sid in range(3):
   bs=[b for q in qs for b in q['branch_reports'] if b['node_id']==sid]
   detail['per_node'][sid]={'branches':len(bs),'compute_ms_per_100_expansions':100*sum(b['stats']['query_compute_slice_wall_ms'] for b in bs)/sum(b['work_spent'] for b in bs),'admission_mean_ms':float(np.mean([b['stats']['query_admission_queue_ms'] for b in bs]))}
  before=json.loads((d/'nodes_before.json').read_text());after=json.loads((d/'nodes_after.json').read_text())
  detail['physical_read_bytes']=[after[k]['process']['storage_read_bytes']-before[k]['process']['storage_read_bytes'] for k in after]
  detail['audit']={'metrics_complete':all(q['stats'].get('metrics_complete',False) for q in qs),'budget':all(q['stats']['expanded']<=200 for q in qs),'no_revisit':all(q['stats']['node_revisit_count']==0 for q in qs),'no_head_expansion':all(q['stats']['router_expand_rpc_count']==0 for q in qs)}
  groups[(topology,20,2,2)].append(detail);legacy.append(detail);allrecalls.append(detail['recall']);total+=len(qs);failures+=detail['failed'];physical+=detail['physical_read_bytes'];audit+=list(detail['audit'].values())
for r in formal:
 case=R/'cases'/r['case'];s=json.loads((case/'result.json').read_text());allrecalls.append(r['recall']);total+=s['query_count'];failures+=r['failed'];physical+=r['physical_read_bytes'];audit+=list(s['audit'].values())

def median(key,metric):return float(np.median([r[metric] for r in groups[key]]))
def phase(key,metric):return float(np.mean([r['stages_ms'][metric]['mean'] for r in groups[key]]))
summary=[]
for key,rows in groups.items():
 summary.append(dict(topology=key[0],heads=key[1],entry_connections=key[2],control_connections=key[3],qps_median=median(key,'qps'),qps_min=min(r['qps'] for r in rows),qps_max=max(r['qps'] for r in rows),p99_median=median(key,'p99_ms'),mean_latency_ms=phase(key,'entry_pool')+phase(key,'routing')+phase(key,'entry_socket')+phase(key,'report_after_entry_ack')+phase(key,'head_merge_return')+phase(key,'head_other')))
(R/'group_summary.json').write_text(json.dumps(summary,indent=2));(R/'legacy_analysis.json').write_text(json.dumps(legacy,indent=2))
(R/'audit.json').write_text(json.dumps(dict(formal_queries=total,failed_queries=failures,audit_passed=all(audit),recall_min=min(allrecalls),recall_max=max(allrecalls),physical_read_delta_sum=sum(physical)),indent=2))

labels=['1 head\nEntry 2 / Return 2','1 head\nEntry 16 / Return 2','1 head\nEntry 64 / Return 2','20 heads\nEntry 2 / Return 2','1 head\nEntry 64 / Return 16']
keys=[(1,2,2),(1,16,2),(1,64,2),(20,2,2),(1,64,16)]
fig,ax=plt.subplots(figsize=(12,5.5));x=np.arange(len(keys));width=.35
for topology,offset,color,label in [('local',-.18,'#4477aa','All 3 data processes on Intel-35'),('remote',.18,'#cc6677','Intel-35: 1 data process; Intel-30: 2')]:
 for i,k in enumerate(keys):
  key=(topology,*k)
  if key not in groups:continue
  vals=[r['qps'] for r in groups[key]];m=np.median(vals)
  ax.bar(x[i]+offset,m,width,color=color,label=label if i==0 else None)
  ax.errorbar(x[i]+offset,m,yerr=[[m-min(vals)],[max(vals)-m]],fmt='none',ecolor='black',capsize=4)
  ax.text(x[i]+offset,m+35,f'{m:.0f}',ha='center',fontsize=9)
ax.set_xticks(x,labels);ax.set_ylabel('QPS (median of 3 runs; whiskers = range)');ax.set_ylim(0,1650)
ax.set_title('Same 2M index, 2000 queries/run, budget 200, 32 workers/node, concurrency 128')
ax.legend(loc='upper left');ax.grid(axis='y',alpha=.2);fig.tight_layout();fig.savefig(R/'qps_comparison.png',dpi=160);fig.savefig(R/'qps_comparison.svg');plt.close(fig)

fig,ax=plt.subplots(figsize=(11,5.5));bottom=np.zeros(len(keys))
for stage,label,color in [('routing','HNSW + route preparation','#332288'),('entry_pool','Wait for head entry connection','#cc6677'),('entry_socket','Entry RPC until ACK','#88ccee'),('head_other','Other head work / return scheduling','#ddcc77'),('report_after_entry_ack','After entry ACK until all reports','#117733'),('head_merge_return','Merge and return','#aa4499')]:
 values=np.array([phase(('remote',*k),stage) for k in keys]);ax.bar(x,values,bottom=bottom,label=label,color=color);bottom+=values
ax.set_xticks(x,labels);ax.set_ylabel('Mean query wall time (ms)');ax.set_title('Two-host tests: head-clock intervals are disjoint')
ax.legend(bbox_to_anchor=(1.01,1),loc='upper left',fontsize=9);ax.grid(axis='y',alpha=.2);fig.tight_layout();fig.savefig(R/'latency_breakdown.png',dpi=160);fig.savefig(R/'latency_breakdown.svg');plt.close(fig)
profiles=[json.loads((R/'cases'/f'remote_p{cap}_profile/profile.json').read_text()) for cap in (2,64)]
fig,ax=plt.subplots(figsize=(7,4.5));u=[p['user_cpu_seconds'] for p in profiles];sy=[p['system_cpu_seconds'] for p in profiles]
ax.bar(['Entry connections = 2','Entry connections = 64'],u,label='User CPU',color='#4477aa');ax.bar(['Entry connections = 2','Entry connections = 64'],sy,bottom=u,label='Kernel CPU',color='#cc6677')
ax.set_ylabel('Head process CPU-seconds per 2000 queries');ax.set_title('Separate diagnostic runs; warmup/serialization excluded');ax.legend();fig.tight_layout();fig.savefig(R/'head_cpu.png',dpi=160);plt.close(fig)
print(json.dumps({'groups':summary,'queries':total,'failures':failures,'all_audits':all(audit),'recall_range':[min(allrecalls),max(allrecalls)],'physical_bytes':sum(physical)},indent=2))
