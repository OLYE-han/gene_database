"""Compare bounded-inflight load without hiding client-side backlog."""
from pathlib import Path
import argparse,csv,gzip,json
import numpy as np
from wait_breakdown import analyze,union,distribution
R=Path(__file__).resolve().parent
parser=argparse.ArgumentParser();parser.add_argument('--plots',action='store_true');args=parser.parse_args()
paths=sorted(R.glob('c*/runs/*/report.json.gz'),key=lambda p:(int(p.parents[2].name[1:]),p.parent.name))
(R/'final_runs.json').write_text(json.dumps([str(p.relative_to(R)) for p in paths],indent=2))
rows=[]
for p in paths:
 with gzip.open(p,'rt') as f:r=json.load(f)
 waits=analyze(p)
 ts={t['branch_id']:t for n in r['node_process_load'] for t in n['peer_lifecycle_after']['execution_timestamps']}
 row=dict(group=p.parents[2].name,run=p.parent.name,concurrency=r['configured_concurrency'],actual_peak=r['actual_peak_inflight'],mean_inflight=r['mean_inflight'],queries=r['query_count'],failed=r['failed_queries'],recall=r['recall_at_k_all_requests'],qps=r['qps'],batch_seconds=r['measured_wall_seconds'])
 for key in ('latency_ms','client_backlog_ms','offered_to_complete_ms'):
  for q in ('mean','p99','max'):row[key+'_'+q]=r[key][q]
 for key in ('admission_queue_ms','compute_queue_ms','accepted_to_worker_ms','completion_queue_all_branches_ms','report_ready_to_sender_ms','native_loop_to_worker_free_ms','transfer_queue_ms','entry_pool_ms'):
  v=waits['branch_stage_ms'].get(key)
  for q in ('mean','p99','max'):row[key+'_'+q]=v[q] if v else None
 for key in ('query_observed_queue_union_ms','query_exposed_queue_union_ms'):
  for q in ('mean','p99','max'):row[key+'_'+q]=waits[key][q]
 row['exposed_wait_over_100ms']=waits['exposed_queue_over_100ms']
 ready=[];perquery=[]
 for query in r['queries']:
  assert all(b['branch_id'] in ts for b in query['branch_reports'])
  intervals=[(ts[b['branch_id']]['accepted_ns'],ts[b['branch_id']]['worker_enter_ns']) for b in query['branch_reports']]
  value=sum(b-a for a,b in union(intervals))/1e6;ready.append(value)
  perquery.append(dict(request_id=query['request_id'],query_id=query['query_id'],client_backlog_ms=query['client_backlog_ms'],submitted_api_ms=query['client_wall_ms'],offered_to_complete_ms=query['offered_to_complete_ms'],server_ready_union_ms=value))
 v=distribution(ready)
 for q in ('mean','p99','max'):row['server_ready_union_ms_'+q]=v[q]
 row['server_ready_over_100ms']=sum(x>=100 for x in ready)
 for key in ('all_protocol_rpc_count','all_protocol_bytes','file_read_bytes','pread_calls','pread_ms','rabitq_ms','exact_distance_ms','server_thread_cpu_ms','expanded','max_expanded_depth','peer_branch_rpc_count','peer_pool_wait_ms','peer_transfer_wall_ms'):
  row['mean_'+key]=float(np.mean([q['stats'].get(key,0) for q in r['queries']]))
 row['physical_read_bytes']=sum(n['process_deltas']['storage_read_bytes'] for n in r['node_process_load'])
 row['node_cpu_cores']=[n['mean_cpu_cores'] for n in r['node_process_load']]
 for kind in ('heartbeat','result','lease','cancel'):row[kind+'_rpc']=sum(q['stats']['control_by_kind'][kind]['rpc_count'] for q in r['queries'])
 out=R/'analysis'/p.parents[2].name/p.parent.name
 with (out/'query_latency_and_backlog.csv').open('w') as f:
  w=csv.DictWriter(f,list(perquery[0]));w.writeheader();w.writerows(perquery)
 selected=next(q for q in r['queries'] if q['query_id']==734744);ids={b['branch_id'] for b in selected['branch_reports']}
 events={'startup':[ts[b] for b in ids],'control':[e for n in r['node_process_load'] for e in n['peer_lifecycle_after']['control_events'] if e['branch_id'] in ids]}
 (out/'selected_query.json').write_text(json.dumps(selected,indent=2));(out/'selected_events.json').write_text(json.dumps(events,indent=2))
 rows.append(row)
 print(json.dumps({k:row[k] for k in ('group','run','qps','latency_ms_p99','accepted_to_worker_ms_p99','query_exposed_queue_union_ms_p99','query_exposed_queue_union_ms_max','exposed_wait_over_100ms','recall')}),flush=True)
(R/'comparison.json').write_text(json.dumps(rows,indent=2))
if rows:
 with (R/'comparison.csv').open('w') as f:
  w=csv.DictWriter(f,list(rows[0]));w.writeheader();w.writerows(rows)
if not args.plots:raise SystemExit
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib.font_manager import FontProperties
font=FontProperties(fname='/usr/share/fonts/truetype/wqy/wqy-zenhei.ttc')
plt.rcParams.update({'font.family':font.get_name(),'font.weight':500,'axes.titleweight':500,'axes.labelweight':500,'axes.unicode_minus':False,'svg.fonttype':'path'})
groups={c:[r for r in rows if r['concurrency']==c] for c in sorted({r['concurrency'] for r in rows})}
fig,axs=plt.subplots(2,2,figsize=(14,9),layout='constrained')
def line(ax,key,label):
 x=list(groups);v=[[r[key] for r in groups[c]] for c in x]
 ax.plot(x,[np.mean(a) for a in v],marker='o',label=label);ax.fill_between(x,[min(a) for a in v],[max(a) for a in v],alpha=.14)
for key,label in [('latency_ms_p99','提交后 API p99'),('accepted_to_worker_ms_p99','分支接纳→计算线程 p99'),('query_exposed_queue_union_ms_p99','每query已观测等待并集 p99')]:line(axs[0,0],key,label)
axs[0,0].axhline(100,color='red',ls='--',label='100 ms目标');axs[0,0].set_ylabel('ms');axs[0,0].set_yscale('log');axs[0,0].set_title('内部延迟随并发降低')
line(axs[0,1],'qps','整批完成吞吐');axs[0,1].set_ylabel('QPS');axs[0,1].set_title('吞吐量的代价')
for key,label in [('latency_ms_p99','提交后 API p99'),('client_backlog_ms_p99','提交前客户端等待 p99'),('offered_to_complete_ms_p99','整批同时产生需求→返回 p99')]:line(axs[1,0],key,label)
axs[1,0].set_ylabel('ms');axs[1,0].set_yscale('log');axs[1,0].set_title('限制并发会将积压留在客户端')
for key,label in [('query_exposed_queue_union_ms_max','每query已观测等待并集最大值'),('server_ready_union_ms_max','每query服务器就绪区间并集最大值')]:line(axs[1,1],key,label)
axs[1,1].axhline(100,color='red',ls='--',label='100 ms目标');axs[1,1].set_ylabel('ms');axs[1,1].set_yscale('log');axs[1,1].set_title('检查尾部，不只看平均值')
for ax in axs.flat:
 ax.set_xscale('log',base=2);ax.set_xticks(list(groups),[str(c) for c in groups]);ax.set_xlabel('客户端同时在途上限');ax.grid(alpha=.2);ax.legend(fontsize=8)
fig.suptitle('2000条固定查询 · C++控制路径 · 每节点32计算线程 · 搜索预算200\n线为重复运行均值，阴影为最小—最大范围；本机三数据进程、热页缓存')
for ext in ('png','svg'):fig.savefig(R/f'concurrency_sweep.{ext}',dpi=150)
