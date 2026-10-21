import gzip,json,statistics
from pathlib import Path
import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
ROOT=Path(__file__).resolve().parent
out=[]
def dist(v):return dict(mean=float(np.mean(v)),p99=float(np.percentile(v,99)),max=float(max(v))) if v else None
for case in sorted((ROOT/'three').iterdir()):

 if case.name.startswith('06_'):continue
 d=json.loads((case/'result.json').read_text());run=Path(d['directory']);ids=set()
 with gzip.open(run/'queries.jsonl.gz','rt') as f:
  for line in f:ids.add(json.loads(line)['request_id'])
 with gzip.open(run/'nodes_after.json.gz','rt') as f:nodes=json.load(f)
 row=dict(case=case.name,adaptive=json.loads((case/'spec.json').read_text())['adaptive'],qps=d['qps'],p99=d['latency_ms']['p99'],nodes=[])
 for sid,node in nodes.items():
  life=node['peer_lifecycle'];values={};add=lambda k,v:values.setdefault(k,[]).append(v)
  for t in life['execution_timestamps']:
   if t['query_request_id'] not in ids:continue
   for key,a,b in [('completion_command_wait_ms','local_done_enqueued_ns','local_done_dequeued_ns'),('finalize_wait_ms','local_done_dequeued_ns','report_ready_ns'),('loop_done_to_release_ms','native_loop_done_ns','compute_released_ns'),('receive_to_accept_ms','receiver_message_ready_ns','accepted_ns')]:
    if a in t and b in t:add(key,(t[b]-t[a])/1e6)
  for e in life['control_events']:
   if e['query_request_id'] in ids and e['kind']=='result':
    for k in ['pool_wait_ms','send_ms','response_wait_ms']:add('result_'+k,e['rpc_counts'][k])
  ac=life['native_control']['network_send'];ac=dict(ac,lanes=ac['reactors'],peak_lanes=ac['peak_reactors'])
  row['nodes'].append(dict(node_id=sid,adaptive_control=ac,metrics={k:dist(v) for k,v in values.items()},process_threads=node['process']['threads']))
 out.append(row)
(ROOT/'control_comparison.json').write_text(json.dumps(out,indent=2))
fig,axes=plt.subplots(1,3,figsize=(15,4),layout='constrained')
for ax,k,title in zip(axes,['completion_command_wait_ms','result_pool_wait_ms','receive_to_accept_ms'],['Completion command wait','Result send queue','Receive to admission']):
 for i,row in enumerate(out):
  vals=[n['metrics'][k]['mean'] for n in sorted(row['nodes'],key=lambda n:n['node_id'])];ax.plot([0,1,2],vals,marker='o',label=row['case'])
 ax.set_xticks([0,1,2],['Intel-35','Intel-32','Intel-30']);ax.set_ylabel('Mean / ms');ax.set_title(title);ax.grid(alpha=.2)
axes[-1].legend(fontsize=8);fig.savefig(ROOT/'control_waits.png',dpi=180);fig.savefig(ROOT/'control_waits.svg');plt.close(fig)
lines=['','## 实际扩容及等待','','以下为正式查询对应的分支/结果事件均值（ms），不是所有分支之和。不同区间可能重叠。','','|轮次|节点|结果发送峰值循环|结束循环|进程线程数|完成命令等待|结果发送前队列|接收至接纳|','|---|---:|---:|---:|---:|---:|---:|---:|']
for r in out:
 for n in sorted(r['nodes'],key=lambda n:n['node_id']):
  a=n['adaptive_control'];m=n['metrics'];lines.append(f"|{r['case']}|{n['node_id']}|{a['peak_lanes']}|{a['lanes']}|{n['process_threads']}|{m['completion_command_wait_ms']['mean']:.3f}|{m['result_pool_wait_ms']['mean']:.3f}|{m['receive_to_accept_ms']['mean']:.3f}|")
lines+=['','![控制与发送等待](control_waits.png)','']
with (ROOT/'REPORT.md').open('a') as f:f.write('\n'.join(lines))
print('\n'.join(lines))
