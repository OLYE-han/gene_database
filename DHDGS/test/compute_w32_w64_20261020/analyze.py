import json, gzip, statistics
from pathlib import Path
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
ROOT=Path(__file__).resolve().parent
names=['00_w32','01_w64','02_w64','03_w32']; rows=[]
for name in names:
 p=ROOT/'three'/name;d=json.loads((p/'result.json').read_text());spec=json.loads((p/'spec.json').read_text());before=json.loads((p/'before.json').read_text())
 assert all(n['worker_threads']==spec['threads'] for n in before['nodes'])
 assert d['failed_queries']==0 and all(d['audit'].values())
 assert d['result_capture']['overflow_batches']==0
 r=dict(case=name,threads=spec['threads'],qps=d['qps'],p99_ms=d['latency_ms']['p99'],mean_ms=d['latency_ms']['mean'],recall=d['recall_at_k_all_requests'],wall_seconds=d['measured_wall_seconds'],node_cpu_cores=[n['mean_cpu_cores'] for n in d['node_process_load']],head_cpu_cores=d['head_process_load'][0]['mean_cpu_cores'],physical_read_bytes=sum(n['process_deltas']['storage_read_bytes'] for n in d['node_process_load']),metrics=d['query_metric_distributions'],capture_queue=d['capture_queue_ms'])
 stages={k:[] for k in ['api_submit_ms','route_queue_ms','routing_ms','data_and_control_ms','merge_queue_ms','merge_ms','result_handoff_ms']}
 with gzip.open(Path(d['directory'])/'queries.jsonl.gz','rt') as f:
  for line in f:
   q=json.loads(line);t=q['stats']['native_head_timestamps']
   stages['api_submit_ms'].append((t['search_enter_ns']-q['client_start_ns'])/1e6)
   for k,a,b in [('route_queue_ms','search_enter_ns','route_started_ns'),('routing_ms','route_started_ns','routing_done_ns'),('data_and_control_ms','routing_done_ns','merge_enqueued_ns'),('merge_queue_ms','merge_enqueued_ns','merge_started_ns'),('merge_ms','merge_started_ns','native_ready_ns')]:stages[k].append((t[b]-t[a])/1e6)
   stages['result_handoff_ms'].append((q['api_future_done_ns']-t['native_ready_ns'])/1e6)
 r['stage_mean_ms']={k:statistics.mean(v) for k,v in stages.items()};rows.append(r)
groups={str(w):{k:statistics.mean(r[k] for r in rows if r['threads']==w) for k in ['qps','p99_ms','mean_ms','recall']} for w in [32,64]}
change={k:(groups['64'][k]/groups['32'][k]-1)*100 for k in ['qps','p99_ms','mean_ms']}
(ROOT/'comparison.json').write_text(json.dumps(dict(runs=rows,averages=groups,change_percent=change),indent=2))
fig,ax=plt.subplots(1,2,figsize=(11,4),layout='constrained')
for a,key,label in zip(ax,['qps','p99_ms'],['QPS (higher is better)','p99 latency, ms (lower is better)']):
 vals=[r[key] for r in rows];a.bar(range(4),vals,color=['#2673ab','#d88b32','#d88b32','#2673ab']);a.set_xticks(range(4),['32 / run 1','64 / run 1','64 / run 2','32 / run 2']);a.set_ylabel(label);a.set_ylim(0,max(vals)*1.18)
 for i,v in enumerate(vals):a.text(i,v,f'{v:,.2f}',ha='center',va='bottom')
fig.suptitle('Three machines | concurrency 256 | budget 200 | 10,000 requests per run')
fig.savefig(ROOT/'comparison.png',dpi=180);fig.savefig(ROOT/'comparison.svg')
lines=['# 每节点 32 / 64 查询线程对照实验','','三台服务器 Intel-35 / Intel-32 / Intel-30；Intel-35 同时运行独立顶层路由。固定并发256、预算200、top-k10、路由/合并线程各4、预热32。每轮10000次请求，由固定2000条验证查询重复5次组成。使用相同索引、源码、查询顺序；运行顺序32→64→64→32。','','|轮次|每节点线程|QPS|p99(ms)|平均(ms)|Recall@10|','|---|---:|---:|---:|---:|---:|']
for r in rows:lines.append(f"|{r['case']}|{r['threads']}|{r['qps']:.2f}|{r['p99_ms']:.2f}|{r['mean_ms']:.2f}|{r['recall']:.5f}|")
lines+=['',f"两轮算术平均：QPS {groups['32']['qps']:.2f} → {groups['64']['qps']:.2f}（{change['qps']:+.2f}%）；每轮p99的算术平均 {groups['32']['p99_ms']:.2f} → {groups['64']['p99_ms']:.2f} ms（{change['p99_ms']:+.2f}%）。该值不是合并样本的p99。",'','## 时延与负载','','各阶段取每查询平均；data + control包含数据搜索、网络、确认及回报，不能直接当作纯计算。计算就绪队列为分支累计指标，可能重叠，不能与顶层阶段相加。','','|轮次|API提交|路由排队|路由计算|数据与控制|合并排队|合并|结果交付|计算就绪累计|搜索线程CPU|','|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|']
for r in rows:
 s=r['stage_mean_ms'];m=r['metrics'];lines.append('|'+r['case']+'|'+ '|'.join(f'{s[k]:.3f}' for k in s)+f"|{m['query_ready_queue_ms']['mean']:.3f}|{m['server_thread_cpu_ms']['mean']:.3f}|")
lines+=['','|轮次|Intel-35数据CPU核|Intel-32数据CPU核|Intel-30数据CPU核|顶层CPU核|物理读字节|','|---|---:|---:|---:|---:|---:|']
for r in rows:lines.append('|'+r['case']+'|'+'|'.join(f'{x:.2f}' for x in r['node_cpu_cores'])+f"|{r['head_cpu_cores']:.2f}|{r['physical_read_bytes']}|")
lines+=['','CPU核数为进程CPU时间/快照窗口，数据节点窗口包含快照获取时间，不完全等于查询窗口，不能据此精确判断瞬时饱和。','','所有运行零失败，审计通过，真实线程数经admin_status确认；原生结果暂存未触发溢出。QPS和延迟终点为完整结果交付Future，不包含之后的编码、JSON和报告处理，也不是外部HTTP SLA。','','查询窗口仅数秒，属于热页缓存短测；两轮重复不能证明长期容量或统计显著性。扩大线程池不保证提速，也不能单独用于证明实现正确或错误。默认配置和常驻服务未修改。','','![QPS与p99对比](comparison.png)','','完整配置、查询时间戳、资源快照和结果位于three各轮目录，汇总见comparison.json。']
(ROOT/'REPORT.md').write_text('\n'.join(lines)+'\n');print(json.dumps(dict(averages=groups,change_percent=change),indent=2))
