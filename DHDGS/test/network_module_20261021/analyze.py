import json,gzip,statistics
from pathlib import Path
import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
ROOT=Path(__file__).resolve().parent
rows=[]
def dist(v):
 a=np.array(v,dtype=float)
 return dict(mean=float(a.mean()),p99=float(np.percentile(a,99)),max=float(a.max()))
for p in sorted((ROOT/'three').iterdir()):
 if not (p/'result.json').exists() or p.name.startswith('06_'):continue
 d=json.loads((p/'result.json').read_text());sp=json.loads((p/'spec.json').read_text());cache=p/'analysis.json'
 if cache.exists() and json.loads(cache.read_text()).get('schema')==2:rows.append(json.loads(cache.read_text()));continue
 vals={}
 def add(k,v):vals.setdefault(k,[]).append(v)
 with gzip.open(Path(d['directory'])/'queries.jsonl.gz','rt') as f:
  for line in f:
   q=json.loads(line);s=q['stats'];t=s['native_head_timestamps']
   for label,a,b in [('route_queue','search_enter_ns','route_started_ns'),('routing','route_started_ns','routing_done_ns'),('entry_dispatch','routing_done_ns','entry_send_done_ns'),('data_control','routing_done_ns','merge_enqueued_ns'),('merge_queue','merge_enqueued_ns','merge_started_ns'),('merge','merge_started_ns','native_ready_ns')]:add(label,(t[b]-t[a])/1e6)
   add('handoff',(q['api_future_done_ns']-t['native_ready_ns'])/1e6)
   for k,v in s['entry_rpc'].items():
    if isinstance(v,(int,float)):add('entry_'+k,v)
   add('report_processing',(t['all_reports_received_ns']-t['last_report_received_ns'])/1e6)
   add('report_to_merge',(t['merge_enqueued_ns']-t['last_report_received_ns'])/1e6)
   add('ack_after_reports',max(0,t['entry_submit_return_ns']-t['all_reports_received_ns'])/1e6)
   for k in ['query_branch_wall_ms','query_transfer_finalize_wait_ms','query_state_other_ms']:add(k,sum(b['stats'][k] for b in q['branch_reports']))
   add('search_wall',sum(b['stats']['query_compute_slice_wall_ms'] for b in q['branch_reports']))
   for k in ['query_ready_queue_ms','server_thread_cpu_ms','pread_ms','rabitq_ms','exact_distance_ms','all_protocol_bytes','all_protocol_rpc_count']:add(k,s[k])
 assert d['failed_queries']==0 and all(d['audit'].values()) and d['result_capture']['overflow_batches']==0
 before=json.loads((p/'before.json').read_text());assert all(n['worker_threads']==64 for n in before['nodes'])
 r=dict(schema=2,name=p.name,spec=sp,qps=d['qps'],p99=d['latency_ms']['p99'],mean=d['latency_ms']['mean'],recall=d['recall_at_k_all_requests'],wall_seconds=d['measured_wall_seconds'],metrics={k:dist(v) for k,v in vals.items()},node_cpu=[n['mean_cpu_cores'] for n in d['node_process_load']],head_cpu=d['head_process_load'][0]['mean_cpu_cores'],physical_reads=sum(n['process_deltas']['storage_read_bytes'] for n in d['node_process_load']))
 cache.write_text(json.dumps(r,indent=2));rows.append(r)
groups={}
for r in rows:groups.setdefault(r['spec']['network_mode'],[]).append(r)
averages={k:{m:statistics.mean(r[m] for r in v) for m in ['qps','p99','mean','recall']} for k,v in groups.items()}
(ROOT/'comparison.json').write_text(json.dumps(dict(runs=rows,averages=averages),indent=2))
fig,axes=plt.subplots(2,1,figsize=(13,8),layout='constrained')
for ax,key,title in zip(axes,['qps','p99'],['Throughput / QPS','Per-run p99 / ms']):
 vals=[r[key] for r in rows];ax.bar(range(len(rows)),vals,color=['#347fa4' if r['name'].startswith('a') else '#d48b39' for r in rows]);ax.set_xticks(range(len(rows)),[r['name'] for r in rows],rotation=30,ha='right');ax.set_ylabel(title);ax.set_ylim(0,max(vals)*1.15)
 for i,v in enumerate(vals):ax.text(i,v,f'{v:.1f}',ha='center',va='bottom',fontsize=8)
fig.suptitle('64 search threads/node | concurrency 256 | budget 200 | 10,000 requests/run')
fig.savefig(ROOT/'comparison.png',dpi=180);fig.savefig(ROOT/'comparison.svg');plt.close(fig)
lines=['# 统一网络模块三机对照实验','','固定三机Intel-35/32/30，每节点64查询线程、并发256、搜索预算200、合并线程4、预热32。每轮10000请求（相同2000查询重复5次）。固定顶层路由16线程/每目标连接32，三组分别为关闭模块资源策略、固定单循环、收发各1至4循环动态扩缩容。adaptive_control全程关闭；固定/动态组每循环每目标连接32，关闭组保留旧通道配置。','','|轮次|路由线程|顶层连接/目标|QPS|p99 ms|平均ms|Recall@10|','|---|---:|---:|---:|---:|---:|---:|']
for r in rows:lines.append(f"|{r['name']}|{r['spec']['route_threads']}|{r['spec']['head_connections']}|{r['qps']:.2f}|{r['p99']:.2f}|{r['mean']:.2f}|{r['recall']:.5f}|")
lines+=['','## 分阶段等待','','以下为每查询均值，单位ms。发送前排队为RPC提交至网络客户端分配处理的间隔，混合连接容量等待与事件循环调度；响应等待包含远端处理与网络事件循环，不能称为纯网络耗时。派发/响应与数据执行重叠，不能相加为总延迟。','','|轮次|路由队列|路由执行|入口发送前排队|入口发送含连接|入口响应等待|数据与控制|计算就绪累计|搜索执行累计|搜索CPU累计|','|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|']
keys=['route_queue','routing','entry_pool_wait_ms','entry_send_ms','entry_response_wait_ms','data_control','query_ready_queue_ms','search_wall','server_thread_cpu_ms']
for r in rows:lines.append('|'+r['name']+'|'+'|'.join(f"{r['metrics'][k]['mean']:.3f}" for k in keys)+'|')
lines+=['','## 负载','','CPU核数为进程CPU时间除以快照窗口，数据节点窗口包含快照采集时间，不能用作精确瞬时CPU饱和度。','','|轮次|Intel-35数据CPU核|Intel-32数据CPU核|Intel-30数据CPU核|顶层CPU核|物理读字节|','|---|---:|---:|---:|---:|---:|---:|']
for r in rows:lines.append('|'+r['name']+'|'+'|'.join(f'{v:.2f}' for v in r['node_cpu'])+f"|{r['head_cpu']:.2f}|{r['physical_reads']}|")
lines+=['','## 验证与边界','','全部结果通过零失败、预算/无重复访问/无路由扩展审计，真实数据线程数64，暂存没有触发容量溢出。服务时延以完整原生结果交付Future为终点，后置编码、JSON持久化与统计不计入QPS。每轮仅数秒，是热缓存短测；两轮均值不能证明统计显著性。原始时间戳、有效YAML、源码快照和审计均保存在各轮目录。未修改生产配置或搜索代码。','','![对比图](comparison.png)']
(ROOT/'REPORT.md').write_text('\n'.join(lines)+'\n');print(json.dumps(averages,indent=2))
