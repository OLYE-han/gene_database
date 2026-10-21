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
for r in rows:groups.setdefault(('early' if r['spec']['early'] else 'wait_ack'),[]).append(r)
averages={k:{m:statistics.mean(r[m] for r in v) for m in ['qps','p99','mean','recall']} for k,v in groups.items()}
(ROOT/'comparison.json').write_text(json.dumps(dict(runs=rows,averages=averages),indent=2))
fig,axes=plt.subplots(2,1,figsize=(12,7),layout='constrained')
for ax,key,title in zip(axes,['qps','p99'],['Throughput / QPS','Per-run p99 / ms']):
 vals=[r[key] for r in rows];ax.bar(range(len(rows)),vals,color=[{'wait_ack':'#8c9ca8','early':'#298ca4'}[('early' if r['spec']['early'] else 'wait_ack')] for r in rows]);ax.set_xticks(range(len(rows)),[r['name'] for r in rows],rotation=25,ha='right');ax.set_ylabel(title);ax.set_ylim(0,max(vals)*1.15)
 for i,v in enumerate(vals):ax.text(i,v,f'{v:.1f}',ha='center',va='bottom',fontsize=8)
fig.suptitle('Early parent publication | 64 search threads/node | concurrency 256 | budget 200')
fig.savefig(ROOT/'comparison.png',dpi=160);plt.close(fig)
lines=['# 父分支本地报告提前发布三机实验','','Intel-35/32/30，每节点64搜索线程、路由16线程、合并4线程、并发256、预算200，每轮10000请求（同样2000查询重复5次），预热32。收发模块启用并固定各一个事件循环，搜索/路由/合并线程数均不变。连接128、结果通道pending上限1024、接收上限4096固定。唯一实验变量为early_parent_report：关闭时等待子分支接纳ACK后发布父报告，开启时本地完成即发布。两组均使用本轮相同的源码和二进制。','','|轮次|提前发布|结果pending上限|QPS|p99 ms|平均 ms|Recall@10|','|---|---:|---:|---:|---:|---:|---:|']
for r in rows:lines.append(f"|{r['name']}|{r['spec']['early']}|{r['spec']['control_capacity']}|{r['qps']:.2f}|{r['p99']:.2f}|{r['mean']:.2f}|{r['recall']:.5f}|")
lines+=['','## 两轮均值','','|父报告发布策略|QPS|各轮p99均值 ms|平均延迟 ms|','|---|---:|---:|---:|']
for k,r in averages.items():lines.append(f"|{k}|{r['qps']:.2f}|{r['p99']:.2f}|{r['mean']:.2f}|")
lines+=['','## 入口及后续等待','','以下为每查询均值，ms；各阶段不全互斥，不能直接相加。RPC响应等待包含远端处理与调度，不等于链路RTT。','','|轮次|入口发送排队|入口发送/建连|入口响应等待|数据及控制|合并队列|路由队列|搜索累计|计算就绪累计|','|---|---:|---:|---:|---:|---:|---:|---:|---:|']
for r in rows:lines.append('|'+r['name']+'|'+'|'.join(f"{r['metrics'][k]['mean']:.3f}" for k in ['entry_pool_wait_ms','entry_send_ms','entry_response_wait_ms','data_control','merge_queue','route_queue','search_wall','query_ready_queue_ms'])+'|')
lines+=['','## CPU及IO','','数据节点CPU为进程快照区间平均核数，包含快照时间。','','|轮次|Intel-35|Intel-32|Intel-30|顶层CPU|物理读字节|每query协议字节|每query协议RPC|','|---|---:|---:|---:|---:|---:|---:|---:|']
for r in rows:lines.append('|'+r['name']+'|'+'|'.join(f'{v:.2f}' for v in r['node_cpu'])+f"|{r['head_cpu']:.2f}|{r['physical_reads']}|{r['metrics']['all_protocol_bytes']['mean']:.0f}|{r['metrics']['all_protocol_rpc_count']['mean']:.2f}|")
lines+=['','![QPS与p99](comparison.png)','','全部请求要求零失败、Recall≥95%、协议审计通过且内存暂存无溢出。计时截至原生结果交付Future，JSON归档与统计在之后执行。每轮约数秒、热缓存，两轮均值不是统计显著性结论；各轮p99平均不等于合并样本p99。连接按需建立，上限扩大并不意味着预先创建全部连接。','']
(ROOT/'REPORT.md').write_text('\n'.join(lines));print(json.dumps(averages,indent=2))
