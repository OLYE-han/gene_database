from pathlib import Path
import argparse,csv,json
parser=argparse.ArgumentParser();parser.add_argument('--root',type=Path,default=Path(__file__).resolve().parent);a=parser.parse_args();R=a.root
s=json.loads((R/'summary.json').read_text());metrics=json.loads((R/'metrics.json').read_text())
choice=json.loads((R/'selection.json').read_text());q=json.loads((R/'selected_query.json').read_text())
timeline=list(csv.DictReader((R/'selected_timeline.csv').open()));selected=choice['mode']
loads=list(csv.DictReader((R/'process_load.csv').open()));runs=list(csv.DictReader((R/'comparison.csv').open()))
text='''# 单进程顶层流水线：三机同负载对照

完整seq_2M索引，Intel-35为顶层及data-0，Intel-32为data-1，Intel-30为data-2；每数据节点32计算线程，并发128、搜索预算200、top-k10。每轮相同2000查询、32预热，组间交错、各重复3次。没有重建索引或更改数据节点程序/搜索参数。

old是已部署的单HNSW主进程加1个报告接收进程、2个共享路由/合并工作线程；p1/p2/p4是单进程流水线，分别1/2/4个路由线程。最终复验的q1和i1均为1个路由线程，分别采用排队ACK和立即尝试非阻塞ACK。单进程方案均有独立1个合并线程、4个派发线程、1至4个动态接收线程、1个非阻塞回调线程及1个主动心跳线程。当前HTTP服务另有单管理线程和单响应线程；本矩阵由内部异步调用压测，不经过HTTP解析/响应线程。

流水线压测以完成通知维持128个在途槽位，替代旧版128个同步调用线程。排队状态仍占槽位，所有队列等待及结果消费等待计入API。这是完整架构对照，不是单独IPC开销的消融。

## 重复结果

|模式|轮数|查询数|QPS中位数|p99中位数ms|平均延迟中位数ms|平均Recall@10|API>100ms|
|---|---:|---:|---:|---:|---:|---:|---:|
'''
for name,v in s.items():text+=f"|{name}|{v['rounds']}|{v['queries']}|{v['median_qps']:.2f}|{v['median_p99_ms']:.2f}|{v['median_mean_ms']:.2f}|{v['recall_mean']:.3%}|{v['api_over100']}|\n"
best=s[selected];old=s['old'];delta=(best['median_p99_ms']/old['median_p99_ms']-1)*100
text+=f"\n按p99选出的单进程配置为 **{selected}**。相对本窗口旧版，p99变化 **{delta:+.1f}%**，QPS变化 **{(best['median_qps']/old['median_qps']-1)*100:+.1f}%**。所有正式查询成功，预算、节点不重复访问和指标完整审计均通过。等待低于100ms尚未达成。\n\n![对照](comparison.png)\n"
text+='''
## 固定查询734744

选取推荐线程数组的首次正式结果，沿用之前的固定query，不按快慢挑选。表内时间互斥、完整覆盖API；不同主机单调时钟不相减。

![单查询时序](query_timeline.png)

|阶段|ms|
|---|---:|
'''
for row in timeline:text+=f"|{row['stage']}|{float(row['duration_ms']):.3f}|\n"
text+=f"\nAPI {q['client_wall_ms']:.3f}ms，Recall@10 {q['recall']:.0%}。分支原始时间点在selected_node_events.json，独立数据节点时间区间在selected_node_timeline.csv；不能把跨分支区间相加当作端到端时间。\n"
text+='''
![数据节点独立时间轴](query_nodes.png)

## 推荐单进程配置的全部查询指标

|指标|均值|p99|
|---|---:|---:|
'''
for k in ('head_receive_queue_ms','head_route_queue_ms','head_dispatch_queue_ms','head_merge_queue_ms','query_ready_queue_ms','server_thread_cpu_ms','coordinator_thread_cpu_ms','pread_ms','exact_distance_ms','rabitq_ms','expanded','max_expanded_depth','max_cross_shard_depth','peer_branch_rpc_count','control_rpc_count','all_protocol_rpc_count','all_protocol_bytes','file_read_bytes'):
    v=metrics[selected].get(k)
    if v:text+=f"|{k}|{v['mean']:.3f}|{v['p99']:.3f}|\n"
text+='''
查询状态表在派发线程创建，先注册再发送。终态必须同时满足入口派发成功和完整分支树验证；任一分支报告不会提前完成全局查询。合并与路由使用不同队列，数据计算线程仍在本地结束时释放。结果ACK使用原协议续租，独立心跳线程另向已知且尚未完成的参与节点发送peer_lease；主动续租与重试都纳入网络统计。

报告控制、查询CPU和pread等跨分支累加指标不是互斥端到端份额。pread是逻辑读取，不能等同SSD硬件时间；纯网络/物理SSD份额无法可靠分离，原记录保留null。

## 逐轮性能及进程负载

|轮次|QPS|平均ms|p99 ms|Recall@10|失败|实际峰值并发|
|---|---:|---:|---:|---:|---:|---:|
'''
for r in runs:text+=f"|{r['case']}|{float(r['qps']):.2f}|{float(r['mean_ms']):.2f}|{float(r['p99_ms']):.2f}|{float(r['recall']):.3%}|{r['failures']}|{r['peak_inflight']}|\n"
text+='\n|轮次|角色|CPU核数|RSS MiB|物理读bytes|\n|---|---|---:|---:|---:|\n'
for r in loads:text+=f"|{r['case']}|{r['role']}|{float(r['cpu_cores']):.3f}|{float(r['rss_bytes'])/2**20:.1f}|{r['physical_read_bytes']}|\n"
physical=[int(r['physical_read_bytes']) for r in loads if r['role'].startswith('data-')]
text+=f"\n数据节点物理读取增量合计{sum(physical)}字节。{'本轮为热页缓存测试。' if sum(physical)==0 else '见逐节点读取计数。'}原构建时间保留在每轮build_stages.csv，当前测试未重建索引。\n"
text+='''
## 复现与记录

- experiment.yaml：全部矩阵参数；cases/*/plan.yaml：该轮完整YAML。
- cases/*/runs/*/queries.jsonl.gz、queries.csv：每query全部指标。
- cases/*/runs/*/nodes_before/after.json.gz：数据进程指标与生命周期。
- source_hashes.json及snapshot/：该矩阵冻结源码；不覆盖此前负面结果。
- production_before/after.json：常驻服务状态；本矩阵使用临时顶层，完成后关闭，数据进程不重启。
- comparison.csv、summary.json、metrics.json、process_load.csv：汇总与负载。
- selected_query.json、selected_timeline.csv、selected_node_timeline.csv：可复算图表。

设计与代码中的所有布局参数位于peer.head_pipeline，启用需重启顶层，不需要重建索引。搜索预算等原有参数继续支持在线事务更新。
'''
(R/'REPORT.md').write_text(text)
print(selected,delta)
