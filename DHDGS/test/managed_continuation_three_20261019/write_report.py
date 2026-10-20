from pathlib import Path
import csv,json
import numpy as np
R=Path(__file__).resolve().parent
analysis=json.loads((R/'analysis.json').read_text());metrics=json.loads((R/'metric_summary.json').read_text())
q=json.loads((R/'selected_query.json').read_text());rx=q['stats']['head_receiver']
audit=json.loads((R/'version_audit.json').read_text());status=json.loads((R/'verified_status.json').read_text())
runs=[json.loads((R/f'run_{i}.json').read_text()) for i in range(3)]
rxstats=list(csv.DictReader((R/'receiver_stage_means.csv').open()));timeline=list(csv.DictReader((R/'selected_flow_timeline.csv').open()))
s=f'''# 注册确认与事件续执行：三机发布复验

版本 `{audit['deployed_release']}`，三台机器各核验{audit['nodes'][0]['checked']}个源码文件一致。Intel-35运行1个HNSW主进程、1个独立接收进程和data-0；Intel-32运行data-1；Intel-30运行data-2。主进程2个就绪阶段工作线程，接收进程4个初始派发线程，独立控制/终态IPC通道。数据节点各32查询线程，并发128、预算200、top-k10、相同2000查询、预热32，复用完整seq_2M索引。新服务ready后停止旧接收版集群，测试后保留新版服务。

## 三轮结果

|轮次|QPS|平均ms|p50 ms|p99 ms|Recall@10|失败|API>100ms/2000|
|---:|---:|---:|---:|---:|---:|---:|---:|
'''
for i,r in enumerate(runs):s+=f"|{i+1}|{r['qps']:.2f}|{r['latency_ms']['mean']:.2f}|{r['latency_ms']['p50']:.2f}|{r['latency_ms']['p99']:.2f}|{r['recall_at_k_all_requests']:.3%}|{r['failed_queries']}|{rxstats[i]['api_over100']}|\n"
s+=f"\nQPS中位数{analysis['median_qps']:.2f}，p99中位数{analysis['median_p99_ms']:.2f}ms。6000条查询全部成功、预算与访问审计通过，数据进程PID和索引fd保持，接收进程PID保持。最终接收状态：{status['head_receivers']}。\n"
s+='''
严格受控消融另见 [优化实验报告](../head_continuation_20261019/REPORT.md)：同一数据进程上，旧同步路径与2个续执行线程的QPS中位数571.13→603.33，p99为334.98→259.61ms，注册实际就绪82.51→1.12ms，终态IPC22.93→3.25ms。1、4个续执行线程没有更优p99。单独分通道虽降低注册等待却恶化p99，未单独发布。

本页是同版本重新部署后的独立复验；不同实验窗口的差值不等于单独的优化因果效果。**系统等待低于100ms仍未达成**，主要剩余等待在顶层首次和合并就绪队列。

## 固定查询734744完整分解

沿用先前固定查询，第1轮正式结果，不按快慢挑选。

![顶层互斥时序](query_flow_timeline.png)

|阶段|ms|
|---|---:|
'''
for row in timeline:s+=f"|{row['stage']}|{float(row['duration_ms']):.3f}|\n"
s+=f"\n总API **{q['client_wall_ms']:.3f}ms**，Recall@10 **{q['recall']:.0%}**。注册实际就绪{rx['registration_ready_ms']:.3f}ms；registration_ms=0表示没有同步注册ACK阻塞，不表示注册无延迟。上表所有阶段互斥并精确覆盖client_wall，初次和恢复排队均已计入。\n"
s+='''
![各节点独立时间轴](query_timeline.png)

![接收进程终态交付](query_receiver.png)

![原生搜索计算区间](query_compute.png)

不同数据节点用各自接纳时刻作为独立原点。顶层及其接收子进程核对同一clock_domain后才进行跨进程相减；网络报告发送到顶层收到之间没有跨机器单调时间差。计算线程本地完成即释放，不等其他分支或顶层合并。API同步调用线程仍会阻塞等待返回并释放CPU/GIL；内部就绪阶段工作线程不等待IPC或数据结果。

## 等待与其余指标

|轮次|注册实际就绪均值ms|终态IPC均值ms|主进程应用状态均值ms|
|---:|---:|---:|---:|
'''
for i,r in enumerate(rxstats):s+='|'+str(i+1)+'|'+ '|'.join(f"{float(r[k]):.3f}" for k in ('registration_mean_ms','ipc_mean_ms','apply_mean_ms'))+'|\n'
s+='''
|指标（6000查询）|均值|p99|
|---|---:|---:|
'''
for key in ('head_stage_queue_ms','head_stage_resume_queue_ms','query_ready_queue_ms','coordinator_thread_cpu_ms','server_thread_cpu_ms','pread_ms','exact_distance_ms','rabitq_ms','expanded','max_expanded_depth','max_cross_shard_depth','peer_branch_rpc_count','control_rpc_count','all_protocol_rpc_count','all_protocol_bytes','file_read_bytes'):
 if key in metrics:s+=f"|{key}|{metrics[key]['mean']:.3f}|{metrics[key]['p99']:.3f}|\n"
s+='''
注册后在接收进程继续初始派发，注册尚未完成时不会发送数据查询；终态Future同时要求入口派发成功与所有分支报告齐备。等待中的查询始终占据128在途名额，未通过改变实际并发、减少搜索预算或省略计时阶段获得收益。数据间扩展仍仅由原扩展函数批准。

累加的分支CPU/pread时间与并行等待不等同于端到端占比。纯网络与物理SSD份额无法可靠分离，保留null。注册/终态IPC是本机开销，不混入跨机协议字节。除新的顶层队列和注册指标外，原召回、网络/读取/计算、跳数、跨节点通信次数均保留在每查询记录。

## 进程负载

|轮次|角色|平均CPU核数|RSS MiB|
|---:|---|---:|---:|
'''
for i,r in enumerate(runs):
 for p in r['head_process_load']:s+=f"|{i+1}|{p['role']}|{p['mean_cpu_cores']:.3f}|{p['process']['rss_bytes']/2**20:.1f}|\n"
 for p in r['node_process_load']:s+=f"|{i+1}|data-{p['shard_id']}|{p['mean_cpu_cores']:.3f}|{p['process']['rss_bytes']/2**20:.1f}|\n"
physical=[p['process_deltas']['storage_read_bytes'] for r in runs for p in r['node_process_load']]
s+=f"\n三个数据节点×三轮物理读取增量：{physical}字节。本轮属于热页缓存测试，pread不代表物理SSD服务时间。CPU核数按各进程观测窗口计算，窗口略有采样差异。没有重新构建索引，原构建阶段资料保留在runs/*/build_stages.csv及seq2m_full原目录；发布编译不能当作索引构建时间。\n"
s+='''
## 工件与复现

README.md给出部署、复测及热配置指令。cluster.yaml保存所有可调参数；benchmark_request.yaml保持2000/128/32。日常复测使用cluster_ctl benchmark，run_test.py只用于首次新旧服务切换。

- runs/*/queries.jsonl.gz与queries.csv：每query原始指标。
- runs/*/nodes_before/after.json.gz、process_load.csv：数据节点生命周期/CPU/磁盘。
- selected_flow_timeline.csv、selected_query.json、selected_events.json：可复算单查询分解。
- receiver_stage_means.csv、head_stage_means.csv、head_process_load.json：注册/IPC/负载。
- verification.json、version_audit.json、verified_status.json：版本及排空核验。
- collection.json：三节点指标统一回收位置（Intel-35的本目录内）。

发布前282项回归280通过、2跳过，包括共享/分离通道、报告早于入口ACK、无报告超时、进程死亡、取消清理、预算热更新及一个工作线程管理多个挂起查询。日志在../head_registration_20261019/regression_final.log。

未采用cProfile异常轮次评估性能或CPU函数占比。该工具的冲突与不一致输出保留在../head_continuation_20261019/profile*/；剩余阶段诊断使用stage_timers/的直接线程CPU/墙钟计时。所有正式对照和本页复验均未启用该剖析器。
'''
(R/'REPORT.md').write_text(s)
print('written',q['client_wall_ms'],analysis['median_qps'],analysis['median_p99_ms'])
