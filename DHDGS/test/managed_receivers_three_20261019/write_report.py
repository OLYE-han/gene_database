from pathlib import Path
import csv,json
import numpy as np
R=Path(__file__).resolve().parent
analysis=json.loads((R/'analysis.json').read_text());m=json.loads((R/'metric_summary.json').read_text())
q=json.loads((R/'selected_query.json').read_text());rx=q['stats']['head_receiver']
compute=json.loads((R/'selected_compute.json').read_text())
audit=json.loads((R/'version_audit.json').read_text());status=json.loads((R/'verified_status.json').read_text())
runs=[json.loads((R/f'run_{i}.json').read_text()) for i in range(3)]
rxstats=list(csv.DictReader((R/'receiver_stage_means.csv').open()))
text=f'''# 独立报告接收进程版：三机常驻复验

源码release `{audit['deployed_release']}` 已同步Intel-35/32/30，三台各核验{audit['nodes'][0]['checked']}个源码文件一致。Intel-35：一个HNSW主进程、一个报告接收子进程、独立data-0；Intel-32：data-1；Intel-30：data-2。三个数据节点每节点32查询线程，客户端并发128，预算200，top-k10，完整2M×2560D索引、同一2000查询，32条预热。旧FIFO集群在新服务ready后正常停止，新服务测试后保留运行。配置和命令见README.md。

## 三轮结果

|轮次|QPS|平均ms|p50 ms|p99 ms|Recall@10|失败|API>100ms|
|---:|---:|---:|---:|---:|---:|---:|---:|
'''
for i,r in enumerate(runs):text+=f"|{i+1}|{r['qps']:.2f}|{r['latency_ms']['mean']:.2f}|{r['latency_ms']['p50']:.2f}|{r['latency_ms']['p99']:.2f}|{r['recall_at_k_all_requests']:.3%}|{r['failed_queries']}|{rxstats[i]['api_over100']}|\n"
text+=f"\n6000条查询全部成功，所有预算守恒、无重复节点访问、无顶层搜索扩展、指标完整性审计通过。QPS中位数{analysis['median_qps']:.2f}，p99中位数{analysis['median_p99_ms']:.2f}ms。数据节点PID和索引fd前后不变，接收进程PID也保持不变；末次状态没有活动查询。当前接收进程PID：{status['head_receivers'][0]['pid']}。\n"
text+='''
同负载15组消融见 [接收进程数报告](../head_receivers_20261019/REPORT.md)：0→1个额外接收进程的p99中位数544.03→318.20ms，QPS473.89→586.14。2/4/8进程均没有优于1进程，因此发布1进程。消融扫描使用原有三数据进程；本页是新release常驻服务复验，两者分别报告，不能把独立时段差值当作严格因果收益。

## 固定查询734744的完整时间图

继续使用先前已选定的查询，不因慢而更换样本。选取本轮第1次正式测试。

![完整时序](query_timeline.png)

|顶层互斥阶段|ms|
|---|---:|
'''
for row in analysis['selected_head_stages']:text+=f"|{row['stage']}|{row['duration_ms']:.3f}|\n"
text+=f"\nAPI总计 **{q['client_wall_ms']:.3f}ms**，Recall@10 **{q['recall']:.0%}**；接收进程注册往返 **{rx['registration_ms']:.3f}ms**（已含在上表入口准备中，不另行相加）。\n"
text+='''
![接收及IPC细分](query_receiver.png)

上图两轨同属Intel-35有效时钟域；下轨与端到端轨道重叠，不能相加。终态IPC包含序列化、通道排队、主进程调度和解码，不是纯内核通信时间。远端数据节点继续各用接纳时刻作为独立原点，不跨机器相减单调时钟。

|数据节点（内部ID）|本地原生循环ms|接纳后释放查询线程ms|
|---|---:|---:|
'''
for c in compute:text+=f"|{c['node']}|{c['loop_wall_ms']:.3f}|{c['thread_released_after_accept_ms']:.3f}|\n"
text+='''
![计算区间](query_compute.png)

计算线程按本地候选耗尽/预算完成释放，不等远端子分支或顶层merge。pread是逻辑文件读取，包含热页缓存及调用调度，本次不能称为物理SSD耗时。

## 接收进程新增等待

|轮次|注册均值ms|终态IPC均值ms|主进程应用状态均值ms|查询恢复残余均值ms|
|---:|---:|---:|---:|---:|
'''
for i,r in enumerate(rxstats):text+='|'+str(i+1)+'|'+ '|'.join(f"{float(r[k]):.3f}" for k in ('registration_mean_ms','ipc_mean_ms','apply_mean_ms','query_resume_mean_ms'))+'|\n'
text+='''
新增接收进程降低了数据回传拥塞，但注册往返和主进程终态处理仍有明显等待。**系统等待低于100ms仍未达成**。异步通知、批量注册等下一步方案需先定位注册入队/出队与确认处理的等待，不能仅靠继续增加接收进程；尚未擅自采用未确认注册后提前派发查询的语义。

## 其余指标

下表对6000查询统计，保留原指标含义。并行分支累计时间不是端到端时间份额。

|指标|均值|p99|
|---|---:|---:|
'''
for key in ('query_ready_queue_ms','server_thread_cpu_ms','pread_ms','exact_distance_ms','rabitq_ms','expanded','max_expanded_depth','max_cross_shard_depth','peer_branch_rpc_count','control_rpc_count','all_protocol_rpc_count','all_protocol_bytes','file_read_bytes'):
 if key in m:text+=f"|{key}|{m[key]['mean']:.3f}|{m[key]['p99']:.3f}|\n"
text+='''
每查询逐阶段占比、图跳数、派发次数、网络字节、逻辑读/计算时间及召回均保存在runs/*/queries.jsonl.gz、queries.csv与汇总CSV。纯网络/物理SSD延迟占比仍不可从现有测量可靠分离，保留null，不把整个RPC或pread算成物理服务时间。

## 负载与构建

|轮次|进程|平均占用CPU核数|RSS MiB|
|---:|---|---:|---:|
'''
for i,r in enumerate(runs):
 for p in r['head_process_load']:text+=f"|{i+1}|{p['role']}|{p['mean_cpu_cores']:.3f}|{p['process']['rss_bytes']/2**20:.1f}|\n"
 for p in r['node_process_load']:text+=f"|{i+1}|data-{p['shard_id']}|{p['mean_cpu_cores']:.3f}|{p['process']['rss_bytes']/2**20:.1f}|\n"
physical=[p['process_deltas']['storage_read_bytes'] for r in runs for p in r['node_process_load']]
text+=f"\n三数据节点×三轮物理读取增量：{physical}字节。本轮是热页缓存测试。CPU平均核数按各进程采样窗口计算，不能直接解释成整机CPU百分比；负载表记录新增接收进程，未漏计这部分资源。索引复用，未重新构建；历史构建阶段见runs/*/build_stages.csv（若该轮导出）及原seq2m_full构建目录，部署编译时间不是索引构建时间。\n"
text+='''
## 工件与验证

- cluster.yaml：完整YAML配置；benchmark_request.yaml：测试请求体。
- version_audit.json、verification.json、verified_status.json：版本、三机拓扑与服务状态核对。
- head_stage_means.csv、receiver_stage_means.csv、entry_transport.csv：逐阶段、注册/IPC与入口池计时。
- selected_query.json、selected_events.json、selected_timeline.csv、selected_receiver_timeline.csv：可复查的单查询原始时间点。
- node_load.csv、head_process_load.json、metric_summary.json：数据/顶层进程负载与其余指标。
- collection.json指向统一拉回Intel-35的三节点结果快照。
- ../head_receivers_20261019/regression.log：完整260项回归258通过、2跳过；receiver_final.log额外确认9项接收专项全部通过。

保持错误记录：首次prepare缺少新部署token失败，补齐独立token后完成部署；失败部署日志不进入正式查询性能结果。旧数据、旧测试结果与索引均保留。
'''
(R/'REPORT.md').write_text(text)
print('report written',q['client_wall_ms'],analysis['median_qps'],analysis['median_p99_ms'])
