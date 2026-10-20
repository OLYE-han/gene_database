from pathlib import Path
import json
R=Path(__file__).resolve().parent;a=json.loads((R/'analysis.json').read_text());rows=a['medians'];old=rows[0];best=next(r for r in rows if r['receivers']==1)
s='''# 顶层独立报告接收进程：三机对照实验

固定完整 seq_2M、Intel-35/32/30 三台数据服务器、每数据节点32查询线程、客户端并发128、预算200、top-k10、原每端点2连接。每组2000条相同查询，预热32条，0/1/2/4/8进程交错各重复3轮，共30000条正式查询，全部成功，预算/节点访问/指标完整性审计通过。数据进程及索引保持不变；每个实验head退出后启动下一组，没有同时压测。0指原主进程接收，1指额外增加1个报告接收进程，始终只有一个HNSW主进程。

## 吞吐与延迟

下表是每组3轮的指标中位数，完整逐轮值见 comparison.csv。

|独立接收进程|QPS|平均 ms|p50 ms|p99 ms|Recall@10|API>100ms/2000|
|---:|---:|---:|---:|---:|---:|---:|
'''
for r in rows:s+=f"|{r['receivers']}|{r['qps']:.2f}|{r['mean']:.2f}|{r['p50']:.2f}|{r['p99']:.2f}|{r['recall']:.3%}|{r['api_over100']:.0f}|\n"
s+=f"\n按p99优先选择1个独立接收进程：相对0进程，QPS提高{(best['qps']/old['qps']-1):.1%}，p99下降{(1-best['p99']/old['p99']):.1%}。但API超过100ms的查询比例上升，不能宣称所有查询均改善。系统等待低于100ms的目标尚未达成。\n"
s+='''
## 等待发生在哪里

下表同为每组3轮均值的中位数，单位ms。接收进程相关字段在0组不存在。数据节点报告指标按分支统计，不能与逐查询阶段直接相加。

|接收进程|入口连接池|入口返回后等齐报告|本机注册往返|终态发布→主进程收到|数据报告就绪→开始发送|数据发送→ACK返回|
|---:|---:|---:|---:|---:|---:|---:|
'''
for r in rows:
 def v(k):return f"{r[k]:.2f}" if k in r else '—'
 s+=f"|{r['receivers']}|{v('pool_mean')}|{v('after_entry_mean')}|{v('registration_mean')}|{v('ipc_delivery_mean')}|{v('report_ready_to_sender_mean')}|{v('report_sender_ack_mean')}|\n"
s+='''
接收进程隔离后，ACK处理不再等待HNSW主进程。1进程组大幅缩短了报告就绪后的发送等待；这一指标包含数据节点发送调度和回传连接获取，不是纯网络延迟。主进程仅在收齐后收到一次终态报告包。

注册仍需在派发初始查询前确认，父进程IPC读写线程与128个入口线程共享主进程GIL。新测量显示注册往返约96ms，终态IPC约23ms。这些包含IPC排队、序列化/解码与调度，不能全部称作内核通信成本。4/8接收进程进一步降低数据端报告等待，却使主进程终态IPC等待约90ms以上，没有改善最终p99；不是接收进程仍然不够多。

1进程与0组均只有一个回调端点，可较好隔离独立进程的作用；2/4/8组同时增加回调端点和总体回传连接容量，不能把全部差异单独归因于GIL。当前没有CPU绑核、内核事件或GIL等待追踪，不能定量归因到具体调度器。

## 完整指标与复现

- experiment.yaml：组顺序、查询量、并发与回调端口。
- cases/*/plan.yaml：每组完整部署/搜索配置。
- cases/*/result.json：QPS、尾延迟、召回、逐指标分布、三数据节点CPU/物理I/O及审计。
- cases/*/runs/*/queries.jsonl.gz：每查询、每分支、网络/逻辑读取/图跳数及生命周期时间点。
- 同目录 nodes_before/after.json.gz、queries.csv、process_load.csv 等保留完整测试资料。
- comparison.csv、analysis.json：全部15轮及分组汇总。
- scan_sources/、scan_source_hashes.json：扫描使用的源代码快照。发布版另外修正了热更新预算的逐查询传递、IPC超限显式失败，并增加接收进程状态与CPU监测；不改变本轮固定预算的搜索和接收策略。发布版另做三轮复验。
- regression.log：完整260项回归，258通过、2跳过；receiver_final.log：9项接收专项通过，含额外的终态包超限检查。跳过项不能视为验证成功。

测试的物理读计数见analysis.json，当前全为0，是热OS页缓存测试；pread仍是逻辑读取，不是物理SSD服务时间。未重建索引，历史构建资料沿用已有完整2M索引。

运行命令（既有cases会拒绝覆盖，重新实验请复制到新的test子目录并同步脚本路径）：

```sh
gene_database/DHDGS/.venv/bin/python gene_database/DHDGS/test/head_receivers_20261019/run_scan.py
gene_database/DHDGS/.venv/bin/python gene_database/DHDGS/test/head_receivers_20261019/analyze.py
```

三机常驻服务部署、同一查询时间图和最终复测见 ../managed_receivers_three_20261019/。
'''
(R/'REPORT.md').write_text(s)
print('report written')
