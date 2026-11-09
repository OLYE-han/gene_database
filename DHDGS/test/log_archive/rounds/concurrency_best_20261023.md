<!-- experiment:concurrency_best_20261023:start -->
## 最优64搜索线程配置的四机并发扫描（concurrency_best_20261023）

**状态**：已完成

**开始时间**：2026-10-23T14:36:55+08:00

**结束时间**：2026-10-23T15:24:45+08:00

**目的 / 要解决的问题**：固定当前吞吐较好的64线程配置，定位并发128至2048的吞吐、尾延迟与排队拐点。

**假设**：增加在途请求可能先提高利用率，接近饱和后主要增加排队；无需假定更高并发必然提高QPS。

**期望效果**：获得可复现的QPS/p99/计算排队/网络排队及资源曲线，并检查所有结果排空。

**环境与配置 / 对照**：Intel-35 HNSW及独立TCP客户端；Intel-31/32/30各64搜索线程；内部真实RDMA；预算200，top10，seq_2M全量索引，2000验证查询循环；每例预热2000、正式200000；沿用lookup_s64的CPU/NUMA/连接策略；动态网络数量上限0（应用层不封顶）。

**本轮改变的变量**

- 并发128、256、512、1024、2048；每组两次，第一遍递增、第二遍递减；其余配置固定。

**代码改动**：不修改搜索或网络运行代码；新增独立实验配置、运行/汇总/绘图与归档脚本。复用上一轮source_lookup冻结源码和已验证二进制。

**登记时 Git HEAD**：af7f9ad0345cea5db5d5a18d46071f50b0444303

**结果与限制**：C128: QPS 12288, p99均值 22.43ms（22.19–22.67）；C256: QPS 18883, p99均值 38.72ms（37.91–39.52）；C512: QPS 21472, p99均值 54.41ms（47.27–61.55）；C1024: QPS 22346, p99均值 101.64ms（78.98–124.29）；C2048: QPS 20745, p99均值 222.46ms（194.50–250.41）。共2,000,000请求，0失败；2,690,729条分支时间戳完整，预算/重访错误0，所有结果消费、终态队列归零并停止测试进程。补齐后指标缺失0。查询来自2000验证向量循环、热缓存，单例正式窗口约9–16秒；两次范围不是置信区间。512与1024均位于吞吐平稳区间，2048增加排队；网络/控制长尾未普遍满足30%目标，固定512近p99样本保留并诊断。完整输出QPS另列。无失败或重跑；原始记录已无损压缩验证并归档，未改搜索/网络源码或默认部署。

**报告、图表与证据**

- [REPORT.md](../../concurrency_best_20261023/REPORT.md)
- [concurrency_curves.png](../../concurrency_best_20261023/concurrency_curves.png)
- [control_and_output.png](../../concurrency_best_20261023/control_and_output.png)
- [network_scaling.png](../../concurrency_best_20261023/network_scaling.png)
- [node_load.png](../../concurrency_best_20261023/node_load.png)
- [concurrency_runs.csv](../../concurrency_best_20261023/concurrency_runs.csv)
- [concurrency_groups.csv](../../concurrency_best_20261023/concurrency_groups.csv)
- [network_resource_summary.csv](../../concurrency_best_20261023/network_resource_summary.csv)
- [quality_audit.json](../../concurrency_best_20261023/quality_audit.json)
- [final_process_and_source_audit.json](../../concurrency_best_20261023/final_process_and_source_audit.json)
- [tail_diagnosis.json](../../concurrency_best_20261023/tail_diagnosis.json)
- [settings.yaml](../../concurrency_best_20261023/settings.yaml)
- [lookup_source_hashes.json](../../concurrency_best_20261023/lookup_source_hashes.json)

**下一步建议**：512是吞吐/时延折中候选，但固定p99网络占比未达30%；若维持该验收口径，优先对1024做更长稳态复测。延迟优先可关注128。进一步定位接收处理与报告控制长尾、热点节点计算排队；不能用更多连接推断等待已消失。维持预算与召回一致，后续增加未重复查询和冷SSD负载验证。

**日志更新时间**：2026-10-23T15:32:36+08:00
**数据保留状态（2026-10-27）**：本轮大体积原始数据已按用户要求清理，报告/图表/汇总和选定样本保留；具体文件见[清理清单](../../storage_cleanup_20261027/README.md)。部分离线重分析需重新采集数据。

<!-- experiment:concurrency_best_20261023:end -->
