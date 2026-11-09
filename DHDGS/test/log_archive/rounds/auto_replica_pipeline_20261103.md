<!-- experiment:auto_replica_pipeline_20261103:start -->
## R001：自动副本预算、完整流水和规划截止时间（auto_replica_pipeline_20261103）

**状态**：已完成：流程验证通过，性能验收未通过

**开始时间**：2026-11-03T16:12:03+08:00

**结束时间**：2026-11-03T17:16:41+08:00

**目的 / 要解决的问题**：修复长测五次规划零复制；R001-A13/A28

**假设**：源相关容量和自动完整流水使自然流中的候选及时发布；长测接纳问题单独保留

**期望效果**：10分钟动态观测，报告失败、发布、净卸载、QPS及队列

**环境与配置 / 对照**：seq_2M完整200K自然循环，D200/C1024，64/68/68计算池，RDMA，暖缓存

**本轮改变的变量**

- 固定：D200/C1024，三数据计算池64/68/68，200K自然回放，暖缓存，RDMA；先120秒observe再600秒dynamic
- 新流程：自动完整h2核心/外层块增长复制；最多64种子、最多1目标；alpha总量≤0.3
- 规划60秒、收尾验证预留15秒；生命周期180秒；冷却10秒；ready结构发布
- 另轮测试窗口完成后隔离部署；主控及数据亲和/分配器沿用；未混入接纳回收修复

**代码改动**：源实际容量预算及预约传递；automatic_pipeline接完整h跳增长复制；统一规划期限、完整块软停止和ID队列期限；独立query证据及READY后发布门禁回滚。仅隔离快照C++规划头文件变化，未覆盖另会话的native_runtime改动。

**登记时 Git HEAD**：53b3b953d0f491019429da3535e182d7e56a804d

**结果与限制**：5次实际规划均发布，25次预算预拒；成功QPS6625.20，失败8.9618%，副本命中0.8463%，计算回源56.9822%，命中Recall94.5709%。59项定向检查与三机各9项网络检查通过；三机已退出，预约无泄漏。 约171/600秒出现搜索断供；短参照与长测不作因果归因，冻结检索代码未包含另一会话R002回收修复。R001仍开放。

**报告、图表与证据**：[完整报告](../../auto_replica_pipeline_20261103/REPORT.md)、[构建时序](../../auto_replica_pipeline_20261103/formal/first_replica_timeline.png)、[QPS/质量](../../auto_replica_pipeline_20261103/formal/throughput_quality.png)、[ready负载](../../auto_replica_pipeline_20261103/formal/ready_pressure.png)、[源码审计](../../auto_replica_pipeline_20261103/snapshot_audit.json)、[原始捕获](../../auto_replica_pipeline_20261103/raw_capture.tar.gz)、[DESIGN](../../../DESIGN.md#automatic-replica-pipeline-20261103)

**下一步建议**：合入接纳回收修复后等时长无LB/自动LB对照；优化入口覆盖与退化观测；补齐拓扑能力和命中群体配对质量验证。

**日志更新时间**：2026-11-03T17:16:41+08:00
<!-- experiment:auto_replica_pipeline_20261103:end -->
