<!-- experiment:hotness_logical_20261026:start -->
## R001指数衰减热度与逻辑区域访问（hotness_logical_20261026）

**状态**：已完成

**开始时间**：2026-10-26T22:04:50+08:00

**结束时间**：2026-10-27T00:22:19+08:00

**目的 / 要解决的问题**：实现线程局部聚合的指数衰减热度和有界top-K；副本作为独立逻辑区域，离开时排除全部副本ID，并测量开销与召回

**假设**：有界后台热度合并能低开销识别热点；逻辑区域隔离支持副本到来源再到目标原片，并明确整区排除的质量代价

**期望效果**：无失败/超预算；区分真实展开和整区排除；完成全量200K对照、质量和热度统计误差/丢弃审计

**环境与配置 / 对照**：Intel-35 HNSW/API+独立TCP客户端；Intel-31/32/30数据节点；RDMA；全2M×2560索引/原始L2；同序200K查询；每节点64搜索线程、预算200、并发1024、top-k10、N16、remote_weight0.25；热度半衰期1000ms/合并25ms/局部1024/队列16/候选65536/top-K4096。

**本轮改变的变量**

- 初始普通Release本机基线/热度；随后统一Release/native本机构建，并修正同机交接不计跨机分支。两种构建不混作开销对照。
- 优化构建：关闭热度/副本的两次基线；仅热度；逻辑隔离+全α排除+动态副本，entry_worksets与hotness_worksets两种选区。每组200K不重复查询，保持顺序。
- 另有一次预热后亲和性采样失败，未正式测量；修复线程退出竞态。完整回归71项和新增误差界1项；在线原集合组含5秒perf采样，性能解释保留仪器与共享机器干扰。
- 子图8192记录上限、RDMA复制与质量门控；统计切图/复制次数及入口/peer访问，完整排除/热度对账、配对Recall；归档压缩/持久化及SHA校验均在正式测量外。

**代码改动**：新增cpp/decayed_hotness.hpp和logical_region.hpp；search_loop.hpp成功展开记录/副本隔离/精确排除集合；native_pool.hpp空闲flush；native_runtime.cpp逻辑接纳与同机交接；native_head.cpp身份/清单校验及真实物理跨机计数；Python热点工作集合选择/维护接口/协议与汇总、YAML校验；test_hotness_logical.py及旧时间敏感等价夹具修正；rdma_benchmark.py测量前重置/结束采样和退出线程容错。DESIGN §46、Research更新。

**登记时 Git HEAD**：00621f3d998fe7eb5f17467d2767b1e7dea4e494

**结果与限制**：七次完整200K共140万请求无失败，预算与同物理节点去重审计无违规；72项不同检查71通过1跳过。热度计数与实际展开、merged+dropped全部对账；热度在线组统计最高节点丢弃0.96831%。热度副本1次切图/复制8192条约123.10MiB，检测到发布31.104s；入口104/peer302次、406条查询，344次出区/461后代无排除ID重展开。QPS998.89/p996132.40ms/全量Recall96.980%；命中群体Recall92.4384%→90.6650%（-1.7734pp）。原工作集合组2次复制，其中1次流结束后发布无流量；命中199条，Recall下降0.5528pp。关闭热度同构建基线重复7843.64/1101.26QPS，存在共享负载与顶层长尾，未证实吞吐收益；不能将差异全归因热度。执行时间点覆盖完整，638条请求的部分控制/transfer统计离线后仍不完整。R001整体未解决。早期夹具失败、无测量启动失败和所有重试保留。

**报告、图表与证据**

- [完整报告](../../hotness_logical_20261026/REPORT.md)
- [对比图](../../hotness_logical_20261026/comparison.png)
- [热度图](../../hotness_logical_20261026/hotness.png)
- [跨副本p99附近查询时序图](../../hotness_logical_20261026/runs/heat_online_r2/cases/heat_online_r2/replica_timeline.png)
- [条件Recall](../../hotness_logical_20261026/runs/heat_online_r2/cases/heat_online_r2/paired_recall.png)
- [全部均值指标](../../hotness_logical_20261026/metrics.csv)
- [逐副本生命周期与访问](../../hotness_logical_20261026/regions.csv)
- [最终源码哈希](../../hotness_logical_20261026/final_source_hashes.json)
- [交付审计](../../hotness_logical_20261026/delivery_audit.json)
- [持久化捕获哈希](../../hotness_logical_20261026/persistent_capture_hashes.json)
- [低QPS离线诊断](../../hotness_logical_20261026/QPS_DIAGNOSIS.md)：2026-10-27补充分析已有数据，无新性能测量；合并排队均值655.57ms、搜索分支之和9.12ms，定位顶层完成路径积压，底层原因待隔离。保留本轮原始起止时间。
- [Research](../../../Research.md#r001) / [DESIGN §46](../../../DESIGN.md#hotness-logical-regions)

**下一步建议**：保持已确认全α排除语义；优先peer部分状态质量门控、工作集合贡献/首次出区质量、校准等待与顶层完成路径；稳定资源窗口交替重复A/B；控制器采集边界冻结、统计丢弃偏差及长测控制指标完整性。几何半径切图未实现，负载均衡默认仍关闭。

**日志更新时间**：2026-10-27T00:25:10+08:00
**数据保留状态（2026-10-27）**：本轮大体积原始数据已按用户要求清理，报告/图表/汇总和选定样本保留；具体文件见[清理清单](../../storage_cleanup_20261027/README.md)。部分离线重分析需重新采集数据。

**数据保留状态（2026-10-29维护）**：本轮旧原始数据已按清单清理；报告、图表、汇总及选定样本保留。删除范围、恢复限制见 [保留记录](../../hotness_logical_20261026/RETENTION_20261029.md)。

<!-- experiment:hotness_logical_20261026:end -->
