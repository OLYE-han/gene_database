<!-- experiment:budget_sweep_20261105:start -->
## 原成本分支与拉取同步策略的搜索预算扫描（budget_sweep_20261105）

**状态**：已完成

**开始时间**：2026-11-05T11:10:38+08:00

**结束时间**：2026-11-05T18:49:01+08:00

**目的 / 要解决的问题**：比较预算16/32/64/128/256下两种完整策略的Recall与QPS，识别约95%召回的成本

**假设**：拉取同步的召回收益可能随预算变化；较小预算可能减少计算但放大控制开销

**期望效果**：不预设策略优劣，获得每档两次同期对照、召回/吞吐/延迟曲线和预算审计

**环境与配置 / 对照**：D16/32/64/128/256，C1536、64搜索线程/数据节点、32路由/16合并线程、头部48物理核共享、API16 reactors×2 handlers、response_batch_size=8；固定预算轮次NUMA布局：Intel31计算CPU0–63/网络128–143/内存NUMA0–3，Intel32/30沿用iso2；每组600K预热+600K测量

**本轮改变的变量**：两策略×五预算×两重复；其余配置固定，修复前成绩不混入；返回路径修复前的三组有效测量保留但不参与新曲线

**代码改动**：生产算法未改；沿用YAML预算审计和原数据搜索代码；头部采用head_return修复版本/48核/API16/批次8，正式测量前统一Intel31到与外部Vivado不重叠的CPU/NUMA，其他节点不变；修复配置覆盖预算残留32的启动前错误并增加20组有效配置校验；实验增加冻结身份断言、外部作业预检、20组正反预算扫描和曲线，不混入修复前结果

**登记时 Git HEAD**：53b3b953d0f491019429da3535e182d7e56a804d

**结果与限制**：20组有效正式请求共12M，零失败及预算/账目错误。D16: QPS-0.67%，Recall+2.7401pp，p99+57.69%；D32: QPS-1.22%，Recall+4.6632pp，p99+99.99%；D64: QPS-1.11%，Recall+2.1940pp，p99+51.57%；D128: QPS-12.55%，Recall+0.3940pp，p99+12.62%；D256: QPS-11.84%，Recall+0.0021pp，p99-16.32%。原方案达到95%门槛的已测最高QPS配置original_d128：Recall=95.1130%，QPS=29755.84，p99=128.381ms；当前方案达到95%门槛的已测最高QPS配置current_d128：Recall=95.5069%，QPS=26021.48，p99=144.578ms。不把完整策略差异归因于单独READ；暖缓存/两重复/非独占边界保留。 启动前预算覆盖错误已修正，未产生查询；补充诊断定位高预算计算池排队，p99样例返回段0.758ms、就绪排队133.35ms，100ms等待目标仍未达到。

**报告、图表与证据**

- test/budget_sweep_after_return_20261105/REPORT.md
- test/budget_sweep_after_return_20261105/budget_curves.png
- test/budget_sweep_after_return_20261105/recall_qps.png
- test/budget_sweep_after_return_20261105/work_and_control.png
- test/budget_sweep_after_return_20261105/compute_queues.png
- test/budget_sweep_after_return_20261105/sample_stage_means.png
- test/budget_sweep_after_return_20261105/comparison.csv
- test/budget_sweep_after_return_20261105/comparison.json
- test/budget_sweep_after_return_20261105/evidence_verification.json
- test/budget_sweep_after_return_20261105/configuration_diff.json
- test/budget_sweep_after_return_20261105/configuration_validation.json
- test/budget_sweep_after_return_20261105/numa_isolation_revision.json
- test/budget_sweep_after_return_20261105/harness_failures.json
- test/budget_sweep_after_return_20261105/execution.json
- test/budget_sweep_after_return_20261105/DIAGNOSIS.md
- test/budget_sweep_after_return_20261105/branch_pull_sync_costs.json
- test/budget_sweep_after_return_20261105/compute_occupancy.json

**下一步建议**：保留唯一分支和全局预算，单独消融READ拉取与同步；拆解额外CPU工作；高预算检查Intel31服务容量/负载分配，低预算检查路由池容量；95%附近加密预算

**日志更新时间**：2026-11-05T18:58:55+08:00
<!-- experiment:budget_sweep_20261105:end -->
