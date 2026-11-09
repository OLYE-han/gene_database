<!-- experiment:replica_revisit_20261026:start -->
## R001可重访搜索与全量在线子图复制（replica_revisit_20261026）

**状态**：已完成

**开始时间**：2026-10-26T16:35:59+08:00

**结束时间**：2026-10-26T18:44:50+08:00

**目的 / 要解决的问题**：修正部分副本场景下物理节点不可重访的路径截断；在完整200K自然查询流中检测失衡、在线切图复制并记录每版本入口/peer访问，验证质量与反应速度

**假设**：按分支身份和预算允许重访，以成功展开驻留限制控制跨机；有限核心/halo、构建时入口权重和低频控制能快速卸载自然热点

**期望效果**：无失败/超预算、本机重复展开受控；不改变查询分布，完整报告复制时间/空间/命中及条件召回，不以整体均值掩盖质量损失

**环境与配置 / 对照**：Intel-35顶层+独立TCP客户端；Intel-31/32/30数据0/1/2；64搜索线程/节点、预算200、top10、1024并发、内部RDMA rsockets。七案每案完整200K不同查询，同洗牌seed20261026，预热2000；2M×2560维原L2索引，热页缓存、共享机器、每配置一次。

**本轮改变的变量**

- revisit_base_r1/online_r1：N32、remote_weight0.25、无本机共享展开表，observe对照dynamic整区影子门控；核心4096/区域8192，1跳halo，256MiB总字节/128MiB编码上限
- claims_base_r2/online_entries_r2：增加原子本机展开表；observe对照逐入口影子门控，最多32探测、实验最少1探测/入口、总合格样本至少8；拒绝/零命中结果保留
- dwell16_base_r3/online_dwell16_r3：N16；dynamic冷却500ms、初始分流0.5、步长0.1、最大0.75、peer目录8192项；此前目标历史p95导致分流持续减半
- online_backlog_r4：同N16参数，目标资格和退让改用当前积压估计，目标阈值16ms；最终源0→目标2。source_v3只改变控制器/测量后快照压缩，C++与v2相同
- 所有测试跨机空前沿提前退出开启并计数；最终影子模式不读取200K真值。工作区默认仍关闭load_balance，默认N32、独立真值门控，未把实验门槛宣布为最优或安全默认

**代码改动**：C++按查询+分支接纳/幂等/续租取消、顶层分支树预算校验、祖先已展开历史与候选分离、驻留/例外、原子本机展开表、peer只读副本目录及明确未接纳回退；Python构建时入口驻留权重、容量检查、影子逐入口门控、当前积压控制、生命周期/访问计数；YAML与热配置、benchmark对账/归档及回归。三版冻结源码，最终171文本文件四机核对、实际加载原生库匹配，见final_source_process_audit.json。

**登记时 Git HEAD**：00621f3d998fe7eb5f17467d2767b1e7dea4e494

**结果与限制**：七案合计140万正式请求均成功且预算≤200；最终58项回归57通过、1后端跳过，早期容量fixture和新增零值字段断言失败已修正且保留。启用本机展开表后同物理节点重复ID为0，最终避免1526385次本机重复，跨机仍约6.34次/query。最终15563.26QPS/p99179.03ms/Recall96.989%；切图/复制各1次，8192记录/129.16MiB，检测至发布4.432s，32入口+448peer访问、478不同查询（0.239%）。命中子集配对Recall93.8703%→93.5356%，9差/4好，低样本根入口门控不足以验证peer部分状态。早期两候选拒绝、后续发布太晚和仅7命中均保留；不能归因全局QPS变化或关闭R001。全轮6次切图/复制；所有案最终队列排空/进程退出，最终343375分支时间覆盖完整，返回时34136条待补统计离线全部对账。热缓存/单次共享机器不外推冷SSD或论文级稳定收益。

**报告、图表与证据**

- [完整报告](../../replica_revisit_20261026/REPORT.md)
- [七案比较](../../replica_revisit_20261026/comparison.png)
- [所有子图版本及访问次数](../../replica_revisit_20261026/regions.csv)
- [实际分流时序](../../replica_revisit_20261026/runs/online_backlog_r4/cases/online_backlog_r4/replica_accesses.png)
- [副本命中近p99查询时序](../../replica_revisit_20261026/runs/online_backlog_r4/cases/online_backlog_r4/replica_timeline.png)
- [三节点负载](../../replica_revisit_20261026/runs/online_backlog_r4/cases/online_backlog_r4/load_timeline.png)
- [全部查询近p99样例](../../replica_revisit_20261026/runs/online_backlog_r4/cases/online_backlog_r4/p99_timeline.png)
- [命中子集配对质量](../../replica_revisit_20261026/runs/online_backlog_r4/cases/online_backlog_r4/paired_hit_cohort.json)
- [最终配置](../../replica_revisit_20261026/settings_v4.yaml)
- [源码/库/清理核对](../../replica_revisit_20261026/final_source_process_audit.json)
- [回归输出](../../replica_revisit_20261026/checks/final_regression/output.log)
- [实现算法](../../../research/R001_BOUNDED_REVISIT.md)
- [DESIGN §45](../../../DESIGN.md#bounded-revisit)
- [Research台账](../../../Research.md)

**下一步建议**：首先以相同前沿/历史/剩余预算独立校准peer副本改道，验证驻留先验与条件召回；再研究有限依赖邻域、多小区域容量调度和增量打包/校验，提升覆盖率与反应速度。保持完整200K自然负载，做交错重复及冷SSD实验，质量/覆盖通过后才评价QPS/p99与源净卸载。R001仍研究中。

**日志更新时间**：2026-10-26T18:51:49+08:00
**数据保留状态（2026-10-27）**：本轮大体积原始数据已按用户要求清理，报告/图表/汇总和选定样本保留；具体文件见[清理清单](../../storage_cleanup_20261027/README.md)。部分离线重分析需重新采集数据。

**数据保留状态（2026-10-29维护）**：本轮旧原始数据已按清单清理；报告、图表、汇总及选定样本保留。删除范围、恢复限制见 [保留记录](../../replica_revisit_20261026/RETENTION_20261029.md)。

<!-- experiment:replica_revisit_20261026:end -->
