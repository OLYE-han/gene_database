<!-- experiment:replica_ready_20261027:start -->
## R001副本就绪即发布与维护流程精简（replica_ready_20261027）

**状态**：已完成

**开始时间**：2026-10-27T16:22:15+08:00

**结束时间**：2026-10-27T16:52:00+08:00

**目的 / 要解决的问题**：R001-A12：缩短副本就绪到发布、取消在线shadow重放，精简无用预热/评分和高成本选图集合操作，验证提前引流及净卸载。

**假设**：逐批效果门禁限制入口覆盖并消耗CPU；候选容量检查反复复制累计集合造成控制线程停顿。保留完整性检查的ready发布和等价集合优化可减少维护时间。

**期望效果**：更早接纳更多查询，同时独立评估相同query ID的源CPU、总CPU及召回，不能把结构就绪等同效果已验证。

**环境与配置 / 对照**：Intel-35顶层+独立API客户端，Intel-31/32/30数据节点。五组各200K原始同序查询、2000预热、并发1024、每节点64搜索线程、预算200、RDMA rsockets、热缓存buffered pread。日志时间为首个case启动至最后case收尾，不含前置开发编译。

**本轮改变的变量**

- observe：只观测，不复制。
- validated：16K核心/32K区域，shadow渐进效果门禁，结构评分开启，额外预热16MiB。
- ready：结构就绪即开放全部计划入口，无shadow回放，改用有上限压力反馈。
- lean_ready：ready+关闭未使用结构评分+额外预热0字节。
- fast_ready：同lean_ready，仅优化容量集合运算、入口成员查找与观测批次查找；计划语义不变。
- 离线选图微基准：1024/4096/8192条合成轨迹、每规模5次交错重复，逐字段核对计划。

**代码改动**：load_balance.py新增publication_mode=ready、明确未验证标识、有上限压力反馈、删除cpu_worksets未用热度RPC、集合差集容量判断和成员查找优化；replicas.py可关闭未用结构评分/重复预热并记录打包/传输/commit/安装阶段；config.py与YAML增加参数和依赖校验，默认仍validated；head_merge.py修复既有Python/native区域和RPC计数字段差异；新增回归、报告与分析脚本，DESIGN §49/Research同步。未改本轮C++搜索、预算或去重语义。

**登记时 Git HEAD**：53b3b953d0f491019429da3535e182d7e56a804d

**结果与限制**：observe: QPS16163.00/p99205.99ms/Recall97.2648%/副本命中0；validated: QPS16160.95/p99181.36ms/Recall97.2582%/副本命中1286；ready: QPS16228.27/p99175.06ms/Recall97.2494%/副本命中6367；lean_ready: QPS16105.08/p99186.93ms/Recall97.2568%/副本命中4785；fast_ready: QPS16100.93/p99190.37ms/Recall97.2498%/副本命中5326。共100万查询零失败，执行器唯一性/无重叠、物理节点内去重与预算审计通过，最终排空。27项针对性回归、聚合4项及后续8项通过（有重复覆盖，不能相加作独立测试数）。初始74项宽范围收集2失败1错误均保留：端口及GIL时序单项重跑通过，聚合差异在旧冻结源码复现后修复。选图微基准约11倍CPU加速，不能当成QPS加速比。局部机制已验证，净吞吐和命中质量结论见报告；R001保持开放。

**报告、图表与证据**

- [完整报告](../../replica_ready_20261027/REPORT.md)
- [结论与建议](../../replica_ready_20261027/CONCLUSION.md)
- [指标曲线](../../replica_ready_20261027/comparison.png)
- [发布流程对照](../../replica_ready_20261027/publication_phases.png)
- [最终配置三节点负载](../../replica_ready_20261027/load_fast_ready.png)
- [最终配置p99时序](../../replica_ready_20261027/runs/fast_ready/cases/fast_ready/p99_admission.png)
- [同ID召回/卸载](../../replica_ready_20261027/paired.json)
- [源码部署核对](../../replica_ready_20261027/verified_deployment.json)
- [完整设置](../../replica_ready_20261027/settings_source_fast.yaml)
- [DESIGN §49](../../../DESIGN.md#replica-ready-publication)、[Research](../../../Research.md)

**下一步建议**：保留validated默认和ready显式实验路径。优先降低完整打包后发送/重复构建索引的READY前成本，提高副本搜索工作覆盖并减少回源新增分支；继续以同ID质量和源净卸载验收，不能只以入口增加关闭R001。

**日志更新时间**：2026-10-27T16:58:34+08:00
**数据保留状态（2026-10-29维护）**：本轮旧原始数据已按清单清理；报告、图表、汇总及选定样本保留。删除范围、恢复限制见 [保留记录](../../replica_ready_20261027/RETENTION_20261029.md)。

<!-- experiment:replica_ready_20261027:end -->
