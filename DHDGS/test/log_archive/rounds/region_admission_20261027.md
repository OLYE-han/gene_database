<!-- experiment:region_admission_20261027:start -->
## R001/R002区域协议接收积压、锁等待与负载均衡修复（region_admission_20261027）

**状态**：已完成

**开始时间**：2026-10-27T14:56:48+08:00

**结束时间**：2026-10-27T15:46:28+08:00

**目的 / 要解决的问题**：R002/R001/R010：定位新旧区域协议QPS差距，减少跨查询锁等待，并验证在线副本流程、质量和卸载。

**假设**：全节点接纳临界区阻塞接收handler，引发积压和无效扩容；按查询同步可恢复并行而保留同查询正确性。

**期望效果**：不增加搜索预算、搜索线程或网络资源恢复吞吐；副本仍须独立通过命中群体质量与净卸载验收。

**环境与配置 / 对照**：Intel-35顶层+客户端；Intel-31/32/30数据节点。七组各200K原始查询、2000预热、并发1024、64搜索线程/节点、预算200、RDMA rsockets、热缓存buffered pread。时间按首个有证据的case启动至最后case收尾，前置开发编译时间未纳入。

**本轮改变的变量**

- profile_new / profile_old：同份细分计时源码，region_tasks true/false，其余一致。
- profile_new / concurrent_base：启用concurrent_region_admission，重型接纳从节点mutex移至查询mutex；算法、资源、广播配置不变。
- concurrent_repeat：优化无副本配置独立复跑。
- concurrent_replica：并行接纳+动态副本，16K核心/32K区域、子预算比例0.5。
- replica_budget75：仅将副本向外子预算比例改为0.75，总预算仍为200。
- replica_region64k：子预算回到0.5，核心/区域上限32K/64K，字节容量改为1GiB。

**代码改动**：native_server.hpp/.cpp及runtime_bindings.hpp新增服务端线程局部阶段计时；native_runtime.cpp新增查询级接纳mutex、节点级短容量预留/提交、幂等重检、异常RAII回收及关闭/过期保护；config.py增加可选参数，configs/dhdgs.yaml启用区域任务与并行接纳；test_region_tasks.py新增重复接纳容量和时间点回归。保留原路径对照；不改变SSD或搜索算法。DESIGN/LOCKS/Research同步记录。

**登记时 Git HEAD**：53b3b953d0f491019429da3535e182d7e56a804d

**结果与限制**：七组共140万查询，全部零失败、预算违规和同物理节点重复展开为零，新协议执行器唯一性/无重叠审计通过，最终排空。旧协议17688 QPS；原新协议6646，优化16705，复跑16510；p99 702.32→166.14/181.73ms。接纳锁累计等待10.82→0.0153/0.0141ms/query，handler累计排队166.07→3.13/3.40ms/query。30项回归通过，三数据节点与本机87个源码文件一致。32K动态副本16119 QPS、2450命中，源CPU下降18.71%，条件Recall95.5469→94.5143%；增加子预算和子图大小未证明净收益。复制/分流流程正常不等于R001已解决。所有负面配置和期间外发布均保留，详见报告。

**报告、图表与证据**

- [完整报告](../../region_admission_20261027/REPORT.md)
- [指标对比](../../region_admission_20261027/comparison.png)
- [优化后p99时序](../../region_admission_20261027/runs/concurrent_base/cases/concurrent_base/p99_admission.png)
- [动态副本负载与发布时间](../../region_admission_20261027/load_concurrent_replica.png)
- [条件召回/卸载配对](../../region_admission_20261027/paired.json)
- [30项回归](../../region_admission_20261027/correctness_full.log)
- [实际部署源码核对](../../region_admission_20261027/verified_deployment.json)
- [完整配置](../../region_admission_20261027/settings_concurrent.yaml)
- [DESIGN §48](../../../DESIGN.md#query-scoped-admission)、[LOCKS](../../../LOCKS.md)、[Research](../../../Research.md)

**下一步建议**：已关闭无关查询串行接纳子问题；R001仍开放。继续区域搜索状态跨授权复用/有界批处理、实际展开进度与候选交接、独立质量门禁和更早的有效入口发布；测量区域FIFO等待，勿以无上限网路线程代替串行区修复。

**日志更新时间**：2026-10-27T15:51:57+08:00
**数据保留状态（2026-10-29维护）**：本轮旧原始数据已按清单清理；报告、图表、汇总及选定样本保留。删除范围、恢复限制见 [保留记录](../../region_admission_20261027/RETENTION_20261029.md)。

<!-- experiment:region_admission_20261027:end -->
