<!-- experiment:replica_pipeline_20261028:start -->
## 流水复制、原生加载与后台选图实现和验证（replica_pipeline_20261028）

**状态**：已完成

**开始时间**：2026-10-28T14:22:25+08:00

**结束时间**：2026-10-28T15:54:34+08:00

**目的 / 要解决的问题**：R001-A13/A14：实现并验证固定布局后的有界流水复制、直接构造不可变overlay的原生加载、有界后台C++选图，缩短副本可用前等待及控制阻塞。

**假设**：提取和发送重叠、删除Python Shard中间层及释放GIL的后台选图能降低维护串行开销；前台收益仍取决于副本实际工作覆盖。

**期望效果**：保留完整性和搜索语义，先验证相同计划的字节/结果等价与取消，再以原始200K自然流检查提前发布、同ID质量和源/总CPU，不能把局部提速称为净负载均衡收益。

**环境与配置 / 对照**：Intel-35顶层+独立API客户端，Intel-31/32/30数据节点；8组各200K原始同序查询、2000预热、并发1024、64搜索线程/节点、预算200、RDMA rsockets、热缓存buffered pread。12次固定真实计划空闲复制及选图CPU微基准；时间覆盖实现/回归至报告收尾，单次测量边界见各case元数据。

**本轮改变的变量**

- observe无副本；baseline/baseline_r2为staged_v1+python+inline。
- pipeline仅流水复制；native仅原生加载；both两者组合且inline；async/async_r2两者组合+native_async。
- 同一真实489.55MiB副本，复制模式×加载模式4配置各3次；合成1024/4096/8192/16384观测，每规模5次交错Python/native选图。
- 首批及固定计划使用source；容量失败路径补强后使用source_final，其余成功路径及搜索配置一致。最初准备的第三次重复未运行，不作为结果。

**代码改动**：新增cpp/replica_load.hpp、cpp/replica_planner.hpp与Python replica_pipeline.py；replica_pack.hpp增加粗粒度完成范围回调，search_loop.hpp增加原生准备/短锁发布，native.cpp绑定原生选图；replicas.py分开传输/业务错误、v2能力/调度/取消/预留与原生加载，load_balance.py后台单任务及失效重检；config.py/YAML加入兼容开关及边界验证；rdma_benchmark.py接收轮次ID避免子测试多条主日志。80项回归、独立构建目录部署、哈希审计、报告/图表及DESIGN/LOCKS/Research更新。保留既有工作区其他改动。

**登记时 Git HEAD**：53b3b953d0f491019429da3535e182d7e56a804d

**结果与限制**：observe: QPS15936.35/p99229.60ms/Recall97.2623%/副本入口命中0；baseline: QPS15996.08/p99180.48ms/Recall97.2484%/副本入口命中6668；pipeline: QPS14923.75/p99262.17ms/Recall97.2279%/副本入口命中10844；native: QPS14301.98/p99279.33ms/Recall97.2347%/副本入口命中11759；both: QPS14183.27/p99308.42ms/Recall97.2204%/副本入口命中13382；async: QPS15455.39/p99207.99ms/Recall97.2261%/副本入口命中12647；baseline_r2: QPS14840.66/p99274.68ms/Recall97.2357%/副本入口命中8997；async_r2: QPS14132.39/p99305.67ms/Recall97.2259%/副本入口命中12030。固定计划READY中位4.584→2.966s，12包字节相同；原生加载及选图局部提速成立。160万查询零失败、预算/物理去重/区域执行审计通过、通信对账完整、最终排空。80项最终回归通过；初期fixture/浮点求和失败、CMake缓存部署失败及修复保留。初次丢ACK/能力回退等额外回归有复用覆盖，不把所有执行数相加为独立测试数。查询QPS与副本召回限制见报告，R001整体均衡仍开放。

**报告、图表与证据**

- [完整报告](../../replica_pipeline_20261028/REPORT.md)
- [固定计划耗时](../../replica_pipeline_20261028/fixed_lifecycle.png)
- [生产/发送重叠](../../replica_pipeline_20261028/stream_overlap.png)
- [自然流指标](../../replica_pipeline_20261028/comparison.png)
- [发布时间](../../replica_pipeline_20261028/publication_phases.png)
- [三节点负载](../../replica_pipeline_20261028/load_async_r2.png)
- [最终p99时序](../../replica_pipeline_20261028/runs/async_r2/cases/async_r2/p99_admission.png)
- [同ID对照](../../replica_pipeline_20261028/paired.json)
- [最终源码核对](../../replica_pipeline_20261028/verified_source_final.json)
- [配置](../../replica_pipeline_20261028/settings_source_final.yaml)
- [DESIGN §50](../../../DESIGN.md#replica-pipeline-native)、[Research](../../../Research.md#r001-a13)

**下一步建议**：保留显式开关与旧默认。A13/A14维护路径局部机制已验证；继续A15净源卸载/工作覆盖选图和A16跨授权搜索状态复用，减少更多副本命中带来的新增分支/重复准备，以命中群体同ID召回、总CPU及端到端指标验收。

**日志更新时间**：2026-10-28T15:54:34+08:00
<!-- experiment:replica_pipeline_20261028:end -->
