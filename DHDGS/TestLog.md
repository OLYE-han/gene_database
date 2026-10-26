# DHDGS 实验日志

时间使用 Asia/Shanghai（UTC+08:00）。围绕同一问题的一轮实验只保留一条汇总，包含各次测试改变的变量、结果及负面结论。
单次测试的时间、配置和失败证据保存在 test/ 下；历史补记不推测缺失的起止时间。

<!-- experiment:network_api_load_20261022:start -->
## 2026-10-22 独立网络 API 与持续结果消费（历史补记）（network_api_load_20261022）

**状态**：已完成（补记）

**开始时间**：2026-10-22；历史实验未采集墙钟起止时间，无法可靠恢复时分秒

**结束时间**：2026-10-22；精确墙钟结束时间未知，单调时间不能直接当作日历时间

**目的 / 要解决的问题**：解耦 DHDGS 与压测/消费模块，验证持续接收结果能否避免最终积压，并观察五档并发下的吞吐与等待。

**假设**：独立原生 TCP 客户端与有界持续消费可排空结果；提高并发可能提高吞吐，但接口排队也可能增加尾延迟。

**期望效果**：25 万主测试查询完整返回、最终队列排空；找出 QPS 与 p99 的取舍，检验消费能力是否限制吞吐。

**环境与配置 / 对照**

- Intel-35 顶层 + 本地数据节点 + 独立压测进程，Intel-32/Intel-30 各一个数据节点；完整 seq_2M 索引、热缓存。
- 每节点 64 搜索线程、预算 200、top-K=10；head 路由 32/合并 8；API 8 reactor × 2 handler、4096 连接。
- 客户端 16 lane（48 工作线程），每 lane 队列 256 条/64 MiB；并发 128/256/512/1024/2048，每档预热 2000、计时 50000；2000 验证向量重复 25 次。另做 2048 并发/32 lane 的 50000 条容量对照。
- [配置](test/network_api_load_20261022/settings.yaml)；[源码哈希](test/network_api_load_20261022/source_hashes.json)；[部署核验](test/network_api_load_20261022/audit.json)。

**本轮改变的变量**

- 客户端并发128/256/512/1024/2048，各50000请求；每节点64搜索线程、预算200固定。
- 额外容量对照：并发2048下客户端lane由16增至32；其他主参数保持一致。

**代码改动**

- 新增 cpp/native_search_api.{hpp,cpp}、python/dhdgs/network_api.py：公共 TCP 查询与原生请求产生、收发、持续消费。
- 新增 tools/network_load.py、search_api_service.py、process_network_results.py、configs/network_api_benchmark.yaml、tests/test_network_search_api.py。
- 调整 cpp/native_server.{hpp,cpp}、cpp/runtime_bindings.hpp、python/dhdgs/cpp_head.py、CMakeLists.txt：延迟响应、外部完成回调及生命周期处理；更新 DESIGN.md §39、test/README.md。
- 仅以上改动归属于本实验；工作区其他既有改动不归入本轮。冻结源码见 /var/tmp/dhdgs_network_api_load_20261022/source；源码哈希为实验版本依据。

**登记时 Git HEAD**：历史运行版本以 source_hashes.json / audit.json 为准，不以补记时 HEAD 代替

**结果与限制**

- 并发 128/256/512/1024/2048：QPS 分别 6909.2/8883.7/8939.0/8781.3/8814.8，p99 分别 47.19/58.48/118.43/177.37/322.29 ms；Recall@10 约 97.03%。
- 主测试 250000 条、容量对照 50000 条全部成功；最终服务/计算/控制/API 队列排空，测试进程退出。4 项 API 专项和 32 项既有回归通过。
- 客户端扩为 32 lane 后 QPS 8930.2（+1.31%），p99 323.62 ms，未显示显著收益；各档单次测量不能证明统计显著性。
- 2048 并发 API 额外时间约 181.10 ms，其中回传约 116.53 ms，不能都解释为纯网络传输；最终消费/同步尾部约 3.8–4.5 秒另列。
- 旧约 12000 QPS 使用原生完成计时、延后输出；新约 8900 QPS 包含 TCP 请求/响应。计时口径和批次数不同，不能作为严格同条件回退比例；当前热缓存、闭环短批次不证明冷 SSD 或无限持续超载能力。

**报告、图表与证据**

- [完整报告](test/network_api_load_20261022/REPORT.md)；[部署与复现](test/network_api_load_20261022/USAGE.md)。
- [QPS、p99 与排队曲线 PNG](test/network_api_load_20261022/curves.png) / [SVG](test/network_api_load_20261022/curves.svg)。
- [2048 并发跨节点近 p99 时序](test/network_api_load_20261022/cases/c2048/p99_timeline.png)；[最终排空图](test/network_api_load_20261022/cases/c2048/backlog.png)。
- [五档汇总](test/network_api_load_20261022/comparison.json)；各 cases/c*/ 中保存逐查询 CSV、p99 图及 summary.json。原始帧与离线 JSON 位于 /var/tmp/dhdgs_network_api_load_20261022/cases。

**下一步建议**

- 给响应编码排队/CPU、socket 发送及客户端解码增加独立时间戳；验证 reactor 内整批编码是否推迟收发，不将推测当作已定位结论。
- 评估将响应编码移出 reactor、限制每轮完成处理数量，并在相同计时口径和查询数下交替复验。
- 256 并发可作为吞吐接近最高且 p99 较低的候选；若优先本轮最低 p99 则选择 128。增加重复轮数和持续负载检验。

**日志更新时间**：2026-10-23T10:22:56+08:00
<!-- experiment:network_api_load_20261022:end -->

<!-- experiment:experiment_logging_20261022:start -->
## 2026-10-22 实验日志流程接入与校验（experiment_logging_20261022）

**状态**：已完成

**开始时间**：2026-10-22T23:15:41+08:00

**结束时间**：2026-10-22T23:15:41+08:00

**目的 / 要解决的问题**：将每轮实验的时间、目的、代码变更、产物与建议持久化，避免仅依赖对话记忆。

**假设**：运行边界自动记录和按 ID 更新可保留成功/失败状态，同时不干扰测量热路径。

**期望效果**：同一实验重复生成报告不重复追加；并发记录不丢失；失败保留证据。

**环境与配置 / 对照**：Intel-35 本地临时目录，离线功能校验；未启动集群或重新运行性能实验。

**代码改动**

- 新增 AGENTS.md、tools/experiment_log.py、test/EXPERIMENT_WORKFLOW.md、TestLog.md。
- 网络 API run.py 接入逐 case 生命周期，write_report.py 接入报告收尾；新增 experiment.yaml 历史补记；更新 test/README.md、.gitignore。

**登记时 Git HEAD**：391a67715264adde28108331e9b16fa5b4add3ed

**结果与限制**：通过幂等更新、成功/异常生命周期、24 个并发记录者完整性检查；3 个 Python 文件语法通过，已有日志全部产物链接存在。未测量新的 QPS/p99。

**报告、图表与证据**

- [流程文档](test/EXPERIMENT_WORKFLOW.md)；[日志工具](tools/experiment_log.py)；[历史实验元数据](test/network_api_load_20261022/experiment.yaml)。
- 验证为临时目录中的离线断言，无性能图表；校验结论保存在本条日志。

**下一步建议**：后续每轮实验使用独立 ID 与目录；完成离线报告后补齐实际结论。旧 runner 使用时接入相同流程，异常退出后检查未结束条目。

**日志更新时间**：2026-10-22T23:15:41+08:00
<!-- experiment:experiment_logging_20261022:end -->

<!-- experiment:rdma_transport_20261023:start -->
## 2026-10-23 独立顶层、三数据节点 TCP/RDMA 对照（rdma_transport_20261023）

**状态**：已完成

**开始时间**：2026-10-23T00:32:00+08:00

**结束时间**：2026-10-23T02:37:14+08:00

**目的 / 要解决的问题**：检验网络协议/带宽是否限制搜索线程扩容收益，并迁移本地分片到 Intel-31。

**假设**：同一 InfiniBand 链路上 RDMA 可降低 TCP 开销；若带宽占用远低于实测容量且 RDMA 没有收益，需继续定位计算/控制等待。

**期望效果**：提供相同拓扑、预算及线程数的 TCP/RDMA 重复对照，保留负结果及实现限制。

**环境与配置 / 对照**：Intel-35 仅 head + 独立压测；Intel-31/32/30 为 shard0/1/2。最新网络 API 最优观测档 C512，其余配置固定；64 搜索线程 TCP/RDMA 交替各3轮，另32/128线程各协议2轮。每轮预热2000、计时50000，预算200/top10。私有 librdmacm 54 同时用于两种传输。 另补6轮64线程轮询1/5μs对照与TCP参照，总计20轮100万正式查询；共享环境、热页缓存。

**本轮改变的变量**

- 内部协议TCP/RDMA；搜索线程32/64/128，主对照按协议交替重复。公共API始终使用TCP。
- 正式测量并发512、每次50000请求、预热2000、预算200固定；主对照14次，轮询补充6次；失败/冒烟另存证据。
- 轮询间隔：RDMA 1/5μs及TCP 50μs参照；链路容量检查对照16/64流。
- 部署与正确性检查：旧/私有RDMA运行库、混合fd轮询和反向RPC；不混入成功性能对照，详见报告与归档。

**代码改动**

- 新增 cpp/native_transport.{hpp,cpp}，为 native_wire/native_server/native_network 接入可选 rsockets 后端。
- 新增配置 native_runtime.network_module.transport / rdma_poll_interval_us；CMake 可选 RDMA 构建；公共 API 显式保留 TCP。
- RDMA 独立接纳线程和按描述符类型区分的非阻塞轮询；新增 tests/test_native_rdma.py。
- Intel-31 分片复制并逐文件 SHA256 核验；测试运行库私有固定至 librdmacm 54，不修改系统库；远端清理构建避免墙钟差异导致旧对象复用。
- 更新 DESIGN.md §40；源码哈希、部署、失败诊断和实验脚本见测试目录。
- 终审修正 native_head.cpp 历史硬编码的 transport 指标，增加聚合回归；修正单独保存哈希，不改写冻结性能记录。

**登记时 Git HEAD**：391a67715264adde28108331e9b16fa5b4add3ed

**结果与限制**：100万正式查询全部成功，Recall@10 97.005%–97.040%，1,352,705/1,352,705分支trace完整，无预算越界/数据节点重访。64线程TCP/RDMA QPS中位数7137/6036、p99为170/730ms；主对照TCP32线程候选QPS7622、p99 151ms。RDMA未改善性能；一个p99样本建连714.5ms，计算线程已及时释放。619条控制指标仍不完整，查询结果完整；全部队列排空。共享背景流量峰值61.54Gbit/s，不能彻底排除网络/其他业务干扰。正式查询物理读增量仅5.28MB，不能外推冷SSD。最终39项回归及TCP-only后端验证通过。

**报告、图表与证据**

- [完整报告](test/rdma_transport_20261023/REPORT.md)
- [线程曲线](test/rdma_transport_20261023/curves.png)
- [等待转移](test/rdma_transport_20261023/queue_shift.png)
- [轮询间隔](test/rdma_transport_20261023/poll_sensitivity.png)
- [p99时序](test/rdma_transport_20261023/cases/02_rdma_s64/p99_timeline.png)
- [逐轮汇总](test/rdma_transport_20261023/comparison.json)
- [最终部署核验](test/rdma_transport_20261023/final_deployment_audit.json)
- [构建与回归](test/rdma_transport_20261023/final_verification.log)
- [复现与部署](test/rdma_transport_20261023/USAGE.md)
- [设计§40](DESIGN.md)
- [单次测试历史日志归档，35条](test/rdma_transport_20261023/RUN_LOG_ARCHIVE.md)

**下一步建议**：默认保留TCP。先验证内部连接预建立/CM接纳长尾，再隔离负载并做CPU/NUMA/缓存剖析；确认计算关键路径后研究候选评分、布局和SSD流水。不能宣称目前已受图搜索串行算法主导。

**日志更新时间**：2026-10-23T10:22:56+08:00
<!-- experiment:rdma_transport_20261023:end -->

<!-- experiment:network_parallel_20261023:start -->
## 四机64搜索线程网络并行与动态扩容优化（network_parallel_20261023）

**状态**：已完成

**开始时间**：2026-10-23T02:44:21+08:00

**结束时间**：2026-10-23T06:00:10+08:00

**目的 / 要解决的问题**：定位RDMA吞吐下降，优化网络线程、连接和排队，向端到端12000QPS及网络时延占比低于30%目标验证

**假设**：按需建连长尾、接收扩容对已有连接无效及过度唤醒影响并行；Intel31搜索成本增长可能形成独立上限

**期望效果**：相同索引/预算/召回下QPS≥12000，逐查询平均与固定近p99样本网络/控制残余占比均严格低于30%；负结果保留。

**环境与配置 / 对照**：Intel35 head+client，Intel31/32/30数据节点，搜索64线程固定，预算200，top10；起点C512/50k查询，后续高并发与突发对照

**本轮改变的变量**

- 固定三数据节点各64搜索线程、预算200/top10、完整seq_2M索引；内部TCP/RDMA对照。
- 负载与预热：并发256/512/1024/2048，请求数5万/10万/20万，预热2000/20000；最终对照固定20万请求、预热2000。长预热且与编译重叠的对照已排除。
- 网络建连：按需建连与预热8连接，对同时建连设置上限2；同步、仅异步socket/connect、完整异步握手分别对照。
- 动态网络配置：基线与queued压力扩容；激进配置发送4→16、接收8→24，最终发送4→32、接收8→32；阈值、采样、冷却及连接迁移批量随方案联合改变，非单变量消融。精确值见settings_all.yaml各case。
- CPU/NUMA布局：默认调度、核隔离、NUMA交错、Intel31搜索CPU0–63/内存0–3与CPU64–127/内存4–7；Intel30网络预留物理核/不绑定/SMT对照。
- 顶层与客户端：路由线程4档与原32档、API接收reactor 8/16、head入口发送4→16或8→32；glibc/jemalloc在数据节点、顶层和客户端分别对照，顶层localalloc及线程放置联合对照。
- 容量与消费：接纳/保留身份扩至4096/262144；最终fdatasync由各lane独立执行改为所有网络生产者结束后执行，保留溢出与接纳失败结果。
- 辅助定位：64线程共享pread描述符/独立open/dup微基准，缓存NUMA位置检查，35秒空闲扩缩容观察。每次实际变量组合、版本与失败状态见ALL_RUNS.md和各case配置。

**代码改动**

- 连接预热、并发建连上限、queued压力与接收连接迁移；部署编译器/退避问题修复见日志。
- 在v1上增加CPU放置及因果派发/完成时间戳；不同YAML进行NUMA/扩容对照。
- 有界异步socket/connect及调用/握手计时；CPU/索引内存位置对照。
- 增加可选完整CM握手异步推进，保留部分移出模式及TCP。
- 修复draining长连接迁移优先级和持久连接缩容；保留同期消费者溢出失败。
- 在v5上增加独立客户端最终同步屏障；本阶段搜索/协议代码固定，变化为YAML、NUMA/分配器/API容量与实验采集。
- 离线报告生成器增加YAML验收门限及逐项判定；按用户确认的双重占比标准重分类已有结果，保留原始测量与起止时间。

**登记时 Git HEAD**：391a67715264adde28108331e9b16fa5b4add3ed

**结果与限制**：RDMA通过验收：并发1024（2轮）、并发2048（1轮）；未通过：并发512（1轮）。验收同时要求QPS≥12000、平均及近p99样本网络/控制残余占比均严格低于30%；不是每条查询的比例保证。 最终指标见final_acceptance.json，主重复轮次QPS 16208.45、p99 146.148ms。

**报告、图表与证据**

- [REPORT.md](test/network_parallel_20261023/REPORT.md)
- [performance_comparison.png](test/network_parallel_20261023/performance_comparison.png)
- [final_concurrency.png](test/network_parallel_20261023/final_concurrency.png)
- [p99_final.png](test/network_parallel_20261023/p99_final.png)
- [ALL_RUNS.csv](test/network_parallel_20261023/ALL_RUNS.csv)
- [final_acceptance.json](test/network_parallel_20261023/final_acceptance.json)
- [acceptance.yaml](test/network_parallel_20261023/acceptance.yaml)
- [recommended.yaml](test/network_parallel_20261023/recommended.yaml)
- [单次测试历史日志归档](test/network_parallel_20261023/RUN_LOG_ARCHIVE.md)

**下一步建议**：在相同资源与质量下验证持续负载和冷SSD；对搜索CPU、内存布局和负载不均做单变量消融。

**日志更新时间**：2026-10-23T10:26:34+08:00
<!-- experiment:network_parallel_20261023:end -->

<!-- experiment:rdma_default_persistence_20261023:start -->
## 默认RDMA方案持久化验证（rdma_default_persistence_20261023）

**状态**：已完成

**开始时间**：2026-10-23T10:26:52+08:00

**结束时间**：2026-10-23T10:46:28+08:00

**目的 / 要解决的问题**：将已验收RDMA实现、默认参数和启动依赖移出实验临时目录

**假设**：固化配置与运行路径不会改变既有检索及传输行为

**期望效果**：四机依赖核验、真实RDMA回归及小规模端到端查询通过

**环境与配置 / 对照**：Intel35顶层，Intel31/32/30数据；64搜索线程、预算200、推荐并发1024；功能验证不宣称新性能收益

**代码改动**：持久化默认RDMA YAML、保留pilot配置、增加独立部署/运行入口、修正旧配置迁移默认值

**登记时 Git HEAD**：391a67715264adde28108331e9b16fa5b4add3ed

**结果与限制**：47项回归通过，含真实IB与默认配置；最终配置2项复核通过。持久化入口预热2000/正式2000，零失败，Recall@10 97.34%，无预算越界/重访，实际RDMA，所有新服务退出。46条协议统计标记未闭合，保留限制。短验证不作为新性能选型。 最终启动固定到发布快照，额外128条端到端请求全部成功，实际RDMA且无预算/重访错误；1条协议统计未闭合。已核验三个远端源码与快照一致。

**报告、图表与证据**

- [默认方案说明](docs/RDMA_DEFAULT.md)
- [持久化验证报告](test/rdma_default_persistence_20261023/REPORT.md)
- [部署核验](test/rdma_default_persistence_20261023/deployment_audit.json)
- [端到端验证](test/rdma_default_smoke_20261023/REPORT.md)
- 未生成新图表：本次为默认配置/部署验证，未执行性能扫描。
- [固定发布入口复核](test/rdma_default_pinned_smoke_20261023/REPORT.md)

**下一步建议**：使用已持久化的默认入口开展后续实验；另行验证持续负载与冷SSD。

**日志更新时间**：2026-10-23T10:46:28+08:00

## 2026-10-23 历史部署数据清理说明（非性能实验）

2026-10-17 seq2m 全量部署的两份 `/tmp/dhdgs_seq2m_full_20261017/payloads/node_{1,2}.tar.gz` 已删除；原始包不可再作为可用证据引用。全部非索引成员已保存为 `artifacts/historical_snapshots/seq2m_full_20261017/node_{1,2}_source_snapshot.tar.gz` 并校验；索引与持久化版本逐文件哈希一致。现存历史报告、图片保持不变。清理范围、释放空间及文件审计见 [清理报告](test/storage_cleanup_before_20261022_20261023/REPORT.md)。

<!-- experiment:search_scaling_unbounded_20261023:start -->
## 搜索线程16/32/48/64/96与无应用层上限动态网络扩缩容（search_scaling_unbounded_20261023）

**状态**：已完成

**开始时间**：2026-10-23T10:38:10+08:00

**结束时间**：2026-10-23T14:11:40+08:00

**目的 / 要解决的问题**：检验搜索线程增加能否提高吞吐、降低计算排队；定位非线性扩展原因并按证据优化

**假设**：解除网络线程/连接数量上限后，搜索工作量与可用物理核决定扩展曲线；NUMA、SMT、负载不均或控制开销可能限制加速

**期望效果**：固定预算及查询质量，获得可解释的线程扩展曲线和网络资源时间序列，不预设线性加速成立

**环境与配置 / 对照**：Intel35 head+独立网络客户端；Intel31/32/30数据；seq_2M完整索引，预算200，top10，初始并发1024，每正式测试20万请求、预热2000；热缓存

**本轮改变的变量**

- 搜索线程16/32/48/64/96，固定并发1024、预算200；原版每档2次，最终版16/32/48各1次、64/96各2次，每次20万请求、预热2000。
- 所有服务端收发reactor、连接及公共API数量上限为0；busy-lane比例0.5、预热连接8、并行建连2、压力4样本、采样10ms、冷却100ms、空闲1500ms。网络资源动态增长/回收；CPU亲和范围固定，客户端容量4096。
- 跳过已见RaBitQ估距关闭/开启；缩短顶层指标与接收扩容锁范围；计数器stripe=1/128；有预算ID直接寻址关闭/64MiB。
- 96线程跨插槽物理核与原插槽物理核+SMT布局诊断；source/source_opt/source_head/source_lookup四个冻结版本、同二进制开关对照；全部负结果与重试保留。

**代码改动**：实现网络数量上限0、繁忙lane比例扩容与空闲回收、轻量网络资源采样、压缩帧读取；增加skip_seen_estimates、有容量预算的native_id_lookup_bytes、可选expansion_counter_stripes及counter_update_ms；缩短顶层监测/接收扩容锁范围。当前工作区重新编译；没有替换既有默认发布快照。DESIGN §43、LOCKS §18及实验复现脚本同步更新。

**登记时 Git HEAD**：6bfdad4cccbfbebeadab3a27ca5f3d93ae97e1c1

**结果与限制**：33次正式测量660万请求全部成功，Recall@10 97.017%–97.060%，8892908个分支完整时序，预算/重访违规0。最终16/32/48/64/96线程QPS均值为6903/12962/19132/21315/19629；64线程比原版均值提升16.1%，两次p99为107.48/127.92ms；平均网络/控制残余16.94%/19.48%，固定近p99样本2.26%/1.33%，均通过既定吞吐与双占比条件。96线程理想线性扩展未达成：最忙节点每分支CPU成本4.961→8.661ms，另有NUMA/SMT及负载不均因素；同插槽SMT诊断22815 QPS但p99 129.60ms。分散计数器未稳定提速，网络仍存在未完全定位的长尾。1条在途心跳导致统计完整性标记未完成，结果与搜索时序完整。最新回归60通过/1项io_uring按配置跳过，四机源码和实际加载二进制核对通过。热缓存、2000条验证向量循环，不能外推冷SSD/百亿数据；64线程完整输出吞吐含最后同步落盘为10629–10820。lookup_s96在预热中因控制会话中断，保留证据后以retry重测；初次配置回归、构建辅助脚本及离线idle字段审计的错误已修正，均未混入成功性能结论。

**报告、图表与证据**

- [完整报告](test/search_scaling_unbounded_20261023/REPORT.md)
- [完整测量/验收/资源表](test/search_scaling_unbounded_20261023/RESULTS.md)
- [吞吐、p99及排队曲线](test/search_scaling_unbounded_20261023/scaling_curves.png)
- [同版本扩展效率](test/search_scaling_unbounded_20261023/scaling_efficiency.png)
- [逐节点计算成本](test/search_scaling_unbounded_20261023/per_node_scaling.png)
- [64线程跨节点p99时序](test/search_scaling_unbounded_20261023/cases/lookup_s64/p99_timeline.png)
- [64线程复测p99时序](test/search_scaling_unbounded_20261023/cases/lookup_s64_repeat/p99_timeline.png)
- [网络长尾负例](test/search_scaling_unbounded_20261023/network_tail_diagnosis.json)
- [推荐配置摘要](test/search_scaling_unbounded_20261023/recommended.yaml)
- [源码哈希核对](test/search_scaling_unbounded_20261023/final_source_audit.json)
- [实际加载二进制/归档核对](test/search_scaling_unbounded_20261023/capture_audit.json)
- [空闲资源回收](test/search_scaling_unbounded_20261023/idle_summary.json)
- [最终进程核验](test/search_scaling_unbounded_20261023/final_process_audit.json)
- [复现步骤](test/search_scaling_unbounded_20261023/USAGE.md)

**下一步建议**：当前吞吐候选为64线程，48线程单次p99更低；96线性目标尚未完成。增加reactor内部时间点，固定节点负载做NUMA局部/远端与量化码加载/算术剖析，再评估紧凑ID、邻居批量处理、NUMA局部缓存及热点分片负载均衡。进一步用更多独立查询、长期流量与冷SSD验证。

**日志更新时间**：2026-10-23T14:11:40+08:00
<!-- experiment:search_scaling_unbounded_20261023:end -->

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

- [REPORT.md](test/concurrency_best_20261023/REPORT.md)
- [concurrency_curves.png](test/concurrency_best_20261023/concurrency_curves.png)
- [control_and_output.png](test/concurrency_best_20261023/control_and_output.png)
- [network_scaling.png](test/concurrency_best_20261023/network_scaling.png)
- [node_load.png](test/concurrency_best_20261023/node_load.png)
- [concurrency_runs.csv](test/concurrency_best_20261023/concurrency_runs.csv)
- [concurrency_groups.csv](test/concurrency_best_20261023/concurrency_groups.csv)
- [network_resource_summary.csv](test/concurrency_best_20261023/network_resource_summary.csv)
- [quality_audit.json](test/concurrency_best_20261023/quality_audit.json)
- [final_process_and_source_audit.json](test/concurrency_best_20261023/final_process_and_source_audit.json)
- [tail_diagnosis.json](test/concurrency_best_20261023/tail_diagnosis.json)
- [settings.yaml](test/concurrency_best_20261023/settings.yaml)
- [lookup_source_hashes.json](test/concurrency_best_20261023/lookup_source_hashes.json)

**下一步建议**：512是吞吐/时延折中候选，但固定p99网络占比未达30%；若维持该验收口径，优先对1024做更长稳态复测。延迟优先可关注128。进一步定位接收处理与报告控制长尾、热点节点计算排队；不能用更多连接推断等待已消失。维持预算与召回一致，后续增加未重复查询和冷SSD负载验证。

**日志更新时间**：2026-10-23T15:32:36+08:00
<!-- experiment:concurrency_best_20261023:end -->
