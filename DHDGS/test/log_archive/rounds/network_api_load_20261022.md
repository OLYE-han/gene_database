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
- [配置](../../network_api_load_20261022/settings.yaml)；[源码哈希](../../network_api_load_20261022/source_hashes.json)；[部署核验](../../network_api_load_20261022/audit.json)。

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

- [完整报告](../../network_api_load_20261022/REPORT.md)；[部署与复现](../../network_api_load_20261022/USAGE.md)。
- [QPS、p99 与排队曲线 PNG](../../network_api_load_20261022/curves.png) / [SVG](../../network_api_load_20261022/curves.svg)。
- [2048 并发跨节点近 p99 时序](../../network_api_load_20261022/cases/c2048/p99_timeline.png)；[最终排空图](../../network_api_load_20261022/cases/c2048/backlog.png)。
- [五档汇总](../../network_api_load_20261022/comparison.json)；各 cases/c*/ 中保存逐查询 CSV、p99 图及 summary.json。原始帧与离线 JSON 位于 /var/tmp/dhdgs_network_api_load_20261022/cases。

**下一步建议**

- 给响应编码排队/CPU、socket 发送及客户端解码增加独立时间戳；验证 reactor 内整批编码是否推迟收发，不将推测当作已定位结论。
- 评估将响应编码移出 reactor、限制每轮完成处理数量，并在相同计时口径和查询数下交替复验。
- 256 并发可作为吞吐接近最高且 p99 较低的候选；若优先本轮最低 p99 则选择 128。增加重复轮数和持续负载检验。

**日志更新时间**：2026-10-23T10:22:56+08:00
**数据保留状态（2026-10-27）**：本轮大体积原始数据已按用户要求清理，报告/图表/汇总和选定样本保留；具体文件见[清理清单](../../storage_cleanup_20261027/README.md)。部分离线重分析需重新采集数据。

<!-- experiment:network_api_load_20261022:end -->
