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

- [完整报告](../../rdma_transport_20261023/REPORT.md)
- [线程曲线](../../rdma_transport_20261023/curves.png)
- [等待转移](../../rdma_transport_20261023/queue_shift.png)
- [轮询间隔](../../rdma_transport_20261023/poll_sensitivity.png)
- [p99时序](../../rdma_transport_20261023/cases/02_rdma_s64/p99_timeline.png)
- [逐轮汇总](../../rdma_transport_20261023/comparison.json)
- [最终部署核验](../../rdma_transport_20261023/final_deployment_audit.json)
- [构建与回归](../../rdma_transport_20261023/final_verification.log)
- [复现与部署](../../rdma_transport_20261023/USAGE.md)
- [设计§40](../../../DESIGN.md)
- [单次测试历史日志归档，35条](../../rdma_transport_20261023/RUN_LOG_ARCHIVE.md)

**下一步建议**：默认保留TCP。先验证内部连接预建立/CM接纳长尾，再隔离负载并做CPU/NUMA/缓存剖析；确认计算关键路径后研究候选评分、布局和SSD流水。不能宣称目前已受图搜索串行算法主导。

**日志更新时间**：2026-10-23T10:22:56+08:00
**数据保留状态（2026-10-27）**：本轮大体积原始数据已按用户要求清理，报告/图表/汇总和选定样本保留；具体文件见[清理清单](../../storage_cleanup_20261027/README.md)。部分离线重分析需重新采集数据。

**数据保留状态（2026-10-29维护）**：本轮旧原始数据已按清单清理；报告、图表、汇总及选定样本保留。删除范围、恢复限制见 [保留记录](../../rdma_transport_20261023/RETENTION_20261029.md)。

<!-- experiment:rdma_transport_20261023:end -->
