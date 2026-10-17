# 每数据节点只启动一次：并行本地搜索验证

2026-10-16（Asia/Shanghai）。根据用户确认，原节点派发后继续本地搜索，后继不接收原节点待扩展的本地候选，也不回访已经访问的数据节点。

## 实现语义

- 每个查询携带 visited_nodes；每个节点最多一个后继。唯一向新节点派发的权限沿访问链传递，祖先节点同时继续本地计算。
- 后继接收目标种子以及其他未访问节点的候选，防止“候选已发现但未转交”被 seen 快照永久挡住。仅属于已访问节点的候选被过滤；当前节点拥有本地副本仍可直接读取。
- 数据服务按 query ID 限制二次启动，同一 branch 的重复提交只 ACK。顶层检查节点唯一性、访问链和预算守恒。去重状态仅在内存与保留窗口内有效，不提供进程崩溃恢复。
- 节点初次派发前仍可因 max_wait_ms 超时或本地耗尽触发派发；派发后不再向外启动搜索。候选等待从本节点真实入队时刻计时，修正了旧实现整轮累加的偏差。
- parent_local / child_remote 的计算可以重叠；不是单执行节点迁移。转发请求带 visited_nodes，最终结果各自回传顶层合并。

## 最终版本测试

复用 4096 × 2560 的平方 L2、RaBitQ 1-bit 索引；同样 128 条查询、预算 256、remote_weight=1、max_wait_ms=1、top-k=10、详细 trace 开启。各并发工作线程预热 4 条。

| 并发 | 成功数 | Recall@10 | 平均 ms | p99 ms | QPS | 平均跨节点 RPC | 最大跨节点 RPC | 节点回访 |
|---|---|---|---|---|---|---|---|---|
| 1 | 128/128 | 0.99921875 | 34.56 | 49.70 | 28.86 | 0.922 | 2 | 0 |
| 4 | 128/128 | 0.99921875 | 213.24 | 330.07 | 18.67 | 0.922 | 2 | 0 |

44 项回归测试全部通过，覆盖原节点派发后继续本地执行、1→2→3 不回访、未访问节点候选正确转交、同查询二次启动拒绝、重试幂等、预算守恒和 TCP 多进程。两个并发组均无查询失败，所有本次服务已停止。

并发 1 每查询平均重复顶点扩展约 1.82 次，并发 4 约 2.09 次：这是本地副本的重复工作，不是重复启动服务器。并发 1 平均协议字节约 75.62 KiB，并发 4 约 77.06 KiB。

当前仍未获得并发 4 的吞吐收益，需要继续定位 CPU/线程/控制路径的瓶颈。此次只验证小索引、回环 TCP 和缓存环境；不能据此推断全量数据上的召回或物理 SSD 性能。

## 记录与复现

- 每组保留首次实现验证及最终修订验证两次独立 runs，以上统计采用最终版本（时间戳较晚）的报告。首次验证后补齐了备用入口候选容量上限和携带候选的传输成本估计。
- provenance.json 对应首轮，final_provenance.json 对应最终版本源码哈希；配置、执行命令、服务日志和回归日志均单独保存。
- index 为既有不可变索引的符号链接；本次未重建索引。
- node_revisit_count、visited_nodes、visited_node_candidates_dropped、remote_candidates_dropped_after_forward 已加入逐查询 CSV。最后一项指父节点停止处理的远端任务，其中可能已转交后继，不等于全局候选丢失。

- 并发 1：[完整报告](concurrency_1/runs/local-cluster-test_20261016T090806_95d3c59a/report.json)、[逐查询指标](concurrency_1/runs/local-cluster-test_20261016T090806_95d3c59a/queries.csv)、[服务状态](concurrency_1/runs/local-cluster-test_20261016T090806_95d3c59a/services.json)、[节点负载](concurrency_1/runs/local-cluster-test_20261016T090806_95d3c59a/process_load.csv)。
- 并发 4：[完整报告](concurrency_4/runs/local-cluster-test_20261016T090819_25f65ba1/report.json)、[逐查询指标](concurrency_4/runs/local-cluster-test_20261016T090819_25f65ba1/queries.csv)、[服务状态](concurrency_4/runs/local-cluster-test_20261016T090819_25f65ba1/services.json)、[节点负载](concurrency_4/runs/local-cluster-test_20261016T090819_25f65ba1/process_load.csv)。
