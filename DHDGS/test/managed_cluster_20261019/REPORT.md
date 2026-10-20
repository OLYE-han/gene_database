# 持久集群管理与跨机验证报告

**实现已完成；指定物理三机测试尚未完成。** Intel-35 与 Intel-30 已完成部署验证，Intel-32 的管理网地址 172.16.33.32 与数据网地址 192.168.33.32 多次 SSH 超时。不能将下述两机诊断作为 Intel-35/32/30 三机性能结果。正式拓扑和参数保存在 [cluster.yaml](cluster.yaml)，操作见 [README.md](README.md)。

## 实现与验收范围

- Intel-35 独立的 HNSW head 进程与 data-0 进程；远端各一个持久 data 进程；初始每节点 6 个查询工作线程。
- head 接收 YAML 参数补丁，排空查询后向三个节点执行带版本的配置事务。数据进程与索引 fd 保留；预算、remote_weight、线程数等可更新。head 的 ef_search 变更通过在排空后替换 HNSW 对象实现。
- head 发起固定查询集 benchmark，拉取三节点前后指标并集中归档。另提供独立 collect 接口。
- 配置失败时拒绝新查询；recover 同步到 head 已持久化选择的版本。节点重启恢复热参数并保持暂停，等待 head 同步。
- 源码 SHA256 清单与归档、编译日志、固定远程启动脚本、启动/停止命令均已提供。复用已有完整分片，没有重传约 21GB 原始向量或重新建图。

最终交付源码版本为 `05b5f3cf0079fd9ca3dd`。另已实际执行正式配置的 `start-data --nodes 0 2`：Intel-35/Intel-30 分别启动 PID 890224/14627，鉴权状态接口正常、启动后暂停等待 head；随后通过管理接口停止并确认端口关闭。证据见 [启动验证](launch_verification.json) 和 [清理验证](launch_cleanup.json)。Intel-32 未启动。

完整回归 **227 项通过**：[regression_final.log](regression_final.log)。此后针对 HNSW ef_search 更改补充的管理集成测试 **5 项通过**：[unit_tests_final.log](unit_tests_final.log)，覆盖实际 TCP、查询后改 ef_search、PID/fd 保持、部分 commit 失败恢复、节点重启持久化、鉴权和非法更新拒绝。

## 两物理机诊断（不是三物理机测试）

完整索引 2M×2560D，L2，top-k10，固定 2000 validation 查询，预热32；一个 head 进程内32个客户端工作线程；原生搜索/原生控制器、buffered sync_pread。实际拓扑：

| 角色 | 物理主机 | 测试 PID | 查询工作线程 |
|---|---|---:|---:|
| head | Intel-35 | 881811 | 客户端并发32 |
| data-0 | Intel-35 | 881808 | 6 |
| data-1（临时替代 Intel-32） | Intel-35 | 881809 | 6 |
| data-2 | Intel-30 | 4211 | 6 |

两个数据节点与 head 同在 Intel-35，另一个数据节点在 Intel-30。SSH 主机实际 hostname 记录在 initial_status，不能根据主机别名推断物理分离。

| 指标 | 初始预算200、remote_weight=1 | 热更新预算160、remote_weight=0.5 |
|---|---:|---:|
| 成功 / 失败查询 | 2000 / 0 | 2000 / 0 |
| Recall@10 | 97.035% | 96.455% |
| QPS | 461.42 | 501.01 |
| API平均延迟 | 65.84 ms | 60.62 ms |
| API p50 | 61.97 ms | 60.24 ms |
| API p95 | 119.75 ms | 116.60 ms |
| API p99 | 170.96 ms | 134.60 ms |
| 查询就绪队列等待均值（分支求和） | 0.538 ms | 0.492 ms |
| 查询就绪队列等待 p99（分支求和） | 2.780 ms | 2.055 ms |
| 查询就绪队列等待最大值（分支求和） | 23.062 ms | 3.666 ms |
| 每查询应用协议 RPC 均值 | 3.052 | 2.995 |
| 每查询应用协议字节均值 | 31,794.9 B | 29,324.7 B |
| 每查询数据线程 CPU 时长均值 | 9.664 ms | 7.698 ms |

第二轮同时改变两个参数，用于验证配置下发和查询效果，**不能将性能差异归因于某一个参数**，也不是选优实验。两轮都满足每查询总预算、无节点重访、无 head 候选扩展、指标完整审计。查询就绪队列等待只是系统等待的一部分，不能据此认定所有等待均低于100ms。

初始轮数据节点 CPU 核数均值分别约2.26/1.56/1.62，采样时各进程实际总线程33/34/33；6仅指查询工作线程。各节点正式窗口物理存储读增量均为0，属于热 OS 页缓存实验；逻辑读取每查询2,457,600 B，不代表这些字节都访问了SSD。性能数据不用于冷SSD、物理三机、不同发生器或不同并发下的容量结论。

## 热更新与回收证据

初始→更新→恢复的三个数据节点 PID 分别保持881808/881809/4211，索引fd均保持3；更新状态确认预算160、remote_weight 0.5，最后恢复预算200、remote_weight 1。见：

- [初始状态](two_host_diagnostic/initial_status.json)
- [更新状态](two_host_diagnostic/changed_status.json)
- [恢复状态](two_host_diagnostic/final_status.json)
- [汇总核验](two_host_diagnostic/verification.json)
- [初始轮指标](two_host_diagnostic/baseline.json)
- [更新轮指标](two_host_diagnostic/changed_benchmark.json)
- [独立 collect 记录](two_host_diagnostic/collection.json)

原始查询、每节点快照、每阶段 CSV 和进程负载均位于 [诊断 runs 目录](two_host_diagnostic/runs)。每轮均由 head 主动拉取节点快照，分块检验 SHA256 和长度后在 Intel-35 归档。构建时间 CSV 源于既有完整索引 manifest，不是这次重新构建的时间。

`run_diagnostic.py` 在 finally 中停止全部自建服务；[cleanup.json](two_host_diagnostic/cleanup.json) 记录三个节点均确认停止。未将临时 data-1 留在本机冒充正式 Intel-32。正式三机集群当前没有保持运行。

## 仍需完成

1. 恢复 Intel-32 SSH（或在 cluster.yaml 更新正确的 SSH 别名、数据地址与已有分片路径）。当前证据在 `deployment_logs/node1_mkdir.log` 和 `prepare_status.json`。
2. 执行操作手册的 prepare/start/status，确认三个不同主机、同一索引epoch和配置版本，ready=true。
3. 用正式 cluster.yaml 运行2000查询，热改参数再测，collect 后检查同PID/fd。此时才能报告物理三机性能。

管理平面与查询端点属于可信实验内网；令牌鉴权不提供TLS。配置更新是短暂停止接收的排空切换，不是零停顿更新。head 内置负载发生器包含Python/GIL调度成本，不应将本轮QPS与旧20客户端进程、每节点32线程的单机实验直接比较。
