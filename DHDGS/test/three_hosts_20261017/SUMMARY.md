# 三台物理机器部署与测试：2026-10-17

部署、运行和退出审计已完成。三种 I/O 实现 × 客户端并发 1/4/8，共 9 组、2,304 个计时请求全部成功，逐查询指标完整。59 项回归测试和最终 2 项部署工具测试通过。三台机器均通过真实 io_uring 读取字节校验。

## 部署位置

| 数据节点 | SSH / 主机名 | 服务地址 | 实验根目录 |
|---|---|---|---|
| 0 | 本机 / sugon-intel-four-numa | 192.168.33.35:19431 | /sharenvme/usershome/xulihan/gene_database/DHDGS/test/three_hosts_20261017 |
| 1 | intel-32 / cxl-four-numa | 192.168.33.32:19432 | /sharenvme/userhome/xulihan/dhdgs_deploy/three_hosts_20261017 |
| 2 | intel-30 / intel-bigmem-nvm-stratix | 192.168.33.30:19433 | /sharenvme/xulihan_dhdgs/three_hosts_20261017 |

两个远端的代码均在其根目录下的 `gene_database/DHDGS/`，虚拟环境在代码目录的 `.venv/`；索引、配置、日志分别在根目录的 `index/`、`configs/`、`logs/`。入口和压测客户端与数据节点 0 共机，第四台独立入口机尚未参与。

本轮服务已全部正常退出；安装文件保留。重启和手动查询命令见 [部署指南](../../DEPLOYMENT.md)。不要向已完成的自动实验目录重复写入结果；新的自动实验应使用新的测试目录和远端根目录。

## 实验范围

- 复用 4,096 × 2,560 pilot 索引，原始 L2、1-bit RaBitQ；没有重新构建全量 2M 索引。
- 128 个不同查询（行 512–639），各重复两次，每组 256 请求；预热和冒烟查询不计入以上数量。
- top-k=10、全局扩展预算 64、frontier=32、local beam=4；每数据节点 1 个计算工作线程。
- 线程池模式每节点 2 个 I/O 线程；异步模式每节点最多 8 个在途请求、96 KiB 在途字节额度、4 MiB 缓冲预留。
- 应用缓存关闭，操作系统页缓存保留，详细追踪开启。所有计时窗口进程物理存储读取字节数均为 0，因此这是热页缓存测试。

## 实测结果

| I/O 实现 | 客户端并发 | QPS | 平均延迟 ms | p99 ms | 平均 Recall@10 |
|---|---:|---:|---:|---:|---:|
| blocking | 1 | 47.66 | 20.91 | 34.61 | 0.996875 |
| blocking | 4 | 102.53 | 38.69 | 75.71 | 0.996484 |
| blocking | 8 | 114.58 | 68.61 | 139.41 | 0.996484 |
| thread_pool | 1 | 33.25 | 30.00 | 45.84 | 0.996094 |
| thread_pool | 4 | 59.24 | 67.21 | 113.43 | 0.996094 |
| thread_pool | 8 | 63.08 | 126.22 | 228.63 | 0.996094 |
| io_uring | 1 | 39.38 | 25.32 | 34.55 | 0.996875 |
| io_uring | 4 | 69.08 | 57.62 | 120.59 | 0.996094 |
| io_uring | 8 | 76.87 | 103.46 | 188.89 | 0.996094 |

按 p99 优先的偏好，并发 1 的尾延迟最低；io_uring 与 blocking 的 p99 只差约 0.06 ms，单次小样本不足以证明改善。blocking 在并发 4/8 的 p99 和 QPS 均更好，保留同步默认。这些结果不能用于推断真实 SSD 读负载下的性能，也不构成全量数据 Recall≥0.99 的保证。后续需要扩大索引、实际触发存储读取、重复试验并使用独立查询集验证。

## 正确性与资源审计

[独立审计](verification.json) 核对了主 YAML、部署源码和索引哈希、三个真实主机名、全连接数据通路及入口回调通路，以及每次服务退出的 PID 和退出码。2,304 个计时请求均无节点回访、无入口 expand RPC、无未经扩展决定函数批准的远端派发，分支预算守恒且未超过全局扩展预算。I/O 调度仅作用于本节点。

每个 `blocking|thread_pool|io_uring/c1|c4|c8/run/` 保存逐查询报告、JSONL/CSV、阶段均值、服务器负载和配置来源；完整原始记录保留在实验机器上。`network_node_*.json` 中的 tcp_connect_ms 是 TCP 建连耗时，并非独立测量的链路 RTT。报告中的索引构建时间来自复用索引的原始构建记录，不代表本次进行了分布式索引构建。

远端 Python 3.10 / NumPy 2.2.6 与本机 Python 3.12 / NumPy 2.5.4 不同，机器硬件也不同；本轮是实际部署验证，不能据此严格分离硬件和软件版本的影响。

## 部署中修复的问题

- intel-30 无法访问包仓库：由本机下载指定 ABI 的 wheel，远端离线安装。
- 默认 GCC 14 与远端系统 libstdc++ 不匹配：显式使用 `/usr/bin/g++` 和独立构建目录。
- intel-32 链接与运行时加载的 liburing 版本不同：将 CMake 选定的库按 SONAME 放入模块私有目录并设置运行时搜索路径，不修改系统库。
- intel-30 旧 liburing 头文件缺少 data64 辅助函数：使用相同 ABI 的 user_data 字段。
- 远端 SSH 登录较慢：服务监督连接复用于连通性检查，避免反复登录。

前三次准备失败日志保留为 `execution_attempt*.log`，ABI 诊断保留为 `uring_abi_diagnostic.log`；这些是成功测试之前的环境准备问题。成功实验记录没有被覆盖。

## 复现与记录入口

- [唯一主配置](deployment.yaml)
- [部署脚本](../../tools/deploy_cluster.py) / [节点辅助脚本](../../tools/deploy_node.py)
- [部署和启动命令](../../DEPLOYMENT.md)
- [独立审计脚本](analyze.py) / [审计结果](verification.json)
- [完成状态](completed.json) / [结果比较](comparison.json)
- [完整回归日志](regression.log) / [部署工具测试](deployment_tests_final.log)

在 DHDGS 目录执行 `.venv/bin/python test/three_hosts_20261017/analyze.py` 可重新审计原始记录；该审计要求部署源码、主配置和索引仍与本轮准备时一致。
