# 当前默认：已验收的四机 RDMA 方案

2026-10-23 将 `network_parallel_20261023` 的完整异步建连方案固化为默认部署。Intel-35 为 HNSW 顶层，Intel-31/32/30 分别为数据节点 0/1/2。配置、运行入口和固定运行库均位于长期目录，不依赖 `test` 中的实验脚本或 `/tmp`、`/var/tmp` 副本。

## 默认参数

|项目|默认值|
|---|---|
|内部传输|rdma-core rsockets RDMA；公共查询 API / 管理仍为 TCP|
|搜索|每节点 64 工作线程；全局预算 200，top-k=10|
|顶层|路由 32、合并 8；按数据节点分配查询维护线程|
|普通发送 reactor|4–32|
|接收 reactor|8–32；data 每 reactor 1 handler，head 每 reactor 2 handlers|
|顶层入口发送 reactor|8–32|
|连接|每 reactor 每目标上限 128；预热 8；并行建连上限 2；head 入口连接上限 16|
|建连|async_connect=true，async_connect_progress=true，完整握手在有界建连池推进|
|扩缩容|queued 压力；发送阈值32、接收阈值16；10ms采样×4、100ms冷却、1500ms空闲|
|长连接迁移|开启，20ms周期，每批8连接；优先排空draining reactor|
|推荐压测|并发1024，预热2000，正式200000；默认客户端16 lane|
|活动/保留容量|4096 / 262144|

网络连接预热不产生搜索请求；远端图搜索仍由原扩展函数批准。每个本地搜索分支使用一个工作线程，父节点派发后继续本地工作。

## 配置入口

- `configs/dhdgs.yaml`：默认顶层完整运行参数，已配置 RDMA、现有 seq_2M 全量索引和实际节点地址。
- `configs/rdma_deployment.yaml`：部署路径、数据节点覆盖项、每机 CPU/NUMA、jemalloc、公共 API 和客户端参数。普通运行参数来自 `runtime_config: dhdgs.yaml`。
- `configs/rdma_validation_selection.json`：固定验证查询行号，不包含向量或认证内容。
- `configs/dhdgs_pilot.yaml`：原本机小规模功能验证配置，显式选择此文件才使用旧本地/TCP路径。

硬件放置保持已验收配置：Intel-31 搜索 CPU64–127、网络0–15、内存交错4–7；Intel-32 搜索0–63、网络64–79、内存交错0–1；Intel-30 搜索0–63、网络64–127（SMT）、内存交错0–3。head 路由/合并0–31、网络32–63，启动使用 localalloc；客户端64–111、内存交错2–3。硬件变更时应重新检查 CPU/NUMA，而不是直接复制编号。

## 使用方法

在 DHDGS 根目录使用现有 `.venv`：

```sh
# 核验持久化远端源码、固定运行库、RDMA能力与CPU集合
.venv/bin/python tools/rdma_default.py check

# 默认按 YAML 的200000请求、并发1024启动独立一轮，并在结束后离线分析
.venv/bin/python tools/rdma_default.py benchmark

# 小规模部署验证（生成新的独立ID，不覆盖既有结果）
.venv/bin/python tools/rdma_default.py benchmark --name rdma_smoke_new --count 2000 --warmup 2000

# 重新分析已完成捕获，不再执行查询
.venv/bin/python tools/rdma_default.py analyze --directory test/rdma_smoke_new
```

`runtime_source` 固定指向 `artifacts/rdma_default/release_20261023` 发布快照；后续工作区中的网络实验改动不会自动替换默认运行代码。快照包含本机原生模块和匹配的私有依赖，远端复用各机已验证模块。

运行入口在测量结束后排空并关闭本轮服务，之后才启动 Python 离线分析；实验记录进入 `TestLog.md`。原始捕获写到 `artifacts/rdma_default/runs`，紧凑结果与报告写到 `test/<run-id>`。本入口是可重复启动的批量验证流程，不是操作系统开机自启配置。已有旧常驻服务不会通过更改 YAML 被热切换。

内部协议必须在所有参与节点一致；不允许只将一台节点切换为 RDMA。不支持 RDMA 时明确报错，不自动改用 TCP。`network.transport: tcp` 表示外层服务/管理接口类别，实际原生内部协议由 `native_runtime.network_module.transport: rdma` 决定。

## 长期运行依赖

本机固定库：`artifacts/rdma_default/rdma_libs/librdmacm.so.1`。三个远端运行根目录分别为：

- Intel-31：`/mnt/nvme0/home/xulh/dhdgs_default_rdma`
- Intel-32：`/sharenvme/userhome/xulihan/dhdgs_default_rdma`
- Intel-30：`/sharenvme/xulihan_dhdgs/default_rdma`

每个目录保存本机专用 native 二进制、源码、私有 RDMA 库和权限0600的认证文件；没有把 Intel-35 编译的二进制跨 CPU 复制。固定 librdmacm 为先前验收的54版文件，SHA-256：`38ec1a77fc63e4d4f2c4db40096d64f9f131f275cffcb187e9ceda32368d1d18`。libibverbs/mlx5 沿用各机系统版本。启动器设置 LD_LIBRARY_PATH 和 jemalloc LD_PRELOAD，仅影响本轮进程。

`tools/rdma_default.py prepare` 可以重新同步部署文件。它在复用远端二进制前核对全部 native 源码哈希；native 代码改变时会明确拒绝复用旧二进制，若要升级默认发布版本，应先建立新的发布快照，再在各机使用 `/usr/bin/g++`、对应 Python 环境执行干净构建。根目录内持久保存了 `Reference_Code/RaBitQ-Library`，可供重新编译。Python环境与已有索引沿用配置中指向的持久路径。

二进制、固定库及认证文件不上传到 Git；源码、YAML与运行工具可纳入版本控制。当前任务没有执行 Git commit/push。

## 选型依据与本次验证

历史已验收 RDMA/C1024 两轮均为200000请求：QPS约17846/16208，p99约123.63/146.15ms，Recall@10约97.02%–97.03%。平均及固定近p99样本网络/控制残余占比均低于30%。C2048单轮QPS约18100，但p99约230.98ms；保留两轮已验证、尾延迟较低的1024为默认。这里的“最佳”指已验收推荐方案，不表示已证明全局最优。

完整历史证据见 `test/network_parallel_20261023/REPORT.md` 和 DESIGN.md §41；这些性能数字不是本次迁移重新测得。本次配置/依赖及2000查询验证见 `test/rdma_default_persistence_20261023/REPORT.md`，日志见 `TestLog.md`。

该配置的证据主要来自热页缓存与本机独立客户端的 TCP loopback API。持续输出吞吐、冷SSD以及跨物理机的客户端首跳另行验证。
