# 三机持久部署与实验入口

intel-30 当前版本的代码目录为 `/sharenvme/xulihan/gene_database/DHDGS`（2026-10-19 已迁移）。现有 `run_data.sh` 和本机 `deployment_node2.json` 已指向新代码目录，索引和 Python 环境仍复用原位置。旧 release 代码路径通过软链接兼容。再次 `prepare` 新版本仍按原部署规则创建 release；本次迁移的是当前版本，详见 [迁移记录](../relocation_intel30_20261019/README.md)。

物理拓扑：Intel-35 上独立的 head 和 data-0 进程；Intel-32 上 data-1；Intel-30 上 data-2。每个数据节点 6 个查询工作线程。辅助接收、派发、控制线程另计。复用完整 seq_2M 的既有分片，搜索预算 200，top-k 10。所有以下命令在 **Intel-35** 执行，起始目录为 `/sharenvme/usershome/xulihan/gene_database/DHDGS`。

## 部署与启动

```sh
cd /sharenvme/usershome/xulihan/gene_database/DHDGS
.venv/bin/python tools/manage_cluster.py --plan test/managed_cluster_20261019/cluster.yaml prepare
.venv/bin/python tools/manage_cluster.py --plan test/managed_cluster_20261019/cluster.yaml start
.venv/bin/python tools/cluster_ctl.py --plan test/managed_cluster_20261019/cluster.yaml status
```

`prepare` 仅上传新版本代码和管理配置，使用原有 Python 依赖，在各远端本地编译 C++；不上传向量、不重建图、不覆盖旧部署。源码包和 SHA256 清单在 `payloads/`，编译日志在 `deployment_logs/`。任一节点失败时返回非零，可修复后只重试该节点：

```sh
.venv/bin/python tools/manage_cluster.py --plan test/managed_cluster_20261019/cluster.yaml prepare --nodes 1
```

`start` 先启动三个数据服务，再启动 head，最后通过 head 同步配置并恢复查询。进程脱离终端运行；各主机 `root/logs/data.log` 保存日志，Intel-35 同目录另有 `head.log`。失败时**不要运行测试**，先 `status` 检查三个节点及 `ready`。

若希望分别启动：

```sh
.venv/bin/python tools/manage_cluster.py --plan test/managed_cluster_20261019/cluster.yaml start-data --nodes 1
.venv/bin/python tools/manage_cluster.py --plan test/managed_cluster_20261019/cluster.yaml start-data --nodes 2
.venv/bin/python tools/manage_cluster.py --plan test/managed_cluster_20261019/cluster.yaml start-data --nodes 0
.venv/bin/python tools/manage_cluster.py --plan test/managed_cluster_20261019/cluster.yaml start-head
```

`--nodes 1` 通过 SSH intel-32 启动，`--nodes 2` 通过 SSH intel-30 启动。`prepare` 还会在远端生成固定路径的前台启动脚本；也可分别使用两个终端执行：

```sh
ssh -t intel-32 /sharenvme/userhome/xulihan/dhdgs_deploy/managed_cluster_20261019/run_data.sh
ssh -t intel-30 /sharenvme/xulihan_dhdgs/managed_cluster_20261019/run_data.sh
```

然后在 Intel-35 启动本地数据节点和 head：

```sh
.venv/bin/python tools/manage_cluster.py --plan test/managed_cluster_20261019/cluster.yaml start-data --nodes 0
.venv/bin/python tools/manage_cluster.py --plan test/managed_cluster_20261019/cluster.yaml start-head
```

不要同时用前台方式和 `start-data` 启动同一节点。数据监听为 192.168.33.35:20851、192.168.33.32:20852、192.168.33.30:20853；节点结果/keep-alive 回传 Intel-35:20880。管理 HTTP 接口仅监听 Intel-35 本地 127.0.0.1:20881。三台服务器需互通数据端口，远端需能访问 Intel-35 的回调端口。管理令牌保存在权限 0600 的 `cluster.token`，不进入 Git。

## 不重部署修改查询参数

编辑 `hot_patch.yaml`，采用有完整层级的键：

```yaml
search.max_expansions: 160
scheduler.remote_weight: 0.5
peer.worker_threads: 6
```

```sh
.venv/bin/python tools/cluster_ctl.py --plan test/managed_cluster_20261019/cluster.yaml configure --request test/managed_cluster_20261019/hot_patch.yaml
.venv/bin/python tools/cluster_ctl.py --plan test/managed_cluster_20261019/cluster.yaml status
```

命令只发往 head；head 负责排空、下发、提交、确认三个节点配置版本一致。保留数据进程和索引，内部查询运行时/线程池可重建。修改 `routing.ef_search` 会在 head 内重新加载 HNSW 对象，不重载数据节点索引。切换期间暂停接收新查询，旧查询按旧配置完成。已成功提交的配置保存在 head/node state 文件中，重启后保留；直接修改原始 `cluster.yaml` 不覆盖已持久化的热参数，应使用 `configure`。

支持：`search` 中 top_k、exclude_self、frontier_size、local_beam、max_expansions、max_inflight、fallback_enabled、max_fallback_entries、stagnation_batches；`routing.ef_search`、`routing.entry_candidates`；`scheduler` 的全部当前字段；`peer.worker_threads`、`peer.child_budget_fraction`、`peer.max_branches`。完整白名单在 `python/dhdgs/managed_node.py:HOT_FIELDS`。

向量维度、距离类型、量化位数、图构建参数、路径/端口、I/O 后端、控制器类型和辅助线程池配置需要停服后重新启动；修改索引定义还需要重建索引。拒绝不在白名单中的热更新，避免假装生效。查询所用版本随结果保存。

配置异常恢复：

```sh
.venv/bin/python tools/cluster_ctl.py --plan test/managed_cluster_20261019/cluster.yaml recover
```

恢复到 head 最后持久化选择的配置；恢复前不接收新查询。数据节点重启默认暂停，必须由 head 恢复。

## 查询、测试、归档

```sh
.venv/bin/python tools/cluster_ctl.py --plan test/managed_cluster_20261019/cluster.yaml query --request test/managed_cluster_20261019/query.yaml
.venv/bin/python tools/cluster_ctl.py --plan test/managed_cluster_20261019/cluster.yaml benchmark --request test/managed_cluster_20261019/benchmark.yaml
.venv/bin/python tools/cluster_ctl.py --plan test/managed_cluster_20261019/cluster.yaml collect
```

`benchmark.yaml` 控制本轮查询数/客户端并发/预热数；当前 2000/32/32。`benchmark_c64.yaml` 为 2000/64/32。客户端负载由 head 进程内工作线程生成；因此同时包含真实 HNSW、head Python 调度和数据检索的耗时，不等同于独立客户端的远程 API 饱和压测。

每轮自动拉取三个节点测试前后指标，校验分块长度和 SHA256，存入 Intel-35：

```
test/managed_cluster_20261019/runs/<时间>_benchmark_<唯一ID>/
  configuration.json           # 完整有效配置和版本
  benchmark_options.json       # 本轮负载
  summary.json                 # QPS、p50/p95/p99、召回、节点负载、正确性审计
  queries.jsonl.gz             # 每查询完整结果、分支、时间戳、预算和路径
  queries.csv                  # 每查询延迟、召回、网络/读盘/计算计数
  query_stage_means.csv        # 阶段汇总
  per_query_stage_means.csv    # 每查询阶段均值
  nodes_before.json.gz         # 节点基线
  nodes_after.json.gz          # 节点最终累计计数、运行时和资源样本
  nodes.csv                   # 每节点查询工作量
  process_load.csv             # 每节点 CPU/物理磁盘/RSS/线程
  build_stages.csv             # 复用索引的原始构建阶段，不是本轮部署时间
```

`collect` 独立拉取当前节点快照到独立 collect 目录，不改变搜索参数。CPU/磁盘按同一节点前后差值；不同机器 monotonic 时间戳不能直接相减。CPU 核数均值以节点快照窗口计算，包含采集边界开销。`file_read_bytes` 为逻辑读取；`storage_read_bytes` 为 Linux 实际存储读取，页缓存命中时可能为零。网络字节为应用协议字节，不含 TCP/IP 包头/重传。管理快照流量在查询计时之外，不计入查询网络指标。

本轮采用完整 2M 语料对应的已核验 L2 ground truth，并固定使用既有 2000 条 validation 查询。失败请求保留且以召回 0 进入总体召回率；同时报告成功数和失败数，不能只看成功请求延迟。

恢复预算 200、remote_weight 1：

```sh
.venv/bin/python tools/cluster_ctl.py --plan test/managed_cluster_20261019/cluster.yaml configure --request test/managed_cluster_20261019/restore.yaml
```

停止当前集群（先排空、停数据节点，再停 head）：

```sh
.venv/bin/python tools/manage_cluster.py --plan test/managed_cluster_20261019/cluster.yaml stop
```

本实现是可信实验内网的管理接口；令牌鉴权并不提供传输加密。数据协议沿用现有可信内网假设。仅通过当前 head 提交受管理的查询，不混用其他旧版本客户端/数据服务。
