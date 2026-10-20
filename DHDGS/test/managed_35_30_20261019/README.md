# 当前部署：Intel-35 + Intel-30，三个数据节点

| 用户编号 | 程序 ID | 主机 | 数据端口 | 查询工作线程 |
|---|---:|---|---:|---:|
| 数据节点1 | 0 | Intel-35 | 21051 | 32 |
| 数据节点2 | 1 | Intel-30 | 21052 | 32 |
| 数据节点3 | 2 | Intel-30 | 21053 | 32 |

Intel-35 另运行一个独立 HNSW head 进程：管理接口127.0.0.1:21081，节点回调192.168.33.35:21080。Intel-30 有两个独立数据进程，合计64个查询工作线程；辅助接收、发送、控制等线程另计。head 的测试客户端并发128，默认2000条查询、预热32、搜索预算200、top-k10。

本目录是正式的新拓扑，旧 `managed_cluster_20261019` 目录保留此前三机配置和诊断记录。最新测试与状态见 [REPORT.md](REPORT.md)。

## 在 Intel-35 操作

```sh
cd /sharenvme/usershome/xulihan/gene_database/DHDGS
PLAN=test/managed_35_30_20261019/cluster.yaml

# 首次或代码升级：部署程序。已有分片保持原位。
.venv/bin/python tools/manage_cluster.py --plan "$PLAN" prepare

# 启动3个数据进程与独立head；同步配置后恢复查询。
.venv/bin/python tools/manage_cluster.py --plan "$PLAN" start
.venv/bin/python tools/cluster_ctl.py --plan "$PLAN" status

# 2000条查询、并发128
.venv/bin/python tools/cluster_ctl.py --plan "$PLAN" benchmark \
  --request test/managed_35_30_20261019/benchmark.yaml

# 顶层主动拉取各节点指标
.venv/bin/python tools/cluster_ctl.py --plan "$PLAN" collect
```

确认 `status.ready=true` 且节点worker_threads均为32后再测。已运行的服务可直接benchmark，无需prepare/start。服务脱离终端运行，重连SSH不会停止。结果统一存放 Intel-35 `test/managed_35_30_20261019/runs/`。

如需单独启动 Intel-30 上两个数据节点，可从 Intel-35 执行：

```sh
.venv/bin/python tools/manage_cluster.py --plan "$PLAN" start-data --nodes 1 2
```

或分别在两个前台终端运行（不要与后台启动重复）：

```sh
ssh -t intel-30 /sharenvme/xulihan_dhdgs/managed_35_30_20261019/node1/run_data.sh
ssh -t intel-30 /sharenvme/xulihan_dhdgs/managed_35_30_20261019/node2/run_data.sh
```

Intel-35 的data-0和head可分别启动：

```sh
.venv/bin/python tools/manage_cluster.py --plan "$PLAN" start-data --nodes 0
.venv/bin/python tools/manage_cluster.py --plan "$PLAN" start-head
```

## 通过顶层修改配置

编辑 `hot_patch.yaml`，例如：

```yaml
search.max_expansions: 160
scheduler.remote_weight: 0.5
peer.worker_threads: 32
```

```sh
.venv/bin/python tools/cluster_ctl.py --plan "$PLAN" configure \
  --request test/managed_35_30_20261019/hot_patch.yaml
```

修改客户端并发时编辑 `benchmark.yaml` 的 `concurrency`，无须配置切换。本轮已固定为128。

修改节点参数时head排空在途查询，再向三个数据进程提交同一配置版本；进程PID和数据索引fd保留。已保存的热配置优先于初始YAML，修改初始YAML不会覆盖正在运行/已持久化的热参数。恢复预算200、remote_weight 1、每节点32线程：

```sh
.venv/bin/python tools/cluster_ctl.py --plan "$PLAN" configure \
  --request test/managed_35_30_20261019/restore.yaml
```

更多可热改字段及事务恢复说明见 [管理协议手册](../managed_cluster_20261019/README.md#不重部署修改查询参数)。其中旧拓扑路径应替换成本目录的新PLAN。

## 数据与日志

- Intel-35 data-0：复用 `test/seq2m_full_20261017/build/index/shard_0`。
- Intel-30 data-1：`/sharenvme/xulihan_dhdgs/managed_35_30_20261019/index/shard_1`。从Intel-35已有完整分片复制，逐文件SHA256核验；证据 `shard_1_manifest.json` 和 `shard_verification.json`。
- Intel-30 data-2：复用 `/sharenvme/xulihan_dhdgs/seq2m_full_20261017/index/shard_2`。
- Intel-30 两个进程的配置、令牌、状态、PID和日志分别在 `managed_35_30_20261019/node1/` 与 `node2/`；共享已经编译的只读代码release目录。
- Intel-35 head/data-0日志在本目录 `logs/head.log`、`logs/data.log`。

单轮结果包含 summary、有效配置版本、完整查询/分支JSONL、每查询CSV、阶段均值CSV、节点前后快照、CPU/磁盘/内存负载。独立collect另建目录。两节点共用Intel-30的CPU、内存带宽和SSD，因此属于两物理机/三逻辑节点实验。现有 `peer_branch_rpc_count` 等按逻辑数据节点统计，包含Intel-30内部两个进程之间的通信；不能等同于跨物理机网卡流量。各机器的单调时间戳不能直接相减。

停止：

```sh
.venv/bin/python tools/manage_cluster.py --plan "$PLAN" stop
```
