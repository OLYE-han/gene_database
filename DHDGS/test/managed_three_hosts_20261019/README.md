# Intel-35 / Intel-32 / Intel-30 三物理机测试与部署

本目录现为旧版broadcast入口的历史记录，服务已停止。当前运行的是 [FIFO入口优化版](../managed_fifo_three_20261019/README.md)，复测请使用新版配置和端口。

| 用户节点 | 内部ID | 主机 | 数据端口 | 查询线程 |
|---|---:|---|---:|---:|
| 数据节点1 | 0 | Intel-35 | 21351 | 32 |
| 数据节点2 | 1 | Intel-32 | 21352 | 32 |
| 数据节点3 | 2 | Intel-30 | 21353 | 32 |

Intel-35另有独立HNSW head进程，管理接口127.0.0.1:21381，数据节点回调192.168.33.35:21380。客户端并发128、搜索预算200、top-k10、remote_weight=1；全套参数在[cluster.yaml](cluster.yaml)，单轮负载在[benchmark.yaml](benchmark.yaml)，重复实验参数在[experiment.yaml](experiment.yaml)。

在Intel-35运行：

```sh
cd /sharenvme/usershome/xulihan/gene_database/DHDGS
PLAN=test/managed_three_hosts_20261019/cluster.yaml
.venv/bin/python tools/cluster_ctl.py --plan "$PLAN" status
.venv/bin/python tools/cluster_ctl.py --plan "$PLAN" benchmark \
  --request test/managed_three_hosts_20261019/benchmark.yaml
.venv/bin/python tools/cluster_ctl.py --plan "$PLAN" collect
```

结果由head拉取并汇总到Intel-35本目录`runs/`。每轮包括逐查询JSONL/CSV、阶段均值、节点前后快照、CPU/内存/物理读写、协议RPC/字节、历史构建阶段及有效配置。查询参数更新仍使用原有`configure --request <patch.yaml>`接口，不需要重新部署索引；例如补丁包含`peer.worker_threads: 32`或`search.max_expansions: 200`。修改并发只需修改benchmark请求。

服务停止与重新启动：

```sh
.venv/bin/python tools/manage_cluster.py --plan "$PLAN" stop
.venv/bin/python tools/manage_cluster.py --plan "$PLAN" start
```

三个分片均复用既有完整索引。Intel-32的shard_1在`/sharenvme/userhome/xulihan/dhdgs_deploy/seq2m_full_20261017/index/shard_1`，本轮逐文件SHA256与Intel-35原分片核验。远端代码、启动脚本、PID和日志分别保存在：

- Intel-32：`/sharenvme/userhome/xulihan/dhdgs_deploy/managed_three_hosts_20261019/`
- Intel-30：`/sharenvme/xulihan_dhdgs/managed_three_hosts_20261019/`
- Intel-35：本目录，`logs/head.log`和`logs/data.log`。

首次部署时Intel-32出现SSH会话建立缓慢，`prepare.py`对该机使用持久SSH会话及较长保活容忍。它只负责Intel-32的准备重试；三个节点的代码清单见`deployment_node*.json`。`run_test.py`包含核验、首次切换和三轮测试，依赖原两机服务仍可管理，不能用它代替已完成切换后的日常benchmark命令。

实验方法见[PLAN.md](PLAN.md)，结果见[REPORT.md](REPORT.md)。单查询时延图固定使用第一轮query 734744，各数据节点使用独立相对时钟。
