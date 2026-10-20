# FIFO入口优化版：Intel-35 / Intel-32 / Intel-30

| 用户节点 | 内部ID | 主机 | 数据端口 | 查询线程 |
|---|---:|---|---:|---:|
| 数据节点1 | 0 | Intel-35 | 21651 | 32 |
| 数据节点2 | 1 | Intel-32 | 21652 | 32 |
| 数据节点3 | 2 | Intel-30 | 21653 | 32 |

Intel-35另有独立HNSW head进程，管理接口127.0.0.1:21681，数据节点回调192.168.33.35:21680。客户端并发128、搜索预算200、top-k10、remote_weight=1；全套参数在[cluster.yaml](cluster.yaml)，单轮负载在[benchmark.yaml](benchmark.yaml)，重复实验参数在[experiment.yaml](experiment.yaml)。

在Intel-35运行：

```sh
cd /sharenvme/usershome/xulihan/gene_database/DHDGS
PLAN=test/managed_fifo_three_20261019/cluster.yaml
.venv/bin/python tools/cluster_ctl.py --plan "$PLAN" status
.venv/bin/python tools/cluster_ctl.py --plan "$PLAN" benchmark \
  --request test/managed_fifo_three_20261019/benchmark.yaml
.venv/bin/python tools/cluster_ctl.py --plan "$PLAN" collect
```

结果由head拉取并汇总到Intel-35本目录`runs/`。每轮包括逐查询JSONL/CSV、阶段均值、节点前后快照、CPU/内存/物理读写、协议RPC/字节、历史构建阶段及有效配置。查询参数更新仍使用原有`configure --request <patch.yaml>`接口，不需要重新部署索引；例如补丁包含`peer.worker_threads: 32`或`search.max_expansions: 200`。修改并发只需修改benchmark请求。

服务停止与重新启动：

```sh
.venv/bin/python tools/manage_cluster.py --plan "$PLAN" stop
.venv/bin/python tools/manage_cluster.py --plan "$PLAN" start
```

三个分片均复用既有完整索引。Intel-32的shard_1在`/sharenvme/userhome/xulihan/dhdgs_deploy/seq2m_full_20261017/index/shard_1`，沿用上一轮逐文件SHA256核验记录，本轮没有改动索引。远端代码、启动脚本、PID和日志分别保存在：

- Intel-32：`/sharenvme/userhome/xulihan/dhdgs_deploy/managed_fifo_three_20261019/`
- Intel-30：`/sharenvme/xulihan_dhdgs/managed_fifo_three_20261019/`
- Intel-35：本目录，`logs/head.log`和`logs/data.log`。

`prepare.py`为三个节点发布相同源码，使用持久SSH会话及较长保活容忍。代码清单见`deployment_node*.json`。`run_test.py`负责从上轮三机集群切换并测试，依赖旧服务仍可管理；切换后日常复测使用上面的benchmark命令。

入口策略同条件对照见[实验报告](../entry_fifo_20261019/REPORT.md)，本次发布后复测见[REPORT.md](REPORT.md)。单查询时延图固定使用第一轮query 734744，各数据节点使用独立相对时钟。

本版源码摘要`44b6d3aa4029a18f8a40`。`peer.connection_pool.head_scheduling: fifo`启用按目标FIFO预约和定向唤醒；`broadcast`保留旧入口行为。只影响顶层池，数据间派发和原生回传池不变，每目标连接容量仍为2。策略暂不支持configure热切换，需要重启head；预算、查询线程等原有热参数仍可按原接口调整。
