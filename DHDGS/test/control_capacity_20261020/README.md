# 控制链容量优化的运行与复验

本目录保存配置、冻结源码、逐查询原始指标、图表、失败实验及报告。选型以QPS优先，平均Recall@10至少约95%，不接受查询失败。结果和限制见 [REPORT.md](REPORT.md)。

**当前状态：实现和候选已验证，尚未替换原常驻入口。** 短测双向扫描选中并发256，QPS中位数1102.84；100000次连续请求虽全部成功，但全程527.28 QPS、p99 6238.23ms，暴露持续性能下降。不能按短测值对外承诺容量。本轮全部临时服务已停止，复现前先启动下面的容量数据节点。

## 入口与数据节点

[candidate.yaml](candidate.yaml) 使用4个查询所属顶层进程，每进程1个路由线程、4个派发线程；每台数据服务器32个计算线程，全局预算200。入口HTTP为 `127.0.0.1:28191`，回调使用Intel-35上的28110–28113；数据节点使用Intel-35/32/30的28251/28252/28253。复用既有完整2M索引，不再分发或构建向量。

快速复验并发为256，来自 [selection_policy.yaml](selection_policy.yaml) 的QPS优先规则及 [selection.json](selection.json)。这是10000请求/轮扫描的胜出值，尚未证明为持续负载最优并发。

数据节点的容量配置见 [capacity_settings.yaml](capacity_settings.yaml)：去重记录上限提高到131072，保留期仍为60秒。在 `capacity_data/cluster.yaml` 中生成对应完整配置。修改该上限需重启对应数据进程，只改head无效。原常驻端口22091/22051–22053保留；同一轮性能测试只使用一套服务，避免竞争资源。此前 direct/independent 两套临时节点已经关闭。

在 `/sharenvme/usershome/xulihan` 运行：

```sh
# 数据节点未运行时启动，SSH路径来自原部署元数据；不会复制索引。
gene_database/DHDGS/.venv/bin/python gene_database/DHDGS/test/control_capacity_20261020/isolated_data.py start --variant capacity

# 在独立终端启动候选入口；进程数是启动参数。
gene_database/DHDGS/.venv/bin/python gene_database/DHDGS/tools/cluster_service.py head --plan gene_database/DHDGS/test/control_capacity_20261020/candidate.yaml
```

另一个终端执行以下命令。`recover` 在集群无在途任务时校验/恢复数据节点配置，不重建索引；不要在其他入口正在压测时执行。

```sh
gene_database/DHDGS/.venv/bin/python gene_database/DHDGS/tools/cluster_ctl.py --plan gene_database/DHDGS/test/control_capacity_20261020/candidate.yaml recover
gene_database/DHDGS/.venv/bin/python gene_database/DHDGS/tools/cluster_ctl.py --plan gene_database/DHDGS/test/control_capacity_20261020/candidate.yaml status
gene_database/DHDGS/.venv/bin/python gene_database/DHDGS/tools/cluster_ctl.py --plan gene_database/DHDGS/test/control_capacity_20261020/candidate.yaml benchmark --request gene_database/DHDGS/test/control_capacity_20261020/benchmark_request.yaml
gene_database/DHDGS/.venv/bin/python gene_database/DHDGS/tools/cluster_ctl.py --plan gene_database/DHDGS/test/control_capacity_20261020/candidate.yaml collect
```

`benchmark_request.yaml` 是推荐并发下的2000查询快速复验。更长的并发扫描由 `throughput_capacity.yaml` 驱动：每轮10000次请求，是同一2000验证查询的五次重复，每次使用新的请求身份，不能当成10000条独立向量查询。停机时先对候选入口执行 `cluster_ctl.py ... shutdown`，再运行 `isolated_data.py stop --variant capacity`；脚本拒绝关闭非空闲数据节点。

## 复现实验

```sh
gene_database/DHDGS/.venv/bin/python gene_database/DHDGS/test/control_capacity_20261020/run.py --plan gene_database/DHDGS/test/control_capacity_20261020/throughput_capacity.yaml
gene_database/DHDGS/.venv/bin/python gene_database/DHDGS/test/control_capacity_20261020/analyze.py final --additional final_remaining --selected lanes
gene_database/DHDGS/.venv/bin/python gene_database/DHDGS/test/control_capacity_20261020/analyze.py throughput_capacity --selected c256
gene_database/DHDGS/.venv/bin/python gene_database/DHDGS/test/control_capacity_20261020/analyze.py sustained --selected c256
gene_database/DHDGS/.venv/bin/python gene_database/DHDGS/test/control_capacity_20261020/windowed.py
```

runner拒绝覆盖既有实验目录。复跑前复制YAML，修改 `directory` 为新目录，并分配未占用的 `port` 回调端口范围；不修改已留档结果。每个case都保留完整plan、spec、载入模块路径、终态状态、压测报告与gzip逐查询日志。当前冻结源码为 `verified_source/`，由 `hashes.json` 和worker的 `source_file` 共同核验。

不要把旧目录 `throughput/01_c256` 的失败结果当作并发256的成功吞吐。它触发的是旧去重保留容量上限；容量实验同时提高管理帧上限以容纳详细节点快照，未缩短去重期、降低预算或关闭指标。

`http_check.py` 使用独立端口执行32条真实HTTP查询和32条管理压测查询，并验证4个所属进程和正常关闭。`regression_final.log` 保留388项通过的回归记录。工作进程日志可用 `run.py --archive` 无损压缩；只在没有性能实验运行时执行。

100000请求连续试验由 `sustained.yaml` 驱动；每轮原始工作进程日志约2.6GiB，另需压测明细和压缩临时空间。检查可用空间后再执行。`stage_means.csv` 是完整互斥阶段的均值，`windows.csv` 按完成时间分桶；节点详细时间戳受60秒保留窗口影响，完整查询的顶层边界和聚合指标仍保留。
