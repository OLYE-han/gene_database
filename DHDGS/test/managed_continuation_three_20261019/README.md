# 三机常驻：注册分通道与事件续执行版

Intel-35：HNSW主进程、独立报告接收子进程、独立data-0。Intel-32：data-1。Intel-30：data-2。完整配置见cluster.yaml；原2M索引复用。

```yaml
peer:
  head_receiver_processes: 1
  head_receiver_ipc: split
  head_query_workers: 2
  head_entry_workers: 4
  worker_threads: 32
```

head_query_workers处理就绪的路由/合并阶段，等待注册/数据结果时不占该工作线程；head_entry_workers是接收进程内部的初始派发线程，不是数据搜索线程。同步API调用线程仍可阻塞等待结果，会释放CPU/GIL。128在途包括所有等待中的查询。

端口：Intel-35 data-0=22051；Intel-32 data-1=22052；Intel-30 data-2=22053；Intel-35报告回调=22080；本机管理API=127.0.0.1:22091。保持私有集群内可达；token不得公开。

在Intel-35的工程上级目录运行：

```sh
gene_database/DHDGS/.venv/bin/python gene_database/DHDGS/tools/cluster_ctl.py --plan gene_database/DHDGS/test/managed_continuation_three_20261019/cluster.yaml status
gene_database/DHDGS/.venv/bin/python gene_database/DHDGS/tools/cluster_ctl.py --plan gene_database/DHDGS/test/managed_continuation_three_20261019/cluster.yaml benchmark --request gene_database/DHDGS/test/managed_continuation_three_20261019/benchmark_request.yaml
gene_database/DHDGS/.venv/bin/python gene_database/DHDGS/tools/cluster_ctl.py --plan gene_database/DHDGS/test/managed_continuation_three_20261019/cluster.yaml collect
```

benchmark_request.yaml为2000查询、并发128、预热32。测试结果集中写入本目录runs，collect将三节点指标快照拉回本机。

原有查询参数可以通过configure热更新。例如把以下内容存为本目录query_patch.yaml，再执行configure：

```yaml
search.max_expansions: 200
peer.worker_threads: 32
scheduler.remote_weight: 1.0
```

```sh
gene_database/DHDGS/.venv/bin/python gene_database/DHDGS/tools/cluster_ctl.py --plan gene_database/DHDGS/test/managed_continuation_three_20261019/cluster.yaml configure --request gene_database/DHDGS/test/managed_continuation_three_20261019/query_patch.yaml
```

head_receiver_processes/head_receiver_ipc/head_query_workers/head_entry_workers为进程布局参数，当前需重启head生效；不属于查询参数热更新白名单，不要求重建数据索引。每次注册传递当前查询预算/分支额度，热更新不会使接收进程使用启动时的旧额度。

prepare.py用于同步release，run_test.py仅用于首次新旧服务切换：新服务ready后停止此前managed_receivers_three版本，执行3轮复验。不要把首次切换脚本作为日常benchmark重复运行。audit_version.py核验三机源码哈希。

完整结果见REPORT.md，query_flow_timeline.png是固定查询734744的互斥时间分解；query_timeline.png保留各节点独立时间轴。注册等待优化成功不等于所有等待低于100ms，剩余就绪队列在报告中保留。
