# 独立报告接收进程版：三机部署与测试

完整配置是本目录 cluster.yaml。Intel-35运行HNSW主进程、一个独立报告接收子进程和data-0；Intel-32运行data-1；Intel-30运行data-2。每数据节点32查询线程，并发128，预算200。复用完整seq_2M索引，不重建。

- 数据端口：Intel-35:21851、Intel-32:21852、Intel-30:21853。
- 报告回调：Intel-35:21880，HNSW入口查询仍走管理API/benchmark。
- 管理API：Intel-35本机127.0.0.1:21891。
- `dhdgs.peer.head_receiver_processes: 1` 表示额外增加一个接收进程。设为P则需要开放head_port至head_port+P-1；不能与管理或数据端口重叠。改变该数需重启head，不需要重新部署数据节点。

在Intel-35工程上级目录运行以下命令；配置中token用于集群认证，不要输出或公开：

```sh
gene_database/DHDGS/.venv/bin/python gene_database/DHDGS/tools/cluster_ctl.py --plan gene_database/DHDGS/test/managed_receivers_three_20261019/cluster.yaml status
gene_database/DHDGS/.venv/bin/python gene_database/DHDGS/tools/cluster_ctl.py --plan gene_database/DHDGS/test/managed_receivers_three_20261019/cluster.yaml benchmark --request gene_database/DHDGS/test/managed_receivers_three_20261019/benchmark_request.yaml
gene_database/DHDGS/.venv/bin/python gene_database/DHDGS/tools/cluster_ctl.py --plan gene_database/DHDGS/test/managed_receivers_three_20261019/cluster.yaml collect
```

查询参数继续通过原configure接口热更新，不需要重启数据节点。按工具 `--help` 的configure参数格式提交search.max_expansions、scheduler.remote_weight等已有热参数。接收进程在每次注册时使用该查询当前预算/分支额度，不会沿用启动时的旧预算。

部署记录：prepare.py同步新release；run_test.py是仅用于首次切换的脚本，在新集群ready后停止此前managed_fifo_three集群，再执行3轮测试。不要再次运行它来日常压测，使用上面的benchmark接口。audit_version.py核验各节点源码哈希。结果均在本目录runs中；collect将三节点快照集中保存到Intel-35。

初次prepare曾因新目录缺少token而中止，未影响旧服务。补齐独立0600权限token后重试，部署故障日志保留，不混入查询性能结果。
