# 16分发线程 / 2确认线程 / 16结果进程

实现与测试结论见[REPORT.md](REPORT.md)。本轮性能劣于原流水线，保留实验模式，未替换常驻入口。`candidate.yaml`只启动Intel-35候选顶层，使用既有三台数据服务，不需要重新分发数据或重建索引。

在`/sharenvme/usershome/xulihan`启动候选入口：

```sh
gene_database/DHDGS/.venv/bin/python gene_database/DHDGS/tools/cluster_service.py head --plan gene_database/DHDGS/test/direct_distribution_20261020/candidate.yaml
```

另一个终端按顺序执行；测试时确保其他入口没有并行压测：

```sh
gene_database/DHDGS/.venv/bin/python gene_database/DHDGS/tools/cluster_ctl.py --plan gene_database/DHDGS/test/direct_distribution_20261020/candidate.yaml recover
gene_database/DHDGS/.venv/bin/python gene_database/DHDGS/tools/cluster_ctl.py --plan gene_database/DHDGS/test/direct_distribution_20261020/candidate.yaml status
gene_database/DHDGS/.venv/bin/python gene_database/DHDGS/tools/cluster_ctl.py --plan gene_database/DHDGS/test/direct_distribution_20261020/candidate.yaml benchmark --request gene_database/DHDGS/test/direct_distribution_20261020/benchmark.yaml
gene_database/DHDGS/.venv/bin/python gene_database/DHDGS/tools/cluster_ctl.py --plan gene_database/DHDGS/test/direct_distribution_20261020/candidate.yaml collect
gene_database/DHDGS/.venv/bin/python gene_database/DHDGS/tools/cluster_ctl.py --plan gene_database/DHDGS/test/direct_distribution_20261020/candidate.yaml shutdown
```

管理地址`127.0.0.1:22791`；两个回调地址`192.168.33.35:22720/22721`。候选结果写入本目录`service/runs/`，结果工作进程写入`service/worker_results/results-<pid>.jsonl`。最终完整查询指标以`runs`内JSONL/CSV为准，工作进程JSONL是在最终写盘/返回IPC计时之前保存的快照。

搜索预算、top-k、ef、remote_weight和数据节点工作线程仍可用原`configure --request patch.yaml`在线修改；无需数据节点重部署。`head_direct`的线程/进程数、确认超时和结果保存路径是顶层启动配置，修改候选YAML后只需重启候选入口。为避免与旧回调连接混淆，对照测试每轮更换回调端口范围。

`experiment.yaml`和`run_scan.py`记录本轮交错实验；已有`cases/`不可覆盖。重测完整矩阵应建立新测试目录、调整回调端口并复制运行脚本。`analyze.py`可直接对现有记录重建统计和图表，不会触发查询。
