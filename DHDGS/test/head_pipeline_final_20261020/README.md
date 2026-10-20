# 单进程顶层流水线测试与使用

本目录是最终交错复验：old为原双进程顶层；q1为单进程、1路由线程、1合并线程、排队发送ACK；i1为同样配置但立即尝试非阻塞发送ACK。各3轮，同一三数据节点、32线程/节点、并发128、预算200、2000查询。每轮独立回调端口，避免重复使用端口时数据节点缓存的旧连接影响实验。

完整结论见REPORT.md；首轮路由1/2/4扫描保存在../head_pipeline_20261019，第二轮ACK尝试在../head_pipeline_ack_20261020。前两轮有少量旧连接重试，不能隐去；最终复验另检查原生控制传输失败计数。新增和原有每query指标保存在各cases/*/runs下。

`candidate.yaml` 是按单进程组p99选择的可运行配置，使用当前三个数据服务的索引和端口，不需要重建或重发数据。它不会自动替换现有常驻顶层。以下命令从Intel-35的 `/sharenvme/usershome/xulihan` 执行；对照期间保持其他顶层空闲，避免对同一数据服务重复施压或同时修改配置。

```sh
gene_database/DHDGS/.venv/bin/python gene_database/DHDGS/tools/cluster_service.py head --plan gene_database/DHDGS/test/head_pipeline_final_20261020/candidate.yaml
```

启动后在另一个终端执行：

```sh
gene_database/DHDGS/.venv/bin/python gene_database/DHDGS/tools/cluster_ctl.py --plan gene_database/DHDGS/test/head_pipeline_final_20261020/candidate.yaml recover
gene_database/DHDGS/.venv/bin/python gene_database/DHDGS/tools/cluster_ctl.py --plan gene_database/DHDGS/test/head_pipeline_final_20261020/candidate.yaml benchmark --request gene_database/DHDGS/test/head_pipeline_final_20261020/benchmark_request.yaml
gene_database/DHDGS/.venv/bin/python gene_database/DHDGS/tools/cluster_ctl.py --plan gene_database/DHDGS/test/head_pipeline_final_20261020/candidate.yaml collect
gene_database/DHDGS/.venv/bin/python gene_database/DHDGS/tools/cluster_ctl.py --plan gene_database/DHDGS/test/head_pipeline_final_20261020/candidate.yaml shutdown
```

候选顶层管理端口22491，回调端口22480；结果写入本目录service/runs。shutdown仅关闭该顶层，数据服务保留。原有搜索参数仍通过configure热更新；peer.head_pipeline的线程布局、接收缩放阈值和ACK策略需要重启顶层。所有可调参数都在candidate.yaml。

`http_smoke.py` 启动另一个临时单进程HTTP顶层，验证32条真实HTTP查询（16并发）及通过HTTP启动32条内部benchmark，检查接收线程峰值不超过4、无接收子进程。该短测试仅验证功能，不能用于稳定性能结论；完成后关闭临时顶层。

运行完整对照用run_scan.py，工件目录不可重复覆盖。analyze.py和write_report.py位于../head_pipeline_20261019，使用 `--root` 指向本目录生成图表及报告。
