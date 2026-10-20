# 多通道回传实验

结论见 [REPORT.md](REPORT.md)，详细设计见 [DESIGN.md](../../DESIGN.md#独立回传通道与报告队列2026-10-20)。新方案降低尾延迟，但吞吐下降，且等待尚未低于100ms；没有替换常驻旧入口。

候选配置保持16分发线程、2确认线程、16结果进程，新增4个独立报告接收进程、每通道独立结果管道与状态表、共享身份注册、2个完成线程。三台数据服务继续使用Intel-35/32/30现有完整seq_2M索引和32查询线程。

在 `/sharenvme/usershome/xulihan` 启动候选顶层：

```sh
gene_database/DHDGS/.venv/bin/python gene_database/DHDGS/tools/cluster_service.py head --plan gene_database/DHDGS/test/report_channels_20261020/candidate.yaml
```

另一个终端依次执行；保证其他入口没有同时压测：

```sh
gene_database/DHDGS/.venv/bin/python gene_database/DHDGS/tools/cluster_ctl.py --plan gene_database/DHDGS/test/report_channels_20261020/candidate.yaml recover
gene_database/DHDGS/.venv/bin/python gene_database/DHDGS/tools/cluster_ctl.py --plan gene_database/DHDGS/test/report_channels_20261020/candidate.yaml benchmark --request gene_database/DHDGS/test/report_channels_20261020/benchmark.yaml
gene_database/DHDGS/.venv/bin/python gene_database/DHDGS/tools/cluster_ctl.py --plan gene_database/DHDGS/test/report_channels_20261020/candidate.yaml collect
gene_database/DHDGS/.venv/bin/python gene_database/DHDGS/tools/cluster_ctl.py --plan gene_database/DHDGS/test/report_channels_20261020/candidate.yaml shutdown
```

管理端口31001；确认端口31020/31021；回传端口31022–31025。结果集中保存到本目录 `service/runs`，结果进程保存返回前快照到 `service/worker_results`。完整API和最终IPC计时以runs内记录为准。

搜索预算、top-k、ef、remote_weight、数据查询线程仍可通过原 `configure --request patch.yaml` 在线调整。回传通道数、后端、注册方式和完成线程属于顶层启动配置，仅需重启顶层；数据节点无需重部署或重建索引。

`striped/` 是最终3轮/组的正式对照；其他子目录保留各轮消融和负面结果。`run_scan.py --phase <目录>` 读取对应 experiment.yaml；已有cases禁止覆盖，重新实验应创建新目录并使用新的回调端口范围。

重新生成最终统计和图表（不发送查询）：

```sh
gene_database/DHDGS/.venv/bin/python gene_database/DHDGS/test/report_channels_20261020/analyze.py striped
gene_database/DHDGS/.venv/bin/python gene_database/DHDGS/test/report_channels_20261020/detail_plot.py
```

`striped/snapshot` 与其哈希记录正式性能代码；`validated_sources` 是最终回归/HTTP验证代码。性能矩阵之后仅增加完成队列满时的明确失败保护，并限制共享注册仅在新模式启用；未将这两个保护性修正计作性能收益。
