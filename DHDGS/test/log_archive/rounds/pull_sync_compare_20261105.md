<!-- experiment:pull_sync_compare_20261105:start -->
## 原成本分支与当前RDMA拉取加候选同步的同期对照（pull_sync_compare_20261105）

**状态**：已完成；当前方案未获得吞吐/延迟收益

**开始时间**：2026-11-05T09:01:29+08:00

**结束时间**：2026-11-05T10:47:23+08:00

**目的 / 要解决的问题**：同资源比较原本无远端数据拉取、无周期候选同步的成本扩展策略与当前完整方案的QPS、延迟和质量

**假设**：当前拉取和候选同步可能降低远端计算或提高召回，但其控制成本也可能抵消吞吐收益

**期望效果**：获得同C1536/D200/64线程的可复核正反序对照，不预设当前方案更快

**环境与配置 / 对照**：Intel35顶层+独立TCP API客户端，Intel31/32/30数据节点；200K自然查询循环、每组600K预热/600K测量，D200/beam11/C1536/64线程，无LB与副本、暖缓存；RDMA消息网络/亲和/目录预留一致

**本轮改变的变量**：原成本策略：remote_pull/candidate_sync及依赖的3开关false；当前5开关true，其他YAML完全一致；同方案重复配置完全相同。有效original_a/current_a2/original_b/current_b2；current_a/current_b作废保留。

**代码改动**：未修改生产算法；隔离冻结部署与5开关YAML对照，实验脚本补充外部CPU/fio预检/最终审计、聚合比较、图表、紧凑监控存档；Research/DESIGN记录结果与边界。

**登记时 Git HEAD**：53b3b953d0f491019429da3535e182d7e56a804d

**结果与限制**：4×600K有效查询零失败，QPS 22775.36→20747.77；mean 67.088→73.655ms、p99 130.949→162.819ms，Recall 96.87133%→97.06019%。预算/账目正确，当前唯一分支0违规且最终候选包三方对账通过。current_a/current_b受外部CPU/fio干扰作废；短测暖缓存，旧区域授权语义与当前不同，不推导独立READ因果。

**报告、图表与证据**

- test/pull_sync_compare_20261105/REPORT.md
- test/pull_sync_compare_20261105/comparison.png
- test/pull_sync_compare_20261105/comparison.csv
- test/pull_sync_compare_20261105/original_p50.png
- test/pull_sync_compare_20261105/original_p99.png
- test/pull_sync_compare_20261105/current_p50.png
- test/pull_sync_compare_20261105/current_p99.png
- test/pull_sync_compare_20261105/queues_and_pull.png
- test/pull_sync_compare_20261105/source_manifest.json
- test/pull_sync_compare_20261105/client_manifest.json
- test/pull_sync_compare_20261105/evidence_verification.json
- test/pull_sync_compare_20261105/external_observations.jsonl
- test/pull_sync_compare_20261105/wait_decisions.jsonl

**下一步建议**：固定唯一分支/预算语义消融成本选择、拉取与候选同步；等Recall约95%的预算扫描；CPU profile定位管理/估距/缓存与控制成本。本轮未执行。

**日志更新时间**：2026-11-05T10:47:23+08:00
<!-- experiment:pull_sync_compare_20261105:end -->
