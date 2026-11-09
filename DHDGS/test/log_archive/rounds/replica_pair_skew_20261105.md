<!-- experiment:replica_pair_skew_20261105:start -->
## 历史热点负载下副本源限定READ与同步对照（replica_pair_skew_20261105）

**状态**：已完成

**开始时间**：2026-11-05T22:22:29+08:00

**结束时间**：2026-11-05T23:04:46+08:00

**目的 / 要解决的问题**：R001/R005/R006/R008/R010：解释自然查询低副本命中，并检验历史热点下副本源限定协议实际卸载及性能

**假设**：热点流提高与固定副本入口匹配，可更充分测量READ与同步的召回、回源及资源收益或成本

**期望效果**：命中率明显高于自然流；测量源实际工作及排队，零失败与范围预算错误，不预设QPS收益

**环境与配置 / 对照**：四机/每数据机64搜索线程/D128/beam11/C1536，冻结上一轮源码和NUMA布局；复用旧200K热点occurrences（42898 unique）

**本轮改变的变量**：N/S30/D80，replica_source协议off/on，正反序两重复；每组600K预热+600K正式；输入与真值同步重排，成员入口份额不变

**代码改动**：生产源码不变；新实验客户端按旧selection重排向量和ID，生成对应truth；复用冻结搜索/客户端二进制与既有实验采集脚本

**登记时 Git HEAD**：53b3b953d0f491019429da3535e182d7e56a804d

**结果与限制**：12组正反序共7.2M正式零失败及预算/范围/候选对账错误；旧热点命中S30 40.5%、D80约55%。S30协议on QPS−23.65%、p99+139.01%，D80 QPS−14.38%、p99+77.58%；Recall分别94.02→95.04%、93.85→95.18%。每命中READ恢复排队141.25/60.07ms，主新增等待在恢复计算；总进程CPU/query+24.07%/+20.85%，源CPU/query无明显净降低。回源率增加，不能作为吞吐优化启用。D80on比同期Noff QPS+40.10%但p99更高，off召回不足95%不能视为等质量对照。首Noff预热监控突发保留，正式窗口均通过阈值；两重复、非独占、暖缓存、静态副本及on联合机制限制明确。 后续离线对齐off组5992观测/布局，证实第三节点漏达16/85和21/66；但93.40%/96.76%净召回差额发生于基线无分支群体，不支持第三节点障碍为主要原因。用户决定暂不采用READ/sync。

**报告、图表与证据**

- test/replica_pair_skew_20261105/REPORT.md
- test/replica_pair_skew_20261105/DIAGNOSIS.md
- test/replica_pair_skew_20261105/comparison.png
- test/replica_pair_skew_20261105/hit_coverage.png
- test/replica_pair_skew_20261105/node_comparison.png
- test/replica_pair_skew_20261105/load_timeline.png
- test/replica_pair_skew_20261105/protocol_costs.png
- test/replica_pair_skew_20261105/s30_on_c_p50.png
- test/replica_pair_skew_20261105/s30_on_c_p99.png
- test/replica_pair_skew_20261105/d80_on_c_p50.png
- test/replica_pair_skew_20261105/d80_on_c_p99.png
- test/replica_pair_skew_20261105/comparison.json
- test/replica_pair_skew_20261105/resources.json
- test/replica_pair_skew_20261105/entry_coverage.json
- test/replica_pair_skew_20261105/workload_identity.json
- test/replica_pair_skew_20261105/source_identity.json
- test/replica_pair_skew_20261105/runs.csv
- test/replica_pair_skew_20261105/branch_diagnosis/REPORT.md
- test/replica_pair_skew_20261105/branch_diagnosis/summary.json
- test/replica_pair_skew_20261105/branch_diagnosis/queries.csv
- test/replica_pair_skew_20261105/branch_diagnosis/paired_hits.csv
- test/replica_pair_skew_20261105/branch_diagnosis/branch_rates.png
- test/replica_pair_skew_20261105/branch_diagnosis/example_flow.png
- test/replica_pair_skew_20261105/branch_diagnosis/analyze.py
- test/replica_pair_skew_20261105/branch_diagnosis/report.py

**下一步建议**：暂不采用READ/sync；在原策略下审计副本导航、回源种子及预算；不把样本分组差额当因果贡献。

**日志更新时间**：2026-11-05T23:55:55+08:00
<!-- experiment:replica_pair_skew_20261105:end -->
