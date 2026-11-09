<!-- experiment:entry_expected_work_sweep_20261104:start -->
## R001 每次入口检索期望回源预算阈值八档收敛实验（entry_expected_work_sweep_20261104）

**状态**：已完成

**开始时间**：2026-11-04T16:39:17+08:00

**结束时间**：2026-11-04T18:07:05+08:00

**目的 / 要解决的问题**：验证频率与回源严重程度联合得分能否保留低回源入口、提高接流与源卸载及QPS

**假设**：按每请求期望预算关闭入口，可能改善条件均值对罕见早回源的误惩罚；实际CPU与质量需独立验证

**期望效果**：逐档严格判定收敛，采集学习过程和两次入口不变正式测量，未达到门槛不得声称收敛

**环境与配置 / 对照**：同200K向量/ID/truth、固定源0到目标1的254369记录/2527入口/3441116781字节图，份额.9，D200/beam11/C1536/64线程每节点、冻结原生库及原CPU/NUMA/RDMA；pull及自动构建/调份额/衰退关闭，暖缓存，tmpfs输出，原生全量聚合与1/1000轨迹，Python整轮结束后解码

**本轮改变的变量**：阈值80,70,60,50,40,30,20,10顺序；关闭最少20回源，成熟最少20完成；成熟覆盖80%，3批1M稳定+2批1M保持检验

**代码改动**：仅Python后台入口判定/配置/审计字段改变；C++搜索、原子计数、网络二进制不变；成熟度分母改完成数

**登记时 Git HEAD**：53b3b953d0f491019429da3535e182d7e56a804d

**结果与限制**：100.4M查询零失败/超预算，18487条关闭均有效、全量计数一致；T80接流3.128%/直接回源48.308%/QPS21006.91，T10为0.312%/13.667%/19643.24；源展开减少4.64%→0.48%；整体Recall96.7432%至96.8816%，命中质量未配对；前后基线-5.05%漂移，不认定稳定最优。所有自有服务PID/端口复核释放，保留初次源0端口超时记录。

**报告、图表与证据**

- test/entry_expected_work_sweep_20261104/PLAN.md
- test/entry_expected_work_sweep_20261104/REPORT.md
- test/entry_expected_work_sweep_20261104/formal/compact_evidence.tar.gz
- test/entry_expected_work_sweep_20261104/FINDINGS.md
- test/entry_expected_work_sweep_20261104/implementation.patch
- test/entry_expected_work_sweep_20261104/source_snapshot.tar.gz
- test/entry_expected_work_sweep_20261104/source_hashes.json
- test/entry_expected_work_sweep_20261104/comparability_audit.json
- test/entry_expected_work_sweep_20261104/analysis_hashes.json
- test/entry_expected_work_sweep_20261104/formal/archive.json
- test/entry_expected_work_sweep_20261104/formal/steady_metrics.csv
- test/entry_expected_work_sweep_20261104/formal/closure_decision_audit.csv
- test/entry_expected_work_sweep_20261104/formal/counter_reconciliation.csv
- test/entry_expected_work_sweep_20261104/formal/shutdown_followup.json
- test/entry_expected_work_sweep_20261104/formal/entrance_convergence.png
- test/entry_expected_work_sweep_20261104/formal/entry_return_tradeoff.png
- test/entry_expected_work_sweep_20261104/formal/load_timeseries.png
- test/entry_expected_work_sweep_20261104/formal/ready_pressure.png
- test/entry_expected_work_sweep_20261104/formal/score_traffic_distribution.png
- test/entry_expected_work_sweep_20261104/formal/source_burden_per_hit.png
- test/entry_expected_work_sweep_20261104/formal/steady_comparison.png
- test/entry_expected_work_sweep_20261104/formal/steady_node_load.png

**下一步建议**：保持新语义，80暂定而非最优；候选更宽阈值及少量份额，冻结稳定目录做短交错对照，再在线保持；补齐同query命中群体Recall和源CPU配对，R001总体保持开放

**日志更新时间**：2026-11-04T18:19:30+08:00
<!-- experiment:entry_expected_work_sweep_20261104:end -->
