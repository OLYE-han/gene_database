<!-- experiment:demand_growth_20261106:start -->
## 查询驱动共享缓存与副本增量学习（demand_growth_20261106）

**状态**：已完成

**开始时间**：2026-11-06T15:05:50+08:00

**结束时间**：2026-11-09T09:38:13+08:00

**目的 / 要解决的问题**：比较保留热点缓存与主动提升top20对源卸载、QPS/p99/Recall的影响

**假设**：严格规则可能凭缓存获得较低维护成本；主动提升可能提高后续本地覆盖，结论由实测确定

**期望效果**：同预算零错误，分开度量缓存与持久图收益、源CPU和完整学习成本，不预设触发规则优劣

**环境与配置 / 对照**：四机：35路由/客户端，31/32/30数据；64工作线程/节点，D128/beam11/C1536，旧热点200K occurrence；最终Intel32只用NUMA0的32物理核SMT，三机52/32/40物理核，网络核有SMT/掩码共享。

**本轮改变的变量**：N/S30/D80；off/cache-only/full_no_singleton/full_hot_batch；100记录/份、top20热点及必要连接、beta源索引10%；增长与冻结阶段

**代码改动**：cpp demand_growth，搜索记录提供器与异步恢复，副本源只读会话，配置/容量/持久化/计量及回归；源/目标/顶层beta额度记账、生命周期引用环、SSD目录覆盖及冻结压测审计修正；热点排名快照修复、Python/原生计数字段对齐、故障日志采集及有界采样归档

**登记时 Git HEAD**：53b3b953d0f491019429da3535e182d7e56a804d

**结果与限制**：最终修复版同源码/同核布局20组均通过资源审计，3600万请求（预热/在线/冻结各1200万），失败及预算/作用域/展开账目错误零，容量与提交文件一致，封口后清缓存且零READ，全部服务已退出。S30 hot相对off：QPS21209→15964，p99150.51→208.56ms，源CPU/query下降43.49%；D80 QPS27960→25305，p99131.18→158.25ms，源CPU下降33.08%。目标Intel30计算就绪升至184.92/134.30ms每分支，源卸载未兑现整体收益；hot召回94.323%/94.406%，未达约95%。strict/cache-only也未证明稳定净收益，默认仍off。两重复共享主机/核数异构、热点重复流、暖缓存/tmpfs核心与注册RAM READ、未耗尽beta及入口训练联动未验收的限制明确保留；N无作用对照也有时间漂移，不把小幅差异或CPU墙钟差额归为缓存/锁因果收益。

**报告、图表与证据**

- test/demand_growth_20261106/REPORT.md
- test/demand_growth_20261106/TABLES.md
- test/demand_growth_20261106/measure_comparison.png
- test/demand_growth_20261106/frozen_comparison.png
- test/demand_growth_20261106/measure_load_comparison.png
- test/demand_growth_20261106/growth_effects.png
- test/demand_growth_20261106/offload_vs_qps.png
- test/demand_growth_20261106/s30_hot_numa_a_r1_measure_p99.png
- test/demand_growth_20261106/timeline_samples.json
- test/demand_growth_20261106/final_audit.json
- test/demand_growth_20261106/source_identity.json
- test/demand_growth_20261106/primary_cases.json
- test/demand_growth_20261106/ATTEMPTS.md
- test/demand_growth_20261106/PROMOTION_RACE.md
- test/demand_growth_20261106/final_shutdown_check.json
- DESIGN.md#replica-demand-growth-20261106
- Research.md
- test/remote_cleanup_20261106/REPORT.md

**下一步建议**：仅建议目标服务容量约束的引流、共享提供器查找/引用/锁成本消融、有限预算质量与D80 strict控制尾部计量，再考虑独立/同空间/冷SSD验证；本轮不实施额外优化，不关闭R001/F30净收益问题。

**日志更新时间**：2026-11-09T09:38:13+08:00
<!-- experiment:demand_growth_20261106:end -->
