<!-- experiment:replica_no_dwell_20261104:start -->
## R001 取消副本源驻留门槛与回源预算观测（replica_no_dwell_20261104）

**状态**：已完成

**开始时间**：2026-11-04T11:30:01+08:00

**结束时间**：2026-11-04T11:45:03+08:00

**目的 / 要解决的问题**：R001/F02/F26：源候选直接参与扩展选择，衡量回源和净卸载权衡

**假设**：更早回源可能改善质量并提高源剩余工作，净收益待测

**期望效果**：预算守恒无失败；报告回源率、分割前后预算、Recall/QPS与排队

**环境与配置 / 对照**：上轮冻结配置，D200/beam11/C1536/64线程，RDMA消息、pull关闭，200K循环，暖缓存

**本轮改变的变量**：删除replica_source_min_expansions=144；其他搜索参数不变；客户端补齐预算累计

**代码改动**：C++移除源专属过滤；删除配置/schema/统计字段；旧配置显式拒绝；独立客户端回源预算累计

**登记时 Git HEAD**：53b3b953d0f491019429da3535e182d7e56a804d

**结果与限制**：600.108530秒12,883,814请求零失败/预算超限；回源83.2585%，剩余98.0008/200，首次源授权76.5933，源实际展开78.6445/回源；QPS21469.14，整体Recall96.7049%，命中92.0091%。入口关闭8665，去重开放2548，接流6.8299%，源0ready82.99ms；历史对照布局与命中群体不同，双群体质量和净卸载未验收。四机与工作区各57项回归通过；初次夹具错误、首次端口审计失败均保留，后续PID/端口核验通过。

**报告、图表与证据**

- test/replica_no_dwell_20261104/REPORT.md
- test/replica_no_dwell_20261104/comparison.png
- test/replica_no_dwell_20261104/return_budget_timeseries.png
- test/replica_no_dwell_20261104/formal/load_balance_comparison.png
- test/replica_no_dwell_20261104/comparison.json
- test/replica_no_dwell_20261104/source_hashes.json
- Research.md
- DESIGN.md#replica-no-dwell-20261104

**下一步建议**：保持源驻留取消；固定同图/同入口/同份额配对检验质量与源工作，再独立评估入口关闭，不恢复硬驻留门槛。

**日志更新时间**：2026-11-04T11:50:05+08:00
<!-- experiment:replica_no_dwell_20261104:end -->
