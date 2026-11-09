<!-- experiment:covered_entry_long_20261104:start -->
## R001 全覆盖入口准入10分钟三机接流测试（covered_entry_long_20261104）

**状态**：已完成

**开始时间**：2026-11-04T10:39:05+08:00

**结束时间**：2026-11-04T11:01:22+08:00

**目的 / 要解决的问题**：测量副本开放入口、接流、回源预算与源排队

**假设**：覆盖入口开放与预算反馈可能扩大有效卸载，收益待实测

**期望效果**：完成600秒测试并报告真实接流/回源/负载及限制

**环境与配置 / 对照**：三数据节点+顶层；D200 C1536；64搜索线程；45秒observe+600秒dynamic，暖缓存

**本轮改变的变量**

- 入口准入模式和后台计数；其余沿用shares配置
- 同轮observe对照无副本

**代码改动**：维护期load_balance.observe日志新增open_entries/closed_entries，无新增搜索统计锁；已实现准入方案按冻结版本部署，服务器/原生紧凑客户端分别记录身份；分析与图表代码归档。

**登记时 Git HEAD**：53b3b953d0f491019429da3535e182d7e56a804d

**结果与限制**：12395610次查询、零失败/预算越界；6/6小图发布，单图51.94–76.45秒；入口版本12850→9524、去重6126，关闭3326且阈值全部有效，计数无饱和并与2714585命中完全对齐。命中21.90%，计算回源78.30%，平均剩余63.77/200；QPS20282→20655(+1.84%)，node0 ready109.30→33.60ms；整体Recall96.93→95.75%(-1.1805pp)、命中90.50%、p99增加、搜索CPU/query+9.48%。仅证明机制和本轮负载改善，不通过质量/吞吐联合验收。保留SSH文件描述符限制引起的首次53项部署自检失败，修正夹具后每机53项通过；node0初次30秒端口审计失败，后续所有自有PID/端口退出空闲。单轮先后对照、暖缓存、外部负载存在；R001开放。

**报告、图表与证据**

- test/covered_entry_long_20261104/REPORT.md
- test/covered_entry_long_20261104/formal/entrances_traffic_returns.png
- test/covered_entry_long_20261104/formal/load_balance_comparison.png
- test/covered_entry_long_20261104/archive_manifest.json
- research/R001_COVERED_ENTRY_ADMISSION.md
- DESIGN.md#covered-entry-admission-20261104

**下一步建议**：固定图和同查询质量/净卸载配对；提高副本搜索质量，联合约束目标接流份额与重叠区域增量收益；不再以入口覆盖不足单独解释QPS。

**日志更新时间**：2026-11-04T11:01:22+08:00
<!-- experiment:covered_entry_long_20261104:end -->
