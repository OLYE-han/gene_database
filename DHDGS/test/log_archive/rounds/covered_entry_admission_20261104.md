<!-- experiment:covered_entry_admission_20261104:start -->
## R001 全覆盖入口与回源预算准入（covered_entry_admission_20261104）

**状态**：已完成

**开始时间**：2026-11-04T10:23:11+08:00

**结束时间**：2026-11-04T10:26:49+08:00

**目的 / 要解决的问题**：验证全部覆盖入口开放、无新增统计锁的回源预算累计与逐入口关闭

**假设**：完整核心证书可与成员入口准入分离，实际回源预算可识别过早回源入口

**期望效果**：预算不超限、每次查询最多一个根、累计统计守恒、阈值准确，无吞吐预设

**环境与配置 / 对照**：本机原生编译与单机三节点TCP正确性测试；非seq2M性能测试

**本轮改变的变量**：旧完整一跳入口资格与全部成员入口；32次回源、50%预算、5秒周期；并发写与目录刷新

**代码改动**：入口全集交集、结构与准入分离、分片原子累计、后台入口关闭、重叠家族单根路由、影子校准诊断化

**登记时 Git HEAD**：53b3b953d0f491019429da3535e182d7e56a804d

**结果与限制**：原生编译、23项定向检查和119项既有回归通过。单机三节点64并发，回源64次、剩余和12736、均值199/200，后台关闭后下一query走源入口；未新跑seq2M三机性能测试，QPS/Recall收益未验收。

**报告、图表与证据**

- test/covered_entry_admission_20261104/REPORT.md
- test/covered_entry_admission_20261104/native_evidence.json
- test/covered_entry_admission_20261104/source_hashes.json
- test/covered_entry_admission_20261104/source_snapshot/
- research/R001_COVERED_ENTRY_ADMISSION.md
- DESIGN.md#covered-entry-admission-20261104

**下一步建议**：固定图与独立请求集，对比64入口、全覆盖不关闭、全覆盖预算关闭，配对检查接流、源CPU/排队、QPS及整体和命中群体Recall。

**日志更新时间**：2026-11-04T10:30:43+08:00
<!-- experiment:covered_entry_admission_20261104:end -->
