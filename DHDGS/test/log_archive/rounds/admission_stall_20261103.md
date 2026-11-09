<!-- experiment:admission_stall_20261103:start -->
## R002：持续负载接纳停顿定位（admission_stall_20261103）

**状态**：已完成（根因定位与局部修复验证；系统级目标未全部解决）

**开始时间**：2026-11-03T16:01:42+08:00

**结束时间**：2026-11-03T16:52:31+08:00

**目的 / 要解决的问题**：R002/R008：定位持续查询低QPS与搜索线程空闲，验证查询上下文回收的针对性修复

**假设**：节点身份锁内回收大批到期上下文导致接纳断供；栈与无LB对照验证

**期望效果**：恢复持续吞吐，保留残余失败及内存稳态限制

**环境与配置 / 对照**：Intel-35顶层+独立API客户端，31/32/30数据；C1024/D200，64/68/68计算线程，200K自然回放，RDMA，暖缓存，retention120s

**本轮改变的变量**

- observe诊断480秒：关闭自动规划，捕获接收等待栈；顶层分配器/NUMA遗漏、ptrace及符号工具重写映射模块干扰，性能对照作废
- control/fixed各600秒，统一jemalloc/NUMA配置；fixed使用锁外context销毁及每轮256个到期context检查
- control发送期无调试/编译干扰；617.34秒后编译与末尾排空重叠，主比较固定前600秒成功完成量，完整QPS仍附限制

**代码改动**：cpp/native_runtime.cpp：提取到期context map节点，锁外销毁、分批256和3项轻量回收计数；config.py/YAML增加兼容可选参数与范围检查；新增生命周期回归；冻结测试runner/汇总绘图、DESIGN/LOCKS/Research更新；未覆盖其它任务改动

**登记时 Git HEAD**：53b3b953d0f491019429da3535e182d7e56a804d

**结果与限制**：16个handler同mutex等待、维护线程QueryContext→RegionInbox→jemalloc/madvise。前600秒成功QPS6692.42→13588.09（+103%），p99 277.91→194.58ms；三池断供采样266.08→2.13s。最终失败386532/4401988→89/8153967；成功Recall97.3953%→97.3481%。11项region+18项native通过；既有聚合测试原版同样失败。初始诊断异常和配置兼容失败已保留。仍有197ms临界区、89例未分类失败、RSS增长；不宣称零失败/<100ms/长期稳态。所有本轮数据进程已退出。

**报告、图表与证据**

- [报告](../../admission_stall_20261103/REPORT.md)
- [对照图](../../admission_stall_20261103/comparison.png)
- [指标](../../admission_stall_20261103/comparison.json)
- [栈证据](../../admission_stall_20261103/evidence/stack_summary.json)
- [原版审计](../../admission_stall_20261103/control/audit.json)
- [修复审计](../../admission_stall_20261103/fixed/audit.json)
- [设计](../../../DESIGN.md#bounded-query-context-reclamation)

**下一步建议**：有界聚合失败类型/样本；细分并限额处理记录及墓碑更新；记录到期积压与回收速率，验证30分钟以上内存稳态；R002系统级目标及R001负载均衡仍开放

**日志更新时间**：2026-11-03T16:52:31+08:00
<!-- experiment:admission_stall_20261103:end -->
