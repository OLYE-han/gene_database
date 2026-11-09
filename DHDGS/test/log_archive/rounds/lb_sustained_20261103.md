<!-- experiment:lb_sustained_20261103:start -->
## R001：30分钟自然200K持续查询观察（lb_sustained_20261103）

**状态**：已完成（性能验收未通过）

**开始时间**：2026-11-03T13:37:30+08:00

**结束时间**：2026-11-03T14:29:21+08:00

**目的 / 要解决的问题**：观察当前自动负载均衡副本大小/次数、接流回源、RDMA和三节点排队CPU

**假设**：持续自然流量下算法可辨识失衡并自动发布；容量与回流可能限制收益

**期望效果**：完成30分钟完整图表，不预设副本成功或QPS改善

**环境与配置 / 对照**：Intel-35顶层及独立客户端；31/32/30数据；D200/C1024；源64目标64+4共享；自然200K循环

**本轮改变的变量**

- pilot 90秒；参照observe 120秒；dynamic 1800秒
- 完成状态retention 600→120秒，max_tombstones 1.2M→12M；其它LB保持推荐配置，pilot后冻结
- pilot选图超过90秒，正式planner_max_age_ms 10,000→360,000；保留发布前重新校验

**代码改动**：隔离native客户端增加duration循环、5秒聚合、稀疏样本；服务搜索代码不变；测试runner与分析脚本；持久化长测汇总/空窗口/失败口径至test/EXPERIMENT_WORKFLOW.md；未改DESIGN能力声明

**登记时 Git HEAD**：53b3b953d0f491019429da3535e182d7e56a804d

**结果与限制**：正式1800秒自然回放12,635,395请求，失败1,089,121（8.62%）；含排空成功QPS6014.58，全局p99约319.23ms、局部p99达120秒，成功Recall97.386%。5次计划、0切割复制发布；3过期/1容量预约/1负载改变。接纳队列拥堵与搜索空闲约843秒，精确锁根因待定位。完整窗口及样本捕获7.51MiB，无全量逐query轨迹；计数审计通过。pilot及收尾问题详见报告。

**报告、图表与证据**

- [报告](../../lb_sustained_20261103/REPORT.md)
- [QPS/p99/召回](../../lb_sustained_20261103/formal/throughput_quality.png)
- [副本尝试](../../lb_sustained_20261103/formal/replica_lifecycle.png)
- [接纳队列](../../lb_sustained_20261103/formal/admission_stalls.png)
- [计算队列](../../lb_sustained_20261103/formal/compute_queues.png)
- [CPU/RSS](../../lb_sustained_20261103/formal/cpu_memory.png)
- [审计](../../lb_sustained_20261103/formal/audit.json)

**下一步建议**：修复自动规划预算传递和端到端时限，接入已验证原生流水；独立定位长周期接纳/回收等待，之后做无LB/动态LB配对长测。R001保持开放。

**日志更新时间**：2026-11-03T14:29:21+08:00
<!-- experiment:lb_sustained_20261103:end -->
