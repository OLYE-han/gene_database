<!-- experiment:candidate_stalls_20261104:start -->
## 分支交付异步化与共享目录扩容停顿修复（candidate_stalls_20261104）

**状态**：已完成；局部优化通过，实际搜索CPU主导目标未达成

**开始时间**：2026-11-04T22:23:06+08:00

**结束时间**：2026-11-05T02:24:36+08:00

**目的 / 要解决的问题**：R005/R006/R010：去除claim和候选交付对搜索的串行停顿；同轮离线复核p99瓶颈和最终通信计量（R008）

**假设**：异步首次接纳和报告交付减少串行依赖；启动预留目录桶避免共享锁内哈希扩容造成的长尾

**期望效果**：同配置QPS提升、p99下降；零失败、预算守恒，召回影响如实报告

**环境与配置 / 对照**：无LB，原200K自然查询，64搜索线程/节点，D200/beam11，Intel35入口及客户端，31/32/30数据节点

**本轮改变的变量**：第一阶段baseline/非阻塞+提前报告/候选合并/接纳池8线程，四配置正反序各两次600K测量；完成池0/4/8及200K预热早期对照单列；第二阶段同推荐组合reserve=0/2000000，ABBA各600K；完整容量12032768补充600K单测；正式各600K预热，均C1536/D200/beam11/64线程/无LB；同轮补充本机报告复制/移动与延迟ACK正确性回归，既有样本离线分析，无新四机压测

**代码改动**：原子首次接纳与异步预算结算；提前报告与交付闭环；每目标单在途候选合并；可选独立接纳池和按消息类别的接收耗时统计；新增启动时受容量上限约束的registry_reserve_entries和共享锁内哈希扩容计时；cpp/native_head.cpp复制/移动共用最终候选通信补齐，tests/test_candidate_stalls.py延迟ACK计数守恒回归

**登记时 Git HEAD**：53b3b953d0f491019429da3535e182d7e56a804d

**结果与限制**：第一阶段4.8M：QPS19222.61→20393.95，p99=267.12→194.58ms，p50略增；协议挂起3.7211→0.0667ms/query。第二阶段ABBA2.4M：共享锁内扩容插入实测最长257.5ms，reserve2M后受监测扩容0，p99=214.84→156.50ms，QPS19937.52→19703.50，不能称吞吐提高。完整容量单次600K为20695.16QPS/p99=150.42ms，Recall97.060%；总7.8M零失败/预算/账目/重复分支错误。全部暖缓存。最终p99样本150.12ms中ready136.10ms/CPU5.64ms；负载启动仍两条约100ms入口长尾。早期池失败、配置0拒绝和FD1024回归失败已修复且保留；外部干扰按十分钟观察后恢复，未停止他人进程。 同轮离线复核：最新600K平均成功pull1.081343批/数据READ5.008652WR/含等待1.053375ms、新增子分支0.239560、同步2.112677包；旧1.633483漏计。第一阶段实际包数减少6.29%，历史回调滞后95.12%收益结论撤回。移动报告漏计旧版复现、新版5项本机回归通过，未三机重新部署或性能测试。

**报告、图表与证据**

- test/candidate_stalls_20261104/REPORT.md
- test/candidate_stalls_20261104/STAGE1_REPORT.md
- test/candidate_stalls_20261104/comparison.png
- test/candidate_stalls_20261104/registry_comparison.png
- test/candidate_stalls_20261104/registry_growth_timeline.png
- test/candidate_stalls_20261104/registry_queue_comparison.png
- test/candidate_stalls_20261104/registry_fullcap_p50.png
- test/candidate_stalls_20261104/registry_fullcap_p99.png
- test/candidate_stalls_20261104/registry_fullcap_p99_cross.png
- test/candidate_stalls_20261104/best_settings.yaml
- test/candidate_stalls_20261104/registry_final_audit_preflight.json
- DESIGN.md#candidate-registry-reserve-20261105
- Research.md
- LOCKS.md#candidate-registry-reserve-20261105
- test/candidate_stalls_20261104/p99_diagnosis/REPORT.md
- test/candidate_stalls_20261104/p99_diagnosis/communication.png
- test/candidate_stalls_20261104/p99_diagnosis/p99_query.png
- test/candidate_stalls_20261104/p99_diagnosis/slow_cross_query.png
- test/candidate_stalls_20261104/p99_diagnosis/slow_cross_pulls.png
- test/candidate_stalls_20261104/p99_diagnosis/compute_queues.png
- test/candidate_stalls_20261104/p99_diagnosis/p99_cohort.png
- test/candidate_stalls_20261104/p99_diagnosis/packet_counter_audit.csv
- test/candidate_stalls_20261104/p99_diagnosis/source_identity.json
- test/candidate_stalls_20261104/p99_diagnosis/metrics_fix.patch

**下一步建议**：首次计算队列的节点失衡/年龄公平、小预算首次派发成本、拉取信用与授权逐请求进度；通用回调最终计量补齐；维持原预算及唯一物理分支；长期回收/冷SSD/副本仍未验收。

**日志更新时间**：2026-11-05T02:24:36+08:00
<!-- experiment:candidate_stalls_20261104:end -->
