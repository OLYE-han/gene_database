<!-- experiment:pull_policy_help_20261102:start -->
## 暂停策略年龄与空闲主池帮助RDMA尾部续跑（pull_policy_help_20261102）

**状态**：已完成

**开始时间**：2026-11-02T09:35:09+08:00

**结束时间**：2026-11-02T10:07:09+08:00

**目的 / 要解决的问题**：分别实现并验证扣除拉取暂停的策略年龄、主池有界帮助尾部续跑，减少拉取反馈和尾队列，评价源负载及QPS

**假设**：暂停计入候选年龄诱发额外max_wait；空闲主池可分担尾池已就绪计算，但源主池满载可能限制QPS收益

**期望效果**：无失败/并行重复执行/预算违规；命中及整体Recall约>=95%；QPS同批稳定改善，不以局部队列下降替代整体验收

**环境与配置 / 对照**：三机源Intel-31/普通Intel-32/副本Intel-30；S30总alpha<=.3、包3442121068B、份额.8、预算200、驻留144、tau39、主64/尾8、批8、上下文64、每源在途16；CPU/网络保持E8；暖缓存，共享机，未停他人任务。

**本轮改变的变量**

- A1/B/A2/H/BH短批：各30K、预热8K，依次只扣暂停/只帮助8/两者组合；基线漂移4.35%。
- CA/CBH独立确认：各11K到达80K + 并发1024饱和200K，各块预热20K；帮助max8、reserve8、queue_low0；不继续扩大矩阵。

**代码改动**：实现异步暂停候选年龄扣除；atomic额度与Pool已有锁的主池有界接纳；新增YAML与兼容校验、帮助CPU/计数/峰值、逐批时间点；25项检查通过。独立changes.patch/change_identity.json及部署source_hashes.json；根native已原子安装并记录hash，未提交Git。

**登记时 Git HEAD**：53b3b953d0f491019429da3535e182d7e56a804d

**结果与限制**：7组9块710000正式请求失败0；25项检查通过，观测预算/实际ID/执行器/ready分解无违规。200K基线/组合QPS12479.54→12606.31(+1.02%，未达稳定3%)；命中Recall94.700%→94.671%未过95%，整体96.710%→96.698%。续跑队列4.108→.670ms、拉取等待6.284→2.486ms、应用层字节率609→504MiB/s、后续max_wait13351→8。11K到达源runnable p95 6.550→2.685ms、端到端p99 34.669→26.100ms；饱和源仍63.9/64槽、约91%源CPU来自未命中副本。目标首次runnable p95 .167→.378ms，有等待转移。共享机/有限重复/暖缓存/时钟采样不确定/迟到传输统计不完备，完整native区间仅抽样；默认关闭。无失败/中断测试；源码及证据已SHA归档Intel32，1007文件111122548B，原始各case另有归档。只清除可重建pyc缓存，未删原数据报告。

**报告、图表与证据**

- [报告](../../pull_policy_help_20261102/REPORT.md)
- [性能图](../../pull_policy_help_20261102/performance_comparison.png)
- [阶段图](../../pull_policy_help_20261102/pull_phase_comparison.png)
- [同到达率负载](../../pull_policy_help_20261102/load_comparison_paced.png)
- [饱和负载](../../pull_policy_help_20261102/load_comparison_saturated.png)
- [CSV](../../pull_policy_help_20261102/comparison.csv)
- [审计](../../pull_policy_help_20261102/audit_final.json)
- [变更身份](../../pull_policy_help_20261102/change_identity.json)
- [原子安装](../../pull_policy_help_20261102/install_identity.json)
- [交付归档清单](../../pull_policy_help_20261102/delivery_manifest.json)
- [服务停止核验](../../pull_policy_help_20261102/postflight.json)
- [研究收尾](../../../research/R001_ASYNC_RDMA_TAIL_PLAN.md)
- [运行合同](../../../DESIGN.md#async-rdma-tail-runtime)

**下一步建议**：R001-A29/R001/R005/R008保持开放；先修拉取与源子授权的候选/授权/停止语义与命中质量，再在alpha<=.3内改善占源CPU主要部分的未命中入口覆盖。仅目标主队列有压力时再测帮助4/8与联合门控，不把局部时延收益当作整体吞吐验收。

**日志更新时间**：2026-11-02T10:07:22+08:00
<!-- experiment:pull_policy_help_20261102:end -->
