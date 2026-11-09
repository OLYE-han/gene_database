<!-- experiment:onesided_pull_20261102:start -->
## 单边RDMA读取与CoTra通信机制评估（onesided_pull_20261102）

**状态**：已完成

**开始时间**：2026-11-02T13:12:05+08:00

**结束时间**：2026-11-02T14:02:03+08:00

**目的 / 要解决的问题**：实现真正IBV_WR_RDMA_READ，降低源读RPC处理；核对CoTra primary/secondary机制可迁移性

**假设**：注册只读索引与量化码，授权后由副本RNIC主动读取可减少源逐批服务CPU；数据依赖与目标安装仍有成本

**期望效果**：首先证明硬件单边读取与搜索预算/质量正确性，再短批测源CPU、队列、QPS，不预设收益

**环境与配置 / 对照**：源Intel-31、副本Intel-30、节点Intel-32、顶层Intel-35；S30总α≤0.3/share .8、预算200/dwell144/阈值39、主64尾8、每批8、上下文64/在途16；单边16QP。各case预热8K、11K/s固定到达和并发1024饱和各30K；全量seq_2M索引、暖缓存、独立测试集合受控回放。

**本轮改变的变量**

- A1/A2：RPC拉取基线；Br：private_cow后单边READ；B2/C：同一最终代码，单条post对链式最多16 WR post。
- B初次只读共享mmap注册失败，无正式请求；4KiB至全文件注册探针对照shared_readonly/private_cow；最终使用private_cow。
- 不改变阈值39、分片/副本布局、预算/区域授权；初始化目录读取与MR成本单列。

**代码改动**：新增cpp/one_sided.hpp/.cpp，真正IBV_WR_RDMA_READ/只读MR/RC QP/有界CQ推进；接入异步尾部续跑与授权；可选链式post；配置和指标、硬件测试、CMake verbs可选依赖。测试通过的本机模块已安装，默认单边/链式均关闭。changes.patch相对本轮before快照，保留既有未提交改动；源码/模块身份与阶段快照均归档。

**登记时 Git HEAD**：53b3b953d0f491019429da3535e182d7e56a804d

**结果与限制**：28项定向检查通过；5个成功case、10块300K正式请求0失败，单边组真实READ且完成错误0。A1/A2饱和QPS13439.87/12756.59，基线漂移−5.08%；B2/C 12929.59/12957.99(+0.22%)，无稳定吞吐或源净CPU收益。链式post/CQ下降93.33%，I/O CPU下降22%，READ次数/字节未减少；码READ和续跑等待仍显著。源注册10.79GiB、RSS增加10.8–10.9GiB，首次注册6.48s，目标16目录合计488MiB；不是SSD单边读。整体Recall约96.6%，命中94.5–94.8%仍未达95%。初次注册失败、两次fixture错误、交付脚本首次误用系统解释器均保留并已修复；不计为成功性能样本。暖缓存/共享机/短块/部分协议计量限制保留。已读CoTra原文；primary=副本可行，Task-Push需源余量门控；WRITE环/租约/布局优化仅研究。R001-A29性能目标仍开放。 报告/图表及最终28项检查已核验；完整交付和5组原始结果已SHA核验并同步，最终收尾文档另存不可变补充归档。

**报告、图表与证据**

- [报告与图](../../onesided_pull_20261102/REPORT.md)
- [性能图](../../onesided_pull_20261102/performance.png) · [门铃图](../../onesided_pull_20261102/doorbell_effect.png) · [阶段图](../../onesided_pull_20261102/pull_phases.png)
- [CoTra迁移分析](../../../research/R001_ONE_SIDED_COTRA.md) · [已验证设计](../../../DESIGN.md#one-sided-replica-read)
- [最终28项检查](../../onesided_pull_20261102/final_checks.log) · [审计](../../onesided_pull_20261102/audit_final.json)
- [交付清单](../../onesided_pull_20261102/delivery_manifest.json) · [源码身份](../../onesided_pull_20261102/change_identity.json) · [模块安装](../../onesided_pull_20261102/install_identity.json) · [停止核验](../../onesided_pull_20261102/postflight.json)
- 五组原始结果及完整交付已SHA验证并在测量后同步至Intel-32本轮evidence_archive；本机保留报告/图/代码与tmpfs工作副本，大文件未加入git。

**下一步建议**：优先分别验证区域租约以减少逐批授权、减少重复邻居码和碎片READ；为超内存SSD场景设计有界注册缓存。Task-Push需源runnable与净卸载门控，不能照搬邻居度阈值2；质量和QPS验收前保持默认关闭，不开展布局长测。

**日志更新时间**：2026-11-02T14:02:16+08:00
<!-- experiment:onesided_pull_20261102:end -->
