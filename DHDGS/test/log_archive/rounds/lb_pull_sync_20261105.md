<!-- experiment:lb_pull_sync_20261105:start -->
## 常驻单边READ与唯一分支同步的副本负载均衡（lb_pull_sync_20261105）

**状态**：停止，需检查首批队列/测量证据

**开始时间**：2026-11-05T11:08:50+08:00

**结束时间**：尚未结束

**目的 / 要解决的问题**：整合常驻单边READ/唯一分支同步与副本接流，验证预算、回源和QPS

**假设**：避免回源重复授权，连续拉取在空闲副本计算可减少源等待；收益待验证

**期望效果**：源实际工作/排队下降且QPS上升，质量损失各≤0.5pp

**环境与配置 / 对照**：同200K/D200/beam11/C1536/64线程；固定α≈.3；连续2批READ后派发；全部入口固定

**本轮改变的变量**：同源码无副本/固定副本份额；READ开关；原成本仅非单因素参考

**代码改动**：副本连续READ/不独占源区域；每query物理规范actor；overlay/base本机联合读；兼容性检查；新增分离展开计数

**登记时 Git HEAD**：53b3b953d0f491019429da3535e182d7e56a804d

**结果与限制**：功能验证通过；首批自动流程停止：TimeoutError('等待窗口超出YAML上限；未抢占其他测试')；未宣称性能收益

**报告、图表与证据**

- test/lb_pull_sync_20261105/REPORT.md
- test/lb_pull_sync_20261105/PLAN.md
- test/lb_pull_sync_20261105/implementation.patch
- test/lb_pull_sync_20261105/regression_v3.log
- test/lb_pull_sync_20261105/client_validation.json
- configs/dhdgs_lb_pull_sync.yaml

**下一步建议**：等待预算扫描所有有效组完成及进程退出，再资源预检运行固定α≈.3/份额0-.5-0短对照，结束后离线分析、审计与绘图

**日志更新时间**：2026-11-05T17:41:59+08:00
<!-- experiment:lb_pull_sync_20261105:end -->
