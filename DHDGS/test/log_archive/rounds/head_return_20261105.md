<!-- experiment:head_return_20261105:start -->
## 顶层合并与API回传长等待定位修复（head_return_20261105）

**状态**：已完成

**开始时间**：2026-11-05T14:24:22+08:00

**结束时间**：2026-11-05T17:22:14+08:00

**目的 / 要解决的问题**：拆解并降低合并入队至客户端接收的50.22ms，复验后恢复预算扫描

**假设**：逐核绑定导致路由与合并竞争；API完成队列/编码/客户端进度贡献后段等待

**期望效果**：不减并发/预算/观测内容，降低等待并提高吞吐，零失败与预算对账通过

**环境与配置 / 对照**：D32/C1536/64搜索线程/beam11/无LB；两阶段CPU/NUMA布局分开比较；第二阶段32/48头部物理核、8/16 API reactors、逐核/共享绑定和批次0/8；每组600K预热+600K正式

**本轮改变的变量**：第一阶段5组机制对照；第二阶段32/48头部物理核、8/16 API reactors四档正反序各2次，同数据布局；干扰pilot及未启动重规划单列保留

**代码改动**：新增head_pin_threads可选共享CPU绑定、API response_batch_size有界编码批次及剩余完成项非阻塞poll；补齐服务端/客户端返回路径计时与线程角色CPU；生产工作区29项网络/API回归通过

**登记时 Git HEAD**：53b3b953d0f491019429da3535e182d7e56a804d

**结果与限制**：同第二阶段布局，QPS 29,069.05 → 35,548.84（+22.29%），端到端p99 88.125 → 93.519 ms（+6.12%）。稀疏样本返回路径均值 33.215 → 1.742 ms，返回路径自身p99 60.782 → 14.659 ms。 返回路径局部修复通过，整体尾延迟与入口等待仍开放；第一阶段负面结果保留。

**报告、图表与证据**

- test/head_return_20261105/REPORT.md
- test/head_return_20261105/comparison.png
- test/head_return_20261105/stage_means.png
- test/head_return_20261105/cpu_distribution.png
- test/head_return_20261105/api_response.png
- test/head_return_20261105/final_p50.png
- test/head_return_20261105/final_p99.png
- test/head_return_20261105/final_comparison.json
- test/head_return_20261105/head_comparison.csv
- test/head_return_20261105/best_settings.yaml
- test/head_return_20261105/root_regression.log

**下一步建议**：使用统一修复配置完成五档预算扫描；入口网络接纳与HNSW排队继续定位，未宣布全系统瓶颈为搜索CPU

**日志更新时间**：2026-11-05T17:22:14+08:00
<!-- experiment:head_return_20261105:end -->
