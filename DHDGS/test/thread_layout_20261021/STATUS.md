# 线程布局实验已完成

源码已上传GitHub。40个早于2026-10-20的旧测试目录已清理，释放12.84GiB，在用索引与配置已保留。

45轮、810000次正式查询全部成功；最高QPS中位数5972.80，基线5592.52（+6.80%），各轮p99中位数116.03/99.51ms。存在重复波动，不宣称统计显著或全局最优。

详见[REPORT.md](REPORT.md)，线程比例及配置见best_layout.yaml、best_data_plan.yaml、best_head_plan.yaml。p99真实跨节点查询时序图见p99/overall/timeline.png。三台机器实验端口已关闭，原220xx服务未重启。
