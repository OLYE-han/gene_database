# 实验已完成

19轮280000请求全部成功，Recall约97.03%。同为64搜索线程，网络4→16，三轮QPS中位数6300.61→6688.88（+6.16%），各轮p99中位数111.42→126.27ms。收益存在波动，接纳锁等待上升；未更改生产默认值。

详见REPORT.md、FINDINGS.md，p99图见p99/timeline.png。参数与复现入口为sweep.yaml、run_sweep.py；离线分析依次运行analyze.py和finalize_report.py。三机实验端口已关闭。
