# p99真实查询时序分解

来源：04_c128_p1024，10000条查询。选择全体查询中服务延迟最接近本轮p99的记录；不筛选跨节点条件，也不使用多轮p99均值选择样本。

本轮p99为129.574548940ms；query_id=382519、job=362，延迟129.573891000ms，相差0.000657940ms，Recall@10=1.00。

![时序图](timeline.png)

顶层面板使用同一主机时钟，阶段互斥且总和等于端到端服务延迟。每个数据节点单独以收到完整报文为零点，不能跨主机直接对齐时间轴或相加。节点内计算、控制、派发存在重叠；虚线表示计算线程释放时刻，之后的控制/网络等待不占用该计算线程。结果ACK时刻与用户Future完成时刻也不同。

|顶层阶段|耗时ms|占服务延迟|
|---|---:|---:|
|API submission|0.035963|0.03%|
|Route queue|0.017424|0.01%|
|HNSW routing|0.818377|0.63%|
|Entry dispatch incl. queue|0.065020|0.05%|
|Data / RPC / control|122.472379|94.52%|
|Report validation / completion gating|5.350482|4.13%|
|Merge queue|0.070467|0.05%|
|Merge|0.322557|0.25%|
|API result handoff|0.421222|0.33%|

## Intel-30 / PARENT

|阶段|本节点相对起点ms|耗时ms|
|---|---:|---:|
|Receive queue / validation|0.000000|18.304732|
|Admission + registration|18.304732|0.145768|
|Compute queue + setup|18.450500|0.357532|
|Search loop|18.808032|7.805833|
|Report build / worker release|26.613865|0.158579|
|Completion command wait|26.741954|2.787942|
|Finalize incl. pending transfer|29.529896|87.018340|
|Schedule + encode / submit|116.548236|0.123827|
|Result outbound queue / event loop|116.672063|0.018498|
|Result send incl. connect|116.690561|0.018754|
|Result response / ACK wait|116.709315|6.626221|
|Cross-node submit to ACK callback|21.723429|94.481430|

## Intel-32 / CHILD

|阶段|本节点相对起点ms|耗时ms|
|---|---:|---:|
|Receive queue / validation|0.000000|0.212877|
|Admission + registration|0.212877|0.058998|
|Compute queue + setup|0.271875|0.191285|
|Search loop|0.463160|6.428877|
|Report build / worker release|6.892037|0.204863|
|Completion command wait|7.032602|3.628413|
|Finalize incl. pending transfer|10.661015|0.009405|
|Schedule + encode / submit|10.670420|0.216332|
|Result outbound queue / event loop|10.886752|0.016937|
|Result send incl. connect|10.903689|0.012171|
|Result response / ACK wait|10.915860|5.414407|

结果RPC排队起点由 sender_enter_ns - round(pool_wait_ms * 1e6) 还原；pool_wait_ms来自底层同一主机时钟的queued_ns与send_start_ns之差。旧message_ready_ns是在异步提交成功后记录，网络线程可能已开始发送，不能用它直接计算发送前队列。还原数值保存在stages.json/CSV，未改写原始时间戳。

接收队列/验证、完成控制和发送前等待均为观测区间，可能包含OS调度及状态锁等待，不能据图将其全部认定为锁、CPU计算或纯链路传输。单条p99附近查询用于定位现象，不代表所有尾延迟样本。selected.json保留完整源记录，stages.json/CSV保存精确阶段数值。
