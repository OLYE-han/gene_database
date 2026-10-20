"""Audit one isolated request and export its complete lifecycle."""
from pathlib import Path
import csv
import gzip
import hashlib
import json
import yaml

R = Path(__file__).resolve().parent
P = R.parents[1]
run = sorted((R / 'isolated/runs').iterdir())[-1]
out = R / 'analysis/isolated' / run.name
out.mkdir(parents=True, exist_ok=True)
with gzip.open(run / 'report.json.gz', 'rt') as f:
    report = json.load(f)
c = yaml.safe_load((run / 'resolved.yaml').read_text())
assert c['experiment']['warmup_queries'] == 0
assert report['query_count'] == report['successful_queries'] == report['actual_peak_inflight'] == 1
assert report['failed_queries'] == 0
q, = report['queries']
assert q['query_id'] == 734744 and q['status'] == 'ok'
assert q['stats']['metrics_complete'] and q['stats']['expanded'] <= 200
assert q['stats']['node_revisit_count'] == q['stats']['router_expand_rpc_count'] == 0
assert q['recall'] == len(set(q['ids']) & set(q['truth_ids'])) / 10
assert len({b['node_id'] for b in q['branch_reports']}) == len(q['branch_reports'])
for b in q['branch_reports']:
    assert b['compute_thread_migrations'] == 0
    assert b['work_grant'] == b['work_spent'] + b['work_unused'] + sum(x['work_grant'] for x in b['children'])
    for child in b['children']:
        assert sum(d['remote'] and d['decision_id'] == child['decision_id'] and d['selected_node'] == child['node_id'] for d in b['decisions']) == 1
startup, control = [], []
for n in report['node_process_load']:
    life = n['peer_lifecycle_after']
    assert life['admission_pending_start'] == life['admission_queue_depth'] == life['unacknowledged'] == 0
    assert life['admission_capacity_rejections'] == life['control_totals']['errors'] == 0
    for queue in [life['compute_queue'], *life['send_queues'].values()]:
        assert queue['queued'] == queue['active'] == queue['capacity_rejections'] == 0
    assert life['compute_queue']['callback_errors'] == 0
    startup.extend(t for t in life['execution_timestamps'] if t['query_request_id'] == q['request_id'])
    control.extend(e for e in life['control_events'] if e['query_request_id'] == q['request_id'])
startup.sort(key=lambda t: t['accepted_ns'])
assert len(startup) == len(q['branch_reports'])
for t in startup:
    chain = [t[k] for k in ('accepted_ns', 'enqueued_ns', 'dequeued_ns', 'executor_enqueue_ns', 'executor_dequeue_ns', 'worker_enter_ns', 'native_search_start_ns', 'native_loop_done_ns', 'native_return_ns', 'worker_available_ns')]
    assert chain == sorted(chain)
    b = next(b for b in q['branch_reports'] if b['branch_id'] == t['branch_id'])
    assert t['worker_thread_id'] == b['compute_thread_id']
services = json.loads((run / 'services.json').read_text())
clients = json.loads((run / 'clients_lifecycle.json').read_text())
assert services['test_completed'] and all(s['stopped'] for s in services['services'])
assert len(clients) == 1 and clients[0]['stopped'] and clients[0]['exit_code'] == 0
manifest = json.loads((R / 'sources.json').read_text())
assert all(hashlib.sha256((P / name).read_bytes()).hexdigest() == digest for name, digest in manifest.items())

def save(name, data):
    (out / name).write_text(json.dumps(data, indent=2, ensure_ascii=False))

save('selected_query.json', q)
save('selected_events.json', dict(startup=startup, control=control))
save('verification.json', dict(queries=1, branches=len(startup), all_successful=True,
    fixed_thread_budget_approved_dispatch_no_revisit=True, services_stopped=True,
    production_source_files_verified=len(manifest), warmup_queries=0))

stages = [
    ('接纳队列', 'enqueued_ns', 'dequeued_ns'),
    ('固定计算队列', 'executor_enqueue_ns', 'executor_dequeue_ns'),
    ('工作线程入口→C++循环开始', 'worker_enter_ns', 'native_search_start_ns'),
    ('C++搜索循环', 'native_search_start_ns', 'native_loop_done_ns'),
    ('循环结束→线程可复用', 'native_loop_done_ns', 'worker_available_ns'),
    ('完成事件等待消费', 'local_done_enqueued_ns', 'local_done_dequeued_ns'),
    ('完成消费→报告就绪', 'local_done_consumed_ns', 'report_ready_ns'),
]
rows = []
for label, left, right in stages:
    rows.append(dict(stage=label, **{f'node_{t["node_id"]}_ms': (t[right]-t[left])/1e6 for t in startup}))
rows.append(dict(stage='报告就绪→发送线程开始', **{f'node_{t["node_id"]}_ms': max(0, min(e['sender_enter_ns'] for e in control if e['kind']=='result' and e['branch_id']==t['branch_id'])-t['report_ready_ns'])/1e6 for t in startup}))
with (out / 'branch_stages.csv').open('w') as f:
    w = csv.DictWriter(f, list(rows[0])); w.writeheader(); w.writerows(rows)

rel = out.relative_to(R)
def table(headers, data):
    return ['| '+' | '.join(headers)+' |', '|'+'|'.join(['---']*len(headers))+'|'] + ['| '+' | '.join(map(str, row))+' |' for row in data]

lines = ['# 无并发单查询时间分解', '',
    f'2026-10-18，运行 `{run.name}`。只提交1条查询，实际峰值在途1，**端到端 {q["client_wall_ms"]:.3f} ms，Recall@10 {q["recall"]:.0%}**。', '',
    '使用此前固定随机选择的查询734744（query row 73656）。完整seq_2M索引、Intel-35本机三个TCP数据进程；每节点32计算线程、搜索预算200及算法配置保持上一轮fastoff不变。只有一个客户端进程、一个查询工作线程；节点仍允许该查询按扩展函数生成跨节点分支。', '',
    '**没有预热查询，整个测试仅执行这一个查询。** 服务和索引装载在计时前完成；查询自身包含首次入口准备、TCP建连和工作线程进入原生调用之前的准备。未清空OS页缓存，三个节点物理storage_read_bytes增量均为0，故不是冷SSD测试。', '',
    f'![完整时序图]({rel}/query_timeline.png)', '',
    '上图不同轨道会重叠；入口“等待分支结果”覆盖下方正在进行的搜索与通信，不能再计作额外纯等待。下图按端到端事件边界划分，可相加但包含工作与等待。黑色菱形表示计算线程可领取下一任务。', '',
    '## 分支阶段', '']
columns = [f'node_{t["node_id"]}_ms' for t in startup]
lines += table(['阶段', *[f'节点{t["node_id"]} / ms' for t in startup]],
               [[x['stage'], *[format(x[key], '.3f') for key in columns]] for x in rows])
lines += ['', '以上不是互斥端到端拆分，不应全部相加。工作线程入口→C++循环开始包含Python适配、可能的首次导入、准备和线程调度；其中大部分发生在native_call之前，现有时间戳不足以逐项归因，不能称为计算队列等待或SSD耗时。', '',
    f'[原生搜索细分图]({rel}/query_search_detail.png) · [全部轨道事件CSV]({rel}/query_timeline.csv) · [阶段CSV]({rel}/branch_stages.csv)', '',
    '## 与2000查询突发对照', '',
    '相同查询在上一轮2000查询突发中总延迟1507.85ms，节点2/1的接纳队列等待分别262.54/592.42ms；本次入队后等待降至0.11/0.34ms（若从accepted计至出队，则为0.13/0.36ms）。该差异说明旧等待主要随并发积压出现，单次向量搜索本身没有固定的几百毫秒等待。它不意味着高并发等待问题已经解决，也不能由一个样本估计p99或稳态吞吐。', '',
    '上一轮另有全体分支时间戳佐证：三个节点在相邻提交之间平均间隔1.269/1.276/1.721ms，而submit调用平均仅0.032/0.064/0.026ms。间隔可能包含等待新任务、共享队列锁竞争、GIL与OS调度，不能全归于某一种锁；计算线程池内部平均队列等待仅4.29ms。长队列主要位于提交计算线程池之前。', '',
    f'本次展开{q["stats"]["expanded"]}个顶点，访问{len(startup)}个数据节点。跨数据节点派发{q["stats"]["peer_branch_rpc_count"]}次，全部协议RPC共{q["stats"]["all_protocol_rpc_count"]}次、{q["stats"]["all_protocol_bytes"]}字节；逻辑文件读取{int(q["stats"]["file_read_bytes"])}字节。协议字节不含TCP/IP等底层开销。', '',
    '## 记录与复现', '',
    '[实验YAML](isolated/experiment.yaml) · [负载YAML](isolated/load_plan.yaml) · [执行日志](execution.log) · [源文件哈希](sources.json)', '',
    '运行 `.venv/bin/python test/single_query_20261018/isolated/run_load.py`（DHDGS目录下），然后依次运行本目录analyze.py、plot_query.py isolated、plot_search.py isolated。端口20361–20363须空闲。脚本不会修改生产搜索代码，所有运行写入独立runs目录。', '',
    '已核验真值、全局预算、固定计算线程、扩展批准、无节点重复访问、队列排空和进程退出。单查询原始summary中的QPS包含实验启动闸门等待，不作为吞吐结论；单样本p99也不具有分布意义。', '']
(R / 'REPORT.md').write_text('\n'.join(lines))
print(R / 'REPORT.md')
