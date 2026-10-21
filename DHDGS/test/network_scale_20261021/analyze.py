"""Offline comparison of joint search/network scaling; never runs in measurement."""
import collections
import gzip
import json
import statistics
import subprocess
from pathlib import Path

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import numpy as np
import yaml

ROOT = Path(__file__).resolve().parent
PROJECT = ROOT.parents[1]
assert (ROOT / 'completed.json').exists()
rows = json.loads((ROOT / 'progress.json').read_text())
nominees = json.loads((ROOT / 'confirmation_candidates.json').read_text())


def distribution(values):
    return dict(count=len(values), mean=float(np.mean(values)), p99=float(np.percentile(values, 99))) if values else dict(count=0, mean=None, p99=None)


def timings(row):
    case = ROOT / 'cases' / row['name']
    result = json.loads((case / 'result.json').read_text())
    run = Path(result['directory'])
    requests = set()
    head = collections.defaultdict(list)
    with gzip.open(run / 'queries.jsonl.gz', 'rt') as stream:
        for line in stream:
            q = json.loads(line)
            requests.add(q['request_id'])
            t = q['stats']['native_head_timestamps']
            for name, start, end in [
                ('route_queue', 'search_enter_ns', 'route_started_ns'),
                ('routing', 'route_started_ns', 'routing_done_ns'),
                ('entry_dispatch', 'routing_done_ns', 'entry_send_done_ns'),
                ('data_rpc_control', 'entry_send_done_ns', 'last_report_received_ns'),
                ('completion_gating', 'last_report_received_ns', 'merge_enqueued_ns'),
                ('merge_queue', 'merge_enqueued_ns', 'merge_started_ns'),
                ('merge', 'merge_started_ns', 'native_ready_ns')]:
                head[name].append((t[end] - t[start]) / 1e6)
    with gzip.open(run / 'nodes_after.json.gz', 'rt') as stream:
        nodes = json.load(stream)
    all_phases = collections.defaultdict(list)
    per_node = {}
    for node_id, node in nodes.items():
        life = node['peer_lifecycle']
        phases = collections.defaultdict(list)
        for t in life['execution_timestamps']:
            if t['query_request_id'] not in requests:
                continue
            for name, start, end in [
                ('receive_queue_validation', 'receiver_message_ready_ns', 'accepted_ns'),
                ('admission_registration', 'accepted_ns', 'enqueued_ns'),
                ('admission_state_lock_wait', 'accept_state_lock_enter_ns', 'accept_state_lock_acquired_ns'),
                ('compute_queue_setup', 'enqueued_ns', 'native_search_start_ns'),
                ('search_loop', 'native_search_start_ns', 'native_loop_done_ns'),
                ('report_build_worker_release', 'native_loop_done_ns', 'compute_released_ns'),
                ('completion_command_queue', 'local_done_enqueued_ns', 'local_done_dequeued_ns'),
                ('completion_to_report', 'local_done_dequeued_ns', 'report_ready_ns')]:
                if start in t and end in t:
                    value = (t[end] - t[start]) / 1e6
                    assert value >= 0, (row['name'], name, value)
                    phases[name].append(value)
            if 'native_register_state_wait_ns' in t:
                phases['native_registration_state_lock_wait'].append(t['native_register_state_wait_ns'] / 1e6)
        events = {}
        for event in life['control_events']:
            if event['query_request_id'] not in requests or event['kind'] != 'result' or not event['succeeded']:
                continue
            key = event['query_request_id'], event['branch_id']
            if key not in events or event['sender_rpc_return_ns'] < events[key]['sender_rpc_return_ns']:
                events[key] = event
        for event in events.values():
            phases['result_outbound_queue'].append(event['rpc_counts']['pool_wait_ms'])
        per_node[node_id] = {name: distribution(values) for name, values in phases.items()}
        for name, values in phases.items():
            all_phases[name].extend(values)
    out = dict(head={k: distribution(v) for k, v in head.items()},
               branch={k: distribution(v) for k, v in all_phases.items()}, nodes=per_node)
    (case / 'timings.json').write_text(json.dumps(out, indent=2))
    return out


groups = []
for nominee in nominees:
    selected = [r for r in rows if r['stage'] == 'confirm' and r['name'].endswith('_' + nominee['label'])]
    assert len(selected) == 3
    groups.append(dict(**nominee, cases=[r['name'] for r in selected],
                       median_qps=statistics.median(r['qps'] for r in selected),
                       min_qps=min(r['qps'] for r in selected), max_qps=max(r['qps'] for r in selected),
                       median_run_p99_ms=statistics.median(r['p99'] for r in selected),
                       recall_range=[min(r['recall'] for r in selected), max(r['recall'] for r in selected)]))
by_label = {g['label']: g for g in groups}
gain = by_label['expanded']['median_qps'] / by_label['matched_net4']['median_qps'] - 1
out = dict(groups=groups, expanded_vs_matched_qps_change=gain,
           cases=len(rows), queries=sum(r['queries'] for r in rows))
(ROOT / 'comparison.json').write_text(json.dumps(out, indent=2))
phase_results = {}
for row in rows:
    if row['stage'] == 'confirm':
        print('Analyze phases:', row['name'], flush=True)
        phase_results[row['name']] = timings(row)

fig, axes = plt.subplots(1, 2, figsize=(12, 4), layout='constrained')
for ax, key, label in zip(axes, ['qps', 'p99'], ['QPS', 'p99 / ms']):
    for search in (32, 64, 128):
        selected = sorted([r for r in rows if r['stage'] == 'screen' and r['layout']['search'] == search], key=lambda r: r['layout']['data_rx'])
        ax.plot([r['layout']['data_rx'] for r in selected], [r[key] for r in selected], marker='o', label=f'{search} search workers')
    ax.set_xticks([4, 8, 16]); ax.set_xlabel('TX / RX reactors on head and each data node')
    ax.set_ylabel(label); ax.grid(alpha=.25); ax.legend()
fig.suptitle('Screening: fixed concurrency256, budget200, total connections128 per client/endpoint')
fig.savefig(ROOT / 'screening.png', dpi=160); plt.close(fig)
fig, axes = plt.subplots(1, 2, figsize=(11, 4), layout='constrained')
for ax, key, title in zip(axes, ['qps', 'p99'], ['QPS', 'Per-run p99 / ms']):
    for i, group in enumerate(groups):
        values = [r[key] for r in rows if r['name'] in group['cases']]
        ax.scatter([i] * 3, values)
        ax.plot([i - .2, i + .2], [statistics.median(values)] * 2, color='black')
    ax.set_xticks(range(len(groups)), [g['label'] for g in groups]); ax.set_ylabel(title); ax.grid(alpha=.25)
fig.savefig(ROOT / 'confirmation.png', dpi=160); plt.close(fig)

representative = min([r for r in rows if r['name'] in by_label['expanded']['cases']],
                     key=lambda r: abs(r['qps'] - by_label['expanded']['median_qps']))
subprocess.run([str(PROJECT / '.venv/bin/python'), str(ROOT / 'plot_p99.py'), '--case',
                str(ROOT / 'cases' / representative['name']), '--output', str(ROOT / 'p99'), '--cross-node'], check=True)

lines = ['# 搜索与网络线程同步扩容实验', '',
         f"完成{len(rows)}轮、{sum(r['queries'] for r in rows)}次正式查询。应用源码4093380及各机编译扩展与上轮相同，核验见verification.json。", '',
         'Intel-35顶层+数据0、Intel-32数据1、Intel-30数据2；全量seq_2M、2560维、top10、预算200、并发256、预热200次。相同2000条验证查询重复，主要命中页缓存。短测每组10000请求；选出搜索>32且网络>4的最好短测组合，与相同搜索线程的网络4组合、上轮最高中位QPS配置各重复3轮，每轮20000请求。', '',
         '网络数量N表示顶层和数据节点各逻辑发送客户端有N个发送事件循环、接收服务器有N个接收循环。每接收循环另有4个请求/报告handler和1个管理handler；不能将N理解为进程全部网路线程数。所有循环固定min=max，连接总上限固定128/逻辑客户端/目标，接收handler总队列额度4096，路由16、合并4。网络线程数与handler数同步增加，无法单独归因其中一个。', '',
         '## 三轮确认', '',
         '|配置|搜索|数据TX/RX|顶层TX/RX|QPS中位数|QPS范围|各轮p99中位数ms|',
         '|---|---:|---|---|---:|---|---:|']
for g in groups:
    l = g['layout']
    lines.append(f"|{g['label']}|{l['search']}|{l['data_tx']}/{l['data_rx']}|{l['head_tx']}/{l['head_rx']}|{g['median_qps']:.2f}|{g['min_qps']:.2f}–{g['max_qps']:.2f}|{g['median_run_p99_ms']:.2f}|")
lines += ['', f'相同搜索线程下，扩大的网络布局相对网络4的QPS中位数变化为{gain*100:+.2f}%。三轮范围与p99应同时比较，不能用单轮最高点证明稳定收益。', '',
          '![三轮确认](confirmation.png)', '', '## 短测矩阵与完整记录', '',
          '![搜索×网络扫描](screening.png)', '',
          '|轮次|搜索|数据TX/RX|顶层TX/RX|QPS|p99 ms|Recall@10|网络计数完整|',
          '|---|---:|---|---|---:|---:|---:|---|']
for r in rows:
    l = r['layout']
    lines.append(f"|{r['name']}|{l['search']}|{l['data_tx']}/{l['data_rx']}|{l['head_tx']}/{l['head_rx']}|{r['qps']:.2f}|{r['p99']:.2f}|{r['recall']:.5f}|{r['metrics_complete']}|")
lines += ['', '## 确认轮次各阶段耗时', '',
          '以下为每分支平均墙钟ms，分支跨节点/并行且控制路径与搜索可重叠，不可直接相加成查询延迟。状态锁等待是接纳时间的子区间；接收排队/校验未进一步分离。完整每节点均值、p99与样本数见各case/timings.json。', '',
          '|轮次|接收排队/校验|接纳注册|其中接纳状态锁等待|计算就绪|搜索循环|完成命令队列|完成至报告发布|结果发送队列|',
          '|---|---:|---:|---:|---:|---:|---:|---:|---:|']
for row in rows:
    if row['stage'] != 'confirm': continue
    m = phase_results[row['name']]['branch']
    keys = ['receive_queue_validation', 'admission_registration', 'admission_state_lock_wait', 'compute_queue_setup',
            'search_loop', 'completion_command_queue', 'completion_to_report', 'result_outbound_queue']
    lines.append('|' + row['name'] + '|' + '|'.join(f"{m[k]['mean']:.3f}" if m[k]['mean'] is not None else '缺失' for k in keys) + '|')
lines += ['', '## 实际线程与其他指标', '',
          '|轮次|数据进程线程0/1/2|顶层线程|数据平均CPU核0/1/2|协议字节/query|RPC/query|逻辑读取字节/query|物理读字节/快照窗口|',
          '|---|---|---:|---|---:|---:|---:|---:|']
for row in rows:
    if row['stage'] != 'confirm': continue
    d = json.loads((ROOT / 'cases' / row['name'] / 'result.json').read_text())
    nodes = d['node_process_load']; head = d['head_process_load'][0]; m = d['query_metric_distributions']
    lines.append('|' + row['name'] + '|' + '/'.join(str(n['process']['threads']) for n in nodes) +
                 f"|{head['process']['threads']}|" + '/'.join(f"{n['mean_cpu_cores']:.2f}" for n in nodes) +
                 f"|{m['all_protocol_bytes']['mean']:.0f}|{m['all_protocol_rpc_count']['mean']:.2f}|{m['file_read_bytes']['mean']:.0f}|" +
                 str(sum(n['process_deltas']['storage_read_bytes'] for n in nodes)) + '|')
lines += ['', 'CPU与物理读量来自进程快照窗口，不能等同精确查询测量窗口；逻辑读取不等于SSD物理读取。完整精确距离、RaBitQ、展开数、深度、CPU、阶段均值、逐query延迟与召回保留于result.json及runs。服务延迟不含等待并发额度的客户端backlog，后者另存。', '',
          '## 跨节点p99附近真实查询', '',
          f"选择expanded组QPS最接近三轮中位数的{representative['name']}，在跨节点请求中选最接近本轮全体p99的样本。各主机时钟独立，不直接相减。", '',
          '![p99时序](p99/timeline.png)', '',
          '## 限制与复现', '',
          '固定并发256检验的是同负载下网络线程扩容。没有同时提高连接额度或并发，不能排除更高到达率下需要更多网络资源。顶层与数据网络同时调整，短测矩阵不能单独归因某一侧；并未修改接纳锁或GIL路径。短测和确认轮次仍只持续数秒，不能替代长时间稳态实验或冷SSD实验。所有请求与慢轮次保留，网络计数不完整标记单列。', '',
          '入口run_sweep.py，参数sweep.yaml，结果progress.json/comparison.json，各轮有效配置保留在case目录。未自动改变生产默认值。', '']
(ROOT / 'REPORT.md').write_text('\n'.join(lines))
print(json.dumps(out, indent=2), flush=True)
