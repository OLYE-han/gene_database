"""Offline comparison of total data receive / admission workers at 4/4, 8/8, 16/16."""
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
phase_results = {}
for row in rows:
    print('Analyze phases:', row['name'], flush=True)
    phase_results[row['name']] = timings(row)
for nominee in nominees:
    selected = [r for r in rows if r['name'].endswith('_' + nominee['label'])]
    assert len(selected) == 3
    groups.append(dict(**nominee, cases=[r['name'] for r in selected],
                       median_qps=statistics.median(r['qps'] for r in selected),
                       min_qps=min(r['qps'] for r in selected), max_qps=max(r['qps'] for r in selected),
                       aggregate_qps=sum(r['queries'] for r in selected) / sum(r['wall_seconds'] for r in selected),
                       median_run_p99_ms=statistics.median(r['p99'] for r in selected),
                       min_run_p99_ms=min(r['p99'] for r in selected), max_run_p99_ms=max(r['p99'] for r in selected),
                       recall_range=[min(r['recall'] for r in selected), max(r['recall'] for r in selected)]))
best_qps = max(groups, key=lambda g: g['median_qps'])
best_p99 = min(groups, key=lambda g: g['median_run_p99_ms'])
baseline = next(g for g in groups if g['layout']['data_rx'] == 4)
phase_keys = ['receive_queue_validation', 'admission_registration', 'admission_state_lock_wait',
              'compute_queue_setup', 'search_loop', 'report_build_worker_release',
              'completion_command_queue', 'completion_to_report', 'result_outbound_queue']
phase_comparison = {}
for group in groups:
    phase_comparison[group['label']] = {
        k: dict(median_of_branch_means_ms=statistics.median(phase_results[name]['branch'][k]['mean'] for name in group['cases']),
                median_of_branch_p99_ms=statistics.median(phase_results[name]['branch'][k]['p99'] for name in group['cases']))
        for k in phase_keys}
out = dict(groups=groups, best_qps=best_qps['label'], best_p99=best_p99['label'],
           cases=len(rows), queries=sum(r['queries'] for r in rows),
           best_qps_change_from_4=best_qps['median_qps'] / baseline['median_qps'] - 1,
           best_p99_change_from_4=best_p99['median_run_p99_ms'] / baseline['median_run_p99_ms'] - 1)
(ROOT / 'comparison.json').write_text(json.dumps(out, indent=2))
(ROOT / 'phase_comparison.json').write_text(json.dumps(phase_comparison, indent=2))

fig, axes = plt.subplots(1, 2, figsize=(11, 4), layout='constrained')
for ax, key, label in zip(axes, ['qps', 'p99'], ['QPS', 'Per-run p99 / ms']):
    for i, group in enumerate(groups):
        values = [r[key] for r in rows if r['name'] in group['cases']]
        ax.scatter([i] * 3, values, s=45)
        ax.plot([i - .2, i + .2], [statistics.median(values)] * 2, color='black')
    ax.set_xticks(range(3), ['4 receive + 4 handlers', '8 + 8', '16 + 16'])
    ax.set_ylabel(label); ax.grid(alpha=.25)
fig.suptitle('64 search workers per node | concurrency256 | budget200 | 3 x 20000 requests per layout')
fig.savefig(ROOT / 'comparison.png', dpi=170); plt.close(fig)

labels = {'receive_queue_validation': '接收排队及校验', 'admission_registration': '接纳与注册',
          'admission_state_lock_wait': '其中接纳状态锁等待', 'compute_queue_setup': '计算队列及设置',
          'search_loop': '搜索循环', 'report_build_worker_release': '报告构造及搜索线程释放',
          'completion_command_queue': '完成命令队列', 'completion_to_report': '完成至报告发布',
          'result_outbound_queue': '结果发送队列'}
representatives = {}
for group in groups:
    row = min([r for r in rows if r['name'] in group['cases']], key=lambda r: abs(r['qps'] - group['median_qps']))
    representatives[group['label']] = row['name']
    subprocess.run([str(PROJECT / '.venv/bin/python'), str(ROOT / 'plot_p99.py'),
                    '--case', str(ROOT / 'cases' / row['name']),
                    '--output', str(ROOT / 'p99' / group['label']), '--cross-node'], check=True)
for objective, group in [('qps', best_qps), ('p99', best_p99)]:
    case = ROOT / 'cases' / representatives[group['label']]
    data_plan = yaml.safe_load((case / 'plan.yaml').read_text())
    head_plan = {**data_plan, 'dhdgs': yaml.safe_load((case / 'head_effective.yaml').read_text())}
    for suffix, value in [('layout', group['layout']), ('data_plan', data_plan), ('head_plan', head_plan)]:
        (ROOT / f'best_{objective}_{suffix}.yaml').write_text(yaml.safe_dump(value, sort_keys=False))

lines = ['# 接收线程与接纳线程一对一配置对比', '',
         f"共{len(rows)}轮、{sum(r['queries'] for r in rows)}次正式查询。用户确认的含义为每数据节点总接收循环/总查询请求handler数量：4/4、8/8、16/16。每个接收循环仅配1个查询handler。", '',
         '## 实验条件与计数口径', '',
         'Intel-35顶层+数据0，Intel-32数据1，Intel-30数据2；全量seq_2M索引、2560维、top10、预算200、并发256，每数据节点64搜索线程。每轮预热200次，再运行20000请求，每组3轮。三轮顺序分别4→8→16、8→16→4、16→4→8。相同2000条查询重复，热缓存为主，应用源码4093380及各机编译扩展哈希固定。', '',
         '本轮只改变数据节点接收循环数量，handler固定每循环1。数据派发与报告客户端各4个发送循环；顶层固定路由16、合并4、接收循环4、每循环4个报告handler、入口/租约客户端各4个发送循环。TCP连接总额度固定128/逻辑发送客户端/目标，接收handler总队列容量4096、最大接收连接4096。连接额度不是接收线程数，也不代表实际总有128条活跃连接。', '',
         '“接纳线程”指native server的查询请求handler，承担请求解码、操作分派和接纳等，仍共享原有Python GIL和查询状态锁；没有将其改造成独立无锁接纳器。每个数据接收循环还各有1个管理handler，另计，不属于查询接纳线程。发送、控制、管理与监测等线程存在，因此4+4不是进程全部线程数。', '',
         '## 三轮结果', '',
         '|接收+接纳|QPS中位数|QPS范围|各轮p99中位数ms|p99范围ms|合并请求/时长QPS|',
         '|---|---:|---|---:|---|---:|']
for g in groups:
    n = g['layout']['data_rx']
    lines.append(f"|{n}+{n}|{g['median_qps']:.2f}|{g['min_qps']:.2f}–{g['max_qps']:.2f}|{g['median_run_p99_ms']:.2f}|{g['min_run_p99_ms']:.2f}–{g['max_run_p99_ms']:.2f}|{g['aggregate_qps']:.2f}|")
lines += ['', f"最高QPS中位数为{best_qps['label']}，最低各轮p99中位数为{best_p99['label']}。三轮存在波动，不将测得的最高值称为全局最优或统计显著优势。各轮p99中位数不是汇总全部请求后的p99。", '',
          '![三轮对比](comparison.png)', '', '## 阶段均值对比', '',
          '单位ms，为各轮每分支均值再取中位数。锁等待是接纳注册的子区间；分支与控制路径可能重叠，不能相加成每query延迟。各节点只在自己的单调时钟内计算时差。', '',
          '|阶段|4+4|8+8|16+16|', '|---|---:|---:|---:|']
for key in phase_keys:
    values = [phase_comparison[g['label']][key]['median_of_branch_means_ms'] for g in groups]
    lines.append('|' + labels[key] + '|' + '|'.join(f'{v:.3f}' for v in values) + '|')
lines += ['', '每节点完整阶段均值、p99与样本数保留于各case/timings.json，阶段组间比较另见phase_comparison.json。', '',
          '## 逐轮性能与资源', '',
          '|轮次|QPS|p99 ms|Recall@10|数据进程线程0/1/2|顶层线程|网络计数完整|',
          '|---|---:|---:|---:|---|---:|---|']
for row in rows:
    d = json.loads((ROOT / 'cases' / row['name'] / 'result.json').read_text())
    threads = '/'.join(str(n['process']['threads']) for n in d['node_process_load'])
    lines.append(f"|{row['name']}|{row['qps']:.2f}|{row['p99']:.2f}|{row['recall']:.5f}|{threads}|{d['head_process_load'][0]['process']['threads']}|{row['metrics_complete']}|")
lines += ['', '|轮次|平均CPU核0/1/2|协议字节/query|RPC/query|pread ms/query|精确距离 ms/query|RaBitQ ms/query|展开顶点数/query|跨节点深度|物理读取字节/快照窗口|',
          '|---|---|---:|---:|---:|---:|---:|---:|---:|---:|']
for row in rows:
    d = json.loads((ROOT / 'cases' / row['name'] / 'result.json').read_text())
    nodes = d['node_process_load']; m = d['query_metric_distributions']
    values = [m[k]['mean'] for k in ['all_protocol_bytes', 'all_protocol_rpc_count', 'pread_ms', 'exact_distance_ms', 'rabitq_ms', 'expanded', 'max_cross_shard_depth']]
    lines.append('|' + row['name'] + '|' + '/'.join(f"{n['mean_cpu_cores']:.2f}" for n in nodes) + '|' +
                 '|'.join(f'{v:.3f}' for v in values) + '|' + str(sum(n['process_deltas']['storage_read_bytes'] for n in nodes)) + '|')
lines += ['', 'CPU与物理读量来自进程快照窗口，包含测量窗口以外的快照开销；pread含页缓存命中，不等于物理SSD读取时间。其余逐query统计、跳数/深度、逻辑读量与线程CPU等保留在result.json及runs中。服务延迟不包含等待并发额度的客户端backlog，后者单独记录。', '',
          '## 各组跨节点p99附近样本', '',
          '各组先选QPS最接近本组三轮中位数的一轮，再在跨节点请求中选最接近该轮全体p99的查询；这不是跨节点子集的p99。数据节点独立时钟面板不能直接对齐或相加。']
for group in groups:
    label = group['label']
    lines += ['', f"{label}：{representatives[label]}", '', f'![{label}](p99/{label}/timeline.png)']
lines += ['', '## 复现与限制', '',
          '参数集中在sweep.yaml；run_sweep.py顺序执行三机测试，analyze.py在全部测量结束后处理记录。每轮完整保留结果，不删除慢查询或慢轮次；网络计数完整性单独标记。结果先保存在内存，测量后持久化；原始记录压缩前后进行SHA256校验。', '',
          '每组仅三轮，每轮数秒，不能替代长时间稳态负载、更多独立查询或冷SSD实验。管理线程也随接收循环数增加，这是当前实现结构的组成部分。没有改变锁或GIL路径，不能用线程数量变化单独证明任何一把锁的因果作用。', '',
          '最高吞吐与最低p99配置分别保存为best_qps_*及best_p99_* YAML，生产默认值未修改。', '']
lines.append('\n## 本轮选择及剩余等待\n\n按此前“优先最低p99，再比较QPS”的偏好，本轮保留8+8作为后续验证候选：相对4+4，QPS中位数增加5.42%，各轮p99中位数降低5.03%；它同时优于16+16的本轮中位QPS和p99。但三轮QPS范围重叠，每轮仅数秒，不能称为已经证明的长期稳定最优配置。\n\n4+4→8+8，接收排队及校验的每分支均值中位数14.561→11.114ms，接纳状态锁等待0.527→1.216ms。8+8→16+16，接收等待仅继续降至10.443ms，接纳状态锁等待却升至2.377ms；搜索循环保持约9ms。此结果支持控制接收并发与共享状态竞争之间的平衡，不支持单纯将接收/接纳线程加到最大。锁等待是接纳总时间的子区间，不能重复相加，锁获取墙钟等待也包含线程恢复调度时间。\n\n8+8代表轮次的跨节点p99附近样本为query_id=1082402、job=19051，延迟119.628068ms、Recall@10=1.00。Intel-35父分支在6.401ms释放计算线程；Intel-32子分支接收排队及校验97.773ms、接纳1.026ms、计算队列及设置3.358ms、搜索7.925ms、完成命令队列4.929ms。父跨机提交至ACK耗时99.112ms不代表父计算线程等待99ms；该线程已释放。接收阶段包含handler排队、解码、GIL获取和操作分派等，当前时间点不能将97.773ms全部归为网络传输或某一把锁。\n\n全部180000请求成功，Recall@10约97.03%，各轮网络计数完整性审计通过，内存暂存无溢出。测试端口已在三台机器关闭。best_p99_layout.yaml与best_qps_layout.yaml本轮均为8+8，配套有效计划已保存，生产默认值未修改。\n')
(ROOT / 'REPORT.md').write_text('\n'.join(lines))
print(json.dumps(out, indent=2), flush=True)


