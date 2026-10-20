"""Audit the new experiment and plot measured time without inventing timestamps."""
from pathlib import Path
import csv
import gzip
import hashlib
import json
import math
import os
import random

import yaml

ROOT = Path(__file__).resolve().parent
PROJECT = ROOT.parents[1]
os.environ.setdefault('MPLCONFIGDIR', str(ROOT / '.mplconfig'))
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib.font_manager import FontProperties

font = FontProperties(fname='/usr/share/fonts/truetype/wqy/wqy-zenhei.ttc')
plt.rcParams.update({'font.family': font.get_name(), 'font.weight': 500,
                     'axes.titleweight': 500, 'axes.labelweight': 500, 'axes.unicode_minus': False,
                     'svg.fonttype': 'path', 'font.size': 10})


def save(path, value):
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2, allow_nan=False))


def table(headers, rows):
    return ['| ' + ' | '.join(headers) + ' |',
            '|' + '|'.join(['---'] * len(headers)) + '|'] + [
                '| ' + ' | '.join(map(str, row)) + ' |' for row in rows]


def close(a, b):
    assert math.isclose(a, b, rel_tol=1e-9, abs_tol=1e-6), (a, b)


def csv_rows(path, rows):
    with path.open('w') as f:
        writer = csv.DictWriter(f, list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def main():
    sources = list((ROOT / 'buffered/runs').glob('*/report.json.gz'))
    assert len(sources) == 1, 'Require exactly one completed experiment'
    source = sources[0]
    run = source.parent
    with gzip.open(source, 'rt') as f:
        report = json.load(f)
    for name, digest in json.loads((ROOT / 'sources.json').read_text()).items():
        assert hashlib.sha256((PROJECT / name).read_bytes()).hexdigest() == digest, name
    queries = report['queries']
    selection = json.loads((ROOT / 'buffered/query_selection.json').read_text())
    assert len(queries) == len({q['request_id'] for q in queries}) == 2000
    assert sorted(q['query_row'] for q in queries) == selection['validation']
    assert not set(selection['validation']) & set(selection['calibration_excluded'])
    with gzip.open(run / 'queries.jsonl.gz', 'rt') as f:
        assert [json.loads(line) for line in f] == queries
    for q in queries:
        assert q['status'] == 'ok' and q['stats']['metrics_complete']
        close(q['recall'], len(set(q['ids']) & set(q['truth_ids'])) / 10)
        s = q['stats']
        assert s['expanded'] <= 200 and s['node_revisit_count'] == s['router_expand_rpc_count'] == 0
        close(s['query_ready_queue_ms'], s['query_admission_queue_ms'] + s['query_executor_start_wait_ms'])
        for b in q['branch_reports']:
            assert b['native_search_loop'] and b['compute_thread_migrations'] == 0
            assert b['work_grant'] == b['work_spent'] + b['work_unused'] + sum(x['work_grant'] for x in b['children'])
            for child in b['children']:
                assert sum(d['remote'] and d['decision_id'] == child['decision_id']
                           and d['selected_node'] == child['node_id'] for d in b['decisions']) == 1
    services = json.loads((run / 'services.json').read_text())
    clients = json.loads((run / 'clients_lifecycle.json').read_text())
    assert services['test_completed'] and len(services['services']) == 3
    assert all(s['stopped'] and s['exit_code'] in (0, -15) for s in services['services'])
    assert len(clients) == 20 and all(c['stopped'] and c['exit_code'] == 0 for c in clients)
    for node in report['node_process_load']:
        life = node['peer_lifecycle_after']
        assert life['admission_mode'] == 'buffered'
        assert life['admission_queue_depth'] == life['admission_pending_start'] == life['unacknowledged'] == 0
        assert life['admission_capacity_rejections'] == life['control_totals']['errors'] == 0
        assert life['admission_peak_pending_start'] <= 2048
    save(ROOT / 'verification.json', {
        'requests_audited': 2000, 'all_successful': True, 'source_hashes_verified': True,
        'frozen_query_rows_and_stored_truth_recall_verified': True,
        'budget_single_visit_approved_dispatch_fixed_thread_verified': True,
        'ready_wait_split_verified': True, 'all_test_services_and_clients_stopped': True,
        'pending_at_end': 0, 'admission_rejections': 0, 'control_errors': 0,
    })
    cfg = yaml.safe_load((ROOT / 'analysis.yaml').read_text())
    assert cfg['selection_population'] == 'successful_multi_node_queries_sorted_by_query_row'
    eligible = sorted([q for q in queries if q['status'] == 'ok' and len(q['branch_reports']) > 1],
                      key=lambda q: q['query_row'])
    q = random.Random(cfg['selection_seed']).choice(eligible)
    out = ROOT / 'query_latency'
    out.mkdir(exist_ok=True)
    save(out / 'selected_query.json', q)
    save(out / 'selection.json', {
        **cfg, 'eligible': len(eligible), 'query_id': q['query_id'], 'query_row': q['query_row'],
        'request_id': q['request_id'], 'source': str(source.relative_to(ROOT)),
        'source_sha256': hashlib.sha256(source.read_bytes()).hexdigest(),
    })
    total = q['client_wall_ms']
    origin = q['client_start_ns']
    close(total, (q['client_end_ns'] - origin) / 1e6)
    s = q['stats']
    parts = s['latency_breakdown_ms']
    top = [('路由与入口处理', parts['routing_wall_ms']),
           ('数据执行与控制等待', parts['peer_execution_and_control_wait_ms']),
           ('入口准备、合并及生命周期剩余', parts['head_merge_and_lifecycle_ms']),
           ('API计时边界外侧差额', total - s['latency_ms'])]
    close(sum(v for _, v in top), total)
    assert all(v >= 0 for _, v in top)
    branches = sorted(q['branch_reports'], key=lambda b: len(b['visited_nodes']))
    life_fields = [('query_admission_queue_ms', '接纳至开始提交计算'),
                   ('query_executor_start_wait_ms', '提交至计算线程进入'),
                   ('query_compute_slice_wall_ms', '搜索执行区间'),
                   ('query_transfer_finalize_wait_ms', '本地结束后派发收尾'),
                   ('query_state_other_ms', '其余状态衔接')]
    windows, detailed, raw, domains = [], [], [], set()
    for b in branches:
        st, sid = b['stats'], b['node_id']
        trace = b['local_batch_trace']
        start = min(x['service_trace']['start_ns'] for x in trace)
        end = max(x['service_trace']['end_ns'] for x in trace)
        assert origin <= start <= end <= q['client_end_ns']
        windows.append({'node': sid, 'start_ms': (start - origin) / 1e6, 'end_ms': (end - origin) / 1e6})
        values = dict.fromkeys(('pread', 'exact_distance', 'rabitq'), 0.)
        service = 0.
        for index, batch in enumerate(trace):
            t = batch['service_trace']
            domains.add(t['clock_domain'])
            service += (t['end_ns'] - t['start_ns']) / 1e6
            for name, a, z in t['spans']:
                assert t['start_ns'] <= a <= z <= t['end_ns']
                values[name] += (z - a) / 1e6
                raw.append({'node': sid, 'batch': index, 'kind': name,
                            'start_ms': (a - origin) / 1e6, 'end_ms': (z - origin) / 1e6,
                            'duration_ms': (z - a) / 1e6})
        for k, v in values.items():
            close(v, st[k + '_ms'])
        close(service, st['service_ms'])
        close(sum(st[k] for k, _ in life_fields), st['query_branch_wall_ms'])
        compute = [('pread调用', values['pread']), ('精确距离', values['exact_distance']),
                   ('RaBitQ估距', values['rabitq']), ('批次内其他', service - sum(values.values())),
                   ('C++循环其余区间', st['branch_wall_ms'] - service),
                   ('Python适配、报告转换及外围等待', st['query_compute_slice_wall_ms'] - st['branch_wall_ms'])]
        assert min(v for _, v in compute) >= 0
        close(sum(v for _, v in compute), st['query_compute_slice_wall_ms'])
        detailed.append({'node': sid, 'lifecycle_ms': {k: st[k] for k, _ in life_fields},
                         'branch_total_ms': st['query_branch_wall_ms'], 'compute_parts_ms': dict(compute),
                         'thread_cpu_ms': st['server_thread_cpu_ms'], 'native_core_ms': st['branch_wall_ms'],
                         'expanded': b['work_spent'], 'transfer': b['peer_transfer'], 'fork_events': b['fork_events']})
    assert len(domains) == 1
    # Cut the full query interval by all measured window boundaries. This also
    # supports overlapping node windows without counting them twice.
    cuts = sorted({0., total} | {w[k] for w in windows for k in ('start_ms', 'end_ms')})
    timeline = []
    for a, z in zip(cuts, cuts[1:]):
        nodes = [w['node'] for w in windows if w['start_ms'] <= a and z <= w['end_ms']]
        timeline.append({'start_ms': a, 'end_ms': z, 'duration_ms': z - a,
                         'percent': 100 * (z - a) / total,
                         'label': '、'.join(f'节点{n}批次窗口' for n in nodes) if nodes else '批次窗口外（未细分）'})
    close(sum(t['duration_ms'] for t in timeline), total)
    csv_rows(out / 'timeline.csv', timeline)
    csv_rows(out / 'record_spans.csv', raw)
    save(out / 'breakdown.json', {'query_id': q['query_id'], 'total_ms': total, 'top_parts_ms': dict(top),
                                 'clock_domain': next(iter(domains)), 'windows': windows,
                                 'timeline': timeline, 'branches': detailed,
                                 'unavailable': ['absolute branch admission/worker/ACK timestamps',
                                                 'per-query control send/receive timestamps',
                                                 'pure GIL/lock wait', 'physical SSD latency']})
    colors = ['#bd833d', '#7599c0', '#228a78', '#a06691', '#a6acb4']
    fig, axes = plt.subplots(3, 1, figsize=(14, 11), layout='constrained')
    ax = axes[0]
    for i, (label, value) in enumerate(top):
        ax.barh(i, value, color=['#7599c0', '#bd833d', '#228a78', '#a6acb4'][i])
        ax.text(value + total * .012, i, f'{value:.3f} ms  ({100 * value / total:.3f}%)', va='center')
    ax.set_yticks(range(len(top)), [x[0] for x in top])
    ax.invert_yaxis()
    ax.set_xlim(0, total * 1.29)
    ax.set_title('① 端到端耗时分组：互斥相加覆盖100%，不表示各阶段的绝对位置', loc='left')
    ax.set_xlabel('毫秒')
    ax = axes[1]
    for i, w in enumerate(windows):
        ax.broken_barh([(0, total)], (i - .28, .56), facecolors='#edf0f3')
        ax.broken_barh([(w['start_ms'], w['end_ms'] - w['start_ms'])], (i - .28, .56), facecolors=colors[2])
        ax.annotate(f"{w['start_ms']:.3f} → {w['end_ms']:.3f} ms", (w['start_ms'], i),
                    xytext=(12, 20), textcoords='offset points', arrowprops={'arrowstyle': '-'})
    ax.set_yticks(range(len(windows)), [f"节点{w['node']}" for w in windows])
    ax.set_ylim(-.65, len(windows) - .25)
    ax.set_xlim(0, total * 1.02)
    ax.set_xlabel(f'相对客户端提交的实测时间 / ms；API返回 = {total:.3f} ms')
    ax.set_title('② 真实时间轴：绿色为首批开始至末批结束；灰色未细分，不能认定为CPU空闲', loc='left')
    ax = axes[2]
    for i, d in enumerate(detailed):
        left = 0.
        for j, (key, label) in enumerate(life_fields):
            value = d['lifecycle_ms'][key]
            ax.barh(i, value, left=left, color=colors[j], label=label if i == 0 else None)
            if value > 80:
                ax.text(left + value / 2, i, f'{value:.2f}', va='center', ha='center', color='white')
            left += value
        ax.text(left + 15, i, f'{left:.2f} ms', va='center')
    ax.set_yticks(range(len(detailed)), [f"节点{d['node']}" for d in detailed])
    ax.invert_yaxis()
    ax.set_xlim(0, max(d['branch_total_ms'] for d in detailed) * 1.16)
    ax.set_title('③ 分支生命周期组成：各自从零画起，不是共同起点；紫色等待已释放计算线程', loc='left')
    ax.set_xlabel('分支自身毫秒；不同分支可能重叠，不能相加为query总延迟')
    ax.legend(loc='upper left', bbox_to_anchor=(0, -.30), ncol=3, fontsize=9)
    path = ' → '.join(str(b['node_id']) for b in branches)
    fig.suptitle(f"查询 ID {q['query_id']}  |  总延迟 {total:.2f} ms  |  节点 {path}  |  Recall@10 {q['recall']:.0%}\n"
                 '完整2M索引 · 本机三进程 · 2000查询并发上限 · 每节点256计算线程 · 预算200', fontsize=15)
    for ext in ('png', 'svg'):
        fig.savefig(out / f'timeline.{ext}', dpi=160)
    plt.close(fig)

    # A separate small-scale figure keeps sub-ms work readable.
    fig, axes = plt.subplots(1, 2, figsize=(15, 6), layout='constrained')
    labels = list(detailed[0]['compute_parts_ms'])
    for i, d in enumerate(detailed):
        axes[0].barh([k + (i - (len(detailed) - 1) / 2) * .32 for k in range(len(labels))],
                     list(d['compute_parts_ms'].values()), height=.30, label=f"节点{d['node']}")
    axes[0].set_yticks(range(len(labels)), labels)
    axes[0].invert_yaxis()
    axes[0].set_xlabel('毫秒：墙钟耗时，包含期间可能发生的调度等待')
    axes[0].set_title('搜索执行区间内部拆分')
    axes[0].legend()
    transfer_labels = [('peer_transfer_queue_ms', '派发线程池提交至执行'), ('peer_pool_wait_ms', '获取连接'),
                       ('peer_connect_ms', '建立连接'), ('peer_send_ms', '编码/发送'),
                       ('peer_response_wait_ms', '等待响应/解码')]
    transfers = [d for d in detailed if d['transfer']['peer_rpc_count']]
    assert len(transfers) == 1, 'Plot expects one outgoing branch; extend layout otherwise'
    tr = transfers[0]['transfer']
    transfer_parts = [(label, tr[key]) for key, label in transfer_labels]
    remainder = tr['peer_transfer_wall_ms'] - sum(tr[k] for k, _ in transfer_labels[1:])
    assert remainder >= 0
    transfer_parts.append(('派发其余状态/边界时间', remainder))
    for i, (label, value) in enumerate(transfer_parts):
        axes[1].barh(i, value, color='#a06691')
        axes[1].text(value + 3, i, f'{value:.3f}', va='center')
    axes[1].set_yticks(range(len(transfer_parts)), [p[0] for p in transfer_parts])
    axes[1].invert_yaxis()
    axes[1].set_xlim(0, max(v for _, v in transfer_parts) * 1.23)
    axes[1].set_title('远程派发拆分：与本地搜索及收尾等待有重叠')
    axes[1].set_xlabel('毫秒；响应等待包含双方调度和处理，不是纯网络RTT')
    fig.suptitle(f"查询 {q['query_id']} 的执行与派发细节：这些耗时不可再加到端到端总时间", fontsize=14)
    for ext in ('png', 'svg'):
        fig.savefig(out / f'compute_dispatch.{ext}', dpi=160)
    plt.close(fig)

    run_rel = str(run.relative_to(ROOT))
    lines = ['# 接纳与远程派发队列：新一轮实验及单查询耗时分解', '',
             '本轮实际运行现有buffered接纳队列和optimized远程派发线程池；没有更换为新的专用派发队列或事件循环。生产搜索代码未修改。', '',
             '## 实验条件', '',
             '- 完整seq_2M索引：2M×2560，raw L2，1-bit RaBitQ，top-10排除自身；复用已构建索引。',
             '- Intel-35本机三个TCP数据进程，共享CPU及SSD；不是物理三机实验。',
             '- 2000条不同验证查询；20客户端进程×100线程，并发上限2000；40次预热不计入结果。',
             '- 每节点计算线程上限256、远程派发16、控制发送16；连接上限8/目标；接纳容量与活动查询上限均2048。',
             '- 预算200、frontier64、local_beam4；同步buffered pread，应用缓存0，详细trace开启。',
             '- 同一冻结查询集合，排除128条历史校准查询；复用全库真值缓存，本轮未重新扫描全库计算真值。',
             '- 一次突发负载并排空，不是持续稳态吞吐测试，也不是容量最优性实验。', '',
             '## 总体结果', '']
    rows = [['成功 / 失败', f"{report['successful_queries']} / {report['failed_queries']}"],
            ['平均Recall@10', f"{report['recall_at_k_all_requests']:.3%}"], ['QPS', f"{report['qps']:.2f}"],
            ['测量窗口', f"{report['measured_wall_seconds']:.3f} s"], ['实际并发峰值', report['actual_peak_inflight']]]
    rows += [[label, f"{report['latency_ms'][key]:.3f} ms"] for key, label in
             [('mean', '平均延迟'), ('p50', 'p50'), ('p95', 'p95'), ('p99', 'p99'), ('max', '最大延迟')]]
    lines += table(['指标', '结果'], rows)
    means = report['query_timing_summary']['cumulative_work']
    mean_fields = life_fields[:4] + [('peer_transfer_queue_ms', '派发队列等待'),
                                    ('peer_response_wait_ms', '派发响应等待'), ('pread_ms', 'pread调用'),
                                    ('exact_distance_ms', '精确距离'), ('rabitq_ms', 'RaBitQ估距')]
    lines += ['', '以下为每query跨分支累计均值，含重叠阶段，不能相加为端到端延迟。', '']
    lines += table(['阶段', '平均ms/query'], [[label, f"{means[k]['mean_ms']:.3f}"] for k, label in mean_fields])
    lines += ['', '## 随机查询与图表', '',
              f"从{len(eligible)}条成功跨节点查询中，按query_row排序，使用固定种子{cfg['selection_seed']}随机抽取，未按耗时挑选。",
              f"查询ID **{q['query_id']}**，query_row={q['query_row']}，request_id=`{q['request_id']}`。路径节点{path}，总延迟 **{total:.3f} ms**，Recall@10={q['recall']:.0%}。",
              '固定种子此次再次抽中了早先control_path示例中的同一query；本报告所有计时来自新运行，不复用旧测量。', '',
              '![端到端分解及真实时间轴](query_latency/timeline.png)', '',
              '端到端分组互斥覆盖全部耗时；真实时间轴只定位已记录的批次窗口；分支生命周期按各自起点展示，不能把不同分支相加。', '']
    lines += table(['端到端分组', '耗时ms', '占总延迟'], [[label, f'{v:.6f}', f'{v / total:.4%}'] for label, v in top])
    lines += ['', '入口准备、合并及生命周期剩余是差额，可能出现在数据执行之前或之后，不能全部放在结果返回之后。', '',
              '## 覆盖客户端提交至返回的时间轴', '',
              '本机所有批次记录共享单调时钟域。窗口外区间可能包含接纳、任务派发、计算提交、报告转换及控制回传，不能归因于单一等待。', '']
    lines += table(['开始ms', '结束ms', '区间', '耗时ms', '占总延迟'],
                   [[f"{t['start_ms']:.3f}", f"{t['end_ms']:.3f}", t['label'], f"{t['duration_ms']:.3f}",
                     f"{t['percent']:.3f}%"] for t in timeline])
    lines += ['', '## 分支生命周期', '']
    headers = ['阶段'] + [f"节点{d['node']} ms" for d in detailed]
    lines += table(headers, [[label] + [f"{d['lifecycle_ms'][key]:.6f}" for d in detailed] for key, label in life_fields]
                   + [['分支合计'] + [f"{d['branch_total_ms']:.6f}" for d in detailed]])
    lines += ['', '接纳至提交包含在接纳队列中的等待；提交至计算线程进入还包含submit内部同步、建线程及调度，不等于纯线程池队列时间。',
              '本地结束后的派发收尾不占用计算线程：optimized模式保存结果并返回，由future回调完成收尾。此处等待远端接纳确认，不是等待远端搜索完成。', '',
              '## 搜索执行与远程派发细分', '', '![计算与派发细分](query_latency/compute_dispatch.png)', '']
    lines += table(headers, [[label] + [f"{d['compute_parts_ms'][label]:.6f}" for d in detailed] for label in labels])
    lines += ['']
    lines += table(headers, [['执行区间合计'] + [f"{d['lifecycle_ms']['query_compute_slice_wall_ms']:.6f}" for d in detailed],
                             ['搜索线程CPU时间（辅助，不再相加）'] + [f"{d['thread_cpu_ms']:.6f}" for d in detailed]])
    lines += ['', 'pread为文件读取调用墙钟时间；精确距离为原始向量精排；RaBitQ为量化估距。批次内其他包含邻接解析、候选整理等。C++其余区间包含预处理、候选维护、扩展选择、派发回调及调度等待。外围差额包含Python适配和报告构造，不能全部归因于GIL。', '']
    lines += table(['远程派发阶段', 'ms'], [[label, f'{v:.6f}'] for label, v in transfer_parts])
    lines += ['', f"派发任务从提交到完成共{tr['peer_transfer_queue_ms'] + tr['peer_transfer_wall_ms']:.3f}ms，其中派发执行{tr['peer_transfer_wall_ms']:.3f}ms。",
              '响应等待包含目标端接收、处理及双方调度；仅凭该计时不能定位为网络拥塞或接纳队列阻塞。入队确认与实际执行已解耦，但并不保证ACK零延迟。', '',
              '## 该查询的其他指标', '']
    lines += table(['指标', '值'], [
        ['节点扩展数', '，'.join(f"节点{d['node']}={d['expanded']}" for d in detailed)],
        ['唯一/重复向量扩展', f"{s['unique_expanded']} / {s['duplicate_expansions']}"],
        ['最大扩展深度 / 最大跨节点深度', f"{s['max_expanded_depth']} / {s['max_cross_shard_depth']}"],
        ['服务器之间搜索派发 / 节点回访', f"{s['peer_branch_rpc_count']} / {s['node_revisit_count']}"],
        ['全部协议RPC（含控制）', s['all_protocol_rpc_count']],
        ['应用协议字节（含控制，不含TCP/IP报头）', s['all_protocol_bytes']],
        ['逻辑文件读取字节 / pread次数', f"{s['file_read_bytes']:.0f} / {s['pread_calls']:.0f}"],
    ])
    lines += ['', '副本向量可能在两个不同节点各扩展一次；重复向量扩展与重复访问服务器是不同指标。', '',
              '## 节点负载与结束状态', '']
    lines += table(['节点', '平均CPU核当量', '结束线程数', 'RSS GiB', '接纳待启动峰值*', '物理读取增量bytes'], [
        [n['shard_id'], f"{n['mean_cpu_cores']:.3f}", n['process']['threads'], f"{n['process']['rss_bytes'] / 2**30:.3f}",
         n['peer_lifecycle_after']['admission_peak_pending_start'], n['process_deltas']['storage_read_bytes']]
        for n in report['node_process_load']])
    lines += ['', '*峰值包含预热。CPU核当量来自进程CPU时间/采样墙钟；256是计算线程上限，不表示256核持续工作。',
              '三进程物理读取增量均为0，本轮为热页缓存读取；不能报告纯SSD延迟占比。各节点结束时队列、待启动任务、未确认分支均为0，无接纳拒绝和控制发送错误；三个服务与20个客户端进程全部停止。', '',
              '## 解释边界与后续优化依据', '',
              '该查询中目标节点接纳至计算提交等待明显大于执行区间。接纳与派发解耦减少计算线程被网络确认占用，但没有消除提交和调度瓶颈。现有数据不足以把剩余时间精确划分为GIL、状态锁、线程池锁或OS调度。',
              '节点1没有子分支，其约0.007ms派发收尾只是完成检查开销，不代表一次远程等待。',
              '当前只有批次区间具备可对齐的绝对时间戳；接纳/派发/控制回传主要记录时长。不能用累计时长倒推出所有阶段的绝对起止，也不能把最后批次至API返回全归为网络回传。',
              '本轮复测已有实现，不是新旧算法配对对照，不据此宣称新的性能提升或最佳队列容量。', '',
              '## 文件与复现', '',
              '- [配置](buffered/experiment.yaml)、[负载参数](buffered/load_plan.yaml)、[随机抽样参数](analysis.yaml)。',
              '- [原始选中查询](query_latency/selected_query.json)、[选择依据](query_latency/selection.json)、[结构化拆分](query_latency/breakdown.json)。',
              '- [完整时间轴CSV](query_latency/timeline.csv)、[读取/距离计算事件CSV](query_latency/record_spans.csv)、[时间分解SVG](query_latency/timeline.svg)。',
              f'- [总体汇总]({run_rel}/summary.json)、[阶段均值]({run_rel}/query_stage_means.csv)、[逐查询CSV]({run_rel}/queries.csv)、[原始记录]({run_rel}/queries.jsonl.gz)。',
              '- [审计](verification.json)、[源代码与配置哈希](sources.json)、[执行日志](execution_unrestricted.log)。',
              '- `buffered/run_load.py`执行新测试；`analyze.py`校验本轮记录并重画图，不发起查询。',
              '- 首次沙箱内启动被禁止建立TCP服务，保留其日志；沙箱外获准运行完成，失败启动不计入2000查询。', '']
    (ROOT / 'REPORT.md').write_text('\n'.join(lines))
    save(ROOT / 'analysis_summary.json', {
        'qps': report['qps'], 'latency_ms': report['latency_ms'], 'recall': report['recall_at_k_all_requests'],
        'selected_query_id': q['query_id'], 'selected_query_ms': total,
        'selected_timeline': timeline,
    })
    print(json.dumps({'query_id': q['query_id'], 'total_ms': total, 'timeline': timeline}, ensure_ascii=False))


if __name__ == '__main__':
    main()
