"""Plot a retained single-branch tail query, using independent per-host clocks."""
from pathlib import Path
import csv
import gzip
import json
import os

ROOT = Path(__file__).resolve().parent
OUT = ROOT / 'sustained' / 'wait_detail'
RUN = ROOT / 'sustained/00_c256/runs/20261020T132936_benchmark_98d4d510'
os.environ.setdefault('MPLCONFIGDIR', str(ROOT / '.mplconfig'))
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib.font_manager import FontProperties
from analyze import timeline, write_csv


def select_query():
    """Use an explicit tail selection rule, never present it as a random sample."""
    OUT.mkdir(exist_ok=True)
    result = json.loads((ROOT / 'sustained/00_c256/result.json').read_text())
    target = result['latency_ms']['p99']
    nodes = json.load(gzip.open(RUN / 'nodes_after.json.gz', 'rt'))
    traces = {}
    required = ('enqueued_ns', 'dequeued_ns', 'worker_enter_ns', 'native_call_ns',
                'native_search_start_ns', 'native_loop_done_ns', 'worker_available_ns',
                'control_result_selected_ns')
    for node in nodes.values():
        for t in node['peer_lifecycle']['execution_timestamps']:
            if all(k in t for k in required):
                traces.setdefault(t['query_request_id'], []).append(t)
    with (RUN / 'queries.csv').open() as f:
        eligible = [r for r in csv.DictReader(f)
                    if r['request_id'] in traces and int(r['branch_count']) == 1
                    and len(traces[r['request_id']]) == 1 and r['status'] == 'ok']
    best = min(eligible, key=lambda r: abs(float(r['latency_ms']) - target))
    rid = best['request_id']
    events = [e for n in nodes.values() for e in n['peer_lifecycle']['control_events']
              if e.get('query_request_id') == rid]
    (OUT / 'data_trace.json').write_text(json.dumps(
        dict(traces=traces[rid], control_events=events), indent=2))
    (OUT / 'selection.json').write_text(json.dumps(dict(
        method='Nearest CSV latency to full-run p99 among successful single-branch '
               'requests with complete retained node timestamps',
        target_p99_ms=target, eligible=len(eligible), request_id=rid, csv=best,
        source_run=str(RUN.relative_to(ROOT))), indent=2))
    with gzip.open(RUN / 'queries.jsonl.gz', 'rt') as f:
        for line in f:
            if rid in line:
                q = json.loads(line)
                if q['request_id'] == rid:
                    (OUT / 'query.json').write_text(json.dumps(q, indent=2))
                    return
    raise RuntimeError('Selected query not found in raw log')


def main():
    if not (OUT / 'query.json').exists():
        select_query()
    q = json.loads((OUT / 'query.json').read_text())
    data = json.loads((OUT / 'data_trace.json').read_text())
    t = data['traces'][0]
    head = timeline(q)
    write_csv(OUT / 'head_stages.csv', head)
    result = next(e for e in data['control_events'] if e['kind'] == 'result')
    assert result['succeeded']

    # All timestamps within this list originate on Intel-32. No head offset.
    stamps = [
        ('数据接收与接纳', t['receiver_message_ready_ns']),
        ('接收处理／接纳', t['enqueued_ns']),
        ('接纳队列等待', t['dequeued_ns']),
        ('提交执行器 → 线程进入', t['worker_enter_ns']),
        ('线程进入 → 取得状态锁', t['worker_state_lock_acquired_ns']),
        ('原生调用准备', t['native_search_start_ns']),
        ('C++ 图搜索', t['native_loop_done_ns']),
        ('搜索结束 → 报告被选中', t['control_result_selected_ns']),
        ('报告选中 → 回传 RPC 完成', result['sender_rpc_return_ns']),
    ]
    node = [dict(stage=name, start_ms=(stamps[i-1][1]-stamps[0][1])/1e6,
                 end_ms=(stamp-stamps[0][1])/1e6,
                 duration_ms=(stamp-stamps[i-1][1])/1e6)
            for i, (name, stamp) in enumerate(stamps) if i]
    assert all(r['duration_ms'] >= 0 for r in node)
    write_csv(OUT / 'node_stages.csv', node)

    def group(name, first, last):
        return dict(stage=name, start_ms=head[first]['start_ms'],
                    end_ms=head[last]['end_ms'],
                    duration_ms=head[last]['end_ms']-head[first]['start_ms'])

    top = [group('接收、输入 IPC 与 HNSW 路由', 0, 6),
           group('顶层派发队列等待', 7, 7),
           group('注册与入口 RPC（含 ACK）', 8, 9),
           group('等待完整报告（含数据排队与检索）', 10, 10),
           group('合并排队、合并与记录', 11, 13),
           group('输出 IPC 与客户端消费', 14, 15)]

    font = FontProperties(fname='/usr/share/fonts/truetype/wqy/wqy-zenhei.ttc')
    plt.rcParams.update({'font.family': font.get_name(), 'font.size': 10,
                         'axes.unicode_minus': False, 'svg.fonttype': 'path'})
    fig, axes = plt.subplots(3, 1, figsize=(14, 13),
                             gridspec_kw={'height_ratios': [1, 1.2, 1]})
    amber, red, blue, green = '#df8a18', '#cc4f41', '#4a82b7', '#27816c'

    def panel(ax, rows, colors, title, xlabel, left=0):
        span = max(r['end_ms'] for r in rows)-left
        for i, (r, color) in enumerate(zip(rows, colors)):
            ax.barh(i, r['duration_ms'], left=r['start_ms'], height=.58,
                    color=color, zorder=3)
            ax.text(1.01, i, f"{r['duration_ms']:,.3f} ms",
                    transform=ax.get_yaxis_transform(), va='center', fontsize=10)
        ax.set_yticks(range(len(rows)), [r['stage'] for r in rows])
        ax.invert_yaxis()
        ax.set_xlim(left, left+span*1.03)
        ax.set_title(title, loc='left', fontsize=12, pad=12, weight='bold')
        ax.set_xlabel(xlabel)
        ax.grid(axis='x', alpha=.18, zorder=0)
        for side in ('top', 'right', 'left'):
            ax.spines[side].set_visible(False)
        ax.tick_params(axis='y', length=0)

    panel(axes[0], top, [blue, amber, blue, red, green, blue],
          f"A  顶层完整时序：{q['client_wall_ms']:,.3f} ms（Intel-35 时钟）",
          '距客户端实际提交的时间 / ms')
    panel(axes[1], node, [blue, red, amber, amber, blue, green, blue, blue],
          'B  同一查询在 Intel-32 内部的时序：接纳队列是主要等待位置',
          '距数据节点消息就绪的时间 / ms（独立起点，不与 A 对齐）')
    axes[1].text((node[1]['start_ms']+node[1]['end_ms'])/2, 1,
                 f"尚未交给计算线程：{node[1]['duration_ms']:,.2f} ms",
                 ha='center', va='center', color='white', weight='bold')
    origin = node[1]['end_ms']
    zoom = [dict(r, start_ms=r['start_ms']-origin, end_ms=r['end_ms']-origin)
            for r in node[2:]]
    panel(axes[2], zoom, [amber, amber, blue, green, blue, blue],
          'C  出队后放大：计算线程执行、释放与报告回传',
          '距 Intel-32 接纳队列出队的时间 / ms')
    available = (t['worker_available_ns']-t['dequeued_ns'])/1e6
    axes[2].axvline(available, color='#333333', linestyle='--', linewidth=1)
    axes[2].text(.99, 1.01, f'计算线程可复用：出队后 {available:.3f} ms（虚线）',
                 transform=axes[2].transAxes, ha='right', fontsize=9)
    fig.suptitle(f"等待发生在哪里？查询 {q['query_id']} · 第 {q['job']} 号任务 · Recall@10={q['recall']:.0%}",
                 x=.03, y=.98, ha='left', fontsize=17, weight='bold')
    fig.text(.03, .945,
             '最新 control_capacity 连续测试：100000 请求，并发 256，三机各 32 个查询线程，预算 200\n'
             '选样规则：在保留完整时间戳的单分支查询中，选择延迟最接近全程 p99 的请求；这是尾延迟样本。',
             fontsize=10, va='top')
    fig.text(.03, .025,
             'A 是互斥的端到端阶段；B/C 是同一查询的数据节点内部观测，不可与 A 重复相加。\n'
             '等待报告包含数据排队、检索和回传，不等于纯网络时间。锁前区间可能含线程调度，不能仅据时长断定忙等。',
             fontsize=9, color='#444444')
    fig.subplots_adjust(left=.28, right=.85, top=.875, bottom=.09, hspace=.58)
    for extension in ('png', 'svg', 'pdf'):
        fig.savefig(OUT / f'wait_timeline.{extension}', dpi=160)
    plt.close(fig)

    summary = dict(query_id=q['query_id'], job=q['job'], request_id=q['request_id'],
                   recall=q['recall'], client_wall_ms=q['client_wall_ms'],
                   head_dispatch_queue_ms=top[1]['duration_ms'],
                   head_report_wait_ms=top[3]['duration_ms'],
                   data_admission_wait_ms=node[1]['duration_ms'],
                   data_native_search_ms=node[5]['duration_ms'],
                   worker_available_after_dequeue_ms=available,
                   report_rpc_after_dequeue_ms=(result['sender_rpc_return_ns']-t['dequeued_ns'])/1e6,
                   completion_to_client_ms=q['client_wall_ms']-head[10]['end_ms'],
                   query_cpu_ms=q['stats']['server_thread_cpu_ms'],
                   client_backlog_before_submission_ms=q['client_backlog_ms'],
                   queue_depth_at_enqueue=t['queue_depth_at_enqueue'],
                   queue_depth_after_dequeue=t['queue_depth_after_dequeue'])
    (OUT / 'summary.json').write_text(json.dumps(summary, indent=2))
    (OUT / 'README.md').write_text(f'''# 单查询等待位置诊断

来源：control_capacity 的 100000 次连续请求测试；本次只分析已有记录，没有重新压测。
请求 `{q['request_id']}`，query {q['query_id']}，job {q['job']}。
在保留完整节点时间戳的成功单分支请求中，选取 CSV 延迟最接近全程 p99 的样本，
选择条件与样本数见 selection.json。不是随机样本，也不代表所有查询的平均水平。

![完整时序与节点内部放大](wait_timeline.png)

- 实际提交至客户端消费：{q['client_wall_ms']:.3f} ms。
- 顶层派发排队：{top[1]['duration_ms']:.3f} ms。
- 顶层等待完整报告：{top[3]['duration_ms']:.3f} ms，包含数据接纳、计算及回传。
- Intel-32 接纳入队至出队：{node[1]['duration_ms']:.3f} ms。
- 原生搜索循环：{node[5]['duration_ms']:.3f} ms；搜索线程 CPU {summary['query_cpu_ms']:.3f} ms。
- 顶层报告齐备至客户端消费：{summary['completion_to_client_ms']:.3f} ms。

数据节点入队时队列深度 {t['queue_depth_at_enqueue']}，出队后深度 {t['queue_depth_after_dequeue']}。
这说明请求长时间停留在接纳队列；没有证明当时所有计算线程都在忙碌，
也没有证明等待来自 SSD、锁竞争或 GIL 的任何单一原因。
计算线程在出队后 {available:.3f} ms 可复用，报告 RPC 在出队后
{summary['report_rpc_after_dequeue_ms']:.3f} ms 完成；回传由其他控制线程执行。
该请求只访问 Intel-32，无子分支，不能用此样本验证多分支结束行为。

A 仅使用 Intel-35 的时钟；B/C 仅使用 Intel-32 的时钟，分别归零，没有跨机时间戳相减。
B/C 是 A 中相关工作的内部视角，不与 A 累加；不把两台机器的零点当成同时发生。
线程进入至拿到状态锁一段可能包括调度延迟；只读这些边界无法判定占 CPU 忙等。
物理读取在本轮为零，搜索经过热页缓存，不代表冷 SSD 性能。

全量压测任务预先生成后受并发 256 限制，这条任务在实际提交前还有
{q['client_backlog_ms']:.3f} ms 的客户端待发时间，图的零点为实际提交。
该待发时间未计入服务响应延迟，没有将它当作数据节点排队。

head_stages.csv 保留全部 16 个互斥阶段；node_stages.csv 保留节点边界；
query.json、data_trace.json 保留该请求的原始证据。
运行 `gene_database/DHDGS/.venv/bin/python gene_database/DHDGS/test/control_capacity_20261020/plot_wait_detail.py`
可重绘 PNG/SVG/PDF。若选样文件不存在，脚本会从本机原始日志重新选取。
''')
    print(json.dumps(summary, indent=2))


if __name__ == '__main__':
    main()
