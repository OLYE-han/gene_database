"""Audit parent publication and offline transfer counters, using local clocks."""
import gzip
import json
from pathlib import Path
import numpy as np

ROOT = Path(__file__).resolve().parent


def dist(values):
    return {"mean": float(np.mean(values)), "p99": float(np.percentile(values, 99)),
            "max": float(max(values))} if values else None


rows = []
for case in sorted((ROOT / 'three').iterdir()):
    result = json.loads((case / 'result.json').read_text())
    run = Path(result['directory'])
    with gzip.open(run / 'nodes_after.json.gz', 'rt') as source:
        nodes = json.load(source)
    traces, events, transfers = {}, {}, {}
    for node in nodes.values():
        life = node['peer_lifecycle']
        for trace in life['execution_timestamps']:
            traces[trace['query_request_id'], trace['branch_id']] = trace
        for event in life['control_events']:
            if event['kind'] == 'result' and event.get('succeeded'):
                events.setdefault((event['query_request_id'], event['branch_id']), []).append(event)
        for record in life['transfer_records']:
            transfers[record['query_request_id'], record['branch_id']] = record
    counts = dict(queries=0, parents=0, ready_before_peer_ack=0, sent_before_peer_ack=0,
                  incomplete_at_delivery=0, incomplete_after_reconciliation=0,
                  reconciled_branches=0, matching_transfer_counters=0)
    durations, samples = [], []
    with gzip.open(run / 'queries.jsonl.gz', 'rt') as source:
        for line in source:
            query = json.loads(line)
            counts['queries'] += 1
            counts['incomplete_at_delivery'] += not query['stats']['metrics_complete_at_delivery']
            counts['incomplete_after_reconciliation'] += not query['stats']['metrics_complete']
            for report in query['branch_reports']:
                counts['reconciled_branches'] += 'peer_transfer_at_publication' in report
                if not report['children']:
                    continue
                key = query['request_id'], report['branch_id']
                trace = traces[key]
                event = min(events[key], key=lambda e: e['sender_enter_ns'])
                assert transfers[key]['pending_transfers'] == 0
                for metric in ('peer_rpc_count', 'peer_request_bytes', 'peer_response_bytes'):
                    assert report['peer_transfer'][metric] == transfers[key][metric], (case, key, metric)
                counts['matching_transfer_counters'] += 1
                counts['parents'] += 1
                counts['ready_before_peer_ack'] += trace['report_ready_ns'] < trace['transfer_worker_return_ns']
                counts['sent_before_peer_ack'] += event['sender_enter_ns'] < trace['transfer_worker_return_ns']
                duration = (trace['report_ready_ns'] - trace['local_done_dequeued_ns']) / 1e6
                durations.append(duration)
                if event['sender_enter_ns'] < trace['transfer_worker_return_ns']:
                    samples.append(dict(request_id=query['request_id'], query_id=query['query_id'], job=query['job'],
                                        node_id=report['node_id'], latency_ms=query['client_wall_ms'],
                                        publication_ms=duration,
                                        sent_ahead_of_peer_ack_ms=(trace['transfer_worker_return_ns']-event['sender_enter_ns'])/1e6))
    rows.append(dict(case=case.name, counts=counts, parent_completion_to_publication_ms=dist(durations),
                     largest_observed_send_lead=sorted(samples, key=lambda s:s['sent_ahead_of_peer_ack_ms'], reverse=True)[:5]))

(ROOT / 'parent_comparison.json').write_text(json.dumps(rows, indent=2))
lines = ['', '## 有子分支的父节点：报告是否真正提前发出', '',
         '以本机时钟比较报告发送开始与子任务接纳ACK回调；不跨机器相减。只统计children非空的父分支。', '',
         '|轮次|父分支数|发布等待均值 ms|发布等待p99 ms|报告就绪早于子ACK|报告发送早于子ACK|交付时指标未齐|离线后指标未齐|',
         '|---|---:|---:|---:|---:|---:|---:|---:|']
for row in rows:
    c, d = row['counts'], row['parent_completion_to_publication_ms']
    lines.append(f"|{row['case']}|{c['parents']}|{d['mean']:.4f}|{d['p99']:.4f}|{c['ready_before_peer_ack']}|{c['sent_before_peer_ack']}|{c['incomplete_at_delivery']}|{c['incomplete_after_reconciliation']}|")
lines += ['', '发布等待定义为local_done_dequeued_ns至report_ready_ns；不包含完成命令排队。指标未齐按查询计数。离线补齐仅更新网络计数，不修改交付时刻或端到端延迟。所有有子分支父报告的最终RPC次数及收发字节均与节点最终快照一致。', '']
with (ROOT / 'REPORT.md').open('a') as target:
    target.write('\n'.join(lines))
print('\n'.join(lines))
