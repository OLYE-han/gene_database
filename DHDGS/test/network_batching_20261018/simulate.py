"""Replay approved transfer timestamps; no network or latency speedup claims."""
from pathlib import Path
import csv
import gzip
import hashlib
import json
from collections import defaultdict
import numpy as np
import yaml

R = Path(__file__).resolve().parent
c = yaml.safe_load((R / 'simulation.yaml').read_text())
source = (R / c['source_report']).resolve()
with gzip.open(source, 'rt') as f:
    report = json.load(f)
qids = {q['request_id'] for q in report['queries']}
traces = {t['branch_id']: t for node in report['node_process_load']
          for t in node['peer_lifecycle_after']['execution_timestamps']
          if t['query_request_id'] in qids}
actions = []
for q in report['queries']:
    assert q['status'] == 'ok'
    for b in q['branch_reports']:
        # Scalar transfer timestamps cannot reconstruct multiple actions.
        assert len(b['children']) <= 1, 'Need per-action timestamps for multi-child branches'
        if not b['children']:
            continue
        child, = b['children']
        assert sum(d['remote'] and d['decision_id'] == child['decision_id'] and d['selected_node'] == child['node_id'] for d in b['decisions']) == 1
        assert b['peer_transfer']['peer_rpc_count'] == 1, 'Retry traces need separate attempts'
        actions.append(dict(source=b['node_id'], target=child['node_id'],
                            enqueued_ns=traces[b['branch_id']]['transfer_enqueue_enter_ns'],
                            request_bytes=b['peer_transfer']['peer_request_bytes']))
assert len(actions) == sum(q['stats']['peer_branch_rpc_count'] for q in report['queries'])
flows = defaultdict(list)
for a in actions:
    flows[a['source'], a['target']].append(a)
for flow in flows.values():
    flow.sort(key=lambda a: a['enqueued_ns'])

def replay(window_ms, max_items):
    batches = []
    reserve = c['envelope_reserve_bytes']
    for flow in flows.values():
        pending, byte_count, deadline = [], reserve, 0
        def flush(at):
            nonlocal pending, byte_count
            if pending:
                batches.append((pending, at))
                pending, byte_count = [], reserve
        for a in flow:
            at = a['enqueued_ns']
            if pending and at >= deadline:
                flush(deadline)
            if pending and byte_count + a['request_bytes'] > c['max_batch_bytes']:
                flush(at)
            if not pending:
                deadline = at + round(window_ms * 1e6)
            pending.append(a)
            byte_count += a['request_bytes']
            if len(pending) >= max_items or byte_count >= c['max_batch_bytes'] or window_ms == 0:
                flush(at)
        flush(deadline)
    waits = [(sent-a['enqueued_ns'])/1e6 for batch,sent in batches for a in batch]
    assert len(waits) == len(actions) and all(-1e-9 <= w <= window_ms+1e-6 for w in waits)
    return dict(window_ms=window_ms, max_items=max_items,
                approved_actions=len(actions), batch_rpcs=len(batches),
                rpc_reduction_fraction=1-len(batches)/len(actions),
                mean_batch_size=len(actions)/len(batches),
                max_observed_batch_size=max(len(b) for b,_ in batches),
                singleton_batches=sum(len(b)==1 for b,_ in batches),
                added_wait_mean_ms=float(np.mean(waits)),
                added_wait_p99_ms=float(np.percentile(waits,99)),
                added_wait_max_ms=max(waits))

rows = [replay(window, size) for size in c['max_items'] for window in c['windows_ms']]
with (R / 'simulation.csv').open('w') as f:
    w = csv.DictWriter(f, list(rows[0])); w.writeheader(); w.writerows(rows)
result = dict(source=str(source), source_sha256=hashlib.sha256(source.read_bytes()).hexdigest(),
              queries=len(qids), approved_actions=len(actions),
              all_protocol_rpcs=sum(q['stats']['all_protocol_rpc_count'] for q in report['queries']),
              standalone_transfer_request_bytes=sum(a['request_bytes'] for a in actions),
              flows={f'{s}->{t}':len(flow) for (s,t),flow in sorted(flows.items())},
              assumptions=['Fixed historical enqueue timestamps; no execution feedback',
                           'Per source-target window measured from oldest item; max items/byte cap flush',
                           'Instant aggregator wakeup and infinite send capacity; no pool/receiver cost',
                           'Bytes cap estimated conservatively from standalone frames plus envelope reserve; no exact batched encoding',
                           'No measured batched wire bytes, QPS, recall or end-to-end latency'], rows=rows)
(R / 'simulation.json').write_text(json.dumps(result, indent=2))
print(json.dumps({k:v for k,v in result.items() if k not in ('rows','assumptions')}))
for row in rows:
    if row['max_items'] == 16:
        print(json.dumps(row))
