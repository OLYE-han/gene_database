"""Inspect retained, same-host lifecycle and GC observations from long runs."""
import gzip
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parent
rows = []
for case in ['30_old_long', '31_new_long']:
    result = json.loads((ROOT / 'short' / case / 'result.json').read_text())
    run = Path(result['directory'])
    before = json.load(gzip.open(run / 'nodes_before.json.gz', 'rt'))
    after = json.load(gzip.open(run / 'nodes_after.json.gz', 'rt'))
    query = json.loads((ROOT / (case + '_slowest_query.json')).read_text())
    for sid, node in after.items():
        gc = node['gc_profile']
        old_gc = before[sid]['gc_profile']
        traces = node['peer_lifecycle']['execution_timestamps']
        events = gc['events']
        row = dict(case=case, node=sid, retained_trace_count=len(traces),
                   retained_gc_events=len(events), gc_event_capacity=gc['capacity'],
                   gc_wall_ms_delta=gc['wall_ms'] - old_gc['wall_ms'],
                   gc_cpu_ms_delta=gc['cpu_ms'] - old_gc['cpu_ms'],
                   gc_count_delta=gc['count'] - old_gc['count'],
                   longest_retained_gc=sorted(events, key=lambda e:e['wall_ms'], reverse=True)[:3],
                   slowest_query_id=query['query_id'], slowest_request_id=query['request_id'],
                   slowest_query_client_ms=query['client_wall_ms'], phase_outliers=[])
        row['slowest_query_traces'] = [t for t in traces if t['query_request_id'] == query['request_id']]
        for phase, a, b in [('admission', 'enqueued_ns', 'dequeued_ns'),
                            ('admission_lock', 'admission_lock_enter_ns', 'admission_lock_acquired_ns'),
                            ('worker_lock', 'worker_enter_ns', 'worker_state_lock_acquired_ns'),
                            ('native', 'native_search_start_ns', 'native_loop_done_ns')]:
            eligible = [t for t in traces if a in t and b in t]
            for t in sorted(eligible, key=lambda t:t[b]-t[a], reverse=True)[:3]:
                overlaps = [dict(generation=e['generation'], wall_ms=e['wall_ms'], cpu_ms=e['cpu_ms'],
                                 overlap_ms=max(0, min(e['end_ns'], t[b]) - max(e['start_ns'], t[a]))/1e6)
                            for e in events if e['start_ns'] < t[b] and e['end_ns'] > t[a]]
                row['phase_outliers'].append(dict(phase=phase, request_id=t['query_request_id'],
                    start_ns=t[a], end_ns=t[b], duration_ms=(t[b]-t[a])/1e6, gc_overlaps=overlaps))
        rows.append(row)
(ROOT / 'long_wait_diagnosis.json').write_text(json.dumps(rows, indent=2))
print(json.dumps([{k:r[k] for k in ['case', 'node', 'retained_trace_count', 'gc_wall_ms_delta',
                                  'gc_cpu_ms_delta', 'gc_count_delta', 'longest_retained_gc']}
                  for r in rows], indent=2))
