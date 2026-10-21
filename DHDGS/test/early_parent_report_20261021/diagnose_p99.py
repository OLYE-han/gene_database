"""Read-only diagnosis of the selected Intel-32 p99 branch and its backlog."""
import gzip
import json
import statistics
from pathlib import Path

ROOT = Path(__file__).resolve().parent
selected = json.loads((ROOT / 'p99/02_early/selected.json').read_text())
branch = next(b for b in selected['branches'] if b['report']['node_id'] == 1)
t, stats = branch['trace'], branch['report']['stats']
origin, accepted = t['receiver_message_ready_ns'], t['accepted_ns']
run = Path(selected['source_run'])
with gzip.open(run / 'nodes_after.json.gz', 'rt') as source:
    node = json.load(source)['1']
traces = node['peer_lifecycle']['execution_timestamps']
older = [r for r in traces if r['receiver_message_ready_ns'] < origin < r['accepted_ns']]
inside = [r for r in traces if origin <= r['accept_state_lock_acquired_ns']
          and r['accept_state_lock_released_ns'] <= accepted]
receiver = node['network_receiver']['workers']
result = dict(
    query_id=selected['query']['query_id'], job=selected['query']['job'], node_id=1,
    request_id=selected['query']['request_id'],
    receive_to_accept_ms=(accepted-origin)/1e6,
    own_admission_mutex_wait_ms=(t['accept_state_lock_acquired_ns']-t['accept_state_lock_enter_ns'])/1e6,
    receive_workers=receiver['workers'], peak_receive_queue=receiver['peak_queued'],
    older_requests_received_but_not_yet_accepted=len(older),
    admissions_fully_inside_wait_window=len(inside),
    admission_lock_hold_sum_ms=sum((r['accept_state_lock_released_ns']-r['accept_state_lock_acquired_ns'])/1e6 for r in inside),
    nested_control_state_wait_sum_ms=sum(r['native_register_state_wait_ns']/1e6 for r in inside),
    nested_control_state_hold_sum_ms=sum(r['native_register_lock_held_ns']/1e6 for r in inside),
    search_wall_ms=stats['branch_wall_ms'], search_cpu_ms=stats['server_thread_cpu_ms'],
    search_wall_minus_cpu_ms=stats['branch_wall_ms']-stats['server_thread_cpu_ms'],
    pread_wall_ms=stats['pread_ms'], rabitq_wall_ms=stats['rabitq_ms'],
    exact_distance_wall_ms=stats['exact_distance_ms'],
    comparisons=[])
for case in sorted((ROOT/'three').iterdir()):
    run = Path(json.loads((case/'result.json').read_text())['directory'])
    rows, wall, cpu = [], [], []
    with gzip.open(run/'queries.jsonl.gz', 'rt') as source:
        for line in source:
            q = json.loads(line)
            for b in q['branch_reports']:
                if b['node_id'] != 1:
                    continue
                s = b['stats']
                wall.append(s['branch_wall_ms']); cpu.append(s['server_thread_cpu_ms'])
                if q['query_id'] == result['query_id']:
                    rows.append(dict(job=q['job'], expanded=s['expanded'], estimates=s['rabitq_evaluations'],
                                     wall_ms=s['branch_wall_ms'], cpu_ms=s['server_thread_cpu_ms'],
                                     pread_ms=s['pread_ms'], rabitq_ms=s['rabitq_ms']))
    result['comparisons'].append(dict(case=case.name, mean_wall_ms=statistics.mean(wall),
                                     median_wall_ms=statistics.median(wall), mean_cpu_ms=statistics.mean(cpu),
                                     repeated_query=rows))
(ROOT/'p99_wait_diagnosis.json').write_text(json.dumps(result, indent=2))
print(json.dumps({k:v for k,v in result.items() if k!='comparisons'}, indent=2))
