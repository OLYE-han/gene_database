"""Select the measured QPS winner under the YAML quality constraints."""
from pathlib import Path
import collections,json,statistics
import yaml
R=Path(__file__).resolve().parent
policy=yaml.safe_load((R/'selection_policy.yaml').read_text())
assert (policy['primary_metric'],policy['direction'],policy['aggregate_across_rounds'])==('qps','maximize','median')
groups=collections.defaultdict(list)
for path in sorted((R/'throughput_capacity').glob('*/result.json')):
    s=json.loads(path.read_text())
    if s['mode'] in policy['candidate_modes']:groups[s['mode']].append(s)
eligible={}
for mode in policy['candidate_modes']:
    rows=groups[mode]
    if len(rows)!=2:raise RuntimeError(f'Missing forward/reverse results: {mode}')
    if any(s['failed_queries']>policy['maximum_failed_queries'] or
           s['recall_at_k_all_requests']<policy['minimum_mean_recall_at_10'] or
           (policy['require_all_audits'] and not all(s['audit'].values())) or
           (policy['p99_limit_ms'] is not None and s['latency_ms']['p99']>policy['p99_limit_ms']) for s in rows):continue
    eligible[mode]=dict(qps_median=statistics.median(s['qps'] for s in rows),
        p99_median_ms=statistics.median(s['latency_ms']['p99'] for s in rows),
        concurrency=rows[0]['configured_concurrency'])
if not eligible:raise RuntimeError('No configuration satisfies selection constraints')
winner=max(eligible,key=lambda m:eligible[m]['qps_median'])
selection=dict(policy=policy,eligible=eligible,winner=winner)
(R/'selection.json').write_text(json.dumps(selection,indent=2))
plan=yaml.safe_load((R/'throughput_capacity.yaml').read_text())
plan.update(directory='sustained',queries=policy['sustained_queries'],
    repeat_selection=policy['sustained_repeat_selection'],order=[winner],
    case_timeout_seconds=policy['sustained_case_timeout_seconds'],port=policy['sustained_callback_port'])
(R/'sustained.yaml').write_text(yaml.safe_dump(plan,sort_keys=False))
candidate=yaml.safe_load((R/'candidate.yaml').read_text());concurrency=eligible[winner]['concurrency']
candidate['control']['benchmark']['concurrency']=concurrency
candidate['control']['max_client_threads']=max(concurrency,candidate['control']['max_client_threads'])
(R/'candidate.yaml').write_text(yaml.safe_dump(candidate,sort_keys=False))
(R/'benchmark_request.yaml').write_text(yaml.safe_dump(dict(queries=2000,concurrency=concurrency,warmup=32)))
print(json.dumps(selection,indent=2))
