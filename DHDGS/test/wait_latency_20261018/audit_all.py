"""Correctness/lifecycle audit also covering runs without detailed tracing."""
from pathlib import Path
import gzip,json

R=Path(__file__).resolve().parent
rows=[]
for source in sorted(R.glob('*/runs/*/report.json.gz')):
    with gzip.open(source,'rt') as f:r=json.load(f)
    run=source.parent;group=source.parents[2]
    selection=json.loads((group/'query_selection.json').read_text())
    queries=r['queries']
    assert len(queries)==len({q['request_id'] for q in queries})==2000
    assert sorted(q['query_row'] for q in queries)==selection['validation']
    assert not set(selection['validation'])&set(selection['calibration_excluded'])
    branches=0
    for q in queries:
        assert q['status']=='ok' and q['stats']['metrics_complete']
        assert q['recall']==len(set(q['ids'])&set(q['truth_ids']))/10
        s=q['stats'];assert s['expanded']<=200
        assert s['node_revisit_count']==s['router_expand_rpc_count']==0
        reports=q['branch_reports'];branches+=len(reports)
        assert len({b['node_id'] for b in reports})==len(reports)
        for b in reports:
            assert b['compute_thread_migrations']==0
            assert b['work_grant']==b['work_spent']+b['work_unused']+sum(ch['work_grant'] for ch in b['children'])
            for ch in b['children']:
                assert sum(d['remote'] and d['decision_id']==ch['decision_id'] and d['selected_node']==ch['node_id'] for d in b['decisions'])==1
    for node in r['node_process_load']:
        life=node['peer_lifecycle_after']
        assert life['admission_pending_start']==life['admission_queue_depth']==life['unacknowledged']==0
        assert life['admission_capacity_rejections']==life['control_totals']['errors']==0
        for q in (life.get('send_queues') or {}).values():assert q['queued']==q['active']==q['capacity_rejections']==0
        if life.get('compute_queue'):
            q=life['compute_queue'];assert q['queued']==q['active']==q['capacity_rejections']==q['callback_errors']==0
    services=json.loads((run/'services.json').read_text())
    clients=json.loads((run/'clients_lifecycle.json').read_text())
    assert services['test_completed'] and all(s['stopped'] for s in services['services'])
    assert len(clients)==20 and all(c['stopped'] and c['exit_code']==0 for c in clients)
    row=dict(group=group.name,run=run.name,queries=len(queries),branches=branches,
             recall=r['recall_at_k_all_requests'],passed=True)
    rows.append(row);print(json.dumps(row),flush=True)
(R/'all_runs_verification.json').write_text(json.dumps(dict(runs=rows,run_count=len(rows),
    queries=sum(r['queries'] for r in rows),failures=0,all_passed=True),indent=2))
