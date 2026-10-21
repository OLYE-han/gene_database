"""Read-only final check of the resident cluster and isolated test listeners."""
import json
from pathlib import Path
import socket
import sys
import yaml

ROOT = Path(__file__).resolve().parent
PROJECT = ROOT.parents[1]
sys.path[:0] = [str(PROJECT / 'tools'), str(PROJECT / 'python')]
from cluster_ctl import request

plan = yaml.safe_load((PROJECT / 'test/managed_continuation_three_20261019/cluster.yaml').read_text())
state = request(plan, 'status')
assert state['ready'] and not state['busy'] and not state['active_queries']
assert all(n['quiescent'] and n['accepting'] for n in state['nodes'])
listeners = []
test_plan = yaml.safe_load((ROOT / 'short/31_new_long/plan.yaml').read_text())
for node in test_plan['dhdgs']['cluster']['nodes']:
    with socket.socket() as sock:
        sock.settimeout(2)
        reachable = sock.connect_ex((node['host'], node['port'])) == 0
    listeners.append(dict(node=node['id'], host=node['host'], port=node['port'], reachable=reachable))
assert not any(n['reachable'] for n in listeners)
for name, source in [('30_old_long', 'source'), ('31_new_long', 'source_v6')]:
    case = ROOT / 'short' / name
    result = json.loads((case / 'result.json').read_text())
    loaded = json.loads((case / 'loaded_sources.json').read_text())
    config = yaml.safe_load((case / 'plan.yaml').read_text())
    assert result['query_count'] == 100000 and result['failed_queries'] == 0
    assert result['recall_at_k_all_requests'] >= .95 and all(result['audit'].values())
    assert result['configured_concurrency'] == 256 and result['threads'] == 32
    assert f'/{source}/python/' in loaded['dhdgs.peer']
    assert all(Path(h['project']).name == source for h in config['deployment']['hosts'])
    assert not config['dhdgs']['peer'].get('adaptive_services', {}).get('enabled', False)
summary = dict(production=state, experimental_listeners=listeners, long_runs_verified=True)
(ROOT / 'final_cluster_check.json').write_text(json.dumps(summary, indent=2))
print(json.dumps(dict(production_ready=state['ready'], active_queries=state['active_queries'],
                      experiments_stopped=True, long_runs_verified=True)))
