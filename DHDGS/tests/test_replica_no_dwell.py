"""Replica source candidates participate in the ordinary expansion policy."""
import copy
import time
import unittest

import numpy as np
import yaml

from dhdgs.config import validate
from test_replicas import ReplicaTests, ROOT
from test_region_tasks import RegionRuntimeTests, mode


class RemovedResidenceConfigTests(unittest.TestCase):
    def test_removed_key_is_rejected_instead_of_silently_reenabling_it(self):
        base = yaml.safe_load((ROOT / 'configs/dhdgs.yaml').read_text())
        validate(copy.deepcopy(base))
        self.assertNotIn('replica_source_min_expansions', base['peer'])
        for old in (0, 144):
            c = copy.deepcopy(base)
            c['peer']['replica_source_min_expansions'] = old
            with self.assertRaisesRegex(ValueError, 'was removed; delete this key'):
                validate(c)


class SourcePolicyTests(unittest.TestCase):
    setUpClass = classmethod(ReplicaTests.setUpClass.__func__)
    tearDownClass = classmethod(ReplicaTests.tearDownClass.__func__)
    setUp = ReplicaTests.setUp
    rpc = ReplicaTests.rpc
    info = ReplicaTests.info
    check_result = RegionRuntimeTests.check_result

    def configure_replica_test(self, c):
        mode(c)
        c['search']['local_beam'] = 1
        c['scheduler'].update(remote_selection='threshold', remote_weight=1e6)
        # The fixture omits the optional pull block, so pulling is disabled.

    def test_policy_can_return_with_local_candidates_and_large_remaining_budget(self):
        info = self.info()
        source, entry = info['source'], info['entry_id']
        target = (source + 1) % 3
        ids = np.load(ROOT / self.c['paths']['output'] / 'index' /
                      f'shard_{source}' / 'ids.npy').tolist()
        members = sorted(set(ids[:max(2, len(ids)//2)] + [entry]))
        version = 'source_policy'
        self.rpc(source, 'source_start', id=version,
                 plan=dict(ids=members, entries=[entry], target=target))
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            state = self.rpc(source, 'status', id=version)
            self.assertNotEqual(state['phase'], 'FAILED', state.get('error'))
            if state['phase'] == 'SENT' and self.rpc(target, 'status', id=version)['phase'] == 'READY':
                break
            time.sleep(.02)
        else:
            self.fail('Replica fixture did not become ready')
        self.engine.runtime.replica_routes([dict(source=source, target=target,
            version=version, entries=[entry], fraction=1., min_score=-1e300,
            max_score=1e300, ttl_ms=10000)])
        q = self.engine.search_async(self.x[0], 0).result(15)
        self.check_result(q, 200)
        gates = [event for branch in q['branch_reports']
                 if branch.get('replica_version') == version
                 for event in branch.get('pull_gate_events', [])]
        self.assertTrue(any(e['local_candidates'] > 0 and
                            e['local_expanded_before'] < 144 and
                            e['remaining_before'] > 56 for e in gates), gates)
        self.assertTrue(any(b.get('logical_region') == f'base:{source}' and
                            b['work_spent'] > 0 for b in q['branch_reports']))
        self.assertNotIn('replica_source_dwell_blocked', q['stats'])


if __name__ == '__main__':
    unittest.main()
