import copy
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest.mock import patch

import yaml
from threadpoolctl import threadpool_limits
from test_system import fixture
from dhdgs.config import load_config, validate
from dhdgs.index import build, Shard
from dhdgs.peer import PeerRuntime, execution_signature


class InitialHeartbeatTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.c, self.x = fixture(self.root)
        self.c['search'].update(execution='peer', max_inflight=3)
        self.c['scheduler']['mode'] = 'adaptive'
        self.c['peer'].update(worker_threads=1, heartbeat_interval_ms=500, lease_timeout_ms=5000)
        self.c['peer']['control']['initial_heartbeat_delay_ms'] = 500
        self.c['peer']['communication'].update(mode='optimized', reuse_connections=False,
                                              control_workers=2, max_pending_control=2)
        with threadpool_limits(limits=1):
            build(self.c)
        self.shard = Shard(Path(self.c['paths']['output']) / 'index/shard_0', 0)

    def tearDown(self):
        self.shard.close()
        self.temp.cleanup()

    def state(self):
        gid = int(self.shard.ids[0])
        return {'query_request_id':'q', 'branch_id':'root', 'parent_branch_id':None, 'token':'secret',
                'visited_nodes':[0], 'entry_node':0, 'target_node':0, 'epoch':self.shard.meta['epoch'],
                'signature':execution_signature(self.c), 'query_f32':self.x[0].tobytes(),
                'work_grant':12, 'branch_slots':1, 'seeds':[[gid,0.,0.,[0],None,0,0,gid,0.]],
                'seen':[gid], 'scale':1., 'decision_seed':42, 'query_id':None,
                'head_endpoint':['127.0.0.1',1]}

    def run_case(self, action, *, delay=500):
        self.c['peer']['control']['initial_heartbeat_delay_ms'] = delay
        entered = threading.Event()
        finish = threading.Event()
        sent = threading.Event()
        calls = []
        def search(*args):
            entered.set()
            finish.wait(3)
            return {'stats':{}}
        def exchange(endpoint, message, *args):
            calls.append((time.monotonic(), copy.deepcopy(message)))
            sent.set()
            return {'lease_ms':5000, 'cancelled':False}, {}
        with patch('dhdgs.peer.run_branch', side_effect=search), patch('dhdgs.peer.exchange', side_effect=exchange):
            runtime = PeerRuntime(self.c, self.shard)
            try:
                runtime.accept(self.state())
                self.assertTrue(entered.wait(2))
                action(runtime, finish, sent, calls)
            finally:
                finish.set()
                runtime.close()

    def test_unchanged_initial_state_waits_then_long_task_heartbeats(self):
        def check(runtime, finish, sent, calls):
            rec = runtime.records[('q','root')]
            self.assertEqual(rec['control_signature'], ((),False,None))
            self.assertAlmostEqual(rec['control_next_at'] - rec['created_at'], .5)
            self.assertFalse(sent.wait(.05), 'unchanged branch sent an immediate heartbeat')
            self.assertTrue(sent.wait(1.2), 'long task did not send its scheduled heartbeat')
            self.assertGreaterEqual(calls[0][0], rec['created_at'] + .5)
            self.assertFalse(calls[0][1]['reports'])
            self.assertFalse(finish.is_set())
        self.run_case(check)

    def test_new_child_topology_bypasses_initial_delay(self):
        def check(runtime, finish, sent, calls):
            self.assertFalse(sent.wait(.03))
            with runtime.lock:
                runtime.records[('q','root')]['children'].append({'branch_id':'child','node_id':1})
            runtime._control_changed(('q','root'))
            self.assertTrue(sent.wait(.3), 'topology update waited for initial heartbeat timer')
            self.assertIn(1, calls[0][1]['involved_nodes'])
            self.assertFalse(calls[0][1]['reports'])
        self.run_case(check)

    def test_completed_result_bypasses_initial_delay(self):
        def check(runtime, finish, sent, calls):
            self.assertFalse(sent.wait(.03))
            finish.set()
            self.assertTrue(sent.wait(.3), 'result waited for initial heartbeat timer')
            self.assertTrue(calls[0][1]['reports'])
        self.run_case(check)

    def test_error_bypasses_initial_delay(self):
        def check(runtime, finish, sent, calls):
            self.assertFalse(sent.wait(.03))
            with runtime.lock:
                rec = runtime.records[('q','root')]
                rec['error'] = 'test failure'
                rec['status'] = 'failed'
            runtime._control_changed(('q','root'))
            self.assertTrue(sent.wait(.3), 'error waited for initial heartbeat timer')
            self.assertEqual(calls[0][1]['error'], 'test failure')
        self.run_case(check)

    def test_zero_delay_preserves_immediate_heartbeat(self):
        def check(runtime, finish, sent, calls):
            self.assertTrue(sent.wait(.3))
            self.assertFalse(calls[0][1]['reports'])
        self.run_case(check, delay=0)

    def test_archived_migration_and_delay_bounds(self):
        raw = {k:copy.deepcopy(v) for k,v in self.c.items() if not k.startswith('_')}
        del raw['peer']['control']
        path = self.root / 'old_control.yaml'
        path.write_text(yaml.safe_dump(raw))
        loaded = load_config(path)
        self.assertEqual(loaded['peer']['control']['initial_heartbeat_delay_ms'], 0)
        self.assertTrue(any('initial_heartbeat' in item for item in loaded['_config_migration']))
        raw['peer']['control'] = {'initial_heartbeat_delay_ms':0,'scheduler':'scan','max_events_per_turn':0,'completion_fast_path':False}
        for delay in (-1, 501):
            raw['peer']['control']['initial_heartbeat_delay_ms'] = delay
            with self.assertRaisesRegex(ValueError, 'heartbeat delay'):
                validate(raw)
        raw['peer']['control'] = {}
        with self.assertRaises(ValueError):
            validate(raw)


if __name__ == '__main__':
    unittest.main()
