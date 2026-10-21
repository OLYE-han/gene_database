"""Head lanes retain real TCP search, budgets and live configuration semantics."""
import copy
import time
from pathlib import Path
from test_managed_cluster import ManagedClusterTests
from dhdgs.managed_cluster import ClusterHead
from dhdgs.config import validate


class HeadLaneManagedTests(ManagedClusterTests):
    def setUp(self):
        super().setUp();old=self.head;old.close()
        old.c['peer']['head_lanes'].update(enabled=True,processes=2,
            results_directory=str(Path(self.work.name)/'lane_results'))
        self.head=ClusterHead(old.c,old.settings,self.token,old.truth)

    def test_invalid_overlap_and_port_range(self):
        c=copy.deepcopy(self.head.c);c.pop('_cluster_revision',None);validate(c)
        c['peer']['head_pipeline']['enabled']=True
        with self.assertRaisesRegex(ValueError,'exclusive'):validate(c)
        c['peer']['head_pipeline']['enabled']=False;c['peer']['head_port']=65535
        with self.assertRaisesRegex(ValueError,'endpoint'):validate(c)

    def test_no_result_pool_and_complete_end_to_end_timing(self):
        self.head.configure({},recover=True)
        result=self.head.query(self.x[0],0);stats=result['stats']
        self.assertEqual(len(self.head.engine.status()),2)
        self.assertEqual(stats['router_expand_rpc_count'],0)
        self.assertEqual(stats['node_revisit_count'],0)
        self.assertAlmostEqual(sum(stats['latency_breakdown_ms'].values()),stats['latency_ms'],places=5)
        self.assertGreaterEqual(stats['head_lane']['worker_received_ns'],stats['head_lane']['submitted_ns'])
        t=stats['head_lane']
        for a,b in [('worker_finished_ns','ipc_pack_enter_ns'),('ipc_pack_enter_ns','ipc_pack_done_ns'),
                    ('ipc_pack_done_ns','ipc_write_lock_enter_ns'),('ipc_write_lock_enter_ns','ipc_write_lock_acquired_ns'),
                    ('ipc_write_lock_acquired_ns','ipc_receive_done_ns'),('ipc_receive_done_ns','ipc_decode_done_ns')]:
            self.assertLessEqual(t[a],t[b])
        self.assertTrue(list((Path(self.work.name)/'lane_results').glob('*.jsonl')))
        self.head.quiescent()

    def test_dead_worker_is_not_reissued_and_live_lane_continues(self):
        self.head.configure({},recover=True)
        w=self.head.engine.workers[0];w['process'].terminate();w['process'].join(2)
        self.assertTrue(w['ready'].is_set())
        deadline=time.monotonic()+3
        while not w['error'] and time.monotonic()<deadline:time.sleep(.005)
        self.assertIsNotNone(w['error'])
        result=self.head.query(self.x[0],0)
        self.assertEqual(result['stats']['head_lane']['index'],1)
        self.head.quiescent()
