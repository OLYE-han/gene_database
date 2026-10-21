"""O(1) admission credit and immutable runtime signature regressions."""
import copy
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from threadpoolctl import threadpool_limits
from test_system import fixture
from dhdgs.index import build, Shard
from dhdgs.peer import PeerRuntime, execution_signature


def eventually(predicate):
    until=time.monotonic()+2
    while time.monotonic()<until:
        if predicate():return
        time.sleep(.002)
    raise AssertionError('condition did not become true')


class AdmissionCounterTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.root=Path(self.temp.name)
        self.c,self.x=fixture(self.root)
        self.c['search'].update(execution='peer',max_inflight=3)
        self.c['scheduler']['mode']='adaptive'
        self.c['peer'].update(worker_threads=1,max_active_queries=2,heartbeat_interval_ms=10,
                              lease_timeout_ms=1000)
        self.c['peer']['communication'].update(mode='optimized',reuse_connections=False)
        self.c['metrics'].update(detailed_trace=False,lifecycle_trace=True,trace_query_ids=[])
        with threadpool_limits(limits=1):build(self.c)
        self.shard=Shard(Path(self.c['paths']['output'])/'index/shard_0',0)

    def tearDown(self):
        self.shard.close();self.temp.cleanup()

    def state(self,qid):
        gid=int(self.shard.ids[0])
        return {'query_request_id':qid,'branch_id':qid+'-root','parent_branch_id':None,'token':'secret',
                'visited_nodes':[0],'entry_node':0,'target_node':0,'epoch':self.shard.meta['epoch'],
                'signature':execution_signature(self.c),'query_f32':self.x[0].tobytes(),
                'work_grant':12,'branch_slots':1,'seeds':[[gid,0.,0.,[0],None,0,0,gid,0.]],
                'seen':[gid],'scale':1.,'decision_seed':42,'query_id':None,
                'head_endpoint':['127.0.0.1',1]}

    def test_capacity_duplicate_ack_and_removal_conserve_counter(self):
        with patch.object(PeerRuntime,'_control'),patch('dhdgs.peer.run_branch',return_value={'stats':{}}):
            runtime=PeerRuntime(self.c,self.shard)
            try:
                first=self.state('first');runtime.accept(first);runtime.accept(self.state('second'))
                eventually(lambda:all(r['status']=='complete' for r in runtime.records.values()))
                self.assertEqual(runtime.active_count,2)
                runtime.accept(first);self.assertEqual(runtime.active_count,2)
                conflict=copy.deepcopy(first);conflict['decision_seed']+=1
                with self.assertRaisesRegex(ValueError,'Conflicting'):runtime.accept(conflict)
                with self.assertRaisesRegex(RuntimeError,'capacity'):runtime.accept(self.state('third'))
                self.assertNotIn(('third','third-root'),runtime.records)
                with runtime.lock:
                    rec=runtime.records[('first','first-root')]
                    runtime._mark_acked_locked(rec);runtime._mark_acked_locked(rec)
                self.assertEqual(runtime.active_count,1)
                runtime.accept(self.state('third'))
                eventually(lambda:runtime.records[('third','third-root')]['status']=='complete')
                self.assertEqual(runtime.active_count,2)
                with runtime.lock:
                    runtime._pop_record_locked(('second','second-root'))
                    runtime._pop_record_locked(('second','second-root'))
                    runtime._pop_record_locked(('first','first-root'))
                self.assertEqual(runtime.active_count,1)
                self.assertEqual(runtime.metrics()['unacknowledged'],1)
            finally:runtime.close()

    def test_accept_does_not_rehash_config_or_scan_record_values(self):
        class NoScan(dict):
            def values(self):raise AssertionError('admission scanned all records')
        with patch.object(PeerRuntime,'_control'),patch('dhdgs.peer.run_branch',return_value={'stats':{}}):
            runtime=PeerRuntime(self.c,self.shard)
            state=self.state('q')
            try:
                runtime.records=NoScan(runtime.records)
                with patch('dhdgs.peer.execution_signature',side_effect=AssertionError('rehashed config')):
                    runtime.accept(state)
                eventually(lambda:runtime.records[('q','q-root')]['status']=='complete')
                t=runtime.records[('q','q-root')]['execution_timestamps']
                self.assertLessEqual(t['accept_state_lock_enter_ns'],t['accept_state_lock_acquired_ns'])
                self.assertLessEqual(t['accept_state_lock_acquired_ns'],t['accepted_ns'])
                self.assertLessEqual(t['accepted_ns'],t['accept_state_lock_released_ns'])
                self.assertLessEqual(t['accept_thread_cpu_enter_ns'],t['accept_thread_cpu_return_ns'])
            finally:
                runtime.records=dict(runtime.records);runtime.close()

    def test_failed_branch_uses_credit_until_ack_or_expiry(self):
        with patch.object(PeerRuntime,'_control'),patch('dhdgs.peer.run_branch',side_effect=RuntimeError('search failed')):
            runtime=PeerRuntime(self.c,self.shard)
            try:
                runtime.accept(self.state('q'))
                eventually(lambda:runtime.records[('q','q-root')]['status']=='failed')
                self.assertEqual(runtime.active_count,1)
                with runtime.lock:runtime._pop_record_locked(('q','q-root'))
                self.assertEqual(runtime.active_count,0)
            finally:runtime.close()

    def test_real_send_ack_decrements_for_legacy_scan_and_event(self):
        def exchange(*args):return {'lease_ms':1000,'cancelled':False},{}
        for mode,scheduler in [('legacy','scan'),('optimized','scan'),('optimized','event')]:
            with self.subTest(mode=mode,scheduler=scheduler):
                self.c['peer']['communication']['mode']=mode
                self.c['peer']['control']['scheduler']=scheduler
                with patch('dhdgs.peer.exchange',side_effect=exchange),patch('dhdgs.peer.run_branch',return_value={'stats':{}}):
                    runtime=PeerRuntime(self.c,self.shard)
                    try:
                        runtime.accept(self.state('q'))
                        eventually(lambda:runtime.records[('q','q-root')]['acked'])
                        self.assertEqual(runtime.active_count,0)
                        self.assertEqual(runtime.metrics()['unacknowledged'],0)
                    finally:runtime.close()

    def test_unacknowledged_expiry_decrements_for_legacy_scan_and_event(self):
        for mode,scheduler in [('legacy','scan'),('optimized','scan'),('optimized','event')]:
            with self.subTest(mode=mode,scheduler=scheduler):
                self.c['peer']['communication']['mode']=mode
                self.c['peer']['control']['scheduler']=scheduler
                self.c['peer']['retention_seconds']=.2
                with patch('dhdgs.peer.exchange',side_effect=OSError('offline')),\
                     patch('dhdgs.peer.run_branch',return_value={'stats':{}}):
                    runtime=PeerRuntime(self.c,self.shard)
                    try:
                        key=('q','q-root');runtime.accept(self.state('q'))
                        eventually(lambda:runtime.records[key]['status']=='complete')
                        self.assertEqual(runtime.active_count,1)
                        with runtime.lock:
                            runtime.records[key]['deadline']=time.monotonic()-1
                            runtime.records[key]['touched']=time.monotonic()-1
                        runtime._control_changed(key)
                        eventually(lambda:key not in runtime.records)
                        self.assertEqual(runtime.active_count,0)
                    finally:runtime.close()

    def test_legacy_expired_record_cannot_ack_and_release_credit_twice(self):
        self.c['peer']['communication']['mode']='legacy'
        self.c['peer']['control']['scheduler']='scan'
        control_loop=PeerRuntime._control
        with patch.object(PeerRuntime,'_control'),patch('dhdgs.peer.run_branch',return_value={'stats':{}}),\
             patch('dhdgs.peer.exchange',return_value=({'lease_ms':1000,'cancelled':False},{})) as exchange:
            runtime=PeerRuntime(self.c,self.shard);control=None
            try:
                key=('q','q-root');runtime.accept(self.state('q'))
                eventually(lambda:runtime.records[key]['status']=='complete')
                with runtime.lock:
                    runtime.records[key]['touched']=time.monotonic()-self.c['peer']['retention_seconds']-1
                control=threading.Thread(target=control_loop,args=(runtime,));control.start()
                eventually(lambda:key not in runtime.records)
                runtime.stop.set();runtime.control_wakeup.set();control.join(2)
                self.assertFalse(control.is_alive())
                self.assertEqual(runtime.active_count,0)
                exchange.assert_not_called()
            finally:
                runtime.close()
                if control is not None:control.join(2)


class LeaseIdentityTests(unittest.TestCase):
    def test_lease_does_not_scan_retained_queries_and_preserves_token(self):
        class NoScan(dict):
            def items(self):raise AssertionError('lease scanned history')
        runtime=PeerRuntime.__new__(PeerRuntime)
        runtime.lock=threading.Lock()
        runtime.c={'peer':{'lease_timeout_ms':1000}}
        record={'state':{'token':'secret'},'cancelled':False,'lease':0}
        runtime.records=NoScan({('q','b'):record})
        runtime.query_visits={'q':('q','b'),'expired':('expired','b')}
        self.assertEqual(runtime.lease('missing','secret'),{'found':False})
        self.assertEqual(runtime.lease('expired','secret'),{'found':False})
        with self.assertRaises(ValueError):runtime.lease('q','wrong')
        self.assertEqual(runtime.lease('q','secret',cancel=True),{'found':True})
        self.assertTrue(record['cancelled'])
        self.assertGreater(record['lease'],time.monotonic())
