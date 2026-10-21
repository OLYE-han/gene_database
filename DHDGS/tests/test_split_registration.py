"""Two-phase native admission retains exactly-once identity and credits."""
import copy
from concurrent.futures import ThreadPoolExecutor
import threading
import time
import unittest
from unittest.mock import patch

import test_peer_communication
from test_peer_communication import eventually
from dhdgs.config import validate
from dhdgs.peer import PeerRuntime


class GatedController:
    def __init__(self, controller, fail=False):
        self.controller=controller;self.entered=threading.Event();self.release=threading.Event()
        self.fail=fail;self.calls=[];self.extra_updates=[]

    def register_branch(self, meta, control):
        self.calls.append(meta['query_request_id'])
        if meta['query_request_id']=='first':
            self.entered.set()
            if not self.release.wait(5):raise TimeoutError('Test registration gate timed out')
            if self.fail:raise RuntimeError('Injected registration failure')
        return self.controller.register_branch(meta, control)

    def take_updates(self):
        extra,self.extra_updates=self.extra_updates,[]
        return extra+self.controller.take_updates()

    def __getattr__(self,name):return getattr(self.controller,name)


class SplitRegistrationTests(unittest.TestCase):
    setUp=test_peer_communication.CommunicationTests.setUp
    tearDown=test_peer_communication.CommunicationTests.tearDown

    def state(self,name):
        state=test_peer_communication.CommunicationTests.state(self,name)
        state.update(branch_slots=0,ancestry=[])
        return state

    def runtime(self,fail=False):
        self.c['peer'].update(execution_model='native',worker_threads=2,max_active_queries=8)
        self.c['peer']['control']['scheduler']='native'
        self.c['peer']['communication']['reuse_connections']=True
        self.c['peer']['compute']['mode']='fixed'
        self.c['peer']['admission'].update(mode='buffered',queue_capacity=8,queue_lock='shared')
        self.c['peer']['locking']={'split_native_registration':True,'native_register_release_gil':True}
        self.c['storage']['io']['backend']='sync_pread'
        self.c['network']['transport']='tcp'
        self.c['metrics'].update(lifecycle_trace=True)
        self.c['scheduler'].update(mode='reference',max_wait_ms=1e9)
        self.c['search']['max_inflight']=1
        runtime=PeerRuntime(self.c,self.shard)
        gate=GatedController(runtime.native_controller,fail);runtime.native_controller=gate
        self.addCleanup(runtime.close);self.addCleanup(gate.release.set)
        return runtime,gate

    def test_registration_releases_runtime_lock_and_duplicate_waits_outside_it(self):
        r,g=self.runtime();state=self.state('first')
        pool=ThreadPoolExecutor(2)
        try:
            first=pool.submit(r.accept,state);self.assertTrue(g.entered.wait(2))
            duplicate=pool.submit(r.accept,copy.deepcopy(state))
            self.assertTrue(r.lock.acquire(timeout=.1));r.lock.release()
            self.assertEqual(r.lease('first','secret'),{'found':True})
            conflict=copy.deepcopy(state);conflict['decision_seed']+=1
            with self.assertRaisesRegex(ValueError,'Conflicting'):r.accept(conflict)
            self.assertEqual(r.accept(self.state('second')),{'accepted':True,'duplicate':False})
            eventually(lambda:r.admission_pending==1)
            self.assertFalse(duplicate.done())
            g.release.set()
            self.assertFalse(first.result(2)['duplicate']);self.assertTrue(duplicate.result(2)['duplicate'])
            self.assertEqual(g.calls.count('first'),1)
            eventually(lambda:r.admission_pending==0)
            t=r.records[('first','first-root')]['execution_timestamps']
            self.assertLessEqual(t['accept_state_lock_released_ns'],t['accept_native_register_enter_ns'])
            self.assertLessEqual(t['accept_native_register_return_ns'],t['accept_commit_lock_acquired_ns'])
            self.assertLessEqual(t['accept_commit_lock_acquired_ns'],t['enqueued_ns'])
        finally:g.release.set();pool.shutdown()

    def test_failure_rolls_back_credits_and_identity(self):
        r,g=self.runtime(fail=True)
        pool=ThreadPoolExecutor(1)
        try:
            first=pool.submit(r.accept,self.state('first'));self.assertTrue(g.entered.wait(2))
            self.assertEqual(r.active_count,1);self.assertEqual(r.admission_pending,1)
            g.release.set()
            with self.assertRaisesRegex(RuntimeError,'Injected'):first.result(2)
            self.assertEqual(r.active_count,0);self.assertEqual(r.admission_pending,0)
            self.assertNotIn('first',r.query_visits);self.assertFalse(r.records)
            g.fail=False;r.accept(self.state('first'))
            self.assertEqual(g.calls.count('first'),2)
        finally:g.release.set();pool.shutdown()

    def test_cancellation_before_commit_never_starts_search(self):
        r,g=self.runtime();pool=ThreadPoolExecutor(1)
        try:
            with patch.object(r,'_run') as run:
                first=pool.submit(r.accept,self.state('first'));self.assertTrue(g.entered.wait(2))
                r.lease('first','secret',cancel=True);g.release.set()
                with self.assertRaisesRegex(TimeoutError,'cancelled'):first.result(2)
                with self.assertRaisesRegex(TimeoutError,'cancelled'):r.accept(self.state('first'))
                run.assert_not_called();self.assertEqual(r.admission_pending,0)
                self.assertEqual(g.calls,['first'])
        finally:g.release.set();pool.shutdown()

    def test_shutdown_fences_commit_and_waits_without_holding_runtime_lock(self):
        r,g=self.runtime();pool=ThreadPoolExecutor(2)
        try:
            with patch.object(r,'_run') as run:
                first=pool.submit(r.accept,self.state('first'));self.assertTrue(g.entered.wait(2))
                closed=pool.submit(r.close);eventually(r.stop.is_set)
                self.assertTrue(r.lock.acquire(timeout=.1));r.lock.release()
                self.assertFalse(closed.done());g.release.set()
                with self.assertRaisesRegex(RuntimeError,'closed'):first.result(2)
                closed.result(2);run.assert_not_called();self.assertEqual(r.admission_pending,0)
        finally:g.release.set();pool.shutdown()

    def test_expired_reservation_is_retained_until_owner_rolls_back(self):
        r,g=self.runtime();pool=ThreadPoolExecutor(1)
        try:
            first=pool.submit(r.accept,self.state('first'));self.assertTrue(g.entered.wait(2))
            g.extra_updates=[dict(query_request_id='first',branch_id='first-root',kind='expired')]
            r.accept(self.state('second'))
            self.assertTrue(r.records[('first','first-root')]['registration_expired'])
            g.release.set()
            with self.assertRaisesRegex(TimeoutError,'expired'):first.result(2)
            self.assertNotIn(('first','first-root'),r.records)
            self.assertIn(('first','first-root'),r.tombstones)
            with self.assertRaisesRegex(ValueError,'Expired'):r.accept(self.state('first'))
        finally:g.release.set();pool.shutdown()

    def test_expired_record_destructor_runs_outside_state_lock(self):
        r,g=self.runtime();lock=r.lock;held=[]
        class Record(dict):
            def __del__(self):held.append(lock.locked())
        key=('expired','branch')
        r.records[key]=Record(acked=True);r.query_visits[key[0]]=key
        g.extra_updates=[dict(query_request_id=key[0],branch_id=key[1],kind='expired')]
        r.accept(self.state('second'))
        self.assertEqual(held,[False])

    def test_invalid_mode_is_rejected(self):
        r,g=self.runtime()
        config={k:v for k,v in self.c.items() if not k.startswith('_')}
        validate(config)
        bad=copy.deepcopy(config);bad['peer']['admission']['mode']='direct'
        with self.assertRaisesRegex(ValueError,'Split registration'):validate(bad)
