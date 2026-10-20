"""No-transfer completion publication must never wait for the state/dirty lock."""
from concurrent.futures import Future
import copy
from pathlib import Path
import tempfile
import time
import threading
from types import MethodType
import unittest
from unittest.mock import patch
import yaml

from dhdgs.config import load_config,validate
from dhdgs.peer import PeerRuntime
import test_control_scheduler
import test_control_delay
import test_peer_communication


class CompletionFastPathTests(unittest.TestCase):
    add=test_control_scheduler.SchedulerTests.add
    finish=test_control_scheduler.SchedulerTests.finish
    finalize=test_control_scheduler.SchedulerTests.finalize

    def setUp(self):
        test_control_scheduler.SchedulerTests.setUp(self)
        self.s.fastpath=True
        self.r.try_publish_no_transfer=MethodType(PeerRuntime.try_publish_no_transfer,self.r)
        self.r._publish_report_locked=MethodType(PeerRuntime._publish_report_locked,self.r)

    def record(self,**kwargs):
        return self.add(lease=1e20,deadline=1e20,created_at=time.monotonic(),cancelled=False,
                        execution_timestamps={},peer_rpc_count=0,**kwargs)

    def test_runtime_lock_busy_queues_and_does_not_publish_inline(self):
        rec=self.record()
        with self.r.lock:
            self.s.local_done(('q','root'),{'stats':{}},[],time.perf_counter())
        self.assertIsNone(rec['report']);self.assertFalse(self.s._local_done_queue.empty())
        self.s.step()
        self.assertEqual(rec['status'],'complete')
        self.assertEqual(self.s.stats['completion_fast_path_lock_busy'],1)
        self.assertEqual(rec['execution_timestamps']['completion_fast_path_fallback'],'lock_busy')

    def test_dirty_lock_busy_does_not_block_fast_publication(self):
        rec=self.record();report={'stats':{}}
        with self.s._dirty_lock:
            self.s.local_done(('q','root'),report,[],time.perf_counter())
        self.assertIs(rec['report'],report);self.assertEqual(rec['status'],'complete')
        self.assertFalse(rec['acked'])
        self.assertTrue(self.s._local_done_queue.empty())
        self.s.step()
        self.assertEqual(self.s.stats['completion_fast_path_hits'],1)
        self.assertEqual(self.r.control_workers.calls[0][0],('q','root'))
        t=rec['execution_timestamps']
        self.assertLessEqual(t['finalize_enter_ns'],t['finalize_acquired_ns'])
        self.assertLessEqual(t['finalize_acquired_ns'],t['report_ready_ns'])
        self.assertLessEqual(t['report_ready_ns'],t['completion_fast_path_return_ns'])
        self.assertLessEqual(t['completion_ready_enqueued_ns'],t['completion_ready_consumed_ns'])
        self.assertNotIn('local_done_enqueued_ns',t)

    def test_nonempty_future_list_is_never_inspected_on_compute(self):
        rec=self.record()
        class Guard:
            def __getattribute__(self,name):raise AssertionError('compute accessed Future')
        self.s.local_done(('q','root'),{'stats':{}},[Guard()],time.perf_counter())
        self.assertIsNone(rec['report'])
        self.assertTrue(self.s._published_queue.empty())
        # Replace only the diagnostic Future before the consumer is allowed in.
        key,report,_,started,enqueued,fallback=self.s._local_done_queue.get_nowait()
        future=Future();future.set_result(None)
        self.s._local_done_queue.put((key,report,[future],started,enqueued,fallback))
        self.s.step()
        self.assertEqual(self.s.stats['completion_fast_path_future_fallback'],1)
        self.assertEqual(rec['status'],'complete')

    def test_fast_ready_selected_before_backlogged_completion_consumer(self):
        self.s.limit=1
        for i in range(20):
            key=(str(i),'root');self.add(key);self.s.dirty(key)
        rec=self.record()
        self.s.local_done(('q','root'),{'stats':{}},[],time.perf_counter())
        original=self.s._consume_local_done
        def consume(limit=None):
            self.assertEqual(self.r.control_workers.calls[0][0],('q','root'))
            return original(limit)
        self.s._consume_local_done=consume
        self.s.step()
        self.assertEqual(rec['status'],'complete')

    def test_cancel_stop_and_expiry_cannot_publish_success(self):
        for kind in ('cancel','stop','lease','deadline'):
            with self.subTest(kind=kind):
                self.r.records.clear();self.r.stop.clear()
                rec=self.record()
                if kind=='cancel':rec['cancelled']=True
                elif kind=='stop':self.r.stop.set()
                else:rec[kind]=0.
                self.s.local_done(('q','root'),{'stats':{}},[],time.perf_counter())
                self.assertEqual(rec['status'],'failed');self.assertIsNone(rec['report'])
                self.assertIn('cancelled or expired',rec['error'])

    def test_inflight_heartbeat_keeps_single_owner_then_sends_result(self):
        rec=self.record(control_inflight=True);self.r.control_active=1
        self.s.local_done(('q','root'),{'stats':{}},[],time.perf_counter());self.s.step()
        self.assertFalse(self.r.control_workers.calls)
        self.finish();self.s.step()
        self.assertEqual(len(self.r.control_workers.calls),1)

    def test_shutdown_drains_fast_notifications_without_republishing(self):
        rec=self.record();report={'stats':{}}
        self.s.local_done(('q','root'),report,[],time.perf_counter())
        self.r.stop.set();self.s.finish_stopped()
        self.assertTrue(self.s._published_queue.empty())
        self.assertIs(rec['report'],report);self.assertEqual(rec['status'],'complete')
        self.assertFalse(self.r.control_workers.calls)

    def test_duplicate_terminal_completion_never_overwrites_report(self):
        rec=self.record();original={'stats':{}}
        self.s.local_done(('q','root'),original,[],time.perf_counter())
        rec['acked']=True
        self.s.local_done(('q','root'),{'stats':{'bad':1}},[],time.perf_counter())
        self.s.step()
        self.assertIs(rec['report'],original);self.assertTrue(rec['acked'])
        self.assertFalse(self.r.control_workers.calls)

    def test_old_config_migrates_disabled_and_requires_bool(self):
        source=Path(__file__).resolve().parents[1]/'configs/dhdgs.yaml'
        raw=yaml.safe_load(source.read_text())
        del raw['peer']['control']['completion_fast_path']
        with tempfile.TemporaryDirectory() as tmp:
            path=Path(tmp)/'old.yaml';path.write_text(yaml.safe_dump(raw))
            self.assertFalse(load_config(path)['peer']['control']['completion_fast_path'])
        raw['peer']['control']['completion_fast_path']=1
        with self.assertRaises(ValueError):validate(raw)


class CompletionFastPathRuntimeTests(unittest.TestCase):
    tearDown=test_control_delay.InitialHeartbeatTests.tearDown
    state=test_control_delay.InitialHeartbeatTests.state

    def setUp(self):
        test_control_delay.InitialHeartbeatTests.setUp(self)
        self.c['peer']['control'].update(scheduler='event',completion_fast_path=True)

    def test_compute_worker_remains_available_with_state_or_dirty_lock_held(self):
        for lock_kind in ('state','dirty'):
            with self.subTest(lock=lock_kind):
                entered=threading.Event();release=threading.Event()
                def search(*args):
                    entered.set();release.wait(2);return {'stats':{}}
                with patch.object(PeerRuntime,'_control'),patch('dhdgs.peer.run_branch',side_effect=search):
                    runtime=PeerRuntime(self.c,self.shard)
                    try:
                        runtime.accept(self.state());self.assertTrue(entered.wait(1))
                        lock=runtime.lock if lock_kind=='state' else runtime.control_scheduler._dirty_lock
                        with lock:
                            release.set()
                            self.assertEqual(runtime.workers.submit(lambda:'next').result(.5),'next')
                            if lock_kind=='state':
                                self.assertIsNone(runtime.records[('q','root')]['report'])
                            else:
                                self.assertIsNotNone(runtime.records[('q','root')]['report'])
                    finally:release.set();runtime.close()
                    self.assertTrue(runtime.control_scheduler._local_done_queue.empty())
                    self.assertTrue(runtime.control_scheduler._published_queue.empty())

    def test_expired_or_cancelled_fast_completion_sends_error_not_result(self):
        for reason in ('cancelled','lease','deadline'):
            with self.subTest(reason=reason):
                entered=threading.Event();release=threading.Event();messages=[]
                def search(*args):
                    entered.set();release.wait(2);return {'stats':{}}
                def exchange(endpoint,message,*args):
                    messages.append(message)
                    return {'lease_ms':5000,'cancelled':False},{}
                with patch('dhdgs.peer.run_branch',side_effect=search),patch('dhdgs.peer.exchange',side_effect=exchange):
                    runtime=PeerRuntime(self.c,self.shard)
                    try:
                        runtime.accept(self.state());self.assertTrue(entered.wait(1))
                        with runtime.lock:
                            runtime.records[('q','root')][reason]=True if reason=='cancelled' else 0.
                        release.set()
                        test_peer_communication.eventually(lambda:runtime.records[('q','root')]['acked'])
                        self.assertTrue(messages)
                        self.assertTrue(any(m['error'] for m in messages))
                        self.assertFalse(any(m['reports'] for m in messages))
                        self.assertEqual(runtime.active_count,0)
                    finally:release.set();runtime.close()


if __name__=='__main__':unittest.main()
