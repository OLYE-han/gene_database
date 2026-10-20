"""Integration tests for keyed notifications and compute completion handoff."""
import copy
import threading
import time
from unittest.mock import patch
from concurrent.futures import Future

import yaml
import test_control_delay
from dhdgs.config import load_config, validate
from dhdgs.peer import PeerRuntime


class EventRuntimeTests(test_control_delay.InitialHeartbeatTests):
    def setUp(self):
        super().setUp()
        self.c['peer']['control']['scheduler']='event'

    def test_compute_returns_while_runtime_lock_is_held_after_search(self):
        entered=threading.Event();release=threading.Event()
        def search(*args):
            entered.set();release.wait(3)
            return {'stats':{}}
        def exchange(*args):return {'lease_ms':5000,'cancelled':False},{}
        with patch('dhdgs.peer.run_branch',side_effect=search),patch('dhdgs.peer.exchange',side_effect=exchange):
            runtime=PeerRuntime(self.c,self.shard)
            try:
                runtime.accept(self.state());self.assertTrue(entered.wait(2))
                with runtime.lock:
                    release.set()
                    # Only one compute worker exists. Its next task cannot
                    # start if completion still needs runtime.lock/finalize.
                    self.assertEqual(runtime.workers.submit(lambda:'available').result(.5),'available')
                until=time.monotonic()+2
                while time.monotonic()<until and runtime.records[('q','root')]['status']!='complete':
                    time.sleep(.005)
                self.assertEqual(runtime.records[('q','root')]['status'],'complete')
            finally:release.set();runtime.close()

    def test_close_drains_completions_after_control_thread_exited(self):
        with patch.object(PeerRuntime,'_control'),patch('dhdgs.peer.run_branch',return_value={'stats':{}}):
            runtime=PeerRuntime(self.c,self.shard)
            try:
                runtime.accept(self.state())
                until=time.monotonic()+2
                while time.monotonic()<until and runtime.admission_pending:
                    time.sleep(.005)
                runtime.workers.submit(lambda:None).result(2)
                runtime.close()
                rec=runtime.records[('q','root')]
                self.assertEqual(rec['status'],'failed')
                self.assertNotIn('pending_report',rec)
                self.assertNotIn('transfer_futures',rec)
                self.assertEqual(runtime.admission_pending,0)
            finally:runtime.close()

    def test_control_scheduler_config_migration_and_validation(self):
        raw={k:copy.deepcopy(v) for k,v in self.c.items() if not k.startswith('_')}
        del raw['peer']['control']['scheduler']
        path=self.root/'old_scheduler.yaml';path.write_text(yaml.safe_dump(raw))
        loaded=load_config(path)
        self.assertEqual(loaded['peer']['control']['scheduler'],'scan')
        self.assertTrue(any('control.scheduler' in x for x in loaded['_config_migration']))
        raw['peer']['control']['scheduler']='invalid'
        with self.assertRaisesRegex(ValueError,'control.scheduler'):validate(raw)
        raw['peer']['control']['scheduler']='event'
        raw['peer']['communication']['mode']='legacy'
        with self.assertRaisesRegex(ValueError,'optimized communication'):validate(raw)

    def test_bounded_config_migrates_to_unbounded_and_rejects_negative(self):
        raw={k:copy.deepcopy(v) for k,v in self.c.items() if not k.startswith('_')}
        del raw['peer']['control']['max_events_per_turn']
        path=self.root/'old_event_limit.yaml';path.write_text(yaml.safe_dump(raw))
        self.assertEqual(load_config(path)['peer']['control']['max_events_per_turn'],0)
        raw['peer']['control']['max_events_per_turn']=-1
        with self.assertRaisesRegex(ValueError,'max_events_per_turn'):validate(raw)

    def test_finalize_timestamps_only_for_attempt_that_consumes_report(self):
        future=Future()
        with patch.object(PeerRuntime,'_control'),patch('dhdgs.peer.run_branch',return_value={'stats':{}}):
            runtime=PeerRuntime(self.c,self.shard)
            try:
                runtime.accept(self.state())
                runtime.workers.submit(lambda:None).result(2)
                key=('q','root');rec=runtime.records[key]
                # Control thread is disabled: consume native/branch completion
                # manually with an unfinished transfer Future.
                rec.update(pending_report={'stats':{}},transfer_futures=[future],
                           finalize_started=time.perf_counter(),execution_timestamps={})
                runtime._finalize_ready(key)
                self.assertNotIn('finalize_enter_ns',rec['execution_timestamps'])
                future.set_result(None);runtime._finalize_ready(key)
                trace=rec['execution_timestamps']
                self.assertLessEqual(trace['finalize_enter_ns'],trace['finalize_acquired_ns'])
                self.assertLessEqual(trace['finalize_acquired_ns'],trace['report_ready_ns'])
                first=dict(trace);runtime._finalize_ready(key)
                self.assertEqual(first,trace)
            finally:runtime.close()
