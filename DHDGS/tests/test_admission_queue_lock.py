"""Queue-lock ablation: dequeue can progress independently of branch-state work."""
from collections import deque
import copy
from pathlib import Path
import tempfile
import threading
import time
import unittest

import yaml
from dhdgs.config import load_config, validate
from dhdgs.peer import PeerRuntime
import test_peer_communication


class AdmissionQueueLockTests(unittest.TestCase):
    def test_dequeue_progress_with_runtime_lock_held(self):
        for independent in (False,True):
            with self.subTest(independent=independent):
                runtime=PeerRuntime.__new__(PeerRuntime)
                runtime.lock=threading.Lock()
                runtime.stop=threading.Event()
                runtime.admission_cv=threading.Condition(threading.Lock() if independent else runtime.lock)
                key=('q','root')
                runtime.records={key:{'execution_timestamps':{}}}
                runtime.admission_queue=deque([key])
                entered=threading.Event();finished=threading.Event()
                def submit(key):
                    entered.set()
                    with runtime.lock:finished.set()
                    runtime.stop.set()
                runtime._submit_branch=submit
                runtime._admission_failed=lambda *_:self.fail('unexpected submit error')
                thread=threading.Thread(target=runtime._admit_loop)
                with runtime.lock:
                    thread.start()
                    if independent:
                        self.assertTrue(entered.wait(1))
                        self.assertFalse(runtime.admission_queue)
                        self.assertIn('dequeued_ns',runtime.records[key]['execution_timestamps'])
                    else:
                        self.assertFalse(entered.wait(.05))
                        self.assertEqual(len(runtime.admission_queue),1)
                    self.assertFalse(finished.is_set())
                thread.join(2)
                self.assertFalse(thread.is_alive())
                self.assertTrue(finished.is_set())

    def test_old_queue_lock_and_gc_settings_migrate_and_validate(self):
        source=Path(__file__).resolve().parents[1]/'configs/dhdgs.yaml'
        c=yaml.safe_load(source.read_text())
        del c['peer']['admission']['queue_lock'];del c['metrics']['gc_trace']
        with tempfile.TemporaryDirectory() as tmp:
            path=Path(tmp)/'old.yaml';path.write_text(yaml.safe_dump(c))
            loaded=load_config(path)
        self.assertEqual(loaded['peer']['admission']['queue_lock'],'shared')
        self.assertFalse(loaded['metrics']['gc_trace'])
        raw={k:copy.deepcopy(v) for k,v in loaded.items() if not k.startswith('_')}
        raw['peer']['admission']['queue_lock']='bad'
        with self.assertRaisesRegex(ValueError,'queue_lock'):validate(raw)
        raw['peer']['admission']['queue_lock']='independent';validate(raw)
        raw['metrics']['gc_trace']=1
        with self.assertRaises(ValueError):validate(raw)


class IndependentAdmissionRuntimeTests(unittest.TestCase):
    """Exercise the original admission contract with an independent queue lock."""
    def setUp(self):
        test_peer_communication.CommunicationTests.setUp(self)
        self.c['peer']['admission']['queue_lock']='independent'

    tearDown=test_peer_communication.CommunicationTests.tearDown
    state=test_peer_communication.CommunicationTests.state
    test_fast_ack=test_peer_communication.CommunicationTests.test_buffered_accept_does_not_wait_for_executor_submission
    test_capacity=test_peer_communication.CommunicationTests.test_admission_capacity_includes_executor_queue_and_reject_is_atomic
    test_cancel_expire=test_peer_communication.CommunicationTests.test_queued_cancel_and_expiry_do_not_execute_search
    test_submit_failure=test_peer_communication.CommunicationTests.test_background_submit_failure_is_explicit_and_dispatcher_survives
    test_timestamps=test_peer_communication.CommunicationTests.test_execution_timestamps_allow_worker_before_submit_returns


if __name__=='__main__':unittest.main()
