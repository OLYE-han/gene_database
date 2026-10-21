"""Adaptive control workers preserve identity, budgets and fixed search capacity."""
from collections import Counter
import threading
import unittest
from unittest.mock import patch
import test_peer_communication
from dhdgs.peer import PeerRuntime

class AdaptiveAdmissionTests(unittest.TestCase):
    setUp=test_peer_communication.CommunicationTests.setUp
    tearDown=test_peer_communication.CommunicationTests.tearDown
    state=test_peer_communication.CommunicationTests.state

    def test_unique_work_and_release_without_waiting_for_other_queries(self):
        self.c['peer']['admission'].update(queue_lock='independent',queue_capacity=32)
        self.c['peer']['compute']['mode']='fixed'
        self.c['peer']['worker_threads']=2
        self.c['peer']['max_active_queries']=32
        a=self.c['peer']['adaptive_services'];a.update(enabled=True,admission=True,
            admission_min=1,admission_max=4,sample_interval_ms=5,cooldown_ms=10,target_wait_ms=1)
        gate=threading.Event();parallel=threading.Event();seen=[];threads=set();lock=threading.Lock()
        original=PeerRuntime._submit_branch
        def submit(r,key):
            with lock:
                threads.add(threading.get_native_id())
                if len(threads)==4:parallel.set()
            if not gate.wait(3):raise TimeoutError('gate')
            return original(r,key)
        def search(state,*args):
            with lock:seen.append(state['query_request_id'])
            return {'stats':{}}
        # Fake search isolates scheduling semantics; native search correctness
        # is exercised by the existing integration suite and real cluster runs.
        with patch.object(PeerRuntime,'_control'),patch.object(PeerRuntime,'_submit_branch',submit),\
             patch('dhdgs.peer.run_branch',side_effect=search):
            r=PeerRuntime(self.c,self.shard)
            try:
                for i in range(20):r.accept(self.state(str(i)))
                self.assertTrue(parallel.wait(3));gate.set()
                test_peer_communication.eventually(lambda:all(x['status']=='complete' for x in r.records.values()))
                self.assertEqual(Counter(seen),Counter(map(str,range(20))))
                self.assertEqual(r.admission_pending,0)
                self.assertEqual(r.workers.metrics()['workers'],2)
                self.assertEqual(r.metrics()['adaptive_admission']['peak_workers'],4)
            finally:gate.set();r.close()
            self.assertEqual(r.admission_pool.metrics()['workers'],0)

    def test_branch_owned_submission_without_runtime_lock(self):
        self.c['peer']['adaptive_services']['submit_without_state_lock']=True
        self.test_unique_work_and_release_without_waiting_for_other_queries()

    def test_branch_owned_start_preserves_pending_accounting(self):
        self.c['peer']['adaptive_services']['submit_without_state_lock']=True
        self.c['peer']['adaptive_services']['start_without_state_lock']=True
        self.test_unique_work_and_release_without_waiting_for_other_queries()
