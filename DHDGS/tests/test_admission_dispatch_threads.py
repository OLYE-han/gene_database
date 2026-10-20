"""Multiple admission consumers share one queue and never duplicate branches."""
from collections import Counter
import copy
import threading
import time
import unittest
from unittest.mock import patch
import yaml

import test_peer_communication
from dhdgs.config import load_config,validate
from dhdgs.peer import PeerRuntime


class AdmissionDispatchTests(unittest.TestCase):
    setUp=test_peer_communication.CommunicationTests.setUp
    tearDown=test_peer_communication.CommunicationTests.tearDown
    state=test_peer_communication.CommunicationTests.state

    def test_unique_consumption_with_parallel_submit_and_full_join(self):
        for queue_lock in ('shared','independent'):
            with self.subTest(queue_lock=queue_lock):
                self.c['peer']['admission'].update(dispatch_threads=4,queue_capacity=32,queue_lock=queue_lock)
                self.c['peer']['max_active_queries']=32
                self.c['peer']['worker_threads']=2
                calls=[];submitters=set();call_lock=threading.Lock()
                gate=threading.Event();all_entered=threading.Event()
                original=PeerRuntime._submit_branch
                def submit(runtime,key):
                    with call_lock:
                        submitters.add(threading.get_native_id())
                        if len(submitters)==4:all_entered.set()
                    if not gate.wait(2):raise TimeoutError('admission gate timed out')
                    return original(runtime,key)
                def search(state,*args):
                    with call_lock:calls.append(state['query_request_id'])
                    return {'stats':{}}
                with patch.object(PeerRuntime,'_control'),patch.object(PeerRuntime,'_submit_branch',submit),\
                     patch('dhdgs.peer.run_branch',side_effect=search):
                    runtime=PeerRuntime(self.c,self.shard)
                    try:
                        for i in range(20):runtime.accept(self.state(str(i)))
                        self.assertTrue(all_entered.wait(2))
                        self.assertIs(runtime.admission_thread,runtime.admission_threads[0])
                        self.assertEqual(runtime.admission_pending,20)
                        gate.set()
                        test_peer_communication.eventually(lambda:all(r['status']=='complete' for r in runtime.records.values()))
                        self.assertEqual(Counter(calls),Counter(map(str,range(20))))
                        self.assertEqual(runtime.admission_pending,0)
                        self.assertEqual(runtime.metrics()['admission_dispatch_threads'],4)
                    finally:gate.set();runtime.close()
                    self.assertTrue(all(not t.is_alive() for t in runtime.admission_threads))

    def test_close_drains_all_consumers_and_cancels_queued_work(self):
        self.c['peer']['admission'].update(dispatch_threads=4,queue_capacity=32)
        self.c['peer']['max_active_queries']=32
        entered=threading.Event();release=threading.Event()
        original=PeerRuntime._submit_branch
        def submit(runtime,key):
            entered.set();release.wait(2);return original(runtime,key)
        with patch.object(PeerRuntime,'_control'),patch.object(PeerRuntime,'_submit_branch',submit),\
             patch('dhdgs.peer.run_branch') as search:
            runtime=PeerRuntime(self.c,self.shard)
            closer=None
            try:
                for i in range(20):runtime.accept(self.state(str(i)))
                self.assertTrue(entered.wait(2))
                closer=threading.Thread(target=runtime.close);closer.start()
                self.assertTrue(runtime.stop.wait(1));release.set();closer.join(2)
                self.assertFalse(closer.is_alive())
                self.assertEqual(runtime.admission_pending,0)
                self.assertFalse(runtime.admission_queue)
                self.assertTrue(all(r['status']=='failed' for r in runtime.records.values()))
                self.assertTrue(all(not t.is_alive() for t in runtime.admission_threads))
                search.assert_not_called()
            finally:
                release.set()
                if closer is not None:closer.join(2)
                runtime.close()

    def test_migration_and_validation(self):
        raw={k:copy.deepcopy(v) for k,v in self.c.items() if not k.startswith('_')}
        del raw['peer']['admission']['dispatch_threads']
        path=self.root/'old_dispatch.yaml';path.write_text(yaml.safe_dump(raw))
        self.assertEqual(load_config(path)['peer']['admission']['dispatch_threads'],1)
        for invalid in (0,-1,True,1.5):
            raw['peer']['admission']['dispatch_threads']=invalid
            with self.assertRaises(ValueError):validate(raw)


if __name__=='__main__':unittest.main()
