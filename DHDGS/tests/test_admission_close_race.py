"""Close cannot overtake an accept that has already reserved its identity."""
import threading
import unittest
from unittest.mock import patch
from dhdgs.peer import PeerRuntime
import test_control_delay


class ClosePublicationTest(unittest.TestCase):
    setUp=test_control_delay.InitialHeartbeatTests.setUp
    tearDown=test_control_delay.InitialHeartbeatTests.tearDown
    state=test_control_delay.InitialHeartbeatTests.state

    def test_close_cannot_overtake_accept_publication(self):
        self.c['peer']['admission']['queue_lock']='independent'
        published=threading.Event();release=threading.Event()
        accept_errors=[]
        class GatedRecords(dict):
            def __setitem__(self,key,value):
                published.set()
                if not release.wait(3):raise TimeoutError('publication gate not released')
                super().__setitem__(key,value)
        with patch.object(PeerRuntime,'_control'),patch('dhdgs.peer.run_branch',return_value={'stats':{}}):
            runtime=PeerRuntime(self.c,self.shard)
            runtime.records=GatedRecords()
            def accept():
                try:runtime.accept(self.state())
                except Exception as exc:accept_errors.append(exc)
            caller=threading.Thread(target=accept)
            closer=threading.Thread(target=runtime.close)
            try:
                caller.start();self.assertTrue(published.wait(2))
                closer.start()
                # On buggy code close sets stop before acquiring state lock,
                # allowing the idle consumer to exit while accept is gated.
                # Fixed close cannot set stop until this publication completes.
                if runtime.stop.wait(.1):
                    runtime.admission_thread.join(1)
                    self.assertFalse(runtime.admission_thread.is_alive())
                release.set();caller.join(2);closer.join(2)
                self.assertFalse(caller.is_alive());self.assertFalse(closer.is_alive())
                self.assertFalse(accept_errors)
                self.assertEqual(runtime.admission_pending,0)
                self.assertFalse(runtime.admission_queue)
                self.assertEqual(runtime.records[('q','root')]['status'],'failed')
            finally:
                release.set();caller.join(2);closer.join(2);runtime.close()


if __name__=='__main__':
    unittest.main(defaultTest='ClosePublicationTest.test_close_cannot_overtake_accept_publication')
