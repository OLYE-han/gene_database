import threading
import time
import unittest
from dhdgs.adaptive_connections import AdaptiveConnectionPool
from test_adaptive_service import until,S

class AdaptiveConnectionsTests(unittest.TestCase):
    def test_grow_only_approved_waiters_then_shrink_and_preserve_reservations(self):
        settings={**S,'connections_min':1,'connections_max':3}
        p=AdaptiveConnectionPool(settings,4);release=threading.Event();found=[];errors=[]
        _,first=p.borrow(('target',1),time.monotonic()+3)
        def wait():
            try:
                sock,entry=p.borrow(('target',1),time.monotonic()+3)
                found.append(entry);release.wait(3);p.release(entry,sock,True)
            except Exception as exc:errors.append(exc)
        ts=[threading.Thread(target=wait) for _ in range(2)]
        try:
            for t in ts:t.start()
            until(lambda:len(found)==2)
            self.assertEqual(first['borrowed'],3)
            self.assertEqual(p.metrics()['endpoints'][0]['peak_limit'],3)
            # Unused destinations do not gain connections or receive requests.
            self.assertEqual(len(p.metrics()['endpoints']),1)
            release.set()
            for t in ts:t.join(2)
            self.assertFalse(errors)
            p.release(first,None,True)
            until(lambda:p.metrics()['endpoints'][0]['limit']==1)
            self.assertEqual(first['borrowed'],0)
        finally:
            release.set();p.close()
            for t in ts:t.join(2)

    def test_close_wakes_waiter_and_does_not_revoke_live_socket(self):
        p=AdaptiveConnectionPool({**S,'connections_min':1,'connections_max':1},2)
        _,entry=p.borrow(('target',1),time.monotonic()+3)
        errors=[]
        def wait():
            try:p.borrow(('target',1),time.monotonic()+3)
            except Exception as exc:errors.append(str(exc))
        t=threading.Thread(target=wait);t.start()
        until(lambda:p.metrics()['endpoints'][0]['waiting']==1)
        p.close();t.join(2)
        self.assertEqual(errors,['RPC pool closed'])
        self.assertEqual(entry['borrowed'],1)
        p.release(entry,None,False)
        self.assertEqual(entry['borrowed'],0)
