import threading
import time
import unittest
from concurrent.futures import CancelledError
from dhdgs.adaptive_service import AdaptiveServicePool

S=dict(sample_interval_ms=5,target_wait_ms=1,cooldown_ms=10,
       downscale_idle_ms=30,growth_step=1,target_utilization=.8,ewma_weight=.2,history_size=128)

def until(fn,timeout=3):
    end=time.monotonic()+timeout
    while not fn():
        if time.monotonic()>end:raise AssertionError('condition not reached')
        time.sleep(.005)

class AdaptiveServiceTests(unittest.TestCase):
    def test_growth_retirement_unique_work_and_fixed_bound(self):
        gate=threading.Event();seen=[];lock=threading.Lock()
        p=AdaptiveServicePool(S,1,4,20,'test-adaptive')
        def task(i):
            gate.wait(3)
            with lock:seen.append(i)
            return i
        try:
            fs=[p.submit(task,i) for i in range(12)]
            until(lambda:p.metrics()['workers']==4 and p.metrics()['active']==4)
            gate.set();self.assertEqual([f.result(3) for f in fs],list(range(12)))
            until(lambda:p.metrics()['workers']==1)
            self.assertEqual(sorted(seen),list(range(12)))
            self.assertEqual(p.metrics()['peak_workers'],4)
            self.assertTrue(any(e['reason']=='idle' for e in p.metrics()['events']))
        finally:gate.set();p.shutdown()
        self.assertEqual(p.metrics()['workers'],0)

    def test_downstream_pressure_and_cancellation_bound(self):
        gate=threading.Event();started=threading.Event()
        p=AdaptiveServicePool(S,1,4,1,'test-pressure',lambda:False)
        try:
            first=p.submit(lambda:(started.set(),gate.wait(3)))
            self.assertTrue(started.wait(1))
            second=p.submit(lambda:self.fail('cancelled work ran'))
            with self.assertRaisesRegex(RuntimeError,'capacity'):p.submit(lambda:None)
            until(lambda:p.metrics()['blocked_growth']>0)
            self.assertEqual(p.metrics()['workers'],1)
            p.shutdown(wait=False,cancel_futures=True)
            with self.assertRaises(CancelledError):second.result()
            gate.set();first.result(2)
        finally:gate.set();p.shutdown()

    def test_failure_and_callback_do_not_kill_worker(self):
        p=AdaptiveServicePool(S,1,2,10,'test-failure')
        try:
            def fail():raise ValueError('expected')
            with self.assertRaises(ValueError):p.submit(fail).result(2)
            self.assertEqual(p.submit(lambda:42).result(2),42)
        finally:p.shutdown()

class UpdateOrderTests(unittest.TestCase):
    def test_expiry_overtakes_ack_without_double_credit(self):
        from dhdgs.peer import PeerRuntime
        r=PeerRuntime.__new__(PeerRuntime)
        r.native_controller=object();r.active_count=1
        r.c={'peer':{'max_tombstones':100,'retention_seconds':60}}
        r.records={('q','b'):{'acked':False}};r.tombstones={};r.query_visits={'q':('q','b')}
        r._native_updates_locked([dict(query_request_id='q',branch_id='b',kind='expired')])
        r._native_updates_locked([dict(query_request_id='q',branch_id='b',kind='acked')])
        self.assertEqual(r.active_count,0)
        self.assertFalse(r.records)
        self.assertIn(('q','b'),r.tombstones)
