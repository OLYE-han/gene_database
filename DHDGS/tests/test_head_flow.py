from concurrent.futures import Future, CancelledError
import threading
import unittest
from dhdgs.head_flow import QueryFlow

class FlowTests(unittest.TestCase):
    def test_one_worker_advances_many_waiting_queries(self):
        flow=QueryFlow(1,8);self.addCleanup(flow.close)
        gates=[Future() for _ in range(8)];started=[threading.Event() for _ in gates];closed=[]
        def query(i):
            try:
                started[i].set();value=yield gates[i]
                return value+1
            finally:closed.append(i)
        results=[flow.submit(query(i)) for i in range(8)]
        self.assertTrue(all(s.wait(2) for s in started))
        rejected=flow.submit(query(0))
        with self.assertRaisesRegex(RuntimeError,'capacity'):rejected.result(1)
        for i in reversed(range(8)):gates[i].set_result(i)
        self.assertEqual([f.result(2) for f in results],list(range(1,9)))
        self.assertEqual(sorted(closed),list(range(8)))

    def test_failure_and_cancel_run_finally_and_release_capacity(self):
        flow=QueryFlow(2,2);self.addCleanup(flow.close)
        started=threading.Event();cleaned=threading.Event();gate=Future()
        def query():
            try:started.set();yield gate
            finally:cleaned.set()
        result=flow.submit(query());self.assertTrue(started.wait(1))
        gate.set_exception(TimeoutError('expired'))
        with self.assertRaises(TimeoutError):result.result(2)
        self.assertTrue(cleaned.is_set())
        gate=Future();started.clear();cleaned.clear()
        result=flow.submit(query());self.assertTrue(started.wait(1))
        flow.close()
        self.assertTrue(result.cancelled());self.assertTrue(cleaned.is_set())

    def test_invalid_yield_does_not_leak_active_record(self):
        flow=QueryFlow(1,1)
        def bad():yield 2
        with self.assertRaises(TypeError):flow.submit(bad()).result(2)
        flow.close()
