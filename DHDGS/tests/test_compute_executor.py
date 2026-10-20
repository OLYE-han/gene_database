import threading
import unittest
from concurrent.futures import CancelledError, ThreadPoolExecutor
from unittest.mock import patch

from dhdgs.compute_executor import FixedComputeExecutor, prestart_executor
from dhdgs.executor_trace import TracedComputeExecutor


class ComputeExecutorTests(unittest.TestCase):
    def test_prestarted_submission_never_creates_threads_or_uses_standard_submit(self):
        pool = FixedComputeExecutor(3, 8)
        try:
            self.assertEqual(pool.metrics()['ready_workers'], 3)
            trace = {}
            with patch.object(threading.Thread, 'start', side_effect=AssertionError('late thread')):
                with patch.object(ThreadPoolExecutor, 'submit', side_effect=AssertionError('global submit')):
                    self.assertEqual(pool.submit_traced(trace, lambda x: x + 1, 4).result(2), 5)
            self.assertLessEqual(trace['executor_enqueue_ns'], trace['executor_dequeue_ns'])
        finally:
            pool.shutdown()

    def test_queue_full_rejects_and_capacity_recovers(self):
        pool = FixedComputeExecutor(1, 1)
        entered = threading.Event()
        release = threading.Event()
        def blocked():
            entered.set()
            release.wait(3)
        try:
            running = pool.submit(blocked)
            self.assertTrue(entered.wait(2))
            queued = pool.submit(lambda: 'second')
            with self.assertRaisesRegex(RuntimeError, 'capacity'):
                pool.submit(lambda: None)
            self.assertEqual(pool.metrics()['capacity_rejections'], 1)
            release.set()
            running.result(2)
            self.assertEqual(queued.result(2), 'second')
            self.assertEqual(pool.submit(lambda: 'third').result(2), 'third')
        finally:
            release.set()
            pool.shutdown()

    def test_cancelled_queued_call_never_executes(self):
        pool = FixedComputeExecutor(1, 2)
        entered = threading.Event()
        release = threading.Event()
        calls = []
        try:
            first = pool.submit(lambda: (entered.set(), release.wait(3)))
            self.assertTrue(entered.wait(2))
            cancelled = pool.submit(lambda: calls.append('unexpected'))
            self.assertTrue(cancelled.cancel())
            release.set()
            first.result(2)
            with self.assertRaises(CancelledError):
                cancelled.result(2)
            self.assertEqual(pool.submit(lambda: 'alive').result(2), 'alive')
            self.assertFalse(calls)
        finally:
            release.set()
            pool.shutdown()

    def test_task_and_callback_baseexception_do_not_kill_worker(self):
        pool = FixedComputeExecutor(1, 4)
        entered = threading.Event()
        release = threading.Event()
        def bad_task():
            raise SystemExit('task failure')
        def bad_callback(future):
            # A callback can inspect pool metrics: no queue lock is held.
            pool.metrics()
            raise SystemExit('callback failure')
        try:
            with self.assertRaises(SystemExit):
                pool.submit(bad_task).result(2)
            blocker = pool.submit(lambda: (entered.set(), release.wait(3)))
            self.assertTrue(entered.wait(2))
            result = pool.submit(lambda: 7)
            result.add_done_callback(bad_callback)
            release.set()
            blocker.result(2)
            self.assertEqual(result.result(2), 7)
            self.assertEqual(pool.submit(lambda: 8).result(2), 8)
            self.assertEqual(pool.metrics()['callback_errors'], 1)
        finally:
            release.set()
            pool.shutdown()

    def test_shutdown_drains_and_rejects_new_work(self):
        pool = FixedComputeExecutor(2, 8)
        futures = [pool.submit(lambda x: x, i) for i in range(6)]
        pool.shutdown()
        self.assertEqual([f.result() for f in futures], list(range(6)))
        self.assertTrue(all(not t.is_alive() for t in pool._threads))
        with self.assertRaisesRegex(RuntimeError, 'closed'):
            pool.submit(lambda: None)
        pool.shutdown()

    def test_shutdown_cancel_handles_throwing_callback_and_running_work(self):
        pool = FixedComputeExecutor(1, 3)
        entered = threading.Event()
        release = threading.Event()
        def bad_callback(future):
            pool.metrics()
            raise SystemExit('callback')
        try:
            active = pool.submit(lambda: (entered.set(), release.wait(3)))
            self.assertTrue(entered.wait(2))
            a = pool.submit(lambda: 1)
            b = pool.submit(lambda: 2)
            a.add_done_callback(bad_callback)
            pool.shutdown(wait=False, cancel_futures=True)
            self.assertTrue(a.cancelled())
            self.assertTrue(b.cancelled())
            self.assertFalse(active.cancelled())
            release.set()
            pool.shutdown()
            self.assertEqual(pool.metrics()['callback_errors'], 1)
        finally:
            release.set()
            pool.shutdown()

    def test_partial_startup_failure_cleans_started_threads(self):
        original = threading.Thread.start
        created = []
        def start(thread):
            if created:
                raise RuntimeError('thread limit')
            original(thread)
            created.append(thread)
        with patch.object(threading.Thread, 'start', start):
            with self.assertRaisesRegex(RuntimeError, 'thread limit'):
                FixedComputeExecutor(3, 2)
        self.assertTrue(created)
        self.assertTrue(all(not t.is_alive() for t in created))

    def test_invalid_capacity(self):
        for workers, capacity in [(0, 1), (1, 0), (True, 1), (1, 1.5)]:
            with self.assertRaises(ValueError):
                FixedComputeExecutor(workers, capacity)

    def test_prestart_ablation_starts_full_standard_pool(self):
        pool = TracedComputeExecutor(max_workers=3)
        try:
            stats = prestart_executor(pool, 3)
            self.assertEqual(stats['workers'], 3)
            self.assertEqual(len(pool._threads), 3)
            with patch.object(threading.Thread, 'start', side_effect=AssertionError('late thread')):
                self.assertEqual(pool.submit(lambda: 9).result(2), 9)
        finally:
            pool.shutdown()

    def test_prestart_failure_releases_existing_barrier_tasks(self):
        pool = TracedComputeExecutor(max_workers=3)
        original = pool.submit
        count = 0
        def submit(*args, **kwargs):
            nonlocal count
            count += 1
            if count == 2:
                raise RuntimeError('submit failed')
            return original(*args, **kwargs)
        try:
            with patch.object(pool, 'submit', side_effect=submit):
                with self.assertRaisesRegex(RuntimeError, 'submit failed'):
                    prestart_executor(pool, 3)
            self.assertEqual(pool.submit(lambda: 10).result(2), 10)
            with self.assertRaises(ValueError):
                prestart_executor(pool, 4)
        finally:
            pool.shutdown()


if __name__ == '__main__':
    unittest.main()
