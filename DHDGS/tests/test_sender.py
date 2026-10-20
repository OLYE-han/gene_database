import threading
import unittest
from concurrent.futures import CancelledError
from unittest.mock import patch

from dhdgs.sender import QueuedSender


class SenderTests(unittest.TestCase):
    def test_prestarted_workers_queue_bound_and_failure_recovery(self):
        started = threading.Event()
        release = threading.Event()
        sender = QueuedSender(1, 1, 'test-sender')
        def blocked():
            started.set()
            release.wait(3)
            return threading.get_ident()
        try:
            with patch.object(threading.Thread, 'start', side_effect=AssertionError('lazy thread creation')):
                running = sender.submit(blocked)
                self.assertTrue(started.wait(1))
                queued = sender.submit(lambda: 42)
                with self.assertRaisesRegex(RuntimeError, 'capacity'):
                    sender.submit(lambda: None)
                release.set()
                self.assertNotEqual(running.result(2), threading.get_ident())
                self.assertEqual(queued.result(2), 42)
                def fail():
                    raise ValueError('send failed')
                with self.assertRaisesRegex(ValueError, 'send failed'):
                    sender.submit(fail).result(2)
                self.assertEqual(sender.submit(lambda: 43).result(2), 43)
            self.assertEqual(sender.metrics()['capacity_rejections'], 1)
        finally:
            release.set()
            sender.shutdown()

    def test_shutdown_cancels_waiting_work_without_inline_socket_work(self):
        started = threading.Event()
        release = threading.Event()
        sender = QueuedSender(1, 1, 'test-sender')
        try:
            first = sender.submit(lambda: (started.set(), release.wait(3)))
            self.assertTrue(started.wait(1))
            pending = sender.submit(lambda: self.fail('cancelled send executed'))
            sender.shutdown(wait=False, cancel_futures=True)
            with self.assertRaises(CancelledError):
                pending.result()
            with self.assertRaisesRegex(RuntimeError, 'closed'):
                sender.submit(lambda: None)
            release.set()
            first.result(2)
        finally:
            release.set()
            sender.shutdown()
        self.assertTrue(all(not t.is_alive() for t in sender.threads))
