"""Fault injection for lock-free RPC socket probing, no network required."""
import threading
import time
import unittest
import tempfile
from pathlib import Path
from unittest.mock import patch

import yaml

from dhdgs.peer_rpc import ConnectionPool


class FakeSocket:
    def __init__(self, close_hook=None):
        self.closes = 0
        self.close_hook = close_hook

    def close(self):
        self.closes += 1
        if self.close_hook is not None:
            self.close_hook()


class PoolReservationTests(unittest.TestCase):
    def idle(self, pool, endpoint, sock):
        found, entry = pool.borrow(endpoint, time.monotonic() + 2)
        self.assertIsNone(found)
        pool.release(entry, sock, True)
        return entry

    def start(self, fn):
        result = []
        done = threading.Event()
        def run():
            try:
                result.append(('ok', fn()))
            except BaseException as exc:
                result.append(('error', exc))
            finally:
                done.set()
        thread = threading.Thread(target=run, daemon=True)
        thread.start()
        return thread, done, result

    def test_probe_does_not_block_other_endpoint_or_exceed_reservation(self):
        pool = ConnectionPool(1, 2, 'reserved')
        sock = FakeSocket()
        entry = self.idle(pool, ('a', 1), sock)
        probing = threading.Event()
        release = threading.Event()
        def select(*args):
            probing.set()
            release.wait(3)
            return [], [], []
        try:
            with patch('dhdgs.peer_rpc.select.select', side_effect=select):
                a, done, result = self.start(lambda: pool.borrow(('a', 1), time.monotonic() + 2))
                self.assertTrue(probing.wait(2))
                b, other_done, other = self.start(lambda: pool.borrow(('b', 2), time.monotonic() + 1))
                self.assertTrue(other_done.wait(1), 'health probe held the pool lock')
                self.assertEqual(other[0][0], 'ok')
                pool.release(other[0][1][1], None, False)
                self.assertEqual(entry['borrowed'], 1)
                with self.assertRaises(TimeoutError):
                    pool.borrow(('a', 1), time.monotonic())
                release.set()
                self.assertTrue(done.wait(2))
                self.assertEqual(result[0][0], 'ok')
                self.assertIs(result[0][1][0], sock)
                pool.release(entry, sock, True)
                a.join(); b.join()
        finally:
            release.set()
            pool.close()

    def test_probe_reservation_prevents_endpoint_eviction(self):
        pool = ConnectionPool(1, 1, 'reserved')
        entry = self.idle(pool, ('a', 1), FakeSocket())
        probing = threading.Event(); release = threading.Event()
        def select(*args):
            probing.set(); release.wait(3)
            return [], [], []
        try:
            with patch('dhdgs.peer_rpc.select.select', side_effect=select):
                worker, done, result = self.start(lambda: pool.borrow(('a', 1), time.monotonic() + 2))
                self.assertTrue(probing.wait(2))
                with self.assertRaisesRegex(TimeoutError, 'endpoint capacity'):
                    pool.borrow(('b', 2), time.monotonic())
                self.assertIs(pool.entries[('a', 1)], entry)
                release.set(); self.assertTrue(done.wait(2))
                pool.release(entry, result[0][1][0], True)
                worker.join()
        finally:
            release.set(); pool.close()

    def test_close_during_probe_unwinds_reserved_socket(self):
        pool = ConnectionPool(1, 1, 'reserved')
        sock = FakeSocket()
        entry = self.idle(pool, ('a', 1), sock)
        probing = threading.Event(); release = threading.Event()
        def select(*args):
            probing.set(); release.wait(3)
            return [], [], []
        try:
            with patch('dhdgs.peer_rpc.select.select', side_effect=select):
                worker, done, result = self.start(lambda: pool.borrow(('a', 1), time.monotonic() + 2))
                self.assertTrue(probing.wait(2))
                closer, closed, _ = self.start(pool.close)
                self.assertTrue(closed.wait(1))
                self.assertEqual(sock.closes, 0)
                release.set(); self.assertTrue(done.wait(2))
                self.assertEqual(result[0][0], 'error')
                self.assertRegex(str(result[0][1]), 'pool closed')
                self.assertEqual(entry['borrowed'], 0)
                self.assertEqual(sock.closes, 1)
                pool.close()
                self.assertEqual(sock.closes, 1)
                worker.join(); closer.join()
        finally:
            release.set(); pool.close()

    def test_probe_exception_releases_reservation(self):
        pool = ConnectionPool(1, 1, 'reserved')
        sock = FakeSocket()
        entry = self.idle(pool, ('a', 1), sock)
        try:
            with patch('dhdgs.peer_rpc.select.select', side_effect=RuntimeError('probe failed')):
                with self.assertRaisesRegex(RuntimeError, 'probe failed'):
                    pool.borrow(('a', 1), time.monotonic() + 2)
            self.assertEqual(entry['borrowed'], 0)
            self.assertEqual(sock.closes, 1)
            found, same = pool.borrow(('a', 1), time.monotonic() + 2)
            self.assertIsNone(found)
            self.assertIs(same, entry)
            pool.release(entry, None, False)
        finally:
            pool.close()

    def test_multiple_bad_idle_candidates_use_one_reservation(self):
        pool = ConnectionPool(3, 1, 'reserved')
        sockets = [FakeSocket() for _ in range(3)]
        entries = [pool.borrow(('a', 1), time.monotonic() + 2)[1] for _ in sockets]
        for entry, sock in zip(entries, sockets):
            pool.release(entry, sock, True)
        seen = []
        def select(read, *args):
            seen.append(entries[0]['borrowed'])
            if len(seen) == 1:
                raise OSError('closed socket')
            return read, [], []
        try:
            with patch('dhdgs.peer_rpc.select.select', side_effect=select):
                found, entry = pool.borrow(('a', 1), time.monotonic() + 2)
            self.assertIsNone(found)
            self.assertEqual(seen, [1, 1, 1])
            self.assertEqual([sock.closes for sock in sockets], [1, 1, 1])
            self.assertEqual(entry['borrowed'], 1)
            pool.release(entry, None, False)
        finally:
            pool.close()

    def test_release_close_does_not_hold_pool_lock(self):
        pool = ConnectionPool(1, 2, 'reserved')
        closing = threading.Event(); release = threading.Event()
        def block():
            closing.set(); release.wait(3)
        _, entry = pool.borrow(('a', 1), time.monotonic() + 2)
        sock = FakeSocket(block)
        try:
            worker, done, _ = self.start(lambda: pool.release(entry, sock, False))
            self.assertTrue(closing.wait(2))
            borrower, borrowed, result = self.start(lambda: pool.borrow(('b', 2), time.monotonic() + 1))
            self.assertTrue(borrowed.wait(1), 'socket close held the pool lock')
            self.assertEqual(result[0][0], 'ok')
            pool.release(result[0][1][1], None, False)
            release.set(); self.assertTrue(done.wait(2))
            worker.join(); borrower.join()
        finally:
            release.set(); pool.close()

    def test_eviction_close_does_not_block_release(self):
        pool = ConnectionPool(1, 2, 'reserved')
        closing = threading.Event(); release = threading.Event()
        def block():
            closing.set(); release.wait(3)
        old = self.idle(pool, ('old', 1), FakeSocket(block))
        _, active = pool.borrow(('active', 2), time.monotonic() + 2)
        try:
            worker, done, result = self.start(lambda: pool.borrow(('new', 3), time.monotonic() + 2))
            self.assertTrue(closing.wait(2))
            releaser, released, _ = self.start(lambda: pool.release(active, None, False))
            self.assertTrue(released.wait(1), 'eviction close held the pool lock')
            self.assertEqual(old['idle'], [])
            release.set(); self.assertTrue(done.wait(2))
            self.assertEqual(result[0][0], 'ok')
            pool.release(result[0][1][1], None, False)
            worker.join(); releaser.join()
        finally:
            release.set(); pool.close()

    def test_pool_close_does_not_hold_lock_and_release_closes_borrowed_socket(self):
        pool = ConnectionPool(1, 2, 'reserved')
        closing = threading.Event(); release = threading.Event()
        def block():
            closing.set(); release.wait(3)
        self.idle(pool, ('idle', 1), FakeSocket(block))
        _, active = pool.borrow(('active', 2), time.monotonic() + 2)
        borrowed_sock = FakeSocket()
        try:
            worker, done, _ = self.start(pool.close)
            self.assertTrue(closing.wait(2))
            releaser, released, _ = self.start(lambda: pool.release(active, borrowed_sock, True))
            self.assertTrue(released.wait(1), 'pool close held the pool lock')
            self.assertEqual(active['borrowed'], 0)
            self.assertEqual(borrowed_sock.closes, 1)
            release.set(); self.assertTrue(done.wait(2))
            worker.join(); releaser.join()
        finally:
            release.set(); pool.close()

    def test_close_error_still_releases_reservation(self):
        def broken_close():
            raise OSError('close failed')
        pool = ConnectionPool(1, 1, 'reserved')
        sock = FakeSocket(broken_close)
        entry = self.idle(pool, ('a', 1), sock)
        try:
            with patch('dhdgs.peer_rpc.select.select', return_value=([sock], [], [])):
                with self.assertRaisesRegex(OSError, 'close failed'):
                    pool.borrow(('a', 1), time.monotonic() + 2)
            self.assertEqual(entry['borrowed'], 0)
            self.assertEqual(sock.closes, 1)
        finally:
            pool.close()

    def test_default_remains_locked_and_invalid_mode_rejected(self):
        self.assertEqual(ConnectionPool(1, 1).probe_mode, 'locked')
        with self.assertRaises(ValueError):
            ConnectionPool(1, 1, 'invalid')

    def test_config_modes_and_archived_migration(self):
        from dhdgs.config import load_config
        source = Path(__file__).resolve().parents[1] / 'configs/dhdgs.yaml'
        raw = yaml.safe_load(source.read_text())
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'config.yaml'
            for mode in ('locked', 'reserved'):
                raw['peer']['connection_pool']['probe_mode'] = mode
                path.write_text(yaml.safe_dump(raw))
                loaded = load_config(path)
                self.assertEqual(loaded['peer']['connection_pool']['probe_mode'], mode)
            del raw['peer']['connection_pool']
            path.write_text(yaml.safe_dump(raw))
            loaded = load_config(path)
            self.assertEqual(loaded['peer']['connection_pool']['probe_mode'], 'locked')
            self.assertTrue(any('connection_pool' in item for item in loaded['_config_migration']))

    def test_partial_or_invalid_pool_configuration_rejected(self):
        from dhdgs.config import load_config
        source = Path(__file__).resolve().parents[1] / 'configs/dhdgs.yaml'
        raw = yaml.safe_load(source.read_text())
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'config.yaml'
            for group in ({}, {'probe_mode': 'unknown'}, {'probe_mode': 'locked', 'unknown': 1}):
                raw['peer']['connection_pool'] = group
                path.write_text(yaml.safe_dump(raw))
                with self.assertRaises(ValueError):
                    load_config(path)


if __name__ == '__main__':
    unittest.main()
