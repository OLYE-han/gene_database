"""Correctness under concurrent reads, accounting, cache eviction and routing."""
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import tempfile
import threading
import unittest
from unittest.mock import patch

import numpy as np
from threadpoolctl import threadpool_limits

from dhdgs import _native
from dhdgs.index import Shard, build
from test_system import fixture


class ForbiddenLock:
    def __enter__(self):
        raise AssertionError('Disabled cache must not acquire its mutex')

    def __exit__(self, *args):
        pass


class ReadConcurrencyTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temp = tempfile.TemporaryDirectory()
        cls.root = Path(cls.temp.name)
        cls.c, cls.x = fixture(cls.root)
        with threadpool_limits(limits=1):
            build(cls.c)
        cls.directory = cls.root/'out/index/shard_0'

    @classmethod
    def tearDownClass(cls):
        cls.temp.cleanup()

    def setUp(self):
        self.shard = Shard(self.directory, 0)
        self.addCleanup(self.shard.close)
        self.ids = list(map(int, self.shard.ids[:4]))
        self.epoch = self.shard.meta['epoch']

    def test_concurrent_expansion_without_cache_mutex_and_exact_accounting(self):
        self.shard.cache_lock = ForbiddenLock()
        expected = self.shard.expand(self.x[0], self.ids, self.epoch)
        before = self.shard.metrics()['counters']
        original = self.shard._expand
        barrier = threading.Barrier(8)

        def synchronized(*args, **kwargs):
            barrier.wait(timeout=10)
            return original(*args, **kwargs)

        with patch.object(self.shard, '_expand', side_effect=synchronized):
            with ThreadPoolExecutor(max_workers=8) as pool:
                replies = list(pool.map(lambda _: self.shard.expand(self.x[0], self.ids, self.epoch), range(64)))
        for reply in replies:
            for key in ('expanded', 'neighbors', 'parents'):
                self.assertEqual(reply[key], expected[key])
        after = self.shard.metrics()['counters']
        self.assertEqual(after['requests']-before['requests'], 64)
        self.assertEqual(after['completed']-before['completed'], 64)
        self.assertEqual(after['active'], 0)
        self.assertEqual(after['failed'], 0)
        self.assertEqual(after['peak_active'], 8)
        for key in ('file_read_bytes', 'pread_calls', 'scanned_edges',
                    'exact_distance_evaluations', 'rabitq_evaluations'):
            self.assertEqual(after[key]-before[key], 64*expected['stats'][key])
        self.assertIsNone(self.shard.cached_record(self.ids[0]))
        self.shard.cache_record(self.ids[0], b'x'*self.shard.meta['record_bytes'])
        self.assertEqual(self.shard.cache_size, 0)

    def test_failed_expansion_is_counted_and_releases_active(self):
        with self.assertRaisesRegex(ValueError, 'epoch'):
            self.shard.expand(self.x[0], self.ids, 'wrong epoch')
        counts = self.shard.metrics()['counters']
        self.assertEqual((counts['requests'], counts['completed'], counts['failed'], counts['active']), (1, 0, 1, 0))

    def test_enabled_lru_remains_bounded_under_concurrent_eviction(self):
        stride = self.shard.meta['record_bytes']
        self.shard.cache_bytes = 2*stride

        def read(i):
            stats = dict(cache_hits=0, pread_calls=0, file_read_bytes=0)
            gid = self.ids[i % len(self.ids)]
            vector, _ = self.shard.read(gid, stats)
            np.testing.assert_array_equal(vector, self.x[gid])

        with ThreadPoolExecutor(max_workers=8) as pool:
            list(pool.map(read, range(256)))
        self.assertEqual(self.shard.cache_size, sum(map(len, self.shard.cache.values())))
        self.assertLessEqual(self.shard.cache_size, self.shard.cache_bytes)

    def test_slow_monitoring_does_not_hold_expansion_mutex(self):
        entered, release = threading.Event(), threading.Event()

        def slow_snapshot():
            entered.set()
            if not release.wait(10):
                raise TimeoutError('Test failed to release monitoring')
            return {}

        with ThreadPoolExecutor(max_workers=2) as pool:
            with patch('dhdgs.index.process_snapshot', side_effect=slow_snapshot):
                monitoring = pool.submit(self.shard.metrics)
                try:
                    self.assertTrue(entered.wait(5))
                    future = pool.submit(self.shard.expand, self.x[0], self.ids, self.epoch)
                    self.assertEqual(len(future.result(timeout=5)['expanded']), len(self.ids))
                finally:
                    release.set()
                monitoring.result(timeout=5)


class NativeReadConcurrencyTests(unittest.TestCase):
    def test_atomic_accounting_and_rejected_metrics_are_not_partially_applied(self):
        counters = _native.ExpansionCounters(['bytes'])
        self.assertTrue(counters.is_lock_free)
        counters.begin()
        with self.assertRaisesRegex(ValueError, 'Unknown'):
            counters.finish(True, {'bytes': 10, 'unregistered': 1})
        self.assertEqual(counters.snapshot()['bytes'], 0)
        self.assertEqual(counters.snapshot()['active'], 1)
        counters.finish(False, {})
        self.assertEqual(counters.snapshot()['failed'], 1)
        with self.assertRaises(RuntimeError):
            counters.finish(False, {})
        with self.assertRaises(ValueError):
            _native.ExpansionCounters(['active'])

    def test_concurrent_hnsw_queries_use_fixed_ef(self):
        x = np.random.default_rng(31).normal(size=(64, 64)).astype('f4')
        ids = np.arange(64, dtype='u8') + np.uint64(2**63)
        router = _native.Router(64, 64, 8, 64, 31)
        router.add(x, ids)
        # Concurrent first queries also exercise the configure-once transition.
        with ThreadPoolExecutor(max_workers=16) as pool:
            replies = list(pool.map(lambda i: router.search(x[i % 64], 1, 64), range(256)))
        self.assertEqual(replies, [[int(ids[i % 64])] for i in range(256)])
        with self.assertRaisesRegex(ValueError, 'fixed'):
            router.search(x[0], 1, 32)
        with self.assertRaisesRegex(ValueError, 'modify'):
            router.add(x[:1], ids[:1])
        with self.assertRaisesRegex(ValueError, 'reload'):
            router.load('unused.hnsw')


if __name__ == '__main__':
    unittest.main()
