"""Full TCP benchmark capture and separate-process reporting."""
import gzip
import json
import os
import threading
from pathlib import Path
from unittest.mock import patch
from dhdgs.benchmark_archive import CaptureArchive
import test_managed_cluster


class NativeMemoryBenchmarkTests(test_managed_cluster.ManagedClusterTests):
    native_head = True

    def test_reporting_does_not_hold_query_or_configuration_gate(self):
        self.head.configure({},recover=True)
        entered=threading.Event();release=threading.Event();out={}
        original=CaptureArchive.flush
        def delayed(archive,during_queries=False):
            if not during_queries:
                entered.set()
                if not release.wait(10):raise TimeoutError('Test report gate')
            return original(archive,during_queries)
        def run():
            try:out['result']=self.head.benchmark({})
            except BaseException as exc:out['error']=exc
        with patch.object(CaptureArchive,'flush',delayed):
            thread=threading.Thread(target=run);thread.start()
            try:
                self.assertTrue(entered.wait(10))
                self.assertIsNone(self.head.busy)
                with self.assertRaisesRegex(RuntimeError,'already running'):self.head.benchmark({})
                self.head.configure({'search.max_expansions':24})
                self.assertEqual(self.head.query(self.x[0],0)['status'],'ok')
            finally:
                release.set();thread.join(15)
            self.assertFalse(thread.is_alive())
        if 'error' in out:raise out['error']
        self.assertEqual(out['result']['successful_queries'],3)

    def test_native_capture_and_report(self):
        self.head.configure({},recover=True)
        run = self.head.benchmark({})
        root = Path(run['directory'])
        with gzip.open(root/'queries.jsonl.gz','rt') as source:
            rows = [json.loads(line) for line in source]
        self.assertEqual(len(rows),3)
        self.assertEqual(run['failed_queries'],0)
        self.assertNotEqual(run['report_processor_pid'],os.getpid())
        self.assertEqual(run['result_capture']['overflow_batches'],0)
        self.assertLessEqual(run['result_capture']['peak_buffer_bytes'],run['result_capture']['memory_limit_bytes'])
        self.assertGreaterEqual(min(q['result_encoding_timestamps']['start_ns'] for q in rows),
                                max(q['client_end_ns'] for q in rows))
        self.assertEqual(run['latency_definition'],'submit_to_api_future_done; archive consumption excluded')
        for q in rows:
            self.assertEqual(q['capture_format'],'native_value_deferred')
            self.assertGreaterEqual(q['result_encoding_timestamps']['start_ns'],q['client_end_ns'])
            self.assertEqual(q['client_end_ns'],q['api_future_done_ns'])
            self.assertGreaterEqual(q['benchmark_observed_end_ns'],q['client_end_ns'])
            self.assertIn('native_ready_ns',q['stats']['native_head_timestamps'])
            self.assertTrue(q['branch_reports'])
        self.assertTrue(all(run['audit'].values()))
        self.assertFalse((root/'capture-unprocessed.bin').exists())


class OverflowMemoryBenchmarkTests(NativeMemoryBenchmarkTests):
    capture_limit = 256

    def test_native_capture_and_report(self):
        self.head.configure({},recover=True)
        run = self.head.benchmark({})
        self.assertEqual(run['failed_queries'],0)
        self.assertEqual(run['result_capture']['oversized_records'],3)
        self.assertLessEqual(run['result_capture']['peak_buffer_bytes'],self.capture_limit)
        self.assertTrue(all(run['audit'].values()))
