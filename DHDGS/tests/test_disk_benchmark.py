"""Managed SSD benchmark returns before offline processing, and exports all metrics."""
import json,subprocess,sys,time
from pathlib import Path
import unittest
from unittest.mock import patch
import test_managed_cluster as managed
from dhdgs.benchmark_archive import processor_env

class DiskBenchmarkTests(unittest.TestCase):
    native_head=True
    disk_results=True
    setUpClass=classmethod(managed.ManagedClusterTests.setUpClass.__func__)
    tearDownClass=classmethod(managed.ManagedClusterTests.tearDownClass.__func__)
    setUp=managed.ManagedClusterTests.setUp
    stop=managed.ManagedClusterTests.stop
    def test_all_measurements_precede_reader_and_report(self):
        self.head.configure({},recover=True)
        with patch('subprocess.run',side_effect=AssertionError('No subprocess during measurements')):
            first=self.head.benchmark({});second=self.head.benchmark({})
        finished=time.monotonic_ns()
        self.assertIsNone(self.head.engine.consumer)
        for run in [first,second]:
            p=Path(run['directory'])
            self.assertTrue(run['offline_report_pending'])
            self.assertFalse((p/'queries.csv').exists())
            self.assertTrue((p/'results/writer-0.frames').exists())
            self.assertTrue(run['result_sink']['all_synced'])
            out=subprocess.run([sys.executable,'-m','dhdgs.benchmark_process','--directory',str(p)],env=processor_env(),capture_output=True,text=True,check=True)
            s=json.loads(out.stdout)
            self.assertEqual(s['successful_queries'],3);self.assertEqual(s['failed_queries'],0)
            self.assertGreaterEqual(s['report_started_ns'],finished)
            self.assertTrue(s['latency_definition'].startswith('native_submit_to_native_ready'))
            self.assertGreater(s['head_process_load'][0]['storage_write_bytes'],0)
            self.assertIn('result_queue_ms',s['result_stage_distributions'])
            self.assertEqual(s['qps_scope'],'retrieval_only')
            self.assertGreater(s['post_retrieval_duration_ms'],0)
            self.assertGreater(s['qps'],s['full_run_qps_including_output_and_sync'])
            self.assertTrue((p/'queries.csv').exists())
