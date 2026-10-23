"""Native retrieval/output phase isolation, offline integrity and failure handling."""
import copy,json,tempfile,unittest
from pathlib import Path
import numpy as np
import test_cpp_runtime as runtime_tests
from dhdgs.search import Engine
from dhdgs.disk_results import DiskRecords

class NativeDiskResultTests(unittest.TestCase):
    setUpClass=classmethod(runtime_tests.NativeRuntimeTests.setUpClass.__func__)
    tearDownClass=classmethod(runtime_tests.NativeRuntimeTests.tearDownClass.__func__)
    config=runtime_tests.NativeRuntimeTests.config
    start=runtime_tests.NativeRuntimeTests.start
    def run_case(self,completion='write',writers=4,batch=7):
        c=self.config(True,True)
        c['native_runtime']['result_sink'].update(enabled=True,writers=writers,batch_records=batch,batch_bytes=2048,completion=completion)
        self.start(c);engine=Engine(c);self.addCleanup(engine.close)
        self.assertIsNone(engine.consumer)
        with self.assertRaisesRegex(RuntimeError,'run_to_disk'):engine.search_async(self.x[0],0)
        path=Path(tempfile.mkdtemp(dir=self.root));count=48;ids=np.arange(count,dtype=np.uint64)
        manifest=engine.run_to_disk(self.x[:count],ids,path/'results',16)
        self.assertTrue(manifest['all_synced']);self.assertFalse(manifest['python_per_query_consumer'])
        self.assertEqual(manifest['completion'],'native_ready')
        self.assertEqual(manifest['output_completion'],completion)
        self.assertEqual(manifest['output_mode'],'deferred')
        self.assertFalse(manifest['output_backpressure'])
        self.assertLessEqual(manifest['peak_retained_bytes_estimate'],manifest['max_retained_bytes'])
        self.assertGreaterEqual(manifest['all_retained_ns'],manifest['last_completion_ns'])
        self.assertGreaterEqual(manifest['output_started_ns'],manifest['all_retained_ns'])
        self.assertGreaterEqual(manifest['all_synced_ns'],manifest['last_output_completion_ns'])
        self.assertEqual(sum(w['records'] for w in manifest['writers']),count)
        self.assertGreater(sum(w['records']>0 for w in manifest['writers']),1 if writers>1 else 0)
        for w in manifest['writers']:self.assertGreaterEqual(w['final_sync_done_ns'],manifest['last_completion_ns'])
        context=dict(selection=list(map(int,ids)),query_ids=list(map(int,ids)),truth=[[0] for _ in ids],config_revision='test',max_message_bytes=c['network']['max_message_bytes'],offered_ns=manifest['offered_ns'])
        (path/'disk_manifest.json').write_text(json.dumps(manifest));(path/'capture_context.json').write_text(json.dumps(context))
        records=list(DiskRecords(path));self.assertEqual(len(records),count)
        self.assertEqual({q['job'] for q in records},set(range(count)))
        self.assertEqual(max(q['client_end_ns'] for q in records),manifest['last_completion_ns'])
        for q in records:
            self.assertEqual(q['status'],'ok');self.assertNotIn('api_future_done_ns',q)
            self.assertEqual(q['stats']['node_revisit_count'],0)
            self.assertLessEqual(q['stats']['expanded'],c['search']['max_expansions'])
            self.assertAlmostEqual(sum(q['stats']['latency_breakdown_ms'].values()),q['client_wall_ms'],places=4)
            self.assertEqual(q['client_end_ns'],q['stats']['native_head_timestamps']['native_ready_ns'])
            # All 48 retrievals (three admission windows) finish before the
            # first serialization or write, even with per-batch fdatasync.
            self.assertGreaterEqual(q['result_sink_timestamps']['encoding_start_ns'],manifest['last_completion_ns'])
            self.assertGreater(q['result_sink_timestamps']['write_start_ns'],q['client_end_ns'])
            self.assertEqual(q['stats']['result_queue_ms'],0)
            self.assertNotIn('result_write_ms',q['stats']['latency_breakdown_ms'])
            self.assertIn('result_write_ms',q['stats']['postprocessing_breakdown_ms'])
        self.assertEqual(engine.runtime.metrics()['completed_pending'],0)
        self.assertEqual(engine.runtime.metrics()['active'],0)
        with self.assertRaisesRegex(ValueError,'already exists'):
            engine.run_to_disk(self.x[:1],ids[:1],path/'results',1)
        # Exact same loaded data services, reference API path, sequential comparison.
        legacy=copy.deepcopy(c);legacy['native_runtime']['result_sink']['enabled']=False
        ref=Engine(legacy)
        try:
            for q in records[:6]:self.assertEqual(q['ids'],ref.search(self.x[q['job']],q['job'])['ids'])
        finally:ref.close()
        return path,manifest
    def test_parallel_deferred_write_and_offline_reconstruction(self):
        self.run_case()
    def test_batch_fdatasync_completion(self):
        self.run_case('fdatasync',2,3)
    def test_truncated_files_are_rejected(self):
        path,m=self.run_case(writers=2)
        filename=Path(m['directory'])/'writer-0.frames'
        with filename.open('r+b') as f:f.truncate(filename.stat().st_size-1)
        with self.assertRaisesRegex(ValueError,'Truncated'):list(DiskRecords(path))
    def test_writer_failure_propagates_without_hanging(self):
        c=self.config(True,True);c['native_runtime']['result_sink']['enabled']=True
        self.start(c);engine=Engine(c);self.addCleanup(engine.close)
        path=Path(tempfile.mkdtemp(dir=self.root))
        # Files cannot be created under a missing parent: startup errors must
        # wake producer/other writers instead of waiting forever.
        with self.assertRaises(RuntimeError):
            engine.run_to_disk(self.x[:4],np.arange(4,dtype=np.uint64),path/'missing'/'results',2)

    def test_retention_budget_fails_explicitly_and_drains_accepted_queries(self):
        c=self.config(True,True)
        c['native_runtime']['result_sink'].update(enabled=True,max_retained_bytes=32768)
        self.start(c);engine=Engine(c);self.addCleanup(engine.close)
        path=Path(tempfile.mkdtemp(dir=self.root))
        with self.assertRaisesRegex(RuntimeError,'retention budget exceeded'):
            engine.run_to_disk(self.x[:48],np.arange(48,dtype=np.uint64),path/'budget',16)
        self.assertEqual(engine.runtime.metrics()['active'],0)
        self.assertEqual(engine.runtime.metrics()['completed_pending'],0)
        self.assertFalse(list((path/'budget').glob('*.frames')))
        # Failure leaves no foreign completed results or callbacks for the next run.
        engine.c['native_runtime']['result_sink']['max_retained_bytes']=8<<30
        m=engine.run_to_disk(self.x[:4],np.arange(4,dtype=np.uint64),path/'recovery',2)
        self.assertEqual(m['count'],4)

    def test_retrieval_errors_release_slots_without_a_consumer(self):
        c=self.config(True,True);c['native_runtime']['result_sink']['enabled']=True
        self.start(c);engine=Engine(c);self.addCleanup(engine.close)
        path=Path(tempfile.mkdtemp(dir=self.root))
        original=engine.runtime.metrics
        # Nonfinite input exercises the retrieval-error completion callback.
        x=self.x[:8].copy();x[2,0]=float('nan')
        m=engine.run_to_disk(x,np.arange(8,dtype=np.uint64),path/'errors',2)
        self.assertEqual(m['count'],8)
        self.assertEqual(original()['active'],0)
        self.assertEqual(original()['completed_pending'],0)
