"""CPU-only parallel extraction, cache recovery and mandatory stage separation."""
import contextlib
import io
import json
from pathlib import Path
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch
import numpy as np
from sampled_seq_ann import sample_queries, search_sampled, SampleProgress, sampling_context, recover_sample
from build_seq_ann_gpu import atomic_json


class SamplingStageTests(unittest.TestCase):
    def test_recover_deleted_metadata(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)
            x=np.random.default_rng(2).normal(size=(30,4)).astype(np.float32)
            parts=[]
            for i in range(2):
                name=f'part_{i:02d}.npy'
                np.save(root/name,x[i*15:(i+1)*15])
                parts.append(dict(id=i,start=i*15,end=(i+1)*15,file=name))
            atomic_json(root/'manifest.json',dict(rows=30,dimension=4,parts=parts))
            args=SimpleNamespace(output=root,k=3,query_count=9,seed=20261013,workers=2,sample_chunk_rows=2)
            work=root/'sampled_search_q9_k3_seed20261013'
            work.mkdir()
            ids=np.sort(np.random.Generator(np.random.PCG64(args.seed)).choice(30,size=9,replace=False))
            target=work/'queries.npy'
            np.save(target,x[ids])
            stamp=target.stat().st_mtime_ns
            with self.assertRaisesRegex(ValueError,'recover-sample'):
                sample_queries(args)
            recover_sample(args)
            self.assertEqual(target.stat().st_mtime_ns,stamp)
            np.testing.assert_array_equal(np.load(work/'query_ids.npy'),ids)
            with patch('sampled_seq_ann.configure_torch',side_effect=RuntimeError('validated before torch')):
                with self.assertRaisesRegex(RuntimeError,'validated before torch'):
                    search_sampled(args)
            # Same valid shape but wrong vectors must never be accepted.
            wrong=x[ids].copy()
            wrong[0]+=1
            np.save(target,wrong)
            with self.assertRaisesRegex(ValueError,'does not match base rows'):
                recover_sample(args)
            self.assertFalse(json.loads((work/'sample_progress.json').read_text())['complete'])
            with self.assertRaisesRegex(ValueError,'Run sample to completion'):
                search_sampled(args)
            with self.assertRaisesRegex(ValueError,'recover-sample'):
                sample_queries(args)
            self.assertNotIn('torch',sys.modules)

    def test_heartbeat_and_resumed_totals(self):
        import threading
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)
            target=root/'part_00.npy'
            checkpoint=target.with_suffix('.progress.json')
            atomic_json(checkpoint,dict(rows_done=3))
            task=(None,None,None,str(target),0,10,13,2)
            reporter=SampleProgress(root,[task],10,13,0.02)
            heartbeat=threading.Event()
            original=reporter.report
            def report():
                original()
                if threading.current_thread() is reporter.thread:
                    heartbeat.set()
            reporter.report=report
            log=io.StringIO()
            with contextlib.redirect_stdout(log), reporter:
                self.assertTrue(heartbeat.wait(2), 'Heartbeat must fire without worker progress')
                status=json.loads((root/'sample_progress.json').read_text())
                self.assertEqual(status['queries_done'],3)
                self.assertEqual(status['percent'],30.0)
                self.assertIsNone(status['eta_seconds'])
                atomic_json(checkpoint,dict(rows_done=5))
                reporter.report()
                status=json.loads((root/'sample_progress.json').read_text())
                self.assertEqual(status['queries_done'],5)
                self.assertGreater(status['rows_per_second'],0)
                reporter.set_phase('merge')
                reporter.merged_to(4)
                status=json.loads((root/'sample_progress.json').read_text())
                self.assertEqual(status['phase'],'merge')
                self.assertEqual(status['percent'],40.0)
            self.assertIn('[sample:extract]',log.getvalue())
            self.assertIn('[sample:merge]',log.getvalue())
            self.assertIn('ETA=',log.getvalue())

    def test_cpu_cache_and_partial_resume(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)
            x=np.arange(137*13,dtype=np.float32).reshape(137,13)+1
            parts=[]
            for i in range(16):
                a,b=137*i//16,137*(i+1)//16
                name=f'part_{i:02d}.npy'
                np.save(root/name,x[a:b])
                parts.append(dict(id=i,start=a,end=b,file=name))
            (root/'manifest.json').write_text(json.dumps(dict(rows=137,dimension=13,parts=parts)))
            (root/'prepare_progress.json').write_text('{"complete":true}')
            args=SimpleNamespace(output=root,k=100,query_count=23,seed=20261013,
                                 workers=4,sample_chunk_rows=2)
            # Initial planning must not open any base NPY header in the parent.
            with patch('build_seq_ann_gpu.validate_array',side_effect=AssertionError('eager base read')):
                with contextlib.redirect_stdout(io.StringIO()):
                    metadata,_,_,_=sampling_context(args,create=True)
                self.assertEqual(metadata.arrays,[])
                with self.assertRaisesRegex(RuntimeError,'Metadata-only'):
                    metadata[:1]
            with patch('sampled_seq_ann.configure_torch',side_effect=AssertionError('must not initialize torch')):
                with self.assertRaisesRegex(ValueError,'sample stage'):
                    search_sampled(args)
                with contextlib.redirect_stdout(io.StringIO()):
                    sample_queries(args)
            self.assertNotIn('torch',sys.modules)
            work=root/'sampled_search_q23_k100_seed20261013'
            ids=np.load(work/'query_ids.npy')
            np.testing.assert_array_equal(np.load(work/'queries.npy'),x[ids])
            stamp=(work/'queries.npy').stat().st_mtime_ns
            sample_queries(args)
            self.assertEqual(stamp,(work/'queries.npy').stat().st_mtime_ns)
            files=sorted((work/'query_parts').glob('*.npy'))
            broken=next(p for p in files if len(np.load(p))>1)
            stamps={p:p.stat().st_mtime_ns for p in files}
            (work/'queries.npy').unlink()
            partial=broken.with_suffix('.partial.npy')
            broken.rename(partial)
            data=np.load(partial,mmap_mode='r+')
            data[1:]=-999
            data.flush()
            del data
            broken.with_suffix('.progress.json').write_text('{"rows_done":1,"complete":false}')
            sample_queries(args)
            np.testing.assert_array_equal(np.load(work/'queries.npy'),x[ids])
            for p in files:
                if p!=broken:
                    self.assertEqual(stamps[p],p.stat().st_mtime_ns)
            self.assertTrue(json.loads((work/'sample_progress.json').read_text())['complete'])
            # Removing prepare's checkpoint must not invalidate a finished dataset.
            (root/'prepare_progress.json').unlink()
            with patch('sampled_seq_ann.configure_torch',side_effect=RuntimeError('validated before torch')):
                with self.assertRaisesRegex(RuntimeError,'validated before torch'):
                    search_sampled(args)
            sample_queries(args)
            # An explicit incomplete checkpoint still blocks search.
            atomic_json(root/'prepare_progress.json',dict(complete=False))
            with self.assertRaisesRegex(ValueError,'Run prepare to completion'):
                search_sampled(args)
            (root/'prepare_progress.json').unlink()
            # Missing checkpoint must not bypass base file length validation.
            damaged=root/parts[0]['file']
            original=damaged.read_bytes()
            with damaged.open('ab') as f:
                f.write(b'bad')
            with self.assertRaisesRegex(ValueError,'Invalid array file length'):
                search_sampled(args)
            original_stat=json.loads((work/'query_cache_config.json').read_text())['base_files'][0]
            damaged.write_bytes(original)
            import os
            os.utime(damaged,ns=(damaged.stat().st_atime_ns,original_stat['mtime_ns']))
            # A modified base must never silently reuse an old query cache.
            path=root/parts[0]['file']
            import os
            st=path.stat()
            os.utime(path,ns=(st.st_atime_ns,st.st_mtime_ns+1000000000))
            with self.assertRaisesRegex(ValueError,'stale query cache'):
                search_sampled(args)


if __name__=='__main__':
    unittest.main()
