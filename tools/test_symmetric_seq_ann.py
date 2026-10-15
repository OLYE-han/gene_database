"""Exact symmetry, pair-count reduction, and crash consistency tests."""
import contextlib
import io
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
import numpy as np
from symmetric_seq_ann import search_symmetric


class SymmetricTests(unittest.TestCase):
    def fixture(self, root, precision='float64', parts=3):
        x = np.random.default_rng(29).normal(size=(23,7)).astype(np.float32)
        x[21] = x[0]
        entries = []
        for i in range(parts):
            start, end = len(x)*i//parts,len(x)*(i+1)//parts
            path = f'part_{i}.npy'
            np.save(root/path,x[start:end])
            entries.append(dict(id=i,start=start,end=end,file=path))
        (root/'manifest.json').write_text(json.dumps(dict(rows=len(x),dimension=7,parts=entries)))
        (root/'prepare_progress.json').write_text('{"complete":true}')
        args = SimpleNamespace(output=root,k=7,precision=precision,query_rows=6,
                               db_rows=3,tile_rows=2,no_prefetch=False)
        return x,args

    def run_search(self,args,**kwargs):
        with contextlib.redirect_stdout(io.StringIO()):
            search_symmetric(args,device='cpu',**kwargs)

    def verify(self,root,x,args):
        y = x.astype(np.float64)
        y /= np.linalg.norm(y,axis=1,keepdims=True)
        scores = y@y.T
        np.fill_diagonal(scores,-np.inf)
        expected = np.sort(scores,axis=1)[:,-args.k:][:,::-1]
        seen = 0
        tol = 1e-12 if args.precision == 'float64' else 3e-6
        paths = sorted((root/'symmetric_search/knn').glob('part_*/*.npz'))
        for path in paths:
            with np.load(path) as result:
                start,end = int(result['query_start']),int(result['query_end'])
                self.assertEqual(start,seen)
                seen=end
                ids=result['ids']
                self.assertTrue(np.all(ids != np.arange(start,end)[:,None]))
                self.assertTrue(all(len(set(row)) == args.k for row in ids.tolist()))
                selected=scores[np.arange(start,end)[:,None],ids]
                np.testing.assert_allclose(selected,expected[start:end],atol=tol,rtol=0)
                np.testing.assert_allclose(result['distances'],1-selected,atol=tol,rtol=0)
        self.assertEqual(seen,len(x))
        state=json.loads((root/'symmetric_search/progress.json').read_text())
        self.assertTrue(state['complete'])
        blocks=len(state['versions'])
        self.assertEqual(state['completed_pairs'],blocks*(blocks+1)//2)
        manifest=json.loads((root/'manifest.json').read_text())
        extra=0
        for p in manifest['parts']:
            for start in range(p['start'],p['end'],args.query_rows):
                end=min(start+args.query_rows,p['end'])
                extra+=sum(min(args.tile_rows,end-t)**2 for t in range(start,end,args.tile_rows))
        self.assertEqual(state['evaluated_pairs'],(len(x)**2+extra)//2)
        self.assertLess(state['evaluated_pairs'],len(x)**2)
        self.assertEqual(len(list((root/'symmetric_search/state').glob('*.npz'))),blocks)
        return paths

    def test_exact_and_resume(self):
        for precision in ['float32','float64']:
            with self.subTest(precision=precision),tempfile.TemporaryDirectory() as directory:
                root=Path(directory)
                x,args=self.fixture(root,precision)
                self.run_search(args,stop_after_pairs=3)
                self.run_search(args)
                paths=self.verify(root,x,args)
                stamps={p:p.stat().st_mtime_ns for p in paths}
                missing=paths[1]
                missing.unlink()
                self.run_search(args)
                self.verify(root,x,args)
                for path in paths:
                    if path!=missing:
                        self.assertEqual(stamps[path],path.stat().st_mtime_ns)

    def test_crash_atomic_two_sided_update(self):
        for stage in ['after_left_write','before_commit','after_commit']:
            with self.subTest(stage=stage),tempfile.TemporaryDirectory() as directory:
                root=Path(directory)
                x,args=self.fixture(root)
                def fail(where,i,j):
                    if where==stage and i==0 and j==1:
                        raise RuntimeError('simulated process crash')
                with self.assertRaisesRegex(RuntimeError,'simulated'):
                    self.run_search(args,fault_hook=fail)
                self.run_search(args)
                self.verify(root,x,args)

    def test_single_diagonal_block_and_no_prefetch(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)
            x,args=self.fixture(root,parts=1)
            args.query_rows=100
            args.no_prefetch=True
            self.run_search(args)
            self.verify(root,x,args)
            self.run_search(args,benchmark=True)
            report=json.loads((root/'symmetric_search/benchmark.json').read_text())
            self.assertEqual(report['evaluated_pairs'],report['total_evaluated_pairs'])


if __name__=='__main__':
    unittest.main()
