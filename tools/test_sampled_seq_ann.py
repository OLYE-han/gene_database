"""Sampled queries must search the full base with reproducible IDs and exact top100."""
import contextlib
import io
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
import numpy as np
from sampled_seq_ann import search_sampled, sample_queries
from build_seq_ann_gpu import ShardedBase
from unittest.mock import patch


class SampledTests(unittest.TestCase):
    def test_full_base_top100_and_resume(self):
        for precision,metric in [(p,m) for p in ['float32','float64'] for m in ['cosine','euclidean']]:
            with self.subTest(precision=precision,metric=metric), tempfile.TemporaryDirectory() as directory:
                root=Path(directory)
                x=np.random.default_rng(45).normal(size=(137,13)).astype(np.float32)
                x[130]=x[0]
                parts=[]
                for i in range(16):
                    start,end=len(x)*i//16,len(x)*(i+1)//16
                    name=f'part_{i:02d}.npy'
                    np.save(root/name,x[start:end])
                    parts.append(dict(id=i,start=start,end=end,file=name))
                (root/'manifest.json').write_text(json.dumps(dict(rows=len(x),dimension=13,parts=parts)))
                (root/'prepare_progress.json').write_text('{"complete":true}')
                args=SimpleNamespace(output=root,k=100,query_count=23,seed=20261013,
                                     precision=precision,metric=metric,query_rows=7,db_rows=11,tile_rows=5,
                                     no_prefetch=False,workers=2,sample_chunk_rows=3)
                with contextlib.redirect_stdout(io.StringIO()):
                    sample_queries(args)
                cache_work=root/'sampled_search_q23_k100_seed20261013'
                if metric == 'euclidean':
                    # Historical cosine state must neither block nor be overwritten by L2.
                    (cache_work/'search_config.json').write_text('{"legacy_cosine":true}')
                    (cache_work/'progress.json').write_text('{"legacy_cosine":true}')
                with contextlib.redirect_stdout(io.StringIO()), patch.object(ShardedBase,'gather',side_effect=AssertionError('search must use query cache')):
                    search_sampled(args,device='cpu')
                work=root/'sampled_search_q23_k100_seed20261013'
                qids=np.load(work/'query_ids.npy')
                np.testing.assert_array_equal(np.load(work/'queries.npy'),x[qids])
                expected_qids=np.sort(np.random.Generator(np.random.PCG64(args.seed)).choice(
                    len(x),size=23,replace=False))
                np.testing.assert_array_equal(qids,expected_qids)
                normalized=x.astype(np.float64)
                normalized/=np.linalg.norm(normalized,axis=1,keepdims=True)
                reference=normalized[qids]@normalized.T
                if metric == 'euclidean':
                    reference=-np.linalg.norm(x[qids,None,:].astype(np.float64)-x[None,:,:].astype(np.float64),axis=2)
                    work=work/'euclidean'
                reference[np.arange(len(qids)),qids]=-np.inf
                best=np.sort(reference,axis=1)[:,-100:][:,::-1]
                paths=sorted((work/'knn').glob('*.npz'))
                seen=0
                for path in paths:
                    with np.load(path) as result:
                        start,end=int(result['query_start']),int(result['query_end'])
                        self.assertEqual(start,seen)
                        seen=end
                        np.testing.assert_array_equal(result['query_ids'],qids[start:end])
                        ids=result['ids']
                        self.assertEqual(ids.shape,(end-start,100))
                        self.assertTrue(np.all(ids!=qids[start:end,None]))
                        self.assertTrue(all(len(set(row))==100 for row in ids.tolist()))
                        self.assertTrue(np.any(~np.isin(ids,qids)))
                        selected=reference[np.arange(start,end)[:,None],ids]
                        tol=(3e-3 if metric=='euclidean' else 3e-6) if precision=='float32' else (1e-7 if metric=='euclidean' else 1e-12)
                        np.testing.assert_allclose(selected,best[start:end],atol=tol,rtol=0)
                        np.testing.assert_allclose(result['distances'],-selected if metric=='euclidean' else 1-selected,atol=tol,rtol=0)
                        self.assertEqual(str(result['metric']),metric)
                        self.assertTrue(np.all(np.diff(result['distances'],axis=1)>=0))
                self.assertEqual(seen,23)
                stamps={p:p.stat().st_mtime_ns for p in paths}
                paths[-1].unlink()
                with contextlib.redirect_stdout(io.StringIO()):
                    search_sampled(args,device='cpu')
                for p in paths[:-1]:
                    self.assertEqual(stamps[p],p.stat().st_mtime_ns)
                np.testing.assert_array_equal(np.load(root/'sampled_search_q23_k100_seed20261013'/'query_ids.npy'),qids)
                self.assertTrue(json.loads((work/'progress.json').read_text())['complete'])
                if metric == 'euclidean':
                    self.assertEqual(json.loads((cache_work/'progress.json').read_text()),{'legacy_cosine':True})
                    self.assertEqual(json.loads((cache_work/'search_config.json').read_text()),{'legacy_cosine':True})
                    config=json.loads((work/'search_config.json').read_text())
                    self.assertEqual(config['metric'],'euclidean')
                    self.assertFalse(config['squared_distances'])
                args.query_count=138
                with self.assertRaisesRegex(ValueError,'query-count'):
                    self.quiet_search(args)

    def quiet_search(self,args):
        with contextlib.redirect_stdout(io.StringIO()):
            search_sampled(args,device='cpu')


if __name__=='__main__':
    unittest.main()
