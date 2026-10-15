"""Exercise saved global neighbor tables and resume on a 16-file database."""
import contextlib
import io
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
import numpy as np
from build_seq_ann_gpu import search


class SearchTests(unittest.TestCase):
    def test_saved_global_neighbors_and_resume(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            x = np.random.default_rng(23).normal(size=(67, 13)).astype(np.float32)
            x[60] = x[0]
            parts = []
            for i in range(16):
                start, end = len(x)*i//16, len(x)*(i+1)//16
                name = f'part_{i:02d}.npy'
                np.save(root / name, x[start:end])
                parts.append(dict(id=i, start=start, end=end, file=name))
            (root / 'manifest.json').write_text(json.dumps(dict(rows=len(x), dimension=13, parts=parts)))
            (root / 'prepare_progress.json').write_text('{"complete": true}')
            args = SimpleNamespace(output=root, k=7, precision='float64', query_rows=3,
                                   db_rows=3, tile_rows=2, no_prefetch=False)
            with contextlib.redirect_stdout(io.StringIO()):
                search(args, device='cpu')
            reference = x.astype(np.float64)
            reference /= np.linalg.norm(reference, axis=1, keepdims=True)
            scores = reference @ reference.T
            np.fill_diagonal(scores, -np.inf)
            best = np.sort(scores, axis=1)[:, -7:][:, ::-1]
            paths = sorted((root / 'knn').glob('part_*/*.npz'))
            expected_start = 0
            for path in paths:
                with np.load(path) as result:
                    start, end = int(result['query_start']), int(result['query_end'])
                    self.assertEqual(start, expected_start)
                    expected_start = end
                    ids = result['ids']
                    self.assertEqual(ids.dtype, np.uint32)
                    self.assertTrue(np.all(ids != np.arange(start,end)[:,None]))
                    np.testing.assert_allclose(scores[np.arange(start,end)[:,None], ids],
                                               best[start:end], atol=1e-12, rtol=0)
                    np.testing.assert_allclose(result['distances'], 1-best[start:end], atol=1e-12, rtol=0)
            self.assertEqual(expected_start, len(x))
            stamps = {p: p.stat().st_mtime_ns for p in paths}
            # Simulate a missing completed query block: the rest must not be rewritten.
            missing = paths[3]
            missing.unlink()
            with contextlib.redirect_stdout(io.StringIO()):
                search(args, device='cpu')
            self.assertTrue(missing.exists())
            for path in paths:
                if path != missing:
                    self.assertEqual(stamps[path], path.stat().st_mtime_ns)
            self.assertTrue(json.loads((root / 'search_progress.json').read_text())['complete'])


if __name__ == '__main__':
    unittest.main()
