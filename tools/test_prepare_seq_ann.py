"""Integration tests for independent part files, resume, and global exact neighbors."""
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

import h5py
import numpy as np
from build_seq_ann_gpu import ShardedBase, database_blocks

SCRIPT = Path(__file__).with_name('build_seq_ann_gpu.py')


class PrepareTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.source = self.root / 'source'
        self.source.mkdir()
        arrays = []
        for index, size in [(0, 9), (2, 7), (10, 11)]:
            x = np.arange(size * 5, dtype=np.float32).reshape(size, 5) + 1 + index * 100
            arrays.append(x)
            with h5py.File(self.source / f'shard_{index}.h5', 'w') as f:
                f.create_dataset('embeddings', data=x, compression='gzip', chunks=(3, 5))
        self.expected = np.concatenate(arrays)[:20]

    def run_prepare(self, output, workers=4, parts=4, check=True):
        result = subprocess.run(
            [sys.executable, str(SCRIPT), 'prepare', '--source', str(self.source),
             '--output', str(output), '--rows', '20', '--copy-rows', '4',
             '--workers', str(workers), '--parts', str(parts)], capture_output=True, text=True,
            env={**os.environ, 'OPENBLAS_NUM_THREADS': '1', 'OMP_NUM_THREADS': '1'})
        if check and result.returncode:
            self.fail(result.stdout + result.stderr)
        return result

    def read_base(self, output):
        return ShardedBase(output, json.loads((output / 'manifest.json').read_text()))

    def test_parallel_serial_and_partial_resume(self):
        serial, parallel = self.root / 'serial', self.root / 'parallel'
        self.run_prepare(serial, 1)
        self.run_prepare(parallel)
        base = self.read_base(parallel)
        np.testing.assert_array_equal(base[:], self.expected)
        np.testing.assert_array_equal(base[3:17], self.expected[3:17])
        files = sorted((parallel / 'base').glob('part_*.npy'))
        self.assertEqual(len(files), 4)
        self.assertFalse((parallel / 'base.npy').exists())
        self.assertFalse((parallel / '.lock').exists())
        for path in files:
            self.assertEqual((serial / 'base' / path.name).read_bytes(), path.read_bytes())
        stamps = [p.stat().st_mtime_ns for p in files]
        self.run_prepare(parallel)
        self.assertEqual(stamps, [p.stat().st_mtime_ns for p in files])
        del base
        path = files[2]
        partial = path.with_suffix('.partial.npy')
        path.rename(partial)
        x = np.load(partial, mmap_mode='r+')
        x[2:] = -999
        x.flush()
        del x
        path.with_suffix('.progress.json').write_text(json.dumps(dict(rows_done=2, complete=False)))
        self.run_prepare(parallel, workers=2)
        np.testing.assert_array_equal(self.read_base(parallel)[:], self.expected)
        for i in [0, 1, 3]:
            self.assertEqual(stamps[i], files[i].stat().st_mtime_ns)
        self.assertTrue(json.loads((parallel / 'prepare_progress.json').read_text())['complete'])
        for prefetch in [False, True]:
            blocks = list(database_blocks(self.read_base(parallel), 3, prefetch))
            np.testing.assert_array_equal(np.concatenate([a for _, a in blocks]), self.expected)
            self.assertEqual([s for s, _ in blocks], [0, 3, 5, 8, 10, 13, 15, 18])

    def test_sixteen_uneven_parts(self):
        output = self.root / 'sixteen'
        self.run_prepare(output, workers=16, parts=16)
        base = self.read_base(output)
        self.assertEqual(len(base.parts), 16)
        self.assertEqual([p['end']-p['start'] for p in base.parts].count(2), 4)
        np.testing.assert_array_equal(base[:], self.expected)

    def test_invalid_vector_is_not_completed(self):
        with h5py.File(self.source / 'shard_2.h5', 'r+') as f:
            f['embeddings'][1, 0] = np.nan
        output = self.root / 'invalid'
        result = self.run_prepare(output, check=False)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('Nonfinite/zero', result.stderr)
        progress = json.loads((output / 'prepare_progress.json').read_text())
        self.assertFalse(progress['complete'])
        self.assertFalse((output / 'base/part_02.npy').exists())

    def test_legacy_directory_not_overwritten(self):
        output = self.root / 'legacy'
        output.mkdir()
        (output / 'manifest.json').write_text('{"version": 1}')
        (output / 'base.npy').write_bytes(b'keep old data')
        result = self.run_prepare(output, check=False)
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual((output / 'base.npy').read_bytes(), b'keep old data')


if __name__ == '__main__':
    unittest.main()
