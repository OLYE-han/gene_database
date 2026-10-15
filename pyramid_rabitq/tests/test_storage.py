import tempfile
import unittest
from pathlib import Path

import h5py
import numpy as np

from pyramid_rabitq.storage import H5Corpus, PartitionMetadata, VectorBlockCache


class StorageTests(unittest.TestCase):
    def test_partition_metadata_indexes_match_dense_predicates(self):
        equal = np.array([3, 1, 3, 2, 1, 4], dtype=np.uint16)
        ranges = np.array([0.5, 9.0, 3.5, 7.0, 2.0, np.nan], dtype=np.float32)
        tag_rows = [[7], [8, 9], [7, 9], [], [8], [10, 7]]
        lengths = np.array([len(row) for row in tag_rows], dtype=np.int64)
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "vectors.h5"
            with h5py.File(path, "w") as handle:
                handle.create_dataset("equal", data=equal)
                handle.create_dataset("range", data=ranges)
                handle.create_dataset(
                    "tags_indptr", data=np.concatenate(([0], np.cumsum(lengths)))
                )
                handle.create_dataset(
                    "tags_indices",
                    data=np.concatenate([np.asarray(row, dtype=np.int32) for row in tag_rows]),
                )
            metadata = PartitionMetadata(path)
            filters = [
                {"field": "equal", "operator": ">=", "value": 2},
                {"field": "range", "operator": "<", "value": 8, "logic": "and"},
                {"field": "tags", "operator": "contains", "value": 7, "logic": "and"},
            ]
            expected = (equal >= 2) & (ranges < 8) & np.array(
                [7 in row for row in tag_rows]
            )
            np.testing.assert_array_equal(metadata.filter(filters), expected)
            self.assertTrue(metadata.may_match(filters))
            self.assertFalse(metadata.may_match([
                {"field": "equal", "operator": "==", "value": 99}
            ]))
            self.assertTrue(metadata.may_match([
                {"field": "equal", "operator": "==", "value": 99},
                {"field": "tags", "operator": "contains", "value": 10, "logic": "or"},
            ]))
            np.testing.assert_array_equal(
                metadata.filter([{"field": "range", "operator": ">", "value": 8}]),
                ranges > 8,
            )

    def test_dense_sample_matches_seeded_rows(self):
        values = np.arange(400, dtype=np.float32).reshape(100, 4)
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "shard_0.h5"
            with h5py.File(path, "w") as handle:
                handle.create_dataset("embeddings", data=values, chunks=(8, 4))
            corpus = H5Corpus(directory, dimension=4)
            selected = np.sort(np.random.default_rng(42).choice(100, 30, replace=False))
            np.testing.assert_array_equal(corpus.sample(30, 42), values[selected])

    def test_block_cache_reads_requested_rows(self):
        values = np.arange(80, dtype=np.float32).reshape(20, 4)
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "vectors.h5"
            with h5py.File(path, "w") as handle:
                handle.create_dataset("embeddings", data=values)
            cache = VectorBlockCache(max_bytes=1024, block_rows=4)
            actual = cache.read_rows(path, np.array([7, 2, 7]))
            np.testing.assert_array_equal(actual, values[[7, 2, 7]])
            self.assertGreater(cache.current_bytes, 0)


if __name__ == "__main__":
    unittest.main()
