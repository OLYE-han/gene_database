import tempfile
import unittest
import json
from pathlib import Path

import h5py
import numpy as np

from pyramid_rabitq.builder import PyramidBuilder
from pyramid_rabitq.config import BuildConfig, HNSWConfig, IVFConfig
from pyramid_rabitq.engine import ReadNodeEngine
from pyramid_rabitq.evaluation import evaluate_recall
from pyramid_rabitq.storage import H5Corpus


class EndToEndTests(unittest.TestCase):
    def test_build_and_exact_rerank(self):
        rng = np.random.default_rng(11)
        dimension = 16
        first = rng.normal(loc=-1.0, scale=0.3, size=(80, dimension)).astype(np.float32)
        second = rng.normal(loc=1.0, scale=0.3, size=(80, dimension)).astype(np.float32)
        values = np.vstack((first, second))
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "source"
            source.mkdir()
            for shard_id, batch in enumerate(np.array_split(values, 2)):
                with h5py.File(source / f"shard_{shard_id}.h5", "w") as handle:
                    handle.create_dataset("embeddings", data=batch)
            config = BuildConfig(
                dimension=dimension,
                num_partitions=2,
                seed=7,
                batch_size=32,
                hnsw=HNSWConfig(
                    sample_size=60, sample_ratio=0.5, m=8, ef_construction=50, ef_search=40,
                    route_nodes=30, route_partitions=2, partition_backend="fallback",
                ),
                ivf=IVFConfig(
                    target_vectors_per_list=256, nprobe=1, train_size=60,
                    candidate_multiplier=8, scan_block_size=32,
                ),
            )
            corpus = H5Corpus(source, dimension)
            index_root = root / "index"
            PyramidBuilder(corpus, config).build(index_root, "v1")
            manifest = json.loads((index_root / "versions/v1/manifest.json").read_text())
            self.assertEqual(manifest["hnsw_sample_vectors"], 80)
            self.assertEqual(manifest["graph_partition_backend"], "fallback")
            self.assertTrue(all(part["ivf_nlist"] == 1 for part in manifest["partitions"]))
            node_parts = np.load(index_root / "versions/v1/router/node_partitions.npy")
            self.assertEqual(len(node_parts), 80)
            self.assertTrue(all(np.count_nonzero(node_parts == part) > 1 for part in (0, 1)))
            with self.assertRaises(FileExistsError):
                PyramidBuilder(corpus, config).build(index_root, "v1")
            engine = ReadNodeEngine(index_root, cache_bytes=1024 * 1024)
            default_response = engine.search(values[9], top_k=2)
            self.assertLessEqual(default_response.approximate_candidates, 2 * 2 * 8)
            response = engine.search(values[9], top_k=1, route_partitions=2, candidate_multiplier=200)
            self.assertEqual(response.hits[0].id, 9)
            self.assertAlmostEqual(response.hits[0].distance, 0.0, places=5)
            exhaustive = engine.search(
                values[9], top_k=1, route_partitions=config.num_partitions,
                candidate_multiplier=len(values),
            )
            self.assertEqual(exhaustive.searched_partitions, [0, 1])
            self.assertEqual(engine.health()["version"], "v1")
            report = evaluate_recall(engine, corpus, queries=3, top_k=3, batch_size=40)
            self.assertEqual(report.mean_recall, 1.0)
            self.assertFalse((index_root / "lsm").exists())
            PyramidBuilder(corpus, config).build(index_root, "v2")
            self.assertEqual(engine.reload(), "v2")
            self.assertEqual(engine.search(values[9], top_k=1).hits[0].id, 9)
            engine.close()

    def test_filtered_search_equal_range_and_tags(self):
        rng = np.random.default_rng(91)
        values = rng.normal(size=(96, 12)).astype(np.float32)
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "source"
            source.mkdir()
            equal = np.arange(96, dtype=np.uint16) % 3
            ranges = np.arange(96, dtype=np.float32)
            tag_rows = [[7] if row % 4 == 0 else [8, 9] for row in range(96)]
            lengths = np.array([len(row) for row in tag_rows], dtype=np.int64)
            with h5py.File(source / "shard_0.h5", "w") as handle:
                handle.create_dataset("embeddings", data=values)
                handle.create_dataset("equal", data=equal)
                handle.create_dataset("range", data=ranges)
                handle.create_dataset("tags_indptr", data=np.concatenate(([0], np.cumsum(lengths))))
                handle.create_dataset("tags_indices", data=np.concatenate(tag_rows).astype(np.int32))
            config = BuildConfig(
                dimension=12, metric="l2", num_partitions=2, batch_size=32,
                hnsw=HNSWConfig(
                    sample_size=48, sample_ratio=0.5, m=8, ef_construction=40,
                    ef_search=30, route_nodes=24, route_partitions=2,
                    partition_backend="fallback",
                ),
                ivf=IVFConfig(
                    nlist=1, nprobe=1, train_size=48, candidate_multiplier=100,
                    scan_block_size=32,
                ),
            )
            index_root = root / "index"
            PyramidBuilder(H5Corpus(source, 12), config).build(index_root, "filtered")
            engine = ReadNodeEngine(index_root, cache_bytes=1024 * 1024)
            filters = [
                {"field": "equal", "operator": "==", "value": 1, "logic": "and"},
                {"field": "range", "operator": ">", "value": 20, "logic": "and"},
                {"field": "tags", "operator": "contains", "value": 7, "logic": "and"},
            ]
            response = engine.search(values[40], top_k=10, scalar_filters=filters)
            self.assertTrue(response.hits)
            for hit in response.hits:
                self.assertEqual(equal[hit.id], 1)
                self.assertGreater(ranges[hit.id], 20)
                self.assertIn(7, tag_rows[hit.id])
            engine.close()


if __name__ == "__main__":
    unittest.main()
