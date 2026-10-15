import tempfile
import unittest
import subprocess
from pathlib import Path
from unittest.mock import patch

import numpy as np

from pyramid_rabitq.partition import BalancedGraphPartitioner, KaFFPaGraphPartitioner
from pyramid_rabitq.router import HNSWRouter


class RouterPartitionTests(unittest.TestCase):
    def test_real_kaffpa_when_installed(self):
        binary = Path(__file__).resolve().parents[1] / ".local/kahip-v3.25/bin/kaffpa"
        if not binary.exists():
            self.skipTest("pinned KaFFPa binary is not installed")
        size = 120
        graph = [
            sorted({(node - 2) % size, (node - 1) % size, (node + 1) % size, (node + 2) % size})
            for node in range(size)
        ]
        weights = np.arange(size, dtype=np.int64) % 7 + 1
        partitioner = KaFFPaGraphPartitioner(str(binary), require_kaffpa=True, seed=7)
        labels = partitioner.partition(graph, 4, weights)
        self.assertEqual(partitioner.backend_used, "kaffpa")
        self.assertEqual(set(labels.tolist()), {0, 1, 2, 3})
        self.assertEqual(
            partitioner.provenance["commit"],
            "2e86d3da2065bcf6ddbf6fb84d2b52f33db1061f",
        )

    def test_kaffpa_adapter_writes_weighted_graph_and_reads_labels(self):
        graph = [[1, 3], [0, 2], [1, 3], [0, 2]]
        weights = np.array([10, 20, 30, 40], dtype=np.int64)
        captured = {}

        def fake_run(command, **kwargs):
            if command[-1] == "--version":
                return subprocess.CompletedProcess(command, 0, stdout="v-test\n", stderr="")
            graph_path = Path(command[1])
            captured["graph"] = graph_path.read_text(encoding="utf-8")
            output = Path(next(value.split("=", 1)[1] for value in command if value.startswith("--output_filename=")))
            output.write_text("0\n0\n1\n1\n", encoding="utf-8")
            return subprocess.CompletedProcess(command, 0, stdout="", stderr="")

        partitioner = KaFFPaGraphPartitioner(require_kaffpa=True)
        with patch("pyramid_rabitq.partition.shutil.which", return_value="/tmp/kaffpa"), patch(
            "pyramid_rabitq.partition.subprocess.run", side_effect=fake_run
        ), patch.object(
            KaFFPaGraphPartitioner,
            "_binary_provenance",
            return_value={"reported_version": "v-test"},
        ):
            labels = partitioner.partition(graph, 2, weights)
        np.testing.assert_array_equal(labels, [0, 0, 1, 1])
        self.assertEqual(partitioner.backend_used, "kaffpa")
        self.assertEqual(partitioner.provenance["reported_version"], "v-test")
        self.assertTrue(captured["graph"].startswith("4 4 10\n10 "))

    def test_required_kaffpa_fails_when_binary_is_missing(self):
        with patch("pyramid_rabitq.partition.shutil.which", return_value=None):
            with self.assertRaisesRegex(RuntimeError, "not found"):
                KaFFPaGraphPartitioner(require_kaffpa=True).partition([[1], [0]], 2)

    def test_optional_kaffpa_records_fallback(self):
        with patch("pyramid_rabitq.partition.shutil.which", return_value=None):
            partitioner = KaFFPaGraphPartitioner(require_kaffpa=False)
            labels = partitioner.partition([[1], [0]], 2)
        self.assertEqual(set(labels.tolist()), {0, 1})
        self.assertEqual(partitioner.backend_used, "fallback")
        self.assertIn("not found", partitioner.fallback_reason)

    def test_fallback_partition_is_balanced(self):
        graph = [[(i - 1) % 12, (i + 1) % 12] for i in range(12)]
        labels = BalancedGraphPartitioner(prefer_metis=False).partition(graph, 3)
        counts = np.bincount(labels, minlength=3)
        self.assertLessEqual(int(counts.max() - counts.min()), 1)

    def test_hnsw_graph_partition_roundtrip(self):
        vectors = np.random.default_rng(4).normal(size=(100, 16)).astype(np.float32)
        router = HNSWRouter.build(vectors, m=8, ef_construction=40)
        graph = router.extract_level0_graph()
        self.assertEqual(len(graph), 100)
        labels = BalancedGraphPartitioner(prefer_metis=False).partition(graph, 4)
        router.set_partitions(labels)
        self.assertTrue(router.route_from_nodes(vectors[0], 16, 2))
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory)
            router.save(path)
            loaded = HNSWRouter.load(path)
            self.assertEqual(loaded.assign(vectors[:3]).shape, (3,))

    def test_router_expands_nodes_to_return_requested_partitions(self):
        vectors = np.random.default_rng(9).normal(size=(40, 8)).astype(np.float32)
        router = HNSWRouter.build(vectors, m=8, ef_construction=40)
        # The closest node can expose only one label; routing must expand the
        # navigation candidates until it finds the requested second partition.
        labels = np.zeros(40, dtype=np.int32)
        labels[20:] = 1
        router.set_partitions(labels)
        routed = router.route_from_nodes(vectors[0], route_nodes=1, partitions=2)
        self.assertEqual(set(routed), {0, 1})


if __name__ == "__main__":
    unittest.main()
