import importlib.util
import json
import tempfile
import unittest
from pathlib import Path

import h5py
import numpy as np


ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location(
    "run_vecbench_yfcc", ROOT / "tools" / "run_vecbench_yfcc.py"
)
RUNNER = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(RUNNER)
SCORE_SPEC = importlib.util.spec_from_file_location(
    "score_vecbench_official", ROOT / "tools" / "score_vecbench_official.py"
)
SCORER = importlib.util.module_from_spec(SCORE_SPEC)
SCORE_SPEC.loader.exec_module(SCORER)


class VecBenchYfccRunnerTests(unittest.TestCase):
    def test_official_vrank_reference_row(self):
        values = {"T0": 229.99, "recall": 0.993, "latency": 0.0586,
                  "qps": 19.41, "T1": 146.29, "T2": 171.36, "T3": 7.75}
        ranks = {name: SCORER.dense_rank(name, value) for name, value in values.items()}
        self.assertEqual(ranks, {"T0": 1, "recall": 2, "latency": 7,
                                 "qps": 9, "T1": 1, "T2": 3, "T3": 2})
        self.assertEqual(round(sum(ranks.values()) / 6.0, 1), 4.2)

    def test_full_profile_is_strict(self):
        self.assertTrue(RUNNER.profile_status(10_000_000, 1920, 3000, None)["shape_valid"])
        status = RUNNER.profile_status(100_000, 1920, 9, 9)
        self.assertFalse(status["shape_valid"])
        self.assertEqual(len(status["deviations"]), 2)

    def test_materialize_and_precomputed_ground_truth(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            corpus = root / "corpus.h5"
            with h5py.File(corpus, "w") as handle:
                handle.create_dataset("embeddings", data=np.arange(12, dtype=np.float32).reshape(3, 4))
                handle.create_dataset("ids", data=np.array([10, 20, 30], dtype=np.int64))
            query_file = root / "queries.yaml"
            query_file.write_text(
                "YFCC:\n"
                "- name: q1\n"
                "  reference_vector_name: 20\n"
                "  vector_field: image_vec\n"
                "  scalar_filters: []\n"
                "  limit: 2\n",
                encoding="utf-8",
            )
            queries = RUNNER.materialize_queries(corpus, query_file, "YFCC")
            self.assertEqual(queries[0]["query_vector"], [4.0, 5.0, 6.0, 7.0])
            truth_file = root / "truth.json"
            truth_file.write_text(json.dumps([{"name": "q1", "result": [20, 10]}]))
            self.assertEqual(RUNNER.load_ground_truth(truth_file, queries)[0]["result"], [20, 10])

            truth_file.write_text(json.dumps([]))
            with self.assertRaisesRegex(ValueError, "ground truth misses"):
                RUNNER.load_ground_truth(truth_file, queries)


if __name__ == "__main__":
    unittest.main()
