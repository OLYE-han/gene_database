import json
from pathlib import Path
import tempfile
import unittest

import numpy as np

from dhdgs.data import Dataset, load_complete_npy, sample_ids
from dhdgs.partition import Model, Threshold, calibrate, fit, memberships, prepare
from dhdgs.scheduler import Action, Candidate, Policy
from dhdgs.experiments import run_partition_study
from dhdgs.cli import assign, inspect, verify_distance


def dataset_at(root, x):
    root.mkdir()
    midpoint = len(x) // 2
    parts = []
    for i, (start, end) in enumerate(((0, midpoint), (midpoint, len(x)))):
        filename = f"part{i}.npy"
        np.save(root / filename, x[start:end].astype(np.float32))
        parts.append({"file": filename, "start": start, "end": end})
    (root / "manifest.json").write_text(json.dumps({
        "rows": len(x), "dimension": x.shape[1], "parts": parts}))


class PartitionTests(unittest.TestCase):
    def setUp(self):
        self.model = Model(np.array([[0, 0], [10, 0], [0, 10]]), np.ones(3)*5, "l2")
        self.x = np.array([[1, 1], [4.8, 1], [4.8, 4.7]])

    def test_geometry_example(self):
        primary, scores = self.model.scores(self.x)
        np.testing.assert_allclose(scores[:, 1:], [[.8, .8], [.04, .8], [.04, .06]])
        selected = memberships(primary, scores, np.arange(3), Threshold("cutoff", .1))
        np.testing.assert_array_equal(selected, [[1, 0, 0], [1, 1, 0], [1, 1, 1]])

    def test_scale_invariance(self):
        scaled = Model(self.model.centers*73, self.model.radii*73, "l2")
        np.testing.assert_allclose(scaled.scores(self.x*73)[1], self.model.scores(self.x)[1])

    def test_ties_high_ids_monotonic_and_rf(self):
        x = np.tile([5., 0.], (100, 1))
        ids = np.arange(100, dtype=np.uint64) + np.uint64(2**63)
        primary, scores = self.model.scores(x)
        previous = np.zeros_like(scores, dtype=bool)
        for rho in (0, .01, .1, .3, .9, 1):
            threshold = calibrate(scores, ids, rho, seed=123)
            result = memberships(primary, scores, ids, threshold, max_copies=3)
            counts = result.sum(axis=1)
            self.assertEqual(int(np.sum(counts > 1)), int(rho*100))
            self.assertTrue(np.all(~previous | result))
            self.assertTrue(np.all(result[np.arange(100), primary]))
            self.assertGreaterEqual(counts.mean()+1e-12, 1+rho)
            self.assertLessEqual(counts.mean(), 1+2*rho+1e-12)
            reverse = memberships(primary[::-1], scores[::-1], ids[::-1], threshold)
            np.testing.assert_array_equal(result, reverse[::-1])
            previous = result

    def test_cap_and_single_shard(self):
        primary, scores = self.model.scores(self.x)
        result = memberships(primary, scores, np.arange(3), Threshold("all"), max_copies=2)
        self.assertTrue(np.all(result.sum(axis=1) == 2))
        one = Model(np.array([[0, 0]]), np.array([1.]), "l2")
        primary, scores = one.scores(self.x)
        cutoff = calibrate(scores, np.arange(3), 1)
        self.assertEqual(cutoff.mode, "none")
        self.assertTrue(memberships(primary, scores, np.arange(3), cutoff).all())

    def test_errors(self):
        with self.assertRaises(ValueError):
            prepare([[0, 0]], "cosine")
        with self.assertRaises(ValueError):
            prepare([[float("nan"), 0]], "l2")
        with self.assertRaises(ValueError):
            Model(np.zeros((2, 2)), np.ones(2), "l2")
        with self.assertRaises(ValueError):
            fit(np.ones((10, 2)), 2, "l2")
        _, scores = self.model.scores(self.x)
        with self.assertRaises(ValueError):
            calibrate(scores, np.array([1, 1, 2]), .2)
        with self.assertRaises(ValueError):
            calibrate(scores, np.arange(3), float("nan"))
        with self.assertRaises(ValueError):
            Threshold("cutoff", float("inf"))

    def test_kmeans_and_persistence(self):
        rng = np.random.default_rng(42)
        x = np.r_[rng.normal(0, .1, (50, 8)), rng.normal(10, .1, (50, 8))]
        model = fit(x, 2, "l2")
        self.assertTrue(model.converged)
        self.assertEqual(len(set(model.scores(x)[0])), 2)
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "model.npz"
            model.save(path)
            restored = Model.load(path)
            np.testing.assert_array_equal(model.scores(x)[1], restored.scores(x)[1])


class DataTests(unittest.TestCase):
    def test_near_zero_label_cancellation_and_corruption(self):
        vector = np.full(2560, .25)
        self.assertEqual(verify_distance(vector, vector, 4.13e-7, "euclidean"), 0)
        with self.assertRaises(ValueError):
            verify_distance(vector, vector, .01, "euclidean")
        with self.assertRaises(ValueError):
            verify_distance(vector, vector, .01, "cosine")

    def test_sampling_large_population(self):
        a = sample_ids(10_000_000_000, 1000, 42)
        np.testing.assert_array_equal(a, sample_ids(10_000_000_000, 1000, 42))
        self.assertEqual(len(np.unique(a)), 1000)
        self.assertTrue(np.any(a > 2**32))
        self.assertEqual(a.dtype, np.dtype("uint64"))

    def test_parts_take_and_changes(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)/"data"
            x = np.arange(40).reshape(10, 4)
            dataset_at(root, x)
            data = Dataset(root)
            np.testing.assert_array_equal(data.take(np.array([9, 0, 5, 5])), x[[9, 0, 5, 5]])
            self.assertEqual(sum(len(b) for _, b in data.blocks(3)), 10)
            with self.assertRaises(ValueError):
                data.take(np.array([-1]))
            with self.assertRaises(ValueError):
                data.take(np.array([2.5]))
            with (root/"part0.npy").open("ab") as handle:
                handle.write(b"x")
            with self.assertRaises(ValueError):
                data.assert_unchanged()
            with self.assertRaises(ValueError):
                Dataset(root)

    def test_truncated_file(self):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp)/"partial.npy"
            np.save(path, np.ones((10, 2560), dtype=np.float32))
            with path.open("r+b") as handle:
                handle.truncate(1000)
            with self.assertRaises(ValueError):
                load_complete_npy(path)

    def test_study_and_streaming_assign(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)/"data"
            rng = np.random.default_rng(8)
            x = rng.normal(size=(120, 8))
            dataset_at(root, x)
            result = run_partition_study(root, Path(temp)/"study", shards=3,
                train_rows=40, calibration_rows=40, evaluation_rows=40,
                metrics=("l2",), rhos=(0, .1, 1))
            self.assertEqual(result["status"], "exploratory_not_ann_benchmark")
            threshold = Path(temp)/"study/threshold_l2_margin_0.1.json"
            manifest = assign(root, Path(temp)/"study/model_l2.npz", threshold,
                              Path(temp)/"assigned", "margin", 3, 17)
            self.assertEqual(sum(manifest["primary_counts"]), 120)
            self.assertEqual(manifest["parts"][-1]["end"], 120)
            self.assertEqual(inspect(root, 10)["sample_rows"], 10)
            with self.assertRaises(ValueError):
                assign(root, Path(temp)/"study/model_l2.npz", threshold,
                       Path(temp)/"invalid", "boundary", 3, 17)


class SchedulerTests(unittest.TestCase):
    def test_remote_batch_vs_local_and_duplicate(self):
        policy = Policy()
        candidate = Candidate(2**40, .3, .01)
        local = Action("local", False, (candidate,), .1)
        single = Action("remote", True, (candidate,), .4)
        self.assertEqual(policy.choose([single, local], .4, .1), local)
        batch = Action("batch", True, tuple(Candidate(i, .3, .01) for i in range(8)), .4)
        self.assertEqual(policy.choose([batch, local], .4, .1), batch)
        duplicate = Action("duplicate", True, (candidate, candidate), .4)
        self.assertEqual(policy.score(single, .4, .1), policy.score(duplicate, .4, .1))

    def test_starvation_and_local_tie(self):
        policy = Policy(max_wait_ms=2)
        local = Action("local", False, (Candidate(1, 0),), .001)
        waiting = Action("remote", True, (Candidate(2, 100),), 100, 2)
        self.assertEqual(policy.choose([local, waiting], .4, .1), waiting)
        same = Action("remote", True, local.candidates, local.cost_ms)
        self.assertEqual(policy.choose([same, local], .4, .1), local)
        self.assertIsNone(policy.choose([], .4, .1))

    def test_invalid_cost(self):
        with self.assertRaises(ValueError):
            Policy().score(Action("bad", True, (Candidate(1, .1),), 0), .4, .1)


if __name__ == "__main__":
    unittest.main()
