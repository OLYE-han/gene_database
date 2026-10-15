import json
import tempfile
import unittest
from pathlib import Path

import numpy as np

import pyramid_rabitq.rabitq as rabitq
from pyramid_rabitq.rabitq import (
    OrthogonalTransform,
    RaBitQIVFIndex,
    packed_sign_dot,
    packed_sign_dot_fastscan,
    to_fastscan_layout,
)


class RaBitQTests(unittest.TestCase):
    def test_filtered_probe_planner_expands_only_until_target(self):
        index = RaBitQIVFIndex(
            metric="l2",
            seed=1,
            centroids=np.array([[0, 0], [1, 0], [2, 0], [3, 0]], dtype=np.float32),
            offsets=np.array([0, 2, 4, 6, 8], dtype=np.int64),
            codes=np.zeros((8, 1), dtype=np.uint8),
            ids=np.arange(8, dtype=np.int64),
            rows=np.arange(8, dtype=np.int64),
            residual_norms=np.ones(8, dtype=np.float32),
            corrections=np.ones(8, dtype=np.float32),
            code_layout="row",
        )
        allowed = np.zeros(8, dtype=bool)
        allowed[4] = True
        self.assertEqual(index.plan_filtered_nprobe([0, 0], allowed, 1, 1), 3)
        self.assertEqual(index.plan_filtered_nprobe([0, 0], allowed, 2, 1), 4)

    def test_packed_dot_matches_expanded_bits(self):
        rng = np.random.default_rng(8)
        dimension = 19
        bits = rng.integers(0, 2, size=(11, dimension), dtype=np.uint8)
        codes = np.packbits(bits, axis=1, bitorder="little")
        query = rng.normal(size=dimension).astype(np.float32)
        expected = (bits.astype(np.float32) * 2.0 - 1.0) @ query
        np.testing.assert_allclose(
            packed_sign_dot(codes, query, dimension), expected, rtol=2e-6, atol=1e-6
        )

    def test_fastscan_matches_row_major_for_unaligned_ranges(self):
        rng = np.random.default_rng(81)
        dimension = 77
        bits = rng.integers(0, 2, size=(69, dimension), dtype=np.uint8)
        codes = np.packbits(bits, axis=1, bitorder="little")
        fast_codes = to_fastscan_layout(codes)
        query = rng.normal(size=dimension).astype(np.float32)
        expected = packed_sign_dot(codes, query, dimension)
        for begin, end in ((0, 69), (1, 7), (7, 39), (31, 33), (32, 64), (63, 69)):
            actual = packed_sign_dot_fastscan(fast_codes, query, dimension, begin, end)
            np.testing.assert_allclose(actual, expected[begin:end], rtol=3e-6, atol=3e-6)

    def test_fastscan_numpy_fallback_matches_native(self):
        rng = np.random.default_rng(82)
        dimension = 35
        codes = np.packbits(
            rng.integers(0, 2, size=(37, dimension), dtype=np.uint8),
            axis=1,
            bitorder="little",
        )
        fast_codes = to_fastscan_layout(codes)
        query = rng.normal(size=dimension).astype(np.float32)
        expected = packed_sign_dot_fastscan(fast_codes, query, dimension, 3, 36)
        native = rabitq._native_packed_sign_dot_fastscan
        try:
            rabitq._native_packed_sign_dot_fastscan = None
            actual = packed_sign_dot_fastscan(fast_codes, query, dimension, 3, 36)
        finally:
            rabitq._native_packed_sign_dot_fastscan = native
        np.testing.assert_allclose(actual, expected, rtol=3e-6, atol=3e-6)

    def test_transform_preserves_norm(self):
        values = np.random.default_rng(1).normal(size=(10, 17)).astype(np.float32)
        transformed = OrthogonalTransform(17, 9).apply(values)
        np.testing.assert_allclose(
            np.linalg.norm(values, axis=1), np.linalg.norm(transformed, axis=1), rtol=2e-5
        )

    def test_build_search_and_roundtrip(self):
        rng = np.random.default_rng(2)
        vectors = rng.normal(size=(200, 24)).astype(np.float32)
        index = RaBitQIVFIndex.build(vectors, nlist=8, seed=3, metric="cosine")
        self.assertEqual(index.code_layout, "fastscan32")
        result = index.search_approx(vectors[17], candidate_k=200, nprobe=8)
        self.assertEqual(len(result.ids), 200)
        self.assertIn(17, result.ids.tolist())
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory)
            index.save(path)
            loaded = RaBitQIVFIndex.load(path)
            self.assertEqual(loaded.code_layout, "fastscan32")
            second = loaded.search_approx(vectors[17], candidate_k=200, nprobe=8)
            np.testing.assert_array_equal(np.sort(result.ids), np.sort(second.ids))

    def test_v1_load_and_conversion_to_fastscan(self):
        rng = np.random.default_rng(83)
        vectors = rng.normal(size=(65, 24)).astype(np.float32)
        built = RaBitQIVFIndex.build(vectors, nlist=5, seed=4, metric="cosine")
        positions = np.arange(len(built.ids), dtype=np.int64)
        row_codes = np.asarray(
            built.codes[positions // 32, :, positions % 32], dtype=np.uint8
        )
        legacy = RaBitQIVFIndex(
            built.metric,
            built.seed,
            built.centroids,
            built.offsets,
            row_codes,
            built.ids,
            built.rows,
            built.residual_norms,
            built.corrections,
            built.scan_block_size,
            "row",
        )
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory)
            legacy.save(path)
            metadata_path = path / "metadata.json"
            metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
            metadata["format_version"] = 1
            metadata.pop("code_layout")
            metadata_path.write_text(json.dumps(metadata), encoding="utf-8")

            loaded_legacy = RaBitQIVFIndex.load(path)
            self.assertEqual(loaded_legacy.code_layout, "row")
            before = loaded_legacy.search_approx(vectors[7], candidate_k=65, nprobe=5)
            RaBitQIVFIndex.convert_path_to_fastscan(path)
            self.assertFalse((path / "codes.npy").exists())
            converted = RaBitQIVFIndex.load(path)
            self.assertEqual(converted.code_layout, "fastscan32")
            after = converted.search_approx(vectors[7], candidate_k=65, nprobe=5)
            np.testing.assert_array_equal(before.ids, after.ids)
            np.testing.assert_allclose(before.distances, after.distances, rtol=3e-6, atol=3e-6)


if __name__ == "__main__":
    unittest.main()
