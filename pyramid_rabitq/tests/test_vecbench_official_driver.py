from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import h5py
import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "third_party" / "VecBench-paper"))

from benchmarks.vecbench_official_driver import Pyramid


class VecBenchOfficialDriverTests(unittest.TestCase):
    def test_fixed_corpus_is_migrated_to_resizable(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            driver = Pyramid({"pyramid": {
                "endpoint": "http://127.0.0.1:1", "dimension": 2,
                "source_dir": str(root / "source"), "index_root": str(root / "index"),
                "build_config": str(root / "build.yaml"),
            }})
            driver.source_dir.mkdir()
            with h5py.File(driver.source_h5, "w") as handle:
                handle.create_dataset("embeddings", data=[[1.0, 2.0]])
                handle.create_dataset("ids", data=[1])
                handle.create_dataset("equal", data=[1], dtype="u2")
                handle.create_dataset("range", data=[1.0], dtype="f4")
                handle.create_dataset("tags_indptr", data=[0, 1])
                handle.create_dataset("tags_indices", data=[3], dtype="i4")
            driver._ensure_resizable()
            with h5py.File(driver.source_h5, "r") as handle:
                self.assertTrue(all(handle[name].maxshape[0] is None for name in handle))

    def test_rebuild_crud_contract(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            config = {"pyramid": {
                "endpoint": "http://127.0.0.1:1", "dimension": 2,
                "source_dir": str(root / "source"), "index_root": str(root / "index"),
                "build_config": str(root / "build.yaml"),
            }}
            driver = Pyramid(config)
            driver.create_table("pyramid", {})
            initial = pd.DataFrame({
                "id": [1, 2], "image_vec": [[1.0, 2.0], [3.0, 4.0]],
                "equal": [1, 2], "range": [0.1, 0.2], "tags": [[4, 5], [6]],
            })
            driver.insert_data(driver.process_data(initial, "pyramid", {}), "pyramid", {})
            with h5py.File(driver.source_h5, "r") as handle:
                self.assertEqual(handle["ids"][:].tolist(), [1, 2])
                self.assertEqual(handle["tags_indptr"][:].tolist(), [0, 2, 3])

            driver._initial_loading = False
            (driver.index_root).mkdir()
            (driver.index_root / "CURRENT").write_text("base\n")
            delta = pd.DataFrame({
                "id": [3], "image_vec": [[5.0, 6.0]], "equal": [3],
                "range": [0.3], "tags": [[7, 8]],
            })
            with patch.object(driver, "_rebuild") as rebuild:
                self.assertGreaterEqual(driver.insert_data(delta, "pyramid", {}), 0.0)
                rebuild.assert_called_once_with("P4")

            update = delta.copy()
            update.at[0, "image_vec"] = [9.0, 10.0]
            update.at[0, "equal"] = 9
            update.at[0, "tags"] = [10, 11, 12]
            with patch.object(driver, "_rebuild") as rebuild:
                self.assertGreaterEqual(driver.update(update), 0.0)
                rebuild.assert_called_once_with("P5")
            with h5py.File(driver.source_h5, "r") as handle:
                self.assertEqual(handle["embeddings"][2].tolist(), [9.0, 10.0])
                self.assertEqual(int(handle["equal"][2]), 9)
                self.assertEqual(handle["tags_indices"][:].tolist(), [4, 5, 6, 10, 11, 12])

            with patch.object(driver, "_rebuild") as rebuild:
                self.assertGreaterEqual(driver.delete_by_ids([3]), 0.0)
                rebuild.assert_called_once_with("P6")
            with h5py.File(driver.source_h5, "r") as handle:
                self.assertEqual(handle["ids"][:].tolist(), [1, 2])
                self.assertEqual(handle["tags_indptr"][:].tolist(), [0, 2, 3])
                self.assertEqual(handle["tags_indices"][:].tolist(), [4, 5, 6])


if __name__ == "__main__":
    unittest.main()
