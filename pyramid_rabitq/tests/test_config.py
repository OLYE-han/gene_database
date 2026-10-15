import tempfile
import unittest
from pathlib import Path

from pyramid_rabitq.config import IVFConfig, load_build_config, load_service_config


class ConfigTests(unittest.TestCase):
    def test_nested_build_config_and_unknown_key(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "build.yaml"
            path.write_text("dimension: 32\nhnsw:\n  m: 12\nivf:\n  nlist: 8\n", encoding="utf-8")
            config = load_build_config(path)
            self.assertEqual(config.hnsw.m, 12)
            self.assertEqual(config.ivf.nlist, 8)
            path.write_text("unexpected: true\n", encoding="utf-8")
            with self.assertRaises(ValueError):
                load_build_config(path)

    def test_target_ivf_list_size_is_bounded_and_resolves_nlist(self):
        config = IVFConfig(target_vectors_per_list=384)
        self.assertEqual(config.resolve_nlist(10_000), 26)
        self.assertEqual(config.resolve_nlist(513), 2)
        self.assertEqual(config.resolve_nlist(255), 1)
        with self.assertRaisesRegex(ValueError, "between 256 and 512"):
            IVFConfig(target_vectors_per_list=1024)

    def test_service_config(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "service.yaml"
            path.write_text("index_root: /tmp/index\nport: 9000\n", encoding="utf-8")
            self.assertEqual(load_service_config(path).port, 9000)
            self.assertFalse(load_service_config(path).require_fastscan)


if __name__ == "__main__":
    unittest.main()
