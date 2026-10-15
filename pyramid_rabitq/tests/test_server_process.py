import socket
import subprocess
import sys
import tempfile
import time
import unittest
import urllib.request
from pathlib import Path

import h5py
import numpy as np

from pyramid_rabitq.builder import PyramidBuilder
from pyramid_rabitq.config import BuildConfig, HNSWConfig, IVFConfig
from pyramid_rabitq.storage import H5Corpus


class ServerProcessTests(unittest.TestCase):
    def test_real_http_health_endpoint(self):
        try:
            probe = socket.socket()
            probe.close()
        except PermissionError:
            self.skipTest("socket creation is disabled by the execution sandbox")
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "source"
            source.mkdir()
            values = np.random.default_rng(5).normal(size=(40, 8)).astype(np.float32)
            with h5py.File(source / "shard_0.h5", "w") as handle:
                handle.create_dataset("embeddings", data=values)
            config = BuildConfig(
                dimension=8,
                num_partitions=1,
                batch_size=16,
                hnsw=HNSWConfig(
                    sample_size=20, m=4, ef_construction=30, ef_search=20,
                    route_nodes=10, route_partitions=1, partition_backend="fallback",
                ),
                ivf=IVFConfig(
                    nlist=2, nprobe=2, train_size=30,
                    candidate_multiplier=10, scan_block_size=16,
                ),
            )
            index_root = root / "index"
            PyramidBuilder(H5Corpus(source, 8), config).build(index_root, "http-test")
            with socket.socket() as sock:
                sock.bind(("127.0.0.1", 0))
                port = sock.getsockname()[1]
            service_config = root / "service.yaml"
            service_config.write_text(
                f"index_root: {index_root}\ncache_bytes: 1048576\nhost: 127.0.0.1\nport: {port}\nworkers: 1\ntop_k_limit: 20\n",
                encoding="utf-8",
            )
            process = subprocess.Popen(
                [sys.executable, "-m", "pyramid_rabitq.api", "--config", str(service_config)],
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
            )
            try:
                deadline = time.time() + 15
                body = None
                while time.time() < deadline:
                    if process.poll() is not None:
                        output = process.stdout.read() if process.stdout else ""
                        self.fail(f"server exited early: {output}")
                    try:
                        with urllib.request.urlopen(f"http://127.0.0.1:{port}/health", timeout=1) as response:
                            body = response.read().decode("utf-8")
                        break
                    except OSError:
                        time.sleep(0.2)
                self.assertIsNotNone(body, "server did not become healthy")
                self.assertIn('"version":"http-test"', body)
            finally:
                process.terminate()
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=5)
                if process.stdout is not None:
                    process.stdout.close()


if __name__ == "__main__":
    unittest.main()
