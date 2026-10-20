import copy
import json
from pathlib import Path
import shutil
import socket
import subprocess
import sys
import tempfile
import unittest

import numpy as np
from threadpoolctl import threadpool_limits
import yaml

from dhdgs import _native
from dhdgs.benchmark import exact_topk, run_benchmark
from dhdgs.config import load_config, snapshot
from dhdgs.data import Dataset
from dhdgs.index import Shard, build
from dhdgs.search import Engine
from dhdgs.transport import TCPTransport
from test_core import dataset_at

ROOT = Path(__file__).resolve().parents[1]


def fixture(root, metric="l2"):
    rng = np.random.default_rng(194)
    x = rng.normal(size=(96, 64)).astype(np.float32)
    x[:32, :8] += 4
    x[32:64, 8:16] += 4
    x[64:, 16:24] += 4
    dataset_at(root/"data", x)
    np.save(root/"queries.npy", x[[0, 35, 70]])
    np.save(root/"query_ids.npy", np.array([0, 35, 70], dtype=np.uint64))
    c = yaml.safe_load((ROOT/"configs/dhdgs.yaml").read_text())
    c["paths"] = {"dataset": "data", "output": "out"}
    c["data"].update(dimension=64, row_limit=96, metric=metric, block_rows=32)
    c["partition"].update(train_rows=48, calibration_rows=24, overlap_fraction=.4)
    c["build"].update(degree=8, candidate_list_size=16)
    c["routing"].update(min_samples=12, max_samples=24, entry_candidates=8, ef_search=24)
    c["search"].update(execution='reference',top_k=5, max_expansions=96, frontier_size=96, max_inflight=1)
    c['peer']['execution_model'] = 'blocking'  # Legacy reference fixture; native has dedicated tests.
    c['peer']['control']['scheduler'] = 'scan'  # Legacy fixture; event has dedicated handoff tests.
    c['storage']['io']['backend'] = 'thread_pool'
    c["scheduler"]["mode"] = "reference"
    c["storage"]["cache_bytes_per_shard"] = 0
    c["query"] = {"vectors": "queries.npy", "ids": "query_ids.npy", "row": 0}
    c["experiment"].update(benchmark_queries=3, exact_block_rows=17)
    path = root/"config.yaml"
    path.write_text(yaml.safe_dump(c))
    return load_config(path), x


class ConfigTests(unittest.TestCase):
    def test_replay_and_relative_paths(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            c, _ = fixture(root)
            self.assertEqual(c["paths"]["dataset"], str(root/"data"))
            snapshot(c, root/"snapshot")
            replay = load_config(root/"snapshot/resolved.yaml")
            self.assertEqual({k:v for k,v in c.items() if not k.startswith("_")},
                             {k:v for k,v in replay.items() if not k.startswith("_")})
            with self.assertRaises(FileExistsError):
                snapshot(c, root/"snapshot")

    def test_invalid_configuration(self):
        raw = yaml.safe_load((ROOT/"configs/dhdgs.yaml").read_text())
        from dhdgs.config import validate
        for section, key, value in [("search", "top_k", True), ("partition", "max_copies", 4),
            ("network", "timeout_seconds", float("nan")), ("search", "frontier_size", 1),
            ("routing", "ef_search", 1), ("scheduler", "mode", "invalid")]:
            c = copy.deepcopy(raw)
            c[section][key] = value
            with self.subTest(key=key), self.assertRaises(ValueError):
                validate(c)
        for mutation in (lambda c: c["search"].update(typo=1), lambda c: c.pop("quantization")):
            c = copy.deepcopy(raw); mutation(c)
            with self.assertRaises(ValueError):
                validate(c)
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp)/"duplicate.yaml"
            path.write_text("version: 1\nversion: 1\n")
            with self.assertRaisesRegex(ValueError, "Duplicate"):
                load_config(path)


class CLITests(unittest.TestCase):
    def test_yaml_build_query_benchmark_commands(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            c, _ = fixture(root)
            path = root/"config.yaml"
            for command in ("validate-config", "inspect", "build", "query", "benchmark"):
                proc = subprocess.run([sys.executable,"-m","dhdgs",command,"--config",str(path)],
                                      text=True,capture_output=True,timeout=20)
                self.assertEqual(proc.returncode,0,proc.stderr)
                result = json.loads(proc.stdout)
                if command == "benchmark":
                    self.assertEqual(result["recall_at_k"],1.)
                    self.assertTrue((Path(result["output"])/"resolved.yaml").is_file())
            proc = subprocess.run([sys.executable,"-m","dhdgs","build","--config",str(path)],
                                  text=True,capture_output=True,timeout=20)
            self.assertEqual(proc.returncode,2)
            self.assertIn("already exists",proc.stderr)

    def test_yaml_study_assignment_and_mismatch(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            c, _ = fixture(root)
            c["experiment"].update(train_rows=32,calibration_rows=16,evaluation_rows=16,
                                    seeds=[42],metrics=["l2"],overlap_fractions=[.4])
            c["assignment"] = {"model": str(root/"out/partition/seed_42/model_l2.npz"),
                               "threshold": str(root/"out/partition/seed_42/threshold_l2_margin_0.4.json")}
            path = root/"config.yaml"
            path.write_text(yaml.safe_dump({k:v for k,v in c.items() if not k.startswith("_")}))
            for command in ("partition-study", "assign"):
                proc = subprocess.run([sys.executable,"-m","dhdgs",command,"--config",str(path)],
                                      text=True,capture_output=True,timeout=20)
                self.assertEqual(proc.returncode,0,proc.stderr)
            c["partition"]["overlap_fraction"] = .1
            path.write_text(yaml.safe_dump({k:v for k,v in c.items() if not k.startswith("_")}))
            proc = subprocess.run([sys.executable,"-m","dhdgs","assign","--config",str(path)],
                                  text=True,capture_output=True,timeout=20)
            self.assertEqual(proc.returncode,2)
            self.assertIn("differs from configured",proc.stderr)


class NativeTests(unittest.TestCase):
    def test_rabitq_dimensions_bits_rotation_and_residual_zero(self):
        rng = np.random.default_rng(5)
        for dim in (64, 2560):
            centers = rng.normal(size=(2, dim)).astype("f4")
            x = rng.normal(size=(32, dim)).astype("f4")
            ci = np.arange(len(x), dtype="u4") % 2
            x[0] = centers[0]
            errors = []
            for bits in (1, 4, 8):
                with self.subTest(dim=dim, bits=bits):
                    q = _native.Quantizer(centers, bits, 42, b"")
                    bank = q.encode(x, ci)
                    d, u = q.score(x[1], *bank, ci)
                    self.assertTrue(np.isfinite(d).all() and np.isfinite(u).all())
                    self.assertTrue(np.all(u >= 0))
                    truth = np.sum((x.astype(float)-x[1].astype(float))**2, axis=1)
                    errors.append(np.mean(np.abs(d-truth)))
                    np.testing.assert_allclose(d[0], truth[0], rtol=1e-5, atol=1e-4)
                    restored = _native.Quantizer(centers, bits, 9, bytes(q.state()))
                    np.testing.assert_array_equal(restored.score(x[1], *bank, ci)[0], d)
                    with self.assertRaises(ValueError):
                        q.score(x[1], bank[0][:, :-1], bank[1], bank[2], ci)
            self.assertLess(errors[-1], errors[0]/10)

    def test_hnsw_uint64_persistence(self):
        x = np.random.default_rng(13).normal(size=(32,64)).astype("f4")
        ids = np.arange(32, dtype=np.uint64)+np.uint64(2**63)
        router = _native.Router(64,32,8,32,42)
        router.add(x, ids)
        with tempfile.TemporaryDirectory() as temp:
            path = str(Path(temp)/"route.hnsw")
            router.save(path)
            restored = _native.Router(64,32,8,32,42); restored.load(path)
            for i in (0, 14, 31):
                self.assertEqual(restored.search(x[i], 1, 32), [int(ids[i])])

    def test_vamana_valid_edges_and_seed(self):
        x = np.random.default_rng(2).normal(size=(40,64)).astype("f4")
        graph = _native.build_vamana(x, 8, 16, [1.,1.2], 42)
        self.assertEqual(graph, _native.build_vamana(x, 8, 16, [1.,1.2], 42))
        for i, edges in enumerate(graph):
            self.assertTrue(0 < len(edges) <= 8)
            self.assertEqual(len(edges), len(set(edges)))
            self.assertNotIn(i, edges)
            self.assertTrue(all(0 <= j < len(x) for j in edges))


class SystemTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.c, self.x = fixture(self.root)
        with threadpool_limits(limits=1):
            self.manifest = build(self.c)

    def tearDown(self):
        self.temp.cleanup()

    def test_full_traversal_exact_self_exclusion_and_global_budget(self):
        engine = Engine(self.c)
        try:
            for i in (0,35,70):
                result = engine.search(self.x[i], i)
                truth, distances = exact_topk(Dataset(self.root/"data"), np.arange(96,dtype="u8"),
                    self.x[i], 5, "l2", i, 17)
                self.assertEqual(result["ids"], truth)
                np.testing.assert_allclose(result["squared_distances"], distances, atol=1e-10)
                self.assertEqual(result["stats"]["expanded"], 96)
                self.assertEqual(result["stats"]["shards_visited"], [0,1,2])
                self.assertEqual(result["stats"]["rpc_count"], 0)
            engine.c["search"]["max_expansions"] = 13
            result = engine.search(self.x[0], 0)
            self.assertEqual(result["stats"]["expanded"], 13)
            self.assertEqual(len(set(result["ids"])), len(result["ids"]))
        finally:
            engine.close()

    def test_replica_union_epoch_and_truncation(self):
        directory = self.root/"out/index"
        shards = [Shard(directory/f"shard_{i}", 0) for i in range(3)]
        try:
            all_edges = {}
            replicas = 0
            for shard in shards:
                for gid in shard.ids:
                    stats = dict(cache_hits=0,file_read_bytes=0,pread_calls=0)
                    _, edges = shard.read(int(gid),stats)
                    current = set(map(int,edges))
                    if int(gid) in all_edges:
                        self.assertEqual(current, all_edges[int(gid)]); replicas += 1
                    all_edges[int(gid)] = current
            self.assertGreater(replicas, 0)
            with self.assertRaisesRegex(ValueError,"epoch"):
                shards[0].expand(self.x[0], [int(shards[0].ids[0])], "wrong")
        finally:
            for shard in shards: shard.close()
        corrupt = self.root/"corrupt"
        shutil.copytree(directory/"shard_0", corrupt)
        with (corrupt/"nodes.bin").open("r+b") as handle:
            handle.truncate(100)
        with self.assertRaisesRegex(ValueError,"Incomplete"):
            Shard(corrupt, 0)

    def test_benchmark_exact_truth_and_no_overwrite(self):
        report = run_benchmark(self.c)
        self.assertEqual(report["recall_at_k"], 1.)
        self.assertTrue(report["target_met_on_this_sample"])
        self.assertEqual(report["indexed_rows"], 96)
        with self.assertRaisesRegex(ValueError,"already exists"):
            build(self.c)
        changed = copy.deepcopy(self.c)
        changed["quantization"]["bits"] = 8
        with self.assertRaisesRegex(ValueError,"Build parameters"):
            Engine(changed)

    def test_adaptive_bounded_search(self):
        for mode in ("adaptive", "local_first"):
            c = copy.deepcopy(self.c)
            c["scheduler"]["mode"] = mode
            c["search"].update(max_inflight=3, max_expansions=23, frontier_size=16)
            engine = Engine(c)
            try:
                result = engine.search(self.x[0],0)
                self.assertEqual(result["stats"]["expanded"],23)
                self.assertNotIn(0, result["ids"])
                self.assertEqual(len(set(result["ids"])),5)
            finally:
                engine.close()

    def test_tcp_three_processes_matches_local_and_rejects_epoch(self):
        c = copy.deepcopy(self.c)
        c["network"]["transport"] = "tcp"
        # Reserve distinct ephemeral ports, then release immediately before starting services.
        reservations = [socket.socket() for _ in range(3)]
        try:
            for node, sock in zip(c["cluster"]["nodes"],reservations):
                sock.bind(("127.0.0.1",0)); node["port"] = sock.getsockname()[1]
        finally:
            for sock in reservations: sock.close()
        path = self.root/"tcp.yaml"
        path.write_text(yaml.safe_dump({k:v for k,v in c.items() if not k.startswith("_")}))
        processes = []
        engine = None
        try:
            for i in range(3):
                proc = subprocess.Popen([sys.executable,"-m","dhdgs","serve","--config",str(path),"--node-id",str(i)],
                    stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True)
                processes.append(proc)
                import select
                ready, _, _ = select.select([proc.stdout], [], [], 20)
                self.assertTrue(ready, "Server readiness timeout")
                line = proc.stdout.readline()
                if not line:
                    self.fail(proc.stderr.read())
                self.assertEqual(json.loads(line)["status"],"listening")
            engine = Engine(c)
            result = engine.search(self.x[35],35)
            truth, _ = exact_topk(Dataset(self.root/"data"), np.arange(96,dtype="u8"),
                                 self.x[35],5,"l2",35,17)
            self.assertEqual(result["ids"],truth)
            self.assertEqual(result["stats"]["expanded"],96)
            self.assertGreater(result["stats"]["rpc_count"],3)
            self.assertGreater(result["stats"]["wire_bytes"],0)
            engine.c["search"]["max_expansions"] = 37
            limited = engine.search(self.x[0], 0)
            local_c = copy.deepcopy(c); local_c["network"]["transport"] = "local"
            local = Engine(local_c)
            try:
                expected = local.search(self.x[0],0)
                self.assertEqual(limited["ids"], expected["ids"])
                self.assertEqual(limited["stats"]["expansions_by_shard"],expected["stats"]["expansions_by_shard"])
            finally:
                local.close()
            transport = TCPTransport(c)
            try:
                with self.assertRaisesRegex(ValueError,"epoch"):
                    transport.expand(0,self.x[0],[0],"wrong")
            finally:
                transport.close()
        finally:
            if engine: engine.close()
            for proc in processes: proc.terminate()
            for proc in processes:
                try: proc.communicate(timeout=10)
                except subprocess.TimeoutExpired:
                    proc.kill(); proc.communicate()

    def test_cosine_pipeline(self):
        sub = self.root/"cosine"; sub.mkdir()
        c, x = fixture(sub, "cosine")
        with threadpool_limits(limits=1):
            build(c)
        engine = Engine(c)
        try:
            result = engine.search(x[0],0)
            truth, distances = exact_topk(Dataset(sub/"data"), np.arange(96,dtype="u8"),x[0],5,"cosine",0,17)
            self.assertEqual(result["ids"],truth)
            np.testing.assert_allclose(result["squared_distances"], distances,atol=1e-12)
            with self.assertRaises(ValueError):
                engine.search(np.zeros(64),None)
        finally:
            engine.close()


if __name__ == "__main__":
    unittest.main()
