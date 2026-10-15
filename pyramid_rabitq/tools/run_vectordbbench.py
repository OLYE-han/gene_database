#!/usr/bin/env python3
"""Evaluate a prebuilt Pyramid index with official VectorDBBench search runners."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import socket
import subprocess
import sys
import time
import urllib.request
import zipfile
from pathlib import Path

import faiss
import h5py
import numpy as np
import yaml

from benchmarks.vectordbbench_adapter import PyramidHTTPVectorDB
from vectordb_bench.backend.runner.mp_runner import MultiProcessingSearchRunner
from vectordb_bench.backend.runner.serial_runner import SerialSearchRunner


def free_ports(count: int) -> list[int]:
    sockets, ports = [], []
    try:
        for _ in range(count):
            sock = socket.socket()
            sock.bind(("127.0.0.1", 0))
            sockets.append(sock)
            ports.append(sock.getsockname()[1])
    finally:
        for sock in sockets:
            sock.close()
    return ports


def wait_for_health(port: int, timeout: float = 180) -> dict:
    deadline = time.time() + timeout
    last_error = None
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    while time.time() < deadline:
        try:
            with opener.open(f"http://127.0.0.1:{port}/health", timeout=2) as response:
                return json.loads(response.read())
        except Exception as error:
            last_error = error
            time.sleep(0.2)
    raise RuntimeError(f"port {port} did not become healthy: {last_error}")


def stop_process(process: subprocess.Popen) -> None:
    if process.poll() is None:
        process.terminate()
        try:
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=5)


def exact_ground_truth(corpus_h5: Path, queries: np.ndarray, k: int, output: Path) -> np.ndarray:
    if output.exists():
        values = np.load(output, allow_pickle=False)
        if values.shape != (len(queries), k):
            raise ValueError(f"cached ground truth has shape {values.shape}, expected {(len(queries), k)}")
        return values
    with h5py.File(corpus_h5, "r") as handle:
        vectors = np.asarray(handle["embeddings"], dtype=np.float32)
    vectors /= np.maximum(np.linalg.norm(vectors, axis=1, keepdims=True), 1e-12)
    normalized_queries = queries.copy()
    normalized_queries /= np.maximum(np.linalg.norm(normalized_queries, axis=1, keepdims=True), 1e-12)
    index = faiss.IndexFlatIP(vectors.shape[1])
    index.add(np.ascontiguousarray(vectors))
    _, ids = index.search(np.ascontiguousarray(normalized_queries), k)
    np.save(output, ids, allow_pickle=False)
    return ids


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--index-root", type=Path, required=True)
    parser.add_argument("--wheel", type=Path, required=True)
    parser.add_argument("--corpus-h5", type=Path, required=True)
    parser.add_argument("--query-h5", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--queries", type=int, default=128)
    parser.add_argument("--k", type=int, default=100)
    parser.add_argument("--nodes", type=int, default=3)
    parser.add_argument("--warmup", type=int, default=200)
    parser.add_argument("--duration", type=int, default=30)
    parser.add_argument("--concurrency", type=int, nargs="+", default=[1, 5, 10, 20, 30, 40, 60, 80])
    args = parser.parse_args()

    args.output.mkdir(parents=True, exist_ok=False)
    package_root = args.output / "package"
    package_root.mkdir()
    with zipfile.ZipFile(args.wheel) as archive:
        archive.extractall(package_root)
    with h5py.File(args.query_h5, "r") as handle:
        queries = np.asarray(handle["embeddings"][: args.queries], dtype=np.float32)
    if len(queries) != args.queries:
        raise ValueError("query file does not contain the requested number of vectors")
    with h5py.File(args.corpus_h5, "r") as handle:
        corpus_vectors = len(handle["embeddings"])
    ground_truth = exact_ground_truth(args.corpus_h5, queries, args.k, args.output / "ground_truth.npy")

    report_path = args.output / "results.json"
    report: dict[str, object] = {
        "status": "running",
        "framework": {
            "name": "Zilliz VectorDBBench",
            "source": "third_party/VectorDBBench",
            "readme_sha256": hashlib.sha256(
                (Path(__file__).resolve().parents[1] / "third_party/VectorDBBench/README.md").read_bytes()
            ).hexdigest(),
        },
        "protocol": {
            "mode": "preloaded search performance",
            "vectors": corpus_vectors,
            "dimension": int(queries.shape[1]),
            "queries": len(queries),
            "k": args.k,
            "nodes": args.nodes,
            "warmup": args.warmup,
            "duration_seconds_per_concurrency": args.duration,
            "concurrency": args.concurrency,
        },
        "wheel": {
            "path": str(args.wheel.resolve()),
            "sha256": hashlib.sha256(args.wheel.read_bytes()).hexdigest(),
        },
    }
    processes, handles = [], []
    ports = free_ports(args.nodes)
    try:
        server_environment = os.environ.copy()
        server_environment["PYTHONPATH"] = str(package_root.resolve())
        for variable in ("OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS", "NUMEXPR_NUM_THREADS"):
            server_environment[variable] = "1"
        for node, port in enumerate(ports):
            config_path = args.output / f"node-{node}.yaml"
            config_path.write_text(yaml.safe_dump({
                "index_root": str(args.index_root.resolve()),
                "cache_bytes": 256 * 1024**2,
                "host": "127.0.0.1",
                "port": port,
                "workers": 1,
                "top_k_limit": max(100, args.k),
                "partition_workers": 4,
                "max_inflight_queries": 32,
                "mmap_indexes": False,
                "require_native_scan": True,
                "require_fastscan": True,
            }, sort_keys=False), encoding="utf-8")
            handle = (args.output / f"node-{node}.log").open("w", encoding="utf-8")
            handles.append(handle)
            processes.append(subprocess.Popen(
                [sys.executable, "-m", "pyramid_rabitq.api", "--config", str(config_path)],
                stdout=handle,
                stderr=subprocess.STDOUT,
                env=server_environment,
            ))
        report["health"] = [wait_for_health(port, timeout=180) for port in ports]
        db = PyramidHTTPVectorDB(
            dim=queries.shape[1],
            db_config={"endpoints": [f"http://127.0.0.1:{port}" for port in ports], "timeout": 120},
        )
        normalized = queries / np.maximum(np.linalg.norm(queries, axis=1, keepdims=True), 1e-12)
        with db.init():
            for number in range(args.warmup):
                db.search_embedding(normalized[number % len(normalized)].tolist(), args.k)

        serial = SerialSearchRunner(
            db=db,
            test_data=normalized.tolist(),
            ground_truth=ground_truth.tolist(),
            k=args.k,
        )
        serial_result, serial_duration = serial.run()
        recall, ndcg, p99, p95, p50, recall_at = serial_result
        report["serial"] = {
            "duration_seconds": serial_duration,
            "recall": recall,
            "ndcg": ndcg,
            "p50_ms": p50 * 1000,
            "p95_ms": p95 * 1000,
            "p99_ms": p99 * 1000,
            "recall_at": {str(key): value for key, value in recall_at.items()},
        }

        concurrent = MultiProcessingSearchRunner(
            db=db,
            test_data=normalized.tolist(),
            k=args.k,
            concurrencies=args.concurrency,
            duration=args.duration,
            concurrency_timeout=600,
        )
        values = concurrent.run()
        names = ("max_qps", "concurrency", "qps", "p99_s", "p95_s", "avg_s", "p50_s")
        raw_concurrent = dict(zip(names, values))
        report["concurrent"] = {
            "max_qps": raw_concurrent["max_qps"],
            "rows": [
                {
                    "concurrency": level,
                    "qps": qps,
                    "p50_ms": p50_s * 1000,
                    "p95_ms": p95_s * 1000,
                    "p99_ms": p99_s * 1000,
                    "average_ms": avg_s * 1000,
                }
                for level, qps, p50_s, p95_s, p99_s, avg_s in zip(
                    raw_concurrent["concurrency"],
                    raw_concurrent["qps"],
                    raw_concurrent["p50_s"],
                    raw_concurrent["p95_s"],
                    raw_concurrent["p99_s"],
                    raw_concurrent["avg_s"],
                )
            ],
        }
        report["status"] = "complete"
    finally:
        for process in processes:
            stop_process(process)
        for handle in handles:
            handle.close()
        report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")


if __name__ == "__main__":
    main()
