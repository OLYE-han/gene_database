#!/usr/bin/env python3
"""Run the official ruc-datalab/VecBench executors against Pyramid read nodes."""

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
from pathlib import Path
from types import SimpleNamespace

import faiss
import h5py
import numpy as np
import yaml


ROOT = Path(__file__).resolve().parents[1]
VECBENCH = ROOT / "third_party" / "VecBench-paper"
sys.path.insert(0, str(VECBENCH))

from benchmarks.vecbench_adapter import PyramidVecBench, PyramidVecBenchFactory
from utils.concurrent import execute_concurrent
from utils.workload_executor import execute


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
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    error = None
    while time.time() < deadline:
        try:
            with opener.open(f"http://127.0.0.1:{port}/health", timeout=2) as response:
                return json.loads(response.read())
        except Exception as current:
            error = current
            time.sleep(0.2)
    raise RuntimeError(f"port {port} did not become healthy: {error}")


def stop_process(process: subprocess.Popen) -> None:
    if process.poll() is None:
        process.terminate()
        try:
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=5)


def exact_truth(corpus_h5: Path, queries: np.ndarray, k: int) -> np.ndarray:
    with h5py.File(corpus_h5, "r") as handle:
        corpus = np.asarray(handle["embeddings"], dtype=np.float32)
    corpus /= np.maximum(np.linalg.norm(corpus, axis=1, keepdims=True), 1e-12)
    normalized = queries / np.maximum(np.linalg.norm(queries, axis=1, keepdims=True), 1e-12)
    index = faiss.IndexFlatIP(corpus.shape[1])
    index.add(np.ascontiguousarray(corpus))
    _, ids = index.search(np.ascontiguousarray(normalized), k)
    return ids


def aggregate(rows: list[dict]) -> dict:
    if not rows:
        return {"queries": 0, "recall": 0.0, "qps": 0.0}
    times = np.asarray([float(row["avg_time"]) for row in rows]) * 1000
    return {
        "queries": len(rows),
        "recall": float(np.mean([row["avg_recall"] for row in rows])),
        "qps": float(rows[0]["qps"]),
        "latency_ms": {
            "mean": float(np.mean(times)),
            "p50": float(np.percentile(times, 50)),
            "p95": float(np.percentile(times, 95)),
            "p99": float(np.percentile(times, 99)),
        },
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--index-root", type=Path, required=True)
    parser.add_argument("--corpus-h5", type=Path, required=True)
    parser.add_argument("--query-h5", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--queries", type=int, default=128)
    parser.add_argument("--k", type=int, default=100)
    parser.add_argument("--nodes", type=int, default=3)
    parser.add_argument("--times", type=int, default=3)
    parser.add_argument("--concurrency", type=int, nargs="+", default=[1, 10, 50])
    args = parser.parse_args()

    args.output.mkdir(parents=True, exist_ok=False)
    with h5py.File(args.query_h5, "r") as handle:
        vectors = np.asarray(handle["embeddings"][: args.queries], dtype=np.float32)
    if len(vectors) != args.queries:
        raise ValueError("query file contains fewer vectors than requested")
    truth = exact_truth(args.corpus_h5, vectors, args.k)
    queries = [
        {
            "name": f"gene_query_{number:04d}",
            "vector_field": "embedding",
            "reference_vector_name": number,
            "query_vector": vector.tolist(),
            "scalar_filters": [],
            "limit": args.k,
        }
        for number, vector in enumerate(vectors)
    ]
    ground_truth = [
        {"name": query["name"], "result": truth[number].tolist()}
        for number, query in enumerate(queries)
    ]
    (args.output / "queries.json").write_text(json.dumps(queries), encoding="utf-8")
    (args.output / "ground_truth.json").write_text(
        json.dumps(ground_truth), encoding="utf-8"
    )

    ports = free_ports(args.nodes)
    endpoints = [f"http://127.0.0.1:{port}" for port in ports]
    processes, handles = [], []
    report_path = args.output / "results.json"
    report = {
        "status": "running",
        "framework": {
            "name": "VecBench: A Controllable Benchmark for Filtered Vector Search",
            "repository": "https://github.com/ruc-datalab/VecBench",
            "commit": "e503262f65421e9a1a3339f0416163c53a72809e",
            "doi": "10.1145/3802125",
            "readme_sha256": hashlib.sha256((VECBENCH / "README.md").read_bytes()).hexdigest(),
            "official_executors": ["utils.workload_executor.execute", "utils.concurrent.execute_concurrent"],
        },
        "scope": {
            "profile": "vector-only compatibility workload",
            "supported": ["prebuilt vector search", "recall", "latency", "concurrent QPS"],
            "unsupported": ["scalar filters", "online insert", "update", "delete", "index build timing"],
        },
        "protocol": {
            "vectors": None,
            "dimension": int(vectors.shape[1]),
            "queries": len(queries),
            "top_k": args.k,
            "nodes": args.nodes,
            "times": args.times,
            "concurrency": args.concurrency,
            "metric": "cosine/IP on L2-normalized vectors",
        },
    }
    with h5py.File(args.corpus_h5, "r") as handle:
        report["protocol"]["vectors"] = len(handle["embeddings"])

    try:
        environment = os.environ.copy()
        environment["PYTHONPATH"] = str(ROOT / "src")
        for variable in ("OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS", "NUMEXPR_NUM_THREADS"):
            environment[variable] = "1"
        for number, port in enumerate(ports):
            config_path = args.output / f"node-{number}.yaml"
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
            handle = (args.output / f"node-{number}.log").open("w", encoding="utf-8")
            handles.append(handle)
            processes.append(subprocess.Popen(
                [sys.executable, "-m", "pyramid_rabitq.api", "--config", str(config_path)],
                stdout=handle,
                stderr=subprocess.STDOUT,
                env=environment,
            ))
        report["health"] = [wait_for_health(port) for port in ports]

        search_params = {"pyramid": ["manifest-fixed"]}
        serial_db = PyramidVecBench({"pyramid": {
            "endpoint": endpoints[0], "dimension": vectors.shape[1], "timeout": 120,
        }})
        serial_db.connect()
        try:
            serial_args = SimpleNamespace(algorithm="pyramid", times=1)
            _, serial_rows = execute(
                serial_db, queries, ground_truth, search_params, serial_args
            )
            report["serial"] = serial_rows[0]
        finally:
            serial_db.close()

        report["concurrent"] = []
        for concurrency in args.concurrency:
            factory = PyramidVecBenchFactory(
                endpoints=endpoints,
                dimension=vectors.shape[1],
                max_connections=concurrency,
            )
            official_args = SimpleNamespace(
                algorithm="pyramid", concurrency=concurrency, times=args.times
            )
            rows = execute_concurrent(
                factory, queries, ground_truth, search_params, official_args
            )
            report["concurrent"].append({
                "concurrency": concurrency,
                **aggregate(rows),
                "official_rows": rows,
            })
        report["status"] = "complete"
    except Exception as error:
        report["status"] = "failed"
        report["error"] = repr(error)
        raise
    finally:
        for process in processes:
            stop_process(process)
        for handle in handles:
            handle.close()
        report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")


if __name__ == "__main__":
    main()
