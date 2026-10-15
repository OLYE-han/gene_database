#!/usr/bin/env python3
"""Run VecBench's official executors on a prepared filtered YFCC index."""

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

import h5py
import numpy as np
import yaml

ROOT = Path(__file__).resolve().parents[1]
VECBENCH = ROOT / "third_party" / "VecBench-paper"
sys.path.insert(0, str(VECBENCH))

from benchmarks.vecbench_adapter import PyramidVecBench, PyramidVecBenchFactory
from pyramid_rabitq.storage import PartitionMetadata
from utils.concurrent import execute_concurrent
from utils.workload_executor import execute


def free_ports(count):
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


def wait_health(port, timeout=180):
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    deadline, error = time.time() + timeout, None
    while time.time() < deadline:
        try:
            with opener.open(f"http://127.0.0.1:{port}/health", timeout=2) as response:
                return json.loads(response.read())
        except Exception as current:
            error = current
            time.sleep(0.2)
    raise RuntimeError(f"node {port} failed health check: {error}")


def stop(process):
    if process.poll() is None:
        process.terminate()
        try:
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=5)


def sha256(path: Path, block_size=8 * 1024**2):
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while block := handle.read(block_size):
            digest.update(block)
    return digest.hexdigest()


def materialize_queries(corpus_h5: Path, query_yaml: Path, dataset: str, maximum=None):
    raw = yaml.safe_load(query_yaml.read_text(encoding="utf-8"))[dataset]
    raw = raw[:maximum] if maximum else raw
    with h5py.File(corpus_h5, "r") as handle:
        ids = np.asarray(handle["ids"], dtype=np.int64)
        by_id = {int(value): number for number, value in enumerate(ids)}
        queries = []
        for item in raw:
            reference = int(item["reference_vector_name"])
            if reference not in by_id:
                continue
            query = dict(item)
            query["query_vector"] = np.asarray(
                handle["embeddings"][by_id[reference]], dtype=np.float32
            ).tolist()
            queries.append(query)
    return queries


def exact_ground_truth(corpus_h5: Path, queries):
    with h5py.File(corpus_h5, "r") as handle:
        vectors = np.asarray(handle["embeddings"], dtype=np.float32)
        ids = np.asarray(handle["ids"], dtype=np.int64)
    metadata = PartitionMetadata(corpus_h5)
    truth = []
    for query in queries:
        vector = np.asarray(query["query_vector"], dtype=np.float32)
        mask = metadata.filter(query.get("scalar_filters", []))
        selected = np.flatnonzero(mask)
        distances = np.sum((vectors[selected] - vector) ** 2, axis=1)
        keep = min(int(query["limit"]), len(selected))
        if keep:
            nearest = np.argpartition(distances, keep - 1)[:keep]
            nearest = nearest[np.argsort(distances[nearest])]
            result = ids[selected[nearest]].tolist()
        else:
            result = []
        truth.append({"name": query["name"], "result": result})
    return truth


def load_ground_truth(path: Path, queries):
    truth = json.loads(path.read_text(encoding="utf-8"))
    by_name = {row["name"]: row for row in truth}
    missing = [query["name"] for query in queries if query["name"] not in by_name]
    if missing:
        raise ValueError(f"ground truth misses {len(missing)} queries, first={missing[0]}")
    return [by_name[query["name"]] for query in queries]


def profile_status(rows, dimension, query_count, maximum):
    reasons = []
    if rows != 10_000_000:
        reasons.append(f"rows={rows}, expected 10000000")
    if dimension != 1920:
        reasons.append(f"dimension={dimension}, expected 1920")
    if query_count != 3000 or maximum is not None:
        reasons.append(f"queries={query_count}, expected 3000 without truncation")
    return {"shape_valid": not reasons, "deviations": reasons}


def aggregate(rows):
    latencies = np.asarray([row["avg_time"] for row in rows], dtype=np.float64) * 1000
    return {
        "recall": float(np.mean([row["avg_recall"] for row in rows])),
        "qps": float(rows[0]["qps"]),
        "p50_ms": float(np.percentile(latencies, 50)),
        "p95_ms": float(np.percentile(latencies, 95)),
        "p99_ms": float(np.percentile(latencies, 99)),
        "official_rows": rows,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--index-root", type=Path, required=True)
    parser.add_argument("--corpus-h5", type=Path, required=True)
    parser.add_argument("--query-yaml", type=Path, required=True)
    parser.add_argument("--ground-truth-json", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--dataset", default="YFCC")
    parser.add_argument("--max-queries", type=int)
    parser.add_argument("--nodes", type=int, default=3)
    parser.add_argument("--times", type=int, default=10)
    parser.add_argument("--concurrency", type=int, nargs="+", default=[1, 10, 50])
    parser.add_argument(
        "--require-full-profile", action="store_true",
        help="reject anything other than YFCC 10M x 1920 with all 3000 queries",
    )
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)
    queries = materialize_queries(
        args.corpus_h5, args.query_yaml, args.dataset, args.max_queries
    )
    if not queries:
        raise ValueError("no query reference IDs exist in this corpus")
    with h5py.File(args.corpus_h5, "r") as handle:
        dimension, rows = int(handle["embeddings"].shape[1]), int(handle["embeddings"].shape[0])
    profile = profile_status(rows, dimension, len(queries), args.max_queries)
    if args.require_full_profile and not profile["shape_valid"]:
        raise ValueError("not a full VecBench YFCC profile: " + "; ".join(profile["deviations"]))
    if args.ground_truth_json:
        truth = load_ground_truth(args.ground_truth_json, queries)
        ground_truth_method = "precomputed"
    else:
        truth = exact_ground_truth(args.corpus_h5, queries)
        ground_truth_method = "local-exact-l2"
    (args.output / "queries.json").write_text(json.dumps(queries), encoding="utf-8")
    (args.output / "ground_truth.json").write_text(json.dumps(truth), encoding="utf-8")

    ports = free_ports(args.nodes)
    endpoints = [f"http://127.0.0.1:{port}" for port in ports]
    processes, handles = [], []
    report = {
        "status": "running", "dataset": "YFCC", "rows": rows,
        "dimension": dimension, "queries": len(queries), "top_k": 100,
        "benchmark_profile": "reconstructed-public",
        "profile_validation": profile,
        "official_leaderboard_eligible": False,
        "ground_truth_method": ground_truth_method,
        "query_sha256": sha256(args.query_yaml),
        "ground_truth_sha256": sha256(args.output / "ground_truth.json"),
        "query_types": ["equal", "range", "contains"],
        "vecbench_commit": "e503262f65421e9a1a3339f0416163c53a72809e",
        "official_executor": "utils.concurrent.execute_concurrent",
        "dynamic_phases": {"P4_insert": "unsupported", "P5_update": "unsupported", "P6_delete": "unsupported"},
        "vrank": None,
    }
    try:
        environment = os.environ.copy()
        environment["PYTHONPATH"] = str(ROOT / "src")
        for variable in ("OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS", "NUMEXPR_NUM_THREADS"):
            environment[variable] = "1"
        for number, port in enumerate(ports):
            config = args.output / f"node-{number}.yaml"
            config.write_text(yaml.safe_dump({
                "index_root": str(args.index_root.resolve()), "cache_bytes": 512 * 1024**2,
                "host": "127.0.0.1", "port": port, "workers": 1,
                "top_k_limit": 100, "partition_workers": 4, "max_inflight_queries": 32,
                "mmap_indexes": False, "require_native_scan": True, "require_fastscan": True,
            }, sort_keys=False), encoding="utf-8")
            handle = (args.output / f"node-{number}.log").open("w", encoding="utf-8")
            handles.append(handle)
            processes.append(subprocess.Popen(
                [sys.executable, "-m", "pyramid_rabitq.api", "--config", str(config)],
                stdout=handle, stderr=subprocess.STDOUT, env=environment,
            ))
        report["health"] = [wait_health(port) for port in ports]
        params = {"pyramid": ["manifest-fixed"]}
        db = PyramidVecBench({"pyramid": {"endpoint": endpoints[0], "dimension": dimension}})
        db.connect()
        try:
            _, overall = execute(
                db, queries, truth, params, SimpleNamespace(algorithm="pyramid", times=1)
            )
            report["P2_serial"] = overall[0]
        finally:
            db.close()
        report["P3_concurrent"] = []
        for concurrency in args.concurrency:
            factory = PyramidVecBenchFactory(endpoints, dimension, concurrency)
            rows_out = execute_concurrent(
                factory, queries, truth, params,
                SimpleNamespace(algorithm="pyramid", concurrency=concurrency, times=args.times),
            )
            report["P3_concurrent"].append({"concurrency": concurrency, **aggregate(rows_out)})
        report["status"] = "complete"
    except Exception as error:
        report["status"], report["error"] = "failed", repr(error)
        raise
    finally:
        for process in processes:
            stop(process)
        for handle in handles:
            handle.close()
        (args.output / "results.json").write_text(json.dumps(report, indent=2), encoding="utf-8")


if __name__ == "__main__":
    main()
