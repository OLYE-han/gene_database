#!/usr/bin/env python3
"""Measure recall and latency as RaBitQ-IVF list size changes."""

from __future__ import annotations

import argparse
import concurrent.futures
import dataclasses
import json
import os
import statistics
import time
from pathlib import Path

import faiss
import h5py
import numpy as np

from pyramid_rabitq.engine import ReadNodeEngine
from pyramid_rabitq.math_utils import prepare_vectors


def atomic_json(path: Path, value: object) -> None:
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, indent=2), encoding="utf-8")
    os.replace(temporary, path)


def percentiles(values: list[float]) -> dict[str, float]:
    array = np.asarray(values, dtype=np.float64)
    return {
        "mean_ms": float(np.mean(array)),
        "p50_ms": float(np.percentile(array, 50)),
        "p95_ms": float(np.percentile(array, 95)),
        "p99_ms": float(np.percentile(array, 99)),
        "maximum_ms": float(np.max(array)),
    }


def activate(index_root: Path, version: str) -> None:
    temporary = index_root / ".CURRENT.ivf-experiment.tmp"
    temporary.write_text(version + "\n", encoding="utf-8")
    os.replace(temporary, index_root / "CURRENT")


def load_queries(path: Path, count: int, dimension: int) -> np.ndarray:
    with h5py.File(path, "r") as handle:
        values = np.asarray(handle["embeddings"][:count], dtype=np.float32)
    return prepare_vectors(values, "cosine")


def build_ground_truth(
    version_dir: Path,
    queries: np.ndarray,
    output: Path,
    top_k: int,
) -> np.ndarray:
    if output.exists():
        return np.load(output, allow_pickle=False)
    dimension = queries.shape[1]
    exact = faiss.IndexIDMap2(faiss.IndexFlatIP(dimension))
    for vector_path in sorted((version_dir / "shards").glob("partition_*/vectors.h5")):
        with h5py.File(vector_path, "r") as handle:
            dataset = handle["embeddings"]
            ids = handle["ids"]
            for begin in range(0, len(dataset), 2048):
                stop = min(begin + 2048, len(dataset))
                exact.add_with_ids(
                    np.ascontiguousarray(dataset[begin:stop], dtype=np.float32),
                    np.ascontiguousarray(ids[begin:stop], dtype=np.int64),
                )
    _, expected = exact.search(np.ascontiguousarray(queries), top_k)
    np.save(output, expected, allow_pickle=False)
    return expected


def recall_metrics(actual: list[list[int]], expected: np.ndarray) -> dict[str, object]:
    result: dict[str, object] = {}
    for top_k in (1, 10, 100):
        per_query = []
        for row, wanted in zip(actual, expected):
            per_query.append(len(set(row[:top_k]) & set(wanted[:top_k].tolist())) / top_k)
        result[f"recall_at_{top_k}"] = {
            "mean": float(np.mean(per_query)),
            "minimum": float(np.min(per_query)),
            "per_query": per_query,
        }
    return result


def evaluate_recall(
    engine: ReadNodeEngine,
    queries: np.ndarray,
    expected: np.ndarray,
    route_partitions: int,
) -> dict[str, object]:
    actual = []
    candidates = []
    started = time.perf_counter()
    for query in queries:
        response = engine.search(query, top_k=100, route_partitions=route_partitions)
        actual.append([hit.id for hit in response.hits])
        candidates.append(response.approximate_candidates)
    result = recall_metrics(actual, expected)
    result["seconds"] = time.perf_counter() - started
    result["approximate_candidates"] = {
        "mean": float(np.mean(candidates)),
        "p95": float(np.percentile(candidates, 95)),
    }
    return result


def exhaustive_check(
    engine: ReadNodeEngine,
    queries: np.ndarray,
    expected: np.ndarray,
) -> dict[str, object]:
    old_config = engine.config
    maximum_nlist = max(index.nlist for index in engine.indexes.values())
    engine.config = dataclasses.replace(
        engine.config,
        ivf=dataclasses.replace(
            engine.config.ivf,
            nprobe=maximum_nlist,
            candidate_multiplier=10000,
        ),
    )
    try:
        actual = []
        for query in queries:
            response = engine.search(
                query,
                top_k=10,
                route_partitions=len(engine.indexes),
                candidate_multiplier=10000,
            )
            actual.append([hit.id for hit in response.hits])
        values = [
            len(set(row) & set(wanted[:10].tolist())) / 10
            for row, wanted in zip(actual, expected)
        ]
        return {"queries": len(values), "mean_recall_at_10": float(np.mean(values)), "minimum": float(np.min(values))}
    finally:
        engine.config = old_config


def profile_phases(engine: ReadNodeEngine, queries: np.ndarray) -> dict[str, object]:
    routing = []
    approximate = []
    exact_fetch_rerank = []
    scanned = []
    for query in queries:
        q = prepare_vectors(query, engine.config.metric)[0]
        started = time.perf_counter()
        partitions = engine.router.route_from_nodes(
            q[None, :], engine.config.hnsw.route_nodes, engine.config.hnsw.route_partitions
        )
        routing.append((time.perf_counter() - started) * 1000)
        approx_ms = 0.0
        exact_ms = 0.0
        scanned_for_query = 0
        for partition in partitions:
            index = engine.indexes[partition]
            started = time.perf_counter()
            result = index.search_approx(q, 200, engine.config.ivf.nprobe)
            approx_ms += (time.perf_counter() - started) * 1000
            scanned_for_query += sum(
                int(index.offsets[list_id + 1] - index.offsets[list_id])
                for list_id in index._probe_lists(q, engine.config.ivf.nprobe)
            )
            started = time.perf_counter()
            vectors = engine.cache.read_rows(engine.vector_paths[partition], result.rows)
            _ = 1.0 - vectors @ q
            exact_ms += (time.perf_counter() - started) * 1000
        approximate.append(approx_ms)
        exact_fetch_rerank.append(exact_ms)
        scanned.append(scanned_for_query)
    return {
        "routing": percentiles(routing),
        "approximate_scan": percentiles(approximate),
        "exact_fetch_rerank": percentiles(exact_fetch_rerank),
        "scanned_codes": {
            "mean": float(np.mean(scanned)),
            "p95": float(np.percentile(scanned, 95)),
        },
    }


def latency_trial(
    engine: ReadNodeEngine,
    queries: np.ndarray,
    requests: int,
    concurrency: int,
) -> dict[str, float | int]:
    def one(number: int) -> float:
        started = time.perf_counter()
        engine.search(queries[number % len(queries)], top_k=10)
        return (time.perf_counter() - started) * 1000

    started = time.perf_counter()
    if concurrency == 1:
        latencies = [one(number) for number in range(requests)]
    else:
        with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as executor:
            latencies = list(executor.map(one, range(requests)))
    elapsed = time.perf_counter() - started
    return {"requests": requests, "concurrency": concurrency, "qps": requests / elapsed, **percentiles(latencies)}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--index-root", type=Path, required=True)
    parser.add_argument("--query-h5", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--sizes", nargs="+", type=int, default=[64, 128, 256, 512, 1024, 2048])
    parser.add_argument("--recall-queries", type=int, default=500)
    parser.add_argument("--latency-requests", type=int, default=2000)
    parser.add_argument("--warmup", type=int, default=200)
    parser.add_argument("--repeats", type=int, default=5)
    parser.add_argument("--concurrency", nargs="+", type=int, default=[1, 2, 4, 8, 12])
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    queries = load_queries(args.query_h5, args.recall_queries, 2560)
    base_version = args.index_root / "versions" / f"ivf-size-{args.sizes[0]}"
    expected = build_ground_truth(base_version, queries, args.output / "ground_truth_top100.npy", 100)
    report_path = args.output / "core_results.json"
    report = json.loads(report_path.read_text()) if report_path.exists() else {
        "protocol": {
            "sizes": args.sizes,
            "recall_queries": args.recall_queries,
            "latency_requests": args.latency_requests,
            "warmup": args.warmup,
            "repeats": args.repeats,
            "concurrency": args.concurrency,
            "query_h5": str(args.query_h5.resolve()),
        },
        "results": {},
    }
    for size in args.sizes:
        version = f"ivf-size-{size}"
        activate(args.index_root, version)
        manifest = json.loads(
            (args.index_root / "versions" / version / "manifest.json").read_text(encoding="utf-8")
        )
        for experiment, nprobe in (("fixed_nprobe", 4), ("fixed_scan_budget", max(1, round(2048 / size)))):
            key = f"{size}:{experiment}"
            item = report["results"].setdefault(key, {})
            engine = ReadNodeEngine(args.index_root, cache_bytes=2 * 1024**3, partition_workers=4)
            engine.config = dataclasses.replace(
                engine.config,
                ivf=dataclasses.replace(engine.config.ivf, nprobe=nprobe),
            )
            item.update({
                "target_vectors_per_list": size,
                "experiment": experiment,
                "nprobe": nprobe,
                "health": engine.health(),
                "partitions": manifest["partitions"],
            })
            if "balanced_recall" not in item:
                item["balanced_recall"] = evaluate_recall(engine, queries, expected, 2)
                item["ivf_only_recall"] = evaluate_recall(engine, queries, expected, 4)
                item["exhaustive_check"] = exhaustive_check(engine, queries[:20], expected[:20])
                item["phase_profile"] = profile_phases(engine, queries[:100])
                atomic_json(report_path, report)
            for number in range(args.warmup):
                engine.search(queries[number % len(queries)], top_k=10)
            trials = item.setdefault("latency_trials", {})
            for concurrency in args.concurrency:
                values = trials.setdefault(str(concurrency), [])
                while len(values) < args.repeats:
                    values.append(latency_trial(engine, queries, args.latency_requests, concurrency))
                    atomic_json(report_path, report)
                    print(key, "concurrency", concurrency, "repeat", len(values), values[-1], flush=True)
            engine.close()
    report["status"] = "complete"
    atomic_json(report_path, report)


if __name__ == "__main__":
    main()
