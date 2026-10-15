#!/usr/bin/env python3
"""Build and benchmark legacy versus KaFFPa/tuned Pyramid indexes."""

from __future__ import annotations

import argparse
import concurrent.futures
import json
import os
import statistics
import time
from pathlib import Path

import faiss
import h5py
import numpy as np

from pyramid_rabitq.builder import PyramidBuilder
from pyramid_rabitq.config import BuildConfig, HNSWConfig, IVFConfig, dump_config
from pyramid_rabitq.engine import ReadNodeEngine
from pyramid_rabitq.math_utils import prepare_vectors
from pyramid_rabitq.storage import H5Corpus


def atomic_json(path: Path, value: object) -> None:
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, indent=2), encoding="utf-8")
    os.replace(temporary, path)


def copy_h5_rows(source: Path, target: Path, rows: int, dimension: int) -> None:
    if target.exists():
        return
    target.parent.mkdir(parents=True, exist_ok=True)
    with h5py.File(source, "r") as input_handle, h5py.File(target, "w") as output_handle:
        source_data = input_handle["embeddings"]
        count = min(rows, len(source_data))
        output = output_handle.create_dataset(
            "embeddings", shape=(count, dimension), dtype=np.float32, chunks=(min(256, count), dimension)
        )
        for begin in range(0, count, 1024):
            output[begin : begin + 1024] = source_data[begin : min(begin + 1024, count)]


def load_queries(path: Path, count: int, dimension: int) -> np.ndarray:
    with h5py.File(path, "r") as handle:
        values = np.asarray(handle["embeddings"][:count], dtype=np.float32)
    if values.shape != (count, dimension):
        raise ValueError("query file does not contain enough vectors")
    return prepare_vectors(values, "cosine")


def ground_truth(source: Path, queries: np.ndarray, top_k: int, output: Path) -> np.ndarray:
    if output.exists():
        return np.load(output, allow_pickle=False)
    with h5py.File(source, "r") as handle:
        vectors = prepare_vectors(np.asarray(handle["embeddings"], dtype=np.float32), "cosine")
    index = faiss.IndexFlatIP(vectors.shape[1])
    index.add(np.ascontiguousarray(vectors))
    _, ids = index.search(np.ascontiguousarray(queries), top_k)
    np.save(output, ids, allow_pickle=False)
    return ids


def recall(engine: ReadNodeEngine, queries: np.ndarray, expected: np.ndarray) -> dict[str, object]:
    values = []
    candidates = []
    for query, wanted in zip(queries, expected):
        response = engine.search(query, top_k=10)
        values.append(len({hit.id for hit in response.hits} & set(wanted.tolist())) / 10)
        candidates.append(response.approximate_candidates)
    rng = np.random.default_rng(20260929)
    samples = np.asarray(values)
    bootstrap = np.mean(rng.choice(samples, size=(10_000, len(samples)), replace=True), axis=1)
    return {
        "queries": len(values),
        "recall_at_10": float(np.mean(values)),
        "minimum": float(np.min(values)),
        "ci95": [float(np.percentile(bootstrap, 2.5)), float(np.percentile(bootstrap, 97.5))],
        "candidate_mean": float(np.mean(candidates)),
        "candidate_p95": float(np.percentile(candidates, 95)),
    }


def latency_trial(engine: ReadNodeEngine, queries: np.ndarray, requests: int, concurrency: int) -> dict[str, float]:
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
    values = np.asarray(latencies)
    return {
        "qps": requests / elapsed,
        "mean_ms": float(np.mean(values)),
        "p50_ms": float(np.percentile(values, 50)),
        "p95_ms": float(np.percentile(values, 95)),
        "p99_ms": float(np.percentile(values, 99)),
    }


def phase_profile(engine: ReadNodeEngine, queries: np.ndarray) -> dict[str, object]:
    routes, scans, exacts, scanned_codes = [], [], [], []
    candidate_k = 10 * engine.config.ivf.candidate_multiplier
    for query in queries:
        started = time.perf_counter()
        partitions = engine.router.route_from_nodes(
            query[None, :], engine.config.hnsw.route_nodes, engine.config.hnsw.route_partitions
        )
        routes.append((time.perf_counter() - started) * 1000)
        scan_ms = exact_ms = 0.0
        code_count = 0
        for partition in partitions:
            index = engine.indexes[partition]
            selected_lists = index._probe_lists(query, engine.config.ivf.nprobe)
            code_count += sum(int(index.offsets[item + 1] - index.offsets[item]) for item in selected_lists)
            started = time.perf_counter()
            result = index.search_approx(query, candidate_k, engine.config.ivf.nprobe)
            scan_ms += (time.perf_counter() - started) * 1000
            started = time.perf_counter()
            vectors = engine.cache.read_rows(engine.vector_paths[partition], result.rows)
            _ = 1.0 - vectors @ query
            exact_ms += (time.perf_counter() - started) * 1000
        scans.append(scan_ms)
        exacts.append(exact_ms)
        scanned_codes.append(code_count)

    def summary(values):
        array = np.asarray(values)
        return {"mean": float(np.mean(array)), "p50": float(np.percentile(array, 50)), "p95": float(np.percentile(array, 95))}

    return {
        "routing_ms": summary(routes),
        "approximate_scan_ms": summary(scans),
        "exact_fetch_ms": summary(exacts),
        "scanned_codes": summary(scanned_codes),
    }


def build_configs(rows: int, partitions: int, dimension: int, kaffpa: Path) -> dict[str, BuildConfig]:
    legacy_nlist = max(1, round(rows / partitions / 1024))
    common = dict(dimension=dimension, metric="cosine", num_partitions=partitions, seed=42, batch_size=512)
    return {
        "legacy": BuildConfig(
            **common,
            hnsw=HNSWConfig(
                sample_size=max(1, round(rows * 0.10)), sample_ratio=0.10,
                m=16, ef_construction=100, ef_search=96, route_nodes=32,
                route_partitions=2, partition_backend="fallback",
            ),
            ivf=IVFConfig(
                nlist=legacy_nlist, nprobe=min(4, legacy_nlist), train_size=min(rows, 10_000),
                candidate_multiplier=20, scan_block_size=4096,
            ),
        ),
        "optimized": BuildConfig(
            **common,
            hnsw=HNSWConfig(
                sample_size=max(1, round(rows * 0.10)), sample_ratio=0.20,
                m=16, ef_construction=100, ef_search=96, route_nodes=64,
                route_partitions=2, partition_backend="kaffpa",
                kaffpa_executable=str(kaffpa.resolve()), require_kaffpa=True,
            ),
            ivf=IVFConfig(
                target_vectors_per_list=384, nprobe=2, train_size=min(rows, 10_000),
                candidate_multiplier=8, scan_block_size=4096,
            ),
        ),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-h5", type=Path, required=True)
    parser.add_argument("--query-h5", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--kaffpa", type=Path, required=True)
    parser.add_argument("--rows", type=int, default=20_000)
    parser.add_argument("--queries", type=int, default=300)
    parser.add_argument("--dimension", type=int, default=2560)
    parser.add_argument("--partitions", type=int, default=4)
    parser.add_argument("--warmup", type=int, default=200)
    parser.add_argument("--requests", type=int, default=1000)
    parser.add_argument("--repeats", type=int, default=3)
    args = parser.parse_args()

    args.output.mkdir(parents=True, exist_ok=True)
    corpus_dir = args.output / "corpus"
    corpus_path = corpus_dir / "shard_0.h5"
    copy_h5_rows(args.data_h5, corpus_path, args.rows, args.dimension)
    queries = load_queries(args.query_h5, args.queries, args.dimension)
    expected = ground_truth(corpus_path, queries, 10, args.output / "ground_truth_top10.npy")
    configs = build_configs(args.rows, args.partitions, args.dimension, args.kaffpa)
    report_path = args.output / "core_results.json"
    report = {"protocol": vars(args), "results": {}}
    report["protocol"] = {key: str(value.resolve()) if isinstance(value, Path) else value for key, value in report["protocol"].items()}

    corpus = H5Corpus(corpus_dir, args.dimension)
    for name, config in configs.items():
        index_root = args.output / f"index_{name}"
        dump_config(config, args.output / f"build_{name}.yaml")
        build_started = time.perf_counter()
        if not (index_root / "CURRENT").exists():
            PyramidBuilder(corpus, config).build(index_root, name)
        build_seconds = time.perf_counter() - build_started
        engine = ReadNodeEngine(index_root, cache_bytes=512 * 1024**2, partition_workers=4)
        item = {
            "build_seconds_this_run": build_seconds,
            "index_bytes": sum(path.stat().st_size for path in index_root.rglob("*") if path.is_file()),
            "health": engine.health(),
            "manifest": engine.manifest,
            "recall": recall(engine, queries, expected),
            "phase_profile": phase_profile(engine, queries[:100]),
            "latency": {},
        }
        for number in range(args.warmup):
            engine.search(queries[number % len(queries)], top_k=10)
        for concurrency in (1, 4, 12):
            item["latency"][str(concurrency)] = [
                latency_trial(engine, queries, args.requests, concurrency)
                for _ in range(args.repeats)
            ]
            print(name, concurrency, item["latency"][str(concurrency)], flush=True)
        report["results"][name] = item
        atomic_json(report_path, report)
        engine.close()
    report["status"] = "complete"
    atomic_json(report_path, report)


if __name__ == "__main__":
    main()
