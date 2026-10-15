#!/usr/bin/env python3
"""Benchmark isolated, packaged v0.2.0 and current read nodes over HTTP."""

from __future__ import annotations

import argparse
import concurrent.futures
import json
import os
import socket
import statistics
import subprocess
import sys
import time
import zipfile
import hashlib
from pathlib import Path

import h5py
import numpy as np
import yaml

from run_single_machine_test import percentile, request_binary, stop_process, wait_for_health


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


def run_version(
    name: str,
    index_root: Path,
    output: Path,
    queries: np.ndarray,
    concurrency_levels: list[int],
    requests: int,
    repeats: int,
    warmup: int,
    package_root: Path,
    wheel: Path,
    ground_truth: np.ndarray | None,
) -> dict[str, object]:
    ports = free_ports(3)
    environment = os.environ.copy()
    environment["PYTHONPATH"] = str(package_root)
    for variable in ("OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS", "NUMEXPR_NUM_THREADS"):
        environment[variable] = "1"
    processes, handles = [], []
    result: dict[str, object] = {
        "name": name,
        "ports": ports,
        "trials": {},
        "wheel": str(wheel.resolve()),
        "wheel_sha256": hashlib.sha256(wheel.read_bytes()).hexdigest(),
    }
    try:
        for node, port in enumerate(ports):
            config_path = output / f"{name}-node-{node}.yaml"
            service_config = {
                "index_root": str(index_root.resolve()),
                "cache_bytes": 256 * 1024**2,
                "host": "127.0.0.1",
                "port": port,
                "workers": 1,
                "top_k_limit": 100,
                "partition_workers": 4,
                "max_inflight_queries": 32,
                "mmap_indexes": False,
                "require_native_scan": True,
            }
            if name == "v0.5.0":
                service_config["require_fastscan"] = True
            config_path.write_text(yaml.safe_dump(service_config, sort_keys=False), encoding="utf-8")
            handle = (output / f"{name}-node-{node}.log").open("w", encoding="utf-8")
            handles.append(handle)
            processes.append(subprocess.Popen(
                [sys.executable, "-m", "pyramid_rabitq.api", "--config", str(config_path)],
                stdout=handle, stderr=subprocess.STDOUT, env=environment,
            ))
        health = [wait_for_health(port) for port in ports]
        result["health"] = health
        result["code_layouts"] = sorted({
            layout for item in health for layout in item.get("code_layouts", ["row"])
        })

        def one(number: int) -> tuple[float, list[int]]:
            query = queries[number % len(queries)]
            port = ports[number % len(ports)]
            started = time.perf_counter()
            response = request_binary(f"http://127.0.0.1:{port}/search/binary", query, timeout=120)
            elapsed = (time.perf_counter() - started) * 1000
            return elapsed, [hit["id"] for hit in response["hits"]]

        references = []
        for number in range(len(queries)):
            _, ids = one(number)
            references.append(ids)
        result["reference_ids"] = references
        if ground_truth is not None:
            recalls = [
                len(set(ids) & set(wanted.tolist())) / len(wanted)
                for ids, wanted in zip(references, ground_truth[: len(references)])
            ]
            result["recall_at_10"] = float(np.mean(recalls))
            result["recall_minimum"] = float(np.min(recalls))
        for number in range(warmup):
            one(number)

        for concurrency in concurrency_levels:
            trials = []
            for repeat in range(repeats):
                latencies, errors = [], []
                started = time.perf_counter()
                with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as executor:
                    futures = [executor.submit(one, number) for number in range(requests)]
                    for number, future in enumerate(futures):
                        try:
                            latency, ids = future.result()
                            if ids != references[number % len(queries)]:
                                raise AssertionError("result IDs changed under load")
                            latencies.append(latency)
                        except Exception as error:
                            errors.append(repr(error))
                elapsed = time.perf_counter() - started
                trial = {
                    "concurrency": concurrency,
                    "requests": requests,
                    "errors": errors,
                    "qps": (requests - len(errors)) / elapsed,
                    "mean_ms": statistics.mean(latencies) if latencies else None,
                    "p50_ms": percentile(latencies, 50) if latencies else None,
                    "p95_ms": percentile(latencies, 95) if latencies else None,
                    "p99_ms": percentile(latencies, 99) if latencies else None,
                }
                trials.append(trial)
                print(name, concurrency, repeat + 1, trial, flush=True)
            result["trials"][str(concurrency)] = trials
    finally:
        for process in processes:
            stop_process(process)
        for handle in handles:
            handle.close()
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--v020-index", type=Path, required=True)
    parser.add_argument("--current-index", type=Path, required=True)
    parser.add_argument("--v020-wheel", type=Path, required=True)
    parser.add_argument("--current-wheel", type=Path, required=True)
    parser.add_argument("--query-h5", type=Path, required=True)
    parser.add_argument("--ground-truth", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--query-pool", type=int, default=128)
    parser.add_argument("--concurrency", type=int, nargs="+", default=[12, 24, 48, 96])
    parser.add_argument("--requests", type=int, default=1000)
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--warmup", type=int, default=200)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)
    package_roots = {}
    for name, wheel in (("v0.2.0", args.v020_wheel), ("v0.5.0", args.current_wheel)):
        root = args.output / f"package-{name}"
        root.mkdir()
        with zipfile.ZipFile(wheel) as archive:
            archive.extractall(root)
        package_roots[name] = root
    with h5py.File(args.query_h5, "r") as handle:
        queries = np.asarray(handle["embeddings"][: args.query_pool], dtype=np.float32)
    ground_truth = None if args.ground_truth is None else np.load(args.ground_truth, allow_pickle=False)
    if ground_truth is not None and len(ground_truth) < len(queries):
        raise ValueError("ground truth has fewer rows than the query pool")
    report = {
        "status": "running",
        "protocol": {
            "query_pool": args.query_pool,
            "concurrency": args.concurrency,
            "requests": args.requests,
            "repeats": args.repeats,
            "warmup": args.warmup,
            "nodes": 3,
            "protocol": "binary float32 HTTP",
            "max_inflight_queries_per_node": 32,
        },
        "results": {},
    }
    report_path = args.output / "results.json"
    try:
        for name, root, wheel in (
            ("v0.2.0", args.v020_index, args.v020_wheel),
            ("v0.5.0", args.current_index, args.current_wheel),
        ):
            report["results"][name] = run_version(
                name, root, args.output, queries, args.concurrency,
                args.requests, args.repeats, args.warmup,
                package_roots[name], wheel, ground_truth,
            )
            report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
        old_ids = report["results"]["v0.2.0"]["reference_ids"]
        new_ids = report["results"]["v0.5.0"]["reference_ids"]
        report["top10_agreement"] = float(np.mean([
            len(set(old) & set(new)) / 10 for old, new in zip(old_ids, new_ids)
        ]))
        report["status"] = "complete"
    finally:
        report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")


if __name__ == "__main__":
    main()
