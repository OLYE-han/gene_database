#!/usr/bin/env python3
"""Run three read-node HTTP benchmarks for IVF-size experiment versions."""

from __future__ import annotations

import argparse
import concurrent.futures
import json
import os
import shutil
import statistics
import subprocess
import sys
import time
from pathlib import Path

import h5py
import numpy as np
import yaml

from run_single_machine_test import (
    percentile,
    request_binary,
    request_json,
    stop_process,
    wait_for_health,
)
from test_read_nodes import free_ports


def atomic_json(path: Path, value: object) -> None:
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, indent=2), encoding="utf-8")
    os.replace(temporary, path)


def make_runtime_index(
    source_root: Path,
    source_version: str,
    runtime_root: Path,
    runtime_version: str,
    nprobe: int,
) -> None:
    source = source_root / "versions" / source_version
    target = runtime_root / "versions" / runtime_version
    if not target.exists():
        target.mkdir(parents=True)
        for name in ("router", "shards"):
            os.symlink((source / name).resolve(), target / name, target_is_directory=True)
        for name in ("node_weights.npy", "source_manifest.json"):
            os.symlink((source / name).resolve(), target / name)
        manifest = json.loads((source / "manifest.json").read_text(encoding="utf-8"))
        manifest["version"] = runtime_version
        manifest["config"]["ivf"]["nprobe"] = nprobe
        (target / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    runtime_root.mkdir(parents=True, exist_ok=True)
    (runtime_root / "CURRENT").write_text(runtime_version + "\n", encoding="utf-8")


def load_trial(
    ports: list[int],
    queries: np.ndarray,
    requests: int,
    concurrency: int,
    binary: bool,
) -> dict[str, object]:
    latencies = []

    def one(number: int) -> float:
        port = ports[number % len(ports)]
        query = queries[number % len(queries)]
        started = time.perf_counter()
        if binary:
            request_binary(f"http://127.0.0.1:{port}/search/binary", query, timeout=60)
        else:
            request_json(
                f"http://127.0.0.1:{port}/search",
                {"vector": query.tolist(), "top_k": 10},
                timeout=60,
            )
        return (time.perf_counter() - started) * 1000

    started = time.perf_counter()
    with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as executor:
        latencies = list(executor.map(one, range(requests)))
    elapsed = time.perf_counter() - started
    return {
        "requests": requests,
        "concurrency": concurrency,
        "qps": requests / elapsed,
        "mean_ms": statistics.mean(latencies),
        "p50_ms": percentile(latencies, 50),
        "p95_ms": percentile(latencies, 95),
        "p99_ms": percentile(latencies, 99),
        "maximum_ms": max(latencies),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--index-root", type=Path, required=True)
    parser.add_argument("--query-h5", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--sizes", nargs="+", type=int, default=[64, 128, 256, 512, 1024, 2048])
    parser.add_argument("--requests", type=int, default=2000)
    parser.add_argument("--warmup", type=int, default=200)
    parser.add_argument("--repeats", type=int, default=5)
    parser.add_argument("--concurrency", type=int, default=12)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    with h5py.File(args.query_h5, "r") as handle:
        queries = np.asarray(handle["embeddings"][:500], dtype=np.float32)
    report_path = args.output / "distributed_results.json"
    report = json.loads(report_path.read_text()) if report_path.exists() else {"results": {}}
    source_root = Path(__file__).resolve().parents[1] / "src"
    environment = os.environ.copy()
    environment["PYTHONPATH"] = str(source_root) + os.pathsep + environment.get("PYTHONPATH", "")
    environment["PYRAMID_ADMIN_TOKEN"] = "ivf-experiment"
    for variable in ("OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS", "NUMEXPR_NUM_THREADS"):
        environment[variable] = "1"

    for size in args.sizes:
        for experiment, nprobe in (("fixed_nprobe", 4), ("fixed_scan_budget", max(1, round(2048 / size)))):
            key = f"{size}:{experiment}"
            item = report["results"].setdefault(
                key,
                {"target_vectors_per_list": size, "experiment": experiment, "nprobe": nprobe},
            )
            if len(item.get("binary_trials", [])) >= args.repeats and len(item.get("json_trials", [])) >= args.repeats:
                continue
            runtime = args.output / "runtime" / key.replace(":", "-") / "index"
            runtime_version = key.replace(":", "-")
            make_runtime_index(args.index_root, f"ivf-size-{size}", runtime, runtime_version, nprobe)
            node_dir = runtime.parent / "nodes"
            node_dir.mkdir(parents=True, exist_ok=True)
            ports = free_ports(3)
            processes: list[subprocess.Popen] = []
            handles = []
            try:
                for node, port in enumerate(ports):
                    config_path = node_dir / f"node-{node}.yaml"
                    config_path.write_text(
                        yaml.safe_dump(
                            {
                                "index_root": str(runtime.resolve()),
                                "cache_bytes": 2 * 1024**3,
                                "host": "127.0.0.1",
                                "port": port,
                                "workers": 1,
                                "top_k_limit": 1000,
                                "partition_workers": 4,
                                "max_inflight_queries": 4,
                                "mmap_indexes": True,
                                "require_native_scan": True,
                                "require_fastscan": True,
                            },
                            sort_keys=False,
                        ),
                        encoding="utf-8",
                    )
                    handle = (node_dir / f"node-{node}.log").open("a", encoding="utf-8")
                    handles.append(handle)
                    processes.append(
                        subprocess.Popen(
                            [sys.executable, "-m", "pyramid_rabitq.api", "--config", str(config_path)],
                            stdout=handle,
                            stderr=subprocess.STDOUT,
                            env=environment,
                        )
                    )
                item["health"] = [wait_for_health(port, timeout=60) for port in ports]
                reference = [
                    request_binary(f"http://127.0.0.1:{port}/search/binary", queries[0])["hits"]
                    for port in ports
                ]
                item["node_consistency"] = all(value == reference[0] for value in reference[1:])
                for number in range(args.warmup):
                    request_binary(
                        f"http://127.0.0.1:{ports[number % 3]}/search/binary",
                        queries[number % len(queries)],
                    )
                for mode in ("binary", "json"):
                    trials = item.setdefault(f"{mode}_trials", [])
                    while len(trials) < args.repeats:
                        trials.append(
                            load_trial(
                                ports,
                                queries,
                                args.requests,
                                args.concurrency,
                                binary=mode == "binary",
                            )
                        )
                        atomic_json(report_path, report)
                        print(key, mode, len(trials), trials[-1], flush=True)
            finally:
                for process in processes:
                    stop_process(process)
                for handle in handles:
                    handle.close()
    report["status"] = "complete"
    atomic_json(report_path, report)


if __name__ == "__main__":
    main()
