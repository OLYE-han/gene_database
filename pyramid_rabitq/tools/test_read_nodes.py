#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import socket
import statistics
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
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


def free_ports(count: int) -> list[int]:
    sockets = []
    ports = []
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


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--index-root", type=Path, required=True)
    parser.add_argument("--query-h5", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--nodes", type=int, default=3)
    parser.add_argument("--requests", type=int, default=60)
    parser.add_argument("--concurrency", type=int, default=12)
    args = parser.parse_args()

    output = args.output_dir.resolve()
    output.mkdir(parents=True, exist_ok=False)
    ports = free_ports(args.nodes)
    with h5py.File(args.query_h5, "r") as handle:
        query = np.asarray(handle["embeddings"][0], dtype=np.float32)
    payload = {"vector": query.tolist(), "top_k": 10}
    source_root = Path(__file__).resolve().parents[1] / "src"
    environment = os.environ.copy()
    environment["PYTHONPATH"] = str(source_root) + os.pathsep + environment.get("PYTHONPATH", "")
    environment["PYRAMID_ADMIN_TOKEN"] = "single-machine-test"
    for variable in ("OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS", "NUMEXPR_NUM_THREADS"):
        environment[variable] = "1"
    processes = []
    handles = []
    report = {"status": "running", "ports": ports, "nodes": args.nodes}
    started = time.perf_counter()
    try:
        for node, port in enumerate(ports):
            config_path = output / f"node-{node}.yaml"
            config_path.write_text(yaml.safe_dump({
                "index_root": str(args.index_root.resolve()),
                "cache_bytes": 64 * 1024**2,
                "host": "127.0.0.1",
                "port": port,
                "workers": 1,
                "top_k_limit": 100,
                "partition_workers": 4,
                "max_inflight_queries": 4,
                "mmap_indexes": True,
                "require_native_scan": True,
                "require_fastscan": True,
            }, sort_keys=False), encoding="utf-8")
            handle = (output / f"node-{node}.log").open("w", encoding="utf-8")
            handles.append(handle)
            processes.append(subprocess.Popen(
                [sys.executable, "-m", "pyramid_rabitq.api", "--config", str(config_path)],
                stdout=handle,
                stderr=subprocess.STDOUT,
                env=environment,
            ))

        report["health"] = [wait_for_health(port) for port in ports]
        results = [request_json(f"http://127.0.0.1:{port}/search", payload) for port in ports]
        reference_ids = [hit["id"] for hit in results[0]["hits"]]
        report["consistency"] = {
            "ok": all([hit["id"] for hit in result["hits"]] == reference_ids for result in results),
            "ids": reference_ids,
        }

        def run_load(binary: bool):
            latencies = []
            errors = []
            load_started = time.perf_counter()

            def query_node(number: int) -> float:
                port = ports[number % len(ports)]
                start = time.perf_counter()
                if binary:
                    result = request_binary(
                        f"http://127.0.0.1:{port}/search/binary", query, timeout=60
                    )
                else:
                    result = request_json(
                        f"http://127.0.0.1:{port}/search", payload, timeout=60
                    )
                if [hit["id"] for hit in result["hits"]] != reference_ids:
                    raise AssertionError("inconsistent search result")
                return (time.perf_counter() - start) * 1000

            with ThreadPoolExecutor(max_workers=args.concurrency) as executor:
                futures = [executor.submit(query_node, number) for number in range(args.requests)]
                for future in as_completed(futures):
                    try:
                        latencies.append(future.result())
                    except Exception as error:
                        errors.append(repr(error))
            elapsed = time.perf_counter() - load_started
            return {
                "requests": args.requests,
                "concurrency": args.concurrency,
                "errors": errors,
                "qps": (args.requests - len(errors)) / elapsed,
                "mean_ms": statistics.mean(latencies) if latencies else None,
                "p50_ms": percentile(latencies, 50) if latencies else None,
                "p95_ms": percentile(latencies, 95) if latencies else None,
                "p99_ms": percentile(latencies, 99) if latencies else None,
            }

        report["load_json"] = run_load(binary=False)
        report["load_binary"] = run_load(binary=True)
        report["reload"] = request_json(
            f"http://127.0.0.1:{ports[1]}/admin/reload",
            payload={},
            headers={"x-admin-token": "single-machine-test", "content-type": "application/json"},
        )
        stop_process(processes[0])
        unavailable = False
        try:
            request_json(f"http://127.0.0.1:{ports[0]}/health", timeout=1)
        except Exception:
            unavailable = True
        survivor = request_json(f"http://127.0.0.1:{ports[1]}/search", payload)
        report["failure"] = {
            "stopped_node_unavailable": unavailable,
            "surviving_node_ok": [hit["id"] for hit in survivor["hits"]] == reference_ids,
        }
        load_errors = report["load_json"]["errors"] + report["load_binary"]["errors"]
        report["status"] = "passed" if report["consistency"]["ok"] and not load_errors else "failed"
    except Exception as error:
        report["status"] = "failed"
        report["error"] = repr(error)
        raise
    finally:
        for process in processes:
            stop_process(process)
        for handle in handles:
            handle.close()
        report["seconds"] = time.perf_counter() - started
        (output / "report.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
        print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
