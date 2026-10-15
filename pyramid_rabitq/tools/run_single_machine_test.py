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
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import h5py
import numpy as np
import yaml

from pyramid_rabitq.builder import PyramidBuilder
from pyramid_rabitq.config import BuildConfig, HNSWConfig, IVFConfig
from pyramid_rabitq.engine import ReadNodeEngine
from pyramid_rabitq.evaluation import evaluate_recall
from pyramid_rabitq.storage import H5Corpus


def percentile(values: list[float], percent: float) -> float:
    return float(np.percentile(np.asarray(values, dtype=np.float64), percent))


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


def request_json(url: str, payload=None, headers=None, timeout: float = 30.0):
    data = None if payload is None else json.dumps(payload).encode("utf-8")
    request_headers = dict(headers or {})
    if data is not None:
        request_headers.setdefault("content-type", "application/json")
    request = urllib.request.Request(
        url,
        data=data,
        headers=request_headers,
        method="GET" if data is None else "POST",
    )
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    with opener.open(request, timeout=timeout) as response:
        return json.loads(response.read().decode("utf-8"))


def request_binary(url: str, vector: np.ndarray, top_k: int = 10, timeout: float = 30.0):
    request = urllib.request.Request(
        url,
        data=np.asarray(vector, dtype="<f4").tobytes(),
        headers={"content-type": "application/octet-stream", "x-top-k": str(top_k)},
        method="POST",
    )
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    with opener.open(request, timeout=timeout) as response:
        return json.loads(response.read().decode("utf-8"))


def create_pilot(source_h5: Path, target: Path, rows: int, shards: int) -> np.ndarray:
    target.mkdir(parents=True, exist_ok=False)
    with h5py.File(source_h5, "r") as source:
        if rows > len(source["embeddings"]):
            raise ValueError(f"requested {rows} rows, source has {len(source['embeddings'])}")
        query = np.asarray(source["embeddings"][0], dtype=np.float32)
        bounds = np.linspace(0, rows, shards + 1, dtype=int)
        for shard_id, (begin, end) in enumerate(zip(bounds[:-1], bounds[1:])):
            with h5py.File(target / f"shard_{shard_id}.h5", "w") as output:
                output.create_dataset(
                    "embeddings",
                    data=np.asarray(source["embeddings"][begin:end], dtype=np.float32),
                    chunks=(min(128, end - begin), source["embeddings"].shape[1]),
                )
                if "Seq_Name" in source:
                    string_type = h5py.string_dtype("utf-8")
                    names = [
                        value.decode("utf-8", errors="replace") if isinstance(value, bytes) else str(value)
                        for value in source["Seq_Name"][begin:end]
                    ]
                    output.create_dataset("Seq_Name", data=names, dtype=string_type)
                if "seq_len" in source:
                    output.create_dataset("seq_len", data=source["seq_len"][begin:end])
    return query


def wait_for_health(port: int, timeout: float = 30.0):
    deadline = time.time() + timeout
    last_error = None
    while time.time() < deadline:
        try:
            return request_json(f"http://127.0.0.1:{port}/health", timeout=2)
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


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-h5", type=Path, required=True)
    parser.add_argument("--work-dir", type=Path, required=True)
    parser.add_argument("--rows", type=int, default=4000)
    parser.add_argument("--source-shards", type=int, default=2)
    parser.add_argument("--partitions", type=int, default=4)
    parser.add_argument("--queries", type=int, default=10)
    parser.add_argument("--requests", type=int, default=60)
    parser.add_argument("--concurrency", type=int, default=12)
    parser.add_argument("--ports", type=int, nargs="+")
    parser.add_argument(
        "--kaffpa-executable",
        type=Path,
        default=Path(__file__).resolve().parents[1] / ".local/kahip-v3.25/bin/kaffpa",
    )
    args = parser.parse_args()
    if args.ports is None:
        args.ports = free_ports(3)

    work_dir = args.work_dir.resolve()
    if work_dir.exists():
        raise FileExistsError(f"test directory already exists: {work_dir}")
    work_dir.mkdir(parents=True)
    pilot_dir = work_dir / "pilot"
    logs_dir = work_dir / "logs"
    logs_dir.mkdir()
    report_path = work_dir / "report.json"
    processes: list[subprocess.Popen] = []
    log_handles = []
    started_at = time.time()
    report: dict[str, object] = {
        "status": "running",
        "source_h5": str(args.source_h5.resolve()),
        "rows": args.rows,
        "partitions": args.partitions,
        "read_nodes": len(args.ports),
    }
    try:
        query = create_pilot(args.source_h5, pilot_dir, args.rows, args.source_shards)
        config = BuildConfig(
            dimension=len(query),
            metric="cosine",
            num_partitions=args.partitions,
            seed=42,
            batch_size=256,
            hnsw=HNSWConfig(
                sample_size=min(800, args.rows),
                sample_ratio=0.20,
                m=16,
                ef_construction=100,
                ef_search=96,
                route_nodes=64,
                route_partitions=min(2, args.partitions),
                partition_backend="kaffpa",
                kaffpa_executable=str(args.kaffpa_executable.resolve()),
                require_kaffpa=True,
            ),
            ivf=IVFConfig(
                target_vectors_per_list=384,
                nprobe=2,
                train_size=min(800, args.rows),
                candidate_multiplier=8,
                scan_block_size=512,
            ),
        )
        config_path = work_dir / "build.yaml"
        config_path.write_text(yaml.safe_dump({
            "dimension": config.dimension,
            "metric": config.metric,
            "num_partitions": config.num_partitions,
            "seed": config.seed,
            "batch_size": config.batch_size,
            "hnsw": config.hnsw.__dict__,
            "ivf": config.ivf.__dict__,
        }, sort_keys=False), encoding="utf-8")

        corpus = H5Corpus(pilot_dir, config.dimension)
        build_started = time.perf_counter()
        index_root = work_dir / "index"
        PyramidBuilder(corpus, config).build(index_root, "single-machine-v1")
        report["build_seconds"] = time.perf_counter() - build_started
        report["index_bytes"] = sum(path.stat().st_size for path in index_root.rglob("*") if path.is_file())

        engine = ReadNodeEngine(index_root, cache_bytes=64 * 1024**2)
        validation_started = time.perf_counter()
        recall = evaluate_recall(engine, corpus, args.queries, top_k=10, batch_size=512)
        report["validation_seconds"] = time.perf_counter() - validation_started
        report["recall"] = recall.as_dict()
        default_result = engine.search(query, top_k=10)
        exhaustive_result = engine.search(
            query,
            top_k=10,
            route_partitions=args.partitions,
            candidate_multiplier=args.rows,
        )
        report["local_query"] = {
            "default_ids": [hit.id for hit in default_result.hits],
            "exhaustive_ids": [hit.id for hit in exhaustive_result.hits],
            "cache_bytes_after_queries": engine.cache.current_bytes,
        }

        source_root = Path(__file__).resolve().parents[1] / "src"
        process_env = os.environ.copy()
        process_env["PYTHONPATH"] = str(source_root) + os.pathsep + process_env.get("PYTHONPATH", "")
        process_env["PYRAMID_ADMIN_TOKEN"] = "single-machine-test"
        for variable in ("OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS", "NUMEXPR_NUM_THREADS"):
            process_env[variable] = "1"
        service_configs = []
        for node, port in enumerate(args.ports):
            service_path = work_dir / f"node-{node}.yaml"
            service_path.write_text(yaml.safe_dump({
                "index_root": str(index_root),
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
            service_configs.append(str(service_path))
            log_handle = (logs_dir / f"node-{node}.log").open("w", encoding="utf-8")
            log_handles.append(log_handle)
            processes.append(subprocess.Popen(
                [sys.executable, "-m", "pyramid_rabitq.api", "--config", str(service_path)],
                stdout=log_handle,
                stderr=subprocess.STDOUT,
                env=process_env,
            ))

        health = [wait_for_health(port) for port in args.ports]
        report["health"] = health
        payload = {"vector": query.tolist(), "top_k": 10}
        node_results = [request_json(f"http://127.0.0.1:{port}/search", payload) for port in args.ports]
        reference_ids = [hit["id"] for hit in node_results[0]["hits"]]
        report["node_consistency"] = {
            "consistent": all([hit["id"] for hit in result["hits"]] == reference_ids for result in node_results),
            "result_ids": reference_ids,
        }

        latencies: list[float] = []
        errors: list[str] = []
        load_started = time.perf_counter()

        def one_request(number: int) -> float:
            port = args.ports[number % len(args.ports)]
            start = time.perf_counter()
            response = request_json(f"http://127.0.0.1:{port}/search", payload, timeout=60)
            ids = [hit["id"] for hit in response["hits"]]
            if ids != reference_ids:
                raise AssertionError(f"node {port} returned inconsistent IDs")
            return (time.perf_counter() - start) * 1000

        with ThreadPoolExecutor(max_workers=args.concurrency) as executor:
            futures = [executor.submit(one_request, number) for number in range(args.requests)]
            for future in as_completed(futures):
                try:
                    latencies.append(future.result())
                except Exception as error:
                    errors.append(repr(error))
        load_seconds = time.perf_counter() - load_started
        report["load"] = {
            "requests": args.requests,
            "concurrency": args.concurrency,
            "errors": errors,
            "qps": (args.requests - len(errors)) / load_seconds,
            "p50_ms": percentile(latencies, 50) if latencies else None,
            "p95_ms": percentile(latencies, 95) if latencies else None,
            "p99_ms": percentile(latencies, 99) if latencies else None,
            "mean_ms": statistics.mean(latencies) if latencies else None,
        }

        reload_response = request_json(
            f"http://127.0.0.1:{args.ports[1]}/admin/reload",
            payload={},
            headers={"x-admin-token": "single-machine-test", "content-type": "application/json"},
        )
        report["reload"] = reload_response

        stopped_port = args.ports[0]
        stop_process(processes[0])
        failed_as_expected = False
        try:
            request_json(f"http://127.0.0.1:{stopped_port}/health", timeout=1)
        except Exception:
            failed_as_expected = True
        survivor = request_json(f"http://127.0.0.1:{args.ports[1]}/search", payload)
        report["failure_test"] = {
            "stopped_port": stopped_port,
            "stopped_node_unreachable": failed_as_expected,
            "survivor_query_ok": [hit["id"] for hit in survivor["hits"]] == reference_ids,
        }
        report["service_configs"] = service_configs
        report["status"] = "passed" if not errors and report["node_consistency"]["consistent"] else "failed"
    except Exception as error:
        report["status"] = "failed"
        report["error"] = repr(error)
        raise
    finally:
        for process in processes:
            stop_process(process)
        for handle in log_handles:
            handle.close()
        report["total_seconds"] = time.time() - started_at
        report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
        print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
