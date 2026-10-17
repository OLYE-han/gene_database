"""Sequential pilot evaluation against an exact scan of the indexed corpus."""
from __future__ import annotations

import hashlib
from pathlib import Path
import platform
import time

import numpy as np

from .data import Dataset, fingerprint, load_complete_npy
from .partition import prepare
from .search import Engine


def query_input(c, count):
    path = Path(c["query"]["vectors"])
    paths = [path]
    vectors = load_complete_npy(path)
    start = c["query"]["row"]
    if (vectors.ndim != 2 or vectors.shape[1] != c["data"]["dimension"]
            or vectors.dtype != np.dtype("<f4") or start+count > len(vectors)):
        raise ValueError("Query file shape/dtype/range mismatch")
    ids = None
    if c["query"]["ids"] is not None:
        paths.append(Path(c["query"]["ids"]))
        ids = load_complete_npy(paths[-1])
        if ids.shape != (len(vectors),) or ids.dtype.kind not in "iu" or np.any(ids < 0):
            raise ValueError("Invalid query IDs")
    snapshots = {p: fingerprint(p) for p in paths}
    return vectors[start:start+count], None if ids is None else ids[start:start+count], snapshots


def exact_topk(dataset, indexed_ids, query, k, metric, excluded_id, block_rows):
    """Stable float64 distances on the same float32 transformed vectors as disk."""
    q = prepare(np.asarray(query)[None, :], metric)[0].astype(np.float32).astype(float)
    best_ids = np.empty(0, dtype=np.uint64)
    best_distances = np.empty(0, dtype=float)
    for start in range(0, len(indexed_ids), block_rows):
        ids = indexed_ids[start:start+block_rows]
        x = prepare(dataset.take(ids), metric).astype(np.float32).astype(float)
        delta = x-q
        distances = np.einsum("ij,ij->i", delta, delta)
        if excluded_id is not None:
            keep = ids != excluded_id
            ids, distances = ids[keep], distances[keep]
        best_ids = np.concatenate((best_ids, ids))
        best_distances = np.concatenate((best_distances, distances))
        chosen = np.lexsort((best_ids, best_distances))[:k]
        best_ids, best_distances = best_ids[chosen], best_distances[chosen]
    if len(best_ids) < k:
        raise ValueError("Insufficient eligible exact neighbors")
    return best_ids.tolist(), best_distances.tolist()


def run_benchmark(c):
    """Closed-loop concurrent load; ground truth and warmup are outside timing."""
    from concurrent.futures import ThreadPoolExecutor
    import threading
    import uuid
    from .metrics import (ResourceSampler, distribution, imbalance,
                          summarize_query_timings, summarize_per_query_timings)

    e = c["experiment"]
    count = e["benchmark_queries"]
    queries, qids, snapshots = query_input(c, count)
    dataset = Dataset(c["paths"]["dataset"])
    engines = []
    engine_load_seconds = []
    try:
        for _ in range(e["benchmark_concurrency"]):
            loading = time.perf_counter()
            engines.append(Engine(c))
            engine_load_seconds.append(time.perf_counter()-loading)
        if dataset.identity() != engines[0].manifest["dataset"]:
            raise ValueError("Benchmark source differs from the index build source")
        indexed = load_complete_npy(Path(c["paths"]["output"])/"index/ids.npy")
        started = time.perf_counter()
        truth = []
        for i, query in enumerate(queries):
            qid = None if qids is None else int(qids[i])
            truth.append(exact_topk(dataset,indexed,query,c["search"]["top_k"],c["data"]["metric"],
                qid if c["search"]["exclude_self"] else None,e["exact_block_rows"]))
        exact_seconds = time.perf_counter()-started
        for engine in engines:
            for i in range(e["warmup_queries"]):
                row = i % count
                engine.search(queries[row],None if qids is None else int(qids[row]))
        def node_snapshot():
            # TCP workers share data services; local engines own separate shard instances.
            if c["network"]["transport"] == "tcp":
                return [{"worker":None,"nodes":engines[0].transport.metrics()}]
            return [{"worker":i,"nodes":engine.transport.metrics()} for i,engine in enumerate(engines)]
        before = node_snapshot()
        sampler = ResourceSampler(c["metrics"]["resource_sample_interval_ms"],c["metrics"]["resource_max_samples"]).start()
        jobs = iter(range(count*e["benchmark_repetitions"]))
        lock, start_event = threading.Lock(), threading.Event()
        rows = []
        def worker(worker_id, engine):
            start_event.wait()
            while True:
                with lock:
                    job = next(jobs,None)
                if job is None:
                    return
                i = job % count
                qid = None if qids is None else int(qids[i])
                request_started = time.perf_counter()
                try:
                    found = engine.search(queries[i],qid)
                except Exception as exc:
                    context = getattr(exc,"dhdgs_metrics",{})
                    found = {"request_id":context.get("request_id",str(uuid.uuid4())), "status":"error",
                        "error_type":type(exc).__name__,"error":str(exc),"ids":[],"squared_distances":[],
                        "stats":context.get("stats",{"latency_ms":(time.perf_counter()-request_started)*1000,
                                                      "metrics_complete":False})}
                truth_ids, distances = truth[i]
                hits = len(set(found["ids"]) & set(truth_ids))
                found["stats"]["queue_wait_ms"] = None  # no measured end-to-end admission/server queue
                row = {"job":job,"worker":worker_id,"repetition":job//count,
                    "query_row":c["query"]["row"]+i,"query_id":qid,"recall":hits/len(truth_ids),
                    "truth_ids":truth_ids,"truth_squared_distances":distances,
                    "returned_count":len(found["ids"]),"duplicate_result_ids":len(found["ids"])-len(set(found["ids"])),
                    **found}
                with lock:
                    rows.append(row)
        try:
            with ThreadPoolExecutor(max_workers=len(engines)) as pool:
                futures = [pool.submit(worker,i,engine) for i,engine in enumerate(engines)]
                measured_start = time.perf_counter_ns()
                start_event.set()
                for future in futures: future.result()
                measured_end = time.perf_counter_ns()
        finally:
            resource_samples = sampler.finish()
        after = node_snapshot()
        rows.sort(key=lambda r:r["job"])
        dataset.assert_unchanged()
        if any(fingerprint(p) != previous for p,previous in snapshots.items()):
            raise ValueError("Query file changed during evaluation")
        successful = [r for r in rows if r["status"]=="ok"]
        errors = [r for r in rows if r["status"]!="ok"]
        seconds = (measured_end-measured_start)/1e9
        per_node = {sid:{} for sid in range(len(c["cluster"]["nodes"]))}
        for row in rows:
            for sid, metrics in row["stats"].get("per_shard",{}).items():
                for key,value in metrics.items():
                    per_node[int(sid)][key] = per_node[int(sid)].get(key,0)+value
        load = []
        for b,a in zip(before,after):
            for sid, current in a["nodes"].items():
                previous = b["nodes"][sid]
                proc, oldproc = current["process"],previous["process"]
                deltas = {key:(None if proc[key] is None or oldproc[key] is None else proc[key]-oldproc[key])
                          for key in ("cpu_seconds","storage_read_bytes","storage_write_bytes")}
                span = (proc["monotonic_ns"]-oldproc["monotonic_ns"])/1e9
                load.append({"worker":a["worker"],"shard_id":sid,"process":proc,
                    "process_window_seconds":span,"process_deltas":deltas,
                    "mean_cpu_cores":None if deltas["cpu_seconds"] is None else deltas["cpu_seconds"]/span,
                    "counter_deltas":{key:value-previous["counters"].get(key,0)
                                      for key,value in current["counters"].items() if key not in ("active","peak_active")},
                    "active_after":current["counters"]["active"],
                    "stored_vectors":current["stored_vectors"],"code_rows":current["code_rows"],
                    "index_file_bytes":current["index_file_bytes"],"cache_bytes_after":current["cache_bytes"],
                    "peer_lifecycle_after":current.get('peer_lifecycle'),
                    "peak_active_since_start":current["counters"]["peak_active"],
                    "resource_samples":[sample for sample in current.get("resource_samples",[])
                        if oldproc["monotonic_ns"] <= sample["monotonic_ns"] <= proc["monotonic_ns"]]})
        recall = float(np.mean([r["recall"] for r in rows]))
        return {
            "kind":"closed_loop_load_benchmark", "index_epoch":engines[0].manifest["epoch"],
            "indexed_rows":len(indexed),"query_count":len(rows),"unique_query_rows":count,
            "concurrency":len(engines),"warmup_queries_per_worker":e["warmup_queries"],
            "top_k":c["search"]["top_k"],"metric":c["data"]["metric"],"transport":c["network"]["transport"],
            "measured_wall_seconds":seconds,"successful_queries":len(successful),"failed_queries":len(errors),
            "qps":len(successful)/seconds,"completed_qps":len(rows)/seconds,"error_fraction":len(errors)/len(rows),
            "recall_qualified_qps":sum(r["recall"]>=e["target_recall"] for r in rows)/seconds,
            "incomplete_result_queries":sum(r["returned_count"]<c["search"]["top_k"] for r in rows),
            "duplicate_result_queries":sum(r["duplicate_result_ids"]>0 for r in rows),
            "error_types":{name:sum(r.get("error_type")==name for r in errors)
                           for name in sorted({r["error_type"] for r in errors})},
            "recall_at_k":recall,"recall_distribution":distribution([r["recall"] for r in rows]),
            "target_recall":e["target_recall"],"target_met_on_this_sample":recall>=e["target_recall"],
            "queries_meeting_recall_fraction":sum(r["recall"]>=e["target_recall"] for r in rows)/len(rows),
            "latency_ms":distribution([r["stats"]["latency_ms"] for r in successful]),
            "query_timing_summary":summarize_query_timings(rows),
            "per_query_timing_means":summarize_per_query_timings(rows),
            "failure_latency_ms":distribution([r["stats"]["latency_ms"] for r in errors]),
            "fallback_query_fraction":sum(r["stats"].get("fallback_entries",0)>0 for r in successful)/len(rows),
            "mean_rpc_count":float(np.mean([r["stats"].get("rpc_count",0) for r in rows])) if not errors else None,
            "mean_expansions":float(np.mean([r["stats"].get("expanded",0) for r in rows])) if not errors else None,
            "per_node_query_totals":per_node,"node_process_load":load,
            "expansion_imbalance":imbalance([v.get("exact_distance_evaluations",0) for v in per_node.values()]),
            "coordinator_resource_samples":resource_samples,
            "build_seconds":engines[0].manifest["build_seconds"],
            "engine_load_seconds":engine_load_seconds,
            "build_profile":engines[0].manifest.get("build_profile"),
            "exact_scan_seconds":exact_seconds,"python":platform.python_version(),"numpy":np.__version__,
            "source_sha256":{p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in Path(__file__).parent.glob("*.py")},
            "limitations":["Closed-loop finite query window; not automatically saturation QPS or steady state",
                "Truth preparation, warmup and loading are outside timed load; OS caches are uncontrolled",
                "File pread metrics are not physical SSD latency; process IO is not attributable per query",
                "Same-host blocked-time attribution uses a shared monotonic clock; other clocks remain unattributed",
                "Transport/runtime waiting includes serialization and scheduling, not pure wire time",
                "CPU/RSS/IO process metrics are shared by all shards in that process; do not sum duplicate PIDs",
                "Per-query failure counters may be partial; failure recall is zero and failures remain in denominators",
                "Small-sample p99 is descriptive; exact truth is for indexed rows only",
                "Detailed traces add protocol bytes and measurement overhead; compare identical instrumentation"],
            "queries":rows}
    finally:
        for engine in engines: engine.close()
