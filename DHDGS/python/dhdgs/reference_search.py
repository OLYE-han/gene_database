"""Global bounded graph search, exact rerank on expansion, remote batch scheduling."""
from __future__ import annotations

from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
import json
import math
from pathlib import Path
import time
import uuid

import numpy as np

from .data import load_complete_npy
from .config import index_settings
from .index import native
from .partition import prepare
from .scheduler import Action, Candidate, Policy
from .transport import LocalTransport, TCPTransport
from .metrics import wait_breakdown


class Engine:
    def __init__(self, c):
        self.c = c
        root = Path(c["paths"]["output"])/"index"
        self.manifest = json.loads((root/"manifest.json").read_text())
        m = self.manifest
        if m.get("build_settings") != index_settings(c):
            raise ValueError("Build parameters differ from the index; restore them or build a new paths.output")
        if (m["version"] != 1 or m["metric"] != c["data"]["metric"]
                or m["dimension"] != c["data"]["dimension"]
                or len(m["shards"]) != len(c["cluster"]["nodes"])):
            raise ValueError("Search configuration is incompatible with built index")
        ids = load_complete_npy(root/"ids.npy")
        member = load_complete_npy(root/"membership.npy")
        if (ids.ndim != 1 or ids.dtype != np.dtype("uint64") or len(ids) != m["rows"]
                or np.any(ids[1:] <= ids[:-1]) or member.dtype != np.dtype("bool")
                or member.shape != (len(ids), len(m["shards"])) or np.any(member.sum(axis=1) == 0)):
            raise ValueError("Invalid global location directory")
        self.owners = {int(gid): tuple(map(int, np.flatnonzero(member[i]))) for i, gid in enumerate(ids)}
        with np.load(root/"route_codes.npz", allow_pickle=False) as bank:
            self.route_bank = {name: np.array(bank[name]) for name in bank.files}
        self.route_positions = {int(gid): i for i, gid in enumerate(self.route_bank["ids"])}
        routing = c["routing"]
        self.router = native().Router(m["dimension"], m["route_samples"], routing["hnsw_m"], routing["ef_construction"], c["seed"])
        self.router.load(str(root/"router.hnsw"))
        self.quantizer = native().Quantizer(self.route_bank["centers"], m["bits"], m["rotation_seed"], (root/"rotation.bin").read_bytes())
        self.transport = LocalTransport(c) if c["network"]["transport"] == "local" else TCPTransport(c)
        self.policy = Policy(c["scheduler"]["kappa"], c["scheduler"]["exploration"], c["scheduler"]["max_wait_ms"])

    def close(self):
        self.transport.close()

    def search(self, vector, query_id=None):
        started = time.perf_counter_ns()
        cpu_started = time.thread_time_ns()
        context = {"request_id": str(uuid.uuid4()), "stats": {}}
        try:
            result = self._search(vector, query_id, context)
        except Exception as exc:
            context["stats"].update(latency_ms=(time.perf_counter_ns()-started)/1e6,
                coordinator_thread_cpu_ms=(time.thread_time_ns()-cpu_started)/1e6,
                metrics_complete=False)
            exc.dhdgs_metrics = context
            raise
        result.update(request_id=context["request_id"], status="ok")
        result["stats"]["coordinator_thread_cpu_ms"] = (time.thread_time_ns()-cpu_started)/1e6
        result["stats"]["metrics_complete"] = result["stats"]["rpc_retries"] == 0
        return result

    def _search(self, vector, query_id, context):
        started = time.perf_counter()
        routing_started = time.perf_counter_ns()
        c, cfg, schedule = self.c, self.c["search"], self.c["scheduler"]
        if query_id is not None and (type(query_id) is not int or not 0 <= query_id < 2**64):
            raise ValueError("query_id must be uint64 or null")
        raw = np.asarray(vector)
        if raw.shape != (self.manifest["dimension"],):
            raise ValueError("Wrong search query dimension")
        query = prepare(raw[None, :], c["data"]["metric"])[0].astype(np.float32)
        k = cfg["top_k"]
        if k > self.manifest["rows"]-int(cfg["exclude_self"] and query_id in self.owners):
            raise ValueError("top_k exceeds eligible index rows")
        entries = list(map(int, self.router.search(query, min(c["routing"]["entry_candidates"], self.manifest["route_samples"]), c["routing"]["ef_search"])))
        positions = np.array([self.route_positions[gid] for gid in entries], dtype=np.int64)
        bank = self.route_bank
        route_quant_started = time.perf_counter_ns()
        ds, us = self.quantizer.score(query, bank["codes"][positions], bank["extra"][positions],
                                      bank["factors"][positions], bank["center_ids"][positions])
        route_quant_ms = (time.perf_counter_ns()-route_quant_started)/1e6
        routing_ms = (time.perf_counter_ns()-routing_started)/1e6
        route_values = {gid: (float(d), float(u)) for gid, d, u in zip(entries, ds, us)}
        home = self.owners[entries[0]][0]
        scale = max(float(np.median(np.abs(ds))), np.finfo(np.float32).eps)
        frontier, discovered = {}, set()
        results = {}
        expanded_ids = set()
        visited_shards = set()
        per_shard = {sid: 0 for sid in range(len(c["cluster"]["nodes"]))}
        service_cost = {sid: schedule["local_batch_ms"]/cfg["local_beam"] for sid in per_shard}
        rpc_overhead = {sid: schedule["remote_rtt_ms"] for sid in per_shard}
        dispatched = 0
        stagnation = 0
        fallback = 0
        provenance, traces, fallback_events = {}, [], []
        rounds = 0
        effective_batches = 0
        wait_parts = wait_breakdown(0,0,[])
        node_metrics = {sid: {} for sid in per_shard}
        stats = {"rpc_count": 0, "wire_bytes": 0, "file_read_bytes": 0, "pread_calls": 0,
                 "cache_hits": 0, "frontier_dropped": 0, "batches": 0,
                 "remote_batches": 0, "max_remote_wait_ms": 0.0,
                 "request_bytes": 0, "response_bytes": 0, "rpc_retries": 0,
                 "scanned_edges": 0, "rabitq_evaluations": len(entries), "exact_distance_evaluations": 0,
                 "pread_ms": 0., "rabitq_ms": 0., "exact_distance_ms": 0., "service_ms": 0.,
                 "server_thread_cpu_ms": 0., "action_ms": 0., "executor_queue_ms": 0.,
                 "candidate_queue_ms_sum": 0.}
        stats["route_rabitq_ms"] = route_quant_ms
        stats["hnsw_distance_evaluations"] = None
        stats["per_shard"] = node_metrics
        context["stats"] = stats

        def enqueue(gid, distance, uncertainty, parent=None):
            if gid not in self.owners or not math.isfinite(distance) or not math.isfinite(uncertainty):
                raise ValueError("Invalid returned neighbor")
            if gid in discovered:
                return
            discovered.add(gid)
            owners = self.owners[gid]
            sid = home if home in owners else (min(owners) if schedule["mode"] == "reference"
                                               else min(owners, key=lambda s: (service_cost[s], s)))
            frontier[gid] = (distance, max(0., uncertainty), sid, time.perf_counter())
            if parent is None:
                provenance[gid] = {"parent":None,"depth":0,"cross_shard_depth":0,"root":gid,"shard":sid}
            else:
                previous = provenance[parent]
                provenance[gid] = {"parent":parent,"depth":previous["depth"]+1,
                    "cross_shard_depth":previous["cross_shard_depth"]+int(previous["shard"] != sid),
                    "root":previous["root"],"shard":sid}

        enqueue(entries[0], *route_values[entries[0]])

        def threshold():
            exact = sorted(results.values())
            return exact[k-1] if len(exact) >= k else max(scale, max(exact, default=scale))

        def try_fallback():
            nonlocal fallback, stagnation
            if not cfg["fallback_enabled"] or fallback >= cfg["max_fallback_entries"]:
                return False
            alternatives = [gid for gid in entries if gid not in discovered]
            alternatives.sort(key=lambda gid: (all(s in visited_shards for s in self.owners[gid]), route_values[gid][0], gid))
            if not alternatives:
                return False
            gid = alternatives[0]
            enqueue(gid, *route_values[gid])
            fallback += 1
            fallback_events.append({"entry":gid,"elapsed_ms":(time.perf_counter()-started)*1000,
                                    "expansions_before":dispatched})
            stagnation = 0
            return True

        inflight = {}
        with ThreadPoolExecutor(max_workers=cfg["max_inflight"]) as pool:
            while frontier or inflight or (dispatched < cfg["max_expansions"] and try_fallback()):
                busy = {info[0] for info in inflight.values()}
                while frontier and len(inflight) < cfg["max_inflight"] and dispatched < cfg["max_expansions"]:
                    now = time.perf_counter()
                    groups = {}
                    for gid, (d, u, sid, added) in frontier.items():
                        if sid not in busy:
                            groups.setdefault(sid, []).append((d, gid, u, added))
                    if not groups:
                        break
                    actions, batches = [], {}
                    for sid, candidates in groups.items():
                        candidates.sort()
                        limit = cfg["local_beam"] if sid == home else c["network"]["batch_size"]
                        batch = candidates[:min(limit, cfg["max_expansions"]-dispatched)]
                        age = (now-min(item[3] for item in batch))*1000
                        wire_estimate = len(query)*4 + len(batch)*16
                        cost = service_cost[sid]*len(batch)
                        if sid != home or c["network"]["transport"] == "tcp":
                            cost += rpc_overhead[sid]+wire_estimate/schedule["bandwidth_bytes_per_ms"]
                        action = Action(str(sid), sid != home,
                            tuple(Candidate(gid, d, u) for d, gid, u, _ in batch), max(cost, 1e-9), age)
                        actions.append(action)
                        batches[str(sid)] = [gid for _, gid, _, _ in batch]
                    if schedule["mode"] == "reference":
                        action = min(actions, key=lambda a: (a.candidates[0].estimated_distance, a.candidates[0].global_id))
                    elif schedule["mode"] == "local_first":
                        expired = [a for a in actions if a.remote and a.wait_ms >= schedule["max_wait_ms"]]
                        action = min(expired, key=lambda a: (-a.wait_ms, a.key)) if expired else min(actions, key=lambda a: (a.remote, a.candidates[0].estimated_distance, a.key))
                    else:
                        action = self.policy.choose(actions, threshold(), scale)
                    sid = int(action.key)
                    batch = batches[action.key]
                    for gid in batch:
                        stats["candidate_queue_ms_sum"] += (now-frontier[gid][3])*1000
                        del frontier[gid]
                    dispatched += len(batch)
                    stats["expanded"] = dispatched
                    visited_shards.add(sid)
                    per_shard[sid] += len(batch)
                    submitted_ns = time.perf_counter_ns()
                    future = pool.submit(self.transport.expand, sid, query, batch, self.manifest["epoch"], context["request_id"])
                    inflight[future] = (sid, batch, submitted_ns)
                    busy.add(sid)
                    stats["batches"] += 1
                    if sid != home:
                        stats["remote_batches"] += 1
                        stats["max_remote_wait_ms"] = max(stats["max_remote_wait_ms"], action.wait_ms)
                if not inflight:
                    break
                wait_start = time.perf_counter_ns()
                done, _ = wait(inflight, return_when=FIRST_COMPLETED)
                wait_end = time.perf_counter_ns()
                rounds += 1
                completed = [future.result() for future in done]
                for key, value in wait_breakdown(wait_start, wait_end, completed).items():
                    wait_parts[key] += value
                for future in done:
                    sid, batch, submitted_ns = inflight.pop(future)
                    response = future.result()  # errors are visible, not converted into empty results
                    if (response["epoch"] != self.manifest["epoch"] or response["shard_id"] != sid
                            or sorted(gid for gid, _ in response["expanded"]) != sorted(batch)):
                        raise ValueError("Expansion response does not match dispatched batch")
                    old = threshold() if len(results) >= k else math.inf
                    expanded_ids.update(batch)
                    for gid, distance in response["expanded"]:
                        if not math.isfinite(distance) or distance < 0:
                            raise ValueError("Invalid exact distance")
                        if not (cfg["exclude_self"] and gid == query_id):
                            results[gid] = distance
                    parents = dict(response["parents"])
                    if any(parent not in batch for parent in parents.values()):
                        raise ValueError("Neighbor discovery parent is not in expanded batch")
                    for gid, d, u in response["neighbors"]:
                        enqueue(int(gid), float(d), float(u), parents[gid])
                    measured = response["stats"]["service_ms"]
                    w = schedule["ewma_weight"]
                    service_cost[sid] = (1-w)*service_cost[sid]+w*max(measured/len(batch), 1e-9)
                    if c["network"]["transport"] == "tcp":
                        observed = max(0., response["stats"]["action_ms"]-measured
                                       -response["stats"]["wire_bytes"]/schedule["bandwidth_bytes_per_ms"])
                        rpc_overhead[sid] = (1-w)*rpc_overhead[sid]+w*observed
                    for key in ("rpc_count", "wire_bytes", "file_read_bytes", "pread_calls", "cache_hits",
                                "request_bytes", "response_bytes", "rpc_retries", "scanned_edges",
                                "rabitq_evaluations", "exact_distance_evaluations", "pread_ms", "rabitq_ms",
                                "exact_distance_ms", "service_ms", "server_thread_cpu_ms", "action_ms"):
                        stats[key] += response["stats"][key]
                        node_metrics[sid][key] = node_metrics[sid].get(key,0)+response["stats"][key]
                    stats["executor_queue_ms"] += max(0., (response["client_start_ns"]-submitted_ns)/1e6)
                    if c["metrics"]["detailed_trace"]:
                        traces.append({"shard_id":sid,"ids":batch,"submitted_ns":submitted_ns,
                            "rpc_request_id":response["rpc_request_id"],"query_request_id":context["request_id"],
                            "client_start_ns":response["client_start_ns"],"client_end_ns":response["client_end_ns"],
                            "service_trace":response["service_trace"],"stats":response["stats"]})
                    effective_batches += int(threshold() < old)
                    stagnation = 0 if threshold() < old else stagnation+1
                if len(frontier) > cfg["frontier_size"]:
                    retained = sorted(frontier, key=lambda gid: (frontier[gid][0], gid))[:cfg["frontier_size"]]
                    stats["frontier_dropped"] += len(frontier)-len(retained)
                    frontier = {gid: frontier[gid] for gid in retained}
                if dispatched < cfg["max_expansions"] and stagnation >= cfg["stagnation_batches"]:
                    try_fallback()
                if dispatched >= cfg["max_expansions"] and not inflight:
                    break
        ranked = sorted(results.items(), key=lambda item: (item[1], item[0]))[:k]
        latency_ms = (time.perf_counter()-started)*1000
        breakdown = {"routing_wall_ms":routing_ms, **wait_parts,
                     "coordinator_other_wall_ms":max(0.,latency_ms-routing_ms-sum(wait_parts.values()))}
        roots = {event["entry"] for event in fallback_events}
        stats.update(expanded=dispatched, exact_reranked=len(results), discovered=len(discovered),
                     pending_at_stop=len(frontier), fallback_entries=fallback,
                     shards_visited=sorted(visited_shards), expansions_by_shard=per_shard,
                     latency_ms=latency_ms, graph_rounds=rounds,
                     max_discovery_depth=max(p["depth"] for p in provenance.values()),
                     max_cross_shard_depth=max(p["cross_shard_depth"] for p in provenance.values()),
                     max_expanded_depth=max((provenance[gid]["depth"] for gid in expanded_ids),default=0),
                     result_paths=[{"id":gid,**provenance[gid]} for gid,_ in ranked],
                     fallback_events=fallback_events,
                     fallback_descendant_expansions=sum(provenance[gid]["root"] in roots for gid in expanded_ids),
                     improved_topk_batches=effective_batches,
                     per_shard=node_metrics,
                     latency_breakdown_ms=breakdown,
                     latency_breakdown_fraction={key:value/latency_ms for key,value in breakdown.items()},
                     pure_network_latency_fraction=None, physical_ssd_latency_fraction=None,
                     server_to_server_migrations=0,
                     stop_reason="budget_exhausted" if dispatched >= cfg["max_expansions"] else "frontier_exhausted",
                     transport=c["network"]["transport"])
        result = {"ids": [gid for gid, _ in ranked], "squared_distances": [d for _, d in ranked],
                "metric": self.manifest["metric"], "epoch": self.manifest["epoch"], "stats": stats}
        if c["metrics"]["detailed_trace"]:
            result["batch_trace"] = traces
            result["discovery_tree"] = [{"id":gid,**p} for gid,p in provenance.items()]
        return result
