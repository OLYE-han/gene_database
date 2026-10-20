"""Pilot Vamana/RaBitQ disk index construction and per-shard batch expansion.

The builder currently retains the selected corpus and graph in RAM. It has an
explicit preflight limit and is not a billion-scale external-memory builder.
"""
from __future__ import annotations

from collections import OrderedDict
import hashlib
import json
import math
import os
from pathlib import Path
import shutil
import struct
import threading
import time
import uuid

import numpy as np

from .config import index_settings, snapshot
from .data import Dataset, load_complete_npy, sample_ids, write_json
from .partition import calibrate, fit, memberships, prepare, summary
from .metrics import Recorder, process_snapshot


MAGIC = b"DHDGS001"
HEADER = struct.Struct("<8sIIQQ")


def native():
    try:
        from . import _native
    except ImportError as exc:
        raise RuntimeError("Native module unavailable. Build DHDGS with CMake first; no fake RaBitQ fallback.") from exc
    return _native


def shard_path(c, sid):
    custom = c["cluster"]["nodes"][sid]["index_directory"]
    return Path(custom) if custom else Path(c["paths"]["output"])/"index"/f"shard_{sid}"


def _connected(adjacency):
    n = len(adjacency)
    reverse = [set() for _ in adjacency]
    for i, neighbors in enumerate(adjacency):
        for j in neighbors:
            if not 0 <= j < n or j == i:
                raise ValueError("Invalid global edge")
            reverse[j].add(i)
    for graph in (adjacency, reverse):
        visited = {0}
        stack = [0]
        while stack:
            for v in graph[stack.pop()]:
                if v not in visited:
                    visited.add(v)
                    stack.append(v)
        if len(visited) != n:
            raise ValueError("Constructed graph is not strongly connected")


def build(c, *, shard_workers=1, graph_threads=1, checkpoint_directory=None, reserve_disk_bytes=0, progress=None):
    from concurrent.futures import ThreadPoolExecutor
    if shard_workers < 1 or graph_threads < 1 or reserve_disk_bytes < 0:
        raise ValueError('Invalid build worker count or disk reserve')
    build_start = time.perf_counter()
    process_before = process_snapshot()
    phase_start, cpu_start = build_start, time.process_time()
    phases = []
    def mark(name):
        nonlocal phase_start, cpu_start
        now, cpu = time.perf_counter(), time.process_time()
        phases.append({"phase":name,"wall_seconds":now-phase_start,"cpu_seconds":cpu-cpu_start})
        phase_start, cpu_start = now, cpu
        if progress is not None: progress({'phase':name,'elapsed_seconds':now-build_start})
    api = native()
    dataset = Dataset(c["paths"]["dataset"])
    if dataset.dimension != c["data"]["dimension"]:
        raise ValueError("Configured dimension differs from source dataset")
    n = min(dataset.rows, c["data"]["row_limit"] or dataset.rows)
    shards = len(c["cluster"]["nodes"])
    p, g = c["partition"], c["build"]
    if n < p["train_rows"] + p["calibration_rows"] or p["train_rows"] < shards:
        raise ValueError("Selected corpus must cover disjoint training/calibration samples")
    # Includes conservative Python edge/set and matrix temporaries, not an OS RSS guarantee.
    estimate = n * dataset.dimension * (24 + shards*c["quantization"]["bits"]/8)
    estimate += n * (g["degree"]+2) * 128 * shards
    if estimate > g["memory_limit_bytes"]:
        raise ValueError(f"In-memory pilot build estimate {int(estimate)} exceeds configured memory limit; lower row_limit or raise limit knowingly")
    root = Path(c["paths"]["output"])/"index"
    if root.exists():
        raise ValueError("Index already exists; choose a new paths.output")
    # Build into a version directory, publish manifest last. No writes to remote custom paths.
    root.mkdir(parents=True)
    snapshot(c, root)
    started = time.perf_counter()
    ids = sample_ids(dataset.rows, n, c["seed"]) if n != dataset.rows else np.arange(n, dtype=np.uint64)
    vectors = prepare(dataset.take(ids), c["data"]["metric"]).astype(np.float32)
    mark("load_sample_prepare")
    order = np.random.default_rng(c["seed"]).permutation(n)
    model = fit(vectors[order[:p["train_rows"]]], shards, "l2", c["seed"],
                p["kmeans_iterations"], p["kmeans_tolerance"])
    if not model.converged:
        raise ValueError("Partition training did not converge; increase kmeans_iterations")
    mark("kmeans")
    # vectors already transformed once; model geometry is always L2 in this space.
    cal = order[p["train_rows"]:p["train_rows"]+p["calibration_rows"]]
    _, cal_scores = model.scores(vectors[cal], p["method"])
    threshold = calibrate(cal_scores, ids[cal], p["overlap_fraction"], c["seed"])
    primary, scores = model.scores(vectors, p["method"])
    membership = memberships(primary, scores, ids, threshold, p["max_copies"])
    if np.any(membership.sum(axis=0) == 0):
        raise ValueError("An empty data shard was produced")
    model.save(root/"partition_model.npz")
    np.save(root/"ids.npy", ids)
    np.save(root/"membership.npy", membership)
    np.save(root/"primary.npy", primary.astype(np.uint32))
    write_json(root/"threshold.json", threshold.to_dict())
    mark("calibration_assignment_metadata")
    # Conservative union bound includes local protected edges and medoid bridges.
    upper_degree = shards*(g['degree']+2)+2*(shards-1)
    upper_stride = math.ceil((dataset.dimension*4+4+upper_degree*8)/c['storage']['page_bytes'])*c['storage']['page_bytes']
    # Ghost code banks contain at most the whole corpus per shard. Bound padded
    # quantization dimension by the next power of two, including factors/IDs.
    padded = 1 << (dataset.dimension-1).bit_length()
    disk_estimate = int(membership.sum())*upper_stride + shards*n*(math.ceil(padded*c['quantization']['bits']/8)+32)
    disk_estimate += c['routing']['max_samples']*(dataset.dimension*4+1024) + n*32
    if shutil.disk_usage(root).free < disk_estimate + reserve_disk_bytes:
        raise ValueError(f'Insufficient space for complete index plus reserve: {disk_estimate} + {reserve_disk_bytes} bytes')
    if progress is not None:
        progress({'phase':'capacity_checked','index_upper_estimate_bytes':disk_estimate,
                  'partition':summary(membership,primary)})
    adjacency = [set() for _ in range(n)]
    medoids = []
    protected_edges = 0
    checkpoint = Path(checkpoint_directory) if checkpoint_directory else None
    if checkpoint is not None: checkpoint.mkdir(parents=True,exist_ok=True)
    checkpoint_key = hashlib.sha256(json.dumps({'settings':index_settings(c),'dataset':dataset.identity(),
        'ids':hashlib.sha256(ids.tobytes()).hexdigest(),
        'membership':hashlib.sha256(membership.tobytes()).hexdigest(),
        'native':hashlib.sha256(Path(api.__file__).read_bytes()).hexdigest(),
        'graph_threads':graph_threads},sort_keys=True).encode()).hexdigest()
    def build_shard(sid):
        positions = np.flatnonzero(membership[:, sid])
        saved = checkpoint/f'shard_{sid}_{checkpoint_key}.npz' if checkpoint else None
        if saved is not None and saved.exists():
            with np.load(saved,allow_pickle=False) as archive:
                packed=archive['neighbors'];lengths=archive['lengths'];elapsed=float(archive['build_seconds'])
            if packed.shape != (len(positions),g['degree']) or np.any(lengths>g['degree']):
                raise ValueError('Invalid graph checkpoint')
            graph=[packed[i,:int(lengths[i])].tolist() for i in range(len(positions))]
            if any(j<0 or j>=len(positions) or j==i for i,edges in enumerate(graph) for j in edges):
                raise ValueError('Invalid checkpoint edge')
            if progress is not None: progress({'phase':'graph_checkpoint_loaded','shard':sid,'original_build_seconds':elapsed})
            return positions,graph
        local = np.ascontiguousarray(vectors[positions])
        def notify(alpha,done,total):
            if progress is not None: progress({'phase':'graph_progress','shard':sid,'alpha':alpha,'completed':done,'total':total})
        tick=time.perf_counter()
        graph = api.build_vamana(local, g['degree'], g['candidate_list_size'], g['prune_alphas'], c['seed']+sid, notify, graph_threads)
        elapsed=time.perf_counter()-tick
        if saved is not None:
            packed=np.zeros((len(positions),g['degree']),dtype=np.uint32)
            lengths=np.array([len(edges) for edges in graph],dtype=np.uint32)
            for i,edges in enumerate(graph):packed[i,:len(edges)]=edges
            temp=saved.with_suffix('.tmp')
            with temp.open('wb') as f:
                np.savez(f,neighbors=packed,lengths=lengths,build_seconds=elapsed)
                f.flush();os.fsync(f.fileno())
            temp.replace(saved)
        if progress is not None:progress({'phase':'graph_complete','shard':sid,'build_seconds':elapsed})
        return positions,graph
    with ThreadPoolExecutor(max_workers=min(shards,shard_workers)) as pool:
      for sid,(positions,graph) in enumerate(pool.map(build_shard,range(shards))):
        for i, neighbors in enumerate(graph):
            adjacency[int(positions[i])].update(int(positions[j]) for j in neighbors)
            # Protected bidirectional cycle guarantees local reachability, even at rho=0.
            if len(positions) > 1:
                for neighbor in (positions[(i-1)%len(positions)], positions[(i+1)%len(positions)]):
                    protected_edges += int(int(neighbor) not in adjacency[int(positions[i])])
                    adjacency[int(positions[i])].add(int(neighbor))
        # Compute medoid in bounded blocks instead of several full fp64 copies.
        best=(float('inf'),0)
        for start in range(0,len(positions),c['data']['block_rows']):
            selected=positions[start:start+c['data']['block_rows']]
            delta=vectors[selected].astype(float)-model.centers[sid]
            d=np.einsum('ij,ij->i',delta,delta);j=int(d.argmin())
            if float(d[j])<best[0]:best=(float(d[j]),int(selected[j]))
        medoids.append(best[1])
        mark(f"shard_{sid}_graph_and_union")
    # Centroid MST with bidirectional medoid links; shared medoid needs no self-edge.
    reached = {0}
    while len(reached) < shards:
        _, a, b = min((float(np.sum((model.centers[a]-model.centers[b])**2)), a, b)
                     for a in reached for b in range(shards) if b not in reached)
        u, v = medoids[a], medoids[b]
        if u != v:
            protected_edges += int(v not in adjacency[u]) + int(u not in adjacency[v])
            adjacency[u].add(v); adjacency[v].add(u)
        reached.add(b)
    _connected(adjacency)
    mark("bridges_connectivity")
    quantizer = api.Quantizer(model.centers.astype(np.float32), c["quantization"]["bits"], c["seed"], b"")
    rotation = bytes(quantizer.state())
    banks = []
    block_rows = c["data"]["block_rows"]
    for start in range(0, n, block_rows):
        banks.append(quantizer.encode(vectors[start:start+block_rows], primary[start:start+block_rows].astype(np.uint32)))
    codes, extra, factors = [np.concatenate([b[i] for b in banks]) for i in range(3)]
    del banks
    mark("rabitq_encode")
    epoch = str(uuid.uuid4())
    shard_info = []
    page = c["storage"]["page_bytes"]
    for sid in range(shards):
        directory = root/f"shard_{sid}"
        directory.mkdir()
        positions = np.flatnonzero(membership[:, sid])
        max_degree = max(len(adjacency[int(i)]) for i in positions)
        stride = math.ceil((dataset.dimension*4+4+max_degree*8)/page)*page
        needed = page+len(positions)*stride
        if shutil.disk_usage(directory).free < needed:
            raise ValueError(f"Insufficient disk space for shard {sid}: {needed} bytes required")
        with (directory/"nodes.bin").open("xb") as handle:
            header = HEADER.pack(MAGIC, 1, dataset.dimension, len(positions), stride)
            handle.write(header + bytes(page-len(header)))
            for i in positions:
                neighbors = ids[sorted(adjacency[int(i)])].astype("<u8")
                payload = vectors[i].astype("<f4").tobytes() + struct.pack("<I", len(neighbors)) + neighbors.tobytes()
                handle.write(payload + bytes(stride-len(payload)))
            handle.flush()
            os.fsync(handle.fileno())
        ghost = set(map(int, positions))
        for i in positions:
            ghost.update(adjacency[int(i)])
        bank = np.array(sorted(ghost), dtype=np.int64)
        np.save(directory/"ids.npy", ids[positions])
        np.save(directory/"code_ids.npy", ids[bank])
        np.save(directory/"code_membership.npy", membership[bank])
        np.save(directory/"codes.npy", codes[bank])
        np.save(directory/"extra.npy", extra[bank])
        np.save(directory/"factors.npy", factors[bank])
        np.save(directory/"center_ids.npy", primary[bank].astype(np.uint32))
        np.save(directory/"centers.npy", model.centers.astype(np.float32))
        (directory/"rotation.bin").write_bytes(rotation)
        info = {"version": 1, "epoch": epoch, "shard_id": sid, "dimension": dataset.dimension,
                "metric": c["data"]["metric"], "rows": len(positions), "record_bytes": stride,
                "header_bytes": page, "max_degree": max_degree, "bits": c["quantization"]["bits"],
                "rotation_seed": c["seed"], "ghost_code_rows": len(bank)-len(positions),
                "code_rows": len(bank), "code_memory_bytes": int(codes[bank].nbytes+extra[bank].nbytes+factors[bank].nbytes),
                "rotation_sha256": hashlib.sha256(rotation).hexdigest()}
        write_json(directory/"manifest.json", info)
        shard_info.append(info)
        mark(f"shard_{sid}_materialize_and_fsync")
    routing = c["routing"]
    sample_count = min(n, routing["max_samples"], max(routing["min_samples"], math.ceil(n*routing["sample_ratio"])))
    route_positions = np.unique(np.r_[sample_ids(n, sample_count, c["seed"]), np.array(medoids, dtype=np.uint64)]).astype(np.int64)
    router = api.Router(dataset.dimension, len(route_positions), routing["hnsw_m"], routing["ef_construction"], c["seed"])
    router.add(vectors[route_positions], ids[route_positions])
    router.save(str(root/"router.hnsw"))
    np.save(root/"route_ids.npy", ids[route_positions])
    # Small router code bank permits initial entry scoring without an extra data RPC.
    np.savez(root/"route_codes.npz", ids=ids[route_positions], codes=codes[route_positions],
             extra=extra[route_positions], factors=factors[route_positions],
             center_ids=primary[route_positions].astype(np.uint32), centers=model.centers.astype(np.float32))
    (root/"rotation.bin").write_bytes(rotation)
    dataset.assert_unchanged()
    mark("hnsw_router_and_publication_prepare")
    manifest = {"version": 1, "epoch": epoch, "kind": "dhdgs_pilot_vamana_rabitq",
                "dataset": dataset.identity(), "dimension": dataset.dimension, "rows": n,
                "metric": c["data"]["metric"], "bits": c["quantization"]["bits"],
                "rotation_seed": c["seed"], "shards": shard_info, "route_samples": len(route_positions),
                "protected_edges_added": protected_edges, "strong_connectivity_verified": True,
                "partition": summary(membership, primary), "build_seconds": time.perf_counter()-build_start,
                "build_profile": {"phases":phases,"process_before":process_before,
                                  "process_after":process_snapshot(), "network_shuffle_bytes":None,
                                  "network_shuffle_seconds":None,
                                  "scope":"central in-memory build; excludes final manifest write"},
                "estimated_build_memory_bytes": int(estimate), "config_sha256": c["_config_sha256"],
                "build_execution": {"shard_workers":shard_workers,"graph_threads":graph_threads,
                                    "parallel_graph_is_nondeterministic":graph_threads>1,
                                    "checkpoint_directory":str(checkpoint) if checkpoint else None,
                                    "disk_upper_estimate_bytes":disk_estimate,"reserve_disk_bytes":reserve_disk_bytes},
                "build_settings": index_settings(c),
                "limitations": ["In-memory pilot construction, not upstream DiskANN out-of-core builder",
                                "Buffered pread with optional application cache; physical SSD IO not established",
                                "Global location directory and per-shard ghost code banks are not billion-scale bounded"]}
    write_json(root/"manifest.json", manifest)
    return manifest


class Shard:
    def __init__(self, directory, cache_bytes):
        self.directory = Path(directory)
        self.meta = json.loads((self.directory/"manifest.json").read_text())
        m = self.meta
        if m["version"] != 1:
            raise ValueError("Unsupported shard format")
        self.ids = load_complete_npy(self.directory/"ids.npy")
        self.code_ids = load_complete_npy(self.directory/"code_ids.npy")
        for values in (self.ids, self.code_ids):
            if values.ndim != 1 or values.dtype != np.dtype("uint64") or np.any(values[1:] <= values[:-1]):
                raise ValueError("Invalid shard IDs")
        if len(self.ids) != m["rows"] or len(self.code_ids) != m["code_rows"]:
            raise ValueError("Shard ID count mismatch")
        self.location = {int(gid): i for i, gid in enumerate(self.ids)}
        self.code_location = {int(gid): i for i, gid in enumerate(self.code_ids)}
        self.codes, self.extra, self.factors, self.center_ids, centers = [
            np.array(load_complete_npy(self.directory/f"{name}.npy"))
            for name in ("codes", "extra", "factors", "center_ids", "centers")]
        rotation = (self.directory/"rotation.bin").read_bytes()
        if hashlib.sha256(rotation).hexdigest() != m["rotation_sha256"]:
            raise ValueError("Rotation checksum mismatch")
        if centers.shape[1] != m["dimension"] or any(len(x) != len(self.code_ids) for x in
            (self.codes, self.extra, self.factors, self.center_ids)):
            raise ValueError("Invalid quantization bank shape")
        self.quantizer = native().Quantizer(centers, m["bits"], m["rotation_seed"], rotation)
        path = self.directory/"nodes.bin"
        if path.stat().st_size != m["header_bytes"]+m["rows"]*m["record_bytes"]:
            raise ValueError("Incomplete node file")
        with path.open("rb") as handle:
            magic, version, dim, rows, stride = HEADER.unpack(handle.read(HEADER.size))
        if (magic, version, dim, rows, stride) != (MAGIC, 1, m["dimension"], m["rows"], m["record_bytes"]):
            raise ValueError("Node header mismatch")
        self.fd = os.open(path, os.O_RDONLY)
        self.cache_bytes = cache_bytes
        self.cache = OrderedDict()
        self.cache_size = 0
        self.lock = threading.Lock()
        self.load = {"requests": 0, "completed": 0, "failed": 0, "active": 0, "peak_active": 0}

    def close(self):
        if self.fd is not None:
            os.close(self.fd)
            self.fd = None

    def cached_record(self, gid):
        if gid not in self.location:
            raise ValueError(f"Vector {gid} is not local")
        with self.lock:
            raw=self.cache.get(gid)
            if raw is not None: self.cache.move_to_end(gid)
            return raw

    def cache_record(self, gid, raw):
        with self.lock:
            if gid not in self.cache and len(raw)<=self.cache_bytes:
                while self.cache and self.cache_size+len(raw)>self.cache_bytes:
                    _,old=self.cache.popitem(last=False);self.cache_size-=len(old)
                self.cache[gid]=raw;self.cache_size+=len(raw)

    def decode_record(self, raw):
        if len(raw)!=self.meta['record_bytes']: raise ValueError('Short node read')
        dim=self.meta['dimension']
        degree=struct.unpack_from('<I',raw,dim*4)[0]
        if degree>self.meta['max_degree'] or dim*4+4+degree*8>len(raw):
            raise ValueError('Invalid node degree')
        vector=np.frombuffer(raw,dtype='<f4',count=dim)
        if not np.isfinite(vector).all(): raise ValueError('Invalid disk vector')
        return vector,np.frombuffer(raw,dtype='<u8',count=degree,offset=dim*4+4)

    def read(self, gid, stats):
        if gid not in self.location:
            raise ValueError(f"Vector {gid} is not on shard {self.meta['shard_id']}")
        with self.lock:
            raw = self.cache.get(gid)
            if raw is not None:
                self.cache.move_to_end(gid)
                stats["cache_hits"] += 1
        if raw is None:
            stride = self.meta["record_bytes"]
            offset = self.meta["header_bytes"] + self.location[gid]*stride
            read_started = time.perf_counter_ns()
            raw = os.pread(self.fd, stride, offset)
            read_ended = time.perf_counter_ns()
            stats["pread_ms"] = stats.get("pread_ms", 0.)+(read_ended-read_started)/1e6
            if "_recorder" in stats:
                stats["_recorder"].spans.append(["pread", read_started, read_ended])
            if len(raw) != stride:
                raise ValueError("Short node read")
            stats["file_read_bytes"] += stride
            stats["pread_calls"] += 1
            with self.lock:
                if gid not in self.cache and stride <= self.cache_bytes:
                    while self.cache and self.cache_size+stride > self.cache_bytes:
                        _, old = self.cache.popitem(last=False)
                        self.cache_size -= len(old)
                    self.cache[gid] = raw
                    self.cache_size += len(raw)
        return self.decode_record(raw)

    def expand(self, query, ids, epoch, records=None, read_stats=None):
        with self.lock:
            self.load["requests"] += 1
            self.load["active"] += 1
            self.load["peak_active"] = max(self.load["active"], self.load["peak_active"])
        try:
            result = self._expand(query, ids, epoch, records, read_stats)
            with self.lock:
                self.load["completed"] += 1
                for key, value in result["stats"].items():
                    if isinstance(value, (int, float)):
                        self.load[key] = self.load.get(key, 0)+value
            return result
        except Exception:
            with self.lock:
                self.load["failed"] += 1
            raise
        finally:
            with self.lock:
                self.load["active"] -= 1

    def metrics(self):
        with self.lock:
            return {"shard_id": self.meta["shard_id"], "process": process_snapshot(),
                    "counters": dict(self.load), "cache_bytes": self.cache_size,
                    "stored_vectors": len(self.ids), "code_rows": len(self.code_ids),
                    "index_file_bytes": sum(p.stat().st_size for p in self.directory.iterdir() if p.is_file())}

    def _expand(self, query, ids, epoch, records=None, read_stats=None):
        recorder = Recorder()
        if epoch != self.meta["epoch"]:
            raise ValueError("Index epoch mismatch")
        q = np.asarray(query, dtype=np.float32)
        if q.shape != (self.meta["dimension"],) or not np.isfinite(q).all():
            raise ValueError("Invalid query")
        if self.meta["metric"] == "cosine" and not np.isclose(np.linalg.norm(q), 1, atol=1e-5):
            raise ValueError("Cosine query must be normalized")
        started = time.perf_counter()
        stats = {"file_read_bytes": 0, "pread_calls": 0, "cache_hits": 0, "pread_ms": 0.,
                 "scanned_edges": 0, "exact_distance_evaluations": 0, "exact_distance_ms": 0.,
                 "_recorder": recorder}
        if read_stats:
            stats.update(read_stats)
        expanded, neighbors, parents = [], set(), {}
        for gid in dict.fromkeys(ids):
            vector, outgoing = (self.read(int(gid), stats) if records is None
                                else self.decode_record(records[int(gid)]))
            exact_started = time.perf_counter_ns()
            delta = q.astype(float)-vector.astype(float)
            expanded.append([int(gid), float(np.dot(delta, delta))])
            exact_ended = time.perf_counter_ns()
            stats["exact_distance_ms"] += (exact_ended-exact_started)/1e6
            recorder.spans.append(["exact_distance",exact_started,exact_ended])
            stats["exact_distance_evaluations"] += 1
            stats["scanned_edges"] += len(outgoing)
            for neighbor in outgoing:
                parents.setdefault(int(neighbor), int(gid))
            neighbors.update(map(int, outgoing))
        targets = sorted(neighbors)
        try:
            positions = np.array([self.code_location[v] for v in targets], dtype=np.int64)
        except KeyError as exc:
            raise ValueError("Missing remote neighbor code") from exc
        quant_started = time.perf_counter_ns()
        estimates, uncertainty = self.quantizer.score(q, self.codes[positions], self.extra[positions],
                                                     self.factors[positions], self.center_ids[positions])
        quant_ended = time.perf_counter_ns()
        stats["rabitq_ms"] = (quant_ended-quant_started)/1e6
        recorder.spans.append(["rabitq",quant_started,quant_ended])
        stats["rabitq_evaluations"] = len(positions)
        if not np.isfinite(estimates).all() or not np.isfinite(uncertainty).all():
            raise ValueError("Non-finite quantized distances")
        del stats["_recorder"]
        trace = recorder.finish()
        stats["service_ms"] = (trace["end_ns"]-trace["start_ns"])/1e6
        stats["server_thread_cpu_ms"] = trace["thread_cpu_ms"]
        return {"expanded": expanded, "parents": [[gid, parents[gid]] for gid in targets],
                "service_trace": trace,
                "neighbors": [[gid, float(d), float(u)] for gid, d, u in zip(targets, estimates, uncertainty)],
                "stats": stats, "epoch": epoch, "shard_id": self.meta["shard_id"]}
