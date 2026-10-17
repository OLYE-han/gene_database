"""One explicit YAML configuration; reject missing/unknown/duplicate keys."""
from __future__ import annotations

import copy
import hashlib
import math
from pathlib import Path

import yaml


class StrictLoader(yaml.SafeLoader):
    pass


def _mapping(loader, node, deep=False):
    output = {}
    for key, value in node.value:
        name = loader.construct_object(key, deep=deep)
        if name in output:
            raise ValueError(f"Duplicate YAML key: {name}")
        output[name] = loader.construct_object(value, deep=deep)
    return output


StrictLoader.add_constructor(yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG, _mapping)

# Types are the schema, not a second set of parameter defaults.
SCHEMA = {
    "version": int, "seed": int,
    "runtime": {"blas_threads": int},
    "metrics": {"detailed_trace": bool, "resource_sample_interval_ms": int, "resource_max_samples": int},
    "paths": {"dataset": str, "output": str},
    "data": {"metric": str, "dimension": int, "row_limit": int, "block_rows": int, "inspection_rows": int},
    "partition": {"method": str, "overlap_fraction": float, "max_copies": int, "train_rows": int,
                  "calibration_rows": int, "kmeans_iterations": int, "kmeans_tolerance": float},
    "build": {"memory_limit_bytes": int, "degree": int, "candidate_list_size": int, "prune_alphas": list},
    "quantization": {"bits": int},
    "storage": {"page_bytes": int, "cache_bytes_per_shard": int,
                "io": {"backend": str, "fallback_backend": str, "worker_threads": int,
                       "max_submit_requests": int, "max_inflight_per_query": int,
                       "max_inflight_per_device": int, "max_inflight_bytes_per_device": int,
                       "buffer_pool_bytes_per_device": int, "max_pending_batches": int,
                       "max_completion_events_per_poll": int, "completion_poll_ms": int}},
    "routing": {"sample_ratio": float, "min_samples": int, "max_samples": int, "hnsw_m": int,
                "ef_construction": int, "ef_search": int, "entry_candidates": int},
    "search": {"execution": str, "top_k": int, "frontier_size": int, "max_expansions": int, "local_beam": int,
               "max_inflight": int, "exclude_self": bool, "fallback_enabled": bool,
               "max_fallback_entries": int, "stagnation_batches": int},
    "peer": {"execution_model": str,"max_branches": int,"child_budget_fraction": float,"worker_threads": int,
             "max_active_queries": int,"heartbeat_interval_ms": int,"lease_timeout_ms": int,
             "query_timeout_seconds": float,"control_timeout_seconds": float,"handoff_timeout_seconds": float,
             "retention_seconds": float,"max_tombstones": int,"head_bind_host": str,
             "head_advertise_host": str,"head_port": int},
    "scheduler": {"mode": str, "kappa": float, "exploration": float, "max_wait_ms": float,
                  "local_batch_ms": float, "remote_rtt_ms": float, "bandwidth_bytes_per_ms": float,
                  "ewma_weight": float,"remote_selection": str,"remote_weight": float,
                  "probability_temperature": float},
    "network": {"transport": str, "batch_size": int, "timeout_seconds": float,
                "max_message_bytes": int, "max_retries": int},
    "cluster": {"nodes": list},
    "query": {"vectors": str, "ids": (str, type(None)), "row": int},
    "assignment": {"model": str, "threshold": str},
    "experiment": {"metrics": list, "overlap_fractions": list, "seeds": list, "train_rows": int,
                   "calibration_rows": int, "evaluation_rows": int, "benchmark_queries": int,
                   "exact_block_rows": int, "target_recall": float,
                   "benchmark_concurrency": int, "benchmark_repetitions": int, "warmup_queries": int},
}


def _check(values, schema, path=""):
    if not isinstance(values, dict) or set(values) != set(schema):
        missing = set(schema) - set(values) if isinstance(values, dict) else set(schema)
        extra = set(values) - set(schema) if isinstance(values, dict) else set()
        raise ValueError(f"Configuration keys at {path or 'root'}: missing={sorted(missing)}, unknown={sorted(extra)}")
    for key, expected in schema.items():
        value = values[key]
        label = f"{path}.{key}".lstrip(".")
        if isinstance(expected, dict):
            _check(value, expected, label)
        elif expected is float:
            if type(value) not in (int, float) or not math.isfinite(value):
                raise ValueError(f"{label} must be a finite number")
        elif isinstance(expected, tuple):
            if type(value) not in expected:
                raise ValueError(f"Invalid type for {label}")
        elif type(value) is not expected:
            raise ValueError(f"{label} must be {expected.__name__}")


def validate(c):
    _check(c, SCHEMA)
    if c["version"] != 1 or not 0 <= c["seed"] < 2**32:
        raise ValueError("Unsupported config version or seed")
    for section, key, choices in [
        ("data", "metric", ("l2", "cosine")),
        ("partition", "method", ("margin", "boundary", "relative_gap")),
        ("scheduler", "mode", ("adaptive", "reference", "local_first")),
        ("network", "transport", ("local", "tcp")),
        ("search", "execution", ("peer", "reference")),
        ("peer", "execution_model", ("blocking", "resumable")),
        ("scheduler", "remote_selection", ("probabilistic", "threshold")),
    ]:
        if c[section][key] not in choices:
            raise ValueError(f"Invalid {section}.{key}")
    zero_allowed = {("data", "row_limit"), ("storage", "cache_bytes_per_shard"),
                    ("search", "max_fallback_entries"), ("network", "max_retries"), ("query", "row"),
                    ("experiment", "warmup_queries")}
    zero_allowed.add(("peer", "head_port"))
    zero_allowed.update({("scheduler", "remote_weight"), ("scheduler", "kappa"),
                         ("scheduler", "remote_rtt_ms")})
    for section, fields in c.items():
        if not isinstance(fields, dict):
            continue
        for key, value in fields.items():
            if type(value) is int and value < (0 if (section, key) in zero_allowed else 1):
                raise ValueError(f"Invalid nonpositive {section}.{key}")
    p, r, s, net, sched = (c[k] for k in ("partition", "routing", "search", "network", "scheduler"))
    nodes = c["cluster"]["nodes"]
    if not nodes:
        raise ValueError("At least one data node required")
    for i, node in enumerate(nodes):
        _check(node, {"id": int, "host": str, "port": int, "index_directory": (str, type(None))}, "cluster.nodes")
        if node["id"] != i or not node["host"] or not 1 <= node["port"] <= 65535:
            raise ValueError("Node IDs must be consecutive from zero; host and port required")
    if len({(n["host"], n["port"]) for n in nodes}) != len(nodes):
        raise ValueError("Duplicate node endpoint")
    if not 0 <= p["overlap_fraction"] <= 1 or p["kmeans_tolerance"] < 0:
        raise ValueError("Invalid overlap fraction or k-means tolerance")
    if not 1 <= p["max_copies"] <= len(nodes):
        raise ValueError("max_copies must not exceed number of data nodes")
    if not 64 <= c["data"]["dimension"] <= 65536 or not 1 <= c["quantization"]["bits"] <= 9:
        raise ValueError("Unsupported RaBitQ dimension or bits")
    alphas = c["build"]["prune_alphas"]
    if (not alphas or any(type(a) not in (int, float) or not math.isfinite(a) or a < 1 for a in alphas)
            or c["build"]["candidate_list_size"] < c["build"]["degree"]):
        raise ValueError("Invalid Vamana pruning parameters")
    if not 0 < r["sample_ratio"] <= 1 or r["min_samples"] > r["max_samples"] or r["hnsw_m"] < 2:
        raise ValueError("Invalid routing sample settings")
    if r["ef_construction"] < r["hnsw_m"] or r["ef_search"] < r["entry_candidates"]:
        raise ValueError("HNSW ef must cover M / entry candidates")
    if s["frontier_size"] < s["top_k"] or s["max_expansions"] < s["top_k"]:
        raise ValueError("Search frontier and budget must cover top_k")
    if sched["mode"] == "reference" and s["max_inflight"] != 1:
        raise ValueError("Reference order requires max_inflight=1")
    if (sched["remote_weight"] < 0 or sched["probability_temperature"]<=0
        or sched["kappa"] < 0 or any(sched[k] <= 0 for k in
        ("exploration", "max_wait_ms", "local_batch_ms", "bandwidth_bytes_per_ms"))
        or sched["remote_rtt_ms"] < 0 or not 0 < sched["ewma_weight"] <= 1):
        raise ValueError("Invalid scheduler costs/weights")
    if net["timeout_seconds"] <= 0 or net["max_message_bytes"] < c["data"]["dimension"]*4+1024:
        raise ValueError("Network timeout/message bound too small")
    page = c["storage"]["page_bytes"]
    if page < 512 or page & (page-1):
        raise ValueError("page_bytes must be a power of two >= 512")
    io = c['storage']['io']
    if io['backend'] not in ('io_uring','thread_pool') or io['fallback_backend'] not in ('error','thread_pool'):
        raise ValueError('Invalid storage.io backend')
    if any(type(v) is int and v <= 0 for v in io.values()):
        raise ValueError('storage.io capacities must be positive')
    if c['peer']['execution_model']=='resumable' and (s['execution']!='peer' or net['transport']!='tcp'):
        raise ValueError('resumable execution requires peer search over TCP data services')
    exp = c["experiment"]
    peer = c["peer"]
    if (peer["lease_timeout_ms"] <= 3*peer["heartbeat_interval_ms"]
            or not 0 < peer["child_budget_fraction"] < 1
            or any(peer[k] <= 0 for k in ("query_timeout_seconds","control_timeout_seconds",
                                         "handoff_timeout_seconds","retention_seconds"))
            or not peer["head_bind_host"] or not peer["head_advertise_host"]
            or peer["head_advertise_host"] in ("0.0.0.0", "::")
            or not 0 <= peer["head_port"] <= 65535):
        raise ValueError("Invalid peer lifecycle settings")
    if peer["head_port"] and exp["benchmark_concurrency"] > 1:
        raise ValueError("Use head_port=0 for multiple benchmark Engine instances")
    if (not exp["metrics"] or any(m not in ("l2", "cosine") for m in exp["metrics"])
            or len(set(exp["metrics"])) != len(exp["metrics"])):
        raise ValueError("Invalid experiment metrics")
    for key, low, high, expected in (("overlap_fractions", 0, 1, (int,float)), ("seeds", 0, 2**32-1, (int,))):
        if not exp[key] or any(type(v) not in expected or not low <= v <= high for v in exp[key]) or len(set(exp[key])) != len(exp[key]):
            raise ValueError(f"Invalid experiment.{key}")
    if not 0 < exp["target_recall"] <= 1:
        raise ValueError("target_recall must be in (0,1]")


def load_config(path):
    path = Path(path).resolve()
    raw = yaml.load(path.read_text(), Loader=StrictLoader)
    migration=[]
    # Old v1 archives predate both async fields. Keep their blocking semantics;
    # hydrate inactive I/O settings from the single canonical YAML, and archive
    # the resulting values. Partly specified new configs still fail strictly.
    if (isinstance(raw,dict) and raw.get('version')==1 and isinstance(raw.get('peer'),dict)
            and isinstance(raw.get('storage'),dict) and 'execution_model' not in raw['peer']
            and 'io' not in raw['storage']):
        defaults=yaml.load((Path(__file__).resolve().parents[2]/'configs/dhdgs.yaml').read_text(),Loader=StrictLoader)
        raw['peer']['execution_model']='blocking'
        raw['storage']['io']=copy.deepcopy(defaults['storage']['io'])
        migration=['peer.execution_model=blocking','storage.io (inactive canonical defaults)']
    validate(raw)
    c = copy.deepcopy(raw)
    for section, key in (("paths","dataset"), ("paths","output"), ("query","vectors"), ("query","ids"),
                         ("assignment", "model"), ("assignment", "threshold")):
        if c[section][key] is not None:
            c[section][key] = str((path.parent / c[section][key]).resolve())
    for node in c["cluster"]["nodes"]:
        if node["index_directory"] is not None:
            node["index_directory"] = str((path.parent / node["index_directory"]).resolve())
    output, source = Path(c["paths"]["output"]), Path(c["paths"]["dataset"])
    if output == source or output.is_relative_to(source):
        raise ValueError("Output must be outside source dataset")
    c["_config_migration"] = migration
    c["_config_path"] = str(path)
    c["_config_sha256"] = hashlib.sha256(path.read_bytes()).hexdigest()
    return c


def snapshot(c, directory):
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / "resolved.yaml"
    with path.open("x") as handle:
        yaml.safe_dump({k: v for k, v in c.items() if not k.startswith("_")}, handle,
                       allow_unicode=True, sort_keys=False)
    from .data import write_json
    write_json(directory / "config_provenance.json", {
        "source": c["_config_path"], "source_sha256": c["_config_sha256"],
        "migration":c.get("_config_migration",[]),
        "resolved_sha256": hashlib.sha256(path.read_bytes()).hexdigest()})


def index_settings(c):
    """Settings whose modification requires a new immutable index."""
    return {"seed": c["seed"], "data": {k: c["data"][k] for k in ("metric", "dimension", "row_limit")},
            "partition": c["partition"],
            "build": {k: c["build"][k] for k in ("degree", "candidate_list_size", "prune_alphas")},
            "quantization": c["quantization"], "page_bytes": c["storage"]["page_bytes"],
            "routing": {k: c["routing"][k] for k in
                        ("sample_ratio", "min_samples", "max_samples", "hnsw_m", "ef_construction")},
            "shards": len(c["cluster"]["nodes"])}
