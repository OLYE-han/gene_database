from __future__ import annotations

import math
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

import yaml


@dataclass(frozen=True)
class HNSWConfig:
    sample_size: int = 100_000
    sample_ratio: float = 0.20
    m: int = 32
    ef_construction: int = 200
    ef_search: int = 128
    route_nodes: int = 64
    route_partitions: int = 2
    partition_backend: str = "kaffpa"
    kaffpa_executable: str = "kaffpa"
    kaffpa_preconfiguration: str = "strong"
    require_kaffpa: bool = True

    def __post_init__(self) -> None:
        if self.sample_size <= 0 or not 0 < self.sample_ratio <= 1:
            raise ValueError("HNSW sample_size must be positive and sample_ratio must be in (0, 1]")
        if self.route_nodes <= 0 or self.route_partitions <= 0:
            raise ValueError("HNSW route_nodes and route_partitions must be positive")
        if self.partition_backend not in {"kaffpa", "metis", "fallback"}:
            raise ValueError("partition_backend must be kaffpa, metis, or fallback")


@dataclass(frozen=True)
class IVFConfig:
    nlist: int | None = None
    target_vectors_per_list: int | None = 384
    nprobe: int = 2
    train_size: int = 100_000
    candidate_multiplier: int = 8
    scan_block_size: int = 4096
    filtered_exact_scan_threshold: int = 4096

    def __post_init__(self) -> None:
        if self.nlist is not None and self.nlist <= 0:
            raise ValueError("IVF nlist must be positive when explicitly configured")
        if self.target_vectors_per_list is not None and not 256 <= self.target_vectors_per_list <= 512:
            raise ValueError("target_vectors_per_list must be between 256 and 512")
        if self.nlist is None and self.target_vectors_per_list is None:
            raise ValueError("one of nlist or target_vectors_per_list is required")
        if self.nprobe <= 0 or self.candidate_multiplier <= 0:
            raise ValueError("IVF nprobe and candidate_multiplier must be positive")
        if self.filtered_exact_scan_threshold < 0:
            raise ValueError("filtered_exact_scan_threshold must be non-negative")

    def resolve_nlist(self, vector_count: int) -> int:
        if vector_count <= 0:
            raise ValueError("cannot resolve nlist for an empty partition")
        if self.nlist is not None:
            return min(self.nlist, vector_count)
        assert self.target_vectors_per_list is not None
        preferred = max(1, int(round(vector_count / self.target_vectors_per_list)))
        # Keep mean occupancy in [256, 512] whenever the partition is large
        # enough. Individual KMeans lists can still be imbalanced.
        minimum_lists = max(1, math.ceil(vector_count / 512))
        maximum_lists = max(1, vector_count // 256)
        return min(max(preferred, minimum_lists), maximum_lists)


@dataclass(frozen=True)
class BuildConfig:
    dimension: int = 2560
    metric: str = "cosine"
    num_partitions: int = 16
    seed: int = 42
    batch_size: int = 4096
    hnsw: HNSWConfig = field(default_factory=HNSWConfig)
    ivf: IVFConfig = field(default_factory=IVFConfig)

    def __post_init__(self) -> None:
        if self.dimension <= 0 or self.num_partitions <= 0:
            raise ValueError("dimension and num_partitions must be positive")
        if self.metric not in {"cosine", "l2"}:
            raise ValueError("metric must be cosine or l2")


@dataclass(frozen=True)
class ServiceConfig:
    index_root: str
    cache_bytes: int = 8 * 1024**3
    host: str = "0.0.0.0"
    port: int = 8080
    workers: int = 1
    top_k_limit: int = 1000
    partition_workers: int = 4
    max_inflight_queries: int = 4
    mmap_indexes: bool = True
    require_native_scan: bool = False
    require_fastscan: bool = False


def _construct(cls: type, data: dict[str, Any]):
    fields = getattr(cls, "__dataclass_fields__")
    kwargs: dict[str, Any] = {}
    for name, value in data.items():
        if name not in fields:
            raise ValueError(f"Unknown {cls.__name__} option: {name}")
        field_type = fields[name].type
        nested = {"hnsw": HNSWConfig, "ivf": IVFConfig}.get(name)
        kwargs[name] = _construct(nested, value) if nested and isinstance(value, dict) else value
    return cls(**kwargs)


def load_build_config(path: str | Path) -> BuildConfig:
    with Path(path).open("r", encoding="utf-8") as handle:
        return _construct(BuildConfig, yaml.safe_load(handle) or {})


def load_service_config(path: str | Path) -> ServiceConfig:
    with Path(path).open("r", encoding="utf-8") as handle:
        return _construct(ServiceConfig, yaml.safe_load(handle) or {})


def dump_config(config: BuildConfig | ServiceConfig, path: str | Path) -> None:
    with Path(path).open("w", encoding="utf-8") as handle:
        yaml.safe_dump(asdict(config), handle, sort_keys=False)
