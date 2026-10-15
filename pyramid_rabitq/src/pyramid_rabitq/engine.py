from __future__ import annotations

import json
import threading
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import numpy.typing as npt

from .config import BuildConfig, HNSWConfig, IVFConfig
from .math_utils import prepare_vectors
from .rabitq import (
    FASTSCAN_AVAILABLE,
    FASTSCAN_BACKEND,
    NATIVE_SCAN_AVAILABLE,
    ApproximateResults,
    RaBitQIVFIndex,
)
from .router import HNSWRouter
from .storage import PartitionMetadata, VectorBlockCache


@dataclass(frozen=True)
class SearchHit:
    id: int
    distance: float
    partition_id: int


@dataclass(frozen=True)
class SearchResponse:
    hits: list[SearchHit]
    version: str
    searched_partitions: list[int]
    approximate_candidates: int


def _config_from_manifest(data: dict[str, Any]) -> BuildConfig:
    return BuildConfig(
        dimension=int(data["dimension"]),
        metric=str(data["metric"]),
        num_partitions=int(data["num_partitions"]),
        seed=int(data["seed"]),
        batch_size=int(data["batch_size"]),
        hnsw=HNSWConfig(**data["hnsw"]),
        ivf=IVFConfig(**data["ivf"]),
    )


class ReadNodeEngine:
    """Local read-node engine: route, RaBitQ scan, exact fetch and rerank."""

    def __init__(
        self,
        index_root: str | Path,
        cache_bytes: int = 8 * 1024**3,
        partition_workers: int = 4,
        mmap_indexes: bool = True,
    ) -> None:
        self.index_root = Path(index_root)
        self.cache = VectorBlockCache(cache_bytes)
        self._reload_lock = threading.RLock()
        self.partition_workers = max(1, int(partition_workers))
        self.mmap_indexes = bool(mmap_indexes)
        self.executor = ThreadPoolExecutor(
            max_workers=self.partition_workers,
            thread_name_prefix="rabitq-partition",
        )
        self._load_current()

    def _load_current(self) -> None:
        version = (self.index_root / "CURRENT").read_text(encoding="utf-8").strip()
        version_dir = self.index_root / "versions" / version
        manifest = json.loads((version_dir / "manifest.json").read_text(encoding="utf-8"))
        config = _config_from_manifest(manifest["config"])
        router = HNSWRouter.load(version_dir / "router")
        indexes: dict[int, RaBitQIVFIndex] = {}
        vector_paths: dict[int, Path] = {}
        scalar_metadata: dict[int, PartitionMetadata] = {}
        for partition in manifest["partitions"]:
            part = int(partition["partition_id"])
            part_dir = version_dir / "shards" / f"partition_{part:05d}"
            indexes[part] = RaBitQIVFIndex.load(
                part_dir / "rabitq", mmap=self.mmap_indexes
            )
            if len(indexes[part].ids) != int(partition["vectors"]):
                raise ValueError(f"partition {part} vector count does not match manifest")
            vector_paths[part] = part_dir / "vectors.h5"
            scalar_metadata[part] = PartitionMetadata(vector_paths[part])
        with self._reload_lock:
            self.version = version
            self.version_dir = version_dir
            self.manifest = manifest
            self.config = config
            self.router = router
            self.indexes = indexes
            self.vector_paths = vector_paths
            self.scalar_metadata = scalar_metadata

    def reload(self) -> str:
        self._load_current()
        return self.version

    def health(self) -> dict[str, Any]:
        return {
            "status": "ok",
            "version": self.version,
            "dimension": self.config.dimension,
            "partitions": len(self.indexes),
            "vectors": self.manifest["total_vectors"],
            "cache_bytes": self.cache.current_bytes,
            "native_scan": NATIVE_SCAN_AVAILABLE,
            "fastscan": FASTSCAN_AVAILABLE,
            "fastscan_backend": FASTSCAN_BACKEND,
            "code_layouts": sorted({index.code_layout for index in self.indexes.values()}),
            "mmap_indexes": self.mmap_indexes,
            "partition_workers": self.partition_workers,
            "graph_partition_backend": self.manifest.get("graph_partition_backend", "legacy"),
            "hnsw_sample_vectors": self.manifest.get("hnsw_sample_vectors"),
            "route_partitions": self.config.hnsw.route_partitions,
            "ivf_nprobe": self.config.ivf.nprobe,
            "candidate_multiplier": self.config.ivf.candidate_multiplier,
            "target_vectors_per_list": (
                None
                if self.config.ivf.nlist is not None
                else self.config.ivf.target_vectors_per_list
            ),
            "filtered_search_planner": "metadata-pruning+exact-small-set+adaptive-nprobe",
            "filtered_exact_scan_threshold": self.config.ivf.filtered_exact_scan_threshold,
            "ivf_nlists": {part: index.nlist for part, index in self.indexes.items()},
        }

    def close(self) -> None:
        self.executor.shutdown(wait=True, cancel_futures=True)

    def _search_partition(
        self, partition: int, query: np.ndarray, candidate_k: int,
        scalar_filters: list[dict[str, object]] | None,
        config: BuildConfig,
        indexes: dict[int, RaBitQIVFIndex],
        vector_paths: dict[int, Path],
        scalar_metadata: dict[int, PartitionMetadata],
    ):
        index = indexes[partition]
        allowed = None
        nprobe = config.ivf.nprobe
        if scalar_filters:
            allowed = scalar_metadata[partition].filter(scalar_filters)
            allowed_count = int(np.count_nonzero(allowed))
            if allowed_count == 0:
                empty_float = np.empty(0, dtype=np.float32)
                empty_int = np.empty(0, dtype=np.int64)
                return partition, ApproximateResults(empty_float, empty_int, empty_int), empty_float
            exact_threshold = config.ivf.filtered_exact_scan_threshold
            if exact_threshold and allowed_count <= exact_threshold:
                positions = np.flatnonzero(allowed[index.rows])
                approximate = ApproximateResults(
                    np.empty(len(positions), dtype=np.float32),
                    np.asarray(index.ids[positions], dtype=np.int64),
                    np.asarray(index.rows[positions], dtype=np.int64),
                )
            else:
                nprobe = index.plan_filtered_nprobe(
                    query, allowed, candidate_k, config.ivf.nprobe
                )
                approximate = index.search_approx(
                    query, candidate_k, nprobe, allowed_rows=allowed
                )
        else:
            approximate = index.search_approx(query, candidate_k, nprobe)
        vectors = self.cache.read_rows(vector_paths[partition], approximate.rows)
        if len(vectors) == 0:
            return partition, approximate, np.empty(0, dtype=np.float32)
        if config.metric == "cosine":
            exact = 1.0 - vectors @ query
        else:
            exact = np.sum((vectors - query) ** 2, axis=1)
        return partition, approximate, exact.astype(np.float32)

    def search(
        self,
        vector: npt.ArrayLike,
        top_k: int = 10,
        *,
        route_partitions: int | None = None,
        candidate_multiplier: int | None = None,
        scalar_filters: list[dict[str, object]] | None = None,
    ) -> SearchResponse:
        if top_k <= 0:
            raise ValueError("top_k must be positive")
        # Pin all mutable references at query start. Reload may publish a new
        # index version concurrently, but this request keeps using this view.
        with self._reload_lock:
            config = self.config
            version = self.version
            router = self.router
            indexes = self.indexes
            vector_paths = self.vector_paths
            scalar_metadata = self.scalar_metadata
        query = prepare_vectors(vector, config.metric)
        if query.shape != (1, config.dimension):
            raise ValueError("exactly one query vector is required")
        partition_count = route_partitions or config.hnsw.route_partitions
        if scalar_filters:
            # Vector-only routing is not safe for filtered nearest neighbors:
            # a routed-away partition can contain the closest qualifying row.
            partitions = [
                part for part in sorted(indexes)
                if scalar_metadata[part].may_match(scalar_filters)
            ]
        elif partition_count >= len(indexes):
            partitions = sorted(indexes)
        else:
            partitions = router.route_from_nodes(
                query,
                config.hnsw.route_nodes,
                partition_count,
            )
        multiplier = candidate_multiplier or config.ivf.candidate_multiplier
        candidate_k = max(top_k, top_k * multiplier)
        futures = [
            self.executor.submit(
                self._search_partition, part, query[0], candidate_k, scalar_filters,
                config, indexes, vector_paths, scalar_metadata,
            )
            for part in partitions
        ]
        results = [future.result() for future in futures]

        best: dict[int, SearchHit] = {}
        candidate_count = 0
        for part, approximate, exact in results:
            candidate_count += len(approximate.ids)
            for vector_id, distance in zip(approximate.ids, exact):
                hit = SearchHit(int(vector_id), float(distance), part)
                previous = best.get(hit.id)
                if previous is None or hit.distance < previous.distance:
                    best[hit.id] = hit
        hits = sorted(best.values(), key=lambda hit: (hit.distance, hit.id))[:top_k]
        return SearchResponse(hits, version, partitions, candidate_count)
