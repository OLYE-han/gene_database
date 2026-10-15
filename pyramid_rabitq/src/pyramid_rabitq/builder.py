from __future__ import annotations

import json
import math
import os
import time
from dataclasses import asdict
from pathlib import Path

import numpy as np

from .config import BuildConfig
from .math_utils import prepare_vectors
from .partition import BalancedGraphPartitioner, KaFFPaGraphPartitioner
from .rabitq import RaBitQIVFIndex
from .router import HNSWRouter
from .storage import H5Corpus, PartitionWriter


class PyramidBuilder:
    """Restartable-enough offline builder with immutable version output."""

    def __init__(self, source: H5Corpus, config: BuildConfig) -> None:
        self.source = source
        self.config = config

    @staticmethod
    def _progress(stage: str, started: float, **fields: object) -> None:
        details = " ".join(f"{key}={value}" for key, value in fields.items())
        print(
            f"[pyramid-build] stage={stage} elapsed_seconds={time.time() - started:.1f} {details}".rstrip(),
            flush=True,
        )

    def build(self, index_root: str | Path, version: str) -> Path:
        root = Path(index_root)
        version_dir = root / "versions" / version
        if version_dir.exists():
            raise FileExistsError(f"immutable index version already exists: {version_dir}")
        version_dir.mkdir(parents=True)
        started = time.time()
        self.source.save_manifest(version_dir / "source_manifest.json")

        sample_count = min(
            self.source.total_rows,
            max(
                self.config.hnsw.sample_size,
                math.ceil(self.source.total_rows * self.config.hnsw.sample_ratio),
            ),
        )
        self._progress(
            "hnsw-start", started, samples=sample_count,
            dimension=self.config.dimension, rows=self.source.total_rows,
        )
        samples = self.source.sample(sample_count, self.config.seed)
        self._progress("sample-loaded", started, vectors=len(samples))
        router = HNSWRouter.build(
            samples,
            self.config.metric,
            self.config.hnsw.m,
            self.config.hnsw.ef_construction,
            self.config.hnsw.ef_search,
        )
        self._progress("hnsw-complete", started, nodes=router.index.ntotal)
        graph = router.extract_level0_graph()
        self._progress("graph-extracted", started, nodes=len(graph))

        # A full streaming pass estimates how much corpus mass each sample node owns.
        node_weights = np.ones(router.index.ntotal, dtype=np.int64)
        weighted_rows = 0
        next_weight_report = 1_000_000
        for _, vectors in self.source.batches(self.config.batch_size):
            _, nodes = router.search_nodes(vectors, 1)
            np.add.at(node_weights, nodes[:, 0], 1)
            weighted_rows += len(vectors)
            if weighted_rows >= next_weight_report or weighted_rows == self.source.total_rows:
                self._progress("node-weighting", started, rows=weighted_rows)
                next_weight_report += 1_000_000
        if self.config.hnsw.partition_backend == "kaffpa":
            partitioner = KaFFPaGraphPartitioner(
                self.config.hnsw.kaffpa_executable,
                preconfiguration=self.config.hnsw.kaffpa_preconfiguration,
                seed=self.config.seed,
                require_kaffpa=self.config.hnsw.require_kaffpa,
            )
        else:
            partitioner = BalancedGraphPartitioner(
                prefer_metis=self.config.hnsw.partition_backend == "metis"
            )
        labels = partitioner.partition(graph, self.config.num_partitions, node_weights)
        partition_backend = getattr(
            partitioner, "backend_used", self.config.hnsw.partition_backend
        )
        router.set_partitions(labels)
        router.save(version_dir / "router")
        np.save(version_dir / "node_weights.npy", node_weights, allow_pickle=False)
        self._progress("graph-partitioned", started, backend=partition_backend)

        shard_root = version_dir / "shards"
        writers = [
            PartitionWriter(
                shard_root / f"partition_{part:05d}" / "vectors.h5",
                self.config.dimension,
                with_metadata=self.source.has_metadata,
            )
            for part in range(self.config.num_partitions)
        ]
        partition_vector_counts = np.zeros(self.config.num_partitions, dtype=np.int64)
        partitioned_rows = 0
        next_partition_report = 1_000_000
        try:
            for ids, vectors, metadata in self.source.batches_with_metadata(self.config.batch_size):
                prepared = prepare_vectors(vectors, self.config.metric)
                parts = router.assign(prepared)
                for part in np.unique(parts):
                    selected = np.flatnonzero(parts == part)
                    selected_metadata = None
                    if metadata is not None:
                        source_indptr = metadata["tags_indptr"]
                        lengths = np.diff(source_indptr)[selected]
                        tag_positions = np.concatenate([
                            np.arange(source_indptr[row], source_indptr[row + 1], dtype=np.int64)
                            for row in selected
                        ]) if len(selected) and int(lengths.sum()) else np.empty(0, dtype=np.int64)
                        selected_metadata = {
                            "equal": metadata["equal"][selected],
                            "range": metadata["range"][selected],
                            "tags_indptr": np.concatenate(([0], np.cumsum(lengths))).astype(np.int64),
                            "tags_indices": metadata["tags_indices"][tag_positions],
                        }
                    writers[int(part)].append(ids[selected], prepared[selected], selected_metadata)
                    partition_vector_counts[int(part)] += len(selected)
                partitioned_rows += len(vectors)
                if partitioned_rows >= next_partition_report or partitioned_rows == self.source.total_rows:
                    self._progress("shard-writing", started, rows=partitioned_rows)
                    next_partition_report += 1_000_000
        finally:
            for writer in writers:
                writer.close()

        partitions: list[dict[str, object]] = []
        for part in range(self.config.num_partitions):
            part_dir = shard_root / f"partition_{part:05d}"
            nlist = self.config.ivf.resolve_nlist(int(partition_vector_counts[part]))
            index = RaBitQIVFIndex.build_h5(
                part_dir / "vectors.h5",
                part_dir / "rabitq",
                nlist=nlist,
                metric=self.config.metric,
                seed=self.config.seed + part,
                train_size=self.config.ivf.train_size,
                batch_size=self.config.batch_size,
                scan_block_size=self.config.ivf.scan_block_size,
            )
            self._progress(
                "ivf-partition-complete", started, partition=part,
                vectors=len(index.ids), nlist=index.nlist,
            )
            partitions.append(
                {
                    "partition_id": part,
                    "vectors": len(index.ids),
                    "ivf_nlist": index.nlist,
                    "target_vectors_per_list": (
                        None
                        if self.config.ivf.nlist is not None
                        else self.config.ivf.target_vectors_per_list
                    ),
                }
            )

        manifest = {
            "format_version": 1,
            "version": version,
            "created_at": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
            "build_seconds": time.time() - started,
            "hnsw_sample_vectors": sample_count,
            "hnsw_sample_ratio_actual": sample_count / self.source.total_rows,
            "graph_partition_backend": partition_backend,
            "graph_partition_fallback_reason": getattr(partitioner, "fallback_reason", None),
            "graph_partitioner": getattr(partitioner, "provenance", None),
            "config": asdict(self.config),
            "total_vectors": self.source.total_rows,
            "scalar_metadata": self.source.has_metadata,
            "partitions": partitions,
        }
        (version_dir / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
        root.mkdir(parents=True, exist_ok=True)
        temporary = root / "CURRENT.tmp"
        temporary.write_text(version + "\n", encoding="utf-8")
        os.replace(temporary, root / "CURRENT")
        self._progress("complete", started, version=version)
        return version_dir
