from __future__ import annotations

import json
from pathlib import Path

import faiss
import numpy as np
import numpy.typing as npt

from .interfaces import RouterIndex
from .math_utils import as_float32_matrix, prepare_vectors


class HNSWRouter(RouterIndex):
    """Sample HNSW used both as a navigation graph and partition router."""

    def __init__(
        self,
        index: faiss.Index,
        metric: str,
        ef_search: int,
        node_partitions: npt.ArrayLike | None = None,
    ) -> None:
        self.index = index
        self.metric = metric
        self.dimension = int(index.d)
        self.ef_search = int(ef_search)
        self.index.hnsw.efSearch = self.ef_search
        self.node_partitions = (
            None if node_partitions is None else np.asarray(node_partitions, dtype=np.int32)
        )
        if self.node_partitions is not None and len(self.node_partitions) != index.ntotal:
            raise ValueError("one partition label is required per HNSW node")

    @classmethod
    def build(
        cls,
        samples: npt.ArrayLike,
        metric: str = "cosine",
        m: int = 32,
        ef_construction: int = 200,
        ef_search: int = 128,
    ) -> "HNSWRouter":
        vectors = prepare_vectors(samples, metric)
        metric_type = faiss.METRIC_INNER_PRODUCT if metric == "cosine" else faiss.METRIC_L2
        index = faiss.IndexHNSWFlat(vectors.shape[1], m, metric_type)
        index.hnsw.efConstruction = ef_construction
        index.hnsw.efSearch = ef_search
        # Faiss otherwise makes multi-hour production builds completely silent.
        # Its verbose mode only reports progress; it does not alter graph construction.
        index.verbose = len(vectors) >= 100_000
        index.add(vectors)
        return cls(index, metric, ef_search)

    def set_partitions(self, labels: npt.ArrayLike) -> None:
        result = np.asarray(labels, dtype=np.int32)
        if result.shape != (self.index.ntotal,) or np.any(result < 0):
            raise ValueError("invalid HNSW partition labels")
        self.node_partitions = result

    def search_nodes(self, vectors, k: int):
        queries = prepare_vectors(vectors, self.metric)
        distances, indices = self.index.search(queries, min(k, self.index.ntotal))
        return distances, indices

    def assign(self, vectors: npt.ArrayLike) -> np.ndarray:
        if self.node_partitions is None:
            raise RuntimeError("router graph has not been partitioned")
        _, nodes = self.search_nodes(vectors, 1)
        return self.node_partitions[nodes[:, 0]]

    def route(self, vector: npt.ArrayLike, partitions: int) -> list[int]:
        if self.node_partitions is None:
            raise RuntimeError("router graph has not been partitioned")
        distances, nodes = self.search_nodes(as_float32_matrix(vector, self.dimension), self.index.ntotal)
        scores: dict[int, float] = {}
        for distance, node in zip(distances[0], nodes[0]):
            if node < 0:
                continue
            partition = int(self.node_partitions[node])
            rank_score = -float(distance) if self.metric == "cosine" else float(distance)
            scores[partition] = min(scores.get(partition, float("inf")), rank_score)
        return [key for key, _ in sorted(scores.items(), key=lambda item: item[1])[:partitions]]

    def route_from_nodes(self, vector: npt.ArrayLike, route_nodes: int, partitions: int) -> list[int]:
        if self.node_partitions is None:
            raise RuntimeError("router graph has not been partitioned")
        target = min(int(partitions), len(np.unique(self.node_partitions)))
        node_count = min(max(int(route_nodes), target), self.index.ntotal)
        scores: dict[int, float] = {}
        votes: dict[int, int] = {}
        while True:
            distances, nodes = self.search_nodes(vector, node_count)
            scores.clear()
            votes.clear()
            for distance, node in zip(distances[0], nodes[0]):
                if node < 0:
                    continue
                part = int(self.node_partitions[node])
                rank_score = -float(distance) if self.metric == "cosine" else float(distance)
                scores[part] = min(scores.get(part, float("inf")), rank_score)
                votes[part] = votes.get(part, 0) + 1
            if len(scores) >= target or node_count == self.index.ntotal:
                break
            # A graph partition may dominate the nearest navigation nodes.
            # Expand only when necessary so the requested partition fan-out is
            # honored without paying for a full-router scan on normal queries.
            node_count = min(self.index.ntotal, node_count * 2)
        ordered = sorted(scores, key=lambda part: (scores[part], -votes[part], part))
        return ordered[:target]

    def extract_level0_graph(self) -> list[list[int]]:
        """Extract the actual level-0 adjacency stored inside Faiss HNSW."""
        hnsw = self.index.hnsw
        offsets = faiss.vector_to_array(hnsw.offsets)
        neighbors = faiss.vector_to_array(hnsw.neighbors)
        level0_width = int(hnsw.nb_neighbors(0))
        graph: list[list[int]] = []
        for node in range(self.index.ntotal):
            start = int(offsets[node])
            row = neighbors[start : start + level0_width]
            graph.append(sorted({int(value) for value in row if value >= 0 and value != node}))
        # Graph partitioners expect undirected adjacency.
        undirected = [set(row) for row in graph]
        for source, row in enumerate(graph):
            for target in row:
                undirected[target].add(source)
        return [sorted(row) for row in undirected]

    def save(self, path: Path) -> None:
        path.mkdir(parents=True, exist_ok=True)
        faiss.write_index(self.index, str(path / "hnsw.faiss"))
        if self.node_partitions is not None:
            np.save(path / "node_partitions.npy", self.node_partitions, allow_pickle=False)
        metadata = {
            "format_version": 1,
            "metric": self.metric,
            "dimension": self.dimension,
            "ef_search": self.ef_search,
            "nodes": int(self.index.ntotal),
        }
        (path / "metadata.json").write_text(json.dumps(metadata, indent=2), encoding="utf-8")

    @classmethod
    def load(cls, path: Path) -> "HNSWRouter":
        metadata = json.loads((path / "metadata.json").read_text(encoding="utf-8"))
        index = faiss.read_index(str(path / "hnsw.faiss"))
        labels_path = path / "node_partitions.npy"
        labels = np.load(labels_path, allow_pickle=False) if labels_path.exists() else None
        return cls(index, metadata["metric"], metadata["ef_search"], labels)
