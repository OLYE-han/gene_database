from __future__ import annotations

import hashlib
import json
import os
import shutil
import subprocess
import tempfile
from pathlib import Path

import numpy as np
import numpy.typing as npt

from .interfaces import GraphPartitioner


def _validate_graph(adjacency: list[list[int]], weights: npt.ArrayLike | None) -> np.ndarray:
    size = len(adjacency)
    for node, row in enumerate(adjacency):
        if any(neighbor < 0 or neighbor >= size or neighbor == node for neighbor in row):
            raise ValueError("adjacency contains an invalid node")
    result = np.ones(size, dtype=np.int64) if weights is None else np.asarray(weights, dtype=np.int64)
    if result.shape != (size,) or np.any(result <= 0):
        raise ValueError("node weights must be positive and match the graph")
    return result


class BalancedGraphPartitioner(GraphPartitioner):
    """PyMetis adapter with a deterministic graph-aware fallback.

    The fallback keeps installation rootless and offline-friendly. Production
    builds should install the optional ``metis`` extra for higher-quality cuts.
    """

    def __init__(self, imbalance: float = 1.10, prefer_metis: bool = True) -> None:
        if imbalance < 1:
            raise ValueError("imbalance must be at least 1")
        self.imbalance = imbalance
        self.prefer_metis = prefer_metis

    def partition(self, adjacency, num_parts, node_weights=None):
        weights = _validate_graph(adjacency, node_weights)
        if num_parts <= 0 or num_parts > len(adjacency):
            raise ValueError("num_parts must be in [1, number of nodes]")
        if self.prefer_metis:
            try:
                import pymetis

                _, labels = pymetis.part_graph(
                    num_parts,
                    adjacency=adjacency,
                    vweights=weights.astype(int).tolist(),
                )
                return np.asarray(labels, dtype=np.int32)
            except ImportError:
                pass
        return self._fallback(adjacency, num_parts, weights)

    def _fallback(self, adjacency, num_parts, weights):
        total = int(weights.sum())
        capacity = max(int(np.ceil(total / num_parts * self.imbalance)), int(weights.max()))
        labels = np.full(len(adjacency), -1, dtype=np.int32)
        loads = np.zeros(num_parts, dtype=np.int64)
        order = sorted(range(len(adjacency)), key=lambda n: (-len(adjacency[n]), -weights[n], n))

        # Seed parts with high-degree nodes, then use neighbor affinity.
        for part, node in enumerate(order[:num_parts]):
            labels[node] = part
            loads[part] += weights[node]
        for node in order[num_parts:]:
            affinity = np.zeros(num_parts, dtype=np.int64)
            for neighbor in adjacency[node]:
                label = labels[neighbor]
                if label >= 0:
                    affinity[label] += 1
            feasible = [p for p in range(num_parts) if loads[p] + weights[node] <= capacity]
            choices = feasible or list(range(num_parts))
            part = min(choices, key=lambda p: (-affinity[p], loads[p], p))
            labels[node] = part
            loads[part] += weights[node]
        # Affinity-first placement may legally leave one part well below the
        # target. Move low-cut-cost nodes until no single move improves the
        # load gap. This also makes the fallback predictable for small pilots.
        average = total / num_parts
        while True:
            heavy = int(np.argmax(loads))
            light = int(np.argmin(loads))
            old_gap = int(loads[heavy] - loads[light])
            candidates = np.flatnonzero(labels == heavy)
            movable: list[tuple[int, int]] = []
            for node in candidates:
                new_heavy = loads[heavy] - weights[node]
                new_light = loads[light] + weights[node]
                new_gap = int(max(new_heavy, new_light) - min(new_heavy, new_light))
                if new_gap >= old_gap or new_light > max(capacity, np.ceil(average)):
                    continue
                same = sum(labels[n] == heavy for n in adjacency[node])
                gained = sum(labels[n] == light for n in adjacency[node])
                movable.append((same - gained, int(node)))
            if not movable:
                break
            _, node = min(movable)
            labels[node] = light
            loads[heavy] -= weights[node]
            loads[light] += weights[node]
        return labels


class KaFFPaGraphPartitioner(GraphPartitioner):
    """Weighted KaFFPa adapter using KaHIP's rootless command-line binary.

    KaFFPa consumes the standard METIS graph format. Keeping the integration
    at the executable boundary makes it possible to deploy a user-built KaHIP
    binary without installing a system package or requiring root access.
    """

    def __init__(
        self,
        executable: str = "kaffpa",
        *,
        imbalance: float = 1.03,
        preconfiguration: str = "strong",
        seed: int = 42,
        require_kaffpa: bool = False,
        fallback: GraphPartitioner | None = None,
    ) -> None:
        if imbalance < 1:
            raise ValueError("imbalance must be at least 1")
        self.executable = os.environ.get("PYRAMID_KAFFPA_EXECUTABLE", executable)
        self.imbalance = float(imbalance)
        self.preconfiguration = preconfiguration
        self.seed = int(seed)
        self.require_kaffpa = bool(require_kaffpa)
        self.fallback = fallback or BalancedGraphPartitioner(prefer_metis=True)
        self.backend_used: str | None = None
        self.fallback_reason: str | None = None
        self.provenance: dict[str, str] | None = None

    @staticmethod
    def _binary_provenance(executable: str) -> dict[str, str]:
        path = Path(executable).resolve()
        digest = hashlib.sha256()
        with path.open("rb") as handle:
            for block in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(block)
        result = {
            "executable": str(path),
            "binary_sha256": digest.hexdigest(),
        }
        metadata_path = path.parent.parent / "share/pyramid-rabitq/kaffpa-provenance.json"
        if metadata_path.exists():
            metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
            result.update({str(key): str(value) for key, value in metadata.items()})
        try:
            version = subprocess.run(
                [str(path), "--version"],
                check=True,
                text=True,
                capture_output=True,
            ).stdout.strip()
            if version:
                result["reported_version"] = version
        except (OSError, subprocess.CalledProcessError):
            pass
        return result

    @staticmethod
    def _write_metis_graph(path: Path, adjacency: list[list[int]], weights: np.ndarray) -> None:
        # KaFFPa expects undirected edges once in the header and 1-based node
        # identifiers in each adjacency row. Format 10 means vertex weights.
        edge_count = sum(len(set(row)) for row in adjacency) // 2
        with path.open("w", encoding="utf-8") as handle:
            handle.write(f"{len(adjacency)} {edge_count} 10\n")
            for node, row in enumerate(adjacency):
                neighbors = sorted({int(value) + 1 for value in row if value != node})
                suffix = "" if not neighbors else " " + " ".join(map(str, neighbors))
                handle.write(f"{int(weights[node])}{suffix}\n")

    @staticmethod
    def _validate_labels(labels: np.ndarray, size: int, num_parts: int) -> np.ndarray:
        result = np.asarray(labels, dtype=np.int32)
        if result.shape != (size,) or np.any(result < 0) or np.any(result >= num_parts):
            raise ValueError("KaFFPa returned invalid partition labels")
        if len(np.unique(result)) != num_parts:
            raise ValueError("KaFFPa returned one or more empty graph partitions")
        return result

    def _fallback_or_raise(self, reason: str, adjacency, num_parts, weights):
        self.fallback_reason = reason
        if self.require_kaffpa:
            raise RuntimeError(reason)
        labels = self.fallback.partition(adjacency, num_parts, weights)
        self.backend_used = "fallback"
        return labels

    def partition(self, adjacency, num_parts, node_weights=None):
        weights = _validate_graph(adjacency, node_weights)
        if num_parts <= 0 or num_parts > len(adjacency):
            raise ValueError("num_parts must be in [1, number of nodes]")
        executable = shutil.which(self.executable)
        if executable is None:
            return self._fallback_or_raise(
                f"KaFFPa executable not found: {self.executable}",
                adjacency,
                num_parts,
                weights,
            )
        with tempfile.TemporaryDirectory(prefix="pyramid-kaffpa-") as directory:
            work = Path(directory)
            graph_path = work / "graph.metis"
            output_path = work / "partitions.txt"
            self._write_metis_graph(graph_path, adjacency, weights)
            command = [
                executable,
                str(graph_path),
                f"--k={num_parts}",
                f"--imbalance={(self.imbalance - 1.0) * 100.0:g}",
                f"--preconfiguration={self.preconfiguration}",
                f"--seed={self.seed}",
                f"--output_filename={output_path}",
            ]
            try:
                completed = subprocess.run(
                    command,
                    check=True,
                    text=True,
                    capture_output=True,
                )
                del completed
                labels = np.loadtxt(output_path, dtype=np.int32, ndmin=1)
                result = self._validate_labels(labels, len(adjacency), num_parts)
            except (OSError, subprocess.CalledProcessError, ValueError) as error:
                return self._fallback_or_raise(
                    f"KaFFPa failed: {error}", adjacency, num_parts, weights
                )
        self.backend_used = "kaffpa"
        self.fallback_reason = None
        self.provenance = self._binary_provenance(executable)
        return result


def estimate_node_weights(router, vectors: npt.ArrayLike, batch_size: int = 4096) -> np.ndarray:
    values = np.asarray(vectors, dtype=np.float32)
    counts = np.ones(router.index.ntotal, dtype=np.int64)
    for start in range(0, len(values), batch_size):
        _, nodes = router.search_nodes(values[start : start + batch_size], 1)
        np.add.at(counts, nodes[:, 0], 1)
    return counts
