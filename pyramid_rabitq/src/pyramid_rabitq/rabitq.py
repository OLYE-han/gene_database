from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

import faiss
import h5py
import numpy as np
import numpy.typing as npt
from scipy.fft import dct

from .math_utils import as_float32_matrix, prepare_vectors

try:
    from ._native import (
        fastscan_backend as _native_fastscan_backend,
        packed_sign_dot as _native_packed_sign_dot,
        packed_sign_dot_fastscan as _native_packed_sign_dot_fastscan,
    )
except ImportError:  # Source-tree tests and unsupported platforms use NumPy.
    _native_fastscan_backend = None
    _native_packed_sign_dot = None
    _native_packed_sign_dot_fastscan = None

NATIVE_SCAN_AVAILABLE = _native_packed_sign_dot is not None
FASTSCAN_AVAILABLE = _native_packed_sign_dot_fastscan is not None
FASTSCAN_BACKEND = _native_fastscan_backend() if _native_fastscan_backend is not None else "numpy"
FASTSCAN_BLOCK_SIZE = 32


_BYTE_SIGNS = (
    ((np.arange(256, dtype=np.uint16)[:, None] >> np.arange(8, dtype=np.uint16)) & 1)
    .astype(np.float32)
    * 2.0
    - 1.0
)


def packed_sign_dot(codes: np.ndarray, query: np.ndarray, dimension: int) -> np.ndarray:
    """Dot packed {-1,+1} codes with a float query without unpacking NxD."""
    if _native_packed_sign_dot is not None:
        return _native_packed_sign_dot(codes, query, dimension)
    width = codes.shape[1]
    padded = np.zeros(width * 8, dtype=np.float32)
    padded[:dimension] = query
    lookup = _BYTE_SIGNS @ padded.reshape(width, 8).T
    byte_indexes = np.arange(width)
    return lookup[codes, byte_indexes].sum(axis=1, dtype=np.float32)


def to_fastscan_layout(codes: np.ndarray) -> np.ndarray:
    """Transpose row-major packed codes into (block, byte, lane) order."""
    values = np.asarray(codes, dtype=np.uint8)
    if values.ndim != 2:
        raise ValueError("row-major codes must be two-dimensional")
    rows, width = values.shape
    blocks = (rows + FASTSCAN_BLOCK_SIZE - 1) // FASTSCAN_BLOCK_SIZE
    padded = np.zeros((blocks * FASTSCAN_BLOCK_SIZE, width), dtype=np.uint8)
    padded[:rows] = values
    return np.ascontiguousarray(
        padded.reshape(blocks, FASTSCAN_BLOCK_SIZE, width).transpose(0, 2, 1)
    )


def packed_sign_dot_fastscan(
    codes: np.ndarray,
    query: np.ndarray,
    dimension: int,
    begin: int,
    end: int,
) -> np.ndarray:
    if _native_packed_sign_dot_fastscan is not None:
        return _native_packed_sign_dot_fastscan(codes, query, dimension, begin, end)
    positions = np.arange(begin, end, dtype=np.int64)
    row_codes = np.asarray(
        codes[positions // FASTSCAN_BLOCK_SIZE, :, positions % FASTSCAN_BLOCK_SIZE],
        dtype=np.uint8,
    )
    return packed_sign_dot(row_codes, query, dimension)


@dataclass(frozen=True)
class ApproximateResults:
    distances: np.ndarray
    ids: np.ndarray
    rows: np.ndarray


class OrthogonalTransform:
    """Fast deterministic randomized orthogonal transform.

    A random sign diagonal followed by an orthonormal DCT and permutation is
    used instead of materializing a dense D x D random matrix.
    """

    def __init__(self, dimension: int, seed: int) -> None:
        rng = np.random.default_rng(seed)
        self.dimension = int(dimension)
        self.signs = rng.choice(np.array([-1.0, 1.0], dtype=np.float32), dimension)
        self.permutation = rng.permutation(dimension)

    def apply(self, values: npt.ArrayLike) -> np.ndarray:
        vectors = as_float32_matrix(values, self.dimension)
        transformed = dct(vectors * self.signs, type=2, norm="ortho", axis=1)
        return np.asarray(transformed[:, self.permutation], dtype=np.float32)


class RaBitQIVFIndex:
    """IVF with one-bit RaBitQ residual codes and exact-rerank locators."""

    FORMAT_VERSION = 2

    def __init__(
        self,
        metric: str,
        seed: int,
        centroids: np.ndarray,
        offsets: np.ndarray,
        codes: np.ndarray,
        ids: np.ndarray,
        rows: np.ndarray,
        residual_norms: np.ndarray,
        corrections: np.ndarray,
        scan_block_size: int = 4096,
        code_layout: str = "row",
    ) -> None:
        self.metric = metric
        self.seed = int(seed)
        self.centroids = np.asarray(centroids, dtype=np.float32)
        self.offsets = np.asarray(offsets, dtype=np.int64)
        self.codes = codes
        self.ids = ids
        self.rows = rows
        self.residual_norms = residual_norms
        self.corrections = corrections
        self.dimension = int(self.centroids.shape[1])
        self.nlist = int(self.centroids.shape[0])
        self.scan_block_size = int(scan_block_size)
        self.code_layout = code_layout
        self.transform = OrthogonalTransform(self.dimension, self.seed)
        self._validate()

    def _validate(self) -> None:
        count = len(self.ids)
        if self.offsets.shape != (self.nlist + 1,) or self.offsets[0] != 0 or self.offsets[-1] != count:
            raise ValueError("invalid IVF offsets")
        if any(len(value) != count for value in (self.rows, self.residual_norms, self.corrections)):
            raise ValueError("RaBitQ arrays have inconsistent row counts")
        width = (self.dimension + 7) // 8
        if self.code_layout == "row":
            if self.codes.shape != (count, width):
                raise ValueError("row-major packed code shape does not match index")
        elif self.code_layout == "fastscan32":
            expected_blocks = (count + FASTSCAN_BLOCK_SIZE - 1) // FASTSCAN_BLOCK_SIZE
            if self.codes.shape != (expected_blocks, width, FASTSCAN_BLOCK_SIZE):
                raise ValueError("FastScan packed code shape does not match index")
        else:
            raise ValueError(f"unsupported code layout: {self.code_layout}")

    @classmethod
    def build(
        cls,
        vectors: npt.ArrayLike,
        ids: npt.ArrayLike | None = None,
        rows: npt.ArrayLike | None = None,
        *,
        nlist: int = 1024,
        metric: str = "cosine",
        seed: int = 42,
        train_size: int = 100_000,
        scan_block_size: int = 4096,
    ) -> "RaBitQIVFIndex":
        prepared = prepare_vectors(vectors, metric)
        count, dimension = prepared.shape
        if count == 0:
            raise ValueError("cannot build an empty index")
        nlist = max(1, min(int(nlist), count))
        rng = np.random.default_rng(seed)
        training_rows = rng.choice(count, min(count, train_size), replace=False)
        training = np.ascontiguousarray(prepared[training_rows])
        kmeans = faiss.Kmeans(dimension, nlist, niter=20, verbose=False, seed=seed, spherical=False)
        kmeans.train(training)
        centroids = np.asarray(kmeans.centroids, dtype=np.float32).reshape(nlist, dimension)
        coarse = faiss.IndexFlatL2(dimension)
        coarse.add(centroids)
        _, assignments = coarse.search(prepared, 1)
        assignments = assignments[:, 0].astype(np.int32)

        residuals = prepared - centroids[assignments]
        norms = np.linalg.norm(residuals, axis=1).astype(np.float32)
        safe_norms = np.maximum(norms, np.finfo(np.float32).eps)
        unit = residuals / safe_norms[:, None]
        transform = OrthogonalTransform(dimension, seed)
        rotated = transform.apply(unit)
        bit_values = rotated >= 0
        packed = np.packbits(bit_values, axis=1, bitorder="little")
        # <unit residual, hypercube vertex>; needed by the RaBitQ estimator.
        corrections = (np.abs(rotated).sum(axis=1) / np.sqrt(dimension)).astype(np.float32)
        corrections = np.maximum(corrections, np.finfo(np.float32).eps)

        order = np.argsort(assignments, kind="stable")
        counts = np.bincount(assignments, minlength=nlist)
        offsets = np.concatenate(([0], np.cumsum(counts))).astype(np.int64)
        vector_ids = np.arange(count, dtype=np.int64) if ids is None else np.asarray(ids, dtype=np.int64)
        vector_rows = np.arange(count, dtype=np.int64) if rows is None else np.asarray(rows, dtype=np.int64)
        if vector_ids.shape != (count,) or vector_rows.shape != (count,):
            raise ValueError("ids and rows must contain one value per vector")
        sorted_codes = packed[order]
        return cls(
            metric,
            seed,
            centroids,
            offsets,
            to_fastscan_layout(sorted_codes),
            vector_ids[order],
            vector_rows[order],
            norms[order],
            corrections[order],
            scan_block_size,
            "fastscan32",
        )

    @classmethod
    def build_h5(
        cls,
        h5_path: Path,
        output_path: Path,
        *,
        nlist: int,
        metric: str,
        seed: int,
        train_size: int,
        batch_size: int = 4096,
        scan_block_size: int = 4096,
    ) -> "RaBitQIVFIndex":
        """Build directly to mmap-compatible arrays without loading a shard.

        The HDF5 file must contain ``embeddings`` and ``ids`` datasets. Two
        sequential passes count IVF lists and then encode/scatter their rows.
        """
        output_path.mkdir(parents=True, exist_ok=True)
        with h5py.File(h5_path, "r") as handle:
            dataset = handle["embeddings"]
            ids_dataset = handle["ids"]
            count, dimension = dataset.shape
            if count == 0 or ids_dataset.shape != (count,):
                raise ValueError("invalid or empty partition HDF5")
            nlist = max(1, min(int(nlist), count, train_size))
            rng = np.random.default_rng(seed)
            training_rows = np.sort(rng.choice(count, min(count, train_size), replace=False))
            training = prepare_vectors(np.asarray(dataset[training_rows], dtype=np.float32), metric)
            kmeans = faiss.Kmeans(dimension, nlist, niter=20, verbose=False, seed=seed)
            kmeans.train(training)
            centroids = np.asarray(kmeans.centroids, dtype=np.float32).reshape(nlist, dimension)
            coarse = faiss.IndexFlatL2(dimension)
            coarse.add(centroids)

            counts = np.zeros(nlist, dtype=np.int64)
            for start in range(0, count, batch_size):
                vectors = prepare_vectors(dataset[start : start + batch_size], metric)
                _, assigned = coarse.search(vectors, 1)
                counts += np.bincount(assigned[:, 0], minlength=nlist)
            offsets = np.concatenate(([0], np.cumsum(counts))).astype(np.int64)

            code_width = (dimension + 7) // 8
            codes = np.lib.format.open_memmap(
                output_path / "codes.npy", mode="w+", dtype=np.uint8, shape=(count, code_width)
            )
            ids = np.lib.format.open_memmap(
                output_path / "ids.npy", mode="w+", dtype=np.int64, shape=(count,)
            )
            rows = np.lib.format.open_memmap(
                output_path / "rows.npy", mode="w+", dtype=np.int64, shape=(count,)
            )
            norms_out = np.lib.format.open_memmap(
                output_path / "residual_norms.npy", mode="w+", dtype=np.float32, shape=(count,)
            )
            corrections_out = np.lib.format.open_memmap(
                output_path / "corrections.npy", mode="w+", dtype=np.float32, shape=(count,)
            )
            transform = OrthogonalTransform(dimension, seed)
            cursors = offsets[:-1].copy()
            epsilon = np.finfo(np.float32).eps
            for start in range(0, count, batch_size):
                stop = min(start + batch_size, count)
                vectors = prepare_vectors(dataset[start:stop], metric)
                _, assigned_matrix = coarse.search(vectors, 1)
                assigned = assigned_matrix[:, 0]
                residuals = vectors - centroids[assigned]
                norms = np.linalg.norm(residuals, axis=1).astype(np.float32)
                unit = residuals / np.maximum(norms, epsilon)[:, None]
                rotated = transform.apply(unit)
                packed = np.packbits(rotated >= 0, axis=1, bitorder="little")
                corrections = np.maximum(
                    np.abs(rotated).sum(axis=1) / np.sqrt(dimension), epsilon
                ).astype(np.float32)
                batch_ids = np.asarray(ids_dataset[start:stop], dtype=np.int64)
                for list_id in np.unique(assigned):
                    source = np.flatnonzero(assigned == list_id)
                    begin = int(cursors[list_id])
                    destination = np.arange(begin, begin + len(source))
                    codes[destination] = packed[source]
                    ids[destination] = batch_ids[source]
                    rows[destination] = start + source
                    norms_out[destination] = norms[source]
                    corrections_out[destination] = corrections[source]
                    cursors[list_id] += len(source)
            for array in (codes, ids, rows, norms_out, corrections_out):
                array.flush()

            fastscan_path = output_path / "codes_fastscan.npy"
            fastscan_codes = np.lib.format.open_memmap(
                fastscan_path,
                mode="w+",
                dtype=np.uint8,
                shape=(
                    (count + FASTSCAN_BLOCK_SIZE - 1) // FASTSCAN_BLOCK_SIZE,
                    code_width,
                    FASTSCAN_BLOCK_SIZE,
                ),
            )
            fastscan_codes[:] = 0
            for start in range(0, count, FASTSCAN_BLOCK_SIZE):
                stop = min(start + FASTSCAN_BLOCK_SIZE, count)
                block = start // FASTSCAN_BLOCK_SIZE
                fastscan_codes[block, :, : stop - start] = np.asarray(codes[start:stop]).T
            fastscan_codes.flush()
            del fastscan_codes
            del codes
            (output_path / "codes.npy").unlink()

        np.save(output_path / "centroids.npy", centroids, allow_pickle=False)
        np.save(output_path / "offsets.npy", offsets, allow_pickle=False)
        metadata = {
            "format_version": cls.FORMAT_VERSION,
            "metric": metric,
            "seed": seed,
            "dimension": dimension,
            "nlist": nlist,
            "vectors": count,
            "scan_block_size": scan_block_size,
            "code_layout": "fastscan32",
        }
        (output_path / "metadata.json").write_text(json.dumps(metadata, indent=2), encoding="utf-8")
        return cls.load(output_path, mmap=True)

    def _probe_lists(self, query: np.ndarray, nprobe: int) -> np.ndarray:
        distances = np.sum((self.centroids - query) ** 2, axis=1)
        nprobe = max(1, min(int(nprobe), self.nlist))
        selected = np.argpartition(distances, nprobe - 1)[:nprobe]
        return selected[np.argsort(distances[selected])]

    def plan_filtered_nprobe(
        self,
        query: npt.ArrayLike,
        allowed_rows: np.ndarray,
        target_candidates: int,
        minimum_nprobe: int,
    ) -> int:
        """Grow probes until centroid-ordered lists contain enough legal rows."""
        allowed = np.asarray(allowed_rows, dtype=bool)
        if allowed.shape != (len(self.ids),):
            raise ValueError("allowed_rows must contain one value per local row")
        q = prepare_vectors(query, self.metric)
        if q.shape != (1, self.dimension):
            raise ValueError("plan_filtered_nprobe accepts exactly one query")
        ordered_lists = self._probe_lists(q[0], self.nlist)
        minimum = max(1, min(int(minimum_nprobe), self.nlist))
        target = max(1, int(target_candidates))
        matches = 0
        for rank, list_id in enumerate(ordered_lists, start=1):
            begin, end = int(self.offsets[list_id]), int(self.offsets[list_id + 1])
            if begin != end:
                matches += int(np.count_nonzero(allowed[self.rows[begin:end]]))
            if rank >= minimum and matches >= target:
                return rank
        return self.nlist

    def search_approx(
        self,
        query: npt.ArrayLike,
        candidate_k: int,
        nprobe: int,
        allowed_rows: np.ndarray | None = None,
    ) -> ApproximateResults:
        if candidate_k <= 0:
            raise ValueError("candidate_k must be positive")
        q = prepare_vectors(query, self.metric)
        if q.shape != (1, self.dimension):
            raise ValueError("search_approx accepts exactly one query")
        query_vector = q[0]
        all_distances: list[np.ndarray] = []
        all_positions: list[np.ndarray] = []
        scale = np.float32(1.0 / np.sqrt(self.dimension))

        selected_lists = self._probe_lists(query_vector, nprobe)
        query_residuals = query_vector[None, :] - self.centroids[selected_lists]
        rotated_queries = self.transform.apply(query_residuals)
        query_norm_squares = np.einsum(
            "ij,ij->i", query_residuals, query_residuals, dtype=np.float32
        )
        for list_offset, list_id in enumerate(selected_lists):
            begin, end = int(self.offsets[list_id]), int(self.offsets[list_id + 1])
            if begin == end:
                continue
            rotated_query = rotated_queries[list_offset]
            query_norm_sq = float(query_norm_squares[list_offset])
            for start in range(begin, end, self.scan_block_size):
                stop = min(start + self.scan_block_size, end)
                positions = np.arange(start, stop, dtype=np.int64)
                if allowed_rows is not None:
                    accepted = np.asarray(allowed_rows[self.rows[start:stop]], dtype=bool)
                    if not np.any(accepted):
                        continue
                    positions = positions[accepted]
                if self.code_layout == "fastscan32" and allowed_rows is not None:
                    # A selective predicate should not pay to scan rejected
                    # lanes. Gather qualifying codes into row-major form and
                    # use the native packed-code kernel on only those rows.
                    row_codes = np.asarray(
                        self.codes[
                            positions // FASTSCAN_BLOCK_SIZE,
                            :,
                            positions % FASTSCAN_BLOCK_SIZE,
                        ],
                        dtype=np.uint8,
                    )
                    projected = packed_sign_dot(
                        row_codes, rotated_query, self.dimension
                    ) * scale
                elif self.code_layout == "fastscan32":
                    projected = packed_sign_dot_fastscan(
                        self.codes, rotated_query, self.dimension, start, stop
                    ) * scale
                else:
                    projected = packed_sign_dot(
                        self.codes[positions], rotated_query, self.dimension
                    ) * scale
                relevant_norms = self.residual_norms[positions]
                relevant_corrections = self.corrections[positions]
                estimated_dot = relevant_norms * projected / relevant_corrections
                estimates = (
                    relevant_norms ** 2 + query_norm_sq - 2.0 * estimated_dot
                )
                all_distances.append(estimates.astype(np.float32))
                all_positions.append(positions)

        if not all_positions:
            empty_float = np.empty(0, dtype=np.float32)
            empty_int = np.empty(0, dtype=np.int64)
            return ApproximateResults(empty_float, empty_int, empty_int)
        distances = np.concatenate(all_distances)
        positions = np.concatenate(all_positions)
        keep = min(candidate_k, len(positions))
        selected = np.argpartition(distances, keep - 1)[:keep]
        selected = selected[np.argsort(distances[selected])]
        positions = positions[selected]
        return ApproximateResults(distances[selected], self.ids[positions], self.rows[positions])

    def save(self, path: Path) -> None:
        path.mkdir(parents=True, exist_ok=True)
        arrays = {
            "centroids": self.centroids,
            "offsets": self.offsets,
            "ids": self.ids,
            "rows": self.rows,
            "residual_norms": self.residual_norms,
            "corrections": self.corrections,
        }
        code_name = "codes_fastscan" if self.code_layout == "fastscan32" else "codes"
        arrays[code_name] = self.codes
        for name, value in arrays.items():
            np.save(path / f"{name}.npy", value, allow_pickle=False)
        metadata = {
            "format_version": self.FORMAT_VERSION,
            "metric": self.metric,
            "seed": self.seed,
            "dimension": self.dimension,
            "nlist": self.nlist,
            "vectors": len(self.ids),
            "scan_block_size": self.scan_block_size,
            "code_layout": self.code_layout,
        }
        (path / "metadata.json").write_text(json.dumps(metadata, indent=2), encoding="utf-8")

    @classmethod
    def load(cls, path: Path, mmap: bool = True) -> "RaBitQIVFIndex":
        metadata = json.loads((path / "metadata.json").read_text(encoding="utf-8"))
        if metadata["format_version"] not in {1, cls.FORMAT_VERSION}:
            raise ValueError("unsupported RaBitQ index format")
        mode = "r" if mmap else None
        load = lambda name: np.load(path / f"{name}.npy", mmap_mode=mode, allow_pickle=False)
        code_layout = metadata.get("code_layout", "row")
        code_name = "codes_fastscan" if code_layout == "fastscan32" else "codes"
        return cls(
            metadata["metric"],
            metadata["seed"],
            load("centroids"),
            load("offsets"),
            load(code_name),
            load("ids"),
            load("rows"),
            load("residual_norms"),
            load("corrections"),
            metadata["scan_block_size"],
            code_layout,
        )

    @classmethod
    def convert_path_to_fastscan(cls, path: Path, remove_row_codes: bool = True) -> None:
        """Convert a copied/unpublished v1 index directory to FastScan layout."""
        metadata_path = path / "metadata.json"
        metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
        if metadata.get("code_layout") == "fastscan32":
            return
        row_path = path / "codes.npy"
        codes = np.load(row_path, mmap_mode="r", allow_pickle=False)
        rows, width = codes.shape
        fast_path = path / "codes_fastscan.npy"
        fast = np.lib.format.open_memmap(
            fast_path,
            mode="w+",
            dtype=np.uint8,
            shape=(
                (rows + FASTSCAN_BLOCK_SIZE - 1) // FASTSCAN_BLOCK_SIZE,
                width,
                FASTSCAN_BLOCK_SIZE,
            ),
        )
        fast[:] = 0
        for start in range(0, rows, FASTSCAN_BLOCK_SIZE):
            stop = min(start + FASTSCAN_BLOCK_SIZE, rows)
            fast[start // FASTSCAN_BLOCK_SIZE, :, : stop - start] = np.asarray(codes[start:stop]).T
        fast.flush()
        del fast
        del codes
        metadata["format_version"] = cls.FORMAT_VERSION
        metadata["code_layout"] = "fastscan32"
        metadata_path.write_text(json.dumps(metadata, indent=2), encoding="utf-8")
        if remove_row_codes:
            row_path.unlink()
