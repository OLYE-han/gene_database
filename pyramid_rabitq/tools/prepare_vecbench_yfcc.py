#!/usr/bin/env python3
"""Prepare VecBench's YFCC source format as metadata-bearing Pyramid HDF5.

The vector projection follows VecBench ``adjust_dimension``.  The public
repository does not publish its equal/range files, so deterministic surrogate
columns are generated and recorded as reconstructed, never as official data.
"""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

import h5py
import numpy as np
import yaml


def load_u8bin(path: Path) -> np.memmap:
    header = np.fromfile(path, dtype=np.int32, count=2)
    if len(header) != 2:
        raise ValueError(f"invalid u8bin: {path}")
    rows, dimension = map(int, header)
    expected = 8 + rows * dimension
    if path.stat().st_size != expected:
        raise ValueError(f"{path}: size {path.stat().st_size}, expected {expected}")
    return np.memmap(path, mode="r", dtype=np.uint8, offset=8, shape=(rows, dimension))


def load_sparse(path: Path):
    header = np.fromfile(path, dtype=np.int64, count=3)
    rows, columns, nnz = map(int, header)
    offset = 24
    indptr = np.memmap(path, mode="r", dtype=np.int64, offset=offset, shape=(rows + 1,))
    offset += (rows + 1) * 8
    indices = np.memmap(path, mode="r", dtype=np.int32, offset=offset, shape=(nnz,))
    offset += nnz * 4
    expected = offset + nnz * 4
    if path.stat().st_size != expected:
        raise ValueError(f"{path}: size {path.stat().st_size}, expected {expected}")
    return rows, columns, indptr, indices


def file_sha256(path: Path, block: int = 8 * 1024**2) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while data := handle.read(block):
            digest.update(data)
    return digest.hexdigest()


def query_overrides(query_file: Path, rows: int):
    queries = yaml.safe_load(query_file.read_text(encoding="utf-8"))["YFCC"]
    equal, ranges = {}, {}
    for query in queries:
        row = int(query["reference_vector_name"]) - 1
        if not 0 <= row < rows:
            continue
        for condition in query.get("scalar_filters", []):
            if condition["field"] == "equal" and condition["operator"] == "==":
                equal[row] = int(condition["value"])
            elif condition["field"] == "range":
                ranges.setdefault(row, []).append(float(condition["value"]))
    return equal, {row: float(np.mean(values)) for row, values in ranges.items()}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--vectors", type=Path, required=True)
    parser.add_argument("--metadata", type=Path, required=True)
    parser.add_argument("--query-file", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--rows", type=int, default=10_000_000)
    parser.add_argument("--dimension", type=int, default=1920)
    parser.add_argument("--batch-size", type=int, default=20_000)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--reuse-min-max", type=Path)
    args = parser.parse_args()

    vectors = load_u8bin(args.vectors)
    sparse_rows, sparse_columns, tag_indptr, tag_indices = load_sparse(args.metadata)
    rows = min(args.rows, len(vectors))
    if sparse_rows != len(vectors):
        raise ValueError("vector and sparse-metadata row counts differ")
    rng = np.random.RandomState(args.seed)
    projection = rng.normal(
        loc=0, scale=np.sqrt(1 / len(vectors)), size=(vectors.shape[1], args.dimension)
    ).astype(np.float32)

    if args.reuse_min_max:
        bounds = json.loads(args.reuse_min_max.read_text(encoding="utf-8"))
        minimum, maximum = float(bounds["minimum"]), float(bounds["maximum"])
    else:
        minimum, maximum = float("inf"), float("-inf")
        for start in range(0, rows, args.batch_size):
            projected = np.asarray(vectors[start : start + args.batch_size], dtype=np.float32) @ projection
            minimum = min(minimum, float(projected.min()))
            maximum = max(maximum, float(projected.max()))

    args.output.mkdir(parents=True, exist_ok=False)
    output_h5 = args.output / "shard_0.h5"
    scalar_rng = np.random.default_rng(args.seed)
    choices = np.arange(1, 501, dtype=np.uint16)
    weights = 1.0 / np.power(choices.astype(np.float64), 1.2)
    equal = scalar_rng.choice(choices, size=rows, p=weights / weights.sum()).astype(np.uint16)
    range_values = scalar_rng.lognormal(mean=0.0, sigma=1.0, size=rows).astype(np.float32)
    equal_overrides, range_overrides = query_overrides(args.query_file, rows)
    for row, value in equal_overrides.items():
        equal[row] = value
    for row, value in range_overrides.items():
        range_values[row] = value

    tag_stop = int(tag_indptr[rows])
    with h5py.File(output_h5, "w") as handle:
        embeddings = handle.create_dataset(
            "embeddings", shape=(rows, args.dimension), dtype="f4",
            chunks=(min(512, rows), args.dimension),
        )
        handle.create_dataset("ids", data=np.arange(1, rows + 1, dtype=np.int64))
        handle.create_dataset("equal", data=equal, chunks=True)
        handle.create_dataset("range", data=range_values, chunks=True)
        handle.create_dataset("tags_indptr", data=np.asarray(tag_indptr[: rows + 1]), chunks=True)
        handle.create_dataset("tags_indices", data=np.asarray(tag_indices[:tag_stop]), chunks=True)
        for start in range(0, rows, args.batch_size):
            stop = min(start + args.batch_size, rows)
            projected = np.asarray(vectors[start:stop], dtype=np.float32) @ projection
            quantized = np.clip((projected - minimum) / (maximum - minimum) * 255, 0, 255)
            embeddings[start:stop] = quantized.astype(np.float32)

    provenance = {
        "profile": "VecBench YFCC reconstructed-public",
        "rows": rows, "source_dimension": int(vectors.shape[1]),
        "dimension": args.dimension, "seed": args.seed,
        "projection_minimum": minimum, "projection_maximum": maximum,
        "tags_columns": sparse_columns, "tags_nnz": tag_stop,
        "official": {"vectors": True, "tags": True, "queries": True},
        "reconstructed": {
            "equal": "Zipf(s=1.2, values=1..500, seed=42; query references overridden)",
            "range": "lognormal(mean=0,sigma=1,seed=42; query references overridden)",
        },
        "sha256": {
            "vectors": file_sha256(args.vectors),
            "metadata": file_sha256(args.metadata),
            "queries": file_sha256(args.query_file),
        },
    }
    (args.output / "provenance.json").write_text(
        json.dumps(provenance, indent=2), encoding="utf-8"
    )


if __name__ == "__main__":
    main()
