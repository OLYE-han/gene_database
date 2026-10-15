#!/usr/bin/env python3
"""Export deterministic VecBench P4/P5/P6 incremental files from Pyramid HDF5."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import h5py
import numpy as np


def scalar_bin(path: Path, values: np.ndarray) -> None:
    with path.open("wb") as handle:
        np.array([len(values), 1], dtype=np.int32).tofile(handle)
        values.tofile(handle)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--corpus-h5", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--ratio", type=float, default=0.2)
    parser.add_argument("--chunk-rows", type=int, default=1_000_000)
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)

    with h5py.File(args.corpus_h5, "r") as handle:
        rows, dimension = map(int, handle["embeddings"].shape)
        count = int(rows * args.ratio)
        selected = np.sort(np.random.default_rng(args.seed).choice(rows, count, replace=False))
        vectors = np.clip(np.asarray(handle["embeddings"][selected]), 0, 255).astype(np.uint8)
        equal = np.asarray(handle["equal"][selected], dtype=np.uint16)
        ranges = np.asarray(handle["range"][selected], dtype=np.float32)
        source_ptr = np.asarray(handle["tags_indptr"], dtype=np.int64)
        lengths = np.diff(source_ptr)[selected]
        positions = np.concatenate([
            np.arange(source_ptr[row], source_ptr[row + 1], dtype=np.int64) for row in selected
        ]) if int(lengths.sum()) else np.empty(0, dtype=np.int64)
        tag_indices = np.asarray(handle["tags_indices"][positions], dtype=np.int32)
        tag_indptr = np.concatenate(([0], np.cumsum(lengths))).astype(np.int64)

    with (args.output / "incremental.base.u8bin").open("wb") as handle:
        np.array([count, dimension], dtype=np.int32).tofile(handle)
        vectors.tofile(handle)
    scalar_bin(args.output / "incremental.equal.u2bin", equal)
    scalar_bin(args.output / "incremental.range.f4bin", ranges)
    with (args.output / "incremental.base.metadata.spmat").open("wb") as handle:
        columns = int(tag_indices.max()) + 1 if len(tag_indices) else 0
        np.array([count, columns, len(tag_indices)], dtype=np.int64).tofile(handle)
        tag_indptr.tofile(handle)
        tag_indices.tofile(handle)
        np.ones(len(tag_indices), dtype=np.float32).tofile(handle)
    (args.output / "incremental_info.json").write_text(json.dumps({
        "total_size": count, "vec_dim": dimension, "chunk_rows": args.chunk_rows,
        "min_inc_id": rows + 1, "max_inc_id": rows + count,
        "seed": args.seed, "ratio": args.ratio,
    }, indent=2), encoding="utf-8")


if __name__ == "__main__":
    main()
