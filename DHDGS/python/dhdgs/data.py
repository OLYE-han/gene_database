"""Read-only, bounded-memory access to prepared NPY datasets."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

import numpy as np


def write_json(path: Path, value: dict) -> None:
    """Create a new result; never overwrite an existing experiment."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("x", encoding="utf-8") as handle:
        json.dump(value, handle, ensure_ascii=False, indent=2, allow_nan=False)
        handle.write("\n")


def fingerprint(path: Path) -> tuple[int, int, int, int]:
    info = path.stat()
    return info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns


def load_complete_npy(path: Path) -> np.memmap:
    before = fingerprint(path)
    try:
        array = np.load(path, mmap_mode="r", allow_pickle=False)
    except (ValueError, EOFError, OSError) as exc:
        raise ValueError(f"Unreadable or incomplete NPY: {path}: {exc}") from exc
    if not isinstance(array, np.memmap):
        raise ValueError(f"Expected a standalone numeric NPY: {path}")
    if before[2] != array.offset + array.nbytes:
        raise ValueError(f"NPY length differs from header: {path}")
    if before != fingerprint(path):
        raise ValueError(f"File changed while opening: {path}")
    return array


class Dataset:
    """Contiguous global row IDs; IDs exposed as uint64, not float."""

    def __init__(self, root: str | Path):
        self.root = Path(root).resolve()
        if not (self.root / "manifest.json").is_file():
            children = list(self.root.glob("*/manifest.json"))
            if len(children) != 1:
                raise ValueError("Specify dataset root containing exactly one manifest.json")
            self.root = children[0].parent
        manifest_path = self.root / "manifest.json"
        self._snapshots = {manifest_path: fingerprint(manifest_path)}
        self.manifest = json.loads(manifest_path.read_text())
        self.rows = int(self.manifest["rows"])
        self.dimension = int(self.manifest["dimension"])
        if self.rows < 1 or self.dimension < 1:
            raise ValueError("Dataset dimensions must be positive")
        self.parts = []
        cursor = 0
        for part in self.manifest["parts"]:
            start, end = int(part["start"]), int(part["end"])
            if start != cursor or end <= start:
                raise ValueError("Parts must be nonempty, ordered and contiguous")
            path = (self.root / part["file"]).resolve()
            if not path.is_relative_to(self.root):
                raise ValueError("Part path must remain within dataset root")
            self._snapshots[path] = fingerprint(path)
            array = load_complete_npy(path)
            if array.dtype != np.dtype("<f4") or array.shape != (end - start, self.dimension):
                raise ValueError(f"Part shape/dtype mismatch: {path}")
            if not array.flags.c_contiguous:
                raise ValueError("Only row-major vector files are supported")
            self.parts.append((start, end, array))
            cursor = end
        if cursor != self.rows:
            raise ValueError("Parts do not cover declared row count")
        self.assert_unchanged()

    def assert_unchanged(self) -> None:
        for path, before in self._snapshots.items():
            if fingerprint(path) != before:
                raise ValueError(f"Dataset changed during operation: {path}")

    def identity(self) -> dict:
        return {
            "root": str(self.root), "rows": self.rows, "dimension": self.dimension,
            "manifest_sha256": hashlib.sha256((self.root / "manifest.json").read_bytes()).hexdigest(),
            "file_snapshots": [
                {"path": str(p.relative_to(self.root)), "bytes": s[2], "mtime_ns": s[3]}
                for p, s in self._snapshots.items()
            ],
            "content_checksum_verified": False,
        }

    def take(self, ids) -> np.ndarray:
        ids = validate_ids(ids)
        if np.any(ids >= self.rows):
            raise ValueError("Global ID outside dataset")
        out = np.empty((len(ids), self.dimension), dtype=np.float32)
        for start, end, array in self.parts:
            mask = (ids >= start) & (ids < end)
            if np.any(mask):
                out[mask] = array[(ids[mask] - start).astype(np.int64)]
        if not np.isfinite(out).all():
            raise ValueError("Input contains non-finite coordinates")
        return out

    def blocks(self, block_rows=1024):
        if block_rows < 1:
            raise ValueError("block_rows must be positive")
        for start, end, array in self.parts:
            for offset in range(0, end - start, block_rows):
                yield start + offset, array[offset:offset + block_rows]


def validate_ids(ids) -> np.ndarray:
    values = np.asarray(ids)
    if values.ndim != 1 or values.dtype.kind not in "ui" or np.any(values < 0):
        raise ValueError("IDs must be a one-dimensional nonnegative integer array")
    return values.astype(np.uint64, copy=False)


def sample_ids(population: int, count: int, seed: int) -> np.ndarray:
    """Floyd sampling: O(count) memory even for a 10B population."""
    if count < 1 or population < count or population > np.iinfo(np.int64).max:
        raise ValueError("Require 1 <= sample count <= population <= int64 maximum")
    rng = np.random.default_rng(seed)
    chosen = set()
    for j in range(population - count, population):
        t = int(rng.integers(0, j + 1))
        chosen.add(j if t in chosen else t)
    return np.array(sorted(chosen), dtype=np.uint64)
