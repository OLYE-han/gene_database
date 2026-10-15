from __future__ import annotations

import json
import re
import threading
from collections import OrderedDict
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Iterator

import h5py
import numpy as np

from .interfaces import VectorSource


SHARD_PATTERN = re.compile(r"shard_(\d+)\.h5$")


@dataclass(frozen=True)
class SourceShard:
    path: str
    shard_id: int
    rows: int
    dimension: int
    size_bytes: int
    mtime_ns: int


class H5Corpus(VectorSource):
    def __init__(self, source_dir: str | Path, dimension: int) -> None:
        self.source_dir = Path(source_dir)
        self._dimension = int(dimension)
        self.shards = self.inspect()
        if not self.shards:
            raise ValueError(f"no shard_*.h5 files found in {self.source_dir}")
        metadata_flags = [self._has_metadata(Path(shard.path)) for shard in self.shards]
        if any(metadata_flags) and not all(metadata_flags):
            raise ValueError("scalar metadata must be present on every source shard")
        self.has_metadata = bool(metadata_flags and metadata_flags[0])

    @staticmethod
    def _has_metadata(path: Path) -> bool:
        with h5py.File(path, "r") as handle:
            keys = {"equal", "range", "tags_indptr", "tags_indices"}
            present = keys.intersection(handle.keys())
            if present and present != keys:
                raise ValueError(f"{path}: incomplete scalar metadata datasets")
            return present == keys

    @property
    def dimension(self) -> int:
        return self._dimension

    @property
    def total_rows(self) -> int:
        return sum(shard.rows for shard in self.shards)

    def inspect(self) -> list[SourceShard]:
        result: list[SourceShard] = []
        paths = sorted(
            self.source_dir.glob("shard_*.h5"),
            key=lambda path: int(SHARD_PATTERN.match(path.name).group(1)),
        )
        for path in paths:
            match = SHARD_PATTERN.match(path.name)
            if match is None:
                continue
            with h5py.File(path, "r") as handle:
                if "embeddings" not in handle:
                    raise ValueError(f"{path}: missing embeddings")
                dataset = handle["embeddings"]
                if dataset.ndim != 2 or dataset.shape[1] != self.dimension:
                    raise ValueError(f"{path}: expected (*, {self.dimension}) embeddings")
                if dataset.dtype != np.float32:
                    raise ValueError(f"{path}: embeddings must be float32")
                rows = int(dataset.shape[0])
            stat = path.stat()
            result.append(
                SourceShard(str(path.resolve()), int(match.group(1)), rows, self.dimension, stat.st_size, stat.st_mtime_ns)
            )
        return result

    def batches(self, batch_size: int) -> Iterator[tuple[np.ndarray, np.ndarray]]:
        for ids, vectors, _ in self.batches_with_metadata(batch_size):
            yield ids, vectors

    def batches_with_metadata(
        self, batch_size: int
    ) -> Iterator[tuple[np.ndarray, np.ndarray, dict[str, np.ndarray] | None]]:
        for shard in self.shards:
            base = np.int64(shard.shard_id) << np.int64(32)
            with h5py.File(shard.path, "r") as handle:
                dataset = handle["embeddings"]
                ids_dataset = handle.get("ids")
                for start in range(0, shard.rows, batch_size):
                    stop = min(start + batch_size, shard.rows)
                    ids = (
                        np.asarray(ids_dataset[start:stop], dtype=np.int64)
                        if ids_dataset is not None
                        else base + np.arange(start, stop, dtype=np.int64)
                    )
                    metadata = None
                    if self.has_metadata:
                        begin_tag = int(handle["tags_indptr"][start])
                        end_tag = int(handle["tags_indptr"][stop])
                        metadata = {
                            "equal": np.asarray(handle["equal"][start:stop], dtype=np.uint16),
                            "range": np.asarray(handle["range"][start:stop], dtype=np.float32),
                            "tags_indptr": np.asarray(
                                handle["tags_indptr"][start : stop + 1], dtype=np.int64
                            ) - begin_tag,
                            "tags_indices": np.asarray(
                                handle["tags_indices"][begin_tag:end_tag], dtype=np.int32
                            ),
                        }
                    yield ids, np.asarray(dataset[start:stop], dtype=np.float32), metadata

    def sample(self, size: int, seed: int) -> np.ndarray:
        size = min(int(size), self.total_rows)
        rng = np.random.default_rng(seed)
        global_rows = np.sort(rng.choice(self.total_rows, size, replace=False))
        output = np.empty((size, self.dimension), dtype=np.float32)
        cursor = 0
        offset = 0
        for shard in self.shards:
            stop_offset = offset + shard.rows
            right = np.searchsorted(global_rows, stop_offset, side="left")
            selected = global_rows[cursor:right] - offset
            if len(selected):
                with h5py.File(shard.path, "r") as handle:
                    dataset = handle["embeddings"]
                    # h5py point selection becomes extremely slow for millions
                    # of rows. At high density, sequential reads touch roughly
                    # the same HDF5 chunks and are dramatically cheaper while
                    # returning the exact same deterministic sample.
                    if len(selected) / shard.rows >= 0.05:
                        read_rows = max(8192, int(dataset.chunks[0]) if dataset.chunks else 8192)
                        sample_cursor = 0
                        for start in range(0, shard.rows, read_rows):
                            stop = min(start + read_rows, shard.rows)
                            sample_stop = int(np.searchsorted(selected, stop, side="left"))
                            if sample_stop == sample_cursor:
                                continue
                            block = np.asarray(dataset[start:stop], dtype=np.float32)
                            local = selected[sample_cursor:sample_stop] - start
                            begin_output = cursor + sample_cursor
                            end_output = cursor + sample_stop
                            output[begin_output:end_output] = block[local]
                            sample_cursor = sample_stop
                    else:
                        output[cursor:right] = dataset[selected]
            cursor = right
            offset = stop_offset
        return output

    def save_manifest(self, path: Path) -> None:
        path.write_text(
            json.dumps({"total_rows": self.total_rows, "shards": [asdict(s) for s in self.shards]}, indent=2),
            encoding="utf-8",
        )


class PartitionWriter:
    def __init__(
        self, path: Path, dimension: int, chunk_rows: int = 512, with_metadata: bool = False
    ) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        self.path = path
        self.handle = h5py.File(path, "w")
        self.embeddings = self.handle.create_dataset(
            "embeddings", shape=(0, dimension), maxshape=(None, dimension), dtype="f4", chunks=(chunk_rows, dimension)
        )
        self.ids = self.handle.create_dataset(
            "ids", shape=(0,), maxshape=(None,), dtype="i8", chunks=(max(chunk_rows, 1024),)
        )
        self.with_metadata = bool(with_metadata)
        if self.with_metadata:
            self.equal = self.handle.create_dataset(
                "equal", shape=(0,), maxshape=(None,), dtype="u2", chunks=(max(chunk_rows, 1024),)
            )
            self.range = self.handle.create_dataset(
                "range", shape=(0,), maxshape=(None,), dtype="f4", chunks=(max(chunk_rows, 1024),)
            )
            self.tags_indptr = self.handle.create_dataset(
                "tags_indptr", data=np.array([0], dtype=np.int64), maxshape=(None,),
                chunks=(max(chunk_rows, 1024),)
            )
            self.tags_indices = self.handle.create_dataset(
                "tags_indices", shape=(0,), maxshape=(None,), dtype="i4",
                chunks=(max(chunk_rows * 4, 4096),)
            )

    def append(
        self, ids: np.ndarray, vectors: np.ndarray, metadata: dict[str, np.ndarray] | None = None
    ) -> None:
        begin = len(self.ids)
        end = begin + len(ids)
        self.embeddings.resize((end, self.embeddings.shape[1]))
        self.ids.resize((end,))
        self.embeddings[begin:end] = vectors
        self.ids[begin:end] = ids
        if self.with_metadata:
            if metadata is None:
                raise ValueError("partition metadata is required")
            self.equal.resize((end,))
            self.range.resize((end,))
            self.equal[begin:end] = metadata["equal"]
            self.range[begin:end] = metadata["range"]
            source_indptr = np.asarray(metadata["tags_indptr"], dtype=np.int64)
            source_indices = np.asarray(metadata["tags_indices"], dtype=np.int32)
            tag_begin = len(self.tags_indices)
            tag_end = tag_begin + len(source_indices)
            self.tags_indices.resize((tag_end,))
            self.tags_indices[tag_begin:tag_end] = source_indices
            self.tags_indptr.resize((end + 1,))
            self.tags_indptr[begin + 1 : end + 1] = tag_begin + source_indptr[1:]

    def close(self) -> None:
        self.handle.flush()
        self.handle.close()


class PartitionMetadata:
    """Indexed scalar columns used to plan and execute filtered searches.

    Equality and range columns retain their dense representation for broad
    predicates and also have sorted row indexes for selective predicates.
    Tags use an inverted index, so ``contains`` never walks the CSR rows.
    """

    def __init__(self, path: Path) -> None:
        with h5py.File(path, "r") as handle:
            self.available = "equal" in handle
            if not self.available:
                self.equal = self.range = np.empty(0)
                self.tag_values = self.tag_offsets = self.tag_rows = np.empty(0, dtype=np.int64)
                self.equal_order = self.range_order = np.empty(0, dtype=np.int64)
                self.equal_sorted = self.range_sorted = np.empty(0)
                return
            self.equal = np.asarray(handle["equal"], dtype=np.uint16)
            self.range = np.asarray(handle["range"], dtype=np.float32)
            indptr = np.asarray(handle["tags_indptr"], dtype=np.int64)
            indices = np.asarray(handle["tags_indices"], dtype=np.int32)
        row_dtype = np.int32 if len(self.equal) <= np.iinfo(np.int32).max else np.int64
        self.equal_order = np.argsort(self.equal, kind="stable").astype(row_dtype)
        self.equal_sorted = self.equal[self.equal_order]
        self.range_order = np.argsort(self.range, kind="stable").astype(row_dtype)
        self.range_sorted = self.range[self.range_order]
        # NumPy comparisons against NaN are always false. Keep NaNs outside
        # indexed bounds so the indexed implementation preserves that rule.
        self.range_valid_end = int(np.count_nonzero(~np.isnan(self.range_sorted)))
        rows = np.repeat(np.arange(len(self.equal), dtype=row_dtype), np.diff(indptr))
        if len(indices):
            order = np.argsort(indices, kind="stable")
            sorted_tags = indices[order]
            self.tag_values, first, counts = np.unique(
                sorted_tags, return_index=True, return_counts=True
            )
            self.tag_offsets = np.concatenate(([0], np.cumsum(counts))).astype(np.int64)
            self.tag_rows = rows[order]
        else:
            self.tag_values = np.empty(0, dtype=np.int32)
            self.tag_offsets = np.array([0], dtype=np.int64)
            self.tag_rows = np.empty(0, dtype=np.int64)

    def contains(self, value: int) -> np.ndarray:
        mask = np.zeros(len(self.equal), dtype=bool)
        position = int(np.searchsorted(self.tag_values, int(value)))
        if position < len(self.tag_values) and int(self.tag_values[position]) == int(value):
            mask[self.tag_rows[self.tag_offsets[position] : self.tag_offsets[position + 1]]] = True
        return mask

    @staticmethod
    def _bounds(sorted_values: np.ndarray, operator: str, value: object) -> tuple[int, int]:
        if operator == "==":
            return (
                int(np.searchsorted(sorted_values, value, side="left")),
                int(np.searchsorted(sorted_values, value, side="right")),
            )
        if operator == "<":
            return 0, int(np.searchsorted(sorted_values, value, side="left"))
        if operator == "<=":
            return 0, int(np.searchsorted(sorted_values, value, side="right"))
        if operator == ">":
            return int(np.searchsorted(sorted_values, value, side="right")), len(sorted_values)
        if operator == ">=":
            return int(np.searchsorted(sorted_values, value, side="left")), len(sorted_values)
        raise ValueError(f"unsupported scalar operator: {operator}")

    def _condition(self, condition: dict[str, object]) -> np.ndarray:
        field, operator = str(condition["field"]), str(condition["operator"])
        value = condition["value"]
        if field == "tags" and operator == "contains":
            return self.contains(int(value))
        if field == "equal":
            sorted_values, order = self.equal_sorted, self.equal_order
        elif field == "range":
            sorted_values = self.range_sorted[: self.range_valid_end]
            order = self.range_order[: self.range_valid_end]
        else:
            raise ValueError(f"unsupported scalar filter: {field} {operator}")
        begin, end = self._bounds(sorted_values, operator, value)
        mask = np.zeros(len(self.equal), dtype=bool)
        mask[order[begin:end]] = True
        return mask

    def may_match(self, filters: list[dict[str, object]]) -> bool:
        """Cheap, false-negative-free partition pruning check."""
        if not self.available:
            raise ValueError("this index has no scalar metadata")
        possible: bool | None = None
        for condition in filters:
            field, operator = str(condition["field"]), str(condition["operator"])
            value = condition["value"]
            if field == "tags" and operator == "contains":
                position = int(np.searchsorted(self.tag_values, int(value)))
                current = (
                    position < len(self.tag_values)
                    and int(self.tag_values[position]) == int(value)
                )
            elif field == "equal":
                begin, end = self._bounds(self.equal_sorted, operator, value)
                current = begin < end
            elif field == "range":
                begin, end = self._bounds(
                    self.range_sorted[: self.range_valid_end], operator, value
                )
                current = begin < end
            else:
                raise ValueError(f"unsupported scalar filter: {field} {operator}")
            if possible is None:
                possible = current
            elif str(condition.get("logic", "and")).lower() == "or":
                possible = possible or current
            else:
                possible = possible and current
        return True if possible is None else possible

    def filter(self, filters: list[dict[str, object]]) -> np.ndarray:
        if not self.available:
            raise ValueError("this index has no scalar metadata")
        result: np.ndarray | None = None
        for condition in filters:
            current = self._condition(condition)
            if result is None:
                result = np.asarray(current, dtype=bool)
            elif str(condition.get("logic", "and")).lower() == "or":
                result |= current
            else:
                result &= current
        return np.ones(len(self.equal), dtype=bool) if result is None else result


class VectorBlockCache:
    """Byte-bounded LRU cache for exact-rerank HDF5 blocks."""

    def __init__(self, max_bytes: int, block_rows: int = 512) -> None:
        self.max_bytes = int(max_bytes)
        self.block_rows = int(block_rows)
        self.current_bytes = 0
        self.items: OrderedDict[tuple[str, int], np.ndarray] = OrderedDict()
        self._lock = threading.RLock()

    def get_block(self, path: Path, block_id: int) -> np.ndarray:
        with self._lock:
            key = (str(path), int(block_id))
            cached = self.items.pop(key, None)
            if cached is not None:
                self.items[key] = cached
                return cached
            with h5py.File(path, "r") as handle:
                dataset = handle["embeddings"]
                begin = block_id * self.block_rows
                block = np.asarray(dataset[begin : min(begin + self.block_rows, len(dataset))], dtype=np.float32)
            if block.nbytes <= self.max_bytes:
                while self.items and self.current_bytes + block.nbytes > self.max_bytes:
                    _, removed = self.items.popitem(last=False)
                    self.current_bytes -= removed.nbytes
                self.items[key] = block
                self.current_bytes += block.nbytes
            return block

    def read_rows(self, path: Path, rows: np.ndarray) -> np.ndarray:
        if len(rows) == 0:
            return np.empty((0, 0), dtype=np.float32)
        requested = np.asarray(rows, dtype=np.int64)
        block_ids = requested // self.block_rows
        offsets = requested % self.block_rows
        result: np.ndarray | None = None
        for block_id in np.unique(block_ids):
            positions = np.flatnonzero(block_ids == block_id)
            block = self.get_block(path, int(block_id))
            if result is None:
                result = np.empty((len(requested), block.shape[1]), dtype=np.float32)
            result[positions] = block[offsets[positions]]
        assert result is not None
        return result
