"""Full VecBench ``BaseVD`` driver with immutable index rebuilds for mutations."""

from __future__ import annotations

import json
import os
import time
import urllib.request
from dataclasses import replace
from pathlib import Path

import h5py
import numpy as np

from benchmarks.vecbench_adapter import PyramidVecBench
from pyramid_rabitq.builder import PyramidBuilder
from pyramid_rabitq.config import load_build_config
from pyramid_rabitq.storage import H5Corpus


class Pyramid(PyramidVecBench):
    def __init__(self, config: dict, db_type: str = "pyramid") -> None:
        super().__init__(config, db_type)
        cfg = self.config
        self.source_dir = Path(os.environ.get("PYRAMID_VECBENCH_SOURCE", cfg["source_dir"]))
        self.index_root = Path(os.environ.get("PYRAMID_VECBENCH_INDEX", cfg["index_root"]))
        self.build_config = Path(os.environ.get("PYRAMID_VECBENCH_BUILD_CONFIG", cfg["build_config"]))
        self.admin_token = os.environ.get("PYRAMID_ADMIN_TOKEN", cfg.get("admin_token", ""))
        self.source_h5 = self.source_dir / "shard_0.h5"
        self._initial_loading = False

    def connect(self) -> None:
        # VecBench calls connect before P0 data loading, when no readable index
        # exists yet. Search connects lazily after P1 has published CURRENT.
        if (self.index_root / "CURRENT").exists():
            super().connect()

    def create_table(self, table_name, schema):
        del table_name, schema
        self.close()
        self.source_dir.mkdir(parents=True, exist_ok=True)
        if self.source_h5.exists():
            self.source_h5.unlink()
        self._initial_loading = True

    def process_data(self, data, table_name, schema):
        del table_name, schema
        required = {"id", "image_vec", "equal", "range", "tags"}
        missing = required.difference(data.columns)
        if missing:
            raise ValueError(f"VecBench batch misses columns: {sorted(missing)}")
        return data

    @staticmethod
    def _tag_csr(tags):
        lengths = np.fromiter((len(value) for value in tags), dtype=np.int64, count=len(tags))
        indptr = np.concatenate(([0], np.cumsum(lengths))).astype(np.int64)
        indices = (
            np.concatenate([np.asarray(value, dtype=np.int32) for value in tags])
            if int(indptr[-1]) else np.empty(0, dtype=np.int32)
        )
        return indptr, indices

    def _append(self, data) -> None:
        vectors = np.asarray(data["image_vec"].tolist(), dtype=np.float32)
        if vectors.ndim != 2 or vectors.shape[1] != self.dimension:
            raise ValueError(f"expected (*, {self.dimension}) image_vec")
        ids = np.asarray(data["id"], dtype=np.int64)
        equal = np.asarray(data["equal"], dtype=np.uint16)
        ranges = np.asarray(data["range"], dtype=np.float32)
        tag_indptr, tag_indices = self._tag_csr(data["tags"].tolist())
        self._ensure_resizable()
        with h5py.File(self.source_h5, "a") as handle:
            if "embeddings" not in handle:
                handle.create_dataset("embeddings", shape=(0, self.dimension), maxshape=(None, self.dimension), dtype="f4", chunks=(512, self.dimension))
                for name, dtype in (("ids", "i8"), ("equal", "u2"), ("range", "f4")):
                    handle.create_dataset(name, shape=(0,), maxshape=(None,), dtype=dtype, chunks=True)
                handle.create_dataset("tags_indptr", data=np.array([0], dtype=np.int64), maxshape=(None,), chunks=True)
                handle.create_dataset("tags_indices", shape=(0,), maxshape=(None,), dtype="i4", chunks=True)
            begin, end = len(handle["ids"]), len(handle["ids"]) + len(ids)
            for name, values in (("embeddings", vectors), ("ids", ids), ("equal", equal), ("range", ranges)):
                dataset = handle[name]
                dataset.resize((end,) + dataset.shape[1:])
                dataset[begin:end] = values
            old_tags = len(handle["tags_indices"])
            handle["tags_indices"].resize((old_tags + len(tag_indices),))
            handle["tags_indices"][old_tags:] = tag_indices
            pointer = handle["tags_indptr"]
            pointer.resize((end + 1,))
            pointer[begin + 1:end + 1] = old_tags + tag_indptr[1:]

    def _ensure_resizable(self) -> None:
        if not self.source_h5.exists():
            return
        with h5py.File(self.source_h5, "r") as source:
            if all(source[name].maxshape[0] is None for name in (
                "embeddings", "ids", "equal", "range", "tags_indptr", "tags_indices"
            )):
                return
            temporary = self.source_h5.with_suffix(".resizable.tmp.h5")
            with h5py.File(temporary, "w") as target:
                rows, dimension = source["embeddings"].shape
                embeddings = target.create_dataset(
                    "embeddings", shape=(rows, dimension), maxshape=(None, dimension),
                    dtype="f4", chunks=(512, dimension),
                )
                for start in range(0, rows, 4096):
                    embeddings[start:start + 4096] = source["embeddings"][start:start + 4096]
                for name in ("ids", "equal", "range", "tags_indptr", "tags_indices"):
                    values = source[name]
                    dataset = target.create_dataset(
                        name, shape=values.shape, maxshape=(None,), dtype=values.dtype, chunks=True
                    )
                    for start in range(0, len(values), 1_000_000):
                        dataset[start:start + 1_000_000] = values[start:start + 1_000_000]
        os.replace(temporary, self.source_h5)

    def _reload_nodes(self) -> None:
        if not self.admin_token:
            return
        request = urllib.request.Request(
            self.endpoint + "/admin/reload", method="POST",
            headers={"x-admin-token": self.admin_token},
        )
        opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
        with opener.open(request, timeout=self.timeout) as response:
            if response.status != 200:
                raise RuntimeError(f"read-node reload failed: HTTP {response.status}")

    def _rebuild(self, phase: str) -> float:
        started = time.perf_counter()
        config = load_build_config(self.build_config)
        executable = Path(config.hnsw.kaffpa_executable)
        if not executable.is_absolute():
            executable = (self.build_config.resolve().parent.parent / executable).resolve()
            config = replace(
                config, hnsw=replace(config.hnsw, kaffpa_executable=str(executable))
            )
        if config.dimension != self.dimension:
            raise ValueError("driver dimension and build config differ")
        version = f"vecbench-{phase}-{time.time_ns()}"
        PyramidBuilder(H5Corpus(self.source_dir, self.dimension), config).build(
            self.index_root, version
        )
        self.close()
        self._reload_nodes()
        return time.perf_counter() - started

    def insert_data(self, data, db, schema):
        del db, schema
        started = time.perf_counter()
        self._append(data)
        append_seconds = time.perf_counter() - started
        if self._initial_loading or not (self.index_root / "CURRENT").exists():
            return append_seconds
        self._rebuild("P4")
        return time.perf_counter() - started

    def create_index(self, index_params, args):
        del index_params, args
        self._initial_loading = False
        return self._rebuild("P1")

    def create_scalar_index(self, index_params=None, args=None):
        del index_params, args
        return 0.0  # scalar metadata is built atomically with every shard index

    def _rows_for_ids(self, ids):
        wanted = np.asarray(ids, dtype=np.int64)
        with h5py.File(self.source_h5, "r") as handle:
            stored = np.asarray(handle["ids"], dtype=np.int64)
        if len(stored) and np.array_equal(stored, np.arange(stored[0], stored[0] + len(stored))):
            rows = wanted - stored[0]
            if np.any(rows < 0) or np.any(rows >= len(stored)):
                raise KeyError("mutation contains unknown IDs")
            return rows
        mapping = {int(value): row for row, value in enumerate(stored)}
        try:
            return np.asarray([mapping[int(value)] for value in wanted], dtype=np.int64)
        except KeyError as error:
            raise KeyError(f"unknown mutation ID {error.args[0]}") from error

    def _rewrite_source(self, updates=None, deleted_rows=()) -> None:
        """Stream a replacement corpus, preserving scalar metadata and tag CSR."""
        update_rows = {} if updates is None else {
            int(row): i for i, row in enumerate(self._rows_for_ids(updates["id"]))
        }
        deleted = set(int(row) for row in deleted_rows)
        temporary = self.source_h5.with_suffix(".rewrite.tmp.h5")
        try:
            with h5py.File(self.source_h5, "r") as source, h5py.File(temporary, "w") as target:
                for name in ("embeddings", "ids", "equal", "range", "tags_indices"):
                    dataset = source[name]
                    target.create_dataset(
                        name, shape=(0,) + dataset.shape[1:],
                        maxshape=(None,) + dataset.shape[1:], dtype=dataset.dtype, chunks=True,
                    )
                target.create_dataset("tags_indptr", data=np.array([0], dtype=np.int64), maxshape=(None,), chunks=True)
                for begin in range(0, len(source["ids"]), 4096):
                    end = min(begin + 4096, len(source["ids"]))
                    keep = np.array([row not in deleted for row in range(begin, end)])
                    batch = {name: source[name][begin:end] for name in ("embeddings", "ids", "equal", "range")}
                    pointers = source["tags_indptr"][begin:end + 1]
                    tag_values = source["tags_indices"][int(pointers[0]):int(pointers[-1])]
                    pointers = pointers - pointers[0]
                    tags = [tag_values[int(pointers[i]):int(pointers[i + 1])].tolist() for i in range(end - begin)]
                    for offset in range(end - begin):
                        update = update_rows.get(begin + offset)
                        if update is not None:
                            record = updates.iloc[update]
                            batch["embeddings"][offset] = record["image_vec"]
                            batch["equal"][offset] = record["equal"]
                            batch["range"][offset] = record["range"]
                            tags[offset] = record["tags"]
                    old_rows = len(target["ids"])
                    for name, values in batch.items():
                        values = values[keep]
                        dataset = target[name]
                        dataset.resize((old_rows + len(values),) + dataset.shape[1:])
                        dataset[old_rows:] = values
                    indptr, indices = self._tag_csr([tag for tag, retain in zip(tags, keep) if retain])
                    old_tags = len(target["tags_indices"])
                    target["tags_indices"].resize((old_tags + len(indices),))
                    target["tags_indices"][old_tags:] = indices
                    target["tags_indptr"].resize((len(target["ids"]) + 1,))
                    target["tags_indptr"][old_rows + 1:] = old_tags + indptr[1:]
            os.replace(temporary, self.source_h5)
        finally:
            if temporary.exists():
                temporary.unlink()

    def update(self, batch_df, batch_size=10000):
        del batch_size
        started = time.perf_counter()
        self._rewrite_source(updates=batch_df)
        self._rebuild("P5")
        return time.perf_counter() - started

    def delete_by_ids(self, selected_ids):
        started = time.perf_counter()
        self._rewrite_source(deleted_rows=self._rows_for_ids(selected_ids))
        self._rebuild("P6")
        return time.perf_counter() - started

    def _reference_vector(self, vector_id: int):
        row = int(self._rows_for_ids([vector_id])[0])
        with h5py.File(self.source_h5, "r") as handle:
            return np.asarray(handle["embeddings"][row], dtype=np.float32)

    def query(self, query_config: dict, param=None):
        if "query_vector" not in query_config:
            query_config = dict(query_config)
            query_config["query_vector"] = self._reference_vector(
                int(query_config["reference_vector_name"])
            ).tolist()
        if self.connection is None:
            super().connect()
        return super().query(query_config, param)
