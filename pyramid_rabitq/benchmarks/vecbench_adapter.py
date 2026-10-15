"""Adapter from the paper VecBench database contract to Pyramid HTTP nodes.

This module targets https://github.com/ruc-datalab/VecBench, not Zilliz's
similarly named VectorDBBench project.
"""

from __future__ import annotations

import http.client
import json
import threading
from queue import Empty, Queue
from urllib.parse import urlsplit

import numpy as np

from databases.base_vd import BaseVD


class PyramidVecBench(BaseVD):
    """Search-only implementation of VecBench's ``BaseVD`` interface."""

    def __init__(self, config: dict, db_type: str = "pyramid") -> None:
        super().__init__(config, db_type)
        cfg = self.config
        self.endpoint = str(cfg["endpoint"]).rstrip("/")
        self.timeout = float(cfg.get("timeout", 120))
        self.dimension = int(cfg["dimension"])
        self.connection: http.client.HTTPConnection | None = None

    def connect(self) -> None:
        parsed = urlsplit(self.endpoint)
        if parsed.scheme != "http" or not parsed.hostname:
            raise ValueError(f"only HTTP endpoints are supported: {self.endpoint}")
        self.connection = http.client.HTTPConnection(
            parsed.hostname, parsed.port or 80, timeout=self.timeout
        )
        self.connection.request("GET", "/health")
        response = self.connection.getresponse()
        body = response.read()
        if response.status != 200:
            raise RuntimeError(f"health check failed: HTTP {response.status}: {body!r}")
        health = json.loads(body)
        if int(health.get("dimension", -1)) != self.dimension:
            raise ValueError(
                f"read-node dimension {health.get('dimension')} != expected {self.dimension}"
            )

    def _unsupported(self, operation: str):
        raise NotImplementedError(
            f"{operation} is unsupported: Pyramid uses an offline immutable index"
        )

    def create_table(self, table_name, schema):
        del table_name, schema
        return self._unsupported("create_table")

    def create_index(self, index_params, args):
        del index_params, args
        return self._unsupported("create_index")

    def create_scalar_index(self, index_params=None, args=None):
        del index_params, args
        return self._unsupported("create_scalar_index")

    def process_data(self, data, table_name, schema):
        del data, table_name, schema
        return self._unsupported("process_data")

    def insert_data(self, data, db, schema):
        del data, db, schema
        return self._unsupported("insert_data")

    def query(self, query_config: dict, param=None):
        """Execute one vector-only VecBench query and return ``(ids, seconds)``."""
        del param  # Search controls are fixed in the immutable index manifest.
        filters = query_config.get("scalar_filters", [])
        vector = np.asarray(query_config["query_vector"], dtype="<f4")
        if vector.shape != (self.dimension,):
            raise ValueError(f"query shape {vector.shape} != ({self.dimension},)")
        limit = int(query_config["limit"])
        if self.connection is None:
            raise RuntimeError("connect() must be called before query()")

        import time

        started = time.perf_counter()
        try:
            self.connection.request(
                "POST",
                "/search/binary",
                body=vector.tobytes(),
                headers={
                    "content-type": "application/octet-stream",
                    "x-top-k": str(limit),
                    "x-scalar-filters": json.dumps(filters, separators=(",", ":")),
                },
            )
            response = self.connection.getresponse()
            body = response.read()
        except (ConnectionError, OSError, http.client.HTTPException):
            self.close()
            self.connect()
            self.connection.request(
                "POST",
                "/search/binary",
                body=vector.tobytes(),
                headers={
                    "content-type": "application/octet-stream",
                    "x-top-k": str(limit),
                    "x-scalar-filters": json.dumps(filters, separators=(",", ":")),
                },
            )
            response = self.connection.getresponse()
            body = response.read()
        elapsed = time.perf_counter() - started
        if response.status != 200:
            raise RuntimeError(f"search failed: HTTP {response.status}: {body[:500]!r}")
        result = json.loads(body)
        ids = [int(hit["id"]) for hit in result["hits"]]
        if not filters and len(ids) != limit:
            raise RuntimeError(f"search returned {len(ids)} hits, expected {limit}")
        return ids, elapsed

    @staticmethod
    def cal_recall(result: list[int], ground_truth: list[int]) -> float:
        truth = set(map(int, ground_truth))
        return len(truth.intersection(map(int, result))) / len(truth) if truth else 0.0

    def close(self) -> None:
        if self.connection is not None:
            self.connection.close()
            self.connection = None


class PyramidVecBenchFactory:
    """Connection pool expected by VecBench's official concurrent executor."""

    def __init__(
        self,
        endpoints: list[str],
        dimension: int,
        max_connections: int,
        timeout: float = 120,
    ) -> None:
        if not endpoints:
            raise ValueError("at least one endpoint is required")
        self.endpoints = tuple(endpoints)
        self.dimension = int(dimension)
        self.timeout = float(timeout)
        self.max_connections = int(max_connections)
        self.connection_pool: Queue[PyramidVecBench] = Queue(maxsize=max_connections)
        self.lock = threading.Lock()
        self.active_connections = 0
        self.created_connections = 0

    def _new_connection(self) -> PyramidVecBench:
        endpoint = self.endpoints[self.created_connections % len(self.endpoints)]
        db = PyramidVecBench(
            {"pyramid": {
                "endpoint": endpoint,
                "dimension": self.dimension,
                "timeout": self.timeout,
            }}
        )
        db.connect()
        self.active_connections += 1
        self.created_connections += 1
        return db

    def get_connection(self) -> PyramidVecBench:
        try:
            return self.connection_pool.get_nowait()
        except Empty:
            with self.lock:
                if self.active_connections < self.max_connections:
                    return self._new_connection()
            return self.connection_pool.get()

    def release_connection(self, connection: PyramidVecBench) -> None:
        self.connection_pool.put(connection)

    def close_all(self) -> None:
        while True:
            try:
                self.connection_pool.get_nowait().close()
            except Empty:
                break
        self.active_connections = 0
