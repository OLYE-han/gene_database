"""VectorDBBench adapter for a prebuilt Pyramid RaBitQ HTTP deployment."""

from __future__ import annotations

import http.client
import json
from contextlib import contextmanager
from urllib.parse import urlsplit

import numpy as np

from vectordb_bench.backend.clients.api import NonRetryableInsertError, VectorDB
from vectordb_bench.backend.payload import PayloadProfile


class PyramidHTTPVectorDB(VectorDB):
    """Search-only VectorDBBench client for Pyramid's binary float32 API."""

    name = "PyramidRaBitQ"
    thread_safe = False

    def __init__(
        self,
        dim: int,
        db_config: dict,
        db_case_config=None,
        collection_name: str = "pyramid-rabitq",
        drop_old: bool = False,
        **kwargs,
    ) -> None:
        del db_case_config, collection_name, kwargs
        if drop_old:
            raise NonRetryableInsertError("Pyramid indexes are built offline; drop_old is unsupported")
        self.dim = int(dim)
        self.endpoints = tuple(str(value).rstrip("/") for value in db_config["endpoints"])
        self.timeout = float(db_config.get("timeout", 120))
        if not self.endpoints:
            raise ValueError("at least one read-node endpoint is required")
        self._connections: list[http.client.HTTPConnection] = []
        self._next_endpoint = 0

    @staticmethod
    def _connection(endpoint: str, timeout: float) -> http.client.HTTPConnection:
        parsed = urlsplit(endpoint)
        if parsed.scheme != "http" or not parsed.hostname:
            raise ValueError(f"only HTTP endpoints are supported: {endpoint}")
        return http.client.HTTPConnection(parsed.hostname, parsed.port or 80, timeout=timeout)

    @contextmanager
    def init(self):
        self._connections = [self._connection(endpoint, self.timeout) for endpoint in self.endpoints]
        try:
            for connection in self._connections:
                connection.request("GET", "/health")
                response = connection.getresponse()
                body = response.read()
                if response.status != 200:
                    raise RuntimeError(f"read-node health check failed: HTTP {response.status}: {body!r}")
                health = json.loads(body)
                if health.get("dimension") != self.dim:
                    raise ValueError(
                        f"read-node dimension {health.get('dimension')} does not match benchmark {self.dim}"
                    )
            yield
        finally:
            for connection in self._connections:
                connection.close()
            self._connections = []

    def need_normalize_cosine(self) -> bool:
        return True

    def insert_embeddings(self, embeddings, metadata, **kwargs):
        del embeddings, metadata, kwargs
        error = NonRetryableInsertError(
            "Pyramid uses offline immutable index builds; online insertion is not implemented"
        )
        return 0, error

    def optimize(self, data_size: int | None = None):
        del data_size
        return None

    def search_embedding(
        self,
        query: list[float],
        k: int = 100,
        payload_profile: PayloadProfile = PayloadProfile.IDS_ONLY,
        tenant: str | None = None,
    ) -> list[int]:
        if tenant is not None:
            raise NotImplementedError("tenant filtering is unsupported")
        if payload_profile != PayloadProfile.IDS_ONLY:
            raise NotImplementedError(f"payload profile {payload_profile} is unsupported")
        if not self._connections:
            raise RuntimeError("VectorDBBench must call init() before search")
        vector = np.asarray(query, dtype="<f4")
        if vector.shape != (self.dim,):
            raise ValueError(f"query must have shape ({self.dim},), got {vector.shape}")
        index = self._next_endpoint % len(self._connections)
        self._next_endpoint += 1
        connection = self._connections[index]
        try:
            connection.request(
                "POST",
                "/search/binary",
                body=vector.tobytes(),
                headers={"content-type": "application/octet-stream", "x-top-k": str(k)},
            )
            response = connection.getresponse()
            body = response.read()
        except (ConnectionError, OSError, http.client.HTTPException):
            connection.close()
            connection = self._connection(self.endpoints[index], self.timeout)
            self._connections[index] = connection
            connection.request(
                "POST",
                "/search/binary",
                body=vector.tobytes(),
                headers={"content-type": "application/octet-stream", "x-top-k": str(k)},
            )
            response = connection.getresponse()
            body = response.read()
        if response.status != 200:
            raise RuntimeError(f"search failed: HTTP {response.status}: {body[:500]!r}")
        result = json.loads(body)
        ids = [int(hit["id"]) for hit in result["hits"]]
        if len(ids) != k:
            raise RuntimeError(f"search returned {len(ids)} hits, expected {k}")
        return ids
