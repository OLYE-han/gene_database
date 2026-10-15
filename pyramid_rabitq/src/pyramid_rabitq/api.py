from __future__ import annotations

import argparse
import json
import os
import threading
from contextlib import asynccontextmanager
from typing import Annotated

import numpy as np
import uvicorn
from fastapi import Body, FastAPI, Header, HTTPException, Request
from pydantic import BaseModel, Field

from .config import load_service_config
from .engine import ReadNodeEngine


class SearchRequest(BaseModel):
    vector: list[float]
    top_k: int = Field(default=10, ge=1)
    route_partitions: int | None = Field(default=None, ge=1)
    candidate_multiplier: int | None = Field(default=None, ge=1)
    scalar_filters: list[dict[str, object]] = Field(default_factory=list)


class SearchHitModel(BaseModel):
    id: int
    distance: float
    partition_id: int


class SearchResponseModel(BaseModel):
    hits: list[SearchHitModel]
    version: str
    searched_partitions: list[int]
    approximate_candidates: int


def create_app(
    index_root: str,
    cache_bytes: int,
    admin_token: str | None = None,
    top_k_limit: int = 1000,
    partition_workers: int = 4,
    max_inflight_queries: int = 4,
    mmap_indexes: bool = True,
    require_native_scan: bool = False,
    require_fastscan: bool = False,
) -> FastAPI:
    query_slots = threading.BoundedSemaphore(max(1, max_inflight_queries))

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        app.state.engine = ReadNodeEngine(
            index_root,
            cache_bytes,
            partition_workers=partition_workers,
            mmap_indexes=mmap_indexes,
        )
        if require_native_scan and not app.state.engine.health()["native_scan"]:
            app.state.engine.close()
            raise RuntimeError("native RaBitQ scanner is required but unavailable")
        health = app.state.engine.health()
        if require_fastscan and (
            not health["fastscan"] or health["code_layouts"] != ["fastscan32"]
        ):
            app.state.engine.close()
            raise RuntimeError("native FastScan and fastscan32 indexes are required")
        try:
            yield
        finally:
            app.state.engine.close()

    app = FastAPI(title="Pyramid RaBitQ Read Node", version="0.6.0", lifespan=lifespan)

    @app.get("/health")
    def health(request: Request):
        return request.app.state.engine.health()

    @app.get("/index")
    def index_info(request: Request):
        engine = request.app.state.engine
        return {"version": engine.version, "manifest": engine.manifest}

    @app.post("/search", response_model=SearchResponseModel)
    def search(payload: SearchRequest, request: Request):
        if payload.top_k > top_k_limit:
            raise HTTPException(status_code=422, detail=f"top_k exceeds limit {top_k_limit}")
        if not query_slots.acquire(timeout=30):
            raise HTTPException(status_code=503, detail="read node is saturated")
        try:
            try:
                response = request.app.state.engine.search(
                    payload.vector,
                    payload.top_k,
                    route_partitions=payload.route_partitions,
                    candidate_multiplier=payload.candidate_multiplier,
                    scalar_filters=payload.scalar_filters,
                )
                return {
                    "hits": [hit.__dict__ for hit in response.hits],
                    "version": response.version,
                    "searched_partitions": response.searched_partitions,
                    "approximate_candidates": response.approximate_candidates,
                }
            except ValueError as error:
                raise HTTPException(status_code=422, detail=str(error)) from error
        finally:
            query_slots.release()

    @app.post("/search/binary", response_model=SearchResponseModel)
    def search_binary(
        payload: Annotated[bytes, Body(media_type="application/octet-stream")],
        request: Request,
        x_top_k: Annotated[int, Header(ge=1)] = 10,
        x_route_partitions: Annotated[int | None, Header(ge=1)] = None,
        x_candidate_multiplier: Annotated[int | None, Header(ge=1)] = None,
        x_scalar_filters: Annotated[str | None, Header()] = None,
    ):
        if x_top_k > top_k_limit:
            raise HTTPException(status_code=422, detail=f"top_k exceeds limit {top_k_limit}")
        vector = np.frombuffer(payload, dtype="<f4")
        if not query_slots.acquire(timeout=30):
            raise HTTPException(status_code=503, detail="read node is saturated")
        try:
            try:
                response = request.app.state.engine.search(
                    vector,
                    x_top_k,
                    route_partitions=x_route_partitions,
                    candidate_multiplier=x_candidate_multiplier,
                    scalar_filters=(
                        json.loads(x_scalar_filters) if x_scalar_filters else None
                    ),
                )
                return {
                    "hits": [hit.__dict__ for hit in response.hits],
                    "version": response.version,
                    "searched_partitions": response.searched_partitions,
                    "approximate_candidates": response.approximate_candidates,
                }
            except ValueError as error:
                raise HTTPException(status_code=422, detail=str(error)) from error
        finally:
            query_slots.release()

    @app.post("/admin/reload")
    def reload_index(
        request: Request,
        x_admin_token: Annotated[str | None, Header()] = None,
    ):
        if not admin_token or x_admin_token != admin_token:
            raise HTTPException(status_code=403, detail="invalid admin token")
        engine = request.app.state.engine
        return {"version": engine.reload()}

    return app


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True)
    args = parser.parse_args()
    config = load_service_config(args.config)
    if config.workers != 1:
        raise ValueError("start one configured process per NUMA/port; in-process uvicorn workers must be 1")
    app = create_app(
        config.index_root,
        config.cache_bytes,
        os.environ.get("PYRAMID_ADMIN_TOKEN"),
        config.top_k_limit,
        config.partition_workers,
        config.max_inflight_queries,
        config.mmap_indexes,
        config.require_native_scan,
        config.require_fastscan,
    )
    uvicorn.run(app, host=config.host, port=config.port, workers=config.workers)


if __name__ == "__main__":
    main()
