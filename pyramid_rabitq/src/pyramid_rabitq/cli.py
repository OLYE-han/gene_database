from __future__ import annotations

import argparse
import json
from pathlib import Path

import h5py
import numpy as np

from .builder import PyramidBuilder
from .config import load_build_config
from .engine import ReadNodeEngine
from .evaluation import evaluate_recall
from .storage import H5Corpus


def _query_from_args(args, dimension: int) -> np.ndarray:
    if args.query_npy:
        value = np.load(args.query_npy, allow_pickle=False)
    elif args.query_h5:
        with h5py.File(args.query_h5, "r") as handle:
            value = handle["embeddings"][args.row]
    else:
        raise ValueError("one of --query-npy or --query-h5 is required")
    value = np.asarray(value, dtype=np.float32).reshape(-1)
    if value.shape != (dimension,):
        raise ValueError(f"query dimension {value.size}, expected {dimension}")
    return value


def main() -> None:
    parser = argparse.ArgumentParser(prog="pyramid-rabitq")
    subparsers = parser.add_subparsers(dest="command", required=True)

    inspect_parser = subparsers.add_parser("inspect", help="validate and summarize source HDF5")
    inspect_parser.add_argument("--source", required=True)
    inspect_parser.add_argument("--dimension", type=int, default=2560)

    build_parser = subparsers.add_parser("build", help="build an immutable index version")
    build_parser.add_argument("--source", required=True)
    build_parser.add_argument("--index-root", required=True)
    build_parser.add_argument("--version", required=True)
    build_parser.add_argument("--config", required=True)

    search_parser = subparsers.add_parser("search", help="query a local read-node index")
    search_parser.add_argument("--index-root", required=True)
    query_group = search_parser.add_mutually_exclusive_group(required=True)
    query_group.add_argument("--query-npy")
    query_group.add_argument("--query-h5")
    search_parser.add_argument("--row", type=int, default=0)
    search_parser.add_argument("--top-k", type=int, default=10)
    search_parser.add_argument("--route-partitions", type=int)
    search_parser.add_argument("--candidate-multiplier", type=int)
    search_parser.add_argument("--cache-bytes", type=int, default=1024**3)

    validate_parser = subparsers.add_parser("validate", help="compare the index with streaming exact search")
    validate_parser.add_argument("--source", required=True)
    validate_parser.add_argument("--index-root", required=True)
    validate_parser.add_argument("--queries", type=int, default=100)
    validate_parser.add_argument("--top-k", type=int, default=10)
    validate_parser.add_argument("--batch-size", type=int, default=4096)
    validate_parser.add_argument("--output")

    args = parser.parse_args()
    if args.command == "inspect":
        corpus = H5Corpus(args.source, args.dimension)
        print(json.dumps({"files": len(corpus.shards), "rows": corpus.total_rows, "dimension": corpus.dimension}, indent=2))
    elif args.command == "build":
        config = load_build_config(args.config)
        corpus = H5Corpus(args.source, config.dimension)
        output = PyramidBuilder(corpus, config).build(args.index_root, args.version)
        print(output)
    elif args.command == "search":
        engine = ReadNodeEngine(args.index_root, args.cache_bytes)
        query = _query_from_args(args, engine.config.dimension)
        response = engine.search(
            query,
            args.top_k,
            route_partitions=args.route_partitions,
            candidate_multiplier=args.candidate_multiplier,
        )
        print(json.dumps({
            "version": response.version,
            "searched_partitions": response.searched_partitions,
            "approximate_candidates": response.approximate_candidates,
            "hits": [hit.__dict__ for hit in response.hits],
        }, indent=2))
    elif args.command == "validate":
        engine = ReadNodeEngine(args.index_root)
        corpus = H5Corpus(args.source, engine.config.dimension)
        report = evaluate_recall(engine, corpus, args.queries, args.top_k, args.batch_size)
        rendered = json.dumps(report.as_dict(), indent=2)
        if args.output:
            Path(args.output).write_text(rendered + "\n", encoding="utf-8")
        print(rendered)


if __name__ == "__main__":
    main()
