from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import sys
import zipfile

import numpy as np

from .data import Dataset, fingerprint, load_complete_npy, sample_ids, write_json
from .experiments import run_partition_study
from .partition import Model, Threshold, memberships


def verify_distance(query, vector, labeled, metric):
    """Compare L2 in squared units to account for cancellation near zero."""
    qnorm, vnorm = np.linalg.norm(query), np.linalg.norm(vector)
    if not np.isfinite(labeled) or labeled < -1e-12:
        raise ValueError("Invalid label distance")
    if metric == "euclidean":
        delta = query - vector
        squared = float(np.dot(delta, delta))
        actual = float(np.sqrt(squared))
        eps = np.finfo(np.float64).eps
        gamma = len(query) * eps / (1 - len(query) * eps)
        # Label generator uses ||q||²+||v||²-2q.v in double precision.
        bound = max(1e-12, 4 * gamma * (qnorm + vnorm)**2)
        valid = abs(labeled*labeled - squared) <= bound
    else:
        if qnorm == 0 or vnorm == 0:
            raise ValueError("Zero vector in cosine labels")
        actual = float(1 - np.dot(query, vector) / (qnorm * vnorm))
        valid = np.isclose(actual, labeled, rtol=1e-8, atol=1e-10)
    if not valid:
        raise ValueError(f"Label distance does not match vectors: {metric}, {actual} vs {labeled}")
    return actual


def _member_prefix(archive, name, rows=None):
    with archive.open(name + ".npy") as stream:
        version = np.lib.format.read_magic(stream)
        if version == (1, 0):
            shape, fortran, dtype = np.lib.format.read_array_header_1_0(stream)
        elif version == (2, 0):
            shape, fortran, dtype = np.lib.format.read_array_header_2_0(stream)
        else:
            raise ValueError("Unsupported label NPY version")
        if fortran or dtype.hasobject:
            raise ValueError("Labels must be numeric/string row-major arrays")
        if rows is not None and shape:
            shape = (min(shape[0], rows), *shape[1:])
        count = int(np.prod(shape))
        raw = stream.read(count * dtype.itemsize)
        if len(raw) != count * dtype.itemsize:
            raise ValueError("Incomplete label member")
        return np.frombuffer(raw, dtype=dtype).reshape(shape)


def inspect(root, count=64, seed=42):
    dataset = Dataset(root)
    if count < 1:
        raise ValueError("Inspection row count must be positive")
    chosen = sample_ids(dataset.rows, min(count, dataset.rows), seed)
    x = dataset.take(chosen).astype(np.float64)
    norms = np.linalg.norm(x, axis=1)
    report = {"dataset": dataset.identity(), "sample_rows": len(chosen), "seed": seed,
              "norm_min": float(norms.min()), "norm_max": float(norms.max()),
              "zero_rows": int(np.sum(norms == 0)), "all_sampled_coordinates_finite": True,
              "checks_are_sampled": True, "ground_truth_membership_verified": False}
    qroot = dataset.root / "queries200K"
    if qroot.exists():
        paths = [qroot / "query_ids.npy", qroot / "queries.npy"]
        snapshots = {p: fingerprint(p) for p in paths}
        qids, queries = [load_complete_npy(p) for p in paths]
        if (qids.ndim != 1 or qids.dtype.kind not in "iu" or len(qids) == 0
                or np.any(qids < 0) or np.any(qids >= dataset.rows)
                or np.any(qids[1:] <= qids[:-1])
                or queries.shape != (len(qids), dataset.dimension) or queries.dtype != np.dtype("<f4")):
            raise ValueError("Invalid query shape/IDs/dtype/order")
        qsample = sample_ids(len(qids), min(count, len(qids)), seed)
        if not np.array_equal(dataset.take(qids[qsample]), queries[qsample]):
            raise ValueError("Query vectors do not match their base IDs")
        digest = hashlib.sha256(qids.astype("<i8").tobytes()).hexdigest()
        config = json.loads((qroot / "query_cache_config.json").read_text())
        if digest != config["query_ids_sha256"]:
            raise ValueError("Query ID checksum mismatch")
        report["queries"] = {"rows": len(qids), "ids_sha256": digest, "sample_matches_base": True}
        report["labels"] = {}
        for metric in ("cosine", "euclidean"):
            files = sorted((qroot / metric / "knn").glob("*.npz"))
            if len(files) != 1:
                raise ValueError("Inspection currently expects one full query-label archive per metric")
            snapshots[files[0]] = fingerprint(files[0])
            with zipfile.ZipFile(files[0]) as archive:
                if (str(_member_prefix(archive, "metric")) != metric
                        or int(_member_prefix(archive, "query_start")) != 0
                        or int(_member_prefix(archive, "query_end")) != len(qids)
                        or not np.array_equal(_member_prefix(archive, "query_ids"), qids)):
                    raise ValueError("Label metric/query range/IDs mismatch")
                neighbors = _member_prefix(archive, "ids", count)
                distances = _member_prefix(archive, "distances", count)
            if (neighbors.ndim != 2 or distances.shape != neighbors.shape or neighbors.shape[1] == 0
                    or neighbors.dtype.kind not in "iu" or np.any(neighbors < 0)
                    or np.any(neighbors >= dataset.rows) or not np.isfinite(distances).all()
                    or np.any(np.diff(distances, axis=1) < 0)
                    or np.any(neighbors == qids[:len(neighbors), None])
                    or any(len(np.unique(row)) != len(row) for row in neighbors)):
                raise ValueError("Invalid sampled ground-truth labels")
            errors = []
            identical_nonzero_labels = 0
            for i, target in enumerate(neighbors[:, 0]):
                query = np.array(queries[i], dtype=float)
                vector = dataset.take(np.array([target], dtype=np.uint64))[0].astype(float)
                actual = verify_distance(query, vector, float(distances[i, 0]), metric)
                identical_nonzero_labels += int(np.array_equal(query, vector) and distances[i, 0] != 0)
                errors.append(abs(float(actual - distances[i, 0])))
            report["labels"][metric] = {"checked_prefix_rows": len(neighbors),
                                         "k": neighbors.shape[1], "recomputed_pairs": len(errors),
                                         "max_absolute_distance_error": max(errors),
                                         "identical_vector_nonzero_labels": identical_nonzero_labels}
        if any(before != fingerprint(p) for p, before in snapshots.items()):
            raise ValueError("Query or label files changed during inspection")
    dataset.assert_unchanged()
    return report


def assign(root, model_path, threshold_path, output, method, max_copies, block_rows,
           expected_metric=None, expected_rho=None, expected_shards=None):
    """Stream assignment records only. This does not shuffle or copy vectors."""
    dataset = Dataset(root)
    model = Model.load(model_path)
    model_sha = hashlib.sha256(Path(model_path).read_bytes()).hexdigest()
    stored = json.loads(Path(threshold_path).read_text())
    if ((expected_metric is not None and model.metric != expected_metric)
            or (expected_rho is not None and stored.get("rho_requested") != expected_rho)
            or (expected_shards is not None and len(model.centers) != expected_shards)):
        raise ValueError("Assignment model/threshold differs from configured metric, rho or shard count")
    if (stored["version"] != 1 or stored["model_sha256"] != model_sha
            or stored["metric"] != model.metric or stored["method"] != method):
        raise ValueError("Threshold model/metric/method mismatch")
    threshold = Threshold(**stored["threshold"])
    output = Path(output)
    if output.resolve().is_relative_to(dataset.root):
        raise ValueError("Assignment output must not be inside source dataset")
    output.mkdir(parents=True, exist_ok=False)
    files = []
    primary_counts = np.zeros(len(model.centers), dtype=np.int64)
    replica_counts = primary_counts.copy()
    overlapped = 0
    for start, vectors in dataset.blocks(block_rows):
        ids = np.arange(start, start + len(vectors), dtype=np.uint64)
        primary, scores = model.scores(vectors, method)
        selected = memberships(primary, scores, ids, threshold, max_copies)
        name = f"{start:020d}.npz"
        np.savez(output / name, global_ids=ids, primary=primary.astype(np.uint32), membership=selected)
        primary_counts += np.bincount(primary, minlength=len(model.centers))
        replica_counts += selected.sum(axis=0)
        overlapped += int(np.sum(selected.sum(axis=1) > 1))
        files.append({"file": name, "start": start, "end": start + len(vectors)})
    dataset.assert_unchanged()
    manifest = {"version": 1, "kind": "assignment_plan_not_shuffled_index", "dataset": dataset.identity(),
                "metric": model.metric, "method": method, "threshold": threshold.to_dict(),
                "max_copies": max_copies, "primary_counts": primary_counts.tolist(),
                "replica_counts": replica_counts.tolist(), "parts": files,
                "replication_factor": float(replica_counts.sum() / dataset.rows),
                "overlap_fraction_observed": overlapped / dataset.rows,
                "model_sha256": model_sha}
    write_json(output / "manifest.json", manifest)
    return manifest


def main(argv=None):
    from datetime import datetime, timezone
    import uuid
    import yaml
    from threadpoolctl import threadpool_limits
    from .config import load_config, snapshot

    parser = argparse.ArgumentParser(description="DHDGS: all runtime parameters come from one YAML")
    commands = parser.add_subparsers(dest="command", required=True)
    for name in ("validate-config", "inspect", "partition-study", "assign", "build", "query", "benchmark", "serve", "local-cluster-test"):
        command = commands.add_parser(name)
        command.add_argument("--config", type=Path, required=True)
        if name == "serve":
            command.add_argument("--node-id", type=int, required=True, help="Role selection, not a tuning override")
    args = parser.parse_args(argv)
    try:
        c = load_config(args.config)
        output = Path(c["paths"]["output"])
        with threadpool_limits(limits=c["runtime"]["blas_threads"]):
            if args.command == "validate-config":
                report = {"status": "valid", "config_sha256": c["_config_sha256"],
                          "output": str(output)}
            elif args.command == "serve":
                from .transport import create_server
                server = create_server(c, args.node_id)
                print(json.dumps({"status": "listening", "node_id": args.node_id,
                                  "endpoint": server.server_address}), flush=True)
                try:
                    server.serve_forever()
                except KeyboardInterrupt:
                    pass
                finally:
                    server.server_close()
                return
            elif args.command == "build":
                from .index import build
                built = build(c)
                report = {"status": "built", "output": str(output/"index"), "rows": built["rows"],
                          "partition": built["partition"], "build_seconds": built["build_seconds"]}
            elif args.command == "partition-study":
                root = output/"partition"
                root.mkdir(parents=True, exist_ok=False)
                snapshot(c, root)
                e, p = c["experiment"], c["partition"]
                for seed in e["seeds"]:
                    run_partition_study(c["paths"]["dataset"], root/f"seed_{seed}",
                        shards=len(c["cluster"]["nodes"]), train_rows=e["train_rows"],
                        calibration_rows=e["calibration_rows"], evaluation_rows=e["evaluation_rows"],
                        seed=seed, metrics=e["metrics"], rhos=e["overlap_fractions"],
                        max_copies=p["max_copies"], iterations=p["kmeans_iterations"],
                        tolerance=p["kmeans_tolerance"])
                report = {"status": "completed", "output": str(root)}
            elif args.command == "assign":
                root = output/"assignments"
                p = c["partition"]
                assigned = assign(c["paths"]["dataset"], c["assignment"]["model"],
                    c["assignment"]["threshold"], root, p["method"], p["max_copies"], c["data"]["block_rows"],
                    c["data"]["metric"], p["overlap_fraction"], len(c["cluster"]["nodes"]))
                snapshot(c, root)
                report = {"status": "completed", "output": str(root), "replication_factor": assigned["replication_factor"]}
            else:
                stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
                root = output/"runs"/f"{args.command}_{stamp}_{uuid.uuid4().hex[:8]}"
                snapshot(c, root)
                if args.command == "inspect":
                    report = inspect(c["paths"]["dataset"], c["data"]["inspection_rows"], c["seed"])
                elif args.command == "local-cluster-test":
                    from .cluster_test import run_local_cluster
                    report = run_local_cluster(c,root)
                elif args.command == "benchmark":
                    from .benchmark import run_benchmark
                    report = run_benchmark(c)
                else:
                    from .benchmark import query_input
                    from .search import Engine
                    vectors, ids, snapshots = query_input(c, 1)
                    engine = Engine(c)
                    try:
                        report = engine.search(vectors[0], None if ids is None else int(ids[0]))
                    finally:
                        engine.close()
                    if any(fingerprint(p) != before for p, before in snapshots.items()):
                        raise ValueError("Query files changed during search")
                write_json(root/"report.json", report)
                if args.command in ("benchmark", "local-cluster-test"):
                    from .metrics import export_query_csv, export_resource_csvs
                    export_query_csv(root/"queries.csv",report["queries"])
                    export_resource_csvs(root,report)
                    with (root/"queries.jsonl").open("x") as handle:
                        for row in report["queries"]:
                            handle.write(json.dumps(row,ensure_ascii=False,allow_nan=False)+"\n")
                    report = {key:report[key] for key in ("kind","query_count","indexed_rows","concurrency",
                        "qps","successful_queries","failed_queries","recall_at_k","latency_ms","build_seconds")}
                report = {"output": str(root), **report}
    except (ValueError, RuntimeError, OSError, KeyError, zipfile.BadZipFile, yaml.YAMLError) as exc:
        parser.exit(2, f"dhdgs: {exc}\n")
    print(json.dumps(report, ensure_ascii=False, allow_nan=False))


if __name__ == "__main__":
    main()
