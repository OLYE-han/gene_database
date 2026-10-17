"""Reproducible geometry experiments. Results do not measure ANN recall/QPS."""

from datetime import datetime, timezone
from pathlib import Path
import hashlib
import platform
import time

import numpy as np

from .data import Dataset, load_complete_npy, sample_ids, write_json
from .partition import calibrate, fit, memberships, prepare, squared_distances, summary


def run_partition_study(root, output, *, shards=3, train_rows=2048, calibration_rows=2048,
                        evaluation_rows=4096, seed=42, metrics=("l2", "cosine"),
                        rhos=(0, .05, .1, .2, .3), max_copies=3, iterations=30,
                        tolerance=1e-6):
    if min(train_rows, calibration_rows, evaluation_rows) < 1 or max_copies < 2:
        raise ValueError("Positive sample counts and at least two allowed copies required")
    dataset = Dataset(root)
    output = Path(output)
    if output.resolve().is_relative_to(dataset.root):
        raise ValueError("Experiment output must not be inside source dataset")
    output.mkdir(parents=True, exist_ok=False)
    query_path = dataset.root / "queries200K/query_ids.npy"
    excluded = load_complete_npy(query_path) if query_path.exists() else np.array([], dtype=np.int64)
    total = train_rows + calibration_rows + evaluation_rows
    pool = sample_ids(dataset.rows, min(dataset.rows, total + len(excluded)), seed)
    pool = pool[~np.isin(pool, excluded)]
    if len(pool) < total:
        raise ValueError("Not enough base rows after excluding official queries")
    np.random.default_rng(seed).shuffle(pool)
    chosen = pool[:total]
    train_ids, cal_ids, eval_ids = np.split(chosen, [train_rows, train_rows + calibration_rows])
    np.savez(output / "sample_ids.npz", train=train_ids, calibration=cal_ids, evaluation=eval_ids)
    train, calibration, evaluation = [dataset.take(ids) for ids in (train_ids, cal_ids, eval_ids)]
    report = {
        "experiment": "E03-E05 geometry pilot", "status": "exploratory_not_ann_benchmark",
        "created_utc": datetime.now(timezone.utc).isoformat(), "dataset": dataset.identity(),
        "python": platform.python_version(), "numpy": np.__version__, "seed": seed,
        "config": {"shards": shards, "train_rows": train_rows, "calibration_rows": calibration_rows,
                   "evaluation_rows": evaluation_rows, "metrics": list(metrics), "rhos": list(rhos),
                   "max_copies": max_copies, "iterations": iterations, "tolerance": tolerance},
        "official_query_ids_excluded": int(len(excluded)),
        "sample_ids_sha256": hashlib.sha256(chosen.astype("<u8").tobytes()).hexdigest(),
        "code_sha256": {p.name: hashlib.sha256(p.read_bytes()).hexdigest()
                        for p in sorted(Path(__file__).parent.glob("*.py"))},
        "results": [],
        "limitations": ["No graph built; no ANN recall, RPC, SSD latency or QPS measured",
                        "One sample/seed; no method-selection conclusion",
                        "All methods share training centers; RF is reported, not forcibly matched"],
    }
    for metric in metrics:
        started = time.perf_counter()
        model = fit(train, shards, metric, seed, iterations, tolerance)
        model.save(output / f"model_{metric}.npz")
        common = {"metric": metric, "training_seconds": time.perf_counter() - started,
                  "iterations": model.iterations, "converged": model.converged}
        primary, _ = model.scores(evaluation)
        unique = np.zeros((len(evaluation), shards), dtype=bool)
        unique[np.arange(len(evaluation)), primary] = True
        report["results"].append({**common, "method": "unique", **summary(unique, primary)})
        distances = squared_distances(prepare(evaluation, metric), model.centers)
        fixed = unique.copy()
        nearest = np.argsort(distances, axis=1, kind="stable")[:, :min(2, shards)]
        fixed[np.arange(len(evaluation))[:, None], nearest] = True
        report["results"].append({**common, "method": "fixed_two", **summary(fixed, primary)})
        for method in ("relative_gap", "boundary", "margin"):
            _, cal_scores = model.scores(calibration, method)
            primary, scores = model.scores(evaluation, method)
            for rho in rhos:
                threshold = calibrate(cal_scores, cal_ids, rho, seed)
                selected = memberships(primary, scores, eval_ids, threshold, max_copies)
                threshold_name = f"threshold_{metric}_{method}_{rho:g}.json"
                write_json(output / threshold_name, {
                    "version": 1, "metric": metric, "method": method,
                    "model_sha256": hashlib.sha256((output / f"model_{metric}.npz").read_bytes()).hexdigest(),
                    "rho_requested": rho, "threshold": threshold.to_dict(),
                })
                report["results"].append({**common, "method": method, "rho_requested": rho,
                                          "threshold": threshold.to_dict(), "threshold_file": threshold_name,
                                          **summary(selected, primary)})
        dataset.assert_unchanged()
    write_json(output / "report.json", report)
    return report
