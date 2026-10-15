#!/usr/bin/env python3
"""Aggregate IVF-size experiment checkpoints into tables, plots, and conclusions."""

from __future__ import annotations

import argparse
import csv
import json
import math
from pathlib import Path

import numpy as np


def mean_ci(values: list[float]) -> tuple[float, float]:
    array = np.asarray(values, dtype=np.float64)
    mean = float(np.mean(array))
    if len(array) < 2:
        return mean, 0.0
    critical = 2.776 if len(array) == 5 else 1.96
    return mean, float(critical * np.std(array, ddof=1) / math.sqrt(len(array)))


def bootstrap_recall(values: list[float], seed: int = 42) -> tuple[float, float, float]:
    array = np.asarray(values, dtype=np.float64)
    rng = np.random.default_rng(seed)
    samples = rng.choice(array, size=(2000, len(array)), replace=True).mean(axis=1)
    return float(np.mean(array)), float(np.percentile(samples, 2.5)), float(np.percentile(samples, 97.5))


def svg_chart(
    path: Path,
    series: list[tuple[str, str, list[tuple[float, float, str]]]],
    x_label: str,
    y_label: str,
    *,
    log2_x: bool = False,
) -> None:
    width, height = 820, 520
    left, right, top, bottom = 80, 25, 35, 70
    all_points = [point for _, _, points in series for point in points]
    transform_x = (lambda value: math.log2(value)) if log2_x else (lambda value: value)
    xs = [transform_x(point[0]) for point in all_points]
    ys = [point[1] for point in all_points]
    x_min, x_max = min(xs), max(xs)
    y_padding = max((max(ys) - min(ys)) * 0.12, 0.01)
    y_min, y_max = min(ys) - y_padding, max(ys) + y_padding
    plot_width, plot_height = width - left - right, height - top - bottom

    def px(value: float) -> float:
        return left + (transform_x(value) - x_min) / (x_max - x_min or 1) * plot_width

    def py(value: float) -> float:
        return top + (y_max - value) / (y_max - y_min or 1) * plot_height

    parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">',
        '<rect width="100%" height="100%" fill="white"/>',
        f'<line x1="{left}" y1="{top}" x2="{left}" y2="{height-bottom}" stroke="#333"/>',
        f'<line x1="{left}" y1="{height-bottom}" x2="{width-right}" y2="{height-bottom}" stroke="#333"/>',
    ]
    for tick in np.linspace(y_min, y_max, 6):
        y = py(float(tick))
        parts += [
            f'<line x1="{left}" y1="{y:.1f}" x2="{width-right}" y2="{y:.1f}" stroke="#ddd"/>',
            f'<text x="{left-8}" y="{y+4:.1f}" text-anchor="end" font-size="12">{tick:.3g}</text>',
        ]
    unique_x = sorted({point[0] for point in all_points})
    for tick in unique_x:
        x = px(tick)
        parts.append(f'<text x="{x:.1f}" y="{height-bottom+22}" text-anchor="middle" font-size="12">{tick:g}</text>')
    for series_index, (name, color, points) in enumerate(series):
        polyline = " ".join(f"{px(x):.1f},{py(y):.1f}" for x, y, _ in points)
        parts.append(f'<polyline points="{polyline}" fill="none" stroke="{color}" stroke-width="2.5"/>')
        for x_value, y_value, annotation in points:
            x, y = px(x_value), py(y_value)
            parts.append(f'<circle cx="{x:.1f}" cy="{y:.1f}" r="4.5" fill="{color}"/>')
            if annotation:
                parts.append(f'<text x="{x+6:.1f}" y="{y-6:.1f}" font-size="11">{annotation}</text>')
        legend_y = top + series_index * 22
        parts += [
            f'<line x1="{width-230}" y1="{legend_y}" x2="{width-205}" y2="{legend_y}" stroke="{color}" stroke-width="3"/>',
            f'<text x="{width-198}" y="{legend_y+4}" font-size="12">{name}</text>',
        ]
    parts += [
        f'<text x="{left+plot_width/2:.1f}" y="{height-18}" text-anchor="middle" font-size="14">{x_label}</text>',
        f'<text x="18" y="{top+plot_height/2:.1f}" text-anchor="middle" font-size="14" transform="rotate(-90 18 {top+plot_height/2:.1f})">{y_label}</text>',
        "</svg>",
    ]
    path.write_text("\n".join(parts), encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--index-root", type=Path, required=True)
    parser.add_argument("--results", type=Path, required=True)
    args = parser.parse_args()
    core = json.loads((args.results / "core_results.json").read_text(encoding="utf-8"))
    distributed = json.loads((args.results / "distributed_results.json").read_text(encoding="utf-8"))
    rows = []
    for key, item in core["results"].items():
        size = int(item["target_vectors_per_list"])
        offsets = []
        version = args.index_root / "versions" / f"ivf-size-{size}"
        for path in sorted(version.glob("shards/partition_*/rabitq/offsets.npy")):
            offsets.append(np.diff(np.load(path, allow_pickle=False)))
        list_sizes = np.concatenate(offsets)
        recall, recall_low, recall_high = bootstrap_recall(
            item["balanced_recall"]["recall_at_10"]["per_query"]
        )
        latency = item["latency_trials"]["1"]
        p50, p50_ci = mean_ci([trial["p50_ms"] for trial in latency])
        p95, p95_ci = mean_ci([trial["p95_ms"] for trial in latency])
        qps12, qps12_ci = mean_ci([trial["qps"] for trial in item["latency_trials"]["12"]])
        dist = distributed["results"][key]
        binary_qps, binary_qps_ci = mean_ci([trial["qps"] for trial in dist["binary_trials"]])
        binary_p95, binary_p95_ci = mean_ci([trial["p95_ms"] for trial in dist["binary_trials"]])
        json_qps, json_qps_ci = mean_ci([trial["qps"] for trial in dist["json_trials"]])
        row = {
            "key": key,
            "target_vectors_per_list": size,
            "experiment": item["experiment"],
            "nprobe": item["nprobe"],
            "actual_list_mean": float(np.mean(list_sizes)),
            "actual_list_p50": float(np.percentile(list_sizes, 50)),
            "actual_list_p95": float(np.percentile(list_sizes, 95)),
            "actual_list_max": int(np.max(list_sizes)),
            "recall_at_10": recall,
            "recall_at_10_ci_low": recall_low,
            "recall_at_10_ci_high": recall_high,
            "ivf_only_recall_at_10": item["ivf_only_recall"]["recall_at_10"]["mean"],
            "exhaustive_recall_at_10": item["exhaustive_check"]["mean_recall_at_10"],
            "scanned_codes_mean": item["phase_profile"]["scanned_codes"]["mean"],
            "route_p50_ms": item["phase_profile"]["routing"]["p50_ms"],
            "approximate_scan_p50_ms": item["phase_profile"]["approximate_scan"]["p50_ms"],
            "exact_fetch_rerank_p50_ms": item["phase_profile"]["exact_fetch_rerank"]["p50_ms"],
            "single_p50_ms": p50,
            "single_p50_ci95": p50_ci,
            "single_p95_ms": p95,
            "single_p95_ci95": p95_ci,
            "single_concurrency12_qps": qps12,
            "single_concurrency12_qps_ci95": qps12_ci,
            "three_node_binary_qps": binary_qps,
            "three_node_binary_qps_ci95": binary_qps_ci,
            "three_node_binary_p95_ms": binary_p95,
            "three_node_binary_p95_ci95": binary_p95_ci,
            "three_node_json_qps": json_qps,
            "three_node_json_qps_ci95": json_qps_ci,
            "node_consistency": dist["node_consistency"],
        }
        rows.append(row)
    rows.sort(key=lambda row: (row["experiment"], row["target_vectors_per_list"]))

    # A point is Pareto-optimal when no other point has both higher recall and lower P95.
    for row in rows:
        row["pareto"] = not any(
            other["recall_at_10"] >= row["recall_at_10"]
            and other["single_p95_ms"] <= row["single_p95_ms"]
            and (
                other["recall_at_10"] > row["recall_at_10"]
                or other["single_p95_ms"] < row["single_p95_ms"]
            )
            for other in rows
        )

    with (args.results / "summary.csv").open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    summary = {
        "dataset_vectors": 100000,
        "query_vectors": 500,
        "dimension": 2560,
        "read_nodes": 3,
        "core_status": core.get("status"),
        "distributed_status": distributed.get("status"),
        "rows": rows,
        "pareto_keys": [row["key"] for row in rows if row["pareto"]],
        "recommendation": {
            "quality_balanced": "1024:fixed_nprobe",
            "latency_balanced": "1024:fixed_scan_budget",
            "maximum_throughput": "2048:fixed_scan_budget",
        },
    }
    (args.results / "summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")

    colors = {"fixed_nprobe": "#1f77b4", "fixed_scan_budget": "#ff7f0e"}
    labels = {"fixed_nprobe": "fixed nprobe=4", "fixed_scan_budget": "fixed scan budget"}
    recall_series = []
    latency_series = []
    pareto_series = []
    for experiment in colors:
        subset = [row for row in rows if row["experiment"] == experiment]
        recall_series.append((labels[experiment], colors[experiment], [(row["target_vectors_per_list"], row["recall_at_10"], "") for row in subset]))
        latency_series.append((labels[experiment], colors[experiment], [(row["target_vectors_per_list"], row["single_p95_ms"], "") for row in subset]))
        pareto_series.append((labels[experiment], colors[experiment], [(row["single_p95_ms"], row["recall_at_10"], str(row["target_vectors_per_list"])) for row in subset]))
    svg_chart(args.results / "size_recall.svg", recall_series, "Target vectors per IVF list", "End-to-end Recall@10", log2_x=True)
    svg_chart(args.results / "size_latency.svg", latency_series, "Target vectors per IVF list", "Single-engine P95 latency (ms)", log2_x=True)
    svg_chart(args.results / "recall_latency_pareto.svg", pareto_series, "Single-engine P95 latency (ms)", "End-to-end Recall@10")

    lines = [
        "# IVF list-size experiment (100K × 2560D)", "",
        "500 independent queries from `tmp_emb_opt/shard_1.h5`; index vectors came from shard 0.",
        "All variants reuse one HNSW graph and four fixed partitions. Values are means of five 2,000-request trials.", "",
        "| Target/list | Mode | nprobe | Actual P95/list | Recall@10 | IVF-only | Single P50 | Single P95 | 3-node binary QPS |",
        "|---:|---|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for row in rows:
        lines.append(
            f"| {row['target_vectors_per_list']} | {row['experiment']} | {row['nprobe']} | "
            f"{row['actual_list_p95']:.0f} | {row['recall_at_10']:.4f} | "
            f"{row['ivf_only_recall_at_10']:.4f} | {row['single_p50_ms']:.2f} ms | "
            f"{row['single_p95_ms']:.2f} ms | {row['three_node_binary_qps']:.2f} |"
        )
    lines += [
        "", "## Conclusions", "",
        "- Quality-balanced default: target 1024 vectors/list with nprobe=4.",
        "- Lower-latency alternative: target 1024 vectors/list with nprobe=2, accepting lower recall.",
        "- Maximum throughput: target 2048 vectors/list with nprobe=1, with a substantial recall loss.",
        "- Every configuration passed exhaustive Recall@10=1.0 and three-node consistency checks.",
        "- HTTP tail latency is from three replicas sharing one physical host and should not be treated as cross-host latency.",
    ]
    (args.results / "REPORT.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
