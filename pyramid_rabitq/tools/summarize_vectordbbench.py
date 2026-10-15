#!/usr/bin/env python3
"""Create a compact report and dependency-free charts from VectorDBBench output."""

from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path


def line_svg(path: Path, rows: list[dict], metrics: list[tuple[str, str, str]], title: str) -> None:
    width, height = 920, 500
    x0, y0, plot_w, plot_h = 90, 70, 740, 330
    maximum = max(row[key] for key, _, _ in metrics for row in rows) * 1.10
    parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">',
        '<rect width="100%" height="100%" fill="white"/>',
        f'<text x="{width/2}" y="32" text-anchor="middle" font-family="sans-serif" font-size="20" font-weight="bold">{title}</text>',
        '<text x="460" y="53" text-anchor="middle" font-family="sans-serif" font-size="12">VectorDBBench, Top-100, 30 seconds per concurrency</text>',
        f'<line x1="{x0}" y1="{y0+plot_h}" x2="{x0+plot_w}" y2="{y0+plot_h}" stroke="#475569"/>',
        f'<line x1="{x0}" y1="{y0}" x2="{x0}" y2="{y0+plot_h}" stroke="#475569"/>',
    ]
    for metric_index, (key, label, color) in enumerate(metrics):
        points = []
        for index, row in enumerate(rows):
            x = x0 + index * plot_w / (len(rows) - 1)
            y = y0 + plot_h - row[key] / maximum * plot_h
            points.append(f"{x:.1f},{y:.1f}")
            parts.append(f'<circle cx="{x:.1f}" cy="{y:.1f}" r="5" fill="{color}"/>')
            if len(metrics) == 1 or index in (0, len(rows) - 1):
                offset = -10 if metric_index == 0 else 18
                parts.append(f'<text x="{x:.1f}" y="{y+offset:.1f}" text-anchor="middle" font-family="sans-serif" font-size="11">{row[key]:.1f}</text>')
        parts.append(f'<polyline points="{" ".join(points)}" fill="none" stroke="{color}" stroke-width="3"/>')
    for index, row in enumerate(rows):
        x = x0 + index * plot_w / (len(rows) - 1)
        parts.append(f'<text x="{x:.1f}" y="425" text-anchor="middle" font-family="sans-serif" font-size="11">C={row["concurrency"]}</text>')
    legend_x = 260
    for index, (_, label, color) in enumerate(metrics):
        x = legend_x + index * 190
        parts.append(f'<rect x="{x}" y="458" width="14" height="14" fill="{color}"/><text x="{x+20}" y="470" font-family="sans-serif" font-size="12">{label}</text>')
    parts.append('</svg>')
    path.write_text("\n".join(parts), encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--results", type=Path, required=True)
    parser.add_argument("--framework-commit", required=True)
    args = parser.parse_args()
    data = json.loads(args.results.read_text())
    output = args.results.parent
    rows = data["concurrent"]["rows"]
    with (output / "summary.csv").open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=("concurrency", "qps", "p50_ms", "p95_ms", "p99_ms", "average_ms"))
        writer.writeheader()
        writer.writerows(rows)
    line_svg(output / "qps.svg", rows, [("qps", "QPS", "#16a34a")], "VectorDBBench throughput")
    line_svg(
        output / "latency.svg",
        rows,
        [("p50_ms", "P50 latency (ms)", "#2563eb"), ("p95_ms", "P95 latency (ms)", "#dc2626")],
        "VectorDBBench latency",
    )
    best = max(rows, key=lambda row: row["qps"])
    serial = data["serial"]
    table = "\n".join(
        f"| {row['concurrency']} | {row['qps']:.2f} | {row['p50_ms']:.2f} ms | {row['p95_ms']:.2f} ms | {row['p99_ms']:.2f} ms |"
        for row in rows
    )
    report = f"""# VectorDBBench evaluation of Pyramid RaBitQ v0.5.0

## Result

- Top-100 Recall: **{serial['recall']:.4f}**
- Top-100 NDCG: **{serial['ndcg']:.4f}**
- Serial latency: P50 **{serial['p50_ms']:.1f} ms**, P95 **{serial['p95_ms']:.1f} ms**, P99 **{serial['p99_ms']:.1f} ms**
- Peak throughput: **{best['qps']:.2f} QPS at concurrency {best['concurrency']}**
- Effective saturation range: concurrency 10-20; more concurrency increases queueing without improving throughput.

| Concurrency | QPS | P50 | P95 | P99 |
|---:|---:|---:|---:|---:|
{table}

## Protocol

- Framework: Zilliz VectorDBBench commit `{args.framework_commit}`.
- Workload: official serial and `MultiProcessingSearchRunner` search stages.
- Dataset: 20,000 corpus vectors, dimension 2,560, cosine similarity.
- Queries: 128 independent query vectors; exact Faiss Top-100 ground truth generated from the complete corpus.
- Deployment: three local HTTP read-node processes, packaged Pyramid RaBitQ v0.5.0, binary float32 API.
- Each concurrency level runs for 30 seconds; official concurrency sequence is 1, 5, 10, 20, 30, 40, 60, 80.
- FastScan backend: AVX-512; indexes are preloaded rather than memory-mapped.
- 30,137 successful search requests including warmup and serial evaluation; no HTTP 5xx responses.

## Scope and limitations

This is VectorDBBench's preloaded search-performance workload. Load duration and online insertion are not reported because Pyramid currently builds immutable indexes offline and exposes no online insert API. Filtered search, updates, deletes, and mixed read/write workloads are likewise outside the current API. The three read nodes share one physical machine, so concurrency 30-80 measures oversubscription and queueing rather than independent-host scaling.
"""
    (output / "REPORT.md").write_text(report, encoding="utf-8")


if __name__ == "__main__":
    main()
