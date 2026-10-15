#!/usr/bin/env python3
"""Summarize high-concurrency packaged v0.2.0 versus v0.5.0 results."""

from __future__ import annotations

import argparse
import csv
import json
import statistics
from pathlib import Path


COLORS = {"v0.2.0": "#64748b", "v0.5.0": "#16a34a"}
LABELS = {"v0.2.0": "Packaged v0.2.0", "v0.5.0": "Current v0.5.0"}


def median_metrics(trials: list[dict]) -> dict[str, float]:
    return {
        key: statistics.median(item[key] for item in trials)
        for key in ("qps", "mean_ms", "p50_ms", "p95_ms", "p99_ms")
    }


def percent(before: float, after: float) -> float:
    return (after / before - 1) * 100


def line_svg(path: Path, summary: dict, metric: str, title: str, lower_better: bool) -> None:
    levels = summary["concurrency"]
    width, height = 900, 480
    x0, y0, plot_w, plot_h = 90, 70, 720, 320
    maximum = max(summary[name][str(level)][metric] for name in COLORS for level in levels) * 1.12
    parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">',
        '<rect width="100%" height="100%" fill="white"/>',
        f'<text x="{width/2}" y="32" text-anchor="middle" font-family="sans-serif" font-size="20" font-weight="bold">{title}</text>',
        f'<text x="{width/2}" y="53" text-anchor="middle" font-family="sans-serif" font-size="12">{"Lower is better" if lower_better else "Higher is better"}; median of three repeats</text>',
        f'<line x1="{x0}" y1="{y0+plot_h}" x2="{x0+plot_w}" y2="{y0+plot_h}" stroke="#475569"/>',
        f'<line x1="{x0}" y1="{y0}" x2="{x0}" y2="{y0+plot_h}" stroke="#475569"/>',
    ]
    for name in COLORS:
        points = []
        for index, level in enumerate(levels):
            x = x0 + index * plot_w / (len(levels) - 1)
            value = summary[name][str(level)][metric]
            y = y0 + plot_h - value / maximum * plot_h
            points.append(f"{x:.1f},{y:.1f}")
            parts.append(f'<circle cx="{x:.1f}" cy="{y:.1f}" r="6" fill="{COLORS[name]}"/>')
            parts.append(f'<text x="{x:.1f}" y="{y-10:.1f}" text-anchor="middle" font-family="sans-serif" font-size="12">{value:.1f}</text>')
            parts.append(f'<text x="{x:.1f}" y="{y0+plot_h+23}" text-anchor="middle" font-family="sans-serif" font-size="12">C={level}</text>')
        parts.append(f'<polyline points="{" ".join(points)}" fill="none" stroke="{COLORS[name]}" stroke-width="4"/>')
    parts += [
        '<rect x="285" y="440" width="15" height="15" fill="#64748b"/><text x="307" y="453" font-family="sans-serif" font-size="12">Packaged v0.2.0</text>',
        '<rect x="475" y="440" width="15" height="15" fill="#16a34a"/><text x="497" y="453" font-family="sans-serif" font-size="12">Current v0.5.0</text>',
        '</svg>',
    ]
    path.write_text("\n".join(parts), encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--results", type=Path, required=True)
    args = parser.parse_args()
    raw = json.loads(args.results.read_text())
    output = args.results.parent
    levels = raw["protocol"]["concurrency"]
    summary: dict[str, object] = {"protocol": raw["protocol"], "concurrency": levels}
    for name in COLORS:
        summary[name] = {
            str(level): median_metrics(raw["results"][name]["trials"][str(level)])
            for level in levels
        }
    summary["changes_percent"] = {}
    for level in levels:
        old = summary["v0.2.0"][str(level)]
        new = summary["v0.5.0"][str(level)]
        summary["changes_percent"][str(level)] = {
            "qps": percent(old["qps"], new["qps"]),
            "p50_ms": percent(old["p50_ms"], new["p50_ms"]),
            "p95_ms": percent(old["p95_ms"], new["p95_ms"]),
            "p99_ms": percent(old["p99_ms"], new["p99_ms"]),
        }
    (output / "summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    with (output / "summary.csv").open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(["concurrency", "version", "qps", "p50_ms", "p95_ms", "p99_ms"])
        for level in levels:
            for name in COLORS:
                item = summary[name][str(level)]
                writer.writerow([level, name, item["qps"], item["p50_ms"], item["p95_ms"], item["p99_ms"]])

    line_svg(output / "qps.svg", summary, "qps", "High-concurrency throughput", False)
    line_svg(output / "p50_latency.svg", summary, "p50_ms", "High-concurrency P50 latency", True)
    line_svg(output / "p95_latency.svg", summary, "p95_ms", "High-concurrency P95 latency", True)

    table_rows = []
    for level in levels:
        old = summary["v0.2.0"][str(level)]
        new = summary["v0.5.0"][str(level)]
        delta = summary["changes_percent"][str(level)]
        table_rows.append(
            f"| {level} | {old['qps']:.2f} | {new['qps']:.2f} | {delta['qps']:+.1f}% | "
            f"{old['p50_ms']:.2f} ms | {new['p50_ms']:.2f} ms | {delta['p50_ms']:+.1f}% | "
            f"{old['p95_ms']:.2f} ms | {new['p95_ms']:.2f} ms | {delta['p95_ms']:+.1f}% |"
        )
    old_result = raw["results"]["v0.2.0"]
    new_result = raw["results"]["v0.5.0"]
    report = f"""# Packaged v0.2.0 versus current v0.5.0 high-concurrency benchmark

This is an end-to-end version/profile comparison. The baseline is served by the unmodified packaged v0.2.0 wheel with its historical routing/IVF profile and row-major native RaBitQ scanner. The current profile is served by the packaged v0.5.0 wheel with increased HNSW sampling, KaFFPa graph partitions, 256-512-vector IVF sizing, reduced probing/candidates, and AVX-512 FastScan. Both use the same 20,000 vectors and query set. A separate controlled row-versus-FastScan benchmark isolates scanning-layout impact.

Wheel provenance:

- v0.2.0: `{old_result['wheel']}` (`sha256:{old_result['wheel_sha256']}`)
- v0.5.0: `{new_result['wheel']}` (`sha256:{new_result['wheel_sha256']}`)

Quality over the 128-query benchmark pool: v0.2.0 Recall@10 = {old_result.get('recall_at_10', float('nan')):.4f}, v0.5.0 Recall@10 = {new_result.get('recall_at_10', float('nan')):.4f}; cross-version Top-10 agreement = {raw.get('top10_agreement', float('nan')):.4f}.

Protocol: three local read nodes, binary float32 HTTP, 128 rotating independent queries, 1,000 requests x 3 repeats per concurrency level, 200 warmups, 32 maximum inflight queries per node. Values below are repeat medians.

| Concurrency | v0.2 QPS | Current QPS | QPS change | v0.2 P50 | Current P50 | P50 change | v0.2 P95 | Current P95 | P95 change |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
""" + "\n".join(table_rows) + """

This single-machine test intentionally drives three read-node processes into saturation. At high concurrency, queueing dominates tail latency; production tail-latency limits should be validated again on separate physical nodes.
"""
    (output / "REPORT.md").write_text(report, encoding="utf-8")


if __name__ == "__main__":
    main()
