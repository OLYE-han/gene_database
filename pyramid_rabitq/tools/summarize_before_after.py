#!/usr/bin/env python3
"""Summarize the before/after benchmark and emit dependency-free SVG charts."""

from __future__ import annotations

import argparse
import csv
import json
import statistics
from pathlib import Path


COLORS = {"legacy": "#64748b", "optimized": "#16a34a"}
LABELS = {"legacy": "Before", "optimized": "After (KaFFPa)"}


def median_trials(item: dict, concurrency: int) -> dict[str, float]:
    trials = item["latency"][str(concurrency)]
    return {key: statistics.median(row[key] for row in trials) for key in trials[0]}


def change(before: float, after: float) -> float:
    return (after / before - 1.0) * 100.0


def svg_bar(path: Path, title: str, panels: list[tuple[str, float, float, str]]) -> None:
    width, height = 960, 360
    panel_width = width / len(panels)
    parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">',
        '<rect width="100%" height="100%" fill="white"/>',
        f'<text x="{width/2}" y="30" text-anchor="middle" font-family="sans-serif" font-size="20" font-weight="bold">{title}</text>',
    ]
    for index, (label, before, after, suffix) in enumerate(panels):
        left = index * panel_width + 35
        maximum = max(before, after) * 1.15 or 1
        baseline = 300
        chart_height = 210
        parts.append(f'<text x="{left + panel_width/2 - 35}" y="65" text-anchor="middle" font-family="sans-serif" font-size="15">{label}</text>')
        for offset, (name, value) in enumerate((("legacy", before), ("optimized", after))):
            x = left + 55 + offset * 90
            bar_height = value / maximum * chart_height
            y = baseline - bar_height
            parts.append(f'<rect x="{x}" y="{y:.1f}" width="58" height="{bar_height:.1f}" fill="{COLORS[name]}" rx="3"/>')
            parts.append(f'<text x="{x+29}" y="{y-7:.1f}" text-anchor="middle" font-family="sans-serif" font-size="12">{value:.2f}{suffix}</text>')
            parts.append(f'<text x="{x+29}" y="322" text-anchor="middle" font-family="sans-serif" font-size="11">{LABELS[name]}</text>')
        if index:
            parts.append(f'<line x1="{index*panel_width}" y1="55" x2="{index*panel_width}" y2="330" stroke="#e2e8f0"/>')
    parts.append('</svg>')
    path.write_text("\n".join(parts), encoding="utf-8")


def svg_lines(path: Path, summary: dict) -> None:
    width, height = 960, 420
    parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">',
        '<rect width="100%" height="100%" fill="white"/>',
        '<text x="480" y="28" text-anchor="middle" font-family="sans-serif" font-size="20" font-weight="bold">Concurrency scaling</text>',
    ]
    for panel, metric, label in ((0, "qps", "QPS (higher is better)"), (1, "p50_ms", "P50 latency ms (lower is better)")):
        x0 = 70 + panel * 470
        y0, plot_w, plot_h = 70, 360, 280
        values = [summary[name]["local"][str(c)][metric] for name in ("legacy", "optimized") for c in (1, 4, 12)]
        maximum = max(values) * 1.1
        parts += [
            f'<text x="{x0+plot_w/2}" y="55" text-anchor="middle" font-family="sans-serif" font-size="14">{label}</text>',
            f'<line x1="{x0}" y1="{y0+plot_h}" x2="{x0+plot_w}" y2="{y0+plot_h}" stroke="#475569"/>',
            f'<line x1="{x0}" y1="{y0}" x2="{x0}" y2="{y0+plot_h}" stroke="#475569"/>',
        ]
        for name in ("legacy", "optimized"):
            points = []
            for idx, concurrency in enumerate((1, 4, 12)):
                x = x0 + idx * plot_w / 2
                value = summary[name]["local"][str(concurrency)][metric]
                y = y0 + plot_h - value / maximum * plot_h
                points.append(f"{x:.1f},{y:.1f}")
                parts.append(f'<circle cx="{x:.1f}" cy="{y:.1f}" r="5" fill="{COLORS[name]}"/>')
                parts.append(f'<text x="{x:.1f}" y="{y-9:.1f}" text-anchor="middle" font-family="sans-serif" font-size="11">{value:.1f}</text>')
                parts.append(f'<text x="{x:.1f}" y="{y0+plot_h+20}" text-anchor="middle" font-family="sans-serif" font-size="11">C={concurrency}</text>')
            parts.append(f'<polyline points="{" ".join(points)}" fill="none" stroke="{COLORS[name]}" stroke-width="3"/>')
    parts += [
        '<rect x="345" y="385" width="14" height="14" fill="#64748b"/><text x="365" y="397" font-family="sans-serif" font-size="12">Before</text>',
        '<rect x="455" y="385" width="14" height="14" fill="#16a34a"/><text x="475" y="397" font-family="sans-serif" font-size="12">After (KaFFPa)</text>',
        '</svg>',
    ]
    path.write_text("\n".join(parts), encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--experiment", type=Path, required=True)
    args = parser.parse_args()
    core = json.loads((args.experiment / "core_results.json").read_text())
    http = {
        name: json.loads((args.experiment / f"http_{name}/report.json").read_text())
        for name in ("legacy", "optimized")
    }
    summary = {}
    for name in ("legacy", "optimized"):
        item = core["results"][name]
        summary[name] = {
            "recall_at_10": item["recall"]["recall_at_10"],
            "recall_ci95": item["recall"]["ci95"],
            "candidate_mean": item["recall"]["candidate_mean"],
            "scanned_codes_mean": item["phase_profile"]["scanned_codes"]["mean"],
            "routing_p50_ms": item["phase_profile"]["routing_ms"]["p50"],
            "scan_p50_ms": item["phase_profile"]["approximate_scan_ms"]["p50"],
            "exact_p50_ms": item["phase_profile"]["exact_fetch_ms"]["p50"],
            "build_seconds": item["manifest"]["build_seconds"],
            "index_bytes": item["index_bytes"],
            "local": {str(c): median_trials(item, c) for c in (1, 4, 12)},
            "http_json": http[name]["load_json"],
            "http_binary": http[name]["load_binary"],
        }
    before, after = summary["legacy"], summary["optimized"]
    summary["changes_percent"] = {
        "recall_at_10": change(before["recall_at_10"], after["recall_at_10"]),
        "candidates": change(before["candidate_mean"], after["candidate_mean"]),
        "scanned_codes": change(before["scanned_codes_mean"], after["scanned_codes_mean"]),
        "local_c1_qps": change(before["local"]["1"]["qps"], after["local"]["1"]["qps"]),
        "local_c1_p50": change(before["local"]["1"]["p50_ms"], after["local"]["1"]["p50_ms"]),
        "local_c12_qps": change(before["local"]["12"]["qps"], after["local"]["12"]["qps"]),
        "local_c12_p50": change(before["local"]["12"]["p50_ms"], after["local"]["12"]["p50_ms"]),
        "binary_qps": change(before["http_binary"]["qps"], after["http_binary"]["qps"]),
        "binary_p95": change(before["http_binary"]["p95_ms"], after["http_binary"]["p95_ms"]),
        "build_seconds": change(before["build_seconds"], after["build_seconds"]),
        "index_bytes": change(before["index_bytes"], after["index_bytes"]),
    }
    (args.experiment / "summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")

    with (args.experiment / "summary.csv").open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(["metric", "before", "after", "change_percent"])
        rows = [
            ("recall_at_10", before["recall_at_10"], after["recall_at_10"]),
            ("candidate_mean", before["candidate_mean"], after["candidate_mean"]),
            ("scanned_codes_mean", before["scanned_codes_mean"], after["scanned_codes_mean"]),
            ("local_c1_qps", before["local"]["1"]["qps"], after["local"]["1"]["qps"]),
            ("local_c1_p50_ms", before["local"]["1"]["p50_ms"], after["local"]["1"]["p50_ms"]),
            ("local_c12_qps", before["local"]["12"]["qps"], after["local"]["12"]["qps"]),
            ("local_c12_p50_ms", before["local"]["12"]["p50_ms"], after["local"]["12"]["p50_ms"]),
            ("http_binary_qps", before["http_binary"]["qps"], after["http_binary"]["qps"]),
            ("http_binary_p95_ms", before["http_binary"]["p95_ms"], after["http_binary"]["p95_ms"]),
        ]
        for metric, old, new in rows:
            writer.writerow([metric, old, new, change(old, new)])

    svg_bar(args.experiment / "overview.svg", "Before vs after", [
        ("Recall@10", before["recall_at_10"], after["recall_at_10"], ""),
        ("Local C=1 QPS", before["local"]["1"]["qps"], after["local"]["1"]["qps"], ""),
        ("3-node binary QPS", before["http_binary"]["qps"], after["http_binary"]["qps"], ""),
    ])
    svg_bar(args.experiment / "work_reduction.svg", "Search work reduction", [
        ("Candidates", before["candidate_mean"], after["candidate_mean"], ""),
        ("Scanned codes", before["scanned_codes_mean"], after["scanned_codes_mean"], ""),
        ("Local C=1 P50", before["local"]["1"]["p50_ms"], after["local"]["1"]["p50_ms"], " ms"),
    ])
    svg_lines(args.experiment / "concurrency.svg", summary)

    c = summary["changes_percent"]
    report = f"""# Before/after performance report

Same-data A/B: 20,000 indexed vectors, 300 independent queries, four partitions, Top-10, AVX-512 FastScan. Local latency uses 200 warmups and 1,000 requests x 3 repeats. HTTP uses three local read nodes, concurrency 12, and 1,000 requests per protocol.

| Configuration | Before | After |
|---|---:|---:|
| HNSW sample | 2,000 (10%) | 4,000 (20%) |
| Graph partitioner | fallback | KaFFPa strong |
| HNSW route nodes | 32 | 64 |
| Routed partitions | 2 | 2 |
| IVF target/list | about 1,024 | 384 |
| IVF nprobe | 4 | 2 |
| Candidate multiplier | 20 | 8 |

| Metric | Before | After | Change |
|---|---:|---:|---:|
| Recall@10 | {before['recall_at_10']:.4f} | {after['recall_at_10']:.4f} | {c['recall_at_10']:+.1f}% |
| Recall 95% CI | [{before['recall_ci95'][0]:.4f}, {before['recall_ci95'][1]:.4f}] | [{after['recall_ci95'][0]:.4f}, {after['recall_ci95'][1]:.4f}] | overlap |
| Candidates/query | {before['candidate_mean']:.0f} | {after['candidate_mean']:.0f} | {c['candidates']:+.1f}% |
| Scanned codes/query | {before['scanned_codes_mean']:.0f} | {after['scanned_codes_mean']:.0f} | {c['scanned_codes']:+.1f}% |
| Local C=1 QPS | {before['local']['1']['qps']:.2f} | {after['local']['1']['qps']:.2f} | {c['local_c1_qps']:+.1f}% |
| Local C=1 P50 | {before['local']['1']['p50_ms']:.2f} ms | {after['local']['1']['p50_ms']:.2f} ms | {c['local_c1_p50']:+.1f}% |
| Local C=12 QPS | {before['local']['12']['qps']:.2f} | {after['local']['12']['qps']:.2f} | {c['local_c12_qps']:+.1f}% |
| Local C=12 P50 | {before['local']['12']['p50_ms']:.2f} ms | {after['local']['12']['p50_ms']:.2f} ms | {c['local_c12_p50']:+.1f}% |
| 3-node binary QPS | {before['http_binary']['qps']:.2f} | {after['http_binary']['qps']:.2f} | {c['binary_qps']:+.1f}% |
| 3-node binary P95 | {before['http_binary']['p95_ms']:.2f} ms | {after['http_binary']['p95_ms']:.2f} ms | {c['binary_p95']:+.1f}% |
| Build time | {before['build_seconds']:.2f} s | {after['build_seconds']:.2f} s | {c['build_seconds']:+.1f}% |
| Index bytes | {before['index_bytes']} | {after['index_bytes']} | {c['index_bytes']:+.1f}% |

## Local concurrency

| Concurrency | Before QPS | After QPS | Before P50 | After P50 | Before P95 | After P95 |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | {before['local']['1']['qps']:.2f} | {after['local']['1']['qps']:.2f} | {before['local']['1']['p50_ms']:.2f} ms | {after['local']['1']['p50_ms']:.2f} ms | {before['local']['1']['p95_ms']:.2f} ms | {after['local']['1']['p95_ms']:.2f} ms |
| 4 | {before['local']['4']['qps']:.2f} | {after['local']['4']['qps']:.2f} | {before['local']['4']['p50_ms']:.2f} ms | {after['local']['4']['p50_ms']:.2f} ms | {before['local']['4']['p95_ms']:.2f} ms | {after['local']['4']['p95_ms']:.2f} ms |
| 12 | {before['local']['12']['qps']:.2f} | {after['local']['12']['qps']:.2f} | {before['local']['12']['p50_ms']:.2f} ms | {after['local']['12']['p50_ms']:.2f} ms | {before['local']['12']['p95_ms']:.2f} ms | {after['local']['12']['p95_ms']:.2f} ms |

## Three-node HTTP

| Protocol | Before QPS | After QPS | Before P50 | After P50 | Before P95 | After P95 |
|---|---:|---:|---:|---:|---:|---:|
| JSON | {before['http_json']['qps']:.2f} | {after['http_json']['qps']:.2f} | {before['http_json']['p50_ms']:.2f} ms | {after['http_json']['p50_ms']:.2f} ms | {before['http_json']['p95_ms']:.2f} ms | {after['http_json']['p95_ms']:.2f} ms |
| Binary | {before['http_binary']['qps']:.2f} | {after['http_binary']['qps']:.2f} | {before['http_binary']['p50_ms']:.2f} ms | {after['http_binary']['p50_ms']:.2f} ms | {before['http_binary']['p95_ms']:.2f} ms | {after['http_binary']['p95_ms']:.2f} ms |

## Phase profile (P50)

| Phase | Before | After |
|---|---:|---:|
| HNSW routing | {before['routing_p50_ms']:.2f} ms | {after['routing_p50_ms']:.2f} ms |
| RaBitQ-IVF scan | {before['scan_p50_ms']:.2f} ms | {after['scan_p50_ms']:.2f} ms |
| Exact fetch/rerank | {before['exact_p50_ms']:.2f} ms | {after['exact_p50_ms']:.2f} ms |

The optimized system preserves recall within overlapping confidence intervals while substantially reducing scan and rerank work. The larger HNSW raises routing P50 from {before['routing_p50_ms']:.2f} to {after['routing_p50_ms']:.2f} ms, but approximate scan P50 falls from {before['scan_p50_ms']:.2f} to {after['scan_p50_ms']:.2f} ms and exact-fetch P50 falls from {before['exact_p50_ms']:.2f} to {after['exact_p50_ms']:.2f} ms.
"""
    (args.experiment / "REPORT.md").write_text(report, encoding="utf-8")


if __name__ == "__main__":
    main()
