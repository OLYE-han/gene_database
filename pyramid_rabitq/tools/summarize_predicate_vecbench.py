#!/usr/bin/env python3
"""Create a report and charts for a filtered VecBench pilot run."""

from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path

import numpy as np


def write_svg(path: Path, concurrency_rows: list[dict], c1: list[dict]) -> None:
    """Write a dependency-free four-panel benchmark chart."""
    width, height = 1100, 760
    colors = ["#2563eb", "#dc2626", "#16a34a"]
    parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">',
        '<rect width="100%" height="100%" fill="white"/>',
        '<style>text{font-family:DejaVu Sans,Arial,sans-serif;fill:#111827}.title{font-size:24px;font-weight:700}.panel{font-size:17px;font-weight:700}.tick{font-size:12px;fill:#4b5563}.value{font-size:12px;font-weight:600}</style>',
        '<text x="550" y="34" text-anchor="middle" class="title">Pyramid RaBitQ filtered VecBench pilot</text>',
    ]

    def panel(x: int, y: int, title: str, values: list[float], labels: list[str],
              maximum: float, color: str, suffix: str = "") -> None:
        chart_w, chart_h = 450, 235
        parts.extend([
            f'<rect x="{x}" y="{y}" width="{chart_w}" height="{chart_h}" rx="8" fill="#f8fafc" stroke="#cbd5e1"/>',
            f'<text x="{x + 18}" y="{y + 28}" class="panel">{title}</text>',
        ])
        baseline = y + 195
        bar_width = 88
        gap = (chart_w - 40 - bar_width * len(values)) / max(1, len(values))
        for number, (value, label) in enumerate(zip(values, labels)):
            bx = x + 28 + number * (bar_width + gap)
            bar_h = 135 * value / maximum if maximum else 0
            by = baseline - bar_h
            parts.append(f'<rect x="{bx}" y="{by:.1f}" width="{bar_width}" height="{bar_h:.1f}" rx="4" fill="{color}"/>')
            parts.append(f'<text x="{bx + bar_width / 2}" y="{by - 7:.1f}" text-anchor="middle" class="value">{value:.2f}{suffix}</text>')
            parts.append(f'<text x="{bx + bar_width / 2}" y="{baseline + 20}" text-anchor="middle" class="tick">{label}</text>')

    concurrency_labels = [f"C={row['concurrency']}" for row in concurrency_rows]
    panel(55, 65, "Throughput", [row["qps"] for row in concurrency_rows],
          concurrency_labels, max(row["qps"] for row in concurrency_rows) * 1.12, colors[0], "")
    panel(595, 65, "P95 latency (ms)", [row["p95_ms"] for row in concurrency_rows],
          concurrency_labels, max(row["p95_ms"] for row in concurrency_rows) * 1.12, colors[1], "")
    predicate_labels = [str(row["predicate"]) for row in c1]
    panel(55, 365, "Mean recall by predicate (C=1)",
          [float(row["mean_recall"]) for row in c1], predicate_labels, 1.05, colors[2], "")
    panel(595, 365, "Mean latency by predicate (C=1, ms)",
          [float(row["mean_latency_ms"]) for row in c1], predicate_labels,
          max(float(row["mean_latency_ms"]) for row in c1) * 1.12, "#7c3aed", "")
    parts.append('<text x="550" y="728" text-anchor="middle" class="tick">YFCC 100K × 1920 · Top-100 · 48 queries · local exact ground truth</text>')
    parts.append("</svg>")
    path.write_text("\n".join(parts), encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-dir", type=Path, required=True)
    args = parser.parse_args()
    run_dir = args.run_dir
    results = json.loads((run_dir / "results.json").read_text(encoding="utf-8"))
    queries = {
        row["name"]: row
        for row in json.loads((run_dir / "queries.json").read_text(encoding="utf-8"))
    }
    if results["status"] != "complete":
        raise ValueError("benchmark did not complete")

    concurrency_rows = results["P3_concurrent"]
    type_rows: list[dict[str, float | int | str]] = []
    for result in concurrency_rows:
        for field in ("equal", "range", "tags"):
            selected = [
                row for row in result["official_rows"]
                if queries[row["name"]]["scalar_filters"][0]["field"] == field
            ]
            latencies = np.asarray([row["avg_time"] * 1000 for row in selected])
            recalls = np.asarray([row["avg_recall"] for row in selected])
            type_rows.append({
                "concurrency": int(result["concurrency"]),
                "predicate": field,
                "queries": len(selected),
                "mean_recall": float(recalls.mean()),
                "min_recall": float(recalls.min()),
                "mean_latency_ms": float(latencies.mean()),
                "p50_latency_ms": float(np.median(latencies)),
            })

    with (run_dir / "concurrency_summary.csv").open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(["concurrency", "recall", "qps", "p50_ms", "p95_ms", "p99_ms"])
        for row in concurrency_rows:
            writer.writerow([
                row["concurrency"], row["recall"], row["qps"],
                row["p50_ms"], row["p95_ms"], row["p99_ms"],
            ])
    with (run_dir / "predicate_summary.csv").open("w", newline="", encoding="utf-8") as handle:
        columns = list(type_rows[0])
        writer = csv.DictWriter(handle, fieldnames=columns)
        writer.writeheader()
        writer.writerows(type_rows)

    c1 = [row for row in type_rows if row["concurrency"] == 1]
    write_svg(run_dir / "performance.svg", concurrency_rows, c1)

    serial = results["P2_serial"]
    c1_by_type = {str(row["predicate"]): row for row in c1}
    concurrency_table = "\n".join(
        f"| {row['concurrency']} | {row['recall']:.4f} | {row['qps']:.2f} | "
        f"{row['p50_ms']:.1f} | {row['p95_ms']:.1f} | {row['p99_ms']:.1f} |"
        for row in concurrency_rows
    )
    type_table = "\n".join(
        f"| {field} | {int(c1_by_type[field]['queries'])} | "
        f"{float(c1_by_type[field]['mean_recall']):.4f} | "
        f"{float(c1_by_type[field]['min_recall']):.4f} | "
        f"{float(c1_by_type[field]['mean_latency_ms']):.1f} |"
        for field in ("equal", "range", "tags")
    )
    profile = results["profile_validation"]
    report = f"""# Pyramid RaBitQ 谓词过滤 VecBench 重测

## 结论

优化后的过滤规划器在 YFCC 100K pilot 的 **48 条可匹配官方查询**上取得
**{concurrency_rows[0]['recall']:.4f} 平均 Recall**。吞吐在并发 10 时达到
**{concurrency_rows[1]['qps']:.2f} QPS**，并发继续提高到 50 后没有增长，表明系统已经受
CPU/内存带宽与 1920 维原始向量精排限制。

## 测试配置

- 数据：YFCC 100,000 × 1,920，L2 距离，Top-100。
- 查询：48 条；equal、双 range、tags contains 各 16 条。
- 服务：3 个本机读节点，每节点 4 个 partition workers，512 MiB vector cache。
- 并发：1、10、50；每条查询重复 3 次，共 432 个正式并发请求。
- Ground truth：从当前 corpus 本地精确计算，SHA256 `{results['ground_truth_sha256']}`。
- 规划器：`{results['health'][0]['filtered_search_planner']}`。

## 并发结果

| 并发 | Recall | QPS | P50 ms | P95 ms | P99 ms |
|---:|---:|---:|---:|---:|---:|
{concurrency_table}

串行 P2：平均延迟 {serial['avg_latency'] * 1000:.1f} ms，P50
{serial['p50_latency'] * 1000:.1f} ms，平均 Recall {serial['avg_recall']:.4f}。

## 按谓词类型（C=1）

| 谓词 | 查询数 | 平均 Recall | 最低 Recall | 平均延迟 ms |
|---|---:|---:|---:|---:|
{type_table}

`range` 是当前主要召回短板；equal 进入小集合精确路径后 Recall 为 1，tags 也接近 1。
C=50 的高延迟主要是饱和后的排队，不代表单请求计算时间同步增加。

## 边界与数据版本说明

- 这不是官方 10M × 1920、3,000 查询的完整 Vrank；验证偏差为：
  `{'; '.join(profile['deviations'])}`。
- 因此 `official_leaderboard_eligible=false`，结果只用于当前实现的回归和规划器评估。
- 旧基线目录使用的是 `yfcc-100k` 数据版本；当前 `CURRENT` 已由后续 P1 实验发布为
  `vecbench-P1-1790736544257995423`，且标量元数据已经变化。旧 ground truth 与当前谓词
  不一致，不能进行可信的数值前后对比。本报告全部使用当前 corpus 重新计算的精确真值。
"""
    (run_dir / "REPORT.md").write_text(report, encoding="utf-8")


if __name__ == "__main__":
    main()
