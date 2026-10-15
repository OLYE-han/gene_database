#!/usr/bin/env python3
"""Create reproducible tables, plots, and a report from a VecBench run."""

from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path

def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("result", type=Path)
    args = parser.parse_args()
    data = json.loads(args.result.read_text(encoding="utf-8"))
    output = args.result.parent
    rows = data["concurrent"]

    with (output / "summary.csv").open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=[
            "concurrency", "qps", "recall_at_100", "mean_ms", "p50_ms", "p95_ms", "p99_ms",
        ])
        writer.writeheader()
        for row in rows:
            writer.writerow({
                "concurrency": row["concurrency"], "qps": row["qps"],
                "recall_at_100": row["recall"], "mean_ms": row["latency_ms"]["mean"],
                "p50_ms": row["latency_ms"]["p50"], "p95_ms": row["latency_ms"]["p95"],
                "p99_ms": row["latency_ms"]["p99"],
            })

    def points(values, x0, y0, width, height):
        maximum = max(values) or 1
        x_max = max(len(values) - 1, 1)
        return " ".join(
            f"{x0 + width * i / x_max:.1f},{y0 + height * (1 - value / maximum):.1f}"
            for i, value in enumerate(values)
        )

    concurrency = [row["concurrency"] for row in rows]
    qps = [row["qps"] for row in rows]
    latency_series = [(label, [r["latency_ms"][key] for r in rows], color) for key, label, color in (
        ("p50", "P50", "#2f6f9f"), ("p95", "P95", "#d17a22"),
        ("p99", "P99", "#6a4c93")
    )]
    left_points = points(qps, 70, 55, 360, 255)
    right_lines = "\n".join(
        f'<polyline points="{points(values, 570, 55, 360, 255)}" fill="none" '
        f'stroke="{color}" stroke-width="3"/><text x="{760 + 55 * i}" y="38" '
        f'fill="{color}" font-size="14">{name}</text>'
        for i, (name, values, color) in enumerate(latency_series)
    )
    labels = "".join(
        f'<text x="{70 + 360 * i / max(len(rows)-1, 1):.1f}" y="335" text-anchor="middle">{value}</text>'
        f'<text x="{570 + 360 * i / max(len(rows)-1, 1):.1f}" y="335" text-anchor="middle">{value}</text>'
        for i, value in enumerate(concurrency)
    )
    svg = f'''<svg xmlns="http://www.w3.org/2000/svg" width="1000" height="390" viewBox="0 0 1000 390">
<rect width="1000" height="390" fill="white"/>
<g font-family="sans-serif" font-size="13" fill="#222">
<text x="250" y="25" text-anchor="middle" font-size="18">VecBench throughput</text>
<text x="750" y="25" text-anchor="middle" font-size="18">End-to-end latency</text>
<path d="M70 55V310H430 M570 55V310H930" fill="none" stroke="#444"/>
<polyline points="{left_points}" fill="none" stroke="#2f6f9f" stroke-width="3"/>
{right_lines}{labels}
<text x="250" y="365" text-anchor="middle">Concurrency</text>
<text x="750" y="365" text-anchor="middle">Concurrency</text>
<text x="22" y="185" transform="rotate(-90 22 185)" text-anchor="middle">QPS (max {max(qps):.1f})</text>
<text x="522" y="185" transform="rotate(-90 522 185)" text-anchor="middle">Latency ms (max {max(v for _, values, _ in latency_series for v in values):.1f})</text>
</g></svg>'''
    (output / "performance.svg").write_text(svg, encoding="utf-8")

    peak = max(rows, key=lambda row: row["qps"])
    serial = data["serial"]
    table = "\n".join(
        f"| {r['concurrency']} | {r['qps']:.2f} | {r['recall']:.4f} | "
        f"{r['latency_ms']['p50']:.2f} | {r['latency_ms']['p95']:.2f} | "
        f"{r['latency_ms']['p99']:.2f} |" for r in rows
    )
    report = f"""# VecBench 兼容评测报告

## 结论

当前 Pyramid RaBitQ 系统在 20,000 × 2,560 维 gene 向量、三读节点、Top-100
工作负载上的 Recall@100 为 **{rows[0]['recall']:.4f}**。峰值吞吐为
**{peak['qps']:.2f} QPS（并发 {peak['concurrency']}）**。提高到并发 50 后吞吐不再增长，
且 P50 延迟显著上升，说明本机 CPU/服务执行队列已经饱和。

| 并发 | QPS | Recall@100 | P50 (ms) | P95 (ms) | P99 (ms) |
|---:|---:|---:|---:|---:|---:|
{table}

官方串行执行器的结果为：平均延迟 {serial['avg_latency'] * 1000:.2f} ms、
P50 {serial['p50_latency'] * 1000:.2f} ms、P95 {serial['p95_latency'] * 1000:.2f} ms、
P99 {serial['p99_latency'] * 1000:.2f} ms、平均召回 {serial['avg_recall']:.4f}。
串行结果包含服务首次加载后的少量冷查询；并发执行器按官方逻辑先预热 10% 查询。

## 方法与边界

- 论文：*VecBench: A Controllable Benchmark for Filtered Vector Search: [Experiments & Analysis]*，
  PACMMOD/SIGMOD 2026，DOI `10.1145/3802125`。
- 源码：`ruc-datalab/VecBench`，固定提交
  `e503262f65421e9a1a3339f0416163c53a72809e`。
- 使用官方 `utils.workload_executor.execute` 和
  `utils.concurrent.execute_concurrent`，没有重写其预热、并发、QPS 或召回逻辑。
- 128 个查询，每个并发档位每条查询执行 3 次，共 384 次正式请求；精确 ground truth
  由完整语料上的 Faiss `IndexFlatIP` 计算。
- 索引固定配置为 HNSW 路由 + KaFFPa 分片 + RaBitQ-IVF FastScan，读取节点使用 AVX-512。
- 当前系统只支持预构建向量检索，不支持 VecBench 的标量过滤及在线
  insert/update/delete。因此这是明确标注的 **vector-only compatibility workload**，
  不是 VecBench 六阶段过滤检索全套跑分，也不存在可诚实报告的单一综合分数。
"""
    (output / "REPORT.md").write_text(report, encoding="utf-8")


if __name__ == "__main__":
    main()
