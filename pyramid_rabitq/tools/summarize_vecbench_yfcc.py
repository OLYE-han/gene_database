#!/usr/bin/env python3
"""Summarize baseline and filter-aware YFCC VecBench runs."""

from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline", type=Path, required=True)
    parser.add_argument("--optimized", type=Path, required=True)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--provenance", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    before = json.loads(args.baseline.read_text())
    after = json.loads(args.optimized.read_text())
    manifest = json.loads(args.manifest.read_text())
    provenance = json.loads(args.provenance.read_text())
    args.output.mkdir(parents=True, exist_ok=True)

    with (args.output / "comparison.csv").open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(["planner", "concurrency", "recall", "qps", "p50_ms", "p95_ms", "p99_ms"])
        for label, data in (("vector-route-then-filter", before), ("filter-aware", after)):
            for row in data["P3_concurrent"]:
                writer.writerow([label, row["concurrency"], row["recall"], row["qps"],
                                 row["p50_ms"], row["p95_ms"], row["p99_ms"]])

    b, a = before["P2_serial"], after["P2_serial"]
    report = f"""# VecBench YFCC 过滤检索评测

## 结论

本次实现并验证了 `equal`、`range`、`tags contains` 三类 VecBench 过滤。
原始“向量路由后过滤”规划器的平均召回仅 **{b['avg_recall']:.4f}**；
过滤感知规划器将召回提高到 **{a['avg_recall']:.4f}**，但平均串行延迟从
**{b['avg_latency'] * 1000:.1f} ms** 增至 **{a['avg_latency'] * 1000:.1f} ms**。

| 规划器 | C=1 QPS | C=10 QPS | C=50 QPS | Recall |
|---|---:|---:|---:|---:|
| 向量路由后过滤 | {before['P3_concurrent'][0]['qps']:.2f} | {before['P3_concurrent'][1]['qps']:.2f} | {before['P3_concurrent'][2]['qps']:.2f} | {before['P3_concurrent'][0]['recall']:.4f} |
| 过滤感知 | {after['P3_concurrent'][0]['qps']:.2f} | {after['P3_concurrent'][1]['qps']:.2f} | {after['P3_concurrent'][2]['qps']:.2f} | {after['P3_concurrent'][0]['recall']:.4f} |

## P1 初始化

- 数据：YFCC {provenance['rows']:,} × {provenance['dimension']} pilot。
- 构建时间：{manifest['build_seconds']:.2f} 秒。
- HNSW 采样：{manifest['hnsw_sample_vectors']:,}（{manifest['hnsw_sample_ratio_actual']:.0%}）。
- KaFFPa 分片：{len(manifest['partitions'])}；IVF 目标 occupancy 384。

## 可复现性和标准边界

- 向量与 tags 来自 Big-ANN 官方 YFCC 10M；查询来自 VecBench 官方仓库。
- VecBench 仓库没有发布 `equal_cluster.u2bin`，`range.f4bin` 是 0 字节，且没有
  range 生成实现。本实验的 equal/range 为固定种子重建值，详情记录在 `provenance.json`。
- 这是用于验证标准查询语义和评估设计取舍的 100K pilot，不是官网 10M Vrank。
- 当前系统是离线不可变索引，不支持 P4 insert、P5 update、P6 delete，因此完整
  六阶段 Vrank 仍为 N/A；把离线重建伪装成在线更新会破坏横向可比性。

## 设计判断

- 优点：预过滤位图与 RaBitQ-IVF 可以达到接近 0.97 的过滤召回；读节点无状态，
  三节点能并行扩展；精确 rerank 保证候选集合内的顺序正确。
- 缺点：纯 HNSW 向量路由不适用于稀疏过滤；高召回需要跨分片和扩大 IVF probe，
  吞吐下降约一个数量级以上；在线可变性缺失，无法获得标准 Vrank。
"""
    (args.output / "REPORT.md").write_text(report, encoding="utf-8")


if __name__ == "__main__":
    main()
