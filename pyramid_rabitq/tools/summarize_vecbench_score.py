#!/usr/bin/env python3
"""Merge split VecBench phases and render a scored six-phase report."""

from __future__ import annotations

import argparse
import json
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--p1", type=Path, required=True)
    parser.add_argument("--dynamic", type=Path, required=True)
    parser.add_argument("--score", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    p1 = json.loads(args.p1.read_text(encoding="utf-8"))
    dynamic = json.loads(args.dynamic.read_text(encoding="utf-8"))
    score = json.loads(args.score.read_text(encoding="utf-8"))
    combined = dict(dynamic)
    combined["P1"] = p1["P1"]
    combined["phases_requested"] = ["P1", "P2", "P3", "P4", "P5", "P6"]
    (args.output_dir / "full_results.json").write_text(
        json.dumps(combined, indent=2), encoding="utf-8"
    )

    metrics = score["metrics"]
    ranks = score["ranks_against_official_table"]
    labels = ["T0", "Recall", "Latency", "QPS", "T1", "T2", "T3"]
    keys = ["T0", "recall", "latency", "qps", "T1", "T2", "T3"]
    colors = ["#2563eb", "#16a34a", "#dc2626", "#7c3aed", "#0891b2", "#d97706", "#4f46e5"]
    bars = []
    for index, (label, key, color) in enumerate(zip(labels, keys, colors)):
        x = 80 + index * 135
        height = ranks[key] * 22
        y = 440 - height
        bars.extend([
            f'<rect x="{x}" y="{y}" width="90" height="{height}" rx="5" fill="{color}"/>',
            f'<text x="{x + 45}" y="{y - 10}" text-anchor="middle" class="value">rank {ranks[key]}</text>',
            f'<text x="{x + 45}" y="468" text-anchor="middle" class="label">{label}</text>',
        ])
    svg = f'''<svg xmlns="http://www.w3.org/2000/svg" width="1100" height="600" viewBox="0 0 1100 600">
<rect width="100%" height="100%" fill="white"/>
<style>text{{font-family:DejaVu Sans,Arial,sans-serif;fill:#111827}}.title{{font-size:28px;font-weight:700}}.score{{font-size:50px;font-weight:700;fill:#2563eb}}.value{{font-size:15px;font-weight:700}}.label{{font-size:16px}}.note{{font-size:14px;fill:#4b5563}}</style>
<text x="550" y="45" text-anchor="middle" class="title">Pyramid RaBitQ VecBench six-phase provisional score</text>
<text x="550" y="112" text-anchor="middle" class="score">Vrank {score['vrank']:.1f}</text>
<line x1="60" y1="440" x2="1040" y2="440" stroke="#94a3b8"/>
{''.join(bars)}
<text x="550" y="530" text-anchor="middle" class="note">Lower rank and lower Vrank are better · YFCC reconstructed-public 100K pilot</text>
<text x="550" y="555" text-anchor="middle" class="note">Formula: sum of seven metric ranks / 6 · compared with published YFCC 10M table</text>
</svg>'''
    (args.output_dir / "score.svg").write_text(svg, encoding="utf-8")

    report = f"""# Pyramid RaBitQ VecBench 六阶段跑分

## 跑分结果

本次通过官方 VecBench 执行路径完成 P1–P6，得到 provisional **Vrank {score['vrank']:.1f}**。
Vrank 越低越好，计算公式为七项指标名次之和除以 6。

| 阶段/指标 | 实测值 | 对官方表名次 |
|---|---:|---:|
| P1 T0 初始化 | {metrics['T0']:.4f} s | {ranks['T0']} |
| P2 Recall@100 | {metrics['recall']:.4f} | {ranks['recall']} |
| P2 平均延迟 | {metrics['latency']:.4f} s | {ranks['latency']} |
| P3 QPS（并发 10） | {metrics['qps']:.4f} | {ranks['qps']} |
| P4 T1 插入 20% | {metrics['T1']:.4f} s | {ranks['T1']} |
| P5 T2 更新 20% | {metrics['T2']:.4f} s | {ranks['T2']} |
| P6 T3 删除 20% | {metrics['T3']:.4f} s | {ranks['T3']} |

名次和为 **{sum(ranks.values())}**，所以 Vrank = {sum(ranks.values())} / 6 = **{score['vrank']:.1f}**。

## 测试口径

- 官方执行器提交：`e503262f65421e9a1a3339f0416163c53a72809e`。
- 数据：YFCC reconstructed-public 100K × 1920，Top-100。
- 查询：48 条，包括 equal、双 range、tags contains 各 16 条。
- 并发：10，每条查询执行 1 次。
- 动态数据：20,000 条，即 base 的 20%；更新方式以输入实验记录为准；历史 LSM 结果不能代表当前全量重建实现。
- 索引：8 个 KaFFPa partitions，RaBitQ-IVF FastScan，过滤规划器为
  `metadata-pruning+exact-small-set+adaptive-nprobe`。

## 结论

- P4/P5/P6 的实测耗时及名次见上表；不同更新实现的结果需分别解释。
- Recall 排名第 5；主要损失仍来自 range 查询的近似 IVF 候选生成。
- 查询延迟和 QPS 均排在参考表之后，是总体分数的主要短板。
- T0 排名第 4，其中两次全量 HNSW 路由分配占据主要构建时间。

## 合规边界

该分数是 `nonstandard-provisional`，不能提交官方排行榜。官方分数要求 YFCC 10M、
3,000 条查询以及官方 ground truth；当前公开文件中没有可用的官方 equal/range 数据和
10M ground truth，本机现有 equal/range 为固定种子重建值。本结果严格复用了官方阶段、
执行器、指标和 Vrank 公式，但数据规模与查询数不满足榜单资格。
"""
    (args.output_dir / "REPORT.md").write_text(report, encoding="utf-8")


if __name__ == "__main__":
    main()
