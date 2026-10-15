#!/usr/bin/env python3
"""Rank a six-phase result against the official VecBench YFCC 10M table."""

from __future__ import annotations

import argparse
import json
from pathlib import Path


BASELINES = {
    "T0": [229.99, 425.39, 562.10, 562.10, 6727.25, 6727.25, 978.38],
    "recall": [0.993, 0.970, 0.960, 0.975, 0.897, 0.257, 0.995, 0.988, 0.455, 0.704],
    "latency": [0.0586, 0.0327, 0.0828, 0.0842, 0.0159, 0.0020, 0.0250, 0.5869, 0.0200, 0.0473],
    "qps": [19.41, 21.26, 101.83, 87.65, 455.19, 1963.25, 47.78, 7.05, 51.17, 50.24],
    "T1": [146.29, 157.04, 495.09, 495.09, 5207.49, 5207.49, 2523.77, 186.92, 640.64, 3060.54],
    "T2": [171.36, 132.96, 622.06, 622.06, 5547.22, 5547.22, 785.43, 145.90, 441.96, 516.50],
    "T3": [7.75, 7.94, 7.00, 7.00, 7.82, 7.82, 10.12, 8.92, 11.12, 182.29],
}
DIRECTION = {"T0": "lower", "recall": "higher", "latency": "lower", "qps": "higher", "T1": "lower", "T2": "lower", "T3": "lower"}


def dense_rank(metric: str, value: float) -> int:
    values = set(BASELINES[metric])
    if DIRECTION[metric] == "lower":
        return 1 + sum(candidate < value for candidate in values)
    return 1 + sum(candidate > value for candidate in values)


def extract(report):
    p2 = report["P2"]["overall"][0]
    p3 = report["P3"]["results"]
    return {
        "T0": float(report["P1"]["T0_seconds"]),
        "recall": float(p2["avg_recall"]),
        "latency": float(p2["avg_latency"]),
        "qps": float(p3[0]["qps"]),
        "T1": float(report["P4"]["T1_seconds"]),
        "T2": float(report["P5"]["T2_seconds"]),
        "T3": float(report["P6"]["T3_seconds"]),
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("result", type=Path)
    parser.add_argument(
        "--p1-result", type=Path,
        help="optional P1-only report when official phases were run separately",
    )
    parser.add_argument("--output", type=Path)
    parser.add_argument("--allow-nonstandard", action="store_true")
    args = parser.parse_args()
    report = json.loads(args.result.read_text(encoding="utf-8"))
    if args.p1_result:
        p1_report = json.loads(args.p1_result.read_text(encoding="utf-8"))
        report["P1"] = p1_report["P1"]
    full = int(report.get("scale", 0)) == 10_000_000
    if not full and not args.allow_nonstandard:
        raise ValueError("official Vrank requires YFCC scale=10000000; use --allow-nonstandard only for a provisional pilot")
    metrics = extract(report)
    ranks = {name: dense_rank(name, value) for name, value in metrics.items()}
    # This exactly reproduces every one-decimal Vrank displayed by the official
    # leaderboard. P2 contributes both recall and latency ranks.
    vrank = round(sum(ranks.values()) / 6.0, 1)
    result = {
        "profile": "official-yfcc-10m" if full else "nonstandard-provisional",
        "metrics": metrics, "ranks_against_official_table": ranks,
        "vrank": vrank,
        "formula": "sum(T0,recall,latency,qps,T1,T2,T3 ranks) / 6",
        "reference": "http://www.x-bench.xin/ leaderboard retrieved 2026-09-30",
    }
    target = args.output or args.result.with_name("score.json")
    target.write_text(json.dumps(result, indent=2), encoding="utf-8")
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
