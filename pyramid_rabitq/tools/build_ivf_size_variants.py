#!/usr/bin/env python3
"""Build IVF-size experiment versions while reusing one fixed Pyramid partition."""

from __future__ import annotations

import argparse
import json
import os
import shutil
import time
from pathlib import Path

import h5py
import numpy as np

from pyramid_rabitq.rabitq import RaBitQIVFIndex


def list_statistics(offsets: np.ndarray) -> dict[str, float | int]:
    sizes = np.diff(offsets).astype(np.int64)
    return {
        "lists": int(len(sizes)),
        "mean": float(np.mean(sizes)),
        "p50": float(np.percentile(sizes, 50)),
        "p95": float(np.percentile(sizes, 95)),
        "maximum": int(np.max(sizes)),
        "minimum": int(np.min(sizes)),
        "empty": int(np.count_nonzero(sizes == 0)),
    }


def build_variant(
    index_root: Path,
    base_version: str,
    target_vectors_per_list: int,
    train_size: int,
    batch_size: int,
) -> Path:
    versions = index_root / "versions"
    base = versions / base_version
    version = f"ivf-size-{target_vectors_per_list}"
    output = versions / version
    if output.exists():
        print(f"skip existing {output}", flush=True)
        return output
    output.mkdir(parents=True)
    started = time.time()
    try:
        for name in ("router",):
            shutil.copytree(base / name, output / name)
        for name in ("node_weights.npy", "source_manifest.json"):
            shutil.copy2(base / name, output / name)

        base_manifest = json.loads((base / "manifest.json").read_text(encoding="utf-8"))
        partition_records = []
        nlists = []
        for partition in base_manifest["partitions"]:
            part = int(partition["partition_id"])
            source_part = base / "shards" / f"partition_{part:05d}"
            target_part = output / "shards" / f"partition_{part:05d}"
            target_part.mkdir(parents=True)
            vector_path = target_part / "vectors.h5"
            os.symlink((source_part / "vectors.h5").resolve(), vector_path)
            with h5py.File(vector_path, "r") as handle:
                rows = int(len(handle["embeddings"]))
            nlist = max(1, int(round(rows / target_vectors_per_list)))
            effective_train_size = min(rows, max(train_size, 39 * nlist))
            print(
                f"{version} partition={part} rows={rows} nlist={nlist} "
                f"train={effective_train_size}",
                flush=True,
            )
            index = RaBitQIVFIndex.build_h5(
                vector_path,
                target_part / "rabitq",
                nlist=nlist,
                metric=base_manifest["config"]["metric"],
                seed=int(base_manifest["config"]["seed"]) + part,
                train_size=effective_train_size,
                batch_size=batch_size,
                scan_block_size=int(base_manifest["config"]["ivf"]["scan_block_size"]),
            )
            nlists.append(nlist)
            partition_records.append(
                {
                    "partition_id": part,
                    "vectors": rows,
                    "nlist": nlist,
                    "list_sizes": list_statistics(index.offsets),
                }
            )

        manifest = base_manifest
        manifest["version"] = version
        manifest["created_at"] = time.strftime("%Y-%m-%dT%H:%M:%S%z")
        manifest["build_seconds"] = time.time() - started
        manifest["target_vectors_per_list"] = target_vectors_per_list
        manifest["rabitq_code_layout"] = "fastscan32"
        manifest["config"]["ivf"]["nlist"] = int(round(np.mean(nlists)))
        manifest["partitions"] = partition_records
        (output / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    except BaseException:
        shutil.rmtree(output)
        raise
    return output


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--index-root", type=Path, required=True)
    parser.add_argument("--base-version", default="ivf-size-64")
    parser.add_argument("--sizes", nargs="+", type=int, default=[128, 256, 512, 1024, 2048])
    parser.add_argument("--train-size", type=int, default=20000)
    parser.add_argument("--batch-size", type=int, default=512)
    args = parser.parse_args()
    for size in args.sizes:
        print(build_variant(args.index_root, args.base_version, size, args.train_size, args.batch_size))


if __name__ == "__main__":
    main()
