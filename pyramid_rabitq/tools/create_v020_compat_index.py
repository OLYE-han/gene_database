#!/usr/bin/env python3
"""Clone an index into the row-major format accepted by the real v0.2 wheel."""

from __future__ import annotations

import argparse
import json
import os
import shutil
from pathlib import Path

import numpy as np

from pyramid_rabitq.rabitq import FASTSCAN_BLOCK_SIZE


def convert(source_root: Path, target_root: Path) -> None:
    if target_root.exists():
        raise FileExistsError(target_root)
    source_version = (source_root / "CURRENT").read_text(encoding="utf-8").strip()
    source = source_root / "versions" / source_version
    target_version = "v0.2.0-wheel"
    target = target_root / "versions" / target_version
    target.parent.mkdir(parents=True)
    shutil.copytree(source, target)
    try:
        for index_path in sorted((target / "shards").glob("partition_*/rabitq")):
            metadata_path = index_path / "metadata.json"
            metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
            count = int(metadata["vectors"])
            fast_path = index_path / "codes_fastscan.npy"
            if fast_path.exists():
                fast = np.load(fast_path, mmap_mode="r", allow_pickle=False)
                width = fast.shape[1]
                rows = np.lib.format.open_memmap(
                    index_path / "codes.npy", mode="w+", dtype=np.uint8, shape=(count, width)
                )
                for start in range(0, count, FASTSCAN_BLOCK_SIZE):
                    stop = min(start + FASTSCAN_BLOCK_SIZE, count)
                    rows[start:stop] = np.asarray(
                        fast[start // FASTSCAN_BLOCK_SIZE, :, : stop - start]
                    ).T
                rows.flush()
                del rows
                del fast
                fast_path.unlink()
            metadata["format_version"] = 1
            # v0.2 ignores unknown metadata, but keeping its original schema
            # makes the compatibility boundary explicit and testable.
            metadata.pop("code_layout", None)
            metadata_path.write_text(json.dumps(metadata, indent=2), encoding="utf-8")
        manifest_path = target / "manifest.json"
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        manifest["version"] = target_version
        hnsw = manifest["config"]["hnsw"]
        for key in (
            "sample_ratio", "partition_backend", "kaffpa_executable",
            "kaffpa_preconfiguration", "require_kaffpa",
        ):
            hnsw.pop(key, None)
        ivf = manifest["config"]["ivf"]
        if ivf.get("nlist") is None:
            nlists = {int(item["ivf_nlist"]) for item in manifest["partitions"]}
            if len(nlists) != 1:
                raise ValueError("v0.2 requires one shared IVF nlist across partitions")
            ivf["nlist"] = nlists.pop()
        ivf.pop("target_vectors_per_list", None)
        manifest.pop("rabitq_code_layout", None)
        manifest["implementation_profile"] = {
            "name": "v0.2.0-wheel",
            "basis": "v0.2 schema and row-major RaBitQ layout",
            "note": "payload cloned from the selected historical profile; served by the unmodified v0.2.0 wheel",
        }
        manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
        temporary = target_root / "CURRENT.tmp"
        temporary.write_text(target_version + "\n", encoding="utf-8")
        os.replace(temporary, target_root / "CURRENT")
    except BaseException:
        shutil.rmtree(target_root)
        raise


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-index", type=Path, required=True)
    parser.add_argument("--target-index", type=Path, required=True)
    args = parser.parse_args()
    convert(args.source_index.resolve(), args.target_index.resolve())
    print(args.target_index.resolve())


if __name__ == "__main__":
    main()
