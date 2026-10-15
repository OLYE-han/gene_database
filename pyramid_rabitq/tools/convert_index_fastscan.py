#!/usr/bin/env python3
"""Copy an immutable v1 index version and convert its RaBitQ codes to FastScan."""

from __future__ import annotations

import argparse
import json
import os
import shutil
from pathlib import Path

from pyramid_rabitq.rabitq import RaBitQIVFIndex


def convert(index_root: Path, source_version: str, target_version: str, activate: bool) -> Path:
    versions = index_root / "versions"
    source = versions / source_version
    target = versions / target_version
    if not source.is_dir():
        raise FileNotFoundError(f"source index version does not exist: {source}")
    if target.exists():
        raise FileExistsError(f"target index version already exists: {target}")

    shutil.copytree(source, target)
    try:
        for rabitq_path in sorted((target / "shards").glob("partition_*/rabitq")):
            RaBitQIVFIndex.convert_path_to_fastscan(rabitq_path)
        manifest_path = target / "manifest.json"
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        manifest["version"] = target_version
        manifest["rabitq_code_layout"] = "fastscan32"
        manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    except BaseException:
        shutil.rmtree(target)
        raise

    if activate:
        temporary = index_root / ".CURRENT.fastscan.tmp"
        temporary.write_text(target_version + "\n", encoding="utf-8")
        os.replace(temporary, index_root / "CURRENT")
    return target


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--index-root", type=Path, required=True)
    parser.add_argument("--source-version", required=True)
    parser.add_argument("--target-version", required=True)
    parser.add_argument("--activate", action="store_true")
    args = parser.parse_args()
    print(convert(args.index_root, args.source_version, args.target_version, args.activate))


if __name__ == "__main__":
    main()
