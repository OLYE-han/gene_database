#!/usr/bin/env bash
set -euo pipefail

project_dir=$(cd "$(dirname "$0")/.." && pwd)
prefix=${1:-"$project_dir/.local/kahip-v3.25"}
jobs=${NCORES:-8}

kahip_tag=v3.25
kahip_commit=2e86d3da2065bcf6ddbf6fb84d2b52f33db1061f
archive_sha256=ec185267ca1c152a6bdd125df887eed651f0c05efea7ad8fb70abdd4381cea65
archive_url="https://codeload.github.com/KaHIP/KaHIP/tar.gz/$kahip_commit"

archive="$prefix/source.tar.gz"
source_dir="$prefix/source"
build_dir="$prefix/build"
binary="$prefix/bin/kaffpa"
provenance="$prefix/share/pyramid-rabitq/kaffpa-provenance.json"

if [[ -x "$binary" && -f "$provenance" ]]; then
    echo "$binary"
    exit 0
fi

for program in curl sha256sum tar cmake; do
    if ! command -v "$program" >/dev/null 2>&1; then
        echo "required build tool is missing: $program" >&2
        exit 1
    fi
done

mkdir -p "$prefix" "$source_dir" "$build_dir" "$prefix/bin" "$(dirname "$provenance")"
if [[ ! -f "$archive" ]]; then
    curl -L --fail --retry 3 --output "$archive" "$archive_url"
fi
echo "$archive_sha256  $archive" | sha256sum -c -

if [[ ! -f "$source_dir/CMakeLists.txt" ]]; then
    tar -xzf "$archive" --strip-components=1 -C "$source_dir"
fi

cmake -S "$source_dir" -B "$build_dir" \
    -DCMAKE_BUILD_TYPE=Release \
    -DNOMPI=ON \
    -DPARHIP=OFF \
    -DUSE_ILP=OFF \
    -DUSE_TCMALLOC=OFF
cmake --build "$build_dir" --target kaffpa --parallel "$jobs"
install -m 0755 "$build_dir/kaffpa" "$binary"
install -m 0644 "$source_dir/LICENSE" "$prefix/share/pyramid-rabitq/KaHIP-LICENSE"

binary_sha256=$(sha256sum "$binary" | awk '{print $1}')
printf '%s\n' \
    '{' \
    '  "implementation": "KaHIP/KaFFPa",' \
    "  \"tag\": \"$kahip_tag\"," \
    "  \"commit\": \"$kahip_commit\"," \
    "  \"source_url\": \"$archive_url\"," \
    "  \"source_sha256\": \"$archive_sha256\"," \
    "  \"binary_sha256\": \"$binary_sha256\"" \
    '}' >"$provenance"

echo "$binary"
