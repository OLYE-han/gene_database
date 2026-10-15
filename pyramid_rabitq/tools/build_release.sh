#!/usr/bin/env bash
set -euo pipefail

project_dir=$(cd "$(dirname "$0")/.." && pwd)
release_dir=${1:-"$project_dir/dist/release"}
dependency_mode=${2:-"--app-only"}
mkdir -p "$release_dir/wheelhouse" "$release_dir/config" "$release_dir/deploy"
find "$release_dir/wheelhouse" -maxdepth 1 -type f -name 'pyramid_rabitq-*.whl' -delete
if [[ "$dependency_mode" == "--with-deps" ]]; then
  python3 -m pip wheel "$project_dir" --wheel-dir "$release_dir/wheelhouse"
else
  python3 -m pip wheel "$project_dir" --no-deps --no-build-isolation --wheel-dir "$release_dir/wheelhouse"
fi
cp "$project_dir/config/service.example.yaml" "$project_dir/config/build.example.yaml" "$release_dir/config/"
cp "$project_dir/deploy/"*.sh "$release_dir/deploy/"
cp "$project_dir/tools/install_kaffpa.sh" "$release_dir/deploy/"
cp "$project_dir/README.md" "$release_dir/"
chmod +x "$release_dir/deploy/"*.sh
tar -C "$release_dir" -czf "$release_dir/../pyramid-rabitq-node.tar.gz" .
sha256sum "$release_dir/../pyramid-rabitq-node.tar.gz" >"$release_dir/../pyramid-rabitq-node.tar.gz.sha256"
echo "$release_dir/../pyramid-rabitq-node.tar.gz"
