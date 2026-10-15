#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 2 ]]; then
  echo "usage: $0 RELEASE_DIR INSTALL_DIR [--offline|--shared-env]" >&2
  exit 2
fi

release_dir=$(cd "$1" && pwd)
install_dir=$2
mode=${3:-}
python_bin=${PYTHON_BIN:-python3}

mkdir -p "$install_dir" "$install_dir/logs" "$install_dir/run"
venv_args=()
if [[ "$mode" == "--shared-env" ]]; then
  venv_args+=(--system-site-packages)
fi
"$python_bin" -m venv "${venv_args[@]}" "$install_dir/venv"
pip_args=(--upgrade --find-links "$release_dir/wheelhouse")
if [[ "$mode" == "--offline" || "$mode" == "--shared-env" ]]; then
  pip_args+=(--no-index)
fi
if [[ "$mode" == "--shared-env" ]]; then
  pip_args+=(--no-deps)
fi
"$install_dir/venv/bin/python" -m pip install "${pip_args[@]}" pyramid-rabitq
"$install_dir/venv/bin/python" - <<'PY'
import faiss, fastapi, h5py, numpy, scipy, uvicorn, yaml
import pyramid_rabitq
from pyramid_rabitq.rabitq import FASTSCAN_AVAILABLE, FASTSCAN_BACKEND, NATIVE_SCAN_AVAILABLE
if not NATIVE_SCAN_AVAILABLE or not FASTSCAN_AVAILABLE:
    raise RuntimeError("native RaBitQ FastScan extension is missing from this installation")
print(
    "dependency check passed",
    pyramid_rabitq.__version__,
    "native_scan=true",
    f"fastscan_backend={FASTSCAN_BACKEND}",
)
PY
cp "$release_dir/config/service.example.yaml" "$install_dir/service.yaml.example"
cp "$release_dir/deploy/start_read_node.sh" "$release_dir/deploy/stop_read_node.sh" \
  "$release_dir/deploy/health_check.sh" "$install_dir/"
chmod +x "$install_dir"/*.sh
echo "installed in $install_dir; copy service.yaml.example to service.yaml and edit index_root"
