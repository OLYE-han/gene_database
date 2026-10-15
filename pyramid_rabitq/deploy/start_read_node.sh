#!/usr/bin/env bash
set -euo pipefail

install_dir=$(cd "$(dirname "$0")" && pwd)
config=${1:-"$install_dir/service.yaml"}
pid_file="$install_dir/run/read-node.pid"
log_file="$install_dir/logs/read-node.log"
mkdir -p "$install_dir/run" "$install_dir/logs"

export OMP_NUM_THREADS=${OMP_NUM_THREADS:-1}
export OPENBLAS_NUM_THREADS=${OPENBLAS_NUM_THREADS:-1}
export MKL_NUM_THREADS=${MKL_NUM_THREADS:-1}
export NUMEXPR_NUM_THREADS=${NUMEXPR_NUM_THREADS:-1}

if [[ -f "$pid_file" ]] && kill -0 "$(<"$pid_file")" 2>/dev/null; then
  echo "read node already running with PID $(<"$pid_file")" >&2
  exit 1
fi

nohup "$install_dir/venv/bin/pyramid-rabitq-server" --config "$config" >>"$log_file" 2>&1 &
pid=$!
echo "$pid" >"$pid_file"
echo "started read node PID $pid; log: $log_file"
