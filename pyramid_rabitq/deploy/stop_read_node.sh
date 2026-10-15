#!/usr/bin/env bash
set -euo pipefail

install_dir=$(cd "$(dirname "$0")" && pwd)
pid_file="$install_dir/run/read-node.pid"
if [[ ! -f "$pid_file" ]]; then
  echo "read node is not running"
  exit 0
fi
pid=$(<"$pid_file")
if kill -0 "$pid" 2>/dev/null; then
  kill "$pid"
  for _ in $(seq 1 30); do
    kill -0 "$pid" 2>/dev/null || break
    sleep 1
  done
fi
rm -f "$pid_file"
echo "read node stopped"

