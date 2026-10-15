#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 4 ]]; then
  echo "usage: $0 RELEASE_TAR HOSTS_FILE REMOTE_INSTALL_DIR SERVICE_CONFIG [INSTALL_MODE]" >&2
  exit 2
fi

release_tar=$(cd "$(dirname "$1")" && pwd)/$(basename "$1")
hosts_file=$2
remote_dir=$3
service_config=$(cd "$(dirname "$4")" && pwd)/$(basename "$4")
install_mode=${5:---shared-env}

while IFS= read -r host; do
  [[ -z "$host" || "$host" == \#* ]] && continue
  echo "deploying $host"
  ssh "$host" mkdir -p "$remote_dir/release"
  scp "$release_tar" "$host:$remote_dir/pyramid-rabitq-node.tar.gz"
  scp "$service_config" "$host:$remote_dir/service.yaml"
  ssh "$host" tar -xzf "$remote_dir/pyramid-rabitq-node.tar.gz" -C "$remote_dir/release"
  ssh "$host" "$remote_dir/release/deploy/install_node.sh" \
    "$remote_dir/release" "$remote_dir/node" "$install_mode"
  ssh "$host" cp "$remote_dir/service.yaml" "$remote_dir/node/service.yaml"
  echo "$host installed; start with: ssh $host $remote_dir/node/start_read_node.sh"
done <"$hosts_file"

