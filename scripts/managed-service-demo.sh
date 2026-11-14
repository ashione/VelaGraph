#!/usr/bin/env bash
# Fresh loopback demonstration only. Never restore or reuse an existing directory.
set -euo pipefail
if [[ $# -ne 1 ]]; then
  echo "usage: $0 NEW_DATA_DIRECTORY" >&2
  exit 2
fi
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$root"
command -v python3 >/dev/null
cargo build --locked -p replicated-graph --example managed_service_demo --bin graph-service >&2
target="$(cargo metadata --locked --no-deps --format-version 1 | python3 -c 'import json,sys; print(json.load(sys.stdin)["target_directory"])')"
exec "$target/debug/examples/managed_service_demo" --data-dir "$1" --service-bin "$target/debug/graph-service" --allow-insecure-development
