#!/usr/bin/env bash
# Run from repository root. Keep outputs; never deletes user data or prior runs.
set -euo pipefail
out=${1:-corehr-results}
if [[ -e "$out" ]]; then echo "Refusing to overwrite $out" >&2; exit 1; fi
mkdir -p "$out"
out=$(cd "$out" && pwd)
cargo build --locked --example corehr_fixture
cargo build --locked -p replicated-graph --example corehr_run
node_profile=${COREHR_NODE_PROFILE:-release}
case "$node_profile" in
  release) cargo build --release --locked -p replicated-graph --bin graph-node ;;
  debug) cargo build --locked -p replicated-graph --bin graph-node ;;
  *) echo "COREHR_NODE_PROFILE must be release or debug" >&2; exit 1 ;;
esac
target=${CARGO_TARGET_DIR:-target}
"$target/debug/examples/corehr_fixture" "$out/smoke-fixture" 2000 50 > "$out/smoke-fixture.jsonl"
"$target/debug/examples/corehr_run" "$out/smoke-fixture" "$out/smoke-db" "$target/$node_profile/graph-node" > "$out/smoke.jsonl"
"$target/debug/examples/corehr_fixture" "$out/full-fixture" 200000 5000 > "$out/full-fixture.jsonl"
"$target/debug/examples/corehr_run" "$out/full-fixture" "$out/full-db" "$target/$node_profile/graph-node" > "$out/full.jsonl"
