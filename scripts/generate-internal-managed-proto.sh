#!/usr/bin/env bash
# Optional maintainer tool. Normal builds use checked-in generated Rust, no protoc.
set -euo pipefail
root=$(cd "$(dirname "$0")/.." && pwd)
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
mkdir "$tmp/src"
cat > "$tmp/Cargo.toml" <<'MANIFEST'
[package]
name = "vela-internal-managed-proto-codegen"
version = "0.0.0"
edition = "2021"
[workspace]
[dependencies]
tonic-prost-build = "=0.14.9"
protoc-bin-vendored = "=3.2.0"
MANIFEST
cat > "$tmp/src/main.rs" <<'RUST'
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let root = std::env::args().nth(1).expect("repository root");
    std::env::set_var("PROTOC", protoc_bin_vendored::protoc_bin_path()?);
    tonic_prost_build::configure()
        .out_dir(format!("{root}/crates/d2-raft/src/managed_rpc"))
        .compile_protos(&[format!("{root}/crates/d2-raft/proto/internal_managed.proto")], &[format!("{root}/crates/d2-raft/proto")])?;
    Ok(())
}
RUST
cargo run --quiet --manifest-path "$tmp/Cargo.toml" -- "$root"
mv "$root/crates/d2-raft/src/managed_rpc/velagraph.internal.managed.v1.rs" "$root/crates/d2-raft/src/managed_rpc/managed_proto.rs"

# Pre-decode limits apply symmetrically to server requests and client replies.
python3 - "$root/crates/d2-raft/src/managed_rpc/managed_proto.rs" <<'PYCODE'
import pathlib,sys
p=pathlib.Path(sys.argv[1]); s=p.read_text()
assert s.count("tonic_prost::ProstCodec::default()") == 2
p.write_text(s.replace("tonic_prost::ProstCodec::default()", "crate::managed_rpc::grpc::BoundedCodec::default()"))
PYCODE
rustfmt --edition 2021 "$root/crates/d2-raft/src/managed_rpc/managed_proto.rs"
