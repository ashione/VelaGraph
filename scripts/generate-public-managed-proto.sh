#!/usr/bin/env bash
# Optional maintainer tool. Normal builds use checked-in generated Rust, no protoc.
set -euo pipefail
root=$(cd "$(dirname "$0")/.." && pwd)
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
mkdir "$tmp/src"
cat > "$tmp/Cargo.toml" <<'MANIFEST'
[package]
name = "vela-public-proto-codegen"
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
        .out_dir(format!("{root}/crates/replicated-graph/src"))
        .compile_protos(&[format!("{root}/crates/replicated-graph/proto/public_managed.proto")], &[format!("{root}/crates/replicated-graph/proto")])?;
    Ok(())
}
RUST
cargo run --quiet --manifest-path "$tmp/Cargo.toml" -- "$root"
mv "$root/crates/replicated-graph/src/velagraph.public.managed.v1.rs" "$root/crates/replicated-graph/src/managed_gateway_public_proto.rs"

python3 - "$root/crates/replicated-graph/src/managed_gateway_public_proto.rs" <<'PYCODE'
import pathlib,sys
p=pathlib.Path(sys.argv[1]); s=p.read_text()
needle="tonic_prost::ProstCodec::default()"
assert s.count(needle)==12, "public RPC codec count changed; review the schema budget"
p.write_text(s.replace(needle,"crate::managed_gateway::grpc_codec::bounded::BoundedCodec::default()"))
PYCODE
rustfmt --edition 2021 "$root/crates/replicated-graph/src/managed_gateway_public_proto.rs"
