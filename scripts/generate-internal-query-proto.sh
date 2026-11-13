#!/usr/bin/env bash
# Optional maintainer tool. Normal builds use checked-in generated Rust, no protoc.
set -euo pipefail
root=$(cd "$(dirname "$0")/.." && pwd)
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
mkdir "$tmp/src"
cat > "$tmp/Cargo.toml" <<'MANIFEST'
[package]
name = "vela-internal-query-proto-codegen"
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
        .out_dir(format!("{root}/crates/replicated-graph/src/remote/distributed/grpc"))
        .compile_protos(&[format!("{root}/crates/replicated-graph/proto/internal_query.proto")], &[format!("{root}/crates/replicated-graph/proto")])?;
    Ok(())
}
RUST
cargo run --quiet --manifest-path "$tmp/Cargo.toml" -- "$root"
mv "$root/crates/replicated-graph/src/remote/distributed/grpc/velagraph.internal.query.v1.rs" "$root/crates/replicated-graph/src/remote/distributed/grpc/query_proto.rs"

# Fixed proof and coordinator ingress collections must be bounded before prost allocates repeated fields.
# Keep this transformation in codegen, rather than editing generated Rust by hand.
python3 - "$root/crates/replicated-graph/src/remote/distributed/grpc/query_proto.rs" <<'PYCODE'
import pathlib, sys
p = pathlib.Path(sys.argv[1])
s = p.read_text()
marker = "pub mod fixed_query_service_client {"
head, tail = s.split(marker, 1)
assert tail.count("tonic_prost::ProstCodec::default()") == 16
replacement = "crate::remote::distributed::grpc::bounded_codec::BoundedCodec::default()"
p.write_text(head + marker + tail.replace("tonic_prost::ProstCodec::default()", replacement))
PYCODE
rustfmt --edition 2021 "$root/crates/replicated-graph/src/remote/distributed/grpc/query_proto.rs"
