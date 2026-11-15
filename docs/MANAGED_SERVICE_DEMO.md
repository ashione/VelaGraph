# Fresh loopback managed-service demo

This source-built development demo creates a **new**, fixed cohort: one
single-voter coordinator, two single-voter graph shards, and a separate public
HTTP/gRPC service process. It reuses the existing managed activation commands,
execution engine and typed transports. It is not production provisioning, HA,
restore, or a reusable existing-cluster activation command.

## Start

Install the Rust/native dependencies in CONTRIBUTING.md and Python 3, then:

```sh
bash scripts/managed-service-demo.sh /tmp/velagraph-first-demo
```

The directory must **not exist**, even as an empty directory. No old data is
deleted, overwritten, reactivated or automatically restored. Build output goes to
stderr. After readiness checks, stdout emits one JSON record with `http`,
`grpc`, `pid` and `node_pids`. Both endpoints bind loopback ephemeral ports;
copy the reported HTTP address into the examples below. The gRPC endpoint uses
`public_managed.proto`, not the HTTP JSON representation.

## Write and query

In another terminal, replace PORT with the announced HTTP port:

```sh
curl --fail-with-body -sS http://127.0.0.1:PORT/preview1/writes:submit \
  -H 'content-type: application/json' \
  -d '{"external_id":"01010101010101010101010101010101","mutation":{"type":"create_vertex","partition":7,"vertex":{"id":"1","labels":["person"],"properties":{}}}}'

curl --fail-with-body -sS http://127.0.0.1:PORT/preview1/queries:open \
  -H 'content-type: application/json' \
  -d '{"read":{"type":"minimum","version":"1"},"ttl_millis":60000,"query":{"partition":7,"text":"g.V(1)"},"page_size":10}'
```

A timed-out write is unknown, not rolled back. Recover its original ID and exact
mutation using `/preview1/writes:status`; do not generate a new ID to blindly
retry. Query responses carry their fixed version and owner-bound session.
See [HTTP contract](PUBLIC_HTTP_PREVIEW.md) for Fetch/Close and errors and
[public gRPC](PUBLIC_GRPC_SERVICE.md) for the typed equivalent.

Press Ctrl-C in the first terminal to stop the service and nodes. The directory
and logs remain for diagnosis. A subsequent invocation with the same directory
is rejected: this demo intentionally does **not** restart or restore those nodes.
Use a new directory for another demo. Never remove data needed for investigation.

## Evidence and limitations

The dedicated managed-service-demo CI job builds this exact example plus
`graph-service`, then runs the real-process
`fresh_demo_write_query_reject_existing_and_stop_children` test. It verifies
fresh startup, HTTP write / gRPC query, original data after rejected duplicate
startup, and child cleanup on interruption. Ordinary `cargo test` does not run
this explicitly ignored release gate.

```sh
cargo build --locked -p replicated-graph --example managed_service_demo --bin graph-service
VELAGRAPH_MANAGED_DEMO_BIN="$PWD/target/debug/examples/managed_service_demo" \
  cargo test --locked -p replicated-graph --test managed_service_demo_process \
  fresh_demo_write_query_reject_existing_and_stop_children -- --ignored --exact --nocapture
```

The example uses fixed shards 7/8 and coordinator 9 solely as demo configuration,
not a query-engine special case. The listeners are unauthenticated plaintext.
Single-voter groups cannot survive node loss; retained-history and query budgets
remain unchanged. Service startup is not a promise that later operations cannot
fail, and this evidence is not global recovery or multi-host performance proof.
Release scope remains tracked by #224; passing this gate alone is not v0.0.1.
