# RPC migration: current paths and remaining gates

Audited against main [40e2b9a](https://github.com/ashione/VelaGraph/commit/40e2b9a419ff941a58761f784c9c7fd500a6bf08), 2026-11-14. This is the current inventory for #218, not a claim of production readiness.

## Supported network paths

| Path | Implementation evidence | Boundary |
| --- | --- | --- |
| Raft and process control | `crates/d2-raft/src/tcp.rs:245–285` selects GrpcNetworkFactory and hands listeners to raft_grpc/control_grpc; send_control delegates to typed request | The TcpNode name and TCP sockets describe the underlying transport, not newline JSON |
| Managed coordinator/member | `managed_rpc.rs:589–609` constructs typed Transport; `managed_rpc/grpc.rs:61–78,498–539` converts protobuf and serves tonic | new_grpc is an alias, not a second compatibility mode |
| Graph library and graph-node | `crates/replicated-graph/src/remote.rs:243–294` binds shared GrpcSnapshotService | Includes original single-shot Snapshot and D1 managed-mode operations, not only default CLI |
| Observed coordinator | `remote/distributed/mod.rs:946–972` uses typed service/client | Observed consistency is unchanged; it is not managed fixed-V consistency |
| Gateway library | `managed_gateway.rs:1235–1250,1367` uses public typed service/client | Identified v2 library context; original public v1 remains separate |
| Public service | `managed_gateway_grpc.rs:565–604` registers Write, Query, FixedSession and ManagedQueryJob only | No internal Raft/restore/participant service is registered on this listener |
| Public HTTP | Hyper adapter, six preview write/query operations | Intentional external HTTP, not a legacy inter-node transport |

Production paths do not select or sniff a second JSON/TCP wire. Retired-wire rejection fixtures, cfg(test) frame helpers, HTTP listeners, in-process calls, storage I/O and object-store HTTP are not evidence of a production fallback.

## Bytes that must not be removed as “legacy JSON”

Typed messages deliberately retain existing storage/canonical bytes. Managed AdmitRequest.request is the exact canonical command identity; payload is the validated durable Plan. The codec maps the operation's protobuf oneof fields, not an opaque whole RPC request. See managed_gateway.rs:343–350, managed_graph.rs:91–108/657–721, request_plans.rs:71–85 and managed_rpc/grpc_codec.rs:1017–1063. Re-encoding these values to arbitrary protobuf would change retry/durable identity.

Raft snapshot data is an existing snapshot image carried with typed metadata/offset/done, not an RPC envelope. Storage record encodings, logical JSON size accounting and historical on-disk versions remain unchanged.

## Verified retirement evidence

#272/#273, #274/#275, #276/#277 and #278/#279 retire the managed-member, coordinator, Gateway and GraphService library paths respectively. The final graph slice was squash-merged as 40e2b9a. Tested synthetic merge 7d93f3ac and merged main have the same tree `2fb3f3e1ccb6b50800640f2d410ea29764fb2359`.

- [CI 43596727000](https://github.com/ashione/VelaGraph/actions/runs/43596727000): Format, Clippy and ordinary tests passed. Includes real typed processes, strict raw protocol rejection, fixed-view fault/concurrency cases, public listener isolation/no-fallback and restart.
- [CoreHR 43596727002](https://github.com/ashione/VelaGraph/actions/runs/43596727002): full-scale and retained 1025-write release lifecycle passed with exact-head artifact provenance.
- Independent static review passed the final candidate. No local compilation is claimed.

A passing later run does not identify the cause of earlier generic Unavailable failures; #247 remains open. Controlled cancellation fixtures prove RPC-stream cancellation, not necessarily physical TCP closure.

## Remaining #218 gates

1. **Generated Go interoperability:** this baseline contains no .go/go.mod or Go CI. Test the real Rust service with generated Go clients, write IDs/replay, typed values/presence, paging/fixed V, deadlines and public/internal isolation. Rust-to-Rust tests are not this evidence.
2. **Cross-language error details:** public codecs currently place custom ErrorDetail bytes in tonic Status::with_details (managed_gateway_grpc_codec.rs:487–495,1588–1595). Compatibility with standard rich-status clients must be verified; do not silently discard machine-readable detail to obtain a green smoke test.
3. **HTTP surface:** the preview adapter supports Submit/Status/Abort and Open/Fetch/Close only (managed_gateway_http_codec.rs:389–442). FixedSession and ManagedQueryJob HTTP parity and a stable HTTP contract are not delivered.
4. **Version/upgrade matrix:** existing services reject unsupported versions and old wire; Raft does not support a mixed JSON/gRPC cluster. This is not rolling-upgrade compatibility or an implemented online restart/recovery plan.
5. **Resource/performance evidence:** retain application admission, budgets and unknown write outcomes. Existing Metadata microbenchmarks are not end-to-end cold/warm, p50/p95/p99, CPU/RSS/network or multi-host scaling evidence.
6. **Service readiness:** trusted/loopback preview is intentional. #221/#222 routing/ownership/readiness and recovery/security/product gates remain distinct.

The next useful closure is real cross-language public service verification, including errors, rather than another transport alias or deleting durable serializers. Keep #218 open until its remaining gates have explicit evidence or an owner-approved scope change.
