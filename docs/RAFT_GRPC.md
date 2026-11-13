# Raft transport v1: typed gRPC

Tracking: [#238](https://github.com/ashione/VelaGraph/issues/238), part of [#218](https://github.com/ashione/VelaGraph/issues/218).

`TcpNode` now runs **Raft peer traffic** using the versioned
`velagraph.internal.raft.v1.RaftService`: Vote, Append and InstallSnapshot.
The lifecycle type retains its existing name; the independent `d2-ctl` control
port now uses its separate [typed control service](CONTROL_GRPC.md). This is not completion of all internal transport
migration. The in-process registry remains a deterministic testing transport.

## Contract

- Tonic/HTTP2 with generated protobuf requests and responses, not a JSON bytes
  tunnel. Vote/log IDs, joint memberships, all log-command variants and D1
  commands are typed. Snapshot chunks are deliberately opaque **existing
  state-machine snapshot bytes**, preserving the snapshot format.
- `RaftNetworkFactory` creates a reusable channel per peer. No application
  transparent retry or fallback: OpenRaft owns election/replication retry.
- Every request includes protocol version 1 and the expected destination node
  ID. Wrong target/version fails before calling Raft. IDs are not authentication.
- Durable log, vote, command identity, history hash and snapshot encodings are
  unchanged. Raft conversion preserves application-invalid legacy commands so
  the state machine can reject them after logging. Types whose original serde
  contract already required validation (Journal, HistoryCursor/Page and
  RetainedPrepare) keep that validation.
- Append success/conflict/higher-vote/partial match and snapshot stream mismatch
  remain typed results. Stopped/storage-failed peers return sanitized Unavailable;
  OpenRaft treats transport failure as unreachable, never as a successful append.

## Admission and lifecycle

64 MiB per protobuf message; a 512 MiB conservative decoded-allocation budget and
64 nesting levels, checked before prost allocates repeated messages. This is a
transport allocation budget (not an arbitrary count of fields), **not a graph size limit**. Oversized aggregate
Append requests return OpenRaft's `PayloadTooLarge` split hint. An indivisible
oversized command cannot be replicated; applications must remain within their
existing mutation limits. Snapshot chunks retain OpenRaft's existing chunking.

Per listener: four admitted requests spanning decoding, execution and response
body consumption, four concurrent HTTP/2 streams per connection, and 256 open
connections. Per process: 16 outbound calls. Exhaustion rejects rather than
creating an unbounded queue. These are explicit resource limits, not a claim of
unlimited cluster scale; deployment-configurable budgets are future work.

The remaining OpenRaft RPC budget is capped at three seconds and sent as gRPC
`grpc-timeout`; client connection/call/decode and server handling are bounded.
Shutdown stops the listener and cancels accepted sockets so slow or idle peers
cannot keep server-owned storage alive. Plaintext, trusted-network development
only; this does not enable authentication, public exposure or production TLS.

## Upgrade and rollback

There is **no mixed JSON-Raft/gRPC-Raft cluster support**. Stop the whole
experimental cluster before upgrading its binaries, keep the same node identities,
addresses and durable directories, then restart all voters. Old newline clients
cannot call the new gRPC listener; the versioned service does not downgrade.
Rolling upgrade/capability negotiation is not delivered by this slice.

Because disk formats are unchanged, binary rollback has no transport-specific
storage migration, but still requires stopping the entire cluster and using
compatible application/storage versions. Never run old and new transport voters
concurrently and assume they form a quorum.

## Verification

- Existing three-independent-process replication, leader kill and durable restart
  test now runs through gRPC Raft.
- Socket tests cover snapshot chunk transfer/mismatch/install and wrong
  version/target rejection.
- DTO tests round-trip all 17 D1 command variants and preserve canonical durable
  JSON identity; application rejection and malformed/payload-budget tests remain.
- Existing lifecycle flood, cancelled shutdown and byte-limit checks remain.

No benchmark claim: transport correctness does not prove improved throughput or
latency. Cold/warm latency, CPU/RSS and wire-byte comparison remain separate work.

### Large-entry latency is a separate limit

The raw legacy 131,072-operation single-entry test verifies receipt and apply
with an explicit three-second RPC budget. It is not a quorum-throughput claim.
The normal process configuration supplies a 100 ms OpenRaft append deadline;
that large debug-build workload did not complete quorum submission within the
test's 30 seconds. The transport migration does not relax that deadline or claim
to fix large indivisible-entry latency. Standard D1 mutation limits and external
APIs remain the supported application path; direct raw Raft callers must enforce
both transport capacity and viable RPC latency before submitting a command.
