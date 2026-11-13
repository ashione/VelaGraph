# D2 transport resource limits and trust boundary

Status: bounded prototype transports, **not production-secure services**.
Process-node Raft and driver/control listeners now both use typed gRPC, through
separate services. The type name `TcpNode` remains the process lifecycle API.

- [Raft transport](RAFT_GRPC.md): Vote, Append and InstallSnapshot, peer deadlines,
  payload accounting, batching hints and known large-entry boundary.
- [Driver/control transport](CONTROL_GRPC.md): the same `--control` port and
  `send_control`/`d2-ctl` operations, without legacy JSON fallback.

The former newline JSON frame reader/writer is replaced, not retained as an
alternate production wire. Its logical byte accounting and conservative control
write allowance are preserved where documented; they are not JSON-in-protobuf.
Typed messages add bounded pre-decode allocation checks and reuse tracked
admission/deadline/connection shutdown. Stored JSON log/snapshot formats are
independent and unchanged. These limits do not establish a whole-process RSS cap.
The separate same-host UDS gateway has been retired (#262).

## Shared operational boundary

Both ports are unauthenticated plaintext for trusted isolated development
networks only. Membership and raw control writes are privileged operations;
loopback does not authenticate callers. No timeout proves a mutation did not
commit, and no transport cancellation rolls back Raft work already submitted.
Do not expose either endpoint publicly or automatically retry non-idempotent
control writes. Production authentication/authorization, encryption and address
policy require a separate design.

The process, slow-peer, resource and shutdown/reopen suites remain required.
Tests must fail rather than skip when CI cannot provide their sockets. Any
remaining old transport is listed explicitly in the [inventory](INTERNAL_GRPC_MANAGED.md#remaining-communication-inventory).

## Bounded durable-log catch-up reads

The durable log reader's `limited_get_log_entries(start, end)` returns an
ordered prefix of the exclusive range `[start, end)`, limited to 256 complete
entries and 1 MiB of stored JSON bytes. Small entries remain batched rather
than forcing one RPC per entry. A first available entry larger than 1 MiB is
returned alone and intact so the soft byte allowance cannot prevent progress.
An empty or reversed range returns no entries; index arithmetic does not add
to `end` or overflow at `u64::MAX`. Normal unrestricted log reads are unchanged.

The log mutex protects selection and copying of the prefix. JSON decoding then
runs outside that mutex over owned bytes, so vote/append operations need not
wait for the complete batch's decode. The raw RocksDB iterator inspects the
next value's length before copying it and stops before copying a value that
would exceed the allowance. This does not bound RocksDB's internal read/cache
allocations or make native I/O preemptible. There can still be a large single
entry, and decoding/encoding/sending/applying that entry may exceed an RPC
budget. Transport framing, admission, timeouts and election settings are not
changed, nor is the storage format.

This bounds aggregate catch-up work, addressing a diagnosed large-backlog
feedback loop. It is not a guarantee of failover/recovery latency or a complete
solution to large-graph availability. Tests cover exact byte/count boundaries,
oversized-entry progress, index extremes, corrupt selected data, immutable
copied prefixes, reopen and small-entry batching. Large-entry and sustained
replication results must be reported separately from these deterministic
storage tests.

The durable-cluster regression also stops one follower, commits 600 small
entries plus twelve 100 KiB payload entries on the quorum, restarts the follower,
checks all 612 values after catch-up without a snapshot, then reopens it again
and checks persistent progress. This is real Raft with an in-process transport;
it is not a TCP latency benchmark or arbitrary-backlog guarantee.
