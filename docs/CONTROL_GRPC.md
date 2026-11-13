# Process-node driver/control transport v1

Tracking: [#251](https://github.com/ashione/VelaGraph/issues/251), part of [#218](https://github.com/ashione/VelaGraph/issues/218).

The existing process-node `--control` endpoint and `d2-ctl`/`send_control` use
versioned typed gRPC/Protobuf. This replaces newline JSON on that same endpoint;
there is no protocol negotiation, legacy fallback or second compatibility
listener. Upgrade the node binary and control clients together. Raft peer traffic
uses its separate [Raft service](RAFT_GRPC.md). Neither migration changes durable
records, checkpoint/recovery or the consensus algorithm.

## Operations and semantics

The transport dispatches the original control handlers:

- `Status`: node, leader and applied/log position.
- `Read`: leader-barrier (linearizable) read; `Get`: local state-machine read,
  deliberately not a linearizable read.
- `Write`: existing raw key/value test-driver write.
- `Initialize`, `AddLearner`, `ChangeMembership`: existing membership paths.
- `D1Capacity`, `D1SetCapacity`: existing D1 capacity query/change semantics,
  preserving structured success and application-error variants.

No JSON is embedded in protobuf fields. Missing operations, wrong versions,
malformed nested values and invalid collection shapes fail before dispatch.
The public Rust `ControlRequest`/`ControlResponse` domain types and CLI commands
remain, but their old network serialization is not accepted.

A timeout, lost reply or shutdown after dispatch is an **unknown outcome** for a
write or membership change. Cancellation does not undo a submitted Raft command.
`Write` has no caller idempotency key; do not automatically replay it. There is no
transparent application retry, leader forwarding or endpoint probing.

## Resource boundary

The migration retains the three-second total RPC budget, four admitted incoming
control operations and sixteen outbound exchanges per process. Protobuf wire
length and pre-decode allocation are bounded; admission includes decoding,
execution, encoding and response delivery rather than ending at handler return.
HTTP/2 multiplexing and flow control do not replace these application budgets.

The historical 64 MiB logical frame allowance (including its former LF byte) and
conservative control-write limit `(64 MiB - 1024) / 4` are preserved. Legacy JSON
size is internal accounting only, not a wire payload; compact protobuf cannot
silently admit a larger logical result. These ceilings are not whole-process RSS
limits. A separate 64 MiB pre-decode allocation budget conservatively accounts
for decoded collections (including 256 bytes per initialized member). Extremely
large collections of tiny members can therefore be rejected before reaching the
old logical frame ceiling; fitting an old frame is not a guarantee of acceptance.
This allocation budget and the logical/wire limits must all pass.
Synchronous storage, already submitted Raft work and allocator overhead
retain their separate limits. No speedup follows merely from using protobuf.

Shutdown closes listeners and tracked connections/handlers before Raft teardown.
Incomplete/slow clients must not keep a stopped node's database alive. Deadline
and cancellation tests cover both incomplete requests and response delivery.

## Trust and compatibility

This is still **unauthenticated plaintext for trusted isolated development
networks**, not production administration. Every reachable client can perform
all listed operations, including membership changes; supplied member addresses
can cause outbound connections. Loopback is not authentication. Preserve the
existing `--allow-insecure-development` guard and protect both peer and control
ports. The migration adds neither TLS nor authorization and does not relax any
OS/network setting.

Legacy newline JSON clients fail explicitly. All node/control clients in an
experiment must use the new binary; do not rely on mixed wire versions. The
in-process deterministic test transport and stored data formats are unchanged.
Remaining single-shard GraphService Probe/Metadata/Write envelopes and legacy
graph/library entrypoints are still tracked in #218. The separate Unix read
gateway is retired (#262). Typed snapshot/query services
do not by themselves migrate those mutation/probe envelopes.

## Verification

Codec tests cover typed command/result round trips, missing/invalid fields,
collection and allocation limits, and logical-budget boundaries. Socket tests
cover slow peers, outbound admission, old-protocol rejection and cleanup/reopen.
The existing multi-process cluster and D1 suites exercise actual control calls
through membership, writes/reads, failover, restart and capacity rejection.
These are correctness/resource checks, not a throughput or multi-host HA claim.
