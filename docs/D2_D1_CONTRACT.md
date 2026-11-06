# D2 replicated D1 application contract v1

This is a single-shard **application-state-machine slice**, not a distributed
graph database or an implementation of the synchronous `StorageNode` trait.
`GraphRecordStore` and `GraphEngine` are not wired through Raft. The independent
`d2-raft` crate has no dependency on the local D1 crate; opaque graph/v1 bytes
are unchanged. Transport security, routing, placement, bounded remote reads,
client deadlines/retry policy and cross-shard transactions remain separate work.

## Commands and replies

All new commands are wrapped in `LogCommand::D1V1(D1Command)`. Old `Noop` and
`TxnBatch` JSON remain readable and byte-compatible. The D1 command variants are:

- `Activate { shard, epoch }`: bind one Raft group to one shard. Activation is
  replicated, allowed only while the legacy data map is empty, and idempotent
  for the same current shard/epoch. It does not import or reinterpret legacy
  data. A different shard is rejected. There is no deactivation command.
- `SetEpoch { shard, expected_epoch, new_epoch }`: monotonically advance the
  ownership epoch using compare-and-set. Equal epochs are allowed; retrying an
  already-achieved target is harmless. Other mismatches fence the request.
  An epoch is not a Raft term: elections do not change the application epoch.
- `Write(D1WriteRequest)`: shard, epoch, 128-bit caller-managed request id,
  explicit `expected_sequence: Option<u64>`, and ordered opaque put/delete
  operations. A missing condition means unconditional, not a weaker retry.

After activation, **legacy `TxnBatch` is rejected**, including the old control
port's `write` command. It cannot bypass fencing or mutate data without updating
sequence/dedup. `Noop` and Raft membership entries remain valid and consume no
application sequence. The TCP control helper reports an application rejection
as an error instead of incorrectly returning a successful log index. It does
not expose a new D1 client API in this slice.

`ApplyResult.d1` is `Some(Result<D1Result, D1Error>)` for application commands,
and `None` for accepted legacy/no-op/membership entries. Rejected legacy writes
carry `Some(Err(LegacyWriteDisabled))`. The existing `value` field remains.
**A successful `raft.client_write` means the log entry applied, not that a D1
mutation succeeded. Callers must inspect the nested application result.** A
rejected command advances the Raft applied marker without changing data,
application sequence, or retry identity. Errors are typed and deterministic:
wrong shard, epoch fencing, sequence conflict, idempotency conflict, invalid
batch, capacity, legacy-mode boundary, and invalid epoch transition.

## Write ordering and retries

Within the serialized state-machine apply operation:

1. Require activation, check shard and exact current epoch, then check nonempty
   batch and size limits.
2. Find a retained request id. Identical **ordered operations and condition**
   return `Duplicate` with the original sequence and operation count. Different
   bytes/order/condition return `IdempotencyConflict`. This happens before
   comparing the condition to the current sequence.
3. For a new request, compare its condition (if present) to the current sequence.
   A mismatch returns `Conflict { expected, current }` and reserves no request id.
4. Check sequence overflow and retained-request capacity. Apply all operations
   in order, advance sequence once, and record the complete retry identity.
5. Atomically persist data operations, D1 metadata, membership, and the Raft
   applied marker in **one synchronous RocksDB WriteBatch**, then publish the
   new in-memory state. A storage failure is a Raft storage error, not an
   application rejection or success.

A lost response can be retried with the same id, operations and condition after
leader change, snapshot transfer or restart. It returns the original sequence,
not the latest one. A stale epoch still fences an exact duplicate. After refreshing
to the current epoch, the same id/operations/condition can return its old result;
epoch is a fence, not part of the retained operation identity. This matches local
D1 semantics. Epoch changes never clear retry records or reset sequence.

Rejected requests do not consume ids. A caller may re-read/revalidate a rejected
operation and submit a refreshed condition. An uncertain outcome must instead
be retried with the original identity before deciding whether it committed.
There is no implicit automatic retry and no wall-clock decision in apply.

Apply receives committed, ordered entries from openraft. Multiple entries in one
apply call have the same results and final state as individual calls. This API is
not a general-purpose way for callers to replay arbitrary out-of-order log entries.
`d1_metadata`, `get_raw` and `scan_raw` are local diagnostic views, not distributed
read guarantees. A caller using the leader must first ensure linearizability;
a complete read-view adapter remains future work.

## Bounds and explicit prototype limitations

A batch contains 1–10,000 operations and at most 16 MiB of key/value bytes.
Dedup retains at most 4,096 successful requests and an accounting budget of
64 MiB: raw key/value bytes + 64 bytes per operation + 128 bytes per request.
This bounds retained work even for empty keys/values; it is **not** a bound on
process RSS, JSON bytes or snapshot size. Invalid or oversized commands fail
without reserving an id. Capacity is checked only after duplicate lookup, so
existing retries continue to work when the budget is full.

There is **no eviction or administrative reset** in v1. The group fails closed
with `DedupCapacity` for further new writes when full. Epoch changes do not
reclaim this budget. Production use needs an explicit client-acknowledged
retention/garbage-collection protocol; silently evicting ids would weaken the
retry guarantee. This conservative limit deliberately bounds the experimental
slice rather than pretending to offer unlimited production throughput.

The state machine still clones its data map per apply and uses full logical
snapshots. It is a correctness prototype, not a production memory/latency design.
The test transport remains unauthenticated and has no production backpressure.
Process restart tests are not power-loss/WAL fault-injection evidence.

## Durable and snapshot format

The existing `sm/data/`, `sm/applied`, `sm/membership` and `sm/snapshot` keys are
unchanged. New `sm/d1` contains required JSON fields in declaration order:
`version` (=1), `shard`, `epoch`, `sequence`, and `dedup`. Each retained record
contains `request` (including its original epoch, condition and ordered bytes)
and its original `sequence`. Missing `sm/d1` means the legacy byte-batch mode;
unknown versions and malformed state fail reopen instead of being interpreted
as empty metadata. Activated state without an applied marker is rejected.

Legacy snapshots remain a JSON array of sorted `[key_bytes, value_bytes]` pairs
until activation. Activated snapshots have the exact envelope:

```json
{"version":1,"entries":[],"d1":{"version":1,"shard":7,"epoch":3,"sequence":0,"dedup":[]}}
```

The Raft `SnapshotMeta` continues to carry membership and the applied log id.
D1 snapshots carry all retained request identities, unlike the local D1
export/import API, which deliberately omits dedup. Install checks required
version/fields, duplicate keys and dedup ownership/sequence/completeness/budget.
It rejects downgrade to a legacy snapshot after activation, shard mismatch, and
an applied-index regression. A valid install replaces data, metadata, applied
marker, membership and saved snapshot in one synchronous batch while holding
the state lock through publication. A failed decode/validation leaves disk and
live state unchanged.

### Upgrade and downgrade

This release reads the immediately preceding opaque `TxnBatch` log, RocksDB
layout and array snapshot format. It does not add migration support for earlier
pre-batch `Put`/`Delete` experiments. Start those experiments with fresh data.
To activate a legacy group, first upgrade **all replicas and clients**, and use
an empty data map. Nonempty legacy data needs a separately designed migration;
activation does not assign it an invented application sequence or retry history.

There is no version negotiation. **Mixed-version replication and rolling
activation are unsupported. Do not downgrade a group after D1 activation.** Old
binaries cannot decode D1V1 logs or snapshots and do not understand `sm/d1`;
old clients can ignore application errors in an additive response field. A
backup alone is not a downgrade migration for newer committed writes.

Rust source compatibility: exhaustive `LogCommand` matches need the new variant;
`ApplyResult` struct literals need `d1: None` for legacy entries. Construct
`StateMachineData` via `Default` and its existing public fields rather than an
exhaustive literal, because D1 metadata is intentionally private. Use
`StateMachine::d1_metadata` for diagnostics; do not mutate metadata directly.

## Evidence and scope

- `tests/d1_contract.rs`: atomic put/delete ordering; duplicate-before-condition;
  changed identity; wrong shard; stale/future epochs and conditions; rejected-id
  reuse; complete durable reopen; epoch CAS; legacy boundaries; golden JSON;
  snapshot transfer, corruption and stale/downgrade rejection; mixed-apply
  determinism.
- `d1.rs` unit tests: batch count/bytes boundaries, dedup count/overhead budget,
  full-budget duplicate retry, and nonmutating overflow.
- `tests/d1_cluster.rs`: three durable voters, deliberately discarded committed
  result, leader shutdown/re-election, duplicate retry, minority partition with
  no acknowledged/applied write, majority retry, healing, rejected-entry marker
  convergence, snapshot/purge and fresh learner installation, all-node restart,
  then original-id retries and a new conditional commit.
- `tests/d1_tcp.rs`: the legacy control port cannot claim successful writes after
  D1 activation. Existing legacy multi-process and openraft conformance suites
  remain required.

The lost-response test discards a known committed result; it is not an exhaustive
network-response-loss simulator. Partition tests use the in-process drop policy;
existing OS-process tests continue to cover the legacy transport. No result here
claims graph-engine integration, multi-host security, automatic routing or
cross-partition transactions.
