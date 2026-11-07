# D2 replicated D1 application contract v1

This is a single-shard **application-state-machine slice**, not a distributed
graph database or an implementation of the synchronous `StorageNode` trait.
The separate [replicated graph adapter](REPLICATED_GRAPH.md) now wires
`GraphRecordStore` through these commands in-process; `GraphEngine` is not wired
through Raft. The independent
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
the separate adapter uses the bounded atomic `d1_read_snapshot` capture after
the barrier, not separate metadata/data reads.

## Bounds and explicit prototype limitations

A batch contains 1–10,000 operations and at most 16 MiB of key/value bytes.
Dedup retains at most 4,096 successful requests and a default accounting budget of
64 MiB (an explicit 128 MiB tier is described below): raw key/value bytes + 64 bytes per operation + 128 bytes per request.
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

Apply stages only changed keys and newly accepted retry records. Full logical
snapshots and the retained in-memory data/retry history remain; this is still a
bounded prototype, not an unlimited production memory/latency design.
The test transport remains unauthenticated and has no production backpressure.
Process restart tests are not power-loss/WAL fault-injection evidence.

## Durable and snapshot format

The existing `sm/data/`, `sm/applied`, `sm/membership` and `sm/snapshot` keys are
unchanged. Disk metadata version 2 stores `sm/d1` with required JSON fields in
this order: `version` (=2), `shard`, `epoch`, and `sequence`. For example:

```json
{"version":2,"shard":7,"epoch":3,"sequence":1}
```

Each retained retry record is stored at `sm/dedup/` followed by its original
sequence as eight big-endian bytes (sequence 1: `736d2f64656475702f0000000000000001`).
Its value preserves the v1 record JSON: `request` (original shard, epoch, request
id, condition and ordered operations) followed by `sequence`. Keys are ordered
by sequence. Every record must match its key; sequence, ownership, uniqueness,
completeness and the original dedup bounds are validated on reopen. Missing
`sm/d1` means legacy byte-batch mode only if there are no retry records; orphan
records and unknown versions fail closed. Activated state requires its applied
marker.

Version 1's complete `sm/d1` JSON remains readable. Opening it is read-only.
The first successful apply atomically writes every retained v1 record to the
incremental range, changes the header to v2, and persists any new data, retry
records, sequence, membership and applied marker in the same synchronous batch.
A pre-commit failure leaves both the published state and v1 disk layout unchanged.
Any reported storage error leaves live state unpublished and is fatal to Raft
storage processing. A real I/O/fsync error may have an uncertain durable outcome:
stop and reopen to establish which complete atomic batch recovered; do not infer
disk rollback from the error. The failure-injection test rejects before RocksDB
write, and the process-crash test kills after successful sync. Neither is a
power-loss or filesystem-corruption proof.
Subsequent applies persist only newly accepted records and small metadata;
rejections and exact retries do not rewrite old request bytes. No records are
pruned or hashed in place of exact identity. Incremental persistence itself does
not grant extra capacity; the explicit administrative command below does.

Apply keeps a changed-key overlay and a metadata/new-record overlay. Requests
see both published and earlier-in-this-call retry history; ordered operations
and legacy activation see earlier changes in the same call. Until fsync succeeds,
the published data, sequence, dedup and marker are untouched. Publication runs
under the same write lock and on the same non-yielding stack, without an await or
detached task between persistence and publication. Failed persistence discards
the overlay. Full snapshot install atomically replaces the complete retry range,
so records belonging to a superseded snapshot cannot survive.

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

For an already D1-aware v1 binary, the new disk-v2 header fails its strict v1
D1 decoder. **Do not start an old binary against a migrated state.db.** An old
binary must not silently load a partial retry history. Upgrade all replicas
before accepting new work; mixed-version operation is not certified. Logical
snapshot and Raft log/wire bytes stay v1-compatible, but that alone is not a
supported in-place downgrade procedure. Preserve a pre-upgrade backup; restoring
it loses later commits unless a separately validated recovery procedure transfers
the complete state and retry identities. This change does not provide such a
downgrade tool or rewrite user data on open.

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

## Snapshot memory and disk envelope v2

Logical snapshots keep their existing sorted legacy array or full D1 object
bytes. Unexpanded D1 state retains its exact v1 encoding; expanded state uses
the explicitly versioned D1 v2 payload described below. Building serializes borrowed state under the existing read lock rather
than cloning the complete key/value map and retry history. Installing decodes
straight into typed byte vectors instead of constructing a JSON `Value` for
every byte. Version, unknown-field, D1 completeness/capacity and duplicate-key
validation remain mandatory; duplicate JSON object fields are now rejected
rather than silently taking the last field. Malformed input never publishes
partial state.

The persisted snapshot has a separate disk-only version. `sm/snapshot` now
contains the strict JSON fields `version` (=2), `meta` (unchanged Openraft
snapshot metadata), and `data_len` (the exact payload byte length).
`sm/snapshot-data` contains the unmodified logical snapshot bytes, avoiding
JSON-encoding those bytes a second time. Both keys are written in one sync
WriteBatch. Snapshot installation includes them in the same batch as data,
D1 metadata/retry records, membership and applied marker; publication occurs
only after successful persistence, without an intervening await.

The previous single-key `{meta,data}` JSON remains readable without mutation
on open. The first successful snapshot build or install atomically replaces it
with the new envelope and payload. Missing payload, orphan payload, a legacy
envelope mixed with a separate payload, length mismatch, unknown versions and
unknown v2 fields fail reopen. An empty graph has the logical payload `[]`, not
an absent payload. This is not a checksum or arbitrary bit-corruption guarantee.

Old binaries reject the v2 envelope because their required `data` field is
absent. Upgrade all replicas before creating new snapshots. Do not open the new
disk layout with an old binary. A rollback must export a complete old-compatible
logical snapshot using the new binary and install it into a **fresh** old-format
database, or restore a pre-upgrade backup with its associated data-loss caveat;
copying the new RocksDB directory is not a downgrade. Logical byte compatibility
alone does not guarantee an old binary has enough memory to restore a large
snapshot. There is no automatic rollback or snapshot-export CLI in this change.
Full logical snapshots, in-memory state and native RocksDB batch copies still
have whole-state costs, so actual peak memory must be measured separately from
request-accounting and frame limits.


## Explicit bounded capacity tier

This is an opt-in prototype capacity increase, not garbage collection or
unlimited sustained throughput. Default behavior stays at 64 MiB and 4,096
successful requests. `EXTENDED_DEDUP_BYTES` is a fixed 128 MiB hard maximum;
only these two byte tiers are accepted. The request-count limit, per-write
limits, query/session limits, Raft frame caps and timeouts do not change.
Retained accounting is unchanged and is not RSS accounting.

`SetDedupCapacity { shard, epoch, expected_bytes, new_bytes }` is a replicated
D1 administrator command. It checks exact shard/epoch fencing before handling
idempotence, rejects unknown tiers and decreases, and compare-and-sets the
current budget. If the requested target is already current, the identical
request succeeds again, even after a lost reply. It does not consume an
application sequence or request identity. It persists the new capacity with
the applied marker in the same sync batch and publishes only afterward.
Other writes in the same apply call observe the newly committed tier.
`InvalidDedupCapacity` and `CapacityConflict` are typed application failures;
transport success alone is not administrative success.

On the explicitly insecure development control port, after a linearizable
read barrier, `D1Capacity` returns the current byte limit, retained accounted
bytes and request count, and both fixed maxima. `D1SetCapacity` submits the
command without retrying or selecting another leader. The operations helper
prints the typed reply and exits nonzero for application rejection:

```
d2-ctl <leader-control-address> d1-capacity 7 3
d2-ctl <leader-control-address> d1-set-capacity 7 3 67108864 134217728
```

Use an explicit operator decision after checking available RAM/disk and the
reported usage. A new write rejected specifically with `DedupCapacity` did not
reserve its identity: after successful expansion it may be resubmitted with
the **same** complete request id, operations and condition. An uncertain write
outcome still requires its original immutable retry, not a fresh id. Expansion
never deletes earlier history, resets sequence, changes ownership epoch, or
makes old ids reusable. Once 128 MiB or 4,096 requests fills, new writes still
fail closed. There is no downgrade, pruning, automatic expansion or retention
acknowledgement protocol.

### Expanded persistence and rollout

Upgrade **all replicas, administrative clients and recovery tooling** before
using the new command. Mixed-version replication, version negotiation and
rolling activation are not supported. Existing writer wire bytes happen to
remain unchanged, but this is not a promise that old clients or binaries can
participate safely in an expanded group.

Unexpanded D1 state continues to serialize exactly as logical version 1, and
its incremental disk header remains version 2. After opting in, logical D1
state has `version:2` plus required `dedup_bytes_limit:134217728`; all prior
request records and original sequences remain present. The outer logical
snapshot envelope stays version 1. The expanded incremental disk header uses
version 3 with fields `version,shard,epoch,sequence,dedup_bytes_limit`. Reopen
and snapshot installation validate the exact version/tier combination, full
history, both bounds and all previous invariants. Snapshot install cannot
reduce an already-established capacity. Metadata-only expansion does not
rewrite the retained retry records.

Old binaries fail closed on expanded disk metadata, new command variants and
expanded D1 snapshots. **An expanded snapshot is not an old-binary rollback
format**, even if it currently contains less than 64 MiB. The logical snapshot
interchange compatibility described above applies only to unexpanded D1 state.
There is no in-place downgrade or lossless export-to-old-tier conversion tool.
Retain a pre-expansion backup; restoring it loses subsequent commits unless a
separately validated recovery procedure is developed. Preserve all replica
databases and complete logical snapshots during upgrade and recovery.
