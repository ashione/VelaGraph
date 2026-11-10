# Opt-in managed shard writes (first D4 prerequisite)

Status: a durable **single-shard write fence and atomic batch identity**, not a
distributed transaction coordinator, MVCC, global version, complete graph view,
or production security. The older GlobalCommittedSnapshot API remains
Unsupported. Later [managed fixed-cohort layers](CURRENT_STATUS.md) supply
coordinated graph writes and certified sessions through separate APIs.

## Why this layer

A future transaction authority cannot enforce global visibility by changing only
the graph/query coordinator: callers can submit opaque D1 writes directly. This
slice puts an opt-in fence in the replicated state machine, so every applied
write observes the same mode, including direct Raft submissions. It does not yet
connect existing graph mutation wrappers to a global authority.

## Default and explicit activation

Unmanaged is the unchanged default. Existing graph clients, wire requests,
snapshots, disk metadata and fixture import workloads do not enable this mode.
Normal graph write wrappers still use unmanaged D1 requests; **they will be
rejected on a managed shard**. Deploy managed-capable writers before opting in.
There is no automatic activation at startup, no fallback to weaker writes and no
new behavior for other shards.

An operator submits EnableManagedWrites with shard, current ownership epoch,
expected applied D1 sequence, and a nonzero 16-byte mode ID. The state machine:

1. Fences shard/epoch and validates the ID.
2. Requires the baseline data to match replay of its retained D1 history; an
   untracked old import or corrupt history cannot be certified by activation.
   Compares the expected sequence with the applied sequence atomically with the
   mode change. A concurrent earlier write makes activation conflict; a later
   unmanaged write is rejected once activation commits.
3. Persists the mode, activation sequence and Raft applied marker in one sync
   batch. Exact activation retries retain the original activation sequence.
   Changing the mode ID or activation sequence is rejected.

The ID is a permanent protocol identity, **not a secret or permission token**.
The development transports remain plaintext and unauthenticated. This feature
is not a substitute for authentication/authorization or trusted deployment.
Activation does not drain or repair historical staged cross-shard graph writes,
certify graph adjacency or establish any global transaction boundary.

## Managed commits and identity

ManagedWrite includes the mode ID and the existing bounded D1WriteRequest.
Its request_id is a shard-local transaction identity. The existing batch limits,
optional expected_sequence CAS, epoch fencing and retained retry budget apply.
One atomic Raft application commits all operations, next local sequence, exact
retry record and applied marker together. The returned sequence is a **local
commit version only**, not a globally coordinated timestamp and not a historical
MVCC read version. Writes without a CAS still do not provide read/write snapshot
isolation.

Pre-activation dedup records are preserved. Reusing any pre-activation identity
for a managed write is rejected, even if its bytes match. Managed exact retries
return their original local commit sequence; changed operations or expected
sequence fail. No dedup eviction or capacity relaxation is introduced.

Inspect and Status on the remote managed API establish a leader read barrier.
Status reports either Committed(sequence) for an identity committed after this
mode's activation, or Unknown. **Unknown is not Aborted**: a timed-out or pending
Raft submission can still commit later, and rejection/absence is not a durable
abort decision. A caller must retain the exact request identity and body and
retry only that request, never invent a new identity after an ambiguous result.
Status identifies a transaction ID; exact-body retry is required to check that
ID's intended operations match. No prepare records, durable abort decisions,
2PC participant protocol or commit-prefix watermark exists in this slice.

## Write-entrypoint coverage and limits

- Existing remote Write, RemoteStorageNode/ReplicatedStorageNode graph writes,
  and direct D1V1::Write commands all reach the state-machine check and are
  rejected after activation, including historical exact unmanaged retries.
- The public mutable StateMachine.state field is removed. Embedding callers
  can no longer mutate the live data map outside application checks. Diagnostic
  tests use an opaque pause_state guard with no data access. This is a Rust API
  tightening; callers that accessed implementation internals must migrate to
  read methods or replicated commands. Default graph/D1 method behavior is unchanged.
- Legacy TxnBatch remains rejected on every activated D1 shard.
- ManagedWrite is the only data-write command accepted in managed mode. It is
  single-shard and does not interpret graph schemas.
- Activate for the same ownership cannot remove the mode. SetEpoch and dedup
  capacity changes preserve it. Mode/epoch validation precedes identity lookup.
- Replicated/remote StorageAdmin import remains Unsupported. Existing fixtures
  imported through ordinary D1 writes are fenced like any other ordinary writer.
- Snapshot installation cannot remove/change an existing mode, regress its
  ownership epoch or sequence, or rewrite retained transaction identities. On an already managed replica,
  incoming history must extend the exact current identity prefix. Every managed
  snapshot's data is independently checked against full history replay; unchanged
  IDs cannot conceal substituted graph bytes. Replay keeps borrowed final values
  per live key, bounded by retained D1 history, without cloning payloads. A
  fresh replica may install a valid managed snapshot and then replay its log.
- LocalStorageNode/GraphEngine are separate local engines, not these Raft shard
  files. This mode makes no claims about them or independent physical copies.
  Filesystem tampering, replacement with an old directory or hostile unauthenticated
  Raft membership/transport is outside this protocol's security boundary.

## Persistence, upgrade and rollback

Unmanaged encodings are unchanged. Managed activation writes disk metadata v4
and logical D1 state v3 (inside the existing snapshot envelope); full retry
records remain in the existing separated key space. New readers validate the
mode ID, activation sequence, capacity tier and complete identity history,
including reachable CAS positions and nondecreasing request epochs. Managed
fresh snapshot restores and disk reopen also compare all data with replay of
that history; a matching ID list alone cannot hide different stored values.
Older binaries reject the new disk/state discriminator or unknown log command
rather than silently accepting unmanaged writes. New code can open old state;
activation is explicit, not an open-time migration.

Upgrade **all participating binaries and callers before activation**. This does
not claim mixed-version rolling upgrades. There is no API to disable or rotate
the mode and no supported online downgrade to an old binary. Do not treat an
old checkpoint restore as a lossless rollback: it omits subsequent managed
commits and needs a separately approved offline recovery/migration procedure.
Keep serving with compatible binaries if activation succeeded. An activation
or write timeout has an uncertain outcome; inspect/retry the exact operation.

A separately enabled [participant role](PARTICIPANT_WRITES.md) additionally fences
ManagedWrite and publishes data only through prepared participant resolution.
Managed-only mode retains the behavior above; role activation is never automatic.

## Tests and remaining work

Tests cover activation racing ordinary remote writes, concurrent managed CAS
transactions, exact identity/status behavior, stale/incorrect modes, sync failure
without publication, disk reopen, snapshot transfer/reopen, old raw command
rejection, metadata discrimination and snapshot attempts to discard the fence
or rewrite committed history. The concurrency case uses actual TCP services
and a live Raft node; it is not a multi-machine availability benchmark.

Next: cover graph mutation clients with a managed transaction coordinator,
introduce durable participant prepare/commit resolution and complete global
commit-prefix watermarks, then data MVCC, query pins, history GC and recovery.
This fence is a prerequisite; enabling it does not make existing distributed
queries globally consistent or make staged cross-shard edges atomic.
