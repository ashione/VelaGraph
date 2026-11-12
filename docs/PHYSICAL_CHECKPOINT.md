# Local physical checkpoints (single replica)

Tracking: #193, within #191. This is a **local backup baseline**, not remote
commit durability, a cluster clone, or a distributed/global checkpoint.

`NodeHandle::physical_checkpoint(new_directory)` backs up the actual durable
Raft log and state-machine pair bound to that handle. In-memory nodes reject it.
`restore_physical_checkpoint(backup, new_directory, group_id, node_id)` verifies
and copies a backup for the **same** replica identity. Recovery output is offline-only: old votes/commit metadata must never be
reintroduced directly into a live cluster, even after stopping the original replica. The API does not start a node or change quorum
membership. There is not yet a TCP control endpoint or object-store uploader.

## Consistent cut

The state publication write lock, snapshot publication read lock, and log-store
mutex are held in that order until both RocksDB checkpoints are created. Snapshot
installation follows the same lock order; the snapshot builder takes state then
snapshot locks. Raft appends, truncation, purge, vote and commit writes cannot
interleave the pair. Writes and reads may pause during checkpoint flush/link work.
This is not a low-pause performance claim; pause duration requires measurement.

The pinned crate is rocksdb 0.25.0 / librocksdb-sys 0.19.0+11.8.1. Its
`Checkpoint::create_checkpoint` uses flush threshold zero. We use the complete
RocksDB-produced directories, not an arbitrary live-SST scan. Relevant upstream
references: [Checkpoints](https://github.com/facebook/rocksdb/wiki/Checkpoints),
[WAL](https://github.com/facebook/rocksdb/wiki/Write-Ahead-Log-%28WAL%29), and
[rust-rocksdb API](https://docs.rs/rocksdb/0.25.0/rocksdb/checkpoint/struct.Checkpoint.html).

A versioned completion manifest records node/group identity, applied LogId,
membership, snapshot metadata, retained/purged/committed log boundaries, vote,
and a sorted file-size/SHA-256 inventory. The retained log is checked for
contiguous indexes and matching entry IDs. Validation and file hashing happen
on the immutable checkpoint after releasing serving locks. Existing snapshot
must cover the purged boundary; otherwise backup fails. **Purge behavior is not
changed**, and this does not repair pre-existing invalid histories.

## Publication and recovery

The caller supplies a nonexistent output directory. Files and directories are
synced before writing the completion manifest last. Cancellation or interruption
before a valid complete manifest leaves an unusable attempt, never an overwritten
previous backup. Output directories are never automatically deleted or reused.

Restore verifies identity, exact inventory, checksums and Raft boundaries before
creating a new destination, streams copies rather than linking mutable DB files,
then uses the existing full state-machine loader to validate applied state,
membership, snapshot and the existing ledger invariants. `RESTORE_INCOMPLETE`
remains until validation and sync complete; node startup refuses that marker.
A failed attempt must be inspected, not started or retried in place. Successful
restore writes `RESTORED_OFFLINE_ONLY`, which normal in-process and TCP startup
reject. Safe live rejoin requires a separately designed learner/reseed/fencing
protocol; this PR deliberately does not implement one. The manifest
is integrity metadata, **not a signature against a malicious backup author**.
Treat the backup directory as immutable and trusted; do not write its SSTs,
which RocksDB can hard-link to source files on the same filesystem.

No recovery removes, rewrites, or overrides existing data. A local backup on the
same device does not survive loss of that device. Successful writes after the cut
are absent from this backup. Existing synchronous local-WAL / Raft ACK semantics
are unchanged. Remote manifest publication, remote-durable ACK, log archival,
fencing, retention/GC, cross-shard restore and rolling upgrades remain separate
work. Restoring one old replica is not proof of safe whole-cluster disaster
recovery; operators must follow the existing consensus membership constraints.

## Verification

The focused suite exercises a real durable single-node Raft lifecycle, concurrent
writes during capture, snapshot membership, empty-directory restore, re-election,
continued writes and duplicate request results in a wholly isolated test registry
(the test alone removes the offline guard after shutting down its sole source node); it also rejects existing output
paths, wrong identity, corrupted files, incomplete manifest/restore and uncovered
purge boundaries. These are correctness tests, not representative backup-size or
latency benchmarks, remote upload tests or a multi-shard recovery proof.
