# Local physical checkpoints (single replica)

Tracking: #193, within #191. This is a **local backup baseline**, not remote
commit durability, a cluster clone, or a distributed/global checkpoint.

`NodeHandle::physical_checkpoint(new_directory)` backs up the actual durable
Raft log and state-machine pair bound to that handle. In-memory nodes reject it.
`restore_physical_checkpoint(backup, new_directory, group_id, node_id)` verifies
and copies a backup for the **same** replica identity. Recovery output is offline-only: old votes/commit metadata must never be
reintroduced directly into a live cluster, even after stopping the original replica. The API does not start a node or change quorum
membership. There is not yet a TCP control endpoint. The separate
[`checkpoint-object-store` transport](OBJECT_STORE_CHECKPOINT_API.md) can upload
an immutable completed checkpoint and download it to a new directory. The
physical validator and offline-only recovery guard still apply after download.

## Startup admission for recovery inputs

Local physical capture, cohort capture and object download reserve their fresh
output directory with `RESTORED_OFFLINE_ONLY`. The producer syncs the entry and
its directory before writing replica data or starting the first cohort member.
The caller must own that path exclusively, including during creation. Both
in-process and TCP Raft startup check reserved entries before binding a node
identity or opening RocksDB. An existing entry of any type blocks startup. A
metadata lookup error also stops startup unless the entry is absent.

| On-disk state | Available operation |
| --- | --- |
| Guard present, no final manifest | Inspect the attempt. Retry capture or download in a fresh directory. |
| Physical manifest present | Verify the inventory, log and state cut, then restore to a fresh offline destination. |
| Cohort generation published | Verify every selected replica and its decision prefix at the requested V. |
| Restore incomplete | Inspect the failed destination. Retry in a fresh directory. |
| Restore complete, offline guard present | Inspect restored state offline. Query activation still requires a separate recovery protocol. |

The guard stays outside the physical manifest and object receipts. Existing
completed backups remain verifiable. Updated nodes also recognize legacy
`checkpoint.json`, `.checkpoint-pending` and `prefix-generation.json` entries
without parsing them. An old interrupted attempt without a guard or any
recognizable record may be indistinguishable from legacy group-zero node data;
keep such attempts isolated from service data directories. This change provides
the offline admission boundary for [#351](https://github.com/ashione/VelaGraph/issues/351).

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

Before reading backup contents or creating a destination, restore requires the
backup root and its `raft.db` / `state.db` entries to be actual directories and
`checkpoint.json` to be a regular file. These component checks do not follow
symlinks; trailing separators and `.` components in the supplied root path are
normalized before both checking and reading. Symlinks and special manifest files
(such as FIFOs) are rejected without opening their contents. This is a narrow
input-type check, not a filesystem sandbox: ancestor symlinks, `..` resolution,
and concurrent path replacement are not secured. The caller must still supply
an immutable, trusted backup tree throughout restore.

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
are unchanged. Immutable remote manifest publication is implemented by the
separate transport; remote-durable write ACK, log archival,
fencing, retention/GC, cross-shard restore and rolling upgrades remain separate
work. Restoring one old replica is not proof of safe whole-cluster disaster
recovery; operators must follow the existing consensus membership constraints.

## Read-only local verification

Call `d2_raft::verify_physical_checkpoint(backup, group_id, node_id)` on a
trusted, immutable local backup. It checks the manifest and exact file
inventory, validates the Raft log boundary, opens the state database read-only
and runs the same eager loader as node startup. It compares the resulting
applied LogId, membership and snapshot metadata to the manifest. Success
requires no restore destination. Plan for the memory and I/O of a full local
load. The caller must retain the immutable input throughout verification.

This is the single-replica inspection prerequisite for
[#349](https://github.com/ashione/VelaGraph/issues/349). A Raft LogId refers to
one replica group's log. The cohort check below also compares managed decision evidence from every
required replica at an explicitly selected V.

## Local fixed-cohort recovery inputs

`d2_raft::cohort_inputs::capture` takes a fresh directory, an expected
authority and cohort, an explicit managed decision ordinal V, one durable local
coordinator replica, and one durable local replica per member in cohort order.
This local layout maps each shard to the Raft group of the same numeric ID.
The directory is caller-owned on a trusted, persistent filesystem.

Physical capture checks that each handle still refers to the node ID and state
machine paired with its log store at durable startup. A relabelled handle or
substituted state machine fails before that replica's backup is created.

The call creates one physical checkpoint per group under `<root>/<group-id>`.
The existing physical capture locks establish each replica's own immutable
Raft/state cut. Captures run serially and may have different LogIds or include
later decisions. Verification reopens the captured coordinator read-only,
reconstructs its replay-validated terminal prefix through V and releases that
database before opening each member. Every member must match the complete cohort
and authority, have an empty baseline, have applied through V, and match the
coordinator's terminal history, including Abort and nonparticipating decisions.
The caller supplies no proof bytes. A hole at or before V or a lagging member
fails the operation. A hole after V leaves earlier complete prefixes available.

Keep all these local directories immutable through publication and subsequent
recovery. Their files survive source shutdown, timeout and verifier drop.
Capture refuses to reuse an existing root and leaves partial attempts on disk.
After interruption, invoke `cohort_inputs::verify` with the intended V, identities
and node IDs to check the files again. Directory existence alone cannot establish
completion. Use a fresh root for another capture and budget disk for all retained
attempts. There is no automatic deletion. Bounds are the existing cohort/profile
limits and one replica load per invocation; startup validation still eagerly
loads one replica's state. Explicit yield points between replicas and before
verification let the caller cancel without starting another replica. An
in-progress replica's synchronous RocksDB work can finish before cancellation
takes effect.

This operation establishes a common **prefix** across immutable inputs. It
cannot start a restored cohort, roll the individual databases back to V, or
recover writes newer than a checkpoint. Global manifest publication and
durable binding of a generation to its V are tracked in
[#350](https://github.com/ashione/VelaGraph/issues/350). The offline-only
restore guard continues to apply.

## Publishing one local decision-prefix generation

`d2_raft::cohort_inputs::generation` supports the D2 decision-prefix input
profile on a trusted local filesystem. It covers the producer's coordinator
and each ordered member's `raft.db` and `state.db`. A deployment that needs a
separate application catalog or temporal database still needs a graph-level
backup protocol. This record cannot establish graph data coverage or query READY.

1. Call `prepare` with a fresh directory and the trusted live handles from one
   in-process registry. The producer captures each replica and hashes the exact
   physical manifest bytes it writes. Retain the directory and all its files
   with exclusive control of the path and its ancestors.
2. Save `Prepared::digest()` with the intended path, authority, cohort and V
   in independent trusted storage. Then call `Prepared::publish().await`.
   Publication reopens every replica and verifies its identity, file inventory,
   log boundary, managed prefix and original producer digest. It fsyncs a
   same-directory temporary record, installs the final name create-only, and
   fsyncs the directory. A retry verifies the components again and accepts
   only byte-identical existing contents.
3. On restart, pass that saved selection to `generation::verify`. It checks
   the entire closure again. If acknowledgement was lost, a successful reopen
   establishes that the selected bytes are present; sync the directory before
   acknowledging durability if the original directory sync was uncertain.
   A missing final record requires a fresh attempt. A digest obtained from
   the backup itself cannot establish its provenance.

The final file is `prefix-generation.json`, a JSON object bounded to 64 KiB.
The `format` tag is `d2-local-decision-prefix-v1`. It contains a random
16-byte generation ID, authority, full cohort (including epochs and capacity
profile), V (`version`), coordinator node ID, ordered member node IDs and
`checkpoint_sha256`. Each 32-byte entry hashes the raw bytes of the physical
`checkpoint.json` in the directory named for that group. The coordinator
comes first, followed by members in cohort order. The selection digest is
SHA-256 of the exact final file bytes. Older raw checkpoint directories remain
usable through the prefix verifier; only a new capture produces this format.

Prepare, publish and verify visit at most one coordinator and 64 members
serially. Each physical verifier reads the full inventory and eagerly opens one
replica state at a time. Provision storage and memory for those inputs.
Cancellation can interrupt between replicas or before publication enters its
synchronous filesystem step. Partial roots and orphan temporary files can remain.
The producer or operator must retain the files through every use. This API
performs no reclamation or provider-side pinning.

## Restoring a selected local cohort offline

Call `d2_raft::cohort_inputs::generation::restore_offline(source, destination,
digest, authority, cohort, version).await` with a published generation and its
independently retained selection. Keep the source immutable. Use a fresh
destination outside the source tree on a trusted filesystem with exclusive
access and space for a full second copy.

The operation durably guards the root, then restores the coordinator followed
by each data member. It checks each physical manifest against the selected
digest, copies both databases into the corresponding group directory, and
preserves the original node identity. Each group has a startup guard before
any database files are written. The restore reopens the copied databases
read-only, checks every inventory and validates their common terminal decision
history through V. It then publishes the exact producer record in the new root
and syncs the directory. After loss of the source, use `generation::verify`
with the same selection on the destination to repeat the physical and prefix
checks.

An interrupted attempt can retain complete individual groups. A missing final
record means whole-cohort completion has not been established. Use a fresh path
for a retry. The guard on the root and every restored group remains in place
after success. The restored files can include decisions after V. Serving
queries, fencing restored votes and recovering an external graph catalog or
temporal store require additional recovery work.

## Verification

The focused suite exercises a real durable single-node Raft lifecycle, concurrent
writes during capture, snapshot membership, empty-directory restore, re-election,
continued writes and duplicate request results in a wholly isolated test registry
(the test alone removes the offline guard after shutting down its sole source node); it also rejects existing output
paths, wrong identity, corrupted files, incomplete manifest/restore and uncovered
purge boundaries. These are correctness tests, not representative backup-size or
latency benchmarks, remote upload tests or a multi-shard recovery proof.

Input-type regressions substitute root/database/manifest symlinks and wrong file
types into a real checkpoint, assert the destination remains absent, and restore
the genuine checkpoint as a positive control, including trailing slash/dot root
paths. Missing and malformed manifests are tested with both genuine DB directories
present. A FIFO manifest runs in a killable child with an independent ten-second
parent watchdog and a completion sentinel; its ignored child entrypoint is
executed by the ordinary parent test.

The macOS CI lane compiles ordinary test targets, runs the focused physical
checkpoint unit/integration and memory-transport tests, and checks all targets
with Clippy. It does not rerun the full ordinary suite or provision S3 on macOS;
the explicit SeaweedFS service roundtrip remains in the transport workflow.

The ignored `physical_checkpoint::scale_tests` additionally writes 2,048 values
of 16 KiB through Raft (32 MiB of logical value bytes), flushing 32 batches into
real state DB files. It requires at least 32 SSTs; a separate fixture enables
blob files and requires at least 16 blobs. Compaction is disabled only in these
fixtures to retain multiple files. Production does not enable blob files.
Both fixtures upload to disposable loopback S3, remove the original source and
backup, download and restore, then compare every value and verify the offline
guard and no-overwrite behavior. They print actual file counts and checkpoint
bytes, including metadata. This is multi-file correctness coverage, not a
production-scale, latency or throughput benchmark. Run explicitly against the
disposable SeaweedFS fixture with:

```sh
cargo test --locked -p d2-raft --lib physical_checkpoint::scale_tests -- --ignored --nocapture --test-threads=1
```

The separate transport CI also exercises a real two-DB physical checkpoint
roundtrip through a temporary SeaweedFS S3-compatible service, followed by offline
restore validation. This is not certification against AWS S3, OSS or TOS, live
cluster rejoin, or a global consistent checkpoint. See the
[transport verification boundary](OBJECT_STORE_CHECKPOINT_API.md).
