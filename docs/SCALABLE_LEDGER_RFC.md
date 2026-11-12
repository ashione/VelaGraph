# RFC: incremental ledger semantics without weakening corruption detection

Status: design and executable boundary model, NOT a production implementation.
Reviewed source: main 31c52eaa438fafa9ba3f1d067a4b0c677a379932, plus the
uncommitted Prepare projection slice in its separate worktree (read-only).
No production hot path, TTL, disk format, grant, or consistency contract changes.
No claim that the 60-second pin failure is fixed. General ledger infrastructure,
not a CoreHR-specific path. PR #178 remains independent.

## 1. Findings from primary sources

* [RocksDB MANIFEST](https://github.com/meta/rocksdb/wiki/MANIFEST): durable
  versions are reconstructed from transactional VersionEdit records; atomic
  groups are applied only when complete. Application: publish a ledger generation
  only when its raw suffix, index edits, checkpoint and applied marker agree.
* [RocksDB WAL tracking](https://github.com/meta/rocksdb/wiki/Track-WAL-in-MANIFEST):
  directory enumeration alone cannot establish completeness of WAL recovery.
  Tracking expected WALs and synced sizes detects missing durable material.
  Application: an index count or self-reported hash is not independent authority;
  completeness requires a manifest bound to the published generation.
* [SQLite WAL](https://sqlite.org/wal.html),
  [WAL-index format](https://sqlite.org/walformat.html): each reader has a fixed
  end mark; a transient index accelerates locating pages. Checkpoint work cannot
  invalidate old readers. Application: bind lookup results and proofs to one
  immutable view; do not share an unversioned transaction-ID cache across cuts.
* [FoundationDB architecture](https://github.com/apple/foundationdb/blob/main/documentation/sphinx/source/architecture.rst)
  and [recovery internals](https://github.com/apple/foundationdb/blob/main/design/recovery-internals.md):
  versioned storage and recovery generations separate serving state from log
  recovery; transaction-system failures require fenced recovery. Application:
  distinguish a committed/published generation from a locally prepared index.

These are design precedents, not evidence that those systems promise VelaGraph's
current next-operation detection of arbitrary unrelated logical-row corruption.

## 2. Existing invariants and costs

`crates/d2-raft/src/event_ledger.rs`:

* `walk`: reads APPLIED and the entire ledger namespace; enforces exact ordered
  keys/domain, row/event/byte counts, canonical encoding and greedy partitioning,
  before/after rolling roots, no foreign/extra/missing rows, and iterator errors.
* `load`: independently replays every original semantic transition, not merely
  a self-consistent disk descriptor. Cold open must retain this authority check.
* `PublishedArchiveCut`: private expectation minted from validated/staged state;
  not a deserializable proof that all underlying disk bytes remain intact.
* `snapshot`: matches D1 metadata and decision manifest and rejects retired
  request namespaces. `verify_active_inner` then closes raw authority; member
  verification also derives ordered terminals and compares the decision index.
* `MemberOverlay`: accepted original commands of this batch, in order. Rejected
  commands and retries must not manufacture new history or permanent IDs.
* `target_at_cut`/`lookup`/`command_lookup`: historical point queries currently
  repeat raw closure/projection. Their answers include original Plan, request,
  exact Transaction and historical state, not only a terminal decision bit.
* Publication: stage all fallible work, sync one WriteBatch, then replace runtime
  and private cut under the same state lock. Full LogId matters even with no
  new event. Failed writes must never publish a prepared cut.

The normative coordinator/member checkpoint documents explicitly preserve
ordinary full raw closure and prohibit silent memory/snapshot repair. The
16 MiB monotonic history budget is a separate capacity limit: indexing cannot
make lifetime unlimited, release quota or imply safe GC.

For H original events and Q historical probes per command, current repeated
semantic projection is approximately O(QH), including JSON decoding and hashing.
Collapsing scans decreases Q, not growth in H. This explains why another local
fusion cannot establish a scalable transaction path.

## 3. The constraint that cannot be optimized away

Let two durable stores differ only at an old row k. If a command does not read k,
any authoritative digest updated by that mutation, or an independent trusted
monitor covering k, its observations are identical for both stores. It cannot
accept the intact store and reject the corrupt one. A cached Merkle root does
not change this: an out-of-band bit flip does not update the root.

Therefore preserving next-operation detection of arbitrary unrelated corruption
requires complete coverage on every operation, or a new storage mechanism that
itself supplies equivalent verified coverage. RocksDB snapshots give a consistent
logical view, not an oracle for all unread physical corruption. RocksDB checksums
on accessed blocks do not establish absence of extra logical keys elsewhere.
Background scrubbing detects latent faults later; it cannot silently replace the
existing immediate detection boundary. Any such policy change requires explicit
review and is NOT proposed as an enabled optimization here.

## 4. Two-stage architecture

### A. Preserve the present contract and remove repeated semantic work

Introduce a **shadow versioned projection index**, initially unused for authority.
One strict original replay builds the index; accepted events update only affected
records through the existing semantic transition functions. Keep ordinary full
closure as an independent oracle during rollout. Do not return index results
until complete cross-validation succeeds. This cannot make ordinary operation
I/O sublinear, but can remove repeated original-event projection.

For an eventual strict fast validator, distinguish exact byte closure from
semantic replay: a version-bound private authenticated inventory may commit to
ordered raw and derived row bytes. Every operation still enumerates every row,
checks errors, lengths and complete inventory, and hashes all bytes against that
trusted inventory. It can avoid repeated JSON deserialization of unchanged bytes
only after the inventory was minted by strict semantic validation. A stored
inventory alone is not trustworthy, and raw-only inventory fails to certify
member decision derivation. New/changed rows still require full semantic checks.
This is O(H bytes), not O(log H), and must preserve public error classification.
It is a candidate follow-on, not implemented here.

### B. Achieve scalable online semantics with a versioned storage boundary

Only after the integrity boundary has an explicit reviewed contract, use an
immutable generation manifest + persistent authenticated ordered index and
incremental semantic state. No silent enablement for disk v16/v17. The proposed
new format number must be allocated after reconciling concurrent changes.

The version root commits to schema/version, role, authority, shard, epoch, mode,
cohort/profile, complete applied LogId, raw head cursor and row inventory root,
transaction/request/original-plan index roots, decision root, terminal/resolved
prefixes, active checkpoint root and accumulated byte/capacity totals. Separate
namespace/domain hash tags prevent swapping index kinds or roles.

Index keys and values (design, no on-disk encoding assigned yet):

1. Transaction ID -> immutable identity, ordinal, original Begin/Plan reference,
   current state, latest event position. Exact retry compares all original input.
2. Request ID -> original request, admission/transaction identity, original Plan
   reference. Drained transactions do not free permanent dedup identity.
3. Ordinal -> transaction ID + terminal/version references; exact original
   transaction reconstruction for member advance and terminal verification.
4. (ID, event position) -> versioned transaction changes. A frozen-cut lookup
   seeks the greatest event <= cut E, with membership/nonmembership/range proof
   against that cut's root. A head lookup must not substitute for the old cut.
5. Raw segment inventory -> exact domain/first/last/count/encoded bytes and hash;
   only the bounded final row is mutable before sealing.

Use a copy-on-write B+tree or equivalent persistent ordered map with fixed maximum
page bytes and fanout. A root reference, not a full per-version map, pins the
view. No unbounded RAM directory of all retired IDs. Ordinary work is affected
pages O(delta log H), bounded tail validation and active semantic state; point
lookups O(log H + result bytes). A membership proof authenticates the queried
page; it does not promise unread historical disk is currently uncorrupted.

## 5. Commit, recovery, migration and views

1. Hold the established state lock; capture exact predecessor generation/view.
2. Evaluate existing transition semantics against current active state and
   indexed historical evidence; replay accepted batch suffix in order.
3. Build changed raw tail/segments, projection pages, decision tail, checkpoint,
   manifest and full applied marker. Rejected commands retain existing marker
   semantics; retries do not mutate original identity records.
4. Validate candidate/root links, row bounds and all capacity accounting before
   issuing the existing sync WriteBatch. No publication on error.
5. After sync success publish one immutable view. Readers hold the old root and
   snapshot together; they cannot combine an old cursor with a new index root.
6. Crash before batch: predecessor remains. Crash after batch before publication:
   reopen committed generation, independently verify raw and derived state, then
   publish. Uncertain sync is fatal to the active handle until recovery resolves
   actual durability; never blindly retry publication from memory.
7. Cold open initially retains full original replay and complete derived closure.
   Checkpoint+suffix recovery is a later feature, requiring an independently
   authenticated checkpoint and verified completeness of all retained artifacts.
8. Migration strictly replays actual legacy bytes, constructs shadow indexes,
   validates equality and atomically writes the new descriptor. Failure leaves
   legacy state intact. Unknown format/partial index generation fails closed.
9. Snapshot install retains independent old-prefix/root/checkpoint and participant,
   commit-map and data-prefix checks. Only the explicit repair path repairs raw
   authority. No index rebuild on ordinary lookup after detecting corruption.
10. Do not introduce GC yet. Future GC needs the minimum reader/recovery/replication
    watermark and permanent idempotency contract. Query TTL is not a GC proof.

## 6. First production vertical slice and gates

The smallest useful slice is **shadow Transaction ID / ordinal index**, with the
old full walk retained. Implement separate files, minimizing conflicts with #178:

* a versioned strict codec with golden vectors and size/count limits;
* a pure projection transition driven by original accepted Commands;
* staged index writes in the same batch, never a second asynchronous write;
* cold-open oracle comparing every shadow row, including extra/missing rows;
* a diagnostic indexed lookup whose exact Transaction equals old projection;
* no switch from raw closure until the integrity inventory proof is reviewed.

This RFC/model does not implement that production slice. It should not be merged
as evidence of a runtime speedup. The shadow slice requires substantial existing
semantic differential coverage before publication; inventing a parallel simplified
transaction reducer would create a second, divergent authority.

Required tests: all Command variants; profile limits; old/new cut; ID conflicts;
R/A holes; accepted/rejected mixed batch; wrong role/cohort/epoch/full LogId;
partial final segment and sealing; extra/missing/substituted raw/index rows;
coherent forged disk root; iterator faults; failure before/at sync; death after
sync before publication; reopen/migration; explicit snapshot repair; no silent
fallback; unrelated raw and decision corruption after an earlier successful read.

Benchmark: history-size x payload-size x unresolved-hole-width matrix; random and
sequential IDs, old-cut and head reads, both capacity profiles. Record decode/hash
bytes, raw walks, index reads, allocations/RSS, sync time, CPU time and p50/p95/p99
whole Submit. Alternate baseline/candidate in one machine with frozen inputs.
Measure first open separately. Run real multi-node write/read lifecycle with the
original 60-second pin and verify exact same V/results. A local index microbenchmark
cannot establish pin lifetime success, distributed throughput or durability.

## 7. Executable boundary model

`ledger_integrity_model.py` demonstrates only the read-set limitation and atomic
publication semantics on a tiny immutable byte inventory. It tests unrelated
corruption, additions, deletion, forged inventory, old views, failed sync and
reopen; reports deterministic byte-read counts at increasing H. Python dict
lookup is not a persistent tree, dict replacement is not RocksDB fsync, and this
model does not validate the full VelaGraph transition system. Its role is to make
an unsafe shortcut reproducible and prevent overstating a proposed optimization.
