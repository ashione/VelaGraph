# Authoritative event ledger

Later opt-in profile: [immutable byte-budget cohort capacity](COHORT_CAPACITY_PROFILE.md)
adds disk v13/v14 only for newly initialized cohorts. This document describes the
published legacy profile and its unchanged v11/v12/count-bound contract.

Status: fixed-cohort live disk state now reconstructs its decision authority from
original, independently stored Command segments. New coordinator disk version 11
and member version 12 no longer embed a complete Journal in `sm/d1`. This removes
the whole-archive live-state rewrite, not all full-history work or retention.

The **256 external requests, 1,024 transactions, 16 MiB reserved Journal** and
participant/D1 limits are unchanged. No epoch reset, expiration, GC, independent
member quota or unlimited write lifetime is introduced. Gremlin, graph records,
logical SnapshotV1 and managed RPC v4 remain unchanged.

## Sole live authority and typed metadata

Version 11 explicitly contains version, shard, epoch, sequence,
`dedup_bytes_limit`, managed mode and one ledger descriptor. Version 12 adds the
participant state and visibility metadata (baseline sequence, epoch, applied
ordinal and commit mapping). Neither schema has a Journal, events, coordinator
archive or full D1State field. Reused optional nested fields must be explicitly
encoded as required by the canonical serializer. Unknown, missing, duplicate,
noncanonical or mixed old/new fields reject. No permissive JSON field deletion is
used to construct these records.

The descriptor is inside `sm/d1`, not a second manifest key. It binds format 1,
role, local shard/epoch/mode, full authority and ordered full cohort, absolute
event count, segment count, exact key-plus-row byte total, history head root and
complete applied Raft LogId. Its marker must equal `sm/applied`, including leader
term/node, and its identity must match the metadata. Member applied ordinal must
still satisfy the full reconstructed state validation; observed resolved history
is not application or read visibility.

On open, the codec parses metadata, replays the complete raw ledger, then assembles
D1State. It loads existing per-sequence D1 retry records, validates the complete
state/data and finally validates the derived external-request and terminal-prefix
indexes. There is no new-format fallback to those indexes, cached memory, an old
archive or a stored snapshot. A legacy header with any raw ledger rows rejects.

## Versioned keys, canonical rows and bounds

All raw rows are in `sm/ledger/`. The exact key is that prefix followed by format
u32 big endian (1), role byte (0 coordinator, 1 member), shard u64, epoch u64, mode
16 bytes, authority shard u64, authority mode 16 bytes, coordinator epoch u64 and
first absolute event position u64. All integers are big endian. The complete
ordered cohort is bound in each row and the descriptor, not abbreviated in a
hash-only key.

A canonical JSON row contains version, full domain, first event position,
before/after roots and original Commands. Event positions start at 1 and never
restart per segment. They differ from transaction ordinals, contiguous resolved
prefix and member applied prefix. Rows are greedy consecutive groups of at most
32 Commands and at most 4 MiB summed canonical Command bytes. Every nonfinal row
must be full by count or unable to accept the next event within its byte bound.
A partial tail grows atomically, preserving its exact original Command prefix;
sealed rows are never rewritten by ordinary apply.

The existing maximum admitted event is 2 MiB. Other variants are structurally
smaller; maximum-width tests cover them. At most 67,585 events are possible under
the unchanged transaction/participant limits. The conservative segment ceiling is
ceil(67,585/32) + floor(16 MiB/(4 MiB - 2 MiB)) + 1 = 2,122. A 16 KiB key/row
header allowance covers full 64-member identity and maximum integer/root widths.
Thus encoded ledger bytes are bounded by 16 MiB + 2,122 * 16 KiB; this is a
validation ceiling derived from accepted Journal histories, not a separately
exhaustible admission pool. Metadata has no 4 MiB row quota: a valid retained
16 MiB intent can expand substantially when serialized as JSON byte arrays.

Roots reuse the [v1 SHA-256 history chain](INCREMENTAL_DECISION_CONSUMPTION.md).
They provide collision-resistant equality/integrity, not authentication or a
replacement for strict replay. Every original event is replayed through the same
Journal append rules, including exact request/Plan retention, admission capacity,
future resolution reservations, prepared evidence and cohort checks. A replayed
no-op is invalid. Full Journal equality remains required for live successors.

The complete namespace is closed: missing, extra, foreign-domain, reordered,
noncanonical, oversized, truncated, malformed, duplicate/no-op or semantically
invalid rows/events reject. Counts, roots and byte totals must match exactly.

## Atomic apply and actual source reads

Every persistent apply takes the existing state write lock, reloads and strictly
replays the current durable authority, and compares it with the published cut.
All Raft entries in that apply operate serially on one reconstructed candidate.
It does not reload the old disk image between entries. The raw ledger stage
checks the exact successor and writes only the growing old tail/new rows, plus
journal-less metadata, data/dedup, derived indexes and Raft markers in one synced
WriteBatch. An applied-only advance or exact retry writes no raw events.

A pre-sync failure publishes nothing and preserves the old bytes. Death after
sync but before publication reopens the complete new cut. An application-level
rejection still follows the existing Raft marker/error-result semantics. Neither
ordinary apply nor source reads repair a changed/corrupt durable authority from
memory. Complete reconstruction on each apply is deliberately conservative and
can increase cost. No speed or benchmark claim accompanies this functional step.

Actual Journal, HistoryPage and HistoryStatus source reads perform the same full
ledger validation under the state lock. Snapshot build also uses this authority.
Real TCP tests corrupt a raw row while a node is live and require these RPCs to
fail. Private ledger-history errors map to existing v4 VisibilityHistory and
ledger I/O failure to Unavailable; no new wire operations or error tags are sent.
Existing derived terminal/read entrypoints still check their requested index
ranges against the published reconstruction. They do not claim to rescan every
unrelated future raw row on every read. Open, apply and the named source reads
check the full raw closure, while terminal-index corruption has its existing
requested-range behavior.

## Complete snapshots and migration

Logical SnapshotV1 and the stored snapshot envelope still contain the complete
Journal, participant intents, D1 retries, data and commit map. They have no
external ledger references. Snapshot build first validates the live ledger;
complete serialization/transfer and in-memory history remain. Installation first
checks the existing local exact history/participant successor, identity, sequence,
applied prefix and data contract. A newer marker or self-reported root cannot
authorize rollback. Only then does one batch clear and rebuild raw rows, metadata,
derived indexes, data/dedup, markers and the stored complete snapshot. An explicit
validated snapshot may repair damaged local rows and remove stale rows. It is
never an automatic open/read/apply fallback.

Published legacy formats remain readable for actual migration: basic D1 versions
1–4, coordinator 5/8/9, participant 6, and versioned member 7/10. Their original
contracts are in D2_D1_CONTRACT, TRANSACTION_DECISION_JOURNAL, PARTICIPANT_WRITES,
VERSIONED_PARTICIPANT_READS, DURABLE_REQUEST_INDEX and DURABLE_DECISION_SEGMENTS.
Bound legacy 5/8/9 and 7/10 atomically migrate on their first successful durable
apply. Failed sync leaves the legacy header with no ledger rows; post-sync death
leaves a complete v11/v12 generation. Binding an old unbound coordinator also
migrates atomically. Unbound and non-cohort modes keep their existing valid writer.
Old binaries reject v11/v12. There is no downgrade or rollback support.

## Refactoring and remaining milestones

- Extract one shared typed disk codec for open, apply and authority-source reads.
  Remove the embedded bound v9/v10 writer and duplicated version decoding from
  the state-machine implementation. Legacy readers remain because those formats
  were published and are migration inputs, not as new bound write alternatives.
- Share strict event replay between archive decoding, history suffix/prefix
  reconstruction and raw ledger recovery. Share the existing history-chain step;
  do not add a second hash or a looser ledger-only transition implementation.
- Reuse fixed-cohort fixtures and the independent-process post-sync crash harness
  for ledger/marker checks instead of adding a second fault-control protocol.
- Keep request and terminal indexes: they serve real point/range reads. They are
  derived from replayed Commands and cannot repair or replace the authority.

The next limit-removal step still needs bounded/lazy authoritative state and
exact old external-ID/Plan lookup, checkpoint/snapshot closure, and durable
pre-admission consumption capacity across **every** cohort member, including
nonparticipants. A coordinator-only grant or unreclaimable per-transaction
orphan reservation is insufficient. This slice preserves the existing common
logical Journal bound and all already-admitted resolution guarantees. Physical
storage/fsync failure remains uncertain, not a reserved logical capacity.
