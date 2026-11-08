# Authoritative event ledger

Current source status: [S2 active checkpoint](COORDINATOR_ACTIVE_CHECKPOINT.md)
replaces the durable bound coordinator runtime and ordinary apply candidate with
a consumed checkpoint and writes disk v16. Raw Commands remain sole authority;
profiles, grants, member formats and RPC v5 are unchanged. This is a draft
implementation, with acceptance still pending.

The sections below preserve the **historical S1 / disk-v15 contract**, including
its full-writer replay and resident-Journal limitations; those are not descriptions
of the new disk-v16 ordinary writer. They still specify actual v15 migration
inputs and the shared raw/read contracts unless S2 explicitly replaces a path.

S1 status: fixed-cohort live disk state reconstructs decision authority from original
Command segments. Bound coordinators in S1 write **disk v15** for both unchanged
profiles; members retain v12 (`LegacyCountV1`) or v14 (`ByteBound16MiBV1`). The
historical v11/v13 coordinator headers remain strict migration inputs. None of
these typed headers embeds the complete Journal in `sm/d1`.

S1 adds used, bounded-retention coordinator LookupRequest/HistoryPage readers and
retires the duplicate bound request index. It does not remove complete resident
Journals from writer/member state, cold replay, snapshots or other control/client
paths. The immutable **16 MiB reserved Journal** budget, D1 historical-data limits
and complete-cohort grants are unchanged. The legacy profile retains its 256-ID /
1,024-transaction caps; the [byte profile](COHORT_CAPACITY_PROFILE.md) retains its
existing byte-derived guards. Only coordinator disk metadata advances to v15;
logical snapshot/history/proof formats and managed RPC v5 are unchanged by S1.
No new profile, dependency or wire operation is introduced.

## Sole live authority and typed metadata

The shared coordinator header (v11/v13/v15) explicitly contains version, shard,
epoch, sequence, `dedup_bytes_limit`, managed mode and one ledger descriptor.
Member v12/v14 adds participant state and visibility metadata (baseline sequence,
epoch, applied ordinal and commit mapping), with v14's capacity activation. Neither
schema has a Journal, events, coordinator archive or full D1State field. Reused
optional nested fields must be explicitly encoded as required by the canonical serializer. Unknown, missing, duplicate,
noncanonical or mixed old/new fields reject. No permissive JSON field deletion is
used to construct these records.

The descriptor is inside `sm/d1`, not a second manifest key. It binds raw format
1 for the legacy profile or 2 for the byte profile, role, local shard/epoch/mode, full authority and ordered full cohort, absolute
event count, segment count, exact key-plus-row byte total, history head root and
complete applied Raft LogId. Its marker must equal `sm/applied`, including leader
term/node, and its identity must match the metadata. Member applied ordinal must
still satisfy the full reconstructed state validation; observed resolved history
is not application or read visibility.

On open, the codec parses metadata, replays the complete raw ledger, then assembles
D1State. It loads existing per-sequence D1 retry records, validates the complete
state/data and validates the terminal-prefix index. Actual legacy indexed formats
also require full external-request index validation; disk v15 requires that its
entire retired request namespace and manifest are absent. There is no new-format
fallback to those indexes, cached memory, an old archive or a stored snapshot.
A pre-ledger header with any raw ledger rows rejects.

## Versioned keys, canonical rows and bounds

All raw rows are in `sm/ledger/`. The exact key is that prefix followed by format
u32 big endian (1 or 2 for the bound profile), role byte (0 coordinator, 1 member),
shard u64, epoch u64, mode 16 bytes, authority shard u64, authority mode 16 bytes, coordinator epoch u64 and
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
the legacy transaction/participant limits. The conservative segment ceiling is
ceil(67,585/32) + floor(16 MiB/(4 MiB - 2 MiB)) + 1 = 2,122. A 16 KiB key/row
header allowance covers full 64-member identity and maximum integer/root widths.
Thus encoded ledger bytes are bounded by 16 MiB + 2,122 * 16 KiB; this is a
validation ceiling derived from accepted Journal histories, not a separately
exhaustible admission pool. Metadata has no 4 MiB row quota: a valid retained
16 MiB intent can expand substantially when serialized as JSON byte arrays.
The unchanged byte profile instead derives ceilings of 239,674 events, 7,499 rows
and 139,640,832 encoded key/row bytes from the same 16 MiB budget. Per-row decoder
and temporary allocations remain bounded by the existing encodings; these are
not measured RSS guarantees.

Roots reuse the [versioned SHA-256 history chain](INCREMENTAL_DECISION_CONSUMPTION.md).
They provide collision-resistant equality/integrity, not authentication or a
replacement for strict cold/writer replay. Those paths replay every original event
through the same Journal append rules, including exact request/Plan retention,
admission capacity, future resolution reservations, prepared evidence and cohort checks. A replayed
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

Journal and participant HistoryStatus source reads still reconstruct and strictly
validate the complete ledger under the state lock; snapshot build does so too.
Coordinator disk-v15 LookupRequest and HistoryPage use the shared walker described
below instead. Private history/I/O failures retain existing error mappings; no new
wire errors or fallback protocol are added. Existing terminal/read entrypoints
keep their requested-index-range checks rather than claiming to scan all unrelated
future rows. Coordinator Summary/Terminal also reject retired request rows or
manifests with a key-only check. In S1 this did not scan unrelated raw or terminal
rows. Current S2 Summary additionally verifies the complete raw cut; Terminal
explicitly materializes full raw history while still checking only the requested
terminal rows. Disk-v15/v16 errors never fall back to a cached Journal.

## Archive-backed coordinator reads (S1)

In historical S1, `PublishedArchiveCut` is private, non-serializable and contains
no Journal, event list, Plan archive or transaction vector. In current S2, the cut
and runtime share one immutable checkpoint Arc containing the bounded active
transaction vector `(R,N]`; neither owns a full Journal. Storage and cut each cache
canonical metadata bytes including the checkpoint. Reading `sm/d1` from the DB
snapshot additionally owns temporary metadata bytes, including checkpoint bytes
up to its structural ceiling. Complete current snapshot bytes remain resident.
The target/page retention descriptions below concern the raw projection, not the
reader's total allocations or the whole process. See the [S2 memory boundary](COORDINATOR_ACTIVE_CHECKPOINT.md#replacement-and-authority).

The cut binds full role/local/authority/cohort/profile identity, exact raw descriptor and metadata bytes, full applied
LogId (including leader identity), resolved frontier and expected decision manifest.
It is prepared only from strict cold-open validation or the exact validated
apply/snapshot candidate. Publication happens under the existing state lock after
the same durable batch has synced; failed sync publishes neither state nor cut.
Blank/membership and no-new-event applies still advance the full marker. A
post-sync/pre-publication crash reconstructs the committed cut on strict reopen.
This expectation is distinct from operation-local Loaded/Staged proofs; their
candidate identity and scope remain unchanged.

Both persistent reader helpers accept the DB and cut, not Journal/D1State. The
real coordinator RPC routes hold the state read lock and use one RocksDB snapshot
for metadata, marker, raw namespace and applicable index checks. One raw walker
is shared with strict load: it verifies exact keys/positions, complete domain,
canonical rows/events, greedy partitions, per-row/event bounds, checked totals,
profile-derived loop ceilings and the entire SHA-256 chain/head against that cut.
Metadata must equal the published bytes; the full applied LogId must match. Missing,
extra, malformed, foreign or mixed-format raw rows reject, including after a found
request or a page's frozen cut. No found, absent or empty-page success is returned
before the complete current namespace closes.

The integrity assumption is collision-resistant equality to a sequence already
validated semantically, not authentication or a self-reported disk root. Cold open
still independently replays every transition and cannot mint a cut from a
semantically invalid but consistently rehashed archive. No caller can supply or
deserialize a cut. Ordinary reads cannot repair missing data.

LookupRequest counts Begin and AdmitRequest, not BindCohort or capacity-confirmation
events, to derive the absolute transaction ordinal. Its raw projection retains
only the requested original request (at most 64 KiB), exact original Plan
(at most 256 KiB), transaction and up to 64 ordered participants, plus one current row and scalar counters.
Matching Prepared/Decide events use the same extracted Transaction transition
checks as strict Journal replay. Pending prepare bits and terminal decisions beyond
an earlier hole remain exact. The unchanged RequestLookup validator runs after
closure; first-plan-wins, original bytes and Unknown-not-Abort semantics remain.
No collection proportional to all external IDs is retained by this reader.

HistoryPage captures roots at the absolute base and frozen-cut event positions,
collects at most 32 events / 4 MiB, and continues verification to the current head.
Its raw projection retains one current row plus one bounded page, in addition to
metadata validation and the shared state described above. Existing V0/empty, overlap,
immutable-cut and future-tail semantics remain. The same-snapshot decision
manifest/frontier check uses the expected published bytes, not a full Journal.

Each read is still **O(total history) disk work**. Existing deadlines and bounded
CPU-permit rules remain; timeout is an error, not partial output or unknown-ID
success. This is reader independence and bounded result retention, not node-wide
lazy recovery, a measured memory reduction, a throughput improvement or a longer
write lifetime. Historical S1 retained full Journal reconstruction on ordinary
apply. S2 removes it from disk-v16 coordinator apply, while open/snapshot/member
and explicit control paths still materialize complete history, including
participant HistoryStatus, grants and client decide/status/Prepare flows.
Complete current snapshot bytes also remain resident.

## Complete snapshots and migration

Logical SnapshotV1 and the stored snapshot envelope still contain the complete
Journal, participant intents, D1 retries, data and commit map. They have no
external ledger references. Snapshot build first validates the live ledger;
complete serialization/transfer and in-memory history remain. Installation first
checks the existing local exact history/participant successor, identity, sequence,
applied prefix and data contract. A newer marker or self-reported root cannot
authorize rollback. Only then does one batch clear and rebuild raw rows, metadata,
terminal indexes, data/dedup, markers and the stored complete snapshot; bound
coordinators are rebuilt directly as disk v15 with all request rows/manifests
cleared. An explicit validated snapshot may repair damaged local rows and remove stale rows. It is
never an automatic open/read/apply fallback.

Published legacy formats remain readable for actual migration: basic D1 versions
1–4, coordinator 5/8/9/11/13, participant 6, and member 7/10. Original format
contracts remain in their respective documents. Before the first successful apply,
old coordinator formats keep their exact legacy lookup/history behavior and
corruption checks; they are not fallback readers for disk v15.

Ordinary coordinator migration strictly replays the old authority and fully
validates any old request index and manifest before deleting them. A missing or
changed legacy row is an error, never permission to rebuild or discard evidence.
The same synced batch writes disk v15, retires every request row/manifest, appends
raw events and updates terminal index/data/marker. Blank and no-event applies also
migrate. Failed sync leaves the complete old format; death after sync reopens the
complete v15 generation. No half-generation is accepted. Bound members retain
v12/v14; earlier member formats still migrate under their existing contract.

A genuinely unbound, request-free coordinator keeps disk v8 and its empty request
manifest until BindCohort; this requires no additional disk version. It has no
general production duplicate-row writer. Disk-v15 open/read/apply/snapshot-build
reject any leftover or reintroduced request row or manifest, even malformed or
foreign keys. Its header must describe a bound coordinator and a valid profile/
raw format with the exact marker; it cannot reinterpret an embedded Journal or
member header. Explicit validated snapshot install retains its authorized repair
semantics. Memory-only and actual pre-ledger legacy paths retain their existing
behavior. Old binaries reject v15; no downgrade or rollback is supported.

## Refactoring and remaining milestones

- Share one namespace/canonical-row/hash walker between strict replay and the two
  bounded observers; do not maintain a parallel scanner or transition grammar.
  Prepared/Decide validation is shared at the Transaction boundary.
- Remove full-Journal reconstruction from the selected disk-v15 reader helpers.
  Retain strict cold/writer replay, full snapshot validation and capacity activation
  checks for their live purposes, not as compatibility-only alternatives.
- Retire bound live request-row staging and manifest summation. The general writer
  remains only behind `cfg(test)` for actual legacy fixtures. Production retains
  the unbound empty-manifest writer, strict legacy readers/validators, no-index
  residue checks and public response types. The terminal index still serves exact
  V evidence and is not retired. No unused authenticated directory, locator or
  index layer is introduced.
- Reuse the existing typed codecs, RPC envelopes, deadline helper, fixed-cohort
  fixtures and independent-process crash protocol. No new dependency is needed.

Subsequent steps must remove full-archive decide/status/Prepare dependencies and
bound terminal evidence transfer, introduce reviewed bounded active write state
and complete-cohort capacity, then close retained-archive checkpoints and streamed
snapshots without complete Journal/byte-vector residency. An unresolved hole or a
lagging nonparticipant cannot be ignored; archive quota/exhaustion and old ID/Plan/V
retention need explicit contracts. Existing grants are not renewable or infinite.
D1's separate retained-history/replay limits remain another finite boundary.
No existing profile's 16 MiB limit is removed, and no checkpoint implies GC.

S1 validation remains a publication gate, not a claimed pass here: require fresh
frozen-source point/page parity and no-Journal tripwires, corruption before/at/after
targets, legacy migration and disk-v15 residue checks, concurrent publication,
pre-sync failure and independent-process post-sync death, old IDs/V and nonparticipant
resolution, the 1,025-write release acceptance and required remote CI. Existing
fixture/test coverage or a previously built executable is not acceptance of the
changed source.
