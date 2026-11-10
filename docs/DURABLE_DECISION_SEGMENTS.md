# Durable terminal decision segments

Current bound durable storage uses original raw-Command authority and
[S2/S3 active checkpoints](CURRENT_STATUS.md#storage-and-protocol-boundaries),
with coordinator/member disk v16/v17 and internal managed RPC v6. The
[byte profile](COHORT_CAPACITY_PROFILE.md) replaces only legacy count caps.
The sections below preserve this slice's historical formats and limits; they do
not describe all current writers. Complete history and the 16 MiB lifetime budget
remain retained.

Status: a bounded storage/read foundation after A2.1. The complete Journal is
still the sole decision authority. The existing **256 external requests, 1,024
transactions and 16 MiB Journal including resolution reservations** remain.
There is no unlimited write lifetime, archive truncation, GC, epoch reset,
independent member quota or new admission/reservation protocol.

## Purpose and exact cuts

Coordinator terminal proofs and participant certified reads now use a persistent
index of exact terminal decisions, located by absolute transaction ordinal.
This closes the functional gap between durable range storage and the actual
read path: an indexed node cannot answer a requested proof by silently projecting
its in-memory Journal when the corresponding stored segment is missing or wrong.

The coordinator indexes exactly 1..resolved_prefix. A terminal decision beyond an
unresolved earlier ordinal stays in the authoritative Journal but is not indexed
until the hole closes. A participant indexes exactly 1..applied_prefix, including
Abort and nonparticipant decisions. Its observed Journal may already resolve
later ordinals; observation does not authorize indexing, serving or consuming them.

Canonical segments start at absolute ordinals 1, 33, 65, and so on. Each has 32
records, except the last partial segment. Its existing prefix is immutable; it
can grow atomically until sealed. No segment-local ordinal restarts, timestamp
ordering, max-terminal shortcut, implicit Abort or transaction-ID reuse exists.
Filling a coordinator hole can publish multiple new segments in one apply batch.
Participant advancement still consumes each ordinal in order under its existing
state-machine checks. A segment is not an independently trusted certificate.

## Disk format and atomicity

Only fixed-cohort modes use the new index. Bound coordinator disk metadata is
**version 9**; bound versioned-member metadata is **version 10**. Unbound modes
retain their existing versions and semantics. The complete logical D1 snapshot,
Journal codecs, external managed graph envelope and Gremlin syntax are unchanged.

The reserved state-machine namespace, separate from graph keys, is:

- Segment key: `sm/decision/`, big-endian u32 format 1, one role byte (0 coordinator,
  1 member), big-endian u64 local shard and epoch, 16 local mode bytes, big-endian
  u64 coordinator shard, 16 coordinator mode bytes, big-endian u64 coordinator
  epoch, then big-endian u64 absolute first ordinal.
- Each canonical strict JSON value has `version`, `domain`, `first`, `records`.
  Domain fields are `role`, `shard`, `epoch`, `mode`, `authority`, `cohort`.
  Role is `Coordinator` or `Member`; authority is shard/mode. The complete cohort
  includes coordinator shard/epoch and every ordered member shard/epoch/mode.
  Records use the unchanged A1 field order: `id`, `ordinal`, `participants`,
  `prepared`, `decision`. Exact intents and all prepared bits, including Abort
  bits, are retained. Request bytes and original Plans are excluded.
- `sm/decision-manifest` stores `version`, `domain`, `through`, `segments`,
  `encoded_bytes`, `applied`. Through is also the exact contiguous record count;
  segments is ceil(through/32). Encoded bytes counts every full key and canonical
  row value. Applied is the same complete Raft applied LogId as `sm/applied`.

Segment values are limited to 1 MiB and manifests to 64 KiB. These are conservative
codec bounds, not independently consumable admission quotas: a worst-width test
covers 32 records with 64 maximum-width participants and the full cohort and fits
below these bounds. At most 32 segment rows exist under the unchanged 1,024 cap.
Integer arithmetic and segment starts are checked; malformed keys, noncanonical
JSON, duplicate/unknown fields, unsupported versions, wrong domains, missing or
extra segments, incorrect records/counters and Commit without prepared evidence
fail closed. Golden tests freeze key, row, manifest and new disk header bytes.

New segments, growth of the old partial tail, manifest, complete authoritative
state, request index, local data/history and Raft applied marker are published in
one synchronous state DB WriteBatch. In-memory publication follows sync under the
same state lock. Sealed segments are not rewritten during apply. An existing
partial tail is verified before replacement, so ordinary apply cannot conceal
its corruption. Rejected admissions do not manufacture terminal records.

This duplicates terminal history on disk; it does not remove the full Journal
from memory or disk. Manifest accounting still walks the bounded complete
terminal prefix on apply. The change is not a throughput or memory reduction
claim, and no performance experiment accompanies it.

## Actual range-backed read path

After the existing leader barrier, coordinator Summary reads and validates the
published manifest. Terminal fetches only the stored segments intersecting 1..V,
checks each against the exact authoritative Journal records under the same state
lock, and constructs the existing checked A1 proof. Member visibility metadata,
Prefix and certified Read similarly use the member index under the same state
lock as snapshot capture. Explicit older Journal-taking local certification
helpers also check the stored cut before returning their legacy evidence.

If V falls inside a segment, the full stored segment is validated but only records
through exact V enter the returned proof. Coordinator/member tails of different
lengths therefore yield identical proofs for their common old V. V=0 checks the
complete manifest/domain and produces no terminal records. An explicit future V
still fails rather than downgrading. The ordinary same-V rows, pinned sessions,
negative Abort identities and original-Plan retries retain their prior behavior.

This is a range-backed **1..V prefix** operation, not a new arbitrary start..end
public RPC. The final A1 proof still contains and checks every record through V;
work and output remain O(V), bounded by the existing limits. No remote success
boolean or single segment substitutes for the complete proof.

Online reads validate the manifest and each requested segment, not the entire
unrelated future range. A corrupt later segment need not invalidate an earlier V
whose segments are intact. Full range closure, missing/extra rows and accounting
are checked on reopen. Real TCP tests delete target segments while nodes remain
running and prove Terminal/Prefix/Read return DecisionIndexHistory, without
in-memory fallback. Summary itself does not claim to certify terminal contents.

This segment-index slice used managed RPC version 3 without changing proof schemas.
[Incremental consumption](INCREMENTAL_DECISION_CONSUMPTION.md) subsequently advances
the internal wire to v4; the A1 proof schema remains unchanged.
New typed errors DecisionIndexHistory and DecisionIndexUnavailable report corrupt
or unavailable index storage. Older strict clients may reject these new error
variants as protocol errors; they must not reinterpret them as absence or Abort.
Older binaries reject disk versions 9/10. Deploy compatible storage binaries before
migration; no online downgrade is provided. Transport trust/authentication and
unrestricted GlobalCommittedSnapshot Unsupported behavior remain unchanged.

## Migration, snapshots and recovery

Validated legacy bound coordinator v5/v8 and versioned-member v7 databases may
continue their explicitly tagged in-memory projection path until their first
successful apply. That apply atomically constructs all segments and switches the
disk header. A failed sync leaves the legacy state and read behavior unchanged.
Legacy formats carrying decision segments/manifests reject as mixed/orphan state.
An indexed v9/v10 database never falls back to legacy projection or repairs itself
on ordinary read/reopen when index evidence is damaged.

Snapshots still contain the complete authoritative Journal, participant intents,
D1 write history and commit mapping. Install first validates all existing exact
history-successor and applied-cut rules, then rebuilds the complete derived range
and clears stale index keys in the same batch as the snapshot and state. Only this
explicit validated snapshot operation may repair a damaged derived index. A newer
Raft marker cannot authorize erasing an older prefix. Whole-memory snapshot build,
transfer/install and eager full-Journal reopen costs remain unchanged. Restoring an
older complete database directory is still not detectable without an independent
trusted high-water anchor.

Tests cover pre-sync failure, real process SIGKILL after sync but before publication
or ACK for coordinator tail growth, member application, legacy migration and
snapshot install; exact reopen/retry and older-prefix reads; 31/32/33 boundaries;
hole closure; member applied versus observed resolved; and the full 1,024 limit
with final reserved Abort and lagging nonparticipant catch-up. These are functional
fault and retention tests, not scale/performance claims.

## Next contract, not implemented here

Prepare still carries the full authoritative Journal. The subsequent
[incremental catch-up flow](INCREMENTAL_DECISION_CONSUMPTION.md) transfers bounded
event suffixes but still retains that complete authority. Moving
to incremental authoritative ledger storage requires a separately versioned
archive/event ledger, event sequence distinct from transaction ordinal, exact
extension anchors, bounded range transfer, original-Plan lookup, durable pending
states and snapshot closure. This derived index alone cannot certify erased or
unseen history and is not a checkpoint authority.

There is currently no independent per-member decision quota: each member consumes
the same bounded Journal, unknown Abort uses that retained Journal as its negative
identity, and successful prepare reserves its future local D1 commit history.
This patch does not invent per-transaction pre-admission reservations that could
be orphaned before admission.

Before introducing independently bounded participant ledgers or removing the common
lifetime bounds, reserve decision-consumption capacity across **every fixed-cohort
member, including nonparticipants**. A deterministic common bound or durable,
epoch/cohort-bound capacity-grant protocol must prove admission/resolution safety,
exact retry, failed-before-admission handling and snapshot/crash behavior.
Coordinator-only reservation is insufficient. Logical capacity cannot guarantee
physical disk/fsync availability; physical failures remain uncertain outcomes.
No existing ID, Plan, old V, unresolved transaction or lagging member may be silently
expired or dropped to reclaim that capacity.
