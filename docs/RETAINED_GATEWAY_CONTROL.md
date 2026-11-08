# Retained gateway control (S4)

This slice consumes bounded target evidence and paged history in the actual
managed graph execute, status, retry, abort and Prepare paths. It keeps both
existing finite capacity profiles and their unchanged admission limits. It is
not a larger archive, GC, grant reuse, authentication, or performance claim.

## Source evidence and trust

`TargetEvidence` contains version 1, the exact observed original-event cursor E
(including complete authority/cohort/profile), contiguous resolved ordinal R,
queried transaction ID, and either explicit absence or one exact transaction
plus its optional original external request/Plan binding. Direct Begin has no
binding; AdmitRequest retains the first original request and Plan. Ordinal,
ordered participant descriptors, prepared bits and decision are all returned.
An old external ID with the same request returns its first Plan, even if the
caller proposes a different new Plan. Changed request bytes conflict.

A durable source holds one publication lock and anchors one RocksDB snapshot to
its private immutable checkpoint, full applied Raft LogId, canonical metadata
and raw descriptor/domain/root. The shared original-event projection counts
admissions, checks the target ordinal against actual admissions at that E, and
finishes the complete namespace/raw closure before returning presence or absence.
Missing, foreign, extra or noncanonical rows do not become Unknown or Abort.
The source never reconstructs a Journal for this target read. Original Plans are
not copied into a new persistent directory or retained-ID map.

The client validates bounded structure and full requested identity. A target
and self-consistent cursor alone are not an inclusion proof: this remains the
existing trusted internal transport. A forged arbitrary root cannot be disproved
by a client holding only that reply. Source disk validation, independent retained
member cut validation, and complete terminal proofs have different roles.

## New deterministic Prepare contract

`D1Command::PrepareRetained` carries a versioned strict payload containing the
exact authority, participant descriptor, D1 request and required source cut.
It carries neither a Journal nor a caller-supplied replacement Plan. Nested
request/operation objects reject unknown and duplicate fields. Optional fields
must be explicitly present, including null where the existing value permits it.
The old `PrepareParticipant` variant retains its original bytes and trusted
legacy semantics; the new command does not silently redefine old Raft entries.

The gateway transfers existing HistoryStatus/Page/SyncHistory pages to the fixed
cut before Prepare. If the member has observed later history, an empty source
page at the old cut is still checked locally; later history is never rolled back.
SyncHistory changes E, not member applied ordinal A or graph data.

At deterministic apply, the member independently proves the exact old cursor
from its private archive and accepted same-batch suffix. It projects only events
through that E, freezes the target and admission count, then finishes all later
closure. The transaction must already exist and be pending at that cut. Exact
ID/descriptor and the original Plan part must authorize the proposed request,
for both profiles. A later admission or decision cannot repair a bad old cut.
Current cohort, activation incarnation, full grants and capacity readiness remain
checked by the existing member helpers. All strict checks precede an idempotent
local return.

After those checks, the existing current-state precedence is preserved. An exact
resident Intent returns its current status, including Prepared while A has not
consumed a later observed terminal. Without an Intent, terminal above A stays
VisibilityUnresolved. An exact consumed archived Abort returns Aborted without
allocating a new tombstone. A terminal-at-cut Prepare is rejected even if a local
retry record exists. Invalid commands, pages and failed sync publish no partial
Intent, graph data, suffix or A; existing applied-marker/error-result semantics
and one-sync durability remain.

## Decision, visibility and uncertain replies

The gateway reads one exact target rather than a full Journal before/after Begin,
Prepare and decision proposals. A failed or lost reply never means Abort. The
same transaction and original Plan are retained for retry. Decision proposals
are followed by exact target readback, including after an uncertain ACK.

Status preserves a known terminal decision above an earlier unresolved hole as
`visible: false`. A numeric ordinal is not a visibility certificate. Whenever a
complete TerminalPrefix is available, its target record must equal the sampled
transaction in every field before the full prefix is compared on every member.
A mismatching or unavailable proof leaves visibility unconfirmed. Catch-up holds
the exact terminal target returned by decision and makes the same comparison.
E (observed events), R (resolved ordinal), and A (member applied ordinal) remain
separate. The existing 30-second decision/catch-up and 5-second status-visibility
budgets, leader barriers, fixed endpoints and production pool limits remain.

Coordinator startup uses source-validated Summary. Capacity confirmation shares
a private compact cohort/grant reader with command identity preflight, including
before/after submission and retry. It cannot replace a retained incarnation or
mint reusable grants. S3's compact participant grant/status readers are reused.

## Compatibility and bounds

Managed wire version is **6**, with strict equality and no v5 fallback. New tags
17 (LookupTarget) and 18 (PrepareRetained) do not change older operation tags.
Whole-Journal Prepare/Advance/export remain explicit compatibility APIs, as does
the unbound in-process participant relay. They are not the live graph path.
Complete terminal proofs/old-V reads and SnapshotV1 remain explicit bounded full
history boundaries. Cold legacy recovery/replay remains strict.

The Raft envelope remains D1V1 with a new uniquely named command variant. Old
binaries reject that variant and cannot replay logs containing it. Same-version
cohort upgrades are required; mixed-binary rolling operation or downgrade with
new entries is unsupported. The existing Raft TCP envelope has no version
negotiation. Unchanged disk16 coordinator, disk17 member, raw formats1/2 and
SnapshotV1 do not imply binary replay compatibility. No state fields, graph keys,
raw original Commands or existing encoded command meanings changed.

Both 16 MiB profiles and lifetime/reservation limits, 64 participants, 64 KiB
external request, 256 KiB original Plan, 2 MiB event, 32-event/4 MiB page,
Intent/D1 and complete proof limits remain. A direct Begin's D1 request retains
its existing 10,000-operation/16 MiB raw-batch limits; it is not narrowed to the
external request or Plan budget. The complete managed frame remains32 MiB and
may reject large legal raw requests whose JSON envelope does not fit. Point
results are bounded, but full closure still performs O(E) I/O. No constant-time,
throughput or whole-process lifetime-independent memory claim is made.

## Validation and diagnostic boundary

The nondefault `d2-raft/test-hooks` feature exposes only a doc-hidden node-scoped
fail-only full-Journal diagnostic. Graph tests explicitly enable it as a dev
dependency, including the complete constructor/clone/decode/load guard chain.
A separate negative-control node must prove the old route actually fails before
new durable gateway paths are accepted. Production defaults do not enable it;
feature selection for production and test builds is recorded separately.

Tests cover direct/admitted targets, strict malformed codecs and complete frame
budgets, old-cut/current-A/same-batch negatives, complete closure corruption,
failed sync, actual lost ACKs, startup/grant retry, holes, proof-target mismatch,
reopen and snapshot. Test presence is not an execution claim. Frozen-source
formatter/clippy/local risk-selected checks and fresh final-head full remote CI,
including the genuine release1025 lifecycle, remain acceptance gates.
