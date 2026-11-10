# Consumed member checkpoint (S3)

Status: merged experimental slice ([PR #113](https://github.com/ashione/VelaGraph/pull/113)).
This is the finite-profile member dependency slice for Issues 108/109, not a
capacity increase. Subsequent [S4 retained control](RETAINED_GATEWAY_CONTROL.md)
uses internal RPC v6; the v5 references below describe S3's original wire boundary.
[S2](COORDINATOR_ACTIVE_CHECKPOINT.md) supplies the consumed coordinator
checkpoint; the same private `History::{Full,Active}` representation now serves
bound durable members. `Coordinator` is a private compatibility alias, not a
second state machine. Active implicit Journal serialization fails.

## Authority and distinct frontiers

Member disk **v17** stores a checkpoint/header plus the original segmented raw
Commands. Raw formats 1/2, the two immutable capacity profiles, SnapshotV1 and
managed RPC v5 are unchanged. Actual published member v12/v14 readers remain;
v17 is not a reinterpretation of their bytes. Old binaries reject v17.

The shared immutable checkpoint retains exact authority/cohort/profile and full
capacity grants, original accounting and absolute E/N/Q/R, and only `(R,N]`.
Member A is the separate durably applied contiguous ordinal. A may be zero while
R is far ahead. Resolved-but-unapplied `(A,R]` is projected from the original raw
Commands. No idle second Journal, all-retired-ID map or Plan archive is retained.
Existing participant Intents, CommitVersion and D1 retry history remain resident
under their separate existing finite budgets.

A non-deserializable private member cut shares the exact checkpoint Arc with the
published state under its write lock. It binds canonical member metadata, complete
local Raft LogId, exact raw descriptor/domain/root and the expected member
terminal manifest through A. The local marker is distinct from observed E.
Each operation anchors one DB snapshot to this cut; disk-provided roots and
metadata caches cannot authorize a substitute history. Complete raw namespace,
canonical partitions/Commands, domain, counts and roots are checked before a
result is returned. Member derived terminal rows are compared during one ordered
active/drain projection, with only one 32-record terminal row resident. The
projection uses shared transaction transitions and contiguous-front reduction,
never maximum terminal ordinal. Later rows still close even for an old target.

The verification projection can retain the historical maximum active interval,
which is bounded by existing profile count/byte ceilings. It does not retain
original Plans, Commands or drained records. Metadata byte buffers and the
current SnapshotV1 remain materialized; this is not a whole-process bound
independent of lifetime.

## Ordinary durable consumption

Actual SyncHistory, AdvanceRetained and Prepare dispatch through checkpoint/raw
consumption. An ordered whole-batch suffix retains only accepted original
Commands, including records already drained from the candidate checkpoint.
A private immutable member overlay borrows the exact verified snapshot and
shares its complete observed checkpoint plus accepted suffix. Only shared
transition-approved appends create successors. Unchanged overlap/read commands
reuse that same-operation evidence without replaying old suffix transitions;
full raw closure and exact incoming overlap checks still run. Local A, Intents,
Prepare and commit-map checks use the current candidate on every command.
Every new transition uses durable prefix plus earlier accepted suffix identities;
future identities cannot authorize older admission replay. Exact no-op retries
and rejected events never become original archive events.

SyncHistory checks base/end/cut roots and overlapping Commands even if local E
is later than the requested cut. It preserves that later observation, and does
not change A or publish data. Per-command candidate state and accepted suffix
are isolated: a page failing after an initially valid event, or conflicting with
a local intent, rolls back the complete page before the next Raft entry runs.
Unchanged local identities inherit the already strict published-cut invariant;
new original Begin/Admit/Prepared/Decide IDs check every matching resident local
record, with compact cohort/grant checks always applied. No cross-operation
lookup cache skips raw closure.

AdvanceRetained first proves the target resolved at the **exact requested cut**,
before even an already-applied retry. The bounded target-only proof counts
admissions and tracks unresolved IDs only among the first target ordinal. At
exact E it verifies the cursor/root and freezes `admitted >= target && empty`.
All later raw rows still close but cannot change this predicate. Its input is
strictly anchored raw history and a private immutable suffix witness constructed
only by the shared transition rule; this is validated-history projection under the existing SHA-256
assumption, not alternate semantic authority. A later Decide or hole closure
cannot authorize an old cut, even if current R and A already exceed the target.

Applying A+1 projects the exact transaction. A local Commit requires its retained
prepare and atomically updates data, D1 retry/sequence, participant terminal,
CommitVersion and A. Abort/nonparticipant progress has no data commit. Consumed
unknown-intent Abort is represented by the exact archived ID/local descriptor,
Abort outcome and ordinal <= A; it allocates no new tombstone even at full Intent
capacity. A future terminal above A remains VisibilityUnresolved. Wrong identity
conflicts; missing or corrupt evidence never becomes Unknown or Abort.

Byte-profile Prepare validates own activation nonce/grant readiness and exact
original AdmitRequest Plan part before returning an idempotent local status or
archived negative Abort. First external-ID/request/Plan behavior and legacy
profile semantics remain unchanged.

A private staged member-terminal witness projects exactly old A+1 through the
candidate A from verified raw plus accepted suffix. It binds the old cut, full
staged descriptor and the exact borrowed candidate. Its member-specific manifest
cannot be supplied for another metadata candidate. Sealed rows remain unchanged;
only the prior partial raw/terminal tails and new rows are staged.

One synchronous WriteBatch publishes metadata/checkpoint, raw tail/new rows,
derived terminals through A, data/retry/sequence, commit map, full marker and
membership. All fallible staging precedes sync. Rejected application commands
retain established marker/error-result semantics. Storage/closure failures abort
the whole batch. Failed sync publishes nothing; post-sync/pre-publication death
reopens the exact committed generation.

## Explicit compatibility, recovery and repair

Memory-only/unbound modes and actual old migration inputs retain Full. Strict
cold open replays all original Commands and checks complete participant/data/
commit-map/derived closures, recomputes the stored checkpoint, then discards the
Journal before publishing v17. Cold recovery is not bounded independently of
retained history.

SnapshotV1 build/install and explicit whole-Journal terminal proof/control
exports remain full-materialization boundaries. Whole-Journal wire Prepare and
legacy AdvanceVisibility remain accepted compatibility inputs; comparing them
to retained member history uses exact raw-prefix evidence, not hidden local
Journal construction. Ordinary member apply, participant status, visibility
metadata, observed-history status and capacity-grant readers use the compact
cut/projections. The no-Journal switch covers their actual worker closures.

Snapshot install independently proves the incoming original prefix at the
private old E/root and recomputes the exact old checkpoint. It separately
preserves old A, commit-map prefix, participant identity/history, activation,
profile, data/retry prefix and complete applied marker. A newer self-reported
root/marker is insufficient. Only explicit validated snapshot installation may
repair corrupted local raw rows. Ordinary reads/apply/open never fall back to
memory or a saved snapshot to repair authority. Disk17 ordinary closure includes
retired request namespaces even for Blank entries: injected orphan rows now cause
a whole-batch storage error with no marker or state publication. This tightens
the prior online Blank behavior; actual legacy disk12/14 inputs and their cold
rejection/no-silent-cleanup boundary remain separate compatibility checks. A
legacy12/14 member must satisfy the complete retired-namespace closure before
its first write can mint disk17 and a private cut; a failed migration retains the
old format, bytes, marker and runtime without clearing the offending rows.
Whole-directory rollback is still undetectable without an external monotonic anchor.

D1 retry/data namespace validation retains its existing cold-open/snapshot
boundary. S3 does not claim a new ordinary-operation full D1/data integrity scan.

## Unchanged bounds and costs

The 16,777,216-byte lifetime history/reservation budget remains monotonic; R and
A release no quota. LegacyCountV1 remains 256 requests/1,024 transactions;
ByteBound16MiBV1 keeps its existing structural ceilings. Intent 16 MiB/count,
D1 4,096 commits and existing 64/128 MiB options, 64 participants, 64 KiB request,
256 KiB Plan, 2 MiB event, 32 events/4 MiB raw row, and complete terminal proof
bounds remain unchanged. No GC, new profile, epoch reset or reusable grants.

Checkpoint canonical ceiling is 22,435,158 bytes. Member metadata has a separate
structural ceiling: `4 * MAX_INTENT_BYTES + 256 * MAX_DEDUP_REQUESTS +
MAX_ACTIVE_CHECKPOINT_BYTES + 16 KiB`. It does not reuse the coordinator's small
shell limit. Byte-array JSON can expand a legal near-16 MiB Intent to near64 MiB;
codec tests retain that real accepted scope, test maximum scalar/identity widths
and the 64-member envelope, and bound commit/Intent vectors with existing policy.
Encoded bytes and resident allocations are distinct.

Complete raw closure is O(E) I/O. Point/Plan/frozen-cut projections may perform
additional full scans on the same snapshot. Accepted batch suffix and staged new
terminals remain bounded by existing lifetime ceilings, not a new small quota.
No throughput, constant-time lookup or unlimited-lifetime claim is made.

## Acceptance gates

Source tests target the actual durable no-Journal path; A0/Rahead, holes and old
cuts including already-applied retry; same-batch suffix/rollback/drain/Prepare;
full Intent/D1 Abort/nonparticipant progress; original Plan/idempotency; namespace
corruption and exact metadata; raw/terminal boundaries; full LogId updates;
strict v12/v14 migration; snapshot successor/repair; failed sync and independent
OS-process post-sync/prepublication lost-ACK crash. Their presence is not a pass.
Frozen-source formatter/clippy/risk-selected tests and full fresh-head remote CI,
including genuine release 1,025 managed writes with existing deadlines, remain
acceptance gates. No previous S2 executable result proves S3.
