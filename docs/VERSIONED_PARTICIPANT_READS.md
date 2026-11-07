# Applied decision prefixes and retained-history participant reads

Status: experimental opt-in participant visibility foundation. It provides a
version-specific read of **one participant** and refuses versions not yet applied
there. It does not implement global query admission, a certified common baseline,
remote graph writer routing, indexed MVCC, GC or production authentication.
GlobalCommittedSnapshot remains Unsupported. Existing weaker read APIs keep their
existing semantics; they must not be presented as version-specific global reads.

## Version identity and explicit activation

A version is an ordinal allocated by Begin in one immutable coordinator authority.
It is not a wall clock, local D1 sequence or Raft log index. Each participant binds
to its existing coordinator shard/mode and fixed ownership epoch. Different
coordinator authorities cannot be combined just because their ordinals match.

EnableVersionedReads requires the participant role, no prior participant intent
records, and an empty coordinator journal with the matching authority. It captures
the local baseline sequence and fixes the ownership epoch. The operation is
irreversible and idempotent. Existing data may be a local baseline, but this does
not certify that independently imported baselines form a consistent global cut.
A future global admission layer must verify the complete participant set,
coordinator/baseline identity and pinned catalog/routing epoch. Do not infer that
certificate from matching local sequence numbers or equal values in a test.

Once active, legacy ResolveParticipant is rejected even for an exact retry. Data
can only be published through AdvanceVisibility. Other raw/managed writers remain
fenced. Ownership epoch changes are rejected; ordinary Raft leadership changes
within the group remain possible. Default and participant-only modes are unchanged.
There is no automatic mode activation or mixed-mode fallback.

## Watermark advancement

AdvanceVisibility carries an explicitly bound coordinator authority and a trusted
journal observation. The in-process relay obtains that observation after a leader
read barrier. Replicas validate the journal and its exact extension of known
history; this is not a cryptographic quorum certificate or an authenticated RPC.

Only ordinal applied_prefix+1 can advance, and it must have a terminal decision.
A resolved coordinator prefix alone is never enough to serve a read:

- Local Commit requires the matching durable prepare. Original data operations,
  local sequence/dedup, participant terminal status, global-to-local sequence map,
  applied prefix and Raft applied marker share one sync batch.
- Local Abort resolves an existing prepare without data writes. For an unknown
  local intent, the retained applied journal itself is the negative identity.
  No extra tombstone allocation can block an already decided Abort at the intent
  memory cap. Late Prepare consults that journal and cannot resurrect the ID.
- A nonparticipant decision still consumes exactly one ordinal without changing
  data. It cannot skip over an unresolved or unapplied earlier decision.

Exact old-prefix retries are accepted only when their journal history is compatible
with retained history and prove the requested ordinal resolved. They never undo
newer application. A rejected advance cannot partially publish terminal status.
Physical storage failure or timeout is an uncertain outcome, not an Abort decision.
Inspect/retry the same identity and prefix after restoring availability.

## Historical reads without a second data history

D1 already retains every successful ordered Put/Delete batch and validates managed
data against complete history replay. This first implementation reuses that history
rather than adding an independently corruptible before-image store. The persisted
ordinal-to-local-sequence map selects exactly the D1 history prefix for read_at(v).
The local baseline is included; later global commits are excluded. Multiple writes
to the same key and deletes follow their original batch order.

A participant with applied_prefix<v returns VisibilityNotApplied. It does not
silently return current data, choose an older version, or mix versions. A lagging
participant therefore prevents a caller from completing that chosen cross-shard
version. A caller may separately choose an older common version, but must use that
same version on every participant and validate the common authority and baseline.
No automatic cross-shard query coordinator is enabled by this slice.

Reads have explicit operation, live-row and live-byte budgets, capped at 4,000,000
replayed operations, 1,000,000 rows and 64 MiB. Intermediate replay state counts too;
a later delete does not excuse exceeding an earlier memory/work bound. Exceeding
any budget returns an error, never partial success. Replay borrows retained payloads
under the shard read lock and clones only the final bounded result. This is a
functional retained-history implementation, **not a performance claim for indexed
MVCC**. A bounded capture can delay local writers; returned views are independent
and remain stable while later writes and prefix advances continue.

The in-process read helper establishes a leader barrier. The StateMachine method
alone is a local diagnostic capture. Results report the requested version, actual
applied prefix, fixed shard/epoch, authority and selected local sequence. The
applied-prefix diagnostic may increase between repeated reads; the data for the
same retained version does not change.

## Retention, limits and recovery

No history is pruned, no query lease expires retained versions, and no compaction
API may discard these D1 identities. Existing prepare admission reserves the future
commit's D1 history slot/bytes. Retention exhaustion rejects new prepares rather
than evicting versions or preventing an admitted commit. Coordinator journal limits
also remain unchanged. This is bounded progress, not infinite write capacity. GC
requires a future protocol including active query pins, unresolved participants,
lagging replicas and recovery requirements; none is guessed here.

Logical D1 version 6 and disk metadata version 7 mark this mode. Old binaries fail
closed; compatible software must be deployed before explicit activation. There is
no mode removal, authority rotation, ownership migration or online downgrade path.

Reopen and snapshot decode reconstruct and validate the full mapping: every applied
local Commit has exact participant/D1 history, every consumed decision is terminal,
no terminal local record lies beyond the watermark, and all data commits are
mapped. A snapshot cannot raise a watermark over missing data, regress an existing
prefix/history, change the fixed epoch, discard retained versions or retroactively
certify previous participant-only writes. Failed sync publishes neither new data
nor its read watermark. Whole-directory rollback remains an unsupported recovery
procedure; this does not add remote backups.

## Tests and remaining gates

Tests cover partial application across two participants, refusal of a future
version, stable older versions after further writes/deletes/repeated keys, failed
sync/reopen, snapshot history preservation, unresolved holes, nonparticipant
advancement, legacy Resolve fencing, read budgets, exact-format golden bytes and
an actual in-process coordinator with independent participant Raft groups. A real
16 MiB intent-cap test proves an unknown Abort still advances and remains negative
after recovery without allocating another intent record.

Still required before global-query support: a certified initial baseline and fixed
participant/catalog cohort, end-to-end remote protocol and managed graph writers,
query admission/pins across a single chosen version, wider concurrent workloads,
and full CoreHR transaction/read verification. Existing full CoreHR CI is a
regression gate, not proof that its graph APIs use these new participant methods.

The optional [fixed-cohort session layer](CERTIFIED_COHORT_READS.md) adds persistent membership and empty-baseline certification. Existing unbound/nonempty modes cannot be automatically upgraded to that contract.
