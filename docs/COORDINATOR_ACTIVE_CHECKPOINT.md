# Consumed coordinator active checkpoint (S2)

Status: **merged experimental slice** ([PR #112](https://github.com/ashione/VelaGraph/pull/112)).
This is not a capacity increase or a measured performance result. The format and
path descriptions below distinguish S2 itself from later merged extensions.
The [S1 ledger contract](AUTHORITATIVE_EVENT_LEDGER.md) remains the historical
contract for actual disk-v15 migration inputs. S2 writes coordinator disk **v16**;
member disk v12/v14, raw formats 1/2, logical SnapshotV1 and managed RPC v5 stay
unchanged in S2. The follow-on [S3 member checkpoint](MEMBER_ACTIVE_CHECKPOINT.md)
writes member v17 while preserving actual v12/v14 migration readers.
[S4 retained control](RETAINED_GATEWAY_CONTROL.md) subsequently introduced managed
RPC v6 and removed ordinary gateway full-Journal control/Prepare dependencies. Tracking: https://github.com/ashione/VelaGraph/issues/108 and
https://github.com/ashione/VelaGraph/issues/109.

## Replacement and authority

The durable bound coordinator uses `Coordinator::Active(Arc<ActiveCheckpoint>)`
instead of a resident or ordinary apply-candidate full Journal. The actual Raft
Coordinator command path consumes this state. It is not an unused checkpoint,
a second historical authority, a new wire API or an additional request directory.
Original segmented Commands remain the sole historical authority.

The checkpoint retains immutable authority/cohort/profile and exact complete
capacity grants, absolute counters, canonical byte accounting, and transactions
in `(R,N]`. It has no original event vector, all-request vector, Plan archive or
resolved terminal archive. `Coordinator::Full` remains for actual legacy and
memory-only operation and explicit full-history boundaries. Serializing an Active
coordinator as a logical full Journal fails unless that boundary explicitly
materializes one.

The private published cut and runtime share the **same immutable checkpoint Arc**.
Candidate mutation is copy-on-write; publication cannot mutate an earlier cut.
The cut also retains exact canonical metadata bytes, descriptor, complete applied
LogId and expected decision manifest. The storage metadata cache and private cut
each retain canonical metadata bytes, including the checkpoint, and complete
`current_snapshot` bytes also remain resident. A DB-snapshot read of `sm/d1` also
owns temporary canonical metadata bytes, including checkpoint bytes up to the
structural ceiling. Bounded target/page projection does not mean total reader
allocation is only one raw row plus its result. This is not a whole-process bound
independent of lifetime, nor a claim that all duplicate serialized bytes vanish.

## Counters, holes and unchanged capacity

- `E`: accepted original event count; retries and rejected commands add no event.
- `N`: admitted transaction count, including direct Begin.
- `Q`: external request count.
- `R`: local contiguous resolved transaction prefix, not member applied prefix,
  common checkpoint, reusable capacity or permission to release a grant.

Exactly one active record exists for each absolute ordinal `R+1..N`, in order.
It retains the original transaction ID, participant descriptors, prepared bits
and optional decision. If nonempty, the first record is unresolved. Terminals
above that hole remain active. Closing the first hole drains only the contiguous
terminal prefix; taking a maximum terminal ordinal would be incorrect. A durable
hole may keep the entire admitted interval active.

Both immutable profiles and complete-cohort permanent grants are unchanged:
`LegacyCountV1` keeps 256 requests / 1,024 transactions; `ByteBound16MiBV1` keeps
its structural maximum of 81,840 transactions or requests, derived from the same
16,777,216-byte reserved Journal budget. Event limits remain profile-specific.
The existing 64 participants, 64 KiB request, 256 KiB original Plan, 2 MiB event,
32 events / 4 MiB raw row, member intent and D1 retained-history limits remain.
These are ceilings, not promised admission counts or additional available quotas.

The canonical checkpoint decode ceiling is **22,435,158 bytes**:
`ceil(4 * 16 MiB / 3) + 64 KiB`. Source tests exercise the actual transaction
codec at integer/byte widths and the complete maximum-width grant/header envelope.
This is a structural bound derived from the old lifetime budget, not a larger
Journal allowance, measured RSS bound or an accepted release result. Bounded
vector decoders check count and canonical bytes before retaining further items;
checked arithmetic and final profile/canonical validation still apply.

## One transition and exact reservation rule

Journal append/replay and checkpoint apply share the deterministic transition
and reservation rule. There is no second admission grammar. Accounting keeps
actual canonical original history bytes (including versioned envelope and event
separators) plus pending future completion bytes. Pending reservations cover
not-yet-Prepared events and the worst terminal event for each unresolved admission.

Prepared/Decide replace the applicable pending reservation with actual event
bytes under the existing arithmetic; an Abort can remove unused future work.
**Advancing R never refunds historical bytes or creates admission credit.**
Draining records is materialization eviction, not history deletion. Exact old
retries retain their original precedence even at full capacity. Changed request,
descriptors or decision conflict; first-request/Plan retention remains unchanged,
including the already specified retry that proposes a different new Plan.
Unknown transaction evidence never means Abort.

## Ordinary disk-v16 apply

Under the existing state-machine write lock, the writer:

1. Uses one RocksDB snapshot to verify the complete raw namespace against the
   private published cut. Exact metadata, complete applied marker, domain,
   counts, canonical rows/Commands, greedy partitions, roots and expected
   decision manifest must agree; retired request rows/manifests must be absent.
   A disk-supplied root cannot certify replacement state.
2. Stages the active candidate and accepted original suffix. Historical request
   and transaction lookups use the shared raw walker and retain only requested
   records. An ordered current-batch overlay includes newly accepted Commands,
   including identities already drained from the candidate in that same batch.
   Retries and rejected commands never enter this overlay.
3. Applies the shared transition, including exact reservations and hole draining.
   Later Prepared/Decide and original-ID retries see durable history plus the
   accepted overlay; no all-ID directory is introduced.
4. Stages only the verified old raw tail and new rows. The terminal writer checks
   the prior partial tail against exact historical records, appends newly
   contiguous terminals, and updates totals from trusted old totals and changed
   row sizes. It does not reconstruct all old terminals merely to recount bytes.
   Ordinary writes preserve sealed raw and terminal rows.
5. Constructs the complete next private cut before sync. One WriteBatch publishes
   checkpoint/header, raw and terminal changes, manifests, applied marker and
   membership. Runtime/cut publication follows successful sync under the lock.

Blank/membership/no-new-event entries refresh the full LogId, including leader
identity. Rejected application commands leave the candidate unchanged while
retaining existing Raft marker/error-result behavior. Failure before sync publishes
nothing; death after sync reopens the committed cut. Ordinary apply never repairs
changed authority from memory. It still checks **all raw history**; there is no
O(1), faster-write or throughput claim. The operation-local suffix and drained
terminal collection remain bounded by the old lifetime limits, not a new small
per-operation quota.

Ordinary remote Command preflight now checks compact coordinator identity/cohort
state rather than unconditionally reconstructing a Journal. Explicit Journal,
grant/control, client status/decide/Prepare, member and Terminal proof paths still
use full temporary or retained materialization where required. Terminal checks
retain exact historical transaction comparison. S1 LookupRequest/HistoryPage
continue to use raw closure without a full Journal. No RPC version changes.

## Strict recovery, migration and snapshot repair

Cold disk-v16 open independently replays all original Commands with the shared
semantics, validates derived closures, derives the checkpoint and compares every
stored field before publishing it. The temporary full Journal is then discarded.
Unknown/duplicate/missing fields, wrong counters/reservations, changed active
records, profile/grant/domain/marker mismatch and corrupt namespaces fail closed.
There is no old-format, stored-snapshot or cached-memory fallback. The private cut
is not deserializable authority; hash equality assumes SHA-256 collision resistance,
not authentication. Cold recovery working memory is not independently bounded.

Actual disk v15 and earlier published coordinator inputs retain their strict
legacy readers until a successful batch migrates them. Migration may use complete
replay and must validate any applicable old request index before retiring it.
The successful batch writes disk v16 and publishes only Active coordinator state;
no full Journal remains beside it. Blank/no-event batches can migrate as well.
Failed sync preserves the old generation. A genuinely unbound coordinator retains
its actual old representation until binding. Members remain v12/v14. Old binaries
reject disk v16; no downgrade or rollback path is supplied.

SnapshotV1 remains self-contained complete authority with full Journal, D1 data,
retries, participant state and commit mapping. Snapshot build explicitly
materializes a temporary Journal from validated frozen raw authority; it does not
serialize an Active coordinator as if it were complete history. Snapshot bytes
remain fully materialized and the current snapshot remains resident.

Install strictly replays the incoming complete authority and enforces the existing
identity/profile/activation/data successor rules. For a local Active coordinator,
it derives the incoming prefix at the independently published old `E`, requires
its computed domain/root to equal the private local descriptor, and requires its
recomputed checkpoint to equal the exact old checkpoint. A newer marker or
incoming self-reported root alone is insufficient. This independently anchored
prefix permits explicit validated snapshot repair even when local raw rows are
damaged; ordinary open/apply cannot repair them. The rebuilt raw authority,
derived closures, disk-v16 checkpoint and snapshot publish atomically. Restoring
an entire consistent older database directory remains undetectable without an
external monotonic anchor.

## Acceptance and remaining work

The source includes checkpoint codec/accounting tests and actual durable-apply
no-full-Journal tripwires, hole/retry/batch and recovery/corruption coverage.
The Full/Active differential tests compare representation and accounting through
the shared grammar; existing pinned golden/error/no-op/limit tests supply separate
regression expectations, not a duplicated transition engine. Their presence is
**not acceptance**. Frozen-source differential and boundary tests,
migration/snapshot and pre-sync/post-sync crash checks, full debug library/
TCP/process regression, formatter/clippy/diff/Python validation, fresh release
1,025 genuine gateway writes with original-ID/old-V/ACK-loss/snapshot recovery,
and required remote CI remain delivery gates. Existing 15s/60s budgets stay fixed.
Earlier source or executable results do not establish acceptance of a later tree.
The merged slice does not waive these gates for subsequent changes.

S3 has since supplied member active state and retained historical negative evidence,
and S4 removed the ordinary gateway control/Prepare Journal exchanges. Removing
16 MiB still requires a separately reviewed finite profile with active and
retained-archive budgets; all-member closed checkpoint certificates before
credit reuse; self-contained streamed snapshot closure; and bounded remaining
full-history operations and O(V) terminal proofs. Offline or lagging members
cannot be replaced silently or release credit. S2 adds no GC, renewable grants,
new profile, larger archive quota or unlimited-lifetime claim.
