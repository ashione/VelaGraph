# Transaction decision journal protocol prototype

Status: **experimental Raft-backed decision ledger**. The opt-in
[participant state and trusted in-process relay](PARTICIPANT_WRITES.md) now use
its decisions; existing graph writers and global query admission are not wired.
`GlobalCommittedSnapshot` remains Unsupported. Existing graph writes and reads
are unchanged. This document is not a declaration that D4 is complete.

Current storage/read extension: the [S2 draft](COORDINATOR_ACTIVE_CHECKPOINT.md)
writes disk v16 for bound coordinators. S1 disk v15 and older formats remain actual
legacy-reader/migration inputs. S1's derived bound request index remains retired.
The [archive-backed coordinator reads](AUTHORITATIVE_EVENT_LEDGER.md#archive-backed-coordinator-reads-s1)
continue to use original raw Commands.
LookupRequest and HistoryPage project bounded results from the complete raw
namespace against a private, previously strict-validated published cut. Their
reader helpers do not require a Journal. S2 also replaces the durable bound
coordinator resident/ordinary apply-candidate Journal with a consumed checkpoint,
while checking complete raw closure. Cold open, explicit control/Terminal paths,
member state and full snapshot materialization still use Journals. Shared
transitions preserve decision semantics and exact reservations; draining the
checkpoint refunds no historical bytes. S2 acceptance is pending, with no
performance or lifetime increase claimed. This changes neither semantics nor the
16 MiB budget. The [opt-in byte profile](COHORT_CAPACITY_PROFILE.md) replaces only legacy
count quotas; the original prototype formats/limits below remain legacy contracts.

The next atomicity layer needs a durable decision authority. A transaction ID,
ordered participant set, each participant's shard/ownership epoch/managed mode,
and immutable intent identity must be bound together before preparation. All
participants must prepare before a commit decision; an abort decision is terminal.
Absence of an ID and an RPC timeout cannot imply abort. The journal deliberately
rejects decisions for unknown IDs, instead of manufacturing tombstones that could
race a future Begin. Abort a known Begin through the same decision authority.

The protocol module implements deterministic transition logic. The D2 adapter
adds an explicit coordinator-only role on an empty managed shard, persists the
journal with the Raft applied marker in one sync batch, and restores it through
strict replay on disk open and snapshot install. It never
accepts a mutated participant set for a reused transaction ID. Participants are
strictly sorted by shard, unique and capped at 64. At most 1,024 transactions and
16 MiB of encoded journal are retained; Begin reserves sufficient bytes for all
remaining prepare acknowledgments and a terminal decision so quota exhaustion
cannot strand an admitted transaction; no eviction, implicit abort or deadline
expiry exists. Exact retries do not append events. Rejections leave state unchanged.
All loops have those limits or the input byte limit as a termination bound.

The journal serializes format version 1, authority identity and transition events.
Decode rejects unknown fields, unknown versions, invalid transition order and
redundant replay events. Derived status and prefix are rebuilt rather than trusted
from serialized values. Installing a replacement over an existing journal requires
an identical authority and exact event-history prefix. Fresh recovery cannot detect
an old whole-directory rollback without an independent durable anchor.

`resolved_prefix` includes both committed and aborted transactions and stops at
an unresolved ordinal. **It is neither a committed-data watermark nor an MVCC
read version**. A Commit event does not prove that data was applied on any shard.
Low-level Prepared entries remain trusted internal protocol inputs, not
cryptographic participant proofs. The in-process participant relay only records
them after the participant Raft group durably accepts the full intent. Exposing this module directly to untrusted callers would not create a
correct distributed transaction system. Intent IDs are not authentication tokens.

## Role and format boundary

D1V1::EnableCoordinator requires a matching managed mode ID, current shard/epoch,
zero data sequence and empty data. It is irreversible and idempotent. In this
role, ManagedWrite returns CoordinatorOnly; raw D1 Write and legacy TxnBatch
remain rejected by the managed/D1 fences. D1V1::Coordinator uses shard/epoch/mode
checks before applying a decision command. Existing APIs do not activate it.
A committed role cannot be erased or have its decision history rewritten by a
snapshot, even if the incoming applied-log marker is newer.

The role uses logical D1 version 4 and disk metadata version 5. Older binaries
reject these discriminators. Non-coordinator encodings are unchanged. Upgrade
all participating binaries before explicit activation; no mixed-version rolling
upgrade or downgrade is supported. The same empty shard cannot later become a
data shard. This is a trusted development protocol, not an authorization system.
The pure Journal API does no I/O; only its D2 command adapter persists results.

## Required integration before global transaction support

1. Implemented prerequisite: an explicit, irreversible coordinator role on a dedicated Raft group.
   Every raw `TxnBatch`, D1 Write and ManagedWrite entry must be rejected in that
   role. A normal managed KV convention cannot protect decisions from other writers.
2. Implemented prerequisite: apply validated coordinator commands, state, Raft applied marker and membership
   in one sync batch, with no publication on sync failure. Version disk metadata
   and snapshot envelopes so older binaries fail closed. Restore must preserve the
   exact authority/history prefix and reject data/coordinator role substitution.
3. Prepare data-shard intents under a participant mode that also fences existing
   ManagedWrite. Persist exact bytes and conditions, reserve bounded capacity,
   and retain conflict locks across restart. Do not publish pending data. Avoid
   deadlock through ordered preparation or explicit abort/retry; never silently
   replace a failed transaction ID. Whole-shard locking is an initial conservative
   option, not an availability/performance promise.
4. Record prepare acknowledgments only after verifying the participant's durable
   intent. Persist one global decision before notifying participants. Recovery
   queries the authoritative decision: Unknown stays unresolved, and partitions
   cannot guess abort. Retain pending state and locks on unavailable authority.
5. Participant resolution validates authority, transaction identity and exact
   decision; atomic data publication plus resolution metadata must be idempotent.
   A coordinator decision alone must never open the global read path.
6. Introduce MVCC data, fixed routing/catalog epoch, applied watermarks, version
   pins and bounded retention. A globally readable prefix requires complete
   participant application and all unresolved holes addressed, not just journal
   decisions. Old query views remain on their original version.
7. Route every graph mutation through this protocol, including staged source and
   reverse adjacency changes. Add crash cuts at every acknowledgment/persistence
   edge, cross-shard concurrent read/write oracles, and full CoreHR CI. Only then
   reconsider the strict query API's Unsupported response.

Graph record keys and values are unchanged. Only explicit coordinator activation
changes D1 metadata format. The ledger encoding is versioned independently; no
online downgrade or automatic role migration exists. No readable global version
is issued, and no participant data is applied by this feature.
