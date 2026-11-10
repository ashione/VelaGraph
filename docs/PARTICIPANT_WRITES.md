# Durable participant intents and trusted coordinator relay

Status: experimental opt-in D2 library slice. A participant now retains prepare
intents, applies a terminal decision atomically on one shard, and can consult the
Raft-backed coordinator through a trusted in-process relay. This is **not globally
atomic query visibility**, MVCC, production authentication, a remote graph writer,
or complete D4. GlobalCommittedSnapshot remains Unsupported.

## Explicit roles and write coverage

EnableParticipant binds an existing managed shard to a distinct coordinator shard
and nonzero authority mode ID. It captures the current data sequence, preserves
baseline data/history, and is irreversible. Exact retries cannot change the
binding. Coordinator and participant roles are mutually exclusive. Existing
unmanaged behavior is unchanged; no graph workload enables this automatically.

Once activated, ordinary D1 Write, legacy TxnBatch and ManagedWrite cannot modify
participant data. Direct replicated commands hit the same checks. The only data
publication path is a prepared participant Commit. Unmanaged graph mutation
wrappers therefore fail explicitly on an opted-in participant; there is no
fallback. The separate [managed graph client](MANAGED_GRAPH_TRANSACTIONS.md) and
[gateway](MANAGED_GRAPH_GATEWAY.md) now supply compatible typed writes for a
certified fixed cohort. Reads remain available.

## Prepare and conflicts

A prepare binds the complete request bytes, transaction ID, shard/epoch/mode and
immutable intent ID. The 32-byte intent ID is a caller-assigned identity, **not a
content digest or authentication proof**. Reusing an ID with another body or
participant identity fails. Historical pre-activation request IDs cannot be reused.
All requests must include expected_sequence matching the current shard sequence.

The initial implementation allows one unresolved prepare per shard. That prepare
retains a conservative shard-wide conditional-write reservation so a read-dependent
batch cannot be invalidated by another write after successful preparation. A new
prepare conflicts while it is pending; no lock is taken on unrelated shards.
Reads and already captured copied snapshots continue. This is not a promise of
zero latency impact or uninterrupted writes to a shard with an unresolved intent.
No cluster-wide stop/drain operation is used, and no graph shape is special-cased.

Prepare writes only intent metadata and the applied-log marker in one sync batch.
It does not change graph bytes or data sequence. It reserves the future D1 commit's
retained request slot/bytes before acknowledging. At most 1,024 intent identities
and 16 MiB of accounted intent memory are retained; request limits remain unchanged.
A terminal decision on an existing intent cannot be blocked by retention quota.
There is no automatic pruning. Exhaustion rejects new work instead of forgetting
retry identities. Ownership epoch changes are rejected while an intent is pending;
Raft leader changes within the group do not change the D1 ownership epoch.

## Decisions, uncertainty and status

Commit atomically writes the original batch, local D1 sequence/dedup identity,
participant terminal status and Raft applied marker. Abort leaves data unchanged
and retains a permanent aborted identity. Exact terminal retries return the same
result; reversing a decision fails. An Abort for an unknown participant ID records
a negative identity, preventing a delayed Prepare from resurrecting that ID.
Unknown Commit is rejected. Unknown status and timeout never imply Abort.

participant_driver::prepare checks the coordinator transaction and participant
identity under a leader read barrier, persists the participant intent via Raft,
then records Prepared in the coordinator. participant_driver::resolve first reads
the coordinator's immutable terminal decision under a leader barrier. If the
coordinator is unknown, undecided or unavailable, it leaves the intent unresolved;
it does not guess or automatically choose a new transaction ID. Each RPC-equivalent
step has a five-second bound; a timed-out write may still apply. Retain exact IDs
and bodies and resume status/resolve rather than rebuilding the mutation.

The relay is deliberately in-process and trusted. Low-level Prepare/Resolve D1
commands are internal replication commands, and Resolve trusts the submitted
coordinator decision. It does not verify a cryptographic quorum certificate or
protect against a malicious component injecting contradictory decision commands.
The current plaintext/unauthenticated development transports are not a security
boundary. Do not expose these primitives as an untrusted transaction service.

The relay's status helper establishes a participant leader barrier. Direct
StateMachine::participant_status is only a local diagnostic read; callers need
the barrier before using it for recovery. Exact-body retry, not ID status alone,
checks that the committed identity matches the intended operations.

## Recovery and versions

Participant activation uses logical D1 version 5 and disk metadata version 6.
Old binaries fail closed. Default/managed-only/coordinator encodings are unchanged.
Upgrade all involved binaries before activation; mixed-version rolling upgrades,
role removal, authority rotation and online downgrade are not supported.

Reopen restores pending intents and their reservations. Snapshot decode validates
participant request identities/conditions and matches every post-activation commit
to exact retained D1 history. Installation over an existing participant cannot
erase or rewrite prepared/terminal identities, change authority, or undo a terminal
decision. Full graph data still matches D1 history replay. Sync failures publish
neither data nor status. Restoring an entire stale directory remains outside the
supported recovery protocol; this does not add remote backup management.

## Optional version-specific reads

A separate [versioned participant mode](VERSIONED_PARTICIPANT_READS.md) fences
legacy Resolve and publishes through contiguous applied coordinator ordinals.
It provides bounded historical reads and uses the applied journal as the negative
identity for unknown Abort, without extra tombstone quota. Participant-only mode
retains the behavior described above. The later
[fixed-cohort session layer](CERTIFIED_COHORT_READS.md) adds restricted common-V
admission; the older unrestricted Global API still rejects it.

## Verified boundary and next work

Tests exercise pending reopen, commit sync failure, duplicate commits, opposing
decisions, competing prepares, old-writer fences, epoch fencing, late-prepare
abort tombstones, snapshot intent removal/rewrites and stable retained read views.
A real in-process coordinator plus two independent Raft groups exercises the relay:
after resolving only the first participant, fresh reads can see new/old values.
That counterexample is intentional: this slice has no unified visibility barrier.

Subsequent merged slices provide managed graph planning, durable original-Plan
recovery, [fixed remote transport](REMOTE_MANAGED_TRANSPORT.md) and certified
fixed-cohort read sessions. Remaining general-purpose work includes automatic
recovery scheduling, finer conflict tracking, indexed MVCC, retention/GC,
dynamic/nonempty-baseline certification and full-scale managed CoreHR validation
([#111](https://github.com/ashione/VelaGraph/issues/111)). Existing full observed
CoreHR CI is a regression gate, not proof that its writers use this protocol.
