# Durable external request binding

Status: internal admission/recovery API used by the separate experimental
[managed graph TCP gateway](MANAGED_GRAPH_GATEWAY.md). This journal API itself
adds no TCP endpoints or remote Global Gremlin.

## One atomic admission

Journal AdmitRequest records an external request ID, exact canonical application
request bytes, versioned complete write Plan bytes, transaction ID and full sorted
participant set. It creates the Begin transaction in the same event and durable
Raft state-machine batch. A failure cannot leave only a request record or only a
Begin. Admission reserves every remaining Prepared and final Decide event using
the same journal-capacity accounting as ordinary Begin.

Only an immutable bound cohort accepts this event. The shared request_plans codec
validates version, nonzero identities, coordinator authority, cohort membership,
ordered unique parts, participant epoch/mode/intent, original CAS presence, each
request's shard/epoch/transaction ID, and aggregate operations/payload limits.
The coordinator also checks that the encoded Plan matches the event's transaction
ID/participant set/cohort and that its validation version is already resolved.
This validates protocol identity, not application-specific graph semantics.

A new external ID cannot adopt a pre-existing transaction ID. The first valid
admission wins. A repeated external ID with identical request bytes returns its
original record without creating another Begin, even if a racing caller prepared
a different random candidate Plan. Different request bytes are an identity conflict.
Every caller must execute the returned original Plan, not its losing candidate.

## Graph API and recovery

GraphCohort::recover_request performs a leader-barrier lookup and checks the exact
request bytes. GraphCohort::admit_request atomically admits a validated candidate
and reads back the winning Plan. These are bounded internal APIs; remote clients
must never be allowed to submit raw Plan bytes or arbitrary participant lists.

A gateway must parse a typed operation and produce a stable canonical byte
encoding that preserves operation kind, complete parameters and type distinctions
(such as integer 1 versus real 1.0), and binds the mapping/cohort contract. This
slice compares bytes exactly and does not claim that arbitrary caller-provided JSON
text is already a canonical application command.

The safe flow is: lookup by external ID and canonical request; if absent, capture
a certified snapshot and prepare a candidate; admit before any participant prepare;
execute the Plan returned by admission. On transport uncertainty or process restart,
look up the same ID and exact request and execute the recovered Plan. Do not acquire
a new validation snapshot or replace its CAS/epoch/payload/intent identities.

The persisted record exists before any prepare can be dispatched through this
flow. A crash after only some participants prepared therefore does not lose the
remaining payload. A crash or lost response after Commit resumes prefix catch-up
and keeps the known Commit decision. A stale recovered CAS is a real conflict, not
a reason to replan silently; explicitly resolve/abort that identity first.
Malformed or incompatible stored Plan data fails closed and never implies Abort.

The original direct execute(Plan) API remains available for trusted in-process
callers and does not implicitly register an external request. Its unadmitted
memory-only plans retain the earlier recovery limitations. Old weaker query/write
interfaces are not upgraded to a network idempotency guarantee by this change.

## Format, limits and trust

Bound journals containing request admissions use codec v3. Existing v1 unbound
and v2 bound histories remain readable. The version tag must match replayed events;
duplicate admission events, rewritten records, rollback snapshots and inconsistent
execution identities are rejected. Plan codec v1 and journal v3 have exact golden
fixtures. The surrounding D1 envelope is unchanged because the journal is separately
versioned. Older binaries reject v3, and no downgrade is provided.

There are at most 256 admitted external requests. Canonical request bytes are capped
at 64 KiB, encoded Plan bytes at 256 KiB, and a fully JSON-encoded admission event at
2 MiB. All events plus reserved resolution bytes must fit the existing 16 MiB
journal budget. The real encoded size, including numeric byte-array expansion,
counts; the per-record cap does not replace the shared journal cap. These internal
bounds are not a substitute for the separate gateway request/response frame bounds.

There is no eviction or ID reuse. Capacity exhaustion rejects new identities without
removing older records, while existing valid retries and already-reserved transaction
resolution remain available. Request payloads remain in the retained coordinator
journal and participant advance observations. The [A1 read path](TERMINAL_PREFIX_EVIDENCE.md) separately
compares exact terminal-prefix projections without those archives. Storage and
write recovery are still bounded functional implementations, not a performance
claim. No credentials, signing keys or authentication protocol is added.
The trusted coordinator/application boundary remains explicit, and all remote graph
GlobalCommittedSnapshot admission is still Unsupported.

[Point recovery](DURABLE_REQUEST_INDEX.md) provides leader-barrier original-Plan
lookup and admission readback without transferring the full Journal. Current bound
coordinator disk v16 (and actual S1 disk-v15 migration inputs) derives that
evidence from raw Commands, retaining only the target request and transaction.
Begin and AdmitRequest determine absolute ordinals;
binding/grant events do not. Matching Prepared/Decide events preserve partial
prepares and terminal decisions even beyond an earlier unresolved hole. Found and
absent results both require full namespace/chain verification against a private
validated published cut; Unknown never means Abort.

The duplicate request index is retired only after strict legacy validation during
normal atomic migration. Actual v15 and older legacy formats keep their readers
until a successful batch writes v16; v15 already requires the index to be absent.
Residual request rows/manifests fail closed in both v15 and v16. Explicit validated
snapshot repair remains supported. See the [storage/read contract](AUTHORITATIVE_EVENT_LEDGER.md).
Managed RPC v5, original Plan bytes, first-plan-wins behavior and existing deadlines
are unchanged. The [S2 draft](COORDINATOR_ACTIVE_CHECKPOINT.md) consumes an active
checkpoint for the durable bound coordinator resident/ordinary apply candidate
and uses compact remote Command preflight. Cold replay, explicit control/Terminal,
end-to-end decide/status/Prepare, member state and complete snapshot materialization
still use full Journals. S2 acceptance is pending. The opt-in [byte profile](COHORT_CAPACITY_PROFILE.md)
replaces the legacy count quotas above, not the 16 MiB budget or local D1 limits.
Every archive lookup remains O(total history); no speed or lifetime extension is
claimed by this reader change.
