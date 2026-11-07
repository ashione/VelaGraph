# Durable external request binding

Status: prerequisite for the future managed graph network gateway. This slice adds
an internal admission/recovery API, not TCP endpoints or remote Global Gremlin.

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

A future gateway must parse a typed operation and produce a stable canonical byte
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
bounds are not a substitute for request/response frame bounds in the future gateway.

There is no eviction or ID reuse. Capacity exhaustion rejects new identities without
removing older records, while existing valid retries and already-reserved transaction
resolution remain available. Request payloads travel with the retained coordinator
journal and participant observations; this is bounded functional storage, not a
performance claim. No credentials, signing keys or authentication protocol is added.
The trusted coordinator/application boundary remains explicit, and all remote graph
GlobalCommittedSnapshot admission is still Unsupported.
