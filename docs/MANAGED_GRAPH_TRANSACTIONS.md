# Managed graph/v2 transactions

Status: experimental trusted in-process end-to-end write/read slice over the
fixed empty-baseline cohort. A separate [managed TCP gateway](MANAGED_GRAPH_GATEWAY.md)
owns these handles and exposes bounded durable requests and fixed-version
sessions. The existing remote GlobalCommittedSnapshot Gremlin interface remains
Unsupported. This is not a production distributed graph API.

## Supported surface and mapping

replicated_graph::managed_graph connects the existing graph/v2 codecs and
validation to coordinator decisions, durable participant intents and certified
cohort snapshots. It supports creating vertices and creating directed edges,
including cross-shard edges, same-shard edges and self-loops. It also supports
vertex property patches, edge property patches and edge deletion. Reads expose vertices,
verified canonical edges and index-observed adjacency from one common version.
Vertex deletion, cascade operations and endpoint/label changes remain unsupported.

The initial mapping is explicit and deterministic: partition as u64 equals shard.
Every cohort shard must fit u32. No caller-supplied partition map can redirect an
endpoint to another member. Multiple partitions per shard, migration and catalog
integration need a future persisted routing manifest. The rule is generic and
contains no CoreHR-specific identifiers, schema or query shortcuts.

A GraphSnapshot can only be built from a complete certified SnapshotSession.
Every read uses its fixed version, retained local sequences, authority and cohort.
Edges validate the canonical record, outgoing index/source endpoint and incoming
index/target endpoint from that same version. Adjacency retains its explicit
index-observed status: arbitrary trusted low-level KV writes could corrupt graph
structure, and this module does not prove adjacency completeness against them.

## Immutable full-participant plan

prepare_vertex and prepare_edge perform no network writes. They validate existence
and absence conditions on the certified view, then create a private immutable Plan
with the original randomly generated source batch ID as its transaction ID, random
per-shard intent identities, authority,
cohort, validation version, ownership modes/epochs, original local CAS sequences and
exact ordered operations. Cloning preserves the entire plan; preparing again is a
new operation, not a retry. Intent IDs identify the plan and are not signatures.

An edge plan always includes both source canonical/outgoing operations and target
incoming operations. When both endpoints share a shard, the existing batch combiner
produces one request, one identity and one CAS, retaining both directions. Clients
cannot supply an incomplete list of participant batches through this API. Aggregate
plan operations and payload bytes are checked against D1 batch caps before sending.

A direct, unadmitted Plan is not a durable retry file. Keep the exact value while
an outcome is uncertain. If the process loses it before every participant has
prepared, the current journal cannot reconstruct the unprepared payload. Operators
must inspect the durable coordinator/participant state and explicitly abort a
still-pending transaction or finish consuming a decided one using the lower-level
recovery API. Automatic crash resumption is not claimed for that direct path. The optional
[durable request admission API](DURABLE_GRAPH_REQUESTS.md) persists a complete Plan
and Begin atomically before prepare, permitting exact request-ID recovery.

## Property read dependencies and deletion

An edge property update changes only the canonical source value, but its validity
also depends on the target endpoint and reverse index. The plan therefore includes
a target guard, prepared from the same certified version: validate both target
records and write the original empty reverse-index value back unchanged under its
original shard CAS. This is a real participant prepare/commit, not a fresh read in
execute. It retains the existing durable lock and sequence/future-capacity admission.

If any target write advanced its sequence after validation, the guard conflicts,
even when the source is unchanged. If the guard prepares first, other target writes
cannot pass that participant's pending intent. This conservative whole-shard guard
prevents the validation dependency from changing between prepare and decision. It
increments target sequence and consumes ordinary dedup/journal capacity despite
leaving the graph value unchanged. It is not a zero-cost read lock.

Vertex property patches preserve identity and labels and use the original vertex
bytes and shard CAS. Edge patches preserve identity, endpoints, label and indexes;
null/set/remove semantics reuse the existing generic graph/v2 patch validation.

Deleting an edge prepares the permanent source tombstone and outgoing-index delete
together with the target incoming-index delete. Both halves share one transaction.
The global ID is selected from the original source batch before sorting members,
so tombstone.operation_id equals the actual transaction/request ID even when the
source shard sorts after the target. Same-shard edges and self-loops combine into
one CAS while keeping all required operations. IDs remain permanently reserved;
current reads hide a tombstone only after verifying its reverse index is gone.
Old certified versions still retain the live edge and both historical indexes.

A new property/delete plan prepared against an already deleted edge is rejected.
A retained delete Plan remains idempotently retryable, including after reopen.
Competing property/delete plans use the existing conflict/explicit-abort contract;
no stale validation is refreshed silently and no vertex cascade is attempted.

## Decision and visibility are separate

execute validates the complete configured cohort and the live identities of the
actual write participants, then durably Begins the exact
participant set, prepares every participant, then requests Commit only when all
prepares are acknowledged. It never refreshes a CAS, epoch, payload or transaction
ID after a failure. Two concurrent executions of one Plan are idempotent. Different
plans for the same graph identity compete through original CAS conditions.

A timeout or unavailable coordinator is uncertain. A rejected Prepare leaves the
same transaction pending; it is not silently discarded or replaced. The caller
can retry that Plan or call abort(Plan). Explicit abort asks the coordinator for a
decision without waiting for participant or nonparticipant availability; if a concurrent execution already committed, the result is Committed,
never falsely Aborted. An unavailable nonparticipant does not block recording a
write decision from an already-prepared Plan, but can keep visible=false. Definitive conflicts require confirming/aborting the old
identity before preparing a replacement against a fresh snapshot.

After a terminal decision, the driver consumes decision prefixes through that
ordinal on every cohort member, including nonparticipants. It never skips an
unresolved earlier ordinal or aborts somebody else's transaction. Each member's
final terminal prefix is compared with the coordinator, not merely its numeric
watermark. Outcomes distinguish Committed/Aborted and visible=true/false. A known
Commit with incomplete application remains Committed with visible=false; retrying
the same Plan continues catch-up without re-preparing or creating another write.

Decision work is bounded by 30 seconds and visibility catch-up by another 30
seconds. On catch-up timeout, the known decision is returned with visible=false.
Cancellation or timeout is not evidence that an already-submitted Raft command did
not execute. inspect/retry the same Plan and identity.

## Read and old-interface boundaries

Only the certified GraphSnapshot path supplies this same-version cut. Existing
weaker/current reads retain their prior behavior and can observe partially applied
transactions; they must not be labelled globally atomic. Existing ordinary graph
and KV writers remain rejected by the managed participant state machine. Trusted
low-level coordinator/participant APIs remain available and do not automatically
validate graph shape, so do not mix arbitrary KV writes with graph invariants.

The snapshot's monotonic expiry is enforced on every new operation and after
validation. Frozen ReadViews share one per-shard copied map. This adapter currently
retains that map alongside the underlying session copy, so payload memory may be
up to twice the session cap, plus map/metadata overhead. It is bounded functional
materialization, not an indexed MVCC or performance claim. Synchronous graph work
should be scheduled on a blocking worker for large inputs. Adjacency output is
bounded to 1,000 entries and 1 MiB accounted payload; oversize returns an error.

The source journals and data history remain retained, with existing capacity
admission rather than GC. No indefinite write availability, durable distributed
pin, authenticated remote access, cluster failover or ownership migration is added.

## Retained control upgrade

The [S4 retained-control contract](RETAINED_GATEWAY_CONTROL.md) now governs actual
execute/status/retry/abort and Prepare: exact bounded target evidence, paged
history, strict old-cut apply and complete proof-target equality. Managed wire6
requires a same-version upgrade; older Raft binaries cannot replay the new
PrepareRetained command. The existing profiles and public query format remain.
