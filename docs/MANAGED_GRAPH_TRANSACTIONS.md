# Managed graph/v2 create transactions

Status: experimental trusted in-process end-to-end create/read slice over the
fixed empty-baseline cohort. The existing remote GlobalCommittedSnapshot Gremlin
interface remains Unsupported. This is not a production distributed graph API.

## Supported surface and mapping

replicated_graph::managed_graph connects the existing graph/v2 codecs and
validation to coordinator decisions, durable participant intents and certified
cohort snapshots. It supports creating vertices and creating directed edges,
including cross-shard edges, same-shard edges and self-loops. Reads expose vertices,
verified canonical edges and index-observed adjacency from one common version.
Properties updates and deletion remain outside this managed surface for now.

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
with one random transaction ID, random per-shard intent identities, authority,
cohort, validation version, ownership modes/epochs, original local CAS sequences and
exact ordered operations. Cloning preserves the entire plan; preparing again is a
new operation, not a retry. Intent IDs identify the plan and are not signatures.

An edge plan always includes both source canonical/outgoing operations and target
incoming operations. When both endpoints share a shard, the existing batch combiner
produces one request, one identity and one CAS, retaining both directions. Clients
cannot supply an incomplete list of participant batches through this API. Aggregate
plan operations and payload bytes are checked against D1 batch caps before sending.

Plan does not serialize and is not a durable retry file. Keep the exact value while
an outcome is uncertain. If the process loses it before every participant has
prepared, the current journal cannot reconstruct the unprepared payload. Operators
must inspect the durable coordinator/participant state and explicitly abort a
still-pending transaction or finish consuming a decided one using the lower-level
recovery API. Automatic crash resumption and durable plan storage are not claimed.

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
