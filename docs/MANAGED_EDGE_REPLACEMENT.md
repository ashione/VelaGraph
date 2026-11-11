# Atomic managed edge replacement

`GraphSnapshot::prepare_edge_replace(old, replacement)` and gateway
`Mutation::ReplaceEdge { edge, replacement }` replace one directed relationship
in a single restricted managed transaction. This is generic graph functionality:
no label, property, endpoint or application schema is special-cased.

Deleting and creating in two separate requests can expose an intermediate
certified version with neither relationship. Replacement instead gives managed
fixed-V readers either the old relationship or the replacement at one transaction
ordinal. A retained old session continues to read the old edge, properties and
both indexes after the replacement commits. New sessions see the new edge only
when the whole cohort can certify that version. Partially applied physical
writes cannot open a newer certified V. Observed/current APIs retain their
weaker contracts and can see partial application.

## Identity and validation

The old edge must be live and complete at the preparation snapshot: its source
endpoint, canonical record, outgoing index, target endpoint and incoming index
are checked through the existing graph/v2 validators. Both replacement endpoints
must already exist at that same snapshot. The replacement supplies its full
new label and property map; properties are not implicitly copied from the old
edge. Stored values retain the existing strict typing and finite-real rules.

The replacement must have a different **qualified edge identity**:
`(source partition, edge ID)`. It may reuse the numeric ID in another source
partition. It may share either endpoint or both endpoints, reverse direction,
or be a self-loop. This operation creates a new edge rather than changing the
identity, label or endpoints of the original record. The old identity remains
permanently reserved by its existing tombstone and cannot be reused. Existing
live or tombstoned replacement identities are rejected.

Validation failure produces no candidate admission by that attempt. As with
other managed writes, this is not evidence that another in-flight invocation
with the same external ID did not admit. Use Status and the exact original
command to resolve uncertain outcomes. Invalid native preparations return the
existing typed graph/storage errors; gateway graph rejections return Rejected,
non-finite values return Protocol, and frame limits return Capacity.

## One immutable plan

Preparation assembles, in order:

1. Old source canonical tombstone and outgoing-index removal.
2. Old target incoming-index removal.
3. New source canonical record and outgoing-index creation.
4. New target incoming-index creation.

The first batch's random request identity becomes the transaction ID before any
shard grouping or sorting, so the old tombstone's operation ID equals the final
transaction ID even when the old source shard sorts last. The shared private
plan assembler combines contributions to the same shard using the existing
`PreparedBatch::combine`: epoch and snapshot sequence must agree, duplicate
operation keys are rejected, and operation order is preserved within a shard.
It generates one participant intent and one original sequence CAS per shard.
The final Plan has exactly six operations across one to four participants.

Every endpoint dependency is covered by an actual participant write. An
intervening write on any participating shard can conservatively conflict,
including unrelated changes and ABA. No CAS or graph state is refreshed after
admission. Earlier successful prepares remain pending until the exact request
is resumed or explicitly aborted; a failed Prepare is not an implicit rollback.
Commit/Abort and visible true/false retain their existing meanings.

The gateway recovers the complete admitted Plan by external ID and canonical
command **before** checking whether the old edge is still live or the new ID is
absent. Exact retry after commit, later graph changes or restart therefore keeps
the original decision and bytes. Changing the old identity or any replacement
field under the same external ID conflicts. A new ID is not a workaround for an
unknown earlier outcome. Durable request recovery, not keeping a newly prepared
Plan in memory, is the crash-resumable gateway path.

## Bounds, compatibility and limits

No storage, tombstone, Raft, durable Plan or canonical-command version changes.
The additive mutation variant uses the existing strict gateway protocol; older
gateways reject it. Upgrade gateway/client binaries before using it. This is not
a mixed-version availability guarantee. Existing mutation canonical bytes and
all capacity, request-frame, intent, retained-history, session and query limits
remain unchanged. At most four batches are assembled; no arbitrary mutation
list or transaction program is accepted.

This does not add vertex deletion/cascades, automatic retry/orphan resolution,
nonempty-baseline certification, dynamic routing, unlimited history, transport
security, authenticated completeness or general D4/D5 completion. Trusted
low-level writes can still violate graph invariants, and missing arbitrary
orphan indexes are not certified by this mutation. No purge/snapshot-install or
streamed-recovery change is included.

## Validation

Native tests enumerate all 15 shard-overlap partitions of the four endpoint
slots, including self-loops and reversed endpoints. They verify six operations,
one intent/CAS per unique shard, old source sorting last, same numeric IDs in
different source partitions, retained old versions, partial application,
per-participant stale CAS, explicit abort and permanent tombstone identity.
Rejection tests cover missing/deleted/existing identities, missing endpoints,
invalid values, bounded records and deliberately missing old indexes/endpoints.
Gateway tests cover strict wire/canonical
identity, request bounds and no admission on rejected candidates.

The independent-process acceptance test uses actual TCP gateway and managed
node endpoints for original-Plan recovery after partial preparation, partial
physical application with certified-V rejection, concurrent reads, preserved
parallel relationships and exact retry after later writes and node restart.
The replacement fixture runs after the original independent-process recovery
scenario closes its sessions and cohort, under that test's unchanged 240-second
harness budget. It does not add another independently scheduled consumer of the
process-wide client quota; its internal concurrent read/write checks and the
suite's default parallelism remain intact. These small functional fixtures are
not a managed full-scale capacity or multi-machine availability claim. See the
test sources:

- `crates/replicated-graph/src/managed_edge_replace_tests.rs`
- `crates/replicated-graph/src/managed_gateway_replace_tests.rs`
- `crates/replicated-graph/tests/support/managed_process_edge_replace.rs`
