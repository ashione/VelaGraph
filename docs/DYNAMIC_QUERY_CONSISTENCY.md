# Dynamic graph consistency: current boundary and implementation plan

Status: design proposal plus executable characterization, **not implemented
cluster snapshot isolation**. This document does not change existing API or
storage semantics. It applies to arbitrary graph data, not a CoreHR shortcut.

## Current behavior

A coordinator opens one immutable session per unique shard, sequentially, before
executing fragments. A leader read barrier precedes each local D1 snapshot. The
snapshot is a bounded copy taken under the local state read lock; copying can
briefly delay local writes, but traversal does not keep that lock. Queries,
hierarchy jobs and property projection retain those original views. Prepared
pages are materialized results, not live property rereads.

Each successful result still carries `Unverified` and a vector of per-shard
stamps. The vector is not one cluster timestamp or consistent cut. A v2 logical
edge can involve multiple non-atomic writes. Validation rejects observed broken
relationships, but cannot establish missing-index completeness or reject every
combination of different sampling times. Read stability is not atomic visibility.

`tests/distributed_query/dynamic_consistency.rs` exercises real data-node
processes through test-only TCP gates, without sleep-based interleaving:

1. Two independent vertices begin with revisions `(0,0)`. Freeze A; block B's
   OpenQuery before forwarding it. Acknowledge A=1, then B=1; release B. A global
   scan/self-edge traversal/property projection returns `(0,1)`, although the
   only real states were `(0,0)`, `(1,0)`, `(1,1)`. This is a **known-boundary
   characterization**, not a passing guarantee of global isolation. A future
   consistent protocol must replace this expectation, not preserve the anomaly.
2. Freeze a query, gate its first fragment, then acknowledge a property write.
   The query retains its old value; a new query sees the change.
3. Start a projected hierarchy job, update during preparation and again between
   result pages. Both pages and replay retain the initial value and stamp vector;
   a fresh query sees the latest value.

Run with `cargo test --locked -p replicated-graph --test distributed_query
 dynamic_consistency_boundaries`. The normal `cargo test --locked` CI also includes
this non-ignored test. These tiny fixtures verify semantics, not throughput,
high availability, arbitrary update combinations, or full-scale dynamic CoreHR.

## Intended contract

At query admission, select one complete committed graph version S and pin its
catalog/routing epoch. All traversal, filters, counts, property projections and
pages refer to S. New queries can choose newer versions; an in-flight query does
not mix updates. If a required version or participant is unavailable, wait within
a bounded deadline or fail explicitly. Never silently refresh, omit a shard, or
return a partial successful count. This is query snapshot isolation, not a claim
that all arbitrary read/write application transactions are serializable.

“Query start” means the coordinator's logical admission point, not the client's
wall clock. A later freshness contract can require S to include a caller's last
acknowledged commit token. Report version lag rather than promising zero lag or
zero latency interference. Unbounded long readers are not supported.

## Runnable implementation slices

### 1. Inventory and fence every mutation route

Before claiming a global contract, map and test direct graph APIs, local/D1
batches, identified remote Write requests, prepared/retried routed writes, v2
canonical/reverse/property/tombstone changes, administrative import/restore and
catalog registration/epoch changes. Raw compatibility/legacy log commands and
bulk seed paths must not bypass the new visibility boundary on an opted-in
cluster. Standalone v1/K3 semantics remain unchanged.

Introduce an explicit versioned capability/mode, not a silent reinterpretation
of existing bytes or commands. Within that mode every mutation either joins the
protocol, is rejected before mutation, or is a fenced offline administrative
operation. Durable admission checks belong at the state-machine boundary as well
as clients, so another client or a retry cannot evade them. Build tests for each
entry point, including direct raw writes, stale epochs and retries after mode
activation. Audit is a prerequisite, not an assertion this inventory is complete.

### 2. Durable transaction identity and atomic visibility

Implement a generic mutation envelope with a stable transaction ID, participant
set, idempotent prepare, conflict/epoch validation, and a replicated commit/abort
decision. Keep intents and old visible versions until resolved. Canonical edge,
reverse indexes, endpoint changes and related properties in one declared logical
mutation become visible together; clients must explicitly group a larger business
operation if it needs that atomicity.

A minimal first runnable slice uses a single replicated decision/ordering group,
static participant membership, and a narrow transaction API. Test success,
retries, conflict rejection and interruption after every participant transition.
A timestamp alone is insufficient: an earlier undecided transaction cannot be
skipped while announcing a later safe committed prefix. Prepared-but-undecided
work remains invisible, retains its old version, and pins safe progress as
required. Resolve it from the durable decision, never from a timeout guess.
Uncertain old v2 operations must be reconciled while writes are fenced before
activation; stopping new requests does not repair existing half-written edges.

### 3. Multi-version reads at one committed cut

Retain graph values, deletions and indexes with commit visibility/version
metadata. Select S from a complete committed prefix and ensure each relevant
shard can serve S. Per-shard applied watermarks must cover committed decisions
and unresolved holes, not just the maximum timestamp seen. Fix catalog mapping
at the same boundary. Add an explicit open-at-version operation and return its
version proof; independent OpenQuery calls or equal sequence numbers cannot
substitute for this protocol.

Test the fractured schedule above expecting an actual consistent state; test
cross-shard edge append/delete/endpoint changes under blocked prepare/apply,
commit-response loss, retry and a lagging shard. Separate result safety from
liveness: a bounded failure is acceptable; a falsely successful mixed result is
not. Keep legacy Unverified queries distinguishable until the complete protocol
is active. Mixed old/new binary operation needs an explicit compatibility and
activation design, not an assumption of rolling-upgrade support.

### 4. Continuous-load acceptance and retention

Pin S through long traversal and projection, or materialize complete validated
results before releasing it. Pages must never reread current properties. Track
the oldest reader, unresolved prepares and their old versions, lagging participant
apply watermarks, and checkpoint/WAL recovery dependencies when calculating the
safe reclamation low-watermark. Include bounded leases and cancellation;
expired versions cause an explicit failure. Bound retained bytes, admission and
concurrent pins. Prevent writer starvation and long-reader storage exhaustion.

Use generic seeded mutation histories and an independent versioned oracle,
covering vertices, properties, parallel/cyclic edges, deletes, filters, dedup,
counts and pagination across multiple shards. Extend the full CoreHR fixture
with deterministic mutations as an additional workload, not a special executor.
Measure query p50/p95/p99, write throughput/latency, snapshot acquisition time,
freshness lag, retained version bytes and failure counts under concurrent load.
Compare with the static baseline at fixed hardware/seed/load; do not claim no
performance impact merely because correctness passes.

## Transitional option and recovery boundary

A short write-admission fence, drain/reconcile of all in-flight logical writes,
then acquisition of all immutable sessions can produce a consistent cut without
MVCC. It requires control of **every** write route and unresolved intent. It can
serve as an explicit prototype/offline mechanism; it is not the final continuous
write solution and must expose its write pause. A barrier on reads alone cannot
make non-atomic writes atomic.

RPO is separate from query consistency. Recovery must retain transaction
decisions, graph versions, catalog epoch, sequence/dedup metadata and relevant
logs at a compatible cut. A correct query snapshot does not make a remote
checkpoint current or preserve acknowledged commits after loss of all replicas.
Checkpoint/WAL retention, commit durability and restore validation require their
own documented durability guarantee. No remote backup or online upgrade support
is claimed by this proposal.
