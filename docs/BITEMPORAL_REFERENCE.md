# Bitemporal reference slice (#196)

Status: executable **in-memory semantic reference**, not durable temporal storage,
not a distributed read API, and not a new query language. `temporal_reference`
is deliberately isolated from `graph/v1`, current edge tombstones and managed V.

## Decisions and comparison

[XTDB 1.x](https://v1-docs.xtdb.com/concepts/bitemporality/) separates when a fact
is learned from when it is effective, including late corrections and future
plans. [MariaDB bitemporal tables](https://mariadb.com/docs/server/reference/sql-structure/temporal-tables/bitemporal-tables)
likewise combine system history and application-time periods. We adopt those
independent coordinates, **not** their API, SQL surface, timestamp ordering or
claims of equivalent indexing. VelaGraph system coordinates must eventually be
certified commit versions, never caller wall clocks or minima of local sequences.

This slice freezes testable reference semantics:

* Valid instants are signed UTC epoch microseconds. Intervals are `[from,to)`;
  `None` means unbounded future, `i64::MIN` is the earliest representable instant.
  Empty/reversed intervals fail. No implicit `now` or timestamp parser exists.
* A revision is identified by its monotonically appended system version and
  position in its atomic batch. A logical vertex or edge identity selects a
  separate revision chain. It is **not** a reused qualified physical v1 ID.
* At `(V,T)`, the newest assertion with version <= V whose interval contains T
  wins. An absent assertion masks older values only inside its interval.
  Revisions outside a correction interval remain visible. Adjacent intervals
  are allowed and not coalesced, preserving provenance. Overlap for the same
  identity within one batch is rejected, not resolved by incidental ordering.
* An edge is visible only if both endpoints are visible at that exact `(V,T)`.
  The reference permits dangling assertions but never exposes them as query
  edges. This is a reference visibility rule, **not** a relaxation of current
  v1 write-time endpoint validation. Atomic batches can change related records.
* A directed candidate path is checked entirely in one immutable view. Missing
  elements return false; disconnected input and excessive length error. This
  helper is not a graph traversal API or a proposed Gremlin dialect.
* An exact known system version is required in this reference. The production
  integration must also support globally committed prefixes with no local changes. Unknown versions error; no fallback
  to latest. The model does not mint or certify these caller-supplied versions.
* History is append-only, without GC or eviction. Capacity exhaustion rejects
  the whole write, preserving older views. No retention promise extends beyond
  the lifetime of this volatile model. Restart intentionally loses it.

## Evidence and limits

The independent dense oracle copies discrete time cells once per model commit;
reference revision resolution instead walks an identity's chain. 48 successive
commits are checked against every prior system row and 16 valid instants (768
comparisons), including deletion and repeated retroactive correction. Other
ordinary unit tests cover boundary instants, future changes, parallel edges,
endpoint deletion/restoration, impossible mixed-time paths, atomic failure and
capacity limits. These tests run in normal `cargo test --locked`; for fast
isolated iteration they also compile with `rustc --edition 2021 --test
src/temporal_reference.rs`. This is no benchmark or scale proof.

Reference caps: 4,096 revisions, 4,096 UTF-8 bytes per value, 64 path edges.
They bound all loops and allocations and do not change any existing graph limit.
Batch overlap validation is quadratic but bounded; lookups scan at most one
bounded chain, not the entire graph. This is deliberately not the eventual
production temporal adjacency/index implementation. Values are opaque strings
for semantic tests, not a new persisted property encoding.

## Next executable integration, not completed here

1. Implement versioned temporal record/adjacency encoding with golden vectors,
   atomic storage writes, reopen tests and a migration policy. Reuse canonical
   Vertex/Edge payload codecs. Existing v1 data must not silently gain a made-up
   historical effective time; explicit import is required.
2. Bind a temporal read backend to the existing `QueryReadBackend` and official
   `GremlinQuery` execution. Resolve each identity lazily with a temporal
   adjacency index, preserving current deadline and memory accounting. Do not
   materialize a second complete graph per query to simulate integration.
3. Add certified cohort V plus T to managed session/page identity; test partial
   apply/Abort and concurrent writes before claiming distributed temporal reads.
4. Define durable retention and checkpoint format evidence before offering
   historical recovery. No purge/install, #124 workaround, GC, schema migration
   or remote upload is performed by this slice. #196 remains open.
