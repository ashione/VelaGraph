# Owned materialized temporal reads

`TemporalStore::materialize(V, limits)` returns a bounded, immutable
`TemporalReadModel` for repeated reads at one local system version. It compiles
revision selection once and retains all business-time intervals at that V.
Queries reuse the existing Gremlin parser/executor over covering in-memory
adjacency rows and validated payloads. No query-time database handle is retained.

This is an explicit optional API for the standalone temporal store. It is not
managed/cohort V, a distributed query path, a service cache, or a new disk format.
Existing format-2 stores and disk-query defaults remain unchanged.

## Use

```rust
use velagraph::{GremlinQuery, temporal_store::{
    TemporalStore, materialized::MaterializationLimits,
}};
# fn example(store: &TemporalStore) -> Result<(), Box<dyn std::error::Error>> {
let model = store.materialize(None, MaterializationLimits::default())?;
let query = GremlinQuery::parse("g.V(1).out('LINK').count()")?;
let at_first_time = model.view(1_000_000).execute_query(&query)?;
let at_later_time = model.view(2_000_000).execute_query(&query)?;
assert_eq!(model.version(), model.stats().version);
# let _ = (at_first_time, at_later_time);
# Ok(())
# }
```

`None` selects the captured local head once. `Some(V)` selects an available
local prefix. V is not a timestamp or a globally unique identity. T uses the
existing UTC-microsecond convention. Global `V()`/`E()` starts remain rejected.

For repeated reads, reuse `GremlinQuery` or `PreparedGremlinQuery` and call
`execute_query` or `execute_prepared`. `execute_gremlin` is a convenience that
parses each invocation; it does not add another hidden plan cache.

## Construction and source integrity

Head, canonical revisions and adjacency candidates are read from one RocksDB
snapshot. Construction validates **all** revision envelopes and positive
payloads at that source head, including revisions newer than selected V. It
checks the exact candidate inventory and earliest-version values against all
canonical positive edge assertions. Missing, extra, malformed, wrong-endpoint
or rewritten-first-version candidates are errors; construction does not silently
repair an invalid source index.

Selected-V assertions are swept by interval boundaries. Highest eligible V wins;
lowest ordinal breaks same-V ties as in canonical key order. Absence participates
in winner selection but emits no positive segment. Half-open endpoints and
unbounded ends remain distinct. Adjacent segments merge only for the same full
payload reference. Endpoint existence is resolved at query T, never inferred
merely from an allocated slot. Labels, properties, edge-ID order, duplicate starts,
parallel edges and self-loop semantics reuse the original executor.

A successful model is fully owned and immutable. Later source writes or source
closure cannot alter it. Eager construction can expose corruption outside a
particular lazy query's read set; this is stronger source validation, not an
identical error-observation contract. Invalid/oversized construction publishes
no partial model and performs no source writes.

## Limits and accounting

`MaterializationLimits` may tighten the following positive maxima:

| Construction limit | Maximum/default |
|---|---:|
| Combined revision and candidate entries | 1,000,000 |
| Combined source key/value bytes | 64 MiB |
| Cumulative conservative construction charges | 256 MiB |
| Retained slots, payloads and covering rows, per collection | 1,000,000 |
| Cooperative construction deadline | 10 seconds |

Charges include raw reads, conservative revision scratch, decoded payloads,
source-inventory entries and output structures. Charges for discarded scratch
or shadowed/future payloads are not refunded. This deliberately conservative
admission policy is not measured live heap or RSS. The retained-accounting
statistic is separate and includes container capacity and coarse map-entry fees.
Native RocksDB state and allocator overhead are outside these logical bounds.

Every materialized query starts fresh budgets. `MaterializedQueryLimits` can
only tighten the maximum/default **65,536 operations** and **64 MiB work bytes**.
Operations count point resolutions, endpoint visibility checks and every visited
candidate, including invisible and label-rejected rows. Work bytes include
metadata visits and the accounted size of every copied record, even when that
copy is later dropped. The existing 64 MiB live query budget, result/hop/parser
limits and cooperative 5-second deadline still apply. Exhaustion is an error,
never a truncated success. `_with_limits` methods support tighter caller limits.

Materialized work accounting differs from the disk path's 4,096 physical
revision/candidate limit. Neither successful-query admission nor failure timing
is claimed identical across those two APIs. Full record/output copies remain;
this API is not an end-to-end zero-copy execution path.

## Lifetime and refresh

The caller owns the model. Share it through `Arc` as needed; queries have
independent budgets. Dropping the last owner releases its retained structures.
To use a newer V, explicitly construct a new model and publish that completed
object. Retain the old model only under its old V. No automatic latest read,
incremental update, cache eviction, or service-level atomic publication is added.

A cache key must include the source graph/store identity or generation as well
as V; numeric V alone cannot identify data from different stores or tenants.
Owning a model is not permission to label it current after source changes.
Construction cost must be amortized over repeated reads. Frequent rebuilds,
large properties and many retained versions can outweigh query-time savings.

## Validation and reproducible example

Tests cover raw-query/canonical parity across V/T, properties and endpoints,
source integrity, absence/corrections, snapshot ownership, reopen, work/copy
budgets, schema/capacity rejection and independent models.

`cargo run --release --locked -p velagraph --example temporal_read_model`
uses generated data only. It separately reports construction and warmed query
latency for disk and materialized views, and checks every result. Its timings
are observations, not pass thresholds or a production SLA.
