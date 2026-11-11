# Query-local path materialization

Issue: [#170](https://github.com/ashione/VelaGraph/issues/170).

The shared routing-v3 executor, including managed fixed-version Traverse and
TraversePage and the single-partition V2Observed server, retains full provenance
only when the complete validated query contains `path()`, `simplePath()` or
`cyclicPath()`. A path filter still requires real paths when followed by count,
scalar projection or selected maps. The official parser, supported Gremlin
subset and entrypoint-specific consistency contracts are unchanged.

## Private representation and compatibility

For other queries, each traverser stores only its logical path length instead
of allocating/cloning a vector of path elements. Vertex/edge identity, scalar
source, the object mask, ordered bag multiplicity and every graph read remain.
The expansion, endpoint, repeat, emission and selection algorithms are shared;
there is no separate optimized executor, persistent cache or cross-query state.

The original cumulative accounting is deliberately unchanged. A logical path
starts at one, grows exactly as its real counterpart would, and becomes zero
when scalar projection clears provenance. Every retained row and emitted copy
is charged using that logical length, even though its actual path vector may
not exist. Work visits, the 10,000-row cap, 32-hop bound, mask admission,
integrity checks, error/check ordering, deadlines and session TTL remain intact.
This reduces actual allocation/copy work without admitting previously
out-of-budget queries. The private row remains within its existing 128-byte
logical base charge. No wire/disk format, result type or cursor changes.

Path consumers reject a missing private representation rather than treating it
as an empty path. Demand analysis exhaustively matches the current step enum;
a future step, especially a nested traversal, must define its path dependency.
Public paths still contain the same implicit edges, while simple/cyclic filters
still use the same actual-object mask. Empty streams and unsupported queries
follow their existing preflight and error behavior.

## Verification and measurement

`routing::experimental::traversal::edges::lazy_path_tests` compares both private
representations through the same bounded repeat implementation. It checks
qualified identities, cyclic multigraph bags, emission, path lengths, masks,
byte/work totals, exact admission cuts and each cancellation point. Existing
actual-path/filter, observed and managed process tests cover the public routes,
fixed-version stability and complete-result pages.

The opt-in release microbenchmark is:

```sh
cargo test --locked --release -p replicated-graph --lib \
  lazy_path_repeat_benchmark -- --ignored --nocapture --test-threads=1
```

It alternates retained/logical ordering over ten samples for 1/8/32-hop cycles
and an 8-hop two-edge branching cycle, with and without post-body emit. Both
modes use the same synthetic read provider and production repeat implementation.
Every run reports its logical-length checksum and charged bytes for equality
checking. Retained
final path-entry counts are also reported; these are not total allocations,
allocator bytes, RSS or source record decoding costs. The provider intentionally
isolates path-processing work and is not a storage, network or parser benchmark.
Timings are observations, never test thresholds or a correctness gate.

This slice does not remove materialized cohort snapshot acquisition, enable
data-local fixed-V fragments, lift managed capacity/lifetime limits or establish
a distributed throughput improvement. Existing observed CoreHR is a regression
gate, not a managed-capacity or end-to-end performance claim. Fresh exact-source
ordinary CI, CoreHR and release1025 evidence is required for acceptance.
