# Synchronous selected-property map validation

Validation date: 2026-11-07. Baseline: main
`09773b319af380ca7da1524395aa3734bb8f3975`, including the strict CI validator and
seven batch-relationship cases. This is a generic graph feature: the execution
code does not depend on CoreHR labels, property names, population, placement or
expected answers. CoreHR is an acceptance workload only.

## Real-process acceptance

Fresh same-host metadata and three data processes were loaded and every fixture
KV record was verified before querying. Both the 2,000-person/50-department smoke
fixture and 200,000-person/5,000-department full fixture passed the strict validator
for all 90 query cases. The full fixture contains 205,000 vertices, 454,998 edges
and 1,569,994 KV records. Supported cases run seven samples each; unsupported cases
must return their exact declared rejection category.

The five added property-map cases each passed all seven samples at full scale:

- Batch people: 4 maps, including the repeated start.
- Batch department membership: 8 maps, preserving relationship multiplicity.
- Distinct departments: 5 maps in first-occurrence order.
- Active department members: 25 selected maps after filtering and limit.
- Distinct colleagues: 25 selected maps after the two-hop join and deduplication.

The independent relationship-list oracle derives identities, ordering and
selected values without reading database output. Missing department-only/person-only
fields remain absent; no schema-specific execution shortcut is used.

## Generic regressions and checks

- 129 replicated-graph library tests passed, including an ordinary-session
  projection after a live deletion: the pinned values, expiry and cumulative
  budget remain unchanged. Output-vector reservation is charged before allocation
  or any RPC.
- Six fixture/oracle tests passed, including both real acceptance scales.
- Both distributed-query process tests passed. A separate non-HR graph exercises
  Unicode keys, Null, integer/real/boolean/text values, empty maps, duplicate starts,
  parallel edges, qualified identities, filtering, deduplication and result caps.
  Oversized properties and an incomplete reverse edge fail without partial maps.
- `cargo fmt --all --check`, all-target clippy with `-D warnings`, and diff checking
  passed.
- The unfiltered `cargo test --locked` was attempted. It reached the existing
  Unix gateway suite, where eight tests failed with `Operation not permitted` or
  child startup disconnection caused by that restriction. Later suites were not
  executed by that command. No test was weakened or skipped in CI; normal and
  full CoreHR CI on the exact PR head remain required before merge.

These runs establish bounded functional behavior, not multi-machine throughput,
HA, a global atomic snapshot or a latency SLA. Concurrent build activity makes
these correctness-run timings unsuitable for performance comparisons.

## Reproduction

Build `corehr_fixture`, `replicated-graph`'s `corehr_run` example, and `graph-node`
in release mode. Generate a fresh fixture with `corehr_fixture OUTPUT 200000 5000`,
then run `corehr_run FIXTURE NEW_DATABASE_DIRECTORY GRAPH_NODE_BINARY` and validate
its JSONL against the generated manifest using `scripts/corehr_ci.py`. The normal
CoreHR workflow generates and checks the same complete 90-case manifest. See
[the contract](QUERY_SPEC.md#distributed-synchronous-selected-property-maps) for
unsupported forms and unchanged synchronous limits.
