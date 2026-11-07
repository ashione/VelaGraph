# Reproducible synthetic CoreHR simulation

This is a deterministic synthetic organization, not private HR data. Run
`scripts/corehr_simulation.sh <new-output-directory>` from the repository root.
It builds a debug-profile harness and release-profile node by default, preserves
fixture and database evidence, and
runs 2,000-person smoke and 200,000-person full workloads. The final JSONL record
must say `complete: true`; a partial output is not a passing run.

## Model and independent oracle

- 200,000 people, 5,000 departments, globally distinct IDs.
- One reporting root, eight-way people tree; one department root, five-way
  department tree. Edges point from employee to manager and child to parent.
- Every person has a primary department. Exactly 20% have a second department,
  and 5% have a third. A 40% preferential cohort selects from a predesignated hot 10% of
  departments; the remaining 60% selects uniformly from all departments. Thus
  the hot pool receives approximately 46% of primary assignments overall. Country, active status, synthetic names and
  multiple-membership properties exercise equality filters and projections.
- Three partitions distribute IDs by `1 + id % 3`. This intentionally makes
  cross-partition edges common, rather than quietly keeping relationships local.
- The oracle scans an independent in-memory relation list in insertion/edge-ID
  order. It preserves bag multiplicity for joins and compares qualified IDs,
  not merely result counts. The encoding path does not consult the oracle to
  decide query results. Unit tests cover tree acyclicity and memberships.

The trees are deliberately balanced and reporting links are generated independently
from department assignments. This makes cross-functional relationships frequent;
the distributions are reproducible synthetic stress shapes, not fitted to an
actual organization or a predictor of production latency.

The fixture generator uses existing public v2 preparation/encoding APIs against
frozen vertex-only read views. `Capture` is only an offline operation collector;
it is **not** a conforming production storage adapter or persistence test. Every
endpoint is present in its frozen view; unique fixture edge IDs prevent duplicate
creation. The runner imports the resulting bytes through real bounded Raft D1
writes and verifies every stored key/value against the fixture using paginated
snapshots. Import speed is **not ordinary graph API mutation throughput**.

## Tested revisions and explicit capacity selection

The recorded capacity baseline used the exact revision
`31a56611bdfe6d269d7dcc6d15cb5032b7dd2bad`, based on main `6ac282f`.
Each shard retains about 69 MB of accounted D1 retry state, slightly above the
unchanged 64 MiB default. The runner verifies a pristine default, explicitly
requests the candidate's 128 MiB capacity only for shards exceeding 64 MiB,
checks the typed response, and verifies exact retained accounting after import.
It does not silently change production defaults. Reproduction requires a
revision containing both the capacity administration from PR #56 and the
incoming-query support from PR #55. A fourth data shard could be a separate
capacity experiment; it is not substituted in these measurements.

## Execution and correctness boundaries

The runner starts four independent OS processes: one metadata group and three
data groups, each with one replica. The coordinator runs in the harness runtime
and communicates over loopback TCP. This tests sharding and process boundaries,
not high availability, follower catch-up, remote network latency or production
security. Services are plaintext development-only. Cross-shard v2 observations
remain `Unverified`, with independently sampled shard snapshots. No atomic global
snapshot or cross-shard transaction is claimed.

The small workload exercises the broader client-side observed Gremlin surface:
manager/direct reports, three-hop ancestry, two-hop descendants, memberships,
filtered members, colleagues through departments (bag semantics), count,
projection and explicit limits. Unsupported recursive `repeat`, sorting,
predicates, `range` pagination and deduplication must reject explicitly.
Department member queries use an explicit 1,000-row limit; no claim is made that
this is an unbounded membership listing. Paged storage verification does not
mean Gremlin supports continuation tokens or offset pagination.

This integrated harness additionally includes the incoming-query implementation
from the separately reviewed incoming-query candidate; its exact production-file hashes are recorded with the
experiment. See `COREHR_GLOBAL_RESULTS.md` for bounded global-scan validation,
`COREHR_DEDUP_RESULTS.md` for the later server-only unique-membership
validation, `COREHR_INCOMING_RESULTS.md` for incoming-query measurements, and
`COREHR_RESULTS.md` for the earlier outgoing-only baseline.

At full scale the broad client session is expected to reject its **200,000 KV /
16 MiB aggregate snapshot limit**. Storage snapshots separately cap each shard
at 1,000,000 KV / 64 MiB. The server distributed executor supports explicit starts
and up to three mixed **outgoing/incoming** hops with filters, projections,
count, limit and server-only vertex `dedup()`. Global vertex starts now have a separate bounded filter/limit/count scan,
with server order (partition ID, local ID), distinct from the legacy client
order (local ID, partition ID). Global adjacency and other global operators
remain explicitly unsupported; see `SERVER_GLOBAL_SCAN.md`. The full run checks
managers, direct reports, department descendants/members and mixed-direction
joins against the same oracle. Recursive repeat, offset pagination, ordering
and predicates must reject explicitly. The extended matrix contains 55 cases:
50 server-supported cases, four Unsupported rejections and one InvalidArgument
rejection for limit(0). Separate client/server oracle sequences cover global
counts, property/label filters, internal page and partition boundaries, and
filter/limit placement. Nine cases
exercise deduplication, unique counts, first retained paths, repeated starts,
empty streams and the non-commutative order of limit/dedup. These nine cases
still explicitly reject client-side execution; support is not inferred across
execution paths. Full-list and path cases use an explicit 1,000-row limit.

Full manager and department ancestor chains additionally run as explicitly
bounded application loops of single-hop queries (64-hop ceiling). They are not
native Gremlin recursion and do not share one atomic snapshot across requests.

## Measurements

JSONL separates fixture generation, bounded-import latency, byte-verification,
client-session acquisition, client queries, server queries (seven sequential
samples), application chains, process RSS/high-water RSS and database file
bytes. Seven samples are a smoke distribution, not a stable production p95 or
load/concurrency benchmark. Timings include debug harness code, the selected node profile and loopback
transport. Set `COREHR_NODE_PROFILE=debug` for an all-debug run, but do not
compare its timings directly with the recorded release-node baseline.
Report candidate revision, hardware and resource constraints alongside results.

The fixture/harness itself changes no production code, grammar, format or
resource cap. The separately reviewed incoming-query, server-only dedup
and bounded global-scan implementations are included in the corresponding
engine revisions. Do not infer universal query support from a passing
simulation. A failure is actionable evidence, not a reason to raise limits or
silently substitute a different query.
