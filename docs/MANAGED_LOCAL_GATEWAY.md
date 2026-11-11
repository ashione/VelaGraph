# Query-bound data-local fixed-V gateway experiment

This additive M1 path retains graph data on explicitly selected participant
processes. It does not increase capture/history budgets or enable the older
observed GlobalCommittedSnapshot consistency option. It is not indexed MVCC,
full managed CoreHR capacity, failover, TLS/authentication or a durable read lease.

## Trusted configuration and protocol

`Gateway::bind_remote_local_queries` requires the original `RemoteConfig` plus
one `LocalGraphRoute` per member in canonical cohort order. Each route supplies
an explicit graph endpoint, graph group/node identity and the **complete** managed
identity matching that member's configured Client. Role, target shard/epoch/mode,
authority, entire cohort including capacity profile and partition=shard all match.
Endpoints are distinct and cannot be supplied by queries or inferred from ports.
The service is plaintext and must be isolated to trusted callers and peers.
Tokens and terminal-prefix evidence are not authentication credentials or
cryptographic proof against a malicious peer.

The additive operations are:

- `OpenLocalQuery { version, ttl_millis, query, page_size }`: execute and validate
  the entire immutable result before publishing its `LocalSession` and first page.
- `FetchLocalQuery { session, query, cursor, page_size }`: replay any known page,
  including the first page (`cursor: None`), without opening or contacting nodes.
- `CloseLocalQuery`: close the retained result. Local sessions are strongly typed
  separately from old materialized sessions and observed node descriptors.

The official Gremlin AST admits explicit nonempty `g.V(ids)`, up to three `out`
or `in` hops, and optional terminal `path` or `count`. Other syntax fails before
any node session allocation. The existing shared executor preserves argument
order, duplicate roots, edge order, qualified paths, canonical/reverse/endpoint
validation and default first-100 non-count semantics. This is a narrower entrypoint
than old managed Traverse, not a silent substitute. Rows retain `Unverified`
adjacency completeness, separately from their certified V.

## Admission, execution and results

The fixed cohort supports at most 64 members, independently of the one-to-eight
execution lanes. See [membership bounds and validation](FIXED_COHORT_MEMBERSHIP.md). Admission selects exactly one V from the
original complete cohort and retains original endpoints, authority, cohort,
complete terminal proof and deadlines. This admitted cut is not itself a read
certificate. Every graph member captures once with its same-lock certified
terminal read after a leader barrier. The gateway receives each actual retained
proof and compares it in full with the original coordinator proof, together with
all descriptor identities. No graph KV is downloaded through this query path.

Before opening any node session, the gateway checks every complete Open request
and worst-case successful reply against the graph RPC's 16 MiB frame boundary.
Fixed-open replies have their own receive branch; old frame limits are unchanged.
Some valid maximal ByteBound histories exceed this transport cap and fail Capacity.

Required fragments use a bounded, ordered shard-lane scheduler over the original
immutable descriptors. Directional expansion stages at most the configured number
of traversers (three by default):
first acquire their adjacency lists, then validate every canonical/reverse/endpoint
dependency, and finally merge by input ordinal and edge order. Across independent
shards at most that many RPCs are in flight; one session always has at most one.
Each wave receives disjoint conservative allowances from the same cumulative
query budget. Large/skewed leaves or insufficient headroom use the original
serial remaining allowance, never an equal split or a retry with more credits.
Out, In, reverse checks and incoming resolutions can overlap when their
capture-dimension reservations fit the unchanged budget. This
is a general leaf/stage execution mechanism, without graph-, label-, ID- or
terminal-query special cases. The observed executor remains unchanged.

Request sequences start at zero and advance exactly once per attempt. Duplicate,
skipped, failed or unknown calls cannot replenish node/global ledgers or cause a
reopen, replica switch or V refresh. Required descriptor access or validation
failure fails the entire query. A failed lane cancels the remaining local RPC
futures; no stage publishes partial rows. All futures are owned by the query,
not detached tasks. When multiple lanes fail, the first observed error need not
be the first error of a serial execution. Node workers already running retain their original view,
ledger, permits and deadline after local cancellation. Cleanup still closes
known descriptors only within the original deadline; expiry is the backstop.
After the last required access, a node failure
may be unobservable while the gateway seals results. Ready and subsequent Fetch
rely on the gateway's retained result, not live nodes.

An atomic edge replacement is a later certified write. Existing descriptors and
pages retain the original qualified edge records at V; queries opened at the
new version see the replacement, including a change of source owner with the
same numerical edge ID. The legacy managed Traverse executor has its own lazy
path representation. This entrypoint continues to use the distributed executor.

The entire result is at most 10,000 rows and 1 MiB; page size is 1..=1000. Every
complete page envelope, including Binding, original query, V/session, cursor and
maximal request-id encoding, must fit 1 MiB before Ready. Cursors are independent
random 256-bit tokens scoped to the exact result. Close/expiry are checked at
lookup, page creation, after encoding and before send; bytes whose transmission
has already begun cannot be revoked. Every replay preserves the
original query, size, V and expiry. A lost Open reply is not a Fetch retry of a
known session; no automatic Open retry or durable resume is provided.

## Resource and deadline boundaries

Aggregate captured rows/bytes/D1 operations remain 10,000 / 2 MiB / 100,000.
Each member receives the remaining capture budget, which also bounds its replay
intermediate state. Final retained capture counters are not peak measurements.
The node's observed and fixed views share the existing eight-slot/128 MiB pool.
The gateway's old/new retained sessions share its existing four reservations;
workers continue owning reservations after close/expiry/cancellation. Fixed opens
also share the existing two-slot observed coordinator execution gate, including
certification, fragments and sealing. Old gateway operation concurrency is unchanged.

Proofs, raw retained history, serde clones, fragment scratch and full certification
are additional bounded costs, not included in D1 replay counts or the gateway's
payload reservation. Active terminal certification may reconstruct all retained
raw history and a temporary Plan-bearing Journal even for a small old V. Existing
profile history byte/event limits remain in force. Synchronous work is not
preempted by async timeout; checks after completion prevent an expired success.
No CPU/lock-time/RSS reduction is claimed. Measurements not present in an evidence
report are unknown, not zero.

TTL is at most 60 seconds from initial admission, with the original acquisition
window and query deadline throughout. No spool, global scan, history GC, purge,
install, durable format change or dependency on #124 is introduced. This parity
slice cannot demonstrate crossing the old 2 MiB capture boundary or completion of
#110/#111; write/bootstrap/history capacity remains an independent blocker.

## Parallel execution validation and scope

The scheduler uses each participant's already-validated total capture bytes as
an upper bound for an individual raw row, including uninterpreted records.
It also uses that same immutable capture's total row count to bound adjacency:
after source validation, both Out and In can scan at most
min(1000, capture_rows - 1) index candidates (saturating subtraction).
Out reserves all canonical checks for those candidates even if a canonical
record is missing, preserving corruption errors instead of exhausting a
speculatively reduced allowance.
No label, ID, graph-specific degree estimate, or query pattern selects a plan.
This removes the unconditional 5004-work reservation that previously prevented
any two Out leaves sharing a wave under the unchanged 10000-work budget. The
bound covers successful MeteredNode gets/scans, server result encoding and client
request/transport charges. It is conservative and checked for overflow. It is
not a fresh memory pool or a statement about allocator RSS. A large capture,
large individual property value, or little remaining budget can still force
serial execution: there is no guarantee of parallelism for every graph. In
particular two 1000-edge worst-case Out leaves cannot fit the existing budget.
Allowances are never divided speculatively and a budget failure is never retried.
No capture protocol, wire version, session ledger, grammar, deadline, TTL or
resource cap changes.
Request counter widths can differ with reserved allowances, so exact encoded-byte
charges need not be bit-for-bit identical to serial execution.

Tests include simultaneous independent lanes, per-session sequencing, bounded
width, forced reversed completion, duplicate ordinals, disjoint allowances,
low-budget fallback, descriptor/sequence/leaf/cost corruption, failure-driven
sibling cancellation, caller cancellation, expired empty stages, and actual
MeteredNode charges versus the bound. A real Raft participant/TCP gateway test
requires both second-hop Out acquisition and reverse validation on two shards
to overlap, and rejects corruption even in a label-filtered edge. Actual metering
covers source-only empty adjacency and a dangling-index corruption error.
Existing input-model bag/path, concurrent-write,
old-V, node-loss, replacement and resource-owner tests remain applicable.

The ignored `fixed_local_parallel_query_measurement` harness performs real
managed writes and fixed-V Open/Close queries on same-host TCP with actual Raft
participants. It reports two warmups plus nine measured samples for outgoing
and incoming three-hop counts, checking V5 and a duplicate-preserving traversal count of 40.
The identical harness can run on serial main for a direct comparison. No injected
network delays or speed assertions are part of the benchmark. This is a small,
unoptimized-build measurement, not full managed CoreHR, multi-host, sustained
throughput, capture scalability, or production performance evidence.

The design follows [A1's single-timestamp data-local graph execution](https://arxiv.org/html/2004.05712v1),
[TiDB's bounded ordered coprocessor workers](https://github.com/pingcap/tidb/blob/master/pkg/store/copr/coprocessor.go),
and [CockroachDB's tracked query shutdown](https://github.com/cockroachdb/cockroach/blob/master/docs/tech-notes/query_shutdown.md).
Those sources motivate mechanisms, not transferred performance claims. Future
batched/paged operators and indexed versioned views can reuse the same execution
context and ordering contract; this slice does not implement them or remove
whole-partition capture/history limits.

The capture-dimension refinement also follows the separation of concurrency
slots and accounted resources in [Trino task properties](https://trino.io/docs/current/admin/properties-task.html)
and [CockroachDB admission control](https://github.com/cockroachdb/cockroach/blob/master/docs/tech-notes/admission_control.md).
Their memory-accounting and non-preemptible-resource constraints motivate safe
admission rather than optimistic fractional grants. No external system's
performance results are transferred to this implementation.

## Configurable bounded shard concurrency

`Gateway::bind_remote_local_queries_with_parallelism(config, routes, bind,
deadline, parallelism)` adds an immutable deployment setting in `1..=8`.
The existing constructor still selects three; one selects serial fragment
execution. Invalid values return `Protocol` before route validation, binding,
or node contact. Query text and the wire protocol cannot change this setting.
A width larger than the cohort is allowed; actual independent lanes are limited
by the available original sessions and conservative byte/work reservations.

The setting also bounds each expansion window: at most eight adjacency lists,
each at most 1000 entries, and at most 8000 dependency checks. This increases the
maximum staged scratch compared with the default three-list window, not the
64 MiB cumulative logical query-byte budget or 10000-work budget. It is not an
RSS cap. More speculative work may run before an error, and error order and
encoded-byte accounting need not equal serial execution. All dependency checks
still precede filtering/materialization, with ordered bags and qualified paths.

The coordinator's two-query admission, graph client's sixteen-call admission,
node's eight fragment workers, shared retained-session pools, capture budgets,
original deadlines and per-session sequencing remain unchanged. Other queries
or observed work may consume those shared slots and cause an explicit admission
failure. A wider setting does not guarantee speedup or parallelism, does not
queue/retry failed RPCs, and does not add fairness or cluster-wide admission.
Deployments should tune downward when query concurrency is high.

This follows the separation between configurable operator concurrency and
resource limits in [Trino task properties](https://trino.io/docs/current/admin/properties-task.html#task-concurrency),
and the overload-protection goals of [CockroachDB admission control](https://github.com/cockroachdb/cockroach/blob/master/docs/tech-notes/admission_control.md).
These are design references, not transferred performance claims. The fixed-member
fixed cohort, immutable full-partition captures, restricted Gremlin subset,
whole-result sealing and retained-history costs remain. Removing those boundaries
needs separately reviewed streaming operators, indexed versioned views and
cohort/flow-control contracts; changing a concurrency setting is not that work.
