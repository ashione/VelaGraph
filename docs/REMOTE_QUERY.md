# Experimental single-partition server query pushdown

This additive trusted-network RPC executes standard Gremlin on the contacted
server instead of downloading the whole shard to the client. It is a functional
prototype, not a production Gremlin server, global query coordinator, or failover
protocol. The existing snapshot APIs, routing APIs, and `graph-client v1 query`
and `experimental observed-query` commands are unchanged.

## Explicit scope and semantics

`replicated_graph::remote::single_query_grpc::query(endpoint, identity, request, deadline)`
issues one typed request over gRPC to one explicitly selected replica. It
never follows leader hints, reconnects, retries, substitutes a snapshot, or falls
back to client-side execution. `identity` checks group/node configuration, not
authentication. All callers provide the configured group/node identity. The request names
one shard and one partition. It does not consult a placement catalog.

- `QueryMode::V1` reuses `GraphRecordStore::execute_gremlin`, the official-grammar
  parser, and the complete existing v1 interpreter and resource accounting.
- `QueryMode::V2Observed` reuses `observed_gremlin_v3`, including edge starts,
  edge/vertex projections, partition-qualified element `dedup()`, tombstone
  validation, qualified paths, and all its
  work/decode limits. Its private scope contains exactly the requested partition.
  If execution encounters a referenced partition outside that scope, including
  another partition on the same physical shard, it fails `UnsupportedScope`.
  It never drops such a reference to manufacture a successful partial traversal.
- Empty `V()`/`E()` means only this explicit partition, not all catalog partitions.
  V2 is always `Unverified`, including empty results, limits and counts: missing
  adjacency indexes remain undetectable. V1 reports `SinglePartition`, retaining
  its existing query contract, not certifying arbitrary opaque-byte writes.

A leader read barrier precedes one bounded atomic D1 snapshot copy. Its actual
`identity`, `shard`, `epoch`, and `sequence` are returned in `QueryStamp`.
All reads use only this immutable copy, even while later writes advance live
state. There are no per-row metadata calls or later leader fences. The stamp is
not a wall-clock timestamp, a placement-catalog sequence, or a cross-shard
consistency certificate. A leadership/epoch change after capture does not replace
the snapshot. There is no multi-shard strong-consistency claim.

Results use `QueryRow`: qualified vertex/edge elements and paths, counts, and
scalar values. V2 scalar rows always include the qualified element that supplied
the value. V1 scalar provenance remains `None`, matching the existing interpreter;
the output partition states the scope without inventing an element identity.
No on-disk format, graph parser, query syntax, or SQL behavior changes.

## Bounds and cancellation

All limits are hard failures, never truncated successes:

| Resource | Limit |
| --- | --- |
| Existing shared TCP request frame | 16 MiB encoded JSON |
| Query text | 16 KiB UTF-8, then the official parser's existing 64 raw structural-mark admission |
| Concurrent executing/capturing queries | 2 per process, across all graph services |
| Each server snapshot | 1,000,000 KV rows / 64 MiB key+value payload |
| Aggregate query snapshot payload | At most 128 MiB (separate from paged-download session admission) |
| Query execution budget | 1–5000 ms, also capped by the server connection deadline |
| V1 interpreter | Existing 3-hop, 10,000-row and 64 MiB logical accounted-memory limits |
| V2 interpreter | Existing 3-hop, 10,000-row/work and 64 MiB cumulative logical-work limits, plus primitive read/decode limits |
| Encoded `QueryOutput` | 2 MiB, including JSON escaping |
| Client accepted response frame | 2 MiB + 1024 bytes for the version/result envelope |

A snapshot is still copied and scanned on the server. Pushdown removes the
whole-shard **network transfer**, not that bounded server-side copy. Accounting
is logical payload/work, not total heap or process RSS. Parser allocations,
B-tree overhead, existing state-machine/dedup storage, bounded primitive scratch,
row conversion, and JSON buffers have their separately bounded costs; they are
not falsely included in a 128 MiB RSS claim. Paged snapshot sessions retain their
separate limits and can coexist with queries.

The server's effective query deadline covers leader barrier, capture, blocking
execution, result admission, final serialization, and response sending after
receipt of the request. The connection deadline additionally bounds request
reading. The client-supplied `deadline` covers connect, send, receive and output
validation; it never resets per read. Compute/read checks are cooperative, not
preemptive OS cancellation. Snapshot copying is bounded but cannot be interrupted
in the middle of its synchronous clone. A blocking worker keeps its admission
permit after a socket/waiter timeout until that worker actually finishes. This
prevents cancelled requests from admitting unbounded detached work.

At expiry the server may close the connection without sending a failure frame.
The client can therefore receive a transport `Timeout`/`Unavailable`, not
necessarily `QueryFailure::Timeout`. Neither case returns partial rows. Result
encoding is bounded before sending any success response.

## Typed errors and compatibility

The outer result is `Result<QueryResponse, RemoteError>` for existing transport,
identity, leader, and snapshot-admission failures. The inner `QueryResponse` is
`Result<QueryOutput, QueryFailure>`. Query failures distinguish syntax,
unsupported features, invalid arguments/traversers, resource limits, timeout,
out-of-scope references, corrupt/incomplete observations, and unavailable
execution. Failures intentionally contain no user query/property strings and no
partial result. Client validation rejects wrong scope/mode/identity, impossible
provenance, non-finite scalar values, invalid paths and oversized output.

Upgrade the selected graph server before using this additive operation. An older
server can reject the unknown request; the client must not silently fall back.
Existing operations remain wire-compatible and existing public entrypoints do
not opt into pushdown implicitly. All traffic remains plaintext and unauthenticated.
Use only the existing explicit insecure-development opt-in on trusted isolated
networks. TLS/authentication, fault recovery, partition placement checks, a durable
query session/cursor, remote query-result paging, and cross-partition execution
are deferred.

## CLI

With the existing validated graph-client configuration, explicitly select a data
replica, shard, and partition:

```sh
graph-client --config client.json --allow-insecure-development \
  v1 server-query --group 2 --node 1 --shard 7 --partition 10 \
  --query "g.V(1).out('knows').values('name')"

graph-client --config client.json --allow-insecure-development \
  experimental server-query --group 2 --node 1 --shard 7 --partition 10 \
  --query "g.E(9).values('weight')"
```

Only that configured `data_nodes` endpoint is contacted. No catalog read or
leader discovery occurs; the caller is responsible for selecting the current
placement. Config `timeout_ms` bounds the client operation and is capped at
5000 ms for server execution. Output is versioned JSON with `execution: "server"`,
`scope: "single_partition"` and the typed `output` object above. These are new
commands; the existing client-side commands and their JSON shapes are unchanged.

## Verification

`tests/remote_query.rs` starts three independent graph-node processes and seeds
more than 3 MiB of canonical v1/v2 data. A single-connection counting proxy
requires exactly the Query operation: selective results are under 4 KiB while
the older 2 MiB whole-snapshot RPC fails. V1 output is compared with the same
local interpreter; v2 checks qualified vertex/edge projection and path
provenance, `Unverified`, and the actual snapshot sequence. Error coverage
includes out-of-scope references, syntax/unsupported constructs, identities,
shard mismatch, request/response limits and execution work limits. Unit tests
cover output validation, encoded limits, immutable/expired reads, oversized
response prefixes, and admission retained after waiter cancellation. This is
functional evidence, not crash/failover, latency or throughput benchmarking.

The separately opt-in [distributed observed query slice](DISTRIBUTED_QUERY.md)
adds its own coordinator and fragment-session protocol. It does not widen this
single-partition RPC or change its validation and consistency contract.

### Shared v3 element deduplication

V2Observed server queries also accept the shared executor's no-argument vertex
and edge `dedup()`. The first qualified identity/path is retained in input order;
filter and limit positions, cumulative work/bytes, and the single-partition
snapshot remain unchanged. It cannot hide an out-of-scope reference or upstream
integrity failure. V1 still rejects dedup, and path dedup, arguments,
scopes and by modulators remain unsupported, including empty input. This does
not add cross-partition execution or enlarge the separate distributed coordinator.
See the [shared contract](MANAGED_FIXED_TRAVERSALS.md#qualified-element-deduplication).

### Shared v3 scalar deduplication

V2Observed additionally accepts `dedup()` after values/label/id. Equality is
strictly typed with equal finite real signed zeros and exact text; first source
and value are retained. Positional limits/count and the single-partition fence
remain. The shared implementation charges each input and unique key/text before
retention; upstream integrity and resource errors still fail the whole query.
This does not expand V1, the separate distributed coordinator or hierarchy jobs.
See the [scalar contract](MANAGED_FIXED_TRAVERSALS.md#scalar-deduplication).


### Shared bounded repeat

The routing-v3 executor and single-partition V2Observed server entrypoint support
one native out/in repeat with times(1..32) and optional unconditional post-body
emit. Combined ordinary/repeated depth is at most 32. Ordered qualified bags,
paths, filters/limits/dedup and fixed-read semantics retain their existing scope;
single-partition server queries still reject cross-partition observations.
All cumulative work/bytes/rows, adjacency and request limits stay unchanged.
V2Observed long paths have at most 66 elements, including an edge start and its
endpoint; clients and executor should be upgraded together. Local/v1 and legacy
observed v1/v2 remain unchanged. See the
[complete shared contract](MANAGED_FIXED_TRAVERSALS.md#bounded-native-repeat).


### Shared selected vertex property maps

The shared routing-v3 executor and single-partition V2Observed server queries
support terminal selected vertex `valueMap` with one to eight bounded keys.
Qualified source, singleton values, missing/Null distinctions and input bags
follow the [shared contract](MANAGED_FIXED_TRAVERSALS.md#selected-vertex-property-maps).
The same frozen provider supplies fields; maps use the shared 2 KiB flat tagged
property cap and unchanged cumulative work/response bounds. Single-partition
scope remains enforced, including edge endpoint and adjacency reads. V1/legacy
observed query surfaces and Unverified consistency are unchanged. Upgrade the
executor and client together; older clients reject these map replies.


### Strict scalar is filtering

V2Observed queries additionally share `is(literal)` after values/id/label, with
strict typed equality, original bag order/provenance and positional is/dedup/limit/range
composition followed optionally by count. Element inputs, predicates and invalid
post-scalar transitions reject before reads, even on empty input. Existing
single-partition fencing and all work/deadline/response bounds remain unchanged;
V1 and server-coordinator queries still reject this extension. See the
[shared contract](MANAGED_FIXED_TRAVERSALS.md#strict-scalar-filtering).


### Shared bounded positional range

Single-partition V2Observed queries share `range(low, high)` on vertex, edge and
scalar streams, with integer bounds `0 <= low <= high <= 1000`. Half-open
selection preserves ordered bags, qualified sources and paths in written step
order. Explicit range suppresses the default cap but never hides upstream
integrity or scope failures; all current inputs consume work. There is no source
pushdown or budget increase. V1 and separate server-coordinator/job entrypoints
remain unchanged. See the [shared contract](MANAGED_FIXED_TRAVERSALS.md#bounded-positional-ranges).


### Shared simple and cyclic path filters

Routing-v3 and single-partition V2Observed queries accept no-argument
`simplePath()`/`cyclicPath()` on element streams. They compare actual visited
objects with typed qualified identity, excluding implicit adjacency edges from
the existing returned provenance path. Bags, positional composition, bounded
repeat/emission, fixed snapshots and Unverified metadata are preserved. Invalid
scalar/modulated/repeat-body forms reject before reads; filters do not hide
upstream integrity/scope failures or enlarge budgets. V1, legacy observed v1/v2
and separate server-coordinator/job entrypoints remain unchanged. See the
[shared contract](MANAGED_FIXED_TRAVERSALS.md#bounded-simple-and-cyclic-path-filters).

The old `remote::query::query` JSON API remains only for explicit library legacy
fixtures. Product graph-node exposes the typed service on --graph, and
graph-client server-query uses it without protocol fallback.
