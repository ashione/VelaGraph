# Managed fixed-version traversals and result pages

Status: bounded, experimental addition to the trusted managed gateway. This uses
one certified empty-baseline fixed cohort and one retained version V, not the
unrestricted catalog or the old remote GlobalCommittedSnapshot API. The latter
continues to return Unsupported. No current/observed snapshot is substituted for
an unavailable V.

## Shared execution, separate consistency envelope

`Operation::Traverse { session, query }` uses the existing routing-v3 Gremlin
executor through a private read-provider trait. The official TinkerPop parser,
whole-AST preflight, traversal ordering, stream validation and work limits are
shared with `ExperimentalReadSession::observed_gremlin_v3`. That old API retains
its per-shard stamps and Unverified observation contract unchanged. Shared
execution returns rows only; it cannot create or relabel a consistency proof.

The managed provider contains only the retained GraphSnapshot and query deadline.
All scans, vertex/edge lookups, adjacency and reverse/endpoint validation use the
same snapshot. Session construction still requires exact coordinator authority,
complete fixed cohort, empty baseline, common V and participant journal proofs.
Remote acquisition uses the existing pinned node identities and internal RPC.
The gateway materializes cohort data before traversal and executes the shared
synchronous traversal locally over those immutable maps. It does not yet adapt
the data-local distributed fragment engine or incremental hierarchy jobs to V.

The external query is ordinary Gremlin text plus partition routing context:

- V/E starts accept explicit IDs (argument order and duplicate IDs preserved), or
  empty V()/E() scans over every fixed-cohort partition in `(ID, partition)` order.
  Explicit IDs belong to the supplied partition; canonical edge IDs belong to
  their source partition. The routing partition must be a cohort member even
  for scans. Missing IDs produce no row.
- out/in/both and outE/inE/bothE accept zero or one label. Adjacency order is
  `(edge ID, source partition)`. Parallel edges and repeated input traversers
  preserve bag multiplicity. Both directions emit a self-loop once per input.
- inV/outV/bothV produce edge endpoints; bothV emits source then target, including
  twice for a self-loop. Paths retain qualified vertices and canonical edges.
- has/hasLabel, values/label/id, limit, terminal path/count use the existing v3
  semantics. Missing properties produce no value; stored Null remains a value.
  Vertex label selects the first stored label, or empty text. Limits execute in
  place. Count runs before the implicit default result cap.
- With no explicit limit, non-count results have the existing default cap of
  100. Explicit limits are 1..=1000. Internal caps never produce partial success.
- Repeat, valueMap, mutations, and unsupported compositions reject
  before data reads, including empty streams. The original smaller `Read` API,
  including its id projection, is unchanged. This is not a second query dialect.

Replies echo exact session and query. Session carries V. QueryData remains
Unverified for adjacency completeness against arbitrary trusted low-level KV
writers; this is distinct from fixed-version consistency. Managed typed writes
are covered only within their documented fixed-cohort contract. Nonempty imported
baselines, dynamic routing, uncovered writers and general transaction isolation
are not certified by this feature.

## Complete-result pagination

`TraversePage { session, query, cursor, page_size }` accepts a positive page size
at most 1000. Omit cursor for the first page. Every continuation repeats the exact
query, partition, session and page size. Reply echoes those fields and includes
`QueryPage { data, next, offset, total }`.

The whole bounded query must succeed before any page is returned. The gateway
then checks every complete response page against the encoded frame limit,
including query/binding echoes and tokens. A large later row therefore fails
before the first successful page, not after early pages appear successful.
No continuation reads the current graph or opens another snapshot.

One immutable result is retained per session. A try-lock reserves its slot before
construction, so competing first-page requests cannot allocate two results or
wait indefinitely. Different query/partition/page-size requests do not replace
an occupied result; they fail Identity. Open another session for another paged
query. Unpaged Traverse remains stateless and can execute different queries on
the same V without replacing the retained result. Concurrent traversal/result
construction on the same session fails Capacity instead of queuing.

Each continuation has an independent random 256-bit token stored in the result's
bounded cursor registry. The registry is bound to session incarnation/token/V,
exact query and page size. Unknown or transplanted tokens fail. Repeating an
issued cursor, or repeating the first-page request, returns the same page and
next token; consuming a page does not invalidate earlier tokens. Tokens are not
credentials or cryptographic proof of an unauthenticated peer's answers.

Close, expiry and gateway restart invalidate the session and its result tokens.
Continuation never extends TTL. Pages already received cannot be revoked. A
lost first-page reply can be replayed on the still-live session; no automatic
retry is performed. An expired/closed session can still fail after earlier pages
were received, even though the query had already completed.

## Bounds and reservations

Existing parser limits remain 16 KiB and 64 structural marks. Shared traversal
allows at most three edge steps, 10,000 intermediate rows, 10,000 work units and
64 MiB cumulative logical work. Adjacency is all-or-error at the existing
1,000-entry/1 MiB per-direction primitive bounds. Snapshot acquisition retains
its existing 10,000 rows, 2 MiB copied payload and 100,000 replay-operation caps.
Queries, page construction, encoding and transmission share a five-second
request deadline, shortened by connection deadline and the original session
TTL (at most 60 seconds).

A complete retained QueryData has at most 10,000 rows and 1 MiB serialized bytes.
The 1 MiB full response bound also applies to every unpaged result and page. A
page cannot increase the full-result limit or request unbounded streaming.
The cursor registries contain at most 9,999 entries each. One 4 MiB logical result
reservation covers result/cursor retention, exact query and bounded page/encoding
scratch, in addition to the existing 4 MiB reservation for two snapshot payload
copies. Four materializations therefore reserve at most **32 MiB** in total,
including active captures and sessions retained by workers after expiry/close.
Reservations are acquired before capture/construction. Write-validation captures
conservatively reserve the same slot although they do not retain page results.

This is not an RSS cap: allocator/map metadata, parser and traversal scratch,
bounded journals and transient unpaged outputs under the eight-worker admission
limit are additional. Source history remains retained without GC; the durable
journal remains limited to 256 external requests/16 MiB. These small bounds do
not support the 200,000-person CoreHR fixture on this managed path.

## Compatibility and verification boundary

The new request/reply variants are additive to protocol v1. Existing Read and
write messages, canonical command bytes, external identities, original durable
Plans and unknown-outcome handling are unchanged. Older peers reject new
variants rather than translating them. Upgrade both endpoints to use them; Rust
consumers exhaustively matching public enums must handle the new variants. No
persisted graph format, ownership rule or wire security setting changes.

Tests reuse three independent durable single-node Raft processes and parent
RPC-only gateway/client access. Small fixtures cover same IDs in two partitions,
parallel edges, cycles, personnel reporting and multiple department membership,
retained V during writes/deletion, paged paths/replays, partial apply rejection,
identity tampering and restart invalidation. These are small functional
counterexamples, not a full CoreHR scale run, multi-host performance measurement,
HA proof or production-security validation. Shared legacy traversal tests remain
part of the regression gate; full workspace CI is required before merge.

## Qualified ID scalars

The shared routing-v3 executor additionally accepts standard `id()` on vertex or
edge streams, including scans, adjacency and endpoint compositions. It preserves
ordered bags and qualified source provenance while projecting the local ID to
Integer(i64). IDs greater than i64::MAX fail explicitly; a later limit/count does
not suppress that failure. Only limit/count may follow scalar projection, checked
before even empty input. Existing work/bytes, result and session limits remain.
The same fixed V supplies elements and IDs; scalar result pages retain the existing
immutable result and cursor rules. Clients validate each integer against its exact
qualified source ID. No new protocol, storage or consistency contract is added.

## Qualified element deduplication

Standard no-argument `dedup()` now applies to vertex or edge streams in the
shared routing-v3 executor and managed Traverse/TraversePage. The single-partition
V2Observed server-query entrypoint also uses that executor and admits the same
element dedup step within its unchanged partition scope; V1 server queries still
reject it. Identity is
`(partition, local ID)`; an edge belongs to its source partition. Same-number
vertices/edges in different partitions remain distinct, and parallel edges with
different IDs remain distinct. The first occurrence retains its complete path
and input position. Filters, limits and dedup execute where written; expansion
after dedup produces ordinary bags again. A self-loop's two bothV endpoints
collapse only when an explicit downstream dedup is present.

Scalar/path dedup, scopes, arguments and by modulators remain unsupported,
including empty streams. The old Read operation and legacy observed v1/v2
entrypoints are unchanged. Parser admission, hop count, defaults and all existing
row/work/byte/deadline limits remain. Each input consumes one work unit, and each
new identity consumes 128 accounted bytes before insertion, plus the existing
row/path charge for retaining its first row. At most 10,000 input rows and keys
can be inspected/retained by a step, further limited by remaining cumulative
budgets. No upstream integrity or resource failure is erased by dedup.

Managed execution and all pages use the same retained V and complete-result
cursor rules. Observed results remain Unverified. This adds no wire or disk
variant, refresh, retry or capacity increase. Older executors explicitly reject
these query compositions; upgrade the executor to use them.
