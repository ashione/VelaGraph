# Incremental native hierarchy counts (experimental)

The dedicated coordinator accepts a separate, explicit job API for larger
hierarchy counts. It does not silently change synchronous Gremlin query limits.
`distributed::jobs::request` sends `Request::Start { start_partition, text }`,
`Advance { token, sequence }`, or `Cancel { token }` to the existing coordinator
endpoint. The query text is parsed by the official Gremlin AST. The first slice
accepts only `g.V(ids).repeat(single in/out).times(1..32)[.emit()][.dedup()].count()`;
the two existing post-emit modulator orders are accepted. Optional edge labels
are supported. No filters, other hops, limit, path, vertex pages, nested repeat,
unbounded recursion, or global start are admitted by this API.

## Progress is not a result

Start pins every configured shard and returns a token, `next_sequence: 0`, and
its fixed snapshot stamps. Advance processes up to eight sequential micro-pages, each containing at most
16 distinct frontier vertices. It yields between completed pages after a
50-millisecond cooperative work quantum; an individual page can take longer,
subject to the unchanged request deadline. The next request uses the returned sequence. The coordinator, not the
client, expands and validates all edges. A client merely polls native work; it
does not execute a per-hop graph query or download graph snapshots.

An unfinished response has `complete: false` and `count: null`. Only a completed
response carries an exact count for the admitted finite traversal. Resource,
corruption, epoch, session, and deadline failures never carry a partial count.
Completion is still `Unverified`: this is observed cross-shard adjacency, not a
proof of global graph completeness or a globally atomic snapshot.

Only the most recent Advance sequence is replayable: its cached response is
returned without repeating graph work, including the final response. Older or
future sequence values are rejected. Start is not automatically retryable.
While an Advance is in flight, another Advance or Cancel can return Unavailable.
Only the last successful sequence is replayable, not arbitrary retries. A
timed-out/disconnected client must not
assume the job survived. Before mutation that can await, the job is removed from
the registry; cancellation of that future cannot leave a partially consumed
frontier resumable. A normal successful step installs the new state and response
atomically before sending it. Cancel removes a known job and closes its views.
There is no recovery across coordinator restart or token renewal.

## Counting semantics and bounded memory

This is a specialized count plan, not a generic path executor. Within each depth,
identical partition-qualified vertices are combined with checked integer weights.
Each weight represents the number of bag traversers. Equal IDs in different
partitions remain distinct. Parallel edges, duplicate starts, convergent paths,
and cycles multiply or add those weights; nothing silently removes them.
Because the admitted body has one adjacency step and no path-dependent operation,
one fixed-snapshot adjacency read is equivalent for all traversers at that vertex.

Exact mode counts the frontier at the requested depth; dead ends vanish. Emit
counts depths 1 through n, excluding starts. Terminal dedup counts distinct
qualified vertices in the final frontier or the full emitted stream, respectively.
There is no per-level dedup of traversal weights and no implicit visited set.

Emit plus terminal dedup uses one identity table: each qualified vertex owns two
alternating depth-weight slots and an emitted flag. Current and next-depth weights
remain separate even on self-cycles. The emitted flag only controls the final
unique count; it never suppresses traversal at later depths. Reusing the same
identity entry avoids storing that vertex independently in frontier, next frontier,
and seen containers. A bounded ordered cursor scans at most 128 identity entries
per micro-page and expands at most 16 active vertices. Inactive seen-only entries
also incur scan work and may require progress requests that
expand no vertices. No full-table reset is hidden at a depth transition. Other
count modes keep their existing weighted-frontier representation.
Count is order-insensitive; this representation is not used to promise ordered
vertex or path pagination. Future pagination needs its own explicitly specified
ordering and continuation contract.

## Explicit new limits

The new API has its own budgets, not a claim that every old query cap is unchanged:

- Maximum two live jobs per coordinator process. At most 128 retained registry
  responses are admitted; completed response caching expires with its job.
- Fixed 120-second total job lifetime, including idle client time. Data sessions
  have independently fixed 120-second leases from acquisition. Neither is renewed;
  the earlier coordinator expiry governs the job. Acquisition and each Advance
  remain bounded by a five-second request deadline.
- At most 250,000 retained identity entries. Emit plus terminal dedup shares one
  entry across its current/next weights and emitted flag; other modes count their
  combined frontier/next-frontier keys. The limit is unchanged, not multiplied by
  the number of roles a shared entry serves. A conservative 128-byte logical
  allowance per allocated tree entry remains, plus bounded page buffers. This is
  not a process-RSS ceiling. Unemitted start identities are also retained and count
  toward the cap in compact mode. No full paths are stored.
- At most eight sequential micro-pages per request, each advancing at most 16
  vertices; 128 leaves per batched shard RPC,
  and 16,384 observed edges retained for one step. Sixteen vertices at the
  existing 1,000-neighbor cap fit this entry bound even before label filtering.
  Existing 16 KiB serialized-fragment and
  2 MiB wire-response ceilings still apply. A very broad single vertex or oversized
  batch fails, rather than silently truncating adjacency.
- Maximum one billion represented paths in a level or emitted bag count; checked
  arithmetic rejects overflow even when a final dedup might reduce the answer.
- Cumulative logical bytes at most 1 GiB and work at most 8,000,000 per job and
  per participating hierarchy session. This is processed logical traffic and
  allocation accounting, not a 1 GiB resident allocation. Costs are never reset
  between advances, and duplicate observations are still charged.

Ordinary synchronous repeat retains its 64 MiB/10,000-work/5-second contract.
Hierarchy sessions reject ordinary fragments; ordinary sessions reject hierarchy
batches. Batch bodies only contain admitted leaf primitives, never nested batches.
Canonical edges, endpoints, reverse indexes, and label-filtered observations are
validated as before. The same pinned views are retained across every step; epoch
changes, restart, or expiry fail rather than refresh a view. Local snapshots stay
on their shard; only bounded typed fragments cross the network.

Expired jobs are swept, and data sessions are independently swept. Explicit close
is best effort; unreachable data replicas retain views only until their original
lease. The service is plaintext and unauthenticated: tokens are temporary bearer
capabilities and this remains restricted to an explicitly trusted network.

## Command-line access

`graph-coordinator hierarchy-start` takes the existing explicit trusted-network
flag, endpoint, partition and Gremlin query. `hierarchy-advance` takes endpoint,
`--token` (the returned 16 bytes encoded as 32 hexadecimal characters), and
`--sequence` (the returned next sequence). `hierarchy-cancel` takes the token.
Each command sends one request and prints its native JSON output. There is no
automatic polling, resubmission, or client-side graph execution. The existing
`--timeout-ms` accepts 1..5000 for every command.

Micro-page coalescing reduces client polling round trips only. Partition RPCs
remain serial, and weighted frontier aggregation remains at the coordinator.
It does not implement shard-local subtree aggregation or parallel RPC execution.
