# Gremlin query-owned logical byte budget

Each execution (local, fresh D1, or retained D1) starts a separate
`MAX_GREMLIN_ACCOUNTED_BYTES = 64 * 1024 * 1024` budget. Exhaustion or checked
arithmetic overflow returns `TraversalLimitExceeded`, never partial results.
Equality with the limit is permitted. This is an **accounted-byte admission
limit**, not a measurement or hard limit on Rust heap usage or process RSS.

## What is charged

The following version-1 accounting rules define the contract. Fees are logical
prices, not claims about the allocator's representation:

- 4096 bytes per execution for bounded control objects, graph lookup/range keys
  with validated labels, and cursor/range headers. Parsed query data is separate.
- 128 bytes per decoded vertex/edge, plus each vertex-label occurrence's
  32-byte slot and UTF-8 byte length. The edge label's embedded slot is included
  in the record base; its UTF-8 bytes are additional.
- 64 bytes per decoded property entry, plus property-name UTF-8 bytes and text
  value UTF-8 bytes. Scalars are included in the entry fee. Repeated singular
  protobuf fields keep their cumulative fees until the record is dropped, even
  when later fields replace them. This deliberately conservative rule preserves
  the existing last-field decode semantics. Duplicate property names still fail.
- 16 bytes per requested path entry; extension reserves the entire new path
  before allocating it. Original and copied paths are charged simultaneously.
- Each query collection's requested capacity times `size_of::<T>()`, including
  scan records, traverser vectors, final result rows, and their reservation
  holders. Capacity starts at four slots and doubles with checked arithmetic.
  `truncate` releases dropped payloads but not retained container slots. A
  consumed input iterator retains its full slot charge until it is dropped.
- 128 bytes per entry in the edge-id adjacency ordering map, separately from
  its row payload. Transferring a row to the next vector retains the payload
  charge and releases the map fee only after the destination slot is admitted.
- Returned raw key/value buffers while they overlap decoding. Local point reads
  also conservatively include their lookup key. D1 pages reserve requested row
  slots, key/value byte allowance, and a possible copied cursor key before
  dispatch. After return, raw bytes and cursor keys are charged by actual length;
  the requested page-slot fee stays until the whole page iterator is dropped.
  A cursor's resume-key charge persists across page requests.

Decoded records and their fees move into traversers. Filters, projection,
truncation, cancellation, storage/decode errors, and early return release charges
through owned RAII reservations. `values` and `label` move data instead of cloning
it. Projection releases the old record fee only after old properties are dropped.
The original input, current source record, adjacency map, accumulated output,
raw pages, edge scratch, and result construction can all overlap; all their
reservations contribute to the same execution budget. Successful return transfers
result ownership to the caller; retaining multiple completed results is not
covered by one execution's limit.

## Admission and boundaries

Every decoded string/label/property fee is reserved before that owned allocation.
Record payload parsing borrows the encoded payload rather than making a full
copy. Row/container/path/map growth is reserved before allocation. No operation
re-encodes a record merely to measure its size.

D1 pages request at most 256 rows and 16 MiB key/value bytes. Their requested byte
allowance can shrink under aggregate pressure; a page that cannot fit that
smaller allowance fails with `TraversalLimitExceeded`. The existing 16 MiB D1
record/page failure remains `StorageNode(ResourceExhausted)` when the independent
D1 cap is the reason. Read-view acquisition still has its separate existing
1,000,000-row / 64 MiB bound, and is not charged here.

The existing point-get and RocksDB iterator interfaces allocate their return
buffers inside the adapter before the query knows their length. Those buffers
are charged immediately after receipt, including local scan lookahead and
out-of-prefix buffers, and before decoding; allocation inside the adapter cannot
be preempted by this API. The same limitation applies to a nonconforming custom
D1 adapter; oversized returned pages are rejected, but the adapter already
allocated them. This is not a pre-allocation guarantee for storage dispatch.

Excluded from the accounting contract: input text, parsed query/parser storage;
shared RocksDB/cache/native memory; retained or newly acquired D1 view data;
adapter-private dispatch/allocation before return; allocator metadata, rounding,
fragmentation and reallocation scratch; unused capacity/internal node packing in
decoded label/property maps beyond their explicit per-entry prices; and error
formatting/diagnostics. The fixed control fee is not an RSS bound. Independent
concurrent executions each have their own budget, not a shared global quota.

## Compatibility and limits

This is an intentional new resource restriction: a query that previously
succeeded may now fail, including low-row-count queries with large properties
and many parallel edges. The initial 64 MiB default is a conservative admission
policy, not a workload capacity guarantee. Normal results, ordering, multiplicity,
snapshot and epoch semantics, graph encoding, SQL, and CRUD APIs are unchanged.
A later `limit` or `count` does not excuse earlier oversized intermediates.
Leading limits still bound initial scans, and D1 need not decode later records.
The existing row/hop/text limits and cooperative deadline remain independent.

The reproducible `gremlin_memory_bench` example exercises ordinary small scans,
projections, three-hop expansion, many short properties, and repeated large
properties. It reports local warm latency distributions and rejection counts;
these are smoke measurements, not service throughput, allocation counts, or RSS
proofs. Tests use injected budgets for exact boundaries and inspect the ledger's
peak and final zero balance. See the PR's recorded benchmark environment/results.

### Recorded smoke run (2026-11-06)

One before/after run, same cloud executor, release build, Rust 1.99.1 and the
same RocksDB native archive. Ordinary scenarios use 50–100 repetitions; the
large-property case uses 20. Timings include execution but exclude parsing and
setup. This shared machine was not otherwise isolated; differences are not
statistically established performance claims.

| Workload | Baseline p50/p95 µs | Accounted p50/p95 µs | Outcome |
|---|---:|---:|---|
| 10,000 small vertices, count | 5428 / 8643 | 5442 / 6195 | all succeed |
| 100 projected results | 58 / 164 | 62 / 77 | all succeed |
| 1000 projected results | 614 / 945 | 620 / 889 | all succeed |
| Three hops, 512 paths | 1269 / 1690 | 1257 / 1565 | all succeed |
| One record, 1000 short properties | 194 / 369 | 184 / 309 | all succeed |
| 100 parallel edges, 1 MiB target text | 58038 / 66883 | 26076 / 33763 | baseline succeeds; new budget rejects 20/20 |

These samples support retaining 64 MiB as the initial default for ordinary
small-record workloads while rejecting the targeted duplicate-property case.
No allocation-count or RSS measurement was made.

A separate deterministic ledger smoke run recorded peak admitted bytes:
5,438,752 for the 10k scan; 51,848 / 434,512 for 100 / 1000 results;
243,177 for three-hop paths; 84,045 for 1000 short properties. The parallel
1 MiB workload reached 66,088,521 admitted bytes before rejecting its next
reservation. All ledgers returned to zero. Run with:
`cargo test --lib accounted_high_water_smoke -- --ignored --nocapture`.

## Global filter/projection execution

The global scan executor uses this same ledger across all partitions. It retains
qualified candidate IDs, then reloads records only from pinned views for element
operators. Reload raw buffers and decoded payloads overlap accumulated projected
results and remain charged. `values`/`label` move payloads through the ordinary
operator implementation. Final output construction keeps all payload reservation
holders, their vector slots, input slots and destination slots charged until the
result is complete. Filters release discarded records; retained element IDs
release record fees before the next operator reload. A late filter or limit
does not exempt the global initial candidate cap. Snapshot-vector storage remains
outside the logical query ledger under the separate GLOBAL_SCAN.md admission
contract; this is not a global atomic snapshot or process RSS bound.
