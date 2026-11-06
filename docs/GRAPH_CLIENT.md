# One-shot graph client (development only)

`graph-client` is a thin command-line wrapper around the existing remote catalog,
routing and graph APIs. It adds no server protocol, storage format, Gremlin
syntax, cross-shard transaction or production security. Build both binaries:

```sh
cargo build --locked -p replicated-graph --bin graph-client --bin graph-node
```

For a complete executable walkthrough with three independent processes, see
[the loopback demo](GRAPH_DEMO.md). Python 3 is needed for that demo and its
integration test, not for the client itself.

## Configuration

All network commands require `--allow-insecure-development`. Connections are
plaintext and unauthenticated; use only an isolated trusted development network.
The configuration and input files must be regular UTF-8 JSON files of at most
1 MiB. Configuration, replica, placement and vertex objects reject unknown or
duplicate fields. Property maps reject duplicate keys. Version 1 is the only
accepted version. IDs are unsigned decimal integers, never floats or strings.

```json
{
  "version": 1,
  "timeout_ms": 10000,
  "metadata": {"group": 1, "shard": 101, "epoch": 1, "replicas": [1]},
  "metadata_node": 1,
  "metadata_nodes": [{"group": 1, "node": 1, "endpoint": "127.0.0.1:7103"}],
  "data_nodes": [
    {"group": 2, "node": 1, "endpoint": "127.0.0.1:7203"},
    {"group": 3, "node": 1, "endpoint": "127.0.0.1:7303"}
  ]
}
```

`timeout_ms` defaults to 10000 and must be 1–60000. Each router operation has its
own absolute deadline. A write performs bounded preparation then a separately
bounded commit; this is not a single end-to-end command timeout. Each replica
list must be sorted, unique and nonempty (at most 31). Metadata endpoints must
exactly match the metadata replica list. Duplicate endpoints, duplicate
`(group,node)` pairs, unspecified IP addresses, port zero and data endpoints in
the metadata group are rejected before I/O. At registration, every placement
replica must already be configured. Graph queries validate the selected catalog
placements against configured endpoints.

`metadata_node` selects the fixed metadata endpoint for catalog lookup and
registration. It must be the leader; these two commands do not discover or retry
another metadata endpoint. Graph query/write routing uses the existing bounded
fixed-candidate resolver. Node IDs may repeat in different groups, and local
vertex/edge IDs may repeat in different partitions.

`--help` emits JSON help without requiring a configuration or opening a network
connection. `config-check` validates configuration offline (including constructing
adapters, which do not contact servers). It still requires the explicit development
flag. Options are separate tokens; repeated or unused options fail closed.

```sh
target/debug/graph-client --help
target/debug/graph-client --config client.json --allow-insecure-development config-check
```

## Commands

The common prefix below is `graph-client --config client.json
--allow-insecure-development`. No command starts servers, changes memberships,
reuses a previously prepared operation or repairs partial writes.

- `catalog lookup --partition 10`: fresh metadata snapshot, returning immutable
  placement and its `metadata_sequence`. A missing partition is an error.
- `catalog register --partition 10 --file placement.json`: one preparation and
  one commit to the selected metadata endpoint. The file is
  `{"version":1,"group":2,"shard":102,"epoch":1,"replicas":[1]}`. Metadata's
  group/shard are reserved. Registration does not initialize or verify the data
  server membership; the operator must start matching groups first.
- `v1 query --partition 10 --query "g.V().count()"`: the existing partition
  Gremlin contract, against graph/v1 records only.
- `v1 global-query --query "g.V().count()"`: the existing bounded global scan
  contract, graph/v1 only. It does not support arbitrary distributed traversals.
- `experimental vertex create --partition 10 --file vertex.json`: isolated
  graph/v2 creation. The file is
  `{"version":1,"id":1,"labels":["person"],"properties":{"name":{"Text":"Alice"}}}`.
  Values use explicit `Null` (JSON string), `{"Integer":42}`, `{"Real":1.5}`,
  `{"Text":"text"}`, or `{"Boolean":true}` tags. Out-of-range integers,
  nonfinite reals and implicit scalar coercions are rejected.
- `experimental edge create --from 10:1 --to 20:1 --id 7 --label knows`:
  isolated graph/v2 edge creation, with an empty properties map. Both endpoints
  must exist. The edge's identity is source-qualified `10:7`. `PARTITION:ID` is
  a CLI argument encoding, not new Gremlin syntax.
- `experimental observed-query --start-partition 10 --query
  "g.V(1).outE('knows').inV().path()"`: existing v3 observed Gremlin execution.
  ID starts are qualified by `start-partition`; empty `g.V()`/`g.E()` starts scan
  the frozen catalog. No-ID starts do not narrow to the supplied partition.

The initial CLI does not expose v1 writes, property updates, deletion or retry.
These absent CLI commands fail explicitly even where a Rust API exists. V2
vertex deletion remains unsupported by the underlying API. Unsupported Gremlin
is rejected by the existing official-grammar parser and query validation.

## JSON output contract

Each invocation emits exactly one versioned JSON document on stdout and uses
stderr for diagnostics. Success has `{"version":1,"ok":true,...}`; errors have
`{"version":1,"ok":false,"error":{...}}` and exit nonzero. A failure is never
wrapped as successful or truncated output. Output-write failures also exit
nonzero; callers must not infer commit failure merely from a missing response.
JSON integers preserve Rust's full numeric ranges; consumers must use an
integer-capable JSON decoder for large `u64` identities.

Query rows have explicit `type`:

```json
{"type":"vertex","partition":10,"id":1}
{"type":"edge","partition":10,"id":7}
{"type":"path","elements":[{"type":"vertex","partition":10,"id":1},{"type":"edge","partition":10,"id":7},{"type":"vertex","partition":20,"id":1}]}
{"type":"count","value":1}
{"type":"value","source":{"type":"vertex","partition":20,"id":1},"value":{"Text":"Bob"}}
```

Experimental scalar rows carry qualified vertex/edge provenance (`source`). V1
scalar rows carry only `partition`, because its underlying result does not retain
an element source; the client never invents one. Counts do not invent an
originating partition. Every experimental query result, including empty streams,
counts and projections, contains `"completeness":"Unverified"` at the top level.
This is a required result property, not a warning that can disappear on stderr.

Queries also include `metadata_sequence` and `snapshots`, with each component
`{"group":2,"shard":102,"epoch":1,"sequence":4}`. These are independently
acquired shard snapshots, **not** a globally consistent timestamp or snapshot.
The CLI keeps all existing limits: for global/experimental sessions at most 32
partitions, 8 shards and aggregate snapshots of 16 MiB/200000 rows, plus existing
query and remote transport caps. Logical resource accounting is not a process
RSS limit. See [remote routing](REMOTE_ROUTING.md), [global scans](GLOBAL_SCAN.md)
and [experimental graph](EXPERIMENTAL_CROSS_EDGES.md).

## Write acknowledgement and unknown outcomes

Experimental creation returns `AllBatchesAcknowledged`, source/reverse request
IDs, qualified identity, `atomic:false` and `retry_supported:false`. The
acknowledgement is **not** an atomic transaction, complete adjacency proof or
proof of recovery. A cross-shard edge may be partially visible between batches.

When a stage fails, the error preserves its exact `stage`, `outcome`, `request_id`
and `source_acknowledged_this_attempt`, together with both original participant
IDs. In particular `Unknown` never means aborted, and an acknowledged source
followed by a failed reverse stage is not rolled back. Failures before any stage
has been dispatched do not invent a stage or acknowledgement. Catalog commit
errors conservatively report `Unknown` with the original request ID and expected
sequence, even if the underlying error is a definite rejection.

This one-shot process submits its original prepared value once. It performs no
implicit reprepare, unknown-outcome replay, repair, durable journal or cross-process
retry. IDs in the output are diagnostic identities, **not** enough information
to reconstruct the private prepared bytes/CAS. A later invocation creates a new
operation; it is never an exact retry of the original. Do not use only the ID to
forge a prepared operation or claim safe recovery. If killed or disconnected
before output is received, a write may have committed even without a saved ID.
A future durable recovery/session design is separate work.
