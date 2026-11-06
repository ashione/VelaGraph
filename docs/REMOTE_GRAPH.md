# Development-only remote graph service

This slice closes the **normal functional path** between a client process and
three independent Raft server processes: graph vertex/edge CRUD, exact prepared
mutation replay, point reads, and standard Gremlin queries. It is not a
production service. All graph, Raft, and control ports are plaintext and
unauthenticated. Use only on an isolated trusted development network. There is
no authorization, TLS, tenant separation, or secret management. The new service
does not relax the existing Linux UDS gateway's peer identity checks and does
not add graph writes to the legacy control protocol.

## Run

Build `cargo build --locked -p replicated-graph --bin graph-node`. Start three
processes with distinct node IDs, durable directories, and ports. For example,
start the following three commands in separate terminals:

```sh
graph-node --allow-insecure-development --id 1 --raft 127.0.0.1:7101 --control 127.0.0.1:7201 --graph 127.0.0.1:7301 --data-dir /tmp/graph-1 --shard 7 --epoch 3 --bootstrap 1=127.0.0.1:7101,2=127.0.0.1:7102,3=127.0.0.1:7103
graph-node --allow-insecure-development --id 2 --raft 127.0.0.1:7102 --control 127.0.0.1:7202 --graph 127.0.0.1:7302 --data-dir /tmp/graph-2 --shard 7 --epoch 3
graph-node --allow-insecure-development --id 3 --raft 127.0.0.1:7103 --control 127.0.0.1:7203 --graph 127.0.0.1:7303 --data-dir /tmp/graph-3 --shard 7 --epoch 3
```

Startup explicitly activates D1 with the supplied shard/epoch. Each process
waits up to 30 seconds for activation; only a leader can submit it. Existing
stored ownership must match exactly. Startup never changes an existing epoch.
Use `--bootstrap` only for first initialization, not a populated cluster restart.
Send SIGINT for graceful shutdown. The legacy `d2-ctl <control> status` exposes
leader ID; callers select its configured graph address themselves. Leader hints
are IDs, never trusted arbitrary network addresses.

Construct the adapter on a live Tokio runtime, then execute synchronous graph
work through `run_blocking`:

```rust,ignore
let node = RemoteStorageNode::new("127.0.0.1:7301".parse()?, 7,
                                  Duration::from_secs(2))?;
let graph = Arc::new(GraphRecordStore::new(node, 7, 0));
run_blocking(move || {
    graph.create_vertex(&Vertex::new(1))?;
    graph.execute_gremlin("g.V().count()")
}).await??;
```

The endpoint must be the current leader for new views and writes. There is no
automatic leader discovery, forwarding, retry, failover, or connection pool.
The Rust library is the client interface; there is no HTTP or general-purpose
remote Gremlin server. All six existing graph CRUD operations and the existing
supported Gremlin subset use the same graph formats and parser, unchanged.

## Snapshot consistency and limits

A snapshot RPC performs a leader/quorum read barrier, then captures shard,
epoch, sequence, and opaque entries under one state-machine lock. No partial
snapshot succeeds. The client executes Gremlin against the resulting immutable
`ReadView`. Each point/scan/page operation checks the selected server's locally
applied epoch through a separate metadata RPC; it never substitutes a newer
snapshot. This is not a new linearizable barrier: a replica may not yet know a
new ownership epoch. A retained view is historical, and an unavailable selected
server can prevent using it even though the bytes are copied locally.

Each snapshot is limited to 100,000 records and 2 MiB raw key/value bytes. Wire
JSON bodies are limited to 16 MiB, with a four-byte big-endian length prefix.
JSON byte-array expansion counts toward the wire cap. A legal local D1 batch
may not fit the remote frame and then fails explicitly. Existing D1 operation,
raw byte, dedup, and query execution limits remain unchanged. Snapshot transfer
is a full copy, not streaming or remote paging; this is intentionally a small
functional prototype, not a scalability claim. Limits bound individual work,
not total retained client snapshots or process RSS.

The server admits at most 16 concurrent connections; adapter clones share 32
outstanding operations. Direct async clients share a 16-RPC process-wide cap.
Deadlines are positive and at most 60 seconds. Frames and operations have absolute deadlines.
Service shutdown cancels and joins connection handlers; already submitted Raft
commands may still commit. Socket
loss or deadline expiry does not revoke an already submitted Raft command.
Direct synchronous calls from Tokio workers reject before acquiring graph
mutation locks; use `run_blocking` instead. Runtime queues and synchronous waits
are bounded. Administrative export/import/epoch changes are unsupported through
the adapter and there is no administrative opcode on the new graph port.

## Writes and uncertain outcomes

Preparation uses one snapshot and creates the existing immutable
`PreparedGraphMutation`. Commit transmits the original shard, epoch, ID,
sequence condition, and operations unchanged. The service applies a single
D1 Raft command and checks its nested application result. Conflict, fencing,
idempotency conflict, and resource limits remain typed errors.

There is **no automatic replay**, including after disconnect. An ambiguous
write is surfaced by the synchronous adapter as `Timeout` with its original
request ID. Keep the prepared handle before submitting, and explicitly retry
that same handle only when appropriate. Never call a convenience CRUD method
again to recover an unknown result: it creates a new identity. A definite
conflict requires deliberate revalidation/preparation; an epoch change fences
an old prepared mutation. Prepared handles are not serialized client journals,
so client-process loss recovery remains outside this slice.

## Evidence and deferred work

`remote_process` starts three independent `graph-node` OS processes and drives
normal vertex/edge CRUD, exact replay without sequence advance, retained-view
consistency, standard Gremlin, follower rejection, and wrong-shard rejection.
Transport and adapter unit tests cover bounded protocol behavior and runtime
boundaries. These tests require local loopback sockets, never external services.

Per the functional-first scope, this new graph protocol does **not** yet claim
validated kill/restart recovery, leader replacement, partitions, quorum loss,
ambiguous committed graph-write recovery, snapshot install during remote graph
traffic, or power-loss guarantees. Existing D2 and in-process graph tests retain
their own narrower evidence; they do not substitute for those remote end-to-end
scenarios. Static partition-to-group routing is now available through
[REMOTE_ROUTING.md](REMOTE_ROUTING.md). Cross-shard transactions, durable client
journals, production authentication, and query pushdown remain deferred.

## Identified groups

For new multi-group deployments use `--group`, identified RPCs, and the static
resolver described in [REMOTE_ROUTING.md](REMOTE_ROUTING.md). The CLI now binds
group/node identity in a durable marker and refuses unmarked nonempty legacy
directories. Do not silently upgrade/adopt old data directories. Legacy clients
remain supported only on group-zero services.
