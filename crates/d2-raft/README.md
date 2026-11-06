# d2-raft

An openraft prototype for D2. It proves the consensus seam with a durable
RocksDB log and an applied opaque key/value state machine shaped for D1 batches.
The TCP node also runs across real OS processes. A versioned D1 application command now carries shard/epoch fencing, conditional
sequence and retry identity through Raft, snapshots and reopen. The D1 graph
`StorageNode` adapter is still a separate slice; the in-process `Cluster::start` path remains an
in-memory test fixture.

See [`D2_D1_CONTRACT.md`](../../docs/D2_D1_CONTRACT.md) for activation, versioned
persistence, upgrade limits, and the deliberately bounded no-eviction retry budget.

## What is implemented

- Versioned `D1V1` commands with typed application results; atomic data,
  sequence, dedup and applied-marker persistence; full retry identity in snapshots.
  Activate only on empty legacy state, and never mix old/new replicas after activation.
- Three-node cluster bootstrap: initialize all voters, or initialize one node,
  add two learners, and change membership.
- Deterministic `LogCommand` (`Noop`, `TxnBatch(Vec<WriteOp>)`) and an applied
  opaque key/value state machine. `WriteOp::Put`/`Delete` carry arbitrary bytes;
  the consensus layer never interprets D1 keys.
- In-process `RaftNetworkFactory` dispatching RPCs directly to peer `Raft`
  handles for deterministic tests.
- A TCP `RaftNetwork`/`RaftNetworkFactory` with newline-delimited JSON RPC, plus
  `d2-node` (one node per process) and `d2-ctl` (driver).
- A three-process integration test: real TCP sockets, node-startup bootstrap,
  replication, leader kill, re-election, a write on the new leader, and
  old-leader restart/catch-up against the same durable directories.
- Directed `DropPolicy` for in-process partition tests.
- Snapshot build and installation by a new learner.
- Leader shutdown, re-election, writes on the new leader, and old-leader
  catch-up.
- `DurableLogStore`: a separate `<data-dir>/raft.db` with `raft/log/<index>`,
  `raft/vote`, `raft/committed` and `raft/last_purged`; append, vote, truncate
  and purge all use synchronous writes.
- A persisted `<data-dir>/state.db` state machine: opaque key/value batches,
  applied index, membership and the latest logical snapshot survive reopen.
  `get_raw`/`scan_raw` expose the D1 byte view; `get`/`scan` remain the UTF-8 D2
  smoke-test view.
- `Node::start_durable` / `Cluster::start_durable` and the restart test in
  `tests/durable.rs`.
- The old in-memory `LogStore` remains for the deterministic in-process tests.
- `openraft::testing::Suite::test_all` passes against the durable RocksDB log
  store and state machine (`tests/conformance.rs`).
- An opaque non-UTF-8 batch test covers atomic put/delete ordering, snapshot
  transfer and reopen (`tests/d1_batch.rs`).

## What is intentionally not implemented

- The D1 graph `StorageNode` adapter and graph-schema records. The durable state
  machine now accepts opaque D1 byte batches, but no graph kernel is wired to it
  yet; `tests/cluster.rs` still uses the UTF-8 key/value smoke fixture.
- Cross-shard transactions, routing, metadata, and failover orchestration (D3/D4).
- A production transport: TCP has no TLS, authentication, backpressure or
  connection pooling, and the control port is a test/operations helper.
- Multi-host deployment manifests or service discovery.
- OS-crash/WAL fault injection. Reopen tests prove process-restart recovery, not
  power-loss behavior under every RocksDB option.
- Migration from the pre-D1 `Put`/`Delete` log commands and snapshot encoding.
  This prototype change uses opaque batches; start experiments with fresh data
  directories.

The in-memory `LogStore` and `Cluster::start` are test fixtures. The TCP node and
`start_durable` use the persisted stores; `raft.db` and `state.db` are separate
RocksDB directories.

## Tests

```sh
cargo test -p d2-raft --test cluster
cargo test -p d2-raft --test multi_process
cargo test -p d2-raft --test durable
cargo test -p d2-raft --test conformance
cargo test -p d2-raft --test d1_batch
cargo test -p d2-raft --test d1_contract
cargo test -p d2-raft --test d1_cluster
cargo test -p d2-raft --test d1_tcp
```

The in-process tests cover 100 replicated writes plus leader loss, a partitioned
leader that cannot commit until healed, snapshot installation by a fresh learner,
and replication of an opaque non-UTF-8 key/value batch. The multi-process test
spawns three `d2-node` children, initializes all
three voters through node 1's `--bootstrap` startup path, writes 50 keys, kills
the leader, writes on the new leader, and restarts the old leader against its
original data directory. The durable test shuts down all three in-process nodes
and reopens the same `raft.db`/`state.db` directories before checking the applied
value and accepting another write. The conformance test runs openraft's storage
suite against fresh durable stores; the D1 batch test applies non-UTF-8 put/delete
operations and verifies snapshot transfer and reopen.

## Running across processes or hosts

Start one process per node; `--raft` is the address it binds and advertises, so
use a reachable host/IP here, not `0.0.0.0`, when peers are on other machines.
Start the non-bootstrap peers first, then the bootstrap node:

```sh
cargo run -p d2-raft --bin d2-node -- --id 2 --raft 10.0.0.2:7002 --control 127.0.0.1:8002 --data-dir ./data/n2
cargo run -p d2-raft --bin d2-node -- --id 3 --raft 10.0.0.3:7003 --control 127.0.0.1:8003 --data-dir ./data/n3
cargo run -p d2-raft --bin d2-node -- --id 1 --raft 10.0.0.1:7001 --control 127.0.0.1:8001 --data-dir ./data/n1 \
  --bootstrap 1=10.0.0.1:7001,2=10.0.0.2:7002,3=10.0.0.3:7003
```

Each node needs its own `--data-dir`; it holds `raft.db` and `state.db`. Use
stable local filesystem storage for these directories and give every node a
different one. The durable test uses temporary directories.

`--bootstrap` calls the same `initialize_cluster`/`TcpNode::bootstrap` API a real
startup coordinator should use; it waits for the initial quorum. Inspect and
write through any control port:

```sh
d2-ctl 127.0.0.1:8001 status
d2-ctl 127.0.0.1:8001 write hello world
d2-ctl 127.0.0.1:8002 get hello
```

For a multi-process local run, use `127.0.0.1` and distinct ports. Writes must go
to the current leader; `status` reports it. `add-learner` and `membership` are
available for dynamic membership experiments, and `initialize_cluster` is
exported for a service's own startup path.

This transport is a validation fixture. It has no authentication or TLS; do not
expose the ports on an untrusted network.
