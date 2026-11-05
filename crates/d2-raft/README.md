# d2-raft

An openraft prototype for D2. It proves the consensus seam before a durable
RocksDB log adapter and the D1 state machine are wired in. The same node code
runs in-process for deterministic tests and across real OS processes over TCP.

## What is implemented

- Three-node cluster bootstrap: `initialize` + two `add_learner` calls +
  `change_membership`.
- Deterministic `LogCommand` (`Noop`, `Put`, `Delete`) and an applied key/value
  state machine.
- In-process `RaftNetworkFactory` dispatching RPCs directly to peer `Raft`
  handles for deterministic tests.
- A TCP `RaftNetwork`/`RaftNetworkFactory` with newline-delimited JSON RPC, plus
  `d2-node` (one node per process) and `d2-ctl` (driver).
- A three-process integration test: real TCP sockets, replication, leader kill,
  re-election, a write on the new leader, and old-leader restart/catch-up.
- Directed `DropPolicy` for in-process partition tests.
- Snapshot build and installation by a new learner.
- Leader shutdown, re-election, writes on the new leader, and old-leader
  catch-up.
- In-memory log store adapted from openraft's `memstore` example.

## What is intentionally not implemented

- Durable RocksDB log storage and a persisted state machine (the next D2 slice).
- D1 storage-node wiring and the graph `StorageNode` adapter.
- Cross-shard transactions, routing, metadata, and failover orchestration (D3/D4).
- A production transport: TCP has no TLS, authentication, backpressure or
  connection pooling, and the control port is a test/operations helper.
- Multi-host deployment manifests or service discovery.
- `openraft::testing::Suite::test_all` for a RocksDB adapter.

The in-memory log is a test fixture, not a production durability claim.

## Tests

```sh
cargo test -p d2-raft --test cluster
cargo test -p d2-raft --test multi_process
```

The in-process tests cover 100 replicated writes plus leader loss, a partitioned
leader that cannot commit until healed, and snapshot installation by a fresh
learner. The multi-process test spawns three `d2-node` children, initializes all
three voters, writes 50 keys, kills the leader, writes on the new leader, and
restarts the old leader with an empty log.

## Running across processes or hosts

Start one process per node; `--raft` is the address it binds and advertises, so
use a reachable host/IP here, not `0.0.0.0`, when peers are on other machines:

```sh
cargo run -p d2-raft --bin d2-node -- --id 1 --raft 10.0.0.1:7001 --control 127.0.0.1:8001
cargo run -p d2-raft --bin d2-node -- --id 2 --raft 10.0.0.2:7002 --control 127.0.0.1:8002
cargo run -p d2-raft --bin d2-node -- --id 3 --raft 10.0.0.3:7003 --control 127.0.0.1:8003
```

Then initialize all voters and inspect/write through any control port:

```sh
d2-ctl 127.0.0.1:8001 init 1=10.0.0.1:7001 2=10.0.0.2:7002 3=10.0.0.3:7003
d2-ctl 127.0.0.1:8001 status
d2-ctl 127.0.0.1:8001 write hello world
d2-ctl 127.0.0.1:8002 get hello
```

For a multi-process local run, use `127.0.0.1` and distinct ports. Writes must go
to the current leader; `status` reports it. `add-learner` and `membership` are
available for dynamic membership experiments.

This transport is a validation fixture. It has no authentication or TLS; do not
expose the ports on an untrusted network.
