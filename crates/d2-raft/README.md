# d2-raft

An in-process openraft prototype for D2. It proves the consensus seam before a
durable RocksDB log adapter and the D1 state machine are wired in.

## What is implemented

- Three-node cluster bootstrap: `initialize` + two `add_learner` calls +
  `change_membership`.
- Deterministic `LogCommand` (`Noop`, `Put`, `Delete`) and an applied key/value
  state machine.
- In-process `RaftNetworkFactory` dispatching RPCs directly to peer `Raft`
  handles. There are no ports, HTTP servers, or sleeps in the network layer.
- Directed `DropPolicy` for partition tests.
- Snapshot build and installation by a new learner.
- Leader shutdown, re-election, writes on the new leader, and old-leader
  catch-up.
- In-memory log store adapted from openraft's `memstore` example.

## What is intentionally not implemented

- Durable RocksDB log storage and a persisted state machine (the next D2 slice).
- D1 storage-node wiring and the graph `StorageNode` adapter.
- Cross-shard transactions, routing, metadata, and failover orchestration (D3/D4).
- gRPC or an out-of-process network transport.
- `openraft::testing::Suite::test_all` for a RocksDB adapter.

The in-memory log is a test fixture, not a production durability claim.

## Tests

```sh
cargo test -p d2-raft --test cluster
```

The three tests cover 100 replicated writes plus leader loss, a partitioned
leader that cannot commit until healed, and snapshot installation by a fresh
learner. Each uses bounded condition waits.
