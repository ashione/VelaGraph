# D2 consensus decision: openraft v0

Status: decision accepted; an in-memory three-node prototype is implemented in
`crates/d2-raft`, including a three-OS-process TCP integration test. The durable
RocksDB log store, state machine, and D1 wiring are the next D2 slice. This
document still freezes the Raft library choice and the prototype boundaries.

Related contracts: [`STORAGE_NODE_INTERFACE.md`](STORAGE_NODE_INTERFACE.md) (D1
storage-node seam), [`TRANSACTIONS.md`](TRANSACTIONS.md) (K3 single-shard
transaction boundary), and [`ROADMAP.md`](ROADMAP.md) (D2 scope).

## Decision

Use **openraft, pinned to exactly `=0.9.25`**, behind a project-owned
`Consensus` interface. Do not build a Raft implementation in D2, and do not
build on openraft 0.10-alpha yet.

Rationale:

- openraft is a complete replicated-state-machine framework: election, log
  matching, replication, compaction, snapshot transfer, joint membership, and
  linearizable-read support are in-crate. D1's partition operations map directly
  onto its `RaftStateMachine`.
- `openraft::testing::Suite::test_all` is a storage conformance suite. It lets
  the prototype prove the RocksDB log/state-machine adapter before a cluster is
  trusted, which is the riskiest D1/D2 seam.
- License is MIT OR Apache-2.0. It is used by Databend meta-service, CnosDB and
  Hiqlite; the project advertises deterministic-simulation and Jepsen testing.
- Pinning `=0.9.25` avoids the edition-2024 / network-v2 churn of 0.10 while
  keeping the bugfix branch. The exact version must be re-checked before adding
  the dependency (`Cargo.lock` change plus license note per R9).

Rejected alternatives:

| Option | Why not now |
|---|---|
| `raft-rs` 0.7 | Raw consensus only: we would own log store, apply driver, transport, snapshots, membership and ReadIndex. The crate release is from 2023; master is active but would require a git pin and TiKV-scale maintenance. |
| `async-raft` | Abandoned since 2021; openraft is its successor and documents the fixed safety bugs. |
| Self-built Raft | Correct joint consensus, snapshot install, vote persistence and crash recovery are months of work; keep only as an explicit fallback if the prototype exposes a fatal fit problem. |
| raft-lite / hiqlite / raft-rs forks | Educational or application-specific; not a framework for an external RocksDB state machine. |

## First prototype slice (one focused PR)

Create `crates/d2-raft` with:

- `types.rs`: `declare_raft_types!`; `LogCommand = TxnBatch(Vec<WriteOp>) | Noop`,
  `ApplyResult`, `NodeId = u64`, node address metadata, logical snapshot data.
- `log_store.rs`: `RaftLogStorage` + `RaftLogReader` over a **separate**
  `raft.db`; keys `raft/log/<index>`, `raft/vote`, `raft/last_purged`; append and
  vote writes use `sync=true`; truncate/purge use bounded range deletes.
- `state_machine.rs`: `RaftStateMachine` over the D1 partition. Apply one
  `WriteBatch` plus an applied-index marker, idempotent by log index; logical
  `scan` dump for snapshots; `install_snapshot` writes the dump and marker.
  Must pass `Suite::test_all`.
- `network.rs`: in-process `RaftNetworkFactory`/`RaftNetwork` dispatching to peer
  `Raft` handles; a `DropPolicy` (allow/deny/delay per from/to) for partitions
  and message loss.
- `tcp.rs`: TCP `RaftNetwork`/`RaftNetworkFactory`, one node per process, plus a
  control port and `d2-node`/`d2-ctl` binaries.
- `node.rs`: `Node::start(id, dir, peers)` / `shutdown()`.
- `tests/cluster.rs`: three in-process nodes; `tests/multi_process.rs`: three
  child processes over TCP, including leader kill and restart.

Prototype acceptance:

1. Bootstrap n1, add n2/n3 as learners, change membership to `{1,2,3}`.
2. Commit 100 single-shard transactions; every replica exposes the same logical
   scan and `ensure_linearizable()` succeeds on each node.
3. Force a snapshot, purge the log, wipe one follower's state machine, restart
   it; it installs the snapshot and converges.
4. Kill the leader while a writer is active; a new leader appears within roughly
   10 election timeouts; applied indexes stay monotonic and no committed entry
   is lost; the old leader rejoins and catches up.
5. Partition the new leader with `DropPolicy`; writes stop; heal; progress
   resumes. Tests use events/condition variables, not fixed sleeps.

Non-goals for this slice: gRPC, multi-partition placement, split/transfer,
joint membership, follower reads, backup/restore.

## Multi-process validation

`crates/d2-raft/tests/multi_process.rs` spawns three `d2-node` OS processes on
localhost TCP ports. It initializes all three voters, commits 50 writes, checks
the value on every process, kills the leader, elects a new leader, writes again,
and restarts the old leader with an empty in-memory log to verify catch-up. The
node binaries can also be started on separate hosts; `d2-node --bootstrap` uses
the same `initialize_cluster`/`TcpNode::bootstrap` API exposed by the crate, and
`d2-ctl` drives status, writes and reads. The TCP framing is newline-delimited JSON carrying the
openraft request/response types.

This proves cross-process consensus and serialization, not production network
security or durability. The transport has no TLS, authentication, connection
pooling or backpressure, and the control port is an operations/test helper.
Multi-host deployment still needs service discovery, credentials and a durable
log store.

## Decisions to freeze before implementation

- **Runtime:** Tokio multi-thread, matching openraft defaults.
- **Log location:** separate `raft.db` so log purge/truncate never touches graph
  keys and graph snapshots never contain Raft log keys.
- **Snapshot form:** logical D1 `scan` dump first; RocksDB checkpoint/file
  streaming is a later optimization. The install path must say whether it swaps
  the partition handle; do not pretend a live handle can be mutated in place
  without coordination.
- **Placement:** static partition -> replica-set table in config; prototype is
  three replicas of one partition. Placement is not a Raft-library feature.
- **Bootstrap:** `initialize` one node, add learners, change membership; make
  restart recover membership from the log and make bootstrap re-runnable.
- **Command contract:** one Raft entry is one committed single-shard transaction
  batch; `LogCommand` is versioned and replay-deterministic. Apply must not read
  wall-clock or non-deterministic state.
- **Reads:** leader-only `ensure_linearizable()` then local read; followers
  return `ForwardToLeader`. D1 `get/scan` during election gets a bounded retry or
  a typed not-leader error, never a stale success.
- **Errors:** map `NotLeader { leader_hint }` into the D1 error surface.
- **Metrics:** expose `Raft::metrics()` (term, last log index, replication lag)
  before adding operational dashboards.

## Risks

- **High:** openraft 0.9 is bugfix-only and 0.10 is alpha. Keep `Consensus` thin
  and run an upgrade spike separately; do not build on 0.10-only APIs.
- **High:** openraft snapshots are full state-machine snapshots. A large
  RocksDB partition needs a checkpoint/streaming design; the prototype validates
  the chunked path.
- **Medium:** openraft assumes completed writes are durable. Missing `sync=true`
  on log/vote writes silently breaks Raft safety; crash/reopen tests are
  mandatory.
- **Medium:** `rocksdb 0.25` requires Rust 1.88 and a C++ toolchain; copy trait
  implementations, not dependency versions, from openraft examples.
- **Medium:** the TCP smoke test proves cross-process framing and leader
  failover, not multi-host network behavior, TLS, or OS-crash recovery. Those
  remain D3/D6 work; storage is still in-memory.

## Sources

- [openraft on crates.io](https://crates.io/crates/openraft)
- [openraft repository](https://github.com/databendlabs/openraft)
- [openraft `RaftStateMachine`](https://docs.rs/openraft/0.9.25/openraft/storage/trait.RaftStateMachine.html)
- [openraft getting started](https://docs.rs/openraft/0.9.25/openraft/docs/getting_started/index.html)
- [raft-rs on crates.io](https://crates.io/crates/raft)
- [tikv/raft-rs](https://github.com/tikv/raft-rs)
- [async-raft on crates.io](https://crates.io/crates/async-raft)
