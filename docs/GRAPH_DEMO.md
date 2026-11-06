# Graph client multi-process demo

This development-only demo launches **three independent OS processes**: one
single-voter metadata group and two single-voter data groups. It registers
partitions 10 and 20, creates distinct vertices with numeric ID 1 in both, creates
experimental cross-shard edge 10:7, and checks this standard observed Gremlin:

```text
g.V(1).outE('knows').inV().path()
```

The start partition is 10. Expected qualified path is vertex 10:1 → edge 10:7 →
vertex 20:1. The returned completeness must be **Unverified**, never complete.
Cross-shard writes are **non-atomic**; observed adjacency cannot establish global
completeness. This is not a failover, replication-quorum, recovery, production
security, or distributed-transaction demonstration. Single-voter groups deliberately
keep the demonstration small and do not tolerate node loss.

## Run

Requires Python 3 (standard library only) and the Rust/C++/libclang prerequisites
in the project README. From the repository root:

```sh
cargo build --locked -p replicated-graph --bins
python3 scripts/graph_client_demo.py \
  --allow-insecure-development \
  --node target/debug/graph-node \
  --client target/debug/graph-client
```

If `CARGO_TARGET_DIR` is set, substitute its binary paths. The explicit flag
acknowledges **unauthenticated plaintext**. All nine Raft, control and graph
sockets bind to dynamically allocated loopback ports. Run only on a trusted,
isolated development machine. Other local users may access these ports.

Each run allocates a fresh temporary directory, printed before starting nodes.
It never opens or empties an existing user data directory and never retries a
write after an uncertain outcome. A port-allocation race fails the run rather
than attaching to another service. Startup and each client invocation have
finite timeouts. Failure includes bounded tails of node logs. Every completed
client invocation is appended to `client-results.jsonl` with arguments, exit
status, raw stdout/stderr, and parsed JSON when available. Failure messages keep
both stdout and stderr, including structured unknown-outcome request IDs and
stage evidence. A client timeout is recorded as unknown and is never retried;
killing the client cannot cancel a server-side write already submitted.

Completion, ordinary failures, SIGTERM, and Ctrl-C stop and reap only child processes
created by this invocation, never processes selected by name or port. Forced
termination of the Python driver (for example SIGKILL) cannot run cleanup.
All data and logs are retained for inspection, including on successful runs.
Review the printed directory and remove it yourself when no longer needed.
Each new run starts from new data rather than resuming these directories.

## Tests

```sh
python3 -m unittest discover -s scripts -p 'test_graph_client_demo.py'
cargo test --locked -p replicated-graph --test graph_demo
```

The Cargo integration target runs both the Python safety suite (with bytecode
cache creation disabled) and the same demo script against Cargo-built binaries. It
checks the qualified path and explicit Unverified result. It also checks nonzero
JSON failures for unsupported Gremlin, a missing partition, a mismatched node
identity, and an unavailable endpoint with a 1 ms configured deadline. The last
check does not depend on a healthy query taking longer than 1 ms. It requires loopback
sockets plus Python 3. It retains its temporary directory just like a manual run.
The successful path also checks catalog placement and metadata sequence,
independent shard snapshot stamps, v1/v2 isolation, experimental global and empty
counts with Unverified completeness, and Bob's partition-qualified projection.
The Python tests exercise preservation of unknown-outcome diagnostics, no retry
after failure or timeout, refusal without explicit plaintext acknowledgment and
child-only cleanup on both normal exit and forced-stop fallback.
