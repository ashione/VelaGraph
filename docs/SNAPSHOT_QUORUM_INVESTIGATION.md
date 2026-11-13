# Snapshot seed quorum investigation (#247)

Status: **original CI cause unresolved**. The bounded recovery policy below
addresses a controlled transient mechanism; it is not proof of the original cause.

## Preserved observation

[Issue #247](https://github.com/ashione/VelaGraph/issues/247) records the original
[failed CI job](https://github.com/ashione/VelaGraph/actions/runs/43377553475/job/131727467953)
and its [artifact](https://github.com/ashione/VelaGraph/actions/runs/43377553475/artifacts/14016036994).
On head `b05a67d2`, tree `a28770a9`, the three-process snapshot differential fixture
failed while seeding 36 vertices with 64 KiB text properties:

- `Cluster::seed` called `GraphRecordStore::create_vertex`.
- Its remote read view failed at `open_snapshot_barrier` with `quorum_not_enough`.
- The phase took 102,065 microseconds; the outer request budget had **not** expired.
- An unchanged-head CI retry and a local targeted run passed. Neither proves the
  original failure was harmless, deterministic, fixed, or caused by runner load.

The pinned OpenRaft 0.9.25 `handle_check_is_leader_request` uses
`config.heartbeat_interval` as each peer confirmation's timeout. Process nodes
currently configure that interval to 100 ms. The remote fixture's ten-second
budget is an **outer** budget, not an override of this internal confirmation
budget. This explains the timing boundary, but not why confirmation failed.

Possible causes still require evidence: peer scheduling or I/O delay, connection
failure, transport admission, or leadership change. The original log does not
identify which seed vertex failed or the replica state at that point. Do not
conflate this with the large single-entry replication case tracked in #240.

## Failure-only fixture evidence

`grpc_snapshot_process` now attaches the seed vertex ordinal to the original
error chain. After failure it queries the three existing control Status endpoints
concurrently with a 300 ms cap per request and records only node/leader IDs,
last-log/applied indexes, or a fixed unavailable/timeout category. It does not
format peer errors, properties, graph records, paths or endpoint strings.

These are **post-error observations**, not an atomic snapshot or evidence of the
state at the exact barrier failure. Collection does not retry a mutation, change
Raft/read deadlines, suppress the original error, change the public error mapping,
or turn the failed test into a pass. At most three read-only requests are issued.
A quorum-loss negative test stops both followers before seeding and requires the
failure to retain both the seed stage and all three observation outcomes.

## Reproduction and next decision

```sh
VELAGRAPH_REMOTE_DIAGNOSTICS=1 cargo test --locked -p replicated-graph \
  --test grpc_snapshot_process -- --nocapture
```

For repetition, record every outcome (including successes), exact source tree,
compiler and CPU/resource conditions. Do not retry failed writes within a run.
Compare failures by stage and replica observations; add transport-level evidence
only when the bounded fixture data cannot distinguish hypotheses. A successful
quorum-loss negative test validates fail-closed reporting, **not** the cause of the
original transient CI failure. Keep #247 open until a causal result or an explicit
remaining-risk decision is reviewed.

## Bounded confirmation recovery proposal

The original CI root cause remains unproven. Controlled process-scheduling tests
on the #249 + #250 tree reproduced the same error with two followers stopped for
180 ms: three of three attempts failed around 102 ms, before the outer deadline.
Three baseline attempts, three 40 ms two-follower pauses and three 180 ms
single-follower pauses succeeded. After resumption all nodes retained the original
leader and a new read succeeded. These experiments prove a sufficient mechanism,
not what happened on the original runner; the isolated probe used an empty graph,
not the original 36-record seed workload.

### Existing-system contracts

- [OpenRaft 0.9.25 ensure_linearizable source](https://docs.rs/openraft/0.9.25/src/openraft/raft/mod.rs.html):
  each call obtains a quorum-confirmed read log ID, then waits for local applied
  state to reach it. Retrying a failed read confirmation is not permission to
  bypass either step. The apply wait has no internal timeout, so the caller's
  absolute deadline must cover it too. Do not assume newer main-branch OpenRaft
  read-policy APIs exist in our pinned dependency.
- [etcd v3.5.17 requestCurrentIndex](https://github.com/etcd-io/etcd/blob/v3.5.17/server/etcdserver/v3_server.go#L808-L878):
  ReadIndex reissue has a separate retry timer and a total request timer; leadership
  changes and shutdown fail the request. Its read loop also waits for the applied
  index. We adopt the separation of bounded confirmation recovery from application
  replay, not its exact timer values or implementation.

### Chosen narrow policy

Remote snapshot/query opening, one-shot snapshot reads and leader probes share a
confirmation helper. It invokes a **fresh** `ensure_linearizable()` on each
attempt, retrying **only** typed `QuorumNotEnough`, up to three total attempts with
10 ms between failures. Attempts, backoff and apply wait share the original
absolute request deadline. Fixed-view acquisition also uses the earlier lease
boundary. No Raft heartbeat/election/append timeout is increased.

Three attempts cap additional quorum traffic at two extra rounds per admitted
request even under persistent failure. The short backoff avoids tight loops;
this is a bounded recovery policy, not a guarantee under arbitrary scheduling
pauses, and not a performance-optimal batching policy. Existing request admission
remains in force. Persistent quorum failure stays an error; the last quorum error
is retained at the cap. Deadline exhaustion is a timeout. NotLeader and fatal
errors stop immediately. Session creation/capture occurs only after success.

There is no automatic leader forwarding, stale read fallback, session replay,
query replay or mutation replay. A write that might already have been dispatched
still requires its original idempotency identity. This differs from #240's large
indivisible logged entry: reissuing read confirmation cannot fix an entry that
cannot replicate, and must not stand in for append admission or progress work.
In-process adapter and managed-operation barriers are unchanged in this slice;
they retain their existing caller timeout and fail-closed semantics.

### Verification and residual risk

`remote::read_barrier::tests` checks typed retry classification, attempt bounds,
no polling after expiry, pending apply timeout and budget consumed during backoff.
The ignored `investigate_read_confirmation_scheduling_window` fixture runs the
controlled matrix against real nodes and requires recovery within the unchanged
budget. Run it explicitly with `--ignored --nocapture`; it is deliberately not a
new mandatory timing-sensitive CI gate. The normal permanent-quorum-loss fixture
must still fail with original seed context. Opt-in diagnostics retain each failed
confirmation's reason even when a later attempt recovers.

Passing these tests addresses the demonstrated transient mechanism; it does not
identify the original CI cause or justify closing #247 on its own. Peer-level
latency evidence and the full seed workload remain useful follow-up work.

The final controlled matrix uses the typed snapshot gRPC client so its 50 ms
budget reaches the server: all three short-budget cases timed out at about 51 ms,
with server diagnostics reporting `expired=true`; all 12 longer-budget cases
passed. Legacy TCP snapshot envelopes do not propagate a shorter client budget;
their server-side configured deadline is unchanged. Do not infer end-to-end
client deadline propagation for that legacy path from the shared helper.
