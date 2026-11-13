# Snapshot seed quorum investigation (#247)

Status: **unresolved**. This is evidence collection, not a reliability fix.

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
