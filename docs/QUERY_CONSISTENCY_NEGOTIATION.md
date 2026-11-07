# Explicit distributed read consistency (protocol v1)

Status: implemented admission and anti-downgrade contract, **not** implementation
of global query snapshot isolation. No write path, storage format, Gremlin syntax,
or legacy query contract changes.

## API

`remote::distributed::consistency` exposes:

- `ReadConsistency::IndependentShardSnapshots`: every participating shard is
  frozen independently. The resulting vector can be fractured, even when all
  writes have successfully acknowledged and graph structure validates.
- `ReadConsistency::GlobalCommittedSnapshot`: requires one complete committed
  global version, with unresolved cross-shard transactions excluded. This mode
  currently returns `QueryFailure::Unsupported` for **every** catalog, including
  one-shard catalogs. Having one selected shard does not establish coverage of
  all writes or transaction participants.
- `query_with_consistency(endpoint, request, required, deadline)` for ordinary
  queries and `job_with_consistency(endpoint, request, required, deadline)` for
  count, row and projected-row jobs, including advance/read/cancel.

These functions retain the ordinary typed result shapes. Success is accepted
only after checking the version, achieved consistency, output operation and all
existing result/stamp validations. They never retry or weaken the request.

The old `distributed::query` and `jobs::request` functions are unchanged and
continue to mean independent shard snapshots with Unverified completeness.
Consistency and structural completeness are distinct: neither this API nor a
future globally consistent read implies complete graph adjacency by itself.

## Wire and compatibility

The explicit API uses a separately shaped envelope:

```json
{"consistency_request":{"version":1,"required":"GlobalCommittedSnapshot","operation":{"Query":{"start_partition":0,"text":"g.V(1)","timeout_millis":5000}}}}
```

A v1 reply has `version` and `result`. A successful result contains `consistency`
and `output` (`Query` or `Job`); a failed result uses the existing typed query
error. Unknown protocol versions return Unsupported. Unknown mode names or
malformed envelopes fail deserialization, never fall through to legacy handling.
Old coordinators reject this shape rather than execute it with weaker semantics;
clients surface their transport/protocol failure and do not retry a legacy shape.

No field is added to existing request or output structs, and no persisted format
changes. New applications can request their actual required guarantee now,
without mistaking old successful queries for globally coherent reads. A future
MVCC implementation must introduce the corresponding protocol/client support;
this client deliberately rejects a server's unsupported global-success claim.

## Admission and resources

The coordinator validates the supported consistency/version **before** session
creation, opening shard RPCs, allocating a job, looking up a job token or changing
its sequence. Global requests fail closed for synchronous queries and every job
operation. In particular, a weaker legacy token cannot be adopted as a globally
consistent job; rejected advance/cancel requests leave it usable with its real
weaker guarantee. This is not an authentication or access-control boundary.

The existing coordinator concurrency limit, frame/text limits, deadline, snapshot
leases, cumulative budgets, cleanup and output checks remain in force. A
negotiation reply's overhead counts against the existing wire limit; it does not
silently enlarge budgets. No writer is paused, fenced or claimed transactional by
this change.

## Validation

- Unit checks reject silent downgrade, unknown version, unimplemented global
  claims, mismatched operation outputs and unknown consistency names.
- Real TCP coordinators with one and two nonresponding shard endpoints reject
  global requests for ordinary queries and all job operations **without opening
  any shard connection**.
- Independent data processes validate unchanged legacy/negotiated query output,
  negotiated job progression and strict rejection without consuming a weaker
  live job token.
- The deterministic concurrent-write scenario uses the explicit independent
  mode to capture the documented fractured vector. This is evidence that the
  weaker mode still has its actual semantics, not a global isolation pass.

## What must precede enabling global reads

The next implementation must establish a durable transaction/version authority,
participant prepare/commit resolution and a complete committed-prefix watermark,
then per-version data reads and pins/GC. The authority must cover vertex/edge
mutation, raw remote D1 writes and administrative import/restore/epoch changes;
query-coordinator-only gating is insufficient. The current activated D1 state
machine already rejects legacy `TxnBatch` writes, but regular `D1V1::Write`
remains per-shard and has no global transaction identity or decision.

Staged cross-shard graph edge writes must not expose only one participant to a
strict read. Restart, ambiguous commit outcomes, unresolved prepares, stale
routing and old clients must fail closed or be resolved by protocol. Merely
parallelizing shard opens, attaching a wall-clock timestamp, or advertising a
capability is not a substitute. See [dynamic consistency design](DYNAMIC_QUERY_CONSISTENCY.md).
