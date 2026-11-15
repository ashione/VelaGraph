# Fixed-session read readiness probe

Status: additive experimental service capability, tracked under #221. This is
not completion of gateway routing/load balancing, restore or high availability.

## What the observation proves

The HTTP and typed gRPC probes share the same gateway state and query-independent
fixed-session acquisition used by local fixed-version queries. A successful
response means that this attempt admitted one managed cohort cut, opened fixed
captures on **every configured graph-query route**, and checked their complete
retained proofs, version, profile and deployment identities. It returns that
observed certified version. It does not execute Gremlin or retain a public cursor.

Checking only the control RPC listener or obtaining an admission proof would not
be sufficient: the graph-query listener is separate and might be unavailable.
`GET /preview1/service` remains a cheap static process-liveness report, not this
probe. Neither that report nor this probe changes the global recovery state.

## HTTP

```sh
curl -X POST http://127.0.0.1:8080/preview1/readiness \
  -H 'Content-Type: application/json' -d '{}'
```

The body must be the empty JSON object. Unknown fields, including a query, are
rejected. The existing `x-velagraph-timeout-ms` request budget applies.
A successful response has this shape:

```json
{"state":"fixed_session_read_captured","version":1,"profile":"parity2_mib_v1","restore_supported":false,"production_ready":false}
```

Failures use the existing HTTP error envelope and a non-success status; an
unavailable route, an identity/proof mismatch, a timeout or an exhausted capacity
budget must not be reported as a successful capture.

## gRPC

`velagraph.public.managed.v1.ReadinessService/Check` accepts
`ReadinessRequest { context }` and returns `ReadinessResponse { version, identity }`.
The existing Context version 1/2 identity and deadline rules apply. The default
profile is the same `Parity2MiBV1` as the HTTP probe. This additive RPC is not the
standard `grpc.health.v1` inexpensive serving-status API, and old servers do not
implement it. Regenerate clients from the checked-in public schema to call it.

## Cost, limits and lifecycle

This is a deliberately **expensive fixed-session capture observation**, not a
cheap generic liveness or fleet-wide load-balancer health check. It uses the
existing default profile and aggregate capture budgets, including the 10,000-row
limit. A larger graph can fail this probe even when another API/profile can serve
it. In particular, it must not be used to claim that the 200,000-person CoreHR
fixture is universally queryable, or that the whole graph service is unavailable
solely because this profile was exhausted.

Only one probe runs per gateway at a time. It shares connection, memory and
execution admission with real work; saturation produces an error rather than an
unbounded queue. Operators must choose a low probe frequency and back off on
failures, not have every client repeatedly capture the entire cohort.

Acquisition respects the original request deadline. Known acquired node sessions
are closed after success/failure, including cancellation cleanup. Teardown has
its own bounded cleanup budget and holds admission while it runs; it does not
extend the caller's acquisition deadline. An Open reply lost before a trustworthy
descriptor reaches the gateway cannot be explicitly closed and expires by its
node TTL. Runtime/process exit can likewise leave TTL cleanup as the backstop.
The probe therefore does not promise zero retained resources after a network or
process failure.

A successful observation is not a lease or guarantee about a later query. Writes,
route failure, resource pressure or process exit can occur immediately afterward.
It is not a write-readiness probe, authorization system, restore controller,
production-readiness certificate, automatic load balancing or failover.

## Evidence

`public_service_process::fixed_readiness_observes_graph_listener_failure` uses
three real managed-node processes and a separate service process. It checks HTTP
and gRPC at V0 and after a committed write, rejects an unknown HTTP request field,
then stops only one node's graph listeners while keeping its managed control/Raft
process alive. The committed write status remains accessible but both probes
must fail. This is local multi-process evidence, not multi-host deployment or
partition-tolerance certification. Cancellation and cleanup checks are separate
lower-level tests; CI results must be recorded for the exact tested revision.
