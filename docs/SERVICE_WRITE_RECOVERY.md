# Public write recovery after a lost reply

A public Submit timeout, disconnect or lost response is an **unknown outcome**,
not rollback. This rule is identical for HTTP and typed gRPC. It also applies
when a shared channel closes or a gateway drains before the caller receives the
reply. Transport reconnection is not permission to replay a mutation.

## Caller procedure

1. Before Submit, retain the external request ID and the complete exact typed
   mutation. Keep the same authority, cohort and mapping configuration.
2. If the response is uncertain, call Status with that original ID **and exact
   mutation**. Another gateway configured for the same managed cohort may
   perform this lookup; it does not need the original gateway's session tokens.
3. Interpret the durable decision and visibility independently:
   - `Committed` preserves the original ordinal/transaction identity. A false
     visibility flag is not an abort and does not authorize a new write ID.
   - `Aborted` preserves the original decision, rather than starting a replacement.
   - `Pending` remains unresolved. Status does not complete participant apply,
     decide, or abort it. Explicit recovery actions retain the original command.
   - `Absent` means absent at this leader-barrier observation only. A delayed
     first Submit can still be admitted later.
4. Never generate a replacement ID just because the first response was lost.
   An explicit Submit with the original ID and identical command uses the existing
   durable original-Plan recovery contract. It is not an automatic retry policy.
   A different command with the same ID conflicts, not a replacement operation.

Status is observational: it does not admit, prepare, decide or catch up members.
A second gateway cannot infer from a network error whether the first Submit
committed. The coordinator's durable identity/decision is the authority, not a
client-side cache or the gateway process that received the original request.
Concurrent gateway writers still obey the existing coordinator ordering and
single-writer deployment limitations; this is not new multi-writer scheduling.

## Query ownership is different

Write recovery by durable ID does not make process-local query sessions,
continuations or jobs portable. Those remain bound to their owner incarnation
and fixed V. A wrong-owner or restarted gateway rejects the token rather than
opening a new latest view. See [the service host](PUBLIC_SERVICE_CLI.md) and
[owned channel limits](GATEWAY_CLIENT_CHANNEL.md).

## Acceptance boundary

The independent-process regression in `public_service_process` exercises two
real public gateways over the same three durable node processes. Its test-only
fault path must suppress a successfully produced Submit response before the
caller receives it, then obtain Status through the other gateway with the
original ID/command. The full returned row set and certified V are checked,
including after an explicit identical request and a conflicting altered request,
so an idempotent final vertex value alone cannot hide duplicate execution.
The query retains `Unverified` completeness: matching the independent row oracle
is not a new cross-partition completeness certificate. This injected case is
already committed/visible; it does not exercise Pending/Absent recovery.

The fixture is same-host and fixed-topology. It does not prove multi-machine HA,
leader failover, gateway session migration, automatic retry, arbitrary concurrent
writers or production transport security. Exact candidate test results and CI
must be recorded separately; this document alone is not a passing test result.
