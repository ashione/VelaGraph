# v0.0.1 candidate capability and compatibility matrix

Source inventory: main `35d0a8ee5a09bfbf0686dcc862d3f972c7cbb1b8`, tree
`0959ff268fc860f74974838b340ff40f3869c303` (2026-11-16, including #297,
#299 and #300). **Not a release announcement or completed acceptance.**
This page consolidates the implemented service surface; the existing
[release plan](RELEASE_PLAN.md#v001--bounded-fixed-topology-experimental-service)
remains the acceptance standard. A merged implementation or test is not proof
that every release requirement is satisfied.

## Start here

- Fresh development cohort: [managed-service demo](MANAGED_SERVICE_DEMO.md).
  It creates separate node/service processes, rejects an existing directory and
  is not a restart, recovery or production provisioning command.
- Existing live, activated, identity-matched cohort: [graph-service](PUBLIC_SERVICE_CLI.md).
  Explicit default-loopback HTTP and optional loopback gRPC ports, development
  opt-in required. Separate trusted-network HTTP opt-in is a platform extension,
  not a new v0.0.1 release or public-exposure certification.
  It cannot install snapshots, clear offline restore guards or force READY.
- Rust reusable domain client: [GatewayClient](GATEWAY_CLIENT_CHANNEL.md).
  One fixed endpoint/binding per client; clones reuse a channel. No discovery,
  endpoint replacement, automatic mutation retry or session failover.
- Generated Go client and real Rust-service example:
  [Go interoperability](../examples/go/managed/README.md). This is a finite
  integration example, not a production SDK or all-method interoperability claim.

## Implemented public method matrix

HTTP routes use the `/preview1` namespace. gRPC names below are under
`velagraph.public.managed.v1`. HTTP and gRPC call the same business state, but
have **different wire formats**. Neither listener dispatches internal Raft,
participant, raw KV/Plan, membership, restore or administrative methods.

| Capability | HTTP | gRPC | Important boundary |
| --- | --- | --- | --- |
| Typed write submit/status/abort | `POST /preview1/writes:submit`, `:status`, `:abort` | `WriteService.Submit`, `Status`, `Abort` | Status needs original external ID **and exact mutation**, not ID alone. Abort is explicit, never inferred from timeout. |
| Data-local Gremlin result session | `POST /preview1/queries:open`, `:fetch`, `:close` | `QueryService.Open`, `Fetch`, `Close` | Supported subset only; immutable V/result and owner-bound token. Latest, exact and supported minimum-V selector retain their existing semantics. |
| Fixed-snapshot session | Not implemented | `FixedSessionService.Open`, `Read`, `Traverse`, `Page`, `Close` | Separate registry/token type from QueryService; bounded cohort capture, not unlimited query or data-local scaling. |
| Managed asynchronous query job | `POST /preview1/jobs:start`, `:get`, `:renew`, `:fetch`, `:cancel` | `ManagedQueryJobService.Start`, `Get`, `Renew`, `Fetch`, `Cancel` | Existing fixed-V job/lifetime/budget contract; not a durable cross-owner job store. |
| Static process liveness | `GET /preview1/service` | No equivalent public liveness RPC | Alive does not certify graph-query routes or restore completion. |
| Bounded fixed-read observation | `POST /preview1/readiness` with `{}` | `ReadinessService.Check` | Expensive default-profile capture, not general LB health, write readiness or restore READY. |

HTTP job parity is implemented in the platform extension; FixedSession parity
remains absent. Do not use
proposed `/v1/...` routes from the target design as runnable APIs. The finite
release plan requires both protocols to support writes and supported queries;
this matrix does not weaken that requirement or claim that additional gRPC-only
methods have HTTP parity.

Writes currently expose create vertex/edge, patch vertex/edge, conditional patch
vertex/edge, delete edge and replace edge. Arbitrary transactions, vertex deletion,
temporal editing and Gremlin mutation scripts are not public managed operations.
Gremlin uses the official grammar with entrypoint-specific supported subsets;
unsupported features fail rather than falling back to observed reads. Certified
same-version selection is not linearizable latest; existing `Unverified`
adjacency completeness is never promoted to `Complete`.

## Compatibility and operating limits

| Area | Current contract |
| --- | --- |
| HTTP encoding | Preview JSON, not protobuf JSON: canonical decimal strings for IDs/versions/Integer properties; lowercase hex tokens. Unknown/duplicate fields and nonfinite values reject. Stable HTTP v1 compatibility is not promised. |
| gRPC encoding | Checked-in typed schema; public Context v1 and identified library Context v2 with exact binding/correlation. Unsupported versions reject. See [error compatibility](PUBLIC_GRPC_ERROR_COMPATIBILITY.md): standard `google.rpc.Status` with typed `ErrorDetail`, not the old bare-detail trailer. Upgrade old clients/servers together; no mixed-version promise. |
| Message/admission budgets | Public request 64 KiB and response 1 MiB; HTTP/gRPC share eight operation slots and retained budgets. Method/profile limits apply independently. Paging does not raise capture or full-result ceilings. |
| Capture profile | The [read probe](FIXED_READ_READINESS.md) uses `Parity2MiBV1`, including 10,000-row capture limit. Failure at this limit says nothing universal about other APIs/profiles or the 200,000-person fixture. |
| Deadlines | Finite request budget up to 60 seconds, shortened by caller/method/session limits; default host maximum 15 seconds. RPC deadline and retained session/job lifetime are different clocks. |
| Session ownership | HTTP/QueryService continuations can cross protocols on the same process, not gateways. Wrong owner, restart, expiry or close rejects; no silent re-open at latest. FixedSession/job tokens have their own contracts. |
| Drain/restart | [Bounded drain](SERVICE_DRAIN.md) stops new requests and waits for admitted calls (default 5 seconds, maximum 60 seconds), then cancels. It does not transfer sessions, roll back writes or provide transparent rolling upgrade. |
| Client cancellation | GatewayClient blocks future DATA polling after expiry/cancel and retains admission while transport owns the body. Already-buffered bytes cannot be recalled. A permanently stalled peer can retain permits until transport recovery or explicit terminal `close()`. Close affects all clones and prevents reconnect; resource reclamation requires the owning runtime to run, and it does not cancel server sessions or settle writes. |
| Write uncertainty | Lost ACK, cancellation or deadline may mean Unknown, not rollback. Recover original ID/exact command via Status. Absent is an observation, not proof that delayed admission is impossible. No automatic write replay/hedging. |
| Deployment/recovery | Fixed topology and development-only; HTTP requires separate explicit opt-in for trusted-network access. No authentication, TLS/public exposure, dynamic ingress balancing, online global restore, automatic node failover or mixed-version rolling recovery claim. Offline restore guards remain enforced. |

Machine-readable failures are part of the interface: HTTP uses `error.code`
with 400 invalid, 409 identity/precondition, 410 unavailable session, 429 capacity,
501 unsupported, 503 uncertain/unavailable and 504 known query deadline.
gRPC uses typed status details; do not parse messages or equate every transport
failure with a known application rejection. See the exact
[HTTP](PUBLIC_HTTP_PREVIEW.md#failure-and-retry-contract) and
[gRPC](PUBLIC_GRPC_SERVICE.md#errors-and-ownership) contracts.

## Evidence and release blockers

| Evidence in main | What it establishes, and what it does not |
| --- | --- |
| Fresh startup #284 | Fresh directory, separate processes, write/query and orderly child cleanup. Not restart/restore or SIGKILL cleanup. |
| Public-service process suite, including #291/#295 | Dual protocols, fixed-V pages, two real gateways, wrong-owner/restart rejection, and injected lost ACK followed by original-ID recovery from another gateway. Explicit same-ID replay preserves original ordinal/V and complete row oracle in this scenario. Not general exactly-once, automatic retry or a network SLA. |
| Probe #292 | Graph-query listener failure rejects a capture while static liveness can remain available. Cleanup has bounded best-effort close and TTL fallback. Not global restore acceptance. |
| Drain #293 | HTTP/gRPC admission stop, admitted calls completing or reaching their deadline. Not session migration. |
| Reused client #294 | Counted connection reuse, cancellation/queue limits and recovery in controlled HTTP/2 tests. Not multi-host throughput, discovery or proof that already-dispatched writes can be cancelled. |
| Terminal client close #297 | Real transport tests cover stalled quota, cold close/no reconnect and complete Submit DATA-before-close returning Unknown. Close initiation is not synchronous reclamation or proof of server rollback. |
| Public quorum failure #299 | HTTP/gRPC reject new query/write success during coordinator inter-voter isolation, then verify original-ID decisions, complete result oracle and old-V pages after explicit healing. Three voters live inside one node child; this is not independent-voter-process HA, automatic leader routing or a historical #128/#247 root-cause finding. |
| Generated Go suite | Real Rust-service typed values/presence, original-ID replay, fixed pages, standard rich errors and listener isolation. Not all-method coverage, production SDK maturity or upgrade compatibility. |

For a release candidate, record the **exact head/base/tested tree**, independent
review and all required CI results. The above source inventory does not replace
that evidence or declare currently running checks successful. CoreHR observed
full-scale tests do not certify full-scale managed same-V execution.

The original paged-snapshot failure tracked by [#128](https://github.com/ashione/VelaGraph/issues/128)
and the snapshot seed/quorum failure in [#247](https://github.com/ashione/VelaGraph/issues/247)
remain unresolved reliability evidence in their recorded diagnosis. See the
[release risk disposition](RELIABILITY_RELEASE_RISKS.md) for the exact evidence
and remaining decision gate. #128 was reopened on 2026-11-16 after a status/acceptance audit: its prior
closed GitHub state was not a root-cause disposition, and its stated acceptance
and last documented analysis still require one. A green run, diagnostics or a test
that did not reproduce the failure is not a repair. Before release, determine
whether each defect affects the advertised finite scope, fix it with evidence or
explicitly exclude the affected feature with a reproducible limitation as the
existing release plan requires. Do not silently remove failing gates.

## Candidate packaging and remaining acceptance

The planned **repository/product prerelease is v0.0.1**. The four Cargo packages
currently declare **0.1.0**; that package metadata is not a released product tag
or a crates.io publication. The initial packaging direction is source-only:
no prebuilt binary or binary compatibility claim follows from it. Preserve the
root `LICENSE`, the OpenRaft-derived `crates/d2-raft/NOTICE` and its two upstream
license copies in source archives. A future binary distribution needs its own
artifact validation, checksums and dependency/notice review.

The source inventory above is not a frozen release candidate. After remaining
risk disposition and documentation are integrated, bind final independent review,
normal CI, real-process Go interoperability, documented clean startup, complete
CoreHR and retained 1025-write lifecycle evidence to the final exact commit/tree.
Keep passed, failed and not-run distinct. #300's opt-in lifecycle timing comparison
is measurement infrastructure, not an extra v0.0.1 feature or release gate.
Do not create a tag/prerelease while required checks or the risk decision remain
unresolved. This inventory update neither lowers #224's scope nor grants a waiver.

This matrix records remaining work rather than marking #218, #221 or #224 done.
Global recovery, elastic routing, production security and later milestones remain
separate; none is implied by a bounded experimental service release.
