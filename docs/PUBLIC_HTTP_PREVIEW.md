# Managed HTTP preview adapter

This is an **experimental library entrypoint**, not the v0.0.1 release or the
final protobuf/HTTP contract. It uses the existing certified managed gateway
business implementation. A [typed public gRPC adapter](PUBLIC_GRPC_SERVICE.md) and
[standalone service host](PUBLIC_SERVICE_CLI.md) are separate slices sharing this
managed business path. A final stable dual-protocol contract and production cluster
startup packaging remain work under [#218](https://github.com/ashione/VelaGraph/issues/218). See the separate
[shared service design](PUBLIC_SERVICE_DESIGN.md).

## Host and lifecycle

After constructing an existing validated live `managed_gateway::Gateway`, call
`gateway.bind_http("127.0.0.1:0".parse()?, deadline).await`. Use its `local_addr()`
and keep both gateway and returned `HttpService` alive. Call `shutdown().await`
for orderly cleanup. Dropping either server signals shutdown. The default `bind_http` rejects non-loopback addresses. The explicit
`bind_http_trusted_network` API and CLI `--allow-insecure-network` opt-in accept
a caller-selected HTTP address; there is still no implicit public bind, TLS or
authentication. Use only a trusted network or authenticated TLS reverse proxy. Loopback alone is not an authentication or browser-attack
boundary; do not expose this preview to untrusted local clients. Host/Origin
policy and credentials belong to the later hardened service entry. Restored/offline nodes cannot bypass their existing startup
guards through this adapter. No public restore or readiness override is added.

`GET /preview1/service` describes a live managed gateway and explicitly reports
`restore_supported=false`, `production_ready=false`. This is **not** a global
restore READY certificate, availability promise or proof that the next request
can obtain a certified view. Every business operation retains its existing
identity/certification checks. A stopping gateway rejects new work.

Both HTTP and public gRPC share the same eight admission slots, retained
query budgets and submit serialization. The first HTTP implementation accepts
one request per connection (`Connection: close` behavior); persistent HTTP client
connections are not advertised. The separate typed public gRPC adapter is now
implemented and shares these same budgets; see [its contract](PUBLIC_GRPC_SERVICE.md). A maintained Hyper/Axum HTTP stack is
used, not a custom HTTP parser. Axum, Hyper and hyper-util are existing locked
MIT dependencies; they are now direct dependencies for this adapter. No new
crate package or version was introduced.

## Preview wire contract

POST requires `Content-Type: application/json`. Request bodies are capped at
64 KiB, responses at 1 MiB. All JSON object levels reject duplicate keys, typed
DTOs reject unknown fields, and nonfinite reals are invalid. One optional
`X-Velagraph-Timeout-Ms` header (1..60000) can shorten, never lengthen, the server
connection deadline. Header/body receive, decode, execution, encode and delivery
consume the same remaining budget. Per-method limits and session expiry shorten
it again. A new page request does not extend the session's lease or change V.

The envelope identifies `api: "velagraph.http.preview1"`. This is **not protobuf
JSON**: integer identities/versions and Integer property values are canonical
decimal strings, while binary IDs/tokens are lowercase hex. `partition`, page
size/offset/total and lease milliseconds are bounded JSON integers. This preview
schema is intentionally isolated under `/preview1`; a future stable HTTP v1 contract may replace it, without an indefinite preview compatibility
layer. No stable v1 client compatibility is promised here.

| POST action | Body |
| --- | --- |
| `/preview1/writes:submit` | external_id + exact typed mutation |
| `/preview1/writes:status` | Same external_id and exact mutation, not ID alone |
| `/preview1/writes:abort` | Same identity/command; explicit existing abort action |
| `/preview1/queries:open` | read selector, ttl_millis, query, page_size |
| `/preview1/queries:fetch` | session, identical query, cursor, identical page_size |
| `/preview1/queries:close` | session |
| `/preview1/jobs:start` | operation_id, read, query, page_size, policy |
| `/preview1/jobs:get` | job |
| `/preview1/jobs:renew` | job, expected_generation |
| `/preview1/jobs:fetch` | job, identical query, cursor, identical page_size |
| `/preview1/jobs:cancel` | job |

Example vertex write body:

```json
{"external_id":"01010101010101010101010101010101","mutation":{"type":"create_vertex","partition":7,"vertex":{"id":"1","labels":["person"],"properties":{"name":{"type":"text","value":"Alice"}}}}}
```

Mutation tags cover the existing managed `create_vertex`, `create_edge`,
`patch_vertex`, `patch_edge`, conditional `patch_vertex_if`/`patch_edge_if`,
`delete_edge`, and `replace_edge`. This is not arbitrary graph transactions,
vertex deletion, temporal editing or Gremlin update scripts. Missing property
and `{"type":"null"}` are distinct. Integer values are strings, Real values
are finite JSON numbers, Text and Boolean have their corresponding JSON types.
The [Real precision correction and upgrade boundary](HTTP_REAL_PRECISION.md)
explains non-transparent parser changes, original-ID compatibility and retained
stored bits; identical old decimal text is not always the same recovery command.

Example query body for a gateway configured with data-local graph routes:

```json
{"read":{"type":"latest"},"ttl_millis":60000,"query":{"partition":7,"text":"g.V(1,2)"},"page_size":1}
```

The read selector is required. Use `{"type":"exact","version":"0"}` for
exact V=0, or `{"type":"minimum","version":"2"}` for the existing minimum-V
path. Latest means a certified safe prefix for the fixed complete cohort, not
linearizable latest. The accepted Gremlin subset is exactly the managed
fixed-local executor's existing subset; unsupported scans/compositions return
`query_unsupported`. There is no observed-query fallback. A gateway without
data-local graph routes cannot serve these queries.

Query responses contain the immutable session (incarnation/token/version),
query, page size and page. The page contains typed rows, original completeness,
next cursor, offset and total. Use the returned session/query unchanged for
fetch, with cursor null to replay the first page or the prior `next` cursor to
advance. Every page is preflighted against the HTTP wire budget before the first
HTTP page is delivered, including sessions originally opened via public gRPC.
The successful whole-result HTTP preflight is cached on that immutable retained
session; later fetches still check liveness and their own response encoding,
but do not re-encode the entire result. Pages cannot exceed the full result or
execution budgets. `Unverified`
completeness is preserved rather than relabelled as complete.

## Failure and retry contract

The HTTP status is accompanied by `error.code`; clients must not parse messages.
Invalid input is 400, identity/precondition conflict 409, unavailable session
410, capacity 429, unsupported query 501, uncertain/unavailable operation 503,
and a known query deadline 504. The adapter does not invent a precise cause
where the existing managed error is Unknown. Unknown, deadline, disconnection
or canceled Submit **does not mean the write was rolled back**. Inspect/recover
the same external ID and command. Do not silently pick a new ID or automatically
hedge/replay writes. Absent is an observation, not a guarantee that a delayed
submission cannot later be admitted.

Committed and Aborted replies retain ordinal and visibility separately; a
committed-but-not-visible result is not changed to failed. Session tokens are
process-local capability identities, not authentication credentials. Wrong
owner/restart/expiry is an error, never recapture at a different V.

Shared Invocation ownership retains work permits and result memory through the
actual HTTP connection result. Failed/canceled delivery cleans up newly opened
sessions; blocking workers keep their reservation until they really finish.
Successful server-side send is not proof of client receipt, so lost replies
remain bounded by the original lease. Closing an existing session does not undo
writes or imply a distributed transaction abort.

## Verification boundary

Focused tests exercise real loopback HTTP with the same managed gateway as public gRPC,
strict codecs and delivery/admission ownership. These are not multi-machine
benchmarks, global restore tests or release-gate evidence. The separate [dual-protocol process test](PUBLIC_SERVICE_CLI.md) adds actual
independent processes and cross-protocol semantics. The fresh-only [development launcher](MANAGED_SERVICE_DEMO.md) is implemented; exact CI/release checks remain required before
v0.0.1; the finite preview feature/compatibility matrix must be explicit. Test results must be read from
the exact submitted commit rather than inferred from this document.

## Fixed-session capture readiness

`POST /preview1/readiness` with `{}` performs the bounded default-profile
[fixed-session read probe](FIXED_READ_READINESS.md), not Gremlin execution.
`GET /preview1/service` remains static liveness. A successful capture observation
is not general service/restore readiness or an inexpensive load-balancer check;
see the linked contract for profile limits, cost and cleanup behavior.


## Managed asynchronous jobs

The five `/preview1/jobs:*` actions reuse the same managed jobs as public gRPC;
they are not a second executor. This closes the preview job lifecycle only.
FixedSession HTTP parity and a stable HTTP v1 schema remain unfinished under
[#218](https://github.com/ashione/VelaGraph/issues/218).

Start example:

```json
{"operation_id":"01010101010101010101010101010101","read":{"type":"latest"},"query":{"partition":7,"text":"g.V(1,2)"},"page_size":1,"policy":{"idle_millis":10000,"execution_millis":10000,"lifetime_millis":30000}}
```

`read` accepts latest or exact (including exact `"0"`); minimum-version job
admission is unsupported and returns `query_unsupported`, never downgraded to
latest. Durations have the existing job-policy limits. Start returns
`type=job_started`, the original operation_id and `job={incarnation,token}`
(lowercase hex, 16/32 bytes). Accepted is not Ready and contains no rows.
Exact repeated Start reuses the existing job; changed arguments with the same
operation ID conflict. A lost HTTP response does not cancel an admitted job;
recover with the same operation ID and exact arguments, never generate a new ID
as an automatic retry. Jobs remain process-local and restart invalidates them.

Get/Cancel take `{"job":{...}}` and return `job_status`/`job_cancelled` with
`status={phase,version,total,failure}`. Phases are `capturing`, `running`,
`sealing`, `ready`, `cancelled`, `expired`, `failed`. Version is a decimal string
or null, total is an integer or null, and failure is null or `{"code":...}`
using the same error table as request errors. No intermediate rows are exposed.
Malformed HTTP hex is invalid_argument (400), while the protobuf bytes codec may
classify malformed lengths as identity mismatch; valid-but-wrong identities
retain the shared domain errors. Representation validation is not a promise of
one-to-one transport status codes.

Renew takes `job` and decimal-string `expected_generation`. Its `job_renewed`
reply includes `grant={generation,remaining_millis}`; generation is a decimal
string. Anchor remaining milliseconds at the request's send time. An exact
renewal replay does not extend expiry. Renewal does not extend the execution or
hard lifetime, and polling/fetching does not renew the lease.

Fetch takes `job`, the identical query/page_size, and optional lowercase-hex
cursor (null for the first page). `job_page` echoes these fields and returns
one fixed decimal-string `version` and the existing typed `page`. HTTP and gRPC
handles/cursors are interchangeable only at the same owning gateway. Fetch
before Ready returns `job_busy`; cancelled/expired/restarted handles cannot
silently obtain a new V. Cancel uses the existing idempotent cancellation rules.

Every immutable page is checked against both protobuf and HTTP JSON output
limits before Ready, including JSON escaping expansion. A result too large for
one public representation fails with resource exhaustion rather than becoming
Ready with undeliverable later pages. The existing retained reservation remains
owned through HTTP encode and socket delivery. Job methods cap the remaining
request budget at five seconds, independently of the job execution/lease budget;
there is no fresh per-phase timeout. All shared admission and recovery restrictions remain in force; external HTTP
requires the separate explicit trusted-network listener opt-in.

## External HTTP access boundary

The CLI requires both `--allow-insecure-development` and
`--allow-insecure-network` plus an explicit `--http ADDRESS:PORT` to accept
non-loopback HTTP. No default wildcard, firewall changes, reverse-proxy deployment,
TLS or authentication are supplied. Internal routes and the gRPC listener remain
loopback-only. HTTP authorization must be supplied by the trusted deployment
boundary; do not expose this plaintext mutation/query API directly to the Internet.

`http_jobs_explicit_trusted_network_independent_process` exercises the whole job
lifecycle over an actual non-loopback host interface with separate node/service
processes. It is same-host networking evidence, not a public-Internet or
multi-host/TLS/security acceptance test. An environment without a routed
non-loopback interface fails the test explicitly instead of silently substituting
loopback. It is ignored in the default offline test suite and run explicitly by
the CI network acceptance step:

```sh
cargo test --locked -p replicated-graph --test http_job_process http_jobs_explicit_trusted_network_independent_process -- --ignored --exact
```

Test execution status must be read from exact-source evidence.
