# Managed HTTP preview adapter

This is an **experimental library entrypoint**, not the v0.0.1 release or the
final protobuf/HTTP contract. It uses the existing certified managed gateway
business implementation. A [typed public gRPC adapter](PUBLIC_GRPC_SERVICE.md) and
[standalone service host](PUBLIC_SERVICE_CLI.md) are separate slices sharing this
managed business path. A final stable dual-protocol contract and clean cluster
startup packaging remain work under [#218](https://github.com/ashione/VelaGraph/issues/218). See the separate
[shared service design](PUBLIC_SERVICE_DESIGN.md).

## Host and lifecycle

After constructing an existing validated live `managed_gateway::Gateway`, call
`gateway.bind_http("127.0.0.1:0".parse()?, deadline).await`. Use its `local_addr()`
and keep both gateway and returned `HttpService` alive. Call `shutdown().await`
for orderly cleanup. Dropping either server signals shutdown. Any non-loopback
bind is rejected; there is no implicit public bind, TLS, authentication or
production exposure. Loopback alone is not an authentication or browser-attack
boundary; do not expose this preview to untrusted local clients. Host/Origin
policy and credentials belong to the later hardened service entry. Restored/offline nodes cannot bypass their existing startup
guards through this adapter. No public restore or readiness override is added.

`GET /preview1/service` describes a live managed gateway and explicitly reports
`restore_supported=false`, `production_ready=false`. This is **not** a global
restore READY certificate, availability promise or proof that the next request
can obtain a certified view. Every business operation retains its existing
identity/certification checks. A stopping gateway rejects new work.

Both HTTP and legacy managed TCP share the same eight admission slots, retained
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
HTTP page is delivered, including sessions originally opened via managed TCP.
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

Focused tests exercise real loopback HTTP with the same managed gateway as TCP,
strict codecs and delivery/admission ownership. These are not multi-machine
benchmarks, global restore tests or release-gate evidence. The separate [dual-protocol process test](PUBLIC_SERVICE_CLI.md) adds actual
independent processes and cross-protocol semantics. Clean startup packaging and exact CI/release checks remain required before
v0.0.1; the finite preview feature/compatibility matrix must be explicit. Test results must be read from
the exact submitted commit rather than inferred from this document.
