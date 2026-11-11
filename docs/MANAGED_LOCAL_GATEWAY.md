# Query-bound data-local fixed-V gateway experiment

This additive path retains graph data on explicitly selected participant
processes. Its original M1 operation retains the parity capture budget; an M2
operation selects a named query-only capture profile. Neither changes history
or write budgets or enables the older observed GlobalCommittedSnapshot option.
It is not indexed MVCC,
full managed CoreHR capacity, failover, TLS/authentication or a durable read lease.

## Trusted configuration and protocol

`Gateway::bind_remote_local_queries` requires the original `RemoteConfig` plus
one `LocalGraphRoute` per member in canonical cohort order. Each route supplies
an explicit graph endpoint, graph group/node identity and the **complete** managed
identity matching that member's configured Client. Role, target shard/epoch/mode,
authority, entire cohort including capacity profile and partition=shard all match.
Endpoints are distinct and cannot be supplied by queries or inferred from ports.
The service is plaintext and must be isolated to trusted callers and peers.
Tokens and terminal-prefix evidence are not authentication credentials or
cryptographic proof against a malicious peer.

The additive operations are:

- `OpenLocalQuery { version, ttl_millis, query, page_size }`: execute and validate
  the entire immutable result under the original 2 MiB policy before publishing
  its `LocalSession` and first page.
- `OpenLocalQueryWithProfile { profile, version, ttl_millis, query, page_size }`:
  explicitly select `Parity2MiBV1` or `LocalRead8MiBV1`. Its matching reply echoes
  the exact profile as well as session, query, page size and first page.
- `FetchLocalQuery { session, query, cursor, page_size }`: replay any known page,
  including the first page (`cursor: None`), without opening or contacting nodes.
- `CloseLocalQuery`: close the retained result. Local sessions are strongly typed
  separately from old materialized sessions and observed node descriptors.

The official Gremlin AST admits explicit nonempty `g.V(ids)`, up to three `out`
or `in` hops, and optional terminal `path` or `count`. Other syntax fails before
any node session allocation. The existing shared executor preserves argument
order, duplicate roots, edge order, qualified paths, canonical/reverse/endpoint
validation and default first-100 non-count semantics. This is a narrower entrypoint
than old managed Traverse, not a silent substitute. Rows retain `Unverified`
adjacency completeness, separately from their certified V.

## Admission, execution and results

The cohort is at most eight members. Admission selects exactly one V from the
original complete cohort and retains original endpoints, authority, cohort,
complete terminal proof and deadlines. This admitted cut is not itself a read
certificate. Every graph member captures once with its same-lock certified
terminal read after a leader barrier. The gateway receives each actual retained
proof and compares it in full with the original coordinator proof, together with
all descriptor identities. No graph KV is downloaded through this query path.

Before opening any node session, the gateway checks every complete Open request
and worst-case successful reply against the graph RPC's 16 MiB frame boundary.
Fixed-open replies have their own receive branch; old frame limits are unchanged.
Some valid maximal ByteBound histories exceed this transport cap and fail Capacity.

All required fragments run serially over the original immutable descriptors.
Request sequences start at zero and advance exactly once per attempt. Duplicate,
skipped, failed or unknown calls cannot replenish node/global ledgers or cause a
reopen, replica switch or V refresh. Required descriptor access or validation
failure fails the entire query. After the last required access, a node failure
may be unobservable while the gateway seals results. Ready and subsequent Fetch
rely on the gateway's retained result, not live nodes.

An atomic edge replacement is a later certified write. Existing descriptors and
pages retain the original qualified edge records at V; queries opened at the
new version see the replacement, including a change of source owner with the
same numerical edge ID. The legacy managed Traverse executor has its own lazy
path representation. This entrypoint continues to use the distributed executor.

The entire result is at most 10,000 rows and 1 MiB; page size is 1..=1000. Every
complete page envelope, including Binding, original query, V/session, cursor and
maximal request-id encoding, must fit 1 MiB before Ready. Cursors are independent
random 256-bit tokens scoped to the exact result. Close/expiry are checked at
lookup, page creation, after encoding and before send; bytes whose transmission
has already begun cannot be revoked. Every replay preserves the
original query, size, V and expiry. A lost Open reply is not a Fetch retry of a
known session; no automatic Open retry or durable resume is provided.

## Resource and deadline boundaries

Aggregate captured rows/bytes/D1 operations are 10,000 / 2 MiB / 100,000 for
`Parity2MiBV1`, and 10,000 / 8 MiB / 100,000 for `LocalRead8MiBV1`. Only captured
payload bytes change. Old OpenLocalQuery, materialized sessions and Submit
retain the original 2 MiB limit.
Each member receives the remaining capture budget, which also bounds its replay
intermediate state. Final retained capture counters are not peak measurements.
The node's observed and fixed views share the existing eight-slot/128 MiB pool.
The gateway's old/new retained sessions share its existing four reservations;
workers continue owning reservations after close/expiry/cancellation. Fixed opens
also share the existing two-slot observed coordinator execution gate, including
certification, fragments and sealing. Old gateway operation concurrency is unchanged.

Proofs, raw retained history, serde clones, fragment scratch and full certification
are additional bounded costs, not included in D1 replay counts or the gateway's
payload reservation. Active terminal certification may reconstruct all retained
raw history and a temporary Plan-bearing Journal even for a small old V. Existing
profile history byte/event limits remain in force. Synchronous work is not
preempted by async timeout; checks after completion prevent an expired success.
No CPU/lock-time/RSS reduction is claimed. Measurements not present in an evidence
report are unknown, not zero.

TTL is at most 60 seconds from initial admission, with the original acquisition
window and query deadline throughout. No spool, global scan, history GC, purge,
install, durable format change or dependency on #124 is introduced. Write,
bootstrap and history capacity remain independent blockers for #110/#111.

## Explicit profile identity and complete frames

The selected profile is part of each exact node descriptor and the immutable
gateway result's admission policy. The new Open client rejects another profile,
a stripped profile or a legacy reply in its place. The old Open accepts only the
old reply. Node profile/proof mismatch fails the whole query and closes any safe,
known descriptor. The default parity profile is omitted from node JSON, retaining
the original wire shape. Missing profile never implies expanded capacity. Unknown
or null profile tags fail; older strict peers reject the new field/operation.
There is no compatibility fallback or inferred profile based on requested bytes.

Public LocalSession fields are unchanged. Fetch and Close do not choose another
policy or recapture; their opaque token refers to one exact retained session and
result. This query policy is distinct from persisted cohort CapacityProfile and
is not a credential or stronger certificate. New fields count toward complete
16 MiB node envelopes and the 1 MiB gateway/page envelopes. Preflight validates
the actual longer profile Open reply, not a shorter legacy reply. The new opcode
uses the same five-second end-to-end query deadline and send/expiry cleanup.

## History, CPU and memory accounting

Both query policies retain full canonical history certification. Each cohort's
16 MiB history reservation, profile event/transaction bounds, participant intents
and D1 retained-write limits are unchanged. ByteBound16MiBV1 has structural maxima
of 239,674 events and 81,840 transactions, not achievable-count promises. Legacy
count limits still apply to legacy cohorts. A small old V still authenticates
later retained history; there is no cached boolean/hash proof shortcut.

The 100,000 D1 replay cap excludes certification and serialization work. Rows
bound map cardinality; copied bytes bound graph payload and replay peaks.
Fragment execution retains 10,000 work/64 MiB cumulative logical limits. The
shared node pool remains eight slots/128 MiB logical payload, with four gateway
reservations and two coordinator execution permits. The 16 MiB full-frame cap
does not cap all simultaneous history/proof clones. Deadline checks cannot
preempt synchronous certification. No CPU-millisecond, lock-time, certification
scratch-peak or total-RSS guarantee is supplied. Unknown measurements remain
unknown, not zero; 128 MiB must not be described as a total RSS cap.

## Capacity evidence and unchanged write boundaries

An 8 MiB query ceiling is not an 8 MiB supported write lifecycle. Durable gateway
admission archives the original command and exact Plan under the unchanged
16 MiB history bound. Large text workloads can hit that history bound before
their graph payload reaches 2 MiB.

The existing public GraphSnapshot::prepare_vertex and GraphCohort::execute API
also performs managed graph validation and Begin/Prepare/Commit/whole-cohort
visibility. It does not archive an unadmitted Plan for crash resumption: callers
must retain the exact Plan for retry. It is not arbitrary KV import. Capacity
evidence using it must keep the original 2 MiB pre-write snapshot gate and state
that it is a mixed-API workload.

The ordinary `managed_local_capacity` integration target creates four direct
typed 510,000-character records and then one 60,000-character gateway record.
The setup records exceed the separate gateway 64 KiB command/256 KiB durable-Plan
caps and are not claimed to pass Submit. The raw graph moves from 2,040,332 to
2,100,415 bytes at V5, only 3,263 bytes beyond the old boundary. The fixture checks
old-reader and next-write rejection, complete ordered duplicate paths, exact
page replay, old V4 and node stop/reopen. A late final Submit is resolved by Status
and at most one identical external-ID/command retry under its original budget.
All independent old-entrypoint/next-write rejection probes run at the same V5
following the retained-page stop/reopen/replay/close checks. Their repeated full
certification must not spend the unrelated Ready session's unchanged 60-second
lease. Restart changes the test graph endpoints, so a new test gateway is explicitly
configured from verified restarted processes only after the original gateway and
tokens finish replay and Close. No old session is refreshed or redirected. Phase
timings expose that ordering; no probe, assertion or budget is removed, but this
fixture does not cover negative probes interleaved with a live Ready result.
Legacy materialized OpenSession/Submit map remote VisibilityReadLimit to Rejected;
local-query capture maps it to Capacity. The test asserts the underlying limit
without changing those existing errors.

The separate non-ignored target is automatically included by the existing
`cargo test --locked` CI command. It shares exact child-process harness source
with managed_process, but has its own parent process and admission pool. Original
test bodies, parallelism, internal concurrency and budgets are unchanged. Reports
must identify the exact source/executable and newly completed checks. Synthetic
8 MiB/+1 node fixtures prove only capture boundaries, not managed graph loading.
Warm debug observations are not cold/release throughput or multi-host HA evidence.
