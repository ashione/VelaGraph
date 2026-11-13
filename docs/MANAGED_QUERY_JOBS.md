# Bounded data-local fixed-version query jobs

Status: experimental opt-in implementation under validation. This is a separate
lifecycle from the original materialized `OpenSession` and synchronous
`OpenLocalQuery` APIs. Their absolute 60-second TTLs and tests are unchanged.
This feature does not fix or waive the existing original-pin acceptance failure.

## API and consistency

Use a gateway configured with explicit managed graph routes. The additive
operations are `StartLocalQueryJob`, `GetLocalQueryJob`, `RenewLocalQueryJob`,
`FetchLocalQueryJob` and `CancelLocalQueryJob`.

Jobs use only `Parity2MiBV1`: 10,000 captured rows, 2 MiB of payload and 100,000
D1 replay operations. `StartLocalQueryJob` has no capture-profile selector. The
separate 8 MiB opt-in belongs only to `OpenLocalQueryWithProfile`, not this
lifecycle. Both renewable node-open validation and the session entrypoint reject
an expanded capture profile before shared-pool admission, even when its requested
bytes fit 2 MiB. Renewal cannot change a descriptor's profile.

Start supplies a nonzero 16-byte operation ID, optional version, exact Query,
page size and JobPolicy. It reserves the same shared session/memory and execution
admission used by existing APIs before scheduling a tracked task. It returns an
opaque JobHandle, not rows or a read certificate. Repeating the same operation ID
and exact arguments returns the same handle while that record is retained;
changed arguments fail Identity. Once the record expires, operation-ID history
is forgotten. This is not durable exactly-once execution or restart recovery.

The worker selects one admitted V, captures each original participant once,
compares every actual retained proof with the original complete certificate,
and executes the existing data-local fragment engine. It never refreshes V,
recaptures a node, changes endpoint or retries a failed fragment with a fresh
ledger. Later managed writes can change the graph but cannot change these views.
Required failures invalidate the whole query; no intermediate rows are published.

The admitted grammar remains nonempty explicit `g.V(ids)`, up to three out/in
steps, and optional terminal path/count. Ordered bags, qualified identities,
parallel edges and original endpoint/reverse checks are preserved. Scans, repeat,
values, limit and other unsupported shapes still reject. This is a generic
lifecycle, not a claim of arbitrary Gremlin support. Observed-query APIs are
unchanged; the legacy SQL engine is removed.

The gateway enters Capturing, Running, Sealing and Ready. Ready is published only
after the complete bounded result and every possible page envelope pass checks.
Get returns phase information but no partial rows. Ready identifies the original
V and total. Fetch repeats the exact query, page size and job handle, and uses an
opaque cursor or None for the first page. Replays return unchanged pages. Node
resources are no longer logically required after the last read; node loss after
that point cannot change a sealed result. Nodes may still retain resources until
a bounded Close succeeds, a worker exits or its local lease expires.

## Four separate time bounds

JobPolicy requires:

- Idle lease: 1,000 through 60,000 milliseconds.
- Execution: positive, at most 120,000 milliseconds.
- Hard lifetime: positive, at most 300,000 milliseconds, and no shorter than idle
  or execution.

Execution and hard lifetime start at gateway admission, never at completion of
capture or renewal. Execution includes capture, traversal and sealing. Ready
retention may continue beyond execution but never beyond original hard lifetime.
One-second idle leases deliberately expire quickly and are not a guarantee of
useful work on a slow deployment.

Control/fragment RPCs use at most the smaller of configured gateway timeout and
five seconds for this profile. Existing gateway and generic graph transport
configuration permit up to 60 seconds; the old fixed-open acquisition cap is
five seconds. Those are different contracts. The job's acquisition also keeps
its original lower-layer 30-second total ceiling and remaining initial lease.
Acquisition is not renewed. The new executor samples current conservative leases
for each RPC rather than passing a 120-second duration to a 60-second protocol.
An in-flight fragment retains its sampled deadline; renewal does not retry or
extend a timed-out fragment.

## Explicit renewal and loss handling

Get and Fetch never renew. Accepted/Capturing renewal returns Busy. For a live
Running, Sealing or Ready job, Renew supplies the last observed generation.
Only one pending round exists. Running obtains grants from all still-required
original nodes before committing the next gateway generation. A duplicate pending
request does not start another round. An exact replay of the last committed
request preserves its generation and absolute server expiry; the public
remaining-milliseconds observation decreases with time and is not byte-identical.
Clients anchor each observation to that RPC's send time, not receipt time.

The old committed lease must still be live when the new grant commits. A pending
renewal is not permission to resurrect an expired session. The original proposed
expiry is not recomputed after I/O. A lost reply can be replayed while still live.
Each gateway job and node descriptor accepts at most 1,024 renewal attempts,
including replays. Generation overflow and attempt exhaustion fail closed.

Gateway-committed grants differ from client-observed acknowledgments. One pending
round can leave partially renewed nodes when the gateway fails. Their finite
local deadlines are the cleanup backstop, not a claim of atomic distributed
rollback. A renewal finishing after the last-read transition rechecks phase under
the job lock: Sealing/Ready no longer require node grants. Cancellation, expiry
and failure always win over a delayed completion.

Node renewal uses a separately typed opt-in open, exact descriptor identity,
monotonic generation, finite local hard lifetime at most 120 seconds and an idle
grant at most 60 seconds. Its stable `granted_millis` replay is anchored to the
original node-request send time. This differs from the public gateway's fresh
remaining-time observation. No process-local Instant is sent over the wire.
Node hard deadlines start at node admission; they are not synchronized absolute
gateway deadlines. Gateway hard time limits authorized execution/publication,
not a simultaneous physical memory-release instant on every host.

## Cancel, shutdown and resource ownership

Cancel makes the job terminal before cleanup and hides retained result data.
Known cancelled handles remain identifiable until their original hard lifetime.
Gateway shutdown cancels tracked jobs and joins their async tasks. Connection
loss during a poll does not silently cancel a job. Abandonment without renewal
expires it. Restart invalidates every handle and cursor; no durable resume exists.

Original four shared gateway session reservations, the shared coordinator gate,
node slots/bytes and all row/work/byte/frame limits remain in force. Renewal never
replenishes fragment counters or reserves fresh capacity. The first implementation
conservatively retains the whole gateway reservation for terminal metadata until
the original hard deadline. Consequently **four cancelled jobs can block both old
and new session admission for up to their remaining hard lifetimes**, even after
payloads and node views have been released. Cancel does not promise immediate
reuse of that slot. This avoids a separate unbounded tombstone/dedup registry.

Blocking workers and in-flight page encoders retain their reservation ownership
until they exit. Removing a registry entry cannot release memory permits early.
Close/expiry are checked before publishing and sending; bytes already transmitted
cannot be recalled. No guarantee bounds shutdown completion for an OS-stalled
blocking worker, and retained permits must not be reassigned in that case.

Capture remains 10,000 rows / 2 MiB / 100,000 replay operations. Full results stay
within 10,000 rows / 1 MiB and every full page envelope must fit 1 MiB. Logical
reservation sizes are not total RSS bounds. Source history remains fully retained
under existing finite history budgets. This feature adds no MVCC index, GC,
purge, snapshot installation or #124 dependency. Reader renewal cannot solve
write-history exhaustion or make unlimited historical versions available.

## Validation boundaries

The companion deterministic state and node tests cover exact idle/hard boundaries,
replay, conflicting generations, fencing, cancellation/last-read race orderings,
cumulative ledgers and last-owner permit retention. The independent-process test
uses an explicitly labelled test-only transport proxy: 12 repeated roots branch
across two shards over two hops, producing 24 ordered paths and 96 required
fragments. Each fragment is delayed one second while genuine TCP managed writes
update properties and delete original edges. Shared proxy observations require
actual overlapping traffic on distinct shards. Each RPC remains separately
bounded and the query remains Running beyond the old 60-second lifetime.
The oracle is a fixture-owned ordered qualified path vector, not production
execution output. An undelayed control is also exercised. The long test runs in
its own automatically discovered integration-test target, using the same shared
child-process fixture. This keeps its client admission slots and long fixture
lock hold separate from legacy tests without changing their deadlines or
assertions.

This is functional lifecycle evidence under injected network latency, not a
compute benchmark, throughput improvement, full-scale managed CoreHR result or
multi-host availability proof. The implementation reuses main
`8d906126`'s generic parallel scheduler from #178/#181. It requires a combined
exact-candidate test run, not evidence borrowed from either original branch.

See [design and official-system research](BOUNDED_QUERY_LIFECYCLE_DESIGN.md).
All exact-head regression and CI outcomes, including failures, must be recorded
before acceptance. New API success never substitutes for the unchanged original
pin, observed CoreHR or release1025 gates.

## Planned typed public job service (#268)

**Design only, not an available API.** This section records the migration target
against main `d9399e7b`. It neither expands the lifecycle above nor retires the
legacy managed gateway. The related fixed-session work (#266 / #267) is separate:
materialized fixed sessions, synchronous local query sessions and jobs must not
share interchangeable tokens. #218 remains the transport umbrella.

### Contract and adapter boundary

Add `ManagedQueryJobService` to the existing
`velagraph.public.managed.v1` endpoint. Reuse `State`,
`Invocation::begin/live/delivery_end`, `Handler::run`, admission, workers,
executor and error mapping. No new listener, executor or transparent retry layer.
Use the same typed Query/Page/Context vocabulary, but a distinct JobHandle with
16-byte incarnation and 32-byte token; neither is a credential.

| Proposed RPC | Existing operation | Request and response contract |
| --- | --- | --- |
| Start | StartLocalQueryJob | Nonzero 16-byte operation ID, optional exact version (absence means latest at the worker's read-cut admission, not at Start RPC or job-slot admission; explicit zero remains exact zero), Query, page size, required JobPolicy. Return operation ID and handle only. |
| Get | GetLocalQueryJob | Exact handle; return handle and phase/status. No partial rows, implicit renewal or mutation of selected V. |
| Renew | RenewLocalQueryJob | Exact handle and expected generation; return handle, generation and remaining milliseconds. Remaining time is anchored at this call's send time, not its receive time. |
| Fetch | FetchLocalQueryJob | Handle, original Query/page size and optional cursor. Return those bindings, fixed V and complete immutable page. Absent cursor is first page; malformed present cursor is not absence. |
| Cancel | CancelLocalQueryJob | Exact handle; return terminal status. A successful cancellation is not immediate physical resource release. |

JobPolicy carries idle/execution/lifetime milliseconds separately from Context's
control-call deadline. Start replay compares the decoded domain arguments, not
protobuf bytes. Existing input validation precedes replay-key comparison: changed
valid arguments return Identity, while malformed queries, page sizes or policies
retain their original validation errors. Do not change this error precedence. Do not expose minimum-version or expanded-profile selectors that
the existing job operation does not support.

Status carries an explicit phase enum, optional V/total and optional typed domain
failure. Ready requires both V and total and no failure; active phases expose
neither V, total nor failure. Terminal results follow the existing
`jobs::valid_status` rules. RPC failure and a successfully retrieved failed-job
status are different outcomes. Preserve domain failure distinctions, including
unsupported scope, rather than silently collapsing them merely because the
existing public RPC status mapper combines some error categories.

### Ownership and bounds that must survive transport replacement

- Start's admitted task belongs to the gateway. Unlike a new synchronous session
  open, a failed response delivery must not remove it. Recovery from a lost Start
  response uses the same operation ID and complete arguments while its record is
  retained. A restart invalidates the handle; durable job recovery is out of scope.
- A dropped Renew must execute the existing pending-round cleanup guard. Running
  requires grants from all still-needed original descriptors; Capturing is Busy.
  Replaying the last committed renewal preserves expiry/generation while remaining
  time decreases. Neither delayed completion nor retry resurrects expired work.
- Fetch retains the job reservation through encoding/delivery and checks Ready
  and deadline again. Cancellation/expiry wins over later publication; bytes
  already sent cannot be recalled. Get or Fetch never extends the idle lease.
- Keep the original control-call cap, idle/execution/hard deadlines, parity
  capture profile, shared pools and accumulated work budgets. Terminal metadata
  retains its shared slot until hard lifetime, as described above.
- Extend the allocation-free schema-aware protobuf prescan for all new requests
  and responses. Reject unknown enum values, missing required messages, duplicate
  fields/oneofs, oversized collections and wrong token widths before unsafe
  allocation or domain execution. Keep both protobuf wire limits and original
  logical page-envelope preflight; smaller protobuf encoding is not permission
  to accept results rejected by existing budgets.

### Required evidence matrix

| Gate | Evidence needed on the implementation candidate |
| --- | --- |
| Typed conversion | Round trips for every phase, exact zero versus absent V, empty versus absent cursor, all failure variants, uint64 generation limits; malformed/duplicate/unknown fields rejected. |
| Admission and replay | Same-ID/same-arguments Start returns same handle; changed valid query, page size, policy or V returns Identity; invalid inputs retain validation-first errors; unsupported grammar and full shared pool fail without creating work. Same-ID replay of retained Cancelled/Failed jobs returns the original handle; after hard-lifetime eviction the ID may admit new work, not durable exactly-once execution. |
| Lost delivery | Drop Start response, replay original request and observe one job. Disconnect Get/Fetch without cancelling the job. Disconnect Renew cannot strand a pending round. |
| Fixed result | Independent small non-HR oracle with qualified IDs, parallel edges and ordered bags; later writes/deletes do not alter V or page replay; no rows before complete sealing. |
| Renewal races | Capturing Busy; committed replay does not extend expiry; concurrent pending renewal, wrong generation, node-grant failure, last-read transition, expiry and cancellation use original linearization. |
| Cancellation and shutdown | Cancel then Fetch rejected; cancelled/failed/expired status remains identifiable until hard expiry; reservation remains held while worker/encoder owns it; shutdown joins owned work. |
| Fencing and recovery | Wrong incarnation/token/query/cursor/page size rejected; restart invalidates handles. No endpoint replacement, fresh-V recapture or automatic new operation ID after uncertainty. |
| Resource parity | Exact/over-boundary logical and protobuf envelopes, original capture/result/work limits, simultaneous old/new API admission; no test timeout or assertion weakening. |
| Process and regression | Actual public gRPC client/server lifecycle using managed graph routes; compare typed results/errors with legacy semantics, not only codec mocks. Independent review and exact-head CI/CoreHR, retaining original pin and release1025 gates. |

Existing deterministic tests in `managed_gateway_jobs.rs` and
`managed_query_lifecycle.rs`, plus `tests/query_job_lifecycle.rs` and its
support fixture, are the semantic base. Extend or reuse them rather than creating
another query engine or accepting observed-query evidence as managed-job proof.
Transport-only performance claims require separate matched measurements.

### Delivery sequence

1. Review this matrix independently of #267; design approval is not implementation
   completion. Keep #268 open and linked to its implementation PR.
2. After #267 integrates, add schema/generated bindings, bounded codec, adapter and
   lifecycle tests on current main. This avoids competing edits to shared public
   proto/generated/handler files while preserving a single endpoint.
3. Record exact candidate evidence and update this document plus public transport
   inventories. Only then assess legacy retirement in its own verified slice;
   do not remove unconverted operations or declare #218 complete.
