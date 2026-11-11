# Bounded data-local fixed-version query jobs

Status: experimental opt-in implementation under validation. This is a separate
lifecycle from the original materialized `OpenSession` and synchronous
`OpenLocalQuery` APIs. Their absolute 60-second TTLs and tests are unchanged.
This feature does not fix or waive the existing original-pin acceptance failure.

## API and consistency

Use a gateway configured with explicit managed graph routes. The additive
operations are `StartLocalQueryJob`, `GetLocalQueryJob`, `RenewLocalQueryJob`,
`FetchLocalQueryJob` and `CancelLocalQueryJob`.

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
lifecycle, not a claim of arbitrary Gremlin support. SQL and observed-query APIs
are unchanged.

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
