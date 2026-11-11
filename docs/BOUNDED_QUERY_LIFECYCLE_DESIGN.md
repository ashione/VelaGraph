# Bounded fixed-version query lifecycle proposal

Status: research and design rationale for the experimental candidate.
The implemented API is documented in [query jobs](MANAGED_QUERY_JOBS.md);
validation is ongoing and this design is not an acceptance report.
Base: main `31c52eaa438fafa9ba3f1d067a4b0c677a379932`.
Related: #110, #111, #174, #176. Independent of #178/#181.

## Problem and the compatibility boundary

An immutable version and a live handle are different guarantees. Later writes
must not change an admitted query's version, ordering, values or certification.
They do not imply that a client can retain resources forever. Expiry must remain
an explicit failure, never an excuse to refresh V or return incomplete success.

There are currently three different lifetimes:

1. Old `OpenSession` materializes a cohort snapshot in the gateway. Its registry
   `Retained.expires` and underlying `d2_raft::cohort_reads::SnapshotSession.expires`
   are both absolute, at most 60 seconds. `GraphSnapshot::store` propagates the
   latter deadline into each FrozenNode. Changing only the registry is not renewal.
2. `OpenLocalQuery` synchronously admits and captures one certified V on fixed
   nodes, executes fragments and seals every result page before returning Ready.
   The accepted connection deadline also limits execution independently of
   the session TTL. Gateway and graph RPC configurations permit at most 60
   seconds; five-second fixture configurations and the five-second fixed-open
   acquisition limit are not a universal five-second fragment deadline.
   The gateway then closes node descriptors. Ready retains
   the result, not a distributed graph snapshot.
3. `FetchLocalQuery` only replays immutable gateway data. Renewing this result
   would extend download time; it would not enable a long-running traversal.

The existing original-pin regression is an old `OpenSession` contract test.
Keep its request, assertions, deadlines and expected original token unchanged.
A new renewable API must not be called a fix for that test, nor used to replace
it. A failure at its original 60-second deadline remains a separate gate with
its original evidence. Likewise, raising only `MAX_TTL`, refreshing on Read, or
reopening V after expiry is not this feature.

## Relevant mature-system mechanisms

- [Elasticsearch PIT](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-open-point-in-time)
  separates an immutable search view from keep-alive extension. Retaining old
  segments consumes disk, file handles and, for updates/deletes, additional
  tracking memory. This motivates explicit resource ownership and close, not
  unlimited VelaGraph leases.
- [MongoDB explicit sessions](https://www.mongodb.com/docs/manual/reference/method/cursor.nocursortimeout/)
  distinguish cursor options from session expiry. Explicit refresh can keep a
  session alive, while session expiry can kill operations and cursors. A cursor
  flag alone cannot solve a parent-session lifetime constraint.
- [PostgreSQL client connection timeouts](https://www.postgresql.org/docs/18/runtime-config-client.html)
  distinguish statement, transaction and idle-in-transaction timeouts. Open
  transactions also interfere with vacuum reclamation. Separate timeout purposes
  are a better model than one enlarged global deadline.
- [CockroachDB protected timestamp RFC](https://github.com/cockroachdb/cockroach/blob/master/docs/RFCS/20191009_gc_protected_timestamps.md)
  describes protecting historical versions against GC and rejecting reads below
  a GC threshold. It is a design source, not evidence that VelaGraph implements
  protected timestamps or that its proposed protocol is correct.

Our conclusion from these sources is a query-owned, finite lease with separate
RPC, acquisition, execution and result-retention deadlines. The chosen numerical
bounds below are proposed engineering defaults, not imported guarantees or
measured capacity claims.

## Recommended delivery target

Implement a new opt-in data-local fixed-V job, reusing the existing official AST,
fixed fragment engine. Main at this base executes serially; the bounded parallel
scheduler is separate pending work in #178/#181 and must be integrated only
through its verified candidate, with joint exact-head validation. Lifecycle must
not implement a competing scheduler or import unpublished changes. Do not first build a
new long-lived gateway-materialized query engine. An old materialized-session
renewal API can be considered separately if explicitly needed; it is not a
prerequisite and is not included here.

Reuse preserves the currently admitted data-local subset: nonempty explicit
V(ids), out/in and terminal path/count. Scans, repeat, values, limit and other
unsupported shapes still reject before admission. Generic means no application
or graph-specific branch; it does not claim arbitrary Gremlin coverage.

Proposed operations, names subject to API review:

- `StartLocalQueryJob`: exact query, partition, optional V, page size, bounded
  lease policy and client-generated operation ID. Reserves a job before work and
  returns an Accepted handle. Accepted contains no rows and is not a certificate.
- `GetLocalQueryJob`: reads Running, Ready or terminal failure. Polling is not
  renewal. Ready includes the original certified V and complete-result metadata.
- `RenewLocalQueryJob`: explicit lease generation advance. It never opens nodes,
  changes V, replays a fragment or resets a work ledger.
- `FetchLocalQueryJob`: available only in Ready, with exact query/page-size/handle
  binding and immutable replay tokens. Fetch is not renewal.
- `CancelLocalQueryJob`: terminal local cancellation and bounded remote cleanup.

Use a separately typed job handle. Older binaries reject these additive variants.
Leave existing OpenSession/Traverse/Page/Close and OpenLocalQuery/Fetch/Close wire
representations, semantics and limits untouched. No automatic translation.

## Time and renewal contract

Proposed first profile:

- Every control and fragment RPC is capped by its configured transport deadline
  and the existing 60-second protocol maximum. Propose five seconds per RPC for
  the first job profile, explicitly a new profile choice, not an old invariant.
- Capture keeps the current lower-layer total acquisition ceiling of 30 seconds,
  shortened by the job execution/lease deadlines and configured acquisition
  budget. Each fixed-open acquisition is capped at five seconds. Renewal never
  restarts acquisition. Acquire all matching actual certificates before executing.
- Idle lease interval: caller chooses 1..60 seconds; fixed for the job.
- Execution deadline: caller chooses at most 120 seconds, measured from Accepted,
  includes capture/queueing/execution/sealing, and never renews.
- Hard lifetime: at most 300 seconds from Accepted, including Ready/page retention.
- Ready may renew the idle lease only up to that same original hard deadline.

The initial practical profile should require enough lease time for bounded
control RPCs; a one-second lease is a deliberate early-expiry test case, not a
promise of useful operation under slow networks. A client should renew well
before its conservative local deadline; renewal after expiry never resurrects.
Monotonic process clocks are local only. Do not transmit an Instant or assume
synchronized wall clocks across nodes.

A job has a random incarnation/token and bounded renewal generation. A renewal
request carries the expected generation. Exactly the next generation may extend
expiry to `min(now + lease_interval, hard_deadline)`. An exact retry of the most
recent renewal returns the same stored grant without extending again. Older,
skipped or conflicting generations fail. Only one last grant is retained, not
an unbounded dedup history. Generation overflow is terminal failure.

On nodes, renewable descriptors are explicitly distinct from the old fixed
session contract. Each stores its original hard cap, lease expiry, generation,
closed/failed state and existing cumulative fragment ledger. Fragment sequence
numbers do not share the renewal sequence and are never reset. Renewal and
expiry/close must linearize on the same lease state; a detached Arc is not
permission to renew an entry removed by Close or cleanup.

The gateway renews all still-needed original nodes before acknowledging a
Running renewal. It conservatively derives node grants from remaining gateway
hard time and RPC send time, never from response receipt. Network uncertainty
may cause earlier failure, never authority to continue past the gateway cap.
Each node additionally enforces a finite local hard cap from its own admission.
The gateway hard deadline bounds accepted results and new dispatch, not a
synchronized physical release instant on every machine. Delayed in-flight opens
and blocking workers can retain resources after it; local node caps, RPC limits
and worker ownership bound normal cleanup. An OS-stalled worker is not covered
by a wall-clock release guarantee, and cannot surrender its permit early.
A late or missing grant fails the job only while its node reads remain required
(Running); completion must recheck phase, old committed expiry, hard deadline and
applicable execution deadline under the job lock. After the last-read transition
to Sealing/Ready, node grants are no longer needed and only the original pending
gateway grant may commit. Terminal states discard it. Partially renewed nodes get bounded
Close attempts and otherwise expire. No new replica, replacement token, changed
proof or fallback to current graph is allowed. Lost renewal responses cannot
cause fresh capture or budget replenishment.

While a job is Running, the gateway owns heartbeat coordination; activity alone
must not hide a departed client. Distinguish the gateway-committed lease from the
client-observed acknowledgement: a lost response cannot make those identical.
One bounded pending renewal round may provisionally extend node grants to its
proposed expiry. Commit the gateway generation/grant only after every required
node grant succeeds and before the old committed lease expires; otherwise mark
the job Failed/Expired and close all nodes. A committed grant survives ACK loss
and exact replay returns it. The client keeps its conservative old deadline
until it observes a successful grant. Pending/committed grants never exceed the
original hard cap and cannot be stacked. Automatic node heartbeats stay within
the gateway-committed client lease, except for that single pending round.
Independent client renewal is necessary while polling or receiving no
intermediate data. Ready stops all node renewals and releases captured graph
resources; only the result reservation remains.

## State machine, publication and cancellation

`Accepted -> Capturing -> Running -> Sealing -> Ready` is the success path.
Ready is nonterminal for retention: it can still enter Cancelled or Expired.
Every nonterminal state can instead enter Failed, Cancelled or Expired. No
terminal state transitions back to Running. A bounded terminal record can remain
until the original hard deadline so repeated Cancel/Get are stable; it must
retain no graph payload. The job registry and idempotency record together remain
under the existing shared session-slot bound.

Serialize phase transitions and renewal commit in one short job-state lock,
never held across RPCs. Accepted/Capturing reject renewal with a typed busy
state; acquisition must finish within its original initial lease and acquisition
deadline. There is no partially captured node-set renewal protocol. Running
renewal snapshots the complete original node
set and phase generation, then validates both before commit. Sealing begins
atomically after the last required read; it no longer requires node renewal.
A renewal completing after that transition revalidates phase and may commit only
the gateway grant, without treating a now-unneeded node failure as a query
failure. Failed, Cancelled and Expired reject renewal; Ready renews result
retention only. Duplicate pending requests must not launch another round. A
replayed expired grant is not usable authority and returns the current terminal
state rather than an apparently successful live lease.

Fetch acquires an Arc only while Ready is live, and rechecks cancellation/expiry
after encoding and before sending. Cancellation removes future reachability;
an in-flight owner keeps its reservation until dropped. Bytes already sent
cannot be recalled. Releasing graph ownership at Sealing/Ready does not assert
that every remote Close was acknowledged.

Reserve admission before scheduling. Jobs live in a gateway-owned tracked task
set, not untracked spawned tasks. A lost Start reply is addressed only by the
same operation ID with byte-equivalent query/policy/binding; it returns the same
job while its bounded record exists. Changed reuse fails Identity. Once forgotten,
a repeated ID is not a durable exactly-once guarantee. The API must state this
explicitly; clients cannot blindly restart an unknown query expecting old V.

Start acknowledgement failure does not create an immortal job: client lease
expiry stops it. Disconnecting an individual Get or Fetch does not implicitly
cancel a valid job. Explicit Cancel, hard expiry and gateway shutdown do. Cancel
marks state terminal before cleanup, prevents Ready publication and is idempotent
for a known handle. Shutdown cancels and joins tracked async tasks; stalled blocking workers retain
their permits and remain outside a bounded shutdown-completion guarantee. Node/worker restart
invalidates process-local tokens; no durable resume is promised.

The complete result must pass existing cumulative work/byte/row checks and every
page-envelope check before the atomic Ready transition. Recheck lease, execution
deadline and cancellation at that transition and after expensive encoding.
Never expose rows from Running or Sealing. Once Ready, node failure does not
invalidate already sealed data. Before the last required read, node failure is
whole-query failure. Preserve this distinction in tests and documentation.

Blocking workers cannot be forcibly aborted safely. They retain graph views,
execution/memory permits and their bounded deadline until exit. Cancellation
checks must occur in existing bounded loops and before publication. Close removes
registry reachability immediately but never releases a permit while a worker
still owns the corresponding memory. The existing limits remain capacity limits;
renewal does not reserve fresh capacity or reset counters.

## Resource and history safety

Reuse existing shared gateway reservations, node session/byte pools and execution
admission. Job and terminal metadata count against those reservations. Do not
create an extra pool that permits old and new API users to double total exposure.
Keep existing capture, full-result, page, frame, fragment and cumulative ledger
limits. A longer time window buys time, not more rows, bytes or work.

Bound every auxiliary operation: at most eight original members per renewal,
one retained last renewal grant per job/node, one tracked task per admitted job,
fixed-size terminal metadata, no unbounded heartbeat queue and no implicit retry
loop. Allow only one renewal round per job at a time and at most 1,024 renewal
attempts per gateway job and per node descriptor, including replay attempts.
Exhaustion fails the job rather than refreshing the counter. Reserve access to
cancellation independently of renewal admission; quantify control-RPC contention
with writes during validation rather than promising starvation freedom. Record
control cost separately from fragment cost and never claim it is free.

Current views are owned materializations and source history is retained under
existing finite history budgets. This proposal does not implement historical GC,
MVCC indexing, purge, snapshot install or durable read protection. It neither
depends on nor reimplements #124. A future GC design would require a separately
reviewed protection protocol before claiming that a renewable handle protects
uncaptured historical storage. Current write-history exhaustion still fails
explicitly and is not fixed by reader leases.

## Required implementation and acceptance work

1. Add one monotonic bounded-lease state machine and deterministic clock-driven
   tests for exact idle/hard boundaries, renewal replay, generations, close races,
   failed/expired non-resurrection and no budget reset. Test exact state, not
   timing tolerance assertions.
2. Add separately typed renewable node open/renew/close operations. Preserve
   exact authority/cohort/V/epoch/incarnation/target binding, shared pools and
   retained certificates. Exercise partial open, partial renewal, response loss,
   worker cancellation and last-owner permit release.
3. Add gateway-owned job scheduling and the additive public protocol. Reuse the
   distributed executor and page sealing. Dispatch each fragment with the minimum
   of configured RPC limit, 60-second protocol cap, job execution remaining and
   currently granted node-lease remaining; passing a 120-second execution end
   directly to the old fragment helper exceeds its protocol cap. Execution owns
   mutable fragment sequences; heartbeat coordination needs separate synchronized
   lease cells rather than a mutex around the entire borrowed session map.
   A renewable node deadline must be
   sampled for each bounded fragment; extending a registry timestamp alone is
   insufficient if FrozenNode/worker deadlines still refer to an old value.
4. Add real independent-process TCP tests with genuine concurrent writes and a
   retained job lasting beyond the original 60-second TTL. Check original V,
   exact ordered qualified rows and immutable page replay using a fixture-owned
   oracle. Exercise updates, edge replacement/deletion, cycles, parallel edges,
   repeated input IDs and cross-shard equal local IDs. No production query
   rewriting, label/ID branches, fake source reads or TTL inflation.
5. Prove a Running query rather than only a Ready download survives: use bounded
   event-controlled fragment barriers to keep required reads outstanding across
   renewal, and separately run an unhooked concurrent workload. Report deliberate
   test suspension separately from actual compute/performance evidence.
6. Fault matrix: coordinator loss after admission, participant loss before/after
   last required read, renewal ACK loss, partial node renewal, gateway restart,
   Cancel during capture/fragment/sealing, idle expiry, hard expiry despite
   repeated renewal and abandoned client. No partial successful pages on failure.
7. Assert permit and retained-byte counts return to baseline after cleanup and
   after blocked workers finish. Show a replacement request cannot claim those
   permits prematurely. Verify progress/fairness of concurrent writes without
   converting timing observations into an availability guarantee.
8. Run the unchanged original-pin test independently, full required fmt/tests/
   clippy/diff checks, independent review and fresh exact-head CI including
   observed CoreHR and release1025. A failure in the old API remains a failure;
   a new API's passing test does not waive it.

## Deliberately not claimed

This design is not implementation, a fix for the original-pin failure, successful
long-query evidence, larger capture capacity, infinite history, a production
resource/RSS bound, dynamic routing, multi-host HA or completion of #110/#111.
Existing 60-second APIs remain useful short-lived contracts. The implementation
must land as a complete opt-in lifecycle, not an unused lease helper or a Ready
cache extension advertised as distributed long-query execution.
