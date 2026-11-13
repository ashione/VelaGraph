# RPC-only default storage and routing chain

Tracking: [#256](https://github.com/ashione/VelaGraph/issues/256), part of [#218](https://github.com/ashione/VelaGraph/issues/218).

## What changes

`RemoteStorageNode::new_identified` now uses typed RPC for probe, local metadata,
D1 writes and full paged-snapshot download. Catalog/routing already construct
identified adapters, so their normal path moves together; there is no new
transport selector. Identity-free `new` is removed instead of guessing a server
identity. Explicit group/node configuration is mandatory, including group zero.

`graph-node --graph ADDR` opens one graph gRPC listener. Listener-free `GraphState`
holds shared Raft/state/session/admission state; creating it opens no socket.
The old split-port `--grpc-snapshot` option is rejected. Raft and driver/control
retain their separate typed listeners. Upgrade clients and nodes together.

Single-partition `graph-client ... server-query` uses a typed query service that
calls the original single-partition executor. V1 remains SinglePartition;
V2Observed remains Unverified. This does not turn a fragment/session API into a
new interpreter, global query plan or consistency claim.

## Invariants retained

- Frozen complete local ReadView, not on-demand remote storage. Existing session,
  download byte/row admission, descriptor/cursor validation, ordered pages and
  best-effort close/expiry are shared with the prior implementation.
- A scoped adapter operation retains one absolute deadline through connection,
  encoding, dispatch and completion; connection does not renew that deadline.
- Metadata is still a locally applied epoch check, not a new quorum barrier.
  Candidate routing, ownership fences and historical-view semantics are unchanged.
- Prepared writes retain their original identities and conditional sequence.
  No transparent fallback, forwarding or automatic mutation replay is added.
  Missing replies can still mean an unknown committed outcome.
- gRPC adapters share the GraphState pools. Expiry and shutdown/drop release
  registries even if another owner keeps GraphState alive. Socket/resource tests
  distinguish listener state from application state.

## Legacy boundary and tests

`GraphService` remains an explicit legacy library wrapper for old wire-specific
callers/parity fixtures; it is not constructed by graph-node. Legacy coordinator,
gateway and Unix entrypoints remain separately inventoried under #218.

Existing JSON fragmentation/malformed-frame/proxy assertions are not deleted or
pretended to test HTTP/2. Their integration suites launch an ignored test-only
fixture through their own test executable, preserving independent processes
without shipping a legacy product binary or adding a product compatibility flag.
The real graph-node scenarios, catalog/routing tests, CLI demo and typed services
exercise the new RPC-only default. Both kinds of evidence are required and are
reported separately. A legacy-fixture success is not evidence that the product
still offers that wire.

## Validation and claims

Record the exact source tree, independent review and actual CI/CoreHR results in
the PR. Check scoped deadlines and snapshot cleanup, owned listener sockets,
removed-option rejection, catalog CRUD/routing, explicit server queries and
multi-process failure/epoch behavior. Do not weaken old assertions to obtain a
transport green light. This migration is not a throughput, production security,
rolling-upgrade or multi-host HA guarantee.

## Migration validation: scoped-session timeout investigation

The default gRPC adapter migration exposed a reproducible timeout in the legacy
interpreter oracle used by `distributed_query_crosses_real_shards_without_snapshot_downloads`.
The oracle deliberately shares one **20-second absolute read-session deadline**
across 38 queries. This limit and all query/assertion inputs were retained.

- Migration baseline local commit `4a944ae`: the 29th query,
  `g.V(1).out().has('missing').out().out().count()`, failed with
  `Graph(Storage(Timeout { request_id: None }))` at 19.7669 seconds measured from
  the oracle loop (the session starts slightly earlier). A separate exact rerun
  reproduced the timeout. This was not dismissed as a flaky test.
- Same migration plus the independently reviewed #257 accepted-socket
  `TCP_NODELAY` change (combined local commit `9fc9a0b5`): all 38 oracle queries
  completed in 7.4328 seconds. The whole scenario, including subsequent failure
  injection and proxy checks, passed in 30.37 seconds.

This is one same-host diagnostic comparison, not a throughput or multi-host
benchmark. The production delta in this comparison is the #257 socket policy;
no timeout was increased and no oracle query was skipped. Reusing this evidence
requires the named source combination; the PR also records final whole-suite and
CI results. Legacy wire-specific assertions still run against explicit test-only
fixtures, while the oracle's catalog/routing adapters use the new default RPC.

The [diagnostic manifest](evidence/rpc-storage-nodelay-comparison.json) records
local source IDs/trees; they are not claimed to be published commit links. The
[exact comparison patch](evidence/rpc-storage-nodelay-comparison.patch) isolates
the production socket-policy delta (plus its associated socket tests). Reverse
that patch from the final combination to reproduce the pre-policy comparison;
keep the same source/query budget and record fresh results rather than assuming
these timings transfer to another machine.

A subsequent comparison on original `26ff73ce` TCP storage completed the same
oracle in 2.8628 seconds (whole scenario 26.66 seconds). Its deletion-budget and
experimental suites also passed (57.97 and 45.50 seconds respectively). NODELAY
alone therefore does **not** establish performance parity for the RPC migration.
A successful-channel cache preserves every metadata check and scoped deadline;
its initial regression run fixed the audit suite but still exhausted two original
60-second scopes. These failures remain acceptance blockers until the final PR
records a verified correction. No timeout or expected budget error is weakened.

### Metadata transport isolation

An ignored same-process, real-socket diagnostic compares original TCP metadata,
a reused typed gRPC client, and the synchronous adapter over that reused client.
All three lanes perform the same 1,000 applied-epoch metadata checks per round;
a test-only counter verifies actual execution, rather than merely counting calls.
Across three interleaved rounds in the unoptimized test build, median p50 was
224 µs (TCP), 748 µs (gRPC), and 764 µs (adapter). This identifies the unary RPC
path, not the synchronous bridge, as the dominant additional cost in this sample.
It does not justify reducing epoch checks or changing consistency semantics.

The [raw microbenchmark results](evidence/rpc-storage-metadata-microbenchmark.json)
record the build mode, source base and uncommitted diagnostic/cache changes.
These are local diagnostic results, not a release-performance claim. Matching
optimized-build comparisons and the full original acceptance suites remain
necessary before concluding that the migration is ready.

A separate [phase-timed run](evidence/rpc-storage-metadata-phases.json) measured
about 24 µs in the server handler and 25 µs in client preparation/validation,
versus about 723 µs awaiting the RPC. The residual includes transport, middleware,
codec scheduling and network/runtime work; it is not proof of a single culprit.
A [temporary h2-only opt-level=1 diagnostic](evidence/rpc-storage-metadata-h2opt1.json)
reduced median RPC p50 from 735 to 667 µs, with noisy tails. It did not establish
acceptance or explain most of the TCP/RPC difference. No profile, CI setting,
logical budget, epoch-check frequency or timeout was changed in the repository.

A matched [RPC-stack opt-level=1 experiment](evidence/rpc-storage-metadata-stackopt1.json)
optimized only the named dependencies through temporary Cargo command arguments.
The application and tests retained their original assertions and deadlines. TCP,
unary RPC and adapter median p50 were 150/416/434 µs. The three complex suites
passed all eight tests (audit 41.12 s, deletion 86.57 s, experimental 63.86 s), but
this **does not pass or replace the original default-debug gate**. A generic
performance correction and matched release evidence are tracked in
[#258](https://github.com/ashione/VelaGraph/issues/258), which blocks #256.

The real-process shard-operation suite additionally verifies a warmed cached
adapter whose write response is gated away: timeout retains the original ID,
the write is observed committed, and only an explicit resubmission returns
Duplicate at the same sequence. The original direct-client unknown-outcome test
is retained. This suite passed in 2.76 s; sequence evidence alone is not a proof
that no duplicate request was transmitted, so source-level no-replay review is
also required.

### Matched release baseline and streaming candidate

The [quiet release baseline](evidence/rpc-storage-metadata-release-baseline.json)
uses frozen `86f2792c` before streaming, Cargo's unchanged release profile, and
an independently built test binary. Three-round median p50 was 80.64 µs (TCP),
119.89 µs (unary), and 136.08 µs (adapter). This is an important qualification:
the large debug-build gap is not a claim of a threefold production penalty.
The frozen diagnostic's hardcoded raw build label is corrected explicitly in
the accompanying metadata; it must not be interpreted as an unoptimized run.

The [quiet default-debug stream candidate](evidence/rpc-storage-metadata-stream-debug.json)
measured TCP/unary/stream/adapter median p50 of 254/793/355/387 µs. Each timed
lane executed 1,000 real Metadata checks in each of three rounds. This supports
amortizing unary framing with the generic stream, not skipping freshness work.
Neither this microbenchmark nor the earlier optimized-dependency experiment
substitutes for unchanged default regression tests, resource/cancellation review,
or a matched release measurement of the final candidate. Stream semantics and
explicit resource/availability limits are described in
[METADATA_STREAM.md](METADATA_STREAM.md).

The [matched release candidate](evidence/rpc-storage-metadata-release-candidate.json)
uses frozen `e6a73c5a` and the unchanged Cargo release profile. In that one binary,
three-round median p50 was TCP 67.28 µs, unary 97.57 µs, stream 51.87 µs and
adapter 45.86 µs. All 12 timed lanes verified exactly 1,000 Metadata executions.
This supports a real local Metadata-path improvement; lane scheduling/noise means
it is not proof that the synchronous adapter is intrinsically faster than the
underlying stream. No multi-host throughput or whole-query latency is implied.

The candidate also passed the original **default-debug** complex acceptance
suites without profile overrides: audit 2/2 (35.27 s), deletion 1/1 (79.22 s total
across its original sessions), and experimental 5/5 (61.18 s). All original
60-second scopes and expected traversal-budget errors were retained. Eleven
additional process/lifecycle suites passed 33 tests, with the original six
ignored fixture/long-experiment entries unchanged. These local results do not
replace exact-head CI or independently reviewed resource/consistency boundaries.

Resource/isolation validation passes all four real-socket tests: connection
saturation/recovery; partial frame timeout with same-connection unary survival;
slow-reader backpressure with its stream permit retained until cancellation or
shutdown; and a real in-flight write sharing a connection with an expiring
Metadata request. The last test pauses state application, observes the write
log append, waits for Metadata's 20 ms deadline while the write remains pending,
then releases application and verifies commit, explicit same-ID Duplicate,
exact sequence increment and stored value. It does not infer a write rollback
from a timeout or claim cross-host fault coverage. Independent review corrected
transport accounting to pinned Hyper's 400 KiB send / 1 MiB receive defaults.

## First exact-head CI integration finding

PR #259 head `07034bd3` failed CoreHR smoke startup in run `43440274350`:
`smoke-queries` exhausted its original 30-second readiness wait. The node had
moved to gRPC, but the three CoreHR executable examples still called the legacy
probe/write/snapshot and coordinator transports. This is a migration omission,
not evidence of a flaky runner or permission problem. The failed artifact
`14045833169` retains stderr and node logs.

The correction migrates those examples to existing typed RPC clients and the
typed coordinator on both ingress and shard routes. It preserves the original
fixture, oracle, budgets and failure assertions; it neither restores the old
listener nor changes timeout/test profiles. Local Rust execution was unavailable
after the cloud environment reset, so this correction requires fresh exact-head
CI rather than inheriting earlier local pass claims.
