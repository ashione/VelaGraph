# Reliability evidence and release risks

This is an evidence boundary, not a release waiver. A passing run does not explain
an earlier intermittent failure. Issue status is not proof of resolved causality.

## Incident scope

| Tracking | Established evidence | Still not established |
| --- | --- | --- |
| [#128](https://github.com/ashione/VelaGraph/issues/128) | Original paged-download failure lacked phase-level evidence; #131 added diagnostics. Subsequent successful observations apply only to their tested revisions/conditions. | The cause of the original `Unavailable`. The issue was reopened on 2026-11-16 after a status/acceptance audit; its final investigation comments and `CURRENT_STATUS.md` still describe an unresolved cause. The prior closure was not a root-cause finding. |
| [#247](https://github.com/ashione/VelaGraph/issues/247) | A controlled two-follower pause reproduces read-confirmation failure. #253 added bounded fresh read confirmations under the original absolute deadline, without retrying mutations. #270 added per-RPC and bounded process diagnostics for a later shard-operation failure. | That scheduling caused either historical CI failure, or that all generic `Unavailable` results share a cause. #270 is diagnosis support, not a production root-cause fix. |

The original #247 failed-attempt artifact retained source provenance but not the
failure-time peer/process state needed for causal reconstruction. A successful
rerun's artifact cannot supply the missing original state.

## Fixed-source follow-up evidence

[Investigation run 43604993679](https://github.com/ashione/VelaGraph/actions/runs/43604993679)
checked out source `40e2b9a419ff941a58761f784c9c7fd500a6bf08` under workflow revision
`fc1c4b30cbbad6c3ef8ad28b112aa39e63a7f82e`. Its two independent hosted runner jobs
finished successfully on 2026-11-14 UTC:

- [Shard-operation job](https://github.com/ashione/VelaGraph/actions/runs/43604993679/job/132477623706): ten fresh suite invocations, each 3 passed; 2.50–2.53 seconds per invocation.
- [Snapshot job](https://github.com/ashione/VelaGraph/actions/runs/43604993679/job/132477623972): ten fresh suite invocations, each 5 passed and 3 ignored; 8.31–8.72 seconds per invocation. Expected `ResourceExhausted` negative-test diagnostics are not failed tests.

These results bound non-reproduction at that source; they neither prove the
historical cause nor certify later revisions.

## Current-main mechanism check

On 2026-11-16 UTC, dot cloud tested source
`a7525e217a2f26ab84c5f9710dc0d0d5ba829483`, tree
`9d60803cb7fd1efe554d6f861886c895b7c4eb4f`, with the commands below:

- Ordinary snapshot suite: **5 passed, 3 ignored**, 7.51 seconds.
- Explicit ignored scheduling experiment: **1 passed**, all 15 internal cases,
  7.38 seconds. Two paused followers for 180 ms recovered in 184.7–187.4 ms;
  the 50 ms budget cases timed out in 50.8–51.7 ms. All post-resume reads passed.
- Shard-operation suite: **3 passed**, 2.53 seconds. The deliberate lost-response
  write case's ambiguity diagnostic is expected and does not imply retry safety.

No new causal failure was reproduced, and no production fix follows from these
runs. Failure-context collection remains exercised by the snapshot suite's
quorum-loss and unresponsive-control negative tests; shard fixtures emit bounded
per-node logs before cleanup on unexpected failure. These are targeted tests,
not a full-suite, release-scale, or production-latency claim.

## Public failure-boundary evidence (#299)

Main includes `public_interfaces_fail_closed_without_coordinator_quorum_and_recover`.
It sends real HTTP/gRPC requests to a separate service process while all links
between three coordinator voters inside one node child are isolated with the
existing `DropPolicy`. New query-open and write-submit calls must return typed
OutcomeUnknown/HTTP 503, not a successful read or commit acknowledgment. After
explicit link restoration and fixture leader election, the test checks original-ID
status before deliberate same-ID replay, complete result oracles and old-V pages.
The existing [service contract](PUBLIC_SERVICE_CLI.md#public-admission-under-coordinator-quorum-loss)
records the exact boundary.

This is additional fail-closed/recovery acceptance, not a reproduction of either
historical incident. It does not establish independent-voter-process availability,
automatic leader routing or public managed multi-voter provisioning. It does not
satisfy #224's unresolved-defect disposition merely by passing. Both #128 and
#247 remain separately tracked; preserve the first unexpected failure and its
exact source before considering any later successful sample.

## Candidate validation and stop conditions

Use the exact release candidate SHA, preserve its source/tree identity and the
complete output, and run with unchanged production budgets and assertions:

```sh
VELAGRAPH_REMOTE_DIAGNOSTICS=1 cargo test -p replicated-graph \
  --test grpc_snapshot_process --locked -- --nocapture
VELAGRAPH_REMOTE_DIAGNOSTICS=1 cargo test -p replicated-graph \
  --test grpc_snapshot_process --locked \
  investigate_read_confirmation_scheduling_window -- --ignored --exact --nocapture
VELAGRAPH_REMOTE_DIAGNOSTICS=1 cargo test -p replicated-graph \
  --test grpc_shard_operations_process --locked -- --nocapture
```

The ignored Unix scheduling experiment deliberately pauses followers. Its 15
cases test five pause/quorum/deadline combinations three times each, including a
50 ms request that must time out and a post-resume read that must succeed. It is
a mechanism regression, not proof of the original incident's cause. Ordinary
suite execution does not run it automatically.

A candidate must stop for investigation on an unexpected unavailable/timeout,
failed recovery after resume, inconsistent snapshot/result, resource leak, or
mutation ambiguity. Preserve the first failing output and bounded child logs;
record the RPC phase and exact source before retrying anything. Expected negative
cases count only when their asserted error matches. Do not mask failure by
increasing timeouts, retrying writes, weakening assertions, or substituting a
successful rerun for a failed candidate result.

For v0.0.1, residual historical uncertainty must be explicitly reviewed alongside
current candidate evidence. No automatic decision to release, close #247, or waive
its acceptance conditions follows from this document.

## Milestone 1 investigation disposition

The [v0.0.1 release decision](https://github.com/ashione/VelaGraph/releases/tag/v0.0.1)
accepts the missing historical evidence for the same-version, fixed-topology
experimental service. The dispositions below apply to that scope.

* **Paged snapshot (#128).** The [original run](https://github.com/ashione/VelaGraph/actions/runs/42512547338)
  cannot distinguish Open failure from failure on a later page. The ordinary
  downloader regression injects Unavailable at each boundary, verifies the
  caller gets an error even after a valid first page, and checks the peer saw
  exactly one Open and a Close only for an acquired session. The independent
  process test retains its original budgets and child evidence. Accept the
  original cause as indeterminate. Investigate a new occurrence from its first
  failed run and the recorded Open/page phase.
* **Snapshot seed quorum (#247).** The [first failed attempt](https://github.com/ashione/VelaGraph/actions/runs/43377553475/job/131727467953)
  identifies a read barrier failure during seed but lacks contemporaneous
  peer state. Bounded read confirmation and the controlled follower-pause
  experiment are documented in the [quorum investigation](SNAPSHOT_QUORUM_INVESTIGATION.md).
  Accept the original trigger as indeterminate. On an ordinary CI differential
  seed failure, retain `snapshot-seed-failure.json` alongside source provenance.
  The vertex, selected leader, child state and bounded control observations
  describe the period **after** the error. Use them with the child phase logs;
  the later differential incident in #348 has no established common cause.
* **Observed session expiry (#311).** For same-version nodes, #335 separates
  Open acquisition from the bounded, nonrenewable snapshot lease. The ordinary
  wire tests cover the old 3-second boundary, the shorter caller deadline while
  another HTTP/2 stream is held, and background reclamation of a snapshot whose
  descriptor could not be delivered. The [original reproduction and provenance](https://github.com/ashione/VelaGraph/issues/311)
  and [queue observation](https://github.com/ashione/VelaGraph/issues/311#issuecomment-6943163297)
  distinguish caller timeout from the relative budget computed before transport
  queueing. Cross-version lease negotiation, exact remote deduction of queue
  time and arbitrary packet loss are outside the published contract. Revisit
  the wire protocol before advertising any of those guarantees.

A new unexpected failure of a required test still stops acceptance. Preserve
the first failed job, tree and artifacts before investigating. Closure of a
milestone investigation does not provide a production availability estimate.
