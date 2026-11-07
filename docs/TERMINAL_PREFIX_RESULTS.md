# A1 terminal-prefix validation and measurement

Measured 2026-11-07 UTC. This is a bounded same-host implementation comparison,
not a 200,000-person managed-graph acceptance or production capacity result.

## Frozen source and method

- Baseline commit `5778f255212bb8854dd2834168003201a136f789`, tree
  `92d571359650c4cff2bbd553139df080a65aa9a5` (the merged main implementation).
- Candidate implementation commit `947c237afb22c7aaa3c0a3bcab107d5835165fc4`, tree
  `34758cafa84ef65bf86af2a171149a4b93cb743d`. This report and its JSON are a later
  documentation-only addition; they do not change the measured implementation.
- Same harness source SHA256:
  `9fb829fe3ede89d9b552193ef9a9bc6f74588109ba053a049704e839be6edf3e`.
- Baseline ELF SHA256:
  `5888251651844e39d38edae1bd5cdc00940a8448983833ca66154f2b4718a554`.
- Candidate ELF SHA256:
  `3a080fa2a38126145eb35a2e3daf6874a6dfaab52b51ddd2ae2f4ef4813a0676`.

Both runs used separate regular-file copies of the same stopped durable fixture:
149 terminal transactions, but a complete-cohort readable version of only 148.
There was no complete historical-128 DB checkpoint, so the earlier non-isolated
128-write diagnostic is not used as the baseline here. Each run reopened one
coordinator and three participant OS processes, each a one-replica Raft group;
the parent had only remote handles. No original DB was opened or modified.

Both used debug builds, one build job, no incremental compilation, identical
native libraries/toolchain, loopback networking and tmpfs database copies. The
baseline and candidate were built and run sequentially, with no concurrent build
or write workload during measurement. There were three warm sequential samples
per requested V, not a randomized benchmark or production latency distribution.
Each main ELF was frozen before the next build. Source inventories, commands,
compiler/runtime logs and hashes accompany the external evidence bundle; the
reproduction harness is in `scripts/prefix-comparison`.

The only graph Submit was the original external-ID1 retry, after timing. It
returned its original Commit/transaction ID, preserved the exact retained Plan,
left the Journal byte-identical, and did not advance the common read version.
No new identities were submitted. Explicit V149 still returned
`Snapshot(Application(VisibilityNotApplied))` in both runs.

## End-to-end capture and exact evidence

Capture is `GraphCohort::snapshot`, including coordinator metadata/evidence RPCs,
all participant barriers, exact proof validation, bounded D1 history replay and
final graph snapshot materialization. Verification and `prepare_vertex` are timed
separately. All expected labels/properties and the next absent vertex were checked
at every requested V. Separate untimed acquisitions supplied exact accounting.

| Requested V | Baseline capture median (range), ms | A1 capture median (range), ms | A1 proof JSON bytes | Rows / replay operations | Raw graph bytes |
|---|---:|---:|---:|---:|---:|
| 1 | 1571.781 (1543.327–1688.204) | 8.752 (8.625–11.750) | 658 | 1 / 1 | 140 |
| 128 | 1557.181 (1516.018–1587.768) | 41.452 (40.900–41.783) | 41,612 | 128 / 128 | 17,921 |
| 148 | 1528.591 (1521.291–1579.278) | 48.774 (48.148–50.154) | 48,073 | 148 / 148 | 20,741 |

The retained full Journal was **1,004,638 bytes** in both variants. The old
acquisition transferred full Journal objects even for V1; the A1 acquisition
requests only the exact V1 projection. These are serialized object lengths, not
a TCP counter or the complete RPC envelope length. The durable archive is not
smaller and latest-V proof growth remains O(V times participant count).

`prepare_vertex` median milliseconds were essentially unchanged:

| V | Baseline | A1 |
|---|---:|---:|
| 1 | 0.034071 | 0.034892 |
| 128 | 0.035914 | 0.034091 |
| 148 | 0.035363 | 0.035163 |

The raw samples include one 1.208 ms baseline preparation and one 0.410 ms A1
preparation outlier. Earlier preparation numbers collected alongside other work
must not be substituted for this isolated comparison. This slice does not claim
to optimize plan preparation or the remaining full-history prepare/advance path.

## Isolated projection/codec diagnostic

A separate candidate-only, offline diagnostic decoded the exact saved 149-terminal
Journal and measured projection/codec operations (three repetitions). These are
not components to add or subtract from network capture timings and are not a CPU
profile. D1 replay work is separately reported above by exact operations and rows;
this run did not isolate its wall-clock time.

| V | Projection median, ms | Proof encode median, ms | Proof decode/validation median, ms |
|---|---:|---:|---:|
| 1 | 0.008232 | 0.029474 | 0.040551 |
| 128 | 0.073720 | 1.355978 | 1.943061 |
| 148 | 0.087080 | 1.539914 | 2.461190 |

Full Journal decode with strict transition/Plan replay remained 98.233 ms median,
and full Journal encode 35.039 ms median. A1 does not remove those recovery/write
costs; it removes their repeated use from fixed-version capture. Equality is
exact structural comparison, not a hash or network authentication claim.

## Functional gates and limits

- Final D2 library: 104 passed, including strict proof/golden/segment tests,
  V0 authority/cohort rejection, future holes, immutable old cuts, journal v2/v3
  replay, same-lock reads, reopen/snapshot reconstruction, old-wire rejection,
  and a protocol-sequence test proving acquisition requests Summary/Terminal
  rather than Journal.
- Replicated-graph library: 186 passed, one pre-existing ignored test.
- Real three-node process scenarios: partial Commit/nonparticipant lag,
  participant/coordinator kill/reopen and exact original-ID recovery passed.
  Concurrent scenario: six newly acquired fixed-V2 sessions while four later
  transactions committed; old properties and exact V2 proof remained stable,
  while the final common version was V6.
- The separate frozen four-node before/after runs above both passed all data,
  lag, original-ID and exact Plan/Journal checks.
- Workspace all-targets clippy with `-D warnings`, formatting and diff whitespace
  checks passed.
- Full local `cargo test --workspace --locked` was attempted: 368 passed before
  eight existing Unix-domain-socket gateway tests failed because this execution
  environment returned `Operation not permitted`; child-start disconnects were
  consequent failures. The suite stopped there, so later targets were not run in
  that invocation. This is not a full-workspace green claim and was not bypassed.

No request/journal/capture/frame limit was raised, no retained history was removed,
and no new authentication, GC, failover, nonempty-baseline certification or
indexed MVCC was added. Whole-directory rollback still needs an independent
external durable anchor. Existing remote GlobalCommittedSnapshot is Unsupported.
The full 200,000-person managed workload and the 257th external request were not
executed by these measurements.

Raw structured results: [summary](evidence/terminal-prefix/summary.json),
[baseline](evidence/terminal-prefix/baseline.json),
[candidate](evidence/terminal-prefix/candidate.json), and
[proof codec](evidence/terminal-prefix/proof-sizes.json).
