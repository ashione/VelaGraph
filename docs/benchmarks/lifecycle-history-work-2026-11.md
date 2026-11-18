# Bounded history serialization work

## CI tiers

Pull requests run the existing
`byte_bound_independent_processes_small_lifecycle_recovery` release test. It runs
the four-write recovery scenario twice, with diagnostics disabled and enabled,
including ACK loss, same-ID recovery, old-version checks, writer restart and
three snapshot restores. It does not certify crossing the old 1024-transaction
boundary. Ordinary unit/boundary tests and the full CoreHR scenario remain required.

Pushes to main, scheduled and manual runs retain the unchanged full 1025-write
release test and complete retained checkpoint/artifact validation. Both tiers
use the same single release builder and immutable current-run handoff, including
source, commit, artifact provenance and binary identity checks; no second build
or alternate unverified executable is introduced.

The stable `CoreHR 200k people / 5k departments` check still requires the builder,
lifecycle and CoreHR scenario to succeed. It also requires `pr-short` for a PR
and `full-1025` for main pushes, scheduled and manual events. Missing or mismatched
tiers, failed/cancelled/skipped jobs and unknown events fail the aggregate gate.
This removes the full 1025-write run from PR latency, while deliberately moving
its end-to-end boundary detection to main. It is a test scheduling choice, not
evidence that the full lifecycle itself runs faster. Both historical check names
(`Retained 1025-write release lifecycle` and the aggregate above) are retained
for branch-protection compatibility. The leaf name is a legacy check context;
the verified tier and job summary, not that name, distinguish PR short recovery
from full 1025-write acceptance. No protection settings are changed.

Only superseded PR runs share a concurrency group and may be automatically
cancelled. Main pushes, scheduled and manual runs use a unique run-ID group with
cancellation disabled. This avoids replacing either running or pending full
validation runs when another commit arrives; disabling cancellation alone in a
shared group would still permit pending-run replacement under GitHub's default
[concurrency behavior](https://docs.github.com/en/actions/how-tos/write-workflows/choose-when-workflows-run/control-workflow-concurrency).
More full runs can therefore remain queued or run concurrently during rapid
pushes, increasing runner usage compared with cancelling older runs.

Full validation remains post-merge by default; a schedule tests its selected tip,
not every earlier commit. Unique groups remove automatic supersession, not the
need to check that each relevant full run actually succeeded. For changes to
retained history, recovery or the acceptance harness, require a successful manual
full run on the exact reviewed head or merge candidate before merging. PR short
success and static review alone do not establish that boundary acceptance.

## Runtime optimization

The retained 1025-write lifecycle repeatedly verifies the complete original
history. The successful [PR 340 CoreHR run](https://github.com/ashione/VelaGraph/actions/runs/44264854620)
spent 2478.179 seconds in the exact release test. This single historical Linux
sample is not a before/after comparison with a local macOS candidate.

The raw row walker previously serialized every event three times to count its
canonical bytes: row admission, observer accounting, and generic history
extension. It now retains the already checked lengths in a fixed 32-entry stack
array and reuses them for the observer, hash and adjacent-row partition check.
This adds 256 bytes on a 64-bit host, independent of history length.

Only the already-counted raw-walker path encodes one event into a fixed slice
and hashes the resulting complete bytes. The slice writer independently checks
the reused length: both an overrun and an unfilled suffix fail.
The hash domain, previous root, absolute event index, canonical byte length and
every serialized byte remain identical. Cursor identity and event-count admission
remain shared with the original extension API. The generic extension retains
its original counting pass and streaming hash. The temporary event buffer is
bounded by the existing 4 MiB hash-input limit and dropped after each event. No
full-history buffer, cross-operation cache, new dependency, persisted format or
wire field is added.
The original per-event, per-row and whole-history bounds are checked in the
same order before observer results.

Only the final bounded row's original events are cloned for the returned writer
tail. Intermediate rows retain their count/byte totals for the next partition
check; every event still reaches the observer and complete hash chain.

These changes reduce repeated serialization and copying. Complete
namespace scans, row canonicalization, event-chain verification, semantic replay,
decision-index verification, reopen and snapshot checks remain in place. It
does not retain a Journal or cache results across snapshots or operations. The
existing 4 MiB event-hash input limit remains enforced before hashing.
Total work still grows with retained history. There is no history GC or claim
of unlimited write lifetime.

## Verification method

- Preserve existing cursor/page golden vectors, malformed history, overlapping
  page, forged proof and capacity rejection tests.
- Compare against the original token-streaming hash implementation for both history
  profiles at 1 KiB, 8191, 8192, 8193, 64 KiB and the 4 MiB event-hash limit;
  reject a one-byte excess with the original error.
- Check that incorrect size hints, invalid empty roots and exhausted cursor
  counts reject without modifying the cursor.
- The optional `canonical_event_hash_cost` microbenchmark rotates the generic hash,
  original raw-walker and length-reusing raw-walker event work, records all twelve
  measured samples after three warmups,
  and compares every digest. It is not a whole-lifecycle performance result.
  The raw-walker comparison includes all three original counting passes versus
  one counting pass and bounded event encoding/hash; it excludes decoding, storage,
  semantic replay and network work, which remain part of the full lifecycle.
- Build the unchanged base and candidate with identical locked dependencies,
  toolchain and release flags. Freeze both binaries before timing. Run the exact
  `byte_bound_independent_processes_1025_writes_retain_original_lifecycle` test
  sequentially, with fresh retained directories and the same diagnostics, host
  and assertions. Keep compilation separate from runtime.
- Require exact successful test output and validate the COMPLETE retained
  checkpoint, original IDs/Plans, old-version proof artifacts, ACK-loss recovery,
  three full snapshot restores, source manifest and executable hashes. Retain
  any failed run; do not reduce the write count or loosen a timeout.

Example microbenchmark (the full lifecycle remains independently required):

```sh
cargo test --release --locked -p d2-raft --lib canonical_event_hash_cost -- --ignored --nocapture --test-threads=1
```

Report source/binary identities, complete run counts, wall time, child CPU,
resource limits and measurement order with each result. Largest-child RSS is
not aggregate peak memory. Local Mach-O execution does not satisfy the separate
hosted Linux artifact-handoff/provenance gate. Hosted CI and independent review
remain required before integration.

## Local candidate measurement, 2026-11-18

On an Apple M2 Pro (12 logical CPUs, 32 GiB), macOS arm64, Rust 1.95.0,
locked dependencies, release opt-level 3 and debug-info 1, the rotated
microbenchmark produced these medians across twelve samples. Each sample
contains 100 events; three warmup rounds were excluded, and every digest was
checked. No build or other test workload overlapped the microbenchmark.

| Event | Original counting + hash, µs/100 | Reused-length encoding + hash, µs/100 | Reduction |
| --- | ---: | ---: | ---: |
| Retained request Plan, 2342 bytes | 1921.5 | 1468.5 | 23.6% |
| Synthetic 1024 bytes | 1129.5 | 804.5 | 28.8% |
| Synthetic 8193 bytes | 8798.0 | 6131.0 | 30.3% |
| Synthetic 65536 bytes | 70835.0 | 46450.0 | 34.4% |

These are event serialization measurements, not complete ledger scans or
whole-request/lifecycle speedups. The intermediate-row clone reduction is not
included in this microbenchmark. Generic hash callers retain their original
implementation: buffering that path was slower and was not retained.

History, ledger and history-sync targeted suites passed 18, 34 and 17 tests,
respectively, including both new boundary/equivalence tests. Two optional
benchmarks were ignored by these filters; the new microbenchmark was then
explicitly run and passed.

The baseline full-lifecycle run was intentionally interrupted at the user's
request to prioritize implementation, after the last reported 800 writes.
Its evidence remains INCOMPLETE. There is no complete local 1025-write A/B
comparison or measured Linux CI reduction for this candidate yet. Do not infer
either from this table, the interrupted prefix, or the historical PR 340 run.
