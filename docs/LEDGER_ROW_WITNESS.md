# Coordinator exact-row witness experiment

This candidate replaces only coordinator `verify_active` raw decoding. It keeps
complete namespace reads, key comparison and byte hashing on every invocation.
Member verification, historical projections, independent semantic staging,
metadata/retired-index checks, full applied LogId checks and cold semantic replay
remain. There is no disk/wire schema, TTL, quota, feature or index change.

## Proof boundary

A private row witness contains the first event position, encoded key+value length
and SHA-256 over a versioned domain tag and length-delimited exact key/value bytes.
It can be minted only from a row accepted by full cold replay, or the exact bytes
of a row emitted by the existing semantic staging path. A loaded descriptor or
stored digest cannot manufacture the witness. The row vector is not serialized.

Each coordinator verification snapshots the current store, checks the complete
applied marker, iterates the full raw namespace, demands exact expected keys and
hashes, and rejects extra/missing/substituted/foreign rows before success. It then
decodes only the final bounded row to reconstruct Tail. Tail identity, first,
last event count, head root and byte bound are checked explicitly. SHA-256
collision resistance is an integrity assumption, not authentication.

Exact byte equality with previously validated canonical rows preserves their
encoding, greedy partitioning, root chain and original semantics. Every raw row visible in the operation's snapshot is still read and hashed.
Later writes retain normal snapshot isolation; physical media/cache behavior is
unchanged. Actual iterator/read errors propagate. There is no background scrub substitution,
unchecked cache hit, or memory repair after detected corruption.

Published cuts own an Arc slice. A blank/membership/rejected batch shares the
same slice while updating the complete applied LogId. Changed history copies the
small row vector, drops/recomputes the final row witness and appends witnesses
for new rows. Exact staged byte publication is part of the original one-sync
WriteBatch/publication protocol. Failed sync leaves the old cut unchanged.
Snapshot replacement rebuilds the inventory; cold reopen still performs complete
original-event semantic replay. The member path stores no inventory.

## Space and work

The element is 48 bytes on the tested target: u64 first, u64 byte count and a
32-byte digest. Existing max_segments bounds are 2,122 Legacy / 7,499 ByteBound,
so one retained payload is at most 101,856 / 359,952 bytes, plus Arc/allocation
metadata. No Commands, Plans, request IDs or transaction collection are retained.

Peak memory is not just one payload: old/new views may overlap, Vec capacity may
grow, and Vec-to-Arc conversion can temporarily coexist with its input. Outstanding internal
readers retaining old cuts extend their lifetimes. Report retained payload and
peak allocator/RSS measurements separately; do not claim a total-RSS bound.

Cold load and each changed row gain a hash operation. Changed publication copies
O(number of rows) small entries; ordinary verification still reads and hashes
O(history bytes). Only redundant semantic decoding is removed. This does not
establish asymptotically constant transactions or unlimited history capacity.

## Measurement boundaries

The ignored verifier test alternates the original full walker and the candidate
on the same cut in one executable. It includes snapshot and metadata checks,
but excludes witness construction. The ignored actual-apply test includes both unresolved histories and fully
resolved histories with terminal-prefix draining. It additionally
includes sync, semantic staging, witness construction/copy and publication. Its
test-only counterfactual disables witness construction and restores the original
verifier for the baseline; there is no production toggle. Both sides compare
complete persisted bytes and checkpoints after every Begin+Abort batch.

These are local coordinator measurements, not complete Submit, network, reader
pin lifecycle or distributed throughput. Acceptance still
requires preserving all existing corruption/recovery tests and measuring net
benefit rather than reporting verifier savings alone.


## Historical optimized experiment (pre-rebase)

Two paired runs use Rust release optimization and the same preexisting native
RocksDB archive for both variants (SHA-256
`30b67f9a1e470aa03232f0efd508426782e723ed6fdae6eaafb9d40011caad60`).
The native archive came from the debug build tree; this is not a claim that the
entire native stack was rebuilt with production optimization. Per-row decoder
instrumentation is absent from these optimized binaries.

Each workload has four warmup pairs followed by 20 measured samples per variant;
ordering alternates. All persisted bytes and checkpoints match after every pair.
The ordinary median below averages the two middle values from the preserved
sample arrays. The compact raw harness line uses the upper middle value.

| Initial transactions | Initial prefix | Run 1 original/candidate (us) | Run 2 original/candidate (us) |
| --- | --- | --- | --- |
| 32 | unresolved | 2740 / 2472.5 | 2550 / 2307 |
| 128 | unresolved | 5918.5 / 5134.5 | 5726.5 / 5268 |
| 512 | unresolved | 20409.5 / 17816 | 21711.5 / 19039 |
| 32 | resolved | 2366 / 2222.5 | 2128.5 / 2039.5 |
| 128 | resolved | 4998.5 / 4195 | 4752.5 / 3964.5 |
| 512 | resolved | 16236 / 12562.5 | 14242.5 / 11779.5 |

Observed median reductions range from 4.2% to 22.6%. Ranges overlap and outliers
remain; this does not establish uniformly better tail latency or a general
throughput improvement. The initial verifier alone benefits more, but that is
not substituted for the measured durable-apply result. The 512-transaction
verifier fixture retains 17 row witnesses (816 bytes payload), far below the
proven worst-case retained bound; peak allocator/RSS was not measured.


## Historical regression gates (pre-rebase)

The complete d2 package optimized run passed 381 tests across 16 targets, with
zero failures and five ignored manual benchmarks. It includes the unchanged
legacy migration/goldens, full semantic cold recovery, forged history rejection,
snapshot replacement/repair, sync and process-crash checks, Raft conformance,
durable clusters, multi-process and TCP adversarial suites. Four new debug
focused tests also passed; two separate ignored cost tests ran successfully.
Final whole-workspace acceptance and original pin-lifecycle behavior must be
reported separately, not inferred from these gates.

Downstream optimized coverage completed: replicated-graph library 280 passed
(two ignored) and distributed_query nine passed. The subsequent managed_process
suite was intentionally interrupted (exit 130) during its 1,025-write lifecycle
test to prioritize the original debug pin scenario. The preserved interrupted
checkpoint was ordinal 189: this is not a completed acceptance test or a pass.
Remaining managed_process cases and remote_process did not run in that command.
The full long lifecycle is a separate outstanding gate. Formatting, diff checks,
and whole-workspace all-target strict clippy passed.

## Historical local primary acceptance failure (pre-rebase)

The original unoptimized `exact_old_prefix_captures_while_other_transactions_commit`
was rerun with its original 60,000 ms session TTL and assertions unchanged. It
failed after 108.22 seconds: the retained V16 pin expired during write 19 in the
dynamic concurrent model. The six fixed V2 acquisitions and four new commits
completed before that later failure. The process fixture spawns `current_exe`,
so parent and child ran the same newly built candidate test executable.

This candidate does **not** repair the original pin-lifecycle failure, eliminate
history-dependent work, or establish whole-workspace acceptance. It is a measured
partial optimization for review only, not a merge-ready performance fix. The
1,025-write lifecycle and remaining downstream process tests are still open gates.
No TTL, test assertion, persistent format, or corruption behavior was relaxed to
turn either incomplete/failed gate into success.


## Current-main revalidation

The candidate is reapplied to main
`5f68639a9b3fef6b6f5c0bebfe85a752c6e68b88`. The d2 implementation is
unchanged between the original main baseline and this main; newer query and
cohort changes are preserved. Historical measurements above are not current-tree
acceptance. Fresh paired measurements, independent integrity/concurrency review,
and complete ordinary/CoreHR CI including retained artifact upload and the
aggregate gate remain required.

The previous PR head `25dfc8b93102051ff2c14ad9202f7b2ec674244a` did later
complete ordinary CI (run 43088586312). Its CoreHR run 43088586149 did
complete the 1,025-write lifecycle execution: job 130760673348 logged
`Managed lifecycle COMPLETE: exact release test exit and checkpoint validated.`
at 2026-11-11T20:40:44.5746077Z. The later artifact upload failed with a
404 `workflow run not found`; the workflow did not achieve aggregate acceptance.
This remote execution result does not erase the earlier local pin failure or
constitute a green current-main gate. The unchanged artifact/aggregate checks
will be rerun on the updated candidate, not waived or marked optional.

The current-main independent review found no new production integrity blocker.
One minimal regression now retains the old VerifiedArchive across a real
successor commit: its pinned bytes still pass both witness verification and the
original full walker, the old cut rejects a fresh snapshot, and the new cut
succeeds without mutating or sharing the old witness slice. Existing sync/crash,
snapshot repair/reopen, and large-row tests remain unchanged. Static review is
not execution acceptance; fresh tests and required CI still gate this revision.
