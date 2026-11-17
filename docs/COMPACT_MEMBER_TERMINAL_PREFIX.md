# Proposal: compact active-member terminal proof construction

Status: implemented and validated locally; pending exact-head CI/integration.
Tracking: #108. This is a narrow active-read slice, not completion of the
full-Journal dependency umbrella.

## Problem and scope

`participant_terminal_prefix` and `certified_terminal_read` have compact wire
results but currently call `certified_decision_prefix` → `decision_prefix`.
For active durable members this reconstructs a complete Journal through
`load_disk_state`, including original requests and Plans. The complete Journal
is then projected into the requested terminal prefix.

Replace only that active participant branch. Preserve the legacy/in-memory
branch, coordinator terminal reads, explicit full-evidence participant reads,
logical snapshots, cold open/recovery, write/apply paths and all storage/wire
formats. In particular, the independent full-evidence same-lock reuse change
must remain independent; this proposal does not edit its two call sites.

## Authority and consistency

Reuse `StateMachine::verify_member_archive` with a private member-prefix wrapper.
The existing wrapper requires the current published member checkpoint Arc,
complete applied LogId and Member role to match the private publication cut.
The cut's RocksDB snapshot verifies persisted D1 metadata and decision manifest
against their published bytes, rejects retired request-index state, and the
raw walker checks the complete applied marker, canonical namespace, partitions,
counts and cryptographic closure against the descriptor.

The current `verify_active_inner` already uses `OrderedTerminals` to observe raw
Commands in order and `CheckpointRows` to compare every persisted derived row.
No new raw walker, persisted cache, caller-supplied witness or trusted boolean
is introduced. The private publication cut originates in strict cold/writer
validation; this read projection is not an alternative way to mint that cut
from arbitrary self-consistent disk metadata.

Both public methods keep the existing state read guard throughout validation,
proof comparison and, for certified reads, `d1.read_at(V, limit)`. A concurrent
publisher cannot change the D1 graph or authority between those steps. No graph
snapshot is reconstructed from the terminal proof, and the existing distributed
leader/cohort preconditions remain the caller's responsibility.

## Construction algorithm

1. Keep the existing public shard/epoch/mode checks and certified-prefix
   visibility, applied-version and zero-baseline checks in their current order.
2. For an active member only, invoke the existing generic archive verifier with
   a closure requesting prefix V. Other representations use `decision_prefix`
   unchanged.
3. Extend `verify_active_inner` with a private terminal observer. Its existing
   callers provide a no-op observer; member-apply tail retention stays unchanged.
   First run `CheckpointRows::observe(tx)`, then collect only emitted records
   with ordinal <= V. `OrderedTerminals` emits only the contiguous resolved
   frontier, never a later decision across an unresolved hole.
4. Store only Terminal fields in canonical 32-record segments. Do not retain
   original Commands, request bytes, Plans or a full Transaction archive.
   Check existing profile count and canonical output byte bounds incrementally
   before retaining the next bounded record. Record collection failures locally
   and stop collecting, but do not short-circuit the verification scan.
5. Finish the entire raw walk, `projection.matches(checkpoint)` and
   `rows.finish()`, including history after V and all derived rows through A.
   Only after those checks succeed may collection errors be returned and
   `TerminalPrefix::from_segments` perform its unchanged final identity,
   count, ordinal, segment, transaction and envelope-byte validation.
6. Compare the whole retained proof to the caller's expected proof as today;
   mismatches fail closed. Certified reads retain the original `read_at` call.

No successful partial proof can escape a damaged suffix. A=consumed ordinal and
R=observed resolved ordinal remain distinct. V must be <= A; raw events and
projection still cover through the observed cut, including R>A and unresolved
(R,N] records. Decision rows are checked through A, not merely V.

## Errors and compatibility

Keep public identity, mode, visibility, baseline, future-version and mismatching
proof rejection behavior. Existing verifier storage/history error mapping is
used without swallowing, retrying or falling back after active verification
fails. Collector failures must not mask raw, checkpoint or decision-closure
failures. Reusing a different physical reader does not promise identical
transient I/O failure schedules or the same internal read count.

No quota is raised and no malformed proof becomes valid. The same final
TerminalPrefix validator remains authoritative. Changes in a corrupt input's
specific error variant must be examined against the former path, rather than
asserting only that some error occurs and silently relaxing its contract.
Active old-V reads now explicitly close all derived rows through A, including
rows after V. This is stronger than the former requested-range index check;
these reads reject suffix index corruption even if the requested prefix is
intact. Mixed corruptions need not preserve the former loader's error ordering.
The shared walker's own checks and errors retain their existing ordering.

## Resource analysis

The response contains O(V) terminal records, an existing API cost, not constant
memory. Legacy retains its 1024-transaction and per-record participant ceilings;
the byte profile retains its count derived from the unchanged 16 MiB original
Journal budget and the existing `MAX_BYTE_PROFILE_TERMINAL_BYTES` canonical
proof ceiling. The final envelope check includes identity and segment overhead.
Canonical serialized bytes are not a claim about allocator overhead or RSS.

At peak the output coexists with the already bounded historical active-interval
projection, one decoded raw row, one decoded decision row, private metadata and
the caller's expected proof. A temporary bounded Terminal may be sized before
it is retained. Drained original requests/Plans are not accumulated. Existing
member-apply partial-tail collection remains at most 31 Transactions.

The operation still scans all retained raw history once and validates all
required derived rows: O(retained bytes/events), not short-prefix I/O. Cold
open, full-history exports, explicit full-evidence APIs and logical snapshots
retain their existing full materialization costs. No claim of bounded cold
recovery, allocation-free reads or whole-process lifetime-independent memory
follows from this change.

## Alternatives not selected

- Keeping full replay preserves the former implementation but retains the
  original request/Plan archive that this read does not return.
- Reading only requested derived rows would not establish their agreement with
  the raw authority or reject a corrupt suffix under the selected contract.
- Verifying first and projecting in a second raw scan duplicates structural work
  already performed by `OrderedTerminals` and increases physical reads.
- Caching a complete proof or Journal across calls adds lifetime/invalidating
  authority requirements absent from an operation-local result.

This slice makes no throughput or latency claim. Any benchmark must separately
account for the stronger all-derived-row verification on old-version requests.

## Verification plan and acceptance

- A new actual-public-API test must fail on the old implementation with the
  full-Journal tripwire enabled around every future poll, not merely set an
  unconsumed per-StateMachine flag. Source Journals are fixture generation only.
- Compare returned proofs against independent Journal truth for both capacity
  profiles, V=0 and 31/32/33 segment edges, old V, and reopen.
- Exercise nonempty graph snapshots, Commit/Abort/nonparticipant decisions,
  unresolved holes, and observed R beyond consumed A without skipping ordinals.
- Preserve wrong identity/mode, future V, nonzero baseline and mismatching
  expected-proof rejection; retain legacy/in-memory control coverage.
- Corrupt raw rows, derived rows and published metadata/manifest after the
  requested V, and add extra/missing namespace rows. Both APIs must reject,
  perform no writes and never return an earlier apparently valid prefix.
- Use a deterministic publication gate to establish the state read guard spans
  proof validation and graph capture, with a concurrent writer demonstrably
  blocked. Do not infer same-lock correctness from an incidental race result.
- Validate collector count/byte boundaries without weakening the existing final
  proof validator or raising transaction/history quotas.
- Run focused regressions, full d2-raft library tests, applicable integration
  tests, formatting, all-target Clippy and diff checks on the final tree.

Independent review and exact-head CI are required before integration. A local
pass is not a cloud/provider certification or evidence that #108 is complete.

## Local validation record

Both new actual-public-API tripwire tests failed on the original implementation
with a full-Journal-access panic, then passed with the active branch above.
The public-read matrix has seven tests and the collector has three; the existing
coordinator/member index test now explicitly distinguishes unchanged coordinator
range reads from strict complete active-member verification.

The implementation tree passed `cargo test --locked -p d2-raft --lib` (389 passed,
5 ignored), the `d1_read_snapshot`, `durable` and `physical_checkpoint` integration
suites (7 passed), `cargo clippy --locked -p d2-raft --all-targets -- -D warnings`,
`cargo fmt --all --check` and diff checks. Ignored tests and remote CI are not
claimed by these local results. No provider, rejoin or cold-recovery guarantee is
added.
