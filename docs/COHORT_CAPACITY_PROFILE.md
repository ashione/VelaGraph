# Immutable byte-budget cohort capacity

VelaGraph's new, explicitly selected `ByteBound16MiBV1` profile extends the
fixed-cohort lifetime beyond the legacy 256 external IDs and 1,024 transactions.
It does not provide unlimited writes. Complete original Command history, exact
external-ID/Plan bindings and full snapshots remain retained. Merged
[S2 coordinator](COORDINATOR_ACTIVE_CHECKPOINT.md) and
[S3 member checkpoints](MEMBER_ACTIVE_CHECKPOINT.md) replace bound durable
resident/ordinary-candidate Journals. [S4 retained control](RETAINED_GATEWAY_CONTROL.md)
removes ordinary gateway full-Journal control/Prepare exchanges. Cold replay,
explicit full-history operations and complete SnapshotV1 materialization remain.

Existing cohorts and the default remain `LegacyCountV1`. Only a genuinely empty,
unbound fixed cohort can select the new profile. There is no online conversion,
including at V0, no epoch reset, ID expiry, history GC or automatic quota increase.
Mutation variants and standard Gremlin text do not change. The new versioned
profile tag is carried inside the existing gateway Binding and its canonical
request bytes; legacy Binding bytes remain identical and old peers reject the new
tag. Gateway framing stays v1. This does not add a `g.addV` mutation dialect.

## One identity and a common finite budget

The profile is part of the existing complete `Cohort` identity. Legacy serialized
cohorts omit it exactly as before; the only accepted explicit tag is
`"profile":"ByteBound16MiBV1"`. Explicit legacy, null and unknown tags reject.
Authority, cohort, epoch, mode and profile must agree in Plans, cursors, proofs,
managed endpoints, raw ledger domains, indexes and snapshots. Original Plan v1
bytes are preserved, not re-encoded on an old-ID retry.

The new profile keeps the existing 16 MiB canonical Journal budget, including
reservations for every admitted transaction's future Prepared and Commit/Abort
events. Strict replay checks that budget at every prefix. It also keeps the
16 MiB participant intent budget, per-request/Plan/event limits, D1's separate
4,096 retained-commit slots and 64/128 MiB retained-write tiers. An old external ID
is still recoverable at exhaustion; a new ID can fail capacity without expiring it.

Independent request, transaction and intent count quotas are replaced only for
this profile by conservative structural decoder/iteration bounds:

- Every original event is at least 70 canonical bytes: at most 239,674 events.
- Every Begin-equivalent admission is at least 205 bytes: at most 81,840
  transactions or external request records.
- Every retained intent is charged at least 256 bytes: at most 65,536 records.

These are consequences of the unchanged byte budgets, not promised write counts
or separately spendable quotas. Actual sizes, reserved completion, local D1
capacity, frame/capture limits and deadlines can stop admission much earlier.
There is no promise that 1,025 maximum-size Plans fit, nor that one member can
commit beyond its unchanged 4,096 D1 slots.

## Permanent capacity binding on every member

Before the coordinator admits any new transaction, every fixed member, including
nonparticipants, must durably bind the same profile. This is a permanent common
history-consumption contract, not a per-transaction reservation with orphan cleanup.
It promises logical capacity for any legal common Journal prefix, not free disk,
network availability or successful user-payload preparation.

The trusted local initialization helper first establishes a real Raft leader
barrier and reads the existing durable binding. For a valid empty baseline only,
it generates a 256-bit OS-random nonce before proposing a dedicated activation
command. Deterministic apply fixes that nonce and the first successful activation's
full Raft LogId in the same synced state batch. Every replica applies identical
bytes. Ordinary remote calls cannot supply an activation nonce.

Retries keep the first binding. An uncertain ACK or competing activation is
resolved by reading the winning durable binding, not by replacing it. A different
nonce cannot overwrite an existing activation. Entropy failure produces no proposal.
The first LogId provides ordering evidence; it is not a unique instance identity.
A fresh instance with identical shard/mode/epoch and identical term/node/index
still draws another nonce and cannot consume the old grant.

Nonce uniqueness relies on OS cryptographic randomness in a 256-bit space. An
exact snapshot restore deliberately preserves the original nonce as recovery of
that state. Complete rollback of all relevant databases to consistent old copies
cannot be detected without an independent external monotonic anchor; this design
does not claim otherwise.

The new-profile coordinator server owns the complete pinned member endpoint set.
Its dedicated confirmation request accepts no addresses or grant receipts. It
reads actual member grant status through leader barriers and durable authority
validation, then proposes one canonical `ConfirmCohortCapacity` event containing
the exact ordered cohort and all immutable activation receipts. The ordinary
Command RPC rejects that event on both client and server. Direct privileged Raft
proposal access remains the existing trusted control-plane boundary; receipts are
not signatures or Byzantine authentication.

Journal replay derives readiness from this event. Begin/Admit before confirmation
fail without reserving an ID or ordinal, including during raw replay/recovery.
Confirmation and admission are serialized by coordinator Raft apply, not merely
by a caller preflight. Repeating an identical confirmation is harmless; changing
any receipt is a conflict. A member going offline afterward affects availability,
not the permanent grant. Restart, snapshot or a later leader marker cannot revoke
or replace its generation.

## Preparation, consumption and visibility

For the new profile, managed Prepare first retains any missing authoritative
confirmation/admission events using the existing bounded SyncHistory path. A
locally later history must be strictly prefix-compatible and is preserved.
Prepare then checks the member's local grant, admitted transaction and exact
original Plan. An ordinary raw Prepare cannot reserve an unadmitted identity.
No new archive-bearing Raft command or second synchronization protocol is added.

Members compare their own persisted activation against their receipt whenever
confirmation is observed or recovered. Unknown Abort and nonparticipant decisions
consume no local intent tombstone. Filling payload, intent bytes or D1 slots can
reject preparation while already admitted decisions remain consumable. Successful
Prepare still reserves its eventual Commit data slot/bytes.

Absolute event positions and transaction ordinals never reset. A confirmation
uses an event position but no transaction ordinal. Unresolved holes block the
contiguous resolved prefix; observed resolved history is not applied visibility.
Each member consumes ordinals in order. All members finally certify the same V,
although their pinned event cuts may differ. Old V reads retain their exact
terminal prefix and data; V0 does not acquire future records.

## Storage and wire versions

The shared codec/replay implementation uses explicit profile gates:

- Journal v4, history cursor/page v2 and TerminalPrefix v2 for the new profile.
- The profile originally introduced managed RPC v5; current peers require
  [S4 RPC v6](RETAINED_GATEWAY_CONTROL.md), with no older-version fallback.
  External gateway framing remains v1.
- Raw ledger/terminal-index format 2; current bound coordinator/member disk
  versions are v16/v17. Earlier v13/v15 coordinators and v14 members remain
  strict migration inputs, not alternative current writers. See the
  [archive-reader](AUTHORITATIVE_EVENT_LEDGER.md),
  [coordinator checkpoint](COORDINATOR_ACTIVE_CHECKPOINT.md) and
  [member checkpoint](MEMBER_ACTIVE_CHECKPOINT.md) contracts.
- Member logical D1 version 7 carries its immutable activation nonce/LogId.
- Legacy Journal v1/v2/v3, history/proof v1, raw/index format 1 and disk v11/v12
  retain their published count-bound meanings. Earlier published migration readers
  remain only for those actual legacy inputs, never as mixed-profile fallback.

Cohort is a public Rust struct, so internal Rust constructors must now name its
profile; choosing `Default::default()` preserves legacy behavior. Deploy upgraded
binaries on every Raft replica and every managed endpoint before using new command
variants. An unchanged outer SnapshotV1 does not imply old binaries can replay new
nested Journal/D1 formats or that rollback is supported.

Typed metadata still contains no full Journal. Cold open and explicit full-history
source paths reconstruct raw Command authority before validating state. S1 apply
also reconstructs it; S2 disk-v16 apply instead consumes the validated checkpoint.
Archive-backed coordinator LookupRequest/HistoryPage scan the complete raw namespace
against a private, previously strict-validated published cut, retaining only the
target/page. Their duplicate request index is retired after strict old-index
validation in the atomic migration batch. One synced batch commits
metadata, appended tail/new segments, indexes, graph state and the applied marker.
Snapshot install checks exact local successor/profile/activation, then atomically
rebuilds complete state. Missing, extra, mixed-version, malformed or corrupt rows
fail closed. No cached Journal, derived index or old snapshot repairs ordinary
reads/apply. Existing requested-range terminal reads retain their documented
scope rather than claiming every read detects unrelated future-row corruption.

## Executable bounds and remaining costs

Actual Command/Terminal codecs establish that each complete terminal record plus
separator is below four thirds of its Begin-equivalent bytes. Complete 1..V
terminal evidence, including bounded identity/segment punctuation, is at most
22,435,158 bytes. The managed frame remains 32 MiB. Rows and proof share that one
frame: a proof fitting alone does not guarantee a snapshot response fits. Exact
frame-boundary tests use the actual combined Reply codec; overflow returns an
explicit error, never a partial successful session or silently narrowed result.

Raw rows remain greedy groups of at most 32 events and 4 MiB of canonical event
bytes, with a 2 MiB maximum event. The count/byte partition proof gives at most
7,499 rows and 139,640,832 encoded key/row bytes including the conservative 16 KiB
per-row overhead. These are derived validation ceilings, not a second admission
pool. Existing per-row/index bounds are not raised to fit the extension.

The original profile/S1 implementation retains complete-memory Journal
reconstruction and full replay on persistent apply. Draft S2 replaces that path
only for the disk-v16 coordinator; cold replay, member and explicit full-history
paths, full logical snapshots and O(V) terminal proofs remain.
The 16 MiB Journal budget is not a bound on total process/snapshot memory: D1,
intents, graph data and JSON byte-array expansion are separate. No performance
experiment or throughput claim accompanies this change. Neither S1's bounded
coordinator readers nor S2's consumed coordinator checkpoint remove the remaining
full-archive paths or lifetime budget.
Extending beyond the common lifetime byte budget still needs bounded live state,
authoritative retained ID/Plan/ordinal recovery, and complete streamed checkpoint/
snapshot closure across every member.

## Refactoring and verification

One capacity policy replaces scattered new-profile numeric overrides. Shared
bounded-vector and canonical-size helpers replace duplicated ledger decoders.
Disk versions reuse the same typed journal-less DTO/load/write paths. The just-
replayed Journal/cursor and the staged root are reused only within that immutable
operation: metadata rejects a proof borrowed from any other candidate, even an
equal-valued clone. Every new request/state lock still revalidates persistent raw
authority. The private boxed cohort avoids growing the public Raft command layout
and leaves all encoded bytes unchanged. Member activation shares its fencing
helper with legacy activation. Managed blocking
operations share one absolute-deadline helper so timed-out barriers cannot retain
CPU permits indefinitely. Test fixtures reuse the existing process protocol.

`getrandom = 0.3.4` is an added direct d2-raft dependency, already pinned and used
elsewhere in this workspace. It comes from the official crates.io registry and
is licensed MIT OR Apache-2.0. It supplies fallible OS entropy; no new package
version, custom random generator or toolchain is introduced.

Acceptance requires 1,025 genuine sequential gateway Submit(CreateVertex) commits
on one participating member plus a nonparticipant, exact original-ID/Plan retries,
old-V Gremlin reads, durable restart/snapshot and unknown-ACK recovery. The long
independent-process harness can retain its DBs and atomic progress/identity evidence
for exact retry after interruption. A partial run is explicitly incomplete, not a
pass; raw Journal fixtures or batched Raft tests do not substitute for this run.
Separate debug tests cover old count gates, full intent/D1 capacity, holes, nonce
replay, strict format/corruption rejection, exact 16 MiB reserved resolution and
whole-frame boundaries. The long 1,025-write case is explicitly ignored in debug
and required by the existing CoreHR release CI workflow. Its gate verifies the
optimized test artifact, exact selected test, one passing test with none ignored,
and a completed checkpoint with recovery/snapshot evidence. A debug CI pass alone
is not this acceptance. RPC/session budgets remain unchanged; release evidence is
not a performance comparison. Final frozen-tree tests and both remote CI gates
remain required for delivery.

The harness-only [retained evidence v2 layout](LIFECYCLE_EVIDENCE.md) stores full
proofs and original RequestRecords once, behind a small strictly validated progress
checkpoint. Every pending/verified durable boundary, exact original comparison
and mandatory release acceptance remains. Strict same-identity v1 migration never
resets progress or rebinds old source/ELF evidence. Diagnostic work counters are
not a runtime performance claim and do not change production storage formats.

The unignored `byte_bound_independent_processes_small_lifecycle_recovery` test
adds four sequential gateway writes with a separate coordinator, writer and
nonparticipant process. It preserves first/last original IDs and Plan bytes,
V0/V1/V4 proofs and Gremlin data through real final-ACK loss, process reopen and
fresh-state snapshot restore for all three roles. Immutable member grants and
the nonparticipant's zero local sequence are checked after recovery. This small
ordinary-test case does not replace the unchanged release 1,025-write lifecycle
or CoreHR gates, cross the 256/1,024 history boundaries, or establish throughput.
