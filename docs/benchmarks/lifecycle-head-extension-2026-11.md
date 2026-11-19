# Member history extension at a verified head

This follow-up builds on PR #345 (`0cb6770`). It changes member history-page
merging only. The 1025-write lifecycle, timeouts, recovery checks, original IDs,
Plans, old-version proofs, three snapshot restores and CI tiers are unchanged.

## Repeated work

Member apply first creates `VerifiedArchive` from one pinned RocksDB snapshot.
That operation verifies the complete raw namespace/hash chain, the active
checkpoint projection, and the complete derived terminal index. An accepted
`MemberOverlay` contains only original commands that subsequently passed the
shared checkpoint transition checks.

Previously, even a new page starting exactly at that overlay's current head
walked all old events again to check overlap. There is no overlapping event in
that case. The new path validates the full incoming page and domain as before,
requires equality of the complete head cursor (not just event count), then
passes every new event through the unchanged `append` implementation.

Overlapping pages and retries from an older position still use the original
walk. A same-count substituted root cannot select the shortcut. Coordinator
paths are deliberately unchanged. Full verification still precedes each new
member operation; cold replay and snapshot validation are unchanged.

This is operation-local reuse, with no new persisted field, cross-operation
cache, full-history collection, resource limit, dependency or RPC change. The
shortcut computes the head using the already bounded accepted suffix. It adds
no retained allocation. New-admission identity lookups and other raw projections
can still scan history, so this does not change overall lifetime complexity to
constant or linear time.

## Validation and measurement

The regression compares the original overlap-walk branch with the optimized
branch on both profiles at 31/32/33 and 1024/1025 boundaries, where permitted.
It checks checkpoint, command and cursor equality, exact scan-count reduction,
old-page retry, empty/nonempty pages after accepted overlay commands, forged
roots/domains/counts, invalid final commands and unchanged input after failure.
New operations and cold reopen must still reject corrupted old history.

The opt-in `member_head_extension_cost` benchmark alternates original/optimized
order with two warmups and twelve measured samples per branch. Both arms include
complete member verification before merging the same terminal page, using the
same database, process and binary. The 32/256/1025-transaction fixtures use
Begin/Prepared/Abort histories with out-of-order completion; they are not the
gateway's full retained-Plan workload. Setup, Raft persistence and gateway/RPC
work are outside these measurements.

```sh
cargo test --release --locked -p d2-raft --lib \
  state_machine::event_ledger::tests::member_head_extension_cost \
  -- --exact --ignored --nocapture
```

The first optimization's manual full run
[44444738204](https://github.com/ashione/VelaGraph/actions/runs/44444738204)
targets `0cb6770` and cannot validate this follow-up. Full lifecycle validation
on the new candidate and comparable before/after measurements remain required;
a faster microbenchmark is not evidence of a full-lifecycle speedup.
