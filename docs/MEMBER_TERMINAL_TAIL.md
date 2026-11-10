# Operation-local member terminal-tail reuse

The durable member apply path needs the previous partial 32-transaction terminal
segment before staging its successor. Its existing archive verification already
reconstructs every original transaction in terminal order while validating all
persisted terminal rows. Apply now retains the final partial segment from this
same verification instead of running a second raw projection over that snapshot.

## Proof boundary

The interval uses the publication cut's consumed frontier **A**, not the observed
resolved prefix **R**: `(A - (A mod 32), A]`. A divisible by 32 has no tail.
`OrderedTerminals` emits each original Transaction after its contiguous terminal
reduction. The callback first performs the unchanged derived-row comparison,
then moves only the selected record into a bounded vector. Full raw namespace
closure, checkpoint equality and derived-row closure all finish before that
vector or the archive can be returned. Collection checks cannot mask earlier
verification errors.

The caller still reads the original tail key and invokes
`check_checkpoint_tail` with the same identity, full applied LogId, manifest,
stored bytes and original transactions. If live A differs from the cut's A, it
follows the old projection and checker path, preserving malformed-state errors.
No valid publication takes that fallback.

Nothing changes the state lock, command staging, accepted overlay, candidate
identity, sync WriteBatch, applied marker or post-sync publication order. No
preflight at a different cut is removed. Ordinary archive readers collect no
tail. The data is neither persisted nor reused across operations.

## Resource boundary

At most 31 Transactions survive the verification callback. Their participant
and prepared vectors retain the existing per-transaction bounds. No original
Commands, Plans, request payloads or retired-ID index is collected. This is a
bounded **additive peak allocation** during verification: the tail can coexist
with the remaining active projection and decoded decision row. Previously, the
tail vector was constructed after that projection had been released.

## Verification

Focused tests compare whole Transactions with both the original independent
projection and Journal truth, cover segment/capacity boundaries and malformed
inputs, and count full raw walks on actual multi-entry member apply. Eligible
apply calls remove exactly one walk; sealed/empty tails remove none. Existing
storage, snapshot, TCP and lifecycle gates remain required.

The ignored `member_apply_tail_fusion_benchmark` alternates old and fused paths
on the same verified cut, including identical terminal-tail validation. Run:

```sh
cargo test --release --locked -p d2-raft --lib \
  member_apply_tail_fusion_benchmark -- --ignored --nocapture --test-threads=1
```

It measures verification and prior-tail preparation only. It is not a whole
Submit benchmark, a remote-runner result or a session-lifetime guarantee.
