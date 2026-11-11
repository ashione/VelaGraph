# Command-local retained transaction projection

`AdvanceRetained` first proves that its requested ordinal is resolved at the
supplied frozen history cut. It then projects the original Transaction from the
member's verified immutable archive and accepted original-command overlay.
The generic member advancement check independently compares that whole
Transaction against `MemberHistory` before applying a decision or returning an
old retry.

The durable retained branch now carries the first successful point projection
in an opaque command-local witness. Only its exact ordinal is reused by the
common equality check. All other ordinal, transaction-ID and request lookups
still delegate to the same borrowed history. No generic check is removed, and
the compatibility `AdvanceVisibility` and materialized Journal paths are unchanged.

## Consistency and errors

The witness constructor remains after member identity and the complete
frozen-cut proof, and before the common observation/ordinal checks. It executes
the original complete point projection with the same error mapping. The raw
namespace and accepted overlay still close completely, including records after
the requested target. Private fields and the constructor's concrete
`MemberOverlay` argument prevent a caller from supplying an arbitrary cached
Transaction. Its immutable borrow cannot cross an overlay replacement.

The common complete Transaction comparison, decision, retry, gap, participant,
intent and dedup checks retain their order. State locks, per-command staging,
sync WriteBatch, applied marker and publication are unchanged. No storage/wire
format, capacity, deadline, durability or read-consistency contract changes.

Removing a redundant physical scan also removes any opportunity to observe a
transient I/O failure arising only in that deleted scan. This is not a promise
of identical physical-read failure schedules: every remaining read still
propagates its error, and corrupt original evidence fails closed. There is no
fallback to an unchecked cut or retry after a failed projection.

## Bounds and validation

The witness holds one original Transaction, whose participant and prepared
vectors retain the existing bound of 64. It contains no Command sequence, Plan,
request payload, retained-ID index or persisted cache. It is dropped before this
command publishes its candidate. Reused results are cloned for the unchanged
common equality check, as the original second projection also returned an owned
Transaction. Keeping that function's owned-input interface can add one bounded
Transaction at peak: the witness, the supplied clone and the comparison clone
can coexist briefly, versus the two original projected values. This is not a
zero-allocation or zero-extra-residency optimization.

Focused tests compare the old two-projection transition with the witnessed path,
exercise other-ordinal delegation and forged transaction rejection, preserve
frozen-cut/old-retry/invalid-input behavior, count actual apply walks, and cover
batch overlays, corrupted tails, failed sync and reopen. Full repository and
lifecycle gates remain required.

The ignored local phase benchmark alternates both paths within one release
executable on the same immutable verified cut:

```sh
cargo test --release --locked -p d2-raft --lib retained_projection_benchmark \
  -- --ignored --nocapture --test-threads=1
```

It times first projection and the common advancement checks, including identical
candidate cloning and result comparisons. Fixture setup, initial archive
verification, frozen-cut proof and persistent publication are outside timing.
The synthetic histories contain aborted transactions and set A to half the
history size, including the smallest one-transaction case. This is not a whole
Submit, remote-runner or session-lifetime benchmark.
