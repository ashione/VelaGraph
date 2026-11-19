# Coordinator terminal-prefix projection design

Status: contract and implementation independently reviewed. Fresh local build
and d2-raft library tests pass (396 passed, 8 ignored); all-target d2-raft
Clippy also passes with warnings denied. Not published or merged.
Baseline: main f62711c9d9fb8a0e5ba30b067371075d73d92b81.
Scope: #108 active coordinator `coordinator_terminal_prefix` only. No wire,
format, publication, purge/install, RPC, or legacy recovery changes.

## Existing contract to preserve

The state publication read lock spans the operation. The current operation
loads durable state before validating caller identity. Its relevant order is:

1. Compare durable metadata with published metadata, then decode and compare
   the applied marker with the published marker.
2. Validate persisted metadata. For archive coordinators reject the retired
   request namespace, then validate the complete raw history and hydrated
   state against the published cut. These failures map through
   `ledger_read_error`, including retired-namespace failures on this path.
3. Check shard, epoch, mode, coordinator role, then requested version against
   the resolved prefix (`VisibilityUnresolved` on an excessive version).
4. Check the decision manifest, then requested decision rows in ascending
   segment order. Each intersecting row is compared in full, including records
   after the requested version within its last segment. Only then is the row
   truncated for the response.

Version zero still validates all raw history and the manifest, but reads no
terminal rows. A damaged unrelated later decision segment does not invalidate
an earlier coordinator prefix. This differs intentionally from the compact
member proof API. Raw history corruption anywhere invalidates every version.
No partial success, fallback to a cached result, repair, or retry is permitted.

The optimization removes complete Journal materialization, not the complete
raw read or output-proportional memory. No bounded-total-work claim follows.

## Reusable authority and necessary changes

Use only a privately minted coordinator `PublishedArchiveCut` under the
publication lock. Legacy and non-archive representations retain the old path.
A byte-identical published metadata header and matching applied marker anchor
its existing validated authority; arbitrary checkpoint-shaped disk data cannot
select the fast path.

Extend the existing coordinator row verifier with an explicit observer mode.
The default mode must keep its current sealed-row decode elision. Projection
mode decodes every already witness-checked row and feeds its events to the
existing `OrderedTerminals`, in the same namespace iteration. Validate the
projection against the checkpoint at completion. Do not add a second walker or
call `verify_active` followed by a separate projection walk. The test-only
original-verifier branch must forward equivalent observations too.

Use the existing terminal Collector for the response. It is output, not an
independent authority. Never terminate raw closure when its requested output
is full or invalid.

### Reviewed seam A: snapshot validation order

`PublishedArchiveCut::snapshot` currently checks decision manifest before raw
closure and maps retired-index errors to RequestIndex errors. Reusing it here
would change observable failures. Do not globally reorder it: other callers
have their own contracts. The coordinator-prefix path needs a narrowly scoped,
reviewed snapshot-anchor helper that preserves the order and error mapping
above, with manifest validation explicitly deferred. Metadata, applied marker,
raw rows, and decision rows must all come from the same pinned snapshot.

### Reviewed seam B: requested-row verification without Journal

`decision_segments::prefix` depends on `View` holding a full Journal;
`CheckpointRows` verifies the entire member namespace. Neither is a drop-in
replacement. A range verifier must use the existing row/domain/manifest codec,
consume projected terminals through the last intersecting segment (bounded by
the resolved frontier), and compare each complete requested row. Keep at most
one additional segment, beyond response records and active projection state.
It must not inspect unrelated later rows.

An index failure observed while projecting must not short-circuit raw closure.
Preserve it for reporting only after successful raw validation AND caller
identity/version validation AND manifest validation. Preserve ascending-row
failure precedence. A naive callback using `?` would incorrectly let a damaged
index mask later raw corruption. This deferred-error protocol requires its own
review before implementation.

## Acceptance gate before enabling the fast path

Use existing coordinator range/member namespace tests in
`decision_segment_tests.rs`, archive recovery prefix tests, original-verifier
comparison fixtures, and Collector tests as regression foundations.
Add differential old/new-path cases for both capacity profiles:

- V0, first ordinal, inside a segment, exact boundary, multiple segments;
- unresolved early holes and later terminal decisions;
- malformed/missing/extra raw rows, including suffix damage beyond the request;
- damaged unrelated later decision row still permits an earlier prefix;
- damage after the requested ordinal inside its intersecting row fails;
- malformed/missing/changed manifest, retired request namespace, changed
  metadata, and applied-marker mismatch;
- wrong shard/epoch/mode and excessive version combined with raw and index
  faults to prove the precedence above, including IO error classifications;
- reopen and legacy fallback remain unchanged;
- successful active requests allocate no complete Journal, increment no full
  hydration counter, and perform exactly one raw namespace walk;
- same-snapshot behavior and no disk/publication mutations on failures.

## Implementation boundary

The active branch adds a coordinator-only snapshot operation and preserves the
manifest mutex acquisition after raw and caller checks. The existing snapshot
helper and its callers are unchanged. The coordinator walker now accepts an
optional event observer; its default sealed-row optimization remains unchanged.
`RequestedRows` retains at most one decoded requested segment and the first
index error, while Collector retains only requested output. Raw verification
and projection completion precede caller validation, manifest validation and
reporting that saved error. No complete Journal is constructed.

Tests compare the new operation against a retained test-only copy of the old
full-history path. Both profiles cover reopen, unresolved holes, V0 and segment
boundaries, range-external damage, a canonical last-record change outside V but
inside its requested segment, all raw/manifest/index fault combinations against
identity/version rejection, metadata/applied/retired namespace failures, and
manifest mutex poisoning. Valid reads assert zero hydration and one raw walk.

No format, wire, RPC, mutation, purge/install, lifecycle or legacy path change.
Local verification on 2026-11-18: independent static review found no blocking
issue; `cargo fmt --all --check`, `git diff --check`, fresh d2-raft library build,
and `cargo test --locked -p d2-raft --lib -- --test-threads=1` passed
(396 passed, 8 ignored). `cargo clippy --locked -p d2-raft --all-targets --
-D warnings` also passed. Exact-head CI, ignored lifecycle tests and full workspace
acceptance are not claimed. IO injection and concurrent-snapshot stress are not
newly implemented by this patch; existing storage error mappings and snapshot
ownership are kept.
