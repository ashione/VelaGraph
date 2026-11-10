# Operation-local HistoryStatus verification

The internal managed `HistoryStatus` RPC reports a participant's observed event
cursor and applied ordinal A. Observing a resolved decision alone does not
advance A. This metadata is not a read certificate or an authenticated proof.

## One validated publication cut

For an active durable member, the RPC previously read visibility metadata and
then history status through two independently locked getters. Both getters
called `verify_active`, which scans all retained raw events and verifies the
member's derived terminal rows. The scan is bounded by the existing history
capacity, but its cost grows with retained history. Two getters repeated that
work and could observe different publication cuts.

The active-member RPC now obtains both results in one internal state-machine
operation. It retains one state read guard and one operation-local
`VerifiedArchive` while constructing visibility identity, cursor and A. The
RocksDB snapshot is verified against the private published cut, including
metadata, applied marker, raw ledger, terminal projection/manifest and rejection
of retired request-index keys. No verified result is cached across requests.
Apply and snapshot publication require the same state's write lock, so neither
can interleave the fields of this observation.

The RPC retains its leader barrier, request admission/deadline, pinned
role/target, authority/cohort, empty-baseline and cursor/A checks. There is no
wire/disk format change, capacity increase, new retry, full-Journal materialization,
fsync change, or skipped fault check. Existing public getters retain their
behavior. Legacy and in-memory state without an active archive witness uses the
original two-getter compatibility path, preserving validation/error order.
Other RPCs and independent requests still perform their own verification.

## Tests and focused measurement

Tests count actual active-member verifier calls per node, including consecutive
real TCP requests, and forbid full-Journal materialization. They cover observed
history ahead of A, retained application, old sealed raw/terminal corruption,
metadata/marker/manifest/retired-index corruption after a successful call,
reopen, identity rejection, and legacy/in-memory fallback. A synchronized reader
and queued apply verify that the response is wholly before publication; the next
request observes the complete successor.

An explicit ignored benchmark compares the old two public getters against the
single active operation on the same durable fixture and binary. Setup and history
population are outside the measured region. Each size uses seven samples per
path, ten observations per sample, alternating order. It checks every cursor and
A, and asserts exactly two versus one full verification per observation. Timing
has no pass threshold. Test-only counters and the inactive synchronization gate
are included in these measurements; neither is compiled into normal serving.
Run it separately from other CPU/I/O-intensive tests:

```sh
cargo test --locked --release -p d2-raft --lib \
  history_status_validation_benchmark -- --ignored --nocapture
```

The fixture uses 0, 32, 256 and 1024 aborted transactions, with half consumed into
A. It measures local read/verification cost, not network latency, gateway Submit,
full lifecycle throughput, cold-cache recovery or multi-host performance. The
optimization halves this RPC's repeated scans; it does not remove the remaining
linear scan or establish an end-to-end speedup.
