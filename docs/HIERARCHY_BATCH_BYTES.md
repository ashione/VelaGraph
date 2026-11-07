# Exact-byte hierarchy batches

The earlier coordinator grouped each partition's leaf fragments into blocks of
128. That row count is not a serialized-size bound: legal 64-byte edge labels
and wide numeric identities can make a `VerifyReverse` or `ResolveIncoming` block
exceed the existing 16,384-byte fragment admission limit. Short labels in a
particular workload do not prove that the general boundary is correct.

A real regression uses two independent data processes and 128 cross-shard edges
with legal 64-byte labels and wide u64 edge IDs. The unmodified PR77/main engine
rejects its outgoing count with `ResourceExhausted`. The fix keeps both limits:
no more than 128 leaves and no more than 16,384 actual encoded bytes per fragment.
It counts the same compact serde JSON representation the receiver validates,
including the envelope, decimal widths, UTF-8 bytes and JSON escapes.

The packing pass moves each existing leaf into exactly one packet and carries
its original output index. It does not invent, coalesce or deduplicate operations.
The existing per-shard serial ordering, cross-shard execution, aggregate budgets,
fixed snapshots and response-integrity checks remain in force. A measurement,
individual-leaf, budget or deadline failure returns no partial batch result.

## Regression coverage

- Valid wide-ID `VerifyReverse` and `ResolveIncoming` leaves split by bytes.
- Legal arbitrary UTF-8 and escaped property keys are measured correctly; label
  grammar is unchanged. No special field names or CoreHR dimensions are used by
  the implementation.
- A valid batch exactly 16,384 bytes is accepted; appending one more leaf splits
  it. A single over-limit leaf fails rather than being truncated.
- The existing 128-leaf ceiling still splits small leaves; all indices and leaf
  payloads survive, and the measured-byte/work charges are asserted exactly.
- Actual incoming and outgoing counts preserve 128 paths and 64 distinct
  vertices; paginated identities preserve duplicate multiplicity and order.
- Missing reverse/canonical records in a later packet remain whole-job failures,
  with no partial count. Snapshot stamps and repeated Advance/Read responses are
  checked by the real-process harness.

CoreHR remains a regression workload, not an execution special case. Full-scale
count and property-page CI must continue to pass under the unchanged budgets;
there is no promise that arbitrary requests bypass those bounds.

## Local validation

Against the PR77 baseline plus this fix, the unchanged 200,000-person /
5,000-department fixture passed all 29 count-matrix outcomes (including the two
expected-error probes) and all 18 ordered identity/property-page outcomes.
Both runners used their existing cumulative budgets and strict validators.
The full fixture remains 205,000 vertices and 454,998 relationships; it is not
reduced to accommodate the new accounting pass. These are correctness and
budget-admission results, not a controlled performance comparison.

The regression suite also passed 133 library tests and all three distributed
process tests, including the wide-edge test that fails on the unmodified engine.
Formatting and all-target Clippy passed. Normal and full CoreHR CI remain merge
gates on the final integrated commit.
