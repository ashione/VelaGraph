# Exact segmented terminal-prefix evidence

Status: experimental A1 read-evidence separation for the trusted fixed-cohort
managed gateway. It does not raise request, journal, graph capture, frame or
session limits; provide GC or indexed MVCC; or enable the older remote
`GlobalCommittedSnapshot` interface.

## Deterministic contract

The durable Journal remains the only decision source. Its independently versioned
v1/v2/v3 event archive, exact external request IDs, canonical request bytes,
complete original Plans, admission reservations, participant prepare checks and
advance/recovery commands are unchanged. Existing records are not rewritten or
removed. Exact retries, uncertain ACKs and crashes retain their existing meaning.

Read evidence is a separate **version 1** strict JSON projection. Its fields, in
canonical serialization order, are `version`, `authority`, `cohort`, `through`,
`segments`. Authority is the coordinator mode; the complete cohort includes the
coordinator shard/epoch and all sorted member shard/epoch/mode identities. Each
record contains, in order, `id`, `ordinal`, `participants`, `prepared`, `decision`.
Participants retain their exact shard/epoch/mode/intent. Prepared bits are retained
for Abort as well as Commit. Neither external request bytes nor Plan bytes occur
in this projection. There are no hashes, signatures or remote boolean certificates.

Only a complete checked prefix is usable. Ordinals must be exactly 1..V in order,
transaction IDs nonzero and unique, participant lists nonempty, strictly sorted,
unique and members of the full cohort, and intent IDs nonzero. Prepared-bit length
must equal participant count; Commit requires every bit true. Decisions are only
Commit or Abort. The enclosing `through` binds the entire sequence and segment
count: each segment has exactly 32 records except the last, whose exact length is
determined by V. Missing/duplicate/reordered/extra records or segments, unknown
fields, unsupported versions and any inconsistent count fail closed. Individual
segments are not certificates and cannot be independently admitted or cached as
validated prefixes. V=0 has no segments but still requires the same full identity,
cohort, empty baselines and complete member capture.

Projection walks only the requested prefix of Journal's incrementally maintained
transaction state. A later unresolved transaction or large admitted Plan does not
enter an old-V proof. An unresolved ordinal at or before V fails. Work and evidence
size remain O(V times participant count), capped by the existing 1,024 transactions
and 64 participants. This is exact bounded segmentation, not constant-size evidence,
a delta-transfer/cache protocol or a journal storage reduction. Wire allocation is
bounded by the unchanged 32 MiB frame limit before receive allocation; encoding
stops at that limit. Strict proof decoding additionally enforces the finite record,
segment and participant caps. Proof metadata/allocation overhead remains outside
the graph payload capture budget, as journal observations were previously.

## One cut, all members

1. After a coordinator leader barrier, obtain only its bounded authority, cohort
   and resolved-prefix summary. This path neither clones nor serializes the archive.
2. Check every configured member against the full persisted cohort and obtain every
   member's leader-observed applied prefix and certified zero baseline. Choose one
   V: the explicit requested version, or the minimum safe watermark. Never lower an
   explicit V.
3. Obtain the coordinator's exact terminal projection at that V after a new leader
   barrier. Check its authority, epoch, whole cohort and exact V against the earlier
   observation. Concurrent append-only writes may proceed between observations.
4. Send that projection to every member. Under the same participant state read lock
   as `read_at(V)`, derive its retained projection, require applied >= V and baseline
   zero, and compare the entire checked evidence exactly. Return both rows and the
   actual retained projection, never just a successful boolean.
5. The client independently compares that returned evidence and all snapshot
   identity/version/budget fields. It creates the private SnapshotSession only after
   every member succeeds within the original aggregate budgets and total deadline.

A lagging nonparticipant still blocks an explicit newer version. Missing prepares,
unresolved holes, changed terminal identities/prepared bits, wrong cohorts, corrupt
or substituted proofs, unavailable members and oversized captures reject the whole
acquisition. No partial session, proof-cache shortcut or weaker-read fallback exists.

## Replay, snapshot and wire compatibility

The proof is derived, not another persisted checkpoint. Recovery still strictly
replays the original Journal events and validates participant applied decisions,
local D1 history and global-to-local commit mapping. Snapshot replacement still
requires exact archive extension, retained versions and non-regressing applied
prefixes. A derived proof cannot raise the stored watermark or certify missing data.
Reopen and snapshot-install tests compare newly derived evidence and historical rows.
Whole-directory rollback still cannot be detected without an independent durable
anchor; this slice does not add one.

Internal managed RPC envelopes introduced **protocol version 2** for Summary and
Terminal operations. Prefix/Read proof payloads use the checked schema above.
Version-1 clients/servers are not interoperable; version mismatch or unknown
operations/payloads fail closed, with no automatic full-journal or weaker fallback.
Deploy compatible peers together. Durable disk formats and the external managed
Gremlin protocol are unchanged. Explicit legacy local Journal-taking Rust helpers
remain compatible, but the new acquisition path does not call them or fetch and
then discard a full Journal.

The later [A2.1 point-recovery index](DURABLE_REQUEST_INDEX.md) advances internal
managed RPC to version 3 and coordinator disk metadata to version 8. Terminal
proof encoding and these read semantics are unchanged; version-1/2 RPC peers are
now rejected rather than used as a fallback.

These checks detect mismatched trusted-peer evidence. The channel remains plaintext
and unauthenticated; a malicious server can fabricate both rows and evidence. This
is not cryptographic attestation or a production network security boundary.

## Validation scope

Tests cover canonical golden encoding and segment boundaries, empty/unbound/future
and unresolved prefixes, corruption and substitution, unknown versions, cross-
authority/cohort/epoch changes, old-V immutability, v2/v3 journal replay, excluded
archive payloads, participant reopen/snapshot reconstruction, old wire rejection,
complete-cohort/nonparticipant lag, original-ID recovery and real independent-process
captures concurrent with continuing writes. Measurements must distinguish old-V,
latest-V, full durable archive bytes and read proof bytes. Prepare/advance still
transport full history; their remaining cost must not be presented as solved.

See [frozen A1 measurements and validation limits](TERMINAL_PREFIX_RESULTS.md).

The [durable terminal-segment index](DURABLE_DECISION_SEGMENTS.md) now backs actual
fixed-cohort Summary/Terminal/Prefix/Read paths. Its disk versions 9/10 retain the
full Journal authority and exact A1 proof schema; old explicit legacy formats may
project until atomic migration. Indexed target corruption never falls back to that
projection. Proof work remains O(V) and all limits above remain unchanged.
