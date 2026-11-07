# Durable request point recovery (A2.1)

Status: an experimental derived coordinator index and bounded point-recovery
operation. **The existing 256 external requests, 1,024 transactions and 16 MiB
Journal admission budget remain unchanged.** This does not implement unlimited
write lifetime, history checkpoints, retention/GC, indexed MVCC or large dynamic
query capacity.

## Identity and retry behavior

The complete replay-validated Journal remains the sole authority. An index row
preserves the exact external ID, canonical application request bytes, original
versioned Plan bytes, transaction ID and assigned ordinal. External and transaction
IDs are separate identities; numeric ID order has no admission meaning. The
coordinator authority, ownership epoch and complete fixed cohort cannot rotate.

The first admitted Plan wins. Exact same-ID/request retries return that Plan,
including after terminal decisions, quota exhaustion, uncertain acknowledgments,
process restart or snapshot transfer. Changed canonical bytes conflict. A stale
original CAS is not silently replanned. Unknown does not mean Abort. Missing,
extra or inconsistent indexed identity is a history error, never permission to
forget the original request. Existing partial-prepare, Commit/Abort and visibility
catch-up behavior is unchanged; a known Commit can remain not yet visible.

## Durable layout and atomic publication

Coordinator **disk metadata version 8** retains the same complete D1 state and
Journal formerly stored by disk version 5. In the same `state.db`, a derived
index uses these reserved state-machine keys, separate from graph data:

- `sm/request/` + big-endian u32 index version 1 + big-endian u64 coordinator
  shard + big-endian u64 epoch + 16 mode bytes + 16 external-ID bytes.
- Each strict JSON row has fields in canonical serialization order:
  `version`, `record`, `ordinal`. Record fields are `id`, `request`, `payload`,
  `transaction_id`; byte arrays preserve exact bytes, not a hash substitute.
- `sm/request-manifest` contains `version`, `authority`, `epoch`, `cohort`,
  `requests`, `encoded_bytes`, `applied`, in that order. `encoded_bytes` is the
  sum of canonical row-value and full key lengths. The initially unbound empty
  coordinator has a null cohort. The manifest is not a read certificate.

New index rows, manifest, authoritative Journal/resolution reservations, Raft
applied marker and membership share one synchronous WriteBatch. Memory is
published only after sync, under the existing state lock. Several admissions in
one apply batch use the same Journal overlay, so racing candidates cannot create
two bindings. Rejections and retries create no new index row. Only newly admitted
rows are written; bounded manifest accounting is recomputed from the authoritative
archive. The existing full-Journal staging/serialization costs remain.

Index rows consume additional disk space; the 16 MiB figure is still the logical
Journal admission/resolution budget, not a new total physical disk quota. Local
storage errors are uncertain outcomes, not transaction Abort. No local sidecar,
independent archive service or asynchronous index publication is introduced.

## Reopen and migration

Reopen strictly replays the original Journal, then checks every index key/domain,
record byte array, ordinal, row count/byte accounting and manifest/applied marker
against it. Missing, extra, duplicate-identity, modified or malformed rows,
unknown versions/fields and incompatible manifests reject the database. The
index is not silently rebuilt when an indexed-format database is damaged.

Legacy disk version 5 remains readable. Before migration, a point operation may
copy just the requested record from its validated in-memory Journal. Its first
successful apply atomically writes every derived index row, the manifest and
version-8 marker. A failed sync leaves the complete version-5 state intact. Any
index or manifest found alongside a legacy/noncoordinator format is rejected as
a mixed-format/orphan state. Once version 8 is active there is no legacy fallback.
Older binaries reject version 8; deploy compatible peers together. No downgrade
procedure is supplied.

## Point-recovery RPC

The trusted internal managed RPC protocol is now **version 3**. Version-1/2
messages are rejected, not retried through an older or weaker operation. The
external managed graph envelope version, request schema and canonical application
command codec are unchanged. Two new typed D1 errors, RequestIndexHistory and
RequestIndexUnavailable, can propagate through its application-error response.
Older strict clients may reject those unfamiliar error variants as protocol
errors; they must not interpret them as absence or Abort. Updated clients are
needed to distinguish them precisely. No authentication, TLS or cryptographic
attestation is added.

LookupRequest accepts the fixed authority/epoch and external ID. After a leader
barrier, it returns the authority/full cohort, resolved prefix, requested external
ID, and either absence or the original request record plus current transaction
state. The index and transaction are read under the same state lock. A point
lookup compares its stored row to the authoritative original record and ordinal;
an indexed missing key cannot become absence. The manifest is checked against
the small retained manifest from the same published state. Reopen validates the
whole range; point reads validate the requested row, not every unrelated row.

The caller independently validates the envelope, exact requested identity,
Plan version/bounds, cohort, transaction ID/ordinal, complete participant list,
prepared bits and terminal-state consistency. Canonical request bytes are compared
exactly at the graph boundary. GraphCohort recovery and post-admission winner
readback use only point recovery; admission uses the bounded Summary plus point
lookup rather than fetching the archive. Server Command validation, execute,
status, prepare and advance still use the complete Journal. This is not a general
write-path optimization. Existing 32 MiB frame and per-record bounds remain.

## Snapshots and rollback boundaries

Logical snapshot version 1 and Journal codec versions 1/2/3 remain unchanged:
the complete authoritative history is included. After the existing strict
snapshot decode and successor validation, installation deletes/rebuilds the
entire derived request range in the same batch as data, metadata, applied marker,
membership and stored snapshot. Failed sync leaves the previous complete state
and index intact. An explicit valid snapshot may replace a damaged derived
index; point/reopen paths do not quietly repair it.

A newer Raft marker cannot authorize dropping or changing an older ID/Plan,
coordinator history, participant intent, retained version or commit mapping.
Pending and partly applied transactions are retained. Old-V session semantics
and A1 terminal proofs do not change. Restoring an older complete database
directory is still undetectable without an independent trusted durable anchor.
This index adds no backup, anti-rollback anchor or history checkpoint hash.

## Gates before any future lifetime-cap removal

Moving request rows alone cannot remove MAX_TRANSACTIONS, participant intent and
D1 retained-write limits, full participant Journal copies, whole-memory snapshots,
eager reopen, historical replay budgets or O(V) terminal evidence. Those need
separate versioned storage/protocol changes and measurements. Do not describe
request 257, a higher constant or a disk split as unbounded operation.

If participant decision ledgers gain independent finite quotas, every admitted
ordinal needs reserved decision-consumption capacity on **all cohort members,
including nonparticipants**. Prove a common deterministic reservation bound or a
replicated credit/reservation protocol before removing lifetime bounds. A
coordinator-only reservation cannot guarantee visibility catch-up at exhaustion.
Keep the prepare-time D1 commit-history reservation as well. Physical disk failure
remains distinct from logical quota admission.

Any later GC additionally needs a proven lifecycle for pinned old V, unresolved
and partly applied transactions, lagging replicas, snapshots and old-ID retries.
This slice performs no live history GC, expiry, ID reuse or epoch reset.

## Validation

Golden tests cover key/row/manifest bytes, old/new coordinator metadata and the
unchanged logical snapshot. Tests cover same-batch competing candidates,
out-of-order external IDs, changed canonical commands, full-request quota with
old retries/reserved Abort, atomic legacy migration, indexed corruption/orphans,
snapshot replacement/failure/history rejection, and real process death after
sync before admission/migration/snapshot publication. RPC fake peers verify
point-only recovery, exact identity/schema validation and old protocol rejection.
Existing managed graph and independent-process scenarios retain partial-commit,
lost-ACK, original-Plan recovery and stable old-V coverage. These are correctness
tests, not a throughput or 200,000-person dynamic consistency claim.

The next [durable terminal-segment foundation](DURABLE_DECISION_SEGMENTS.md) uses
bound coordinator disk version 9 and bound member version 10. The request index
remains unchanged; terminal proofs now validate their requested persisted segments.
All request/transaction/Journal caps and full archive retention still apply.
