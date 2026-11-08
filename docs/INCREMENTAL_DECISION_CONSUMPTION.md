# Incremental decision-history consumption

Subsequent live-storage format: [authoritative event ledger](AUTHORITATIVE_EVENT_LEDGER.md)
replaces embedded bound Journal disk headers with v11/v12 raw-event authority.
The original slice below documents its historical formats; logical full-history
retention, snapshots and all lifetime limits still apply.

Managed graph catch-up now transfers bounded event suffixes and consumes decisions
from each member's retained history. It no longer carries the complete Journal in
every `Advance` RPC or Raft command. This is an internal protocol change, not a
new public query language, arbitrary public history API, or unlimited-write mode.

## Authority and limits

The complete replay-validated Journal remains the sole authority on both the
coordinator and every fixed-cohort member. Original external request bytes and
original Plans remain retained exactly. The existing **256 request identities,
1,024 transactions and 16 MiB reserved Journal** limits remain unchanged. No
history, unresolved transaction, old ID, or old read version is expired or GC'd.

The event position is the exact length of the durable original Command vector.
It is not the maximum terminal ordinal, contiguous resolved prefix, or participant
applied watermark. It survives crash and snapshot because the entire vector is
persisted. Reopen fully replays that authority and recomputes its cursor. There is
no second persisted counter, root cache, or independently trusted ledger index.

`SyncHistory` can observe decisions beyond applied, including across unresolved
ordinal holes. It never publishes graph data, terminal local status, commit-map
entries, or a new applied watermark. It validates the complete candidate against
all local intents, including prepared identities beyond applied. A conflicting
future descriptor fails before any application-state publication.

## Cursor format and hash assumption

History cursor format v1 contains `version`, complete `authority` (coordinator
shard and mode), the ordered full `cohort` (coordinator epoch and every member's
shard/epoch/mode), absolute `event_count`, and a 32-byte SHA-256 root. Counts are
bounded by `1024 * (64 + 2) + 1` original events. Unknown fields/versions and invalid
cohorts are rejected. A bound Journal already contains the initial BindCohort;
the identity-bound zero cursor exists for precise prefix hashing, not epoch reset.

The v1 hash bytes are:

- Root zero: SHA-256 of `d2-raft/journal-history/v1\0`, a u64 big-endian length,
  then compact canonical JSON `{authority,cohort}` in that field order.
- Root i: SHA-256 of `d2-raft/journal-history/event/v1\0`, root i-1, the u64
  big-endian absolute 1-based event index, u64 big-endian canonical byte length,
  and compact canonical JSON of the original typed Command.

The hash format is fixed independently of Journal encoding v2 versus v3. Canonical
Command bytes include exact request/Plan byte arrays; supplied overlapping Commands
are also compared by exact typed equality. Hashes are always recomputed from the
locally retained fully replayed Journal. They never authorize deleting history,
accepting a cached boolean certificate, or skipping the original semantic replay.

Omitting an old prefix from a transfer explicitly replaces its byte-for-byte
transport comparison with the collision-resistance assumption of SHA-256. This is
an integrity/equality commitment, **not authentication, a signature, or a Byzantine
consensus proof**. The internal transport still requires the existing trusted
network, pinned full identities, and leader barriers. A1 final read evidence remains
the unchanged complete exact terminal projection through the selected V.

RustCrypto `sha2 = 0.10.9` is pinned from the official crates.io registry. Its
license is `MIT OR Apache-2.0`; it provides reviewed SHA-256 rather than a custom
hash implementation. Cargo.lock pins all transitive versions and registry
checksums. No new toolchain or build-time code generator is required.

## Bounded transfer and exact retries

A leader-barrier member HistoryStatus samples the cursor and applied watermark
under one state lock. Coordinator HistoryPage verifies that base cursor against its
actual Journal. The first page pins the coordinator's current full event count/root
as `cut`; all continuation requests use that exact cut, even if new events append.

Each page carries format v1, base/end/cut cursors and original Commands for the
half-open event range `[base.event_count, end.event_count)`. It contains at most
32 events and 4 MiB of summed canonical Command bytes, separately bounded by the
transport frame. Existing admitted individual events fit because Admit is capped
at 2 MiB. The total event bound is unchanged. Empty pages are legal only when
base, end and cut are identical. Nonempty remaining history must make progress;
a short page is not completion unless its end equals the exact frozen cut.

The receiver rejects a base beyond its local event count. It verifies base against
local authority, compares every supplied overlapping Command exactly, and appends
unknown Commands using the original strict replay and reservation checks. A no-op
idempotent command appearing at a new event offset is invalid. Missing, reordered,
extra, conflicting or malformed events/anchors cannot be silently ignored. The
supplied event chain must produce end, and a locally available cut must match.
An unseen future cut is only a transfer target: receiving an intermediate page
does not establish that the entire cut is retained.

Each SyncHistory command stages the candidate under the state-machine lock and
persists it with the Raft marker and existing derived indexes in one sync batch.
Failure publishes no application-state change; Raft may still record an application
error at its normal applied-log position. Resending an already present page is safe
only after the same exact overlap/anchor checks. An unknown ACK is retried once
with the identical page/cut, inside the existing overall deadline.

## Consuming retained decisions

AdvanceRetained carries the full target/authority identity, one absolute ordinal,
and the frozen cut cursor, with no archive. Under the same apply lock it verifies
that cut against the local Journal and replays its prefix to establish that the
requested ordinal lies within its contiguous resolved prefix. It preserves any
locally observed history beyond the cut and invokes the existing advance logic on
an unpublished candidate containing that complete retained Journal.

New application must be exactly applied+1; holes and out-of-order decisions cannot
be overtaken. Exact already-applied retries validate cut identity/history before
returning the current applied watermark. The original Commit prepare/data capacity,
Abort negative-identity and nonparticipant checks remain in force. Graph data,
commit mapping, applied prefix, derived terminal segments and Raft marker publish
atomically in one batch. An old V still reads only its old commit prefix.

GraphCohort::advance_to uses this flow for **every** fixed-cohort member, including
nonparticipants. Each member pins its own event cut after its cursor is observed;
those event cuts may differ. Every member must still certify the same selected
absolute ordinal V. It finishes with the coordinator's exact Terminal(V) projection
and each member's exact Prefix response. A matching cursor or watermark alone is
not the read certificate. Admission, Prepare, decision recovery and status paths
still use full Journals; this slice does not claim those dependencies are removed.

## Upgrade and recovery

Internal managed RPC is now **v4** with explicit HistoryStatus, HistoryPage,
SyncHistory and AdvanceRetained operations and response tags. v1/v2/v3 peers are
rejected. There is no new-protocol failure fallback to old wire or archive-bearing
catch-up. The explicit low-level full Advance helper remains available in v4 for
existing internal callers, but managed graph catch-up does not use it.

New Raft command variants require upgraded binaries on every member of each Raft
group before using the new flow. Old strict decoders cannot replay these variants.
Use a coordinated upgrade; mixed old/new managed peers are unsupported. Upgrading
does not migrate/reset a cohort or allow rollback of observed history.

No durable encoding change is necessary: all new states are already representable
as a full valid Journal plus its unchanged applied prefix. Journal v1/v2/v3,
coordinator disk-v9, bound member disk-v10, existing segment keys, and complete
snapshot formats remain unchanged. Explicit legacy index migration remains as
before. Reopen/snapshot validate the complete authority and derived indexes, not a
stored hash. Snapshot installation still requires exact history successors and
nonregressing applied/commit-map state; a newer Raft marker cannot erase a page
that was durably synced but not yet consumed.

## What remains before lifetime bounds can change

This reduces full-archive transfer and Raft-command payload dependence in catch-up.
It does **not** reduce retained authority storage or full-snapshot size. Root
verification and cut-prefix replay are still O(retained history), within the
unchanged bounds. There are no throughput or scale claims here.

There is no new independent member quota. Every member replays a prefix of the
same bounded coordinator Journal under the same reservation rules. Unknown Abort
and nonparticipants allocate no local intent; successful prepare already reserves
future local Commit data history. Logical capacity does not guarantee progress
through physical disk-full/fsync failures, whose outcomes remain uncertain.

Removing lifetime bounds still requires a genuinely authoritative segmented ledger,
exact old-ID/original-Plan recovery, snapshot closure, and durable pre-admission
capacity grants or an equivalent common-bound proof across the complete cohort,
including nonparticipants. Coordinator-only capacity is insufficient. Failed-before-
admission grants must not become unreclaimable orphan reservations. None of those
future guarantees is implied by this transport slice.
