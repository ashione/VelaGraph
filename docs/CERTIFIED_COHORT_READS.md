# Fixed-cohort empty-baseline snapshot sessions

Status: experimental trusted in-process KV snapshot primitive, layered on the
participant versioned reads. This does **not** enable GlobalCommittedSnapshot in
the remote graph API, certify imported nonempty graphs, implement indexed MVCC,
or add a durable distributed GC lease.

## Persistent graph boundary

A coordinator may explicitly BindCohort only before its first Begin. The binding
records the coordinator shard and ownership epoch, and a strictly sorted unique
list of 1–64 participant shard/epoch/mode identities. Coordinator mode is already
bound by the journal authority. Members cannot include the coordinator. Exact
binding retries are idempotent; changing the binding or binding after any prior
transaction is rejected. All later Begin commands accept only exact member
identities, including through the low-level command interface. Transaction intent
IDs remain per-transaction identities, not part of the cohort.

A bound coordinator cannot change ownership epoch. Snapshot replacement cannot
remove or rewrite binding history; fresh recovery checks the binding against the
coordinator identity. Ordinary Raft leadership changes are allowed. Membership or
ownership migration needs a future explicit protocol and is not silently inferred.

Journal encoding v1 remains for unbound histories. Bound histories use v2 and must
replay a valid first BindCohort event; version tags and contents must agree. Older
binaries fail closed on v2. Existing D1 metadata envelopes retain their versions,
because the embedded journal is independently versioned. No downgrade is supplied.

## Bootstrap and baseline certification

1. Activate an empty coordinator and bind its intended complete cohort.
2. Enable participant and versioned modes on every member, using that initial
   bound journal. Each member must have sequence zero: no prior data writes,
   even if later deleted. It must match the binding and coordinator authority.
3. Only after activation, submit ordinary managed participant transactions through
   the existing coordinator/prepare/decision/advance protocol.

An old unbound versioned participant cannot be relabelled as bound, including by
snapshot replacement. Nonempty baselines remain usable with the old participant
API but cannot join these certified sessions. Identical values or local sequence
numbers do not certify independently imported databases.

The certificate here is a checked contract across trusted in-process Raft handles,
not a cryptographic signature or adversarial-client authentication scheme. Restoring
an entire old coordinator directory, cloning its identity into another cluster, or
untrusted direct command forgery requires external identity/fencing controls; this
primitive does not solve those deployment threats.

## One chosen version, all members

cohort_reads::acquire takes handles for the complete persisted member list in its
canonical order. Missing, extra, duplicated, or mismatched identities fail. It
observes the bound coordinator journal after a leader barrier and chooses one V,
either explicitly requested or the minimum of that resolved prefix and all
leader-observed member applied prefixes. This selects one common safe watermark,
not the newest coordinator decision that might still be unapplied. V cannot cross
an unresolved decision. An explicitly requested version is never downgraded.

Every member then passes its own leader barrier and captures read_at(V). Under the
same local state lock, the retained terminal transaction prefix must equal the
coordinator observation: authority, cohort, IDs, ordinals, participant sets,
prepared evidence and decisions. Matching only the numeric watermark is not enough.
Every member must have an empty baseline and applied prefix at least V, including
members which did not participate in particular transactions.

No SnapshotSession is returned until all captures succeed. A partial apply, missing
prepare, lagging/nonparticipant shard, incompatible history, identity mismatch or
unavailable node makes the entire acquisition fail, never a partial result. For
an automatically selected version, a lagging shard lowers the common watermark;
for an explicit newer version it causes rejection. The
caller may separately retry a chosen older common version. Current/weaker graph
reads remain unchanged and are not certified by this API.

Writes continue during acquisition and afterwards. Because every captured shard
uses the same immutable prefix V, later writes cannot change the view or combine
old and new transactions. Sequential materialization does not require stopping the
cluster. Local bounded replay may briefly hold a shard read lock and delay writers.

## Bounded local pin and expiry

The returned private-field session owns immutable copied shard snapshots. Its
monotonic-clock lifetime is at most 60 seconds, measured from acquisition start;
all new accessor calls fail after expiry. Acquisition also has a total 30-second
maximum, not a separate timeout multiplied by member count. Explicit post-capture
deadline checks cover synchronous replay overrunning a timer poll.

Rows, live payload bytes and replay operations have aggregate budgets, capped at
1,000,000 rows, 64 MiB and 4,000,000 operations. Each next shard receives the remaining
budget before materialization. Intermediate replay state counts. Exhaustion fails
closed even if a not-yet-read shard might turn out to be empty. Metadata, bounded
journal observations and map allocation overhead are additional to the payload
budget. Failure drops all partial captures.

This is a process-local materialized pin, not a restartable token, renewable lease,
or server-side reservation. A node failure after successful capture does not change
the copied view. Process exit loses the session. Expiry prevents new API access; it
cannot revoke data a caller already copied or a reference previously borrowed.
Dropping the session releases its memory. A caller retaining expired sessions still
owns that memory; there is no background registry or automatic eviction.

Source history still has no GC. Admission rejects new work at the existing journal,
intent or dedup capacity rather than removing versions needed by a read. A future
GC/migration design must introduce durable safe floors and real pin coordination
before reclaiming historical source state.
