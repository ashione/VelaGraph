# Query-bound data-local fixed-V gateway experiment

This additive M1 path retains graph data on explicitly selected participant
processes. It does not increase capture/history budgets or enable the older
observed GlobalCommittedSnapshot consistency option. It is not indexed MVCC,
full managed CoreHR capacity, failover, TLS/authentication or a durable read lease.

## Trusted configuration and protocol

`Gateway::bind_remote_local_queries` requires the original `RemoteConfig` plus
one `LocalGraphRoute` per member in canonical cohort order. Each route supplies
an explicit graph endpoint, graph group/node identity and the **complete** managed
identity matching that member's configured Client. Role, target shard/epoch/mode,
authority, entire cohort including capacity profile and partition=shard all match.
Endpoints are distinct and cannot be supplied by queries or inferred from ports.
The service is plaintext and must be isolated to trusted callers and peers.
Tokens and terminal-prefix evidence are not authentication credentials or
cryptographic proof against a malicious peer.

The additive operations are:

- `OpenLocalQuery { version, ttl_millis, query, page_size }`: execute and validate
  the entire immutable result before publishing its `LocalSession` and first page.
- `FetchLocalQuery { session, query, cursor, page_size }`: replay any known page,
  including the first page (`cursor: None`), without opening or contacting nodes.
- `CloseLocalQuery`: close the retained result. Local sessions are strongly typed
  separately from old materialized sessions and observed node descriptors.

The official Gremlin AST admits explicit nonempty `g.V(ids)`, up to three `out`
or `in` hops, and optional terminal `path` or `count`. Other syntax fails before
any node session allocation. The existing shared executor preserves argument
order, duplicate roots, edge order, qualified paths, canonical/reverse/endpoint
validation and default first-100 non-count semantics. This is a narrower entrypoint
than old managed Traverse, not a silent substitute. Rows retain `Unverified`
adjacency completeness, separately from their certified V.

## Admission, execution and results

The cohort is at most eight members. Admission selects exactly one V from the
original complete cohort and retains original endpoints, authority, cohort,
complete terminal proof and deadlines. This admitted cut is not itself a read
certificate. Every graph member captures once with its same-lock certified
terminal read after a leader barrier. The gateway receives each actual retained
proof and compares it in full with the original coordinator proof, together with
all descriptor identities. No graph KV is downloaded through this query path.

Before opening any node session, the gateway checks every complete Open request
and worst-case successful reply against the graph RPC's 16 MiB frame boundary.
Fixed-open replies have their own receive branch; old frame limits are unchanged.
Some valid maximal ByteBound histories exceed this transport cap and fail Capacity.

All required fragments run serially over the original immutable descriptors.
Request sequences start at zero and advance exactly once per attempt. Duplicate,
skipped, failed or unknown calls cannot replenish node/global ledgers or cause a
reopen, replica switch or V refresh. Required descriptor access or validation
failure fails the entire query. After the last required access, a node failure
may be unobservable while the gateway seals results. Ready and subsequent Fetch
rely on the gateway's retained result, not live nodes.

An atomic edge replacement is a later certified write. Existing descriptors and
pages retain the original qualified edge records at V; queries opened at the
new version see the replacement, including a change of source owner with the
same numerical edge ID. The legacy managed Traverse executor has its own lazy
path representation. This entrypoint continues to use the distributed executor.

The entire result is at most 10,000 rows and 1 MiB; page size is 1..=1000. Every
complete page envelope, including Binding, original query, V/session, cursor and
maximal request-id encoding, must fit 1 MiB before Ready. Cursors are independent
random 256-bit tokens scoped to the exact result. Close/expiry are checked at
lookup, page creation, after encoding and before send; bytes whose transmission
has already begun cannot be revoked. Every replay preserves the
original query, size, V and expiry. A lost Open reply is not a Fetch retry of a
known session; no automatic Open retry or durable resume is provided.

## Resource and deadline boundaries

Aggregate captured rows/bytes/D1 operations remain 10,000 / 2 MiB / 100,000.
Each member receives the remaining capture budget, which also bounds its replay
intermediate state. Final retained capture counters are not peak measurements.
The node's observed and fixed views share the existing eight-slot/128 MiB pool.
The gateway's old/new retained sessions share its existing four reservations;
workers continue owning reservations after close/expiry/cancellation. Fixed opens
also share the existing two-slot observed coordinator execution gate, including
certification, fragments and sealing. Old gateway operation concurrency is unchanged.

Proofs, raw retained history, serde clones, fragment scratch and full certification
are additional bounded costs, not included in D1 replay counts or the gateway's
payload reservation. Active terminal certification may reconstruct all retained
raw history and a temporary Plan-bearing Journal even for a small old V. Existing
profile history byte/event limits remain in force. Synchronous work is not
preempted by async timeout; checks after completion prevent an expired success.
No CPU/lock-time/RSS reduction is claimed. Measurements not present in an evidence
report are unknown, not zero.

TTL is at most 60 seconds from initial admission, with the original acquisition
window and query deadline throughout. No spool, global scan, history GC, purge,
install, durable format change or dependency on #124 is introduced. This parity
slice cannot demonstrate crossing the old 2 MiB capture boundary or completion of
#110/#111; write/bootstrap/history capacity remains an independent blocker.
