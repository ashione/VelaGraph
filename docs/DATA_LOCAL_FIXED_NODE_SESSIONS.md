# Fixed-version data-node sessions (M1 internal protocol)

This is the node half of an additive, query-bound managed read path. It does not
change observed `OpenQuery`, managed `OpenSession`/`Read`/`Traverse`/`TraversePage`,
consistency negotiation, graph storage formats, or the existing capture budgets.
A complete gateway admission and result-sealing implementation is also required;
a node session by itself is not a complete-cohort read certificate.

## Trust and identity

The graph service may use `GraphService::start_managed` to serve the exact same
Raft and state-machine handle as a managed-RPC endpoint. Graph and managed ports
remain explicit server configuration. Do not derive a graph port from a managed
port or let query text select replicas. The gateway must bind every graph
endpoint/group/node to the exact managed Client, Target and full Binding.

Fixed operations require an explicit matching graph identity, even on group zero.
`FixedDescriptor` is separate from the observed descriptor and contains graph
identity, shard, epoch, local sequence, node incarnation, random token, authority,
complete cohort/profile, fixed version and managed mode. Exact descriptor equality
is required; the same numeric V or local sequence does not permit substitution.
Tokens and terminal proofs are not authentication or cryptographic proof against
a malicious peer. The transport remains plaintext, trusted-network only, with no
TLS, authorization, tenant isolation, automatic failover or forwarding.

## Open and capture

The gateway must first check the supported AST, complete persisted cohort (at most
64 members), partition=shard mapping and all fixed endpoint bindings. It must
admit one fixed version using the complete-cohort protocol, then run `preflight`
for **every** member before opening any member. Unsupported scopes must not leave
node sessions behind. Fixed open transports the exact `TerminalPrefix`, remaining
aggregate `ReadLimit`, remaining TTL, and acquisition duration (at most five
seconds). The gateway's absolute expiry includes all admission and acquisition.

The complete encoded JSON request and worst-case successful response must each
fit the existing 16 MiB graph frame body. The worst-case response includes the
actual full proof plus maximum-width descriptor and cost fields. The new open
operation uses a 16 MiB client receive limit; old query/open replies retain their
1 MiB + 1024 byte limit. Fragment replies keep the old small reply bound. The
32-bit transport length header is outside the frame body's existing limit.

The ByteBound profile permits a canonical terminal proof of up to 22,435,158
bytes, so this transport does **not** cover every otherwise valid retained
history. Capacity rejection is explicit. There is no hash-only certificate,
segmented proof transport, truncation, or fallback.

A node starts its lease before preflight/barrier/capture, establishes a leader
barrier and reserves one shared slot and the full remaining capture byte bound.
It calls `certified_terminal_read` once. Data and the actual retained proof come
from one state lock; empty baseline, identity, applied prefix, exact proof and
version-to-local-sequence mapping are checked. The returned entries move into
`ReadView`, rather than cloning a retained snapshot. The gateway receives the
actual proof, descriptor and capture dimensions, never the raw key/value map.
Only after capture and all checks does the registry publish the node session.

The initial aggregate parity limits remain 10,000 KV rows, 2 MiB payload and
100,000 replayed D1 WriteOps. The gateway opens members serially and subtracts
actual retained dimensions from the next allowance. The underlying replay checks
live rows/payload after every Put, including data later deleted. Final small
results cannot bypass the capture peak. `CaptureCost.rows/bytes` are **final
retained** dimensions; `replay_operations` counts D1 operations. Neither is a
measurement of replay peak, certification CPU, lock time or RSS.

## Resource bounds and certification costs

Observed and fixed node sessions share the same process-wide eight slots and
128 MiB logical payload pool. Fixed capture reserves its entire permitted payload
before replay and releases only unused payload afterwards. Close/expiry removes
registry reachability; an active worker's Arc still owns its view, slot and byte
permits until that worker ends. Cancellation does not stop synchronous work or
refund its resources. The fragment worker pool is also shared.

These are logical payload bounds, not total-memory bounds. Additional bounded
costs include BTreeMap metadata, descriptors/cohorts, full proof objects and
serialization clones, RPC decode/encode frames, and retained result buffers.
Existing graph connection admission limits these overlapping operations.

Certification has a separate existing bound: retained history remains subject
to its 16 MiB journal budget including terminal reservations. Legacy history has
at most 1,024 transactions and 67,585 structural events; ByteBound history has
at most 81,840 transactions and 239,674 structural events. These structural
ceilings do not promise that the corresponding histories fit in the byte budget.
Active durable certification can load and reconstruct the entire retained event
history and a temporary Plan-bearing Journal, even when reading an old small V.
The 100,000 D1 replay cap does not count that work. This slice does not change
history bounds, add a new certification cache, or introduce purge/install/GC.
Synchronous certification is not preempted by an expired async timer; deadlines
are checked again after it returns. Certification memory, CPU and lock-time
measurements must be recorded separately, or reported as unknown.

## Fragments and failure

M1 permits only Exists, Out, In, ResolveIncoming and VerifyReverse fragments,
reusing the existing metered graph operations. Global scans, jobs, filters and
projection fragments are rejected. The gateway supports only its separately
preflighted narrow standard-Gremlin subset; a fragment API is not a new language.

Fragments execute serially with both the node's cumulative ledger and the
remaining global allowance. `request_sequence` starts at zero. A duplicate,
skipped or out-of-order sequence poisons the session instead of replaying work.
Any execution/budget failure also poisons it. Consumed work is retained after
failure; it cannot be recovered by replay, a new allowance, or an unknown reply.
A gateway with an unknown response must fail the whole query, without replacing
a replica, reopening a session, or selecting a new V. Failed sessions remain
closable and otherwise expire at their original deadline.

Before and after each required fragment, current shard/epoch/managed mode is
checked. Required descriptor accesses or validations that fail cause whole-query
failure. After the final required access, a data-node fault may be unobservable
while the gateway seals its retained complete result. Do not promise detection of
every node restart before Ready. Gateway close/expiry remain strict; Ready fetches
must use the retained result, without requiring the data nodes to remain alive.

## Acceptance boundary

Node unit tests cover V0 and old V across later writes, exact descriptor fields,
foreign proof rejection, Put-then-Delete capture peaks, sequence/budget poisoning,
wrong partitions, expiry/incarnation/epoch, shared permits retained by a cancelled
worker after close, frame boundaries and a real fixed graph RPC. The transport
has a separate synthetic large-proof test; it is not a certified managed fixture.

Complete input-model graph oracles, gateway all-page rejection/replay, fault timing,
real multi-process managed tests, exact-head ordinary CI, observed full CoreHR and
fresh release1025 remain required for an integrated M1 release. Parity at 2 MiB
does not prove crossing that boundary. No 205,000-vertex managed capacity, unlimited
write lifetime, or completion of #110/#111 is claimed.
