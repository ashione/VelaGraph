# D2 TCP resource limits and trust boundary

Status: bounded prototype transport, **not a production-secure service**.
These limits apply to `crates/d2-raft/src/tcp.rs`, not to the separate read-only
UDS graph gateway, storage internals, or an authenticated remote graph API.

## Protocol and compatibility

`TCP_PROTOCOL_VERSION = 0` explicitly names the existing newline-delimited JSON
format. There is no version field or negotiation handshake. Requests and replies
retain their existing serde enum representations and pinned openraft 0.9.25
payloads. For example, a control status request is `"Status"` followed by LF.
There is one request and one reply per TCP connection; pipelining is unsupported.
A future incompatible format needs explicit negotiation or a separate endpoint.
No on-disk migration is involved.

All four directions (Raft request/reply and control request/reply) have a
64 MiB encoded frame cap **including the terminating LF**. JSON escaping counts
against the cap. Existing valid frames within this cap are unchanged. EOF without
LF is now rejected as an incomplete frame instead of being interpreted as a
complete request. Previously uncapped oversized control replies and oversized
outbound encodings now fail. Do not depend on mixed-version peers for these
resource guarantees: old peers still have the old unbounded read behavior.

The bounded reader checks each buffered slice before reserving/copying it and
fails when the delimiter cannot fit within the limit. Its input buffer is fixed
size. The JSON writer checks before each output append; it never serializes an
unbounded temporary string/vector and checks escaped output bytes, not merely
source string length. Capacity growth requests never exceed the frame cap;
allocator metadata/rounding are outside that accounting. Recursion limits from
serde_json remain enabled. Decode failures do not dispatch a request.

## Admission, deadlines, and shutdown

- Each node has at most four admitted handlers on its Raft listener and four on
  its control listener. Completed handlers are reaped. Admission stops before
  accepting another socket when full; pending connections stay in the OS backlog
  and are subject to OS limits. No task, waiter, or frame is allocated for them.
  There is no fairness guarantee; slow peers can consume all slots temporarily.
- At most sixteen outbound exchanges run concurrently **per process**, shared
  by control and Raft callers. Excess calls fail immediately, without encoding,
  connecting, or spawning a waiter. This is deliberate prototype backpressure,
  not a production traffic-priority policy or a per-tenant quota.
- An outbound exchange has one three-second deadline, starting before encoding
  and covering connect, write, read and decode. Raft calls use the shorter of
  three seconds and `RPCOption::hard_ttl()`. A server handler has one three-second
  deadline from admission through read, decode, dispatch, encode and write.
  Receiving occasional bytes does not reset either deadline. Time in the OS
  backlog precedes server admission; callers have their own total deadline.
- Serialization checks the same deadline during output. Decode and synchronous
  storage work cannot be preempted by Tokio; bounded input and a post-decode
  deadline check prevent dispatch after expiration, but a scheduler stall or
  synchronous storage stall can delay actual cancellation. This is not a hard
  real-time CPU/storage bound.
- `TcpNode::shutdown()` stops both listeners, aborts and joins all their active
  connection tasks, then shuts down Raft. No detached connection handler is
  intentionally retained. Once the shutdown future is polled, an owned cleanup
  task finishes even if the caller cancels its wait. Cancellation drops sockets
  and frame allocations.
  Dropping the node's stop sender also signals its listener tasks to stop.

These are bounds on admitted transport work and encoded buffers, **not a bound on
whole-process RSS**. Deserialized structures, application values, openraft work
already submitted, snapshots, RocksDB caches and allocator overhead are separate.
The legacy 64 MiB limit is intentionally preserved and permits substantial
memory consumption under load. Requests exceeding it fail rather than being
silently truncated. Oversized encoded AppendEntries batches return openraft's
typed `PayloadTooLarge` hint so replication can reduce the number of entries.
A multi-entry batch that exhausts the encoding time budget gets the same hint;
network timeouts do not. Single entries must fit both byte and encoding-time
budgets; the transport cannot split them.
This change does not add chunking for an individual oversized log entry. Control
`Write` rejects combined key/value UTF-8 bytes above `(64 MiB - 1024) / 4` before
submission, reserving room for byte-array JSON expansion and the Raft envelope.
That conservative cap is an intentional tightening of the legacy control API.
The public raw `raft` handle is still caller-managed: callers must bound single
entries so they fit the wire cap. Snapshot transfer retains openraft's default
chunk sizing; its pinned version does not expose a public byte-size retry hint.

A timeout/disconnect/shutdown after dispatch is an **uncertain outcome** for a
write or membership change. Cancelling a handler cannot roll back work already
submitted to Raft. The legacy control `Write` has no caller idempotency identity;
a retry can create another log entry. Versioned D1 retry identities retain their
separate application-layer contract. A failed response encoding also cannot undo
a committed mutation. No timeout error is proof that a mutation did not commit.

## Security boundary

Both ports remain unauthenticated plaintext TCP. Any reachable peer can send Raft
messages; any reachable control client can read values, write, initialize a
cluster, add learners, or change membership. Control-supplied member addresses
can cause outbound connections. There is no TLS, identity check, authorization,
replay protection, tenant isolation, or safe exposure to an untrusted network.
Binding to loopback alone does not authenticate other local processes.

Run only inside a trusted, isolated test environment with network access controls
for **both** ports and all advertised peer addresses. Do not publish either port
to the Internet or use this control endpoint as a production administration API.
Bounded frames and connection counts reduce accidental or malicious resource
amplification; they do not prevent denial of service or make hostile clients safe.
Connection failures intentionally do not log attacker-supplied JSON payloads.
Production readiness needs a separately reviewed authentication/authorization,
encryption, address policy, resource/CPU budget, and operational design.

## Validation

`tcp::framing_tests` verifies exact limits, delimiter-less rejection before
consuming extra input, incomplete EOF, unchanged legacy status bytes, escaped
output limits and slow-frame total deadlines without sockets. `tests/tcp_limits.rs`
uses real sockets for slow requests/replies, oversized control replies,
simultaneous incomplete connections, shutdown cleanup, database reopen, and a
normal committed write/read after a slow request expires. Existing D2
multi-process, membership, failover, restart, snapshot and D1 rejection tests
remain mandatory. These tests must fail, not skip, when a CI environment cannot
provide the sockets they require.
