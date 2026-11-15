# Owned gateway gRPC channel

`managed_gateway::GatewayClient` is the reusable Rust domain client for one
fixed public gateway address and one exact `Binding`. It is a connection-reuse
slice of #221, not a multi-gateway load balancer or session failover mechanism.

```rust,ignore
use replicated_graph::managed_gateway::GatewayClient;

let client = GatewayClient::new(address, binding);
let shared = client.clone();
let reply = shared.request(operation, deadline).await?;
```

Construction performs no I/O. The first request lazily establishes a channel;
clones share that channel and a single cold-connection initializer. Keep the
client within the Tokio runtime that drives its transport. There is no global
endpoint cache or background discovery. Drop all clones when the endpoint is no
longer needed; an in-flight call may still own its channel until it completes or
is cancelled. Idle channels occupy a server connection slot, so callers must
bound their own set of client objects. Creating a client per operation defeats
reuse. The original free `managed_gateway::request` remains a one-shot helper
using the same implementation, not a second protocol.

## Bounds and identity

Every request still obtains the shared process-wide eight-call admission permit
before connection work, encoding and dispatch. Cold-channel waiting, connect,
HTTP/2 readiness, the RPC and response validation share the caller's original
absolute deadline for waiting on a result. The endpoint buffer and transport concurrency stay bounded
by the existing eight-call limit. A failed/cancelled initial connection does not
poison the client: a later **explicit call** may try connection setup again. No
failed RPC is replayed by this client. A shared channel may still perform
background transport reconnection after one caller times out, bounded separately
by the transport connection timeout. The caller deadline bounds waiting for the
RPC result; it is not a promise that all channel background work has stopped.
A per-call body guard rejects DATA that Hyper has not yet polled when the call
expires or is cancelled. It cannot retract bytes already buffered by HTTP/2 or
the socket, and an expired call can still have an unknown write outcome.
The outbound body retains the admission lease until it is consumed or dropped,
even if its caller has already timed out. Thus a blocked transport cannot evade
the eight-call budget by accumulating abandoned request bodies. A saturated
channel fails admission rather than accumulating an unbounded backlog. This is
not a process-wide bound on all caller-owned idle channels. If the peer never
releases its stream quota, these leases can remain occupied until the transport
recovers or closes. Caller timeout is not immediate transport-resource cleanup.
The guard cannot retract a frame already returned to Hyper, including a race
where cancellation follows its final deadline check.

The address and binding cannot change after construction. Each operation gets
its own correlation nonce and sends context version 2; replies and typed errors
must echo both nonce and exact authority/cohort/mapping. Reconnection can create
a fresh socket only to the same address. It is not discovery of a new owner or
an authenticated peer identity check. A process reusing that address cannot make
an old cursor/session/job valid: the existing incarnation-bound token checks
still apply, with no re-open-at-latest fallback.

The public server may close a whole HTTP/2 connection when a response stalls at
its deadline. Sharing a channel therefore shares that failure domain: other
multiplexed calls can fail. The client does not convert that failure into success
or retry. A failed write can have an unknown outcome; recover the original
external ID and exact command through the existing Status contract. Dropping a
client or timing out an RPC is not a write rollback.

## Acceptance boundary

The connection regression uses the real public gRPC adapter through a counting
TCP relay, so success responses alone cannot masquerade as channel reuse.
Tests cover repeated/clone calls, cold-call admission and deadlines, disconnect
without mutation replay, and the existing response/session identity contract.
A single-stream HTTP/2 quota with acknowledged SETTINGS provides a real queued
body barrier: repeated expired Submit calls must not reach full body dispatch
after the quota is released. This controlled-peer test does not claim that a
request already delivered to a real gateway can be cancelled or rolled back.
Record exact test results for the candidate before claiming this acceptance has
passed. No multi-machine throughput, dynamic routing, automatic retry, HA or
session migration is implied. Internal graph/member channels have their own
identity and resource contracts; this does not replace them.
