# Bounded public service drain

The loopback `graph-service` host supports bounded removal of one service
process. This is request draining, not transparent gateway failover or a
mixed-version rolling-upgrade guarantee. The existing fixed cohort and server
configuration still apply. See [service ingress ownership](PUBLIC_SERVICE_CLI.md).

## Lifecycle

On Ctrl-C or Unix SIGTERM, the host starts drain on its HTTP and gRPC adapters
before awaiting either. The adapters stop accepting new connections and reject
new requests, including requests arriving on an existing connection after the
admission fence. Already admitted HTTP requests and gRPC calls may finish and
deliver their responses within the earlier of their original deadline and the
drain deadline. Draining never resets a caller deadline or extends a session
lease.

`--drain-timeout-ms` sets the maximum request-drain interval, from 1 through
60000 milliseconds; the default is 5000. Both listeners use the same removal
phase rather than serial full grace periods. At the deadline, remaining work
and response delivery are cancelled. Process scheduling and final task cleanup
mean this is not a real-time exit SLA. Dropping adapter handles or shutting down
the underlying gateway remains the immediate-stop fallback.

## Client obligations

- Finish active sessions before removing their owner if continued pagination is
  needed. Drain does not wait for a whole cursor lease or job lifecycle, persist
  jobs, transfer sessions, or forward an old token to another gateway.
- A response lost during removal does not prove that a mutation was rolled back.
  Recover its status using the same original request identity, not a new ID or
  automatic arbitrary mutation replay.
- A new gateway may open a new explicit view. It cannot relabel an old owner's
  cursor or silently substitute its current view.
- Remove the endpoint from ingress selection before termination in an external
  deployment. There is no built-in fleet load balancer, shared session store,
  restored-graph activation, or automatic health-based routing in this slice.

No public administration or drain HTTP/RPC endpoint is added. Process signals
and library adapter handles control termination; public APIs remain graph
operations. SIGKILL cannot perform graceful draining.

## Evidence and limits

The independent-process test
`process_drain_finishes_admitted_request_and_bounds_stalled_body` uses the real
`graph-service`, three managed node processes and another independent gateway.
HTTP `Expect: 100-continue` establishes that a request body is already being
read before SIGTERM. The test waits until both listeners reject new connections,
then finishes the body and checks the complete successful read-probe response.
It verifies that the other gateway remains usable. Separate cases leave a body
unfinished to verify bounded termination and a shorter caller timeout remains
shorter than the drain grace period. These are controlled loopback tests, not
multi-host failure, network partition, all-cancellation remote resource
reclamation, or write-ack delivery guarantees. Library adapter tests separately
cover protocol-level request admission and cancellation behavior.
