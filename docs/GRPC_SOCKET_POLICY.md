# TCP_NODELAY on custom gRPC incoming streams

Related investigation: [#247](https://github.com/ashione/VelaGraph/issues/247).
This is a transport latency policy, **not** a conclusion about the original CI
failure and not a change to read consistency, durability or retry semantics.

## Why explicit accepted-socket configuration is required

Tonic 0.14.9's server and channel builders default to `tcp_nodelay=true`. However,
[Server::serve_with_incoming](https://docs.rs/tonic/0.14.9/tonic/transport/struct.Server.html#method.serve_with_incoming)
explicitly ignores the server's TCP socket settings when the caller supplies its
own incoming stream. The shutdown variant uses the same generic IO path. Our
bounded/lifetime-tracked incoming streams supply already accepted Tokio sockets;
setting the builder option does not configure those sockets.

Set `TCP_NODELAY` on each accepted production gRPC socket before giving it to
Tonic. A socket-option failure is surfaced as an incoming IO error, with the
socket and its admission permit released. The client `Endpoint` default was
already enabled and is unchanged. This avoids Nagle buffering on small outgoing
server frames; it can increase small-packet traffic. It is not a bandwidth,
latency or availability guarantee on every network.

Coverage:

| Incoming adapter | Services sharing the socket |
| --- | --- |
| `d2-raft/raft_grpc/admission::incoming` | Raft Vote/Append/InstallSnapshot |
| `d2-raft/raft_grpc/admission::incoming_with_limits` | driver/control |
| `d2-raft/managed_rpc/grpc` | managed control/member |
| `replicated-graph/remote/paged/grpc` | snapshot, shard query/fixed query, single-shard operations |
| `replicated-graph/remote/distributed/grpc/coordinator_transport` | coordinator observed query/jobs |
| `replicated-graph/managed_gateway_grpc` | public managed gRPC write/query |

Framing, wire versions, message limits, admission limits, deadlines, cancellation,
flush durability and leadership rules are unchanged. Legacy TCP and HTTP adapters
are not silently changed. Tests assert the actual accepted Raft/control sockets'
option, including permit release, rather than relying on a timing threshold.
Existing real-process transport suites exercise the production adapters.

## Controlled experiment (2026-11-13)

Original failure source tree `a28770a920b666f1777c71649d3ce0d68dd263de`, checkout
`e1379ef48107cf5eaf42e6106c9166ef80c2776c` (same tree as event head `b05a67d2`).
Cloud Linux 6.18.57, Rust 1.100.0, debug/test symbols off, incremental off, build
jobs=2. Test process and child nodes inherited CPU affinity `0-3`. This matches
CPU count, not the original GitHub runner's complete hardware/environment.

First, ten unchanged original complete snapshot differential runs all passed
(21.54–22.62 s), without the later bounded-confirmation recovery policy. This did
not reproduce the original failure.

For a single-variable A/B, a local experiment patch made **only Raft accepted
sockets** set `nodelay` to the selected boolean. One otherwise identical binary
was used in alternating off/on/off/on/off/on blocks. Each block started a fresh
three-node cluster, opened and closed 100 empty snapshots through the original
remote API, and timed each Open. Each Open performs fresh Raft quorum confirmation;
Close time was outside the sample. No injected pause, write replay or timeout
change. All 600 calls succeeded. Results, microseconds (sorted sample indexes
94/98 for p95/p99):

| Block | NODELAY | n | median | p95 | p99 | max |
| --- | --- | --- | --- | --- | --- | --- |
| 1 | off | 100 | 2354.5 | 45407 | 49521 | 72672 |
| 2 | on | 100 | 1978 | 2934 | 6188 | 7661 |
| 3 | off | 100 | 2680 | 45794 | 54878 | 60491 |
| 4 | on | 100 | 2135.5 | 2910 | 3181 | 3200 |
| 5 | off | 100 | 2432 | 45901 | 46746 | 48044 |
| 6 | on | 100 | 2231 | 2975 | 3692 | 3760 |

The same binary then ran the unchanged complete 36 × 64 KiB seed and snapshot
comparison, off/on/off/on: **21.90 / 21.97 / 22.10 / 22.86 s**, all passed.
There is **no demonstrated end-to-end seed speedup**. The repeatable microbenchmark
tail reduction establishes the socket setting's effect in this experiment, not
that Nagle/delayed ACK caused the original 102 ms CI quorum failure. There was no
packet capture or failure-time scheduler/I/O trace from that CI run. Keep the
original-cause investigation open.

To reproduce on the original checkout, append an ignored test to
`grpc_snapshot_process.rs` that starts its existing `Cluster`, then repeats
`remote::open_snapshot(cluster.tcp(), Some(cluster.identity()), SHARD, BUDGET)`
100 times, records `Instant::elapsed().as_micros()` for Open only and closes every
returned descriptor. Temporarily set accepted Raft sockets' `set_nodelay(false)`
or `set_nodelay(true)` in the same experiment binary (an environment toggle in a
local patch is sufficient), and alternate six independent test processes with
`taskset -c 0-3`. Never retain that experiment toggle in production. The normal
complete differential test is the separate end-to-end control. Capture all raw
samples, exits, source patch and binary hash; do not retry failed samples away.

[Raw samples and metadata](evidence/raft-nodelay-20261113.json) and the exact
[original-tree experiment patch](evidence/raft-nodelay-20261113.patch) are retained
for review. The patch is evidence, not an applied production environment switch.

Evidence integrity (SHA-256):
- Local experiment patch: `7f271b58f1b8ca501a29b7ccfcb9ae4b7f6cb7cafd91984504faab57a6c850f4`
- Microbenchmark log: `b86835f40e0bf774b6ff21f4b8a4a43d72b8f94b00846b44662848912245d537`
- Complete-seed A/B log: `e31acc1dcd23e5ea096beffb08e944957c5572d8763f24a441c20d732daa5036`
- Experiment graph-node binary: `471e4301eac9d15357bc6b0aad16d910f5bfdc44a8e84073f38952b1f307182e`
- Experiment test binary: `9433be7e5acc327cabe070a57d2f1d405882469b573b3339a6a57d9090c13af6`
These identify the original-tree experiment, not the current production build.
