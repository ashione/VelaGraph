# Single-shard operations over typed gRPC

> Current transport baseline: main `40e2b9a` (#279, 2026-11-14). Supported
> graph, coordinator, Gateway, managed-member, Raft and control network paths use
> typed gRPC; old JSON/TCP library listeners and fallback are retired. Public HTTP
> remains intentional. Historical transport details below are not a supported
> compatibility mode. See [current RPC audit](RPC_MIGRATION_STATUS.md).


Tracking: [#254](https://github.com/ashione/VelaGraph/issues/254), part of [#218](https://github.com/ashione/VelaGraph/issues/218).

The graph gRPC listener exposes a separate versioned service for `Probe`,
`Metadata` and `Write`, alongside snapshot/query services. It calls the existing
application handlers directly, not a TCP/JSON proxy. Protobuf contains typed D1
requests/results/errors; the storage bytes in write operations retain their
original meaning. It does not change Raft or durable formats.

## Semantics

- **Probe** checks configured identity/shard and a leader read barrier. It is not
  authentication and does not open or copy a snapshot.
- **Metadata** returns the selected replica's locally applied shard/epoch/sequence.
  It is deliberately not upgraded to a fresh linearizable read.
- **Write** retains the original request ID, expected sequence, epoch, batch
  validation, duplicate detection and application outcomes. Clients do not retry,
  forward or fall back to legacy TCP. A timeout or lost reply after submission is
  an **unknown outcome**, not proof that the write did not happen. Preserve the
  original identity when later resolving that outcome.

Identity matching protects against accidental routing, not an untrusted caller.
This is plaintext development transport behind the existing
`--allow-insecure-development` guard, not production authorization.

## CLI

Use the address printed as `graph=...` by `graph-node --graph ADDR`. This is now
the sole typed graph endpoint. The old `--grpc-snapshot` split-port option is
removed; no automatic protocol detection occurs.

```sh
graph-client --allow-insecure-development rpc probe \
  --endpoint 127.0.0.1:7401 --group 1 --node 1 --shard 7
graph-client --allow-insecure-development rpc metadata \
  --endpoint 127.0.0.1:7401 --group 1 --node 1 --shard 7
graph-client --allow-insecure-development rpc write \
  --endpoint 127.0.0.1:7401 --group 1 --node 1 --shard 7 --file write.json
```

`write.json` is a `D1WriteRequest` using the existing serde representation. JSON
is only the local CLI input and output format, never the internal wire format.
Use valid graph storage records when writing graph data. These low-level commands
are not a graph schema importer. Input files retain the CLI's 1 MiB cap;
`--timeout-ms` must be 1 through 60000, default 3000. Neither is a graph process
memory limit. Arguments and write shard are checked before dispatch.

## Limits and migration boundary

The service shares the graph listener's admission and lifecycle rather than
adding an independent unbounded pool. Existing logical frame and D1 batch limits
remain separate from protobuf wire/allocation limits: compact encoding must not
silently permit a previously oversized logical request. The 16 MiB logical, wire
and decoded-allocation budgets are independent. The allocation prescan charges
256 bytes per nested object, so extreme collections of tiny operations can fail
earlier than the old logical frame limit. Old-frame acceptance is not a promise
of byte-for-byte acceptance under the new allocation guard. Library clients retain a reusable channel; connect and each operation have their
own bounded budgets. The CLI wraps connect plus its one operation in a single
total timeout. Already committed mutations cannot be undone by
cancelling a response.

The default `RemoteStorageNode` catalog/routing chain also uses typed RPC after
#256. Explicit library legacy/parity services remain separately inventoried;
`graph-node` never opens those listeners. #218 remains open for other legacy
coordinator/gateway/Unix paths. This is not a multi-host HA or performance claim.

## Validation

Required gates are codec/strict-input tests, differential handler outcomes,
separate-process typed operations and CLI checks, incompatible-endpoint rejection,
admission/deadline/shutdown tests, independent review, and exact-head CI/CoreHR.
The PR records actual passed, failed and not-run stages; protocol selection alone
is not evidence of correctness or throughput improvement.

### Failure evidence

The real-process shard-operations fixture adds RPC/phase context without replacing
underlying errors. On failure, panic, startup failure or cancellation it reports
each started child's PID and exit state before cleanup, plus at most the last
16 KiB of its combined stdout/stderr. This synthetic fixture does not log request
payloads or tokens. Successful runs suppress these tails. The normal CI test log
artifact retains the output under its existing retention policy.

A generic `Unavailable` string does not prove that a write occurred, nor identify
a read-quorum, transport or process failure. The main failure in run 43519111727
is recorded in #247 as related evidence, not a proven recurrence of its original
snapshot-barrier cause. These diagnostics do not replay requests, extend budgets,
relax assertions or establish a root-cause fix.
