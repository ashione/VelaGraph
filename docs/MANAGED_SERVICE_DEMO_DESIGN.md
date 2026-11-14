# Fresh managed service demo: proposal and design

Tracking: [issue #224](https://github.com/ashione/VelaGraph/issues/224),
[PR #284](https://github.com/ashione/VelaGraph/pull/284), milestone 1
(v0.0.1 clean-startup acceptance).

Status: proposed implementation and executable acceptance gate. Final acceptance
depends on the exact candidate and merge-tree CI, not this document alone.

## Problem, scope, and options

A user needs to start a small managed graph, obtain public HTTP/gRPC endpoints,
write data and query it without invoking a private ignored test as a launcher.
Existing activation helpers demonstrate protocol rules, but are not a public
startup or recovery interface.

| Option | Benefit | Tradeoff / decision |
| --- | --- | --- |
| Invoke the private ignored fixture | Reuses test setup immediately | Couples users to test internals and a harness lifecycle; not a supported startup path. Rejected. |
| One in-process-only demonstration | Less process supervision | Cannot demonstrate a distinct service and independent node processes or their shutdown. Not selected for this gate. |
| General production bootstrap/restore | Eventually needed for deployment | Requires durable membership, recovery fencing and readiness contracts beyond this preview. Deferred, not simulated. |
| Explicit fresh-only independent-process demo | Small honest path using existing activation semantics and real public service | Single-host, single-voter, no restore; these limitations are explicit. **Selected.** |

The demo is a bounded user-facing example, not a production bootstrap command.
It starts two participant processes (shards 7 and 8), one coordinator process
(shard 9), and a separate actual graph-service process. Each shard has one voter;
three processes do not imply a replicated three-voter HA group.

## Interface and state

Implementation:
`crates/replicated-graph/examples/managed_service_demo.rs`.

Required arguments:

- `--data-dir PATH`: the directory must not exist, even if it would be empty.
  Its parent must already exist.
- `--service-bin PATH`: path to the matching graph-service executable.
- `--allow-insecure-development`: explicit acknowledgement of plaintext,
  unauthenticated loopback operation.

Optional `--http ADDR` and `--grpc ADDR` default to `127.0.0.1:0`.
Non-loopback listeners and unknown/repeated arguments reject. Dynamic ports
avoid assigning a shared fixed development port.

The parent creates the root directory atomically with create_dir; each node
also creates its own previously absent directory. An existing directory or
symlink is not permission to activate, restore, delete, or overwrite its data.
The internal node mode only accepts the three demo shard IDs and retains the
same fresh-directory requirement.

State transitions are:

1. Validate arguments and executable; create a fresh root.
2. Launch each independent node and initialize its single-voter durable group.
3. Apply existing Activate and EnableManagedWrites rules. The coordinator
   enables its coordinator role and binds the cohort; participants enable their
   role and versioned reads with the same cohort journal.
4. Bind actual managed and graph RPC services, then record node readiness.
5. Generate the matching service configuration and launch graph-service.
6. After valid service readiness, publish one JSON line on stdout and supervise
   children until cancellation or failure.

There is no alternate activation algorithm or test-only network shortcut.
The chosen constants describe this demo topology, not a new engine restriction.

The single stdout announcement includes `pid`, `http`, `grpc`,
`node_pids`, `service_pid`, `binding`, `fresh_only: true`,
`restore_supported: false`, and `production_ready: false`.
Logs go to stderr; the service's own announcement is kept in service.jsonl.
Readiness checks the launched PID and bounded, complete JSON (64 KiB maximum).
A service announcement is not a durable recovery marker or proof that any
future request will succeed.

## Lifecycle, failure, and rollback

Each child readiness wait has a 30-second deadline; this is not a single
30-second deadline for the entire sequential startup. Cancellation during
startup drops process guards, which kill and reap already started children.
Failed startup retains directories and diagnostics rather than deleting them.

During serving the parent monitors child exits. Ctrl-C or SIGTERM initiates
cleanup. On Linux it stops the public service first, then nodes in reverse
order, allowing up to five seconds per child before kill/reap fallback.
Cleanup errors produce failure rather than a successful shutdown claim.
Non-Linux fallback uses process kill; the current signal acceptance gate is
Linux-specific.

Rollback means stop the demo and preserve its files. It does not mean reopening
the same directory with an older binary or automatically undoing durable
activation. A subsequent demonstration needs another fresh path; this launcher
refuses both live and stopped existing data directories.

SIGKILL, parent process abortion, machine loss, and power loss cannot execute
Rust Drop cleanup. Child survival or retained partial directories is therefore
possible in those cases. This example does not establish parent-death signals,
process-group supervision, automatic orphan recovery, or production service
management. Inspect the announced PIDs and retained logs when cleanup did not
run. Do not interpret the presence of data as permission to restore it with
this fresh-only tool.

## Validation and evidence

The dedicated integration test
`crates/replicated-graph/tests/managed_service_demo_process.rs` launches the
user-facing binary through `VELAGRAPH_MANAGED_DEMO_BIN`, not a private test
bootstrap. Although marked ignored for the ordinary test suite, the dedicated
clean-startup CI gate must explicitly execute it and supply the exact-build
binary; a missing binary is an error, not a silent skip.

The test contains these acceptance assertions:

- An existing empty directory is rejected promptly, without serving or adding
  files.
- A fresh launch announces distinct parent/service/node PIDs, loopback HTTP
  and gRPC endpoints, and truthful scope flags.
- A real HTTP create-vertex request is observed by a generated gRPC query
  returning the qualified vertex and a nonzero version; its session closes.
- A second launch against the live directory rejects, preserves a sentinel,
  and leaves the original queried version unchanged.
- SIGINT exits successfully and all announced child PIDs disappear.
- A later launch against the stopped directory still rejects and preserves
  the sentinel.
- Bounded stdout/stderr evidence is retained on CI failure when configured;
  graph data is not copied as diagnostic output.

Non-loopback rejection and startup-cancellation cleanup are implementation
contracts, not claims that the listed end-to-end test exercises every failure
interleaving. SIGKILL recovery is explicitly outside the supported contract.
The HTTP assertion plus subsequent gRPC read proves the tested write became
visible; it is not a general guarantee that every HTTP 200 means a final
committed decision.

Before closing the milestone gate, associate the final exact head, CI run,
independent review and verified merge tree with #224/#284. Compilation,
startup, cross-protocol behavior, negative-directory cases and child cleanup
must all pass. This design makes no claim of production readiness, HA,
multi-host deployment, restart/restore support, or authentication.

## Migration and follow-up boundary

No public request schema, durable record encoding or existing deployment is
migrated by this launcher. Existing consumers may continue using graph-service
with their explicit configuration. Quick-start documentation can point to
this example only with its fresh-only and loopback limitations.

Production bootstrap, coordinated checkpoint restore, recovery-before-serving,
membership management and operational supervision remain separate designs and
milestone work. They must not be inferred as delivered from this demo gate.
