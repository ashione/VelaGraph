# Experimental Fragment diagnostic observation

This opt-in internal diagnostic is not a query fix, public API, tracing protocol,
release qualification, or attribution of historical failures (#128/#247).
Two original scenarios passed once on the earlier diagnostic tree; those runs
only failed to reproduce the failures. They do not discharge the release risks.

## Activation and scope

`VELAGRAPH_REMOTE_DIAGNOSTICS=1` must be set before starting each participating
coordinator and member process. The existing diagnostic switch is cached once per
process; changing it after startup does not toggle observation. Any value other
than `1` disables the sink. Disable by restarting without the variable.
No default enablement, deployment change, or CI workflow change is included.

Only observed Fragment RPCs generate a fresh random 128-bit call ID. The deadline
is fixed before generation; random-generation failure omits correlation rather
than changing the RPC result. The ID accompanies the request as
`x-velagraph-diag-call`. The receiver accepts exactly one 32-character lowercase
hex value only when its local diagnostic switch is enabled. Missing, malformed,
or duplicate values are ignored, not rejected. There is no response echo.
An enabled coordinator with a disabled member may emit client-only records;
a disabled coordinator does not generate these IDs.

IDs are not credentials, operation identity, proof of authorship, or audit data.
A peer can supply/replay a valid value. Do not infer causal identity from matching
IDs in uncontrolled traffic. In controlled, independent, non-replayed requests,
a matching server removal and client status binds that one RPC, not a whole query.
The records contain no session token, query, payload, leader address, error message,
or arbitrary error details. Process IDs, call IDs, error categories and elapsed
expiry age remain operational metadata and require ordinary log access controls.

## Meaning and limits

A server `matching_expired_entry_removed` record is produced only when this lookup
actually removes the matching expired descriptor during the original sweep, then
finds it absent after identity validation. It is emitted after registry locks are
released. An already swept/closed entry may produce only a client status record.
`remote_status` records the received tonic code and existing mapped category;
it does not independently establish whether the status originated at a server,
proxy, or transport. `local_elapsed` is distinct from remote DeadlineExceeded.
No errors, leases, budgets, retries, sweep order, or query assertions are changed.

The shared process sink permits at most 64 lines, including its truncation marker,
with at most 768 bytes per line. Other diagnostic categories share this allowance.
It makes one best-effort synchronous stderr write per record, without retries.
Missing records can mean opt-out, exhausted allowance, invalid metadata, prior
sweep, failed I/O or no observed event. They are not evidence that expiry did not
occur. A blocked stderr can delay completion; bounded formatting/line count does
not establish zero timing or performance overhead. IDs and metadata allocation,
random generation and expiry matching also add work. No throughput benchmark or
production latency guarantee has been established.

## Minimum controlled observation procedure

Before enabling in any deployment or CI, review that destination's authorization
and log retention/access policy. Use a fixed source/tree, toolchain and documented
original workload and budgets; do not enlarge load, extend a lease, remap errors,
or relax an oracle to obtain a result. Record process roles and enabled settings.
Collect stdout and stderr separately, continuously drain child pipes, preserve the
original exit status, and bind build receipt and before/after executable hashes.
Use isolated run IDs and bounded evidence storage; log loss, truncation, failed
collection or supervision interruption means insufficient evidence, never PASS.
Retain the first outcome. Do not silently rerun a failing workload.

For a failure, inspect the exact failing assertion and RPC evidence separately.
A matching fresh call ID with actual removal and client SessionUnavailable narrows
that RPC's mechanism. It cannot retrospectively establish the cause of old CI
failures or substitute for a contract-level fix and exact final-tree validation.
Observation across long-lived processes will cease at the shared cap: this change
is suitable for bounded diagnostic runs, not continuous lossless tracing. Do not
reset the counter or restart production merely to bypass the cap.

## Validation boundary and integration gates

Earlier frozen-tree tests covered metadata parsing, full status-map equivalence,
scope cancellation, concurrent loopback correlation and already-swept controls.
A separate controlled test used the actual Handler, client random generation and
production stderr sink, with independent registries and process-level opt-in/off
and invalid-metadata controls. Entries were injected already expired: this does
not validate natural OpenQuery lifecycle, the complete host's admission/background
cleanup, or cross-process log collection. These results do not automatically apply
to a new integration tree.

Before publication or merge: independently review patch equivalence against the
actual latest main; compile and run narrow tests and Clippy on the exact tree;
repeat only the controlled Handler/sink acceptance on that tree; obtain normal
repository CI including required retained/core gates. Preserve all first failures.
Confirm the release scope does not present this diagnostic as resolving open query
risks. Public upload, CI enablement and deployment remain separate authorized
operations. This document does not authorize or schedule them.
