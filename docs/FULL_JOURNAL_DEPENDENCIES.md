# Full Journal dependency inventory

Scope: the retained D2 history implementation after active coordinator/member
checkpoints and physical checkpoint transport. This is an inventory for #108,
not a claim that cold recovery, logical snapshots, or every administrative API
has bounded cost. Physical checkpoint transport does not change these contracts.

## Production boundaries

| Boundary | Current dependency | Retained safety requirement |
| --- | --- | --- |
| `StateMachine::open` → `state_codec::load` → `event_ledger::load` | Cold open replays retained raw events into a complete Journal, validates derived checkpoints and hydrated state, then publishes active state. Legacy envelopes may embed complete history. | Reject corrupt/mismatched raw history, checkpoint/domain metadata, and applied identity. Do not substitute a trusted-looking checkpoint for cold validation. |
| `StateMachine::load_disk_state` | Temporary complete materialization under the caller's state publication lock. Checks persisted metadata against published metadata and validates loaded state. | A cached active checkpoint alone is not the authority for these explicit full-history operations. |
| `coordinator_journal` and managed RPC `checked_journal` / `Value::Journal` | Explicit compatibility/export operation returns a complete Journal; remote clients decode it. | Preserve full archive semantics; not an ordinary active status lookup. |
| `coordinator_terminal_prefix` | Temporarily loads complete authority and checks requested terminal rows. | Preserve terminal index/history agreement. |
| `participant_prefix_evidence`, `certified_participant_read_with_evidence` and their wrappers | Complete local retained history is compared with supplied full coordinator evidence and the decision index. The read also returns the selected graph snapshot. | Same-lock evidence and snapshot; cohort, baseline, version, shard, epoch and mode checks; no partial evidence success. |
| Logical snapshot build/receive/install/current retention | `build_snapshot` loads complete persisted state and serializes into a buffer. `begin_receiving_snapshot` buffers received bytes; `install_snapshot` decodes full state/history, validates and reconstructs durable state; `get_current_snapshot` clones retained snapshot bytes. | Logical snapshot compatibility remains distinct from physical backup/restore. No bounded-memory snapshot claim. |
| Legacy/general apply fallback | Loads complete state when neither active coordinator nor active member fast path handles the command. | Preserve migration, activation and legacy behavior; removing a fallback requires command-specific evidence. |

## Conditional legacy fallbacks, not active-path dependencies

`participant_history_status`, `participant_capacity_grant`, and
`participant_prepare_history` first use verified member checkpoints/overlays.
Their legacy fallback can materialize complete history. Likewise
`coordinator_history_page`, target lookup, and coordinator binding/grant reads
use published archive/checkpoint evidence when available and retain full-state
fallbacks for other representations. `decision_prefix` materializes an active
member only when its caller has not already supplied validated complete state.

Do not count the mere presence of a `load_disk_state` call as proof that the
active branch executes it. Conversely, successful active-path tests do not prove
that cold open or compatibility operations are bounded.

`participant_terminal_prefix` and `certified_terminal_read` return compact
terminal proofs. Their active-member branch now uses
`certified_decision_prefix` → `verified_member_prefix`, validating the anchored
archive without constructing a complete Journal. The legacy/in-memory branch
still uses `decision_prefix`. This active-path improvement does not remove the
explicit full-evidence APIs or cold-open/snapshot materialization listed above;
no constant-work or bounded cold-recovery claim follows from compact proofs.

## Narrow reduction: reuse within one explicit evidence operation

Both full-evidence participant methods already materialize and validate complete
history before checking the supplied terminal prefix. Pass that same hydrated
state to `decision_prefix`, rather than the original active state, so index
validation does not trigger a second identical complete load. The publication
read lock spans both checks; no shared cache or longer-lived authority is added.

The test-only load counter measures actual `load_disk_state` hydration attempts,
not allocation size or total raw reads. Each successful durable full-evidence
call must hydrate once, while still validating the decision index. This removes
one redundant replay, not the remaining full-history cost. Cold open, snapshots,
formats, wire contracts, archive retention and purge/install are unchanged.

## Remaining work

Any further removal needs a separately reviewed authority model and tests for
corrupt raw rows, stale metadata, decision-index disagreement, reopen and legacy
migration. In particular, #108 remains an umbrella for dependencies not removed
here; checkpoint retirement and prefix purge/install are independent work.
