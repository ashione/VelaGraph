# HTTP Real parsing correction and identity compatibility

This is a **non-transparent preview input-semantics correction**, not a storage
migration, an automatic repair of old values, or a mixed-version compatibility
promise. It does not change the HTTP JSON field type: Real remains a finite JSON
number, distinct from Integer's decimal string.

## Defect and correction

With the previously selected `serde_json` 1.0.154 default parser, some finite
JSON decimals decode to different binary64 bits from the nearest representable
value, including decimals emitted by that library's own serializer. Examples:

| Raw decimal | Intended nearest bits | Prior parser bits |
| --- | --- | --- |
| `-3.4847638523912474e139` | `dce76903a462a0d4` | `dce76903a462a0d3` |
| `8648971181991391.0` | `433eba3186f9a5df` | `433eba3186f9a5de` |

The public HTTP codec's actual Submit decoding reproduced the first mismatch.
Enable `serde_json/float_roundtrip` for the service crate so its real-number
input conversion preserves binary64 round trips. Fixed decimal/IEEE-754 golden
vectors cover both examples, the minimum subnormal, minimum positive normal,
maximum finite value and negative zero through Submit, Status and Abort, as well
as scalar response serialization and reparsing. This is finite regression
coverage, not exhaustive testing of every possible decimal spelling.

## Existing IDs and existing stored values

The gateway binds each external ID to canonical bytes of its **decoded typed
mutation**, under the existing `managed-graph-command-v1` encoding. For an affected
input, corrected float parsing changes those typed bits and therefore canonical
command bytes, even if the caller resends the identical original HTTP text.

- A prior committed decision is not rebound or rewritten by this fix.
- Affected same-text Submit, Status and Abort after the correction can return
  `identity_mismatch`. Do not advertise them as transparent cross-version retries.
- The original exact typed command, including the bits actually accepted by the
  old parser, remains the recovery identity. A caller that has independently
  retained that command can use the typed API for Status. Merely knowing intended
  decimal text is not proof that those were the originally accepted bits.
- Do not guess a new ID, automatically try old/new parser fallbacks, repair a
  record while recovering its status, or infer that an uncertain old write aborted.
- Stored graph Real values use protobuf fixed64 bytes. Previously accepted bits
  remain those bits; this change cannot recover the user's original intent.

A controlled local old/new service experiment submitted the second raw decimal
to the frozen pre-fix binary. Typed reads established the old bits at V1. With
only the service replaced, the same raw text failed identity matching for all
three write actions, while original-bit typed Status preserved the original
transaction ID and committed ordinal. A distinct intentional vertex creation on
the corrected service stored the correct bits at V2 without changing the old
vertex. The coordinator/shards were started by the corrected test build and remained
running throughout; no legacy node was cold-reopened. This is service replacement
and identity evidence, not a whole-cohort crash/reopen or live restore test.

## Rollback and mixed-parser identity boundary

The risk is bidirectional. A corrected gateway can accept a decimal as NEW_BITS,
then a rolled-back old HTTP parser can interpret the same raw text as OLD_BITS.
For that already committed external ID, Submit, Status and Abort can therefore
reject `identity_mismatch` after rollback too. Keeping the old executable is not
by itself a transparent rollback plan.

A separate controlled gateway-cycle experiment verified this reverse direction.
After the old-service vertex1 and corrected-service vertex2 decisions described
above, only the gateway was rolled back. All three raw same-text vertex2 actions
rejected with HTTP409/identity_mismatch. After each rejection, typed Status with
the originally accepted NEW_BITS preserved vertex2's exact external ID,
transaction and ordinal2; typed OLD_BITS Status rejected. Old vertex1 raw Status
still recovered its original decision, providing a healthy old-parser control.
Switching the gateway back to corrected restored raw vertex2 Status success and
kept raw vertex1 Status rejection; both exact typed identities and the known
fixture's two rows/bits at V2 remained unchanged. No additional write, repair,
new-ID retry or mutation-budget change was used.

The corrected experiment ran once successfully. Its first version had failed
before rollback because a new test assertion expected `SinglePartition` instead
of the documented FixedSession Traverse `Unverified` completeness value. That
first failure was retained and classified as an overstrong test oracle, not a
product rollback failure. The corrected test asserts `Unverified` explicitly;
its exact finite fixture row comparison is not a certificate of arbitrary-writer
adjacency completeness. The evidence received independent source/result review,
not an independent dynamic rerun.

Nodes were started by the corrected test build and remained running throughout.
This does not validate old-node disk reopen, old/cohort binary downgrade,
arbitrarily concurrent mixed-parser gateways, rolling availability or recovery
from a crash during migration. No mixed-parser routing mode is supported by this
change. An upgrade plan must cover forward migration AND rollback, including
which exact accepted typed commands/build features remain available for recovery
and how incompatible HTTP clients/gateways are kept from serving the same IDs.
Do not automatically try both parsers or silently substitute a new external ID.

## Feature unification and scope

Cargo features are additive within the selected build. Selecting the service
crate enables this parser feature on the shared `serde_json` dependency used by
`replicated-graph`, `velagraph` and `d2-raft`; a workspace build can also unify it
with `checkpoint-object-store`. It is **not confined to one HTTP function**.
Notably, `graph-client` JSON input containing Real values also uses the corrected
parser. The core `velagraph` crate also explicitly selects `float_roundtrip` for
the optional [incremental memory checkpoint](INCREMENTAL_MEMORY_GRAPH.md), whose
versioned JSON payload contains Real properties. Its standalone core regression
checks exact finite binary64 restoration, including subnormals and negative zero.
Other separately built packages need not select the same feature set;
do not infer universal feature equivalence across arbitrary downstream builds.

A scoped source audit found current root graph records encode doubles as binary
fixed64; root dedup, D2 Raft entries/coordinator request records and snapshots
carry integer metadata plus opaque byte vectors; checkpoint manifests use
strings and integers and transfer graph files as bytes. The D2 outer JSON
record decoder does not reinterpret embedded canonical command bytes as doubles.
No float-valued persisted JSON field directly reinterpreted by this feature was
identified in those paths at the time of that audit. The new incremental memory
checkpoint is an explicit exception and requires the corrected parser directly.
This is not a guarantee for all historical
formats, arbitrary export/import JSON, downstream structs or mixed builds.

The package version, storage key layout and canonical serialization format are
not changed here. JSON **decoding semantics** are changed. Exact-build storage,
golden, protocol and recovery gates are still required; they cannot be replaced
by the source audit or one successful upgrade experiment.

## Upgrade boundary

Fresh experimental deployments may use the corrected parser with clients that
preserve the documented Real/Integer distinction. Existing deployments with Real
writes need a separately reviewed inventory of original accepted command
identities and recovery requirements before service replacement. There is no
automated migration or transparent mixed-parser rolling-upgrade support in this
change. Keep an old build/feature identity with retained evidence; never silently
reinterpret historical receipts as corrected commands. Publishing or merging
this fix alone does not declare v0.0.1 released or existing data repaired.
