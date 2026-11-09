# Retained lifecycle harness evidence v2

This is a test-harness evidence layout, not a change to production storage,
managed RPC, graph records, SnapshotV1, capacity or deadlines. The mandatory
release test remains exactly
`byte_bound_independent_processes_1025_writes_retain_original_lifecycle`.
It still performs 1,025 serial genuine gateway writes, the unknown-ACK case,
original-ID/request/first-Plan comparisons, complete old-version proofs and
Gremlin reads, independent process reopen, three complete snapshot installs and
all recovery assertions. Helper fixtures do not replace that acceptance.

## Format and authority

Fresh runs write only `progress.json` format 2. It keeps the full binding,
externally verified source and frozen-ELF SHA-256 values, actual executable path,
timeout, verified ordinal, pending original external ID, member grants and
recovery/completion flags. The gate still hashes actual source and selected/frozen
ELF bytes before and after execution, requires the exact optimized artifact and
one passing selected test with zero ignored tests, and starts in a new directory.
It rejects v1 as fresh acceptance, incomplete execution and absent/corrupt refs.

The `originals` and `proofs` maps contain strict version-1 descriptors:
`format`, `kind`, `key`, `relative_path`, `byte_length`, `sha256`. Unknown fields,
duplicate/noncanonical ordinal keys, wrong kind/key/path/version, invalid lengths
or hashes fail. Only these fixed payload paths exist at fresh COMPLETE:

- `artifacts/original-1.json`, `original-256.json`, `original-1024.json`,
  `original-1025.json`: each full original RequestRecord, including its exact
  canonical request bytes, first Plan bytes and transaction identity.
- `artifacts/proof-v0.json`, `proof-v1.json`, `proof-v256.json`,
  `proof-v1024.json`, `proof-v1025.json`: the exact typed proof serialization
  originally captured, not a digest-only or reserialized-Value substitute.

`migration` is null on a fresh run. A migrated run has one additional required
strict `legacy_checkpoint` descriptor for `artifacts/legacy-checkpoint-v1.json`,
containing the complete raw pre-migration checkpoint. There is no parallel active
writer, duplicate identity manifest or automatic evidence-repair source.

SHA-256 binds content, not authorship. Byte-identical copied payloads are
indistinguishable; this does not claim provenance authentication, Byzantine
protection or protection against arbitrary unsafe manipulation inside the process.

## Bounds

Proof artifacts reuse `MAX_BYTE_PROFILE_TERMINAL_BYTES = 22,435,158` from the
production bounded terminal codec. A complete RequestRecord JSON bound is the
empty four-field schema punctuation plus four times the sum of the existing
65,536-byte request, 262,144-byte Plan and two 16-byte IDs. Four bytes per JSON
byte covers three decimal digits and a separator. This is below the unchanged
32 MiB managed frame; decoded request and Plan limits are also checked.

The legacy input bound includes four full records, five full proof byte arrays
(up to fourfold decimal expansion), the actual serialized identity/path envelope,
and one existing managed-frame allowance for remaining small metadata. It does
not introduce a smaller fixture cap or enlarge any production budget. File length
is checked before allocating its payload. Typed decoding and exact fixture
identity/phase validation follow the digest check.

## Writer ownership and publication

The test uses the existing official rustix safe filesystem APIs. The run directory
is opened component by component without following symlinks, and one private
CLOEXEC file descriptor holds an exclusive nonblocking directory flock for the
writer's lifetime. No lock file is created before identity checking. An immutable
ownership-valid latch cannot be restored after injected loss. The fd is not
cloned/exposed/unlocked in normal operation. Root and artifact directory identities
use device/inode, not mtimes which normal child activity changes. A second open
lock probe is diagnostic, not proof of who owns the lock. This tested contract
uses Unix directory locking and Linux/macOS no-replace rename APIs.

For each newly captured payload:

1. Validate the original typed historical value; compute its descriptor.
2. Create/reuse only its recognized staging file, write all bytes and fsync it.
3. Publish the final filename with no-replace rename, then fsync the directory.
   Creating the artifact directory also requires fsync of the run directory.
4. Read back and verify exact length/hash before making a private published ref.
5. Only then serialize and atomically replace progress, retaining its own file
   fsync, rename and directory fsync.

A rename followed by failed directory fsync does not publish a ref. On resume,
existing candidate bytes must match the original capture/migration preimage and
both durability barriers are repeated. Differing final files are never overwritten.
Path traversal, symlink directories/targets, hardlink aliases, unexpected names
and replacement of an owned referenced inode fail closed.

EVERY pre-send pending-ID checkpoint and EVERY post-visible-Commit verified
checkpoint remains a separate durable write. A clean uninterrupted run still has
2,055 progress writes and 4,110 progress file/directory fsync calls. Artifact
publication adds its own syncs; it does not replace or merge these checkpoints.

## Validation and crash boundaries

Complete bounded read/hash/type validation occurs at first publication, strict
load/migration, before COMPLETE and independently in the external gate. Existing
old-ID/old-V assertions use the validated immutable in-memory originals and fresh
RPC comparisons, never bytes accepted on inode/length alone.

An ordinary small save checks ownership, directory identities, regular single-link
referenced files, owned inode identity and length. It does not repeatedly rehash or
serialize all historical proofs. Same-inode, same-length in-place corruption may
not fail that small save, but must fail the next full validation: resume, COMPLETE
or external gate. The cooperative lock cannot exclude unrelated malicious writers;
this is not a claim of continuous tamper detection. Tests exercise that distinction.

A crash leaves either old valid progress with staged/orphan files, or complete new
progress whose refs were already durable. Only V0 setup and the current pending
representative milestone can explain normal orphans. Directory discovery never
advances verified state or adopts a ref. Existing fresh-RPC capture may confirm
byte-for-byte equality with an orphan; it cannot overwrite differing historical
bytes. Referenced missing/corrupt evidence is fatal and is never reconstructed
from current DB, fresh RPC or migration backup. COMPLETE rejects unresolved
staging, orphans and extra files in the artifact namespace. Shard DBs and snapshot
controls outside that namespace are not mistaken for evidence artifacts.

## Strict v1 migration, never reset or rebind

The sole legacy reader validates the original raw checkpoint and unchanged full
binding/source/ELF/path identity before any write. It validates actual reachable
v1 states, including empty initial setup, ordinary pending original IDs and the
special verified-1024/pending-1025 checkpoint with original 1025 but no proof 1025.
Required historical milestones cannot be missing. Existing migration artifacts
must match the complete v1 preimage before publication begins.

It durably preserves that raw preimage, extracts payloads only from its embedded
original values and exact proof bytes, then publishes v2 progress last. Interruption
leaves original valid v1 or fully valid v2. Once v2 is authoritative, its backup is
required for closure but can never repair a broken ref. Verified ordinal, pending
ID, first Plans, proofs, grants and recovery flags never reset. Updating the timeout
and clearing `completed` to rerun recovery assertions retains the old resume rule.

Same-identity migration fixtures prove representation compatibility, not
cross-build continuation. A new source/ELF cannot resume a frozen old executable's
run under the unchanged identity checks. Old evidence must not be relabeled, old
hashes injected to disguise a new ELF, or current DB values used to fill gaps.

## Diagnostic work accounting

Attempt-local counters and monotonic spans separate progress serialization/bytes,
staging writes, file sync, rename, directory sync, artifact reads/hashes/writes,
gateway Submit, milestone assertions and recovery/reopen/snapshot work. Nested
spans are labeled and must not be summed as disjoint wall time. Bounded summaries
and a final/error-drop summary go to retained stderr; no extra metric fsync is
added to each operation. Failed and interrupted output remains evidence of an
incomplete attempt, not acceptance.

Equivalent v1 encoded lengths use cached immutable embedded payload lengths plus
actual small metadata serialization and exact map punctuation/digit widths. Tests
compare them byte-for-byte with real v1 serialization across setup, milestones,
escaped/non-ASCII paths and numeric widths. This is work/byte accounting, not a
speedup claim. Fresh successful helper accounting checks 2,055/4,110, nine payload
writes, four original encodings, 18 artifact reads and 27 hashes (initial hash,
publication read-back and final closure), not hidden per-save proof processing.

Elapsed-time comparisons require separately coordinated same-fixture, release
profile and budget evidence, with compilation separated. No timing correctness
threshold, enlarged timeout, production optimization or general benchmark is
introduced. Fresh full release lifecycle and CoreHR remain required.

`sha2 = 0.10.9` (MIT OR Apache-2.0, official crates.io, already locked) is a test-only
direct dependency for standard SHA-256 rather than a custom hash implementation.
The already locked official `rustix = 1.1.6` (Apache-2.0 OR Apache-2.0 WITH
LLVM-exception OR MIT) enables filesystem APIs only as a dev dependency. No new
package version, production dependency, installed toolchain or downloaded source
is introduced.

## Same-run build handoff and CI scheduling

[Issue #126](https://github.com/ashione/VelaGraph/issues/126) separates release
compilation from the fresh lifecycle runtime. `lifecycle-build` has a 90-minute
job budget and produces an immutable artifact; `lifecycle` depends on it and
keeps its own unchanged 90-minute budget. The independent CoreHR scenario still
has 90 minutes. The serial build-plus-runtime budget is therefore up to 180
minutes, with another five minutes for the aggregate, excluding queue time.
This is a scheduling/budget change, not faster writes, an expected duration,
or a guarantee that runtime finishes. All 1,025 writes, durable checkpoints,
fsyncs, old-ID/Plan/old-V assertions, ACK loss, reopen and snapshot phases and
15/60-second request budgets are unchanged.

The existing `managed_lifecycle_ci.py` has explicit `--mode build` and
`--mode runtime --handoff PATH` entrypoints. The old combined entrypoint is
retired. Runtime has no Cargo installation, build, cache restore or fallback.
It reaches the existing exact-test and complete-checkpoint validators only
after validating the required builder's handoff. Its output directory and the
lifecycle directory must be new. A zero-test, ignored, interrupted or incomplete
run is never acceptance. The stable `CoreHR 200k people / 5k departments` check
runs with `always()` and requires build, lifecycle runtime and scenario to all
be `success`; failed, cancelled and skipped jobs fail it.

The trust route is GitHub Actions, not a manifest's claim:

1. Both checkouts use the immutable `github.sha`, including the merge candidate
   SHA for pull requests, and independently verify clean tracked source and Git
   commit/tree. The builder runs the same exact locked release Cargo command.
2. `actions/upload-artifact@v4` uploads only `handoff.zip`. Explicit build-job
   outputs carry its immutable artifact ID and the inner ZIP's SHA-256 and byte
   length. The name includes the current run ID and attempt.
3. Runtime uses read-only Actions API calls to check the artifact's repository,
   run and head SHA, plus the current attempt's workflow path, event and repository.
   API `head_sha` is checked against the platform event's PR head for PRs, not
   confused with the tested merge SHA; both are separately bound in the handoff.
   Only the workflow's existing PR/push/schedule/manual events are accepted.
   The runtime adds only `actions: read` to the existing `contents: read` scope.
4. `actions/download-artifact@v4` receives only the build job's artifact ID and
   uses its current-run default. There is no URL, repository, token or run-ID
   override. Missing metadata, failed API/download or a different attempt fails
   closed. **Re-run all jobs** after a failed runtime: “re-run failed jobs” cannot
   reuse a successful builder from an earlier attempt.
5. Before any chmod or executable invocation, runtime independently compares
   current source and Cargo inputs, validates the closed archive and all declared
   lengths/hashes, and selects exactly the recorded optimized `managed_process`
   compiler artifact. Absolute Cargo paths are checked against the declared build
   root, not the runtime runner's path or an arbitrary basename. Only the validated
   Linux x86_64 `managed_process` ELF becomes executable.

The version-1 handoff contains exactly `handoff.json`, `source.json`, full
`build.jsonl`, `dependencies.json`, `managed_process`, `Cargo.lock`, and three
workspace manifests (`cargo-root.toml`, `cargo-d2-raft.toml`,
`cargo-replicated-graph.toml`). It binds repository/run/attempt/event/PR head,
checkout SHA/tree, source digest, exact build command and compiler artifact.
Every payload has an exact byte length and SHA-256. The dependency manifest
records observed compiled package IDs, versions/sources, profiles and features,
cross-checked against the independently checked-in lockfile and workspace Cargo
manifests. The lockfile retains the dependency graph; unused target-specific
packages need not have compiled artifacts. Required `d2-raft` library release
profile and `default,test-hooks` features are checked. This is not a bundle of
all external/system libraries or proof against a compromised builder.

ZIP members have fixed ASCII flat names, stored/uncompressed data and regular
non-executable modes. Missing/extra/duplicate names, traversal, links, nonregular
files, unsupported flags/compression, malformed headers, comments and trailing
data are rejected. Member lengths are bounded before reads: 1 MiB for the handoff
and each Cargo input, 16 MiB for source, 64 MiB each for Cargo output/dependencies,
and 512 MiB for the ELF; total archive size is bounded by those limits plus 64 KiB
of ZIP headers. Extraction never uses archive-controlled paths or permissions.
Generated build/runtime output, target files and Python bytecode are excluded
from source hashing. The runtime retains the validated API provenance and handoff
alongside execution evidence; partial failures still retain logs, `progress.json`,
`progress.json.tmp` and the complete `artifacts/**` reference closure.

Runtime requires the runner image's `libstdc++6`, `libgcc-s1` and `libc6`
(including libm and the Linux loader), not libclang or a shared RocksDB library.
Read-only `readelf` inspection of the retained prior ELF found only those runtime
libraries; its runner log installed/updated none of them. This verifies the prior
image, not every future `ubuntu-latest` image. A missing/incompatible runtime
library fails the fresh test rather than triggering a build or install fallback.

The configured Rust cache prefix, main-only save condition and failure policy
are unchanged. Moving the cache step from job `lifecycle` to `lifecycle-build`
can change the cache action's implicit job-derived key and cause an initial miss;
no shared-key workaround or cache-policy change is introduced. Cache reuse only
accelerates Cargo dependencies and never substitutes a previous run's executable
or acceptance evidence. SHA-256 checks content integrity; the supported same-run
artifact route supplies provenance, not adversarial-build attestation.
