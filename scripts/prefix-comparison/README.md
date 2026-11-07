# Stopped-fixture prefix comparison

Opt-in diagnostic; the recorded A1 run is described in `docs/TERMINAL_PREFIX_RESULTS.md`. The same source is staged into independent copies of each checkout. The baseline checkout, original fixture DBs and frozen evidence/binaries are not modified (the explicitly selected warm Cargo target is reused). This is read-mostly: reopening copied Raft DBs can persist normal Raft state, and the only submitted graph command is exact original external ID1 retry. No new external ID is submitted. Temporary DB copies use regular file copies, never hardlinks. A run has one parent plus four child OS processes; runs are sequential. This is same-host debug evidence, not production/disk/HA evidence.

Invocation from an environment with the repository's existing cargo/toolchain/native library setup:

```sh
python3 scripts/prefix-comparison/run.py \
  --baseline /workspace/scratch/1462420af989/fixed-version-traversal \
  --candidate /workspace/scratch/1462420af989/incremental-prefix-evidence \
  --output /tmp/prefix-comparison-results-NEW-UNIQUE \
  --target-dir /workspace/scratch/1462420af989/raft-state-target \
  --execute
```

The output directory must not exist. Debug/test builds use one build job, no incremental compilation and debug info disabled for both variants. Inherited toolchain, RUSTFLAGS and native dependency environment are identical. The required `--target-dir` explicitly selects the existing warm Cargo target to avoid a fresh dependency build under the disk-space constraint. Coordinate exclusive target ownership with the root worker before invoking the runner; do not run while workspace tests or another build own that target. Each completed main ELF is copied into the output directory before its run and before the next build, preserving baseline and candidate binaries independently of subsequent target updates. Frozen evidence directories are never modified. Source staging includes tracked changes and unignored new files. Do not modify either checkout while staging/building. Each source build and its measurement run complete before the next starts.

`baseline.json` and `candidate.json` contain three repeats each of explicit V1, V128 and V148 capture times and prepare_vertex times. Capture measures only GraphCohort::snapshot; property checks and prepare_vertex are separate. Exact rows, key/value bytes and replay operations come from a separate untimed cohort acquisition. That extra acquisition may warm caches before subsequent samples; these are warm sequential comparisons, not cold-process per-sample numbers. All expected vertex labels/properties and the next absent vertex are asserted at each V. V149 must fail; latest V must equal 148. Every retained external command and transaction identity is checked. ID1 retry checks original transaction ID, original ordinal, exact retained request/plan payload equality and byte-for-byte unchanged Journal; latest V148 is reverified afterward.

Full Journal RPC transfer/decode and local encode measurements are separate from capture timers. The encoded byte count is the Journal payload length, not a TCP byte counter or full RPC envelope length. No full-history diagnostic is performed inside the capture timer. Newly prepared plans are never executed. Candidate-only TerminalPrefix codec measurements belong in a separate artifact.

`*-build.jsonl`, `*-build.stderr.log`, retained executables, `*-run.log` and `manifest.json` provide compiler output, exact commands/configuration and harness/binary hashes. Each persistent staged tree has a path-to-SHA256 inventory plus its aggregate SHA256. Original checkout dirty status and tracked HEAD diff are retained and hashed; original HEAD is explicitly context only, never labeled the measured commit. Optional proof compiler stderr and input Journal hash are retained too. The runner stops on any failure. Source files were rustfmt-checked. The recorded A1 invocation compiled and passed both variants; every new invocation still performs compilation and runtime validation.

Optional `--journal /absolute/path/to/recovered-stopped-journal.json` stages `proof_sizes.rs` only in the candidate's examples and runs it after both main comparisons, using the identical build configuration. `proof-sizes.json` records V0/1/128/148/149 projection, serde encoding/validation and exact structural equality times/bytes (three repeats), with full-Journal codec costs separately. This source requires candidate TerminalPrefix APIs and is never staged into baseline. The journal path is mandatory for this optional diagnostic and must explicitly identify the saved 149-terminal JSON.
