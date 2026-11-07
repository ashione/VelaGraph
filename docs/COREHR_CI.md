# CoreHR full-scale CI contract

`CoreHR full scale` runs the **complete 200,000-person / 5,000-department**
scenario on every pull-request open, reopen and update, every push to `main`,
manually through `workflow_dispatch`, and daily at **03:17 UTC**. GitHub schedules
can be delayed; cron runs the default branch. New runs cancel obsolete runs for
the same event/ref. It is not a small smoke-only substitute, and has no path
filters. This workflow is a PR check; making it a required branch-protection
check is a separate repository setting, not changed by this patch.

A dedicated Ubuntu runner builds all binaries in release mode with two Cargo
build jobs and runs the harness/node with two Tokio workers. Cold builds include
static RocksDB and may be expensive. A separate release cache avoids mixing the
existing debug/test cache. The 90-minute job timeout bounds runner spending,
not a fabricated query-latency pass threshold. Existing 120-second job leases,
five-second request deadlines, and all production row/byte/work/identity limits
remain unchanged and are still tested.

## Coverage

1. Generate a deterministic 2,000/50 smoke fixture, then the full 200,000/5,000
   fixture. See [model and limitations](COREHR_SIMULATION.md): eight-way people
   and five-way department trees, primary memberships plus second memberships
   for 20% and third memberships for 5%, skewed department distribution, and
   cross-shard relationships. These are synthetic shapes, not fitted HR data.
2. Run the existing `corehr_run` oracle matrix on each fixture, preserving all
   representative cases (including memberships, multi-membership joins, mixed
   directions, counts and deduplication), explicit unsupported/resource failures,
   and person/department ancestor-chain checks. Seven sequential samples per
   supported query are retained.
3. Independently import the same fixture into fresh databases and run the native
   count-job matrix. Nine cases have three samples each, including CEO subtree
   **199999**, exact level six **162551**, empty depth 32, duplicate-start bag
   **399998**, unique duplicate-start **199999**, department subtree **4999** and
   exact level six **1094**. Two extra probes, fixed snapshot vectors, sequential
   advances, terminal replay, no partial counts, cancellation and unchanged
   synchronous wide-traversal rejection are checked.
4. Both harnesses verify every stored key/value on all three shards against the
   fixture. A separate Python validator rejects missing/duplicate query cases,
   sample gaps, truncated evidence, wrong counts, missing cleanup and unsuccessful
   terminal records. It checks the exact partition-to-KV-row mapping, not just
   the aggregate row count. Every progress/outcome snapshot vector must contain
   each expected shard exactly once with the fixture's group/node/epoch identity
   and a positive integral sequence; vectors must remain unchanged within a job.
   Nonzero process exits fail immediately. No automatic retry
   turns a failed correctness run into a pass.

Requires three explicit prerequisites: the CoreHR count-job harness (PR #65),
compact shared-identity dedup support (PR #66), and the separate **nine-case
compact count-job harness upgrade**. PRs #65 and #66 alone are insufficient:
the older seven-case runner lacks the duplicate-CEO and department-dedup cases.
The validator rejects that older output even when its terminal record says
complete. Publish this workflow only on a revision containing all three.
Older full CEO-dedup budget rejections are historical evidence, not an acceptable
success for this gate.

## Measurements and retained evidence

The job summary shows observed full-scale hierarchy counts and median durations.
The 14-day artifact contains raw JSONL (including each RPC latency and snapshots),
execution revision, compiler/platform/CPU/memory details, SHA-256 binary identities,
fixture manifests, stderr and node logs. `summary.json` includes representative
query latency summaries, all hierarchy duration samples/request counts, process
RSS/high-water RSS and database bytes. The raw databases and binary fixture
payloads are excluded. Artifacts also run on failure where possible; absence of
a completed summary is never a pass. Build failures have Actions logs but may
have no runtime artifact.

Timings are observations, not hard performance gates. Three/seven sequential
samples do not establish production p95 or concurrent-load throughput. Import
uses encoded fixtures via bounded Raft writes, not ordinary graph-mutation
throughput. Four local processes use one replica per group, so this tests process
and shard boundaries, not high availability or a remote network. Snapshots remain
Unverified and are not globally atomic. Memory readings are observations, not an
RSS limit.

## Local reproduction

From a revision containing all three prerequisites:

```sh
cargo build --release --locked --example corehr_fixture
cargo build --release --locked -p replicated-graph --example corehr_run --example corehr_jobs_run --bin graph-node
python3 -m unittest discover -s scripts -p test_corehr_ci.py
TOKIO_WORKER_THREADS=2 python3 scripts/corehr_ci.py new-corehr-ci-results
```

`CARGO_TARGET_DIR` is supported. Output directories must not already exist; prior
runs are never overwritten. The local command has no independent wall-clock
kill timer; CI applies its operational timeout. All harness request/lease limits
still apply locally. The script's summary is created only after smoke and full
runs both pass.
