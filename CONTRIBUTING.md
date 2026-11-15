# Contributing to VelaGraph

External contributors are welcome: documentation, reproducible bugs, tests,
architecture reviews and implementation all help. VelaGraph is experimental;
prefer small, reviewable changes that preserve explicit contracts over broad
compatibility layers or dataset-specific shortcuts.

## Before starting

1. Read [README](README.md), [AGENTS.md](AGENTS.md) and the relevant
   [implementation rules](docs/IMPLEMENTATION_RULES.md).
2. Find an existing [issue](https://github.com/ashione/VelaGraph/issues), or open
   one explaining the problem, desired behavior and acceptance criteria. Discuss
   large API, storage-format or architecture changes before implementing them.
3. Check open PRs and comment on the issue before taking on substantial work to
   avoid duplication. Do not assume assignment or a review deadline.
4. Read [licensing details](docs/LICENSING.md): project-authored material defaults
   to Apache-2.0, with existing component and third-party exceptions. Check
   provenance and preserve notices for external material. Ask maintainers when
   applicable terms are unclear. No separate CLA or DCO is introduced.

## Development environment

CI uses Linux (Ubuntu), current stable Rust, a C++ toolchain, Clang/libclang and
Python 3. Install Rust using its official distribution and native dependencies
through your OS package manager. Debian/Ubuntu native packages are
`build-essential clang libclang-dev`. Set `LIBCLANG_PATH` if discovery fails.
The first native RocksDB build is slow and uses several GiB of disk space.

Fork the repository, clone your fork, create a topic branch from current `main`,
and keep generated databases, credentials and build outputs out of commits.
Run commands from the repository root:

```sh
cargo build --locked -p replicated-graph --bins
```

This builds the CLI demo entrypoints; see [quick start](README.md#quick-start).
For a full workspace build, use `cargo build --workspace --locked`.
On constrained machines, `CARGO_BUILD_JOBS=2`, `CARGO_PROFILE_DEV_DEBUG=0` and
`CARGO_PROFILE_TEST_DEBUG=0` reduce build pressure, as in CI. With
`CARGO_TARGET_DIR`, update script binary paths accordingly. Do not share a live
RocksDB directory between processes; each node needs its own directory.

## Tests and review evidence

For implementation changes, run the repository baseline before requesting review:

```sh
cargo fmt --all --check
cargo test --locked
cargo clippy --all-targets --locked -- -D warnings
git diff --check
```

Start iteration with the affected package/test target, then run the applicable
baseline. For documentation-only changes, validate paths, examples and statements
against current code; state clearly which runtime checks were not rerun.
An inability to run a check is not a pass: record the exact blocker in the PR.

The workspace's default members exclude `checkpoint-object-store`. For changes
there, also run:

```sh
cargo test --locked -p checkpoint-object-store
cargo clippy --locked -p checkpoint-object-store --all-targets -- -D warnings
```

| Layer | Purpose | What it does not prove |
| --- | --- | --- |
| [Ordinary CI](.github/workflows/ci.yml) | Format, default-member tests and Clippy on separate jobs | Ignored long tests and all workspace members are not implicitly covered |
| [CoreHR](.github/workflows/corehr.yml) | Full observed scenario plus release 1,025-write managed lifecycle and recovery | Full-scale managed CoreHR or multi-machine HA |
| [Object-store checkpoint](.github/workflows/object-store-checkpoint.yml) | Native transport tests and explicit SeaweedFS S3-compatible integration | Production AWS/OSS/TOS bucket conformance |

The long lifecycle is deliberately ignored by ordinary debug tests. Its release
build and execution are separate CI jobs; the scenario is independently run.
This guide describes existing workflows, not new path-based skip rules or a
relaxation of required checks. Do not disable a gate to obtain a green PR.
For local reproduction and evidence formats see [CoreHR CI](docs/COREHR_CI.md)
and [checkpoint transport](docs/OBJECT_STORE_CHECKPOINT_API.md).

## Proposals and design records

For substantial architecture, distributed protocol, persistence, recovery or
cross-cutting API changes, keep a complete proposal and design document in
`docs/` before treating implementation as ready for review. A checklist or PR
summary alone is not a substitute. Small localized fixes may use an issue/PR
explanation proportional to their scope.

Record the problem and goals/non-goals, current evidence, alternatives and
tradeoffs, chosen design, interfaces and state transitions, consistency and
failure semantics, compatibility/migration, resource/deadline bounds, rollout
and verification plan, and unresolved questions. Distinguish proposed, accepted,
implemented and validated status; acceptance of a design is not delivery.

Link the document bidirectionally with its tracking issue, implementation PRs
and release milestone/Project item. Update the document with material design
changes and final evidence in the implementation PR. Preserve superseded designs
as explicitly historical records with replacement links, not contradictory
current instructions. Do not retroactively claim a missing design was reviewed
or that planned behavior already exists.

## Change standards

- Use the official Gremlin parser; reject unsupported behavior explicitly.
- Keep optimization generic. Measure representative workloads, correctness,
  resource use and regressions; do not hard-code CoreHR assumptions.
- Preserve deterministic encoding and result semantics. Format changes need a
  version, migration policy, golden vectors and reopen/recovery tests.
- State the actual consistency path: local, observed or managed. Never turn an
  unknown outcome into success or claim stronger guarantees than the evidence.
- Bound work and memory. Test failure, cancellation, limits and concurrent writes
  where relevant, not just the happy path.
- Refactor alongside feature work: remove obsolete paths and update their docs;
  avoid dead code and speculative compatibility layers.
- Record benchmarks with commit, build profile, hardware/resources, dataset,
  command and repeated results. Separate single-host functional validation from
  distributed throughput or availability claims.

### Documentation freshness is part of feature delivery

Every behavior-changing PR must inspect the affected README entry, runnable
examples/CLI guide, architecture contract, `docs/CURRENT_STATUS.md` and roadmap.
Update the affected documents in the same PR, or state why each needs no change.
Record the supported path, defaults, removed flags, limitations and verification
scope; distinguish implemented behavior from planned acceptance. Do not wait for a
separate end-of-project documentation sweep. Preserve historical benchmark reports
as dated evidence rather than relabelling old measurements as current results.

## Pull requests

Open a **draft PR** early for work in progress. Link the issue with `Refs #N`;
use `Closes #N` only when the issue's complete acceptance criteria are satisfied.
Describe behavior, scope, risks, tests actually run and remaining limitations.
Include relevant documentation in the same change. Keep unrelated cleanup out.

When ready, request review and respond to feedback with focused changes. Re-run
checks affected by revisions. Maintainers decide readiness and integration order;
do not approve or merge your own PR. Separately reviewing maintainers or
explicitly owner-authorized automation handle merges under the same review and
applicable check requirements; this guide grants no repository access.
A green job alone is not evidence that an
entire issue is complete. No review or release SLA is currently promised.

## Working together

Be respectful, specific and constructive. Critique code and evidence rather than
people; harassment and discriminatory behavior are not welcome. Make room for
contributors with different experience and communication styles. Keep issues
focused, avoid sharing personal information and follow maintainer moderation.

Do not put credentials, private datasets or exploit-ready security details in
public issues or logs. Check the repository's Security tab for any available
private reporting channel before disclosure. A dedicated security response
process remains a maintainer decision, not a service promised by this document.
