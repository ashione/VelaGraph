# Read-only checkpoint verification command: proposal and design

Tracking: [#305](https://github.com/ashione/VelaGraph/issues/305), [#191](https://github.com/ashione/VelaGraph/issues/191), following
[#302](https://github.com/ashione/VelaGraph/issues/302) /
[#303](https://github.com/ashione/VelaGraph/pull/303), v0.0.2 backup transport.
Status: implementation candidate; final review and exact-tree gates pending.
No global recovery, remote authority or product release acceptance is implied.

## Problem and decision

The existing verifier requires embedding Rust. Operators need a small command
that checks a retained receipt against an explicitly selected backend without
staging a potentially large checkpoint on disk. A weaker metadata-only HEAD
probe cannot prove the bytes. A second verification engine would drift from
`download`. An automatic latest/restore command would conflate discovery,
retention, global-cut validation and READY. These alternatives are rejected.

Use one binary in the existing adapter crate, and reuse the existing complete
verifier. No new dependency version, storage format, backend protocol, daemon, listener,
scheduler, discovery or mutation API is introduced. The existing locked
`reqwest` 0.13.5 transport is now a direct dependency solely to disable redirects
and proxies. Its MIT OR Apache-2.0 license is compatible; no source is copied.

## API and authority separation

`CheckpointReader::new(store, prefix, limits)` exposes only `verify(receipt)`.
It wraps the same private implementation as `CheckpointStore`; it does not expose
an upload accessor. Construction does not assert `AtomicCreateVerified`, because
read-only GET verification does not prove a provider's write precondition.
`CheckpointStore::new` still requires that explicit capability for the write API.
Common constructor validation remains shared. This is API separation, not IAM:
operators should independently use read-only backend credentials.

## Command and input contract

`checkpoint-verify --config FILE --receipt FILE [--allow-http-loopback]`

Both inputs are caller-owned regular files, not directories, symlinks, stdin,
FIFOs or URLs. Their metadata and actual reads are limited to 64 KiB each.
Unknown JSON fields, duplicate/unknown flags, missing arguments and malformed
receipts fail. Inputs must not be concurrently replaced; this command does not
provide an OS-level sandbox against a hostile local file owner.

Config contains endpoint, bucket, region, prefix and all five verification
budgets. There are no credential fields or credential command-line arguments.
Only `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` and optional
`AWS_SESSION_TOKEN` are imported into the SDK static credential provider.
No ambient AWS endpoint, metadata-provider, addressing or client settings are
imported. Missing/empty credentials fail without network discovery. The HTTP
client disables redirects and proxies, keeps certificate validation, and uses
HTTPS-only outside explicit loopback mode. CLI version 1 does not support IAM
role/metadata discovery, AWS profiles, S3 Express or custom proxies. HTTPS is normal. The test flag permits plain HTTP only to an explicit
numeric loopback address with a nonzero port; non-loopback HTTP, URL userinfo,
queries and fragments reject. The command makes no provider-certification claim.
The operator must supply a trusted endpoint; it is not a general URL-fetch API.

The shared manifest/file/chunk/total-byte checks apply. Timeout is explicit,
nonzero and capped at 24 hours; it bounds the whole asynchronous verify call,
including credential/network waits and SDK retries. It is not a hard CPU or OS
scheduling deadline. Local input loading precedes this timer. Memory consists
of bounded input/manifest metadata and one chunk, subject to SDK overhead;
these are not strict process RSS limits.

## Outputs and failure behavior

Only complete success writes one JSON line to stdout:
`{"status":"verified","files":2,"bytes":16394,"chunks":18}`.
Counts have the existing logical-file/byte/chunk-reference meanings, not unique
objects or actual network transfer. Failure exits nonzero, leaves stdout empty,
and writes a stable JSON category to stderr. Raw SDK/OS errors, input values,
local paths, object names and credentials are not echoed. No local checkpoint
or remote object is created. Broken output streams exit unsuccessfully without
panicking; `output_io` may follow a partial stdout write, and an unwritable
stderr cannot be guaranteed to receive JSON. `--help` is the only non-verification stdout case.

Categories distinguish arguments, input I/O/type/size/JSON, endpoint/config,
backend construction, invalid checkpoint, limit, integrity, deadline and object
store errors. They deliberately do not expose provider error text. Operators
must diagnose detailed credential/endpoint configuration through their provider
without assuming an unavailable object means corrupt content.

Success is an observation of bytes at one call, not a lease against deletion,
valid RocksDB state, a global certified cut, future readability, restore, READY,
remote-durable ACK or zero-RPO. #191, #193 and #124 remain separate and open.

## Verification and rollout

- Unit and independent-process argument/input/endpoint/error-output tests.
- Existing adapter fault/roundtrip tests, plus read-only reader equivalence.
- Disposable SeaweedFS test invokes the real command with synthetic credentials:
  exact successful counts, missing-object failure and corrupt-object failure.
  This reuses the existing S3 gate, not a new production bucket or cloud claim.
- Independent review; formatting, Clippy, package tests and applicable repository
  CI. Record local, ignored, blocked and remote results separately.

There is no migration or automatic installation. Build the explicit package and
binary; existing upload/download callers retain their constructor and behavior.
