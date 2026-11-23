# Immutable checkpoint object transport (experimental)

Refs #191 and #193. This is backup transport, **not** remote-durable write ACK,
zero-RPO, object-store-authoritative graph storage, automatic live rejoin or GC.
The separate local two-DB physical checkpoint validator remains authoritative.
It does not alter or bypass the unresolved #124 purge fence.

## Implemented boundary

`crates/checkpoint-object-store` exposes:

- `CheckpointStore::new(Arc<dyn ObjectStore>, prefix, Limits, capability)`
- `upload(completed_checkpoint_directory, checkpoint_id) -> Receipt`
- `download(&Receipt, new_directory)`
- `verify(&Receipt) -> Verification` (read-only complete byte verification)
- `CheckpointReader::new(store, prefix, limits)` exposes read operations without
  asserting conditional-create support.

The operator supplies a backend (locked Apache `object_store` 0.14.2), endpoint,
region, **existing** bucket and nonempty prefix. Credentials stay in the backend's
SDK credential provider, not manifests or logs. The library creates no bucket,
account, policy or access key. S3-compatible HTTP is exercised by explicit tests;
OSS/TOS native adapters and bucket-mode conformance are not delivered here.

Input must be a completed, caller-owned **immutable physical checkpoint**. Never
scan a live RocksDB SST directory. The transport preserves checkpoint.json bytes,
file names, size/SHA-256 and all node/group/applied/membership/log metadata. It
validates the inventory and bytes but does not interpret consensus metadata.
After download, call `restore_physical_checkpoint` to validate both databases and
produce a guarded offline restore. Never remove `RESTORED_OFFLINE_ONLY` to turn
this transport into a live-cluster rejoin protocol.

## Object layout and publication

- `<prefix>/blobs/<sha256>`: immutable bounded chunks of file bytes.
- `<prefix>/checkpoints/<id>/checkpoint.json`: remote envelope format 1, containing
  the complete original physical manifest string and ordered file/chunk inventory.
- `Receipt { checkpoint_id, manifest_sha256 }`: retain outside the disposable DB.
  The SHA-256 anchors reads; there is no mutable latest pointer or LIST discovery.

All file data streams through configurable chunks (default 8 MiB, allowed 1 byte
to 16 MiB), with one network operation at a time. This is a transfer-buffer limit,
not a graph/process-size limit. Total bytes, file count, manifest bytes and an
absolute operation timeout are separately configurable. Exceeding any budget is
an error, never truncated success. Metadata accounting also bounds chunk records.

Each chunk uses **atomic create-if-absent** (`PutMode::Create`) followed by GET and
SHA/size verification. ETag and application SHA are not interchangeable. No
HEAD-before-PUT imitation of conditional creation is allowed. The caller must
explicitly supply `AtomicCreateVerified` for its actual provider/bucket settings.
This capability marker is an integration precondition, not an automatic proof of
cloud-provider correctness. CI races two same-key creates to check its endpoint.

The SDK multipart API does not expose the same create precondition; this first
slice intentionally uses independent bounded objects, **not S3 multipart upload**.
It does not need to buffer an entire SST. Native provider multipart is follow-up.

Upload hashes local file bytes while transferring. A mismatch may leave orphan
chunks but cannot publish a manifest. The remote manifest is conditionally created
**last**, after all chunks verify. This is a single-object publication point, not
an atomic transaction over multiple objects. A lost write response is reconciled
by reading and checking exact size/SHA. Unverifiable outcomes remain unknown.
The caller may retry the same immutable input, ID **and chunk size**. A changed
chunk size changes the envelope and conflicts; use a new ID for a new layout.

Download verifies the receipt digest, envelope/version, physical inventory,
paths and complete chunk lengths before creating a destination. Each downloaded
chunk and reconstructed file is hashed. Missing/corrupt objects fail closed.
Only new directories are accepted. File/DB-directory data is synced, a temporary
completion manifest is written/synced, then renamed to checkpoint.json. Finally
the destination and parent directories are synced. Errors after publication may
leave a complete backup: validate rather than overwrite it. Earlier failures
leave no completion manifest. No automatic destructive cleanup is provided.

Reject absolute/traversal/duplicate paths and symlinks in expected local input
components. Concurrent replacement of the caller-owned local directory is outside
this contract. The bucket must retain blobs during restore: an unrelated lifecycle
rule can delete a referenced object, and this API cannot lock the whole bucket.
No list/delete/admin API, mutable root CAS, fencing epoch or retention lease is
introduced. Unreferenced partial uploads are not usable backups.

## Provider differences (official sources checked 2026-11-12)

| Provider | Evidence and conservative gate |
| --- | --- |
| AWS S3 | Conditional PutObject and CompleteMultipartUpload are supported. A multipart 409 requires restarting the multipart session; 412 is a failed precondition. Treat response loss as ambiguous rather than assuming the write failed. |
| Alibaba OSS | forbid-overwrite exists, but PutObject documentation says it is ineffective when bucket versioning is **enabled or suspended**. Do not advertise atomic create in those configurations. No replacement CAS is assumed. |
| Volcano TOS | Official PutObject/SDK documentation lists If-Match and forbid-overwrite; multipart supports forbid-overwrite and CRC64. Verify operation and version-mode behavior independently before claiming the same capability. |

Sources: [AWS conditions](https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-writes.html),
[AWS completion](https://docs.aws.amazon.com/AmazonS3/latest/API/API_CompleteMultipartUpload.html),
[AWS checksums](https://docs.aws.amazon.com/AmazonS3/latest/userguide/mpuoverview.html),
[OSS PutObject](https://www.alibabacloud.com/help/en/oss/developer-reference/putobject),
[OSS completion](https://www.alibabacloud.com/help/en/oss/developer-reference/completemultipartupload),
[TOS PutObject](https://docs.volcengine.com/docs/torch-object-storage/put-object?lang=en),
[TOS SDK changes](https://github.com/volcengine/ve-tos-golang-sdk/blob/main/CHANGELOG.md).

ETags and provider version IDs are opaque metadata, not full-content checksums.
A future mutable root needs true provider CAS or an external consensus registry,
non-repeating publication generation, and separately enforced writer fencing.
None certifies a multi-shard durable transaction by itself. Periodic checkpoints
still omit acknowledged writes after their cut; remote durable log tails and
commit decisions are a separate architecture task.

## Verification layers and CI

| Suite | Data/backend | Claim supported by a pass |
| --- | --- | --- |
| `checkpoint-object-store` | Synthetic files / memory + injected faults | Checksums, chunking, lost ACKs, concurrency, budgets and fail-closed publication |
| `checkpoint-object-store --test s3 -- --ignored` | Synthetic files / real SDK HTTP to SeaweedFS | Actual compatible service create/upload/download, not RocksDB recovery |
| `d2-raft --test physical_checkpoint_s3` | Actual two-DB checkpoint / memory objects | Producer→transport→offline restore, not real S3 |
| `d2-raft --test physical_checkpoint_s3 -- --ignored` | Actual two-DB checkpoint / SeaweedFS HTTP | Full real-service chain after that exact run passes |

The isolated CI service is [SeaweedFS](https://github.com/seaweedfs/seaweedfs),
[Apache-2.0](https://github.com/seaweedfs/seaweedfs/blob/master/LICENSE), official
release 4.53 with the binary checksum pinned in `scripts/seaweedfs-ci.sh`.
MinIO is not the new default: its official repository is archived and the
[vendor legal page](https://www.min.io/legal) says community maintenance ended.
An “S3-compatible” claim alone is not an acceptance test.

CI runs parallel transport/recovery jobs with finite deadlines and path filters,
loopback-only services, temporary storage and dummy credentials. Tests themselves
reject non-loopback endpoints and never use ambient cloud credentials. Logs are
retained briefly; no checkpoint data is cached. The transport tests run without
RocksDB compilation, while the separate physical job owns the native build.
Versioning matrix, multipart, service restart and proxy fault injection remain
follow-up acceptance work. Injected Rust failures do not certify cloud behavior.

The initial workspace was reset before these changes were published. Sources
were reconstructed from the recorded implementation, not claimed byte-identical
to its old local commits. Only results from the reconstructed revision count as
current verification. The prior environment denied AF_UNIX service sockets;
that earlier SeaweedFS startup failure was not a passing real-service test.

## Read-only backup verification

`CheckpointStore::verify(&receipt) -> Verification` reads and verifies the
receipt-bound envelope, physical inventory, every referenced chunk and the
reconstructed SHA-256 of each file, without creating a local directory or
writing remote objects. It shares the download validator and chunk reader;
it is not a second, weaker metadata-only check. Operators can check backup
integrity before spending local disk space on staging a download.

The returned `files`, `bytes` and `chunks` count logical file contents and chunk
references (not unique objects or network overhead). Empty files are hashed;
repeated chunks are checked for each reference. No report is returned until all
files pass. The same manifest, file-count, total-byte, chunk-size and absolute
operation deadline limits apply. Memory retains bounded manifest metadata and
one chunk, not all checkpoint bytes. Missing objects, changed bytes, malformed
inventory and timeout are errors, not partial verification success.

This is evidence of the bytes observed during that call only. It does not pin
remote objects against later lifecycle deletion or mutation, certify a global
checkpoint cut, open RocksDB, restore a node, prove live rejoin or permit READY.
A subsequent download revalidates all bytes, and offline physical restore still
requires the separate authoritative validator and guard. No existing storage
format, remote-ACK or retention contract changes. This independent #191 backup
transport slice does not complete #191/#193 or the v0.0.2 recovery milestone.

## Operator verification command

The `checkpoint-verify` binary uses the same verifier through the read-only
`CheckpointReader` API; no conditional-create assertion is needed for reading.
It does not download, upload, delete, list, restore or mark a graph READY.
See the [proposal and design](CHECKPOINT_VERIFY_CLI_DESIGN.md) for boundaries.

Build and run against a **trusted existing backend**, using preferably read-only credentials in `AWS_ACCESS_KEY_ID` and
`AWS_SECRET_ACCESS_KEY`, plus optional `AWS_SESSION_TOKEN`. The command supplies
these values to the SDK; it does not read profiles or discover role/metadata
credentials. Do not put credentials on the command line:

```sh
cargo build --locked -p checkpoint-object-store --bin checkpoint-verify
./target/debug/checkpoint-verify --config verify-config.json --receipt receipt.json
```

`receipt.json` is the exact retained `Receipt` from upload, not an ID guessed
from LIST. Both JSON files must be caller-owned regular files of at most 64 KiB;
unknown fields reject. Do not put credentials in either file or arguments.
A config example (replace the example endpoint, bucket and prefix) is:

```json
{
  "endpoint": "https://s3.us-east-1.amazonaws.com",
  "bucket": "existing-checkpoint-bucket",
  "region": "us-east-1",
  "prefix": "graph-backup",
  "limits": {
    "chunk_bytes": 8388608,
    "manifest_bytes": 16777216,
    "files": 100000,
    "total_bytes": 1099511627776,
    "timeout_millis": 300000
  }
}
```

Choose budgets compatible with the uploaded backup and your resource policy.
Timeout must be 1–86,400,000 ms. HTTPS certificate validation stays enabled.
`--allow-http-loopback` permits an explicit numeric loopback HTTP endpoint only
for disposable local compatibility testing; it is not a nonlocal insecure mode.
Redirects and proxies are disabled. Ambient AWS endpoint/transport settings
do not override this config. S3 Express, role discovery and custom proxy
configuration are not supported by this command. There is no public-service
listener or node configuration change.

Success prints one JSON line containing `status: "verified"`, `files`, `bytes`
and `chunks`. Failure prints a sanitized JSON `status: "failed"` and `code` to
stderr, leaves stdout empty and exits nonzero. An output-stream failure can
leave a partial line; an unwritable stderr cannot receive the error report. Missing objects/credential/network
failures can report `object_store`; this is not a precise corruption diagnosis.
The command intentionally does not echo endpoints, paths or backend error text.
Do not interpret a successful byte check as global recovery or cloud-provider
certification. Verification grants no retention lease; a later download must
still revalidate and use the guarded physical restore path.

## Fixed-cohort D2 decision-prefix transport

The optional `d2-cohort` crate feature transports one published
[local decision-prefix generation](PHYSICAL_CHECKPOINT.md#publishing-one-local-decision-prefix-generation).
Supply the trusted producer's local SHA-256, expected authority, complete cohort
(including epochs and capacity profile) and decision ordinal V. The selection
covers the coordinator and each member's D2 Raft and state databases.
Application catalogs and separate temporal stores require additional coverage.

1. Call `CheckpointStore::stage_prefix` on the caller-retained, immutable local
   root. It selects the local producer record and requires its exact physical
   digest for each uploaded group. Each replica uses the existing physical
   manifest and chunk transport. The prefix can already contain verified
   components from an interrupted attempt.
2. Save `StagedPrefix::receipt()` outside the backup with the backend, prefix,
   and intended cut. The receipt binds the local producer bytes and the exact
   remote envelope. Call `publish(scratch_parent)` on the staged value. It
   downloads all components into a private temporary directory, checks every
   chunk and file, and reopens the captured databases to verify the identities
   and decision history through V. Only then does it conditionally create the
   generation envelope and reconcile the response by a digest-checked GET.
3. After restart or uncertain acknowledgement, use
   `CheckpointReader::verify_prefix` with the saved receipt, same backend and
   prefix, expected cut and caller-owned, access-controlled scratch parent. This operation repeats the
   complete download and semantic verification without issuing a backend
   write. An unreadable or missing object fails the call. A retry of staging
   and publication from retained immutable input requires the same chunk size.

The envelope at `<prefix>/d2-prefixes/<local-sha256>/generation.json` contains
the format tag `d2-prefix-transport-v1`, the exact UTF-8 local record in
`local_record`, and the ordered `replicas` list of physical transport receipts.
Checkpoint IDs are `d2-<local-sha256>-<decimal-group>`, coordinator first and
then members in cohort order. The outer digest is SHA-256 of the exact envelope
bytes. Existing single-replica objects and local generation formats are unchanged.

Each stage, publication or verification call gets one configured timeout. Each
uses at most 65 groups (the coordinator and up to 64 members), one transfer at a
time. File count and logical payload bytes are charged across the whole cohort.
Each physical envelope is subject to the ordinary manifest limit. The published
outer envelope is capped at the smaller of 256 KiB and that limit, and its
embedded local record at 64 KiB. Allow scratch space for all replica payloads,
their physical manifests and filesystem overhead, and memory for opening one
replica's state. Synchronous local RocksDB verification or filesystem cleanup
can finish after cancellation or deadline. Publication and verification require
successful removal of the scratch directory before reporting success. A failed
or cancelled operation, a filesystem cleanup error, or a process crash can
leave temporary data under the scratch parent. Partial remote objects survive.

The caller must ensure the backend retains every object during publication,
verification and later use. Reads attest the bytes observed on that invocation.
The feature adds no retention lease, live restore, or query readiness guarantee.
The cross-process fixture uses real D2 replicas and a filesystem-backed object
store with injected response loss. Cloud provider behavior requires independent
testing against the actual backend and bucket settings.
