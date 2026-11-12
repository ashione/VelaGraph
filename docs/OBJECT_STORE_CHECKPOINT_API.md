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
