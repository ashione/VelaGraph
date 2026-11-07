# CoreHR incremental hierarchy count validation

This run tests PR64 fix head 5e14a14b62c58f902bcc423df45f6f30f20bb9f8, verified equivalent to tested local commit 63697b794cf8264c5277b24d77afdc38abd870a8 (tree bee260eac7ac3179c268d488328cfdd5c28dd5a3), not the earlier PR64 head. Exact source and executable hashes are included in the published `corehr-count-job-results.json` (`source_sha256` and `binary_sha256`). These are count jobs with Start/Advance/Cancel, not the existing synchronous query endpoint.

The unchanged deterministic fixture has 200,000 people, 5,000 departments, 454,998 edges and 1,569,994 encoded KV records. Three independent data processes plus metadata run on the same cloud host, one replica per group, plaintext loopback. Every KV is compared before queries. Import uses offline prepared v2 records through D1/Raft, not a production mutation API. Snapshot completeness remains Unverified; this is neither a global atomic snapshot nor an HA benchmark.

Each job has a fixed 120-second lease, individual requests limited to 5 seconds, 8 million work units, 1 GiB cumulative logical bytes, 250,000 combined frontier/next/seen keys, and bounded aggregate edge pages of 16,384. These limits were not raised. The fix reduces advance frontier width from 128 to 16 while retaining leaf RPC batch size 128. All nonmatching-label adjacency is still validated.

## Preserved failed attempt

The original formal candidate 7a32b621 (tree fecf6d3d) passed the small fixture but failed the full department-root probe: ResourceExhausted after 5 requests and 0.728 seconds instead of 4,999. A 125-department frontier encountered roughly 23,000 adjacency entries, including unrelated membership edges, exceeding the aggregate page bound. The earlier CEO-only engineering preflight did not expose this shape. The failed source, database, logs and FAILED_RECEIPT.json remain in corehr-jobs-final-evidence. No full three-sample matrix was completed on that candidate. Its measurements are not attributed to the fixed version.

## Oracle and boundaries

An independent parent-walk oracle enumerates all people and department depths. Expected CEO emit32 count is 199,999, exact level6 is 162,551, exact level32 is zero, and duplicate-CEO emit32 is 399,998. Department emit32 is 4,999 and exact level6 is 1,094. The full CEO dedup case intentionally rejects: seen199,999 plus final frontier162,551 exceeds the 250,000 combined-key cap. It must not return a partial count. The small fixture fits that cap and succeeds.

The driver checks every progress sequence and all three fixed snapshot stamps, verifies counts only on completion, replays the terminal advance, cancels completed jobs, and verifies subsequent advance is unavailable. A separate cancellation test stops an active job. Both original synchronous wide-tree queries, including the variant with a trailing limit(1), must still reject. No bearer tokens are serialized into evidence.

## Results

See the published `corehr-count-job-results.json` for complete sample counts, exact timings, per-job request counts and resources after successful completion. RPC latency percentiles within a job are not job-latency percentiles. Three samples provide a narrow reproducibility check, not a production latency distribution. Release nodes and a development-profile driver run with TOKIO_WORKER_THREADS=2, without concurrent heavy workloads. Historical reports and timings are unchanged.

Full run exited zero in 337.518 seconds; smoke exited zero in 6.084 seconds. All 1,569,994 full KV records were verified. The full matrix has 18 successful count samples and three expected dedup resource rejections; smoke has 21 successful samples. Two preliminary probes per fixture are reported separately. All snapshot, sequence, replay, cancellation and unchanged synchronous rejection checks passed.

Three-sample job medians (seconds):

- ceo_emit32: 19.270; 12,503 requests; result 199999.
- ceo_emit32_dedup: 6.497; 1,321 requests; result ResourceExhausted.
- ceo_exact32: 19.401; 12,503 requests; result 0.
- ceo_exact6: 9.507; 2,343 requests; result 162551.
- department_emit32: 7.921; 318 requests; result 4999.
- department_exact6: 7.153; 249 requests; result 1094.
- duplicate_ceo_emit32: 18.708; 12,503 requests; result 399998.

## Reproduction

Run `scripts/corehr_count_jobs.sh <new-output-directory>` from the repository root for fresh smoke and full fixtures and databases. The script refuses an existing output directory.

Build graph-node in release mode and corehr_jobs_run as an example. Generate a fixture with the existing CoreHR model and run the example with three positional arguments: fixture directory, a fresh nonexistent database directory, and the release graph-node executable. Set COREHR_JOBS_MATRIX=1 and TOKIO_WORKER_THREADS=2. The existing fixture format and D1 capacity setup are unchanged. Do not point the fresh database argument at retained evidence. The example includes the independent breadth-first tree count unit test and can be exercised with cargo test -p replicated-graph --example corehr_jobs_run. See docs/COREHR_SIMULATION.md for fixture generation and the baseline limitations.

Resource observations cover import, verification and the entire run, not an isolated job: full data-node VmHWM/RSS values were 849,128, 868,392 and 848,544 KiB (about 829–848 MiB); the driver reached 245,412 KiB (about 240 MiB). Recorded database files totaled 830,783,230 bytes. Logical frontier/session allowances are not process RSS caps. These observations alone do not establish a leak or attribute memory to the grain fix. Advance progress pages are work scheduling, not result pagination.
