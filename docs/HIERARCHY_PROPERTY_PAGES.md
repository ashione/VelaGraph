# Fixed-snapshot hierarchy property pages

The explicit result-job API adds a restricted standard Gremlin terminal
`valueMap('name','active','country')`. For example:

`g.V(1).repeat(__.in('reports_to')).emit().times(32).dedup().valueMap('name','active','country')`

Use the existing StartRows/Advance/Read/Cancel API and CLI. Declare the projection
at Start with page_size 1..=128. ID-only result jobs remain unchanged, including
their 1..=1000 page size. Ordinary synchronous/distributed query execution still
rejects this newly parsed projection step explicitly; only the documented
hierarchy result-job slice executes it.

## Standard projection semantics

The official grammar parses valueMap. The admitted terminal form requires 1–8
distinct, nonempty string keys of at most 64 UTF-8 bytes each. Unicode keys are
supported. Zero-key all-property queries, includeTokens/boolean forms, by
modulators, extra steps after valueMap, properties on intermediate edges, and
other projection operators are unsupported. Existing repeat/dedup restrictions
remain. This adds no custom Gremlin spelling or alternative query language.

Read returns a distinct `ProjectedPage`, with the same cursor, done and snapshot
metadata as an ID Page. Each row contains `source`, its qualified vertex
identity, and `properties`, a canonical map from requested keys to singleton
lists of typed Values. This matches vertex valueMap for the existing
single-valued property store. Missing keys are omitted; an explicitly stored
Null is a singleton Null list, not absence. A vertex with no requested keys still
produces an empty map row. Integer, Real, Text and Boolean types are preserved;
non-finite reals and malformed records are errors.

Projection never changes row order, multiplicity or terminal vertex dedup.
Equal maps from different vertices remain distinct, and duplicate traversal
occurrences remain duplicate rows. The source identity is transport provenance,
not an invented valueMap property.

The synthetic CoreHR fixture has name, active, country and person-only
multi_department properties. It has no status or department_id scalar.
Department identifiers are represented by member_of relationships; this feature
does not synthesize, rename or join them into the property map.

## One snapshot and one failure boundary

After traversal finishes, preparation continues with bounded batches of selected
properties on exactly the original retained sessions. Each vertex occurrence is
canonically fetched once for all requested keys. The server validates identity,
requested-key membership, value shape and size before reusing a channel or
installing a batch. Parallel shard execution restores original row positions.

No projected row is readable until every projection succeeds. Only then does
Ready become true and the original sessions close. Read serves immutable maps
without graph I/O; old aligned offsets can be replayed until the original fixed
120-second expiry. Progress is not partial result delivery. Error, cancellation,
expiry or interrupted Advance invalidates unfinished preparation. No retry
opens a new view, and no phase renews the lease.

An existing ID-only Ready job cannot acquire new properties retroactively: its
sessions are already closed. Start a new explicitly projected job instead; its
snapshot vector is a new query, not a continuation of the old one. Independently
pinned shard snapshots still carry Unverified completeness, not global atomicity.

## Explicit new-mode resource contract

Existing count and ID-only contracts are unchanged. Projected jobs retain their
original traversal ceiling of **1 GiB / 8,000,000 work** and add a separate
preparation-stage ceiling of **768 MiB / 2,000,000 work**. The combined coordinator
upper bound is **1.75 GiB / 10,000,000 work**. The completed traversal charge is
retained separately; starting projection is not a reset of that ledger.

Data-node sessions keep their original cumulative **1 GiB / 8,000,000 work**
limits across both phases. Their meters, descriptors and leases are never reset.
A concentrated single-shard query may therefore reject even when a coordinator
phase still has allowance. Independent shard reservations use the current phase
allowance; they never duplicate the remaining global budget per worker.

Other bounds:
- At most 250,000 results and the existing 750,000 combined live traversal entries.
- At most 2048 tagged-JSON bytes per selected scalar map, measured by a bounded
  counting writer without allocating an oversized encoded buffer.
- At most 64 MiB of retained encoded selected maps, with additional typed map,
  key, value and vector allocations charged before retention.
- Internal projection batches are at most 128 leaves and shrink according to actual encoded key and identity sizes to fit the existing 16 KiB fragment cap, including maximum-length escaped keys.
- At most 128 projected rows per page. Singleton-list/source wrapping stays below
  4096 bytes per row; complete fixed-size pages are guaranteed below the existing
  2 MiB response limit before Ready, not truncated later.
- One reusable delivery workspace reservation of 4096 + 32768 × page_size bytes
  in the projection phase. Existing admission bounds concurrent response copies.
- The same two shared job permits remain held through Ready until cancel/expiry.

These are logical accounting and explicit bounded buffers, not an allocator RSS
promise. Internal limits and insufficient phase/session quota fail safely before
Ready; no successful short prefix replaces an over-budget result. No storage
format, TLS setting, or persistent access permission changes are involved.

## Verification

Tests cover the official parser, missing versus Null, all scalar types, Unicode
and escaping, oversized maps, forged identities/keys, conservative reservations,
phase exhaustion and deadline rejection, unchanged traversal charges, duplicate
rows, immutable replay, and mutation after Start with original-snapshot property
results. The full CoreHR CI extends the existing eleven identity-result cases
with seven property cases, including every map for the full 199,999-person CEO
and dedup result, departments, missing keys, page size one and empty results.
