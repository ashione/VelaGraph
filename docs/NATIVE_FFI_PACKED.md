# Native packed query results and owned views

## Design

Keep ABI1 JSON operations 1–6 compatible. Add `QUERY_PACKED=7` with the same JSON
request and a versioned, immutable binary result. The Rust kernel still produces
its normal bounded owned results; packing writes one native buffer. Go can read
that buffer directly through an explicit result owner, avoiding result JSON and
its native-to-Go whole-buffer copy. Existing `Store.Query` materializes the same
owned Go result from this representation. `Store.QueryView` provides lazy access
without allocating a Go row object/slice for every returned row.

Request JSON, parsed-plan binding and the kernel's own result materialization
remain. This is zero-copy access across the result boundary, not a claim that the
entire graph query performs no copies. Text returned as an ordinary Go string,
metadata returned as bytes, and `Materialize` intentionally copy into Go ownership.
Numeric/ID/path access and text comparison read the native buffer directly.

`vg_buffer_acquire(buffer, vg_buffer_view*)` returns an independent registered
lease, pointer and length. The immutable bytes and their wire reservation survive
retirement of the original buffer handle. Releasing the lease ends that borrow;
C callers must synchronize raw-pointer use with release. The Go SDK hides both
the pointer and lease and uses one shared owner with a read/write lock: Close
waits for active accesses, clears the byte view and releases the lease. Copied
BatchView/RowView wrappers share the owner; subsequent access returns ErrClosed.
No raw borrowed slice or string escapes a safe Go method. Views are independent
of the store and require explicit Close; ordinary Query closes automatically.

## Result format: VGQR0001

All integers are little-endian uint64 words, with no alignment requirement and no
Rust/C struct casting. Signed integers and finite reals use their 64-bit bit
patterns. Offset/length arithmetic is checked before reading or allocating.

The 64-byte header contains eight words:

| Byte offset | Value |
|---:|---|
| 0 | ASCII `VGQR0001` |
| 8 | Captured sequence/V |
| 16 | Number of subquery results |
| 24 | Total row count |
| 32 | Metadata byte offset |
| 40 | Metadata byte length |
| 48 | Flags: bit 0 metadata present; other bits zero |
| 56 | Complete buffer byte length |

Immediately following the header are 16-byte subquery descriptors
`[first_row, row_count]`, forming a contiguous partition of the row table. Then
come 24-byte rows `[tag, a, b]`. Metadata and variable payloads follow these tables.
Offsets address the beginning of the whole buffer. Variable payloads follow
metadata in row order with no gaps, overlap or trailing bytes. This canonical
layout bounds aggregate owned materialization by frame size; repeated rows may
not alias a shared large payload to amplify Go allocations.

| Tag | Row | a | b |
|---:|---|---|---|
| 1 | Vertex | ID | 0 |
| 2 | Edge | ID | 0 |
| 3 | Integer value | int64 bits | 0 |
| 4 | Real value | IEEE754 bits | 0 |
| 5 | Boolean value | 0 or 1 | 0 |
| 6 | Null value | 0 | 0 |
| 7 | Text value | UTF-8 byte offset | byte length |
| 8 | Count | count | 0 |
| 9 | Path | element byte offset | element count |

A path element is 16 bytes `[tag (1 vertex / 2 edge), ID]`. Empty text/path is
valid. Absent temporal metadata and present empty memory metadata remain distinct.
The decoder rejects unsupported magic/flags, invalid row partitions, overflow,
out-of-range payloads, invalid tags/UTF-8/nonfinite reals and malformed paths.
Existing row/work/output/aggregate-wire/handle limits still apply. The packing
size is admitted before allocation; cancellation and allocation errors release
all pending output reservations. Existing JSON calls retain their JSON schema.

## Validation

Compare JSON and packed results for all row/value kinds, empty results, duplicate
starts, Unicode/NUL, signed extremes, maximum IDs, negative zero, paths and both
backends. Exercise malformed binary layouts with hand-derived fixtures, exact
output-capacity boundaries, lease retention, Close racing reads, repeated Close,
resource exhaustion and cancellation. Keep scalar/path access allocation-free.
Run the old complete Query benchmark unchanged; measure QueryView separately with
its consumption and Close included. Report Go CPU profiles and allocation counts
separately from latency runs, with no concurrent builds/tests during timing.
