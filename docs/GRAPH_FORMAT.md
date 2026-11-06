# Graph storage format (v1)

This document fixes the K1 on-disk contract. It is intentionally boring:
versioned keys, deterministic protobuf-compatible payloads, and one atomic
partition per mutation.

## Keys

```text
graph/v1/p/<partition>/vertex/<vertex-id>            -> binary vertex
graph/v1/p/<partition>/edge/<edge-id>                -> binary edge
graph/v1/p/<partition>/out/<source>/<label>/<edge>   -> empty
graph/v1/p/<partition>/in/<target>/<label>/<edge>    -> empty
```

- `v1` is the format version. A future incompatible layout uses a new segment;
  old data is never silently reinterpreted.
- `<partition>` is a big-endian `u32`.
- Vertex and edge ids are big-endian `u64`.
- Labels match `[A-Za-z_][A-Za-z0-9_]{0,63}`; the trailing `/` makes the label
  an unambiguous separator, including the `A` / `AB` prefix case.
- A vertex and its adjacency entries share a partition. A mutation's
  `WriteBatch` is atomic only inside that partition. Cross-partition
  transactions belong to D4, not this layer.
- v1 requires both endpoints of an edge in the same partition. Cross-partition
  boundary edges need a D1 placement protocol and are deliberately not
  pretended here.

## Payload schema

Payloads are hand-encoded protobuf wire format: field numbers and wire types
below are the compatibility contract. The same schema is checked in as
[`proto/graph.proto`](../proto/graph.proto) for future RPC/Raft codegen; a
generated `.proto` is not required at build time, and no `protoc` dependency is
added.

```proto
message Record {
  uint32 version = 1;   // 1
  Kind kind = 2;        // 1 = vertex, 2 = edge
  bytes payload = 3;
}

message Vertex {
  uint64 id = 1;
  repeated string labels = 2;
  repeated Property properties = 3;
}

message Edge {
  uint64 id = 1;
  uint64 from = 2;
  uint64 to = 3;
  string label = 4;
  repeated Property properties = 5;
}

message Property {
  string name = 1;
  Value value = 2;
}

message Value {
  oneof kind {
    bool   null    = 1;
    sint64 integer = 2;
    double real    = 3;
    string text    = 4;
    bool   boolean = 5;
  }
}
```

Properties are encoded in name order, so equal logical records have equal
bytes. This matters for Raft log comparison, hashing, and golden tests.

## Decode rules

- Unknown fields are skipped for forward compatibility; they are not preserved
  when a record is rewritten. A rolling upgrade must therefore gate writes to
  nodes that understand the new fields. Existing field numbers and wire types
  must never change.
- A missing version, kind, payload, id, edge endpoint, label, property name or
  property value is a corruption error.
- Duplicate property names, truncated fields, invalid UTF-8, non-finite REALs,
  records over 16 MiB, more than 4096 labels, and more than 65,536 properties
  are rejected.
- `v1` records are not self-describing beyond `Record.version`; the key's
  `v1` segment and `Record.kind` must both agree with the access path.

## Golden vectors

Vertex `{id: 1, labels: ["A"], properties: {x: 1}}`:

```text
080110011a0e08011201411a070a017812021002
```

Edge `{id: 7, from: 1, to: 2, label: "K", properties: {x: true}}`:

```text
080110021a1208071001180222014b2a070a017812022801
```

Any change to these bytes requires a new format version and a migration plan.

## Isolated v2 append-only experiment

[EXPERIMENTAL_CROSS_EDGES.md](EXPERIMENTAL_CROSS_EDGES.md) documents a separate
versioned namespace and explicitly non-atomic source/reverse append protocol.
It does not change this document's existing v1/K3/Gremlin contract or complete
D4/D5. Its out/in observations cannot certify missing-index completeness.

### Reviewed experimental edge deletion extension

The opt-in v2 edge mutation API additionally supports permanent tombstone deletion.
It is an explicit R2/R4/R12 scope exception, not a change to v1 single-batch,
failed-create, cascade, K3 or query contracts. Cross-shard source tombstone/out
removal and target in removal are staged, not atomic; same-shard deletion uses
one CAS batch. IDs are never reused. Reads validate visible residual indexes and
charge tombstone candidates before skipping them. Recovery and GC are unsupported.
See [deletion contract and versioned format](EXPERIMENTAL_CROSS_EDGES.md#additive-permanent-edge-deletion)
for upgrade restrictions, bounds and attempt-local retry outcomes.
