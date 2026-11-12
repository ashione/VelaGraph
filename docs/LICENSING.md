# Licensing

VelaGraph project-authored code and documentation are licensed under the
Apache License, Version 2.0, except where a file or component carries a separate
license declaration. See the repository-root [LICENSE](../LICENSE).

## Existing component licenses

The following crates retain their existing `MIT OR Apache-2.0` declarations:

- `crates/d2-raft`
- `crates/replicated-graph`
- `crates/checkpoint-object-store`

These declarations are not replaced by the repository default. This change does
not withdraw permissions already granted for earlier versions of these crates.

## Third-party material

Third-party code and dependencies retain their own licenses, copyright notices,
and attribution requirements; the repository default does not relicense them.
In particular, the OpenRaft-derived log store is documented in
[`crates/d2-raft/NOTICE`](../crates/d2-raft/NOTICE), with its original MIT and
Apache-2.0 license texts retained alongside it.

Distributors must review the licenses and notices of the dependencies actually
included in their source or binary distribution. A root LICENSE is not a complete
third-party attribution bundle. Review changes to Cargo.lock and bundled code
when preparing a release.

Do not copy external code into this repository without checking its license and
preserving the applicable notices and provenance. No separate CLA or DCO
requirement is introduced by this licensing change.
