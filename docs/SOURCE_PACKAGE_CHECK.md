# Source-only package structure check

This is a finite packaging check, not a release, a build or clean-system
acceptance. The planned initial product release remains source-only v0.0.1;
Cargo package versions are separate. The release gates in [RELEASE_PLAN.md](RELEASE_PLAN.md)
and historical risk dispositions remain mandatory.

From a checkout, create an uncompressed, **unprefixed** source tar:

```sh
git archive --format=tar HEAD > /tmp/velagraph-source.tar
python3 scripts/check_source_archive.py /tmp/velagraph-source.tar
python3 -m unittest discover -s scripts -p 'test_check_source_archive.py'
```

The checker itself needs Python 3 and no Git, Rust/native build, dependency
cache or network access. It does not extract or execute archive members. The
caller creates the archive; the checker can run separately without `.git`.

The `unprefixed-source-tar-v1` profile checks:

- Nonempty root license, OpenRaft-derived NOTICE and both upstream license
  copies, Cargo manifests/lockfile and the documented quick-start source,
  schema, launcher and release/risk documentation entrypoints.
- No `.git` path components, absolute/traversal/aliased paths, duplicate entries,
  file/directory collisions, links, sparse files or special files. Ordinary
  files and directories only. Ordinary file names cannot end in `/`; directory
  trailing slashes are normalized by the tar reader. Symlinks are not part of
  this initial profile.
- At most 64 MiB input, 10,000 entries, 16 MiB per file and 128 MiB total file
  bytes; a bounded input snapshot is used for both hashing and inspection.
  Two terminating zero blocks are required; only block-aligned zero padding may
  follow. Appended garbage or a second tar is rejected rather than ignored.
  Compressed or top-level-prefixed archives are deliberately unsupported.
- Success-only JSON with the archive SHA-256, counts and required-file hashes.
  Missing, empty, malformed and over-budget inputs exit nonzero.

The list checks packaging **presence**, not license legal sufficiency, source
completeness, dependency vendoring, a security audit, or trust in archive
contents. Hashes identify the inspected bytes; they are not signatures or an
authentication proof. No archive content should be run merely because this
check passes. It does not validate Markdown links or execute QuickStart.

CI creates a tar from the exact event head and retains the commit, tree and
report without a rebuild or package publication. The checker’s fixtures cover
missing required files, empty files, malformed/truncated input, path/link/type
rejection, collisions and limits. Real source-archive startup and examples
still need separate final-candidate execution with dependencies stated; cached
builds do not establish a clean-machine install. This gate creates no tag or
release and grants no waiver for #128/#247 or #224 acceptance.
