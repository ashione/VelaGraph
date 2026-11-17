#!/usr/bin/env python3
"""Validate the finite source-only packaging profile without extracting or building."""
import argparse
import hashlib
import json
import pathlib
import tarfile

MAX_ARCHIVE_BYTES = 64 * 1024 * 1024
MAX_FILE_BYTES = 16 * 1024 * 1024
MAX_TOTAL_BYTES = 128 * 1024 * 1024
MAX_ENTRIES = 10000
REQUIRED = (
    'LICENSE', 'README.md', 'CONTRIBUTING.md', 'Cargo.toml', 'Cargo.lock',
    'crates/d2-raft/NOTICE', 'crates/d2-raft/third-party/openraft/LICENSE-MIT',
    'crates/d2-raft/third-party/openraft/LICENSE-APACHE',
    'crates/d2-raft/Cargo.toml', 'crates/replicated-graph/Cargo.toml',
    'crates/checkpoint-object-store/Cargo.toml',
    'examples/quickstart.rs', 'scripts/managed-service-demo.sh',
    'crates/replicated-graph/examples/managed_service_demo.rs',
    'crates/replicated-graph/src/bin/graph-service.rs',
    'crates/replicated-graph/proto/public_managed.proto',
    'docs/MANAGED_SERVICE_DEMO.md', 'docs/RELEASE_PLAN.md',
    'docs/V0_0_1_CAPABILITY_MATRIX.md', 'docs/RELIABILITY_RELEASE_RISKS.md',
)


def checked_name(name, directory=False):
    # No extraction occurs. Reject aliases before duplicate/required matching.
    if name.endswith('/') and not directory:
        raise ValueError('ordinary file path must not end with slash')
    parts = name.rstrip('/').split('/')
    if (not name or name.startswith('/') or '\\' in name
            or any(p in ('', '.', '..', '.git') or ':' in p for p in parts)):
        raise ValueError('invalid or repository-metadata archive path')
    return '/'.join(parts)


def inspect_archive(path):
    path = pathlib.Path(path)
    if not path.is_file() or path.stat().st_size > MAX_ARCHIVE_BYTES:
        raise ValueError('archive must be a regular file at most 64 MiB')
    # Read a bounded immutable snapshot, so checksum and parsing use same bytes.
    with path.open('rb') as stream:
        data = stream.read(MAX_ARCHIVE_BYTES + 1)
    if len(data) > MAX_ARCHIVE_BYTES:
        raise ValueError('archive exceeds 64 MiB')
    import io
    seen = set()
    files = {}
    directories = set()
    total = 0
    with tarfile.open(fileobj=io.BytesIO(data), mode='r:') as archive:
        for index, member in enumerate(archive):
            if index >= MAX_ENTRIES:
                raise ValueError('archive exceeds entry limit')
            name = checked_name(member.name, directory=member.isdir())
            if name in seen:
                raise ValueError('duplicate archive path')
            seen.add(name)
            parents = {parent.as_posix() for parent in pathlib.PurePosixPath(name).parents}
            if parents.intersection(files):
                raise ValueError('file used as archive directory')
            directories.update(parents)
            if member.isdir():
                directories.add(name)
                continue
            if not member.isfile() or member.issparse():
                raise ValueError('only ordinary files and directories are supported')
            if member.size < 0 or member.size > MAX_FILE_BYTES:
                raise ValueError('archive member exceeds file size limit')
            total += member.size
            if total > MAX_TOTAL_BYTES:
                raise ValueError('archive exceeds total file size limit')
            payload = archive.extractfile(member)
            if payload is None:
                raise ValueError('missing archive payload')
            with payload:
                contents = payload.read(MAX_FILE_BYTES + 1)
            if len(contents) != member.size:
                raise ValueError('incomplete archive payload')
            if name in directories:
                raise ValueError('archive directory replaced with file')
            files[name] = {'bytes': member.size,
                           'sha256': hashlib.sha256(contents).hexdigest()}
        end = archive.offset
    # tarfile deliberately tolerates absent terminators and ignores bytes after
    # its first EOF. This packaging profile instead consumes the whole input:
    # two zero blocks are mandatory, with only block-aligned zero padding after.
    tail = data[end:]
    if len(data) % tarfile.BLOCKSIZE or len(tail) < 2 * tarfile.BLOCKSIZE or any(tail):
        raise ValueError('missing tar terminator or nonzero/truncated trailing data')
    for name in REQUIRED:
        if name not in files or files[name]['bytes'] == 0:
            raise ValueError('required nonempty source file missing: ' + name)
    return {'format': 1, 'profile': 'unprefixed-source-tar-v1',
            'archive_sha256': hashlib.sha256(data).hexdigest(),
            'files': len(files), 'entries': len(seen), 'file_bytes': total,
            'required_files': {name: files[name] for name in REQUIRED},
            'build_executed': False, 'release_accepted': False}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('archive', help='uncompressed, unprefixed git archive tar')
    args = parser.parse_args()
    try:
        result = inspect_archive(args.archive)
    except (OSError, ValueError, tarfile.TarError) as error:
        parser.exit(1, 'source archive rejected: ' + str(error) + '\n')
    print(json.dumps(result, sort_keys=True, indent=2))


if __name__ == '__main__':
    main()
