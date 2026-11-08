#!/usr/bin/env python3
"""Mandatory release functional gate for the synthetic retained-write lifecycle."""
import hashlib
import json
import os
import pathlib
import re
import shutil
import subprocess
import sys
import time


TEST = 'byte_bound_independent_processes_1025_writes_retain_original_lifecycle'
BUILD = ['cargo', 'test', '--release', '--locked', '-p', 'replicated-graph',
         '--test', 'managed_process', '--no-run', '--message-format=json']


def require(ok, message):
    if not ok:
        raise ValueError(message)


def sha256(path):
    with path.open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def source_snapshot(root, out):
    """Hash working files, not just HEAD/index; include non-ignored untracked files."""
    names = subprocess.check_output(
        ['git', 'ls-files', '--cached', '--others', '--exclude-standard', '-z'], cwd=root)
    target = (root / os.environ.get('CARGO_TARGET_DIR', 'target')).resolve()
    files = []
    for name in sorted(set(os.fsdecode(n) for n in names.split(b'\0') if n)):
        path = root / name
        # Build products, Python bytecode and this run's evidence are not sources.
        if (path.is_relative_to(out) or path.is_relative_to(target)
                or '__pycache__' in path.parts):
            continue
        entry = {'path': name}
        if path.is_symlink():
            entry.update(kind='symlink', target=os.readlink(path), sha256=sha256(path))
        elif path.exists():
            require(path.is_file(), f'non-file source entry: {name}')
            entry.update(kind='file', executable=bool(path.stat().st_mode & 0o111),
                         sha256=sha256(path))
        else:
            entry['kind'] = 'deleted'
        files.append(entry)
    require(bool(files), 'empty source manifest')
    manifest = {'format': 1, 'files': files}
    encoded = json.dumps(manifest, sort_keys=True, separators=(',', ':')).encode()
    return {'sha256': hashlib.sha256(encoded).hexdigest(), **manifest}


def select_artifact(records, root):
    require(all(isinstance(r, dict) for r in records), 'invalid Cargo JSON record')
    candidates = [r for r in records if r.get('reason') == 'compiler-artifact'
                  and r.get('target', {}).get('name') == 'managed_process']
    require(len(candidates) == 1, 'missing or multiple managed_process Cargo artifacts')
    artifact = candidates[0]
    target, profile = artifact.get('target', {}), artifact.get('profile', {})
    require(target.get('kind') == ['test'] and target.get('test') is True
            and target.get('src_path') == str(root / 'crates/replicated-graph/tests/managed_process.rs')
            and artifact.get('manifest_path') == str(root / 'crates/replicated-graph/Cargo.toml'),
            'wrong managed_process package, target kind, or source')
    require(profile.get('test') is True and profile.get('debug_assertions') is False
            and profile.get('opt_level') in ('1', '2', '3', 's', 'z'),
            'managed_process must be an optimized release test without debug assertions')
    require(isinstance(artifact.get('executable'), str) and artifact['executable'],
            'missing managed_process executable')
    endings = [r for r in records if r.get('reason') == 'build-finished']
    require(len(endings) == 1 and endings[0].get('success') is True,
            'missing successful Cargo build completion')
    return artifact


def validate_output(stdout, exit_code):
    require(exit_code == 0, f'lifecycle test exited {exit_code}')
    require(re.findall(r'^running (\d+) tests?$', stdout, re.M) == ['1'],
            'lifecycle selection did not run exactly one test')
    results = re.findall(r'^test (\S+) \.\.\. (.*)$', stdout, re.M)
    require(results == [(TEST, 'ok')], 'wrong, ignored, skipped, or failed lifecycle test')
    summaries = re.findall(r'^test result:.*$', stdout, re.M)
    require(len(summaries) == 1 and re.fullmatch(
        r'test result: ok\. 1 passed; 0 failed; 0 ignored; 0 measured; '
        r'\d+ filtered out; finished in [0-9.]+s', summaries[0]),
        'missing exact one-pass, zero-ignored lifecycle result')


def validate_checkpoint(checkpoint, source_hash, elf_hash, executable):
    require(isinstance(checkpoint, dict), 'invalid lifecycle checkpoint')
    require(type(checkpoint.get('format')) is int and checkpoint['format'] == 1,
            'wrong lifecycle checkpoint format')
    require(checkpoint.get('source_sha256') == source_hash
            and checkpoint.get('elf_sha256') == elf_hash
            and checkpoint.get('executable') == str(executable),
            'checkpoint source, ELF, or executable identity mismatch')
    require(checkpoint.get('completed') is True
            and type(checkpoint.get('verified_ordinal')) is int
            and checkpoint['verified_ordinal'] == 1025
            and 'pending_external_id' in checkpoint
            and checkpoint['pending_external_id'] is None,
            'incomplete lifecycle checkpoint or pending external request')
    require(checkpoint.get('ack_loss_recovered') is True
            and checkpoint.get('snapshots_restored') is True,
            'missing ACK-loss recovery or full snapshot restores')
    # Validate retained evidence presence; Rust owns the actual protocol assertions.
    binding = checkpoint.get('binding', {})
    require(isinstance(binding, dict)
            and binding.get('cohort', {}).get('profile') == 'ByteBound16MiBV1',
            'missing byte-bound cohort binding')
    for key, expected in [('originals', {'1', '256', '1024', '1025'}),
                          ('proofs', {'0', '1', '256', '1024', '1025'})]:
        values = checkpoint.get(key)
        require(isinstance(values, dict) and set(values) == expected
                and all(values.values()), f'missing retained {key}')
    grants = checkpoint.get('grants')
    require(isinstance(grants, list) and len(grants) == 2
            and all(isinstance(g, dict) and isinstance(g.get('member'), dict) for g in grants)
            and {g['member'].get('shard') for g in grants} == {7, 8},
            'missing complete-cohort grants')


def run_gate(root, out):
    out.mkdir()  # A CI invocation never resumes, overwrites or accepts old evidence.
    metadata = {'status': 'INCOMPLETE', 'test': TEST, 'build_command': BUILD,
                'executions': []}

    def save():
        (out / 'execution.json').write_text(json.dumps(metadata, indent=2) + '\n')

    def summary(text):
        (out / 'summary.md').write_text(text + '\n')

    def run(name, command, env, stdout_suffix):
        started = time.monotonic()
        with (out / f'{name}.{stdout_suffix}').open('x') as stdout, \
                (out / f'{name}.stderr').open('x') as stderr:
            result = subprocess.run(command, cwd=root, env=env, stdout=stdout, stderr=stderr)
        metadata['executions'].append({'name': name, 'command': command,
                                       'exit_code': result.returncode,
                                       'wall_seconds': time.monotonic() - started})
        save()
        require(result.returncode == 0, f'{name} exited {result.returncode}; inspect retained logs')
        return result.returncode

    try:
        summary('Managed lifecycle INCOMPLETE: build, exact test exit and checkpoint must all pass.')
        save()
        source = source_snapshot(root, out)
        (out / 'source.json').write_text(json.dumps(source, indent=2) + '\n')
        metadata['source_sha256'] = source['sha256']
        env = {**os.environ, 'CARGO_TERM_COLOR': 'never'}
        run('build', BUILD, env, 'jsonl')
        records = [json.loads(line) for line in (out / 'build.jsonl').read_text().splitlines() if line]
        artifact = select_artifact(records, root)
        cargo_executable = pathlib.Path(artifact['executable']).resolve(strict=True)
        require(cargo_executable.is_file() and os.access(cargo_executable, os.X_OK),
                'ELF is not executable')
        with cargo_executable.open('rb') as stream:
            require(stream.read(4) == b'\x7fELF', 'managed_process artifact is not an ELF')
        cargo_hash = sha256(cargo_executable)
        executable = out / 'managed_process'
        shutil.copy2(cargo_executable, executable)
        elf_hash = sha256(executable)
        require(elf_hash == cargo_hash, 'frozen ELF differs from the selected Cargo artifact')
        metadata.update(artifact=artifact, cargo_executable=str(cargo_executable),
                        cargo_elf_sha256=cargo_hash, executable=str(executable), elf_sha256=elf_hash)
        save()
        require(source_snapshot(root, out)['sha256'] == source['sha256'],
                'source changed during the build')
        retained = out / 'lifecycle'
        retained.mkdir()
        env.update(BYTE_BOUND_PROCESS_DIRECTORY=str(retained),
                   BYTE_BOUND_PROCESS_SOURCE_SHA256=source['sha256'],
                   BYTE_BOUND_PROCESS_ELF_SHA256=elf_hash)
        command = [str(executable), TEST, '--exact', '--nocapture', '--test-threads=1',
                   '--format=pretty', '--color=never']
        exit_code = run('test', command, env, 'stdout')
        validate_output((out / 'test.stdout').read_text(), exit_code)
        checkpoint = json.loads((retained / 'progress.json').read_text())
        validate_checkpoint(checkpoint, source['sha256'], elf_hash, executable)
        require(source_snapshot(root, out)['sha256'] == source['sha256'],
                'source changed during the lifecycle test')
        require(sha256(executable) == elf_hash, 'ELF changed during the lifecycle test')
        metadata['status'] = 'COMPLETE'
        save()
        summary('Managed lifecycle COMPLETE: exactly one release test passed, 1025 writes verified, '
                'no pending request, ACK-loss recovery and full snapshot restores verified.\n\n'
                'Synthetic same-host functional evidence, not a performance or multi-host HA claim.\n\n'
                f"Source SHA-256: {source['sha256']}\n\nELF SHA-256: {elf_hash}")
        print('Managed lifecycle COMPLETE: exact release test exit and checkpoint validated.')
    except (Exception, KeyboardInterrupt) as error:
        metadata.update(status='INCOMPLETE', error=str(error))
        save()
        summary(f'Managed lifecycle INCOMPLETE: {error}. Inspect retained logs and checkpoint.')
        raise


def main():
    try:
        root = pathlib.Path(__file__).resolve().parents[1]
        out = pathlib.Path(sys.argv[1] if len(sys.argv) > 1 else 'managed-lifecycle-ci-results').resolve()
        run_gate(root, out)
    except (Exception, KeyboardInterrupt) as error:
        print(f'Managed lifecycle INCOMPLETE: {error}', file=sys.stderr)
        return 1
    return 0


if __name__ == '__main__':
    sys.exit(main())
