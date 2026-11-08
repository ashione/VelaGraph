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


ORIGINAL_KEYS = {1, 256, 1024, 1025}
PROOF_KEYS = {0, 1, 256, 1024, 1025}
PROOF_LIMIT = (4 * 16 * 1024 * 1024 + 2) // 3 + 64 * 1024
ORIGINAL_LIMIT = len(b'{"id":[],"request":[],"payload":[],"transaction_id":[]}') + 4 * (64 * 1024 + 256 * 1024 + 32)
FRAME_LIMIT = 32 * 1024 * 1024
LEGACY_LIMIT = FRAME_LIMIT + 4 * ORIGINAL_LIMIT + 5 * (4 * PROOF_LIMIT + 32)
CHECKPOINT_FIELDS = {'format', 'binding', 'source_sha256', 'elf_sha256', 'executable',
                     'harness_timeout_secs', 'verified_ordinal', 'pending_external_id',
                     'originals', 'proofs', 'grants', 'ack_loss_recovered',
                     'snapshots_restored', 'completed'}


def unique_json(data):
    def object_pairs(pairs):
        value = {}
        for key, item in pairs:
            require(key not in value, f'duplicate JSON key: {key}')
            value[key] = item
        return value
    return json.loads(data, object_pairs_hook=object_pairs,
                      parse_constant=lambda value: require(False, f'invalid JSON number: {value}'))


def fields(value, expected, label):
    require(isinstance(value, dict) and set(value) == set(expected), f'invalid {label} fields')


def integer(value, minimum=0, maximum=2**64 - 1):
    return type(value) is int and minimum <= value <= maximum


def byte_array(value, length=None, maximum=None):
    return (isinstance(value, list) and (length is None or len(value) == length)
            and (maximum is None or len(value) <= maximum)
            and all(integer(v, 0, 255) for v in value))


def same_json(left, right):
    if type(left) is not type(right):
        return False
    if isinstance(left, dict):
        return set(left) == set(right) and all(same_json(left[k], right[k]) for k in left)
    if isinstance(left, list):
        return len(left) == len(right) and all(same_json(a, b) for a, b in zip(left, right))
    return left == right


def expected_binding():
    return {'authority': {'shard': 9, 'mode': [9] * 16},
            'cohort': {'coordinator_shard': 9, 'coordinator_epoch': 1,
                       'members': [{'shard': n, 'epoch': 1, 'mode': [n] * 16} for n in (7, 8)],
                       'profile': 'ByteBound16MiBV1'},
            'mapping': 'PartitionEqualsShardV1'}


def original_id(ordinal):
    return list(ordinal.to_bytes(16, 'big'))


def validate_original(value, ordinal):
    fields(value, {'id', 'request', 'payload', 'transaction_id'}, 'original RequestRecord')
    require(byte_array(value['id'], 16) and value['id'] == original_id(ordinal)
            and byte_array(value['transaction_id'], 16) and any(value['transaction_id'])
            and byte_array(value['request'], maximum=64 * 1024)
            and byte_array(value['payload'], maximum=256 * 1024), 'invalid original bytes/ID')
    request = unique_json(bytes(value['request']))
    require(same_json(request, ['managed-graph-command-v1', expected_binding(),
                        {'CreateVertex': {'partition': 7, 'vertex': {
                            'id': ordinal, 'labels': ['person'], 'properties': {}}}}]),
            'original canonical request changed')
    plan = unique_json(bytes(value['payload']))
    fields(plan, {'version', 'id', 'authority', 'cohort', 'validation_version', 'parts'}, 'original Plan')
    require(type(plan['version']) is int and plan['version'] == 1
            and byte_array(plan['id'], 16) and plan['id'] == value['transaction_id']
            and same_json(plan['authority'], expected_binding()['authority'])
            and same_json(plan['cohort'], expected_binding()['cohort'])
            and type(plan['validation_version']) is int and plan['validation_version'] == ordinal - 1
            and isinstance(plan['parts'], list) and len(plan['parts']) == 1, 'invalid original Plan identity')
    part = plan['parts'][0]
    fields(part, {'descriptor', 'request'}, 'Plan part')
    descriptor = part['descriptor']
    fields(descriptor, {'shard', 'epoch', 'mode', 'intent'}, 'participant descriptor')
    require(integer(descriptor['shard'], 7, 7) and integer(descriptor['epoch'], 1, 1) and byte_array(descriptor['mode'], 16) and descriptor['mode'] == [7] * 16
            and byte_array(descriptor['intent'], 32) and any(descriptor['intent']), 'invalid Plan participant')
    write = part['request']
    fields(write, {'shard', 'epoch', 'request_id', 'expected_sequence', 'operations'}, 'Plan write')
    require(integer(write['shard'], 7, 7) and integer(write['epoch'], 1, 1) and byte_array(write['request_id'], 16) and write['request_id'] == plan['id']
            and type(write['expected_sequence']) is int and write['expected_sequence'] == ordinal - 1
            and isinstance(write['operations'], list) and 1 <= len(write['operations']) <= 10000,
            'invalid Plan request')
    total = 0
    for operation in write['operations']:
        require(isinstance(operation, dict) and len(operation) == 1, 'invalid Plan operation')
        kind = next(iter(operation))
        require(kind in ('Put', 'Delete'), 'unknown Plan operation')
        payload = operation[kind]
        fields(payload, {'key', 'value'} if kind == 'Put' else {'key'}, 'Plan operation')
        for data in payload.values():
            require(byte_array(data), 'invalid Plan operation bytes')
            total += len(data)
    require(total <= 16 * 1024 * 1024, 'Plan operation byte budget exceeded')
    # Rust owns application semantics and actual fresh-RPC equality assertions.


def validate_proof(value, version):
    fields(value, {'version', 'authority', 'cohort', 'through', 'segments'}, 'terminal proof')
    require(type(value['version']) is int and value['version'] == 2
            and byte_array(value['authority'], 16) and value['authority'] == [9] * 16 and same_json(value['cohort'], expected_binding()['cohort'])
            and type(value['through']) is int and value['through'] == version
            and isinstance(value['segments'], list) and len(value['segments']) == (version + 31) // 32,
            'invalid proof identity/version')
    ids = set()
    ordinal = 0
    for segment in value['segments']:
        require(isinstance(segment, list) and len(segment) == min(32, version - ordinal), 'invalid proof segment')
        for terminal in segment:
            ordinal += 1
            fields(terminal, {'id', 'ordinal', 'participants', 'prepared', 'decision'}, 'terminal')
            require(byte_array(terminal['id'], 16) and any(terminal['id'])
                    and tuple(terminal['id']) not in ids and type(terminal['ordinal']) is int
                    and terminal['ordinal'] == ordinal and terminal['decision'] == 'Commit'
                    and terminal['prepared'] == [True] and type(terminal['prepared'][0]) is bool
                    and isinstance(terminal['participants'], list) and len(terminal['participants']) == 1,
                    'invalid complete terminal Commit')
            ids.add(tuple(terminal['id']))
            participant = terminal['participants'][0]
            fields(participant, {'shard', 'epoch', 'mode', 'intent'}, 'terminal participant')
            require(integer(participant['shard'], 7, 7) and integer(participant['epoch'], 1, 1) and byte_array(participant['mode'], 16) and participant['mode'] == [7] * 16
                    and byte_array(participant['intent'], 32) and any(participant['intent']), 'invalid proof participant')
    require(ordinal == version, 'incomplete proof')


def open_directory(path):
    # Walk each component, not just the leaf: no symlink parent substitution.
    path = pathlib.Path(path)
    flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC
    fd = os.open('/' if path.is_absolute() else '.', flags)
    try:
        for name in path.parts:
            if name in ('/', '.'):
                continue
            require(name != '..', 'noncanonical lifecycle directory')
            child = os.open(name, flags, dir_fd=fd)
            os.close(fd)
            fd = child
        return fd
    except BaseException:
        os.close(fd)
        raise


def read_owned(directory_fd, name, limit):
    import stat
    fd = os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC, dir_fd=directory_fd)
    with os.fdopen(fd, 'rb') as stream:
        before = os.fstat(stream.fileno())
        require(stat.S_ISREG(before.st_mode) and before.st_nlink == 1
                and 0 < before.st_size <= limit, 'unsafe or oversized evidence file')
        data = stream.read(limit + 1)
        after = os.fstat(stream.fileno())
        require(len(data) == before.st_size == after.st_size, 'evidence changed while reading')
        return data


def read_checkpoint(directory):
    root = open_directory(directory)
    try:
        return unique_json(read_owned(root, 'progress.json', FRAME_LIMIT))
    finally:
        os.close(root)


def validate_checkpoint(checkpoint, source_hash, elf_hash, executable, directory):
    fields(checkpoint, CHECKPOINT_FIELDS | {'migration'}, 'v2 checkpoint')
    require(type(checkpoint['format']) is int and checkpoint['format'] == 2,
            'fresh lifecycle acceptance requires v2; v1 is migration input only')
    require(checkpoint['source_sha256'] == source_hash and checkpoint['elf_sha256'] == elf_hash
            and checkpoint['executable'] == str(executable), 'checkpoint source, ELF, or executable identity mismatch')
    require(same_json(checkpoint['binding'], expected_binding()), 'wrong complete cohort binding')
    require(integer(checkpoint['harness_timeout_secs'], 1, 86400), 'invalid harness timeout')
    require(checkpoint['completed'] is True and type(checkpoint['verified_ordinal']) is int
            and checkpoint['verified_ordinal'] == 1025 and checkpoint['pending_external_id'] is None,
            'incomplete lifecycle checkpoint or pending external request')
    require(checkpoint['ack_loss_recovered'] is True and checkpoint['snapshots_restored'] is True,
            'missing ACK-loss recovery or full snapshot restores')
    grants = checkpoint['grants']
    require(isinstance(grants, list) and len(grants) == 2, 'missing complete-cohort grants')
    for grant, member in zip(grants, expected_binding()['cohort']['members']):
        fields(grant, {'member', 'activation'}, 'member grant')
        require(same_json(grant['member'], member), 'grant member mismatch')
        activation = grant['activation']
        fields(activation, {'nonce', 'log_id'}, 'member activation')
        require(byte_array(activation['nonce'], 32) and any(activation['nonce']), 'invalid activation nonce')
        marker = activation['log_id']
        fields(marker, {'leader_id', 'index'}, 'activation LogId')
        fields(marker['leader_id'], {'term', 'node_id'}, 'activation leader')
        require(integer(marker['index'], 1) and integer(marker['leader_id']['term'], 1)
                and integer(marker['leader_id']['node_id']), 'invalid activation LogId')
    root = open_directory(directory)
    artifacts = None
    try:
        artifacts = os.open('artifacts', os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC, dir_fd=root)
        expected_files = set()
        originals, proofs = {}, {}

        def payload(reference, kind, key, name, bound):
            fields(reference, {'format', 'kind', 'key', 'relative_path', 'byte_length', 'sha256'}, 'artifact ref')
            require(type(reference['format']) is int and reference['format'] == 1
                    and reference['kind'] == kind and type(reference['key']) is int and reference['key'] == key
                    and reference['relative_path'] == 'artifacts/' + name
                    and integer(reference['byte_length'], 1, bound)
                    and isinstance(reference['sha256'], str) and re.fullmatch('[0-9a-f]{64}', reference['sha256']),
                    'invalid artifact kind/key/path/length/hash')
            require(name not in expected_files, 'duplicate artifact alias')
            expected_files.add(name)
            data = read_owned(artifacts, name, bound)
            require(len(data) == reference['byte_length'] and hashlib.sha256(data).hexdigest() == reference['sha256'],
                    'missing, corrupt, truncated or replaced artifact')
            return data

        for label, keys, kind, prefix, bound, found in [
                ('originals', ORIGINAL_KEYS, 'original', 'original-', ORIGINAL_LIMIT, originals),
                ('proofs', PROOF_KEYS, 'proof', 'proof-v', PROOF_LIMIT, proofs)]:
            refs = checkpoint[label]
            require(isinstance(refs, dict) and set(refs) == {str(key) for key in keys}, f'missing retained {label}')
            for key in sorted(keys):
                data = payload(refs[str(key)], kind, key, f'{prefix}{key}.json', bound)
                value = unique_json(data)
                (validate_original if kind == 'original' else validate_proof)(value, key)
                found[key] = (value, data)
        for version, (proof, _) in proofs.items():
            for ordinal, (original, _) in originals.items():
                if ordinal <= version:
                    index = ordinal - 1
                    require(proof['segments'][index // 32][index % 32]['id'] == original['transaction_id'],
                            'original/proof transaction identity mismatch')
        migration = checkpoint['migration']
        if migration is not None:
            raw = payload(migration, 'legacy_checkpoint', 1, 'legacy-checkpoint-v1.json',
                          LEGACY_LIMIT + len(str(executable).encode()) * 6)
            legacy = unique_json(raw)
            fields(legacy, CHECKPOINT_FIELDS, 'legacy migration checkpoint')
            require(type(legacy['format']) is int and legacy['format'] == 1, 'invalid migration format')
            for field in ('binding', 'source_sha256', 'elf_sha256', 'executable'):
                require(same_json(legacy[field], checkpoint[field]), 'cross-identity migration backup')
            through = legacy['verified_ordinal']
            require(integer(through, 0, 1025) and integer(legacy['harness_timeout_secs'], 1, 86400), 'invalid migration progress')
            pending = legacy['pending_external_id']
            require(pending is None or (through < 1025 and byte_array(pending, 16) and pending == original_id(through + 1)), 'invalid legacy pending ID')
            require((legacy['grants'] == [] or same_json(legacy['grants'], grants)), 'migration member generation changed')
            old_originals, old_proofs = legacy['originals'], legacy['proofs']
            require(isinstance(old_originals, dict) and isinstance(old_proofs, dict), 'invalid legacy evidence maps')
            required_originals = {str(n) for n in ORIGINAL_KEYS if n <= through}
            if through == 1024 and pending is not None and '1025' in old_originals:
                required_originals.add('1025')
            require(set(old_originals) == required_originals, 'missing legacy original')
            initial = through == 0 and pending is None and not old_originals and not old_proofs and not legacy['grants']
            require(initial or set(old_proofs) == {str(n) for n in PROOF_KEYS if n <= through}, 'missing legacy proof')
            require(initial or same_json(legacy['grants'], grants), 'missing legacy grants')
            require(type(legacy['ack_loss_recovered']) is bool and legacy['ack_loss_recovered'] == (through == 1025)
                    and type(legacy['snapshots_restored']) is bool and type(legacy['completed']) is bool
                    and (not legacy['snapshots_restored'] or through == 1025)
                    and (not legacy['completed'] or (legacy['snapshots_restored'] and pending is None)), 'unreachable legacy flags')
            for key, original in old_originals.items():
                require(same_json(original, originals[int(key)][0]), 'migration original changed')
            for key, proof in old_proofs.items():
                require(byte_array(proof, maximum=PROOF_LIMIT) and bytes(proof) == proofs[int(key)][1], 'migration proof bytes changed')
        require(set(os.listdir(artifacts)) == expected_files, 'artifact closure contains missing, orphan, staging or extra files')
    finally:
        if artifacts is not None:
            os.close(artifacts)
        os.close(root)


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
        checkpoint = read_checkpoint(retained)
        validate_checkpoint(checkpoint, source['sha256'], elf_hash, executable, retained)
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
