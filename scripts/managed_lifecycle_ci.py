#!/usr/bin/env python3
"""Mandatory release functional gate for the synthetic retained-write lifecycle."""
import argparse
import hashlib
import io
import json
import os
import pathlib
import re
import shutil
import stat
import struct
import subprocess
import sys
import time
import tomllib
import zipfile


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


# The artifact service supplies provenance; hashes below supply content integrity.
# These constants are intentionally not CLI/user-selectable download destinations.
BUILD_JOB = 'lifecycle-build'
CARGO_FILES = {'Cargo.lock': 'Cargo.lock', 'cargo-root.toml': 'Cargo.toml',
               'cargo-replicated-graph.toml': 'crates/replicated-graph/Cargo.toml',
               'cargo-d2-raft.toml': 'crates/d2-raft/Cargo.toml'}
HANDOFF_LIMITS = {'handoff.json': 1024 * 1024, 'source.json': 16 * 1024 * 1024,
                  'build.jsonl': 64 * 1024 * 1024, 'dependencies.json': 64 * 1024 * 1024,
                  'managed_process': 512 * 1024 * 1024,
                  **{name: 1024 * 1024 for name in CARGO_FILES}}
CONTEXT_KEYS = ('GITHUB_REPOSITORY', 'GITHUB_RUN_ID', 'GITHUB_RUN_ATTEMPT',
                'GITHUB_SHA', 'GITHUB_EVENT_NAME', 'GITHUB_REF', 'GITHUB_WORKFLOW',
                'LIFECYCLE_EVENT_HEAD_SHA', 'GITHUB_REPOSITORY_ID')


def ci_context():
    context = {key: os.environ.get(key, '') for key in CONTEXT_KEYS}
    require(all(isinstance(value, str) and value and '\n' not in value for value in context.values()),
            'missing CI handoff context')
    require(re.fullmatch('[0-9a-f]{40}', context['GITHUB_SHA'])
            and all(re.fullmatch('[1-9][0-9]*', context[key])
                    for key in ('GITHUB_RUN_ID', 'GITHUB_RUN_ATTEMPT')), 'invalid CI identity')
    require(context['GITHUB_EVENT_NAME'] in ('pull_request', 'push', 'schedule', 'workflow_dispatch')
            and re.fullmatch('[0-9a-f]{40}', context['LIFECYCLE_EVENT_HEAD_SHA'])
            and re.fullmatch('[1-9][0-9]*', context['GITHUB_REPOSITORY_ID']), 'unsupported event/head/repository')
    require(context['GITHUB_EVENT_NAME'] == 'pull_request'
            or context['LIFECYCLE_EVENT_HEAD_SHA'] == context['GITHUB_SHA'], 'non-PR event SHA mismatch')
    return context


def validate_provenance(data):
    context = ci_context()
    fields(data, {'artifact', 'run'}, 'artifact service response')
    artifact, run = data['artifact'], data['run']
    require(isinstance(artifact, dict) and isinstance(run, dict), 'invalid artifact service metadata')
    workflow = artifact.get('workflow_run')
    require(isinstance(workflow, dict), 'missing artifact workflow identity')
    require(str(artifact.get('id')) == os.environ.get('LIFECYCLE_ARTIFACT_ID')
            and artifact.get('name') == f"managed-lifecycle-build-{context['GITHUB_RUN_ID']}-{context['GITHUB_RUN_ATTEMPT']}"
            and artifact.get('expired') is False, 'wrong/expired builder artifact or old attempt')
    require(str(workflow.get('id')) == context['GITHUB_RUN_ID']
            and str(workflow.get('repository_id')) == context['GITHUB_REPOSITORY_ID']
            and workflow.get('head_sha') == context['LIFECYCLE_EVENT_HEAD_SHA'], 'wrong artifact run/repository/head')
    require(str(run.get('id')) == context['GITHUB_RUN_ID']
            and str(run.get('run_attempt')) == context['GITHUB_RUN_ATTEMPT']
            and run.get('head_sha') == context['LIFECYCLE_EVENT_HEAD_SHA']
            and run.get('event') == context['GITHUB_EVENT_NAME']
            and run.get('path') == '.github/workflows/corehr.yml'
            and isinstance(run.get('repository'), dict)
            and str(run['repository'].get('id')) == context['GITHUB_REPOSITORY_ID']
            and run['repository'].get('full_name') == context['GITHUB_REPOSITORY'],
            'wrong workflow run attempt, event, head, repository or workflow')


def read_provenance():
    require(os.environ.get('LIFECYCLE_BUILD_RESULT') == 'success', 'build job did not succeed')
    require(re.fullmatch('[1-9][0-9]*', os.environ.get('LIFECYCLE_ARTIFACT_ID', '')),
            'missing required build-job artifact ID')
    path = pathlib.Path(os.environ['LIFECYCLE_ARTIFACT_METADATA'])
    directory = open_directory(path.parent)
    try:
        data = unique_json(read_owned(directory, path.name, 1024 * 1024))
        validate_provenance(data)
        return data
    finally:
        os.close(directory)


def checkout_identity(root):
    commit, tree = subprocess.check_output(
        ['git', 'rev-parse', 'HEAD', 'HEAD^{tree}'], cwd=root).decode().splitlines()
    require(all(re.fullmatch('[0-9a-f]{40}', value) for value in (commit, tree)),
            'invalid checkout commit/tree')
    require(not subprocess.check_output(['git', 'status', '--porcelain', '--untracked-files=no'], cwd=root),
            'handoff requires clean tracked source')
    require(commit == ci_context()['GITHUB_SHA'], 'checkout is not the exact event SHA')
    return {'commit': commit, 'tree': tree}


def dependency_manifest(root, records, build_root):
    """Cross-check actual compiled dependencies against independently checked-in inputs."""
    locked = tomllib.loads((root / 'Cargo.lock').read_text())['package']
    allowed = {(p['name'], p['version'], p.get('source')) for p in locked}
    local = {}
    for relative in CARGO_FILES.values():
        if relative.endswith('.toml'):
            package = tomllib.loads((root / relative).read_text())['package']
            local[str(build_root / relative)] = (package['name'], package['version'])
    compiled = []
    identities = set()
    workspace = set()
    for row in records:
        if row.get('reason') != 'compiler-artifact':
            continue
        package_id = row.get('package_id')
        require(isinstance(package_id, str) and '#' in package_id, 'missing compiled dependency ID')
        source, versioned = package_id.rsplit('#', 1)
        manifest_path = row.get('manifest_path')
        if manifest_path in local:
            name, version = local[manifest_path]
            require(source == 'path+' + pathlib.Path(manifest_path).parent.as_uri()
                    and versioned in (version, f'{name}@{version}'), 'wrong workspace dependency identity')
            lock_source = None
            workspace.add(manifest_path)
        else:
            require('@' in versioned and source.startswith('registry+'), 'unrecognized compiled dependency source')
            name, version = versioned.rsplit('@', 1)
            lock_source = source
        require((name, version, lock_source) in allowed, 'compiled dependency not in exact Cargo.lock')
        features = row.get('features')
        require(isinstance(features, list) and all(isinstance(feature, str) for feature in features)
                and features == sorted(set(features)), 'missing/noncanonical compiled dependency features')
        require(isinstance(row.get('profile'), dict) and isinstance(row.get('target'), dict),
                'missing compiled dependency profile/target')
        if name == 'd2-raft':
            require(features == ['default', 'test-hooks'], 'wrong d2-raft release test features')
            require(row['target'].get('name') == 'd2_raft' and row['target'].get('kind') == ['lib']
                    and row['target'].get('src_path') == str(build_root / 'crates/d2-raft/src/lib.rs')
                    and row['profile'].get('opt_level') == '3'
                    and row['profile'].get('debug_assertions') is False
                    and integer(row['profile'].get('debuginfo'), 0, 0) and row['profile'].get('test') is False,
                    'wrong d2-raft release library artifact')
        observed = {key: row[key] for key in ('package_id', 'manifest_path', 'target', 'profile', 'features')}
        identity = json.dumps(observed, sort_keys=True)
        require(identity not in identities, 'duplicate compiled dependency artifact')
        identities.add(identity)
        compiled.append(observed)
    require(workspace == set(local), 'missing workspace compiled dependency')
    require(any(row['target'].get('name') == 'd2_raft' and row['target'].get('kind') == ['lib']
                for row in compiled), 'missing actual d2-raft library artifact')
    return {'format': 1, 'lock_sha256': sha256(root / 'Cargo.lock'), 'compiled': compiled,
            'cargo_manifests': {relative: sha256(root / relative)
                                for relative in CARGO_FILES.values() if relative.endswith('.toml')}}


def validate_elf(data):
    require(len(data) >= 64 and data[:7] == b'\x7fELF\x02\x01\x01', 'not a 64-bit little-endian ELF')
    header = struct.unpack('<16sHHIQQQIHHHHHH', data[:64])
    require(header[1] in (2, 3) and header[2:4] == (62, 1) and header[8] == 64
            and header[9] == 56 and header[10] > 0
            and 64 <= header[5] <= len(data) - header[9] * header[10],
            'invalid Linux x86_64 ELF header/program table')


def validate_handoff_artifact(artifact, build_root):
    require(artifact.get('features') == [] and artifact['profile'].get('opt_level') == '3'
            and integer(artifact['profile'].get('debuginfo'), 0, 0)
            and artifact['profile'].get('overflow_checks') is False, 'wrong exact release profile/features')
    executable = pathlib.Path(artifact['executable'])
    require(executable.parent == build_root / 'target/release/deps'
            and re.fullmatch('managed_process-[0-9a-f]+', executable.name)
            and artifact.get('filenames') == [str(executable)], 'wrong release executable path/closure')


def handoff_descriptor(path):
    return {'byte_length': path.stat().st_size, 'sha256': sha256(path)}


def write_handoff(root, out, source, artifact, records):
    require(os.environ.get('GITHUB_JOB') == BUILD_JOB, 'handoff producer is not the required build job')
    identity = checkout_identity(root)
    validate_handoff_artifact(artifact, root)
    validate_elf((out / 'managed_process').read_bytes())
    dependencies = dependency_manifest(root, records, root)
    (out / 'dependencies.json').write_text(json.dumps(dependencies, sort_keys=True, indent=2) + '\n')
    for name, source_path in CARGO_FILES.items():
        shutil.copyfile(root / source_path, out / name)
    manifest = {'format': 1, 'producer_job': BUILD_JOB, 'context': ci_context(),
                **identity, 'build_root': str(root), 'source_sha256': source['sha256'],
                'build_command': BUILD, 'artifact': artifact,
                'files': {name: handoff_descriptor(out / name)
                          for name in HANDOFF_LIMITS if name != 'handoff.json'}}
    (out / 'handoff.json').write_text(json.dumps(manifest, sort_keys=True, indent=2) + '\n')
    archive = out / 'handoff.zip'
    with zipfile.ZipFile(archive, 'x', compression=zipfile.ZIP_STORED, allowZip64=False) as bundle:
        for name in sorted(HANDOFF_LIMITS):
            data = (out / name).read_bytes()
            require(0 < len(data) <= HANDOFF_LIMITS[name], 'oversized/empty handoff member')
            entry = zipfile.ZipInfo(name)
            entry.create_system = 3
            entry.external_attr = (stat.S_IFREG | 0o644) << 16
            bundle.writestr(entry, data)
    return archive


def read_handoff_archive(path, expected_hash=None, expected_length=None, download_closure=False):
    # Read one bounded, no-follow, single-link file into stable bytes. Hash and
    # parse those same bytes, never re-open a possibly replaced path. No extractall.
    directory = open_directory(path.parent)
    try:
        if download_closure:
            require(os.listdir(directory) == ['handoff.zip'] and path.name == 'handoff.zip',
                    'downloaded artifact must contain only handoff.zip')
        info = os.stat(path.name, dir_fd=directory, follow_symlinks=False)
        require(stat.S_ISREG(info.st_mode) and info.st_nlink == 1, 'unsafe handoff archive')
        raw = read_owned(directory, path.name, sum(HANDOFF_LIMITS.values()) + 65536)
        after = os.stat(path.name, dir_fd=directory, follow_symlinks=False)
        require((info.st_dev, info.st_ino, info.st_size) == (after.st_dev, after.st_ino, after.st_size)
                and stat.S_ISREG(after.st_mode) and after.st_nlink == 1,
                'handoff archive replaced while reading')
    finally:
        os.close(directory)
    if expected_hash is not None:
        require(len(raw) == expected_length and hashlib.sha256(raw).hexdigest() == expected_hash,
                'bundle differs from required build-job output')
    payloads = {}
    with io.BytesIO(raw) as stream, zipfile.ZipFile(stream) as bundle:
        entries = bundle.infolist()
        require(len(entries) == len(HANDOFF_LIMITS)
                and {entry.filename for entry in entries} == set(HANDOFF_LIMITS),
                'handoff archive closure has missing, duplicate or extra entries')
        require(not bundle.comment, 'unexpected ZIP comment')
        stream.seek(-22, os.SEEK_END)
        end = struct.unpack('<4s4H2LH', stream.read(22))
        require(end[0] == b'PK\x05\x06' and end[1:3] == (0, 0)
                and end[3:5] == (len(entries), len(entries)) and end[7] == 0
                and end[6] == bundle.start_dir and end[5] + end[6] + 22 == len(raw),
                'malformed or trailing ZIP data')
        offset = 0
        for entry in entries:
            require(entry.orig_filename == entry.filename and entry.create_system == 3
                    and entry.external_attr == (stat.S_IFREG | 0o644) << 16
                    and not entry.is_dir() and entry.flag_bits == 0
                    and entry.compress_type == zipfile.ZIP_STORED and not entry.extra and not entry.comment
                    and entry.extract_version == 20 and entry.create_version == 20
                    and entry.reserved == 0 and entry.volume == 0
                    and entry.header_offset == offset and entry.file_size == entry.compress_size
                    and 0 < entry.file_size <= HANDOFF_LIMITS[entry.filename],
                    'unsafe, linked, executable or malformed ZIP member')
            stream.seek(offset)
            header = struct.unpack('<4s5H3L2H', stream.read(30))
            require(header[0] == b'PK\x03\x04' and header[1:4] == (20, 0, 0)
                    and header[6:9] == (entry.CRC, entry.compress_size, entry.file_size)
                    and header[-1] == 0 and header[-2] == len(entry.filename.encode('ascii')),
                    'invalid ZIP local header')
            offset += 30 + header[-2] + entry.file_size
            payloads[entry.filename] = bundle.read(entry)  # CRC and local/central name checked here.
        require(offset == bundle.start_dir, 'unexpected ZIP data between members')
    return payloads


def accept_handoff(root, out, archive, source):
    provenance = read_provenance()
    expected_hash = os.environ.get('LIFECYCLE_BUNDLE_SHA256', '')
    expected_length = os.environ.get('LIFECYCLE_BUNDLE_LENGTH', '')
    require(re.fullmatch('[0-9a-f]{64}', expected_hash)
            and re.fullmatch('[1-9][0-9]*', expected_length), 'missing trusted bundle digest/length')
    payloads = read_handoff_archive(archive, expected_hash, int(expected_length), download_closure=True)
    manifest = unique_json(payloads['handoff.json'])
    fields(manifest, {'format', 'producer_job', 'context', 'commit', 'tree', 'build_root',
                      'source_sha256', 'build_command', 'artifact', 'files'}, 'handoff')
    require(type(manifest['format']) is int and manifest['format'] == 1
            and manifest['producer_job'] == BUILD_JOB and same_json(manifest['context'], ci_context()),
            'wrong handoff version, producer or current run/event identity')
    identity = checkout_identity(root)
    require(manifest['commit'] == identity['commit'] and manifest['tree'] == identity['tree'],
            'wrong handoff SHA/tree')
    require(manifest['build_command'] == BUILD,
            'wrong handoff Cargo commands')
    fields(manifest['files'], set(HANDOFF_LIMITS) - {'handoff.json'}, 'handoff file closure')
    for name, ref in manifest['files'].items():
        fields(ref, {'byte_length', 'sha256'}, 'handoff file descriptor')
        require(integer(ref['byte_length'], 1, HANDOFF_LIMITS[name])
                and ref['byte_length'] == len(payloads[name])
                and isinstance(ref['sha256'], str) and re.fullmatch('[0-9a-f]{64}', ref['sha256'])
                and hashlib.sha256(payloads[name]).hexdigest() == ref['sha256'],
                'handoff member length/hash mismatch')
    require(manifest['source_sha256'] == source['sha256']
            and same_json(unique_json(payloads['source.json']), source), 'wrong handoff source manifest')
    for name, source_path in CARGO_FILES.items():
        require(payloads[name] == (root / source_path).read_bytes(), 'wrong Cargo manifest/lockfile')
    build_root = pathlib.Path(manifest['build_root'])
    require(build_root.is_absolute() and '..' not in build_root.parts, 'invalid build root')
    records = [unique_json(line) for line in payloads['build.jsonl'].splitlines() if line]
    artifact = select_artifact(records, build_root)
    require(same_json(artifact, manifest['artifact']), 'selected Cargo artifact mismatch')
    validate_handoff_artifact(artifact, build_root)
    require(same_json(unique_json(payloads['dependencies.json']), dependency_manifest(root, records, build_root)),
            'wrong compiled dependency/lockfile/Cargo manifest identity')
    validate_elf(payloads['managed_process'])
    # Everything is validated before any executable is materialized or invoked.
    for name, data in payloads.items():
        if name != 'source.json':  # Locally measured source.json already exists.
            with (out / name).open('xb') as stream:
                stream.write(data)
    (out / 'artifact-service.json').write_text(json.dumps(provenance, indent=2) + '\n')
    executable = out / 'managed_process'
    executable.chmod(0o755)
    return artifact, executable, manifest['files']['managed_process']['sha256']


def validate_results(builder, lifecycle, scenario):
    require((builder, lifecycle, scenario) == ('success', 'success', 'success'),
            'build, lifecycle runtime and CoreHR scenario must all succeed')


def run_gate(root, out, mode, handoff=None):
    require(mode in ('build', 'runtime'), 'explicit build or runtime mode required')
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
        if mode == 'runtime':
            artifact, executable, elf_hash = accept_handoff(root, out, handoff, source)
            metadata.update(artifact=artifact, executable=str(executable), elf_sha256=elf_hash,
                            cargo_elf_sha256=elf_hash, handoff_artifact_id=os.environ['LIFECYCLE_ARTIFACT_ID'],
                            handoff_bundle_sha256=os.environ['LIFECYCLE_BUNDLE_SHA256'],
                            handoff_bundle_length=int(os.environ['LIFECYCLE_BUNDLE_LENGTH']))
            save()
        else:
            run('build', BUILD, env, 'jsonl')
            records = [unique_json(line) for line in (out / 'build.jsonl').read_text().splitlines() if line]
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
            archive = write_handoff(root, out, source, artifact, records)
            require(source_snapshot(root, out)['sha256'] == source['sha256'],
                    'source changed while packaging the build')
            metadata['status'] = 'BUILD_COMPLETE'
            save()
            with open(os.environ['GITHUB_OUTPUT'], 'a') as outputs:
                outputs.write(f'bundle-sha256={sha256(archive)}\nbundle-length={archive.stat().st_size}\n')
            summary('Managed lifecycle BUILD_COMPLETE: same-run handoff prepared; runtime acceptance is still required.')
            return
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
        parser = argparse.ArgumentParser(description=__doc__)
        parser.add_argument('out', nargs='?', default='managed-lifecycle-ci-results')
        parser.add_argument('--mode', choices=('build', 'runtime', 'aggregate', 'verify-provenance'), required=True)
        parser.add_argument('--handoff', type=pathlib.Path)
        args = parser.parse_args()
        if args.mode == 'verify-provenance':
            read_provenance()
        elif args.mode == 'aggregate':
            validate_results(os.environ.get('LIFECYCLE_BUILD_RESULT'), os.environ.get('LIFECYCLE_RESULT'),
                             os.environ.get('COREHR_RESULT'))
        else:
            require((args.handoff is not None) == (args.mode == 'runtime'), 'runtime requires one handoff archive')
            run_gate(root, pathlib.Path(args.out).resolve(), args.mode, args.handoff)
    except (Exception, KeyboardInterrupt) as error:
        print(f'Managed lifecycle INCOMPLETE: {error}', file=sys.stderr)
        return 1
    return 0


if __name__ == '__main__':
    sys.exit(main())
