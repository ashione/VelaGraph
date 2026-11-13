import contextlib
import copy
import io
import hashlib
import shutil
import json
import os
import pathlib
import subprocess
import stat
import struct
import warnings
import zipfile
import tempfile
import unittest
from unittest.mock import patch

import managed_lifecycle_ci as gate


def artifact(root, per_unit=False):
    executable = (root / 'target/release/build/replicated-graph/8aeb7834ed459655/out/managed_process-8aeb7834ed459655'
                  if per_unit else root / 'target/release/deps/managed_process-abcdef')
    return {'reason': 'compiler-artifact',
            'manifest_path': str(root / 'crates/replicated-graph/Cargo.toml'),
            'target': {'name': 'managed_process', 'kind': ['test'], 'test': True,
                       'src_path': str(root / 'crates/replicated-graph/tests/managed_process.rs')},
            'profile': {'test': True, 'debug_assertions': False, 'opt_level': '3',
                        'debuginfo': 0, 'overflow_checks': False},
            'features': [],
            'package_id': 'path+' + (root / 'crates/replicated-graph').as_uri() + '#0.1.0',
            'filenames': [str(executable)],
            'executable': str(executable)}


def output(name=gate.TEST, result='ok', passed=1, ignored=0, running=1):
    return (f'\nrunning {running} test\ntest {name} ... {result}\n\n'
            f'test result: ok. {passed} passed; 0 failed; {ignored} ignored; '
            '0 measured; 10 filtered out; finished in 123.45s\n\n')


def encoded(value):
    return json.dumps(value, separators=(',', ':'), ensure_ascii=False).encode()


def checkpoint(executable, directory, source_hash='a' * 64, elf_hash='b' * 64, migrated=False):
    # Real complete artifact files with typed synthetic protocol fixtures. The
    # subprocess fixture tests gate plumbing, not an actual lifecycle execution.
    directory = pathlib.Path(directory)
    artifacts = directory / 'artifacts'
    artifacts.mkdir()
    binding = gate.expected_binding()
    saved = {'format': 2, 'source_sha256': source_hash, 'elf_sha256': elf_hash,
             'executable': str(executable), 'harness_timeout_secs': 21600,
             'verified_ordinal': 1025, 'pending_external_id': None, 'completed': True,
             'ack_loss_recovered': True, 'snapshots_restored': True, 'binding': binding,
             'originals': {}, 'proofs': {}, 'migration': None,
             'grants': [{'member': member, 'activation': {'nonce': [member['shard']] * 32,
                         'log_id': {'leader_id': {'term': 1, 'node_id': 1}, 'index': 1}}}
                        for member in binding['cohort']['members']]}
    originals, proofs = {}, {}
    participant = {'shard': 7, 'epoch': 1, 'mode': [7] * 16, 'intent': [1] * 32}

    def retain(kind, key, name, data):
        (artifacts / name).write_bytes(data)
        return {'format': 1, 'kind': kind, 'key': key, 'relative_path': 'artifacts/' + name,
                'byte_length': len(data), 'sha256': hashlib.sha256(data).hexdigest()}

    for key in sorted(gate.ORIGINAL_KEYS):
        plan = {'version': 1, 'id': gate.original_id(key), 'authority': binding['authority'],
                'cohort': binding['cohort'], 'validation_version': key - 1,
                'parts': [{'descriptor': participant, 'request': {'shard': 7, 'epoch': 1,
                           'request_id': gate.original_id(key), 'expected_sequence': key - 1,
                           'operations': [{'Put': {'key': [1], 'value': [2]}}]}}]}
        request = ['managed-graph-command-v1', binding, {'CreateVertex': {'partition': 7,
                   'vertex': {'id': key, 'labels': ['person'], 'properties': {}}}}]
        value = {'id': gate.original_id(key), 'request': list(encoded(request)),
                 'payload': list(encoded(plan)), 'transaction_id': gate.original_id(key)}
        originals[str(key)] = value
        saved['originals'][str(key)] = retain('original', key, f'original-{key}.json', encoded(value))
    for key in sorted(gate.PROOF_KEYS):
        rows = [{'id': gate.original_id(n), 'ordinal': n, 'participants': [participant],
                 'prepared': [True], 'decision': 'Commit'} for n in range(1, key + 1)]
        value = {'version': 2, 'authority': [9] * 16, 'cohort': binding['cohort'],
                 'through': key, 'segments': [rows[n:n + 32] for n in range(0, key, 32)]}
        data = encoded(value)
        proofs[str(key)] = list(data)
        saved['proofs'][str(key)] = retain('proof', key, f'proof-v{key}.json', data)
    if migrated:
        legacy = {k: copy.deepcopy(v) for k, v in saved.items() if k != 'migration'}
        legacy.update(format=1, originals=originals, proofs=proofs)
        saved['migration'] = retain('legacy_checkpoint', 1, 'legacy-checkpoint-v1.json', encoded(legacy))
    return saved


class ValidatorTests(unittest.TestCase):
    def test_select_exact_optimized_release_artifact(self):
        root = pathlib.Path('/synthetic')
        selected = artifact(root)
        rows = [selected, {'reason': 'build-finished', 'success': True}]
        self.assertEqual(gate.select_artifact(rows, root), selected)
        mutations = [lambda a: a['target'].update(name='other'),
                     lambda a: a['target'].update(kind=['bin']),
                     lambda a: a['target'].update(test=False),
                     lambda a: a['target'].update(src_path='/wrong/managed_process.rs'),
                     lambda a: a.update(manifest_path='/wrong/Cargo.toml'),
                     lambda a: a['profile'].update(test=False),
                     lambda a: a['profile'].update(debug_assertions=True),
                     lambda a: a['profile'].pop('debug_assertions'),
                     lambda a: a['profile'].update(opt_level='0'),
                     lambda a: a.update(executable=None)]
        for mutate in mutations:
            changed = copy.deepcopy(rows)
            mutate(changed[0])
            with self.subTest(mutate=mutate), self.assertRaises(ValueError):
                gate.select_artifact(changed, root)
        for changed in (rows[1:], rows + [selected], rows[:1],
                        [selected, {'reason': 'build-finished', 'success': False}]):
            with self.assertRaises(ValueError):
                gate.select_artifact(changed, root)

    def test_exact_cargo_release_layouts_preserve_path_and_filename_closure(self):
        root = pathlib.Path('/synthetic')
        for per_unit in (False, True):
            good = artifact(root, per_unit)
            gate.validate_handoff_artifact(good, root)
            paths = [
                '/outside/target/release/deps/managed_process-abcdef',
                str(root / 'target/debug/deps/managed_process-abcdef'),
                str(root / 'target/release/build/other/8aeb7834ed459655/out/managed_process-8aeb7834ed459655'),
                str(root / 'target/release/build/replicated-graph/ffffffffffffffff/out/managed_process-8aeb7834ed459655'),
                str(root / 'target/release/build/replicated-graph/abcdef/out/managed_process-abcdef'),
                str(root / 'target/release/build/replicated-graph/8aeb7834ed459655/managed_process-8aeb7834ed459655'),
                str(root / 'target/release/build/replicated-graph/8aeb7834ed459655/out/other-8aeb7834ed459655'),
                str(root / 'target/release/deps/../elsewhere/managed_process-abcdef'),
            ]
            for path in paths:
                bad = copy.deepcopy(good)
                bad.update(executable=path, filenames=[path])
                with self.subTest(per_unit=per_unit, path=path), self.assertRaises(ValueError):
                    gate.validate_handoff_artifact(bad, root)
            for names in ([], [good['executable'], '/extra'], ['/other']):
                bad = copy.deepcopy(good)
                bad['filenames'] = names
                with self.subTest(per_unit=per_unit, filenames=names), self.assertRaises(ValueError):
                    gate.validate_handoff_artifact(bad, root)

    def test_exact_test_pass_required(self):
        gate.validate_output(output(), 0)
        bad = [output(running=0, passed=0), output(result='ignored', passed=0, ignored=1),
               output(result='skipped'), output(name='wrong_test'), output(result='FAILED'),
               output(passed=0), output(ignored=1), output(running=2), output() + output(),
               output().replace('1 passed;', '1 passed; 1 skipped;'),
               output().replace('test result:', 'missing result:')]
        for text in bad:
            with self.subTest(text=text), self.assertRaises(ValueError):
                gate.validate_output(text, 0)
        with self.assertRaises(ValueError):
            gate.validate_output(output(), 9)

    def test_complete_matching_checkpoint_required(self):
        with tempfile.TemporaryDirectory() as directory:
            executable = pathlib.Path('/synthetic/managed_process')
            valid = checkpoint(executable, directory)
            gate.validate_checkpoint(valid, 'a' * 64, 'b' * 64, executable, directory)
            for field, value in [('format', 1), ('format', True), ('source_sha256', 'c' * 64),
                                 ('elf_sha256', 'c' * 64), ('executable', '/wrong'),
                                 ('completed', False), ('completed', 1),
                                 ('verified_ordinal', 1024), ('verified_ordinal', 1025.0),
                                 ('pending_external_id', [0] * 16), ('ack_loss_recovered', False),
                                 ('snapshots_restored', False), ('originals', {}),
                                 ('proofs', {}), ('grants', []), ('binding', {}),
                                 ('harness_timeout_secs', 0),
                                 ('binding', {**gate.expected_binding(), 'cohort': {**gate.expected_binding()['cohort'], 'coordinator_epoch': True}})]:
                changed = copy.deepcopy(valid)
                changed[field] = value
                with self.subTest(field=field, value=value), self.assertRaises(ValueError):
                    gate.validate_checkpoint(changed, 'a' * 64, 'b' * 64, executable, directory)
            for field in valid:
                changed = copy.deepcopy(valid)
                del changed[field]
                with self.subTest(missing=field), self.assertRaises(ValueError):
                    gate.validate_checkpoint(changed, 'a' * 64, 'b' * 64, executable, directory)
            changed = copy.deepcopy(valid)
            changed['extra'] = 1
            with self.assertRaises(ValueError):
                gate.validate_checkpoint(changed, 'a' * 64, 'b' * 64, executable, directory)

    def test_reference_paths_lengths_hashes_kinds_and_fields_are_strict(self):
        with tempfile.TemporaryDirectory() as directory:
            valid = checkpoint('/synthetic/managed_process', directory)
            changes = [('relative_path', '/tmp/proof-v1.json'), ('relative_path', 'artifacts/../proof-v1.json'),
                       ('relative_path', 'artifacts/proof-v0.json'), ('byte_length', 0),
                       ('byte_length', True), ('byte_length', 1.0), ('byte_length', gate.PROOF_LIMIT + 1),
                       ('sha256', '0' * 64), ('sha256', 'A' * 64), ('kind', 'original'),
                       ('key', 0), ('key', True), ('format', 2), ('extra', False)]
            for field, value in changes:
                changed = copy.deepcopy(valid)
                changed['proofs']['1'][field] = value
                with self.subTest(field=field, value=value), self.assertRaises(ValueError):
                    gate.validate_checkpoint(changed, 'a' * 64, 'b' * 64, '/synthetic/managed_process', directory)
            for field in valid['proofs']['1']:
                changed = copy.deepcopy(valid)
                del changed['proofs']['1'][field]
                with self.assertRaises(ValueError):
                    gate.validate_checkpoint(changed, 'a' * 64, 'b' * 64, '/synthetic/managed_process', directory)

    def test_real_files_missing_truncated_corrupt_unsafe_and_extra_reject(self):
        for case in ('missing', 'truncate', 'empty', 'flip', 'symlink', 'hardlink', 'extra', 'staging', 'parent_symlink', 'malformed_typed', 'wrong_proof_version'):
            with self.subTest(case=case), tempfile.TemporaryDirectory() as directory:
                root = pathlib.Path(directory)
                valid = checkpoint('/synthetic/managed_process', root)
                path = root / 'artifacts/proof-v1.json'
                data = path.read_bytes()
                if case == 'missing': path.unlink()
                elif case == 'truncate': path.write_bytes(data[:-1])
                elif case == 'empty': path.write_bytes(b'')
                elif case == 'flip': path.write_bytes(bytes([data[0] ^ 1]) + data[1:])
                elif case == 'symlink':
                    path.unlink()
                    path.symlink_to('proof-v0.json')
                elif case == 'hardlink': os.link(path, root / 'alias')
                elif case == 'extra': (root / 'artifacts/extra.json').write_text('{}')
                elif case == 'staging': (root / 'artifacts/proof-v1.json.tmp').write_bytes(data)
                elif case == 'parent_symlink':
                    (root / 'artifacts').rename(root / 'elsewhere')
                    (root / 'artifacts').symlink_to('elsewhere', target_is_directory=True)
                else:
                    value = gate.unique_json(data)
                    if case == 'malformed_typed': value = {'synthetic': True}
                    else: value['through'] = 0
                    data = encoded(value)
                    path.write_bytes(data)
                    valid['proofs']['1'].update(byte_length=len(data), sha256=hashlib.sha256(data).hexdigest())
                with self.assertRaises((ValueError, OSError)):
                    gate.validate_checkpoint(valid, 'a' * 64, 'b' * 64, '/synthetic/managed_process', root)

    def test_same_identity_migration_backup_is_required_and_cannot_repair(self):
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            valid = checkpoint('/synthetic/managed_process', root, migrated=True)
            gate.validate_checkpoint(valid, 'a' * 64, 'b' * 64, '/synthetic/managed_process', root)
            backup = root / 'artifacts/legacy-checkpoint-v1.json'
            raw = backup.read_bytes()
            legacy = gate.unique_json(raw)
            legacy['elf_sha256'] = 'c' * 64
            changed = encoded(legacy)
            backup.write_bytes(changed)
            valid['migration'].update(byte_length=len(changed), sha256=hashlib.sha256(changed).hexdigest())
            with self.assertRaises(ValueError):
                gate.validate_checkpoint(valid, 'a' * 64, 'b' * 64, '/synthetic/managed_process', root)
            backup.write_bytes(raw)
            valid['migration'].update(byte_length=len(raw), sha256=hashlib.sha256(raw).hexdigest())
            (root / 'artifacts/proof-v1.json').unlink()
            with self.assertRaises(OSError):
                gate.validate_checkpoint(valid, 'a' * 64, 'b' * 64, '/synthetic/managed_process', root)
            self.assertFalse((root / 'artifacts/proof-v1.json').exists())

    def test_copied_bundle_requires_every_artifact_and_rejects_v1(self):
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            source, copied = root / 'source', root / 'copied'
            source.mkdir()
            valid = checkpoint('/synthetic/managed_process', source)
            shutil.copytree(source, copied)
            gate.validate_checkpoint(valid, 'a' * 64, 'b' * 64, '/synthetic/managed_process', copied)
            for name in sorted((copied / 'artifacts').iterdir()):
                saved = name.read_bytes()
                name.unlink()
                with self.assertRaises(OSError):
                    gate.validate_checkpoint(valid, 'a' * 64, 'b' * 64, '/synthetic/managed_process', copied)
                name.write_bytes(saved)
            valid['format'] = 1
            with self.assertRaises(ValueError):
                gate.validate_checkpoint(valid, 'a' * 64, 'b' * 64, '/synthetic/managed_process', copied)

    def test_hash_valid_boolean_id_bytes_reject_like_rust(self):
        for case, position, boolean in ((case, position, boolean)
                                        for case in ('external', 'plan', 'write', 'legacy_pending')
                                        for position, boolean in ((15, True), (0, False))):
            with self.subTest(case=case, position=position, boolean=boolean), tempfile.TemporaryDirectory() as directory:
                root = pathlib.Path(directory)
                saved = checkpoint('/synthetic/managed_process', root, migrated=case == 'legacy_pending')
                if case == 'legacy_pending':
                    path = root / 'artifacts/legacy-checkpoint-v1.json'
                    value = gate.unique_json(path.read_bytes())
                    value.update(verified_ordinal=1024, pending_external_id=gate.original_id(1025),
                                 ack_loss_recovered=False, snapshots_restored=False, completed=False)
                    value['proofs'].pop('1025')
                    reference = saved['migration']
                    # An actual reachable pending-1025 migration remains valid.
                    data = encoded(value)
                    path.write_bytes(data)
                    reference.update(byte_length=len(data), sha256=hashlib.sha256(data).hexdigest())
                    gate.validate_checkpoint(saved, 'a' * 64, 'b' * 64, '/synthetic/managed_process', root)
                    self.assertEqual(value['pending_external_id'][position], int(boolean))
                    value['pending_external_id'][position] = boolean
                else:
                    path = root / 'artifacts/original-1.json'
                    value = gate.unique_json(path.read_bytes())
                    reference = saved['originals']['1']
                    gate.validate_checkpoint(saved, 'a' * 64, 'b' * 64, '/synthetic/managed_process', root)
                    if case == 'external':
                        self.assertEqual(value['id'][position], int(boolean))
                        value['id'][position] = boolean
                    else:
                        plan = gate.unique_json(bytes(value['payload']))
                        if case == 'plan':
                            self.assertEqual(plan['id'][position], int(boolean))
                            plan['id'][position] = boolean
                        else:
                            self.assertEqual(plan['parts'][0]['request']['request_id'][position], int(boolean))
                            plan['parts'][0]['request']['request_id'][position] = boolean
                        value['payload'] = list(encoded(plan))
                data = encoded(value)
                path.write_bytes(data)
                reference.update(byte_length=len(data), sha256=hashlib.sha256(data).hexdigest())
                with self.assertRaises(ValueError):
                    gate.validate_checkpoint(saved, 'a' * 64, 'b' * 64, '/synthetic/managed_process', root)

    def test_duplicate_json_keys_and_nonfinite_values_reject(self):
        for raw in ('{"format":2,"format":2}', '{"proofs":{"1":{},"1":{}}}', '{"value":NaN}'):
            with self.assertRaises(ValueError):
                gate.unique_json(raw)

    def test_hash_actual_tracked_untracked_and_deleted_source_not_outputs(self):
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            subprocess.run(['git', 'init', '-q', str(root)], check=True)
            tracked = root / 'tracked.rs'
            tracked.write_text('original\n')
            subprocess.run(['git', '-C', str(root), 'add', 'tracked.rs'], check=True)
            out = root / 'results'
            out.mkdir()
            (root / 'target').mkdir()
            with patch.dict(os.environ, {'CARGO_TARGET_DIR': 'target'}):
                original = gate.source_snapshot(root, out)['sha256']
                tracked.write_text('working tree change\n')
                changed = gate.source_snapshot(root, out)['sha256']
                self.assertNotEqual(original, changed)
                (root / 'untracked.rs').write_text('new project source\n')
                untracked = gate.source_snapshot(root, out)['sha256']
                self.assertNotEqual(changed, untracked)
                (out / 'progress.json').write_text('synthetic output\n')
                (root / 'target/binary').write_text('synthetic build output\n')
                self.assertEqual(untracked, gate.source_snapshot(root, out)['sha256'])
                tracked.unlink()
                deleted = gate.source_snapshot(root, out)
                self.assertNotEqual(untracked, deleted['sha256'])
                self.assertIn({'path': 'tracked.rs', 'kind': 'deleted'}, deleted['files'])


class HandoffFixture:
    # Synthetic ELF header only, never execute it. All subprocesses are mocked.
    def __init__(self, root, per_unit=False):
        self.root = root
        self.build = root / 'target/build-results'
        self.download = root / 'target/download'
        self.download.mkdir(parents=True)
        self.archive = self.download / 'handoff.zip'
        self.selected = artifact(root, per_unit)
        self.binary = pathlib.Path(self.selected['executable'])
        self.binary.parent.mkdir(parents=True)
        header = struct.pack('<16sHHIQQQIHHHHHH', b'\x7fELF\x02\x01\x01' + b'\0' * 9,
                             3, 62, 1, 0, 64, 0, 0, 64, 56, 1, 0, 0, 0)
        self.binary.write_bytes(header + b'\0' * 56)
        self.binary.chmod(0o755)
        self.records = []
        files = {'source.rs': 'synthetic project source\n'}
        lock = 'version = 4\n'
        for name, path in [('velagraph', 'Cargo.toml'),
                           ('d2-raft', 'crates/d2-raft/Cargo.toml'),
                           ('replicated-graph', 'crates/replicated-graph/Cargo.toml')]:
            files[path] = f'[package]\nname = "{name}"\nversion = "0.1.0"\n'
            lock += f'\n[[package]]\nname = "{name}"\nversion = "0.1.0"\n'
            row = copy.deepcopy(self.selected)
            row.update(package_id='path+' + (root / path).parent.as_uri() + f'#{name}@0.1.0',
                       manifest_path=str(root / path), executable=None,
                       features=['default', 'test-hooks'] if name == 'd2-raft' else [])
            row['target'].update(name=name.replace('-', '_'), kind=['lib'], src_path=str((root / path).parent / 'src/lib.rs'))
            row['profile']['test'] = False
            self.records.append(row)
        files['Cargo.lock'] = lock
        for path, data in files.items():
            target = root / path
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(data)
        self.files = b'\0'.join(path.encode() for path in sorted(files)) + b'\0'
        self.records += [self.selected, {'reason': 'build-finished', 'success': True}]
        self.env = {'GITHUB_REPOSITORY': 'owner/project', 'GITHUB_REPOSITORY_ID': '23',
                    'GITHUB_RUN_ID': '42', 'GITHUB_RUN_ATTEMPT': '2', 'GITHUB_SHA': 'a' * 40,
                    'GITHUB_EVENT_NAME': 'pull_request', 'GITHUB_REF': 'refs/pull/117/merge',
                    'LIFECYCLE_EVENT_HEAD_SHA': 'c' * 40, 'GITHUB_WORKFLOW': 'CoreHR full scale',
                    'GITHUB_JOB': gate.BUILD_JOB, 'GITHUB_OUTPUT': str(root / 'target/github-output'),
                    'LIFECYCLE_BUILD_RESULT': 'success', 'LIFECYCLE_ARTIFACT_ID': '123',
                    'LIFECYCLE_ARTIFACT_METADATA': str(root / 'target/api.json'), 'CARGO_TARGET_DIR': 'target'}
        self.provenance = {'artifact': {'id': 123, 'name': 'managed-lifecycle-build-42-2', 'expired': False,
                                       'workflow_run': {'id': 42, 'repository_id': 23, 'head_sha': 'c' * 40}},
                           'run': {'id': 42, 'run_attempt': 2, 'head_sha': 'c' * 40, 'event': 'pull_request',
                                   'path': '.github/workflows/corehr.yml',
                                   'repository': {'id': 23, 'full_name': 'owner/project'}}}
        pathlib.Path(self.env['LIFECYCLE_ARTIFACT_METADATA']).write_text(json.dumps(self.provenance))

    @contextlib.contextmanager
    def context(self):
        with patch.dict(os.environ, self.env), \
                patch.object(gate, 'checkout_identity', return_value={'commit': 'a' * 40, 'tree': 'b' * 40}), \
                patch.object(gate.subprocess, 'check_output', return_value=self.files), \
                contextlib.redirect_stdout(io.StringIO()):
            yield

    def prepare(self):
        self.build.mkdir()
        with self.context():
            source = gate.source_snapshot(self.root, self.build)
            (self.build / 'source.json').write_text(json.dumps(source))
            (self.build / 'build.jsonl').write_text(''.join(json.dumps(r) + '\n' for r in self.records))
            shutil.copyfile(self.binary, self.build / 'managed_process')
            gate.write_handoff(self.root, self.build, source, self.selected, self.records)
        self.download_build()
        return self

    def download_build(self):
        shutil.copyfile(self.build / 'handoff.zip', self.archive)
        self.bind_archive()

    def bind_archive(self):
        self.env.update(LIFECYCLE_BUNDLE_SHA256=gate.sha256(self.archive),
                        LIFECYCLE_BUNDLE_LENGTH=str(self.archive.stat().st_size))

    def rewrite(self, change):
        payloads = gate.read_handoff_archive(self.archive)
        change(payloads)
        with zipfile.ZipFile(self.archive, 'w') as bundle:
            for name, data in sorted(payloads.items()):
                entry = zipfile.ZipInfo(name)
                entry.create_system = 3
                entry.external_attr = (stat.S_IFREG | 0o644) << 16
                bundle.writestr(entry, data)
        self.bind_archive()

    def change_manifest(self, change):
        def mutate(payloads):
            manifest = gate.unique_json(payloads['handoff.json'])
            change(manifest)
            payloads['handoff.json'] = encoded(manifest)
        self.rewrite(mutate)

    def change_payload(self, name, change):
        def mutate(payloads):
            payloads[name] = change(payloads[name])
            manifest = gate.unique_json(payloads['handoff.json'])
            manifest['files'][name] = {'byte_length': len(payloads[name]),
                                       'sha256': hashlib.sha256(payloads[name]).hexdigest()}
            payloads['handoff.json'] = encoded(manifest)
        self.rewrite(mutate)


class GateTests(unittest.TestCase):
    def run_fixture(self, root, out, test_output=None, exit_code=0, mutate=None,
                    build_exit=0, artifact_mutate=None, mutate_artifacts=None):
        fixture = HandoffFixture(root)
        selected, binary = fixture.selected, fixture.binary
        calls = []

        def fake_run(command, cwd, env, stdout, stderr):
            calls.append((command, env))
            if command == gate.BUILD:
                if artifact_mutate:
                    artifact_mutate(selected)
                fixture.records[-1]['success'] = build_exit == 0
                stdout.write(''.join(json.dumps(row) + '\n' for row in fixture.records))
                return subprocess.CompletedProcess(command, build_exit)
            self.assertEqual(command, [str(out / 'managed_process'), gate.TEST, '--exact',
                                      '--nocapture', '--test-threads=1', '--format=pretty', '--color=never'])
            self.assertEqual(pathlib.Path(command[0]).read_bytes(), binary.read_bytes())
            self.assertTrue(os.access(command[0], os.X_OK))
            self.assertEqual(env['BYTE_BOUND_PROCESS_ELF_SHA256'], gate.sha256(binary))
            stdout.write(output() if test_output is None else test_output)
            saved = checkpoint(command[0], env['BYTE_BOUND_PROCESS_DIRECTORY'],
                               env['BYTE_BOUND_PROCESS_SOURCE_SHA256'], env['BYTE_BOUND_PROCESS_ELF_SHA256'])
            if mutate:
                mutate(saved)
            if mutate_artifacts:
                mutate_artifacts(pathlib.Path(env['BYTE_BOUND_PROCESS_DIRECTORY']))
            (pathlib.Path(env['BYTE_BOUND_PROCESS_DIRECTORY']) / 'progress.json').write_text(json.dumps(saved))
            stderr.write('synthetic child stderr\n')
            return subprocess.CompletedProcess(command, exit_code)

        with fixture.context(), patch.object(gate.subprocess, 'run', side_effect=fake_run):
            gate.run_gate(root, fixture.build, 'build')
        fixture.download_build()
        with fixture.context(), patch.object(gate.subprocess, 'run', side_effect=fake_run):
            gate.run_gate(root, out, 'runtime', fixture.archive)
        return calls

    def test_fresh_complete_run_preserves_exact_frozen_elf_and_metadata(self):
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            out = root / 'results'
            calls = self.run_fixture(root, out)
            self.assertEqual(len(calls), 2)
            execution = json.loads((out / 'execution.json').read_text())
            self.assertEqual(execution['status'], 'COMPLETE')
            self.assertEqual(execution['elf_sha256'], execution['cargo_elf_sha256'])
            self.assertEqual(execution['executable'], str(out / 'managed_process'))
            self.assertEqual(execution['source_sha256'], json.loads((out / 'source.json').read_text())['sha256'])
            self.assertIn('COMPLETE', (out / 'summary.md').read_text())
            self.assertIn('synthetic child stderr', (out / 'test.stderr').read_text())

    def test_failures_remain_incomplete_without_retry_or_success_summary(self):
        scenarios = [{'test_output': output(running=0, passed=0)},
                     {'test_output': output(result='ignored', passed=0, ignored=1)},
                     {'test_output': output(name='wrong_test')}, {'exit_code': 7},
                     {'build_exit': 2},
                     {'artifact_mutate': lambda a: a['profile'].update(debug_assertions=True)},
                     {'mutate': lambda c: c.update(elf_sha256='c' * 64)},
                     {'mutate': lambda c: c.update(source_sha256='c' * 64)},
                     {'mutate': lambda c: c.update(completed=False)},
                     {'mutate': lambda c: c.update(format=1)},
                     {'mutate_artifacts': lambda p: (p / 'artifacts/proof-v1.json').unlink()},
                     {'mutate_artifacts': lambda p: (p / 'artifacts/proof-v1.json').write_bytes(b'corrupt')},
                     {'mutate_artifacts': lambda p: (p / 'artifacts/unexpected.json').write_text('{}')},
                     {'mutate': lambda c: c.update(verified_ordinal=1024)},
                     {'mutate': lambda c: c.update(pending_external_id=[0] * 16)}]
        for scenario in scenarios:
            with self.subTest(scenario=scenario), tempfile.TemporaryDirectory() as directory:
                root = pathlib.Path(directory)
                out = root / 'results'
                with self.assertRaises((ValueError, OSError)):
                    self.run_fixture(root, out, **scenario)
                evidence = out if out.exists() else root / 'target/build-results'
                self.assertEqual(json.loads((evidence / 'execution.json').read_text())['status'], 'INCOMPLETE')
                self.assertTrue((evidence / 'summary.md').read_text().startswith('Managed lifecycle INCOMPLETE:'))

    def test_existing_success_evidence_is_not_reused_or_overwritten(self):
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            out = root / 'results'
            out.mkdir()
            summary = out / 'summary.md'
            summary.write_text('old evidence')
            with patch.object(gate.subprocess, 'run') as run, self.assertRaises(FileExistsError):
                gate.run_gate(root, out, 'runtime', root / 'handoff.zip')
            run.assert_not_called()
            self.assertEqual(summary.read_text(), 'old evidence')

    def test_cli_failure_is_nonzero_and_says_incomplete(self):
        with patch.object(gate, 'run_gate', side_effect=ValueError('synthetic failure')), \
                patch.object(gate.sys, 'argv', ['managed_lifecycle_ci.py', '--mode', 'build']), \
                contextlib.redirect_stderr(io.StringIO()) as stderr:
            self.assertEqual(gate.main(), 1)
        self.assertIn('INCOMPLETE: synthetic failure', stderr.getvalue())


class HandoffTests(unittest.TestCase):
    def assert_rejected_before_execution(self, fixture):
        out = fixture.root / 'results'
        with fixture.context(), patch.object(gate.subprocess, 'run') as run, \
                patch.object(pathlib.Path, 'chmod') as chmod, self.assertRaises(Exception):
            gate.run_gate(fixture.root, out, 'runtime', fixture.archive)
        self.assertFalse(run.called, 'invalid handoff reached execution')
        chmod.assert_not_called()
        self.assertFalse((out / 'managed_process').exists())
        self.assertEqual(json.loads((out / 'execution.json').read_text())['status'], 'INCOMPLETE')

    def test_matching_handoff_is_valid_without_rebuilding(self):
        with tempfile.TemporaryDirectory() as directory:
            fixture = HandoffFixture(pathlib.Path(directory)).prepare()
            out = fixture.root / 'results'
            out.mkdir()
            with fixture.context(), patch.object(gate.subprocess, 'run') as run:
                source = gate.source_snapshot(fixture.root, out)
                (out / 'source.json').write_text(json.dumps(source))
                selected, executable, digest = gate.accept_handoff(fixture.root, out, fixture.archive, source)
            run.assert_not_called()
            self.assertEqual(selected, fixture.selected)
            self.assertEqual(executable.read_bytes(), fixture.binary.read_bytes())
            self.assertEqual(digest, gate.sha256(fixture.binary))
            self.assertEqual({p.name for p in out.iterdir()}, set(gate.HANDOFF_LIMITS) | {'artifact-service.json'})
            self.assertEqual({p.name for p in out.iterdir() if p.stat().st_mode & 0o111}, {'managed_process'})

    def test_cargo_per_unit_release_handoff_keeps_all_provenance_checks(self):
        with tempfile.TemporaryDirectory() as directory:
            fixture = HandoffFixture(pathlib.Path(directory), per_unit=True).prepare()
            out = fixture.root / 'results'
            out.mkdir()
            with fixture.context(), patch.object(gate.subprocess, 'run') as run:
                source = gate.source_snapshot(fixture.root, out)
                (out / 'source.json').write_text(json.dumps(source))
                selected, executable, digest = gate.accept_handoff(fixture.root, out, fixture.archive, source)
            run.assert_not_called()
            self.assertEqual(selected, fixture.selected)
            self.assertEqual(executable.read_bytes(), fixture.binary.read_bytes())
            self.assertEqual(digest, gate.sha256(fixture.binary))

    def test_build_and_runtime_checkout_paths_may_differ_without_rebinding_source(self):
        with tempfile.TemporaryDirectory() as directory:
            parent = pathlib.Path(directory)
            build = HandoffFixture(parent / 'builder').prepare()
            runtime = HandoffFixture(parent / 'runtime')
            shutil.copyfile(build.archive, runtime.archive)
            runtime.bind_archive()
            out = runtime.root / 'results'
            out.mkdir()
            with runtime.context(), patch.object(gate.subprocess, 'run') as run:
                source = gate.source_snapshot(runtime.root, out)
                (out / 'source.json').write_text(json.dumps(source))
                selected, _, _ = gate.accept_handoff(runtime.root, out, runtime.archive, source)
            self.assertFalse(run.called)
            self.assertEqual(selected['target']['src_path'], str(build.root / 'crates/replicated-graph/tests/managed_process.rs'))
            self.assertNotEqual(build.root, runtime.root)

    def test_handoff_manifest_identity_and_closure_fail_closed(self):
        changes = [lambda m: m.pop('commit'), lambda m: m.update(commit='d' * 40),
                   lambda m: m.pop('tree'), lambda m: m.update(tree='d' * 40),
                   lambda m: m.update(format=True), lambda m: m.update(format=2),
                   lambda m: m.update(producer_job='other-job'), lambda m: m.update(build_root='../old'),
                   lambda m: m['context'].update(GITHUB_RUN_ID='41'),
                   lambda m: m['context'].update(GITHUB_RUN_ATTEMPT='1'),
                   lambda m: m['context'].update(GITHUB_SHA='c' * 40),
                   lambda m: m['context'].update(LIFECYCLE_EVENT_HEAD_SHA='a' * 40),
                   lambda m: m['context'].update(GITHUB_REPOSITORY='other/repository'),
                   lambda m: m['context'].update(GITHUB_EVENT_NAME='workflow_run'),
                   lambda m: m.update(source_sha256='d' * 64),
                   lambda m: m['files'].pop('managed_process'),
                   lambda m: m['files']['managed_process'].update(sha256='d' * 64),
                   lambda m: m['files']['managed_process'].update(byte_length=True),
                   lambda m: m['files']['managed_process'].update(byte_length=1),
                   lambda m: m['files'].update(extra={}),
                   lambda m: m['artifact']['profile'].update(opt_level='0'),
                   lambda m: m['build_command'].append('--features=unapproved')]
        for change in changes:
            with self.subTest(change=change), tempfile.TemporaryDirectory() as directory:
                fixture = HandoffFixture(pathlib.Path(directory)).prepare()
                fixture.change_manifest(change)
                self.assert_rejected_before_execution(fixture)

    def test_missing_or_wrong_trusted_job_outputs_fail_closed(self):
        for key, bad in [('LIFECYCLE_BUILD_RESULT', 'cancelled'), ('LIFECYCLE_BUILD_RESULT', 'skipped'),
                         ('LIFECYCLE_BUILD_RESULT', 'failure'), ('LIFECYCLE_ARTIFACT_ID', ''),
                         ('LIFECYCLE_ARTIFACT_ID', '124'), ('LIFECYCLE_BUNDLE_SHA256', ''),
                         ('LIFECYCLE_BUNDLE_SHA256', 'f' * 64), ('LIFECYCLE_BUNDLE_LENGTH', ''),
                         ('LIFECYCLE_BUNDLE_LENGTH', '1'), ('GITHUB_SHA', ''),
                         ('GITHUB_EVENT_NAME', 'workflow_run'), ('GITHUB_RUN_ATTEMPT', '3')]:
            with self.subTest(key=key, bad=bad), tempfile.TemporaryDirectory() as directory:
                fixture = HandoffFixture(pathlib.Path(directory)).prepare()
                fixture.env[key] = bad
                self.assert_rejected_before_execution(fixture)

    def test_api_provenance_cannot_be_replaced_by_bundle_self_claims(self):
        changes = [lambda p: p.pop('run'), lambda p: p['artifact'].pop('workflow_run'),
                   lambda p: p['artifact'].update(id=122), lambda p: p['artifact'].update(expired=True),
                   lambda p: p['artifact'].update(name='managed-lifecycle-build-42-1'),
                   lambda p: p['artifact']['workflow_run'].update(id=41),
                   lambda p: p['artifact']['workflow_run'].update(repository_id=24),
                   lambda p: p['artifact']['workflow_run'].update(head_sha='a' * 40),
                   lambda p: p['run'].update(id=41), lambda p: p['run'].update(run_attempt=1),
                   lambda p: p['run'].update(head_sha='a' * 40),
                   lambda p: p['run'].update(event='push'), lambda p: p['run'].update(path='.github/workflows/ci.yml'),
                   lambda p: p['run']['repository'].update(id=24),
                   lambda p: p['run']['repository'].update(full_name='other/repo')]
        for change in changes:
            with self.subTest(change=change), tempfile.TemporaryDirectory() as directory:
                fixture = HandoffFixture(pathlib.Path(directory)).prepare()
                change(fixture.provenance)
                pathlib.Path(fixture.env['LIFECYCLE_ARTIFACT_METADATA']).write_text(json.dumps(fixture.provenance))
                self.assert_rejected_before_execution(fixture)

    def test_independent_source_cargo_dependency_and_elf_checks(self):
        def change_json(change):
            def mutate(raw):
                value = gate.unique_json(raw)
                change(value)
                return encoded(value)
            return mutate
        cases = [('source.json', change_json(lambda v: v.update(sha256='f' * 64))),
                 ('Cargo.lock', lambda raw: raw + b'\n# changed\n'),
                 ('cargo-root.toml', lambda raw: raw.replace(b'0.1.0', b'0.1.1')),
                 ('cargo-replicated-graph.toml', lambda raw: raw + b'\n# changed\n'),
                 ('dependencies.json', change_json(lambda v: v['compiled'].pop())),
                 ('dependencies.json', change_json(lambda v: v.update(lock_sha256='f' * 64))),
                 ('dependencies.json', change_json(lambda v: v['compiled'][0].update(features=['extra']))),
                 ('managed_process', lambda raw: b'not an ELF'),
                 ('managed_process', lambda raw: raw[:4]),
                 ('managed_process', lambda raw: raw[:18] + b'\x03\x00' + raw[20:])]
        for name, change in cases:
            with self.subTest(name=name), tempfile.TemporaryDirectory() as directory:
                fixture = HandoffFixture(pathlib.Path(directory)).prepare()
                fixture.change_payload(name, change)  # Rebind hashes to test semantic validation too.
                self.assert_rejected_before_execution(fixture)

    def test_cargo_profile_features_dependency_or_path_forgery_rejected(self):
        changes = [lambda rows: rows[-2]['profile'].update(debug_assertions=True),
                   lambda rows: rows[-2]['profile'].update(opt_level='0'),
                   lambda rows: rows[-2]['profile'].update(debuginfo=2),
                   lambda rows: rows[-2]['profile'].update(debuginfo=False),
                   lambda rows: rows[-2].update(features=['extra']),
                   lambda rows: rows[-2].update(executable='/old/managed_process-abcdef'),
                   lambda rows: rows[-2].update(filenames=['/old/managed_process-abcdef']),
                   lambda rows: rows[-2]['target'].update(src_path='/different/managed_process.rs'),
                   lambda rows: rows[-2].update(manifest_path='/different/Cargo.toml'),
                   lambda rows: rows[0].update(package_id='registry+unknown#unknown@1.0.0'),
                   lambda rows: rows[0].update(package_id=rows[0]['package_id'].replace('0.1.0', '0.2.0')),
                   lambda rows: rows[1].update(features=['default']),
                   lambda rows: rows[1]['profile'].update(opt_level='0'),
                   lambda rows: rows[1]['target'].update(src_path='/wrong/lib.rs'),
                   lambda rows: rows[1].update(package_id='path+file:///wrong#0.1.0'),
                   lambda rows: rows.pop(1), lambda rows: rows.insert(0, copy.deepcopy(rows[0])),
                   lambda rows: rows.pop(), lambda rows: rows[-1].update(success=False)]
        for change in changes:
            with self.subTest(change=change), tempfile.TemporaryDirectory() as directory:
                fixture = HandoffFixture(pathlib.Path(directory)).prepare()
                def mutate(payloads):
                    rows = [gate.unique_json(line) for line in payloads['build.jsonl'].splitlines()]
                    change(rows)
                    payloads['build.jsonl'] = b''.join(encoded(row) + b'\n' for row in rows)
                    manifest = gate.unique_json(payloads['handoff.json'])
                    # Forge self-consistent fields; independent validation must still reject.
                    manifest['artifact'] = next(row for row in rows if row.get('target', {}).get('name') == 'managed_process')
                    manifest['files']['build.jsonl'] = {'byte_length': len(payloads['build.jsonl']),
                        'sha256': hashlib.sha256(payloads['build.jsonl']).hexdigest()}
                    payloads['handoff.json'] = encoded(manifest)
                fixture.rewrite(mutate)
                self.assert_rejected_before_execution(fixture)

    def test_zip_closure_names_modes_and_formats_reject_before_execute(self):
        cases = ['missing', 'extra', 'duplicate', 'traversal', 'absolute', 'backslash', 'symlink',
                 'directory', 'fifo', 'executable', 'deflate', 'extra-field', 'comment', 'trailing',
                 'truncated', 'nul-name', 'encrypted', 'bad-crc', 'outer-extra', 'archive-link', 'archive-hardlink']
        for case in cases:
            with self.subTest(case=case), tempfile.TemporaryDirectory() as directory:
                fixture = HandoffFixture(pathlib.Path(directory)).prepare()
                original = fixture.archive.read_bytes()
                payloads = gate.read_handoff_archive(fixture.archive)
                if case in ('trailing', 'truncated', 'nul-name', 'encrypted', 'bad-crc'):
                    bad = original
                    if case == 'trailing':
                        bad += b'extra'
                    elif case == 'truncated':
                        bad = bad[:-1]
                    elif case == 'nul-name':
                        bad = bad.replace(b'managed_process', b'managed\0process')
                    elif case == 'encrypted':
                        bad = bad[:6] + b'\x01\x00' + bad[8:]
                    else:
                        bad = bad[:40] + bytes([bad[40] ^ 1]) + bad[41:]
                    fixture.archive.write_bytes(bad)
                elif case == 'outer-extra':
                    (fixture.download / 'unexpected').write_text('extra')
                elif case in ('archive-link', 'archive-hardlink'):
                    original_path = fixture.root / 'target/original.zip'
                    fixture.archive.rename(original_path)
                    if case == 'archive-link':
                        fixture.archive.symlink_to(original_path)
                    else:
                        os.link(original_path, fixture.archive)
                else:
                    with warnings.catch_warnings(), zipfile.ZipFile(fixture.archive, 'w') as bundle:
                        warnings.simplefilter('ignore', UserWarning)
                        for name, data in payloads.items():
                            if case == 'missing' and name == 'managed_process':
                                continue
                            entry = zipfile.ZipInfo(name)
                            entry.create_system = 3
                            entry.external_attr = (stat.S_IFREG | 0o644) << 16
                            if name == 'managed_process':
                                if case in ('traversal', 'absolute', 'backslash'):
                                    entry.filename = {'traversal': '../managed_process', 'absolute': '/managed_process',
                                                      'backslash': '..\\managed_process'}[case]
                                if case in ('symlink', 'directory', 'fifo', 'executable'):
                                    entry.external_attr = {'symlink': stat.S_IFLNK | 0o777,
                                                           'directory': stat.S_IFDIR | 0o755,
                                                           'fifo': stat.S_IFIFO | 0o644,
                                                           'executable': stat.S_IFREG | 0o755}[case] << 16
                                if case == 'deflate':
                                    entry.compress_type = zipfile.ZIP_DEFLATED
                                if case == 'extra-field':
                                    entry.extra = b'\x01\x00\x00\x00'
                                if case == 'comment':
                                    entry.comment = b'comment'
                            bundle.writestr(entry, data)
                        if case in ('extra', 'duplicate'):
                            bundle.writestr('extra' if case == 'extra' else 'managed_process', b'extra')
                fixture.bind_archive()
                self.assert_rejected_before_execution(fixture)

    def test_archive_replacement_during_read_rejected_before_execute(self):
        with tempfile.TemporaryDirectory() as directory:
            fixture = HandoffFixture(pathlib.Path(directory)).prepare()
            original = gate.read_owned
            def replace_after_read(fd, name, limit):
                data = original(fd, name, limit)
                if name == 'handoff.zip':
                    replacement = fixture.root / 'target/replacement.zip'
                    replacement.write_bytes(data)
                    replacement.replace(fixture.archive)
                return data
            with patch.object(gate, 'read_owned', side_effect=replace_after_read):
                self.assert_rejected_before_execution(fixture)

    def test_member_bounds_enforced_before_materialization(self):
        for name in ('handoff.json', 'managed_process', 'dependencies.json', 'build.jsonl'):
            with self.subTest(name=name), tempfile.TemporaryDirectory() as directory:
                fixture = HandoffFixture(pathlib.Path(directory)).prepare()
                # Tiny test bound, no allocation or CPU-heavy zip bomb needed.
                with patch.dict(gate.HANDOFF_LIMITS, {name: 1}):
                    self.assert_rejected_before_execution(fixture)

    def test_aggregate_rejects_every_non_success_result(self):
        gate.validate_results('success', 'success', 'success')
        for index in range(3):
            for result in ('failure', 'cancelled', 'skipped', '', None):
                results = ['success'] * 3
                results[index] = result
                with self.subTest(results=results), self.assertRaises(ValueError):
                    gate.validate_results(*results)

    def test_clean_checkout_and_event_sha_are_independently_required(self):
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            subprocess.run(['git', 'init', '-q', str(root)], check=True)
            (root / 'source').write_text('one')
            subprocess.run(['git', '-C', str(root), 'add', 'source'], check=True)
            subprocess.run(['git', '-C', str(root), '-c', 'user.name=Fixture', '-c', 'user.email=fixture@example.invalid',
                            'commit', '-qm', 'fixture'], check=True)
            sha = subprocess.check_output(['git', '-C', str(root), 'rev-parse', 'HEAD']).decode().strip()
            env = {'GITHUB_REPOSITORY': 'owner/repo', 'GITHUB_REPOSITORY_ID': '23', 'GITHUB_RUN_ID': '42',
                   'GITHUB_RUN_ATTEMPT': '1', 'GITHUB_SHA': sha, 'LIFECYCLE_EVENT_HEAD_SHA': sha,
                   'GITHUB_EVENT_NAME': 'push', 'GITHUB_REF': 'refs/heads/main', 'GITHUB_WORKFLOW': 'CoreHR full scale'}
            with patch.dict(os.environ, env):
                self.assertEqual(gate.checkout_identity(root)['commit'], sha)
                (root / 'generated').write_text('ignored by clean tracked check, included by source_snapshot')
                gate.checkout_identity(root)
                (root / 'source').write_text('dirty')
                with self.assertRaises(ValueError):
                    gate.checkout_identity(root)
                (root / 'source').write_text('one')
                with patch.dict(os.environ, {'GITHUB_SHA': 'f' * 40, 'LIFECYCLE_EVENT_HEAD_SHA': 'f' * 40}), self.assertRaises(ValueError):
                    gate.checkout_identity(root)

    def test_provenance_path_is_exported_at_runtime_not_job_expression_scope(self):
        text = (pathlib.Path(__file__).resolve().parents[1] / '.github/workflows/corehr.yml').read_text()
        runtime = text.split('  lifecycle:', 1)[1].split('  scenario:', 1)[0]
        before_steps = runtime.split('    steps:', 1)[0]
        # GitHub's context-availability contract excludes runner from jobs.<id>.env.
        # https://docs.github.com/en/actions/reference/workflows-and-actions/contexts#context-availability
        def check_job_scope(prefix):
            self.assertNotIn('${{ runner.', prefix)
            self.assertNotIn('LIFECYCLE_ARTIFACT_METADATA:', prefix)
        check_job_scope(before_steps)
        with self.assertRaises(AssertionError):
            check_job_scope(before_steps + '      LIFECYCLE_ARTIFACT_METADATA: ${{ runner.temp }}/metadata.json\n')
        script = runtime.split('          script: |', 1)[1].split('      - name:', 1)[0]
        self.assertIn("core.exportVariable('LIFECYCLE_ARTIFACT_METADATA',", script)
        self.assertIn("require('path').join(process.env.RUNNER_TEMP, 'managed-lifecycle-artifact-service.json')", script)
        self.assertLess(script.index('core.exportVariable('), script.index('fs.writeFileSync('))

    def test_workflow_routes_only_required_current_run_artifact_and_preserves_gates(self):
        text = (pathlib.Path(__file__).resolve().parents[1] / '.github/workflows/corehr.yml').read_text()
        builder = text.split('  lifecycle-build:', 1)[1].split('  lifecycle:', 1)[0]
        runtime = text.split('  lifecycle:', 1)[1].split('  scenario:', 1)[0]
        aggregate = text.split('  full-scale:', 1)[1]
        self.assertIn('needs: [lifecycle-build]', runtime)
        self.assertIn('timeout-minutes: 90', runtime)
        self.assertIn('artifact-id: ${{ steps.upload_handoff.outputs.artifact-id }}', builder)
        self.assertIn('bundle-sha256: ${{ steps.build_handoff.outputs.bundle-sha256 }}', builder)
        self.assertIn('bundle-length: ${{ steps.build_handoff.outputs.bundle-length }}', builder)
        self.assertIn('getArtifact', runtime)
        self.assertIn('getWorkflowRunAttempt', runtime)
        self.assertLess(runtime.index('--mode verify-provenance'), runtime.index('uses: actions/download-artifact@v4'))
        download = runtime.split('uses: actions/download-artifact@v4', 1)[1].split('      - name:', 1)[0]
        self.assertIn('artifact-ids: ${{ needs.lifecycle-build.outputs.artifact-id }}', download)
        for forbidden in ('run-id:', 'repository:', 'github-token:', 'url:', 'name:'):
            self.assertNotIn(forbidden, '\n'.join(line for line in download.splitlines() if not line.lstrip().startswith('#')))
        for forbidden in ('cargo ', 'rust-cache', 'rust-toolchain', 'apt-get', 'continue-on-error'):
            self.assertNotIn(forbidden, runtime)
        self.assertIn('managed-lifecycle-ci-results/Cargo.lock', runtime)
        self.assertIn('managed-lifecycle-ci-results/cargo-*.toml', runtime)
        self.assertIn('managed-lifecycle-ci-results/lifecycle/artifacts/**', runtime)
        self.assertIn('managed-lifecycle-ci-results/lifecycle/progress.json.tmp', runtime)
        self.assertIn('name: CoreHR 200k people / 5k departments', aggregate)
        self.assertIn('needs: [lifecycle-build, lifecycle, scenario]', aggregate)
        self.assertIn('if: always()', aggregate)
        self.assertIn('--mode aggregate', aggregate)
        self.assertNotIn('continue-on-error', text)


class RunnerMetadataTests(unittest.TestCase):
    def test_counter_units_and_unknown_telemetry(self):
        with patch.object(gate.os, 'sysconf', side_effect=[100, 4096]):
            info = gate.runner_metadata()
            self.assertEqual(info['clock_ticks_per_second'], 100)
            self.assertEqual(info['page_bytes'], 4096)
        for values in ([ValueError(), OSError()], [-1, 0], [AttributeError(), ValueError()]):
            with self.subTest(values=values), patch.object(gate.os, 'sysconf', side_effect=values), \
                    patch.object(gate.os, 'cpu_count', return_value=None):
                info = gate.runner_metadata()
                self.assertIsNone(info['clock_ticks_per_second'])
                self.assertIsNone(info['page_bytes'])
                self.assertIsNone(info['logical_cpus'])


if __name__ == '__main__':
    unittest.main()
