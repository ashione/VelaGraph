import contextlib
import copy
import io
import json
import os
import pathlib
import subprocess
import tempfile
import unittest
from unittest.mock import patch

import managed_lifecycle_ci as gate


def artifact(root):
    return {'reason': 'compiler-artifact',
            'manifest_path': str(root / 'crates/replicated-graph/Cargo.toml'),
            'target': {'name': 'managed_process', 'kind': ['test'], 'test': True,
                       'src_path': str(root / 'crates/replicated-graph/tests/managed_process.rs')},
            'profile': {'test': True, 'debug_assertions': False, 'opt_level': '3'},
            'executable': str(root / 'target/release/deps/managed_process-fixture')}


def output(name=gate.TEST, result='ok', passed=1, ignored=0, running=1):
    return (f'\nrunning {running} test\ntest {name} ... {result}\n\n'
            f'test result: ok. {passed} passed; 0 failed; {ignored} ignored; '
            '0 measured; 10 filtered out; finished in 123.45s\n\n')


def checkpoint(executable, source_hash='a' * 64, elf_hash='b' * 64):
    # Synthetic validator data only; Rust verifies the actual request/proof bytes.
    return {'format': 1, 'source_sha256': source_hash, 'elf_sha256': elf_hash,
            'executable': str(executable), 'verified_ordinal': 1025,
            'pending_external_id': None, 'completed': True,
            'ack_loss_recovered': True, 'snapshots_restored': True,
            'binding': {'cohort': {'profile': 'ByteBound16MiBV1'}},
            'originals': {str(n): {'synthetic': True} for n in (1, 256, 1024, 1025)},
            'proofs': {str(n): [1] for n in (0, 1, 256, 1024, 1025)},
            'grants': [{'member': {'shard': n}} for n in (7, 8)]}


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
        executable = pathlib.Path('/synthetic/managed_process')
        valid = checkpoint(executable)
        gate.validate_checkpoint(valid, 'a' * 64, 'b' * 64, executable)
        for field, value in [('format', 2), ('format', True), ('source_sha256', 'c' * 64),
                             ('elf_sha256', 'c' * 64), ('executable', '/wrong'),
                             ('completed', False), ('completed', 1),
                             ('verified_ordinal', 1024), ('verified_ordinal', 1025.0),
                             ('pending_external_id', [0] * 16), ('ack_loss_recovered', False),
                             ('snapshots_restored', False), ('originals', {}),
                             ('proofs', {}), ('grants', []), ('binding', {})]:
            changed = copy.deepcopy(valid)
            changed[field] = value
            with self.subTest(field=field, value=value), self.assertRaises(ValueError):
                gate.validate_checkpoint(changed, 'a' * 64, 'b' * 64, executable)
        for field in valid:
            changed = copy.deepcopy(valid)
            del changed[field]
            with self.subTest(missing=field), self.assertRaises(ValueError):
                gate.validate_checkpoint(changed, 'a' * 64, 'b' * 64, executable)

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


class GateTests(unittest.TestCase):
    def run_fixture(self, root, out, test_output=None, exit_code=0, mutate=None,
                    build_exit=0, artifact_mutate=None):
        selected = artifact(root)
        (root / 'source.rs').write_text('synthetic project source\n')
        binary = pathlib.Path(selected['executable'])
        binary.parent.mkdir(parents=True)
        binary.write_bytes(b'\x7fELFsynthetic test fixture; never executed')
        binary.chmod(0o755)
        calls = []

        def fake_run(command, cwd, env, stdout, stderr):
            calls.append((command, env))
            if command == gate.BUILD:
                if artifact_mutate:
                    artifact_mutate(selected)
                stdout.write(json.dumps(selected) + '\n')
                stdout.write(json.dumps({'reason': 'build-finished', 'success': build_exit == 0}) + '\n')
                return subprocess.CompletedProcess(command, build_exit)
            self.assertEqual(command, [str(out / 'managed_process'), gate.TEST, '--exact',
                                      '--nocapture', '--test-threads=1', '--format=pretty', '--color=never'])
            self.assertEqual(pathlib.Path(command[0]).read_bytes(), binary.read_bytes())
            self.assertTrue(os.access(command[0], os.X_OK))
            self.assertEqual(env['BYTE_BOUND_PROCESS_ELF_SHA256'], gate.sha256(binary))
            stdout.write(output() if test_output is None else test_output)
            saved = checkpoint(command[0], env['BYTE_BOUND_PROCESS_SOURCE_SHA256'],
                               env['BYTE_BOUND_PROCESS_ELF_SHA256'])
            if mutate:
                mutate(saved)
            (pathlib.Path(env['BYTE_BOUND_PROCESS_DIRECTORY']) / 'progress.json').write_text(json.dumps(saved))
            stderr.write('synthetic child stderr\n')
            return subprocess.CompletedProcess(command, exit_code)

        with patch.object(gate.subprocess, 'run', side_effect=fake_run), \
                patch.object(gate.subprocess, 'check_output', return_value=b'source.rs\0'), \
                contextlib.redirect_stdout(io.StringIO()):
            gate.run_gate(root, out)
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
                     {'mutate': lambda c: c.update(verified_ordinal=1024)},
                     {'mutate': lambda c: c.update(pending_external_id=[0] * 16)}]
        for scenario in scenarios:
            with self.subTest(scenario=scenario), tempfile.TemporaryDirectory() as directory:
                root = pathlib.Path(directory)
                out = root / 'results'
                with self.assertRaises(ValueError):
                    self.run_fixture(root, out, **scenario)
                self.assertEqual(json.loads((out / 'execution.json').read_text())['status'], 'INCOMPLETE')
                self.assertTrue((out / 'summary.md').read_text().startswith('Managed lifecycle INCOMPLETE:'))

    def test_existing_success_evidence_is_not_reused_or_overwritten(self):
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            out = root / 'results'
            out.mkdir()
            summary = out / 'summary.md'
            summary.write_text('old evidence')
            with patch.object(gate.subprocess, 'run') as run, self.assertRaises(FileExistsError):
                gate.run_gate(root, out)
            run.assert_not_called()
            self.assertEqual(summary.read_text(), 'old evidence')

    def test_cli_failure_is_nonzero_and_says_incomplete(self):
        with patch.object(gate, 'run_gate', side_effect=ValueError('synthetic failure')), \
                patch.object(gate.sys, 'argv', ['managed_lifecycle_ci.py']), \
                contextlib.redirect_stderr(io.StringIO()) as stderr:
            self.assertEqual(gate.main(), 1)
        self.assertIn('INCOMPLETE: synthetic failure', stderr.getvalue())


if __name__ == '__main__':
    unittest.main()
