import copy
import json
import unittest
import pathlib
import subprocess
import sys
import tempfile
from unittest.mock import patch

from managed_timing_compare import ORDER, PREFIX, TEST, analyze, bounded_run, run


class ComparisonTest(unittest.TestCase):
    def setUp(self):
        self.stdout = (f'running 1 test\ntest {TEST} ... ok\n'
                       'test result: ok. 1 passed; 0 failed; 0 ignored; 4 filtered out;\n')
        self.record = {'format': 1, 'warmups': 2, 'samples': [
            {'index': i, 'enabled': enabled, 'lifecycle_elapsed_ns': 120 if enabled else 100}
            for i, enabled in enumerate(ORDER)]}

    def parse(self, record=None, code=0):
        return analyze(self.stdout, PREFIX + json.dumps(record or self.record), code)

    def test_balanced_blocks_and_raw_samples(self):
        result = self.parse()
        self.assertEqual(result['disabled_median_ns'], 100)
        self.assertEqual(result['enabled_median_ns'], 120)
        self.assertEqual([b['enabled_minus_disabled_ns'] for b in result['blocks']], [20, 20])
        self.assertEqual(result['samples'], self.record['samples'])

    def test_reject_failed_ignored_wrong_or_multiple_test(self):
        for stdout in (self.stdout.replace(' ... ok', ' ... ignored'),
                       self.stdout.replace(TEST, 'different_test'),
                       self.stdout + 'running 1 test\n',
                       self.stdout.replace('1 passed', '0 passed')):
            with self.subTest(stdout=stdout), self.assertRaises(ValueError):
                analyze(stdout, PREFIX + json.dumps(self.record), 0)
        with self.assertRaises(ValueError):
            self.parse(code=1)

    def test_reject_missing_duplicate_records(self):
        for stderr in ('', (PREFIX + json.dumps(self.record) + '\n') * 2):
            with self.assertRaises(ValueError):
                analyze(self.stdout, stderr, 0)

    def test_reject_incomplete_or_reordered_samples(self):
        for samples in (self.record['samples'][:-1], list(reversed(self.record['samples']))):
            record = copy.deepcopy(self.record)
            record['samples'] = samples
            with self.assertRaises(ValueError):
                self.parse(record)

    def test_reject_invalid_values_and_types(self):
        for key, value in (('index', True), ('index', -1), ('enabled', 0),
                           ('lifecycle_elapsed_ns', True), ('lifecycle_elapsed_ns', 0),
                           ('lifecycle_elapsed_ns', -1), ('lifecycle_elapsed_ns', 1.5),
                           ('lifecycle_elapsed_ns', 2 ** 128)):
            record = copy.deepcopy(self.record)
            record['samples'][0][key] = value
            with self.subTest(key=key, value=value), self.assertRaises(ValueError):
                self.parse(record)

    def test_elapsed_time_is_not_an_extra_performance_gate(self):
        self.record['samples'][0]['lifecycle_elapsed_ns'] = 241_000_000_000
        self.assertTrue(self.parse()['complete'])

    def test_negative_difference_is_preserved_not_claimed_as_speedup(self):
        for sample in self.record['samples']:
            if sample['enabled']:
                sample['lifecycle_elapsed_ns'] = 90
        self.assertEqual(self.parse()['blocks'][0]['enabled_minus_disabled_ns'], -10)

    def test_reject_boolean_or_unknown_record_version(self):
        for version in (True, 2, "1"):
            record = copy.deepcopy(self.record)
            record['format'] = version
            with self.assertRaises(ValueError):
                self.parse(record)

    def test_runner_identity_and_failure_records(self):
        # A synthetic executable validates the evidence collector, not graph behavior.
        for failure in (None, 'source', 'binary', 'test'):
            with self.subTest(failure=failure), tempfile.TemporaryDirectory() as temp:
                root = pathlib.Path(temp)
                output = root / 'evidence'
                binary = root / 'synthetic-test'
                binary.write_bytes(b'synthetic binary')
                snapshots = [{'sha256': 'same'}, {'sha256': 'changed' if failure == 'source' else 'same'}]

                def execute(command, cwd, stdout, stderr, timeout):
                    if command[0] == 'cargo':
                        stdout.write('{}\n')
                        return 0
                    stdout.write(self.stdout)
                    stderr.write(PREFIX + json.dumps(self.record))
                    if failure == 'binary':
                        binary.write_bytes(b'changed')
                    return 1 if failure == 'test' else 0

                with patch('managed_timing_compare.source_snapshot', side_effect=snapshots), \
                     patch('managed_timing_compare.subprocess.check_output', return_value='synthetic'), \
                     patch('managed_timing_compare.select_artifact', return_value={'executable': str(binary)}), \
                     patch('managed_timing_compare.bounded_run', side_effect=execute):
                    if failure:
                        with self.assertRaises(ValueError):
                            run(root, output)
                        self.assertFalse((output / 'comparison.json').exists())
                        self.assertFalse(json.loads((output / 'execution.json').read_text())['complete'])
                    else:
                        result = run(root, output)
                        self.assertTrue(result['complete'])
                        self.assertTrue(json.loads((output / 'execution.json').read_text())['complete'])
                        self.assertTrue((output / 'source-before.json').exists())
                        self.assertTrue((output / 'source-after.json').exists())
                    with self.assertRaises(FileExistsError):
                        run(root, output)

    def test_bounded_process_success_failure_and_timeout(self):
        with tempfile.TemporaryFile() as output:
            for code in (0, 3):
                self.assertEqual(bounded_run(
                    [sys.executable, '-c', f'raise SystemExit({code})'],
                    pathlib.Path.cwd(), output, output, 5), code)
            with self.assertRaises(subprocess.TimeoutExpired):
                bounded_run([sys.executable, '-c', 'import time; time.sleep(10)'],
                            pathlib.Path.cwd(), output, output, 0.05)


if __name__ == '__main__':
    unittest.main()
