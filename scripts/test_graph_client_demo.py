"""Safety checks for the development-only demo driver (no server needed)."""
import subprocess
import json
import tempfile
import sys
from pathlib import Path
import unittest
from unittest.mock import Mock

import graph_client_demo as demo


class DemoSafetyTests(unittest.TestCase):
    def test_requires_explicit_plaintext_acknowledgment(self):
        result = subprocess.run(
            [sys.executable, str(Path(demo.__file__)), '--node', '/unused/node',
             '--client', '/unused/client'], capture_output=True, text=True, timeout=5)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('--allow-insecure-development is required', result.stderr)

    def test_cleanup_only_received_children(self):
        running, exited, unrelated = Mock(), Mock(), Mock()
        running.poll.return_value = None
        exited.poll.return_value = 0
        demo.stop_processes([running, exited])
        running.terminate.assert_called_once_with()
        exited.terminate.assert_not_called()
        running.wait.assert_called_once_with(timeout=5)
        exited.wait.assert_called_once_with(timeout=5)
        unrelated.assert_not_called()
        running.kill.assert_not_called()

    def test_unresponsive_child_is_killed_and_reaped(self):
        child = Mock()
        child.poll.return_value = None
        child.wait.side_effect = [subprocess.TimeoutExpired('node', 5), 0]
        demo.stop_processes([child])
        child.terminate.assert_called_once_with()
        child.kill.assert_called_once_with()
        self.assertEqual(child.wait.call_count, 2)

    def test_failed_client_preserves_unknown_ids_and_is_not_retried(self):
        from unittest.mock import patch
        response = {'ok': False, 'error': {'outcome': 'Unknown',
                    'source_request_id': 'source-123', 'reverse_request_id': 'reverse-456',
                    'stage': 'Reverse', 'source_acknowledged_this_attempt': True}}
        with tempfile.TemporaryDirectory() as root, patch.object(demo.subprocess, 'run') as run:
            run.return_value = subprocess.CompletedProcess(
                [], 1, json.dumps(response), 'write did not receive a complete acknowledgement')
            with self.assertRaises(RuntimeError) as raised:
                demo.invoke('/client', Path(root) / 'config.json', 'experimental', 'edge', 'create')
            for detail in ('Unknown', 'source-123', 'reverse-456', 'Reverse',
                           'source_acknowledged_this_attempt', 'complete acknowledgement'):
                self.assertIn(detail, str(raised.exception))
            run.assert_called_once()
            record = json.loads((Path(root) / 'client-results.jsonl').read_text())
            self.assertEqual(record['response'], response)
            self.assertEqual(record['exit'], 1)
            self.assertIn('complete acknowledgement', record['stderr'])

    def test_client_timeout_is_logged_as_unknown_without_retry(self):
        from unittest.mock import patch
        with tempfile.TemporaryDirectory() as root, patch.object(demo.subprocess, 'run') as run:
            run.side_effect = subprocess.TimeoutExpired('client', 20, b'partial output', b'diagnostic')
            with self.assertRaisesRegex(RuntimeError, 'no retry'):
                demo.invoke('/client', Path(root) / 'config.json', 'experimental', 'edge', 'create')
            run.assert_called_once()
            record = json.loads((Path(root) / 'client-results.jsonl').read_text())
            self.assertEqual(record['outcome'], 'unknown-client-timeout')
            self.assertEqual(record['stdout'], 'partial output')
            self.assertEqual(record['stderr'], 'diagnostic')

    def test_log_io_failure_cannot_hide_received_identity_or_timeout(self):
        from unittest.mock import patch
        evidence = json.dumps({'ok': False, 'error': {'outcome': 'Unknown',
                               'request_id': 'original-id', 'stage': 'Source'}})
        for timeout in (False, True):
            for operation in ('open', 'write', 'close'):
                with self.subTest(timeout=timeout, operation=operation), \
                        patch.object(demo.subprocess, 'run') as run, \
                        patch.object(Path, 'open') as open_log:
                    if operation == 'open':
                        open_log.side_effect = OSError('disk full')
                    elif operation == 'write':
                        open_log.return_value.__enter__.return_value.write.side_effect = OSError('disk full')
                    else:
                        open_log.return_value.__exit__.side_effect = OSError('disk full')
                    if timeout:
                        run.side_effect = subprocess.TimeoutExpired('client', 20,
                                                                   evidence.encode(), b'network stalled')
                    else:
                        run.return_value = subprocess.CompletedProcess([], 1, evidence, 'network stalled')
                    with self.assertRaises(RuntimeError) as raised:
                        demo.invoke('/client', '/config', 'experimental', 'edge', 'create')
                    for detail in ('original-id', 'Unknown', 'Source', 'network stalled', 'no retry'):
                        self.assertIn(detail, str(raised.exception))
                    if timeout:
                        self.assertIn('unknown-client-timeout', str(raised.exception))
                    run.assert_called_once()


if __name__ == '__main__':
    unittest.main()
