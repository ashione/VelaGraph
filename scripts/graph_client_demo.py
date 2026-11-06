#!/usr/bin/env python3
"""Run an isolated, plaintext, non-atomic observed graph demonstration."""
import argparse
import json
import os
from pathlib import Path
import socket
import signal
import subprocess
import sys
import tempfile
import time


def run_client(client, config, args, timeout=20):
    log_path = Path(config).parent / 'client-results.jsonl'
    try:
        result = subprocess.run(
            [str(client), '--config', str(config), '--allow-insecure-development', *args],
            capture_output=True, text=True, timeout=timeout, check=False,
        )
    except subprocess.TimeoutExpired as error:
        # Timeout kills the client, not a write already submitted to the server.
        record = {'args': args, 'config': str(config), 'exit': None,
                  'outcome': 'unknown-client-timeout',
                  'stdout': (error.stdout or b'').decode(errors='replace')
                  if isinstance(error.stdout, bytes) else error.stdout,
                  'stderr': (error.stderr or b'').decode(errors='replace')
                  if isinstance(error.stderr, bytes) else error.stderr}
        with log_path.open('a') as log:
            log.write(json.dumps(record) + '\n')
        raise RuntimeError(f'Client timed out; no retry. Inspect {log_path}: {record}') from error
    record = {'args': args, 'config': str(config), 'exit': result.returncode,
              'stdout': result.stdout, 'stderr': result.stderr}
    try:
        record['response'] = json.loads(result.stdout)
    except json.JSONDecodeError:
        pass
    with log_path.open('a') as log:
        log.write(json.dumps(record) + '\n')
    return result


def invoke(client, config, *args, timeout=20):
    result = run_client(client, config, args, timeout)
    if result.returncode:
        raise RuntimeError(f'graph-client {args}: stdout={result.stdout} '
                           f'stderr={result.stderr}')
    response = json.loads(result.stdout)
    if response.get('ok') is not True:
        raise RuntimeError(f'graph-client rejected {args}: {response}')
    return response


def expect_failure(client, config, *args):
    result = run_client(client, config, args)
    response = json.loads(result.stdout)
    if result.returncode == 0 or response.get('ok') is not False:
        raise RuntimeError(f'Expected nonzero JSON failure for {args}: {response}')
    return response


def stop_processes(processes):
    # Child handles are the only authority for cleanup. Never pkill by name.
    for process in processes:
        if process.poll() is None:
            process.terminate()
    for process in processes:
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=5)


def run_demo(node, client):
    root = Path(tempfile.mkdtemp(prefix='graph-client-demo-'))
    print(f'Demo directory (retained): {root}', flush=True)
    print('WARNING: trusted loopback only; unauthenticated plaintext. '
          'Cross-shard writes are non-atomic; observed results are Unverified.', flush=True)
    processes, logs, reservations = [], [], []
    try:
        for _ in range(9):
            sock = socket.socket()
            sock.bind(('127.0.0.1', 0))
            reservations.append(sock)
        addresses = [f'127.0.0.1:{sock.getsockname()[1]}' for sock in reservations]
        # Ports cannot be handed to graph-node. A competing bind fails explicitly.
        for sock in reservations:
            sock.close()
        for group in range(1, 4):
            raft, control, graph = addresses[(group - 1) * 3:group * 3]
            log = (root / f'group-{group}.log').open('w')
            logs.append(log)
            processes.append(subprocess.Popen([
                str(node), '--allow-insecure-development', '--id', '1',
                '--group', str(group), '--shard', str(100 + group), '--epoch', '1',
                '--raft', raft, '--control', control, '--graph', graph,
                '--data-dir', str(root / f'group-{group}'), '--bootstrap', f'1={raft}',
            ], stdout=log, stderr=subprocess.STDOUT))
        config = root / 'config.json'
        config.write_text(json.dumps({
            'version': 1, 'timeout_ms': 10000,
            'metadata': {'group': 1, 'shard': 101, 'epoch': 1, 'replicas': [1]},
            'metadata_node': 1,
            'metadata_nodes': [{'group': 1, 'node': 1, 'endpoint': addresses[2]}],
            'data_nodes': [{'group': group, 'node': 1, 'endpoint': addresses[group * 3 - 1]}
                           for group in (2, 3)],
        }))
        deadline = time.monotonic() + 40
        while True:
            if any(process.poll() is not None for process in processes):
                raise RuntimeError('A graph-node exited during startup')
            if all('graph=' in (root / f'group-{group}.log').read_text()
                   for group in range(1, 4)):
                break
            if time.monotonic() >= deadline:
                raise RuntimeError('Timed out waiting for graph-node readiness')
            time.sleep(0.05)
        invoke(client, config, 'config-check')
        for partition, group, name in ((10, 2, 'Alice'), (20, 3, 'Bob')):
            placement = root / f'placement-{partition}.json'
            placement.write_text(json.dumps({'version': 1, 'group': group,
                                            'shard': 100 + group, 'epoch': 1,
                                            'replicas': [1]}))
            invoke(client, config, 'catalog', 'register', '--partition', str(partition),
                   '--file', str(placement))
            vertex = root / f'vertex-{partition}.json'
            vertex.write_text(json.dumps({'version': 1, 'id': 1, 'labels': ['person'],
                                         'properties': {'name': {'Text': name}}}))
            invoke(client, config, 'experimental', 'vertex', 'create', '--partition',
                   str(partition), '--file', str(vertex))
        invoke(client, config, 'experimental', 'edge', 'create', '--from', '10:1',
               '--to', '20:1', '--id', '7', '--label', 'knows')
        response = invoke(client, config, 'experimental', 'observed-query',
                          '--start-partition', '10', '--query',
                          "g.V(1).outE('knows').inV().path()")
        expected = [{'type': 'vertex', 'partition': 10, 'id': 1},
                    {'type': 'edge', 'partition': 10, 'id': 7},
                    {'type': 'vertex', 'partition': 20, 'id': 1}]
        if response.get('completeness') != 'Unverified':
            raise RuntimeError(f'Missing Unverified completeness: {response}')
        if response.get('rows') != [{'type': 'path', 'elements': expected}]:
            raise RuntimeError(f'Unexpected qualified path: {response}')
        expected_stamps = [{'group': 2, 'shard': 102, 'epoch': 1, 'sequence': 2},
                           {'group': 3, 'shard': 103, 'epoch': 1, 'sequence': 2}]
        if response.get('snapshots') != expected_stamps:
            raise RuntimeError(f'Unexpected independent shard snapshots: {response}')
        for partition, group in ((10, 2), (20, 3)):
            lookup = invoke(client, config, 'catalog', 'lookup', '--partition', str(partition))
            expected_placement = {'group': group, 'shard': 100 + group,
                                  'epoch': 1, 'replicas': [1]}
            if (lookup.get('placement') != expected_placement
                    or lookup.get('metadata_sequence') != 2):
                raise RuntimeError(f'Unexpected catalog placement: {lookup}')
            count = invoke(client, config, 'v1', 'query', '--partition', str(partition),
                           '--query', 'g.V().count()')
            if count.get('rows') != [{'type': 'count', 'value': 0}]:
                raise RuntimeError(f'V2 records leaked into v1 partition: {count}')
        count = invoke(client, config, 'v1', 'global-query', '--query', 'g.V().count()')
        if count.get('rows') != [{'type': 'count', 'value': 0}]:
            raise RuntimeError(f'V2 records leaked into v1 global scan: {count}')
        for query, expected_rows in (
            ('g.V().count()', [{'type': 'count', 'value': 2}]),
            ('g.V(999).count()', [{'type': 'count', 'value': 0}]),
            ("g.V(1).out('knows').values('name')", [
                {'type': 'value', 'source': {'type': 'vertex', 'partition': 20, 'id': 1},
                 'value': {'Text': 'Bob'}}]),
        ):
            observed = invoke(client, config, 'experimental', 'observed-query',
                              '--start-partition', '10', '--query', query)
            if (observed.get('rows') != expected_rows
                    or observed.get('completeness') != 'Unverified'):
                raise RuntimeError(f'Unexpected observed result for {query}: {observed}')
        print('Verified catalog placements, v1/v2 isolation, observed counts, '
              'projection provenance, and independent shard snapshots', flush=True)
        expect_failure(client, config, 'experimental', 'observed-query',
                       '--start-partition', '10', '--query', 'g.V(1).repeat(out())')
        expect_failure(client, config, 'experimental', 'observed-query',
                       '--start-partition', '999', '--query', 'g.V(1)')
        wrong_identity = json.loads(config.read_text())
        wrong_identity['metadata']['replicas'] = [99]
        wrong_identity['metadata_node'] = 99
        wrong_identity['metadata_nodes'][0]['node'] = 99
        bad_config = root / 'wrong-identity.json'
        bad_config.write_text(json.dumps(wrong_identity))
        expect_failure(client, bad_config, 'catalog', 'lookup', '--partition', '10')
        # Bound but non-listening socket ensures a refused endpoint, without a
        # timing assertion that a healthy operation must miss a 1 ms deadline.
        with socket.socket() as unavailable:
            unavailable.bind(('127.0.0.1', 0))
            offline = json.loads(config.read_text())
            offline['timeout_ms'] = 1
            offline['metadata_nodes'][0]['endpoint'] = (
                f'127.0.0.1:{unavailable.getsockname()[1]}')
            offline_config = root / 'offline.json'
            offline_config.write_text(json.dumps(offline))
            expect_failure(client, offline_config, 'catalog', 'lookup', '--partition', '10')
        print('Error checks verified: unsupported query, missing partition, '
              'wrong identity, unavailable endpoint with 1 ms deadline', flush=True)
        print(json.dumps(response, sort_keys=True), flush=True)
        print('Demo verified: 10:1 -> edge 10:7 -> 20:1; completeness=Unverified', flush=True)
        return root
    except BaseException:
        for group in range(1, 4):
            path = root / f'group-{group}.log'
            if path.exists():
                print(f'--- {path} ---\n{path.read_text()[-12000:]}', file=sys.stderr)
        raise
    finally:
        for sock in reservations:
            sock.close()
        stop_processes(processes)
        for log in logs:
            log.close()
        print(f'Stopped only demo-owned processes. Data/logs retained at {root}', flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--node', required=True, type=Path, help='built graph-node executable')
    parser.add_argument('--client', required=True, type=Path, help='built graph-client executable')
    parser.add_argument('--allow-insecure-development', action='store_true',
                        help='acknowledge unauthenticated plaintext loopback services')
    args = parser.parse_args()
    if not args.allow_insecure_development:
        parser.error('--allow-insecure-development is required')
    for path in (args.node, args.client):
        if not path.is_file() or not os.access(path, os.X_OK):
            parser.error(f'not an executable file: {path}')
    def interrupted(_signum, _frame):
        raise KeyboardInterrupt

    signal.signal(signal.SIGTERM, interrupted)
    try:
        run_demo(args.node.resolve(), args.client.resolve())
    except (Exception, KeyboardInterrupt) as error:
        print(f'Demo failed: {error}', file=sys.stderr)
        return 1
    return 0


if __name__ == '__main__':
    sys.exit(main())
