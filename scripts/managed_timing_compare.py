#!/usr/bin/env python3
"""Opt-in same-binary lifecycle diagnostic comparison, never a release gate."""
import argparse
import json
import pathlib
import os
import signal
import platform
import re
import statistics
import subprocess

from managed_lifecycle_ci import require, select_artifact, sha256, source_snapshot

TEST = 'managed_diagnostics_controlled_comparison'
PREFIX = 'managed diagnostic comparison: '
ORDER = [False, True, True, False, True, False, False, True]


def analyze(stdout, stderr, code):
    require(code == 0, f'comparison exited {code}')
    require(re.findall(r'^running (\d+) tests?$', stdout, re.M) == ['1'],
            'expected exactly one test')
    require(re.findall(r'^test (\S+) \.\.\. (.*)$', stdout, re.M) == [(TEST, 'ok')],
            'wrong, skipped or failed test')
    require(len(re.findall(r'^test result: ok\. 1 passed; 0 failed; 0 ignored;',
                           stdout, re.M)) == 1, 'missing test result')
    records = [json.loads(line[len(PREFIX):]) for line in stderr.splitlines()
               if line.startswith(PREFIX)]
    require(len(records) == 1, 'missing/duplicate comparison record')
    record = records[0]
    require(type(record) is dict and type(record.get('format')) is int and record['format'] == 1
            and type(record.get('warmups')) is int and record['warmups'] == 2,
            'unsupported comparison record')
    samples = record.get('samples')
    require(type(samples) is list and len(samples) == len(ORDER), 'incomplete samples')
    for i, (sample, enabled) in enumerate(zip(samples, ORDER)):
        require(type(sample) is dict and type(sample.get('index')) is int
                and sample['index'] == i and sample.get('enabled') is enabled,
                'wrong sample identity/order')
        value = sample.get('lifecycle_elapsed_ns')
        require(type(value) is int and 0 < value < 2 ** 128,
                'invalid lifecycle duration')
    disabled = [s['lifecycle_elapsed_ns'] for s in samples if not s['enabled']]
    enabled = [s['lifecycle_elapsed_ns'] for s in samples if s['enabled']]
    blocks = []
    for offset in (0, 4):
        block = samples[offset:offset + 4]
        on = statistics.mean(s['lifecycle_elapsed_ns'] for s in block if s['enabled'])
        off = statistics.mean(s['lifecycle_elapsed_ns'] for s in block if not s['enabled'])
        blocks.append({'start_index': offset, 'enabled_mean_ns': on,
                       'disabled_mean_ns': off, 'enabled_minus_disabled_ns': on - off,
                       'enabled_over_disabled_ratio': on / off})
    return {'format': 1, 'complete': True, 'samples': samples, 'blocks': blocks,
            'disabled_median_ns': statistics.median(disabled),
            'enabled_median_ns': statistics.median(enabled),
            'limitations': ['whole lifecycle, not pure instrumentation cost',
                            'same host, order balanced, not randomized or isolated',
                            'two warmups excluded; four measured samples per mode',
                            'no timing threshold, release approval or speedup claim']}


def bounded_run(command, root, stdout, stderr, timeout):
    """Own a new process group so timeout/interruption also stops fixture children."""
    process = subprocess.Popen(command, cwd=root, stdout=stdout, stderr=stderr,
                               start_new_session=True)
    try:
        return process.wait(timeout=timeout)
    except BaseException:
        try:
            os.killpg(process.pid, signal.SIGTERM)
        except ProcessLookupError:
            pass
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            pass
        # The parent may have exited while a child still owns the group.
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        process.wait()
        raise


def run(root, output):
    output.mkdir(parents=True, exist_ok=False)
    initial = source_snapshot(root, output)
    (output / 'source-before.json').write_text(json.dumps(initial, indent=2) + '\n')
    execution = {'format': 1, 'complete': False, 'platform': platform.platform(),
                 'logical_cpu_count': os.cpu_count(),
                 'rustc': subprocess.check_output(['rustc', '-Vv'], cwd=root, text=True).strip(),
                 'cargo': subprocess.check_output(['cargo', '-V'], cwd=root, text=True).strip(),
                 'build_environment': {name: os.environ.get(name) for name in
                                       ('RUSTFLAGS', 'CARGO_ENCODED_RUSTFLAGS',
                                        'CARGO_BUILD_JOBS', 'CARGO_TARGET_DIR',
                                        'CARGO_PROFILE_RELEASE_DEBUG', 'CARGO_PROFILE_RELEASE_LTO')},
                 'head': subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=root,
                                                 text=True).strip(),
                 'tree': subprocess.check_output(['git', 'rev-parse', 'HEAD^{tree}'], cwd=root,
                                                 text=True).strip(),
                 'source_sha256': initial['sha256'], 'test': TEST}
    build = ['cargo', 'test', '--release', '--locked', '-p', 'replicated-graph',
             '--test', 'managed_process', '--no-run', '--message-format=json']
    execution['build'] = build
    (output / 'execution.json').write_text(json.dumps(execution, indent=2) + '\n')
    with (output / 'build.jsonl').open('w') as stdout, (output / 'build.stderr').open('w') as stderr:
        code = bounded_run(build, root, stdout, stderr, timeout=3600)
    require(code == 0, 'build failed; inspect build.stderr')
    artifact = select_artifact([json.loads(line) for line in
                                (output / 'build.jsonl').read_text().splitlines()], root)
    binary = pathlib.Path(artifact['executable'])
    digest = sha256(binary)
    command = [str(binary), TEST, '--exact', '--ignored', '--nocapture', '--test-threads=1']
    execution.update(binary_sha256=digest, command=command)
    (output / 'execution.json').write_text(json.dumps(execution, indent=2) + '\n')
    with (output / 'test.stdout').open('w') as stdout, (output / 'test.stderr').open('w') as stderr:
        code = bounded_run(command, root, stdout, stderr, timeout=2500)
    final = source_snapshot(root, output)
    (output / 'source-after.json').write_text(json.dumps(final, indent=2) + '\n')
    require(final['sha256'] == initial['sha256'], 'source changed during comparison')
    require(sha256(binary) == digest, 'test binary changed during comparison')
    summary = analyze((output / 'test.stdout').read_text(),
                      (output / 'test.stderr').read_text(), code)
    summary.update(source_sha256=initial['sha256'], binary_sha256=digest)
    (output / 'comparison.json').write_text(json.dumps(summary, indent=2) + '\n')
    execution['complete'] = True
    (output / 'execution.json').write_text(json.dumps(execution, indent=2) + '\n')
    return summary


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=pathlib.Path, required=True,
                        help='new evidence directory; never overwrites a previous run')
    args = parser.parse_args()
    root = pathlib.Path(__file__).resolve().parents[1]
    print(json.dumps(run(root, args.output.resolve()), indent=2))
