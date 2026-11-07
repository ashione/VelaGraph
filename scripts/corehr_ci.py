#!/usr/bin/env python3
"""Full-size synthetic CoreHR correctness gate; measurements are not SLAs."""
import collections
from corehr_result_ci import validate as validate_results
import hashlib
import json
import os
import pathlib
import platform
import statistics
import subprocess
import sys
import time


def require(ok, message):
    if not ok:
        raise ValueError(message)


def read_records(path):
    with path.open() as stream:
        return [json.loads(line) for line in stream if line.strip()]


def fixture_shards(manifest):
    shards = manifest.get('shards')
    require(isinstance(shards, list) and len(shards) == 3, 'invalid fixture shard manifest')
    expected = {}
    for shard in shards:
        partition, rows = shard.get('partition'), shard.get('kv_rows')
        require(type(partition) is int and partition in (1, 2, 3)
                and partition not in expected and type(rows) is int and rows > 0,
                'invalid or duplicate fixture shard')
        expected[partition] = rows
    return expected


def validate_snapshots(snapshots, shards):
    # These identities and epoch are part of the deterministic three-shard harness,
    # not a generic restriction on the database's snapshot protocol.
    require(isinstance(snapshots, list) and len(snapshots) == len(shards),
            'missing shard snapshots')
    seen = set()
    for stamp in snapshots:
        require(isinstance(stamp, dict), 'invalid snapshot stamp')
        shard = stamp.get('shard')
        identity = stamp.get('identity')
        require(type(shard) is int and shard in shards and shard not in seen,
                'wrong or duplicate snapshot shard')
        require(type(stamp.get('epoch')) is int and stamp['epoch'] == 1
                and type(stamp.get('sequence')) is int and stamp['sequence'] > 0,
                'invalid snapshot epoch or sequence')
        require(isinstance(identity, dict)
                and type(identity.get('group')) is int and identity['group'] == shard
                and type(identity.get('node')) is int and identity['node'] == 1,
                'wrong snapshot identity')
        seen.add(shard)
    require(seen == set(shards), 'incomplete snapshot shard set')


def validate(records, manifest, jobs=False):
    require(isinstance(records, list) and bool(records), 'empty output')
    require(all(isinstance(r, dict) and isinstance(r.get('event'), str) for r in records),
            'invalid evidence record')
    common = {'capacity_before', 'explicit_capacity_upgrade', 'loaded_shard', 'load',
              'capacity_after', 'all_kv_verified', 'client_session', 'client_query', 'resources'}
    allowed = common | ({'job_matrix_begin', 'synchronous_guard', 'job_cancel_probe',
                         'job_begin', 'job_progress', 'job_cleanup', 'job_outcome',
                         'job_matrix_complete'} if jobs else {'server_query', 'application_chain'})
    require(all(r['event'] in allowed for r in records), 'unexpected or failure event')
    require(sum(r['event'] == 'resources' for r in records) == 1, 'duplicate terminal record')
    end = records[-1]
    require(end.get('event') == 'resources' and end.get('complete') is True
            and type(end.get('query_failures')) is int and end['query_failures'] == 0, 'missing successful terminal record')
    require(not any(r['event'] in ('job_failure', 'matrix_failure') for r in records),
            'job failure in output')
    expected_shards = fixture_shards(manifest)
    verified = [r for r in records if r['event'] == 'all_kv_verified']
    require(len(verified) == len(expected_shards), 'missing or duplicate verified shard')
    actual_shards = {}
    for row in verified:
        shard, count = row.get('shard'), row.get('rows')
        require(type(shard) is int and shard not in actual_shards and type(count) is int,
                'invalid or duplicate verified shard')
        actual_shards[shard] = count
    require(actual_shards == expected_shards, 'incomplete per-shard fixture verification')
    # Fixture verification must precede every query, not merely occur somewhere.
    verified_so_far = set()
    for row in records:
        if row['event'] == 'all_kv_verified':
            verified_so_far.add(row['shard'])
        elif row['event'] in {'server_query', 'application_chain', 'job_begin',
                               'job_progress', 'job_cleanup', 'job_outcome'}:
            require(verified_so_far == set(expected_shards), 'query precedes fixture verification')
    if not jobs:
        queries = manifest.get('queries')
        require(isinstance(queries, list) and bool(queries)
                and all(isinstance(c, dict) and isinstance(c.get('name'), str) for c in queries),
                'invalid query manifest')
        cases = {c['name']: c for c in queries}
        require(len(cases) == len(queries), 'duplicate manifest query')
        actual = [r for r in records if r['event'] == 'server_query']
        require(collections.Counter(r['name'] for r in actual) == collections.Counter(cases.keys()),
                'missing or duplicated representative query')
        for r in actual:
            c = cases[r['name']]
            supported = c.get('server_supported') is True
            require(len(r['samples']) == (7 if supported else 1), 'wrong query sample count')
            for sample in r['samples']:
                if supported:
                    require(sample.get('correct') is True, 'representative oracle failed')
                else:
                    require(sample.get('expected_rejection') == c.get('server_expected_error', 'Unsupported'),
                            'wrong query rejection')
        chains = [r for r in records if r['event'] == 'application_chain']
        require(len(chains) == 2 and all(r.get('correct') is True for r in chains),
                'missing, duplicate or failed hierarchy chain checks')
        return {'queries': len(actual), 'resources': end,
                'latency': {r['name']: r['latency'] for r in actual}}
    people, departments = manifest['people'], manifest['departments']
    def level(total, fanout, depth):
        first, width = 1, 1
        for _ in range(depth):
            first += width
            width *= fanout
        return max(0, min(width, total - first + 1))
    expected = {'ceo_emit32': people - 1, 'ceo_exact6': level(people, 8, 6),
                'ceo_exact32': 0, 'duplicate_ceo_emit32': 2 * (people - 1),
                'department_emit32': departments - 1,
                'department_exact6': level(departments, 5, 6),
                'ceo_emit32_dedup': people - 1,
                'duplicate_ceo_emit32_dedup': people - 1,
                'department_emit32_dedup': departments - 1}
    outcomes = [r for r in records if r['event'] == 'job_outcome']
    matrix = [r for r in outcomes if r['phase'] == 'matrix']
    require(collections.Counter((r['name'], r['sample']) for r in matrix)
            == collections.Counter((name, sample) for name in expected for sample in range(3)),
            'missing or duplicate hierarchy samples')
    probes = [r for r in outcomes if r['phase'] == 'probe']
    require(collections.Counter(r['name'] for r in probes)
            == collections.Counter(['department_emit32', 'ceo_emit32_dedup'])
            and all(r['sample'] == 0 for r in probes) and len(outcomes) == 29, 'missing probes')
    for r in outcomes:
        require(type(r.get('count')) is int and type(r.get('sample')) is int, 'invalid job integer')
        require(r.get('correct') is True and r.get('count') == expected[r['name']]
                and r.get('replay_verified') is True and r.get('partial_count_returned') is False,
                'wrong hierarchy oracle or replay')
    groups = collections.defaultdict(list)
    for r in records:
        if r['event'] in ('job_begin', 'job_progress', 'job_cleanup', 'job_outcome'):
            groups[(r['name'], r['phase'], r['sample'])].append(r)
    require(len(groups) == len(outcomes), 'incomplete extra job')
    active = None
    stage = None
    for row in records:
        event = row['event']
        if event not in {'job_begin', 'job_progress', 'job_cleanup', 'job_outcome'}:
            require(active is None, 'non-job event interrupts active job')
            continue
        key = (row['name'], row['phase'], row['sample'])
        require(type(row['sample']) is int, 'invalid job sample')
        if event == 'job_begin':
            require(active is None, 'overlapping job begin')
            active, stage = key, 'begin'
        else:
            require(active == key, 'job event without matching begin')
            if event == 'job_progress':
                require(stage in ('begin', 'progress'), 'progress after terminal or cleanup')
                require(type(row.get('complete')) is bool and type(row.get('sequence')) is int,
                        'invalid progress types')
                require(type(row.get('count')) is int if row['complete'] else row.get('count') is None,
                        'invalid progress count')
                stage = 'terminal' if row['complete'] else 'progress'
            elif event == 'job_cleanup':
                require(stage == 'terminal', 'cleanup before terminal progress')
                stage = 'cleanup'
            else:
                require(stage == 'cleanup', 'outcome before cleanup')
                active, stage = None, None
    require(active is None, 'unfinished active job')
    for rows in groups.values():
        require(sum(r['event'] == 'job_begin' for r in rows) == 1
                and sum(r['event'] == 'job_outcome' for r in rows) == 1, 'missing job boundaries')
        progress = [r for r in rows if r['event'] == 'job_progress']
        require(bool(progress), 'missing job progress')
        for record in progress:
            validate_snapshots(record.get('snapshots'), expected_shards)
        require([r['sequence'] for r in progress] == list(range(len(progress))), 'sequence gap')
        require(all(r['snapshots'] == progress[0]['snapshots'] for r in progress), 'snapshot changed')
        require(all(r['count'] is None and not r['complete'] for r in progress[:-1]), 'partial count')
        outcome = next(r for r in rows if r['event'] == 'job_outcome')
        require(progress[-1]['complete'] is True and progress[-1]['count'] == outcome['count'], 'terminal mismatch')
        validate_snapshots(outcome.get('fixed_snapshots'), expected_shards)
        require(outcome['fixed_snapshots'] == progress[0]['snapshots'], 'outcome snapshot changed')
        cleanup = [r for r in rows if r['event'] == 'job_cleanup']
        require(len(cleanup) == 1 and cleanup[0]['cancelled'] is True and cleanup[0]['advance_unavailable'] is True,
                'cleanup failed')
    guards = [r for r in records if r['event'] == 'synchronous_guard']
    require(len(guards) == 2 and all(r.get('expected_rejection') == 'ResourceExhausted' for r in guards),
            'missing, duplicate or failed synchronous guards')
    cancellations = [r for r in records if r['event'] == 'job_cancel_probe']
    require(len(cancellations) == 1 and cancellations[0].get('correct') is True
            and cancellations[0].get('partial_count_returned') is False
            and cancellations[0].get('advance_unavailable') is True, 'missing active cancellation')
    validate_snapshots(cancellations[0].get('fixed_snapshots'), expected_shards)
    completions = [r for r in records if r['event'] == 'job_matrix_complete']
    require(len(completions) == 1 and completions[0].get('complete') is True
            and records[-2] is completions[0],
            'missing, duplicate or failed matrix completion')
    return {'resources': end, 'cases': {name: {
        'count': expected[name],
        'seconds': [r['seconds'] for r in matrix if r['name'] == name],
        'median_seconds': statistics.median(r['seconds'] for r in matrix if r['name'] == name),
        'requests': [r['requests'] for r in matrix if r['name'] == name],
        'rpc_latency': [r['rpc_latency'] for r in matrix if r['name'] == name],
    } for name in expected}}


def main():
    out = pathlib.Path(sys.argv[1] if len(sys.argv) > 1 else 'corehr-ci-results').resolve()
    out.mkdir()  # Never overwrite a previous run.
    target = pathlib.Path(os.environ.get('CARGO_TARGET_DIR', 'target')).resolve() / 'release'
    binaries = {name: target / suffix for name, suffix in {
        'fixture': 'examples/corehr_fixture', 'queries': 'examples/corehr_run',
        'jobs': 'examples/corehr_jobs_run', 'results': 'examples/corehr_result_pages', 'node': 'graph-node'}.items()}
    metadata = {'revision': subprocess.check_output(['git', 'rev-parse', 'HEAD'], text=True).strip(),
                'platform': platform.platform(), 'cpu_count': os.cpu_count(),
                'github_event_name': os.environ.get('GITHUB_EVENT_NAME'),
                'github_run_id': os.environ.get('GITHUB_RUN_ID'),
                'rustc': subprocess.check_output(['rustc', '--version'], text=True).strip(),
                'profile': 'release fixture, harnesses and node',
                'tokio_worker_threads': os.environ.get('TOKIO_WORKER_THREADS', '2'),
                'binaries_sha256': {k: hashlib.file_digest(p.open('rb'), 'sha256').hexdigest()
                                    for k, p in binaries.items()}, 'executions': []}
    for name, path in [('cpu', '/proc/cpuinfo'), ('memory', '/proc/meminfo')]:
        if pathlib.Path(path).exists():
            metadata[name] = pathlib.Path(path).read_text()
    def save():
        (out / 'execution.json').write_text(json.dumps(metadata, indent=2) + '\n')
    save()
    def run(name, command, jobs=False):
        start = time.monotonic()
        env = {**os.environ, 'TOKIO_WORKER_THREADS': metadata['tokio_worker_threads']}
        env.pop('COREHR_JOBS_MATRIX', None)
        if jobs:
            env['COREHR_JOBS_MATRIX'] = '1'
        with (out / f'{name}.jsonl').open('w') as stdout, (out / f'{name}.stderr').open('w') as stderr:
            result = subprocess.run([str(s) for s in command], stdout=stdout, stderr=stderr, env=env)
        metadata['executions'].append({'name': name, 'exit_code': result.returncode,
                                       'wall_seconds': time.monotonic() - start})
        save()
        require(result.returncode == 0, f'{name} exited {result.returncode}; inspect retained evidence')
    results = {}
    for size, people, departments in [('smoke', 2000, 50), ('full', 200000, 5000)]:
        fixture = out / f'{size}-fixture'
        run(f'{size}-fixture', [binaries['fixture'], fixture, people, departments])
        manifest = json.loads((fixture / 'manifest.json').read_text())
        require((manifest['people'], manifest['departments']) == (people, departments), 'wrong fixture size')
        require(manifest['vertices'] == people + departments
                and manifest['multi_department_people'] == people // 5
                and manifest['three_department_people'] == people // 20
                and manifest['cross_partition_edges'] > 0, 'wrong fixture relationships')
        for kind in ('queries', 'jobs', 'results'):
            name = f'{size}-{kind}'
            run(name, [binaries[kind], fixture, out / f'{name}-db', binaries['node']], kind == 'jobs')
            records = read_records(out / f'{name}.jsonl')
            results[name] = validate_results(records, manifest) if kind == 'results' else validate(records, manifest, kind == 'jobs')
    (out / 'summary.json').write_text(json.dumps(results, indent=2) + '\n')
    lines = ['# CoreHR correctness and measured performance', '',
             f"Revision: {metadata['revision']}", '',
             'Release fixture/harness/node; two Tokio workers; real loopback processes.',
             'Full fixture: 200,000 people, 5,000 departments, three data shards.',
             'All fixture KV records, representative queries, hierarchy counts, and ordered vertex result pages passed.', '',
             'Timing samples are observations, not performance SLAs or production p95 estimates.', '',
             '| Full hierarchy case | Count | Median seconds |', '| --- | ---: | ---: |']
    for name, row in results['full-jobs']['cases'].items():
        lines.append(f"| {name} | {row['count']} | {row['median_seconds']:.3f} |")
    lines += ['', 'See summary.json for all query latency samples, request counts, process RSS/high-water RSS and database bytes.',
              'Snapshots are Unverified and not globally atomic; each shard has one replica.']
    (out / 'summary.md').write_text('\n'.join(lines) + '\n')


if __name__ == '__main__':
    main()
