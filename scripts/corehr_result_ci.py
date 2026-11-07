#!/usr/bin/env python3
"""Fail-closed evidence checks for corehr_result_pages; timing is not an SLA.

The Rust runner compares actual ordered rows and replays pages. This validator
checks its attestations and complete case coverage, not raw rows absent from logs.
"""
import math


MATRIX_ORACLE = 'input relationship list; every qualified identity in ordered BFS'
ROW_ORACLE = 'input relationship BFS; not count/progress'


def require(condition, message):
    if not condition:
        raise ValueError(message)


def integer(value, minimum=0):
    return type(value) is int and value >= minimum


def number(value):
    return type(value) in (int, float) and math.isfinite(value) and value >= 0


def subtree(total, root, fanout, depth, emit):
    """Independent arithmetic oracle for numbered breadth-first fixture trees."""
    lo = hi = root
    count = 0
    width = 0
    for _ in range(depth):
        lo, hi = fanout * (lo - 1) + 2, min(total, fanout * (hi - 1) + fanout + 1)
        width = max(0, hi - lo + 1)
        count += width
        if not width:
            break
    return count if emit else width


def expected_cases(people, departments):
    manager = subtree(people, 7, 8, 32, True)
    # name, roots, label, depth, emit, dedup, page size, ordered row count
    rows = [
        ('ceo_all', [1], 'reports_to', 32, True, False, 1000, people - 1),
        ('duplicate_manager', [7, 7], 'reports_to', 32, True, False, 1000, 2 * manager),
        ('duplicate_manager_dedup', [7, 7], 'reports_to', 32, True, True, 997, manager),
        ('ceo_exact6', [1], 'reports_to', 6, False, False, 1000, subtree(people, 1, 8, 6, False)),
        ('ceo_exact32_empty', [1], 'reports_to', 32, False, False, 1, 0),
        ('direct_reports_page1', [1], 'reports_to', 1, True, False, 1, 8),
        ('ordered_roots', [7, 1, 7], 'reports_to', 2, True, False, 7,
         2 * subtree(people, 7, 8, 2, True) + subtree(people, 1, 8, 2, True)),
        ('departments', [people + 1], 'parent_department', 32, True, False, 137, departments - 1),
        ('department_exact6', [people + 1], 'parent_department', 6, False, False, 137,
         subtree(departments, 1, 5, 6, False)),
        ('duplicate_ceo_rejected', [1, 1], 'reports_to', 32, True, False, 1000, 2 * (people - 1)),
        ('ceo_all_dedup', [1], 'reports_to', 32, True, True, 1000, people - 1),
    ]
    result = {}
    for name, roots, label, depth, emit, dedup, size, count in rows:
        query = "g.V({}).repeat(__.in('{}')){}.times({}){}".format(
            ','.join(map(str, roots)), label, '.emit()' if emit else '', depth,
            '.dedup()' if dedup else '')
        result[name] = {'query': query, 'page_size': size, 'count': count}
    return result


def snapshots(value):
    require(isinstance(value, list) and len(value) == 3, 'missing fixed snapshots')
    found = {}
    for stamp in value:
        require(isinstance(stamp, dict), 'invalid snapshot')
        shard = stamp.get('shard')
        identity = stamp.get('identity')
        require(integer(shard, 1) and shard in (1, 2, 3) and shard not in found,
                'invalid or duplicate snapshot shard')
        require(type(stamp.get('epoch')) is int and stamp['epoch'] == 1
                and integer(stamp.get('sequence'), 1), 'invalid snapshot epoch/sequence')
        require(isinstance(identity, dict)
                and type(identity.get('group')) is int and identity['group'] == shard
                and type(identity.get('node')) is int and identity['node'] == 1,
                'invalid snapshot identity')
        found[shard] = stamp
    return found


def validate(records, manifest):
    """Return verified coverage/measurements, or raise ValueError on bad evidence."""
    require(isinstance(manifest, dict), 'invalid manifest')
    people, departments = manifest.get('people'), manifest.get('departments')
    require(type(people) is int and type(departments) is int
            and (people, departments) in ((200000, 5000), (2000, 50)),
            'rows gate requires full200k/5000 or smoke2000/50 fixture')
    full = people == 200000
    shards = manifest.get('shards')
    require(isinstance(shards, list) and len(shards) == 3, 'invalid fixture shards')
    expected_shards = {}
    for shard in shards:
        require(isinstance(shard, dict), 'invalid fixture shard')
        partition, count = shard.get('partition'), shard.get('kv_rows')
        require(integer(partition, 1) and partition in (1, 2, 3)
                and partition not in expected_shards and integer(count, 1), 'invalid fixture shard count')
        expected_shards[partition] = count
    require(isinstance(records, list) and bool(records), 'empty result evidence')
    require(all(isinstance(row, dict) and isinstance(row.get('event'), str) for row in records),
            'invalid evidence record')
    end = records[-1]
    require(end.get('event') == 'result_matrix_complete' and end.get('correct') is True,
            'missing successful terminal result')
    require(type(end.get('cases')) is int and end['cases'] == 11
            and type(end.get('people')) is int and end['people'] == people
            and type(end.get('departments')) is int and end['departments'] == departments
            and end.get('full_fixture') is full and end.get('oracle') == MATRIX_ORACLE,
            'incorrect terminal coverage/fixture/oracle')
    allowed = {'capacity_before', 'explicit_capacity_upgrade', 'loaded_shard', 'load',
               'capacity_after', 'all_kv_verified', 'result_begin', 'result_outcome',
               'result_rejection', 'result_matrix_complete'}
    require(all(row['event'] in allowed for row in records), 'unexpected or failure event')
    require(sum(row['event'] == 'result_matrix_complete' for row in records) == 1,
            'duplicate terminal result')
    cases = expected_cases(people, departments)
    verified, begun, finished = {}, set(), {}
    active, fixed = None, None
    for row in records[:-1]:
        event = row['event']
        if event == 'all_kv_verified':
            shard, count = row.get('shard'), row.get('rows')
            require(not begun and integer(shard, 1) and shard in expected_shards
                    and shard not in verified and integer(count, 1)
                    and count == expected_shards[shard], 'missing/wrong/duplicate verified shard')
            verified[shard] = count
        if event not in ('result_begin', 'result_outcome', 'result_rejection'):
            continue
        require(verified == expected_shards, 'results precede complete fixture verification')
        name = row.get('name')
        require(isinstance(name, str) and name in cases, 'unknown result case')
        case = cases[name]
        if event == 'result_begin':
            require(active is None and name not in begun, 'duplicate or overlapping case')
            require(row.get('query') == case['query']
                    and integer(row.get('page_size'), 1) and row['page_size'] == case['page_size']
                    and integer(row.get('expected_rows')) and row['expected_rows'] == case['count'],
                    'wrong begin query/page size/oracle count')
            begun.add(name)
            active = name
            continue
        require(active == name and name not in finished, 'outcome without matching unique begin')
        rejected = full and name == 'duplicate_ceo_rejected'
        if rejected:
            require(event == 'result_rejection' and row.get('expected_rejection') == 'ResourceExhausted'
                    and row.get('before_ready') is True and type(row.get('rows_exposed')) is int
                    and row['rows_exposed'] == 0 and integer(row.get('oracle_rows'))
                    and row['oracle_rows'] == case['count'], 'wrong or partial full duplicate rejection')
        else:
            require(event == 'result_outcome' and row.get('correct') is True,
                    'successful result required (including smoke duplicate CEO)')
            pages = max(1, (case['count'] + case['page_size'] - 1) // case['page_size'])
            require(integer(row.get('verified_ordered_identities'))
                    and row['verified_ordered_identities'] == case['count']
                    and integer(row.get('page_size'), 1) and row['page_size'] == case['page_size']
                    and integer(row.get('pages'), 1) and row['pages'] == pages,
                    'wrong ordered row count/page count/page size')
            require(row.get('old_and_arbitrary_page_replay') is True and row.get('oracle') == ROW_ORACLE,
                    'missing ordered-oracle/replay attestation')
            current = snapshots(row.get('fixed_snapshots'))
            require(fixed is None or current == fixed, 'fixed fixture snapshots changed')
            fixed = current
            require(integer(row.get('preparation_advances'), 1)
                    and row['preparation_advances'] <= 20000, 'invalid preparation advances')
            require(number(row.get('preparation_seconds')) and number(row.get('seconds'))
                    and row['preparation_seconds'] <= row['seconds'], 'invalid preparation duration')
            latency = row.get('read_latency')
            require(isinstance(latency, dict) and type(latency.get('n')) is int
                    and latency['n'] == pages, 'wrong read sample count')
            require(all(number(latency.get(key)) for key in ('min_ms', 'max_ms', 'p50_ms', 'p95_ms'))
                    and latency['min_ms'] <= latency['p50_ms'] <= latency['p95_ms'] <= latency['max_ms'],
                    'invalid read latency summary')
        require(number(row.get('seconds')), 'invalid outcome duration')
        finished[name] = row
        active = None
    require(active is None and begun == set(cases) and set(finished) == set(cases),
            'missing or truncated result cases')
    require(verified == expected_shards, 'incomplete fixture verification')
    return {'cases': finished, 'case_count': len(finished), 'full_fixture': full,
            'ceo_ordered_rows': finished['ceo_all']['verified_ordered_identities'],
            'ceo_dedup_ordered_rows': finished['ceo_all_dedup']['verified_ordered_identities'],
            'terminal': end}
