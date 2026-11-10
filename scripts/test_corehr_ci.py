import copy
import unittest
from corehr_ci import validate, validate_client_phases


def client_evidence(cases):
    rows = []
    for phase in ('legacy', 'repeat_explicit', 'repeat_global'):
        group = [c for c in cases if c.get('client_phase', 'legacy') == phase]
        if not group and phase != 'legacy':
            continue
        rows.append({'event': 'client_session', 'phase': phase, 'planned_cases': len(group),
                     'error': None, 'error_category': None, 'skip_reason': None})
        for case in group:
            error = 'client_expected' not in case and (
                case.get('expect_error') is True or case.get('client_expect_unsupported') is True
                or 'client_expected_error' in case)
            result = {'expected_rejection': 'typed rejection', 'error_category': case.get(
                'client_expected_error', 'Unsupported')} if error else {'correct': True}
            rows.append({'event': 'client_query', 'phase': phase, 'name': case['name'],
                         'query': case['query'], 'result': result})
    return rows


class EvidenceTests(unittest.TestCase):
    def setUp(self):
        self.manifest = {'shards': [{'partition': i, 'kv_rows': i} for i in range(1, 4)], 'queries': [
            {'name': 'membership', 'query': 'g.V(1)', 'server_supported': True},
            {'name': 'unsupported', 'query': 'g.V(1).unsupported()',
             'client_expect_unsupported': True, 'server_expected_error': 'Unsupported'}]}
        self.records = [{'event': 'all_kv_verified', 'shard': i, 'rows': i} for i in range(1, 4)]
        self.records += [
            {'event': 'server_query', 'name': 'membership', 'latency': {},
             'samples': [{'correct': True}] * 7},
            {'event': 'server_query', 'name': 'unsupported', 'latency': {},
             'samples': [{'expected_rejection': 'Unsupported'}]},
            {'event': 'application_chain', 'correct': True},
            {'event': 'application_chain', 'correct': True},
            {'event': 'resources', 'complete': True, 'query_failures': 0}]
        self.records[-1:-1] = client_evidence(self.manifest['queries'])

    def test_complete(self):
        self.assertEqual(validate(self.records, self.manifest)['queries'], 2)

    def test_reject_missing_or_duplicate_query(self):
        for rows in [self.records[:3] + self.records[4:], self.records[:3] + [self.records[3]] + self.records[3:]]:
            with self.assertRaises(ValueError):
                validate(rows, self.manifest)

    def test_reject_false_success(self):
        for field, value in [('complete', False), ('query_failures', 1), ('event', 'job_progress')]:
            rows = copy.deepcopy(self.records)
            rows[-1][field] = value
            with self.assertRaises(ValueError):
                validate(rows, self.manifest)

    def test_reject_partial_and_wrong_oracle(self):
        rows = copy.deepcopy(self.records)
        rows[3]['samples'][0] = {'correct': False}
        with self.assertRaises(ValueError):
            validate(rows, self.manifest)
        with self.assertRaises(ValueError):
            validate(self.records[:-1], self.manifest)
        with self.assertRaises(ValueError):
            validate(self.records[1:], self.manifest)

    def test_reject_absent_jobs_even_with_terminal_success(self):
        self.manifest.update(people=200000, departments=5000)
        with self.assertRaises(ValueError):
            validate(self.records, self.manifest, jobs=True)


    def job_records(self):
        self.manifest.update(people=200000, departments=5000)
        counts = {'ceo_emit32': 199999, 'ceo_exact6': 162551, 'ceo_exact32': 0,
                  'duplicate_ceo_emit32': 399998, 'department_emit32': 4999,
                  'department_exact6': 1094, 'ceo_emit32_dedup': 199999,
                  'duplicate_ceo_emit32_dedup': 199999, 'department_emit32_dedup': 4999}
        stamps = [{'shard': i, 'epoch': 1, 'sequence': 2, 'identity': {'group': i, 'node': 1}} for i in range(1, 4)]
        rows = self.records[:3] + [{'event': 'client_session', 'phase': 'legacy',
                                   'planned_cases': 0, 'error': None, 'error_category': None, 'skip_reason': 'jobs_matrix'}]
        for name, phase, sample in ([(name, 'matrix', sample) for name in counts for sample in range(3)]
                                    + [(name, 'probe', 0) for name in ['department_emit32', 'ceo_emit32_dedup']]):
            key = {'name': name, 'phase': phase, 'sample': sample}
            rows += [{**key, 'event': 'job_begin'},
                     {**key, 'event': 'job_progress', 'sequence': 0, 'snapshots': copy.deepcopy(stamps), 'count': None, 'complete': False},
                     {**key, 'event': 'job_progress', 'sequence': 1, 'snapshots': copy.deepcopy(stamps), 'count': counts[name], 'complete': True},
                     {**key, 'event': 'job_cleanup', 'cancelled': True, 'advance_unavailable': True},
                     {**key, 'event': 'job_outcome', 'correct': True, 'count': counts[name],
                      'replay_verified': True, 'partial_count_returned': False, 'seconds': 1.0,
                      'requests': 2, 'rpc_latency': {}, 'fixed_snapshots': copy.deepcopy(stamps)}]
        return rows + [{'event': 'synchronous_guard', 'expected_rejection': 'ResourceExhausted'}] * 2 + [
            {'event': 'job_cancel_probe', 'correct': True, 'partial_count_returned': False, 'advance_unavailable': True, 'fixed_snapshots': copy.deepcopy(stamps)},
            {'event': 'job_matrix_complete', 'complete': True}, self.records[-1]]

    def test_complete_jobs(self):
        records = self.job_records()
        self.assertEqual(validate(records, self.manifest, jobs=True)['cases']['ceo_emit32']['count'], 199999)

    def test_reject_job_corruption(self):
        records = self.job_records()
        for event, field, value in [('job_outcome', 'count', 199998),
                                    ('job_progress', 'sequence', 99),
                                    ('job_progress', 'count', 1),
                                    ('job_cleanup', 'cancelled', False),
                                    ('job_outcome', 'replay_verified', False)]:
            rows = copy.deepcopy(records)
            next(r for r in rows if r['event'] == event)[field] = value
            with self.subTest(event=event, field=field), self.assertRaises(ValueError):
                validate(rows, self.manifest, jobs=True)

    def test_reject_missing_job_sample(self):
        rows = [r for r in self.job_records() if not
                (r.get('name') == 'ceo_emit32' and r.get('sample') == 2)]
        with self.assertRaises(ValueError):
            validate(rows, self.manifest, jobs=True)


    def test_reject_shard_redistribution_or_wrong_identity(self):
        for mutate in (lambda r: r[0].update(shard=4),
                       lambda r: (r[0].update(rows=2), r[1].update(rows=1)),
                       lambda r: r[0].update(shard=True),
                       lambda r: r[0].update(rows=True)):
            rows = copy.deepcopy(self.records)
            mutate(rows)
            with self.assertRaises(ValueError):
                validate(rows, self.manifest)

    def test_reject_bad_manifest(self):
        for shards in ([{'partition': 1, 'kv_rows': 1}] * 3,
                       [{'partition': i, 'kv_rows': 1} for i in (1, 2, 4)],
                       [{'partition': i, 'kv_rows': True} for i in (1, 2, 3)]):
            manifest = copy.deepcopy(self.manifest)
            manifest['shards'] = shards
            with self.assertRaises(ValueError):
                validate(self.records, manifest)

    def test_reject_malformed_snapshot_vectors(self):
        valid = self.job_records()
        mutations = [lambda s: s.clear(), lambda s: s.__setitem__(2, copy.deepcopy(s[0])),
                     lambda s: s[0].update(shard=4), lambda s: s[0].update(epoch=0),
                     lambda s: s[0].update(epoch=True), lambda s: s[0].update(sequence=-1),
                     lambda s: s[0].update(sequence=True), lambda s: s[0].update(identity={}),
                     lambda s: s[0]['identity'].update(group=2),
                     lambda s: s[0]['identity'].update(node=0)]
        for mutate in mutations:
            rows = copy.deepcopy(valid)
            # Mutate every vector consistently: equality alone must not pass.
            for row in rows:
                for key in ('snapshots', 'fixed_snapshots'):
                    if key in row:
                        mutate(row[key])
            with self.assertRaises(ValueError):
                validate(rows, self.manifest, jobs=True)

    def test_reject_changed_outcome_stamp(self):
        rows = self.job_records()
        next(r for r in rows if r['event'] == 'job_outcome')['fixed_snapshots'][0]['sequence'] += 1
        with self.assertRaises(ValueError):
            validate(rows, self.manifest, jobs=True)

    def test_reject_old_seven_case_harness(self):
        rows = [r for r in self.job_records() if r.get('name') not in
                ('duplicate_ceo_emit32_dedup', 'department_emit32_dedup')]
        with self.assertRaisesRegex(ValueError, 'missing or duplicate hierarchy samples'):
            validate(rows, self.manifest, jobs=True)

    def test_reject_unknown_failure_and_duplicate_terminal_events(self):
        for jobs in (False, True):
            records = self.job_records() if jobs else self.records
            for extra in ({'event': 'unexpected_failure'}, {'event': 'matrix_failure'}, records[-1]):
                rows = records[:-1] + [extra] + records[-1:]
                with self.subTest(jobs=jobs, event=extra['event']), self.assertRaises(ValueError):
                    validate(rows, self.manifest, jobs=jobs)

    def test_reject_wrong_terminal_scalar_type(self):
        for value in (False, 0.0, '0', None):
            rows = copy.deepcopy(self.records)
            rows[-1]['query_failures'] = value
            with self.subTest(value=value), self.assertRaises(ValueError):
                validate(rows, self.manifest)

    def test_reject_duplicate_manifest_case(self):
        self.manifest['queries'].append(copy.deepcopy(self.manifest['queries'][0]))
        with self.assertRaises(ValueError):
            validate(self.records, self.manifest)

    def test_reject_query_before_fixture_verification(self):
        for jobs in (False, True):
            records = self.job_records() if jobs else self.records
            rows = records[3:4] + records[:3] + records[4:]
            with self.subTest(jobs=jobs), self.assertRaises(ValueError):
                validate(rows, self.manifest, jobs=jobs)

    def test_reject_lifecycle_permutations_for_every_sample(self):
        records = self.job_records()
        begins = [i for i, row in enumerate(records) if row['event'] == 'job_begin']
        # Every one of the 27 matrix samples and both probes must follow:
        # begin -> nonterminal progress -> terminal progress -> cleanup -> outcome.
        for start in begins:
            for left, right in ((0, 1), (0, 4), (2, 3), (3, 4)):
                rows = copy.deepcopy(records)
                rows[start + left], rows[start + right] = rows[start + right], rows[start + left]
                with self.subTest(start=start, swap=(left, right)), self.assertRaises(ValueError):
                    validate(rows, self.manifest, jobs=True)

    def test_reject_overlapping_or_interrupted_jobs(self):
        records = self.job_records()
        start = next(i for i, row in enumerate(records) if row['event'] == 'job_begin')
        rows = copy.deepcopy(records)
        rows[start + 1], rows[start + 5] = rows[start + 5], rows[start + 1]
        with self.assertRaises(ValueError):
            validate(rows, self.manifest, jobs=True)
        rows = records[:start + 1] + [{'event': 'load'}] + records[start + 1:]
        with self.assertRaises(ValueError):
            validate(rows, self.manifest, jobs=True)

    def test_reject_truthy_cleanup_and_numeric_booleans(self):
        records = self.job_records()
        mutations = [('job_cleanup', 'cancelled', 1),
                     ('job_cleanup', 'advance_unavailable', 'true'),
                     ('job_progress', 'complete', 0),
                     ('job_progress', 'sequence', False),
                     ('job_outcome', 'count', 199999.0)]
        for event, field, value in mutations:
            rows = copy.deepcopy(records)
            next(r for r in rows if r['event'] == event)[field] = value
            with self.subTest(event=event, field=field), self.assertRaises(ValueError):
                validate(rows, self.manifest, jobs=True)
        rows = copy.deepcopy(records)
        next(r for r in rows if r['event'] == 'job_progress' and r['complete'])['count'] = 199999.0
        with self.assertRaises(ValueError):
            validate(rows, self.manifest, jobs=True)
        # Mutate every record of sample zero together so grouping still agrees.
        rows = copy.deepcopy(records)
        for row in rows:
            if row.get('sample') == 0:
                row['sample'] = False
        with self.assertRaises(ValueError):
            validate(rows, self.manifest, jobs=True)

    def test_reject_malformed_evidence_records(self):
        for bad in (None, 3, [], {}, {'event': 1}):
            with self.subTest(record=bad), self.assertRaises(ValueError):
                validate([bad] + self.records, self.manifest)

    def test_reject_extra_success_or_failure_attestations(self):
        for jobs, event, key, good, bad in (
                (False, 'application_chain', 'correct', True, False),
                (True, 'synchronous_guard', 'expected_rejection', 'ResourceExhausted', 'WRONG'),
                (True, 'job_matrix_complete', 'complete', True, False)):
            records = self.job_records() if jobs else self.records
            for value in (good, bad, None, 1):
                rows = records[:-1] + [{'event': event, key: value}] + records[-1:]
                with self.subTest(event=event, extra=value), self.assertRaises(ValueError):
                    validate(rows, self.manifest, jobs=jobs)
            rows = copy.deepcopy(records)
            next(r for r in rows if r['event'] == event)[key] = bad
            with self.subTest(event=event, replacement=bad), self.assertRaises(ValueError):
                validate(rows, self.manifest, jobs=jobs)

    def test_reject_premature_matrix_completion(self):
        records = self.job_records()
        completion = next(r for r in records if r['event'] == 'job_matrix_complete')
        remaining = [r for r in records if r is not completion]
        for index in (0, 3, 8, len(remaining) - 2):
            rows = remaining[:index] + [completion] + remaining[index:]
            with self.subTest(index=index), self.assertRaises(ValueError):
                validate(rows, self.manifest, jobs=True)


class ClientPhaseTests(unittest.TestCase):
    def setUp(self):
        self.manifest = {'shards': [{'partition': i, 'kv_rows': i, 'raw_bytes': 100}
                                    for i in range(1, 4)], 'queries': [
            {'name': 'old', 'query': 'g.V(1)'},
            {'name': 'broad', 'query': 'g.V(1).repeat(__.in()).emit().times(32)',
             'client_phase': 'repeat_explicit', 'expect_error': True,
             'client_expected': {'count': 1999}},
            {'name': 'too_deep', 'query': 'g.V(1).out().repeat(__.out()).times(32)',
             'expect_error': True, 'client_expected_error': 'ResourceExhausted'},
            {'name': 'global', 'query': "g.V().has('id',1).repeat(__.out()).times(2)",
             'client_phase': 'repeat_global'}]}
        self.rows = client_evidence(self.manifest['queries'])

    def test_fixed_phases_and_client_override(self):
        validate_client_phases(self.rows, self.manifest, False)
        self.assertEqual([r['name'] for r in self.rows if r['event'] == 'client_query'],
                         ['old', 'too_deep', 'broad', 'global'])

    def test_every_missing_duplicate_or_reordered_client_event_rejects(self):
        for i in range(len(self.rows)):
            for rows in (self.rows[:i] + self.rows[i + 1:],
                         self.rows[:i] + [self.rows[i]] + self.rows[i:]):
                with self.subTest(index=i), self.assertRaises(ValueError):
                    validate_client_phases(rows, self.manifest, False)
        for i in range(len(self.rows) - 1):
            rows = copy.deepcopy(self.rows)
            rows[i], rows[i + 1] = rows[i + 1], rows[i]
            with self.subTest(swap=i), self.assertRaises(ValueError):
                validate_client_phases(rows, self.manifest, False)

    def test_wrong_phase_count_query_or_oracle_rejects(self):
        mutations = [
            (0, 'phase', 'repeat_explicit'), (0, 'planned_cases', True),
            (0, 'planned_cases', 3), (0, 'skip_reason', 'jobs_matrix'),
            (1, 'phase', 'repeat_global'), (1, 'query', 'g.V(2)'),
            (1, 'result', {'correct': False}),
            (2, 'result', {'expected_rejection': 'typed', 'error_category': 'Unsupported'}),
            (2, 'result', {'correct': True}),
        ]
        for index, key, value in mutations:
            rows = copy.deepcopy(self.rows)
            rows[index][key] = value
            with self.subTest(key=key, value=value), self.assertRaises(ValueError):
                validate_client_phases(rows, self.manifest, False)

    def test_no_timeout_refresh_or_later_capacity_rejection(self):
        for i, row in enumerate(self.rows):
            if row['event'] != 'client_session':
                continue
            for error in ('Timeout', 'ResourceExhausted'):
                rows = copy.deepcopy(self.rows)
                rows[i]['error'] = error
                with self.subTest(index=i, error=error), self.assertRaises(ValueError):
                    validate_client_phases(rows, self.manifest, False)

    def test_large_fixture_capacity_failure_occurs_once_before_queries(self):
        manifest = copy.deepcopy(self.manifest)
        manifest['shards'][0]['kv_rows'] = 200_001
        first = {**self.rows[0], 'error': 'Route(Storage(ResourceExhausted))',
                 'error_category': 'ResourceExhausted'}
        validate_client_phases([first], manifest, False)
        with self.assertRaises(ValueError):
            validate_client_phases([first], self.manifest, False)
        for suffix in ([first], self.rows[1:]):
            with self.assertRaises(ValueError):
                validate_client_phases([first] + suffix, manifest, False)

    def test_capacity_evidence_requires_typed_category_and_integer_bytes(self):
        manifest = copy.deepcopy(self.manifest)
        manifest['shards'][0]['raw_bytes'] = 16 * 1024 * 1024 + 1
        first = {**self.rows[0], 'error': 'Route(Storage(ResourceExhausted))',
                 'error_category': 'ResourceExhausted'}
        validate_client_phases([first], manifest, False)
        for category in (None, 'Timeout', 'Unexpected'):
            with self.assertRaises(ValueError):
                validate_client_phases([{**first, 'error': 'Timeout("ResourceExhausted")',
                                         'error_category': category}], manifest, False)
        for value in (True, -1, 1.5, '16777217', None):
            bad = copy.deepcopy(manifest)
            bad['shards'][1]['raw_bytes'] = value
            with self.subTest(value=value), self.assertRaises(ValueError):
                validate_client_phases([first], bad, False)
        for field in ('error', 'error_category'):
            missing = dict(first)
            del missing[field]
            with self.assertRaises(ValueError):
                validate_client_phases([missing], manifest, False)
        rows = copy.deepcopy(self.rows)
        rows[0]['error_category'] = 'ResourceExhausted'
        with self.assertRaises(ValueError):
            validate_client_phases(rows, self.manifest, False)

    def test_matrix_admission_has_explicit_zero_query_scope(self):
        rows = [{'event': 'client_session', 'phase': 'legacy', 'planned_cases': 0,
                 'error': None, 'error_category': None, 'skip_reason': 'jobs_matrix'}]
        validate_client_phases(rows, self.manifest, True)
        for changes in ({'skip_reason': None}, {'planned_cases': 1}, {'phase': 'repeat_explicit'}):
            with self.assertRaises(ValueError):
                validate_client_phases([{**rows[0], **changes}], self.manifest, True)
        with self.assertRaises(ValueError):
            validate_client_phases(rows + [self.rows[1]], self.manifest, True)

    def test_manifest_phase_or_duplicate_case_rejects(self):
        for phase in (None, 'dynamic_retry', 1):
            manifest = copy.deepcopy(self.manifest)
            manifest['queries'][0]['client_phase'] = phase
            with self.assertRaises(ValueError):
                validate_client_phases(self.rows, manifest, False)
        manifest = copy.deepcopy(self.manifest)
        manifest['queries'].append(manifest['queries'][0])
        with self.assertRaises(ValueError):
            validate_client_phases(self.rows, manifest, False)


if __name__ == '__main__':
    unittest.main()
