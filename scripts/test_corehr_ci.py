import copy
import unittest
from corehr_ci import validate


class EvidenceTests(unittest.TestCase):
    def setUp(self):
        self.manifest = {'shards': [{'partition': i, 'kv_rows': i} for i in range(1, 4)], 'queries': [
            {'name': 'membership', 'server_supported': True},
            {'name': 'unsupported', 'server_expected_error': 'Unsupported'}]}
        self.records = [{'event': 'all_kv_verified', 'shard': i, 'rows': i} for i in range(1, 4)]
        self.records += [
            {'event': 'server_query', 'name': 'membership', 'latency': {},
             'samples': [{'correct': True}] * 7},
            {'event': 'server_query', 'name': 'unsupported', 'latency': {},
             'samples': [{'expected_rejection': 'Unsupported'}]},
            {'event': 'application_chain', 'correct': True},
            {'event': 'application_chain', 'correct': True},
            {'event': 'resources', 'complete': True, 'query_failures': 0}]

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
        rows = self.records[:3]
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
        rows = copy.deepcopy(records)
        rows[4], rows[8] = rows[8], rows[4]
        with self.assertRaises(ValueError):
            validate(rows, self.manifest, jobs=True)
        rows = records[:4] + [{'event': 'load'}] + records[4:]
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


if __name__ == '__main__':
    unittest.main()
