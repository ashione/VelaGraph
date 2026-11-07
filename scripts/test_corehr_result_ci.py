import copy
import unittest

from corehr_result_ci import MATRIX_ORACLE, ROW_ORACLE, expected_cases, validate


def evidence(full=True):
    people, departments = (200000, 5000) if full else (2000, 50)
    manifest = {'people': people, 'departments': departments,
                'shards': [{'partition': i, 'kv_rows': 100 * i} for i in (1, 2, 3)]}
    rows = [{'event': 'all_kv_verified', 'shard': i, 'rows': 100 * i} for i in (1, 2, 3)]
    stamps = [{'shard': i, 'epoch': 1, 'sequence': 5,
               'identity': {'group': i, 'node': 1}} for i in (1, 2, 3)]
    for name, case in expected_cases(people, departments).items():
        rows.append({'event': 'result_begin', 'name': name, 'query': case['query'],
                     'page_size': case['page_size'], 'expected_rows': case['count']})
        if full and name == 'duplicate_ceo_rejected':
            rows.append({'event': 'result_rejection', 'name': name, 'seconds': 2.0,
                         'expected_rejection': 'ResourceExhausted', 'before_ready': True,
                         'rows_exposed': 0, 'oracle_rows': case['count']})
            continue
        pages = max(1, (case['count'] + case['page_size'] - 1) // case['page_size'])
        rows.append({'event': 'result_outcome', 'name': name, 'correct': True,
                     'verified_ordered_identities': case['count'], 'pages': pages,
                     'projection_keys': case['projection_keys'],
                     'result_variant': 'ProjectedPage' if case['projection_keys'] is not None else 'Page',
                     'verified_property_maps': case['count'] if case['projection_keys'] is not None else 0,
                     'verified_property_entries': case['property_entries'],
                     'verified_missing_property_keys': case['missing_property_keys'],
                     'verified_empty_property_maps': case['empty_property_maps'],
                     'page_size': case['page_size'], 'preparation_advances': 2,
                     'preparation_seconds': 1.0, 'seconds': 2.0,
                     'fixed_snapshots': copy.deepcopy(stamps),
                     'old_and_arbitrary_page_replay': True, 'oracle': ROW_ORACLE,
                     'read_latency': {'n': pages, 'min_ms': 0.1, 'p50_ms': 0.2,
                                      'p95_ms': 0.3, 'max_ms': 0.4}})
    rows.append({'event': 'result_matrix_complete', 'correct': True, 'cases': 18,
                 'people': people, 'departments': departments, 'full_fixture': full,
                 'oracle': MATRIX_ORACLE})
    return rows, manifest


class ResultEvidenceTests(unittest.TestCase):
    def test_full_and_smoke(self):
        for full, count in ((True, 199999), (False, 1999)):
            rows, manifest = evidence(full)
            result = validate(rows, manifest)
            self.assertEqual(result['case_count'], 18)
            self.assertEqual(result['ceo_ordered_rows'], count)
            self.assertEqual(result['ceo_dedup_ordered_rows'], count)
            self.assertEqual(result['full_fixture'], full)
            self.assertEqual(result['projected_ceo_property_maps'], count)
            self.assertEqual(result['projected_ceo_dedup_property_maps'], count)

    def test_independent_expected_counts(self):
        cases = expected_cases(200000, 5000)
        self.assertEqual(cases['ceo_exact6']['count'], 162551)
        self.assertEqual(cases['department_exact6']['count'], 1094)
        self.assertEqual(cases['ordered_roots']['count'], 216)
        self.assertEqual(cases['duplicate_ceo_rejected']['count'], 399998)
        self.assertEqual(expected_cases(2000, 50)['ceo_exact6']['count'], 0)

    def test_projection_expected_shapes_and_counts(self):
        cases = expected_cases(200000, 5000)
        self.assertEqual(len(cases), 18)
        self.assertEqual(sum(c['projection_keys'] is None for c in cases.values()), 11)
        for name in ('projected_ceo_all', 'projected_ceo_all_dedup'):
            case = cases[name]
            self.assertEqual(case['count'], 199999)
            self.assertEqual(case['page_size'], 128)
            self.assertEqual(case['property_entries'], 599997)
            self.assertEqual(case['projection_keys'], ['name', 'active', 'country'])
            self.assertTrue(case['query'].endswith(".valueMap('name','active','country')"))
        self.assertIn(".dedup().valueMap(", cases['projected_ceo_all_dedup']['query'])
        self.assertEqual(cases['projected_duplicate_manager']['count'], cases['duplicate_manager']['count'])
        departments = cases['projected_departments_missing_property']
        self.assertEqual(departments['count'], 4999)
        self.assertEqual(departments['missing_property_keys'], 4999)
        self.assertEqual(departments['property_entries'], 14997)
        self.assertEqual(departments['empty_property_maps'], 0)
        missing = cases['projected_missing_key_empty_maps']
        self.assertEqual(missing['count'], 8)
        self.assertEqual(missing['missing_property_keys'], 8)
        self.assertEqual(missing['empty_property_maps'], 8)
        self.assertEqual(missing['property_entries'], 0)
        self.assertEqual(cases['projected_direct_reports_page1']['property_entries'], 32)
        self.assertEqual(cases['projected_empty']['count'], 0)

    def test_projection_attestations_fail_closed_for_every_case(self):
        rows, manifest = evidence()
        for name, case in expected_cases(200000, 5000).items():
            if case['projection_keys'] is None:
                continue
            original = next(r for r in rows if r.get('name') == name and r['event'] == 'result_outcome')
            for key in original:
                mutated = copy.deepcopy(rows)
                outcome = next(r for r in mutated if r.get('name') == name and r['event'] == 'result_outcome')
                del outcome[key]
                with self.subTest(name=name, missing=key), self.assertRaises(ValueError):
                    validate(mutated, manifest)
            mutations = [
                ('result_variant', 'Page'), ('result_variant', 'Progress'),
                ('projection_keys', None), ('projection_keys', []),
                ('projection_keys', ['department_id', 'status']),
                ('verified_ordered_identities', case['count'] + 1),
                ('page_size', 1000), ('pages', 0), ('correct', False),
                ('old_and_arbitrary_page_replay', False),
            ]
            for field in ('verified_property_maps', 'verified_property_entries',
                          'verified_missing_property_keys', 'verified_empty_property_maps'):
                mutations.extend((field, value) for value in (True, -1, original[field] + 1))
            for key, value in mutations:
                mutated = copy.deepcopy(rows)
                outcome = next(r for r in mutated if r.get('name') == name and r['event'] == 'result_outcome')
                outcome[key] = value
                with self.subTest(name=name, key=key, value=value), self.assertRaises(ValueError):
                    validate(mutated, manifest)

    def test_projection_query_cannot_be_id_count_or_different_properties(self):
        rows, manifest = evidence()
        for name, case in expected_cases(200000, 5000).items():
            if case['projection_keys'] is None:
                continue
            prefix = case['query'].split('.valueMap(')[0]
            for query in (prefix, prefix + '.count()', prefix + ".valueMap('status')"):
                mutated = copy.deepcopy(rows)
                begin = next(r for r in mutated if r.get('name') == name and r['event'] == 'result_begin')
                begin['query'] = query
                with self.subTest(name=name, query=query), self.assertRaises(ValueError):
                    validate(mutated, manifest)

    def test_projection_duplicate_begin_and_outcome_for_every_case(self):
        rows, manifest = evidence()
        for name, case in expected_cases(200000, 5000).items():
            if case['projection_keys'] is None:
                continue
            for event in ('result_begin', 'result_outcome'):
                index = next(i for i, r in enumerate(rows) if r.get('name') == name and r['event'] == event)
                with self.subTest(name=name, event=event), self.assertRaises(ValueError):
                    validate(rows[:index] + [rows[index]] + rows[index:], manifest)

    def test_id_cases_cannot_claim_projected_results(self):
        rows, manifest = evidence()
        for key, value in (('result_variant', 'ProjectedPage'), ('verified_property_maps', 199999),
                           ('projection_keys', ['name', 'active', 'country'])):
            mutated = copy.deepcopy(rows)
            next(r for r in mutated if r['event'] == 'result_outcome')[key] = value
            with self.subTest(key=key), self.assertRaises(ValueError):
                validate(mutated, manifest)

    def test_missing_each_case(self):
        rows, manifest = evidence()
        for name in expected_cases(200000, 5000):
            with self.subTest(name=name), self.assertRaises(ValueError):
                validate([row for row in rows if row.get('name') != name], manifest)

    def test_duplicate_begin_outcome_and_terminal(self):
        rows, manifest = evidence()
        for event in ('result_begin', 'result_outcome', 'result_rejection', 'result_matrix_complete'):
            index = next(i for i, row in enumerate(rows) if row['event'] == event)
            with self.subTest(event=event), self.assertRaises(ValueError):
                validate(rows[:index] + [rows[index]] + rows[index:], manifest)

    def test_every_truncated_prefix(self):
        rows, manifest = evidence()
        for end in range(len(rows)):
            with self.subTest(end=end), self.assertRaises(ValueError):
                validate(rows[:end], manifest)

    def test_wrong_outcome_fields(self):
        rows, manifest = evidence()
        for key, value in (
                ('correct', False), ('correct', 1), ('verified_ordered_identities', 199998),
                ('verified_ordered_identities', True), ('pages', 199), ('pages', 0),
                ('pages', True), ('page_size', 1001), ('page_size', 999),
                ('old_and_arbitrary_page_replay', False), ('old_and_arbitrary_page_replay', 1),
                ('oracle', 'count only'), ('fixed_snapshots', []), ('preparation_advances', 0),
                ('preparation_advances', True), ('preparation_seconds', 3.0),
                ('seconds', float('nan')), ('read_latency', {})):
            mutated = copy.deepcopy(rows)
            outcome = next(row for row in mutated if row['event'] == 'result_outcome')
            outcome[key] = value
            with self.subTest(key=key, value=value), self.assertRaises(ValueError):
                validate(mutated, manifest)

    def test_missing_every_required_outcome_field(self):
        rows, manifest = evidence()
        first = next(row for row in rows if row['event'] == 'result_outcome')
        for key in first:
            mutated = copy.deepcopy(rows)
            del next(row for row in mutated if row['event'] == 'result_outcome')[key]
            with self.subTest(key=key), self.assertRaises(ValueError):
                validate(mutated, manifest)

    def test_wrong_begin_fields_and_event_order(self):
        rows, manifest = evidence()
        for key, value in (('name', 'unknown'), ('query', 'g.V(1).count()'),
                           ('expected_rows', 10000), ('page_size', 0)):
            mutated = copy.deepcopy(rows)
            next(row for row in mutated if row['event'] == 'result_begin')[key] = value
            with self.subTest(key=key), self.assertRaises(ValueError):
                validate(mutated, manifest)
        rows[3], rows[4] = rows[4], rows[3]
        with self.assertRaises(ValueError):
            validate(rows, manifest)

    def test_terminal_fields(self):
        rows, manifest = evidence()
        for key, value in (('correct', False), ('cases', 10), ('people', 2000),
                           ('departments', 50), ('full_fixture', False), ('oracle', 'count')):
            mutated = copy.deepcopy(rows)
            mutated[-1][key] = value
            with self.subTest(key=key), self.assertRaises(ValueError):
                validate(mutated, manifest)

    def test_rejection_boundaries(self):
        rows, manifest = evidence()
        for key, value in (('before_ready', False), ('rows_exposed', 1), ('rows_exposed', False),
                           ('oracle_rows', 399997), ('expected_rejection', 'Unavailable'),
                           ('seconds', -1)):
            mutated = copy.deepcopy(rows)
            next(row for row in mutated if row['event'] == 'result_rejection')[key] = value
            with self.subTest(key=key), self.assertRaises(ValueError):
                validate(mutated, manifest)
        smoke, smoke_manifest = evidence(False)
        rejection = next(row for row in rows if row['event'] == 'result_rejection')
        index = next(i for i, row in enumerate(smoke) if row.get('name') == 'duplicate_ceo_rejected'
                     and row['event'] == 'result_outcome')
        smoke[index] = rejection
        with self.assertRaises(ValueError):
            validate(smoke, smoke_manifest)

    def test_full_duplicate_cannot_succeed(self):
        rows, manifest = evidence()
        rejection = next(row for row in rows if row['event'] == 'result_rejection')
        rejection.update(event='result_outcome', correct=True, verified_ordered_identities=399998)
        with self.assertRaises(ValueError):
            validate(rows, manifest)

    def test_fixture_and_snapshot_corruption(self):
        rows, manifest = evidence()
        for mutation in (
                lambda rs: rs.pop(0),
                lambda rs: rs[0].update(shard=True),
                lambda rs: rs[0].update(rows=101),
                lambda rs: rs.insert(0, copy.deepcopy(rs[0])),
                lambda rs: next(r for r in rs if r['event'] == 'result_outcome')['fixed_snapshots'][0].update(shard=2),
                lambda rs: next(r for r in rs if r['event'] == 'result_outcome')['fixed_snapshots'][0].update(sequence=0),
                lambda rs: next(r for r in rs if r['event'] == 'result_outcome')['fixed_snapshots'][0]['identity'].update(node=2),
                lambda rs: next(r for r in rs if r['event'] == 'result_outcome')['fixed_snapshots'][0].update(sequence=6)):
            mutated = copy.deepcopy(rows)
            mutation(mutated)
            with self.assertRaises(ValueError):
                validate(mutated, manifest)

    def test_bad_latency(self):
        rows, manifest = evidence()
        for key, value in (('n', 1), ('n', True), ('min_ms', -1), ('p95_ms', 0.1),
                           ('max_ms', float('inf')), ('p50_ms', '1')):
            mutated = copy.deepcopy(rows)
            next(r for r in mutated if r['event'] == 'result_outcome')['read_latency'][key] = value
            with self.subTest(key=key), self.assertRaises(ValueError):
                validate(mutated, manifest)

    def test_empty_result_requires_one_page(self):
        rows, manifest = evidence()
        next(r for r in rows if r.get('name') == 'ceo_exact32_empty'
             and r['event'] == 'result_outcome')['pages'] = 0
        with self.assertRaises(ValueError):
            validate(rows, manifest)

    def test_unknown_and_failure_records(self):
        rows, manifest = evidence()
        for extra in ({'event': 'result_failure'}, {'event': 'job_progress'}, {}, None):
            with self.subTest(extra=extra), self.assertRaises(ValueError):
                validate(rows[:-1] + [extra, rows[-1]], manifest)

    def test_unsupported_or_malformed_manifest(self):
        rows, manifest = evidence()
        for field, value in (('people', True), ('people', 10000), ('departments', 4999),
                             ('shards', []), ('shards', [{'partition': 1, 'kv_rows': 100}] * 3)):
            changed = copy.deepcopy(manifest)
            changed[field] = value
            with self.subTest(field=field), self.assertRaises(ValueError):
                validate(rows, changed)


if __name__ == '__main__':
    unittest.main()
