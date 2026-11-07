import copy
import json
import unittest

from e2e_postgres import assert_fixture_layout, normalized_catalog


class PostgresFixtureTests(unittest.TestCase):
    def windows(self, counts, tenants=('1:2', '7:9')):
        return [{'id': i + 1, 'generation': i + 1, 'tenant': tenants[i // 3],
                 'source_id': 'hot', 'start_ms': 100 + i % 3 * 100,
                 'end_ms': 200 + i % 3 * 100, 'pack_count': count}
                for i, count in enumerate(counts)]

    def test_rotated_packs_preserve_exact_logical_windows(self):
        counts = [4, 3, 5, 2, 6, 4]
        layout = assert_fixture_layout(self.windows(counts), sum(counts), 100, 400, 100,
                                       ['1:2', '7:9'], require_multiple=True)
        self.assertEqual((layout['windows'], layout['packs'], layout['empty_windows']), (6, 24, 0))

    def test_migration_preserves_verified_empty_and_rotated_windows(self):
        counts = [0, 3, 5]
        layout = assert_fixture_layout(self.windows(counts), sum(counts), 100, 400, 100,
                                       ['1:2'], empty_tenant='1:2', require_multiple=True)
        self.assertEqual(layout['empty_windows'], 1)
        self.assertEqual(layout['packs'], 8)

    def test_rotation_must_not_hide_missing_or_invalid_coverage(self):
        windows = self.windows([0, 3, 5])
        mutations = {
            'missing window': lambda rows: rows.pop(),
            'duplicate window': lambda rows: rows.__setitem__(2, dict(rows[1])),
            'unknown source': lambda rows: rows[1].update(source_id='other'),
            'partial window': lambda rows: rows[1].update(end_ms=250),
            'generation gap': lambda rows: rows[1].update(generation=4),
            'unexpected payload for empty window': lambda rows: rows[0].update(pack_count=2),
            'empty nonempty window': lambda rows: rows[1].update(pack_count=0),
            'rotation did not happen': lambda rows: rows[1].update(pack_count=1),
        }
        for label, mutate in mutations.items():
            with self.subTest(label=label):
                rows = copy.deepcopy(windows)
                mutate(rows)
                with self.assertRaises(AssertionError):
                    assert_fixture_layout(rows, sum(row['pack_count'] for row in rows), 100, 400, 100,
                                          ['1:2'], empty_tenant='1:2', require_multiple=True)
        with self.assertRaises(AssertionError):
            assert_fixture_layout(windows, 7, 100, 400, 100, ['1:2'], '1:2', True)

    def test_normalization_ignores_database_ids_but_preserves_descriptor_order(self):
        source = {'state': {'catalog_id': 'sqlite', 'schema_version': 1, 'vm_version': 'v-test',
                            'generation': 1, 'contract_json': json.dumps({'window_ms': 100}),
                            'blob_json': json.dumps({'type': 's3'})},
                  'coverage': [{'tenant': '1:2', 'source_id': 'hot', 'end_ms': 200}],
                  'windows': self.windows([2]),
                  'packs': [{'window_id': 1, 'ordinal': 0, 'data_key': 'first', 'index_key': 'first-index'},
                            {'window_id': 1, 'ordinal': 1, 'data_key': 'second', 'index_key': 'second-index'}]}
        destination = copy.deepcopy(source)
        destination['state'].update(catalog_id='postgres', schema_version=2)
        destination['windows'][0]['id'] = 41
        for pack in destination['packs']:
            pack['window_id'] = 41
        destination['packs'].reverse()
        self.assertEqual(normalized_catalog(source), normalized_catalog(destination))
        destination['packs'][0]['ordinal'], destination['packs'][1]['ordinal'] = 0, 1
        self.assertNotEqual(normalized_catalog(source), normalized_catalog(destination))


if __name__ == '__main__':
    unittest.main()
