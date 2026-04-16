# -*- coding: utf-8 -*-

import shutil
import tempfile
import unittest

from hdb import HDB


class TestHDBStructureLevelRS(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.mkdtemp(prefix='hdb_structure_rs_')
        self.hdb = HDB(config_override={'data_dir': self.temp_dir, 'enable_background_repair': False})

    def tearDown(self):
        self.hdb.close()
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def _packet(self, text: str) -> dict:
        sa_items = []
        for idx, ch in enumerate(text):
            sa_items.append({
                'id': f'sa_sg_{text}_{idx}',
                'object_type': 'sa',
                'content': {'raw': ch, 'display': ch, 'normalized': ch},
                'stimulus': {'role': 'feature', 'modality': 'text'},
                'energy': {'er': 1.0, 'ev': 0.0},
                'ext': {'packet_context': {'sequence_index': idx}},
            })
        return {
            'id': f'spkt_sg_{text}',
            'object_type': 'stimulus_packet',
            'sa_items': sa_items,
            'csa_items': [],
            'grouped_sa_sequences': [
                {'group_index': 0, 'source_type': 'current', 'origin_frame_id': 'frame_sg', 'sa_ids': [item['id'] for item in sa_items], 'csa_ids': []}
            ],
            'energy_summary': {'current_total_er': float(len(sa_items)), 'current_total_ev': 0.0},
            'source': {'parent_ids': []},
        }

    def _seed_atomic(self, token: str, trace_id: str) -> str:
        result = self.hdb.run_stimulus_level_retrieval_storage(
            stimulus_packet=self._packet(token),
            trace_id=trace_id,
        )
        structure_ids = result['data'].get('seeded_atomic_structure_ids') or result['data'].get('new_structure_ids') or []
        self.assertEqual(len(structure_ids), 1)
        return structure_ids[0]

    def _snapshot(self, entries: list[tuple[str, float, float]]) -> dict:
        return {
            'summary': {'active_item_count': len(entries)},
            'top_items': [
                {
                    'id': f'node_{index}',
                    'ref_object_type': 'st',
                    'ref_object_id': structure_id,
                    'display': structure_id,
                    'er': er,
                    'ev': ev,
                }
                for index, (structure_id, er, ev) in enumerate(entries, start=1)
            ],
        }

    def test_group_is_created_from_overlap_then_matched_via_owner_chain(self):
        structure_a = self._seed_atomic('A', 'sg_seed_a')
        structure_b = self._seed_atomic('B', 'sg_seed_b')
        structure_x = self._seed_atomic('X', 'sg_seed_x')
        structure_y = self._seed_atomic('Y', 'sg_seed_y')
        structure_z = self._seed_atomic('Z', 'sg_seed_z')

        first_snapshot = self._snapshot([
            (structure_a, 5.0, 0.2),
            (structure_b, 3.0, 0.2),
            (structure_x, 2.0, 0.2),
        ])
        first_result = self.hdb.run_structure_level_retrieval_storage(
            state_snapshot=first_snapshot,
            trace_id='sg_round_1',
            top_n=3,
        )
        self.assertTrue(first_result['success'])
        self.assertEqual(first_result['data']['new_group_ids'], [])
        self.assertEqual(first_result['data']['matched_group_ids'], [])
        owner_a_db = self.hdb._structure_store.get_db_by_owner(structure_a)
        self.assertIsNotNone(owner_a_db)
        self.assertEqual(len(owner_a_db.get('group_residual_table', [])), 1)
        self.assertEqual(owner_a_db.get('group_table', []), [])

        second_snapshot = self._snapshot([
            (structure_a, 5.0, 0.2),
            (structure_b, 3.0, 0.2),
            (structure_y, 2.0, 0.2),
        ])
        second_result = self.hdb.run_structure_level_retrieval_storage(
            state_snapshot=second_snapshot,
            trace_id='sg_round_2',
            top_n=3,
        )
        self.assertTrue(second_result['success'])
        self.assertEqual(len(second_result['data']['new_group_ids']), 1)
        group_id = second_result['data']['new_group_ids'][0]
        owner_a_db = self.hdb._structure_store.get_db_by_owner(structure_a)
        self.assertTrue(any(entry.get('group_id') == group_id for entry in owner_a_db.get('group_table', [])))
        group_obj = self.hdb._group_store.get(group_id)
        self.assertIsNotNone(group_obj)
        self.assertEqual(group_obj.get('required_structure_ids'), [structure_a, structure_b])
        self.assertEqual(len(group_obj.get('local_db', {}).get('residual_table', [])), 2)

        third_snapshot = self._snapshot([
            (structure_a, 5.0, 0.2),
            (structure_b, 3.0, 0.2),
            (structure_z, 2.0, 0.2),
        ])
        third_result = self.hdb.run_structure_level_retrieval_storage(
            state_snapshot=third_snapshot,
            trace_id='sg_round_3',
            top_n=3,
        )
        self.assertTrue(third_result['success'])
        self.assertIn(group_id, third_result['data']['matched_group_ids'])
        self.assertGreaterEqual(third_result['data']['round_count'], 1)
        selected_group = next(
            (detail.get('selected_group') for detail in third_result['data']['debug']['round_details'] if detail.get('selected_group')),
            None,
        )
        self.assertIsNotNone(selected_group)
        self.assertEqual(selected_group.get('group_id'), group_id)

    def test_invalid_overlap_without_owner_containment_does_not_create_common_group(self):
        structure_a = self._seed_atomic('A', 'sg_owner_a')
        structure_b = self._seed_atomic('B', 'sg_owner_b')
        structure_c = self._seed_atomic('C', 'sg_owner_c')

        first_result = self.hdb.run_structure_level_retrieval_storage(
            state_snapshot=self._snapshot([
                (structure_a, 5.0, 0.2),
                (structure_b, 4.0, 0.2),
            ]),
            trace_id='sg_invalid_overlap_1',
            top_n=2,
        )
        self.assertTrue(first_result['success'])
        owner_a_db = self.hdb._structure_store.get_db_by_owner(structure_a)
        self.assertIsNotNone(owner_a_db)
        self.assertEqual(len(owner_a_db.get('group_residual_table', [])), 1)
        self.assertEqual(owner_a_db.get('group_table', []), [])

        second_result = self.hdb.run_structure_level_retrieval_storage(
            state_snapshot=self._snapshot([
                (structure_b, 4.0, 0.2),
                (structure_a, 5.0, 0.2),
                (structure_c, 3.0, 0.2),
            ]),
            trace_id='sg_invalid_overlap_2',
            top_n=3,
        )
        self.assertTrue(second_result['success'])
        self.assertEqual(second_result['data']['new_group_ids'], [])

        owner_a_db = self.hdb._structure_store.get_db_by_owner(structure_a)
        self.assertIsNotNone(owner_a_db)
        self.assertEqual(owner_a_db.get('group_table', []), [])
        self.assertEqual(len(owner_a_db.get('group_residual_table', [])), 2)


if __name__ == '__main__':
    unittest.main()
