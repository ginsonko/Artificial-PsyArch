# -*- coding: utf-8 -*-

import shutil
import tempfile
import unittest

from hdb import HDB


class TestHDBSelfCheck(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.mkdtemp(prefix='hdb_self_check_')
        self.hdb = HDB(config_override={'data_dir': self.temp_dir, 'enable_background_repair': False})

    def tearDown(self):
        self.hdb.close()
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def _packet(self, text: str) -> dict:
        sa_items = []
        for idx, ch in enumerate(text):
            sa_items.append({
                'id': f'sa_sc_{idx}',
                'object_type': 'sa',
                'content': {'raw': ch, 'display': ch, 'normalized': ch},
                'stimulus': {'role': 'feature', 'modality': 'text'},
                'energy': {'er': 1.0, 'ev': 0.0},
                'ext': {'packet_context': {'sequence_index': idx}},
            })
        return {
            'id': f'spkt_sc_{text}',
            'object_type': 'stimulus_packet',
            'sa_items': sa_items,
            'csa_items': [],
            'grouped_sa_sequences': [
                {'group_index': 0, 'source_type': 'current', 'origin_frame_id': 'frame_sc', 'sa_ids': [item['id'] for item in sa_items], 'csa_ids': []}
            ],
            'energy_summary': {'current_total_er': float(len(sa_items)), 'current_total_ev': 0.0},
            'source': {'parent_ids': []},
        }

    def test_self_check_detects_local_db_damage_and_repair_clears_it(self):
        result = self.hdb.run_stimulus_level_retrieval_storage(stimulus_packet=self._packet('你好'), trace_id='sc_seed')
        structure_id = (result['data']['new_structure_ids'] or result['data'].get('seeded_atomic_structure_ids', []))[0]

        self.hdb._structure_store.add_diff_entry(
            structure_id,
            target_id='st_missing',
            content_signature='sig_missing_target',
            base_weight=0.4,
        )
        self.hdb._structure_store.add_group_table_entry(
            structure_id,
            group_id='sg_missing',
            required_structure_ids=[structure_id, 'st_missing'],
            avg_energy_profile={structure_id: 1.0, 'st_missing': 0.0},
            base_weight=1.0,
        )

        check_result = self.hdb.self_check_hdb(trace_id='sc_check', target_id=structure_id)
        self.assertTrue(check_result['success'])
        issue_types = {item['type'] for item in check_result['data']['issues']}
        self.assertIn('dangling_diff_ref', issue_types)
        self.assertIn('dangling_group_table_group_ref', issue_types)
        self.assertIn('dangling_local_group_ref', issue_types)

        repair_result = self.hdb.repair_hdb(
            trace_id='sc_repair',
            target_id=structure_id,
            repair_scope='targeted',
            repair_actions=['drop_invalid_entry'],
            background=False,
        )
        self.assertTrue(repair_result['success'])
        self.assertEqual(repair_result['data']['status'], 'completed')

        check_again = self.hdb.self_check_hdb(trace_id='sc_check_again', target_id=structure_id)
        issue_types_again = {item['type'] for item in check_again['data']['issues']}
        self.assertNotIn('dangling_diff_ref', issue_types_again)
        self.assertNotIn('dangling_group_table_group_ref', issue_types_again)
        self.assertNotIn('dangling_local_group_ref', issue_types_again)

    def test_self_check_detects_memory_activation_ref_mismatch(self):
        self.hdb._episodic_store.append(
            {
                'event_summary': 'memory mismatch',
                'structure_refs': [],
                'group_refs': [],
                'meta': {'ext': {'memory_material': {'memory_kind': 'structure_group'}}},
            },
            trace_id='sc_em',
        )
        memory_id = next(iter(self.hdb._episodic_store._items.keys()))
        self.hdb._memory_activation_store._items[memory_id] = {
            'id': memory_id,
            'memory_id': memory_id,
            'object_type': 'memory_activation',
            'display_text': memory_id,
            'event_summary': 'memory mismatch',
            'structure_refs': ['st_missing'],
            'group_refs': ['sg_missing'],
            'backing_structure_ids': ['st_missing'],
            'source_structure_ids': ['st_missing'],
            'er': 0.0,
            'ev': 1.0,
            'last_delta_er': 0.0,
            'last_delta_ev': 1.0,
            'last_decay_delta_er': 0.0,
            'last_decay_delta_ev': 0.0,
            'total_delta_er': 0.0,
            'total_delta_ev': 1.0,
            'hit_count': 1,
            'update_count': 1,
            'mode_totals': {},
            'mode_totals_er': {},
            'mode_totals_ev': {},
            'recent_events': [],
            'feedback_count': 0,
            'last_feedback_er': 0.0,
            'last_feedback_ev': 0.0,
            'total_feedback_er': 0.0,
            'total_feedback_ev': 0.0,
            'last_feedback_at': 0,
            'recent_feedback_events': [],
            'created_at': 0,
            'last_updated_at': 0,
            'last_trace_id': '',
            'last_tick_id': '',
        }

        check_result = self.hdb.self_check_hdb(trace_id='sc_mem_check', target_id=memory_id)
        self.assertTrue(check_result['success'])
        issue_types = {item['type'] for item in check_result['data']['issues']}
        self.assertIn('dangling_memory_activation_structure_ref', issue_types)
        self.assertIn('dangling_memory_activation_group_ref', issue_types)
        self.assertIn('memory_activation_ref_mismatch', issue_types)


if __name__ == '__main__':
    unittest.main()
