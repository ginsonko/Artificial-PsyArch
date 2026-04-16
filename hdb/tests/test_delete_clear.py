# -*- coding: utf-8 -*-

import glob
import os
import shutil
import tempfile
import unittest

from hdb import HDB


class TestHDBDeleteClear(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.mkdtemp(prefix='hdb_delete_clear_')
        self.hdb = HDB(config_override={'data_dir': self.temp_dir, 'enable_background_repair': False})

    def tearDown(self):
        if getattr(self, 'hdb', None) is not None:
            self.hdb.close()
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def _packet(self, text: str) -> dict:
        sa_items = []
        for idx, ch in enumerate(text):
            sa_items.append({
                'id': f'sa_dc_{idx}',
                'object_type': 'sa',
                'content': {'raw': ch, 'display': ch, 'normalized': ch},
                'stimulus': {'role': 'feature', 'modality': 'text'},
                'energy': {'er': 1.0, 'ev': 0.0},
                'ext': {'packet_context': {'sequence_index': idx}},
            })
        return {
            'id': f'spkt_dc_{text}',
            'object_type': 'stimulus_packet',
            'sa_items': sa_items,
            'csa_items': [],
            'grouped_sa_sequences': [
                {'group_index': 0, 'source_type': 'current', 'origin_frame_id': 'frame_dc', 'sa_ids': [item['id'] for item in sa_items], 'csa_ids': []}
            ],
            'energy_summary': {'current_total_er': float(len(sa_items)), 'current_total_ev': 0.0},
            'source': {'parent_ids': []},
        }

    def test_delete_and_clear_full_remove_runtime_artifacts(self):
        result = self.hdb.run_stimulus_level_retrieval_storage(stimulus_packet=self._packet('你好'), trace_id='dc_seed')
        structure_id = (result['data']['new_structure_ids'] or result['data'].get('seeded_atomic_structure_ids', []))[0]

        delete_result = self.hdb.delete_structure(structure_id=structure_id, trace_id='dc_delete', delete_mode='safe_detach')
        self.assertTrue(delete_result['success'])
        self.assertTrue(delete_result['data']['deleted'])

        query = self.hdb.query_structure_database(structure_id=structure_id, trace_id='dc_query')
        self.assertFalse(query['success'])

        self.hdb._register_issue({'issue_type': 'manual_test_issue', 'target_id': structure_id})
        repair = self.hdb.repair_hdb(trace_id='dc_repair', repair_scope='global_quick', background=False)
        self.assertTrue(repair['success'])
        self.assertEqual(repair['data']['status'], 'completed')
        self.assertTrue(glob.glob(os.path.join(self.temp_dir, 'repair', 'repair_job_*.json')))

        clear_result = self.hdb.clear_hdb(trace_id='dc_clear', reason='unit_test_reset', operator='tester', clear_mode='full')
        self.assertTrue(clear_result['success'])
        self.assertGreaterEqual(clear_result['data']['cleared_repair_file_count'], 1)

        self.hdb.close()
        self.hdb = None

        reopened = HDB(config_override={'data_dir': self.temp_dir, 'enable_background_repair': False})
        try:
            snapshot = reopened.get_hdb_snapshot(trace_id='dc_snapshot')['data']
            self.assertEqual(snapshot['summary']['structure_count'], 0)
            self.assertEqual(snapshot['summary']['group_count'], 0)
            self.assertEqual(snapshot['summary']['episodic_count'], 0)
            self.assertEqual(snapshot['summary']['issue_count'], 0)
            self.assertEqual(len(reopened._repair.jobs), 0)
            self.assertFalse(glob.glob(os.path.join(self.temp_dir, 'repair', 'repair_job_*.json')))
        finally:
            reopened.close()


if __name__ == '__main__':
    unittest.main()
