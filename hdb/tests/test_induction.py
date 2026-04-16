# -*- coding: utf-8 -*-

import shutil
import tempfile
import unittest

from hdb import HDB


class TestHDBInduction(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.mkdtemp(prefix='hdb_induction_')
        self.hdb = HDB(config_override={'data_dir': self.temp_dir, 'enable_background_repair': False})

    def tearDown(self):
        self.hdb.close()
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def _packet(self, text: str) -> dict:
        sa_items = []
        for idx, ch in enumerate(text):
            sa_items.append({
                'id': f'sa_in_{text}_{idx}',
                'object_type': 'sa',
                'content': {'raw': ch, 'display': ch, 'normalized': ch},
                'stimulus': {'role': 'feature', 'modality': 'text'},
                'energy': {'er': 1.0, 'ev': 0.0},
                'ext': {'packet_context': {'sequence_index': idx}},
            })
        return {
            'id': f'spkt_in_{text}',
            'object_type': 'stimulus_packet',
            'sa_items': sa_items,
            'csa_items': [],
            'grouped_sa_sequences': [
                {'group_index': 0, 'source_type': 'current', 'origin_frame_id': 'frame_in', 'sa_ids': [item['id'] for item in sa_items], 'csa_ids': []}
            ],
            'energy_summary': {'current_total_er': float(len(sa_items)), 'current_total_ev': 0.0},
            'source': {'parent_ids': []},
        }

    def _store_packet_as_structure(self, packet: dict, *, trace_id: str) -> dict:
        profile = self.hdb._cut.build_sequence_profile_from_stimulus_packet(packet)
        payload = self.hdb._cut.make_structure_payload_from_profile(
            profile,
            confidence=0.9,
            ext={'kind': 'test_seed', 'relation_type': 'test_seed'},
        )
        structure_obj, _ = self.hdb._structure_store.create_structure(
            structure_payload=payload,
            trace_id=trace_id,
            tick_id=trace_id,
            origin='test_seed',
            origin_id=packet.get('id', trace_id),
            parent_ids=[],
        )
        self.hdb._pointer_index.register_structure(structure_obj)
        return structure_obj

    def test_induction_generates_targets_from_local_diff_table(self):
        self.hdb.run_stimulus_level_retrieval_storage(stimulus_packet=self._packet('你好呀'), trace_id='in_seed_1')
        self.hdb.run_stimulus_level_retrieval_storage(stimulus_packet=self._packet('你好！'), trace_id='in_seed_2')

        source_id = None
        for structure_obj in self.hdb._structure_store.iter_structures():
            structure_db = self.hdb._structure_store.get_db_by_owner(structure_obj['id'])
            if structure_db and structure_db.get('diff_table'):
                source_id = structure_obj['id']
                break
        self.assertIsNotNone(source_id)

        state_snapshot = {
            'summary': {'active_item_count': 1},
            'top_items': [
                {
                    'id': 'runtime_source_1',
                    'ref_object_type': 'st',
                    'ref_object_id': source_id,
                    'display': source_id,
                    'er': 2.0,
                    'ev': 0.8,
                }
            ],
        }
        result = self.hdb.run_induction_propagation(state_snapshot=state_snapshot, trace_id='in_run', max_source_items=1)
        self.assertTrue(result['success'])
        self.assertGreater(result['data']['total_delta_ev'], 0.0)
        self.assertTrue(result['data']['induced_target_count'] > 0 or result['data']['propagated_target_count'] > 0)
        self.assertTrue(result['data']['induction_targets'])
        # EV propagation consumes only a fraction of source EV (ev_propagation_ratio).
        ev_ratio = float(self.hdb._config.get('ev_propagation_ratio', 0.28))
        self.assertAlmostEqual(result['data']['total_ev_consumed'], 0.8 * ev_ratio, places=6)
        self.assertEqual(len(result['data']['source_ev_consumptions']), 1)
        self.assertAlmostEqual(result['data']['source_ev_consumptions'][0]['consumed_ev'], 0.8 * ev_ratio, places=6)

    def test_induction_aggregates_duplicate_target_paths_by_unique_structure(self):
        self.hdb.run_stimulus_level_retrieval_storage(stimulus_packet=self._packet('A'), trace_id='dup_seed_a')
        structure_ab = self._store_packet_as_structure(self._packet('AB'), trace_id='dup_seed_ab')
        structure_ac = self._store_packet_as_structure(self._packet('AC'), trace_id='dup_seed_ac')

        atomic_a = None
        for structure_obj in self.hdb._structure_store.iter_structures():
            flat_tokens = list(structure_obj.get('structure', {}).get('flat_tokens', []))
            if flat_tokens == ['A']:
                atomic_a = structure_obj
        self.assertIsNotNone(atomic_a)
        self.assertIsNotNone(structure_ab)
        self.assertIsNotNone(structure_ac)

        self.hdb._structure_store.add_diff_entry(
            atomic_a['id'],
            target_id=structure_ab['id'],
            content_signature=structure_ab.get('structure', {}).get('content_signature', ''),
            base_weight=0.55,
            residual_existing_signature='',
            residual_incoming_signature='shadow',
            ext={'relation_type': 'incoming_extension'},
        )
        self.hdb._structure_store.add_diff_entry(
            atomic_a['id'],
            target_id=structure_ac['id'],
            content_signature=structure_ac.get('structure', {}).get('content_signature', ''),
            base_weight=0.65,
            residual_existing_signature='',
            residual_incoming_signature='tail',
            ext={'relation_type': 'incoming_extension'},
        )

        state_snapshot = {
            'summary': {'active_item_count': 1},
            'top_items': [
                {
                    'id': 'runtime_source_dup',
                    'ref_object_type': 'st',
                    'ref_object_id': atomic_a['id'],
                    'display': 'A',
                    'er': 0.6,
                    'ev': 0.9,
                }
            ],
        }
        result = self.hdb.run_induction_propagation(state_snapshot=state_snapshot, trace_id='dup_induction', max_source_items=1)
        self.assertTrue(result['success'])

        ev_targets = [item for item in result['data']['induction_targets'] if item.get('modes') == ['ev_propagation']]
        self.assertEqual(
            len([item for item in ev_targets if item.get('target_structure_id') == structure_ab['id']]),
            1,
        )
        self.assertEqual(
            len([item for item in ev_targets if item.get('target_structure_id') == structure_ac['id']]),
            1,
        )
        # EV propagation uses ev_propagation_ratio fraction of source EV.
        ev_ratio = float(self.hdb._config.get('ev_propagation_ratio', 0.28))
        self.assertAlmostEqual(sum(item.get('delta_ev', 0.0) for item in ev_targets), 0.9 * ev_ratio, places=6)

    def test_induction_includes_owner_raw_residual_memory(self):
        self.hdb.run_stimulus_level_retrieval_storage(
            stimulus_packet=self._packet('AB'),
            trace_id='residual_owner_seed',
            max_rounds=1,
        )

        atomic_a = None
        residual_target_id = None
        for structure_obj in self.hdb._structure_store.iter_structures():
            if list(structure_obj.get('structure', {}).get('flat_tokens', [])) == ['A']:
                atomic_a = structure_obj
                break
        self.assertIsNotNone(atomic_a)

        owner_db = self.hdb._structure_store.get_db_by_owner(atomic_a['id'])
        self.assertIsNotNone(owner_db)
        residual_entries = [
            entry for entry in owner_db.get('diff_table', [])
            if entry.get('entry_type') == 'raw_residual'
            and entry.get('ext', {}).get('relation_type') == 'stimulus_raw_residual'
        ]
        self.assertEqual(len(residual_entries), 1)
        residual_memory_id = (residual_entries[0].get('memory_refs', []) or [''])[-1]
        self.assertTrue(residual_memory_id.startswith('em_'))

        state_snapshot = {
            'summary': {'active_item_count': 1},
            'top_items': [
                {
                    'id': 'runtime_source_residual_owner',
                    'ref_object_type': 'st',
                    'ref_object_id': atomic_a['id'],
                    'display': 'A',
                    'er': 1.2,
                    'ev': 0.9,
                }
            ],
        }
        result = self.hdb.run_induction_propagation(
            state_snapshot=state_snapshot,
            trace_id='residual_owner_induction',
            max_source_items=1,
            enable_ev_propagation=True,
            enable_er_induction=True,
        )
        self.assertTrue(result['success'])
        memory_targets = [
            item for item in result['data']['induction_targets']
            if item.get('memory_id') == residual_memory_id
        ]
        self.assertTrue(memory_targets)
        self.assertIn('er_induction', {tuple(item.get('modes', []))[0] for item in memory_targets})
        self.assertIn('ev_propagation', {tuple(item.get('modes', []))[0] for item in memory_targets})

    def test_memory_activation_pool_merges_same_memory_id_across_modes(self):
        append_result = self.hdb.append_episodic_memory(
            episodic_payload={
                'event_summary': 'memory_for_activation',
                'structure_refs': ['st_a', 'st_b'],
                'group_refs': [],
            },
            trace_id='memory_pool_seed',
        )
        self.assertTrue(append_result['success'])
        memory_id = append_result['data']['episodic_id']

        apply_result = self.hdb.apply_memory_activation_targets(
            targets=[
                {
                    'projection_kind': 'memory',
                    'memory_id': memory_id,
                    'target_display_text': 'memory_for_activation',
                    'delta_ev': 0.4,
                    'sources': ['st_a'],
                    'modes': ['ev_propagation'],
                    'backing_structure_id': 'st_a',
                },
                {
                    'projection_kind': 'memory',
                    'memory_id': memory_id,
                    'target_display_text': 'memory_for_activation',
                    'delta_ev': 0.6,
                    'sources': ['st_b'],
                    'modes': ['er_induction'],
                    'backing_structure_id': 'st_b',
                },
            ],
            trace_id='memory_pool_apply',
        )
        self.assertTrue(apply_result['success'])
        self.assertEqual(apply_result['data']['applied_count'], 1)
        self.assertAlmostEqual(apply_result['data']['total_delta_ev'], 1.0, places=6)

        snapshot = self.hdb.get_memory_activation_snapshot(trace_id='memory_pool_snapshot', limit=8)
        self.assertTrue(snapshot['success'])
        self.assertEqual(snapshot['data']['summary']['count'], 1)
        item = snapshot['data']['items'][0]
        self.assertEqual(item['memory_id'], memory_id)
        self.assertAlmostEqual(item['ev'], 1.0, places=6)
        self.assertAlmostEqual(item['mode_totals']['ev_propagation'], 0.4, places=6)
        self.assertAlmostEqual(item['mode_totals']['er_induction'], 0.6, places=6)
        self.assertEqual(set(item['source_structure_ids']), {'st_a', 'st_b'})


if __name__ == '__main__':
    unittest.main()
