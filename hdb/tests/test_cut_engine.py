# -*- coding: utf-8 -*-

import unittest

from hdb._cut_engine import CutEngine
from hdb._pointer_index import PointerIndex


class TestCutEngine(unittest.TestCase):
    def _csa_group(self, anchor: str, attrs: list[str], *, group_index: int = 0) -> dict:
        units = [
            {
                "unit_id": f"feature_{group_index}_{anchor}",
                "token": anchor,
                "unit_role": "feature",
                "sequence_index": 0,
                "group_index": group_index,
                "source_group_index": group_index,
                "source_type": "text",
                "origin_frame_id": f"frame_{group_index}",
                "display_visible": True,
            }
        ]
        member_ids = [units[0]["unit_id"]]
        for index, attr in enumerate(attrs, start=1):
            unit_id = f"attr_{group_index}_{index}_{attr}"
            units.append(
                {
                    "unit_id": unit_id,
                    "token": attr,
                    "unit_role": "attribute",
                    "sequence_index": index,
                    "group_index": group_index,
                    "source_group_index": group_index,
                    "source_type": "text",
                    "origin_frame_id": f"frame_{group_index}",
                    "display_visible": False,
                    "bundle_anchor_unit_id": units[0]["unit_id"],
                }
            )
            member_ids.append(unit_id)
        return {
            "group_index": group_index,
            "source_type": "text",
            "origin_frame_id": f"frame_{group_index}",
            "units": units,
            "csa_bundles": [
                {
                    "bundle_id": f"bundle_{group_index}_{anchor}",
                    "anchor_unit_id": units[0]["unit_id"],
                    "member_unit_ids": member_ids,
                }
            ],
        }

    def test_maximum_common_part_detects_contiguous_overlap(self):
        engine = CutEngine()
        result = engine.maximum_common_part(['你', '好', '呀'], ['你', '好', '！'])
        self.assertEqual(result['common_tokens'], ['你', '好'])
        self.assertEqual(result['common_length'], 2)
        self.assertEqual(result['residual_existing_tokens'], ['呀'])
        self.assertEqual(result['residual_incoming_tokens'], ['！'])

    def test_build_internal_packet_preserves_fragment_energy_totals(self):
        engine = CutEngine()
        packet = engine.build_internal_stimulus_packet(
            [
                {
                    "fragment_id": "frag_001",
                    "sequence_groups": [
                        {"group_index": 0, "source_type": "internal", "origin_frame_id": "frag_001", "tokens": ["A", "B"]},
                        {"group_index": 1, "source_type": "internal", "origin_frame_id": "frag_001", "tokens": ["C"]},
                    ],
                    "flat_tokens": ["A", "B", "C"],
                    "er_hint": 1.2,
                    "ev_hint": 0.6,
                }
            ],
            trace_id="cut_trace",
            tick_id="cut_tick",
        )
        total_er = sum(item["energy"]["er"] for item in packet["sa_items"])
        total_ev = sum(item["energy"]["ev"] for item in packet["sa_items"])
        self.assertAlmostEqual(total_er, 1.2, places=6)
        self.assertAlmostEqual(total_ev, 0.6, places=6)

    def test_build_internal_packet_collapses_fragment_groups_into_one_cooccurrence_group(self):
        engine = CutEngine()
        packet = engine.build_internal_stimulus_packet(
            [
                {
                    "fragment_id": "frag_001",
                    "sequence_groups": [
                        {"group_index": 0, "source_type": "internal", "origin_frame_id": "frag_001", "tokens": ["A", "B"]},
                        {"group_index": 1, "source_type": "internal", "origin_frame_id": "frag_001", "tokens": ["C"]},
                    ],
                    "flat_tokens": ["A", "B", "C"],
                    "er_hint": 1.2,
                    "ev_hint": 0.6,
                },
                {
                    "fragment_id": "frag_002",
                    "sequence_groups": [
                        {"group_index": 0, "source_type": "internal", "origin_frame_id": "frag_002", "tokens": ["D"]},
                    ],
                    "flat_tokens": ["D"],
                    "er_hint": 0.4,
                    "ev_hint": 0.2,
                },
            ],
            trace_id="cut_trace",
            tick_id="cut_tick",
        )
        self.assertEqual(len(packet["grouped_sa_sequences"]), 1)
        profile = engine.build_sequence_profile_from_stimulus_packet(packet)
        self.assertEqual(len(profile["sequence_groups"]), 1)
        self.assertCountEqual(profile["sequence_groups"][0]["tokens"], ["A", "B", "C", "D"])

    def test_build_internal_packet_uses_unique_runtime_csa_ids(self):
        engine = CutEngine()
        left_group = self._csa_group("A", ["x"])
        right_group = self._csa_group("B", ["y"])
        left_group["csa_bundles"][0]["bundle_id"] = "common_bundle_0"
        right_group["csa_bundles"][0]["bundle_id"] = "common_bundle_0"

        packet = engine.build_internal_stimulus_packet(
            [
                {
                    "fragment_id": "frag_001",
                    "sequence_groups": [left_group],
                    "flat_tokens": ["A", "x"],
                    "er_hint": 1.0,
                    "ev_hint": 0.0,
                },
                {
                    "fragment_id": "frag_002",
                    "sequence_groups": [right_group],
                    "flat_tokens": ["B", "y"],
                    "er_hint": 1.0,
                    "ev_hint": 0.0,
                },
            ],
            trace_id="cut_trace",
            tick_id="cut_tick",
        )

        csa_ids = [item["id"] for item in packet["csa_items"]]
        self.assertEqual(len(csa_ids), 2)
        self.assertEqual(len(set(csa_ids)), 2)
        self.assertTrue(all(csa_id.startswith("csa_internal_") for csa_id in csa_ids))

    def test_merge_stimulus_packets_appends_internal_into_last_external_group(self):
        engine = CutEngine()
        external_packet = {
            "id": "spkt_external",
            "object_type": "stimulus_packet",
            "current_frame_id": "spkt_external",
            "echo_frame_ids": [],
            "sa_items": [
                {
                    "id": "sa_echo_0",
                    "object_type": "sa",
                    "content": {"raw": "X", "display": "X", "normalized": "X"},
                    "stimulus": {"role": "feature", "modality": "text"},
                    "energy": {"er": 0.5, "ev": 0.0},
                    "source": {"parent_ids": []},
                    "ext": {"packet_context": {"group_index": 0, "source_group_index": 0, "source_type": "echo", "origin_frame_id": "f0", "sequence_index": 0}},
                },
                {
                    "id": "sa_current_0",
                    "object_type": "sa",
                    "content": {"raw": "Y", "display": "Y", "normalized": "Y"},
                    "stimulus": {"role": "feature", "modality": "text"},
                    "energy": {"er": 1.0, "ev": 0.0},
                    "source": {"parent_ids": []},
                    "ext": {"packet_context": {"group_index": 1, "source_group_index": 1, "source_type": "current", "origin_frame_id": "f1", "sequence_index": 1}},
                },
            ],
            "csa_items": [],
            "echo_frames": [],
            "grouped_sa_sequences": [
                {"group_index": 0, "source_type": "echo", "origin_frame_id": "f0", "sa_ids": ["sa_echo_0"], "csa_ids": [], "source_group_index": 0},
                {"group_index": 1, "source_type": "current", "origin_frame_id": "f1", "sa_ids": ["sa_current_0"], "csa_ids": [], "source_group_index": 1},
            ],
            "energy_summary": {"total_er": 1.5, "total_ev": 0.0},
        }
        internal_packet = engine.build_internal_stimulus_packet(
            [
                {
                    "fragment_id": "frag_001",
                    "sequence_groups": [
                        {"group_index": 0, "source_type": "internal", "origin_frame_id": "frag_001", "tokens": ["A"]},
                        {"group_index": 1, "source_type": "internal", "origin_frame_id": "frag_001", "tokens": ["B"]},
                    ],
                    "flat_tokens": ["A", "B"],
                    "er_hint": 1.0,
                    "ev_hint": 0.2,
                }
            ],
            trace_id="cut_trace",
            tick_id="cut_tick",
        )

        merged = engine.merge_stimulus_packets(external_packet, internal_packet, trace_id="merge_trace", tick_id="merge_tick")

        self.assertEqual(len(merged["grouped_sa_sequences"]), 2)
        self.assertEqual(merged["grouped_sa_sequences"][0]["sa_ids"], ["sa_echo_0"])
        self.assertEqual(merged["grouped_sa_sequences"][0]["csa_ids"], [])

        expected_last_group_sa_ids = ["sa_current_0"] + [item["id"] for item in internal_packet["sa_items"]]
        self.assertEqual(
            merged["grouped_sa_sequences"][1]["sa_ids"],
            expected_last_group_sa_ids,
        )

        profile = engine.build_sequence_profile_from_stimulus_packet(merged)
        self.assertEqual(len(profile["sequence_groups"]), 2)
        self.assertCountEqual(profile["sequence_groups"][1]["tokens"], ["Y", "A", "B"])

        units_by_id = {
            unit["unit_id"]: unit
            for unit in profile["sequence_groups"][1]["units"]
        }
        self.assertEqual(units_by_id["sa_current_0"]["source_type"], "current")
        for internal_item in internal_packet["sa_items"]:
            self.assertEqual(units_by_id[internal_item["id"]]["source_type"], "internal")

    def test_sequence_signature_is_group_order_sensitive_but_group_internal_order_relaxed(self):
        engine = CutEngine()
        left = [
            {"group_index": 0, "source_type": "text", "origin_frame_id": "f1", "tokens": ["B", "A"]},
            {"group_index": 1, "source_type": "text", "origin_frame_id": "f1", "tokens": ["C"]},
        ]
        same_groups_different_token_order = [
            {"group_index": 0, "source_type": "text", "origin_frame_id": "f1", "tokens": ["A", "B"]},
            {"group_index": 1, "source_type": "text", "origin_frame_id": "f1", "tokens": ["C"]},
        ]
        reversed_group_order = [
            {"group_index": 0, "source_type": "text", "origin_frame_id": "f1", "tokens": ["C"]},
            {"group_index": 1, "source_type": "text", "origin_frame_id": "f1", "tokens": ["A", "B"]},
        ]
        self.assertEqual(engine.sequence_groups_to_signature(left), engine.sequence_groups_to_signature(same_groups_different_token_order))
        self.assertNotEqual(engine.sequence_groups_to_signature(left), engine.sequence_groups_to_signature(reversed_group_order))

    def test_maximum_common_part_does_not_treat_reversed_group_order_as_full_match(self):
        engine = CutEngine()
        existing = [
            {"group_index": 0, "source_type": "text", "origin_frame_id": "f1", "tokens": ["A", "B"]},
            {"group_index": 1, "source_type": "text", "origin_frame_id": "f1", "tokens": ["C"]},
        ]
        incoming = [
            {"group_index": 0, "source_type": "text", "origin_frame_id": "f2", "tokens": ["C"]},
            {"group_index": 1, "source_type": "text", "origin_frame_id": "f2", "tokens": ["A", "B"]},
        ]
        result = engine.maximum_common_part(existing, incoming)
        self.assertLess(result["common_length"], 3)
        self.assertNotEqual(result["common_signature"], engine.sequence_groups_to_signature(existing))

    def test_csa_partial_overlap_keeps_bundle_only_when_anchor_and_attr_survive(self):
        engine = CutEngine()
        existing = [self._csa_group("A", ["x", "y"])]
        incoming = [self._csa_group("A", ["x"])]
        result = engine.maximum_common_part(existing, incoming)

        self.assertEqual(result["common_length"], 2)
        self.assertEqual(len(result["common_groups"]), 1)
        self.assertEqual(len(result["common_groups"][0]["csa_bundles"]), 1)
        self.assertEqual(result["common_groups"][0]["tokens"], ["A"])

        residual_existing = result["residual_existing_groups"][0]
        residual_tokens = [unit["token"] for unit in residual_existing["units"]]
        self.assertEqual(residual_tokens, ["y"])
        self.assertEqual(len(residual_existing["csa_bundles"]), 0)

    def test_csa_anchor_only_overlap_degrades_to_plain_sa(self):
        engine = CutEngine()
        existing = [self._csa_group("A", ["x"])]
        incoming = [
            {
                "group_index": 0,
                "source_type": "text",
                "origin_frame_id": "frame_0",
                "tokens": ["A"],
            }
        ]
        result = engine.maximum_common_part(existing, incoming)

        self.assertEqual(result["common_length"], 1)
        self.assertEqual(result["common_tokens"], ["A"])
        self.assertEqual(len(result["common_groups"][0]["csa_bundles"]), 0)
        self.assertEqual(result["residual_existing_groups"][0]["tokens"], ["x"])
        self.assertEqual(len(result["residual_existing_groups"][0]["csa_bundles"]), 0)

    def test_numeric_attribute_units_can_match_approximately_within_same_group(self):
        engine = CutEngine()
        engine.set_pointer_index(PointerIndex({}))
        existing = [
            {
                "group_index": 0,
                "source_type": "text",
                "origin_frame_id": "f1",
                "units": [
                    {
                        "unit_id": "feature_existing",
                        "token": "A",
                        "unit_role": "feature",
                        "sequence_index": 0,
                        "group_index": 0,
                        "source_group_index": 0,
                        "source_type": "text",
                        "origin_frame_id": "f1",
                        "display_visible": True,
                    },
                    {
                        "unit_id": "attr_existing",
                        "token": "stimulus_intensity:1.0",
                        "unit_role": "attribute",
                        "attribute_name": "stimulus_intensity",
                        "attribute_value": 1.0,
                        "sequence_index": 1,
                        "group_index": 0,
                        "source_group_index": 0,
                        "source_type": "text",
                        "origin_frame_id": "f1",
                        "display_visible": False,
                        "bundle_anchor_unit_id": "feature_existing",
                    },
                ],
                "csa_bundles": [
                    {
                        "bundle_id": "bundle_existing",
                        "anchor_unit_id": "feature_existing",
                        "member_unit_ids": ["feature_existing", "attr_existing"],
                    }
                ],
            }
        ]
        incoming = [
            {
                "group_index": 0,
                "source_type": "text",
                "origin_frame_id": "f2",
                "units": [
                    {
                        "unit_id": "feature_incoming",
                        "token": "A",
                        "unit_role": "feature",
                        "sequence_index": 0,
                        "group_index": 0,
                        "source_group_index": 0,
                        "source_type": "text",
                        "origin_frame_id": "f2",
                        "display_visible": True,
                    },
                    {
                        "unit_id": "attr_incoming",
                        "token": "stimulus_intensity:1.1",
                        "unit_role": "attribute",
                        "attribute_name": "stimulus_intensity",
                        "attribute_value": 1.1,
                        "sequence_index": 1,
                        "group_index": 0,
                        "source_group_index": 0,
                        "source_type": "text",
                        "origin_frame_id": "f2",
                        "display_visible": False,
                        "bundle_anchor_unit_id": "feature_incoming",
                    },
                ],
                "csa_bundles": [
                    {
                        "bundle_id": "bundle_incoming",
                        "anchor_unit_id": "feature_incoming",
                        "member_unit_ids": ["feature_incoming", "attr_incoming"],
                    }
                ],
            }
        ]

        result = engine.maximum_common_part(existing, incoming)

        self.assertEqual(result["common_length"], 2)
        self.assertEqual(result["matched_existing_unit_count"], 2)
        self.assertEqual(result["matched_incoming_unit_count"], 2)
        self.assertEqual(result["residual_existing_signature"], "")
        self.assertEqual(result["residual_incoming_signature"], "")
        self.assertNotEqual(result["common_signature"], engine.sequence_groups_to_signature(existing))
        common_units = result["common_groups"][0]["units"]
        self.assertTrue(any(str(unit.get("unit_signature", "")).startswith("AN:stimulus_intensity:") for unit in common_units))


if __name__ == '__main__':
    unittest.main()
