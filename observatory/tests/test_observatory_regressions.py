# -*- coding: utf-8 -*-

from __future__ import annotations

from pathlib import Path

from observatory._app import ObservatoryApp
from observatory._render_html import export_cycle_html
from observatory._render_terminal import render_cycle_report


def _sa(unit_id: str, token: str, seq: int, *, role: str = "feature", attribute_name: str = "", attribute_value=None) -> dict:
    return {
        "unit_id": unit_id,
        "token": token,
        "display_text": token,
        "sequence_index": seq,
        "unit_role": role,
        "role": role,
        "attribute_name": attribute_name,
        "attribute_value": attribute_value,
        "object_type": "sa",
        "display_visible": True,
    }


def _stimulus_group(group_index: int, units: list[dict], bundles: list[dict] | None = None) -> dict:
    return {
        "group_index": group_index,
        "source_type": "current",
        "origin_frame_id": f"frame_{group_index}",
        "units": units,
        "csa_bundles": bundles or [],
    }


def _structure_unit(structure_id: str, nested_groups: list[dict], seq: int = 0) -> dict:
    return {
        "unit_id": structure_id,
        "object_type": "st",
        "unit_signature": f"ST:{structure_id}",
        "sequence_index": seq,
        "structure_display_text": structure_id,
        "structure_grouped_display_text": "",
        "structure_sequence_groups": nested_groups,
    }


def _placeholder(token: str, seq: int = 0) -> dict:
    return {
        "unit_id": f"placeholder_{seq}",
        "object_type": "st_placeholder",
        "is_placeholder": True,
        "token": token,
        "display_text": token,
        "sequence_index": seq,
    }


def test_clear_all_resets_cached_reports_and_memory_pool():
    app = ObservatoryApp(
        config_override={
            "history_limit": 4,
            "export_html": False,
            "export_json": False,
        }
    )
    try:
        report = app.run_cycle("你好")
        assert report["trace_id"]
        assert app._last_report is not None
        assert app._report_history

        app.clear_all()

        assert app._last_report is None
        assert app._report_history == []
        snapshot = app.hdb.get_memory_activation_snapshot(trace_id="after_clear")["data"]
        assert snapshot["summary"]["count"] == 0
    finally:
        app.close()


def test_run_cycle_accepts_empty_text_without_crash():
    """Regression: empty string input should not crash the Observatory run_cycle()."""
    app = ObservatoryApp(
        config_override={
            "history_limit": 2,
            "export_html": False,
            "export_json": False,
        }
    )
    try:
        report = app.run_cycle("")
        assert report.get("trace_id")
        # tick_id is required for cross-module observability (even if sensor input is invalid).
        assert report.get("tick_id")
        sensor = report.get("sensor", {}) or {}
        assert sensor.get("success") is False
        assert sensor.get("code") in {"INPUT_VALIDATION_ERROR", "OK"}  # keep tolerant to future allow_empty_text
    finally:
        app.close()


def test_memory_feedback_only_uses_new_activation_delta():
    app = ObservatoryApp(
        config_override={
            "history_limit": 4,
            "export_html": False,
            "export_json": False,
        }
    )
    try:
        report = app.run_cycle("你好")
        first_feedback_count = report.get("memory_feedback", {}).get("applied_count", 0)
        snapshot = app.hdb.get_memory_activation_snapshot(trace_id="feedback_snapshot")["data"]
        items = snapshot.get("items", [])
        if not items:
            return
        for item in items:
            item["last_delta_er"] = 0.0
            item["last_delta_ev"] = 0.0
            app.hdb._memory_activation_store._persist_item(item)
            app.hdb._memory_activation_store._items[item["memory_id"]] = item

        no_feedback = app._apply_memory_feedback(memory_items=items, trace_id="manual_feedback_check", tick_id="manual_feedback_check")
        assert no_feedback["applied_count"] == 0
        assert no_feedback["total_feedback_er"] == 0.0
        assert no_feedback["total_feedback_ev"] == 0.0
        assert first_feedback_count >= 0
    finally:
        app.close()


def test_observatory_renders_semantic_notation_without_double_braces(tmp_path):
    stim_group0 = _stimulus_group(
        0,
        [
            _sa("u_you", "你", 0),
            _sa("u_intensity", "stimulus_intensity:1.1", 1, role="attribute", attribute_name="stimulus_intensity", attribute_value=1.1),
        ],
        bundles=[
            {
                "bundle_id": "b_you_intensity",
                "anchor_unit_id": "u_you",
                "member_unit_ids": ["u_you", "u_intensity"],
            }
        ],
    )
    stim_group1 = _stimulus_group(1, [_sa("u_is", "是", 0)])
    st_who = _structure_unit("st_who", [_stimulus_group(0, [_sa("who", "谁", 0)])], seq=0)
    st_is = _structure_unit("st_is", [_stimulus_group(0, [_sa("is", "是", 0)])], seq=1)

    structure_groups = [
        {
            "group_index": 0,
            "source_type": "group",
            "origin_frame_id": "sg_demo",
            "units": [st_who, st_is],
            "csa_bundles": [],
        }
    ]
    selected_structure = {
        "structure_id": "st_phrase",
        "display_text": "legacy_structure",
        "sequence_groups": [stim_group0, stim_group1],
        "common_part": {"common_groups": [stim_group0]},
    }
    merged_groups = [
        {
            "group_index": 0,
            "source_type": "current",
            "tokens": ["你", "stimulus_intensity:1.1"],
            "sa_count": 2,
            "csa_count": 1,
            "csa_bundles": ["(你 + stimulus_intensity:1.1)"],
            "csa_bundle_defs": list(stim_group0.get("csa_bundles", [])),
            "units": list(stim_group0.get("units", [])),
            "sequence_groups": [stim_group0],
        },
        {
            "group_index": 1,
            "source_type": "current",
            "tokens": ["是"],
            "sa_count": 1,
            "csa_count": 0,
            "csa_bundles": [],
            "csa_bundle_defs": [],
            "units": list(stim_group1.get("units", [])),
            "sequence_groups": [stim_group1],
        },
    ]
    report = {
        "trace_id": "semantic_notation_demo",
        "sensor": {
            "groups": merged_groups,
            "units": [],
            "feature_units": [],
            "mode": "advanced",
            "tokenizer_backend": "jieba",
            "sa_count": 3,
            "csa_count": 1,
            "input_text": "你是",
            "normalized_text": "你是",
            "echo_frames_used": [],
            "fatigue_summary": {},
        },
        "maintenance": {"before_summary": {}, "after_summary": {}, "summary": {}, "events": []},
        "attention": {"top_items": [], "structure_items": []},
        "structure_level": {
            "result": {
                "cam_stub_count": 2,
                "round_count": 1,
                "matched_group_ids": ["sg_demo"],
                "new_group_ids": [],
                "fallback_used": False,
                "debug": {
                    "cam_items": [
                        {"structure_id": "st_who", "display_text": "legacy", "sequence_groups": [_stimulus_group(0, [_sa("who", "谁", 0)])], "er": 0.5, "ev": 0.0, "total_energy": 0.5, "base_weight": 1.0, "recent_gain": 1.0, "fatigue": 0.0},
                    ],
                    "round_details": [
                        {
                            "round_index": 1,
                            "anchor": {"structure_id": "st_who", "display_text": "legacy", "sequence_groups": [_stimulus_group(0, [_sa("who", "谁", 0)])], "anchor_score": 1.0, "er": 0.5, "ev": 0.0},
                            "budget_before": {"st_who": {"er": 0.5, "ev": 0.0, "total": 0.5}},
                            "budget_after": {"st_who": {"er": 0.2, "ev": 0.0, "total": 0.2}},
                            "chain_steps": [],
                            "selected_group": {
                                "group_id": "sg_demo",
                                "display_text": "legacy_group",
                                "sequence_groups": structure_groups,
                                "score": 1.0,
                                "base_similarity": 1.0,
                                "coverage_ratio": 1.0,
                                "structure_ratio": 1.0,
                                "wave_similarity": 1.0,
                                "required_structures": [{"structure_id": "st_who", "display_text": "谁"}, {"structure_id": "st_is", "display_text": "是"}],
                                "bias_structures": [],
                                "common_part": {"common_groups": structure_groups},
                            },
                            "storage_summary": {
                                "owner_display_text": "sg_demo",
                                "owner_kind": "sg",
                                "resolved_db_id": "sdb_demo",
                                "new_group_ids": [],
                                "new_structure_ids": [],
                                "actions": [
                                    {
                                        "type": "append_raw_residual",
                                        "type_zh": "追加原始残差信息",
                                        "storage_table": "local_db.residual_table",
                                        "storage_table_zh": "结构组本地库.残差表",
                                        "entry_id": "sgr_demo",
                                        "raw_display_text": "legacy_raw",
                                        "raw_sequence_groups": [
                                            {
                                                "group_index": 0,
                                                "source_type": "group",
                                                "origin_frame_id": "sg_demo",
                                                "units": [_placeholder("SELF[sg_demo]", 0), st_who],
                                                "csa_bundles": [],
                                            }
                                        ],
                                        "canonical_display_text": "legacy_canonical",
                                        "canonical_sequence_groups": structure_groups,
                                        "memory_id": "em_demo",
                                    }
                                ],
                            },
                            "candidate_groups": [],
                            "internal_fragments": [],
                        }
                    ],
                    "new_group_details": [],
                },
            }
        },
        "cache_neutralization": {
            "priority_summary": {"priority_neutralized_item_count": 0, "priority_event_count": 0, "consumed_er": 0.0, "consumed_ev": 0.0, "input_flat_token_count": 0, "residual_flat_token_count": 0},
            "input_packet": {"display_text": "legacy_input", "sequence_groups": [stim_group0, stim_group1], "total_er": 1.0, "total_ev": 0.0, "flat_tokens": []},
            "residual_packet": {"display_text": "legacy_residual", "sequence_groups": [stim_group1], "total_er": 0.3, "total_ev": 0.0, "flat_tokens": []},
            "priority_events": [],
        },
        "merged_stimulus": {
            "display_text": "legacy_merged",
            "sequence_groups": [stim_group0, stim_group1],
            "total_er": 1.0,
            "total_ev": 0.0,
            "groups": merged_groups,
        },
        "stimulus_level": {
            "result": {
                "round_count": 1,
                "matched_structure_ids": ["st_phrase"],
                "new_structure_ids": [],
                "remaining_stimulus_sa_count": 0,
                "fallback_used": False,
                "runtime_projection_structures": [],
                "debug": {
                    "round_details": [
                        {
                            "round_index": 1,
                            "anchor": {"display_text": "你", "token": "你", "source_type": "current", "group_index": 0, "sequence_index": 0, "er": 0.8, "ev": 0.0},
                            "focus_group_text_before": "legacy_focus",
                            "focus_group_sequence_groups_before": [stim_group0],
                            "remaining_grouped_text_before": "legacy_remaining_before",
                            "remaining_sequence_groups_before": [stim_group0, stim_group1],
                            "remaining_grouped_text_after": "legacy_remaining_after",
                            "remaining_sequence_groups_after": [stim_group1],
                            "chain_steps": [],
                            "candidate_details": [
                                {
                                    "structure_id": "st_phrase",
                                    "display_text": "legacy_structure",
                                    "sequence_groups": [stim_group0, stim_group1],
                                    "eligible": True,
                                    "exact_match": True,
                                    "full_structure_included": True,
                                    "competition_score": 0.9,
                                    "stimulus_match_ratio": 0.9,
                                    "structure_match_ratio": 1.0,
                                    "chain_depth": 0,
                                    "owner_structure_id": "",
                                    "match_mode": "candidate_match",
                                    "common_part": {"common_groups": [stim_group0]},
                                }
                            ],
                            "selected_match": {**selected_structure, "competition_score": 0.9, "match_score": 0.9, "coverage_ratio": 0.9, "structure_match_ratio": 1.0, "exact_match": True, "full_structure_included": True},
                            "effective_transfer_fraction": 0.9,
                            "transferred_er": 0.9,
                            "transferred_ev": 0.0,
                            "created_common_structure": selected_structure,
                            "created_residual_structure": {"structure_id": "st_is", "display_text": "legacy_is", "sequence_groups": [stim_group1]},
                            "created_fresh_structure": None,
                        }
                    ]
                },
            }
        },
        "pool_apply": {
            "apply_result": {"new_item_count": 0, "updated_item_count": 0, "merged_item_count": 0, "neutralized_item_count": 0, "state_delta_summary": {"total_delta_er": 0.0, "total_delta_ev": 0.0}},
            "priority_summary": {"priority_neutralized_item_count": 0, "priority_event_count": 0, "consumed_er": 0.0, "consumed_ev": 0.0, "input_flat_token_count": 0, "residual_flat_token_count": 0},
            "input_packet": {"display_text": "legacy_input", "sequence_groups": [stim_group0, stim_group1], "total_er": 1.0, "total_ev": 0.0, "flat_tokens": []},
            "residual_packet": {"display_text": "legacy_residual", "sequence_groups": [stim_group1], "total_er": 0.3, "total_ev": 0.0, "flat_tokens": []},
            "priority_events": [],
            "bias_projection": [],
            "runtime_projection": [],
            "events": [],
        },
        "induction": {
            "result": {"source_item_count": 0, "propagated_target_count": 0, "induced_target_count": 0, "total_delta_ev": 0.0, "total_ev_consumed": 0.0, "debug": {"source_details": []}},
            "applied_targets": [],
        },
        "final_state": {
            "state_snapshot": {"summary": {"active_item_count": 0, "high_er_item_count": 0, "high_ev_item_count": 0, "high_cp_item_count": 0, "object_type_counts": {}}, "top_items": []},
            "state_energy_summary": {"total_er": 0.0, "total_ev": 0.0, "total_cp": 0.0, "energy_by_type": {}},
            "hdb_snapshot": {"summary": {"structure_count": 0, "group_count": 0, "episodic_count": 0, "issue_count": 0, "active_repair_job_count": 0}, "recent_structures": []},
        },
        "exports": {},
    }

    terminal_report = render_cycle_report(report)
    assert "记号说明 / Notation" in terminal_report
    assert "{你 + stimulus_intensity:1.1 + (你 + stimulus_intensity:1.1)} || {是}" in terminal_report
    assert "{[{谁}] / [{是}]}" in terminal_report
    assert "{{谁}}" not in terminal_report

    html_path = export_cycle_html(report, tmp_path / "semantic_notation.html")
    html_text = Path(html_path).read_text(encoding="utf-8")
    assert "记号说明 / Notation" in html_text
    assert "{[{谁}] / [{是}]}" in html_text
    assert "{你 + stimulus_intensity:1.1 + (你 + stimulus_intensity:1.1)} || {是}" in html_text
    assert "{{谁}}" not in html_text
