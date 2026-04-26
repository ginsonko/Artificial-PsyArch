# -*- coding: utf-8 -*-

import os
import shutil
import sys
import tempfile
from pathlib import Path

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from observatory._app import ObservatoryApp
from observatory._render_html import export_cycle_html
from observatory._render_terminal import render_cycle_report
from state_pool._id_generator import reset_id_generator


HI = "\u4f60\u597d"
YA = "\u5440"
QUESTION = "\uff1f"


def _seed_structure():
    return {
        "id": "st_seed_hi_ya",
        "object_type": "st",
        "sub_type": "stimulus_sequence_structure",
        "content": {"raw": f"{HI}/{YA}", "display": f"{HI}/{YA}", "normalized": f"{HI}/{YA}"},
        "energy": {"er": 0.0, "ev": 2.0},
        "structure": {
            "display_text": f"{HI}/{YA}",
            "flat_tokens": [HI, YA],
            "sequence_groups": [
                {"group_index": 0, "source_type": "current", "origin_frame_id": "seed", "tokens": list(HI + YA)},
            ],
        },
    }


@pytest.fixture
def app():
    reset_id_generator()
    temp_hdb_dir = tempfile.mkdtemp(prefix="observatory_hdb_")
    instance = ObservatoryApp(
        config_override={
            "export_html": False,
            "export_json": False,
            "auto_open_html_report": False,
            "web_auto_open_browser": False,
            "state_pool_enable_placeholder_interfaces": False,
            "state_pool_enable_script_broadcast": False,
            "hdb_enable_background_repair": False,
            "hdb_data_dir": temp_hdb_dir,
        }
    )
    yield instance
    instance.close()
    shutil.rmtree(temp_hdb_dir, ignore_errors=True)


def test_apply_packet_to_pool_observes_priority_neutralization(app):
    insert_result = app.pool.insert_runtime_node(_seed_structure(), trace_id="seed_trace", source_module="pytest")
    assert insert_result["success"] is True

    packet = app.sensor.ingest_text(text=f"{HI}{YA}{QUESTION}", trace_id="sensor_trace", tick_id="sensor_tick")["data"]["stimulus_packet"]
    packet_total_er = float(packet.get("energy_summary", {}).get("total_er", 0.0))

    app._run_state_pool_maintenance("cycle_trace", "cycle_tick")
    app._build_attention_memory_stub("cycle_trace", "cycle_tick")
    apply_result, events, residual_packet = app._apply_packet_to_pool(packet, "cycle_trace", "cycle_tick")

    assert apply_result["priority_neutralized_item_count"] == 1
    assert any(event.get("event_type") == "priority_stimulus_neutralization" for event in events)
    assert float(residual_packet.get("energy_summary", {}).get("total_er", 0.0)) < packet_total_er


def test_run_cycle_report_surfaces_priority_neutralization(app, tmp_path):
    insert_result = app.pool.insert_runtime_node(_seed_structure(), trace_id="seed_cycle_trace", source_module="pytest")
    assert insert_result["success"] is True

    report = app.run_cycle(text=f"{HI}{YA}{QUESTION}")
    pool_apply = report["pool_apply"]

    assert pool_apply["priority_summary"]["priority_neutralized_item_count"] == 1
    assert len(pool_apply["priority_events"]) == 1
    assert pool_apply["input_packet"]["total_er"] > pool_apply["residual_packet"]["total_er"]

    terminal_report = render_cycle_report(report)
    assert "priority_stimulus_neutralization" in terminal_report
    assert "matched_sig=" in terminal_report

    html_path = export_cycle_html(report, tmp_path / "priority_report.html")
    with open(html_path, "r", encoding="utf-8") as fh:
        html_text = fh.read()

    assert "priority neutralization events" in html_text
    assert "priority neutralization packet delta" in html_text


def test_run_cycle_maintains_separate_memory_activation_pool(app):
    report = app.run_cycle(text=f"{HI}{YA}!")
    memory_activation = report["memory_activation"]

    assert memory_activation["apply_result"]["applied_count"] > 0
    assert memory_activation["snapshot"]["summary"]["count"] > 0
    assert not any(
        item.get("ref_object_type") == "em"
        for item in report["final_state"]["state_snapshot"].get("top_items", [])
    )


def test_memory_feedback_stimulus_packet_projects_er_and_ev_without_em_runtime_nodes(app):
    clear_result = app.pool.clear_state_pool(trace_id="stimulus_feedback_pool_clear", reason="unit_test_reset")
    assert clear_result["success"] is True

    grouped_text = f"{{{HI} + stimulus_intensity:1.1}} / {{{YA}}}"
    append_result = app.hdb.append_episodic_memory(
        episodic_payload={
            "event_summary": "stimulus_feedback_memory",
            "structure_refs": [],
            "group_refs": [],
            "meta": {
                "ext": {
                    "display_text": grouped_text,
                    "memory_material": {
                        "memory_kind": "stimulus_packet",
                        "grouped_display_text": grouped_text,
                        "sequence_groups": [
                            {
                                "group_index": 0,
                                "source_group_index": 0,
                                "origin_frame_id": "seed_frame_0",
                                "units": [
                                    {
                                        "unit_id": "unit_hi",
                                        "token": HI,
                                        "display_text": HI,
                                        "sequence_index": 0,
                                        "unit_role": "anchor",
                                        "role": "anchor",
                                        "value_type": "discrete",
                                    },
                                    {
                                        "unit_id": "unit_energy",
                                        "token": "stimulus_intensity:1.1",
                                        "display_text": "stimulus_intensity:1.1",
                                        "sequence_index": 1,
                                        "unit_role": "attribute",
                                        "role": "attribute",
                                        "attribute_name": "stimulus_intensity",
                                        "attribute_value": 1.1,
                                        "value_type": "numerical",
                                    },
                                ],
                                "csa_bundles": [
                                    {
                                        "bundle_id": "bundle_hi_energy",
                                        "anchor_unit_id": "unit_hi",
                                        "member_unit_ids": ["unit_hi", "unit_energy"],
                                    }
                                ],
                            },
                            {
                                "group_index": 1,
                                "source_group_index": 1,
                                "origin_frame_id": "seed_frame_1",
                                "units": [
                                    {
                                        "unit_id": "unit_ya",
                                        "token": YA,
                                        "display_text": YA,
                                        "sequence_index": 0,
                                        "unit_role": "anchor",
                                        "role": "anchor",
                                        "value_type": "discrete",
                                    }
                                ],
                                "csa_bundles": [],
                            },
                        ],
                        "unit_energy_profile": {
                            "unit_hi": 0.5,
                            "unit_energy": 0.3,
                            "unit_ya": 0.2,
                        },
                        "group_energy_profile": {"0": 0.8, "1": 0.2},
                    },
                }
            },
        },
        trace_id="append_stimulus_feedback_memory",
    )
    assert append_result["success"] is True
    memory_id = append_result["data"]["episodic_id"]

    apply_result = app.hdb.apply_memory_activation_targets(
        targets=[
            {
                "projection_kind": "memory",
                "memory_id": memory_id,
                "target_display_text": grouped_text,
                "delta_er": 0.6,
                "delta_ev": 0.4,
                "sources": ["st_seed_feedback"],
                "modes": ["manual_recall"],
            }
        ],
        trace_id="apply_stimulus_feedback_memory",
    )
    assert apply_result["success"] is True

    memory_item = app.hdb.query_memory_activation(
        memory_id=memory_id,
        trace_id="query_stimulus_feedback_memory",
    )["data"]["item"]
    feedback_result = app._apply_memory_feedback(
        memory_items=[memory_item],
        trace_id="stimulus_feedback",
        tick_id="stimulus_feedback",
    )

    assert feedback_result["applied_count"] == 1
    assert feedback_result["total_feedback_er"] == pytest.approx(0.6, abs=1e-6)
    assert feedback_result["total_feedback_ev"] == pytest.approx(0.4, abs=1e-6)
    assert feedback_result["items"][0]["packet"]["total_er"] == pytest.approx(0.6, abs=1e-6)
    assert feedback_result["items"][0]["packet"]["total_ev"] == pytest.approx(0.4, abs=1e-6)

    state_data = app.get_state_snapshot_data(top_k=32)
    assert state_data["energy_summary"]["total_er"] == pytest.approx(0.6, abs=1e-6)
    assert state_data["energy_summary"]["total_ev"] == pytest.approx(0.4, abs=1e-6)
    assert not any(
        item.get("ref_object_type") == "em"
        for item in state_data["snapshot"].get("top_items", [])
    )
    assert all(
        item.get("ref_object_type") == "sa"
        for item in state_data["snapshot"].get("top_items", [])
    )


def test_memory_feedback_structure_group_projects_er_and_ev_without_em_runtime_nodes(app):
    app.run_cycle(text=f"{HI}{YA}!")
    hdb_snapshot = app.hdb.get_hdb_snapshot(trace_id="seed_structure_hdb")["data"]
    structure_id = hdb_snapshot["recent_structures"][0]["structure_id"]
    assert structure_id

    clear_result = app.pool.clear_state_pool(trace_id="structure_feedback_pool_clear", reason="unit_test_reset")
    assert clear_result["success"] is True

    append_result = app.hdb.append_episodic_memory(
        episodic_payload={
            "event_summary": "structure_feedback_memory",
            "structure_refs": [structure_id],
            "group_refs": [],
            "meta": {
                "ext": {
                    "display_text": HI,
                    "memory_material": {
                        "memory_kind": "structure_group",
                        "grouped_display_text": HI,
                        "structure_refs": [structure_id],
                        "structure_items": [
                            {
                                "structure_id": structure_id,
                                "display_text": HI,
                                "grouped_display_text": HI,
                            }
                        ],
                        "structure_energy_profile": {structure_id: 1.0},
                    },
                }
            },
        },
        trace_id="append_structure_feedback_memory",
    )
    assert append_result["success"] is True
    memory_id = append_result["data"]["episodic_id"]

    apply_result = app.hdb.apply_memory_activation_targets(
        targets=[
            {
                "projection_kind": "memory",
                "memory_id": memory_id,
                "target_display_text": HI,
                "delta_er": 0.75,
                "delta_ev": 0.25,
                "sources": [structure_id],
                "modes": ["manual_recall"],
            }
        ],
        trace_id="apply_structure_feedback_memory",
    )
    assert apply_result["success"] is True

    memory_item = app.hdb.query_memory_activation(
        memory_id=memory_id,
        trace_id="query_structure_feedback_memory",
    )["data"]["item"]
    feedback_result = app._apply_memory_feedback(
        memory_items=[memory_item],
        trace_id="structure_feedback",
        tick_id="structure_feedback",
    )

    assert feedback_result["applied_count"] == 1
    assert feedback_result["total_feedback_er"] == pytest.approx(0.75, abs=1e-6)
    assert feedback_result["total_feedback_ev"] == pytest.approx(0.25, abs=1e-6)

    state_data = app.get_state_snapshot_data(top_k=32)
    assert state_data["energy_summary"]["total_er"] == pytest.approx(0.75, abs=1e-6)
    assert state_data["energy_summary"]["total_ev"] == pytest.approx(0.25, abs=1e-6)
    assert not any(
        item.get("ref_object_type") == "em"
        for item in state_data["snapshot"].get("top_items", [])
    )
    assert any(
        item.get("ref_object_type") == "st"
        for item in state_data["snapshot"].get("top_items", [])
    )


def test_observatory_static_flow_order_matches_runtime():
    app_js = Path(__file__).resolve().parents[1] / "web_static" / "app.js"
    html_renderer = Path(__file__).resolve().parents[1] / "_render_html.py"

    app_js_text = app_js.read_text(encoding="utf-8")
    html_text = html_renderer.read_text(encoding="utf-8")

    # Frontend flow blocks may be renumbered when optional stages (e.g. Cognitive Stitching) are enabled/disabled.
    # 前端流程块的序号可能会因为“可选阶段开关”（例如认知拼接）而变化，因此这里按标题关键词做顺序约束，不依赖固定编号。
    assert app_js_text.index("缓存中和") < app_js_text.index("刺激级查存一体")
    assert app_js_text.index("刺激级查存一体") < app_js_text.index("状态池回写与结构投影")
    assert html_text.rindex("<a href='#cache'>") < html_text.rindex("<a href='#stimulus'>")
    assert html_text.rindex("<a href='#stimulus'>") < html_text.rindex("<a href='#projection'>")
    # NOTE:
    # We allow a bounded setInterval for the Action/Drive runtime monitoring auto-refresh.
    # 允许用于行动模块运行态监控的 setInterval（可启动/可停止），以满足“实时监控”验收需求。
    assert "setInterval(" in app_js_text
    assert "clearInterval(" in app_js_text
    assert "actionRuntimeAutoTimer" in app_js_text
    # The UI title is Chinese-first but should still contain the bilingual keyword.
    # UI 标题中文优先，但仍应包含双语关键词，便于稳定检索与回归测试。
    assert ("记忆反哺" in app_js_text) and ("Memory Feedback" in app_js_text)
    assert app_js_text.count("function renderSettingsInput(") == 1
    assert app_js_text.count("function fmtMemoryActivationCard(") == 1
    assert app_js_text.count("async function refreshDashboard(") == 1


def test_observatory_renders_grouped_csa_and_structure_storage_details(tmp_path):
    grouped_text = f"[{HI} + stimulus_intensity:1.1] / [{YA} + stimulus_intensity:1.1] / !"
    report = {
        "trace_id": "obs_dbg_001",
        "sensor": {},
        "maintenance": {"before_summary": {}, "after_summary": {}, "summary": {}, "events": []},
        "attention": {"top_items": [], "structure_items": []},
        "structure_level": {
            "result": {
                "cam_stub_count": 1,
                "round_count": 1,
                "matched_group_ids": [],
                "new_group_ids": [],
                "fallback_used": False,
                "debug": {
                    "cam_items": [
                        {
                            "structure_id": "st_000239",
                            "display_text": HI,
                            "grouped_display_text": HI,
                            "er": 0.4988,
                            "ev": 0.0,
                            "total_energy": 0.4988,
                        }
                    ],
                    "round_details": [
                        {
                            "round_index": 1,
                            "anchor": {
                                "structure_id": "st_000239",
                                "display_text": HI,
                                "grouped_display_text": HI,
                                "anchor_score": 1.0,
                            },
                            "budget_before": {
                                "st_000239": {"er": 0.4988, "ev": 0.0, "total": 0.4988},
                            },
                            "budget_after": {
                                "st_000239": {"er": 0.4988, "ev": 0.0, "total": 0.4988},
                            },
                            "chain_steps": [
                                {"owner_kind": "st", "owner_id": "st_000239", "owner_display_text": HI, "candidate_count": 0}
                            ],
                            "selected_group": {
                                "group_id": "sg_single_st_000239",
                                "display_text": HI,
                                "grouped_display_text": HI,
                                "score": 1.0,
                                "base_similarity": 1.0,
                                "coverage_ratio": 1.0,
                                "structure_ratio": 1.0,
                                "wave_similarity": 1.0,
                                "required_structures": [{"structure_id": "st_000239", "display_text": HI}],
                                "bias_structures": [],
                                "common_part": {"common_display": HI},
                            },
                            "storage_summary": {
                                "owner_display_text": HI,
                                "owner_kind": "st",
                                "resolved_db_id": "sdb_000239",
                                "new_group_ids": [],
                                "new_structure_ids": [],
                                "actions": [
                                    {
                                        "type": "append_raw_residual",
                                        "type_zh": "追加原始残差信息",
                                        "storage_table": "group_residual_table",
                                        "storage_table_zh": "结构数据库.结构组残差表",
                                        "entry_id": "sgr_000001",
                                        "raw_display_text": f"SELF[st_000239:{HI}] / {YA} / stimulus_intensity:1.1",
                                        "canonical_display_text": f"{HI} / {YA} / stimulus_intensity:1.1",
                                        "memory_id": "em_000128",
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
            "priority_summary": {
                "priority_neutralized_item_count": 0,
                "priority_event_count": 0,
                "consumed_er": 0.0,
                "consumed_ev": 0.0,
                "input_flat_token_count": 0,
                "residual_flat_token_count": 0,
            },
            "input_packet": {"display_text": grouped_text, "total_er": 2.2, "total_ev": 0.0, "flat_tokens": []},
            "residual_packet": {"display_text": grouped_text, "total_er": 2.2, "total_ev": 0.0, "flat_tokens": []},
            "priority_events": [],
        },
        "merged_stimulus": {
            "display_text": grouped_text,
            "total_er": 2.2,
            "total_ev": 0.0,
            "groups": [
                {
                    "group_index": 0,
                    "source_type": "current",
                    "display_text": f"[{HI} + stimulus_intensity:1.1]",
                    "tokens": [HI, "stimulus_intensity:1.1"],
                    "sa_count": 2,
                    "csa_count": 1,
                    "csa_bundles": [f"CSA[{HI} + stimulus_intensity:1.1]"],
                },
                {
                    "group_index": 1,
                    "source_type": "current",
                    "display_text": f"[{YA} + stimulus_intensity:1.1]",
                    "tokens": [YA, "stimulus_intensity:1.1"],
                    "sa_count": 2,
                    "csa_count": 1,
                    "csa_bundles": [f"CSA[{YA} + stimulus_intensity:1.1]"],
                },
                {
                    "group_index": 2,
                    "source_type": "current",
                    "display_text": "[!]",
                    "tokens": ["!"],
                    "sa_count": 1,
                    "csa_count": 0,
                    "csa_bundles": [],
                },
            ],
        },
        "stimulus_level": {
            "result": {
                "round_count": 1,
                "matched_structure_ids": ["st_000245"],
                "new_structure_ids": ["st_000247"],
                "remaining_stimulus_sa_count": 0,
                "fallback_used": False,
                "runtime_projection_structures": [
                    {
                        "projection_kind": "memory",
                        "memory_id": "em_000128",
                        "display_text": grouped_text,
                        "er": 0.6,
                        "ev": 0.0,
                        "reason": "matched_structure",
                    }
                ],
                "debug": {
                    "round_details": [
                        {
                            "round_index": 1,
                            "anchor": {
                                "display_text": HI,
                                "token": HI,
                                "source_type": "current",
                                "group_index": 0,
                                "sequence_index": 0,
                                "er": 1.1,
                                "ev": 0.0,
                            },
                            "focus_group_text_before": f"[{HI} + stimulus_intensity:1.1]",
                            "remaining_grouped_text_before": grouped_text,
                            "remaining_grouped_text_after": grouped_text,
                            "chain_steps": [{"owner_display_text": HI, "owner_structure_id": "st_000239", "candidate_count": 0}],
                            "candidate_details": [
                                {
                                    "structure_id": "st_000245",
                                    "display_text": grouped_text,
                                    "grouped_display_text": grouped_text,
                                    "eligible": True,
                                    "exact_match": True,
                                    "full_structure_included": True,
                                    "competition_score": 0.88,
                                    "stimulus_match_ratio": 0.88,
                                    "structure_match_ratio": 1.0,
                                    "chain_depth": 0,
                                    "owner_structure_id": "",
                                    "match_mode": "candidate_match",
                                    "common_part": {"common_display": grouped_text},
                                }
                            ],
                            "selected_match": {
                                "structure_id": "st_000245",
                                "display_text": grouped_text,
                                "grouped_display_text": grouped_text,
                                "competition_score": 0.88,
                                "match_score": 0.88,
                                "coverage_ratio": 0.88,
                                "structure_match_ratio": 1.0,
                                "exact_match": True,
                                "full_structure_included": True,
                                "match_mode": "candidate_match",
                                "common_part": {"common_display": grouped_text},
                            },
                            "effective_transfer_fraction": 0.88,
                            "transferred_er": 1.936,
                            "transferred_ev": 0.0,
                            "created_common_structure": {
                                "structure_id": "st_000245",
                                "display_text": grouped_text,
                                "grouped_display_text": grouped_text,
                            },
                            "created_residual_structure": {
                                "structure_id": "st_000247",
                                "display_text": grouped_text,
                                "grouped_display_text": grouped_text,
                            },
                            "created_fresh_structure": None,
                        }
                    ]
                },
            }
        },
        "pool_apply": {
            "apply_result": {
                "new_item_count": 1,
                "updated_item_count": 0,
                "merged_item_count": 0,
                "neutralized_item_count": 0,
                "state_delta_summary": {"total_delta_er": 0.6, "total_delta_ev": 0.0},
            },
            "priority_summary": {
                "priority_neutralized_item_count": 0,
                "priority_event_count": 0,
                "consumed_er": 0.0,
                "consumed_ev": 0.0,
                "input_flat_token_count": 0,
                "residual_flat_token_count": 0,
            },
            "input_packet": {"display_text": grouped_text, "total_er": 2.2, "total_ev": 0.0, "flat_tokens": []},
            "residual_packet": {"display_text": grouped_text, "total_er": 2.2, "total_ev": 0.0, "flat_tokens": []},
            "priority_events": [],
            "bias_projection": [],
            "runtime_projection": [
                {
                    "projection_kind": "memory",
                    "memory_id": "em_000128",
                    "display_text": grouped_text,
                    "er": 0.6,
                    "ev": 0.0,
                    "reason": "stimulus_runtime_memory",
                    "result": "inserted",
                }
            ],
            "events": [],
        },
        "induction": {
            "result": {
                "source_item_count": 1,
                "propagated_target_count": 0,
                "induced_target_count": 1,
                "total_delta_ev": 0.5,
                "total_ev_consumed": 0.0,
                "debug": {
                    "source_details": [
                        {
                            "source_structure_id": "st_000245",
                            "display_text": grouped_text,
                            "source_er": 1.0,
                            "source_ev": 0.0,
                            "candidate_entries": [
                                {
                                    "mode": "er_induction",
                                    "projection_kind": "memory",
                                    "memory_id": "em_000128",
                                    "target_display_text": grouped_text,
                                    "normalized_share": 1.0,
                                    "entry_count": 1,
                                    "delta_ev": 0.5,
                                    "runtime_weight": 1.2,
                                    "base_weight": 1.1,
                                    "recent_gain": 1.0,
                                    "fatigue": 0.0,
                                }
                            ],
                        }
                    ]
                },
            },
            "applied_targets": [
                {
                    "projection_kind": "memory",
                    "memory_id": "em_000128",
                    "display_text": grouped_text,
                    "ev": 0.5,
                    "result": "applied",
                }
            ],
        },
        "final_state": {
            "state_snapshot": {"summary": {"active_item_count": 0, "high_er_item_count": 0, "high_ev_item_count": 0, "high_cp_item_count": 0, "object_type_counts": {}}, "top_items": []},
            "state_energy_summary": {"total_er": 0.0, "total_ev": 0.0, "total_cp": 0.0, "energy_by_type": {}},
            "hdb_snapshot": {"summary": {"structure_count": 0, "group_count": 0, "episodic_count": 0, "issue_count": 0, "active_repair_job_count": 0}, "recent_structures": []},
        },
        "exports": {},
    }

    terminal_report = render_cycle_report(report)
    assert "局部库动作 / Local DB Action" in terminal_report
    assert "原始残差/Raw=SELF[st_000239:你好] / 呀 / stimulus_intensity:1.1" in terminal_report
    assert "还原后/Canonical=你好 / 呀 / stimulus_intensity:1.1" in terminal_report
    assert "关联记忆/em_id=em_000128" in terminal_report
    assert f"轮前残余 / Remaining before={grouped_text}" in terminal_report
    assert f"新建共同结构 / New common structure={grouped_text}[st_000245]" in terminal_report
    assert "类型/Kind=记忆 / Memory" in terminal_report

    html_path = export_cycle_html(report, tmp_path / "grouped_observatory.html")
    html_text = Path(html_path).read_text(encoding="utf-8")
    assert "局部库动作 / Local DB Action" in html_text
    assert "写入动作 / Write actions" in html_text
    assert "em_000128" in html_text
    assert grouped_text in html_text
    assert "记忆 / Memory" in html_text
