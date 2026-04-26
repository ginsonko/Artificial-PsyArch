# -*- coding: utf-8 -*-

import os
import shutil
import sys
import tempfile

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from observatory._app import ObservatoryApp
from observatory._render_html import export_cycle_html
from observatory._render_terminal import render_cycle_report
from state_pool._id_generator import reset_id_generator


@pytest.fixture
def app():
    reset_id_generator()
    temp_hdb_dir = tempfile.mkdtemp(prefix="observatory_cs_hdb_")
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
            "enable_cognitive_stitching": True,
            "enable_structure_level_retrieval_storage": False,
        }
    )
    yield instance
    instance.close()
    shutil.rmtree(temp_hdb_dir, ignore_errors=True)


def _seed_hdb_structure(app: ObservatoryApp, display: str, tokens: list[str]) -> dict:
    structure_obj, _ = app.hdb._structure_store.create_structure(
        structure_payload={
            "display_text": display,
            "flat_tokens": list(tokens),
            "sequence_groups": [
                {
                    "group_index": index,
                    "source_type": "current",
                    "origin_frame_id": f"seed_{display}",
                    "tokens": [token],
                }
                for index, token in enumerate(tokens)
            ],
            "content_signature": display,
            "base_weight": 1.0,
        },
        trace_id=f"seed_{display}",
    )
    return structure_obj


def _runtime_projection(structure_obj: dict, *, er: float, ev: float) -> dict:
    display = structure_obj.get("structure", {}).get("display_text", "") or structure_obj.get("id", "")
    return {
        "id": structure_obj["id"],
        "object_type": "st",
        "sub_type": structure_obj.get("sub_type", "stimulus_sequence_structure"),
        "content": {"raw": display, "display": display, "normalized": display},
        "energy": {"er": er, "ev": ev},
        "structure": dict(structure_obj.get("structure", {})),
    }


def _event_runtime_projection(
    component_refs: list[str],
    component_displays: list[str],
    *,
    er: float,
    ev: float,
    component_ledger: list[dict] | None = None,
) -> dict:
    display = " -> ".join(component_displays)
    ledger = list(component_ledger or [])
    if not ledger:
        share = round(1.0 / float(max(1, len(component_refs))), 8)
        ledger = [
            {
                "index": index,
                "ref_id": component_ref,
                "display": component_displays[index],
                "tokens": [component_displays[index]],
                "profile_share": share,
                "er": round(float(er) * share, 8),
                "ev": round(float(ev) * share, 8),
                "cp_abs": round(abs(float(er) * share - float(ev) * share), 8),
            }
            for index, component_ref in enumerate(component_refs)
        ]
    return {
        "id": "cs_event::" + "::".join(component_refs),
        "object_type": "st",
        "sub_type": "cognitive_stitching_event",
        "content": {"raw": display, "display": display, "normalized": display},
        "energy": {"er": er, "ev": ev},
        "structure": {
            "display_text": display,
            "flat_tokens": list(component_displays),
            "sequence_groups": [
                {
                    "group_index": index,
                    "source_type": "cognitive_stitching",
                    "origin_frame_id": "seed_event",
                    "tokens": [token],
                }
                for index, token in enumerate(component_displays)
            ],
            "member_refs": list(component_refs),
            "content_signature": "cs_event::" + "::".join(component_refs) + "|" + "|".join(component_refs),
            "ext": {
                "cognitive_stitching": {
                    "stage": "pytest_seed",
                    "component_count": len(component_refs),
                    "component_profile": [
                        {
                            "index": entry.get("index", index),
                            "ref_id": entry.get("ref_id", component_refs[index]),
                            "display": entry.get("display", component_displays[index]),
                            "share": entry.get("profile_share", 0.0),
                        }
                        for index, entry in enumerate(ledger)
                    ],
                    "component_ledger": ledger,
                }
            },
        },
    }


def _seed_hdb_event_structure(app: ObservatoryApp, *, member_refs: list[str], display: str) -> dict:
    event_ref_id = "cs_event::" + "::".join(member_refs)
    res = app.hdb.upsert_cognitive_stitching_event_structure(
        event_ref_id=event_ref_id,
        member_refs=list(member_refs),
        display_text=str(display or event_ref_id),
        diff_rows=None,
        trace_id="pytest_seed_cs_event",
        tick_id="pytest_seed_cs_event",
        reason="pytest_seed",
        max_diff_entries=0,
        link_members_to_event=True,
    )
    assert res["success"] is True
    sid = str((res.get("data", {}) or {}).get("structure_id", "") or "")
    assert sid
    obj = app.hdb._structure_store.get(sid) or {}
    assert str(obj.get("sub_type", "") or "") == "cognitive_stitching_event_structure"
    return obj


def test_run_cycle_surfaces_cognitive_stitching_report_and_narrative_view(app, tmp_path):
    structure_a = _seed_hdb_structure(app, "A", ["A"])
    structure_b = _seed_hdb_structure(app, "B", ["B"])
    add_edge = app.hdb._structure_store.add_diff_entry(
        owner_structure_id=structure_a["id"],
        target_id=structure_b["id"],
        content_signature=structure_b.get("structure", {}).get("content_signature", "B"),
        base_weight=1.2,
        entry_type="structure_ref",
        ext={"relation_type": "pytest_cs_pair"},
    )
    assert add_edge is not None

    inserted_a = app.pool.insert_runtime_node(
        _runtime_projection(structure_a, er=1.5, ev=0.2),
        trace_id="seed_runtime_a",
        source_module="pytest",
    )
    inserted_b = app.pool.insert_runtime_node(
        _runtime_projection(structure_b, er=1.1, ev=0.3),
        trace_id="seed_runtime_b",
        source_module="pytest",
    )
    assert inserted_a["success"] is True
    assert inserted_b["success"] is True

    report = app.run_cycle(text=None)

    cs = report["cognitive_stitching"]
    assert cs["enabled"] is True
    assert cs["action_count"] >= 1
    assert cs["created_count"] >= 1
    assert cs["narrative_top_items"]
    assert report["structure_level"]["result"]["message"] == "disabled_by_switch"
    # When structure-level retrieval is disabled, endogenous stimulus must still exist:
    # it is built from CAM (attention memory) and constrained by internal resolution (DARL+PARS).
    internal_pkt = report.get("internal_stimulus", {}) or {}
    assert int(internal_pkt.get("sa_count", 0) or 0) > 0
    internal_resolution = (report.get("structure_level", {}) or {}).get("result", {}).get("internal_resolution", {}) or {}
    assert int(internal_resolution.get("detail_budget", 0) or 0) > 0
    assert int(internal_resolution.get("raw_unit_count", 0) or 0) > 0
    assert any(str(a.get("event_structure_id", "") or "").startswith("st_") for a in cs.get("actions", []))
    first_sid = next((str(a.get("event_structure_id", "") or "") for a in cs.get("actions", []) if str(a.get("event_structure_id", "") or "")), "")
    assert first_sid
    persisted = app.hdb._structure_store.get(first_sid) or {}
    assert str(persisted.get("sub_type", "") or "") == "cognitive_stitching_event_structure"

    terminal_report = render_cycle_report(report)
    assert "Cognitive Stitching" in terminal_report

    html_path = export_cycle_html(report, tmp_path / "cognitive_stitching_report.html")
    with open(html_path, "r", encoding="utf-8") as fh:
        html_text = fh.read()
    assert "CS narrative top" in html_text


def test_run_cycle_can_extend_existing_cs_event_seed(app):
    structure_a = _seed_hdb_structure(app, "A", ["A"])
    structure_b = _seed_hdb_structure(app, "B", ["B"])
    structure_c = _seed_hdb_structure(app, "C", ["C"])
    add_edge = app.hdb._structure_store.add_diff_entry(
        owner_structure_id=structure_b["id"],
        target_id=structure_c["id"],
        content_signature=structure_c.get("structure", {}).get("content_signature", "C"),
        base_weight=1.25,
        entry_type="structure_ref",
        ext={"relation_type": "pytest_cs_extend"},
    )
    assert add_edge is not None

    event_ab_obj = _seed_hdb_event_structure(app, member_refs=[structure_a["id"], structure_b["id"]], display="A -> B")
    event_ab_runtime = app.hdb.make_runtime_structure_object(event_ab_obj["id"], er=1.6, ev=0.2, reason="pytest_seed_event")
    assert isinstance(event_ab_runtime, dict)
    inserted_event = app.pool.insert_runtime_node(
        event_ab_runtime,
        trace_id="seed_runtime_event_ab",
        source_module="pytest",
    )
    inserted_c = app.pool.insert_runtime_node(
        _runtime_projection(structure_c, er=1.2, ev=0.2),
        trace_id="seed_runtime_c",
        source_module="pytest",
    )
    assert inserted_event["success"] is True
    assert inserted_c["success"] is True

    report = app.run_cycle(text=None)

    cs = report["cognitive_stitching"]
    assert cs["enabled"] is True
    assert cs["extended_count"] >= 1
    assert any(action.get("action_family") == "extend_event" for action in cs.get("actions", []))

    expected_ref_id = f"cs_event::{structure_a['id']}::{structure_b['id']}::{structure_c['id']}"
    assert any(str(action.get("event_ref_id", "") or "") == expected_ref_id for action in cs.get("actions", []))
    candidates = list(app.hdb._pointer_index.query_candidates_by_signature(expected_ref_id))
    assert candidates
    assert any(
        str((app.hdb._structure_store.get(cid) or {}).get("sub_type", "") or "") == "cognitive_stitching_event_structure"
        and str(((app.hdb._structure_store.get(cid) or {}).get("structure", {}) or {}).get("content_signature", "") or "") == expected_ref_id
        for cid in candidates
    )


def test_cache_neutralization_reports_event_component_complement(app):
    inserted_event = app.pool.insert_runtime_node(
        _event_runtime_projection(
            ["st_a", "st_b"],
            ["A", "B"],
            er=0.2,
            ev=1.8,
            component_ledger=[
                {"index": 0, "ref_id": "st_a", "display": "A", "tokens": ["A"], "profile_share": 0.1, "er": 0.2, "ev": 0.0, "cp_abs": 0.2},
                {"index": 1, "ref_id": "st_b", "display": "B", "tokens": ["B"], "profile_share": 0.9, "er": 0.0, "ev": 1.8, "cp_abs": 1.8},
            ],
        ),
        trace_id="seed_runtime_event_ab_component",
        source_module="pytest",
    )
    assert inserted_event["success"] is True

    packet = {
        "id": "pkt_b_verify",
        "object_type": "stimulus_packet",
        "sa_items": [
            {
                "id": "sa_b",
                "object_type": "sa",
                "content": {"raw": "B", "display": "B", "value_type": "discrete"},
                "stimulus": {"role": "feature", "modality": "text"},
                "energy": {"er": 1.0, "ev": 0.0},
                "ext": {"packet_context": {"group_index": 0, "sequence_index": 0, "source_type": "current"}},
                "created_at": 0,
                "updated_at": 0,
            }
        ],
        "csa_items": [],
        "grouped_sa_sequences": [
            {"group_index": 0, "source_type": "current", "origin_frame_id": "pkt_b_verify", "sa_ids": ["sa_b"], "csa_ids": []},
        ],
        "energy_summary": {"total_er": 1.0, "total_ev": 0.0, "current_total_er": 1.0, "current_total_ev": 0.0},
        "trace_id": "pkt_b_verify",
    }

    cache = app._neutralize_packet_against_pool(packet, trace_id="cache_component_trace", tick_id="cache_component_tick")

    assert cache["priority_summary"]["priority_neutralized_item_count"] == 1
    assert cache["priority_summary"]["event_component_neutralization_count"] == 1
    assert cache["priority_summary"]["event_component_cp_drop_sum"] > 0.0
    assert cache["priority_diagnostics"]
    assert cache["priority_diagnostics"][0]["neutralization_mode"] == "event_component_complementary"
    assert "B" in cache["priority_diagnostics"][0]["matched_components"]
