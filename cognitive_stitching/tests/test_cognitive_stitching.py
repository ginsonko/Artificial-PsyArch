# -*- coding: utf-8 -*-

from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from cognitive_stitching.main import CognitiveStitchingEngine
from state_pool import StatePool


class _FakeStore:
    def __init__(self):
        self.by_ref: dict[str, dict] = {}

    def get_by_ref(self, ref_id: str):
        return self.by_ref.get(ref_id)


class _FakePool:
    def __init__(self, items: list[dict]):
        self._items = list(items)
        self._store = _FakeStore()
        self.energy_calls: list[dict] = []
        self.insert_calls: list[dict] = []

    def get_state_snapshot(self, *, trace_id: str, tick_id: str, top_k=None, sort_by="cp_abs", **kwargs):
        del trace_id, tick_id, sort_by, kwargs
        items = list(self._items[: top_k or None])
        return {"success": True, "data": {"snapshot": {"summary": {"active_item_count": len(items)}, "top_items": items}}}

    def apply_energy_update(self, *, target_item_id: str, delta_er: float, delta_ev: float, trace_id: str, tick_id: str, reason: str, source_module: str, **kwargs):
        del trace_id, tick_id, source_module, kwargs
        self.energy_calls.append(
            {
                "target_item_id": target_item_id,
                "delta_er": float(delta_er),
                "delta_ev": float(delta_ev),
                "reason": reason,
            }
        )
        for item in self._items:
            if item.get("item_id") != target_item_id:
                continue
            item["er"] = round(max(0.0, float(item.get("er", 0.0)) + float(delta_er)), 8)
            item["ev"] = round(max(0.0, float(item.get("ev", 0.0)) + float(delta_ev)), 8)
            item["cp_abs"] = round(abs(float(item["er"]) - float(item["ev"])), 8)
            item["salience_score"] = round(max(float(item["er"]), float(item["ev"])), 8)
            break
        return {"success": True, "data": {"after": {}}}

    def insert_runtime_node(self, *, runtime_object: dict, trace_id: str, tick_id: str, allow_merge: bool, source_module: str, reason: str):
        del trace_id, tick_id, allow_merge, source_module, reason
        event_item = {
            "item_id": f"spi_{runtime_object['id']}",
            "ref_object_id": runtime_object["id"],
            "ref_object_type": runtime_object["object_type"],
            "display": runtime_object.get("content", {}).get("display", runtime_object["id"]),
            "er": float(runtime_object.get("energy", {}).get("er", 0.0)),
            "ev": float(runtime_object.get("energy", {}).get("ev", 0.0)),
            "cp_abs": abs(float(runtime_object.get("energy", {}).get("er", 0.0)) - float(runtime_object.get("energy", {}).get("ev", 0.0))),
            "salience_score": max(float(runtime_object.get("energy", {}).get("er", 0.0)), float(runtime_object.get("energy", {}).get("ev", 0.0))),
        }
        self._items.insert(0, event_item)
        self._store.by_ref[runtime_object["id"]] = {"id": event_item["item_id"], "ref_object_id": runtime_object["id"]}
        self.insert_calls.append(runtime_object)
        return {"success": True, "data": {"item_id": event_item["item_id"]}}


class _FakeStructureStore:
    def __init__(self, structures: dict[str, dict], dbs: dict[str, dict]):
        self._structures = structures
        self._dbs = dbs

    def get(self, structure_id: str):
        return self._structures.get(structure_id)

    def get_db_by_owner(self, structure_id: str):
        return self._dbs.get(structure_id)


class _FakeWeight:
    @staticmethod
    def compute_runtime_weight(*, base_weight: float, recent_gain: float, fatigue: float) -> float:
        return float(base_weight) * float(recent_gain) / (1.0 + float(fatigue))


class _FakeHDB:
    def __init__(self, structures: dict[str, dict], dbs: dict[str, dict]):
        self._structure_store = _FakeStructureStore(structures, dbs)
        self._weight = _FakeWeight()


def _structure(structure_id: str, display: str, tokens: list[str]) -> dict:
    return {
        "id": structure_id,
        "object_type": "st",
        "structure": {
            "display_text": display,
            "flat_tokens": list(tokens),
            "content_signature": display,
        },
        "stats": {"base_weight": 1.0, "recent_gain": 1.0, "fatigue": 0.0},
    }


def _event_runtime_object(component_ids: list[str], component_displays: list[str], *, er: float, ev: float) -> dict:
    event_ref_id = "cs_event::" + "::".join(component_ids)
    display = " -> ".join(component_displays)
    return {
        "id": event_ref_id,
        "object_type": "st",
        "sub_type": "cognitive_stitching_event",
        "content": {"raw": display, "display": display, "normalized": display},
        "energy": {"er": er, "ev": ev},
    }


def test_disabled_mode_is_noop():
    engine = CognitiveStitchingEngine(config_override={"enabled": False})
    pool = _FakePool(items=[])
    hdb = _FakeHDB(structures={}, dbs={})

    result = engine.run(pool=pool, hdb=hdb, trace_id="cs_disabled", tick_id="cs_disabled")

    assert result["success"] is True
    assert result["data"]["enabled"] is False
    assert result["data"]["action_count"] == 0
    assert not pool.energy_calls
    assert not pool.insert_calls


def test_empty_cognitive_stitching_meta_container_is_not_treated_as_event():
    """
    Regression:
    Some runtime paths may create an empty `meta.ext.cognitive_stitching` dict container.
    The CS engine must NOT treat "any dict" as an event marker, otherwise plain ST items
    will be incorrectly shown in the CS narrative panel.
    """
    engine = CognitiveStitchingEngine(config_override={"enabled": True})
    item = {
        "ref_object_type": "st",
        "ref_object_id": "st_plain",
        "ref_snapshot": {"structure_ext": {}},
        "meta": {"ext": {"cognitive_stitching": {}}},  # empty container should not count
    }
    assert engine._is_cognitive_stitching_event_state_item(item) is False


def test_enabled_mode_creates_event_from_diff_table_edge():
    structures = {
        "st_a": _structure("st_a", "A", ["A"]),
        "st_b": _structure("st_b", "B", ["B"]),
    }
    dbs = {
        "st_a": {
            "diff_table": [
                {
                    "target_id": "st_b",
                    "base_weight": 1.2,
                    "entry_type": "structure_ref",
                }
            ]
        }
    }
    pool = _FakePool(
        items=[
            {"item_id": "spi_a", "ref_object_id": "st_a", "ref_object_type": "st", "display": "A", "er": 1.5, "ev": 0.3, "cp_abs": 1.2, "salience_score": 1.5},
            {"item_id": "spi_b", "ref_object_id": "st_b", "ref_object_type": "st", "display": "B", "er": 1.1, "ev": 0.2, "cp_abs": 0.9, "salience_score": 1.1},
        ]
    )
    hdb = _FakeHDB(structures=structures, dbs=dbs)
    engine = CognitiveStitchingEngine(config_override={"enabled": True, "max_events_per_tick": 1, "min_candidate_score": 0.05})

    result = engine.run(pool=pool, hdb=hdb, trace_id="cs_enabled", tick_id="cs_enabled")

    assert result["success"] is True
    assert result["data"]["action_count"] == 1
    assert result["data"]["created_count"] == 1
    assert pool.insert_calls
    assert len(pool.energy_calls) >= 2
    assert pool.insert_calls[0]["structure"]["member_refs"] == ["st_a", "st_b"]
    assert len(pool.insert_calls[0]["structure"]["ext"]["cognitive_stitching"]["component_ledger"]) == 2
    assert result["data"]["narrative_top_items"]


def test_existing_event_can_extend_to_new_right_end():
    structures = {
        "st_a": _structure("st_a", "A", ["A"]),
        "st_b": _structure("st_b", "B", ["B"]),
        "st_c": _structure("st_c", "C", ["C"]),
    }
    dbs = {
        "st_b": {
            "diff_table": [
                {
                    "target_id": "st_c",
                    "base_weight": 1.3,
                    "entry_type": "structure_ref",
                }
            ]
        }
    }
    event_runtime = _event_runtime_object(["st_a", "st_b"], ["A", "B"], er=1.5, ev=0.2)
    pool = _FakePool(
        items=[
            {"item_id": "spi_event_ab", "ref_object_id": event_runtime["id"], "ref_object_type": "st", "display": "A -> B", "er": 1.5, "ev": 0.2, "cp_abs": 1.3, "salience_score": 1.5},
            {"item_id": "spi_c", "ref_object_id": "st_c", "ref_object_type": "st", "display": "C", "er": 1.2, "ev": 0.2, "cp_abs": 1.0, "salience_score": 1.2},
        ]
    )
    pool._store.by_ref[event_runtime["id"]] = {"id": "spi_event_ab", "ref_object_id": event_runtime["id"]}
    hdb = _FakeHDB(structures=structures, dbs=dbs)
    engine = CognitiveStitchingEngine(
        config_override={
            "enabled": True,
            "max_events_per_tick": 1,
            "min_candidate_score": 0.05,
            "enable_event_extend": True,
            "enable_event_merge": False,
        }
    )

    result = engine.run(pool=pool, hdb=hdb, trace_id="cs_extend", tick_id="cs_extend")

    assert result["success"] is True
    assert result["data"]["extended_count"] == 1
    assert pool.insert_calls
    assert any(call.get("id") == "cs_event::st_a::st_b::st_c" for call in pool.insert_calls)
    assert result["data"]["actions"][0]["action_family"] == "extend_event"


def test_event_to_event_bridge_merge_creates_longer_event():
    structures = {
        "st_a": _structure("st_a", "A", ["A"]),
        "st_b": _structure("st_b", "B", ["B"]),
        "st_c": _structure("st_c", "C", ["C"]),
        "st_d": _structure("st_d", "D", ["D"]),
        "st_e": _structure("st_e", "E", ["E"]),
        "st_f": _structure("st_f", "F", ["F"]),
        "st_g": _structure("st_g", "G", ["G"]),
        "st_h": _structure("st_h", "H", ["H"]),
    }
    dbs = {
        "st_d": {
            "diff_table": [
                {
                    "target_id": "st_e",
                    "base_weight": 1.25,
                    "entry_type": "structure_ref",
                }
            ]
        }
    }
    left_event = _event_runtime_object(["st_a", "st_b", "st_c", "st_d"], ["A", "B", "C", "D"], er=1.8, ev=0.3)
    right_event = _event_runtime_object(["st_e", "st_f", "st_g", "st_h"], ["E", "F", "G", "H"], er=1.6, ev=0.3)
    pool = _FakePool(
        items=[
            {"item_id": "spi_left", "ref_object_id": left_event["id"], "ref_object_type": "st", "display": "A -> B -> C -> D", "er": 1.8, "ev": 0.3, "cp_abs": 1.5, "salience_score": 1.8},
            {"item_id": "spi_right", "ref_object_id": right_event["id"], "ref_object_type": "st", "display": "E -> F -> G -> H", "er": 1.6, "ev": 0.3, "cp_abs": 1.3, "salience_score": 1.6},
        ]
    )
    pool._store.by_ref[left_event["id"]] = {"id": "spi_left", "ref_object_id": left_event["id"]}
    pool._store.by_ref[right_event["id"]] = {"id": "spi_right", "ref_object_id": right_event["id"]}
    hdb = _FakeHDB(structures=structures, dbs=dbs)
    engine = CognitiveStitchingEngine(
        config_override={
            "enabled": True,
            "max_events_per_tick": 1,
            "min_candidate_score": 0.05,
            "enable_event_extend": False,
            "enable_event_merge": True,
        }
    )

    result = engine.run(pool=pool, hdb=hdb, trace_id="cs_merge", tick_id="cs_merge")

    assert result["success"] is True
    assert result["data"]["merged_count"] == 1
    assert pool.insert_calls
    assert any(call.get("id") == "cs_event::st_a::st_b::st_c::st_d::st_e::st_f::st_g::st_h" for call in pool.insert_calls)
    assert result["data"]["actions"][0]["action_family"] == "merge_event"


def test_run_event_grasp_binds_attribute_to_event_in_cam():
    engine = CognitiveStitchingEngine(config_override={"enabled": True})
    pool = StatePool(config_override={"log_dir": ""})

    event_ref_id = "cs_event::st_a::st_b"
    runtime_object = {
        "id": event_ref_id,
        "object_type": "st",
        "sub_type": "cognitive_stitching_event",
        "content": {"raw": "A -> B", "display": "A -> B", "normalized": "A -> B"},
        "energy": {"er": 1.0, "ev": 0.2, "ownership_level": "aggregated_from_st", "computed_from_children": True},
        "structure": {
            "display_text": "A -> B",
            "flat_tokens": ["A", "B"],
            "token_count": 2,
            "sequence_groups": [
                {"group_index": 0, "tokens": ["A"]},
                {"group_index": 1, "tokens": ["B"]},
            ],
            "member_refs": ["st_a", "st_b"],
            "content_signature": event_ref_id,
            "semantic_signature": event_ref_id,
            "ext": {
                "cognitive_stitching": {
                    "member_refs": ["st_a", "st_b"],
                    "component_profile": [
                        {"index": 0, "ref_id": "st_a", "display": "A", "share": 0.55},
                        {"index": 1, "ref_id": "st_b", "display": "B", "share": 0.45},
                    ],
                    "component_ledger": [
                        {"index": 0, "ref_id": "st_a", "display": "A", "tokens": ["A"], "profile_share": 0.55, "er": 0.6, "ev": 0.1, "cp_abs": 0.5},
                        {"index": 1, "ref_id": "st_b", "display": "B", "tokens": ["B"], "profile_share": 0.45, "er": 0.4, "ev": 0.1, "cp_abs": 0.3},
                    ],
                }
            },
        },
        "source": {"parent_ids": ["st_a", "st_b"]},
    }
    insert_res = pool.insert_runtime_node(
        runtime_object=runtime_object,
        trace_id="eg_insert",
        tick_id="eg_insert",
        allow_merge=False,
        source_module="test",
        reason="insert_event",
    )
    item_id = str(insert_res.get("data", {}).get("item_id", "") or "")
    assert item_id

    attention_snapshot = {
        "top_items": [
            {"item_id": item_id, "ref_object_type": "st", "ref_object_id": event_ref_id},
        ]
    }
    res = engine.run_event_grasp(pool=pool, attention_snapshot=attention_snapshot, trace_id="eg", tick_id="eg")
    assert res["success"] is True
    assert res["data"]["emitted_count"] == 1

    state_item = pool._store.get(item_id)
    bound = list((state_item.get("ext", {}) or {}).get("bound_attributes", []) or [])
    assert any((a.get("content", {}) or {}).get("attribute_name") == "event_grasp" for a in bound)

    narrative = engine._collect_narrative_top_items(pool=pool, trace_id="eg", tick_id="eg")
    assert narrative
    assert narrative[0].get("event_grasp", 0.0) > 0.0


def test_idle_consolidate_materializes_esdb_overlay():
    structures = {
        "st_a": _structure("st_a", "A", ["A"]),
        "st_b": _structure("st_b", "B", ["B"]),
        "st_c": _structure("st_c", "C", ["C"]),
    }
    dbs = {
        # Create event: st_a -> st_b
        # Overlay to materialize: st_a/st_b -> st_c
        "st_a": {"diff_table": [
            {"target_id": "st_b", "base_weight": 1.2, "entry_type": "structure_ref"},
            {"target_id": "st_c", "base_weight": 1.0, "entry_type": "structure_ref"},
        ]},
        "st_b": {"diff_table": [{"target_id": "st_c", "base_weight": 0.8, "entry_type": "structure_ref"}]},
    }
    pool = _FakePool(
        items=[
            {"item_id": "spi_a", "ref_object_id": "st_a", "ref_object_type": "st", "display": "A", "er": 1.4, "ev": 0.2, "cp_abs": 1.2, "salience_score": 1.4},
            {"item_id": "spi_b", "ref_object_id": "st_b", "ref_object_type": "st", "display": "B", "er": 1.2, "ev": 0.2, "cp_abs": 1.0, "salience_score": 1.2},
        ]
    )
    hdb = _FakeHDB(structures=structures, dbs=dbs)
    engine = CognitiveStitchingEngine(config_override={"enabled": True, "max_events_per_tick": 1, "min_candidate_score": 0.05})

    run_res = engine.run(pool=pool, hdb=hdb, trace_id="cs_esdb", tick_id="cs_esdb")
    assert run_res["success"] is True
    assert run_res["data"]["created_count"] == 1

    cons = engine.idle_consolidate(hdb=hdb, trace_id="cs_esdb_cons", tick_id="cs_esdb_cons")
    assert cons["success"] is True

    event_id = "cs_event::st_a::st_b"
    assert event_id in engine._esdb
    es_entry = engine._esdb[event_id]
    assert es_entry.get("materialized") is True
    assert es_entry.get("parents") == []
    mat = list(es_entry.get("materialized_diff_table", []) or [])
    assert any(row.get("target_id") == "st_c" for row in mat)


def test_materialized_esdb_overlay_still_merges_delta_updates():
    structures = {
        "st_a": _structure("st_a", "A", ["A"]),
        "st_b": _structure("st_b", "B", ["B"]),
        "st_c": _structure("st_c", "C", ["C"]),
        "st_d": _structure("st_d", "D", ["D"]),
    }
    dbs = {
        "st_a": {"diff_table": [{"target_id": "st_b", "base_weight": 1.2, "entry_type": "structure_ref"}]},
        "st_b": {"diff_table": [{"target_id": "st_c", "base_weight": 1.0, "entry_type": "structure_ref"}]},
        "st_c": {"diff_table": [{"target_id": "st_d", "base_weight": 1.5, "entry_type": "structure_ref"}]},
    }
    pool = _FakePool(
        items=[
            {"item_id": "spi_a", "ref_object_id": "st_a", "ref_object_type": "st", "display": "A", "er": 1.4, "ev": 0.2, "cp_abs": 1.2, "salience_score": 1.4},
            {"item_id": "spi_b", "ref_object_id": "st_b", "ref_object_type": "st", "display": "B", "er": 1.2, "ev": 0.2, "cp_abs": 1.0, "salience_score": 1.2},
        ]
    )
    hdb = _FakeHDB(structures=structures, dbs=dbs)
    engine = CognitiveStitchingEngine(config_override={"enabled": True, "max_events_per_tick": 1, "min_candidate_score": 0.05})

    run_res = engine.run(pool=pool, hdb=hdb, trace_id="cs_esdb2", tick_id="cs_esdb2")
    assert run_res["success"] is True
    assert run_res["data"]["created_count"] == 1

    cons = engine.idle_consolidate(hdb=hdb, trace_id="cs_esdb2_cons", tick_id="cs_esdb2_cons")
    assert cons["success"] is True

    event_id = "cs_event::st_a::st_b"
    assert event_id in engine._esdb
    assert engine._esdb[event_id].get("materialized") is True

    # After consolidation, delta can still grow; overlay open should merge it.
    engine._esdb_delta_upsert_edge(
        event_ref_id=event_id,
        target_id="st_d",
        base_weight=9.0,
        source_ref="st_c",
        tick_id="cs_esdb2_delta",
        action_type="extend_event",
        distance=0,
    )
    rows, total = engine._esdb_open_overlay_top_diff_entries(event_ref_id=event_id, hdb=hdb)
    assert total > 0.0
    assert any(r.get("target_id") == "st_d" for r in rows)
