# -*- coding: utf-8 -*-
"""
Tests for the IESM declarative rules engine.
IESM 声明式规则引擎测试。

Why / 为什么要测：
- Prototype stage changes quickly; tests keep the core rule semantics stable.
  原型阶段迭代快，用测试锁定核心语义，避免回归。
"""

from __future__ import annotations

from innate_script._rules_engine import evaluate_rules, normalize_rules_doc


def _normalize(raw: dict) -> dict:
    doc, errors, _warnings = normalize_rules_doc(raw)
    assert not errors, f"unexpected errors: {errors}"
    return doc


def test_parse_tick_index_via_evaluate_rules() -> None:
    doc = _normalize(
        {
            "rules_schema_version": "1.0",
            "rules_version": "t",
            "enabled": True,
            "defaults": {},
            "rules": [
                {
                    "id": "noop",
                    "title": "noop",
                    "enabled": True,
                    "priority": 1,
                    "cooldown_ticks": 0,
                    "when": {"timer": {"at_tick": 999999}},
                    "then": [{"log": "never"}],
                    "note": "",
                }
            ],
        }
    )

    engine = evaluate_rules(
        doc=doc,
        trace_id="t",
        tick_id="cycle_0012",
        tick_index=None,
        cfs_signals=[],
        state_windows=[],
        now_ms=None,
        runtime_state={},
    )
    assert engine.get("tick_index") == 12


def test_cfs_focus_directive_happy_path() -> None:
    doc = _normalize(
        {
            "rules_schema_version": "1.0",
            "rules_version": "t",
            "enabled": True,
            "defaults": {"focus_directive": {"ttl_ticks": 2, "focus_boost": 0.9, "deduplicate_by": "target_ref_object_id"}},
            "rules": [
                {
                    "id": "focus_on_dissonance",
                    "title": "CFS -> focus",
                    "enabled": True,
                    "priority": 10,
                    "cooldown_ticks": 0,
                    "when": {"cfs": {"kinds": ["dissonance"], "min_strength": 0.3}},
                    "then": [{"focus": {"from": "cfs_matches", "match_policy": "all"}}],
                    "note": "",
                }
            ],
        }
    )

    engine = evaluate_rules(
        doc=doc,
        trace_id="t",
        tick_id="cycle_0001",
        tick_index=None,
        cfs_signals=[
            {
                "kind": "dissonance",
                "strength": 0.5,
                "target": {"target_ref_object_id": "st_0001", "target_ref_object_type": "st", "target_display": "test"},
                "reasons": ["cp_abs>=threshold"],
            }
        ],
        state_windows=[],
        now_ms=123,
        runtime_state={},
    )

    directives = (engine.get("directives") or {}).get("focus_directives") or []
    assert len(directives) == 1
    d0 = directives[0]
    assert d0.get("directive_type") == "attention_focus"
    assert d0.get("source_kind") == "dissonance"
    assert d0.get("target_ref_object_id") == "st_0001"
    assert d0.get("ttl_ticks") == 2


def test_allow_timer_flag_disables_timer_predicates() -> None:
    doc = _normalize(
        {
            "rules_schema_version": "1.0",
            "rules_version": "t",
            "enabled": True,
            "defaults": {},
            "rules": [
                {
                    "id": "timer_test",
                    "title": "Timer",
                    "enabled": True,
                    "priority": 10,
                    "cooldown_ticks": 0,
                    "when": {"timer": {"at_tick": 1}},
                    "then": [{"log": "hi"}],
                    "note": "",
                }
            ],
        }
    )

    engine_on = evaluate_rules(
        doc=doc,
        trace_id="t",
        tick_id="cycle_0001",
        tick_index=None,
        cfs_signals=[],
        state_windows=[],
        now_ms=None,
        runtime_state={},
        allow_timer=True,
    )
    assert len(engine_on.get("triggered_rules") or []) == 1

    engine_off = evaluate_rules(
        doc=doc,
        trace_id="t",
        tick_id="cycle_0001",
        tick_index=None,
        cfs_signals=[],
        state_windows=[],
        now_ms=None,
        runtime_state={},
        allow_timer=False,
    )
    assert len(engine_off.get("triggered_rules") or []) == 0


def test_cooldown_ticks_bookkeeping() -> None:
    doc = _normalize(
        {
            "rules_schema_version": "1.0",
            "rules_version": "t",
            "enabled": True,
            "defaults": {},
            "rules": [
                {
                    "id": "cooldown_rule",
                    "title": "Cooldown rule",
                    "enabled": True,
                    "priority": 10,
                    "cooldown_ticks": 3,
                    "when": {"timer": {"every_n_ticks": 1}},
                    "then": [{"log": "fire"}],
                    "note": "",
                }
            ],
        }
    )

    runtime_state: dict = {}
    fired_ticks: list[int] = []
    for tick in (1, 2, 3, 4):
        engine = evaluate_rules(
            doc=doc,
            trace_id="t",
            tick_id=f"cycle_{tick:04d}",
            tick_index=tick,
            cfs_signals=[],
            state_windows=[],
            now_ms=None,
            runtime_state=runtime_state,
            allow_timer=True,
        )
        if engine.get("triggered_rules"):
            fired_ticks.append(tick)

    # With cooldown=3, it should fire at tick=1 and tick=4.
    # cooldown=3 时，应在 tick=1 和 tick=4 触发。
    assert fired_ticks == [1, 4]


def test_state_window_predicate_and_emit_script_action() -> None:
    doc = _normalize(
        {
            "rules_schema_version": "1.0",
            "rules_version": "t",
            "enabled": True,
            "defaults": {},
            "rules": [
                {
                    "id": "sw_emit",
                    "title": "StateWindow -> emit_script",
                    "enabled": True,
                    "priority": 10,
                    "cooldown_ticks": 0,
                    "when": {"state_window": {"stage": "maintenance", "fast_cp_rise_min": 2}},
                    "then": [{"emit_script": {"script_id": "innate_state_window_cp_rise", "script_kind": "window_trigger", "trigger": "fast_cp_rise"}}],
                    "note": "",
                }
            ],
        }
    )

    packet = {"summary": {"fast_cp_rise_item_count": 2, "fast_cp_drop_item_count": 0}, "candidate_triggers": []}
    engine = evaluate_rules(
        doc=doc,
        trace_id="t",
        tick_id="cycle_0001",
        tick_index=1,
        cfs_signals=[],
        state_windows=[{"stage": "maintenance", "packet": packet}],
        now_ms=100,
        runtime_state={},
    )
    scripts = engine.get("triggered_scripts") or []
    assert len(scripts) == 1
    assert scripts[0].get("script_id") == "innate_state_window_cp_rise"


def test_focus_from_state_window_candidates() -> None:
    doc = _normalize(
        {
            "rules_schema_version": "1.0",
            "rules_version": "t",
            "enabled": True,
            "defaults": {"focus_directive": {"ttl_ticks": 2, "focus_boost": 0.9, "deduplicate_by": "target_item_id"}},
            "rules": [
                {
                    "id": "sw_focus",
                    "title": "StateWindow candidates -> focus",
                    "enabled": True,
                    "priority": 10,
                    "cooldown_ticks": 0,
                    "when": {"state_window": {"stage": "any", "fast_cp_rise_min": 1}},
                    "then": [{"focus": {"from": "state_window_candidates", "match_policy": "all", "deduplicate_by": "target_item_id"}}],
                    "note": "",
                }
            ],
        }
    )

    packet = {
        "summary": {"fast_cp_rise_item_count": 1, "fast_cp_drop_item_count": 0},
        "candidate_triggers": [
            {"item_id": "spi_0001", "trigger_hint": "fast_cp_rise", "value": 0.7, "display": "candidate A"},
        ],
    }
    engine = evaluate_rules(
        doc=doc,
        trace_id="t",
        tick_id="cycle_0001",
        tick_index=1,
        cfs_signals=[],
        state_windows=[{"stage": "maintenance", "packet": packet}],
        now_ms=100,
        runtime_state={},
    )
    directives = (engine.get("directives") or {}).get("focus_directives") or []
    assert len(directives) == 1
    assert directives[0].get("target_item_id") == "spi_0001"

