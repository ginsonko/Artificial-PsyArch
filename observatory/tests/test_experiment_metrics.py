# -*- coding: utf-8 -*-

from __future__ import annotations

from observatory.experiment.metrics import extract_tick_metrics


def test_extract_tick_metrics_distinguishes_action_attempts_and_successes():
    report = {
        "trace_id": "trace_demo",
        "tick_id": "cycle_0003",
        "started_at": 1,
        "finished_at": 2,
        "sensor": {},
        "final_state": {"state_snapshot": {"summary": {}}, "state_energy_summary": {}, "hdb_snapshot": {"summary": {}}},
        "attention": {},
        "maintenance": {},
        "structure_level": {"result": {}},
        "stimulus_level": {"result": {}},
        "internal_stimulus": {},
        "merged_stimulus": {},
        "cache_neutralization": {},
        "pool_apply": {},
        "induction": {"result": {}},
        "memory_activation": {"snapshot": {"summary": {}, "items": []}, "apply_result": {}, "feedback_result": {}},
        "cognitive_feeling": {"cfs_signals": []},
        "emotion": {"nt_state_after": {}},
        "action": {
            "executed_actions": [
                {"action_kind": "recall", "success": False, "attempted": True},
                {"action_kind": "recall", "success": True},
                {"action_kind": "weather_stub", "success": True, "attempted": False},
                {"action_kind": "attention_focus", "success": True},
            ],
            "nodes": [],
        },
        "timing": {"steps_ms": {}},
        "time_sensor": {},
    }

    metrics = extract_tick_metrics(report=report, dataset_tick={"tick_index": 3, "input_text": "", "input_is_empty": True})

    assert metrics["action_attempted_count"] == 3
    assert metrics["action_attempted_recall"] == 2
    assert metrics["action_scheduled_weather_stub"] == 1
    assert metrics["action_executed_count"] == 3
    assert metrics["action_executed_recall"] == 1
    assert metrics["action_executed_attention_focus"] == 1
