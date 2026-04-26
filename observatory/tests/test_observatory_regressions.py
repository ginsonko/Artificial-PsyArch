# -*- coding: utf-8 -*-

from __future__ import annotations

from observatory._app import ObservatoryApp


def test_clear_all_resets_cached_reports_and_runtime_state():
    app = ObservatoryApp(
        config_override={
            "history_limit": 4,
            "export_html": False,
            "export_json": False,
        }
    )
    try:
        report = app.run_cycle("你好")
        assert report
        assert app._last_report is not None
        assert app._report_history

        app.time_sensor._delayed_tasks["demo"] = {"target_item_id": "spi_demo", "due_tick": 3}
        app.time_sensor._task_fatigue_until_tick["demo"] = 5
        app.action._nodes["act_demo"] = {"action_id": "act_demo"}
        app.action._pending_async_completions.append({"action_id": "act_demo", "due_tick_number": 8})
        app.cognitive_stitching._esdb["cs_event::demo"] = {"event_ref_id": "cs_event::demo"}
        app.attention._total_calls = 7
        app.hdb._structure_retrieval._internal_resolution_cursor["st_demo"] = 2

        app.clear_all()

        assert app._last_report is None
        assert app._report_history == []
        assert app.time_sensor._delayed_tasks == {}
        assert app.time_sensor._task_fatigue_until_tick == {}
        assert app.action._nodes == {}
        assert app.action._pending_async_completions == []
        assert app.cognitive_stitching._esdb == {}
        assert app.attention._total_calls == 0
        assert app.hdb._structure_retrieval._internal_resolution_cursor == {}
        snapshot = app.hdb.get_memory_activation_snapshot(trace_id="after_clear")["data"]
        assert snapshot["summary"]["count"] == 0
    finally:
        app.close()


def test_run_cycle_accepts_empty_text_without_crash():
    app = ObservatoryApp(
        config_override={
            "history_limit": 2,
            "export_html": False,
            "export_json": False,
        }
    )
    try:
        report = app.run_cycle("")
        assert report.get("tick_id")
        sensor = report.get("sensor", {}) or {}
        assert sensor.get("success") is False
        assert sensor.get("code") in {"INPUT_VALIDATION_ERROR", "OK"}
    finally:
        app.close()


def test_goal_b_switch_forces_character_sa_sensor_mode():
    app = ObservatoryApp(
        config_override={
            "enable_goal_b_char_sa_string_mode": True,
            "sensor_default_mode": "advanced",
            "sensor_tokenizer_backend": "jieba",
            "sensor_enable_token_output": True,
            "sensor_enable_char_output": False,
            "export_html": False,
            "export_json": False,
        }
    )
    try:
        override = app._sensor_config_override()
        assert override["default_mode"] == "simple"
        assert override["tokenizer_backend"] == "none"
        assert override["enable_token_output"] is False
        assert override["enable_char_output"] is True
        assert override["enable_goal_b_char_sa_string_mode"] is True
    finally:
        app.close()


def test_second_goal_b_cycle_with_html_export_does_not_crash():
    app = ObservatoryApp(
        config_override={
            "enable_goal_b_char_sa_string_mode": True,
            "enable_structure_level_retrieval_storage": False,
            "enable_cognitive_stitching": True,
            "export_html": True,
            "export_json": False,
        }
    )
    try:
        rep1 = app.run_cycle("你好啊")
        rep2 = app.run_cycle("你好")
        assert isinstance(rep1, dict)
        assert isinstance(rep2, dict)
        assert "merged_stimulus" in rep2
    finally:
        app.close()


def test_goal_b_internal_stimulus_keeps_single_string_group():
    app = ObservatoryApp(
        config_override={
            "enable_goal_b_char_sa_string_mode": True,
            "enable_structure_level_retrieval_storage": False,
            "enable_cognitive_stitching": True,
            "export_html": False,
            "export_json": False,
        }
    )
    try:
        app.run_cycle("你好啊")
        report = app.run_cycle("你好")
        internal_raw = report.get("internal_stimulus_raw", {}) or {}
        seqs = list(internal_raw.get("sequence_groups", []) or [])

        merged = report.get("merged_stimulus", {}) or {}
        groups = list(merged.get("groups", []) or [])
        assert groups
        assert all("+" not in str(g.get("semantic_display_text", "")) for g in groups)

        if seqs:
            assert len(seqs) == 1
            seq = seqs[0]
            assert seq.get("order_sensitive") is True
            assert seq.get("string_unit_kind") == "char_sequence"
            assert seq.get("string_token_text") == "你好啊"
            assert any(bool(g.get("contains_internal_group", False)) for g in groups)
            assert any(int(g.get("internal_string_group_count", 0) or 0) >= 1 for g in groups)
    finally:
        app.close()
