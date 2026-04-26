# -*- coding: utf-8 -*-

from __future__ import annotations

import json
import shutil
import uuid
from pathlib import Path
from types import SimpleNamespace

from observatory.experiment.runner import RunOptions, _resolve_time_sensor_runtime_overrides, run_dataset
from observatory.experiment.storage import DatasetFileRef, imported_datasets_dir, resolve_run_dir


def test_resolve_time_sensor_runtime_overrides_uses_dataset_defaults_when_options_empty():
    normalized_doc = {
        "dataset_id": "demo_tick_dataset",
        "time_basis": "tick",
        "tick_dt_ms": 3000,
    }

    basis, tick_interval_sec = _resolve_time_sensor_runtime_overrides(
        normalized_doc=normalized_doc,
        options=RunOptions(),
    )

    assert basis == "tick"
    assert tick_interval_sec == 3.0


def test_resolve_time_sensor_runtime_overrides_preserves_explicit_options():
    normalized_doc = {
        "dataset_id": "demo_tick_dataset",
        "time_basis": "tick",
        "tick_dt_ms": 3000,
    }

    basis, tick_interval_sec = _resolve_time_sensor_runtime_overrides(
        normalized_doc=normalized_doc,
        options=RunOptions(time_sensor_time_basis="wallclock", tick_interval_sec=9.5),
    )

    assert basis == "wallclock"
    assert tick_interval_sec == 9.5


class _NoopSensor:
    def clear_echo_pool(self, trace_id: str):
        return {"success": True, "trace_id": trace_id}


class _NoopPool:
    def clear_state_pool(self, trace_id: str, reason: str, operator: str):
        return {"success": True, "trace_id": trace_id, "reason": reason, "operator": operator}


class _NoopHDB:
    def clear_hdb(self, trace_id: str, reason: str, operator: str):
        return {"success": True, "trace_id": trace_id, "reason": reason, "operator": operator}


class _FakeExperimentApp:
    def __init__(self):
        self._config = {}
        self.time_sensor = SimpleNamespace(_config={})
        self.sensor = _NoopSensor()
        self.pool = _NoopPool()
        self.hdb = _NoopHDB()
        self._last_report = None
        self._report_history = []
        self.tick_counter = 0

    def run_cycle(self, text=None, labels=None):
        self.tick_counter += 1
        input_text = str(text or "")
        labels = labels if isinstance(labels, dict) else {}
        executed_actions = []
        if "触发回忆" in input_text:
            executed_actions.append({"action_kind": "recall", "success": True})
        if "QUERY_WEATHER_OK" in input_text:
            executed_actions.append({"action_kind": "weather_stub", "success": True})
        teacher_rwd = float(labels.get("teacher_rwd", 0.0) or 0.0)
        teacher_pun = float(labels.get("teacher_pun", 0.0) or 0.0)
        report = {
            "trace_id": f"trace_{self.tick_counter}",
            "tick_id": f"tick_{self.tick_counter}",
            "tick_counter": self.tick_counter,
            "started_at": self.tick_counter,
            "finished_at": self.tick_counter + 1,
            "sensor": {"input_text": input_text},
            "final_state": {
                "state_snapshot": {
                    "summary": {"active_item_count": 1},
                    "top_items": [
                        {
                            "item_id": "spi_anchor",
                            "ref_object_id": "st_anchor",
                            "ref_object_type": "st",
                            "display": "anchor",
                        }
                    ],
                },
                "state_energy_summary": {},
                "hdb_snapshot": {"summary": {}},
            },
            "attention": {
                "top_items": [
                    {
                        "item_id": "spi_anchor",
                        "ref_object_id": "st_anchor",
                        "ref_object_type": "st",
                        "display": "anchor",
                    }
                ]
            },
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
            "emotion": {"nt_state_after": {}, "rwd_pun_snapshot": {"rwd": teacher_rwd, "pun": teacher_pun}},
            "action": {"executed_actions": executed_actions, "nodes": []},
            "teacher_feedback": {
                "teacher_rwd": teacher_rwd,
                "teacher_pun": teacher_pun,
                "applied_count": 1 if (teacher_rwd > 0.0 or teacher_pun > 0.0) else 0,
                "mode": "bind_attribute",
                "anchor": str(labels.get("teacher_anchor", "") or ""),
            },
            "timing": {"steps_ms": {}, "total_logic_ms": 0.0},
            "time_sensor": {},
        }
        self._last_report = report
        self._report_history.append(report)
        return report


def _write_imported_dataset(name: str, text: str) -> DatasetFileRef:
    base = imported_datasets_dir()
    base.mkdir(parents=True, exist_ok=True)
    path = base / name
    path.write_text(text, encoding="utf-8")
    return DatasetFileRef(source="imported", rel_path=path.name)


def _read_jsonl(path: Path) -> list[dict]:
    rows = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if line.strip():
            rows.append(json.loads(line))
    return rows



def test_expectation_contract_metric_eq_treats_missing_action_count_as_zero():
    from observatory.experiment.expectation_contracts import _evaluate_condition_item

    matched, detail = _evaluate_condition_item(
        {"kind": "metric_eq", "metric": "action_executed_weather_stub", "value": 0},
        report={},
        metrics={},
    )

    assert matched is True
    assert detail["current"] == 0
    assert detail["target"] == 0

def test_run_dataset_expectation_contract_success_emits_synthetic_feedback_tick():
    dataset_name = f"contract_success_{uuid.uuid4().hex}.yaml"
    run_id = f"test_contract_success_{uuid.uuid4().hex}"
    ref = _write_imported_dataset(
        dataset_name,
        """dataset_id: contract_success_demo
seed: 1
time_basis: tick
tick_dt_ms: 100
episodes:
  - id: ep_contract_success
    ticks:
      - text: 发起延迟期望
        labels:
          expectation_contract:
            id: recall_contract
            within_ticks: 2
            success_conditions:
              kind: action_executed_kind_min
              action_kind: recall
              min_count: 1
            on_success:
              teacher_rwd: 0.3
              feedback_text: 系统反馈：执行成功
              feedback_note: delayed reward after recall
      - text: 触发回忆
""",
    )
    app = _FakeExperimentApp()

    try:
        result = run_dataset(app=app, dataset_ref=ref, options=RunOptions(), run_id=run_id)
        assert result["success"] is True
        manifest = result["manifest"]
        assert manifest["status"] == "completed"
        assert manifest["source_tick_done"] == 2
        assert manifest["synthetic_tick_done"] == 1
        assert manifest["executed_tick_done_total"] == 3
        assert manifest["expectation_contracts"]["registered_count"] == 1
        assert manifest["expectation_contracts"]["success_count"] == 1
        assert manifest["expectation_contracts"]["failure_count"] == 0

        run_dir = resolve_run_dir(run_id)
        metrics_rows = _read_jsonl(run_dir / "metrics.jsonl")
        assert len(metrics_rows) == 3
        assert metrics_rows[-1]["tick_source"] == "expectation_contract_feedback"
        assert metrics_rows[-1]["synthetic_tick"] is True
        assert metrics_rows[-1]["expectation_contract_outcome"] == "success"
        assert metrics_rows[-1]["teacher_rwd"] == 0.3

        events_rows = _read_jsonl(run_dir / "expectation_contract_events.jsonl")
        settled = [row for row in events_rows if row.get("event") == "settled"]
        assert settled
        assert settled[-1]["outcome"] == "success"
        assert settled[-1]["frozen_anchor"]["teacher_anchor_ref_object_id"] == "st_anchor"
    finally:
        dataset_path = imported_datasets_dir() / dataset_name
        if dataset_path.exists():
            dataset_path.unlink()
        run_dir = resolve_run_dir(run_id)
        if run_dir.exists():
            shutil.rmtree(run_dir, ignore_errors=True)


def test_run_dataset_expectation_contract_run_end_failure_emits_timeout_feedback_tick():
    dataset_name = f"contract_failure_{uuid.uuid4().hex}.yaml"
    run_id = f"test_contract_failure_{uuid.uuid4().hex}"
    ref = _write_imported_dataset(
        dataset_name,
        """dataset_id: contract_failure_demo
seed: 1
time_basis: tick
tick_dt_ms: 100
episodes:
  - id: ep_contract_failure
    ticks:
      - text: 发起但不会满足
        labels:
          expectation_contract:
            id: missing_recall_contract
            within_ticks: 1
            success_conditions:
              kind: action_executed_kind_min
              action_kind: recall
              min_count: 1
            on_failure:
              teacher_pun: 0.4
              feedback_text: 系统反馈：没有执行
              feedback_note: delayed punish at run end
""",
    )
    app = _FakeExperimentApp()

    try:
        result = run_dataset(app=app, dataset_ref=ref, options=RunOptions(), run_id=run_id)
        assert result["success"] is True
        manifest = result["manifest"]
        assert manifest["status"] == "completed"
        assert manifest["source_tick_done"] == 1
        assert manifest["synthetic_tick_done"] == 1
        assert manifest["executed_tick_done_total"] == 2
        assert manifest["expectation_contracts"]["registered_count"] == 1
        assert manifest["expectation_contracts"]["success_count"] == 0
        assert manifest["expectation_contracts"]["failure_count"] == 1

        run_dir = resolve_run_dir(run_id)
        metrics_rows = _read_jsonl(run_dir / "metrics.jsonl")
        assert len(metrics_rows) == 2
        assert metrics_rows[-1]["tick_source"] == "expectation_contract_feedback"
        assert metrics_rows[-1]["expectation_contract_outcome"] == "failure"
        assert metrics_rows[-1]["teacher_pun"] == 0.4

        events_rows = _read_jsonl(run_dir / "expectation_contract_events.jsonl")
        settled = [row for row in events_rows if row.get("event") == "settled"]
        assert settled
        assert settled[-1]["outcome"] == "failure"
        assert settled[-1]["reason"] == "run_end"
    finally:
        dataset_path = imported_datasets_dir() / dataset_name
        if dataset_path.exists():
            dataset_path.unlink()
        run_dir = resolve_run_dir(run_id)
        if run_dir.exists():
            shutil.rmtree(run_dir, ignore_errors=True)


def test_run_dataset_expectation_contract_duplicate_spec_ids_are_separate_instances():
    dataset_name = f"contract_duplicate_weather_{uuid.uuid4().hex}.yaml"
    run_id = f"test_contract_duplicate_weather_{uuid.uuid4().hex}"
    ref = _write_imported_dataset(
        dataset_name,
        """dataset_id: contract_duplicate_weather_demo
seed: 1
time_basis: tick
tick_dt_ms: 100
episodes:
  - id: ep_contract_duplicate_weather
    ticks:
      - text: weak weather request
        labels:
          expectation_contract:
            id: reused_weather_contract
            within_ticks: 1
            success_conditions:
              kind: action_executed_kind_min
              action_kind: weather_stub
              min_count: 1
            on_failure:
              teacher_pun: 0.2
              feedback_text: weather missing
      - text: idle after weak request
      - text: second weather request
        labels:
          expectation_contract:
            id: reused_weather_contract
            within_ticks: 1
            success_conditions:
              kind: action_executed_kind_min
              action_kind: weather_stub
              min_count: 1
            on_success:
              teacher_rwd: 0.3
              feedback_text: weather executed
      - text: settle second QUERY_WEATHER_OK request
""",
    )
    app = _FakeExperimentApp()

    try:
        result = run_dataset(app=app, dataset_ref=ref, options=RunOptions(max_ticks=10), run_id=run_id)
        assert result["success"] is True
        manifest = result["manifest"]
        assert manifest["expectation_contracts"]["registered_count"] == 2
        assert manifest["expectation_contracts"]["success_count"] == 1
        assert manifest["expectation_contracts"]["failure_count"] == 1

        run_dir = resolve_run_dir(run_id)
        events_rows = _read_jsonl(run_dir / "expectation_contract_events.jsonl")
        registered = [row for row in events_rows if row.get("event") == "registered"]
        settled = [row for row in events_rows if row.get("event") == "settled"]
        assert len(registered) == 2
        assert len(settled) == 2
        assert {row["outcome"] for row in settled} == {"success", "failure"}
        assert registered[0]["spec_id"] == registered[1]["spec_id"] == "reused_weather_contract"
        assert registered[0]["contract_id"] != registered[1]["contract_id"]
    finally:
        dataset_path = imported_datasets_dir() / dataset_name
        if dataset_path.exists():
            dataset_path.unlink()
        run_dir = resolve_run_dir(run_id)
        if run_dir.exists():
            shutil.rmtree(run_dir, ignore_errors=True)
