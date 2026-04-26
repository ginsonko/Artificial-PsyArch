# -*- coding: utf-8 -*-
"""
# sanitized
=============================

# sanitized
  # sanitized
  # sanitized
  # sanitized
  # sanitized

English (short):
  Local observatory application for AP prototype testing and monitoring.
"""

from __future__ import annotations

import copy
import json
import os
import shlex
import sys
import time
import webbrowser
from pathlib import Path
from typing import Any

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from attention import AttentionFilter
from attention.main import _DEFAULT_CONFIG as ATTENTION_DEFAULT_CONFIG
from cognitive_feeling import CognitiveFeelingSystem
from cognitive_stitching import CognitiveStitchingEngine
from cognitive_feeling.main import _DEFAULT_CONFIG as CFS_DEFAULT_CONFIG
from cognitive_stitching.main import _DEFAULT_CONFIG as COGNITIVE_STITCHING_DEFAULT_CONFIG
from emotion import EmotionManager
from emotion.main import _DEFAULT_CONFIG as EMOTION_DEFAULT_CONFIG
from hdb import HDB
from hdb.main import _DEFAULT_CONFIG as HDB_DEFAULT_CONFIG
from hdb._cut_engine import CutEngine
from hdb._id_generator import next_id
from hdb._sequence_display import (
    format_group_display,
    format_semantic_group_display,
    format_semantic_sequence_groups,
    format_sequence_groups,
)
from state_pool.main import StatePool, _DEFAULT_CONFIG as STATE_POOL_DEFAULT_CONFIG
from text_sensor import TextSensor
from text_sensor.main import _DEFAULT_CONFIG as TEXT_SENSOR_DEFAULT_CONFIG
from time_sensor import TimeSensor
from time_sensor.main import TIME_SENSOR_DEFAULT_CONFIG
from innate_script import InnateScriptManager
from innate_script.main import _DEFAULT_CONFIG as IESM_DEFAULT_CONFIG
from action import ActionManager
from action.main import _DEFAULT_CONFIG as ACTION_DEFAULT_CONFIG
from energy_balance import EnergyBalanceController
from energy_balance.main import _DEFAULT_CONFIG as ENERGY_BALANCE_DEFAULT_CONFIG

from ._config_layout import build_config_view, coerce_updates_by_defaults, load_yaml_dict, save_annotated_config
from ._render_html import export_cycle_html
from ._render_terminal import (
    format_help,
    render_check_report,
    render_cycle_report,
    render_group_report,
    render_hdb_snapshot,
    render_header,
    render_repair_report,
    render_state_snapshot,
    render_structure_report,
    render_episodic_report,
)


DEFAULT_CONFIG = {
    "attention_top_n": 16,
    "attention_stub_consume_energy": True,
    "attention_memory_energy_ratio": 0.5,
    "snapshot_top_k": 24,
    # sanitized
    # sanitized
    # sanitized
    "cfs_source_mode": "iesm",
    "export_html": True,
    "export_json": True,
    "auto_open_html_report": False,
    "history_limit": 24,
    "default_launch_mode": "web",
    "web_host": "127.0.0.1",
    "web_port": 8765,
    "web_auto_open_browser": True,
    "sensor_default_mode": "advanced",
    "sensor_tokenizer_backend": "jieba",
    "sensor_enable_token_output": True,
    "sensor_enable_char_output": False,
    "sensor_enable_echo": True,
    "sensor_include_echoes_in_packet": True,
    "state_pool_enable_placeholder_interfaces": False,
    "state_pool_enable_script_broadcast": False,
    "hdb_enable_background_repair": True,
    "input_chunking_enabled": True,
    "input_chunk_soft_limit": 12,
    "input_chunk_hard_limit": 20,
    "projection_fatigue_enabled": True,
    "projection_fatigue_decay": 0.82,
    "projection_fatigue_step": 0.28,
    "projection_fatigue_min_effective_ev": 0.03,
    "projection_fatigue_min_effective_er": 0.03,
}

OBSERVATORY_CONFIG_SCHEMA = {
    "title": "Observatory Config",
    "description": "Configuration schema for the observatory prototype.",
    "groups": [],
}

def _load_yaml_config(path: str) -> dict:
    return load_yaml_dict(path)


def _serialize_simple_yaml_scalar(value: Any) -> str:
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return str(value)
    text = str(value)
    escaped = text.replace("\\", "\\\\").replace('"', '\\"')
    return f"\"{escaped}\""


def _dump_simple_yaml(data: dict[str, Any], indent: int = 0) -> str:
    lines: list[str] = []
    prefix = " " * indent
    for key, value in data.items():
        if isinstance(value, dict):
            lines.append(f"{prefix}{key}:")
            if value:
                lines.append(_dump_simple_yaml(value, indent + 2))
            else:
                lines.append(f"{prefix}  {{}}")
        elif isinstance(value, list):
            if not value:
                lines.append(f"{prefix}{key}: []")
                continue
            lines.append(f"{prefix}{key}:")
            for item in value:
                if isinstance(item, dict):
                    lines.append(f"{prefix}  -")
                    lines.append(_dump_simple_yaml(item, indent + 4))
                else:
                    lines.append(f"{prefix}  - {_serialize_simple_yaml_scalar(item)}")
        else:
            lines.append(f"{prefix}{key}: {_serialize_simple_yaml_scalar(value)}")
    return "\n".join(lines)


def _write_yaml_config(path: str, data: dict[str, Any]) -> None:
    try:
        import yaml

        with open(path, "w", encoding="utf-8") as fh:
            yaml.safe_dump(data, fh, allow_unicode=True, sort_keys=False)
        return
    except ImportError:
        pass

    content = _dump_simple_yaml(data).strip() + "\n"
    with open(path, "w", encoding="utf-8") as fh:
        fh.write(content)


class ObservatoryApp:
    def __init__(self, config_path: str = "", config_override: dict | None = None):
        self._config_path = config_path or os.path.join(os.path.dirname(__file__), "config", "observatory_config.yaml")
        self._config = self._build_config(config_override)
        self.output_dir = Path(__file__).resolve().parent / "outputs"
        self.output_dir.mkdir(parents=True, exist_ok=True)

        self.sensor = TextSensor(
            config_override=self._sensor_config_override()
        )
        # sanitized
        # sanitized
        self.time_sensor = TimeSensor()
        self.pool = StatePool(
            config_override=self._state_pool_config_override()
        )
        self.hdb = HDB(config_override=self._hdb_config_override())
        self.attention = AttentionFilter(
            config_override=self._attention_config_override()
        )
        self.cfs = CognitiveFeelingSystem()
        self.cognitive_stitching = CognitiveStitchingEngine()
        self.emotion = EmotionManager()
        self.iesm = InnateScriptManager()
        # sanitized
        # sanitized
        # sanitized
        self.action = ActionManager()
        # sanitized
        # sanitized
        # sanitized
        # sanitized
        # sanitized
        self.energy_balance = EnergyBalanceController()
        self.cut_engine = CutEngine(config=self._cut_engine_config_override())
        self.tick_counter = 0
        self._last_report: dict[str, Any] | None = None
        self._report_history: list[dict[str, Any]] = []
        self._started_at = int(time.time() * 1000)
        self._pending_focus_directives: list[dict[str, Any]] = []
        self._last_modulation: dict[str, Any] = {}
        self._pending_external_text_chunks: list[str] = []
        self._current_external_source_text: str = ""
        self._projection_fatigue: dict[str, float] = {}
        # sanitized
        # sanitized
        self._hdb_config_base: dict[str, Any] = dict(self.hdb._config)
        self.cut_engine.update_config(self._cut_engine_config_override())
        self.cut_engine.update_config(self._cut_engine_config_override())
        self._silence_jieba_logs()

    def close(self) -> None:
        self.sensor._logger.close()
        self.time_sensor.close()
        self.pool._logger.close()
        self.hdb.close()
        self.attention.close()
        self.cfs.close()
        self.cognitive_stitching.close()
        self.emotion.close()
        self.iesm.close()
        self.action.close()
        self.energy_balance.close()

    def _split_input_text_for_ticks(self, text: str) -> list[str]:
        raw = str(text or "")
        if not raw:
            return []
        if not bool(self._config.get("input_chunking_enabled", True)):
            return [raw]
        soft_limit = max(4, int(self._config.get("input_chunk_soft_limit", 12) or 12))
        hard_limit = max(soft_limit, int(self._config.get("input_chunk_hard_limit", 20) or 20))
        chunks: list[str] = []
        current = ""
        natural_breaks = set("。！？!?；;，,、\n\r")
        for ch in raw:
            current += ch
            if len(current) >= hard_limit:
                chunks.append(current)
                current = ""
                continue
            if len(current) >= soft_limit and ch in natural_breaks:
                chunks.append(current)
                current = ""
        if current:
            chunks.append(current)
        return [c for c in chunks if str(c).strip()]

    def _enqueue_external_text(self, text: str) -> list[str]:
        chunks = self._split_input_text_for_ticks(text)
        if chunks:
            self._current_external_source_text = str(text or "")
        self._pending_external_text_chunks.extend(chunks)
        return chunks

    def _dequeue_external_text_for_tick(self) -> str | None:
        if not self._pending_external_text_chunks:
            self._current_external_source_text = ""
            return None
        chunk = self._pending_external_text_chunks.pop(0)
        if not self._pending_external_text_chunks:
            # Keep source text visible for the current tick; clear on next empty dequeue.
            pass
        return chunk

    def _decay_projection_fatigue(self) -> None:
        decay = max(0.0, min(1.0, float(self._config.get("projection_fatigue_decay", 0.82) or 0.82)))
        next_state: dict[str, float] = {}
        for key, value in self._projection_fatigue.items():
            v = max(0.0, float(value) * decay)
            if v >= 1e-6:
                next_state[key] = v
        self._projection_fatigue = next_state

    def _projection_fatigue_key(self, item: dict) -> str:
        projection_kind = str(item.get("projection_kind", "structure") or "structure")
        memory_id = str(item.get("memory_id", "") or "")
        structure_id = str(item.get("structure_id", item.get("target_structure_id", "")) or "")
        backing_structure_id = str(item.get("backing_structure_id", "") or "")
        display_text = str(item.get("display_text", item.get("grouped_display_text", "")) or "").strip()
        reason = str(item.get("reason", "") or "")
        stable_ref = backing_structure_id or structure_id or display_text or memory_id
        return "|".join([projection_kind, stable_ref, reason])

    def _apply_projection_fatigue_to_item(self, item: dict) -> dict | None:
        if not bool(self._config.get("projection_fatigue_enabled", True)):
            return dict(item)
        key = self._projection_fatigue_key(item)
        fatigue = max(0.0, float(self._projection_fatigue.get(key, 0.0) or 0.0))
        effective = dict(item)
        effective_er = max(0.0, float(item.get("er", 0.0) or 0.0)) / (1.0 + fatigue)
        effective_ev = max(0.0, float(item.get("ev", 0.0) or 0.0)) / (1.0 + fatigue)
        effective["er"] = round(float(effective_er), 8)
        effective["ev"] = round(float(effective_ev), 8)
        effective["projection_fatigue"] = round(float(fatigue), 8)
        min_er = max(0.0, float(self._config.get("projection_fatigue_min_effective_er", 0.03) or 0.03))
        min_ev = max(0.0, float(self._config.get("projection_fatigue_min_effective_ev", 0.03) or 0.03))
        if effective_er < min_er and effective_ev < min_ev:
            return None
        return effective

    def _mark_projection_fatigue(self, item: dict) -> None:
        if not bool(self._config.get("projection_fatigue_enabled", True)):
            return
        key = self._projection_fatigue_key(item)
        step = max(0.0, float(self._config.get("projection_fatigue_step", 0.28) or 0.28))
        self._projection_fatigue[key] = round(float(self._projection_fatigue.get(key, 0.0) or 0.0) + step, 8)

    def next_trace(self, prefix: str = "cycle") -> str:
        self.tick_counter += 1
        return f"{prefix}_{self.tick_counter:04d}"

    def print_header(self) -> None:
        print(render_header())
        print(format_help())

    def run_cycle(self, text: str | None = None, *, labels: dict[str, Any] | None = None) -> dict:
        trace_id = self.next_trace("cycle")
        tick_id = trace_id
        self._decay_projection_fatigue()
        # sanitized
        # sanitized
        # sanitized
        cycle_t0 = time.perf_counter()
        timing_steps_ms: dict[str, int] = {}
        report: dict[str, Any] = {
            "trace_id": trace_id,
            # sanitized
            # sanitized
            "tick_id": tick_id,
            "started_at": int(time.time() * 1000),
            "observatory": {
                "module": "observatory",
                "config": dict(self._config),
                "output_dir": str(self.output_dir),
            },
        }

        # Optional per-tick labels (experiment/teacher signals).
        # sanitized
        # sanitized
        # sanitized
        tick_labels = labels if isinstance(labels, dict) else {}
        report["tick_labels"] = dict(tick_labels) if tick_labels else {}

        external_packet = None
        sensor_result = None
        queued_chunks: list[str] = []
        if text is not None and str(text).strip():
            queued_chunks = self._enqueue_external_text(str(text))
        tick_text = self._dequeue_external_text_for_tick()
        report["input_queue"] = {
            "queued_from_new_input_count": len(queued_chunks),
            "pending_count_after_dequeue": len(self._pending_external_text_chunks),
            "tick_text": tick_text or "",
            "source_text": str(self._current_external_source_text or ""),
            "queued_preview": queued_chunks[:8],
        }
        if tick_text is not None:
            t0 = time.perf_counter()
            sensor_result = self.sensor.ingest_text(text=tick_text, trace_id=trace_id, tick_id=tick_id)
            # sanitized
            # sanitized
            # sanitized
            # sanitized
            try:
                if isinstance(sensor_result, dict) and bool(sensor_result.get("success", False)):
                    data = sensor_result.get("data", {}) if isinstance(sensor_result.get("data", {}), dict) else {}
                    if isinstance(data.get("stimulus_packet"), dict):
                        external_packet = copy.deepcopy(data.get("stimulus_packet"))
            except Exception:
                external_packet = None
            report["sensor"] = self._build_sensor_report(tick_text, sensor_result)
            timing_steps_ms["sensor_ms"] = int((time.perf_counter() - t0) * 1000)
        else:
            report["sensor"] = {
                "success": False,
                "code": "INPUT_VALIDATION_ERROR",
                "message": "当前 tick 没有可供文本感受器处理的输入。",
                "input_text": "",
                "feature_sa_count": 0,
                "attribute_sa_count": 0,
                "csa_bundle_count": 0,
                "echo_pool_size": int(getattr(self.sensor, "_echo_pool_size", 0) or 0) if getattr(self, "sensor", None) is not None else 0,
                "echo_current_round": int(getattr(self.sensor, "_echo_current_round", 0) or 0) if getattr(self, "sensor", None) is not None else 0,
            }
            timing_steps_ms["sensor_ms"] = 0

        t0 = time.perf_counter()
        report["maintenance"] = self._run_state_pool_maintenance(trace_id, tick_id)
        timing_steps_ms["maintenance_ms"] = int((time.perf_counter() - t0) * 1000)
        report["memory_activation"] = {
            "maintenance": self.hdb.tick_memory_activation_pool(trace_id=trace_id, tick_id=tick_id)["data"],
            "apply_result": {
                "applied_count": 0,
                "total_delta_er": 0.0,
                "total_delta_ev": 0.0,
                "total_delta_energy": 0.0,
                "items": [],
            },
            "seed_targets": [],
            "feedback_result": {
                "applied_count": 0,
                "total_feedback_er": 0.0,
                "total_feedback_ev": 0.0,
                "total_feedback_energy": 0.0,
                "items": [],
                "record_result": {
                    "recorded_count": 0,
                    "total_feedback_er": 0.0,
                    "total_feedback_ev": 0.0,
                    "total_feedback_energy": 0.0,
                    "items": [],
                },
            },
            "snapshot": {
                "summary": {"count": 0, "total_er": 0.0, "total_ev": 0.0, "total_energy": 0.0, "top_total_energy": 0.0},
                "items": [],
                "sort_by": "energy_desc",
            },
        }

        # sanitized
        modulation_in = self._last_modulation.get("attention", {}) if isinstance(self._last_modulation, dict) else {}
        focus_directives_in: list[dict[str, Any]] = []
        for directive in self._pending_focus_directives:
            if not isinstance(directive, dict):
                continue
            ttl = int(directive.get("ttl_ticks", 0) or 0)
            if ttl > 0:
                focus_directives_in.append(directive)
        report["modulation_inputs"] = {
            "attention": dict(modulation_in) if isinstance(modulation_in, dict) else {},
            "focus_directives": [dict(item) for item in focus_directives_in[:16]],
        }

        # sanitized
        # sanitized
        hdb_mod_in = self._last_modulation.get("hdb", {}) if isinstance(self._last_modulation, dict) else {}
        hdb_mod_apply = self._apply_hdb_modulation_for_tick(
            modulation=hdb_mod_in if isinstance(hdb_mod_in, dict) else {},
            trace_id=trace_id,
            tick_id=tick_id,
        )
        report["modulation_inputs"]["hdb"] = dict(hdb_mod_in) if isinstance(hdb_mod_in, dict) else {}
        report["modulation_applied"] = {"hdb": hdb_mod_apply}

        t0 = time.perf_counter()
        attention_snapshot, attention_report = self._build_attention_memory_stub(
            trace_id,
            tick_id,
            focus_directives=focus_directives_in,
            modulation=modulation_in,
        )
        report["attention"] = attention_report
        timing_steps_ms["attention_ms"] = int((time.perf_counter() - t0) * 1000)

        # sanitized
        decayed: list[dict[str, Any]] = []
        for directive in self._pending_focus_directives:
            if not isinstance(directive, dict):
                continue
            ttl = int(directive.get("ttl_ticks", 0) or 0)
            ttl -= 1
            if ttl <= 0:
                continue
            decayed.append({**directive, "ttl_ticks": ttl})
        self._pending_focus_directives = decayed

        t0 = time.perf_counter()
        structure_result = self.hdb.run_structure_level_retrieval_storage(
            state_snapshot=attention_snapshot,
            trace_id=trace_id,
            tick_id=tick_id,
            # sanitized
            # sanitized
            attention_mode="cam_snapshot",
            top_n=max(
                1,
                sum(1 for it in (attention_snapshot.get("top_items", []) or []) if str(it.get("ref_object_type", "")) == "st"),
            ),
            enable_storage=bool(self._config.get("enable_structure_level_retrieval_storage", False)),
            max_rounds=(None if bool(self._config.get("enable_structure_level_retrieval_storage", False)) else 0),
        )
        structure_data = (structure_result.get("data", {}) or {}) if isinstance(structure_result, dict) else {}
        internal_fragments = list(structure_data.get("internal_stimulus_fragments", []) or [])
        if (not internal_fragments) and (not bool(self._config.get("enable_structure_level_retrieval_storage", False))):
            try:
                cam_only = self.hdb._structure_retrieval._run_cam_internal_stimulus_only(
                    items=list((attention_snapshot or {}).get("top_items", []) or []),
                    trace_id=trace_id,
                    tick_id=tick_id,
                    cut_engine=self.cut_engine,
                )
                if isinstance(cam_only, dict):
                    internal_fragments = list(cam_only.get("internal_stimulus_fragments", []) or [])
                    if internal_fragments:
                        structure_data["internal_stimulus_fragments"] = internal_fragments
                        structure_data["internal_resolution"] = dict(cam_only.get("internal_resolution", {}) or {})
                        structure_data.setdefault("debug", {})
                        if isinstance(structure_data.get("debug", {}), dict):
                            structure_data["debug"]["cam_internal_only"] = dict(cam_only.get("debug", {}) or {})
            except Exception as exc:
                structure_data.setdefault("debug", {})
                if isinstance(structure_data.get("debug", {}), dict):
                    structure_data["debug"]["cam_internal_only_error"] = str(exc)
        internal_packet = self.hdb.build_internal_stimulus_packet(
            internal_fragments,
            trace_id=trace_id,
            tick_id=tick_id,
        )
        combined_packet = self.hdb.merge_stimulus_packets(external_packet, internal_packet, trace_id=trace_id, tick_id=tick_id)
        report["structure_level"] = {"result": structure_data}
        report["internal_stimulus_raw"] = internal_packet
        report["merged_stimulus_raw"] = combined_packet
        report["merged_stimulus"] = self._describe_stimulus_packet(combined_packet)
        timing_steps_ms["structure_level_ms"] = int((time.perf_counter() - t0) * 1000)

        t0 = time.perf_counter()
        structure_bias_projection = self._project_runtime_structures(
            structure_data.get("bias_projections", []),
            trace_id=trace_id,
            tick_id=tick_id,
        )
        cache_neutralization = self._neutralize_packet_against_pool(combined_packet, trace_id, tick_id)
        residual_packet = cache_neutralization["residual_packet_raw"]
        report["cache_neutralization"] = {
            "input_packet": cache_neutralization["input_packet"],
            "residual_packet": cache_neutralization["residual_packet"],
            "priority_events": cache_neutralization["priority_events"],
            "priority_diagnostics": cache_neutralization.get("priority_diagnostics", []),
            "priority_summary": cache_neutralization["priority_summary"],
        }
        report["pool_apply"] = {
            "apply_result": {},
            "events": [],
            "priority_events": cache_neutralization["priority_events"],
            "priority_diagnostics": cache_neutralization.get("priority_diagnostics", []),
            "bias_projection": structure_bias_projection,
            "input_packet": cache_neutralization["input_packet"],
            "residual_packet": cache_neutralization["residual_packet"],
            "priority_summary": dict(cache_neutralization["priority_summary"]),
        }
        timing_steps_ms["cache_neutralization_ms"] = int((time.perf_counter() - t0) * 1000)

        t0 = time.perf_counter()
        stimulus_result = self.hdb.run_stimulus_level_retrieval_storage(
            stimulus_packet=residual_packet,
            trace_id=trace_id,
            tick_id=tick_id,
        )
        stimulus_data = stimulus_result["data"]
        report["stimulus_level"] = {"result": stimulus_data}
        timing_steps_ms["stimulus_level_ms"] = int((time.perf_counter() - t0) * 1000)

        landing_packet = stimulus_data.get("residual_stimulus_packet", residual_packet)
        t0 = time.perf_counter()
        apply_result, apply_events, landed_packet = self._apply_packet_to_pool(
            landing_packet,
            trace_id,
            tick_id,
            disable_priority_neutralization=True,
        )
        runtime_projection = self._project_runtime_structures(
            stimulus_data.get("runtime_projection_structures", []),
            trace_id=trace_id,
            tick_id=tick_id,
        )
        report["pool_apply"]["apply_result"] = apply_result
        report["pool_apply"]["events"] = apply_events
        report["pool_apply"]["landed_packet"] = self._describe_stimulus_packet(landed_packet)
        report["pool_apply"]["runtime_projection"] = runtime_projection
        timing_steps_ms["pool_apply_ms"] = int((time.perf_counter() - t0) * 1000)

        t0 = time.perf_counter()
        try:
            cs_result = self.cognitive_stitching.run(
                pool=self.pool,
                hdb=self.hdb,
                trace_id=trace_id,
                tick_id=tick_id,
            )
            cs_data = (cs_result.get("data", {}) or {}) if isinstance(cs_result, dict) else {}
        except Exception as exc:
            cs_data = {"enabled": bool(self._config.get("enable_cognitive_stitching", False)), "reason": "exception", "error": {"message": str(exc)}}
        report["cognitive_stitching"] = cs_data
        timing_steps_ms["cognitive_stitching_ms"] = int((time.perf_counter() - t0) * 1000)

        t0 = time.perf_counter()
        induction_snapshot = self.pool.get_state_snapshot(
            trace_id=f"{trace_id}_induction_snapshot",
            tick_id=tick_id,
            top_k=int(self._config["snapshot_top_k"]),
        )["data"]["snapshot"]
        induction_result = self.hdb.run_induction_propagation(
            state_snapshot=induction_snapshot,
            trace_id=trace_id,
            tick_id=tick_id,
            max_source_items=8,
        )
        induction_data = induction_result["data"]
        source_ev_events = self._apply_induction_source_consumptions(
            induction_data.get("source_ev_consumptions", []),
            trace_id,
            tick_id,
        )
        induction_targets = list(induction_data.get("induction_targets", []))
        structure_targets = [
            item for item in induction_targets if str(item.get("projection_kind", "structure")) != "memory"
        ]
        memory_targets = [
            item for item in induction_targets if str(item.get("projection_kind", "structure")) == "memory"
        ]
        memory_seed_targets = self._collect_memory_activation_seed_targets(report)
        combined_memory_targets = memory_targets + memory_seed_targets
        applied_targets = self._apply_induction_targets(structure_targets, trace_id, tick_id)
        memory_apply_result = self.hdb.apply_memory_activation_targets(
            targets=combined_memory_targets,
            trace_id=trace_id,
            tick_id=tick_id,
        )["data"]
        memory_feedback_result = self._apply_memory_feedback(
            memory_items=memory_apply_result.get("items", []),
            trace_id=trace_id,
            tick_id=tick_id,
        )
        memory_snapshot = self.hdb.get_memory_activation_snapshot(
            trace_id=f"{trace_id}_memory_activation_snapshot",
            limit=24,
            sort_by="energy_desc",
        )["data"]
        report["induction"] = {
            "result": induction_data,
            "source_ev_events": source_ev_events,
            "applied_targets": applied_targets,
            "structure_target_count": len(structure_targets),
            "memory_target_count": len(memory_targets),
            "memory_seed_target_count": len(memory_seed_targets),
            "memory_target_total_count": len(combined_memory_targets),
        }
        report["memory_activation"]["apply_result"] = memory_apply_result
        report["memory_activation"]["seed_targets"] = memory_seed_targets
        report["memory_activation"]["feedback_result"] = memory_feedback_result
        report["memory_activation"]["snapshot"] = memory_snapshot
        report["memory_feedback"] = memory_feedback_result
        timing_steps_ms["induction_and_memory_ms"] = int((time.perf_counter() - t0) * 1000)

        # =============================================================== #
        # sanitized
        # =============================================================== #
        # sanitized
        # sanitized
        # sanitized
        # sanitized
        # sanitized
        t0 = time.perf_counter()
        try:
            ts_res = self.time_sensor.run_time_feeling_tick(
                pool=self.pool,
                trace_id=trace_id,
                tick_id=tick_id,
                now_ms=int(report.get("started_at", 0) or 0) or None,
                memory_activation_snapshot=memory_snapshot,
                memory_feedback_result=memory_feedback_result,
            )
            report["time_sensor"] = ts_res.get("data", {}) if isinstance(ts_res, dict) else {}
        except Exception as exc:
            report["time_sensor"] = {"error": str(exc)}
        timing_steps_ms["time_sensor_ms"] = int((time.perf_counter() - t0) * 1000)

        # =============================================================== #
        # sanitized
        # =============================================================== #
        # sanitized
        # sanitized
        # sanitized
        # sanitized
        t0 = time.perf_counter()
        try:
            report["teacher_feedback"] = self._apply_teacher_feedback(
                labels=tick_labels,
                report=report,
                trace_id=trace_id,
                tick_id=tick_id,
            )
        except Exception as exc:
            report["teacher_feedback"] = {"ok": False, "code": "EXCEPTION", "message": f"teacher_feedback failed: {exc}"}
        timing_steps_ms["teacher_feedback_ms"] = int((time.perf_counter() - t0) * 1000)

        # =============================================================== #
        # sanitized
        # =============================================================== #
        # sanitized
        # sanitized
        # sanitized
        #
        # sanitized
        # sanitized
        # sanitized

        cfs_source_mode = str(self._config.get("cfs_source_mode", "iesm") or "iesm").strip().lower() or "iesm"
        t0 = time.perf_counter()
        cfs_data: dict[str, Any] = {}
        cfs_signals: list[dict[str, Any]] = []

        if cfs_source_mode in {"legacy", "module", "cfs"}:
            # Legacy CFS module path (transition / comparison only).
            # sanitized
            cfs_snapshot = self.pool.get_state_snapshot(
                trace_id=f"{trace_id}_cfs_snapshot",
                tick_id=tick_id,
                top_k=int(self._config.get("snapshot_top_k", 24)),
            )["data"]["snapshot"]
            cfs_result = self.cfs.run_cfs(
                pool=self.pool,
                state_snapshot=cfs_snapshot,
                cam_snapshot=attention_snapshot,
                attention_report=report.get("attention", {}),
                trace_id=trace_id,
                tick_id=tick_id,
                context={
                    "structure_level": report.get("structure_level", {}).get("result", {}),
                    "stimulus_level": report.get("stimulus_level", {}).get("result", {}),
                    "induction": report.get("induction", {}).get("result", {}),
                    "cache_neutralization": report.get("cache_neutralization", {}),
                },
            )
            cfs_data = cfs_result.get("data", {}) or {}
            report["cognitive_feeling"] = cfs_data
            cfs_signals = list(cfs_data.get("cfs_signals", []) or [])
        else:
            # Preferred path: IESM rules generate CFS signals.
            # sanitized
            cfs_data = {
                "cfs_signals": [],
                "writes": {"runtime_nodes": [], "attribute_bindings": []},
                "meta": {"tick_number": int(self.tick_counter), "source_mode": "iesm_rules"},
            }
            report["cognitive_feeling"] = cfs_data
            cfs_signals = []

        timing_steps_ms["cfs_ms"] = int((time.perf_counter() - t0) * 1000)
        t_iesm0 = time.perf_counter()

        innate_script_report: dict[str, Any] = {
            "active_scripts": self.iesm.get_active_scripts(trace_id=f"{trace_id}_iesm_scripts").get("data", {}),
            "state_window_checks": [],
            "focus": {},
        }
        maint_packet: dict[str, Any] = {}
        apply_packet: dict[str, Any] = {}
        try:
            # sanitized
            maint_packet = self.pool._snapshot.build_script_check_packet(
                events=report.get("maintenance", {}).get("events", []),
                pool_store=self.pool._store,
                trace_id=f"{trace_id}_iesm_maint",
                tick_id=tick_id,
            )
            maint_check = self.iesm.check_state_window(maint_packet, trace_id=trace_id).get("data", {})
            innate_script_report["state_window_checks"].append(
                {"stage": "maintenance", "packet_summary": maint_packet.get("summary", {}), "check": maint_check}
            )
        except Exception:
            innate_script_report["state_window_checks"].append({"stage": "maintenance", "error": "packet_build_failed"})

        try:
            # sanitized
            apply_events = report.get("pool_apply", {}).get("events", [])
            apply_packet = self.pool._snapshot.build_script_check_packet(
                events=apply_events,
                pool_store=self.pool._store,
                trace_id=f"{trace_id}_iesm_apply",
                tick_id=tick_id,
            )
            apply_check = self.iesm.check_state_window(apply_packet, trace_id=trace_id).get("data", {})
            innate_script_report["state_window_checks"].append(
                {"stage": "pool_apply", "packet_summary": apply_packet.get("summary", {}), "check": apply_check}
            )
        except Exception:
            innate_script_report["state_window_checks"].append({"stage": "pool_apply", "error": "packet_build_failed"})

        # sanitized
        # Build runtime context for IESM metric predicates.
        innate_rules_context = self._build_innate_rules_context(
            report=report,
            pool_snapshot=None,  # use live StatePool store
            emotion_state=None,  # IESM runs before EMgr update; use current snapshot + CFS-derived rwd/pun
            cfs_signals=cfs_signals,
            trace_id=trace_id,
            tick_id=tick_id,
        )

        # sanitized
        tick_rules_result = self.iesm.run_tick_rules(
            trace_id=trace_id,
            tick_id=tick_id,
            tick_index=int(self.tick_counter),
            cfs_signals=cfs_signals,
            state_windows=[
                {"stage": "maintenance", "packet": maint_packet},
                {"stage": "pool_apply", "packet": apply_packet},
            ],
            context=innate_rules_context,
            dry_run=False,
        )
        tick_rules_data = tick_rules_result.get("data", {}) or {}
        directives = tick_rules_data.get("directives", {}) or {}

        # If CFS is sourced from IESM rules, treat directives.cfs_signals as canonical.
        # sanitized
        # sanitized
        if cfs_source_mode not in {"legacy", "module", "cfs"}:
            cfs_signals = list(directives.get("cfs_signals", []) or [])
            report["cognitive_feeling"] = {
                "cfs_signals": cfs_signals,
                "writes": {"runtime_nodes": [], "attribute_bindings": []},
                "meta": {"tick_number": int(self.tick_counter), "source_mode": "iesm_rules"},
            }
        pool_effects = list(directives.get("pool_effects", []) or [])
        pool_effect_apply = {}
        if pool_effects:
            # Apply pool effects immediately so the same tick can affect later steps/snapshots.
            # sanitized
            pool_effect_apply = self._apply_innate_pool_effects(
                effects=pool_effects,
                context=innate_rules_context,
                trace_id=trace_id,
                tick_id=tick_id,
            )

        # Enrich episodic memory material with runtime-bound attributes (CFS/time-feeling/rwd/pun tags).
        # sanitized
        # sanitized
        try:
            enrich_res = self._enrich_tick_episodic_memory_with_bound_attributes(report=report, trace_id=trace_id, tick_id=tick_id)
        except Exception as exc:
            enrich_res = {"ok": False, "code": "EXCEPTION", "message": f"enrich episodic memory failed: {exc}"}
        try:
            stim_res = (report.get("stimulus_level", {}) or {}).get("result", {})
            if isinstance(stim_res, dict):
                stim_res["episodic_memory_enrichment"] = enrich_res
        except Exception:
            pass
        focus_data = {
            # sanitized
            # Note: This is the rule-engine output list, not necessarily the legacy CFS module output.
            "cfs_signals": list(directives.get("cfs_signals", []) or []),
            "focus_directives": list(directives.get("focus_directives", []) or []),
            "emotion_updates": dict(directives.get("emotion_updates", {}) or {}),
            "action_triggers": list(directives.get("action_triggers", []) or []),
            "pool_effects": pool_effects,
            "pool_effect_apply": pool_effect_apply,
            "episodic_memory_enrichment": enrich_res,
            "audit": tick_rules_data.get("audit", {}) or {},
            "triggered_rules": list(tick_rules_data.get("triggered_rules", []) or []),
            "triggered_scripts": list(tick_rules_data.get("triggered_scripts", []) or []),
        }
        innate_script_report["focus"] = focus_data
        innate_script_report["tick_rules"] = {
            "code": tick_rules_result.get("code", ""),
            "triggered_rule_count": len(focus_data.get("triggered_rules", []) or []),
            "focus_directive_count": len(focus_data.get("focus_directives", []) or []),
            "emotion_update_key_count": len((focus_data.get("emotion_updates") or {}).keys()),
            "action_trigger_count": len(focus_data.get("action_triggers", []) or []),
            "pool_effect_count": len(focus_data.get("pool_effects", []) or []),
        }
        new_directives = list(focus_data.get("focus_directives", []) or [])
        new_action_triggers = list(focus_data.get("action_triggers", []) or [])
        if new_directives:
            # sanitized
            # sanitized
            # sanitized
            action_enabled = bool(getattr(self, "action", None) and getattr(self.action, "_config", {}).get("enabled", True))
            if not action_enabled:
                # sanitized
                existing_by_id = {
                    str(item.get("directive_id", "")): item
                    for item in self._pending_focus_directives
                    if isinstance(item, dict) and str(item.get("directive_id", ""))
                }
                for directive in new_directives:
                    if not isinstance(directive, dict):
                        continue
                    did = str(directive.get("directive_id", ""))
                    if not did:
                        continue
                    existing_by_id[did] = directive
                self._pending_focus_directives = list(existing_by_id.values())

        report["innate_script"] = innate_script_report
        timing_steps_ms["iesm_ms"] = int((time.perf_counter() - t_iesm0) * 1000)

        # =============================================================== #
        # sanitized
        # =============================================================== #
        # sanitized
        t0 = time.perf_counter()
        # Compute rwd/pun override from the *current* pool (after IESM/time-sensor binding).
        # sanitized
        # sanitized
        rwd_pun_override = None
        try:
            rows = []
            for item in list(self.pool._store.get_all()):  # type: ignore[attr-defined]
                if not isinstance(item, dict):
                    continue
                row = self.pool._snapshot._build_top_item_summary(item)  # type: ignore[attr-defined]
                if isinstance(row, dict):
                    rows.append(row)
            rwd_pun_override = self._estimate_rwd_pun_from_pool_items(rows, trace_id=trace_id, tick_id=tick_id)
        except Exception:
            rwd_pun_override = None

        # External teacher reward/punish can add on top of pool aggregation.
        # sanitized
        try:
            tfb = report.get("teacher_feedback", {}) if isinstance(report.get("teacher_feedback", {}), dict) else {}
            teacher_rwd = float(tfb.get("teacher_rwd", 0.0) or 0.0)
            teacher_pun = float(tfb.get("teacher_pun", 0.0) or 0.0)
            if teacher_rwd > 0.0 or teacher_pun > 0.0:
                base = dict(rwd_pun_override or {})
                base_rwd = float(base.get("rwd", 0.0) or 0.0)
                base_pun = float(base.get("pun", 0.0) or 0.0)
                merged_rwd = self._clamp01(base_rwd + max(0.0, teacher_rwd))
                merged_pun = self._clamp01(base_pun + max(0.0, teacher_pun))
                detail = dict(base.get("detail", {}) or {}) if isinstance(base.get("detail", {}), dict) else {}
                detail.update(
                    {
                        "teacher_rwd": round(float(max(0.0, teacher_rwd)), 8),
                        "teacher_pun": round(float(max(0.0, teacher_pun)), 8),
                        "teacher_mode": str(tfb.get("mode", "") or ""),
                        "teacher_anchor": str(tfb.get("anchor", "") or ""),
                    }
                )
                rwd_pun_override = {
                    **base,
                    "rwd": round(float(merged_rwd), 8),
                    "pun": round(float(merged_pun), 8),
                    "source": f"{str(base.get('source', '') or 'pool_items')}+teacher",
                    "detail": detail,
                }
        except Exception:
            pass
        emotion_result = self.emotion.update_emotion_state(
            {
                "cfs_signals": cfs_signals,
                "tick_id": tick_id,
                "emotion_updates": focus_data.get("emotion_updates", {}),
                "rwd_pun_override": rwd_pun_override or {},
            },
            trace_id=trace_id,
            tick_id=tick_id,
        )
        emotion_data = emotion_result.get("data", {}) or {}
        report["emotion"] = emotion_data
        timing_steps_ms["emotion_ms"] = int((time.perf_counter() - t0) * 1000)
        # sanitized
        # sanitized
        next_modulation: dict[str, Any] = dict(emotion_data.get("modulation", {}) or {})

        # =============================================================== #
        # sanitized
        # =============================================================== #

        # sanitized
        # sanitized
        # sanitized
        # sanitized
        # sanitized
        t0 = time.perf_counter()
        action_result = self.action.run_action_cycle(
            trace_id=trace_id,
            tick_id=tick_id,
            tick_index=int(self.tick_counter),
            cfs_signals=cfs_signals,
            emotion_state=emotion_data,
            innate_focus_directives=new_directives,
            innate_action_triggers=new_action_triggers,
            memory_activation_snapshot=memory_snapshot,
        )
        action_data = action_result.get("data", {}) or {}
        report["action"] = action_data
        timing_steps_ms["action_ms"] = int((time.perf_counter() - t0) * 1000)

        # sanitized
        focus_directives_out = list(action_data.get("focus_directives_out", []) or [])
        if focus_directives_out:
            # sanitized
            existing_by_id = {
                str(item.get("directive_id", "")): item
                for item in self._pending_focus_directives
                if isinstance(item, dict) and str(item.get("directive_id", ""))
            }
            for directive in focus_directives_out:
                if not isinstance(directive, dict):
                    continue
                did = str(directive.get("directive_id", ""))
                if not did:
                    continue
                existing_by_id[did] = directive
            self._pending_focus_directives = list(existing_by_id.values())

        # sanitized
        action_mod_out = action_data.get("modulation_out", {}) or {}
        if isinstance(action_mod_out, dict):
            for key, value in action_mod_out.items():
                if isinstance(value, dict) and isinstance(next_modulation.get(key), dict):
                    next_modulation[key] = {**dict(next_modulation.get(key) or {}), **dict(value)}
                else:
                    next_modulation[key] = value
        # sanitized
        # sanitized
        # sanitized

        # =============================================================== #
        # sanitized
        # =============================================================== #
        # sanitized
        # sanitized
        # sanitized
        #
        # sanitized
        # sanitized
        # sanitized
        # sanitized
        t0 = time.perf_counter()
        recall_requests = [x for x in (action_data.get("recall_requests_out", []) or []) if isinstance(x, dict)]
        recall_apply_results: list[dict[str, Any]] = []
        recall_feedback_results: list[dict[str, Any]] = []
        recall_total_target_count = 0

        for req in recall_requests:
            targets = list(req.get("map_targets", req.get("targets", [])) or [])
            targets = [t for t in targets if isinstance(t, dict)]
            if not targets:
                continue
            recall_total_target_count += len(targets)

            apply_data = self.hdb.apply_memory_activation_targets(
                targets=targets,
                trace_id=f"{trace_id}_recall_map",
                tick_id=tick_id,
            ).get("data", {}) or {}
            recall_apply_results.append(apply_data)

            # NOTE:
            # sanitized
            # sanitized
            fb_data = self._apply_memory_feedback(
                memory_items=list(apply_data.get("items", []) or []),
                trace_id=f"{trace_id}_recall_feedback",
                tick_id=tick_id,
            )
            recall_feedback_results.append(fb_data)

        recall_memory_snapshot_after: dict[str, Any] = {}
        if recall_apply_results:
            try:
                recall_memory_snapshot_after = self.hdb.get_memory_activation_snapshot(
                    trace_id=f"{trace_id}_recall_map_snapshot",
                    limit=16,
                    sort_by="energy_desc",
                ).get("data", {}) or {}
            except Exception:
                recall_memory_snapshot_after = {}

        if recall_requests:
            action_data["recall_side_effects"] = {
                "request_count": len(recall_requests),
                "target_count": int(recall_total_target_count),
                "apply_results": recall_apply_results,
                "feedback_results": recall_feedback_results,
                "memory_snapshot_after": recall_memory_snapshot_after,
            }
            # sanitized
            report.setdefault("memory_activation", {})
            report["memory_activation"]["snapshot_after_action"] = recall_memory_snapshot_after

        timing_steps_ms["action_recall_side_effect_ms"] = int((time.perf_counter() - t0) * 1000)

        t0 = time.perf_counter()
        final_state_snapshot = self.pool.get_state_snapshot(
            trace_id=f"{trace_id}_final_snapshot",
            tick_id=tick_id,
            top_k=None,
        )["data"]["snapshot"]
        hdb_snapshot = self.hdb.get_hdb_snapshot(trace_id=f"{trace_id}_hdb_snapshot", top_k=12)["data"]
        report["final_state"] = {
            "state_snapshot": final_state_snapshot,
            "state_energy_summary": self._summarize_state_snapshot(final_state_snapshot),
            "hdb_snapshot": hdb_snapshot,
        }
        timing_steps_ms["final_snapshot_ms"] = int((time.perf_counter() - t0) * 1000)

        # =============================================================== #
        # sanitized
        # =============================================================== #
        # sanitized
        # sanitized
        # sanitized
        # sanitized
        t0 = time.perf_counter()
        ebc_data: dict[str, Any] = {}
        try:
            es = report.get("final_state", {}).get("state_energy_summary", {}) or {}
            total_er = float(es.get("total_er", 0.0) or 0.0)
            total_ev = float(es.get("total_ev", 0.0) or 0.0)
            ebc_res = self.energy_balance.update_from_energy_summary(
                trace_id=f"{trace_id}_ebc",
                tick_id=tick_id,
                tick_index=int(self.tick_counter),
                total_er=total_er,
                total_ev=total_ev,
            )
            if isinstance(ebc_res, dict):
                ebc_data = ebc_res.get("data", {}) or {}
        except Exception as exc:
            ebc_data = {"error": str(exc)}
        report["energy_balance"] = ebc_data
        timing_steps_ms["energy_balance_ms"] = int((time.perf_counter() - t0) * 1000)

        # Merge EBC HDB scales into next_modulation (multiplicative).
        # sanitized
        try:
            hdb_scales = ebc_data.get("hdb_scales_out", {}) if isinstance(ebc_data, dict) else {}
            if isinstance(hdb_scales, dict) and hdb_scales:
                hdb_mod = next_modulation.get("hdb", {}) if isinstance(next_modulation.get("hdb", {}), dict) else {}
                hdb_mod = dict(hdb_mod)
                for k, v in hdb_scales.items():
                    key = str(k or "").strip()
                    if not key:
                        continue
                    try:
                        scale = float(v or 1.0)
                    except Exception:
                        scale = 1.0
                    if not (scale > 0.0):
                        scale = 1.0
                    try:
                        existing = float(hdb_mod.get(key, 1.0) or 1.0)
                    except Exception:
                        existing = 1.0
                    hdb_mod[key] = round(float(existing) * float(scale), 8)
                next_modulation["hdb"] = hdb_mod
        except Exception:
            pass

        # Commit the final merged modulation for the next tick.
        # sanitized
        self._last_modulation = dict(next_modulation)

        # sanitized
        total_logic_ms = int((time.perf_counter() - cycle_t0) * 1000)
        timing_steps_ms["total_logic_ms"] = int(total_logic_ms)
        report["timing"] = {
            "total_logic_ms": int(total_logic_ms),
            "steps_ms": dict(timing_steps_ms),
        }
        report["finished_at"] = int(time.time() * 1000)
        report["exports"] = self._export_report(trace_id, report)
        self._last_report = report
        self._report_history.append(report)
        history_limit = max(1, int(self._config.get("history_limit", 24)))
        if len(self._report_history) > history_limit:
            self._report_history = self._report_history[-history_limit:]
        if self._config.get("auto_open_html_report", False):
            self.open_report(trace_id, open_browser=True)
        return report

    def show_state_snapshot(self, top_k: str | int | None = None) -> str:
        snapshot = self.pool.get_state_snapshot(trace_id="cmd_snap", top_k=None if top_k == "all" else top_k)["data"]["snapshot"]
        return render_state_snapshot(snapshot, None if top_k == "all" else top_k)

    def show_hdb_snapshot(self) -> str:
        snapshot = self.hdb.get_hdb_snapshot(trace_id="cmd_hdb", top_k=12)["data"]
        return render_hdb_snapshot(snapshot)

    def show_structure(self, structure_id: str) -> str:
        result = self.hdb.query_structure_database(structure_id=structure_id, trace_id="cmd_st")
        if not result["success"]:
            return result["message"]
        return render_structure_report(result["data"])

    def show_group(self, group_id: str) -> str:
        result = self.hdb.query_group(group_id=group_id, trace_id="cmd_sg")
        if not result["success"]:
            return result["message"]
        return render_group_report(result["data"])

    def show_episodic(self, limit: int = 10) -> str:
        result = self.hdb.get_recent_episodic(trace_id="cmd_em", limit=limit)
        return render_episodic_report(result["data"])

    def open_report(self, target: str = "latest", *, open_browser: bool = True) -> str:
        html_path = self.output_dir / ("latest.html" if target in {"", "latest"} else f"{target}.html")
        if not html_path.exists():
            return f"未找到报告 / Report not found: {html_path}"
        opened = False
        if open_browser:
            try:
                opened = webbrowser.open(html_path.resolve().as_uri())
            except Exception:
                opened = False
        return json.dumps(
            {
                "html_path": str(html_path),
                "opened": opened,
            },
            ensure_ascii=False,
            indent=2,
        )

    def run_tick_cycles(self, count: int = 1) -> list[dict]:
        reports = []
        for _ in range(max(1, int(count))):
            reports.append(self.run_cycle(text=None))
        return reports

    def get_last_report(self) -> dict[str, Any] | None:
        return self._last_report

    def get_report(self, trace_id: str = "latest") -> dict[str, Any] | None:
        if trace_id in {"", "latest"}:
            return self._last_report
        report_path = self.output_dir / f"{trace_id}.json"
        if not report_path.exists():
            return None
        try:
            return json.loads(report_path.read_text(encoding="utf-8"))
        except Exception:
            return None

    def get_recent_cycle_summaries(self, limit: int | None = None) -> list[dict]:
        items = self._report_history[-(limit or len(self._report_history)) :]
        summaries = []
        for report in reversed(items):
            matched_structure_ids = list(report.get("stimulus_level", {}).get("result", {}).get("matched_structure_ids", []))
            new_structure_ids = list(report.get("stimulus_level", {}).get("result", {}).get("new_structure_ids", []))
            matched_group_ids = list(report.get("structure_level", {}).get("result", {}).get("matched_group_ids", []))
            new_group_ids = list(report.get("structure_level", {}).get("result", {}).get("new_group_ids", []))
            summaries.append(
                {
                    "trace_id": report.get("trace_id", ""),
                    "started_at": report.get("started_at", 0),
                    "finished_at": report.get("finished_at", 0),
                    "input_text": report.get("sensor", {}).get("input_text", ""),
                    "sensor_mode": report.get("sensor", {}).get("mode", ""),
                    "structure_rounds": report.get("structure_level", {}).get("result", {}).get("round_count", 0),
                    "stimulus_rounds": report.get("stimulus_level", {}).get("result", {}).get("round_count", 0),
                    "attention_memory_count": report.get("attention", {}).get("memory_item_count", 0),
                    "attention_consumed_total": report.get("attention", {}).get("consumed_total_energy", 0.0),
                    "matched_structures": matched_structure_ids,
                    "new_structures": new_structure_ids,
                    "matched_groups": matched_group_ids,
                    "new_groups": new_group_ids,
                    "matched_structure_refs": self._build_cycle_structure_refs(matched_structure_ids),
                    "new_structure_refs": self._build_cycle_structure_refs(new_structure_ids),
                    "matched_group_refs": self._build_cycle_group_refs(matched_group_ids),
                    "new_group_refs": self._build_cycle_group_refs(new_group_ids),
                    "total_delta_ev": report.get("induction", {}).get("result", {}).get("total_delta_ev", 0.0),
                    "memory_activation_applied_count": report.get("memory_activation", {}).get("apply_result", {}).get("applied_count", 0),
                    "memory_feedback_applied_count": report.get("memory_activation", {}).get("feedback_result", {}).get("applied_count", 0),
                    "memory_feedback_total_er": report.get("memory_activation", {}).get("feedback_result", {}).get("total_feedback_er", 0.0),
                    "memory_feedback_total_ev": report.get("memory_activation", {}).get("feedback_result", {}).get("total_feedback_ev", 0.0),
                    "memory_activation_total_er": report.get("memory_activation", {}).get("snapshot", {}).get("summary", {}).get("total_er", 0.0),
                    "memory_activation_total_ev": report.get("memory_activation", {}).get("snapshot", {}).get("summary", {}).get("total_ev", 0.0),
                    "cfs_signal_count": len(report.get("cognitive_feeling", {}).get("cfs_signals", []) or []),
                    "nt_state": dict(report.get("emotion", {}).get("nt_state_after", {}) or {}),
                }
            )
        return summaries

    def _build_cycle_structure_refs(self, structure_ids: list[str]) -> list[dict[str, Any]]:
        refs: list[dict[str, Any]] = []
        for structure_id in list(dict.fromkeys(structure_ids)):
            if not structure_id:
                continue
            structure_obj = self.hdb._structure_store.get(structure_id)
            display_text = structure_id
            signature = ""
            flat_tokens: list[str] = []
            if structure_obj:
                payload = structure_obj.get("structure", {})
                display_text = payload.get("display_text", structure_id)
                signature = payload.get("content_signature", "")
                flat_tokens = list(payload.get("flat_tokens", []))
            refs.append(
                {
                    "structure_id": structure_id,
                    "display_text": display_text,
                    "content_signature": signature,
                    "flat_tokens": flat_tokens,
                }
            )
        return refs

    def _build_cycle_group_refs(self, group_ids: list[str]) -> list[dict[str, Any]]:
        refs: list[dict[str, Any]] = []
        for group_id in list(dict.fromkeys(group_ids)):
            if not group_id:
                continue
            if group_id.startswith("sg_single_"):
                structure_id = group_id.removeprefix("sg_single_")
                refs.append(
                    {
                        "group_id": group_id,
                        "synthetic": True,
                        "required_structures": self._build_cycle_structure_refs([structure_id] if structure_id else []),
                        "bias_structures": [],
                    }
                )
                continue
            group_obj = self.hdb._group_store.get(group_id)
            required_ids = list(group_obj.get("required_structure_ids", [])) if group_obj else []
            bias_ids = list(group_obj.get("bias_structure_ids", [])) if group_obj else []
            refs.append(
                {
                    "group_id": group_id,
                    "required_structures": self._build_cycle_structure_refs(required_ids),
                    "bias_structures": self._build_cycle_structure_refs(bias_ids),
                }
            )
        return refs

    def get_dashboard_data(self) -> dict[str, Any]:
        snapshot_top_k = int(self._config.get("snapshot_top_k", 24))
        state_snapshot = self.pool.get_state_snapshot(
            trace_id="dashboard_state",
            top_k=snapshot_top_k,
        )["data"]["snapshot"]
        hdb_snapshot = self.hdb.get_hdb_snapshot(trace_id="dashboard_hdb", top_k=snapshot_top_k)["data"]
        sensor_runtime = self.sensor.get_runtime_snapshot(trace_id="dashboard_sensor")["data"]
        time_sensor_runtime = self.time_sensor.get_runtime_snapshot(trace_id="dashboard_time_sensor")["data"]
        # sanitized
        energy_balance_runtime = {}
        try:
            energy_balance_runtime = self.energy_balance.get_runtime_snapshot(trace_id="dashboard_energy_balance")  # type: ignore[attr-defined]
        except Exception:
            energy_balance_runtime = {}
        return {
            "meta": {
                "started_at": self._started_at,
                "tick_counter": self.tick_counter,
                "last_cycle_id": self._last_report.get("trace_id", "") if self._last_report else "",
                "output_dir": str(self.output_dir),
            },
            "last_report": self._last_report,
            "recent_cycles": self.get_recent_cycle_summaries(limit=int(self._config.get("history_limit", 24))),
            "state_snapshot": state_snapshot,
            "state_energy_summary": self._summarize_state_snapshot(state_snapshot),
            "hdb_snapshot": hdb_snapshot,
            "sensor_runtime": sensor_runtime,
            "time_sensor_runtime": time_sensor_runtime,
            "energy_balance_runtime": energy_balance_runtime,
            "module_configs": self.get_config_bundle(),
            "placeholder_modules": self.get_placeholder_modules(),
        }

    def get_state_snapshot_data(self, top_k: int | None = None) -> dict[str, Any]:
        snapshot = self.pool.get_state_snapshot(
            trace_id="api_state_snapshot",
            top_k=top_k,
        )["data"]["snapshot"]
        return {
            "snapshot": snapshot,
            "energy_summary": self._summarize_state_snapshot(snapshot),
        }

    def get_hdb_snapshot_data(self, top_k: int = 12) -> dict[str, Any]:
        return self.hdb.get_hdb_snapshot(trace_id="api_hdb_snapshot", top_k=top_k)["data"]

    def get_action_runtime_data(self) -> dict[str, Any]:
        """
        Action runtime snapshot for real-time monitoring.
        # sanitized
        """
        if not getattr(self, "action", None):
            return {"enabled": False, "message": "行动模块未初始化 / action module not initialized", "data": {}}
        return self.action.get_runtime_snapshot(trace_id="api_action_runtime")["data"]

    def stop_action_nodes(
        self,
        *,
        mode: str,
        value: Any = None,
        hold_ticks: int = 2,
        reason: str = "manual_stop",
        trace_id: str = "api_action_stop",
    ) -> dict[str, Any]:
        """
        Stop/cancel action nodes (exposed to Web UI).
        # sanitized
        """
        if not getattr(self, "action", None):
            return {"success": False, "code": "STATE_ERROR", "message": "行动模块未初始化 / action not initialized", "data": {}}
        res = self.action.stop_actions(
            trace_id=trace_id,
            mode=str(mode or ""),
            value=value,
            hold_ticks=int(hold_ticks or 0),
            reason=str(reason or "manual_stop"),
        )
        return res

    def get_structure_data(self, structure_id: str) -> dict[str, Any]:
        result = self.hdb.query_structure_database(structure_id=structure_id, trace_id="api_structure")
        if not result["success"]:
            raise ValueError(result["message"])
        return result["data"]

    def get_group_data(self, group_id: str) -> dict[str, Any]:
        if group_id.startswith("sg_single_"):
            structure_id = group_id.removeprefix("sg_single_")
            return {
                "group": {
                    "id": group_id,
                    "synthetic": True,
                    "group_kind": "implicit_single_st",
                    "required_structure_ids": [structure_id] if structure_id else [],
                    "bias_structure_ids": [],
                    "avg_energy_profile": {structure_id: 1.0} if structure_id else {},
                },
                "required_structures": self._build_cycle_structure_refs([structure_id] if structure_id else []),
                "bias_structures": [],
            }
        result = self.hdb.query_group(group_id=group_id, trace_id="api_group")
        if not result["success"]:
            raise ValueError(result["message"])
        return result["data"]

    def get_episodic_data(self, limit: int = 10) -> dict[str, Any]:
        return self.hdb.get_recent_episodic(trace_id="api_episodic", limit=limit)["data"]

    def run_check(self, target: str | None = None) -> str:
        result = self.hdb.self_check_hdb(trace_id="cmd_check", target_id=target)
        return render_check_report(result["data"])

    def run_repair(self, target: str) -> str:
        result = self.hdb.repair_hdb(
            trace_id="cmd_repair",
            target_id=target,
            repair_scope="targeted",
            background=False,
        )
        return render_repair_report(result["data"])

    def run_repair_all(self) -> str:
        result = self.hdb.repair_hdb(
            trace_id="cmd_repair_all",
            repair_scope="global_quick",
            background=True,
        )
        return render_repair_report(result["data"])

    def stop_repair(self, job_id: str) -> str:
        result = self.hdb.stop_repair_job(repair_job_id=job_id, trace_id="cmd_stop_repair")
        if not result["success"]:
            return result["message"]
        return render_repair_report(result["data"])

    def clear_hdb(self) -> str:
        result = self.hdb.clear_hdb(trace_id="cmd_clear_hdb", reason="interactive_reset", operator="researcher")
        return json.dumps(result["data"], ensure_ascii=False, indent=2)

    def clear_all(self) -> str:
        self.sensor.clear_echo_pool(trace_id="cmd_clear_sensor")
        self.pool.clear_state_pool(trace_id="cmd_clear_pool", reason="interactive_reset", operator="researcher")
        result = self.hdb.clear_hdb(trace_id="cmd_clear_all", reason="interactive_reset", operator="researcher")
        try:
            self.time_sensor.clear_runtime_state(trace_id="cmd_clear_time_sensor", reason="interactive_reset")
        except Exception:
            pass
        try:
            self.action.clear_runtime_state(trace_id="cmd_clear_action", reason="interactive_reset")
        except Exception:
            pass
        try:
            self.attention.clear_runtime_state(trace_id="cmd_clear_attention", reason="interactive_reset")
        except Exception:
            pass
        try:
            self.cognitive_stitching.clear_runtime_state(trace_id="cmd_clear_cognitive_stitching", reason="interactive_reset")
        except Exception:
            pass
        self._last_report = None
        self._report_history = []
        return json.dumps(result["data"], ensure_ascii=False, indent=2)

    def show_config(self) -> str:
        payload = {
            "sensor_backend": self.sensor.get_runtime_snapshot()["data"]["config_summary"]["tokenizer_backend"],
            "sensor_tokenizer_available": self.sensor.get_runtime_snapshot()["data"]["config_summary"]["tokenizer_available"],
            "hdb_core": {
                key: self.hdb._config[key]
                for key in [
                    "stimulus_level_max_rounds",
                    "structure_level_max_rounds",
                    "ev_propagation_threshold",
                    "er_induction_threshold",
                    "fallback_lookup_max_candidates",
                ]
            },
            "observatory": dict(self._config),
            "observatory_config_path": self._config_path,
            "output_dir": str(self.output_dir),
        }
        return json.dumps(payload, ensure_ascii=False, indent=2)

    def reload_all(self) -> str:
        self._config = self._build_config()
        payload = {
            "observatory": "OK",
            "text_sensor": self.sensor.reload_config(trace_id="cmd_reload_sensor")["code"],
            "time_sensor": self.time_sensor.reload_config(trace_id="cmd_reload_time_sensor")["code"],
            "state_pool": self.pool.reload_config(trace_id="cmd_reload_pool")["code"],
            "hdb": self.hdb.reload_config(trace_id="cmd_reload_hdb")["code"],
            "attention": self.attention.reload_config(trace_id="cmd_reload_attention")["code"],
            "cognitive_feeling": self.cfs.reload_config(trace_id="cmd_reload_cfs")["code"],
            "emotion": self.emotion.reload_config(trace_id="cmd_reload_emotion")["code"],
            "innate_script": self.iesm.reload_config(trace_id="cmd_reload_iesm")["code"],
            "action": self.action.reload_config(trace_id="cmd_reload_action")["code"],
        }
        self._apply_runtime_overrides()
        return json.dumps(payload, ensure_ascii=False, indent=2)

    def loop(self) -> None:
        self.print_header()
        try:
            while True:
                line = input("\nAP-OBS> ").strip().lstrip("\ufeff\ufffe")
                if not line:
                    continue
                if line in {"quit", "exit"}:
                    break
                if line == "help":
                    print(format_help())
                    continue
                if line.startswith("text "):
                    print(render_cycle_report(self.run_cycle(text=line[5:])))
                    continue

                parts = shlex.split(line)
                cmd = parts[0]
                if cmd == "tick":
                    count = int(parts[1]) if len(parts) > 1 else 1
                    for _ in range(max(1, count)):
                        print(render_cycle_report(self.run_cycle(text=None)))
                elif cmd == "snap":
                    arg = parts[1] if len(parts) > 1 else str(self._config["snapshot_top_k"])
                    top_k: str | int | None = "all" if arg == "all" else int(arg)
                    print(self.show_state_snapshot(top_k))
                elif cmd == "hdb":
                    print(self.show_hdb_snapshot())
                elif cmd == "st" and len(parts) > 1:
                    print(self.show_structure(parts[1]))
                elif cmd == "sg" and len(parts) > 1:
                    print(self.show_group(parts[1]))
                elif cmd == "em":
                    limit = int(parts[1]) if len(parts) > 1 else 10
                    print(self.show_episodic(limit))
                elif cmd == "check":
                    target = parts[1] if len(parts) > 1 else None
                    print(self.run_check(target))
                elif cmd == "repair" and len(parts) > 1:
                    print(self.run_repair(parts[1]))
                elif cmd == "repair_all":
                    print(self.run_repair_all())
                elif cmd == "stop_repair" and len(parts) > 1:
                    print(self.stop_repair(parts[1]))
                elif cmd == "clear_hdb":
                    print(self.clear_hdb())
                elif cmd == "clear_all":
                    print(self.clear_all())
                elif cmd == "config":
                    print(self.show_config())
                elif cmd == "reload":
                    print(self.reload_all())
                elif cmd == "open_report":
                    target = parts[1] if len(parts) > 1 else "latest"
                    print(self.open_report(target))
                else:
                    print(render_cycle_report(self.run_cycle(text=line)))
        finally:
            self.close()

    def _build_config(self, config_override: dict | None = None) -> dict:
        config = dict(DEFAULT_CONFIG)
        config.update(_load_yaml_config(self._config_path))
        if config_override:
            config.update(config_override)
        return config

    def _sensor_config_override(self) -> dict[str, Any]:
        goal_b_char_sa_string_mode = bool(self._config.get("enable_goal_b_char_sa_string_mode", False))
        default_mode = self._config.get("sensor_default_mode", "advanced")
        tokenizer_backend = self._config.get("sensor_tokenizer_backend", "jieba")
        enable_token_output = bool(self._config.get("sensor_enable_token_output", True))
        enable_char_output = bool(self._config.get("sensor_enable_char_output", False))
        if goal_b_char_sa_string_mode:
            default_mode = "simple"
            tokenizer_backend = "none"
            enable_token_output = False
            enable_char_output = True
        return {
            "default_mode": default_mode,
            "tokenizer_backend": tokenizer_backend,
            "enable_goal_b_char_sa_string_mode": goal_b_char_sa_string_mode,
            "enable_token_output": enable_token_output,
            "enable_char_output": enable_char_output,
            "enable_echo": bool(self._config.get("sensor_enable_echo", True)),
            "include_echoes_in_stimulus_packet_objects": bool(self._config.get("sensor_include_echoes_in_packet", True)),
        }

    def _state_pool_config_override(self) -> dict[str, Any]:
        return {
            "enable_placeholder_interfaces": bool(self._config.get("state_pool_enable_placeholder_interfaces", False)),
            "enable_script_broadcast": bool(self._config.get("state_pool_enable_script_broadcast", False)),
        }

    def _hdb_config_override(self) -> dict[str, Any]:
        override = {
            "enable_background_repair": bool(self._config.get("hdb_enable_background_repair", True)),
            "enable_goal_b_char_sa_string_mode": bool(self._config.get("enable_goal_b_char_sa_string_mode", False)),
        }
        if self._config.get("hdb_data_dir"):
            override["data_dir"] = self._config.get("hdb_data_dir")
        return override

    def _apply_hdb_modulation_for_tick(
        self,
        *,
        modulation: dict[str, Any] | None,
        trace_id: str,
        tick_id: str,
    ) -> dict[str, Any]:
        """
        Apply HDB modulation scales for the current tick (best-effort).
        # sanitized

        Design / 闁荤姳鐒﹀畷姗€顢橀崫銉﹀暫濞达絽鎼禒顖炴煥?
        - modulation 闂佸搫顦崕鎾吹濠婂嫮鈻斿┑鐘辫兌椤?tick 闂?EMgr/Action 闁哄鐗婇幐鎼佸吹椤撱垺鏅柛褏娅漧f._last_modulation["hdb"]闂佹寧绋戦ˇ顓㈠焵?
        # sanitized
          闂佺粯甯熷▔娑㈠箖濡ゅ懎绀冪€广儱妫楅惁?scale 闁荤姳绶ょ槐鏇㈡偩?effective 闂佺锕﹂鏇㈠焵?
        # sanitized
        """
        mod = modulation if isinstance(modulation, dict) else {}
        base = getattr(self, "_hdb_config_base", None)
        if not isinstance(base, dict) or not base:
            base = dict(getattr(self.hdb, "_config", {}) or {})
            self._hdb_config_base = dict(base)

        applied: dict[str, Any] = {}

        def apply_scale(scale_key: str, cfg_key: str, *, min_value: float | None = None) -> None:
            # Reset to baseline first (avoid drift).
            try:
                base_val = float(base.get(cfg_key, self.hdb._config.get(cfg_key, 0.0) or 0.0) or 0.0)
            except Exception:
                base_val = float(self.hdb._config.get(cfg_key, 0.0) or 0.0)
            self.hdb._config[cfg_key] = base_val

            try:
                scale = float(mod.get(scale_key, 1.0) or 1.0)
            except Exception:
                scale = 1.0
            if not (scale > 0.0):
                scale = 1.0

            eff = float(base_val) * float(scale)
            if min_value is not None:
                eff = max(float(min_value), float(eff))
            self.hdb._config[cfg_key] = float(eff)

            # Only record when non-trivial (still reset even if trivial).
            if abs(float(scale) - 1.0) > 1e-9:
                applied[cfg_key] = {
                    "base": round(float(base_val), 8),
                    "scale": round(float(scale), 8),
                    "effective": round(float(eff), 8),
                    "scale_key": scale_key,
                }

        # sanitized
        apply_scale("base_weight_er_gain_scale", "base_weight_er_gain", min_value=0.0)
        apply_scale("base_weight_ev_wear_scale", "base_weight_ev_wear", min_value=0.0)
        # sanitized
        apply_scale("ev_propagation_threshold_scale", "ev_propagation_threshold", min_value=0.0)
        apply_scale("ev_propagation_ratio_scale", "ev_propagation_ratio", min_value=0.0)
        apply_scale("er_induction_ratio_scale", "er_induction_ratio", min_value=0.0)

        try:
            # Update only the affected engines (fast enough for prototype).
            # sanitized
            self.hdb._weight.update_config(self.hdb._config)
            self.hdb._stimulus.update_config(self.hdb._config)
            self.hdb._structure_retrieval.update_config(self.hdb._config)
            self.hdb._induction.update_config(self.hdb._config)
        except Exception as exc:
            return {"error": str(exc), "applied": applied}

        return {"applied": applied, "base_refreshed": True, "tick_id": tick_id, "trace_id": trace_id}

    def _attention_config_override(self) -> dict[str, Any]:
        """Runtime overrides for the attention module."""
        return {
            "top_n": int(self._config.get("attention_top_n", 16)),
            "consume_energy": bool(self._config.get("attention_stub_consume_energy", True)),
            "memory_energy_ratio": float(self._config.get("attention_memory_energy_ratio", 0.5)),
        }

    def _cut_engine_config_override(self) -> dict[str, Any]:
        return {
            "enable_goal_b_char_sa_string_mode": bool(self._config.get("enable_goal_b_char_sa_string_mode", False)),
        }

    def _apply_runtime_overrides(self) -> None:
        sensor_override = self._sensor_config_override()
        self.sensor._config.update(sensor_override)
        self.sensor._normalizer.update_config(self.sensor._config)
        self.sensor._segmenter.update_config(self.sensor._config)
        self.sensor._scorer.update_config(self.sensor._config)
        self.sensor._echo_mgr.update_config(self.sensor._config)
        self.sensor._logger.update_config(
            log_dir=self.sensor._config.get("log_dir", ""),
            max_file_bytes=self.sensor._config.get("log_max_file_bytes", 0),
        )

        pool_override = self._state_pool_config_override()
        self.pool._config.update(pool_override)
        self.pool._store.update_config(self.pool._config)
        self.pool._energy.update_config(self.pool._config)
        self.pool._neutralization.update_config(self.pool._config)
        self.pool._merge.update_config(self.pool._config)
        self.pool._binding.update_config(self.pool._config)
        self.pool._maintenance.update_config(self.pool._config)
        self.pool._snapshot.update_config(self.pool._config)
        self.pool._history.update_config(self.pool._config)
        self.pool._logger.update_config(
            log_dir=self.pool._config.get("log_dir", ""),
            max_file_bytes=self.pool._config.get("log_max_file_bytes", 0),
        )

        hdb_override = self._hdb_config_override()
        self.hdb._config.update(hdb_override)
        self.hdb._weight.update_config(self.hdb._config)
        self.hdb._pointer_index.update_config(self.hdb._config)
        self.hdb._maintenance.update_config(self.hdb._config)
        self.hdb._snapshot.update_config(self.hdb._config)
        self.hdb._cut.update_config(self.hdb._config)
        self.hdb._stimulus.update_config(self.hdb._config)
        self.hdb._structure_retrieval.update_config(self.hdb._config)
        self.hdb._induction.update_config(self.hdb._config)
        self.hdb._memory_activation_store.update_config(self.hdb._config)
        self.hdb._self_check.update_config(self.hdb._config)
        self.hdb._delete.update_config(self.hdb._config)
        self.hdb._repair.update_config(self.hdb._config)
        self.hdb._logger.update_config(
            log_dir=self.hdb._config.get("log_dir", ""),
            max_file_bytes=int(self.hdb._config.get("log_max_file_bytes", 0)),
        )
        # Refresh baseline after config changes (avoid drift in per-tick modulation).
        # sanitized
        self.cut_engine.update_config(self._cut_engine_config_override())
        self._hdb_config_base = dict(self.hdb._config)

        self.cut_engine.update_config(self._cut_engine_config_override())
        attention_override = self._attention_config_override()
        self.attention._config.update(attention_override)
        self.attention._logger.update_config(
            log_dir=self.attention._config.get("log_dir", ""),
            max_file_bytes=int(self.attention._config.get("log_max_file_bytes", 0)),
        )

    def _module_config_specs(self) -> dict[str, dict[str, Any]]:
        return {
            "observatory": {
                "path": self._config_path,
                "defaults": dict(DEFAULT_CONFIG),
                "effective": lambda: dict(self._config),
                "runtime_override": lambda: {},
            },
            "text_sensor": {
                "path": self.sensor._config_path,
                "defaults": dict(TEXT_SENSOR_DEFAULT_CONFIG),
                "effective": lambda: dict(self.sensor._config),
                "runtime_override": self._sensor_config_override,
            },
            "time_sensor": {
                "path": self.time_sensor._config_path,
                "defaults": dict(TIME_SENSOR_DEFAULT_CONFIG),
                "effective": lambda: dict(self.time_sensor._config),
                "runtime_override": lambda: {},
            },
            "state_pool": {
                "path": self.pool._config_path,
                "defaults": dict(STATE_POOL_DEFAULT_CONFIG),
                "effective": lambda: dict(self.pool._config),
                "runtime_override": self._state_pool_config_override,
            },
            "hdb": {
                "path": self.hdb._config_path,
                "defaults": dict(HDB_DEFAULT_CONFIG),
                "effective": lambda: dict(self.hdb._config),
                "runtime_override": self._hdb_config_override,
            },
            "attention": {
                "path": self.attention._config_path,
                "defaults": dict(ATTENTION_DEFAULT_CONFIG),
                "effective": lambda: dict(self.attention._config),
                "runtime_override": self._attention_config_override,
            },
            "cognitive_stitching": {
                "path": self.cognitive_stitching._config_path,
                "defaults": dict(COGNITIVE_STITCHING_DEFAULT_CONFIG),
                "effective": lambda: dict(self.cognitive_stitching._config),
                "runtime_override": lambda: {},
            },
            "cognitive_feeling": {
                "path": self.cfs._config_path,
                "defaults": dict(CFS_DEFAULT_CONFIG),
                "effective": lambda: dict(self.cfs._config),
                "runtime_override": lambda: {},
            },
            "emotion": {
                "path": self.emotion._config_path,
                "defaults": dict(EMOTION_DEFAULT_CONFIG),
                "effective": lambda: dict(self.emotion._config),
                "runtime_override": lambda: {},
            },
            "innate_script": {
                "path": self.iesm._config_path,
                "defaults": dict(IESM_DEFAULT_CONFIG),
                "effective": lambda: dict(self.iesm._config),
                "runtime_override": lambda: {},
            },
            "action": {
                "path": self.action._config_path,
                "defaults": dict(ACTION_DEFAULT_CONFIG),
                "effective": lambda: dict(self.action._config),
                "runtime_override": lambda: {},
            },
            "energy_balance": {
                "path": self.energy_balance._config_path,
                "defaults": dict(ENERGY_BALANCE_DEFAULT_CONFIG),
                "effective": lambda: dict(self.energy_balance._config),
                "runtime_override": lambda: {},
            },
        }

    def get_config_bundle(self) -> dict[str, Any]:
        bundle: dict[str, Any] = {}
        for module_name, spec in self._module_config_specs().items():
            bundle[module_name] = build_config_view(
                module_name=module_name,
                path=spec["path"],
                defaults=spec["defaults"],
                file_values=load_yaml_dict(spec["path"]),
                effective=spec["effective"](),
                runtime_override=spec["runtime_override"](),
            )
        return bundle

    def save_module_config(self, module_name: str, values: dict[str, Any]) -> dict[str, Any]:
        normalized_name = str(module_name).strip().lower()
        specs = self._module_config_specs()
        if normalized_name not in specs:
            raise ValueError(f"unsupported module_name: {module_name}")
        spec = specs[normalized_name]
        coerced, rejected = coerce_updates_by_defaults(spec["defaults"], values or {})
        merged = save_annotated_config(
            path=spec["path"],
            defaults=spec["defaults"],
            updates=coerced,
        )
        self.reload_all()
        return {
            "module": normalized_name,
            "path": spec["path"],
            "saved_values": coerced,
            "rejected_values": rejected,
            "file_values": merged,
            "config_bundle": self.get_config_bundle(),
        }

    # =============================================================== #
    # sanitized
    # =============================================================== #

    def get_innate_rules_data(self) -> dict[str, Any]:
        """Expose IESM rules bundle for the web UI."""
        return self.iesm.get_rules_bundle(trace_id="api_innate_rules", include_file_yaml=True)["data"]

    def validate_innate_rules(self, *, doc: dict[str, Any] | None = None, yaml_text: str | None = None) -> dict[str, Any]:
        """Validate innate rules doc/yaml and return normalized preview."""
        result = self.iesm.validate_rules(trace_id="api_innate_rules_validate", doc=doc, yaml_text=yaml_text)
        data = result.get("data", {}) or {}
        return {
            "valid": bool(result.get("success", False)),
            "code": result.get("code", ""),
            "message": result.get("message", ""),
            "errors": list(data.get("errors", []) or []),
            "warnings": list(data.get("warnings", []) or []),
            "normalized_doc": data.get("normalized_doc", {}) or {},
            "yaml_preview": str(data.get("yaml_preview", "") or ""),
        }

    def save_innate_rules(self, *, doc: dict[str, Any] | None = None, yaml_text: str | None = None) -> dict[str, Any]:
        """doc"""
        result = self.iesm.save_rules(trace_id="api_innate_rules_save", doc=doc, yaml_text=yaml_text)
        return {
            "saved": bool(result.get("success", False)),
            "code": result.get("code", ""),
            "message": result.get("message", ""),
            "data": result.get("data", {}) or {},
            "error": result.get("error", {}) or {},
        }

    def reload_innate_rules(self) -> dict[str, Any]:
        """Reload innate rules from disk."""
        result = self.iesm.reload_rules(trace_id="api_innate_rules_reload")
        return {
            "reloaded_ok": bool(result.get("success", False)),
            "code": result.get("code", ""),
            "message": result.get("message", ""),
            "data": result.get("data", {}) or {},
        }

    def simulate_innate_rules(self) -> dict[str, Any]:
        """Simulate rules on the last report context (dry-run)."""
        if not self._last_report:
            return {"ok": False, "message": "no last report yet"}
        trace_id = str(self._last_report.get("trace_id", "latest") or "latest")
        tick_id = trace_id

        cfs_signals = list((self._last_report.get("cognitive_feeling", {}) or {}).get("cfs_signals", []) or [])
        maint_events = list((self._last_report.get("maintenance", {}) or {}).get("events", []) or [])
        apply_events = list((self._last_report.get("pool_apply", {}) or {}).get("events", []) or [])

        try:
            maint_packet = self.pool._snapshot.build_script_check_packet(
                events=maint_events,
                pool_store=self.pool._store,
                trace_id=f"{trace_id}_sim_maint",
                tick_id=tick_id,
            )
        except Exception:
            maint_packet = {}
        try:
            apply_packet = self.pool._snapshot.build_script_check_packet(
                events=apply_events,
                pool_store=self.pool._store,
                trace_id=f"{trace_id}_sim_apply",
                tick_id=tick_id,
            )
        except Exception:
            apply_packet = {}

        # Build context from the last report (prefer report snapshots), so metric predicates can work in simulate.
        # sanitized
        pool_snapshot = (self._last_report.get("final_state", {}) or {}).get("state_snapshot") or {}
        emotion_state = self._last_report.get("emotion", {}) or {}
        sim_context = self._build_innate_rules_context(
            report=self._last_report,
            pool_snapshot=pool_snapshot if isinstance(pool_snapshot, dict) else None,
            emotion_state=emotion_state if isinstance(emotion_state, dict) else None,
            cfs_signals=cfs_signals,
            trace_id=trace_id,
            tick_id=tick_id,
        )

        sim = self.iesm.run_tick_rules(
            trace_id=trace_id,
            tick_id=tick_id,
            # Provide a real tick_index so delta/avg_rate metrics can use history (dry-run won't mutate runtime_state).
            # sanitized
            tick_index=int(self.tick_counter),
            cfs_signals=cfs_signals,
            state_windows=[
                {"stage": "maintenance", "packet": maint_packet},
                {"stage": "pool_apply", "packet": apply_packet},
            ],
            context=sim_context,
            dry_run=True,
        )
        return {"ok": bool(sim.get("success", False)), "code": sim.get("code", ""), "message": sim.get("message", ""), "data": sim.get("data", {}) or {}}

    # ================================================================== #
    # Innate Rules Context + Pool Effects                                 #
    # sanitized
    # ================================================================== #

    def _build_innate_rules_context(
        self,
        *,
        report: dict[str, Any] | None,
        pool_snapshot: dict[str, Any] | None,
        emotion_state: dict[str, Any] | None,
        cfs_signals: list[dict] | None,
        trace_id: str,
        tick_id: str,
    ) -> dict[str, Any]:
        """
        Build the runtime context for IESM metric predicates.
        # sanitized

        # sanitized
          - pool: total_er/total_ev/total_cp_delta/total_cp_abs/energy_concentration/effective_peak_count/complexity_score
          - pool_items: list of item summaries (selectors use display/attrs/etc.)
          - cam: size/energy_concentration (閻熸粎澧楅幐鍛婃櫠閻樻祴鏋栭柕濠忕畱婢瑰牓鎮规担瑙勭凡缂佽鍊归幏鍛村箻鐎涙ê鏋€闁?
          - memory_activation: item_count/total_ev (闁荤姳鐒﹀妯兼崲閸屾粍灏庨悗锝庡幖閸樺瓨鎱ㄩ崷顓炐ｉ柟鎻掔－閹?
          - emotion: {nt:{}, rwd, pun}
          - stimulus: {residual_ratio}
          - retrieval: {stimulus:{best_match_score, grasp_score}}

        Notes / 说明:
        - IESM runs before EMgr update in run_cycle, so emotion_state may be None.
          # sanitized
          # sanitized
        """
        report = report if isinstance(report, dict) else {}
        cfs_signals = list(cfs_signals or [])

        # ---- pool_items ----
        pool_items: list[dict[str, Any]] = []
        if isinstance(pool_snapshot, dict) and isinstance(pool_snapshot.get("top_items"), list):
            for row in pool_snapshot.get("top_items", []) or []:
                if isinstance(row, dict):
                    pool_items.append(dict(row))
        else:
            # Use live pool store (no sort) to avoid heavy snapshots during tick runtime.
            # sanitized
            try:
                all_items = list(self.pool._store.get_all())
            except Exception:
                all_items = []
            for item in all_items:
                if not isinstance(item, dict):
                    continue
                try:
                    summary = self.pool._snapshot._build_top_item_summary(item)  # type: ignore[attr-defined]
                    if isinstance(summary, dict):
                        pool_items.append(summary)
                except Exception:
                    continue

        # Ensure total_energy is available for selector.top_n.
        # sanitized
        input_queue = report.get("input_queue", {}) if isinstance(report.get("input_queue", {}), dict) else {}
        input_source_text = str(input_queue.get("source_text", "") or report.get("sensor", {}).get("input_text", "") or "")
        input_tick_text = str(input_queue.get("tick_text", "") or report.get("sensor", {}).get("input_text", "") or "")
        if input_source_text or input_tick_text:
            input_display = input_source_text or input_tick_text
            input_detail = input_tick_text if input_tick_text and input_tick_text != input_display else ""
            pool_items.append(
                {
                    "item_id": "ctx_input_current",
                    "ref_object_id": "ctx_input_current",
                    "ref_object_type": "input",
                    "display": input_display,
                    "display_text": input_display,
                    "display_detail": input_detail,
                    "attribute_displays": [],
                    "feature_displays": [input_tick_text] if input_tick_text else [],
                    "bound_attribute_displays": [],
                    "er": 0.0,
                    "ev": 0.0,
                    "cp_delta": 0.0,
                    "cp_abs": 0.0,
                    "total_energy": 0.0,
                }
            )

        for row in pool_items:
            try:
                er = float(row.get("er", 0.0) or 0.0)
                ev = float(row.get("ev", 0.0) or 0.0)
                row["total_energy"] = round(max(0.0, er) + max(0.0, ev), 8)
            except Exception:
                row["total_energy"] = 0.0

        total_er = round(sum(float(r.get("er", 0.0) or 0.0) for r in pool_items), 8)
        total_ev = round(sum(float(r.get("ev", 0.0) or 0.0) for r in pool_items), 8)
        total_cp_delta = round(sum(float(r.get("cp_delta", 0.0) or 0.0) for r in pool_items), 8)
        total_cp_abs = round(sum(float(r.get("cp_abs", 0.0) or 0.0) for r in pool_items), 8)

        # Energy concentration (Herfindahl index on (er+ev)).
        # sanitized
        # sanitized
        # sanitized
        energies = [max(0.0, float(r.get("total_energy", 0.0) or 0.0)) for r in pool_items]
        e_sum = float(sum(energies))
        if e_sum > 1e-12:
            energy_concentration = round(sum((e / e_sum) ** 2 for e in energies if e > 1e-12), 8)
        else:
            energy_concentration = 0.0

        # Effective peak count (inverse Herfindahl), roughly interpretable as "number of peaks".
        # sanitized
        #
        # sanitized
        # sanitized
        if float(energy_concentration) > 1e-12:
            effective_peak_count = float(round(1.0 / float(energy_concentration), 8))
        else:
            effective_peak_count = 0.0

        # ---- CAM (Current Attention Memory) ----
        # sanitized
        cam_size = 0
        cam_concentration = 0.0
        try:
            att = report.get("attention", {}) if isinstance(report.get("attention", {}), dict) else {}
            cam_size = int(att.get("memory_item_count", 0) or 0)
            cam_items = list(att.get("top_items", []) or [])
            cam_energies = []
            for it in cam_items:
                if not isinstance(it, dict):
                    continue
                # Prefer extracted memory energy if available; fall back to current er/ev.
                # sanitized
                er = float(it.get("memory_er", it.get("er", 0.0)) or 0.0)
                ev = float(it.get("memory_ev", it.get("ev", 0.0)) or 0.0)
                cam_energies.append(max(0.0, er) + max(0.0, ev))
            s = float(sum(cam_energies))
            if s > 1e-12:
                cam_concentration = float(round(sum((e / s) ** 2 for e in cam_energies if e > 1e-12), 8))
            else:
                cam_concentration = 0.0
        except Exception:
            cam_size = 0
            cam_concentration = 0.0

        # sanitized
        # sanitized
        # sanitized
        # sanitized
        # sanitized
        # sanitized
        #
        # sanitized
        # sanitized
        # sanitized
        # sanitized
        try:
            size_min = 6.0
            size_max = 24.0
            if size_max <= size_min:
                size_max = size_min + 1.0
            size_norm = (float(cam_size) - size_min) / (size_max - size_min)
            size_norm = max(0.0, min(1.0, float(size_norm)))

            peak_min = 1.0
            peak_max = 12.0
            if peak_max <= peak_min:
                peak_max = peak_min + 1.0
            peak_norm = (float(effective_peak_count) - peak_min) / (peak_max - peak_min)
            peak_norm = max(0.0, min(1.0, float(peak_norm)))

            # sanitized
            complexity_score = 0.55 * size_norm + 0.45 * peak_norm
            complexity_score = max(0.0, min(1.0, float(complexity_score)))
            complexity_score = float(round(complexity_score, 8))
        except Exception:
            complexity_score = 0.0

        # ---- Memory Activation Pool (MAP) ----
        # sanitized
        map_item_count = 0
        map_total_ev = 0.0
        try:
            snap = (report.get("memory_activation", {}) or {}).get("snapshot", {}) or {}
            items = list(snap.get("items", []) or [])
            map_item_count = len([x for x in items if isinstance(x, dict)])
            map_total_ev = float(((snap.get("summary", {}) or {}).get("total_ev", 0.0) or 0.0))
        except Exception:
            map_item_count = 0
            map_total_ev = 0.0

        # ---- stimulus metrics ----
        # Residual ratio: (after stimulus retrieval) / (before stimulus retrieval).
        # sanitized
        residual_ratio = 0.0
        try:
            before = report.get("cache_neutralization", {}).get("residual_packet", {}) or {}
            after = report.get("pool_apply", {}).get("landed_packet", {}) or {}
            before_total = float(before.get("total_er", 0.0) or 0.0) + float(before.get("total_ev", 0.0) or 0.0)
            after_total = float(after.get("total_er", 0.0) or 0.0) + float(after.get("total_ev", 0.0) or 0.0)
            residual_ratio = float(after_total / before_total) if before_total > 1e-12 else 0.0
        except Exception:
            residual_ratio = 0.0

        best_match_score = 0.0
        match_scores: dict[str, float] = {}
        best_match_target_id = ""
        best_match_target_display = ""
        match_displays: dict[str, str] = {}
        try:
            rounds = list(
                (report.get("stimulus_level", {}) or {})
                .get("result", {})
                .get("debug", {})
                .get("round_details", [])
                or []
            )
            for rd in rounds:
                if not isinstance(rd, dict):
                    continue
                sm = rd.get("selected_match") or {}
                if not isinstance(sm, dict):
                    continue
                score = float(sm.get("match_score", 0.0) or 0.0)
                best_match_score = max(best_match_score, score)

                # Per-target match score map (best-effort).
                # sanitized
                sid = str(
                    sm.get("structure_id", "")
                    or sm.get("structure_db_id", "")
                    or sm.get("structure_signature", "")
                    or ""
                ).strip()
                if sid:
                    match_scores[sid] = max(float(match_scores.get(sid, 0.0) or 0.0), float(score))
            best_match_score = round(float(best_match_score), 8)
            if match_scores:
                best_match_target_id = max(match_scores.items(), key=lambda kv: float(kv[1] or 0.0))[0]

            # Best-effort: resolve display text for retrieval targets (st_*).
            # sanitized
            try:
                # Helper: structure_id -> display_text
                def _st_display(sid: str) -> str:
                    if not sid or not str(sid).startswith("st_"):
                        return ""
                    st_obj = self.hdb._structure_store.get(str(sid))  # type: ignore[attr-defined]
                    if not isinstance(st_obj, dict):
                        return ""
                    block = st_obj.get("structure", {}) if isinstance(st_obj.get("structure", {}), dict) else {}
                    return str(block.get("display_text", "") or sid)

                if best_match_target_id:
                    best_match_target_display = _st_display(best_match_target_id) or str(best_match_target_id)
                for sid in list(match_scores.keys()):
                    disp = _st_display(sid)
                    if disp:
                        match_displays[str(sid)] = disp
            except Exception:
                best_match_target_display = best_match_target_display or ""
                match_displays = match_displays or {}
        except Exception:
            best_match_score = 0.0
            match_scores = {}
            best_match_target_id = ""
            best_match_target_display = ""
            match_displays = {}

        # ---- structure-level retrieval metrics ----
        # sanitized
        #
        # sanitized
        # sanitized
        # sanitized
        # sanitized
        structure_best_match_score = 0.0
        structure_match_scores: dict[str, float] = {}
        structure_best_match_target_id = ""
        structure_best_match_target_display = ""
        structure_match_displays: dict[str, str] = {}
        try:
            rounds = list(
                (report.get("structure_level", {}) or {})
                .get("result", {})
                .get("debug", {})
                .get("round_details", [])
                or []
            )
            for rd in rounds:
                if not isinstance(rd, dict):
                    continue
                # Current HDB structure-level debug uses "selected_group" as the main selected record.
                # sanitized
                sel = rd.get("selected_group") or rd.get("selected_match") or {}
                if not isinstance(sel, dict):
                    continue
                try:
                    score = float(
                        sel.get("score", sel.get("competition_score", sel.get("match_score", 0.0)))
                        or 0.0
                    )
                except Exception:
                    score = 0.0
                structure_best_match_score = max(structure_best_match_score, score)

                gid = str(sel.get("group_id", "") or sel.get("id", "") or "").strip()
                if gid:
                    structure_match_scores[gid] = max(float(structure_match_scores.get(gid, 0.0) or 0.0), float(score))
            structure_best_match_score = round(float(structure_best_match_score), 8)
            if structure_match_scores:
                structure_best_match_target_id = max(structure_match_scores.items(), key=lambda kv: float(kv[1] or 0.0))[0]

            # Best-effort display for group ids (sg_*). GroupStore has no direct display_text,
            # so we keep a readable fallback: "sg_xxx" (future: derive from required structures).
            # sanitized
            if structure_best_match_target_id:
                structure_best_match_target_display = str(structure_best_match_target_id)
            for gid in list(structure_match_scores.keys()):
                if str(gid):
                    structure_match_displays[str(gid)] = str(gid)
        except Exception:
            structure_best_match_score = 0.0
            structure_match_scores = {}
            structure_best_match_target_id = ""
            structure_best_match_target_display = ""
            structure_match_displays = {}

        # sanitized
        # sanitized
        # sanitized
        # sanitized
        # sanitized
        # sanitized
        #
        # sanitized
        # sanitized
        # sanitized
        # sanitized
        # sanitized
        # sanitized
        # sanitized
        try:
            # sanitized
            # sanitized
            m_lo = 0.40
            m_hi = 0.95
            if m_hi <= m_lo:
                m_hi = m_lo + 1e-6
            match_norm = (float(best_match_score) - float(m_lo)) / (float(m_hi) - float(m_lo))
            match_norm = max(0.0, min(1.0, float(match_norm)))

            rr = float(residual_ratio)
            rr = max(0.0, min(1.0, rr))
            residual_complement = 1.0 - rr

            # sanitized
            # sanitized
            #
            # sanitized
            has_structure = bool(structure_match_scores) or float(structure_best_match_score) > 1e-9
            if has_structure:
                s_lo = 0.20
                s_hi = 0.90
                if s_hi <= s_lo:
                    s_hi = s_lo + 1e-6
                structure_norm = (float(structure_best_match_score) - float(s_lo)) / (float(s_hi) - float(s_lo))
                structure_norm = max(0.0, min(1.0, float(structure_norm)))
            else:
                structure_norm = 0.0

            # sanitized
            # sanitized
            # sanitized
            # sanitized
            best_row: dict[str, Any] | None = None
            if best_match_target_id:
                for row in pool_items:
                    if not isinstance(row, dict):
                        continue
                    rid = str(row.get("ref_object_id", "") or "").strip()
                    if rid and rid == str(best_match_target_id):
                        best_row = row
                        break
                    aliases = row.get("ref_alias_ids", [])
                    if isinstance(aliases, list) and str(best_match_target_id) in {str(x) for x in aliases if str(x)}:
                        best_row = row
                        break

            ev_stability = 0.0
            ev_coverage = 0.0
            cp_relief = 0.0
            if best_row:
                # sanitized
                best_ev = float(best_row.get("ev", 0.0) or 0.0)
                best_ev_rate = float(best_row.get("ev_change_rate", best_row.get("delta_ev", 0.0)) or 0.0)
                rel = abs(best_ev_rate) / max(1e-6, abs(best_ev))
                k_rel = 0.35  # 闂佸憡顨呴敃銊╁灳濠婂懍鐒婇柛婵嗗椤斿﹪鏌ㄥ☉娆樺姇el=k 闂佸搫鍟晶搴ゅ綂闁诲氦顫夌喊宥夊焵椤戭兘鍋撳?.5闂佹寧绋戦悧鍡氥亹閺屻儱瑙﹂幖杈剧悼閺侀箖鏌熺粙娆炬█闁绘稒鐟╁銊╂嚋閸偅顔嶉梺纭咁嚃閸犳盯鎯冮悢鐓庣煑闁稿矉濡囩粈?
                ev_stability = 1.0 / (1.0 + (rel / max(1e-9, k_rel)))
                ev_stability = max(0.0, min(1.0, float(ev_stability)))

                # sanitized
                ev_sum = max(1e-9, float(total_ev))
                ev_coverage = float(best_ev) / float(ev_sum)
                ev_coverage = max(0.0, min(1.0, float(ev_coverage)))

                # sanitized
                cp_abs_rate = float(best_row.get("cp_abs_rate", best_row.get("delta_cp_abs", 0.0)) or 0.0)
                relief = max(0.0, -cp_abs_rate)
                # Softcap to (0,1): relief/(relief+k)
                k_relief = 0.30
                cp_relief = float(relief / (relief + max(1e-9, k_relief))) if relief > 0.0 else 0.0
                cp_relief = max(0.0, min(1.0, float(cp_relief)))

            # sanitized
            # sanitized
            # sanitized
            # sanitized
            if has_structure:
                grasp_score = (
                    0.30 * match_norm
                    + 0.25 * residual_complement
                    + 0.15 * structure_norm
                    + 0.15 * ev_stability
                    + 0.10 * ev_coverage
                    + 0.05 * cp_relief
                )
            else:
                grasp_score = (
                    0.35 * match_norm
                    + 0.30 * residual_complement
                    + 0.15 * ev_stability
                    + 0.10 * ev_coverage
                    + 0.10 * cp_relief
                )

            grasp_score = max(0.0, min(1.0, float(grasp_score)))
            grasp_score = float(round(grasp_score, 8))
        except Exception:
            grasp_score = 0.0

        # Also store these metrics into report for observability (best-effort).
        # sanitized
        try:
            stim_res = (report.get("stimulus_level", {}) or {}).get("result", {})
            if isinstance(stim_res, dict):
                metrics = stim_res.setdefault("metrics", {})
                if isinstance(metrics, dict):
                    metrics["residual_ratio"] = round(float(residual_ratio), 8)
                    metrics["best_match_score"] = round(float(best_match_score), 8)
                    metrics["grasp_score"] = round(float(grasp_score), 8)
                    metrics["match_score_target_count"] = len(match_scores)
                    metrics["best_match_target_id"] = str(best_match_target_id or "")
        except Exception:
            pass

        # Also store structure-level match metrics for observability (best-effort).
        # sanitized
        try:
            st_res = (report.get("structure_level", {}) or {}).get("result", {})
            if isinstance(st_res, dict):
                metrics = st_res.setdefault("metrics", {})
                if isinstance(metrics, dict):
                    metrics["best_match_score"] = round(float(structure_best_match_score), 8)
                    metrics["match_score_target_count"] = len(structure_match_scores)
                    metrics["best_match_target_id"] = str(structure_best_match_target_id or "")
        except Exception:
            pass

        # ---- emotion ----
        nt_state: dict[str, float] = {}
        if isinstance(emotion_state, dict) and isinstance(emotion_state.get("nt_state_after"), dict):
            nt_state = {str(k): float(v) for k, v in (emotion_state.get("nt_state_after") or {}).items() if str(k)}
        else:
            # Snapshot from EMgr (previous tick state).
            try:
                snap = self.emotion.get_emotion_snapshot(trace_id=f"{trace_id}_emotion_snapshot_for_rules").get("data", {}) or {}
                channels = (snap.get("nt_state_snapshot", {}) or {}).get("channels", {}) or {}
                if isinstance(channels, dict):
                    for ch, row in channels.items():
                        if not str(ch):
                            continue
                        if isinstance(row, dict) and "value" in row:
                            nt_state[str(ch)] = float(row.get("value", 0.0) or 0.0)
            except Exception:
                nt_state = {}

        # Expand NT aliases for readability (Chinese-first).
        # sanitized
        # sanitized
        # sanitized
        try:
            snap = self.emotion.get_emotion_snapshot(trace_id=f"{trace_id}_emotion_labels_for_rules").get("data", {}) or {}
            labels = snap.get("nt_channel_labels", {}) if isinstance(snap.get("nt_channel_labels", {}), dict) else {}
            if labels:
                for ch, v in list(nt_state.items()):
                    lab = str(labels.get(ch, "") or "").strip()
                    if not lab:
                        continue
                    # sanitized
                    if lab not in nt_state:
                        nt_state[lab] = float(v)
                    # sanitized
                    short = lab.split("(", 1)[0].strip()
                    if short and short not in nt_state:
                        nt_state[short] = float(v)
        except Exception:
            pass

        # sanitized
        # ----------------------------------------------------
        # sanitized
        # sanitized
        # sanitized
        # sanitized
        #
        # sanitized
        # sanitized
        rwd_pun_pool = self._estimate_rwd_pun_from_pool_items(pool_items, trace_id=trace_id, tick_id=tick_id)
        rwd = float(rwd_pun_pool.get("rwd", 0.0) or 0.0)
        pun = float(rwd_pun_pool.get("pun", 0.0) or 0.0)

        return {
            "pool": {
                "total_er": total_er,
                "total_ev": total_ev,
                "total_energy": round(float(total_er) + float(total_ev), 8),
                "item_count": len(pool_items),
                "total_cp_delta": total_cp_delta,
                "total_cp_abs": total_cp_abs,
                "energy_concentration": float(energy_concentration),
                "effective_peak_count": float(effective_peak_count),
                "complexity_score": float(complexity_score),
            },
            "pool_items": pool_items,
            "cam": {"size": int(cam_size), "energy_concentration": float(cam_concentration)},
            "memory_activation": {"item_count": int(map_item_count), "total_ev": float(map_total_ev)},
            "emotion": {
                "nt": nt_state,
                "rwd": float(rwd),
                "pun": float(pun),
                "rwd_pun_source": str(rwd_pun_pool.get("source", "") or "pool_items"),
                "rwd_pun_detail": dict(rwd_pun_pool.get("detail", {}) or {}),
            },
            "stimulus": {"residual_ratio": round(float(residual_ratio), 8)},
            "retrieval": {
                "stimulus": {
                    "best_match_score": round(float(best_match_score), 8),
                    "grasp_score": round(float(grasp_score), 8),
                    "best_match_target_id": str(best_match_target_id or ""),
                    "best_match_target_display": str(best_match_target_display or ""),
                    # match_scores: {target_id -> score}. Target id is typically structure_id/st_*.
                    # sanitized
                    "match_scores": dict(match_scores),
                    "match_displays": dict(match_displays),
                }
                ,
                "structure": {
                    "best_match_score": round(float(structure_best_match_score), 8),
                    "best_match_target_id": str(structure_best_match_target_id or ""),
                    "best_match_target_display": str(structure_best_match_target_display or ""),
                    # match_scores: {group_id -> score}. Target id is typically sg_*.
                    # sanitized
                    "match_scores": dict(structure_match_scores),
                    "match_displays": dict(structure_match_displays),
                },
            },
            "meta": {"trace_id": trace_id, "tick_id": tick_id, "built_at_ms": int(time.time() * 1000)},
        }

    # ================================================================== #
    # Reward/Punish Aggregation                                            #
    # sanitized
    # ================================================================== #

    @staticmethod
    def _clamp01(x: float) -> float:
        try:
            v = float(x)
        except Exception:
            v = 0.0
        return max(0.0, min(1.0, v))

    @staticmethod
    def _softcap(x: float, *, k: float) -> float:
        """
        Soft-saturating mapping x -> x/(x+k).
        # sanitized
        """
        try:
            v = float(x)
        except Exception:
            v = 0.0
        k = max(1e-9, float(k))
        if v <= 0.0:
            return 0.0
        return float(v / (v + k))

    @staticmethod
    def _row_has_bound_attribute(row: dict[str, Any], attr_name: str) -> bool:
        """doc"""
        if not isinstance(row, dict) or not attr_name:
            return False
        names = row.get("bound_attribute_names", [])
        if isinstance(names, list) and attr_name in {str(x) for x in names if str(x)}:
            return True
        hay = " ".join(str(x) for x in (row.get("bound_attribute_displays", []) or []) if str(x))
        return attr_name in hay

    # ================================================================== #
    # Teacher Feedback (External Reward/Punish)                           #
    # sanitized
    # ================================================================== #

    def _apply_teacher_feedback(
        self,
        *,
        labels: dict[str, Any] | None,
        report: dict[str, Any] | None,
        trace_id: str,
        tick_id: str,
    ) -> dict[str, Any]:
        """
        Apply external teacher feedback to the runtime StatePool.

        Goals / 闂佺儵鏅╅崰妤呮偉閿濆鏅?
        # sanitized
        # sanitized
        # sanitized

        # sanitized
        - teacher_rwd / teacher_pun: float in [0,1]
        - teacher_anchor: cam_top1 | pool_top1_total | pool_top1_total_any | specific_item | specific_ref | none
        - teacher_anchor_item_id / teacher_anchor_ref_object_id / teacher_anchor_ref_object_type
        - teacher_anchor_ref_object_types: ['st', 'sa', ...] (default ['st'])
        - tool_feedback_rwd / tool_feedback_pun: aliases for teacher_rwd/pun (for tool experiments)
        """
        labels = labels if isinstance(labels, dict) else {}
        report = report if isinstance(report, dict) else {}

        # Allow a nested "teacher" dict, but keep top-level keys as the stable protocol.
        teacher = labels.get("teacher") if isinstance(labels.get("teacher"), dict) else {}

        def _pick(keys: list[str], *, default: Any = None) -> Any:
            for k in keys:
                if k in teacher:
                    return teacher.get(k)
                if k in labels:
                    return labels.get(k)
            return default

        def _as_float(v: Any) -> float:
            try:
                return float(v)
            except Exception:
                return 0.0

        teacher_rwd = self._clamp01(_as_float(_pick(["teacher_rwd", "tool_feedback_rwd", "rwd"], default=0.0)))
        teacher_pun = self._clamp01(_as_float(_pick(["teacher_pun", "tool_feedback_pun", "pun"], default=0.0)))
        mode = str(_pick(["teacher_mode", "mode"], default="bind_attribute") or "bind_attribute").strip() or "bind_attribute"

        anchor = str(_pick(["teacher_anchor", "anchor"], default="pool_top1_total") or "pool_top1_total").strip() or "pool_top1_total"
        if anchor == "pool_top1":
            anchor = "pool_top1_total"
        if anchor == "pool_top1_any":
            anchor = "pool_top1_total_any"

        allow_types = _pick(["teacher_anchor_ref_object_types", "ref_object_types"], default=None)
        ref_object_types: list[str] = []
        if isinstance(allow_types, list):
            ref_object_types = [str(x) for x in allow_types if str(x).strip()]
        if not ref_object_types:
            ref_object_types = ["st"]

        explicit_item_id = str(_pick(["teacher_anchor_item_id", "item_id"], default="") or "").strip()
        explicit_ref_id = str(_pick(["teacher_anchor_ref_object_id", "ref_object_id"], default="") or "").strip()
        explicit_ref_type = str(_pick(["teacher_anchor_ref_object_type", "ref_object_type"], default="") or "").strip()
        contains_text = str(_pick(["teacher_anchor_contains_text", "contains_text"], default="") or "").strip()
        note = str(_pick(["teacher_note", "note", "teacher_reason", "reason"], default="") or "").strip()

        # Early exit: no feedback provided.
        if teacher_rwd <= 0.0 and teacher_pun <= 0.0:
            return {
                "ok": True,
                "mode": mode,
                "anchor": anchor,
                "teacher_rwd": 0.0,
                "teacher_pun": 0.0,
                "applied_count": 0,
                "applied": [],
                "message": "no teacher feedback on this tick",
            }

        if anchor in {"none", "off", "disabled"}:
            return {
                "ok": True,
                "mode": mode,
                "anchor": anchor,
                "teacher_rwd": round(float(teacher_rwd), 8),
                "teacher_pun": round(float(teacher_pun), 8),
                "applied_count": 0,
                "applied": [],
                "message": "teacher feedback ignored by anchor policy",
            }

        # ---- Resolve target ----
        target_item_id = ""
        target_row: dict[str, Any] = {}
        resolve_reason = ""

        # (1) Specific item id
        if explicit_item_id:
            it = self.pool._store.get(explicit_item_id)  # type: ignore[attr-defined]
            if isinstance(it, dict):
                target_item_id = explicit_item_id
                try:
                    target_row = self.pool._snapshot._build_top_item_summary(it)  # type: ignore[attr-defined]
                except Exception:
                    target_row = {}
                resolve_reason = "specific_item"

        # (2) Specific ref id
        if not target_item_id and explicit_ref_id:
            it = self.pool._store.get_by_ref(explicit_ref_id)  # type: ignore[attr-defined]
            if isinstance(it, dict):
                if explicit_ref_type and str(it.get("ref_object_type", "")) != explicit_ref_type:
                    pass
                else:
                    target_item_id = str(it.get("id", "") or "")
                    try:
                        target_row = self.pool._snapshot._build_top_item_summary(it)  # type: ignore[attr-defined]
                    except Exception:
                        target_row = {}
                    resolve_reason = "specific_ref"

        # (3) CAM top1
        if not target_item_id and anchor == "cam_top1":
            att = report.get("attention", {}) if isinstance(report.get("attention", {}), dict) else {}
            top_items = list(att.get("top_items", []) or [])
            for r in top_items:
                if not isinstance(r, dict):
                    continue
                if ref_object_types and str(r.get("ref_object_type", "")) not in set(ref_object_types):
                    continue
                iid = str(r.get("item_id", "") or "").strip()
                if iid:
                    target_item_id = iid
                    target_row = dict(r)
                    resolve_reason = "cam_top1"
                    break

        # (4) Contains text
        if not target_item_id and (contains_text or anchor.startswith("contains_text")):
            needle = contains_text
            if not needle and ":" in anchor:
                needle = anchor.split(":", 1)[1].strip()
            if needle:
                # Use a cheap scan on the live pool store.
                try:
                    all_items = list(self.pool._store.get_all())  # type: ignore[attr-defined]
                except Exception:
                    all_items = []
                for it in all_items:
                    if not isinstance(it, dict):
                        continue
                    try:
                        row = self.pool._snapshot._build_top_item_summary(it)  # type: ignore[attr-defined]
                    except Exception:
                        continue
                    if not isinstance(row, dict):
                        continue
                    if ref_object_types and str(row.get("ref_object_type", "")) not in set(ref_object_types):
                        continue
                    hay = " ".join(
                        [
                            str(row.get("display", "") or ""),
                            str(row.get("display_detail", "") or ""),
                            " ".join(str(x) for x in (row.get("attribute_displays", []) or []) if str(x)),
                            " ".join(str(x) for x in (row.get("feature_displays", []) or []) if str(x)),
                            " ".join(str(x) for x in (row.get("bound_attribute_displays", []) or []) if str(x)),
                        ]
                    )
                    if needle in hay or needle.lower() in hay.lower():
                        target_item_id = str(row.get("item_id", "") or "")
                        target_row = dict(row)
                        resolve_reason = f"contains_text:{needle}"
                        break

        # (5) Default: pool top1 by total_energy
        if not target_item_id and anchor in {"pool_top1_total", "pool_top1_total_any"}:
            prefer_any = anchor == "pool_top1_total_any"
            try:
                all_items = list(self.pool._store.get_all())  # type: ignore[attr-defined]
            except Exception:
                all_items = []
            best_it: dict[str, Any] | None = None
            best_total = -1.0
            allow = set(ref_object_types)
            for it in all_items:
                if not isinstance(it, dict):
                    continue
                if (not prefer_any) and allow and str(it.get("ref_object_type", "")) not in allow:
                    continue
                e = it.get("energy", {}) if isinstance(it.get("energy", {}), dict) else {}
                try:
                    total = float(e.get("er", 0.0) or 0.0) + float(e.get("ev", 0.0) or 0.0)
                except Exception:
                    total = 0.0
                if total > best_total:
                    best_total = total
                    best_it = it
            if best_it is not None:
                target_item_id = str(best_it.get("id", "") or "")
                try:
                    target_row = self.pool._snapshot._build_top_item_summary(best_it)  # type: ignore[attr-defined]
                except Exception:
                    target_row = {}
                resolve_reason = f"{anchor}:top_by_total_energy"

        if not target_item_id:
            return {
                "ok": False,
                "mode": mode,
                "anchor": anchor,
                "teacher_rwd": round(float(teacher_rwd), 8),
                "teacher_pun": round(float(teacher_pun), 8),
                "ref_object_types": list(ref_object_types),
                "applied_count": 0,
                "applied": [],
                "message": "teacher feedback provided but no anchor target found",
                "resolve_reason": resolve_reason or "no_target",
            }

        # ---- Apply bindings ----
        applied: list[dict[str, Any]] = []

        def bind_attr(*, attr_name: str, attr_value: float, display: str) -> None:
            target_ref_id = str(target_row.get("ref_object_id", "") or target_item_id)
            attr_id = f"sa_teacher_attr_{attr_name}_{target_ref_id}"
            attribute_sa = {
                "id": attr_id,
                "object_type": "sa",
                "content": {
                    "raw": f"{attr_name}:{round(float(attr_value), 8)}",
                    "display": display,
                    "value_type": "numerical",
                    "attribute_name": attr_name,
                    "attribute_value": round(float(attr_value), 8),
                },
                "stimulus": {"role": "attribute", "modality": "external"},
                "energy": {"er": 0.0, "ev": 0.0},
                "meta": {
                    "ext": {
                        "bound_from": "teacher_feedback",
                        "trace_id": trace_id,
                        "tick_id": tick_id,
                        "mode": mode,
                        "anchor": anchor,
                        "resolve_reason": resolve_reason,
                        "note": note,
                    }
                },
            }
            res = self.pool.bind_attribute_node_to_object(
                target_item_id=target_item_id,
                attribute_sa=attribute_sa,
                trace_id=f"{trace_id}_teacher_bind_attr",
                tick_id=tick_id,
                source_module="teacher_feedback",
                reason=f"teacher_feedback:{attr_name}",
            )
            applied.append(
                {
                    "attribute_name": attr_name,
                    "attribute_sa_id": attr_id,
                    "target_item_id": target_item_id,
                    "success": bool(res.get("success", False)),
                    "code": str(res.get("code", "") or ""),
                    "data": res.get("data", {}) if isinstance(res.get("data", {}), dict) else {},
                }
            )

        if teacher_rwd > 0.0:
            bind_attr(attr_name="teacher_reward_signal", attr_value=teacher_rwd, display="teacher_reward_signal")
        if teacher_pun > 0.0:
            bind_attr(attr_name="teacher_punish_signal", attr_value=teacher_pun, display="teacher_punish_signal")

        return {
            "ok": True,
            "mode": mode,
            "anchor": anchor,
            "teacher_rwd": round(float(teacher_rwd), 8),
            "teacher_pun": round(float(teacher_pun), 8),
            "ref_object_types": list(ref_object_types),
            "resolve_reason": resolve_reason,
            "target": {
                "item_id": target_item_id,
                "ref_object_id": str(target_row.get("ref_object_id", "") or ""),
                "ref_object_type": str(target_row.get("ref_object_type", "") or ""),
                "display": str(target_row.get("display", "") or ""),
            },
            "applied_count": len(applied),
            "applied": applied[:8],
        }

    def _estimate_rwd_pun_from_pool_items(self, pool_items: list[dict[str, Any]], *, trace_id: str, tick_id: str) -> dict[str, Any]:
        """
        Estimate global reward/punish signals from pool_items.
        # sanitized

        # sanitized
        # sanitized
        # sanitized
        # sanitized

        # sanitized
        # sanitized
        # sanitized
        """
        cfg = getattr(self.emotion, "_config", {}) or {}
        agg = cfg.get("rwd_pun_pool_aggregation", {}) if isinstance(cfg.get("rwd_pun_pool_aggregation", {}), dict) else {}

        reward_attr = str(agg.get("reward_attr_name", "reward_signal") or "reward_signal").strip() or "reward_signal"
        punish_attr = str(agg.get("punish_attr_name", "punish_signal") or "punish_signal").strip() or "punish_signal"
        ev_min = float(agg.get("ev_min", 0.0) or 0.0)

        rwd_pred_ev = 0.0
        pun_pred_ev = 0.0
        rwd_got_er = 0.0
        pun_got_er = 0.0

        for row in pool_items or []:
            if not isinstance(row, dict):
                continue
            ev = float(row.get("ev", 0.0) or 0.0)
            der = float(row.get("delta_er", 0.0) or 0.0)
            if self._row_has_bound_attribute(row, reward_attr):
                if ev >= ev_min:
                    rwd_pred_ev += max(0.0, ev)
                if der > 0.0:
                    rwd_got_er += der
            if self._row_has_bound_attribute(row, punish_attr):
                if ev >= ev_min:
                    pun_pred_ev += max(0.0, ev)
                if der > 0.0:
                    pun_got_er += der

        k_pred = float(agg.get("k_pred", 1.0) or 1.0)
        k_got = float(agg.get("k_got", 0.5) or 0.5)
        w_pred = float(agg.get("w_pred", 0.7) or 0.7)
        w_got = float(agg.get("w_got", 0.3) or 0.3)
        w_sum = max(1e-9, abs(w_pred) + abs(w_got))
        w_pred = w_pred / w_sum
        w_got = w_got / w_sum

        rwd = self._clamp01(w_pred * self._softcap(rwd_pred_ev, k=k_pred) + w_got * self._softcap(rwd_got_er, k=k_got))
        pun = self._clamp01(w_pred * self._softcap(pun_pred_ev, k=k_pred) + w_got * self._softcap(pun_got_er, k=k_got))

        return {
            "rwd": round(float(rwd), 8),
            "pun": round(float(pun), 8),
            "source": "pool_items",
            "detail": {
                "reward_attr_name": reward_attr,
                "punish_attr_name": punish_attr,
                "ev_min": ev_min,
                "rwd_pred_ev_sum": round(float(rwd_pred_ev), 8),
                "pun_pred_ev_sum": round(float(pun_pred_ev), 8),
                "rwd_got_er_sum": round(float(rwd_got_er), 8),
                "pun_got_er_sum": round(float(pun_got_er), 8),
                "k_pred": k_pred,
                "k_got": k_got,
                "w_pred": round(float(w_pred), 6),
                "w_got": round(float(w_got), 6),
            },
        }

    # ================================================================== #
    # Episodic Memory Enrichment                                          #
    # sanitized
    # ================================================================== #

    def _enrich_tick_episodic_memory_with_bound_attributes(self, *, report: dict[str, Any], trace_id: str, tick_id: str) -> dict[str, Any]:
        """
        Enrich current tick episodic memory material with runtime bound attributes.
        # sanitized
        """
        try:
            stim_res = (report.get("stimulus_level", {}) or {}).get("result", {}) or {}
            memory_id = str(stim_res.get("episodic_memory_id", "") or "").strip()
        except Exception:
            memory_id = ""
        if not memory_id:
            return {"ok": False, "code": "NO_MEMORY_ID", "message": "No episodic_memory_id in this tick stimulus_level result."}

        try:
            episodic_obj = self.hdb._episodic_store.get(memory_id)  # type: ignore[attr-defined]
        except Exception:
            episodic_obj = None
        if not isinstance(episodic_obj, dict):
            return {"ok": False, "code": "MEMORY_NOT_FOUND", "message": f"未找到情景记忆 / episodic memory not found: {memory_id}"}

        meta = episodic_obj.get("meta", {}) if isinstance(episodic_obj.get("meta", {}), dict) else {}
        ext = meta.get("ext", {}) if isinstance(meta.get("ext", {}), dict) else {}
        mm = ext.get("memory_material", {}) if isinstance(ext.get("memory_material", {}), dict) else {}
        if str(mm.get("memory_kind", "")) != "stimulus_packet":
            return {"ok": False, "code": "SKIP_KIND", "message": "跳过当前记忆类型 / skip memory_kind=" + str(mm.get("memory_kind"))}
        enrich_meta = mm.get("runtime_enrichment", {}) if isinstance(mm.get("runtime_enrichment", {}), dict) else {}
        if enrich_meta.get("bound_attributes_included") is True:
            return {"ok": True, "code": "OK_ALREADY", "message": "runtime_enrichment already contains bound attributes", "data": enrich_meta}

        seq_groups = list(mm.get("sequence_groups", []) or [])
        if not seq_groups:
            return {"ok": False, "code": "EMPTY_MATERIAL", "message": "memory_material.sequence_groups is empty"}

        include_exact = {"reward_signal", "punish_signal", "teacher_reward_signal", "teacher_punish_signal"}
        max_attrs_per_anchor = 6
        added_unit_count = 0
        added_bundle_count = 0
        anchor_hit_count = 0

        for group in seq_groups:
            if not isinstance(group, dict):
                continue
            units = list(group.get("units", []) or [])
            if not units:
                continue
            existing_unit_ids = {str(u.get("unit_id", "")) for u in units if isinstance(u, dict) and str(u.get("unit_id", ""))}
            try:
                next_si = max(int(u.get("sequence_index", 0) or 0) for u in units if isinstance(u, dict)) + 1
            except Exception:
                next_si = 0

            bundles = group.get("csa_bundles", [])
            if not isinstance(bundles, list):
                bundles = []
            existing_bundle_keys = {str(b.get("bundle_id", "") or "") for b in bundles if isinstance(b, dict) and str(b.get("bundle_id", ""))}

            for u in list(units):
                if not isinstance(u, dict):
                    continue
                role = str(u.get("unit_role", u.get("role", "feature")) or "feature").strip() or "feature"
                if role == "attribute":
                    continue
                anchor_unit_id = str(u.get("unit_id", "") or "").strip()
                if not anchor_unit_id:
                    continue
                try:
                    st_item = self.pool._store.get_by_ref(anchor_unit_id)  # type: ignore[attr-defined]
                except Exception:
                    st_item = None
                if not isinstance(st_item, dict):
                    continue
                bound_attrs = (st_item.get("ext", {}) or {}).get("bound_attributes", [])
                if not isinstance(bound_attrs, list) or not bound_attrs:
                    continue

                selected_attrs: list[dict] = []
                for attr in bound_attrs:
                    if not isinstance(attr, dict):
                        continue
                    content = attr.get("content", {}) if isinstance(attr.get("content", {}), dict) else {}
                    attr_name = str(content.get("attribute_name", "") or "").strip()
                    raw = str(content.get("raw", "") or "")
                    if not attr_name:
                        if ":" in raw:
                            attr_name = raw.split(":", 1)[0].strip()
                        else:
                            attr_name = raw.strip()
                    if not attr_name:
                        continue
                    if attr_name in include_exact or attr_name.startswith("cfs_"):
                        selected_attrs.append(attr)

                if not selected_attrs:
                    continue

                anchor_hit_count += 1
                member_unit_ids = [anchor_unit_id]
                for attr in selected_attrs[:max_attrs_per_anchor]:
                    attr_unit_id = str(attr.get("id", "") or "").strip()
                    if not attr_unit_id or attr_unit_id in existing_unit_ids:
                        continue
                    content = attr.get("content", {}) if isinstance(attr.get("content", {}), dict) else {}
                    attr_name = str(content.get("attribute_name", "") or "").strip()
                    raw = str(content.get("raw", "") or "")
                    if not attr_name:
                        if ":" in raw:
                            attr_name = raw.split(":", 1)[0].strip()
                        else:
                            attr_name = raw.strip()
                    display = str(content.get("display", "") or raw or attr_unit_id)
                    token = display
                    if attr_name and attr_name not in token:
                        token = f"{token}闂佹寧绋戝鏈紅tr_name闂?"
                    attribute_value = content.get("attribute_value")
                    value_type = str(content.get("value_type", "numerical" if attribute_value is not None else "discrete") or "discrete")
                    units.append(
                        {
                            "object_type": "sa",
                            "unit_id": attr_unit_id,
                            "token": token,
                            "display_text": token,
                            "unit_role": "attribute",
                            "attribute_name": attr_name,
                            "attribute_value": attribute_value,
                            "value_type": value_type,
                            "sequence_index": int(next_si),
                            "group_index": int(group.get("group_index", 0) or 0),
                            "origin_frame_id": memory_id,
                            "source_type": "runtime_enrichment",
                        }
                    )
                    existing_unit_ids.add(attr_unit_id)
                    member_unit_ids.append(attr_unit_id)
                    next_si += 1
                    added_unit_count += 1

                if len(member_unit_ids) >= 2:
                    bundle_id = f"enrich::{anchor_unit_id}"
                    if bundle_id in existing_bundle_keys:
                        continue
                    bundles.append({"bundle_id": bundle_id, "anchor_unit_id": anchor_unit_id, "member_unit_ids": member_unit_ids})
                    existing_bundle_keys.add(bundle_id)
                    added_bundle_count += 1

            group["units"] = units
            group["csa_bundles"] = bundles
            # Keep tokens in sync (best-effort).
            try:
                group["tokens"] = [str(x.get("token", "")) for x in units if isinstance(x, dict) and str(x.get("token", ""))]
            except Exception:
                pass

        mm["sequence_groups"] = seq_groups
        mm["runtime_enrichment"] = {
            "bound_attributes_included": True,
            "memory_id": memory_id,
            "tick_id": tick_id,
            "trace_id": trace_id,
            "added_unit_count": int(added_unit_count),
            "added_bundle_count": int(added_bundle_count),
            "anchor_hit_count": int(anchor_hit_count),
            "include_exact": sorted(list(include_exact)),
            "max_attrs_per_anchor": int(max_attrs_per_anchor),
            "built_at_ms": int(time.time() * 1000),
        }
        ext["memory_material"] = mm
        meta["ext"] = ext
        episodic_obj["meta"] = meta
        try:
            self.hdb._episodic_store.update(episodic_obj)  # type: ignore[attr-defined]
        except Exception as exc:
            return {"ok": False, "code": "UPDATE_FAILED", "message": f"更新情景记忆失败 / episodic_store.update failed: {exc}"}

        return {"ok": True, "code": "OK", "message": "已将绑定属性写回情景记忆 / runtime enrichment written back to episodic memory", "data": dict(mm.get("runtime_enrichment", {}) or {})}

    def _apply_innate_pool_effects(
        self,
        *,
        effects: list[dict[str, Any]],
        context: dict[str, Any],
        trace_id: str,
        tick_id: str,
    ) -> dict[str, Any]:
        """
        Apply IESM pool_effects to StatePool (safe executor).
        # sanitized

        Safety / 闁诲海鎳撻ˇ顖炲矗韫囨洜椹抽柡宥庡亝濞堬綁鏌?
        - 闂佸憡鐟禍婵嬪极椤曗偓楠炴劖鎷呴悜姗嗕槐闂佸憡鑹剧粔鏉戠暦?effect_type闂佹寧绋戝绌檕l_energy / pool_bind_attribute闂?
        # sanitized
        # sanitized
        """
        effects = [e for e in (effects or []) if isinstance(e, dict)]
        pool_items = list(context.get("pool_items", []) or [])
        pool_items = [it for it in pool_items if isinstance(it, dict)]

        def select_items(selector: dict[str, Any] | None) -> list[dict[str, Any]]:
            if not selector or not isinstance(selector, dict):
                return list(pool_items)
            mode = str(selector.get("mode", "all") or "all").strip()
            rows = list(pool_items)

            ref_types = selector.get("ref_object_types")
            if isinstance(ref_types, list):
                allow = {str(x) for x in ref_types if str(x)}
                if allow:
                    rows = [r for r in rows if str(r.get("ref_object_type", "")) in allow]

            if mode in {"all", "any"}:
                return rows
            if mode == "specific_item":
                iid = str(selector.get("item_id", "") or "").strip()
                return [r for r in rows if str(r.get("item_id", "")) == iid] if iid else []
            if mode == "specific_ref":
                rid = str(selector.get("ref_object_id", "") or "").strip()
                rtype = str(selector.get("ref_object_type", "") or "").strip()
                out = [r for r in rows if str(r.get("ref_object_id", "")) == rid] if rid else []
                if rtype:
                    out = [r for r in out if str(r.get("ref_object_type", "")) == rtype]
                return out
            if mode == "contains_text":
                needle = str(selector.get("contains_text", "") or "").strip()
                if not needle:
                    return []
                needle_low = needle.lower()
                out: list[dict[str, Any]] = []
                for r in rows:
                    hay = " ".join(
                        [
                            str(r.get("display", "") or ""),
                            str(r.get("display_detail", "") or ""),
                            " ".join(str(x) for x in (r.get("attribute_displays", []) or []) if str(x)),
                            " ".join(str(x) for x in (r.get("feature_displays", []) or []) if str(x)),
                            " ".join(str(x) for x in (r.get("bound_attribute_displays", []) or []) if str(x)),
                        ]
                    )
                    if needle in hay or needle_low in hay.lower():
                        out.append(r)
                return out
            if mode == "top_n":
                try:
                    n = int(selector.get("top_n", 8) or 8)
                except Exception:
                    n = 8
                n = max(1, min(512, n))
                rows.sort(key=lambda r: float(r.get("total_energy", 0.0) or 0.0), reverse=True)
                return rows[:n]
            return rows

        def coerce_float(v: Any, default: float = 0.0) -> float:
            try:
                if v is None or v == "":
                    return float(default)
                return float(v)
            except Exception:
                return float(default)

        applied: list[dict[str, Any]] = []
        skipped: list[dict[str, Any]] = []

        cap = 64  # single tick cap / 闂?tick 婵炴垶鎸搁敃顏勵瀶?
        for eff in effects[:cap]:
            et = str(eff.get("effect_type", "") or "")
            spec = eff.get("spec") if isinstance(eff.get("spec"), dict) else {}
            rule_id = str(eff.get("rule_id", "") or "")
            effect_id = str(eff.get("effect_id", "") or "")

            if et == "pool_energy":
                delta_er = coerce_float(spec.get("delta_er", spec.get("er", 0.0)), 0.0)
                delta_ev = coerce_float(spec.get("delta_ev", spec.get("ev", 0.0)), 0.0)
                if abs(delta_er) < 1e-12 and abs(delta_ev) < 1e-12:
                    skipped.append({"effect_id": effect_id, "effect_type": et, "rule_id": rule_id, "reason": "delta_both_zero"})
                    continue

                # Resolve targets
                targets: list[dict[str, Any]] = []
                selector = spec.get("selector") if isinstance(spec.get("selector"), dict) else None
                if selector:
                    targets = select_items(selector)
                else:
                    item_id = str(spec.get("target_item_id", "") or spec.get("item_id", "") or "").strip()
                    if item_id:
                        targets = [r for r in pool_items if str(r.get("item_id", "")) == item_id]
                    if not targets:
                        ref_id = str(spec.get("ref_object_id", "") or spec.get("target_ref_object_id", "") or "").strip()
                        ref_type = str(spec.get("ref_object_type", "") or spec.get("target_ref_object_type", "") or "").strip()
                        if ref_id:
                            targets = [r for r in pool_items if str(r.get("ref_object_id", "")) == ref_id and (not ref_type or str(r.get("ref_object_type", "")) == ref_type)]

                # Optional create-if-missing (only for specific ref targets)
                if not targets and bool(spec.get("create_if_missing", False)):
                    ref_id = str(spec.get("ref_object_id", "") or spec.get("target_ref_object_id", "") or "").strip()
                    if ref_id and (max(0.0, delta_er) > 0.0 or max(0.0, delta_ev) > 0.0):
                        obj_type = str(spec.get("create_ref_object_type", "") or spec.get("ref_object_type", "") or "sa").strip() or "sa"
                        display = str(spec.get("create_display", "") or spec.get("display", "") or ref_id)
                        runtime_obj = {
                            "id": ref_id,
                            "object_type": obj_type,
                            "content": {"raw": display, "display": display, "value_type": "discrete"},
                            "energy": {"er": round(max(0.0, delta_er), 8), "ev": round(max(0.0, delta_ev), 8)},
                        }
                        insert = self.pool.insert_runtime_node(
                            runtime_object=runtime_obj,
                            trace_id=f"{trace_id}_iesm_pool_energy_create",
                            tick_id=tick_id,
                            allow_merge=True,
                            source_module="innate_script",
                            reason=f"iesm_pool_energy_create:{rule_id or 'rule'}",
                        )
                        applied.append(
                            {
                                "effect_id": effect_id,
                                "effect_type": et,
                                "rule_id": rule_id,
                                "op": "create",
                                "ref_object_id": ref_id,
                                "ref_object_type": obj_type,
                                "success": bool(insert.get("success", False)),
                                "code": insert.get("code", ""),
                                "data": insert.get("data", {}) or {},
                            }
                        )
                        # After create, refresh pool_items list for later effects (best-effort).
                        try:
                            new_item = self.pool._store.get_by_ref(ref_id)
                            if new_item and isinstance(new_item, dict):
                                summary = self.pool._snapshot._build_top_item_summary(new_item)  # type: ignore[attr-defined]
                                if isinstance(summary, dict):
                                    summary["total_energy"] = round(max(0.0, float(summary.get("er", 0.0) or 0.0)) + max(0.0, float(summary.get("ev", 0.0) or 0.0)), 8)
                                    pool_items.append(summary)
                        except Exception:
                            pass
                        continue

                if not targets:
                    skipped.append({"effect_id": effect_id, "effect_type": et, "rule_id": rule_id, "reason": "no_targets"})
                    continue

                # Apply to each target (cap per effect).
                reason = str(spec.get("reason", "") or f"iesm_pool_energy:{rule_id}").strip()
                for t in targets[:24]:
                    tid = str(t.get("item_id", "") or "")
                    if not tid:
                        continue
                    res = self.pool.apply_energy_update(
                        target_item_id=tid,
                        delta_er=float(delta_er),
                        delta_ev=float(delta_ev),
                        trace_id=f"{trace_id}_iesm_pool_energy",
                        tick_id=tick_id,
                        reason=reason,
                        source_module="innate_script",
                    )
                    applied.append(
                        {
                            "effect_id": effect_id,
                            "effect_type": et,
                            "rule_id": rule_id,
                            "op": "update",
                            "target_item_id": tid,
                            "success": bool(res.get("success", False)),
                            "code": res.get("code", ""),
                            "data": res.get("data", {}) or {},
                        }
                    )
                continue

            if et == "pool_bind_attribute":
                selector = spec.get("selector") if isinstance(spec.get("selector"), dict) else None
                targets = select_items(selector) if selector else []

                if not targets:
                    item_id = str(spec.get("target_item_id", "") or spec.get("item_id", "") or "").strip()
                    if item_id:
                        targets = [r for r in pool_items if str(r.get("item_id", "")) == item_id]
                if not targets:
                    ref_id = str(spec.get("ref_object_id", "") or spec.get("target_ref_object_id", "") or "").strip()
                    ref_type = str(spec.get("ref_object_type", "") or spec.get("target_ref_object_type", "") or "").strip()
                    if ref_id:
                        targets = [r for r in pool_items if str(r.get("ref_object_id", "")) == ref_id and (not ref_type or str(r.get("ref_object_type", "")) == ref_type)]

                if not targets:
                    skipped.append({"effect_id": effect_id, "effect_type": et, "rule_id": rule_id, "reason": "no_targets"})
                    continue

                attr = spec.get("attribute") if isinstance(spec.get("attribute"), dict) else spec
                raw = str(attr.get("raw", "") or attr.get("attribute_raw", "") or "").strip()
                display = str(attr.get("display", "") or attr.get("attribute_display", "") or raw or "attribute")
                value_type = str(attr.get("value_type", "") or ("numerical" if isinstance(attr.get("attribute_value"), (int, float)) else "discrete"))
                attr_name = str(attr.get("attribute_name", "") or "").strip()
                attr_value = attr.get("attribute_value")
                if not attr_name:
                    # best-effort infer from raw "name:value"
                    if ":" in raw:
                        attr_name = raw.split(":", 1)[0].strip()
                    else:
                        attr_name = raw.strip() or "attribute"

                modality = str(attr.get("modality", "") or "internal")
                er = coerce_float(attr.get("er", 0.0), 0.0)
                ev = coerce_float(attr.get("ev", 0.0), 0.0)
                reason = str(spec.get("reason", "") or f"iesm_pool_bind_attribute:{rule_id}").strip()

                for t in targets[:24]:
                    tid = str(t.get("item_id", "") or "")
                    if not tid:
                        continue
                    # Stable attribute id for deduplication on the same target+name.
                    # sanitized
                    target_ref_id = str(t.get("ref_object_id", "") or tid)
                    attr_id = f"sa_iesm_attr_{attr_name}_{target_ref_id}"
                    attribute_sa = {
                        "id": attr_id,
                        "object_type": "sa",
                        "content": {
                            "raw": raw or f"{attr_name}:{attr_value}",
                            "display": display,
                            "value_type": value_type,
                            "attribute_name": attr_name,
                            "attribute_value": attr_value,
                        },
                        "stimulus": {"role": "attribute", "modality": modality},
                        "energy": {"er": float(er), "ev": float(ev)},
                        # meta.ext: keep minimal provenance for observability and downstream memory enrichment.
                        # sanitized
                        "meta": {
                            "ext": {
                                "bound_from": "iesm_pool_bind_attribute",
                                "rule_id": rule_id,
                                "rule_title": str(eff.get("rule_title", "") or ""),
                                "rule_phase": str(eff.get("rule_phase", "") or ""),
                                "rule_priority": int(eff.get("rule_priority", 0) or 0),
                                "reason": reason,
                                "trace_id": trace_id,
                                "tick_id": tick_id,
                            }
                        },
                    }
                    res = self.pool.bind_attribute_node_to_object(
                        target_item_id=tid,
                        attribute_sa=attribute_sa,
                        trace_id=f"{trace_id}_iesm_bind_attr",
                        tick_id=tick_id,
                        source_module="innate_script",
                        reason=reason,
                    )
                    applied.append(
                        {
                            "effect_id": effect_id,
                            "effect_type": et,
                            "rule_id": rule_id,
                            "target_item_id": tid,
                            "attribute_sa_id": attr_id,
                            "success": bool(res.get("success", False)),
                            "code": res.get("code", ""),
                            "data": res.get("data", {}) or {},
                        }
                    )
                continue

            skipped.append({"effect_id": effect_id, "effect_type": et, "rule_id": rule_id, "reason": "unsupported_effect_type"})

        return {
            "applied_count": len(applied),
            "skipped_count": len(skipped),
            "applied": applied[:256],
            "skipped": skipped[:256],
        }

    def get_placeholder_modules(self) -> list[dict[str, Any]]:
        return [
            {
                "module": "attention",
                "title": "注意力模块",
                "status": "MVP 可用",
                # sanitized
            },
            {
                "module": "cognitive_feeling",
                "title": "认知感受模块",
                "status": "规划中",
                # sanitized
            },
            {
                "module": "emotion",
                "title": "情绪模块",
                "status": "规划中",
                # sanitized
            },
            {
                "module": "innate_script",
                "title": "先天脚本模块",
                "status": "规划中",
                # sanitized
            },
            {
                "module": "action",
                "title": "行动模块",
                "status": "MVP 可用",
                # sanitized
            },
        ]

    def _build_sensor_report(self, text: str, sensor_result: dict) -> dict:
        # sanitized
        # sanitized
        if not isinstance(sensor_result, dict):
            return {"input_text": text, "success": False, "code": "SENSOR_RESULT_INVALID", "message": "sensor_result 不是 dict / sensor_result is not a dict"}
        if not bool(sensor_result.get("success", False)):
            return {
                "input_text": text,
                "success": False,
                "code": str(sensor_result.get("code", "") or ""),
                "message": str(sensor_result.get("message", "") or ""),
                "error": sensor_result.get("error", {}) if isinstance(sensor_result.get("error", {}), dict) else {},
                # sanitized
            }

        data = sensor_result.get("data", {}) if isinstance(sensor_result.get("data", {}), dict) else {}
        packet = data.get("stimulus_packet", {}) if isinstance(data.get("stimulus_packet", {}), dict) else {}
        sensor_frame = data.get("sensor_frame", {}) if isinstance(data.get("sensor_frame", {}), dict) else {}
        trace_id = str(((sensor_result.get("meta", {}) or {}).get("trace_id", "")) or "")
        runtime_snapshot = self.sensor.get_runtime_snapshot(trace_id=f"{trace_id}_sensor_runtime")["data"]
        unit_rows = self._describe_packet_units(packet)
        groups = self._describe_packet_groups(packet)
        # sanitized
        # sanitized
        # sanitized
        # sanitized
        csa_bundle_count = sum(int(g.get("csa_count", 0) or 0) for g in groups if isinstance(g, dict))
        feature_sa_count = sum(1 for row in unit_rows if str(row.get("role", "") or "") != "attribute")
        attribute_sa_count = sum(1 for row in unit_rows if str(row.get("role", "") or "") == "attribute")
        return {
            "input_text": text,
            "success": True,
            "normalized_text": sensor_frame.get("normalized_text", text),
            "mode": data.get("tokenization_summary", {}).get("mode", ""),
            "tokenizer_backend": runtime_snapshot["config_summary"]["tokenizer_backend"],
            "tokenizer_available": runtime_snapshot["config_summary"]["tokenizer_available"],
            "tokenizer_fallback": data.get("tokenization_summary", {}).get("tokenizer_fallback", False),
            "sa_count": len(packet.get("sa_items", [])),
            "csa_count": len(packet.get("csa_items", [])),
            "feature_sa_count": feature_sa_count,
            "attribute_sa_count": attribute_sa_count,
            "csa_bundle_count": csa_bundle_count,
            "groups": groups,
            "units": unit_rows,
            "feature_units": unit_rows,
            "echo_frames_used": list(data.get("echo_frames_used", [])),
            "echo_decay_summary": data.get("echo_decay_summary", {}),
            "fatigue_summary": data.get("fatigue_summary", {}),
        }

    def _run_state_pool_maintenance(self, trace_id: str, tick_id: str) -> dict:
        before_snapshot = self.pool.get_state_snapshot(trace_id=f"{trace_id}_maint_before", tick_id=tick_id, top_k=None)["data"]["snapshot"]
        before_count = self.pool._history.size
        start_ms = int(time.time() * 1000)
        result = self.pool.tick_maintain_state_pool(
            trace_id=f"{trace_id}_maint",
            tick_id=tick_id,
            apply_decay=True,
            apply_neutralization=True,
            apply_prune=True,
            apply_merge=True,
            enable_script_broadcast=False,
        )
        after_snapshot = self.pool.get_state_snapshot(trace_id=f"{trace_id}_maint_after", tick_id=tick_id, top_k=None)["data"]["snapshot"]
        return {
            "summary": result["data"],
            "before_summary": before_snapshot.get("summary", {}),
            "after_summary": after_snapshot.get("summary", {}),
            "events": self._collect_history_events(before_count, start_ms),
        }

    def _build_attention_memory_stub(self, trace_id: str, tick_id: str, *, focus_directives: list[dict] | None = None, modulation: dict | None = None) -> tuple[dict, dict]:
        modulation = modulation or {}
        base_top_n = int(self._config.get("attention_top_n", 16))
        effective_top_n = int(modulation.get("top_n", base_top_n) or base_top_n)
        result = self.attention.build_cam_from_pool(
            self.pool,
            trace_id=trace_id,
            tick_id=tick_id,
            top_n=effective_top_n,
            consume_energy=bool(self._config.get("attention_stub_consume_energy", True)),
            memory_energy_ratio=float(self._config.get("attention_memory_energy_ratio", 0.5)),
            focus_directives=focus_directives,
            modulation=modulation,
        )
        if not result.get("success"):
            return (
                {
                    "snapshot_id": f"{trace_id}_cam",
                    "object_type": "runtime_snapshot",
                    "sub_type": "cam_snapshot_error_fallback",
                    "schema_version": "1.1",
                    "trace_id": trace_id,
                    "tick_id": tick_id,
                    "summary": {"active_item_count": 0},
                    "top_items": [],
                },
                {"top_items": [], "structure_items": []},
            )

        data = result.get("data", {}) or {}
        cam_snapshot = data.get("cam_snapshot", {}) or {}
        attention_report = data.get("attention_report", {}) or {}
        if "memory_snapshot_summary" not in attention_report and "cam_snapshot_summary" in attention_report:
            attention_report["memory_snapshot_summary"] = attention_report.get("cam_snapshot_summary", {})
        return cam_snapshot, attention_report

    def _make_attention_memory_snapshot(self, *, selected_items: list[dict], trace_id: str, tick_id: str) -> dict:
        top_items = []
        type_counts: dict[str, int] = {}
        high_er = 0
        high_ev = 0
        high_cp = 0
        for item in selected_items:
            memory_er = round(float(item.get("memory_er", 0.0)), 8)
            memory_ev = round(float(item.get("memory_ev", 0.0)), 8)
            cp_delta = round(memory_er - memory_ev, 8)
            cp_abs = round(abs(cp_delta), 8)
            copied = dict(item)
            copied["er"] = memory_er
            copied["ev"] = memory_ev
            copied["cp_delta"] = cp_delta
            copied["cp_abs"] = cp_abs
            copied["salience_score"] = round(max(memory_er, memory_ev), 8)
            top_items.append(copied)

            ref_type = copied.get("ref_object_type", "unknown")
            type_counts[ref_type] = type_counts.get(ref_type, 0) + 1
            if memory_er >= 0.5:
                high_er += 1
            if memory_ev >= 0.5:
                high_ev += 1
            if cp_abs >= 0.5:
                high_cp += 1

        return {
            "snapshot_id": f"{trace_id}_attention_memory",
            "object_type": "runtime_snapshot",
            "sub_type": "attention_memory_stub_snapshot",
            "schema_version": "1.1",
            "trace_id": trace_id,
            "tick_id": tick_id,
            "summary": {
                "active_item_count": len(top_items),
                "high_er_item_count": high_er,
                "high_ev_item_count": high_ev,
                "high_cp_item_count": high_cp,
                "object_type_counts": type_counts,
            },
            "top_items": top_items,
        }

    @staticmethod
    def _attention_priority(item: dict) -> float:
        total_energy = float(item.get("er", 0.0)) + float(item.get("ev", 0.0))
        cp_abs = float(item.get("cp_abs", 0.0))
        salience = float(item.get("salience_score", 0.0))
        updated_at = float(item.get("updated_at", 0.0))
        return round(total_energy * 1.25 + cp_abs * 0.35 + salience * 0.15 + updated_at * 1e-12, 12)

    def _neutralize_packet_against_pool(self, packet: dict, trace_id: str, tick_id: str) -> dict:
        input_packet_summary = self._describe_stimulus_packet(packet)
        if not packet.get("sa_items") and not packet.get("csa_items"):
            return {
                "input_packet": input_packet_summary,
                "residual_packet": input_packet_summary,
                "residual_packet_raw": packet,
                "priority_events": [],
                "priority_diagnostics": [],
                "priority_summary": {
                    "priority_neutralized_item_count": 0,
                    "priority_event_count": 0,
                    "priority_diagnostic_count": 0,
                    "input_total_er": round(float(input_packet_summary.get("total_er", 0.0)), 8),
                    "input_total_ev": round(float(input_packet_summary.get("total_ev", 0.0)), 8),
                    "residual_total_er": round(float(input_packet_summary.get("total_er", 0.0)), 8),
                    "residual_total_ev": round(float(input_packet_summary.get("total_ev", 0.0)), 8),
                    "consumed_er": 0.0,
                    "consumed_ev": 0.0,
                    "input_flat_token_count": len(input_packet_summary.get("flat_tokens", [])),
                    "residual_flat_token_count": len(input_packet_summary.get("flat_tokens", [])),
                },
            }

        before_count = self.pool._history.size
        start_ms = int(time.time() * 1000)
        neutralization_result = self.pool._priority_neutralize_stimulus_packet(
            stimulus_packet=packet,
            tick_number=self.pool._tick_counter + 1,
            trace_id=f"{trace_id}_cache_neutralize",
            tick_id=tick_id,
            source_module="observatory",
        )
        priority_events = self._collect_history_events(before_count, start_ms)
        if not priority_events:
            priority_events = [
                self._enrich_history_event(event)
                for event in neutralization_result.get("events", [])
            ]
        residual_packet = neutralization_result.get("residual_packet", packet)
        residual_packet_summary = self._describe_stimulus_packet(residual_packet)
        priority_diagnostics = list(neutralization_result.get("diagnostics", []))
        return {
            "input_packet": input_packet_summary,
            "residual_packet": residual_packet_summary,
            "residual_packet_raw": residual_packet,
            "priority_events": priority_events,
            "priority_diagnostics": priority_diagnostics,
            "priority_summary": {
                "priority_neutralized_item_count": int(neutralization_result.get("neutralized_item_count", 0)),
                "priority_event_count": len(priority_events),
                "priority_diagnostic_count": len(priority_diagnostics),
                "input_total_er": round(float(input_packet_summary.get("total_er", 0.0)), 8),
                "input_total_ev": round(float(input_packet_summary.get("total_ev", 0.0)), 8),
                "residual_total_er": round(float(residual_packet_summary.get("total_er", 0.0)), 8),
                "residual_total_ev": round(float(residual_packet_summary.get("total_ev", 0.0)), 8),
                "consumed_er": round(float(input_packet_summary.get("total_er", 0.0)) - float(residual_packet_summary.get("total_er", 0.0)), 8),
                "consumed_ev": round(float(input_packet_summary.get("total_ev", 0.0)) - float(residual_packet_summary.get("total_ev", 0.0)), 8),
                "input_flat_token_count": len(input_packet_summary.get("flat_tokens", [])),
                "residual_flat_token_count": len(residual_packet_summary.get("flat_tokens", [])),
            },
        }

    def _apply_packet_to_pool(
        self,
        packet: dict,
        trace_id: str,
        tick_id: str,
        disable_priority_neutralization: bool = False,
    ) -> tuple[dict, list[dict], dict]:
        if not packet.get("sa_items") and not packet.get("csa_items"):
            return {}, [], {
                "id": "",
                "object_type": "stimulus_packet",
                "sa_items": [],
                "csa_items": [],
                "grouped_sa_sequences": [],
                "energy_summary": {"total_er": 0.0, "total_ev": 0.0},
            }
        before_count = self.pool._history.size
        start_ms = int(time.time() * 1000)
        original_priority_flag = bool(self.pool._config.get("enable_priority_stimulus_neutralization", True))
        if disable_priority_neutralization:
            self.pool._config["enable_priority_stimulus_neutralization"] = False
        try:
            result = self.pool.apply_stimulus_packet(
                stimulus_packet=packet,
                trace_id=f"{trace_id}_pool_apply",
                tick_id=tick_id,
                source_module="observatory",
            )
        finally:
            if disable_priority_neutralization:
                self.pool._config["enable_priority_stimulus_neutralization"] = original_priority_flag
        return (
            result.get("data", {}),
            self._collect_history_events(before_count, start_ms),
            result.get("data", {}).get("residual_stimulus_packet", packet),
        )

    def _project_runtime_structures(self, projections: list[dict], trace_id: str, tick_id: str) -> list[dict]:
        def _is_attribute_only_structure(structure_block: dict) -> bool:
            """
            Detect attribute-only structures (should NOT become standalone StatePool objects).
            # sanitized

            Why / 婵炴垶鎹佸銊у垝閸喓鈻曢柛顐墰缁?
              # sanitized
              # sanitized

            Rule / 闁荤喐鐟ョ€氼剟宕归鐐存櫖闁革富鎽怭闂佹寧绋戦¨鈧紒?
              - 闂佸吋鐪归崕宕囧垝閵娾晛鍑犻柛鏇ㄥ幗閻?sequence_groups.units 婵炴垶鎼╅崢濂告偤閵娾晛鎹堕柕濞垮€栧畷鏌ユ煙?unit_role != attribute 闂?token闂佹寧绋戦懟顖炲垂椤栨粍濯奸柕鍫濆缁€瀣槈閹惧磭校婵℃彃鎽滈惀顏堫敍濮樿鲸鍓戦梺璇″劯閸涱垱灏濋梺鍝勵儏鐎氬摜妲?
              # sanitized
            """
            if not isinstance(structure_block, dict):
                return False
            groups = structure_block.get("sequence_groups", [])
            tokens_seen = 0
            feature_seen = 0
            if isinstance(groups, list) and groups:
                for g in groups:
                    if not isinstance(g, dict):
                        continue
                    units = g.get("units", [])
                    if isinstance(units, list) and units:
                        for u in units:
                            if not isinstance(u, dict):
                                continue
                            tok = str(u.get("token", "") or "")
                            if not tok:
                                continue
                            tokens_seen += 1
                            role = str(u.get("unit_role", "") or "")
                            if role != "attribute":
                                feature_seen += 1
                    else:
                        # Legacy fallback: if no units, treat tokens as features (cannot decide attribute-only).
                        # sanitized
                        return False
            else:
                # No groups: fallback to flat_tokens, assume not attribute-only.
                # sanitized
                return False
            return tokens_seen > 0 and feature_seen == 0

        results = []
        for item in projections:
            projection_kind = str(item.get("projection_kind", "structure") or "structure")
            memory_id = str(item.get("memory_id", ""))
            structure_id = str(item.get("structure_id", ""))
            backing_structure_id = str(item.get("backing_structure_id", structure_id))
            if projection_kind == "memory" and memory_id:
                results.append(
                    {
                        "projection_kind": projection_kind,
                        "memory_id": memory_id,
                        "structure_id": structure_id,
                        "display_text": str(item.get("display_text", memory_id)),
                        "er": round(float(item.get("er", 0.0)), 8),
                        "ev": round(float(item.get("ev", 0.0)), 8),
                        "reason": item.get("reason", ""),
                        "result": "memory_projection_skipped_state_pool",
                    }
                )
                continue

            # Skip structures that should not become ordinary StatePool anchors.
            try:
                st_obj = self.hdb._structure_store.get(structure_id)  # type: ignore[attr-defined]
                st_block = (st_obj or {}).get("structure", {}) if isinstance(st_obj, dict) else {}
                st_sub_type = str((st_obj or {}).get("sub_type", "") or "") if isinstance(st_obj, dict) else ""
                st_signature = str((st_block or {}).get("content_signature", "") or "") if isinstance(st_block, dict) else ""
                if _is_attribute_only_structure(st_block):
                    results.append(
                        {
                            "projection_kind": projection_kind,
                            "memory_id": memory_id,
                            "structure_id": structure_id,
                            "target_item_id": "",
                            "target_ref_object_id": structure_id,
                            "target_ref_object_type": "st",
                            "display_text": (st_block.get("display_text") if isinstance(st_block, dict) else "") or structure_id,
                            "er": round(float(item.get("er", 0.0)), 8),
                            "ev": round(float(item.get("ev", 0.0)), 8),
                            "reason": item.get("reason", ""),
                            "result": "skipped_attribute_only_structure",
                        }
                    )
                    continue
                if st_sub_type == "cognitive_stitching_event_structure" or st_signature.startswith("cs_event::"):
                    results.append(
                        {
                            "projection_kind": projection_kind,
                            "memory_id": memory_id,
                            "structure_id": structure_id,
                            "target_item_id": "",
                            "target_ref_object_id": structure_id,
                            "target_ref_object_type": "st",
                            "display_text": (st_block.get("display_text") if isinstance(st_block, dict) else "") or structure_id,
                            "er": round(float(item.get("er", 0.0)), 8),
                            "ev": round(float(item.get("ev", 0.0)), 8),
                            "reason": item.get("reason", ""),
                            "result": "skipped_cognitive_stitching_event_structure",
                        }
                    )
                    continue
            except Exception:
                # Best-effort: if detection fails, do not block projection.
                pass

            runtime_object = self.hdb.make_runtime_structure_object(
                structure_id,
                er=float(item.get("er", 0.0)),
                ev=float(item.get("ev", 0.0)),
                reason=item.get("reason", "hdb_projection"),
            )
            if not runtime_object:
                continue
            insert_result = self.pool.insert_runtime_node(
                runtime_object=runtime_object,
                trace_id=f"{trace_id}_projection",
                tick_id=tick_id,
                source_module="hdb",
                reason=item.get("reason", "hdb_projection"),
            )
            # For observability: carry the resulting StatePool item_id so other modules
            # (e.g. TimeSensor binding, IESM scripts) can reference the exact runtime anchor.
            # sanitized
            ir_data = insert_result.get("data", {}) if isinstance(insert_result, dict) else {}
            target_item_id = ""
            if isinstance(ir_data, dict):
                target_item_id = str(ir_data.get("item_id", "") or ir_data.get("target_item_id", "") or "")
            results.append(
                {
                    "projection_kind": projection_kind,
                    "memory_id": memory_id,
                    "structure_id": structure_id,
                    "target_item_id": target_item_id,
                    "target_ref_object_id": structure_id,
                    "target_ref_object_type": "st",
                    "display_text": runtime_object.get("content", {}).get("display", structure_id),
                    "er": round(float(item.get("er", 0.0)), 8),
                    "ev": round(float(item.get("ev", 0.0)), 8),
                    "reason": item.get("reason", ""),
                    "result": insert_result.get("message", ""),
                }
            )
            try:
                self._mark_projection_fatigue(item)
            except Exception:
                pass
        return results

    def _collect_memory_activation_seed_targets(self, report: dict) -> list[dict]:
        """
        Seed memory activation directly from newly written residual-memory records.

        This keeps fresh episodic memories visible even in cycles where the current
        induction source energy in StatePool is too small to cross the induction
        threshold, while still preserving the separate memory pool design.
        """
        er_ratio = max(0.0, float(self.hdb._config.get("er_induction_ratio", 0.22)))
        ev_ratio = max(0.0, float(self.hdb._config.get("ev_propagation_ratio", 0.28)))
        seed_targets: list[dict] = []

        stimulus_rounds = list(
            report.get("stimulus_level", {}).get("result", {}).get("debug", {}).get("round_details", [])
        )
        for round_detail in stimulus_rounds:
            residual = dict(round_detail.get("created_residual_structure", {}) or {})
            memory_id = str(residual.get("memory_id", ""))
            if not memory_id:
                continue
            delta_ev = round(
                max(
                    0.0,
                    float(round_detail.get("transferred_er", 0.0)) * er_ratio
                    + float(round_detail.get("transferred_ev", 0.0)) * ev_ratio,
                ),
                8,
            )
            if delta_ev <= 0.0:
                continue
            matched_structure_id = str((round_detail.get("selected_match") or {}).get("structure_id", ""))
            target_display_text = (
                str(residual.get("canonical_grouped_display_text", ""))
                or str(residual.get("canonical_display_text", ""))
                or memory_id
            )
            seed_targets.append(
                {
                    "projection_kind": "memory",
                    "memory_id": memory_id,
                    "backing_structure_id": matched_structure_id,
                    "target_display_text": target_display_text,
                    "delta_ev": delta_ev,
                    "sources": [matched_structure_id] if matched_structure_id else [],
                    "modes": ["residual_storage_seed"],
                }
            )

        structure_rounds = list(
            report.get("structure_level", {}).get("result", {}).get("debug", {}).get("round_details", [])
        )
        structure_summaries = {
            int(item.get("round_index", 0)): dict(item)
            for item in report.get("structure_level", {}).get("result", {}).get("round_summaries", [])
            if int(item.get("round_index", 0)) > 0
        }
        for round_detail in structure_rounds:
            storage_summary = dict(round_detail.get("storage_summary", {}) or {})
            actions = list(storage_summary.get("actions", []) or [])
            if not actions:
                continue
            round_summary = structure_summaries.get(int(round_detail.get("round_index", 0)), {})
            delta_ev = round(
                max(
                    0.0,
                    float(round_summary.get("matched_er_total", 0.0)) * er_ratio
                    + float(round_summary.get("matched_ev_total", 0.0)) * ev_ratio,
                ),
                8,
            )
            if delta_ev <= 0.0:
                continue
            selected_group = dict(round_detail.get("selected_group", {}) or {})
            source_ids = [
                str(item.get("structure_id", ""))
                for item in selected_group.get("required_structures", [])
                if str(item.get("structure_id", ""))
            ]
            for action in actions:
                if str(action.get("type", "")) != "append_raw_residual":
                    continue
                memory_id = str(action.get("memory_id", ""))
                if not memory_id:
                    continue
                target_display_text = (
                    str(action.get("canonical_grouped_display_text", ""))
                    or str(action.get("canonical_display_text", ""))
                    or memory_id
                )
                seed_targets.append(
                    {
                        "projection_kind": "memory",
                        "memory_id": memory_id,
                        "backing_structure_id": str(storage_summary.get("owner_id", "")),
                        "target_display_text": target_display_text,
                        "delta_ev": delta_ev,
                        "sources": list(dict.fromkeys(source_ids)),
                        "modes": ["residual_storage_seed"],
                    }
                )
        filtered_targets: list[dict] = []
        for item in seed_targets:
            projection_probe = {
                "projection_kind": "memory",
                "memory_id": item.get("memory_id", ""),
                "structure_id": item.get("backing_structure_id", ""),
                "backing_structure_id": item.get("backing_structure_id", ""),
                "display_text": item.get("target_display_text", ""),
                "er": 0.0,
                "ev": float(item.get("delta_ev", 0.0) or 0.0),
                "reason": "memory_seed_target",
            }
            effective = self._apply_projection_fatigue_to_item(projection_probe)
            if effective is None:
                continue
            next_item = dict(item)
            next_item["delta_ev"] = round(float(effective.get("ev", item.get("delta_ev", 0.0)) or 0.0), 8)
            next_item["projection_fatigue"] = round(float(effective.get("projection_fatigue", 0.0) or 0.0), 8)
            filtered_targets.append(next_item)
        return filtered_targets

    def _apply_memory_feedback(self, *, memory_items: list[dict], trace_id: str, tick_id: str) -> dict:
        feedback_items: list[dict] = []
        feedback_results: list[dict] = []
        feedback_bucket_counts: dict[str, int] = {}

        for item in memory_items or []:
            memory_id = str(item.get("memory_id", ""))
            if not memory_id:
                continue
            # Only the newly assigned activation delta of this round may feed back.
            # The pool's retained live energy is not a per-round replay budget.
            delta_er = round(max(0.0, float(item.get("last_delta_er", 0.0))), 8)
            delta_ev = round(max(0.0, float(item.get("last_delta_ev", 0.0))), 8)
            if delta_er <= 0.0 and delta_ev <= 0.0:
                continue
            episodic_obj = self.hdb._episodic_store.get(memory_id)
            if not episodic_obj:
                continue
            memory_material = dict(episodic_obj.get("meta", {}).get("ext", {}).get("memory_material", {}) or {})
            memory_kind = str(memory_material.get("memory_kind", ""))
            if memory_kind == "stimulus_packet":
                feedback_bucket_key = str(memory_material.get("grouped_display_text", "") or item.get("display_text", memory_id) or memory_id)
                bucket_seen = int(feedback_bucket_counts.get(feedback_bucket_key, 0) or 0)
                projection_probe = {
                    "projection_kind": "memory_feedback_packet",
                    "memory_id": memory_id,
                    "display_text": str(memory_material.get("grouped_display_text", "") or item.get("display_text", memory_id)),
                    "grouped_display_text": str(memory_material.get("grouped_display_text", "") or ""),
                    "er": delta_er,
                    "ev": delta_ev,
                    "reason": "memory_feedback_stimulus_packet",
                }
                effective_feedback = self._apply_projection_fatigue_to_item(projection_probe)
                if effective_feedback is None:
                    continue
                delta_er = round(max(0.0, float(effective_feedback.get("er", delta_er) or 0.0)), 8)
                delta_ev = round(max(0.0, float(effective_feedback.get("ev", delta_ev) or 0.0)), 8)
                if bucket_seen > 0:
                    crowd_ratio = 1.0 / float(1 + bucket_seen)
                    delta_er = round(delta_er * crowd_ratio, 8)
                    delta_ev = round(delta_ev * crowd_ratio, 8)
                if delta_er <= 0.0 and delta_ev <= 0.0:
                    continue
                packet_result = self._build_memory_feedback_stimulus_packet(
                    memory_id=memory_id,
                    memory_material=memory_material,
                    total_er=delta_er,
                    total_ev=delta_ev,
                    trace_id=trace_id,
                    tick_id=tick_id,
                )
                packet = packet_result.get("packet")
                if not packet:
                    continue
                apply_result, events, landed_packet = self._apply_packet_to_pool(
                    packet,
                    trace_id=f"{trace_id}_memory_feedback",
                    tick_id=tick_id,
                    disable_priority_neutralization=True,
                )
                target_texts = list(packet_result.get("target_display_texts", []))
                feedback_items.append(
                    {
                        "memory_id": memory_id,
                        "delta_er": delta_er,
                        "delta_ev": delta_ev,
                        "feedback_kind": "stimulus_packet",
                        "target_count": len(target_texts),
                        "grouped_display_text": str(memory_material.get("grouped_display_text", "")),
                        "target_display_texts": target_texts,
                    }
                )
                feedback_results.append(
                    {
                        "memory_id": memory_id,
                        "memory_kind": "stimulus_packet",
                        "display_text": str(item.get("display_text", memory_id)),
                        "grouped_display_text": str(memory_material.get("grouped_display_text", "")),
                        "delta_er": delta_er,
                        "delta_ev": delta_ev,
                        "same_tick_bucket_rank": int(bucket_seen + 1),
                        "projection_fatigue": round(float(effective_feedback.get("projection_fatigue", 0.0) or 0.0), 8),
                        "target_count": len(target_texts),
                        "target_display_texts": target_texts,
                        "packet": self._describe_stimulus_packet(packet),
                        "landed_packet": self._describe_stimulus_packet(landed_packet),
                        "apply_result": apply_result,
                        "events": events,
                    }
                )
                feedback_bucket_counts[feedback_bucket_key] = bucket_seen + 1
                self._mark_projection_fatigue(projection_probe)
                continue

            if memory_kind == "structure_group":
                projections = self._build_memory_feedback_structure_projections(
                    memory_id=memory_id,
                    memory_material=memory_material,
                    total_er=delta_er,
                    total_ev=delta_ev,
                )
                if not projections:
                    continue
                effective_projections: list[dict] = []
                skipped_projection_count = 0
                for projection in projections:
                    effective = self._apply_projection_fatigue_to_item(projection)
                    if effective is None:
                        skipped_projection_count += 1
                        continue
                    effective_projections.append(effective)
                if not effective_projections:
                    continue
                projection_results = self._project_runtime_structures(
                    effective_projections,
                    trace_id=f"{trace_id}_memory_feedback",
                    tick_id=tick_id,
                )
                target_texts = [
                    str(projection.get("display_text", projection.get("structure_id", "")))
                    for projection in effective_projections
                    if str(projection.get("structure_id", ""))
                ]
                feedback_items.append(
                    {
                        "memory_id": memory_id,
                        "delta_er": delta_er,
                        "delta_ev": delta_ev,
                        "feedback_kind": "structure_group",
                        "target_count": len(target_texts),
                        "grouped_display_text": str(memory_material.get("grouped_display_text", "")),
                        "target_display_texts": target_texts,
                        "skipped_projection_count": skipped_projection_count,
                    }
                )
                feedback_results.append(
                    {
                        "memory_id": memory_id,
                        "memory_kind": "structure_group",
                        "display_text": str(item.get("display_text", memory_id)),
                        "grouped_display_text": str(memory_material.get("grouped_display_text", "")),
                        "delta_er": delta_er,
                        "delta_ev": delta_ev,
                        "target_count": len(target_texts),
                        "target_display_texts": target_texts,
                        "skipped_projection_count": skipped_projection_count,
                        "projections": projection_results,
                    }
                )

        record_result = self.hdb.record_memory_feedback(
            feedback_items=feedback_items,
            trace_id=trace_id,
            tick_id=tick_id,
        )["data"]
        total_feedback_er = round(sum(float(item.get("delta_er", 0.0)) for item in feedback_results), 8)
        total_feedback_ev = round(sum(float(item.get("delta_ev", 0.0)) for item in feedback_results), 8)
        return {
            "applied_count": len(feedback_results),
            "total_feedback_er": total_feedback_er,
            "total_feedback_ev": total_feedback_ev,
            "total_feedback_energy": round(total_feedback_er + total_feedback_ev, 8),
            "items": feedback_results,
            "record_result": record_result,
        }

    def _build_memory_feedback_structure_projections(
        self,
        *,
        memory_id: str,
        memory_material: dict,
        total_er: float,
        total_ev: float,
    ) -> list[dict]:
        structure_items = list(memory_material.get("structure_items", []))
        ordered_structure_ids = [
            str(item.get("structure_id", ""))
            for item in structure_items
            if str(item.get("structure_id", ""))
        ]
        if not ordered_structure_ids:
            ordered_structure_ids = [
                str(structure_id)
                for structure_id in memory_material.get("structure_refs", [])
                if str(structure_id)
            ]
        if not ordered_structure_ids:
            return []
        er_allocations = self._allocate_weighted_values(
            keys=ordered_structure_ids,
            raw_weights=dict(memory_material.get("structure_energy_profile", {}) or {}),
            total_value=total_er,
        )
        ev_allocations = self._allocate_weighted_values(
            keys=ordered_structure_ids,
            raw_weights=dict(memory_material.get("structure_energy_profile", {}) or {}),
            total_value=total_ev,
        )
        display_lookup = {
            str(item.get("structure_id", "")): str(item.get("display_text", item.get("grouped_display_text", item.get("structure_id", ""))))
            for item in structure_items
            if str(item.get("structure_id", ""))
        }
        return [
            {
                "projection_kind": "structure",
                "memory_id": memory_id,
                "structure_id": structure_id,
                "display_text": display_lookup.get(structure_id, structure_id),
                "er": round(float(er_allocations.get(structure_id, 0.0)), 8),
                "ev": round(float(ev_allocations.get(structure_id, 0.0)), 8),
                "reason": "memory_feedback",
            }
            for structure_id in ordered_structure_ids
            if float(er_allocations.get(structure_id, 0.0)) > 0.0 or float(ev_allocations.get(structure_id, 0.0)) > 0.0
        ]

    def _build_memory_feedback_stimulus_packet(
        self,
        *,
        memory_id: str,
        memory_material: dict,
        total_er: float,
        total_ev: float,
        trace_id: str,
        tick_id: str,
    ) -> dict:
        sequence_groups = list(memory_material.get("sequence_groups", []))
        if not sequence_groups or (total_er <= 0.0 and total_ev <= 0.0):
            return {"packet": None, "target_display_texts": []}

        ordered_unit_ids = [
            str(unit.get("unit_id", ""))
            for group in sequence_groups
            for unit in group.get("units", [])
            if str(unit.get("unit_id", ""))
        ]
        er_allocations = self._allocate_weighted_values(
            keys=ordered_unit_ids,
            raw_weights=dict(memory_material.get("unit_energy_profile", {}) or {}),
            total_value=total_er,
        )
        ev_allocations = self._allocate_weighted_values(
            keys=ordered_unit_ids,
            raw_weights=dict(memory_material.get("unit_energy_profile", {}) or {}),
            total_value=total_ev,
        )

        now_ms = int(time.time() * 1000)
        packet_id = next_id("mfpkt")
        sa_items: list[dict] = []
        csa_items: list[dict] = []
        grouped_sequences: list[dict] = []
        packet_sequence_index = 0

        for group_order, group in enumerate(sequence_groups):
            units = sorted(
                [dict(unit) for unit in group.get("units", []) if isinstance(unit, dict)],
                key=lambda item: int(item.get("sequence_index", 0)),
            )
            if not units:
                continue
            packet_group_index = len(grouped_sequences)
            source_group_index = int(group.get("source_group_index", group.get("group_index", packet_group_index)))
            origin_frame_id = str(group.get("origin_frame_id", memory_id)) or memory_id
            group_unit_id_map: dict[str, str] = {}
            created_sa_by_id: dict[str, dict] = {}
            group_sa_ids: list[str] = []
            group_csa_ids: list[str] = []

            for unit in units:
                original_unit_id = str(unit.get("unit_id", ""))
                if not original_unit_id:
                    continue
                token = str(unit.get("token", unit.get("display_text", "")) or "")
                if (
                    token.startswith("{") and token.endswith("}")
                    and not bool(group.get("order_sensitive", False))
                    and str(group.get("string_unit_kind", "") or "") != "char_sequence"
                ):
                    # Goal B safety: do not replay presentation-wrapped structure tokens back into SA.
                    continue
                sa_id = next_id("sa_memfb")
                unit_role = str(unit.get("unit_role", unit.get("role", "feature")))
                attribute_name = str(unit.get("attribute_name", ""))
                attribute_value = unit.get("attribute_value")
                if attribute_name:
                    content = {
                        "raw": token,
                        "display": token,
                        "normalized": token,
                        "value_type": "numerical" if attribute_value is not None else str(unit.get("value_type", "discrete") or "discrete"),
                        "attribute_name": attribute_name,
                        "attribute_value": attribute_value,
                    }
                else:
                    content = {
                        "raw": token,
                        "display": token,
                        "normalized": token,
                        "value_type": str(unit.get("value_type", "discrete") or "discrete"),
                    }
                packet_context = {
                    "source_type": "memory_feedback",
                    "group_index": packet_group_index,
                    "source_group_index": source_group_index,
                    "origin_frame_id": origin_frame_id,
                    "echo_depth": 0,
                    "round_created": 0,
                    "decay_count": 0,
                    "sequence_index": packet_sequence_index,
                    "order_sensitive": bool(group.get("order_sensitive", False)),
                    "string_unit_kind": str(group.get("string_unit_kind", "") or ""),
                    "string_token_text": str(group.get("string_token_text", "") or ""),
                }
                sa_obj = {
                    "id": sa_id,
                    "object_type": "sa",
                    "content": content,
                    "stimulus": {
                        "role": unit_role,
                        "modality": "memory_feedback",
                        "order_sensitive": bool(group.get("order_sensitive", False)),
                        "string_unit_kind": str(group.get("string_unit_kind", "") or ""),
                        "string_token_text": str(group.get("string_token_text", "") or ""),
                    },
                    "energy": {
                        "er": round(float(er_allocations.get(original_unit_id, 0.0)), 8),
                        "ev": round(float(ev_allocations.get(original_unit_id, 0.0)), 8),
                    },
                    "source": {
                        "module": "observatory",
                        "interface": "memory_feedback",
                        "origin": "episodic_memory_feedback",
                        "origin_id": memory_id,
                        "parent_ids": [],
                    },
                    "ext": {
                        "packet_context": packet_context,
                    },
                    "created_at": now_ms,
                    "updated_at": now_ms,
                }
                group_unit_id_map[original_unit_id] = sa_id
                created_sa_by_id[sa_id] = sa_obj
                sa_items.append(sa_obj)
                group_sa_ids.append(sa_id)
                packet_sequence_index += 1

            for bundle in group.get("csa_bundles", []):
                anchor_id = group_unit_id_map.get(str(bundle.get("anchor_unit_id", "")), "")
                member_ids = [
                    group_unit_id_map.get(str(member_id), "")
                    for member_id in bundle.get("member_unit_ids", [])
                    if group_unit_id_map.get(str(member_id), "")
                ]
                member_ids = list(dict.fromkeys(member_ids))
                if not anchor_id or len(member_ids) < 2:
                    continue
                csa_id = next_id("csa_memfb")
                csa_obj = {
                    "id": csa_id,
                    "object_type": "csa",
                    "anchor_sa_id": anchor_id,
                    "member_sa_ids": member_ids,
                    "content": {
                        "display": created_sa_by_id.get(anchor_id, {}).get("content", {}).get("display", ""),
                        "raw": created_sa_by_id.get(anchor_id, {}).get("content", {}).get("raw", ""),
                    },
                    "bundle_summary": {
                        "member_count": len(member_ids),
                        "display_total_er": round(
                            sum(float(created_sa_by_id.get(member_id, {}).get("energy", {}).get("er", 0.0)) for member_id in member_ids),
                            6,
                        ),
                        "display_total_ev": round(
                            sum(float(created_sa_by_id.get(member_id, {}).get("energy", {}).get("ev", 0.0)) for member_id in member_ids),
                            6,
                        ),
                    },
                    "ext": {
                        "packet_context": {
                            "group_index": packet_group_index,
                            "source_group_index": source_group_index,
                            "origin_frame_id": origin_frame_id,
                            "source_type": "memory_feedback",
                            "sequence_index": int(
                                created_sa_by_id.get(anchor_id, {}).get("ext", {}).get("packet_context", {}).get("sequence_index", 0)
                            ),
                        }
                    },
                    "created_at": now_ms,
                    "updated_at": now_ms,
                }
                csa_items.append(csa_obj)
                group_csa_ids.append(csa_id)
                for member_id in member_ids:
                    sa_obj = created_sa_by_id.get(member_id)
                    if not sa_obj:
                        continue
                    if member_id != anchor_id and sa_obj.get("stimulus", {}).get("role") == "attribute":
                        sa_obj.setdefault("source", {}).setdefault("parent_ids", [])
                        sa_obj["source"]["parent_ids"] = [anchor_id]

            grouped_sequences.append(
                {
                    "group_index": packet_group_index,
                    "source_type": "memory_feedback",
                    "origin_frame_id": origin_frame_id,
                    "sa_ids": group_sa_ids,
                    "csa_ids": group_csa_ids,
                    "source_group_index": source_group_index,
                    "order_sensitive": bool(group.get("order_sensitive", False)),
                    "string_unit_kind": str(group.get("string_unit_kind", "") or ""),
                    "string_token_text": str(group.get("string_token_text", "") or ""),
                }
            )

        total_packet_er = round(sum(float(item.get("energy", {}).get("er", 0.0)) for item in sa_items), 6)
        total_packet_ev = round(sum(float(item.get("energy", {}).get("ev", 0.0)) for item in sa_items), 6)
        packet = {
            "id": packet_id,
            "object_type": "stimulus_packet",
            "sub_type": "memory_feedback_stimulus_packet",
            "schema_version": "1.1",
            "packet_type": "memory_feedback",
            "current_frame_id": packet_id,
            "echo_frame_ids": [],
            "sa_items": sa_items,
            "csa_items": csa_items,
            "echo_frames": [],
            "grouped_sa_sequences": grouped_sequences,
            "energy_summary": {
                "total_er": total_packet_er,
                "total_ev": total_packet_ev,
                "current_total_er": total_packet_er,
                "current_total_ev": total_packet_ev,
                "echo_total_er": 0.0,
                "echo_total_ev": 0.0,
                "combined_context_er": total_packet_er,
                "combined_context_ev": total_packet_ev,
                "ownership_level": "sa",
                "echo_merged_into_objects": False,
            },
            "trace_id": trace_id,
            "tick_id": tick_id or trace_id,
            "created_at": now_ms,
            "updated_at": now_ms,
            "source": {
                "module": "observatory",
                "interface": "memory_feedback",
                "origin": "episodic_memory_feedback",
                "origin_id": memory_id,
                "parent_ids": [memory_id],
            },
            "status": "active",
            "ext": {
                "memory_id": memory_id,
                "grouped_display_text": str(memory_material.get("grouped_display_text", "")),
            },
            "meta": {"confidence": 0.7, "field_registry_version": "1.1", "debug": {}, "ext": {}},
        }
        target_display_texts = [
            str(unit.get("content", {}).get("display", ""))
            for unit in sa_items
            if str(unit.get("content", {}).get("display", ""))
        ]
        return {"packet": packet, "target_display_texts": target_display_texts}

    @staticmethod
    def _allocate_weighted_values(*, keys: list[str], raw_weights: dict[str, float], total_value: float) -> dict[str, float]:
        ordered_keys = [str(key) for key in keys if str(key)]
        if not ordered_keys:
            return {}
        total_value = round(max(0.0, float(total_value)), 8)
        if total_value <= 0.0:
            return {key: 0.0 for key in ordered_keys}
        positive_weights = {
            key: max(0.0, float(raw_weights.get(key, 0.0)))
            for key in ordered_keys
        }
        total_weight = sum(positive_weights.values())
        if total_weight <= 0.0:
            positive_weights = {key: 1.0 for key in ordered_keys}
            total_weight = float(len(ordered_keys))
        allocations: dict[str, float] = {}
        remaining = total_value
        for index, key in enumerate(ordered_keys):
            if index == len(ordered_keys) - 1:
                allocations[key] = round(max(0.0, remaining), 8)
                continue
            value = round(total_value * positive_weights[key] / total_weight, 8)
            allocations[key] = value
            remaining = round(remaining - value, 8)
        return allocations

    def _project_structure_ids(self, structure_ids: list[str], trace_id: str, tick_id: str, *, er: float, ev: float, reason: str) -> list[dict]:
        projections = []
        for structure_id in structure_ids:
            projections.append({"structure_id": structure_id, "er": er, "ev": ev, "reason": reason})
        return self._project_runtime_structures(projections, trace_id, tick_id)

    def _apply_induction_targets(self, targets: list[dict], trace_id: str, tick_id: str) -> list[dict]:
        projections = []
        for target in targets:
            item = {
                "projection_kind": target.get("projection_kind", "structure"),
                "memory_id": target.get("memory_id", ""),
                "structure_id": target.get("target_structure_id", ""),
                "backing_structure_id": target.get("backing_structure_id", target.get("target_structure_id", "")),
                "display_text": target.get("target_display_text", ""),
                "er": 0.0,
                "ev": float(target.get("delta_ev", 0.0)),
                "reason": "induction_target",
            }
            effective = self._apply_projection_fatigue_to_item(item)
            if effective is not None:
                projections.append(effective)
        return self._project_runtime_structures(projections, trace_id, tick_id)

    def _apply_induction_source_consumptions(self, consumptions: list[dict], trace_id: str, tick_id: str) -> list[dict]:
        results = []
        for item in consumptions:
            structure_id = str(item.get("source_structure_id", ""))
            consumed_ev = max(0.0, float(item.get("consumed_ev", 0.0)))
            if not structure_id or consumed_ev <= 0.0:
                continue
            state_item = self.pool._store.get_by_ref(structure_id)
            if not state_item:
                continue
            available_ev = max(0.0, float(state_item.get("energy", {}).get("ev", 0.0)))
            delta_ev = -min(consumed_ev, available_ev)
            if delta_ev >= 0.0:
                continue
            result = self.pool.apply_energy_update(
                target_item_id=state_item.get("id", ""),
                delta_er=0.0,
                delta_ev=delta_ev,
                trace_id=f"{trace_id}_induction_source",
                tick_id=tick_id,
                reason="induction_source_ev_consumed",
            )
            results.append(
                {
                    "source_structure_id": structure_id,
                    "target_item_id": state_item.get("id", ""),
                    "delta_ev": round(delta_ev, 8),
                    "result": result.get("message", ""),
                }
            )
        return results

    def _collect_history_events(self, before_count: int, since_ms: int) -> list[dict]:
        recent_count = max(0, self.pool._history.size - before_count)
        events = self.pool._history.get_recent(recent_count)
        enriched = []
        for event in events:
            if event.get("timestamp_ms", 0) < since_ms:
                continue
            enriched.append(self._enrich_history_event(event))
        return enriched

    def _describe_stimulus_packet(self, packet: dict) -> dict:
        profile = self.cut_engine.build_sequence_profile_from_stimulus_packet(packet)
        unit_rows = self._describe_packet_units(packet, profile=profile)
        groups = self._describe_packet_groups(packet, profile=profile)
        flat_tokens = [str(unit.get("display", "")) for unit in unit_rows if str(unit.get("display", ""))]
        total_er = round(sum(float(unit.get("er", 0.0)) for unit in unit_rows), 8)
        total_ev = round(sum(float(unit.get("ev", 0.0)) for unit in unit_rows), 8)
        semantic_display_text = format_semantic_sequence_groups(list(profile.get("sequence_groups", [])), context="stimulus")
        return {
            "packet_id": packet.get("id", ""),
            "display_text": " / ".join(group.get("display_text", "") for group in groups if group.get("display_text", "")),
            "grouped_display_text": " / ".join(group.get("display_text", "") for group in groups if group.get("display_text", "")),
            "semantic_display_text": semantic_display_text,
            "semantic_grouped_display_text": semantic_display_text,
            "visible_text": profile.get("display_text", ""),
            "flat_tokens": flat_tokens,
            "sequence_groups": [
                {
                    **dict(group),
                    "units": [dict(unit) for unit in group.get("units", []) if isinstance(unit, dict)],
                    "csa_bundles": [dict(bundle) for bundle in group.get("csa_bundles", []) if isinstance(bundle, dict)],
                }
                for group in profile.get("sequence_groups", [])
                if isinstance(group, dict)
            ],
            "groups": groups,
            "units": unit_rows,
            "feature_units": unit_rows,
            "total_er": total_er,
            "total_ev": total_ev,
        }

    def _describe_packet_groups(self, packet: dict, *, profile: dict | None = None) -> list[dict]:
        profile = profile or self.cut_engine.build_sequence_profile_from_stimulus_packet(packet)
        groups = []
        packet_groups_by_index = {
            int(group.get("group_index", -1) or -1): dict(group)
            for group in (packet.get("grouped_sa_sequences", []) or [])
            if isinstance(group, dict)
        }
        for group in profile.get("sequence_groups", []):
            raw_group = packet_groups_by_index.get(int(group.get("group_index", -1) or -1), {})
            raw_ext = dict(raw_group.get("ext", {}) or {}) if isinstance(raw_group, dict) else {}
            units = sorted(group.get("units", []), key=lambda item: int(item.get("sequence_index", 0)))
            total_er = round(sum(float(item.get("er", 0.0)) for item in units), 8)
            total_ev = round(sum(float(item.get("ev", 0.0)) for item in units), 8)
            all_tokens = [str(unit.get("token", "")) for unit in units if str(unit.get("token", ""))]
            visible_tokens = [
                str(unit.get("token", ""))
                for unit in units
                if str(unit.get("token", "")) and (bool(unit.get("display_visible", False)) or bool(unit.get("is_placeholder", False)))
            ]
            bundle_displays = self._describe_group_bundles(group)
            cloned_group = {
                **dict(group),
                "units": [dict(unit) for unit in group.get("units", []) if isinstance(unit, dict)],
                "csa_bundles": [dict(bundle) for bundle in group.get("csa_bundles", []) if isinstance(bundle, dict)],
            }
            groups.append(
                {
                    "group_index": group.get("group_index", 0),
                    "source_type": group.get("source_type", ""),
                    "origin_frame_id": group.get("origin_frame_id", ""),
                    "contains_internal_group": bool(raw_ext.get("contains_internal_group", False)),
                    "internal_merge_mode": str(raw_ext.get("internal_merge_mode", "") or ""),
                    "internal_string_group_count": len((raw_ext.get("internal_string_groups", []) or [])),
                    "display_text": self._format_group_display(group),
                    "semantic_display_text": format_semantic_group_display(cloned_group, context="stimulus"),
                    "token_text": " / ".join(all_tokens),
                    "visible_text": "".join(visible_tokens),
                    "tokens": all_tokens,
                    "visible_tokens": visible_tokens,
                    "sa_count": len(units),
                    "csa_count": len(bundle_displays),
                    "unit_count": len(units),
                    "csa_bundles": bundle_displays,
                    "csa_bundle_defs": [dict(bundle) for bundle in group.get("csa_bundles", []) if isinstance(bundle, dict)],
                    "units": [dict(unit) for unit in units if isinstance(unit, dict)],
                    "sequence_groups": [cloned_group],
                    "total_er": total_er,
                    "total_ev": total_ev,
                    "total_energy": round(total_er + total_ev, 8),
                }
            )
        return groups

    def _describe_packet_units(self, packet: dict, *, profile: dict | None = None) -> list[dict]:
        profile = profile or self.cut_engine.build_sequence_profile_from_stimulus_packet(packet)
        rows = []
        for group in profile.get("sequence_groups", []):
            bundle_by_unit = self._map_group_unit_bundles(group)
            for unit in sorted(group.get("units", []), key=lambda item: int(item.get("sequence_index", 0))):
                rows.append(
                    {
                        "id": unit.get("unit_id", ""),
                        "display": unit.get("token", ""),
                        "role": unit.get("unit_role", ""),
                        "unit_kind": unit.get("object_type", "sa"),
                        "source_type": unit.get("source_type", "current"),
                        "group_index": unit.get("group_index", group.get("group_index", 0)),
                        "sequence_index": unit.get("sequence_index", 0),
                        "attribute_name": unit.get("attribute_name", ""),
                        "attribute_value": unit.get("attribute_value"),
                        "bundle_display": bundle_by_unit.get(str(unit.get("unit_id", "")), ""),
                        "display_visible": bool(unit.get("display_visible", False)),
                        "er": round(float(unit.get("er", 0.0)), 8),
                        "ev": round(float(unit.get("ev", 0.0)), 8),
                        "total_energy": round(float(unit.get("total_energy", 0.0)), 8),
                        "fatigue": round(float(unit.get("fatigue", 0.0)), 8),
                        "suppression_ratio": round(float(unit.get("suppression_ratio", 0.0)), 6),
                        "er_before_fatigue": round(float(unit.get("er_before_fatigue", unit.get("er", 0.0))), 8),
                        "er_after_fatigue": round(float(unit.get("er_after_fatigue", unit.get("er", 0.0))), 8),
                        "window_count": int(unit.get("window_count", 0) or 0),
                        "threshold_count": int(unit.get("threshold_count", 0) or 0),
                        "window_rounds": int(unit.get("window_rounds", 0) or 0),
                        "sensor_round": int(unit.get("sensor_round", 0) or 0),
                        "sensor_fatigue": dict(unit.get("sensor_fatigue", {}) or {}),
                    }
                )
        return rows

    def _describe_feature_units(self, packet: dict) -> list[dict]:
        return self._describe_packet_units(packet)

    def _describe_group_bundles(self, group: dict) -> list[str]:
        units_by_id = {
            str(unit.get("unit_id", "")): unit
            for unit in group.get("units", [])
            if str(unit.get("unit_id", ""))
        }
        displays = []
        for bundle in group.get("csa_bundles", []):
            member_tokens = [
                str(units_by_id.get(str(member_id), {}).get("token", ""))
                for member_id in bundle.get("member_unit_ids", [])
                if str(units_by_id.get(str(member_id), {}).get("token", ""))
            ]
            if member_tokens:
                displays.append(f"({' + '.join(member_tokens)})")
            else:
                displays.append(str(bundle.get("bundle_signature", "")))
        return displays

    def _format_group_display(self, group: dict) -> str:
        ext = group.get("ext", {}) if isinstance(group.get("ext", {}), dict) else {}
        string_groups = ext.get("string_groups", []) if isinstance(ext.get("string_groups", []), list) else []
        if string_groups:
            rendered_parts = []
            for sg in string_groups:
                if not isinstance(sg, dict):
                    continue
                part_text = str(sg.get("string_token_text", "") or "")
                if not part_text:
                    part_text = format_semantic_group_display(dict(sg), context="stimulus") or format_group_display(sg.get("units", []), sg.get("csa_bundles", []))
                part_text = str(part_text or "").strip()
                if part_text.startswith("{") and part_text.endswith("}"):
                    part_text = part_text[1:-1]
                if part_text:
                    rendered_parts.append(part_text)
            if rendered_parts:
                return "{" + " / ".join(rendered_parts) + "}"
        return format_group_display(group.get("units", []), group.get("csa_bundles", []))

    def _map_group_unit_bundles(self, group: dict) -> dict[str, str]:
        bundle_map: dict[str, str] = {}
        bundle_displays = self._describe_group_bundles(group)
        for bundle, display in zip(group.get("csa_bundles", []), bundle_displays):
            for member_id in bundle.get("member_unit_ids", []):
                member_text = str(member_id)
                if member_text:
                    bundle_map[member_text] = display
        return bundle_map

    def _export_report(self, trace_id: str, report: dict) -> dict:
        json_path = self.output_dir / f"{trace_id}.json"
        html_path = self.output_dir / f"{trace_id}.html"
        latest_json = self.output_dir / "latest.json"
        latest_html = self.output_dir / "latest.html"
        if self._config.get("export_json", True):
            payload = json.dumps(report, ensure_ascii=False, indent=2)
            json_path.write_text(payload, encoding="utf-8")
            latest_json.write_text(payload, encoding="utf-8")
        if self._config.get("export_html", True):
            export_cycle_html(report, html_path)
            export_cycle_html(report, latest_html)
        return {
            "json_path": str(json_path),
            "html_path": str(html_path),
            "latest_json_path": str(latest_json),
            "latest_html_path": str(latest_html),
        }

    def _silence_jieba_logs(self) -> None:
        try:
            import jieba

            jieba.setLogLevel(60)
        except Exception:
            pass

    def _summarize_state_snapshot(self, snapshot: dict) -> dict:
        items = list(snapshot.get("top_items", []))
        total_er = sum(float(item.get("er", 0.0)) for item in items)
        total_ev = sum(float(item.get("ev", 0.0)) for item in items)
        total_cp = sum(float(item.get("cp_abs", 0.0)) for item in items)
        energy_by_type: dict[str, dict[str, float]] = {}
        for item in items:
            ref_type = item.get("ref_object_type", "unknown")
            bucket = energy_by_type.setdefault(ref_type, {"count": 0, "total_er": 0.0, "total_ev": 0.0, "total_cp": 0.0})
            bucket["count"] += 1
            bucket["total_er"] += float(item.get("er", 0.0))
            bucket["total_ev"] += float(item.get("ev", 0.0))
            bucket["total_cp"] += float(item.get("cp_abs", 0.0))
        for bucket in energy_by_type.values():
            bucket["total_er"] = round(bucket["total_er"], 8)
            bucket["total_ev"] = round(bucket["total_ev"], 8)
            bucket["total_cp"] = round(bucket["total_cp"], 8)
        return {
            "total_er": round(total_er, 8),
            "total_ev": round(total_ev, 8),
            "total_cp": round(total_cp, 8),
            "energy_by_type": energy_by_type,
            "top_er_items": sorted(items, key=lambda item: item.get("er", 0.0), reverse=True)[:8],
            "top_ev_items": sorted(items, key=lambda item: item.get("ev", 0.0), reverse=True)[:8],
            "top_cp_items": sorted(items, key=lambda item: item.get("cp_abs", 0.0), reverse=True)[:8],
        }

    def _enrich_history_event(self, event: dict) -> dict:
        enriched = dict(event)
        target_item = self.pool._store.get(event.get("target_item_id", ""))
        if target_item:
            ref_snapshot = target_item.get("ref_snapshot", {})
            enriched["target_display"] = ref_snapshot.get("content_display", event.get("target_item_id", ""))
            enriched["target_detail"] = ref_snapshot.get("content_display_detail", "")
            enriched["target_ref_object_id"] = target_item.get("ref_object_id", "")
            enriched["target_ref_object_type"] = target_item.get("ref_object_type", "")
        else:
            enriched["target_display"] = event.get("target_item_id", "")
            enriched["target_detail"] = ""
            enriched["target_ref_object_id"] = ""
            enriched["target_ref_object_type"] = ""
        return enriched






















