# -*- coding: utf-8 -*-
"""
Experiment Metrics Extraction
=============================

Goal:
- turn a full `report` dict (from ObservatoryApp.run_cycle) into a compact,
  per-tick metrics record for plotting and paper evidence.

Principles:
- never crash the batch runner due to a missing field
- keep output stable and auditable (flat keys; mostly numbers)
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


def _as_int(v: Any, default: int = 0) -> int:
    try:
        return int(v)
    except Exception:
        return int(default)


def _as_float(v: Any, default: float = 0.0) -> float:
    try:
        return float(v)
    except Exception:
        return float(default)


def _as_str(v: Any, default: str = "") -> str:
    try:
        s = str(v)
    except Exception:
        return default
    return s


def _as_list(v: Any) -> list:
    return list(v) if isinstance(v, list) else []


def _as_dict(v: Any) -> dict:
    return dict(v) if isinstance(v, dict) else {}


CORE_CFS_KINDS: tuple[str, ...] = (
    "dissonance",
    "pressure",
    "pressure_unverified",
    "expectation",
    "surprise",
    "correct_event",
    "grasp",
    "complexity",
    "repetition",
)


@dataclass(frozen=True)
class MetricsSchema:
    version: str = "v0"


def extract_tick_metrics(*, report: dict[str, Any], dataset_tick: dict[str, Any] | None = None) -> dict[str, Any]:
    """Extract a compact metrics record from a cycle report.

    `dataset_tick` is optional but recommended for slicing/plotting.
    """

    dt = _as_dict(dataset_tick)

    trace_id = _as_str(report.get("trace_id", ""))
    tick_id = _as_str(report.get("tick_id", ""))

    sensor = _as_dict(report.get("sensor"))
    input_text = _as_str(dt.get("input_text", "")) or _as_str(sensor.get("input_text", ""))
    input_is_empty = bool(dt.get("input_is_empty", False)) or (not input_text)

    final_state = _as_dict(report.get("final_state"))
    final_snapshot = _as_dict(final_state.get("state_snapshot"))
    snapshot_summary = _as_dict(final_snapshot.get("summary"))
    energy_summary = _as_dict(final_state.get("state_energy_summary"))
    hdb_snapshot = _as_dict(final_state.get("hdb_snapshot"))
    hdb_summary = _as_dict(hdb_snapshot.get("summary"))

    attention = _as_dict(report.get("attention"))

    structure_level = _as_dict(report.get("structure_level"))
    structure_result = _as_dict(structure_level.get("result"))
    stimulus_level = _as_dict(report.get("stimulus_level"))
    stimulus_result = _as_dict(stimulus_level.get("result"))

    induction = _as_dict(report.get("induction"))
    induction_result = _as_dict(induction.get("result"))

    memory_activation = _as_dict(report.get("memory_activation"))
    map_snapshot = _as_dict(memory_activation.get("snapshot"))
    map_summary = _as_dict(map_snapshot.get("summary"))
    map_apply = _as_dict(memory_activation.get("apply_result"))
    map_feedback = _as_dict(memory_activation.get("feedback_result"))

    cfs = _as_dict(report.get("cognitive_feeling"))
    cfs_signals = _as_list(cfs.get("cfs_signals"))

    # Per-kind max strength
    cfs_max_by_kind: dict[str, float] = {k: 0.0 for k in CORE_CFS_KINDS}
    cfs_count_by_kind: dict[str, int] = {k: 0 for k in CORE_CFS_KINDS}
    cfs_total_strength = 0.0
    for sig in cfs_signals:
        if not isinstance(sig, dict):
            continue
        kind = _as_str(sig.get("kind", "")).strip()
        strength = _as_float(sig.get("strength", 0.0))
        cfs_total_strength += max(0.0, strength)
        if kind in cfs_max_by_kind:
            cfs_count_by_kind[kind] = int(cfs_count_by_kind.get(kind, 0)) + 1
            if strength > float(cfs_max_by_kind.get(kind, 0.0) or 0.0):
                cfs_max_by_kind[kind] = float(strength)

    emotion = _as_dict(report.get("emotion"))
    nt_after = _as_dict(emotion.get("nt_state_after"))
    # Normalize channel keys (keep original for audit; provide stable fields for plots)
    nt = {
        "OXY": _as_float(nt_after.get("OXY", nt_after.get("催产素", 0.0))),
        "DA": _as_float(nt_after.get("DA", nt_after.get("多巴胺", 0.0))),
        "END": _as_float(nt_after.get("END", nt_after.get("内啡肽", 0.0))),
        "COR": _as_float(nt_after.get("COR", nt_after.get("皮质醇", 0.0))),
        "ADR": _as_float(nt_after.get("ADR", nt_after.get("肾上腺素", 0.0))),
        "SER": _as_float(nt_after.get("SER", nt_after.get("血清素", 0.0))),
    }

    action = _as_dict(report.get("action"))
    executed_actions = _as_list(action.get("executed_actions"))
    executed_kind_counts: dict[str, int] = {}
    for row in executed_actions:
        if not isinstance(row, dict):
            continue
        kind = _as_str(row.get("action_kind", "")).strip() or _as_str(row.get("kind", "")).strip()
        if not kind:
            continue
        executed_kind_counts[kind] = int(executed_kind_counts.get(kind, 0)) + 1

    # For plotting in a paper UI, keep the record flat.
    record: dict[str, Any] = {
        "schema_version": MetricsSchema().version,
        # identifiers
        "tick_index": _as_int(dt.get("tick_index", report.get("tick_counter", 0))),
        "trace_id": trace_id,
        "tick_id": tick_id,
        "started_at_ms": _as_int(report.get("started_at", 0)),
        "finished_at_ms": _as_int(report.get("finished_at", 0)),
        # dataset slicing fields (optional)
        "dataset_id": _as_str(dt.get("dataset_id", "")),
        "episode_id": _as_str(dt.get("episode_id", "")),
        "episode_repeat_index": _as_int(dt.get("episode_repeat_index", 0)),
        "tick_in_episode_index": _as_int(dt.get("tick_in_episode_index", 0)),
        "tags": _as_list(dt.get("tags", [])),
        # input
        "input_is_empty": bool(input_is_empty),
        "input_len": len(input_text or ""),
        # pool summary
        "pool_active_item_count": _as_int(snapshot_summary.get("active_item_count", 0)),
        "pool_high_cp_item_count": _as_int(snapshot_summary.get("high_cp_item_count", 0)),
        "pool_total_er": _as_float(energy_summary.get("total_er", 0.0)),
        "pool_total_ev": _as_float(energy_summary.get("total_ev", 0.0)),
        "pool_total_cp": _as_float(energy_summary.get("total_cp", 0.0)),
        # attention summary
        "cam_item_count": _as_int(attention.get("cam_item_count", attention.get("cam_count", 0))),
        "attention_memory_item_count": _as_int(attention.get("memory_item_count", 0)),
        "attention_consumed_total_energy": _as_float(attention.get("consumed_total_energy", 0.0)),
        # retrieval rounds
        "structure_round_count": _as_int(structure_result.get("round_count", 0)),
        "stimulus_round_count": _as_int(stimulus_result.get("round_count", 0)),
        # induction + MAP
        "induction_total_delta_ev": _as_float(induction_result.get("total_delta_ev", 0.0)),
        "map_count": _as_int(map_summary.get("count", hdb_summary.get("memory_activation_count", 0))),
        "map_total_er": _as_float(map_summary.get("total_er", hdb_summary.get("memory_activation_total_er", 0.0))),
        "map_total_ev": _as_float(map_summary.get("total_ev", hdb_summary.get("memory_activation_total_ev", 0.0))),
        "map_apply_count": _as_int(map_apply.get("applied_count", 0)),
        "map_feedback_count": _as_int(map_feedback.get("applied_count", 0)),
        "map_feedback_total_ev": _as_float(map_feedback.get("total_feedback_ev", 0.0)),
        # hdb counts
        "hdb_structure_count": _as_int(hdb_summary.get("structure_count", 0)),
        "hdb_group_count": _as_int(hdb_summary.get("group_count", 0)),
        "hdb_episodic_count": _as_int(hdb_summary.get("episodic_count", 0)),
        # cfs summary
        "cfs_signal_count": len([x for x in cfs_signals if isinstance(x, dict)]),
        "cfs_total_strength": round(float(cfs_total_strength), 8),
        # emotion / NT
        "nt_OXY": nt["OXY"],
        "nt_DA": nt["DA"],
        "nt_END": nt["END"],
        "nt_COR": nt["COR"],
        "nt_ADR": nt["ADR"],
        "nt_SER": nt["SER"],
        # action
        "action_executed_count": len([x for x in executed_actions if isinstance(x, dict)]),
        "action_executed_attention_focus": int(executed_kind_counts.get("attention_focus", 0)),
        "action_executed_recall": int(executed_kind_counts.get("recall", 0)),
        "action_executed_diverge_mode": int(executed_kind_counts.get("attention_diverge_mode", 0)),
        "action_executed_focus_mode": int(executed_kind_counts.get("attention_focus_mode", 0)),
    }

    # Flatten core CFS kinds for charting
    for kind in CORE_CFS_KINDS:
        record[f"cfs_{kind}_max"] = round(float(cfs_max_by_kind.get(kind, 0.0) or 0.0), 8)
        record[f"cfs_{kind}_count"] = int(cfs_count_by_kind.get(kind, 0) or 0)

    # Optional labels pass-through (if present) for later specialized plots.
    labels = dt.get("labels")
    if isinstance(labels, dict) and labels:
        record["labels"] = dict(labels)

    return record

