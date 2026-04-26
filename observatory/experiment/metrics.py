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

# Live-state CFS attributes (bound attributes in StatePool).
# These are not "one-tick peaks"; they represent maintained runtime state with decay.
CORE_CFS_BOUND_ATTRS: tuple[str, ...] = (
    "cfs_dissonance",
    "cfs_correctness",
    "cfs_pressure",
    "cfs_expectation",
    "cfs_grasp",
    "cfs_complexity",
    "cfs_surprise",
    "cfs_correct_event",
    "cfs_repetition",
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
    input_queue = _as_dict(report.get("input_queue"))
    input_queue_tick_text = _as_str(input_queue.get("tick_text", ""))
    input_is_empty = bool(dt.get("input_is_empty", False)) or (not input_text)

    final_state = _as_dict(report.get("final_state"))
    final_snapshot = _as_dict(final_state.get("state_snapshot"))
    snapshot_summary = _as_dict(final_snapshot.get("summary"))
    energy_summary = _as_dict(final_state.get("state_energy_summary"))
    hdb_snapshot = _as_dict(final_state.get("hdb_snapshot"))
    hdb_summary = _as_dict(hdb_snapshot.get("summary"))

    attention = _as_dict(report.get("attention"))
    cam_snapshot_summary = _as_dict(attention.get("cam_snapshot_summary"))
    memory_snapshot_summary = _as_dict(attention.get("memory_snapshot_summary"))
    maintenance = _as_dict(report.get("maintenance"))
    maintenance_summary = _as_dict(maintenance.get("summary"))
    maintenance_before = _as_dict(maintenance.get("before_summary"))
    maintenance_after = _as_dict(maintenance.get("after_summary"))

    structure_level = _as_dict(report.get("structure_level"))
    structure_result = _as_dict(structure_level.get("result"))
    stimulus_level = _as_dict(report.get("stimulus_level"))
    stimulus_result = _as_dict(stimulus_level.get("result"))
    internal_stimulus = _as_dict(report.get("internal_stimulus"))
    merged_stimulus = _as_dict(report.get("merged_stimulus"))
    cache_neutralization = _as_dict(report.get("cache_neutralization"))
    cache_input_pkt = _as_dict(cache_neutralization.get("input_packet"))
    cache_residual_pkt = _as_dict(cache_neutralization.get("residual_packet"))
    pool_apply = _as_dict(report.get("pool_apply"))
    pool_apply_result = _as_dict(pool_apply.get("apply_result"))
    landed_pkt = _as_dict(pool_apply.get("landed_packet"))
    internal_resolution = _as_dict(structure_result.get("internal_resolution"))

    induction = _as_dict(report.get("induction"))
    induction_result = _as_dict(induction.get("result"))

    memory_activation = _as_dict(report.get("memory_activation"))
    map_snapshot = _as_dict(memory_activation.get("snapshot"))
    map_summary = _as_dict(map_snapshot.get("summary"))
    map_apply = _as_dict(memory_activation.get("apply_result"))
    map_feedback = _as_dict(memory_activation.get("feedback_result"))

    cfs = _as_dict(report.get("cognitive_feeling"))
    cfs_signals = _as_list(cfs.get("cfs_signals"))

    cognitive_stitching = _as_dict(report.get("cognitive_stitching"))
    cs_narrative = _as_list(cognitive_stitching.get("narrative_top_items"))
    cs_top_grasp = 0.0
    cs_top_total_energy = 0.0
    if cs_narrative and isinstance(cs_narrative[0], dict):
        cs_top_grasp = _as_float(cs_narrative[0].get("event_grasp", 0.0))
        cs_top_total_energy = _as_float(cs_narrative[0].get("total_energy", 0.0))

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
    rwd_pun_snapshot = _as_dict(emotion.get("rwd_pun_snapshot"))
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
    action_nodes = _as_list(action.get("nodes"))
    attempted_kind_counts: dict[str, int] = {}
    executed_kind_counts: dict[str, int] = {}
    scheduled_kind_counts: dict[str, int] = {}
    action_attempted_count = 0
    action_executed_count = 0
    for row in executed_actions:
        if not isinstance(row, dict):
            continue
        kind = _as_str(row.get("action_kind", "")).strip() or _as_str(row.get("kind", "")).strip()
        if not kind:
            continue
        attempted = bool(row.get("attempted", True))
        if attempted:
            action_attempted_count += 1
            attempted_kind_counts[kind] = int(attempted_kind_counts.get(kind, 0)) + 1
        else:
            scheduled_kind_counts[kind] = int(scheduled_kind_counts.get(kind, 0)) + 1
        if bool(row.get("success", False)):
            action_executed_count += 1
            executed_kind_counts[kind] = int(executed_kind_counts.get(kind, 0)) + 1
    action_drive_vals: list[float] = []
    action_drive_active_count = 0
    for row in action_nodes:
        if not isinstance(row, dict):
            continue
        drive = _as_float(row.get("drive", 0.0))
        action_drive_vals.append(drive)
        if drive > 0.05:
            action_drive_active_count += 1

    timing = _as_dict(report.get("timing"))
    steps_ms = _as_dict(timing.get("steps_ms"))

    time_sensor = _as_dict(report.get("time_sensor"))
    ts_bucket_updates = _as_list(time_sensor.get("bucket_updates"))
    ts_attr_bindings = _as_list(time_sensor.get("attribute_bindings"))
    ts_delayed_tasks = _as_dict(time_sensor.get("delayed_tasks"))
    ts_delayed_registered = _as_dict(ts_delayed_tasks.get("registered"))
    ts_bucket_energy_vals: list[float] = []
    for row in ts_bucket_updates:
        if not isinstance(row, dict):
            continue
        ts_bucket_energy_vals.append(_as_float(row.get("assigned_energy", 0.0)))
    ts_bucket_energy_sum = float(sum(ts_bucket_energy_vals)) if ts_bucket_energy_vals else 0.0
    ts_bucket_energy_max = float(max(ts_bucket_energy_vals)) if ts_bucket_energy_vals else 0.0
    external_sa_count = _as_int(
        sensor.get(
            "sa_count",
            _as_int(sensor.get("feature_sa_count", 0)) + _as_int(sensor.get("attribute_sa_count", 0)),
        )
    )
    internal_sa_count = _as_int(
        internal_stimulus.get("sa_count", internal_stimulus.get("unit_count", internal_resolution.get("selected_unit_count", 0)))
    )
    internal_csa_count = _as_int(internal_stimulus.get("csa_count", 0))
    internal_flat_token_count = _as_int(internal_stimulus.get("flat_token_count", len(_as_list(internal_stimulus.get("flat_tokens", []))) or internal_sa_count))
    internal_to_external_sa_ratio = round(float(internal_sa_count) / max(1.0, float(max(1, external_sa_count))), 8)
    internal_resolution_structure_count_selected = _as_int(
        internal_resolution.get("structure_count_selected", internal_resolution.get("structure_count", 0))
    )
    internal_resolution_structure_count_total = _as_int(
        internal_resolution.get("structure_count_total", internal_resolution_structure_count_selected)
    )
    internal_resolution_structure_count_dropped = _as_int(
        internal_resolution.get(
            "structure_count_dropped",
            max(0, internal_resolution_structure_count_total - internal_resolution_structure_count_selected),
        )
    )

    # For plotting in a paper UI, keep the record flat.
    #
    # tick_index semantics (重要):
    # - tick_index is the *executed* tick index (monotonic, 0-based), derived from report.tick_counter when available.
    #   This keeps charts stable even when synthetic ticks (expectation feedback) are inserted between source ticks.
    # - dataset_tick_index preserves the original expanded-dataset index (dt.tick_index) for slicing/audit.
    #
    # Rationale:
    # - If we mix "dataset tick_index" for source ticks and "executed tick_counter" for synthetic ticks, X may go
    #   backwards (because executed includes synthetic steps but dataset index does not), producing long diagonal lines.
    tick_counter = _as_int(report.get("tick_counter", 0))
    executed_tick_index = max(0, tick_counter - 1) if tick_counter > 0 else _as_int(dt.get("tick_index", 0))
    record: dict[str, Any] = {
        "schema_version": MetricsSchema().version,
        # identifiers
        "tick_index": int(executed_tick_index),
        "trace_id": trace_id,
        "tick_id": tick_id,
        "started_at_ms": _as_int(report.get("started_at", 0)),
        "finished_at_ms": _as_int(report.get("finished_at", 0)),
        # dataset slicing fields (optional)
        "dataset_tick_index": _as_int(dt.get("tick_index", -1), -1),
        "dataset_id": _as_str(dt.get("dataset_id", "")),
        "episode_id": _as_str(dt.get("episode_id", "")),
        "episode_repeat_index": _as_int(dt.get("episode_repeat_index", 0)),
        "tick_in_episode_index": _as_int(dt.get("tick_in_episode_index", 0)),
        "tags": _as_list(dt.get("tags", [])),
        "tick_source": _as_str(dt.get("tick_source", "dataset")) or "dataset",
        "synthetic_tick": bool(dt.get("synthetic_tick", False)),
        "expectation_contract_id": _as_str(dt.get("expectation_contract_id", "")),
        "expectation_contract_outcome": _as_str(dt.get("expectation_contract_outcome", "")),
        "source_dataset_tick_index": _as_int(dt.get("source_dataset_tick_index", -1), -1),
        # input
        "input_is_empty": bool(input_is_empty),
        "input_len": len(input_text or ""),
        "input_text_preview": (input_text or "")[:80],
        "input_queue_tick_text_preview": (input_queue_tick_text or "")[:80],
        # text sensor echo diagnostics (important for long-run budget)
        "sensor_echo_pool_size": _as_int(sensor.get("echo_pool_size", 0)),
        "sensor_echo_current_round": _as_int(sensor.get("echo_current_round", 0)),
        "sensor_feature_sa_count": _as_int(sensor.get("feature_sa_count", 0)),
        "sensor_attribute_sa_count": _as_int(sensor.get("attribute_sa_count", 0)),
        "sensor_csa_bundle_count": _as_int(sensor.get("csa_bundle_count", 0)),
        "sensor_echo_frames_used_count": len(_as_list(sensor.get("echo_frames_used"))),
        # stimulus packet sizes (external + internal merge)
        "external_sa_count": external_sa_count,
        "internal_sa_count": internal_sa_count,
        "internal_csa_count": internal_csa_count,
        "internal_flat_token_count": internal_flat_token_count,
        "internal_total_er": _as_float(internal_stimulus.get("total_er", 0.0)),
        "internal_total_ev": _as_float(internal_stimulus.get("total_ev", 0.0)),
        "internal_to_external_sa_ratio": internal_to_external_sa_ratio,
        "internal_minus_external_sa_count": int(internal_sa_count - external_sa_count),
        "merged_flat_token_count": _as_int(merged_stimulus.get("flat_token_count", len(_as_list(merged_stimulus.get("flat_tokens", []))))),
        "cache_input_flat_token_count": _as_int(cache_input_pkt.get("flat_token_count", len(_as_list(cache_input_pkt.get("flat_tokens", []))))),
        "cache_residual_flat_token_count": _as_int(cache_residual_pkt.get("flat_token_count", len(_as_list(cache_residual_pkt.get("flat_tokens", []))))),
        "landed_flat_token_count": _as_int(landed_pkt.get("flat_token_count", len(_as_list(landed_pkt.get("flat_tokens", []))))),
        "cache_priority_consumed_er": _as_float(cache_neutralization.get("priority_summary", {}).get("consumed_er", 0.0) if isinstance(cache_neutralization.get("priority_summary"), dict) else 0.0),
        "cache_priority_consumed_ev": _as_float(cache_neutralization.get("priority_summary", {}).get("consumed_ev", 0.0) if isinstance(cache_neutralization.get("priority_summary"), dict) else 0.0),
        # structure-level internal resolution (DARL + PARS) summary
        "internal_fragment_count": len(_as_list(structure_result.get("internal_stimulus_fragments"))),
        "internal_resolution_structure_count_total": internal_resolution_structure_count_total,
        "internal_resolution_structure_count_selected": internal_resolution_structure_count_selected,
        "internal_resolution_structure_count_dropped": internal_resolution_structure_count_dropped,
        "internal_resolution_max_structures_per_tick": _as_int(internal_resolution.get("max_structures_per_tick", 0)),
        "internal_resolution_detail_budget": _as_int(internal_resolution.get("detail_budget", 0)),
        "internal_resolution_raw_unit_count": _as_int(internal_resolution.get("raw_unit_count", 0)),
        "internal_resolution_raw_unit_count_total_candidates": _as_int(internal_resolution.get("raw_unit_count_total_candidates", internal_resolution.get("raw_unit_count", 0))),
        "internal_resolution_selected_unit_count": _as_int(internal_resolution.get("selected_unit_count", 0)),
        "internal_resolution_rich_candidate_count": _as_int(internal_resolution.get("rich_candidate_count", 0)),
        "internal_resolution_rich_selected_count": _as_int(internal_resolution.get("rich_selected_count", 0)),
        # pool summary
        "pool_active_item_count": _as_int(snapshot_summary.get("active_item_count", 0)),
        "pool_high_cp_item_count": _as_int(snapshot_summary.get("high_cp_item_count", 0)),
        "pool_total_er": _as_float(energy_summary.get("total_er", 0.0)),
        "pool_total_ev": _as_float(energy_summary.get("total_ev", 0.0)),
        "pool_total_cp": _as_float(energy_summary.get("total_cp", 0.0)),
        # attention summary
        # cam_item_count: use the canonical AttentionFilter report fields
        # (legacy compat: tolerate older keys if present).
        "cam_item_count": _as_int(
            cam_snapshot_summary.get(
                "active_item_count",
                attention.get(
                    "cam_item_count",
                    attention.get(
                        "cam_count",
                        attention.get("top_item_count", attention.get("memory_item_count", len(_as_list(attention.get("top_items"))))),
                    ),
                ),
            )
        ),
        "attention_memory_item_count": _as_int(attention.get("memory_item_count", 0)),
        "attention_consumed_total_energy": _as_float(attention.get("consumed_total_energy", 0.0)),
        "attention_cam_item_cap": _as_int(attention.get("cam_item_cap", attention.get("top_n", 0))),
        "attention_state_pool_candidate_count": _as_int(attention.get("state_pool_candidate_count", 0)),
        "attention_skipped_memory_item_count": _as_int(attention.get("skipped_memory_item_count", 0)),
        # retrieval rounds
        "structure_round_count": _as_int(structure_result.get("round_count", 0)),
        "stimulus_round_count": _as_int(stimulus_result.get("round_count", 0)),
        "stimulus_new_structure_count": _as_int(stimulus_result.get("new_structure_count", 0)),
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
        # maintenance / pool landing
        "maintenance_event_count": len(_as_list(maintenance.get("events"))),
        "maintenance_before_active_item_count": _as_int(maintenance_before.get("active_item_count", 0)),
        "maintenance_after_active_item_count": _as_int(maintenance_after.get("active_item_count", 0)),
        "maintenance_before_high_cp_item_count": _as_int(maintenance_before.get("high_cp_item_count", 0)),
        "maintenance_after_high_cp_item_count": _as_int(maintenance_after.get("high_cp_item_count", 0)),
        "maintenance_delta_active_item_count": _as_int(maintenance_after.get("active_item_count", 0)) - _as_int(maintenance_before.get("active_item_count", 0)),
        "maintenance_delta_high_cp_item_count": _as_int(maintenance_after.get("high_cp_item_count", 0)) - _as_int(maintenance_before.get("high_cp_item_count", 0)),
        "pool_apply_new_item_count": _as_int(pool_apply_result.get("new_item_count", 0)),
        "pool_apply_updated_item_count": _as_int(pool_apply_result.get("updated_item_count", 0)),
        "pool_apply_merged_item_count": _as_int(pool_apply_result.get("merged_item_count", 0)),
        "pool_apply_total_delta_er": _as_float(pool_apply_result.get("state_delta_summary", {}).get("total_delta_er", 0.0) if isinstance(pool_apply_result.get("state_delta_summary"), dict) else 0.0),
        "pool_apply_total_delta_ev": _as_float(pool_apply_result.get("state_delta_summary", {}).get("total_delta_ev", 0.0) if isinstance(pool_apply_result.get("state_delta_summary"), dict) else 0.0),
        "pool_apply_total_delta_cp": _as_float(pool_apply_result.get("state_delta_summary", {}).get("total_delta_cp", 0.0) if isinstance(pool_apply_result.get("state_delta_summary"), dict) else 0.0),
        # cognitive stitching (CS)
        "cs_enabled": int(bool(cognitive_stitching.get("enabled", False))),
        "cs_seed_structure_count": _as_int(cognitive_stitching.get("seed_structure_count", 0)),
        "cs_seed_event_count": _as_int(cognitive_stitching.get("seed_event_count", 0)),
        "cs_candidate_count": _as_int(cognitive_stitching.get("candidate_count", 0)),
        "cs_action_count": _as_int(cognitive_stitching.get("action_count", 0)),
        "cs_created_count": _as_int(cognitive_stitching.get("created_count", 0)),
        "cs_extended_count": _as_int(cognitive_stitching.get("extended_count", 0)),
        "cs_merged_count": _as_int(cognitive_stitching.get("merged_count", 0)),
        "cs_reinforced_count": _as_int(cognitive_stitching.get("reinforced_count", 0)),
        "cs_narrative_top_grasp": round(float(cs_top_grasp), 8),
        "cs_narrative_top_total_energy": round(float(cs_top_total_energy), 8),
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
        # reward / punish (global snapshot used by EMgr)
        "rwd_pun_rwd": _as_float(rwd_pun_snapshot.get("rwd", 0.0)),
        "rwd_pun_pun": _as_float(rwd_pun_snapshot.get("pun", 0.0)),
        # action
        "action_attempted_count": int(action_attempted_count),
        "action_attempted_attention_focus": int(attempted_kind_counts.get("attention_focus", 0)),
        "action_attempted_recall": int(attempted_kind_counts.get("recall", 0)),
        "action_attempted_diverge_mode": int(attempted_kind_counts.get("attention_diverge_mode", 0)),
        "action_attempted_focus_mode": int(attempted_kind_counts.get("attention_focus_mode", 0)),
        "action_executed_count": int(action_executed_count),
        "action_executed_attention_focus": int(executed_kind_counts.get("attention_focus", 0)),
        "action_executed_recall": int(executed_kind_counts.get("recall", 0)),
        "action_executed_diverge_mode": int(executed_kind_counts.get("attention_diverge_mode", 0)),
        "action_executed_focus_mode": int(executed_kind_counts.get("attention_focus_mode", 0)),
        "action_node_count": len([x for x in action_nodes if isinstance(x, dict)]),
        "action_drive_max": round(float(max(action_drive_vals) if action_drive_vals else 0.0), 8),
        "action_drive_mean": round(float(sum(action_drive_vals) / len(action_drive_vals)) if action_drive_vals else 0.0, 8),
        "action_drive_active_count": int(action_drive_active_count),
        # performance / observability
        "timing_total_logic_ms": _as_float(timing.get("total_logic_ms", steps_ms.get("total_logic_ms", 0.0))),
        "timing_sensor_ms": _as_float(steps_ms.get("sensor_ms", 0.0)),
        "timing_maintenance_ms": _as_float(steps_ms.get("maintenance_ms", 0.0)),
        "timing_cognitive_stitching_ms": _as_float(steps_ms.get("cognitive_stitching_ms", 0.0)),
        "timing_attention_ms": _as_float(steps_ms.get("attention_ms", 0.0)),
        "timing_structure_level_ms": _as_float(steps_ms.get("structure_level_ms", 0.0)),
        "timing_cache_neutralization_ms": _as_float(steps_ms.get("cache_neutralization_ms", 0.0)),
        "timing_stimulus_level_ms": _as_float(steps_ms.get("stimulus_level_ms", 0.0)),
        "timing_pool_apply_ms": _as_float(steps_ms.get("pool_apply_ms", 0.0)),
        "timing_event_grasp_ms": _as_float(steps_ms.get("event_grasp_ms", 0.0)),
        "timing_induction_and_memory_ms": _as_float(steps_ms.get("induction_and_memory_ms", 0.0)),
        "timing_time_sensor_ms": _as_float(steps_ms.get("time_sensor_ms", 0.0)),
        # time sensor observability (used by adaptive tuning)
        "time_sensor_bucket_update_count": len([x for x in ts_bucket_updates if isinstance(x, dict)]),
        "time_sensor_bucket_energy_sum": round(float(ts_bucket_energy_sum), 8),
        "time_sensor_bucket_energy_max": round(float(ts_bucket_energy_max), 8),
        "time_sensor_attribute_binding_count": len([x for x in ts_attr_bindings if isinstance(x, dict)]),
        "time_sensor_memory_used_count": _as_int(time_sensor.get("memory_used_count", 0)),
        "time_sensor_delayed_task_table_size": _as_int(ts_delayed_tasks.get("table_size", 0)),
        "time_sensor_delayed_task_executed_count": _as_int(ts_delayed_tasks.get("executed_count", 0)),
        "time_sensor_delayed_task_registered_count": _as_int(ts_delayed_registered.get("registered_count", 0)),
        "time_sensor_delayed_task_updated_count": _as_int(ts_delayed_registered.get("updated_count", 0)),
        "time_sensor_delayed_task_pruned_count": _as_int(ts_delayed_registered.get("pruned_count", 0)),
        "time_sensor_delayed_task_skipped_capacity_count": _as_int(ts_delayed_registered.get("skipped", {}).get("capacity", 0) if isinstance(ts_delayed_registered.get("skipped"), dict) else 0),
        "timing_teacher_feedback_ms": _as_float(steps_ms.get("teacher_feedback_ms", 0.0)),
        "timing_cfs_ms": _as_float(steps_ms.get("cfs_ms", 0.0)),
        "timing_iesm_ms": _as_float(steps_ms.get("iesm_ms", 0.0)),
        "timing_emotion_ms": _as_float(steps_ms.get("emotion_ms", 0.0)),
        "timing_action_ms": _as_float(steps_ms.get("action_ms", 0.0)),
        "timing_final_snapshot_ms": _as_float(steps_ms.get("final_snapshot_ms", 0.0)),
        "timing_energy_balance_ms": _as_float(steps_ms.get("energy_balance_ms", 0.0)),
    }

    # Dynamic per-action_kind counters
    # ------------------------------------------------------------
    # 说明：
    # - 期望契约（Expectation Contracts）支持条件 kind=action_executed_kind_min，它会在 metrics 里读取
    #   `action_executed_{action_kind}` 这种动态键。
    # - 因此这里必须把“所有出现过的 action_kind”都输出出来，而不能只输出固定白名单。
    # - 同理，`action_attempted_{action_kind}` 便于调试“哪些行动在频繁尝试但长期失败”。
    for kind, count in attempted_kind_counts.items():
        k = str(kind).strip()
        if not k:
            continue
        record[f"action_attempted_{k}"] = int(count)
    for kind, count in scheduled_kind_counts.items():
        k = str(kind).strip()
        if not k:
            continue
        record[f"action_scheduled_{k}"] = int(count)
    for kind, count in executed_kind_counts.items():
        k = str(kind).strip()
        if not k:
            continue
        record[f"action_executed_{k}"] = int(count)

    # Flatten core CFS kinds for charting
    for kind in CORE_CFS_KINDS:
        record[f"cfs_{kind}_max"] = round(float(cfs_max_by_kind.get(kind, 0.0) or 0.0), 8)
        record[f"cfs_{kind}_count"] = int(cfs_count_by_kind.get(kind, 0) or 0)

    # Live-state CFS (runtime-bound attributes)
    # ------------------------------------------------------------
    # This comes from StatePool snapshot summary aggregation:
    #   summary.bound_attribute_energy_totals[attribute_name]
    # and reflects *maintained* state with half-life decay, not just tick-level peaks.
    bound_totals = snapshot_summary.get("bound_attribute_energy_totals", {})
    if not isinstance(bound_totals, dict):
        bound_totals = {}
    for attr_name in CORE_CFS_BOUND_ATTRS:
        row = bound_totals.get(attr_name, {})
        if not isinstance(row, dict):
            row = {}
        record[f"{attr_name}_live_total_er"] = round(float(_as_float(row.get("total_er", 0.0))), 8)
        record[f"{attr_name}_live_total_ev"] = round(float(_as_float(row.get("total_ev", 0.0))), 8)
        record[f"{attr_name}_live_total_energy"] = round(float(_as_float(row.get("total_energy", 0.0))), 8)
        record[f"{attr_name}_live_item_count"] = int(_as_int(row.get("item_count", 0)))
        record[f"{attr_name}_live_attribute_count"] = int(_as_int(row.get("attribute_count", 0)))

    # Optional labels pass-through (if present) for later specialized plots.
    labels = dt.get("labels")
    if isinstance(labels, dict) and labels:
        record["labels"] = dict(labels)

        # Common teacher/external feedback labels (paper-friendly flat fields).
        teacher = labels.get("teacher") if isinstance(labels.get("teacher"), dict) else {}
        tr = labels.get("teacher_rwd", teacher.get("rwd", labels.get("tool_feedback_rwd", 0.0)))
        tp = labels.get("teacher_pun", teacher.get("pun", labels.get("tool_feedback_pun", 0.0)))
        record["label_teacher_rwd"] = _as_float(tr, 0.0)
        record["label_teacher_pun"] = _as_float(tp, 0.0)
        record["label_should_call_weather"] = _as_int(labels.get("should_call_weather", labels.get("tool_should_call_weather", 0)), 0)

    # Teacher feedback apply result (from report, after clamping + anchor resolution).
    tfb = _as_dict(report.get("teacher_feedback"))
    record["teacher_rwd"] = _as_float(tfb.get("teacher_rwd", 0.0))
    record["teacher_pun"] = _as_float(tfb.get("teacher_pun", 0.0))
    record["teacher_applied_count"] = _as_int(tfb.get("applied_count", 0))

    return record

