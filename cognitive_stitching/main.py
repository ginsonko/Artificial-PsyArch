# -*- coding: utf-8 -*-
"""
Cognitive Stitching (CS) runtime engine.

Current rollout scope:
- pair create remains supported
- existing CS events can re-enter the candidate pool as active seeds
- conservative right-end event extension is enabled
- conservative event-to-event bridge merge is enabled
- weak matching and same-path fatigue stay numeric, never hard-blocking
"""

from __future__ import annotations

import math
import os
import time
from typing import Any


def _parse_simple_yaml_scalar(raw: str) -> Any:
    text = raw.strip()
    if not text:
        return ""
    if len(text) >= 2 and text[0] == text[-1] and text[0] in {'"', "'"}:
        return text[1:-1]
    lowered = text.lower()
    if lowered in {"true", "false"}:
        return lowered == "true"
    if lowered in {"null", "none", "~"}:
        return None
    try:
        if any(marker in text for marker in (".", "e", "E")):
            return float(text)
        return int(text)
    except ValueError:
        return text


def _load_simple_yaml_config(path: str) -> dict[str, Any]:
    if not os.path.exists(path):
        return {}
    data: dict[str, Any] = {}
    try:
        with open(path, "r", encoding="utf-8") as fh:
            for raw_line in fh:
                line = raw_line.strip()
                if not line or line.startswith("#") or ":" not in raw_line:
                    continue
                key, raw_value = raw_line.split(":", 1)
                key = key.strip()
                if not key:
                    continue
                value_text = raw_value.split("#", 1)[0].strip()
                data[key] = _parse_simple_yaml_scalar(value_text)
    except Exception:
        return {}
    return data


def _load_yaml_config(path: str) -> dict[str, Any]:
    try:
        import yaml

        with open(path, "r", encoding="utf-8") as fh:
            data = yaml.safe_load(fh)
        return data if isinstance(data, dict) else _load_simple_yaml_config(path)
    except ImportError:
        return _load_simple_yaml_config(path)
    except Exception:
        return _load_simple_yaml_config(path)


_DEFAULT_CONFIG = {
    "enabled": True,
    "snapshot_top_k": 24,
    "max_seed_items": 8,
    "max_outgoing_edges_per_seed": 8,
    "max_events_per_tick": 1,
    "max_context_k": 2,
    "max_event_head_match_components": 3,
    "max_event_component_count": 8,
    # Event degeneration / 事件退化（组分淘汰）
    # 说明：
    # - 认知拼接事件（CS Event）在长期运行中可能出现“组分退化”：某些组分逐渐失去能量贡献，
    #   最终不再值得保留在事件结构里。
    # - 当退化发生时，我们会把“退化后的事件结构”当作一个新的长期结构写入 HDB，
    #   并建立链式索引，从而保证事件结构始终是“健全的”（可 O(1) 指针打开、可被刺激级查存一体发现）。
    #
    # 重要设计取舍：
    # - 退化判定使用纯数值阈值（share + absolute energy），不硬编码语义规则。
    # - 为了避免在热路径中引入不可控的写放大，本轮退化处理有数量上限。
    "enable_event_degeneration": True,
    # 每 tick 最多处理多少个事件的退化（按事件总能量从高到低挑选）。
    "event_degeneration_max_events_per_tick": 2,
    # 退化后事件最少保留的组分数（小于该值则不生成新的事件结构）。
    "event_degeneration_min_components": 2,
    # share 阈值：某组分在事件中的能量占比（profile_share）低于该值，才有资格被淘汰。
    "event_degeneration_share_threshold": 0.06,
    # absolute 阈值：组分在本事件中分到的绝对能量（share * total_energy）低于该值，才会被淘汰。
    # 注意：该阈值与 share 阈值是“同时满足”才淘汰，以避免小能量事件被过早拆空。
    "event_degeneration_min_component_energy": 0.04,
    "enable_event_extend": True,
    "enable_event_merge": True,
    # Event grasp (a cognitive feeling bound to ES objects).
    # Trigger: event in CAM + energy above threshold (see design doc section 14).
    "enable_event_grasp": True,
    "event_grasp_min_total_energy": 0.25,
    "event_grasp_max_events_per_tick": 4,
    "event_grasp_energy_weight": 1.2,
    "event_grasp_balance_weight": 1.3,
    "event_grasp_margin_weight": 0.8,
    "event_grasp_bias": -1.0,
    "event_grasp_sigmoid_temperature": 1.0,
    "event_grasp_attribute_name": "event_grasp",
    # ESDB (in-memory) lazy merge (parents+delta) and idle consolidation.
    "enable_esdb_overlay": True,
    # How to open context DB for events:
    # - tail_components: current phase2 conservative behavior (open tail component ST DBs)
    # - event_overlay: open ES overlay DB only
    # - hybrid: open both and take the best candidates (default, still capped by max_outgoing_edges_per_seed)
    "event_context_open_mode": "hybrid",
    "esdb_overlay_parent_beta": 0.35,
    "esdb_overlay_top_k": 16,
    "esdb_overlay_cache_ttl_ms": 2500,
    "esdb_materialize_top_n": 96,
    # ESDB delta: small runtime-only outgoing edge cache for ES (to avoid "parents-only overlay" stalling).
    # This is NOT persisted to HDB in current phases.
    "enable_esdb_delta": True,
    "esdb_delta_import_top_k_per_tail": 6,
    "esdb_delta_max_entries": 96,
    "esdb_delta_distance_decay": 0.75,
    "esdb_delta_merge_beta": 0.25,
    "esdb_delta_min_weight": 0.0001,
    # Persist ES into HDB (event structures must be "健全的长期结构").
    # 说明：本开关为真时，CS 将尽量让事件以 HDB-backed 结构形式存在（有独立数据库指针，O(1) 可索引定位）。
    "enable_persist_events_to_hdb": True,
    # Whether newly created CS event runtime items should enter StatePool as
    # ordinary runtime structures. Default is OFF to avoid polluting the main
    # runtime chain with event-shell structures.
    "insert_event_runtime_items_into_state_pool": False,
    "persist_events_max_diff_entries": 96,
    # Idle consolidation budgets / guards
    "idle_consolidate_max_events": 256,
    "idle_consolidate_clear_all_caches": True,
    "idle_consolidate_cache_est_bytes_per_row": 220,
    "min_seed_total_energy": 0.45,
    "min_candidate_score": 0.22,
    "min_event_total_energy": 0.10,
    "base_absorb_ratio": 0.12,
    "pair_absorb_ratio_cap": 0.22,
    "extend_absorb_scale": 0.92,
    "merge_absorb_scale": 0.84,
    "same_pair_fatigue_decay": 0.72,
    "same_pair_fatigue_step": 0.35,
    "same_pair_fatigue_cap": 1.6,
    "same_pair_fatigue_floor_scale": 0.25,
    "edge_ratio_weight": 0.42,
    "energy_balance_weight": 0.23,
    "match_strength_weight": 0.20,
    "runtime_weight_weight": 0.15,
    "context_support_weight": 0.18,
    "bridge_span_weight": 0.08,
    "anchor_distance_penalty": 0.18,
    "containment_match_scale": 0.78,
    "weak_overlap_match_scale": 0.42,
    "weak_overlap_min_ratio": 0.5,
    "event_prefix_match_scale": 1.0,
    "event_prefix_weak_scale": 0.36,
    "event_id_prefix": "cs_event",
    "display_joiner": " -> ",
    "narrative_top_k": 6,
}


class CognitiveStitchingEngine:
    def __init__(self, config_path: str = "", config_override: dict | None = None):
        self._config_path = config_path or os.path.join(
            os.path.dirname(__file__), "config", "cognitive_stitching_config.yaml"
        )
        self._config = self._build_config(config_override)
        self._pair_fatigue: dict[str, float] = {}
        # ESDB: in-memory event DB with lazy merge metadata (parents+delta).
        # Note: this is runtime-only for now; consolidation can flatten chains and clear caches.
        self._esdb: dict[str, dict[str, Any]] = {}
        # Idle consolidation last result snapshot (for observability/UI).
        self._idle_consolidation_count_total = 0
        self._last_idle_consolidation: dict[str, Any] | None = None
        self._last_report: dict[str, Any] = {}

    def close(self) -> None:
        return

    def clear_runtime_state(self, trace_id: str = "cs_clear_runtime", reason: str = "runtime_reset") -> dict:
        result = {
            "cleared_pair_fatigue_count": len(self._pair_fatigue),
            "cleared_esdb_event_count": len(self._esdb),
            "had_last_idle_consolidation": self._last_idle_consolidation is not None,
            "had_last_report": bool(self._last_report),
            "idle_consolidation_count_before": int(self._idle_consolidation_count_total),
        }
        self._pair_fatigue.clear()
        self._esdb.clear()
        self._idle_consolidation_count_total = 0
        self._last_idle_consolidation = None
        self._last_report = {}
        return {
            "success": True,
            "code": "OK",
            "message": f"cognitive stitching runtime cleared ({reason})",
            "trace_id": trace_id,
            "data": result,
        }

    def reload_config(self, trace_id: str = "cs_reload", config_path: str | None = None) -> dict:
        path = config_path or self._config_path
        try:
            fresh = dict(_DEFAULT_CONFIG)
            fresh.update(_load_yaml_config(path))
            self._config = fresh
            return {
                "success": True,
                "code": "OK",
                "message": "Cognitive stitching config reloaded",
                "trace_id": trace_id,
                "data": {"config": dict(self._config)},
            }
        except Exception as exc:
            return {
                "success": False,
                "code": "CONFIG_ERROR",
                "message": f"reload failed: {exc}",
                "trace_id": trace_id,
                "error": {"message": str(exc)},
            }

    def run(
        self,
        *,
        pool,
        hdb,
        trace_id: str,
        tick_id: str,
    ) -> dict:
        start_time = time.time()
        try:
            self._decay_pair_fatigue()
            if not bool(self._config.get("enabled", False)):
                report = self._empty_report(enabled=False, reason="disabled")
                self._last_report = report
                return self._make_response(
                    True,
                    "OK",
                    "cognitive stitching disabled",
                    report,
                    trace_id,
                    tick_id,
                    start_time,
                )

            # Event degeneration (component pruning) / 事件退化（组分淘汰）：
            # - Runs before snapshot so the rest of CS tick sees the updated pool state.
            # - Best-effort and bounded (never raising).
            degeneration = self._maybe_degenerate_events(
                pool=pool,
                hdb=hdb,
                trace_id=trace_id,
                tick_id=tick_id,
            )

            snapshot_resp = pool.get_state_snapshot(
                trace_id=f"{trace_id}_cs_pre",
                tick_id=tick_id,
                top_k=int(self._config.get("snapshot_top_k", 24)),
                sort_by="cp_abs",
            )
            if not snapshot_resp.get("success"):
                report = self._empty_report(enabled=True, reason="snapshot_error")
                report["event_degeneration"] = degeneration
                self._last_report = report
                return self._make_response(
                    False,
                    "STATE_SNAPSHOT_ERROR",
                    "state snapshot failed",
                    report,
                    trace_id,
                    tick_id,
                    start_time,
                )

            snapshot = snapshot_resp.get("data", {}).get("snapshot", {}) or {}
            active_items = self._collect_active_items(snapshot=snapshot, hdb=hdb)
            candidates = self._build_candidates(active_items=active_items, hdb=hdb)
            actions = self._apply_candidates(
                candidates=candidates,
                pool=pool,
                hdb=hdb,
                trace_id=trace_id,
                tick_id=tick_id,
            )
            narrative_top_items = self._collect_narrative_top_items(pool=pool, trace_id=trace_id, tick_id=tick_id)

            # ESDB lightweight runtime summary (bounded + cheap).
            esdb_event_count = len(self._esdb)
            esdb_materialized_event_count = 0
            esdb_delta_entry_total = 0
            for _eid, _entry in list(self._esdb.items()):
                if not isinstance(_entry, dict):
                    continue
                if bool(_entry.get("materialized", False)):
                    esdb_materialized_event_count += 1
                esdb_delta_entry_total += len(list(_entry.get("delta_diff_table", []) or []))

            report = {
                "enabled": True,
                "stage": "phase2_contextual_event_stitching",
                "reason": "ok",
                "seed_structure_count": len(active_items),
                "seed_plain_structure_count": sum(1 for item in active_items if item.get("kind") == "structure"),
                "seed_event_count": sum(1 for item in active_items if item.get("kind") == "event"),
                "candidate_count": len(candidates),
                "action_count": len(actions),
                "created_count": sum(1 for item in actions if item.get("action_family") == "create_event" and not str(item.get("action", "")).startswith("reinforce_")),
                "extended_count": sum(1 for item in actions if item.get("action_family") == "extend_event" and not str(item.get("action", "")).startswith("reinforce_")),
                "merged_count": sum(1 for item in actions if item.get("action_family") == "merge_event" and not str(item.get("action", "")).startswith("reinforce_")),
                "reinforced_count": sum(1 for item in actions if str(item.get("action", "")).startswith("reinforce_")),
                "pair_fatigue_state_size": len(self._pair_fatigue),
                "esdb_event_count": int(esdb_event_count),
                "esdb_materialized_event_count": int(esdb_materialized_event_count),
                "esdb_delta_entry_total": int(esdb_delta_entry_total),
                "candidate_preview": [self._candidate_preview(item) for item in candidates[:8]],
                "actions": actions,
                "narrative_top_items": narrative_top_items,
                "event_degeneration": degeneration,
            }
            self._last_report = report
            return self._make_response(
                True,
                "OK",
                "cognitive stitching completed",
                report,
                trace_id,
                tick_id,
                start_time,
            )
        except Exception as exc:
            report = self._empty_report(enabled=bool(self._config.get("enabled", False)), reason="exception")
            report["error"] = {"message": str(exc)}
            self._last_report = report
            return self._make_response(
                False,
                "CS_RUNTIME_ERROR",
                f"cognitive stitching failed: {exc}",
                report,
                trace_id,
                tick_id,
                start_time,
            )

    def run_event_grasp(
        self,
        *,
        pool,
        attention_snapshot: dict,
        trace_id: str,
        tick_id: str,
        reason: str = "event_grasp_tick",
    ) -> dict:
        """Emit "event_grasp" as runtime-bound numerical attribute on ES items in CAM.

        Design alignment: see cognitive_stitching/docs/认知拼接模块设计文档.md section 14.
        """
        start_time = time.time()
        try:
            if not bool(self._config.get("enable_event_grasp", True)):
                data = {
                    "enabled": False,
                    "reason": "disabled_by_config",
                    "signals": [],
                    "attribute_bindings": [],
                }
                return self._make_response(True, "OK_DISABLED", "event grasp disabled", data, trace_id, tick_id, start_time)

            if pool is None or not hasattr(pool, "bind_attribute_node_to_object"):
                data = {
                    "enabled": True,
                    "reason": "pool_missing_bind_attribute",
                    "signals": [],
                    "attribute_bindings": [],
                }
                return self._make_response(True, "OK_NOOP", "pool cannot bind attribute nodes", data, trace_id, tick_id, start_time)

            selected = [
                it
                for it in list((attention_snapshot or {}).get("top_items", []) or [])
                if isinstance(it, dict)
                and str(it.get("ref_object_type", "") or "") == "st"
            ]
            if not selected:
                data = {"enabled": True, "reason": "no_event_in_cam", "signals": [], "attribute_bindings": []}
                return self._make_response(True, "OK", "no event grasp emitted", data, trace_id, tick_id, start_time)

            min_total_energy = max(0.0, float(self._config.get("event_grasp_min_total_energy", 0.25)))
            max_events = max(1, int(self._config.get("event_grasp_max_events_per_tick", 4)))
            attr_name = str(self._config.get("event_grasp_attribute_name", "event_grasp") or "event_grasp").strip() or "event_grasp"

            # Fetch live state items so grasp can reflect post-neutralization/post-absorption energies.
            live_events: list[dict[str, Any]] = []
            cam_event_count = 0
            for it in selected:
                item_id = str(it.get("item_id", "") or "").strip()
                if not item_id:
                    continue
                state_item = self._get_state_item_by_id(pool=pool, item_id=item_id)
                if not isinstance(state_item, dict):
                    continue
                if not self._is_cognitive_stitching_event_state_item(state_item):
                    continue
                cam_event_count += 1
                ref_id = str(state_item.get("ref_object_id", "") or "")
                event_ref_id = self._extract_event_ref_id_from_state_item(state_item) or ref_id
                energy = dict(state_item.get("energy", {}) or {})
                er = max(0.0, float(energy.get("er", 0.0) or 0.0))
                ev = max(0.0, float(energy.get("ev", 0.0) or 0.0))
                total = round(er + ev, 8)
                if total < min_total_energy:
                    continue
                live_events.append(
                    {
                        "item_id": item_id,
                        "ref_object_id": ref_id,
                        "event_ref_id": event_ref_id,
                        "display": str((state_item.get("ref_snapshot", {}) or {}).get("content_display", "") or ref_id),
                        "er": round(er, 8),
                        "ev": round(ev, 8),
                        "total_energy": total,
                        "state_item": state_item,
                    }
                )

            if not live_events:
                reason_key = "no_cognitive_stitching_event_in_cam" if cam_event_count <= 0 else "below_energy_threshold"
                data = {"enabled": True, "reason": reason_key, "signals": [], "attribute_bindings": []}
                return self._make_response(True, "OK", "no event grasp emitted", data, trace_id, tick_id, start_time)

            live_events.sort(key=lambda row: float(row.get("total_energy", 0.0)), reverse=True)
            live_events = live_events[:max_events]

            # Precompute runner-up energy for margin.
            totals = [float(row.get("total_energy", 0.0) or 0.0) for row in live_events]
            margin_by_item: dict[str, float] = {}
            for index, row in enumerate(live_events):
                score = float(row.get("total_energy", 0.0) or 0.0)
                runner = float(totals[index + 1]) if index + 1 < len(totals) else 0.0
                margin_by_item[str(row.get("item_id", "") or "")] = self._clamp01((score - runner) / max(abs(score), 1e-9))

            signals: list[dict[str, Any]] = []
            bindings: list[dict[str, Any]] = []
            for row in live_events:
                state_item = row.get("state_item")
                item_id = str(row.get("item_id", "") or "")
                ref_id = str(row.get("ref_object_id", "") or "")
                event_ref_id = str(row.get("event_ref_id", "") or ref_id)
                total = float(row.get("total_energy", 0.0) or 0.0)
                margin = float(margin_by_item.get(item_id, 0.0) or 0.0)
                balance = self._event_internal_balance_from_ledger(state_item)
                grasp = self._compute_event_grasp(total_energy=total, balance=balance, margin=margin)

                attr_sa = self._build_numerical_attribute_sa(
                    attribute_name=attr_name,
                    attribute_value=grasp,
                    target_item_id=item_id,
                    target_ref_object_id=ref_id,
                    trace_id=trace_id,
                    tick_id=tick_id,
                    sub_type="event_grasp_attribute_presence",
                    display_prefix="事件把握感",
                )
                try:
                    bind_res = pool.bind_attribute_node_to_object(
                        target_item_id=item_id,
                        attribute_sa=attr_sa,
                        trace_id=f"{trace_id}_cs_event_grasp_bind",
                        tick_id=tick_id,
                        source_module="cognitive_stitching",
                        reason=reason,
                    )
                except Exception as exc:
                    bind_res = {"success": False, "code": "EXCEPTION", "message": str(exc)}

                signals.append(
                    {
                        "event_item_id": item_id,
                        "event_structure_id": ref_id,
                        "event_ref_id": event_ref_id,
                        "event_display": str(row.get("display", "") or ref_id),
                        "er": round(float(row.get("er", 0.0) or 0.0), 8),
                        "ev": round(float(row.get("ev", 0.0) or 0.0), 8),
                        "total_energy": round(total, 8),
                        "balance": round(balance, 8),
                        "margin": round(margin, 8),
                        "grasp": round(grasp, 8),
                    }
                )
                bindings.append(
                    {
                        "event_item_id": item_id,
                        "attribute_sa_id": str(attr_sa.get("id", "") or ""),
                        "attribute_name": attr_name,
                        "attribute_value": round(grasp, 8),
                        "success": bool(bind_res.get("success", False)),
                        "code": str(bind_res.get("code", "") or ""),
                    }
                )

            data = {
                "enabled": True,
                "reason": "ok",
                "selected_event_count": len(live_events),
                "emitted_count": len(signals),
                "signals": signals,
                "attribute_bindings": bindings,
            }
            return self._make_response(True, "OK", "event grasp emitted", data, trace_id, tick_id, start_time)
        except Exception as exc:
            data = {"enabled": True, "reason": "exception", "signals": [], "attribute_bindings": [], "error": {"message": str(exc)}}
            return self._make_response(False, "CS_EVENT_GRASP_ERROR", f"event grasp failed: {exc}", data, trace_id, tick_id, start_time)

    def idle_consolidate(
        self,
        *,
        hdb,
        trace_id: str,
        tick_id: str,
        reason: str = "idle_consolidation",
        max_events: int | None = None,
    ) -> dict:
        """Idle-time consolidation for CS runtime stores (ESDB overlay + cache release)."""
        start_time = time.time()
        try:
            if not bool(self._config.get("enable_esdb_overlay", True)):
                data = {"enabled": False, "reason": "disabled_by_config", "event_count": len(self._esdb)}
                return self._make_response(True, "OK_DISABLED", "esdb overlay disabled", data, trace_id, tick_id, start_time)

            now_ms = int(time.time() * 1000)
            before_event_count = len(self._esdb)
            before_depths = [self._esdb_parent_depth(event_id, set()) for event_id in list(self._esdb.keys())]
            before_avg_depth = round(sum(before_depths) / max(1, len(before_depths)), 6) if before_depths else 0.0
            before_max_depth = int(max(before_depths)) if before_depths else 0
            before_parent_count_total = 0
            before_delta_entry_total = 0
            before_materialized_event_count = 0
            before_materialized_entry_total = 0
            for _eid, _entry in list(self._esdb.items()):
                if not isinstance(_entry, dict):
                    continue
                before_parent_count_total += len(list(_entry.get("parents", []) or []))
                before_delta_entry_total += len(list(_entry.get("delta_diff_table", []) or []))
                if bool(_entry.get("materialized", False)):
                    before_materialized_event_count += 1
                    before_materialized_entry_total += len(list(_entry.get("materialized_diff_table", []) or []))

            # Optional cache release for all ES entries.
            clear_all_caches = bool(self._config.get("idle_consolidate_clear_all_caches", True))
            cache_bytes_per_row = int(self._config.get("idle_consolidate_cache_est_bytes_per_row", 220) or 220)
            cache_bytes_per_row = max(0, min(5000, cache_bytes_per_row))
            released_cache_event_count = 0
            released_cache_row_total = 0
            if clear_all_caches:
                for _eid, _entry in list(self._esdb.items()):
                    if not isinstance(_entry, dict):
                        continue
                    cache = _entry.get("runtime_cache")
                    if isinstance(cache, dict) and cache:
                        released_cache_event_count += 1
                        cached_rows = cache.get("overlay_cached_rows")
                        if isinstance(cached_rows, list):
                            released_cache_row_total += len(cached_rows)
                    _entry["runtime_cache"] = {}

            materialize_top_n = max(1, int(self._config.get("esdb_materialize_top_n", 96)))
            ids = list(self._esdb.keys())
            ids.sort(key=lambda eid: int((self._esdb.get(eid, {}) or {}).get("updated_at_ms", 0) or 0), reverse=True)
            # Guard: cap how many events we consolidate in one call.
            # Rationale: ES can grow large in long runs; consolidation must remain bounded.
            effective_max_events = max_events
            if effective_max_events is None:
                effective_max_events = self._config.get("idle_consolidate_max_events", 256)
            try:
                effective_max_events = int(effective_max_events) if effective_max_events is not None else None
            except Exception:
                effective_max_events = None
            if effective_max_events is not None:
                if int(effective_max_events) <= 0:
                    ids = []
                else:
                    ids = ids[: int(effective_max_events)]

            materialized_count = 0
            materialized_entry_total = 0
            for event_id in ids:
                entry = self._esdb.get(event_id)
                if not isinstance(entry, dict):
                    continue
                # Materialize overlay top-N (flatten parent chain) + clear runtime cache.
                diff_table = self._esdb_materialize_diff_table(event_ref_id=event_id, hdb=hdb, top_n=materialize_top_n)
                entry["materialized"] = True
                entry["materialized_diff_table"] = diff_table
                entry["parents_before_consolidation"] = list(entry.get("parents", []) or [])
                entry["parents"] = []
                entry["runtime_cache"] = {}
                entry["updated_at_ms"] = int(time.time() * 1000)
                materialized_count += 1
                materialized_entry_total += len(diff_table)

            # Optional: persist consolidated ES into HDB (disabled by default).
            persist_results: list[dict] = []
            if bool(self._config.get("enable_persist_events_to_hdb", False)) and hasattr(hdb, "upsert_cognitive_stitching_event_structure"):
                max_diff_entries = int(self._config.get("persist_events_max_diff_entries", 96) or 96)
                max_diff_entries = max(0, min(512, max_diff_entries))
                structure_store = getattr(hdb, "_structure_store", None)
                for event_id in ids:
                    entry = self._esdb.get(event_id)
                    if not isinstance(entry, dict):
                        continue
                    components = [str(x) for x in (entry.get("components", []) or []) if str(x)]
                    displays = self._resolve_component_displays(components=components, structure_store=structure_store) if structure_store is not None else []
                    if not displays:
                        displays = list(components)
                    display_text = self._event_display_from_components(displays)
                    diff_rows = list(entry.get("materialized_diff_table", []) or [])
                    try:
                        res = hdb.upsert_cognitive_stitching_event_structure(
                            event_ref_id=str(event_id),
                            member_refs=list(components),
                            display_text=str(display_text),
                            diff_rows=diff_rows,
                            trace_id=f"{trace_id}_cs_persist_event",
                            tick_id=tick_id,
                            reason=str(reason or ""),
                            max_diff_entries=int(max_diff_entries),
                        )
                    except Exception as exc:
                        res = {"success": False, "code": "EXCEPTION", "message": str(exc), "data": {"event_ref_id": str(event_id)}}
                    persist_results.append(
                        {
                            "event_ref_id": str(event_id),
                            "success": bool(res.get("success", False)),
                            "code": str(res.get("code", "") or ""),
                            "structure_id": str((res.get("data", {}) or {}).get("structure_id", "") or ""),
                            "created": bool((res.get("data", {}) or {}).get("created", False)),
                            "diff_upserted_count": int((res.get("data", {}) or {}).get("diff_upserted_count", 0) or 0),
                        }
                    )

            after_depths = [self._esdb_parent_depth(event_id, set()) for event_id in list(self._esdb.keys())]
            after_avg_depth = round(sum(after_depths) / max(1, len(after_depths)), 6) if after_depths else 0.0
            after_max_depth = int(max(after_depths)) if after_depths else 0
            after_parent_count_total = 0
            after_delta_entry_total = 0
            after_materialized_event_count = 0
            after_materialized_entry_total = 0
            for _eid, _entry in list(self._esdb.items()):
                if not isinstance(_entry, dict):
                    continue
                after_parent_count_total += len(list(_entry.get("parents", []) or []))
                after_delta_entry_total += len(list(_entry.get("delta_diff_table", []) or []))
                if bool(_entry.get("materialized", False)):
                    after_materialized_event_count += 1
                    after_materialized_entry_total += len(list(_entry.get("materialized_diff_table", []) or []))

            data = {
                "enabled": True,
                "reason": str(reason or ""),
                "timestamp_ms": int(now_ms),
                "event_count": int(before_event_count),
                "consolidated_event_count": int(materialized_count),
                "materialized_top_n": int(materialize_top_n),
                "materialized_diff_entry_total": int(materialized_entry_total),
                "avg_parent_depth_before": float(before_avg_depth),
                "avg_parent_depth_after": float(after_avg_depth),
                "max_parent_depth_before": int(before_max_depth),
                "max_parent_depth_after": int(after_max_depth),
                "parent_ref_total_before": int(before_parent_count_total),
                "parent_ref_total_after": int(after_parent_count_total),
                "delta_diff_entry_total_before": int(before_delta_entry_total),
                "delta_diff_entry_total_after": int(after_delta_entry_total),
                "materialized_event_count_before": int(before_materialized_event_count),
                "materialized_event_count_after": int(after_materialized_event_count),
                "materialized_entry_total_before": int(before_materialized_entry_total),
                "materialized_entry_total_after": int(after_materialized_entry_total),
                "persist_events_enabled": bool(self._config.get("enable_persist_events_to_hdb", False)),
                "persisted_event_count": int(sum(1 for r in persist_results if bool(r.get("success", False)))),
                "persist_results_preview": persist_results[: min(8, len(persist_results))],
                "idle_consolidate_clear_all_caches": bool(clear_all_caches),
                "released_cache_event_count": int(released_cache_event_count),
                "released_cache_row_total": int(released_cache_row_total),
                "released_cache_est_bytes": int(released_cache_row_total) * int(cache_bytes_per_row),
                "effective_max_events": int(effective_max_events) if effective_max_events is not None else None,
            }
            resp = self._make_response(True, "OK", "cognitive stitching idle consolidation completed", data, trace_id, tick_id, start_time)
            try:
                self._idle_consolidation_count_total = int(getattr(self, "_idle_consolidation_count_total", 0) or 0) + 1
                self._last_idle_consolidation = dict(resp)
            except Exception:
                pass
            return resp
        except Exception as exc:
            data = {"enabled": True, "reason": "exception", "error": {"message": str(exc)}}
            return self._make_response(False, "CS_IDLE_CONSOLIDATION_ERROR", f"idle consolidation failed: {exc}", data, trace_id, tick_id, start_time)

    def _build_config(self, config_override: dict | None = None) -> dict[str, Any]:
        config = dict(_DEFAULT_CONFIG)
        config.update(_load_yaml_config(self._config_path))
        if config_override:
            config.update(config_override)
        return config

    def _decay_pair_fatigue(self) -> None:
        decay = max(0.0, min(1.0, float(self._config.get("same_pair_fatigue_decay", 0.72))))
        retained: dict[str, float] = {}
        for key, value in self._pair_fatigue.items():
            next_value = round(max(0.0, float(value) * decay), 8)
            if next_value > 1e-6:
                retained[key] = next_value
        self._pair_fatigue = retained

    def _collect_active_items(self, *, snapshot: dict, hdb) -> list[dict]:
        items = list(snapshot.get("top_items", []) or [])
        structure_store = getattr(hdb, "_structure_store", None)
        if structure_store is None:
            return []

        prefix = str(self._config.get("event_id_prefix", "cs_event"))
        min_total_energy = max(0.0, float(self._config.get("min_seed_total_energy", 0.45)))
        active: list[dict] = []
        for order_index, item in enumerate(items):
            if str(item.get("ref_object_type", "") or "") != "st":
                continue

            ref_id = str(item.get("ref_object_id", "") or "")
            if not ref_id:
                continue

            er = round(max(0.0, float(item.get("er", 0.0) or 0.0)), 8)
            ev = round(max(0.0, float(item.get("ev", 0.0) or 0.0)), 8)
            total_energy = round(er + ev, 8)
            if total_energy < min_total_energy:
                continue

            structure_obj = None
            try:
                structure_obj = structure_store.get(ref_id)
            except Exception:
                structure_obj = None

            if isinstance(structure_obj, dict) and self._is_cognitive_stitching_event_structure_obj(structure_obj):
                event_item = self._build_active_event_item(
                    item_id=str(item.get("item_id", "") or ""),
                    ref_id=ref_id,
                    display=str(item.get("display", "") or ""),
                    er=er,
                    ev=ev,
                    total_energy=total_energy,
                    order_index=order_index,
                    structure_store=structure_store,
                    structure_obj=structure_obj,
                )
                if event_item:
                    active.append(event_item)
                continue

            if ref_id.startswith(f"{prefix}::"):
                event_item = self._build_active_event_item(
                    item_id=str(item.get("item_id", "") or ""),
                    ref_id=ref_id,
                    display=str(item.get("display", "") or ref_id),
                    er=er,
                    ev=ev,
                    total_energy=total_energy,
                    order_index=order_index,
                    structure_store=structure_store,
                    structure_obj=None,
                )
                if event_item:
                    active.append(event_item)
                continue

            if not isinstance(structure_obj, dict):
                continue

            display = self._structure_display(structure_obj) or str(item.get("display", "") or ref_id)
            tokens = list(structure_obj.get("structure", {}).get("flat_tokens", []) or [])
            if not tokens:
                tokens = [display]
            runtime_weight = self._runtime_weight(hdb=hdb, structure_obj=structure_obj)
            active.append(
                {
                    "kind": "structure",
                    "item_id": str(item.get("item_id", "") or ""),
                    "ref_object_id": ref_id,
                    "display": display,
                    "tokens": tokens,
                    "components": [ref_id],
                    "component_displays": [display],
                    "structure_obj": structure_obj,
                    "er": er,
                    "ev": ev,
                    "total_energy": total_energy,
                    "runtime_weight": runtime_weight,
                    "balance_energy": total_energy,
                    "balance_weight": runtime_weight,
                    "order_index": order_index,
                }
            )

        active.sort(
            key=lambda item: (
                float(item.get("total_energy", 0.0)),
                float(item.get("runtime_weight", 0.0)),
                -int(item.get("order_index", 0)),
            ),
            reverse=True,
        )
        return active[: max(1, int(self._config.get("max_seed_items", 8)) * 3)]

    def _build_active_event_item(
        self,
        *,
        item_id: str,
        ref_id: str,
        display: str,
        er: float,
        ev: float,
        total_energy: float,
        order_index: int,
        structure_store,
        structure_obj: dict | None,
    ) -> dict | None:
        if isinstance(structure_obj, dict):
            structure_block = structure_obj.get("structure", {}) if isinstance(structure_obj.get("structure", {}), dict) else {}
            components = [str(x) for x in (structure_block.get("member_refs", []) or []) if str(x)]
            components = list(dict.fromkeys(components))
            if len(components) >= 2:
                event_ref_id = str(structure_block.get("content_signature", "") or "").strip()
                if not event_ref_id:
                    ext = structure_block.get("ext", {}) if isinstance(structure_block.get("ext", {}), dict) else {}
                    cs_meta = ext.get("cognitive_stitching", {}) if isinstance(ext.get("cognitive_stitching", {}), dict) else {}
                    event_ref_id = str(cs_meta.get("event_ref_id", "") or cs_meta.get("cs_event_ref_id", "") or "").strip()
                component_displays = self._resolve_component_displays(components=components, structure_store=structure_store)
                if not component_displays:
                    component_displays = list(components)
                runtime_weight = round(max(total_energy, er, ev, 0.01), 8)
                balance_divisor = max(1.0, math.sqrt(float(len(components))))
                return {
                    "kind": "event",
                    "item_id": str(item_id or ""),
                    "ref_object_id": str(structure_obj.get("id", "") or ref_id),
                    "event_ref_id": event_ref_id,
                    "display": str(structure_block.get("display_text", "") or display or self._event_display_from_components(component_displays)),
                    "tokens": list(component_displays),
                    "components": list(components),
                    "component_displays": list(component_displays),
                    "structure_obj": structure_obj,
                    "er": er,
                    "ev": ev,
                    "total_energy": total_energy,
                    "runtime_weight": runtime_weight,
                    "balance_energy": round(total_energy / balance_divisor, 8),
                    "balance_weight": round(runtime_weight / balance_divisor, 8),
                    "order_index": order_index,
                }

        # Legacy runtime-only event (ref_id itself is the event_ref_id).
        components = self._parse_event_components(ref_id)
        if len(components) < 2:
            return None
        component_displays = self._resolve_component_displays(components=components, structure_store=structure_store)
        if not component_displays:
            component_displays = list(components)
        runtime_weight = round(max(total_energy, er, ev, 0.01), 8)
        balance_divisor = max(1.0, math.sqrt(float(len(components))))
        return {
            "kind": "event",
            "item_id": str(item_id or ""),
            "ref_object_id": ref_id,
            "event_ref_id": ref_id,
            "display": display or self._event_display_from_components(component_displays),
            "tokens": list(component_displays),
            "components": list(components),
            "component_displays": list(component_displays),
            "structure_obj": None,
            "er": er,
            "ev": ev,
            "total_energy": total_energy,
            "runtime_weight": runtime_weight,
            "balance_energy": round(total_energy / balance_divisor, 8),
            "balance_weight": round(runtime_weight / balance_divisor, 8),
            "order_index": order_index,
        }

    def _build_candidates(self, *, active_items: list[dict], hdb) -> list[dict]:
        if not active_items:
            return []
        structure_store = getattr(hdb, "_structure_store", None)
        if structure_store is None:
            return []

        max_seed_items = max(1, int(self._config.get("max_seed_items", 8)))
        best_by_signature: dict[str, dict] = {}
        active_by_ref = {
            str(item.get("ref_object_id", "")): item
            for item in active_items
            if str(item.get("ref_object_id", ""))
        }

        for source in active_items[:max_seed_items]:
            if source.get("kind") == "structure":
                self._collect_pair_create_candidates(
                    source=source,
                    active_items=active_items,
                    active_by_ref=active_by_ref,
                    hdb=hdb,
                    best_by_signature=best_by_signature,
                )
                continue

            if source.get("kind") == "event":
                self._collect_event_bridge_candidates(
                    source=source,
                    active_items=active_items,
                    active_by_ref=active_by_ref,
                    hdb=hdb,
                    best_by_signature=best_by_signature,
                )

        ranked = sorted(
            best_by_signature.values(),
            key=lambda item: (
                float(item.get("score", 0.0)),
                float(item.get("context_ratio", 0.0)),
                float(item.get("edge_weight_ratio", 0.0)),
                float(item.get("match_strength", 0.0)),
            ),
            reverse=True,
        )
        return ranked

    def _collect_pair_create_candidates(
        self,
        *,
        source: dict,
        active_items: list[dict],
        active_by_ref: dict[str, dict],
        hdb,
        best_by_signature: dict[str, dict],
    ) -> None:
        entries, positive_total_weight = self._top_diff_entries(
            structure_store=getattr(hdb, "_structure_store", None),
            owner_ref_id=source["ref_object_id"],
        )
        if not entries or positive_total_weight <= 0.0:
            return

        for entry in entries:
            target_id = str(entry.get("target_id", "") or "")
            target_structure = self._get_structure(hdb=hdb, structure_id=target_id)
            if not target_structure:
                continue

            matched = self._resolve_target_match(
                target_structure=target_structure,
                active_items=active_items,
                active_by_ref=active_by_ref,
                source_ref_id=source["ref_object_id"],
                allow_event_targets=False,
            )
            if not matched or matched.get("item", {}).get("kind") != "structure":
                continue

            new_components = [source["ref_object_id"], matched["item"]["ref_object_id"]]
            if not self._is_component_sequence_valid(new_components):
                continue

            candidate = self._score_candidate(
                action_type="create_event",
                source=source,
                target=matched["item"],
                entry=entry,
                positive_total_weight=positive_total_weight,
                matched=matched,
                context_hits=1,
                closest_distance=0,
                new_components=new_components,
                absorb_scale=1.0,
            )
            if candidate:
                self._upsert_candidate(best_by_signature=best_by_signature, candidate=candidate)

    def _collect_event_bridge_candidates(
        self,
        *,
        source: dict,
        active_items: list[dict],
        active_by_ref: dict[str, dict],
        hdb,
        best_by_signature: dict[str, dict],
    ) -> None:
        enable_extend = bool(self._config.get("enable_event_extend", True))
        enable_merge = bool(self._config.get("enable_event_merge", True))
        if not enable_extend and not enable_merge:
            return

        max_context_k = max(1, int(self._config.get("max_context_k", 2)))
        tail_refs = list(source.get("components", []) or [])[-max_context_k:]
        if not tail_refs:
            return

        open_mode = str(self._config.get("event_context_open_mode", "tail_components") or "tail_components").strip().lower()
        if open_mode in {"event_overlay", "hybrid"} and bool(self._config.get("enable_esdb_overlay", True)):
            overlay_entries, overlay_total_weight = self._esdb_open_overlay_top_diff_entries(
                event_ref_id=str(source.get("event_ref_id", "") or source.get("ref_object_id", "") or ""),
                hdb=hdb,
            )
            if overlay_entries and overlay_total_weight > 0.0:
                for entry in overlay_entries:
                    target_structure = self._get_structure(hdb=hdb, structure_id=str(entry.get("target_id", "") or ""))
                    if not target_structure:
                        continue

                    matched = self._resolve_target_match(
                        target_structure=target_structure,
                        active_items=active_items,
                        active_by_ref=active_by_ref,
                        source_ref_id=str(source.get("ref_object_id", "") or ""),
                        allow_event_targets=enable_merge,
                    )
                    if not matched:
                        continue

                    target_item = matched.get("item", {}) or {}
                    if target_item.get("kind") == "structure":
                        if not enable_extend:
                            continue
                        if str(target_item.get("ref_object_id", "")) in list(source.get("components", []) or []):
                            continue
                        action_type = "extend_event"
                        new_components = list(source.get("components", []) or []) + [str(target_item.get("ref_object_id", "") or "")]
                        absorb_scale = float(self._config.get("extend_absorb_scale", 0.92))
                    else:
                        if not enable_merge:
                            continue
                        action_type = "merge_event"
                        new_components = self._merge_event_components(
                            left=list(source.get("components", []) or []),
                            right=list(target_item.get("components", []) or []),
                        )
                        absorb_scale = float(self._config.get("merge_absorb_scale", 0.84))

                    if not self._is_component_sequence_valid(new_components):
                        continue

                    support_hits = int((entry.get("ext", {}) or {}).get("support_hits", 1) or 1)
                    support_hits = max(1, min(max_context_k, support_hits))

                    candidate = self._score_candidate(
                        action_type=action_type,
                        source=source,
                        target=target_item,
                        entry=entry,
                        positive_total_weight=float(overlay_total_weight),
                        matched=matched,
                        context_hits=support_hits,
                        closest_distance=0,
                        new_components=new_components,
                        absorb_scale=absorb_scale,
                    )
                    if candidate:
                        self._upsert_candidate(best_by_signature=best_by_signature, candidate=candidate)

            if open_mode == "event_overlay":
                return

        for context_ref in reversed(tail_refs):
            entries, positive_total_weight = self._top_diff_entries(
                structure_store=getattr(hdb, "_structure_store", None),
                owner_ref_id=context_ref,
            )
            if not entries or positive_total_weight <= 0.0:
                continue

            for entry in entries:
                target_structure = self._get_structure(hdb=hdb, structure_id=str(entry.get("target_id", "") or ""))
                if not target_structure:
                    continue

                matched = self._resolve_target_match(
                    target_structure=target_structure,
                    active_items=active_items,
                    active_by_ref=active_by_ref,
                    source_ref_id=source["ref_object_id"],
                    allow_event_targets=enable_merge,
                )
                if not matched:
                    continue

                target_item = matched.get("item", {}) or {}
                if target_item.get("kind") == "structure":
                    if not enable_extend:
                        continue
                    if str(target_item.get("ref_object_id", "")) in list(source.get("components", []) or []):
                        continue
                    action_type = "extend_event"
                    new_components = list(source.get("components", []) or []) + [str(target_item.get("ref_object_id", "") or "")]
                    absorb_scale = float(self._config.get("extend_absorb_scale", 0.92))
                else:
                    if not enable_merge:
                        continue
                    action_type = "merge_event"
                    new_components = self._merge_event_components(
                        left=list(source.get("components", []) or []),
                        right=list(target_item.get("components", []) or []),
                    )
                    absorb_scale = float(self._config.get("merge_absorb_scale", 0.84))

                if not self._is_component_sequence_valid(new_components):
                    continue

                context_hits, closest_distance = self._estimate_context_support(
                    source_event=source,
                    target_item=target_item,
                    hdb=hdb,
                )
                if context_hits <= 0:
                    continue

                candidate = self._score_candidate(
                    action_type=action_type,
                    source=source,
                    target=target_item,
                    entry=entry,
                    positive_total_weight=positive_total_weight,
                    matched=matched,
                    context_hits=context_hits,
                    closest_distance=closest_distance,
                    new_components=new_components,
                    absorb_scale=absorb_scale,
                )
                if candidate:
                    self._upsert_candidate(best_by_signature=best_by_signature, candidate=candidate)

    def _top_diff_entries(self, *, structure_store, owner_ref_id: str) -> tuple[list[dict], float]:
        if structure_store is None or not owner_ref_id:
            return [], 0.0
        source_db = structure_store.get_db_by_owner(owner_ref_id)
        if not isinstance(source_db, dict):
            return [], 0.0
        raw_entries = [
            entry
            for entry in list(source_db.get("diff_table", []) or [])
            if isinstance(entry, dict) and str(entry.get("target_id", "") or "")
        ]
        if not raw_entries:
            return [], 0.0
        max_outgoing = max(1, int(self._config.get("max_outgoing_edges_per_seed", 8)))
        entries = sorted(
            raw_entries,
            key=lambda item: float(item.get("base_weight", 0.0) or 0.0),
            reverse=True,
        )[:max_outgoing]
        positive_total_weight = sum(max(0.0, float(entry.get("base_weight", 0.0) or 0.0)) for entry in entries)
        return entries, positive_total_weight

    def _resolve_target_match(
        self,
        *,
        target_structure: dict,
        active_items: list[dict],
        active_by_ref: dict[str, dict],
        source_ref_id: str,
        allow_event_targets: bool,
    ) -> dict | None:
        target_ref_id = str(target_structure.get("id", "") or "")
        target_tokens = list(target_structure.get("structure", {}).get("flat_tokens", []) or [])
        if not target_tokens:
            target_tokens = [self._structure_display(target_structure)]

        if target_ref_id and target_ref_id in active_by_ref and target_ref_id != source_ref_id:
            exact_item = active_by_ref[target_ref_id]
            if exact_item.get("kind") == "structure":
                return {
                    "item": exact_item,
                    "mode": "exact",
                    "strength": 1.0,
                    "matched_span": max(1, len(target_tokens)),
                    "prefix_components": 1,
                }

        best: dict | None = None
        for item in active_items:
            if str(item.get("ref_object_id", "")) == source_ref_id:
                continue
            if item.get("kind") == "event" and not allow_event_targets:
                continue
            row = self._match_target_structure_to_item(target_structure=target_structure, item=item)
            if not row:
                continue
            if best is None:
                best = row
                continue
            if float(row.get("strength", 0.0)) > float(best.get("strength", 0.0)):
                best = row
                continue
            if float(row.get("strength", 0.0)) == float(best.get("strength", 0.0)) and int(row.get("matched_span", 0)) > int(best.get("matched_span", 0)):
                best = row
        return best

    def _match_target_structure_to_item(self, *, target_structure: dict, item: dict) -> dict | None:
        if item.get("kind") == "event":
            return self._match_target_structure_to_event_item(target_structure=target_structure, event_item=item)
        return self._match_target_structure_to_structure_item(target_structure=target_structure, item=item)

    def _match_target_structure_to_structure_item(self, *, target_structure: dict, item: dict) -> dict | None:
        target_ref_id = str(target_structure.get("id", "") or "")
        target_tokens = list(target_structure.get("structure", {}).get("flat_tokens", []) or [])
        if not target_tokens:
            target_tokens = [self._structure_display(target_structure)]

        if target_ref_id and target_ref_id == str(item.get("ref_object_id", "") or ""):
            return {
                "item": item,
                "mode": "exact",
                "strength": 1.0,
                "matched_span": max(1, len(target_tokens)),
                "prefix_components": 1,
            }

        candidate_tokens = list(item.get("tokens", []) or [])
        if not candidate_tokens:
            return None

        contiguous_ratio = self._contiguous_subsequence_ratio(target_tokens, candidate_tokens)
        if contiguous_ratio > 0.0:
            return {
                "item": item,
                "mode": "containment",
                "strength": round(float(self._config.get("containment_match_scale", 0.78)) * contiguous_ratio, 8),
                "matched_span": max(1, len(target_tokens)),
                "prefix_components": 1,
            }

        lcs_len = self._lcs_length(target_tokens, candidate_tokens)
        if lcs_len <= 0:
            return None
        overlap_ratio = round(float(lcs_len) / max(1, len(target_tokens)), 8)
        if overlap_ratio < float(self._config.get("weak_overlap_min_ratio", 0.5)):
            return None
        return {
            "item": item,
            "mode": "weak_overlap",
            "strength": round(float(self._config.get("weak_overlap_match_scale", 0.42)) * overlap_ratio, 8),
            "matched_span": lcs_len,
            "prefix_components": 1,
        }

    def _match_target_structure_to_event_item(self, *, target_structure: dict, event_item: dict) -> dict | None:
        target_tokens = list(target_structure.get("structure", {}).get("flat_tokens", []) or [])
        if not target_tokens:
            target_tokens = [self._structure_display(target_structure)]

        component_displays = list(event_item.get("component_displays", []) or [])
        if not component_displays:
            return None

        max_head_components = max(1, int(self._config.get("max_event_head_match_components", 3)))
        max_head_components = min(max_head_components, len(component_displays))
        best: dict | None = None
        for prefix_components in range(1, max_head_components + 1):
            prefix_tokens = component_displays[:prefix_components]
            prefix_match_len = self._common_prefix_length(target_tokens, prefix_tokens)
            if prefix_match_len > 0:
                target_cover = float(prefix_match_len) / max(1, len(target_tokens))
                prefix_cover = float(prefix_match_len) / max(1, len(prefix_tokens))
                strength = float(self._config.get("event_prefix_match_scale", 1.0)) * math.sqrt(target_cover * prefix_cover)
                row = {
                    "item": event_item,
                    "mode": "event_prefix_exact" if target_cover == 1.0 and prefix_cover == 1.0 else "event_prefix",
                    "strength": round(strength, 8),
                    "matched_span": prefix_match_len,
                    "prefix_components": prefix_components,
                }
                if best is None or float(row["strength"]) > float(best.get("strength", 0.0)) or (
                    float(row["strength"]) == float(best.get("strength", 0.0))
                    and int(row["matched_span"]) > int(best.get("matched_span", 0))
                ):
                    best = row

            lcs_len = self._lcs_length(target_tokens, prefix_tokens)
            if lcs_len <= 0:
                continue
            overlap_ratio = round(float(lcs_len) / max(1, len(target_tokens)), 8)
            if overlap_ratio < float(self._config.get("weak_overlap_min_ratio", 0.5)):
                continue
            prefix_ratio = round(float(lcs_len) / max(1, len(prefix_tokens)), 8)
            strength = float(self._config.get("event_prefix_weak_scale", 0.36)) * math.sqrt(overlap_ratio * prefix_ratio)
            row = {
                "item": event_item,
                "mode": "event_prefix_weak",
                "strength": round(strength, 8),
                "matched_span": lcs_len,
                "prefix_components": prefix_components,
            }
            if best is None or float(row["strength"]) > float(best.get("strength", 0.0)) or (
                float(row["strength"]) == float(best.get("strength", 0.0))
                and int(row["matched_span"]) > int(best.get("matched_span", 0))
            ):
                best = row
        return best

    def _estimate_context_support(self, *, source_event: dict, target_item: dict, hdb) -> tuple[int, int]:
        structure_store = getattr(hdb, "_structure_store", None)
        if structure_store is None:
            return 0, max(1, int(self._config.get("max_context_k", 2)))

        max_context_k = max(1, int(self._config.get("max_context_k", 2)))
        tail_refs = list(source_event.get("components", []) or [])[-max_context_k:]
        support_hits = 0
        closest_distance = max_context_k
        for distance, context_ref in enumerate(reversed(tail_refs)):
            entries, _ = self._top_diff_entries(structure_store=structure_store, owner_ref_id=context_ref)
            if not entries:
                continue
            hit = False
            for entry in entries:
                target_structure = self._get_structure(hdb=hdb, structure_id=str(entry.get("target_id", "") or ""))
                if not target_structure:
                    continue
                row = self._match_target_structure_to_item(target_structure=target_structure, item=target_item)
                if row and float(row.get("strength", 0.0)) > 0.0:
                    hit = True
                    break
            if hit:
                support_hits += 1
                closest_distance = min(closest_distance, distance)
        return support_hits, closest_distance

    def _score_candidate(
        self,
        *,
        action_type: str,
        source: dict,
        target: dict,
        entry: dict,
        positive_total_weight: float,
        matched: dict,
        context_hits: int,
        closest_distance: int,
        new_components: list[str],
        absorb_scale: float,
    ) -> dict | None:
        max_component_count = max(2, int(self._config.get("max_event_component_count", 8)))
        if len(new_components) > max_component_count:
            return None

        edge_weight = max(0.0, float(entry.get("base_weight", 0.0) or 0.0))
        if edge_weight <= 0.0 or positive_total_weight <= 0.0:
            return None
        edge_ratio = edge_weight / positive_total_weight

        energy_balance = self._energy_balance_ratio(
            float(source.get("balance_energy", source.get("total_energy", 0.0))),
            float(target.get("balance_energy", target.get("total_energy", 0.0))),
        )
        runtime_balance = self._energy_balance_ratio(
            float(source.get("balance_weight", source.get("runtime_weight", 0.0))),
            float(target.get("balance_weight", target.get("runtime_weight", 0.0))),
        )

        max_context_k = max(1, int(self._config.get("max_context_k", 2)))
        context_ratio = round(float(context_hits) / max(1, min(len(source.get("components", []) or []), max_context_k)), 8)

        max_event_head_match_components = max(1, int(self._config.get("max_event_head_match_components", 3)))
        bridge_span_ratio = round(
            min(1.0, float(matched.get("matched_span", 1) or 1) / max(1, max_event_head_match_components)),
            8,
        )

        anchor_penalty = max(0.0, float(self._config.get("anchor_distance_penalty", 0.18)))
        anchor_scale = round(1.0 / (1.0 + anchor_penalty * max(0, int(closest_distance))), 8)

        drain_sig = self._candidate_signature(action_type=action_type, new_components=new_components)
        fatigue_before = round(float(self._pair_fatigue.get(drain_sig, 0.0) or 0.0), 8)
        fatigue_scale = self._fatigue_scale(fatigue_before)

        base_score = (
            float(self._config.get("edge_ratio_weight", 0.42)) * edge_ratio
            + float(self._config.get("energy_balance_weight", 0.23)) * energy_balance
            + float(self._config.get("match_strength_weight", 0.20)) * float(matched.get("strength", 0.0))
            + float(self._config.get("runtime_weight_weight", 0.15)) * runtime_balance
            + float(self._config.get("context_support_weight", 0.18)) * context_ratio
            + float(self._config.get("bridge_span_weight", 0.08)) * bridge_span_ratio
        )
        score = round(max(0.0, base_score * anchor_scale * fatigue_scale), 8)
        if score < max(0.0, float(self._config.get("min_candidate_score", 0.22))):
            return None

        return {
            "candidate_signature": drain_sig,
            "action_type": action_type,
            "source": source,
            "target": target,
            "new_components": list(new_components),
            "edge_target_id": str(entry.get("target_id", "") or ""),
            "edge_weight": round(edge_weight, 8),
            "edge_weight_ratio": round(edge_ratio, 8),
            "match_mode": matched.get("mode", ""),
            "match_strength": round(float(matched.get("strength", 0.0)), 8),
            "matched_span": int(matched.get("matched_span", 1) or 1),
            "prefix_components": int(matched.get("prefix_components", 1) or 1),
            "energy_balance": round(energy_balance, 8),
            "runtime_balance": round(runtime_balance, 8),
            "context_hits": int(context_hits),
            "context_ratio": context_ratio,
            "closest_distance": int(closest_distance),
            "bridge_span_ratio": bridge_span_ratio,
            "anchor_scale": anchor_scale,
            "fatigue_before": fatigue_before,
            "fatigue_scale": round(fatigue_scale, 8),
            "score": score,
            "absorb_scale": round(max(0.0, absorb_scale), 8),
        }

    def _apply_candidates(self, *, candidates: list[dict], pool, hdb, trace_id: str, tick_id: str) -> list[dict]:
        max_events = max(1, int(self._config.get("max_events_per_tick", 1)))
        min_event_total = max(0.0, float(self._config.get("min_event_total_energy", 0.10)))
        base_ratio = max(0.0, float(self._config.get("base_absorb_ratio", 0.12)))
        cap_ratio = max(base_ratio, float(self._config.get("pair_absorb_ratio_cap", 0.22)))
        fatigue_step = max(0.0, float(self._config.get("same_pair_fatigue_step", 0.35)))
        fatigue_cap = max(0.01, float(self._config.get("same_pair_fatigue_cap", 1.6)))

        structure_store = getattr(hdb, "_structure_store", None)
        persist_enabled = bool(self._config.get("enable_persist_events_to_hdb", False)) and hasattr(hdb, "upsert_cognitive_stitching_event_structure") and hasattr(hdb, "make_runtime_structure_object")
        max_diff_entries = int(self._config.get("persist_events_max_diff_entries", 96) or 96)
        max_diff_entries = max(0, min(512, max_diff_entries))

        actions: list[dict] = []
        for candidate in candidates[:max_events]:
            source = candidate["source"]
            target = candidate["target"]

            # Guard: if we cannot debit energy from both sides, do not create events (avoid energy creation).
            if not str(source.get("item_id", "") or "").strip() or not str(target.get("item_id", "") or "").strip():
                continue

            score = max(0.0, float(candidate.get("score", 0.0)))
            absorb_ratio = min(
                cap_ratio,
                base_ratio * (0.55 + score) * max(0.0, float(candidate.get("absorb_scale", 1.0))),
            )
            absorb_ratio = round(max(0.0, absorb_ratio), 8)

            src_er = round(max(0.0, float(source.get("er", 0.0))) * absorb_ratio, 8)
            src_ev = round(max(0.0, float(source.get("ev", 0.0))) * absorb_ratio, 8)
            tgt_er = round(max(0.0, float(target.get("er", 0.0))) * absorb_ratio, 8)
            tgt_ev = round(max(0.0, float(target.get("ev", 0.0))) * absorb_ratio, 8)
            event_er = round(src_er + tgt_er, 8)
            event_ev = round(src_ev + tgt_ev, 8)
            event_total = round(event_er + event_ev, 8)
            if event_total < min_event_total:
                continue

            component_refs = list(candidate.get("new_components", []) or [])
            event_id = self._event_ref_id_from_components(component_refs)
            event_display = self._event_display_from_components(
                self._resolve_component_displays(
                    components=list(component_refs),
                    structure_store=structure_store,
                )
            )

            action_type = str(candidate.get("action_type", "create_event") or "create_event")
            action_name = action_type
            event_item_id = ""
            event_structure_id = ""
            existing_item = None

            if persist_enabled:
                # Hot-path persistence: ensure the event is a HDB-backed "健全长期结构".
                component_state = self._build_event_component_state(
                    component_refs=list(component_refs),
                    component_displays=self._resolve_component_displays(components=list(component_refs), structure_store=structure_store),
                    source_components=list(source.get("components", []) or []),
                    target_components=list(target.get("components", []) or []),
                    source_absorbed_er=src_er,
                    source_absorbed_ev=src_ev,
                    target_absorbed_er=tgt_er,
                    target_absorbed_ev=tgt_ev,
                )
                cs_ext = {
                    "stage": "phase2_contextual_event_stitching",
                    "action_type": action_type,
                    "source_ref_id": str(source.get("ref_object_id", "") or ""),
                    "target_ref_id": str(target.get("ref_object_id", "") or ""),
                    "edge_target_id": str(candidate.get("edge_target_id", "") or ""),
                    "match_mode": str(candidate.get("match_mode", "") or ""),
                    "match_strength": round(float(candidate.get("match_strength", 0.0)), 8),
                    "edge_weight_ratio": round(float(candidate.get("edge_weight_ratio", 0.0)), 8),
                    "candidate_score": round(float(candidate.get("score", 0.0)), 8),
                    "context_k": int(candidate.get("context_hits", 1) or 1),
                    "matched_span": int(candidate.get("matched_span", 1) or 1),
                    "component_count": int(len(component_refs)),
                    "member_refs": list(component_refs),
                    "component_profile": list(component_state.get("component_profile", []) or []),
                    "component_ledger": list(component_state.get("component_ledger", []) or []),
                    "last_tick_id": str(tick_id or ""),
                }
                try:
                    persist_res = hdb.upsert_cognitive_stitching_event_structure(
                        event_ref_id=str(event_id),
                        member_refs=list(component_refs),
                        display_text=str(event_display or event_id),
                        diff_rows=None,  # keep HDB writes conservative on hot-path; idle_consolidate can fill outgoing edges.
                        trace_id=f"{trace_id}_cs_persist_hot",
                        tick_id=tick_id,
                        reason=f"cognitive_stitching_hot_path:{action_type}",
                        max_diff_entries=int(max_diff_entries),
                        sequence_groups=None,
                        flat_tokens=None,
                        cs_ext=cs_ext,
                        link_members_to_event=True,
                    )
                except Exception as exc:
                    persist_res = {"success": False, "code": "EXCEPTION", "message": str(exc), "data": {"event_ref_id": str(event_id)}}

                if not bool(persist_res.get("success", False)):
                    # If HDB persistence fails, do not create/modify runtime energies (keep the tick conservative).
                    continue
                event_structure_id = str((persist_res.get("data", {}) or {}).get("structure_id", "") or "")
                if not event_structure_id:
                    continue

                # Locate existing runtime item by structure_id (primary), then by event_ref_id alias (legacy).
                existing_item = self._get_existing_state_item_by_ref(pool=pool, ref_object_id=event_structure_id)
                if existing_item is None:
                    existing_item = self._get_existing_state_item_by_ref(pool=pool, ref_object_id=event_id)

                if existing_item is not None:
                    action_name = f"reinforce_{action_type}"
                    self._ensure_event_component_state(item=existing_item)
                    event_item_id = str(existing_item.get("id", "") or "")
                    self._safe_bind_ref_alias(pool=pool, item_id=event_item_id, ref_alias_id=event_id)
                    self._safe_apply_energy_update(
                        pool=pool,
                        item_id=event_item_id,
                        delta_er=event_er,
                        delta_ev=event_ev,
                        trace_id=f"{trace_id}_cs_event_update",
                        tick_id=tick_id,
                        reason="cognitive_stitching_reinforce_event",
                    )
                    self._apply_event_component_ledger_delta(
                        item=existing_item,
                        component_refs=list(component_refs),
                        component_displays=self._resolve_component_displays(components=list(component_refs), structure_store=structure_store),
                        source_components=list(source.get("components", []) or []),
                        target_components=list(target.get("components", []) or []),
                        source_absorbed_er=src_er,
                        source_absorbed_ev=src_ev,
                        target_absorbed_er=tgt_er,
                        target_absorbed_ev=tgt_ev,
                    )
                else:
                    runtime_object = hdb.make_runtime_structure_object(
                        event_structure_id,
                        er=float(event_er),
                        ev=float(event_ev),
                        reason="cognitive_stitching_event_create",
                    )
                    if not isinstance(runtime_object, dict):
                        continue
                    if bool(self._config.get("insert_event_runtime_items_into_state_pool", False)):
                        insert_result = pool.insert_runtime_node(
                            runtime_object=runtime_object,
                            trace_id=f"{trace_id}_cs_insert",
                            tick_id=tick_id,
                            allow_merge=False,
                            source_module="cognitive_stitching",
                            reason="cognitive_stitching_event_create",
                        )
                        event_item_id = str(insert_result.get("data", {}).get("item_id", "") or "")
                        self._safe_bind_ref_alias(pool=pool, item_id=event_item_id, ref_alias_id=event_id)
                    else:
                        event_item_id = ""
            else:
                # Legacy runtime-only event path (event_id is ref_object_id).
                existing_item = self._get_existing_state_item_by_ref(pool=pool, ref_object_id=event_id)
                if existing_item is not None:
                    action_name = f"reinforce_{action_type}"
                    self._ensure_event_component_state(item=existing_item)
                    self._safe_apply_energy_update(
                        pool=pool,
                        item_id=str(existing_item.get("id", "") or ""),
                        delta_er=event_er,
                        delta_ev=event_ev,
                        trace_id=f"{trace_id}_cs_event_update",
                        tick_id=tick_id,
                        reason="cognitive_stitching_reinforce_event",
                    )
                    self._apply_event_component_ledger_delta(
                        item=existing_item,
                        component_refs=list(component_refs),
                        component_displays=self._resolve_component_displays(components=list(component_refs), structure_store=structure_store),
                        source_components=list(source.get("components", []) or []),
                        target_components=list(target.get("components", []) or []),
                        source_absorbed_er=src_er,
                        source_absorbed_ev=src_ev,
                        target_absorbed_er=tgt_er,
                        target_absorbed_ev=tgt_ev,
                    )
                    event_item_id = str(existing_item.get("id", "") or "")
                else:
                    runtime_object = self._build_event_runtime_object(
                        component_refs=list(component_refs),
                        event_id=event_id,
                        event_er=event_er,
                        event_ev=event_ev,
                        candidate=candidate,
                        tick_id=tick_id,
                        hdb=hdb,
                        source_absorbed_er=src_er,
                        source_absorbed_ev=src_ev,
                        target_absorbed_er=tgt_er,
                        target_absorbed_ev=tgt_ev,
                    )
                    if bool(self._config.get("insert_event_runtime_items_into_state_pool", False)):
                        insert_result = pool.insert_runtime_node(
                            runtime_object=runtime_object,
                            trace_id=f"{trace_id}_cs_insert",
                            tick_id=tick_id,
                            allow_merge=False,
                            source_module="cognitive_stitching",
                            reason="cognitive_stitching_event_create",
                        )
                        event_item_id = str(insert_result.get("data", {}).get("item_id", "") or "")
                    else:
                        event_item_id = ""

            if not str(event_item_id or "").strip():
                continue

            # Debit energy from both sides AFTER we know the event runtime item exists (avoid "energy loss" on failure).
            self._safe_apply_energy_update(
                pool=pool,
                item_id=str(source.get("item_id", "") or ""),
                delta_er=-src_er,
                delta_ev=-src_ev,
                trace_id=f"{trace_id}_cs_deduct",
                tick_id=tick_id,
                reason="cognitive_stitching_absorb_source",
            )
            self._safe_apply_energy_update(
                pool=pool,
                item_id=str(target.get("item_id", "") or ""),
                delta_er=-tgt_er,
                delta_ev=-tgt_ev,
                trace_id=f"{trace_id}_cs_deduct",
                tick_id=tick_id,
                reason="cognitive_stitching_absorb_target",
            )

            # ESDB overlay bookkeeping (parents+delta metadata, runtime-only).
            self._esdb_upsert_event(
                event_ref_id=event_id,
                components=list(component_refs),
                parent_refs=[
                    str(source.get("ref_object_id", "") or ""),
                    str(target.get("ref_object_id", "") or ""),
                ],
                tick_id=tick_id,
                action_type=str(candidate.get("action_type", "") or "create_event"),
            )
            # Delta update: cache a small outgoing edge set from tail components for event_overlay mode.
            try:
                self._esdb_refresh_delta_from_tail_components(
                    event_ref_id=event_id,
                    components=list(component_refs),
                    hdb=hdb,
                    tick_id=tick_id,
                    action_type=str(candidate.get("action_type", "") or "create_event"),
                )
            except Exception:
                pass

            fatigue_after = round(min(fatigue_cap, float(candidate.get("fatigue_before", 0.0)) + fatigue_step), 8)
            self._pair_fatigue[str(candidate.get("candidate_signature", "") or "")] = fatigue_after
            actions.append(
                {
                    "action": action_name,
                    "action_family": str(candidate.get("action_type", "") or "create_event"),
                    "event_ref_id": event_id,
                    "event_structure_id": str(event_structure_id or ""),
                    "event_item_id": event_item_id,
                    "event_display": event_display,
                    "event_component_count": len(list(candidate.get("new_components", []) or [])),
                    "source_ref_id": str(source.get("ref_object_id", "") or ""),
                    "source_display": str(source.get("display", "") or ""),
                    "source_kind": str(source.get("kind", "") or ""),
                    "target_ref_id": str(target.get("ref_object_id", "") or ""),
                    "target_display": str(target.get("display", "") or ""),
                    "target_kind": str(target.get("kind", "") or ""),
                    "edge_target_id": str(candidate.get("edge_target_id", "") or ""),
                    "match_mode": str(candidate.get("match_mode", "") or ""),
                    "matched_span": int(candidate.get("matched_span", 1) or 1),
                    "prefix_components": int(candidate.get("prefix_components", 1) or 1),
                    "context_k": int(candidate.get("context_hits", 1) or 1),
                    "context_distance": int(candidate.get("closest_distance", 0) or 0),
                    "score": round(score, 8),
                    "edge_weight": round(float(candidate.get("edge_weight", 0.0)), 8),
                    "edge_weight_ratio": round(float(candidate.get("edge_weight_ratio", 0.0)), 8),
                    "match_strength": round(float(candidate.get("match_strength", 0.0)), 8),
                    "absorb_ratio": absorb_ratio,
                    "absorbed_er": event_er,
                    "absorbed_ev": event_ev,
                    "absorbed_total": event_total,
                    "source_absorbed_er": src_er,
                    "source_absorbed_ev": src_ev,
                    "target_absorbed_er": tgt_er,
                    "target_absorbed_ev": tgt_ev,
                    "fatigue_before": round(float(candidate.get("fatigue_before", 0.0)), 8),
                    "fatigue_after": fatigue_after,
                }
            )
        return actions

    def _maybe_degenerate_events(self, *, pool, hdb, trace_id: str, tick_id: str) -> dict[str, Any]:
        """
        Event degeneration / 事件退化（组分淘汰）
        ---------------------------------------

        Goal:
        - When a CS event contains some very weak components, the event may "degenerate"
          into a shorter event. This keeps long-run event structures healthy and avoids
          "ultra-long / noisy events" dominating the pool.

        What we guarantee:
        - If persistence is enabled, the degenerated event is persisted into HDB via
          `upsert_cognitive_stitching_event_structure`, so the new structure is indexable
          and discoverable by stimulus-level retrieval (when enabled).
        - Energy conservation: we move ER/EV from the old event item to the new/target
          event item, then set the old one to ~0 energy (so it will be pruned naturally).

        Design constraints:
        - Purely numeric rules (share + absolute energy); no semantic hacks.
        - Bounded work per tick.
        - Best-effort: never raising to avoid breaking the main tick pipeline.
        """
        enabled = bool(self._config.get("enable_event_degeneration", True))
        max_events = int(self._config.get("event_degeneration_max_events_per_tick", 2) or 2)
        max_events = max(0, min(32, max_events))
        min_components = int(self._config.get("event_degeneration_min_components", 2) or 2)
        min_components = max(2, min(16, min_components))
        share_threshold = float(self._config.get("event_degeneration_share_threshold", 0.06) or 0.06)
        share_threshold = max(0.0, min(1.0, share_threshold))
        min_component_energy = float(self._config.get("event_degeneration_min_component_energy", 0.04) or 0.04)
        min_component_energy = max(0.0, float(min_component_energy))

        if not enabled:
            return {
                "enabled": False,
                "reason": "disabled_by_config",
                "max_events_per_tick": int(max_events),
                "min_components": int(min_components),
                "share_threshold": float(share_threshold),
                "min_component_energy": float(min_component_energy),
                "degenerated_count": 0,
                "actions_preview": [],
            }

        persist_enabled = (
            bool(self._config.get("enable_persist_events_to_hdb", False))
            and hasattr(hdb, "upsert_cognitive_stitching_event_structure")
            and hasattr(hdb, "make_runtime_structure_object")
        )
        if not persist_enabled:
            return {
                "enabled": True,
                "reason": "persist_disabled",
                "max_events_per_tick": int(max_events),
                "min_components": int(min_components),
                "share_threshold": float(share_threshold),
                "min_component_energy": float(min_component_energy),
                "degenerated_count": 0,
                "actions_preview": [],
            }

        store = getattr(pool, "_store", None)
        if store is None or not hasattr(store, "get_all"):
            return {
                "enabled": True,
                "reason": "pool_store_missing",
                "max_events_per_tick": int(max_events),
                "min_components": int(min_components),
                "share_threshold": float(share_threshold),
                "min_component_energy": float(min_component_energy),
                "degenerated_count": 0,
                "actions_preview": [],
            }

        structure_store = getattr(hdb, "_structure_store", None)
        max_diff_entries = int(self._config.get("persist_events_max_diff_entries", 96) or 96)
        max_diff_entries = max(0, min(512, max_diff_entries))

        # Pick candidate events by total energy (descending).
        try:
            raw_items = [item for item in list(store.get_all()) if self._is_cognitive_stitching_event_state_item(item)]
        except Exception:
            raw_items = []
        raw_items.sort(
            key=lambda it: float((it.get("energy", {}) or {}).get("er", 0.0) or 0.0) + float((it.get("energy", {}) or {}).get("ev", 0.0) or 0.0),
            reverse=True,
        )

        actions: list[dict[str, Any]] = []
        handled = 0
        for item in raw_items:
            if max_events <= 0 or handled >= max_events:
                break

            source_item_id = str(item.get("id", "") or "").strip()
            if not source_item_id:
                continue

            # Canonical event id (cs_event::<...>) and its component list.
            old_event_ref_id = self._extract_event_ref_id_from_state_item(item)
            if not old_event_ref_id or not self._is_event_ref_id(old_event_ref_id):
                continue
            old_components = list(self._parse_event_components(old_event_ref_id))
            if len(old_components) <= min_components:
                continue

            energy = item.get("energy", {}) if isinstance(item.get("energy", {}), dict) else {}
            old_er = round(max(0.0, float(energy.get("er", 0.0) or 0.0)), 8)
            old_ev = round(max(0.0, float(energy.get("ev", 0.0) or 0.0)), 8)
            old_total = round(old_er + old_ev, 8)
            if old_total <= 1e-9:
                continue

            # Extract component shares from ledger (if present). If missing, fall back to uniform shares.
            cs_meta = self._ensure_event_component_state(item=item)
            ledger = list((cs_meta or {}).get("component_ledger", []) or [])
            share_by_ref_raw: dict[str, float] = {}
            for entry in ledger:
                if not isinstance(entry, dict):
                    continue
                ref_id = str(entry.get("ref_id", "") or "").strip()
                if not ref_id:
                    continue
                try:
                    share_by_ref_raw[ref_id] = max(0.0, float(entry.get("profile_share", 0.0) or 0.0))
                except Exception:
                    continue

            shares: dict[str, float] = {}
            if share_by_ref_raw:
                total_share = 0.0
                for ref_id in old_components:
                    total_share += float(share_by_ref_raw.get(ref_id, 0.0) or 0.0)
                if total_share > 1e-9:
                    for ref_id in old_components:
                        shares[ref_id] = float(share_by_ref_raw.get(ref_id, 0.0) or 0.0) / total_share

            if not shares:
                uniform = 1.0 / float(max(1, len(old_components)))
                shares = {ref_id: uniform for ref_id in old_components}

            removed: list[str] = []
            kept: list[str] = []
            for ref_id in old_components:
                share = float(shares.get(ref_id, 0.0) or 0.0)
                component_energy = share * old_total
                if share < share_threshold and component_energy < min_component_energy:
                    removed.append(ref_id)
                else:
                    kept.append(ref_id)

            # If we would collapse below the minimum component count, keep the strongest components as a skeleton.
            # 若阈值规则会把事件“全部删光/删到不足最少保留组分数”，则保留最强的若干组分作为骨架，
            # 让超长事件能够自然退化为更短、可叙事的核心事件。
            if len(kept) < min_components and len(old_components) >= min_components:
                kept_set = set(kept)
                ranked = sorted(
                    list(old_components),
                    key=lambda rid: (
                        float(shares.get(rid, 0.0) or 0.0),
                        float(shares.get(rid, 0.0) or 0.0) * float(old_total),
                    ),
                    reverse=True,
                )
                for rid in ranked:
                    if rid in kept_set:
                        continue
                    kept.append(rid)
                    kept_set.add(rid)
                    if len(kept) >= min_components:
                        break
                removed = [rid for rid in old_components if rid not in kept_set]

            # Guard: do not degenerate if nothing is removed, or we would collapse below min_components.
            if not removed:
                continue
            if len(kept) < min_components:
                continue

            new_event_ref_id = self._event_ref_id_from_components(kept)
            if not new_event_ref_id or new_event_ref_id == old_event_ref_id:
                continue

            # Persist (or resolve) the new event structure in HDB.
            new_displays = self._resolve_component_displays(components=list(kept), structure_store=structure_store)
            new_display_text = self._event_display_from_components(new_displays) or str(new_event_ref_id)
            try:
                persist_res = hdb.upsert_cognitive_stitching_event_structure(
                    event_ref_id=str(new_event_ref_id),
                    member_refs=list(kept),
                    display_text=str(new_display_text),
                    diff_rows=None,
                    trace_id=f"{trace_id}_cs_degenerate",
                    tick_id=tick_id,
                    reason="cognitive_stitching_event_degeneration",
                    max_diff_entries=int(max_diff_entries),
                    sequence_groups=None,
                    flat_tokens=None,
                    cs_ext={
                        "stage": "event_degeneration",
                        "source_event_ref_id": str(old_event_ref_id),
                        "removed_component_refs": list(removed),
                        "kept_component_refs": list(kept),
                        "last_tick_id": str(tick_id or ""),
                    },
                    link_members_to_event=True,
                )
            except Exception as exc:
                persist_res = {"success": False, "code": "EXCEPTION", "message": str(exc), "data": {}}
            if not bool(persist_res.get("success", False)):
                continue
            new_structure_id = str((persist_res.get("data", {}) or {}).get("structure_id", "") or "").strip()
            if not new_structure_id:
                continue

            # Locate or create the target runtime item, then move energy into it.
            target_item = self._get_existing_state_item_by_ref(pool=pool, ref_object_id=new_structure_id) or self._get_existing_state_item_by_ref(pool=pool, ref_object_id=new_event_ref_id)
            target_item_id = str((target_item or {}).get("id", "") or "").strip()
            created_target_item = False

            if target_item is None:
                runtime_object = hdb.make_runtime_structure_object(
                    new_structure_id,
                    er=float(old_er),
                    ev=float(old_ev),
                    reason="cognitive_stitching_event_degeneration",
                )
                if not isinstance(runtime_object, dict):
                    continue
                insert_res = pool.insert_runtime_node(
                    runtime_object=runtime_object,
                    trace_id=f"{trace_id}_cs_degenerate_insert",
                    tick_id=tick_id,
                    allow_merge=True,  # safe: we already checked get_by_ref; allow semantic merge if needed
                    source_module="cognitive_stitching",
                    reason="cognitive_stitching_event_degeneration",
                )
                insert_data = insert_res.get("data", {}) if isinstance(insert_res.get("data", {}), dict) else {}
                target_item_id = str(insert_data.get("item_id", "") or insert_data.get("target_item_id", "") or "").strip()
                if not target_item_id:
                    continue
                created_target_item = bool(insert_data.get("inserted", False)) and not bool(insert_data.get("merged", False))
            else:
                # Transfer-in to an existing item.
                self._safe_apply_energy_update(
                    pool=pool,
                    item_id=target_item_id,
                    delta_er=float(old_er),
                    delta_ev=float(old_ev),
                    trace_id=f"{trace_id}_cs_degenerate_in",
                    tick_id=tick_id,
                    reason="cognitive_stitching_event_degeneration_transfer_in",
                )

            # Bind the canonical event_ref_id as an alias to the target item.
            self._safe_bind_ref_alias(pool=pool, item_id=target_item_id, ref_alias_id=new_event_ref_id)

            # Transfer-out from the old event item (energy conservation).
            self._safe_apply_energy_update(
                pool=pool,
                item_id=source_item_id,
                delta_er=-float(old_er),
                delta_ev=-float(old_ev),
                trace_id=f"{trace_id}_cs_degenerate_out",
                tick_id=tick_id,
                reason="cognitive_stitching_event_degeneration_transfer_out",
            )

            # Patch a fresh component ledger onto the target item (so later component-neutralization is well-defined).
            try:
                target_state_item = store.get(target_item_id) if hasattr(store, "get") else None
            except Exception:
                target_state_item = None
            if isinstance(target_state_item, dict):
                target_cs_meta = self._ensure_event_component_state(item=target_state_item)
                display_by_ref = {
                    ref_id: disp
                    for ref_id, disp in zip(list(kept), list(new_displays))
                    if str(ref_id) and str(disp)
                }
                normalized_ledger: list[dict[str, Any]] = []
                profile_rows: list[dict[str, Any]] = []
                # Normalize shares within kept components (sum=1).
                share_sum = sum(float(shares.get(ref_id, 0.0) or 0.0) for ref_id in kept)
                if share_sum <= 1e-9:
                    share_sum = 1.0
                for index, ref_id in enumerate(kept):
                    share = float(shares.get(ref_id, 0.0) or 0.0) / share_sum
                    disp = display_by_ref.get(ref_id, ref_id)
                    entry_er = round(old_er * share, 8)
                    entry_ev = round(old_ev * share, 8)
                    normalized_ledger.append(
                        {
                            "index": int(index),
                            "ref_id": ref_id,
                            "display": disp,
                            "tokens": [disp] if disp else [],
                            "profile_share": round(share, 8),
                            "er": entry_er,
                            "ev": entry_ev,
                            "cp_abs": round(abs(entry_er - entry_ev), 8),
                        }
                    )
                    profile_rows.append({"index": int(index), "ref_id": ref_id, "display": disp, "share": round(share, 8)})
                target_cs_meta["event_ref_id"] = str(new_event_ref_id)
                target_cs_meta["member_refs"] = list(kept)
                target_cs_meta["component_ledger"] = normalized_ledger
                target_cs_meta["component_profile"] = profile_rows
                target_cs_meta["degenerated_from_event_ref_id"] = str(old_event_ref_id)
                target_cs_meta["last_tick_id"] = str(tick_id or "")
                try:
                    store.update(target_item_id, target_state_item)
                except Exception:
                    pass

            handled += 1
            actions.append(
                {
                    "action": "degenerate_event",
                    "source_item_id": source_item_id,
                    "source_structure_id": str(item.get("ref_object_id", "") or ""),
                    "source_event_ref_id": str(old_event_ref_id),
                    "target_item_id": target_item_id,
                    "target_structure_id": str(new_structure_id),
                    "target_event_ref_id": str(new_event_ref_id),
                    "removed_component_refs": list(removed),
                    "kept_component_refs": list(kept),
                    "transferred_er": float(old_er),
                    "transferred_ev": float(old_ev),
                    "transferred_total": float(old_total),
                    "created_target_item": bool(created_target_item),
                }
            )

        return {
            "enabled": True,
            "reason": "ok",
            "max_events_per_tick": int(max_events),
            "min_components": int(min_components),
            "share_threshold": float(share_threshold),
            "min_component_energy": float(min_component_energy),
            "candidate_event_count": len(raw_items),
            "degenerated_count": int(handled),
            "actions_preview": actions[:12],
        }

    def _build_event_runtime_object(
        self,
        *,
        component_refs: list[str],
        event_id: str,
        event_er: float,
        event_ev: float,
        candidate: dict,
        tick_id: str,
        hdb,
        source_absorbed_er: float,
        source_absorbed_ev: float,
        target_absorbed_er: float,
        target_absorbed_ev: float,
    ) -> dict:
        component_displays = self._resolve_component_displays(
            components=list(component_refs),
            structure_store=getattr(hdb, "_structure_store", None),
        )
        if not component_displays:
            component_displays = list(component_refs)
        display = self._event_display_from_components(component_displays)
        signature = f"{event_id}|{'|'.join(component_refs)}"
        component_state = self._build_event_component_state(
            component_refs=list(component_refs),
            component_displays=list(component_displays),
            source_components=list(candidate.get("source", {}).get("components", []) or []),
            target_components=list(candidate.get("target", {}).get("components", []) or []),
            source_absorbed_er=source_absorbed_er,
            source_absorbed_ev=source_absorbed_ev,
            target_absorbed_er=target_absorbed_er,
            target_absorbed_ev=target_absorbed_ev,
        )
        return {
            "id": event_id,
            "object_type": "st",
            "sub_type": "cognitive_stitching_event",
            "content": {"raw": display, "display": display, "normalized": signature},
            "energy": {
                "er": round(event_er, 8),
                "ev": round(event_ev, 8),
                "ownership_level": "aggregated_from_st",
                "computed_from_children": True,
            },
            "structure": {
                "display_text": display,
                "flat_tokens": list(component_displays),
                "token_count": len(component_displays),
                "sequence_groups": [
                    {
                        "group_index": index,
                        "source_type": "cognitive_stitching",
                        "origin_frame_id": tick_id,
                        "tokens": [component_display],
                    }
                    for index, component_display in enumerate(component_displays)
                ],
                "member_refs": list(component_refs),
                "content_signature": signature,
                "semantic_signature": signature,
                "ext": {
                    "cognitive_stitching": {
                        "stage": "phase2_contextual_event_stitching",
                        "action_type": str(candidate.get("action_type", "") or ""),
                        "source_ref_id": str(candidate.get("source", {}).get("ref_object_id", "") or ""),
                        "target_ref_id": str(candidate.get("target", {}).get("ref_object_id", "") or ""),
                        "edge_target_id": str(candidate.get("edge_target_id", "") or ""),
                        "match_mode": str(candidate.get("match_mode", "") or ""),
                        "match_strength": round(float(candidate.get("match_strength", 0.0)), 8),
                        "edge_weight_ratio": round(float(candidate.get("edge_weight_ratio", 0.0)), 8),
                        "candidate_score": round(float(candidate.get("score", 0.0)), 8),
                        "context_k": int(candidate.get("context_hits", 1) or 1),
                        "matched_span": int(candidate.get("matched_span", 1) or 1),
                        "component_count": len(component_refs),
                        "member_refs": list(component_refs),
                        "component_profile": list(component_state.get("component_profile", [])),
                        "component_ledger": list(component_state.get("component_ledger", [])),
                    }
                },
            },
            "source": {
                "parent_ids": [
                    str(candidate.get("source", {}).get("ref_object_id", "") or ""),
                    str(candidate.get("target", {}).get("ref_object_id", "") or ""),
                ],
            },
        }

    def _build_event_component_state(
        self,
        *,
        component_refs: list[str],
        component_displays: list[str],
        source_components: list[str],
        target_components: list[str],
        source_absorbed_er: float,
        source_absorbed_ev: float,
        target_absorbed_er: float,
        target_absorbed_ev: float,
    ) -> dict[str, list[dict]]:
        display_by_ref: dict[str, str] = {}
        for index, component_ref in enumerate(component_refs):
            display = ""
            if index < len(component_displays):
                display = str(component_displays[index] or "")
            if not display:
                display = str(component_ref or "")
            display_by_ref[str(component_ref or "")] = display

        ledger_by_ref: dict[str, dict[str, Any]] = {}
        for component_ref in component_refs:
            ref_id = str(component_ref or "")
            if not ref_id:
                continue
            display = display_by_ref.get(ref_id, ref_id)
            ledger_by_ref[ref_id] = {
                "ref_id": ref_id,
                "display": display,
                "tokens": [display] if display else [],
                "profile_share": 0.0,
                "er": 0.0,
                "ev": 0.0,
            }

        self._distribute_absorbed_energy_to_components(
            ledger_by_ref=ledger_by_ref,
            component_refs=list(component_refs),
            contributor_components=list(source_components),
            delta_er=float(source_absorbed_er),
            delta_ev=float(source_absorbed_ev),
        )
        self._distribute_absorbed_energy_to_components(
            ledger_by_ref=ledger_by_ref,
            component_refs=list(component_refs),
            contributor_components=list(target_components),
            delta_er=float(target_absorbed_er),
            delta_ev=float(target_absorbed_ev),
        )

        total_energy = round(
            max(0.0, float(source_absorbed_er) + float(source_absorbed_ev) + float(target_absorbed_er) + float(target_absorbed_ev)),
            8,
        )
        component_profile: list[dict] = []
        component_ledger: list[dict] = []
        component_count = max(1, len(component_refs))
        fallback_share = round(1.0 / float(component_count), 8)
        for index, component_ref in enumerate(component_refs):
            ref_id = str(component_ref or "")
            entry = ledger_by_ref.get(ref_id) or {
                "ref_id": ref_id,
                "display": display_by_ref.get(ref_id, ref_id),
                "tokens": [display_by_ref.get(ref_id, ref_id)] if display_by_ref.get(ref_id, ref_id) else [],
                "profile_share": 0.0,
                "er": 0.0,
                "ev": 0.0,
            }
            entry_total = round(max(0.0, float(entry.get("er", 0.0)) + float(entry.get("ev", 0.0))), 8)
            profile_share = (
                round(entry_total / total_energy, 8)
                if total_energy > 0.0
                else fallback_share
            )
            entry["profile_share"] = profile_share
            entry["er"] = round(max(0.0, float(entry.get("er", 0.0))), 8)
            entry["ev"] = round(max(0.0, float(entry.get("ev", 0.0))), 8)
            entry["cp_abs"] = round(abs(float(entry["er"]) - float(entry["ev"])), 8)
            component_profile.append(
                {
                    "index": index,
                    "ref_id": ref_id,
                    "display": str(entry.get("display", "") or ref_id),
                    "share": profile_share,
                }
            )
            component_ledger.append(
                {
                    "index": index,
                    "ref_id": ref_id,
                    "display": str(entry.get("display", "") or ref_id),
                    "tokens": list(entry.get("tokens", []) or []),
                    "profile_share": profile_share,
                    "er": entry["er"],
                    "ev": entry["ev"],
                    "cp_abs": entry["cp_abs"],
                }
            )
        return {
            "component_profile": component_profile,
            "component_ledger": component_ledger,
        }

    @staticmethod
    def _distribute_absorbed_energy_to_components(
        *,
        ledger_by_ref: dict[str, dict[str, Any]],
        component_refs: list[str],
        contributor_components: list[str],
        delta_er: float,
        delta_ev: float,
    ) -> None:
        refs = [
            str(ref_id or "")
            for ref_id in (contributor_components or component_refs or [])
            if str(ref_id or "")
        ]
        if not refs:
            return
        share = 1.0 / float(len(refs))
        for ref_id in refs:
            entry = ledger_by_ref.get(ref_id)
            if entry is None:
                continue
            entry["er"] = round(float(entry.get("er", 0.0)) + max(0.0, float(delta_er)) * share, 8)
            entry["ev"] = round(float(entry.get("ev", 0.0)) + max(0.0, float(delta_ev)) * share, 8)

    @staticmethod
    def _ensure_event_component_state(*, item: dict) -> dict[str, Any]:
        meta = item.setdefault("meta", {})
        meta_ext = meta.setdefault("ext", {})
        cs_meta = meta_ext.get("cognitive_stitching")
        if not isinstance(cs_meta, dict):
            ref_snapshot = item.get("ref_snapshot", {}) or {}
            structure_ext = ref_snapshot.get("structure_ext", {}) or {}
            ref_cs_meta = structure_ext.get("cognitive_stitching")
            cs_meta = dict(ref_cs_meta) if isinstance(ref_cs_meta, dict) else {}
            meta_ext["cognitive_stitching"] = cs_meta
        cs_meta.setdefault("member_refs", list((item.get("ref_snapshot", {}) or {}).get("member_refs", []) or []))
        cs_meta.setdefault("component_profile", [])
        cs_meta.setdefault("component_ledger", [])
        return cs_meta

    def _apply_event_component_ledger_delta(
        self,
        *,
        item: dict,
        component_refs: list[str],
        component_displays: list[str],
        source_components: list[str],
        target_components: list[str],
        source_absorbed_er: float,
        source_absorbed_ev: float,
        target_absorbed_er: float,
        target_absorbed_ev: float,
    ) -> None:
        cs_meta = self._ensure_event_component_state(item=item)
        ledger = list(cs_meta.get("component_ledger", []) or [])
        if not ledger:
            ref_snapshot = item.get("ref_snapshot", {}) or {}
            existing_refs = list(cs_meta.get("member_refs", []) or ref_snapshot.get("member_refs", []) or component_refs)
            existing_displays = list(ref_snapshot.get("flat_tokens", []) or component_displays)
            fallback_state = self._build_event_component_state(
                component_refs=existing_refs,
                component_displays=existing_displays,
                source_components=list(existing_refs),
                target_components=[],
                source_absorbed_er=float(item.get("energy", {}).get("er", 0.0) or 0.0),
                source_absorbed_ev=float(item.get("energy", {}).get("ev", 0.0) or 0.0),
                target_absorbed_er=0.0,
                target_absorbed_ev=0.0,
            )
            ledger = list(fallback_state.get("component_ledger", []) or [])
            cs_meta["component_profile"] = list(fallback_state.get("component_profile", []) or [])
            cs_meta["member_refs"] = list(existing_refs)

        ledger_by_ref = {
            str(entry.get("ref_id", "") or ""): entry
            for entry in ledger
            if str(entry.get("ref_id", "") or "")
        }
        self._distribute_absorbed_energy_to_components(
            ledger_by_ref=ledger_by_ref,
            component_refs=list(component_refs),
            contributor_components=list(source_components),
            delta_er=float(source_absorbed_er),
            delta_ev=float(source_absorbed_ev),
        )
        self._distribute_absorbed_energy_to_components(
            ledger_by_ref=ledger_by_ref,
            component_refs=list(component_refs),
            contributor_components=list(target_components),
            delta_er=float(target_absorbed_er),
            delta_ev=float(target_absorbed_ev),
        )

        total_energy = round(
            sum(
                max(0.0, float(entry.get("er", 0.0))) + max(0.0, float(entry.get("ev", 0.0)))
                for entry in ledger_by_ref.values()
            ),
            8,
        )
        normalized_ledger: list[dict] = []
        normalized_profile: list[dict] = []
        member_refs = list(cs_meta.get("member_refs", []) or component_refs)
        for index, ref_id in enumerate(member_refs):
            ref_text = str(ref_id or "")
            entry = ledger_by_ref.get(ref_text)
            if entry is None:
                display = ""
                if index < len(component_displays):
                    display = str(component_displays[index] or "")
                entry = {
                    "index": index,
                    "ref_id": ref_text,
                    "display": display or ref_text,
                    "tokens": [display] if display else [],
                    "profile_share": 0.0,
                    "er": 0.0,
                    "ev": 0.0,
                }
            entry["index"] = index
            entry["er"] = round(max(0.0, float(entry.get("er", 0.0))), 8)
            entry["ev"] = round(max(0.0, float(entry.get("ev", 0.0))), 8)
            entry["cp_abs"] = round(abs(float(entry["er"]) - float(entry["ev"])), 8)
            current_total = round(float(entry["er"]) + float(entry["ev"]), 8)
            profile_share = round(current_total / total_energy, 8) if total_energy > 0.0 else round(1.0 / float(max(1, len(member_refs))), 8)
            entry["profile_share"] = profile_share
            normalized_ledger.append(
                {
                    "index": index,
                    "ref_id": ref_text,
                    "display": str(entry.get("display", "") or ref_text),
                    "tokens": list(entry.get("tokens", []) or []),
                    "profile_share": profile_share,
                    "er": entry["er"],
                    "ev": entry["ev"],
                    "cp_abs": entry["cp_abs"],
                }
            )
            normalized_profile.append(
                {
                    "index": index,
                    "ref_id": ref_text,
                    "display": str(entry.get("display", "") or ref_text),
                    "share": profile_share,
                }
            )

        cs_meta["component_ledger"] = normalized_ledger
        cs_meta["component_profile"] = normalized_profile

    def _collect_narrative_top_items(self, *, pool, trace_id: str, tick_id: str) -> list[dict]:
        store = getattr(pool, "_store", None)
        if store is not None and hasattr(store, "get_all"):
            try:
                raw_items = [
                    item
                    for item in list(store.get_all())
                    if self._is_cognitive_stitching_event_state_item(item)
                ]
                raw_items.sort(
                    key=lambda item: (
                        float(item.get("energy", {}).get("er", 0.0) or 0.0)
                        + float(item.get("energy", {}).get("ev", 0.0) or 0.0),
                        float(item.get("energy", {}).get("salience_score", 0.0) or 0.0),
                    ),
                    reverse=True,
                )
                narrative = []
                for item in raw_items[: max(1, int(self._config.get("narrative_top_k", 6)))]:
                    structure_id = str(item.get("ref_object_id", "") or "")
                    event_ref_id = self._extract_event_ref_id_from_state_item(item) or structure_id
                    member_refs = list((item.get("ref_snapshot", {}) or {}).get("member_refs", []) or [])
                    component_count = len(self._parse_event_components(event_ref_id)) if self._is_event_ref_id(event_ref_id) else len(member_refs)
                    es_entry = self._esdb.get(event_ref_id)
                    es_parent_depth = self._esdb_parent_depth(event_ref_id, set()) if isinstance(es_entry, dict) else 0
                    narrative.append(
                        {
                            "item_id": str(item.get("id", "") or ""),
                            "ref_object_id": event_ref_id,
                            "structure_id": structure_id,
                            "display": str(item.get("ref_snapshot", {}).get("content_display", "") or event_ref_id),
                            "er": round(float(item.get("energy", {}).get("er", 0.0) or 0.0), 8),
                            "ev": round(float(item.get("energy", {}).get("ev", 0.0) or 0.0), 8),
                            "cp_abs": round(float(item.get("energy", {}).get("cognitive_pressure_abs", 0.0) or 0.0), 8),
                            "salience_score": round(float(item.get("energy", {}).get("salience_score", 0.0) or 0.0), 8),
                            "event_grasp": self._extract_bound_numerical_attribute(
                                item,
                                attribute_name=str(self._config.get("event_grasp_attribute_name", "event_grasp") or "event_grasp"),
                            ),
                            "total_energy": round(
                                float(item.get("energy", {}).get("er", 0.0) or 0.0)
                                + float(item.get("energy", {}).get("ev", 0.0) or 0.0),
                                8,
                            ),
                            "component_count": int(component_count),
                            "esdb_parent_depth": int(es_parent_depth),
                            "esdb_parent_count": len(list(es_entry.get("parents", []) or [])) if isinstance(es_entry, dict) else 0,
                            "esdb_delta_entry_count": len(list(es_entry.get("delta_diff_table", []) or [])) if isinstance(es_entry, dict) else 0,
                            "esdb_materialized": bool(es_entry.get("materialized", False)) if isinstance(es_entry, dict) else False,
                            "esdb_materialized_entry_count": len(list(es_entry.get("materialized_diff_table", []) or [])) if isinstance(es_entry, dict) else 0,
                            "esdb_update_count": int(es_entry.get("update_count", 0) or 0) if isinstance(es_entry, dict) else 0,
                        }
                    )
                return narrative
            except Exception:
                pass

        snapshot_resp = pool.get_state_snapshot(
            trace_id=f"{trace_id}_cs_post",
            tick_id=tick_id,
            top_k=max(12, int(self._config.get("snapshot_top_k", 24))),
            sort_by="cp_abs",
        )
        if not snapshot_resp.get("success"):
            return []
        snapshot = snapshot_resp.get("data", {}).get("snapshot", {}) or {}
        items = [item for item in list(snapshot.get("top_items", []) or []) if str(item.get("ref_object_type", "") or "") == "st"]
        narrative = []
        limit = max(1, int(self._config.get("narrative_top_k", 6)))
        for item in items:
            if len(narrative) >= limit:
                break
            item_id = str(item.get("item_id", "") or "").strip()
            state_item = self._get_state_item_by_id(pool=pool, item_id=item_id)
            if isinstance(state_item, dict):
                if not self._is_cognitive_stitching_event_state_item(state_item):
                    continue
                structure_id = str(state_item.get("ref_object_id", "") or "")
                event_ref_id = self._extract_event_ref_id_from_state_item(state_item) or structure_id
                member_refs = list((state_item.get("ref_snapshot", {}) or {}).get("member_refs", []) or [])
                component_count = len(self._parse_event_components(event_ref_id)) if self._is_event_ref_id(event_ref_id) else len(member_refs)
                es_entry = self._esdb.get(event_ref_id)
                es_parent_depth = self._esdb_parent_depth(event_ref_id, set()) if isinstance(es_entry, dict) else 0
                narrative.append(
                    {
                        "item_id": item_id,
                        "ref_object_id": event_ref_id,
                        "structure_id": structure_id,
                        "display": str(state_item.get("ref_snapshot", {}).get("content_display", "") or event_ref_id),
                        "er": round(float(state_item.get("energy", {}).get("er", 0.0) or 0.0), 8),
                        "ev": round(float(state_item.get("energy", {}).get("ev", 0.0) or 0.0), 8),
                        "cp_abs": round(float(state_item.get("energy", {}).get("cognitive_pressure_abs", 0.0) or 0.0), 8),
                        "salience_score": round(float(state_item.get("energy", {}).get("salience_score", 0.0) or 0.0), 8),
                        "event_grasp": self._extract_bound_numerical_attribute(
                            state_item,
                            attribute_name=str(self._config.get("event_grasp_attribute_name", "event_grasp") or "event_grasp"),
                        ),
                        "total_energy": round(
                            float(state_item.get("energy", {}).get("er", 0.0) or 0.0)
                            + float(state_item.get("energy", {}).get("ev", 0.0) or 0.0),
                            8,
                        ),
                        "component_count": int(component_count),
                        "esdb_parent_depth": int(es_parent_depth),
                        "esdb_parent_count": len(list(es_entry.get("parents", []) or [])) if isinstance(es_entry, dict) else 0,
                        "esdb_delta_entry_count": len(list(es_entry.get("delta_diff_table", []) or [])) if isinstance(es_entry, dict) else 0,
                        "esdb_materialized": bool(es_entry.get("materialized", False)) if isinstance(es_entry, dict) else False,
                        "esdb_materialized_entry_count": len(list(es_entry.get("materialized_diff_table", []) or [])) if isinstance(es_entry, dict) else 0,
                        "esdb_update_count": int(es_entry.get("update_count", 0) or 0) if isinstance(es_entry, dict) else 0,
                    }
                )
                continue

            # Very minimal fallback for unit tests: when PoolStore doesn't support `get()`,
            # we can still surface legacy runtime-only events that use cs_event::<...> as ref_object_id.
            legacy_ref_id = str(item.get("ref_object_id", "") or "").strip()
            if not self._is_event_ref_id(legacy_ref_id):
                continue
            es_entry = self._esdb.get(legacy_ref_id)
            es_parent_depth = self._esdb_parent_depth(legacy_ref_id, set()) if isinstance(es_entry, dict) else 0
            narrative.append(
                {
                    "item_id": item_id,
                    "ref_object_id": legacy_ref_id,
                    "structure_id": legacy_ref_id,
                    "display": str(item.get("display", "") or legacy_ref_id),
                    "er": round(float(item.get("er", 0.0) or 0.0), 8),
                    "ev": round(float(item.get("ev", 0.0) or 0.0), 8),
                    "cp_abs": round(float(item.get("cp_abs", 0.0) or 0.0), 8),
                    "salience_score": round(float(item.get("salience_score", 0.0) or 0.0), 8),
                    "event_grasp": 0.0,
                    "total_energy": round(float(item.get("er", 0.0) or 0.0) + float(item.get("ev", 0.0) or 0.0), 8),
                    "component_count": len(self._parse_event_components(legacy_ref_id)),
                    "esdb_parent_depth": int(es_parent_depth),
                    "esdb_parent_count": len(list(es_entry.get("parents", []) or [])) if isinstance(es_entry, dict) else 0,
                    "esdb_delta_entry_count": len(list(es_entry.get("delta_diff_table", []) or [])) if isinstance(es_entry, dict) else 0,
                    "esdb_materialized": bool(es_entry.get("materialized", False)) if isinstance(es_entry, dict) else False,
                    "esdb_materialized_entry_count": len(list(es_entry.get("materialized_diff_table", []) or [])) if isinstance(es_entry, dict) else 0,
                    "esdb_update_count": int(es_entry.get("update_count", 0) or 0) if isinstance(es_entry, dict) else 0,
                }
            )
        return narrative

    @staticmethod
    def _clamp01(value: float) -> float:
        try:
            return max(0.0, min(1.0, float(value)))
        except Exception:
            return 0.0

    @staticmethod
    def _get_state_item_by_id(*, pool, item_id: str) -> dict | None:
        if not item_id:
            return None
        store = getattr(pool, "_store", None)
        if store is None or not hasattr(store, "get"):
            return None
        try:
            item = store.get(item_id)
            return item if isinstance(item, dict) else None
        except Exception:
            return None

    @staticmethod
    def _safe_bind_ref_alias(*, pool, item_id: str, ref_alias_id: str) -> None:
        """Bind a ref_object_id alias to an existing state_item (best-effort, never raising)."""
        if not item_id or not ref_alias_id:
            return
        store = getattr(pool, "_store", None)
        if store is None or not hasattr(store, "bind_ref_alias"):
            return
        try:
            store.bind_ref_alias(str(item_id), str(ref_alias_id))
        except Exception:
            return

    def _is_cognitive_stitching_event_structure_obj(self, structure_obj: dict) -> bool:
        if not isinstance(structure_obj, dict):
            return False
        if str(structure_obj.get("sub_type", "") or "") == "cognitive_stitching_event_structure":
            return True
        structure_block = structure_obj.get("structure", {}) if isinstance(structure_obj.get("structure", {}), dict) else {}
        ext = structure_block.get("ext", {}) if isinstance(structure_block.get("ext", {}), dict) else {}
        return isinstance(ext.get("cognitive_stitching"), dict)

    def _is_cognitive_stitching_event_state_item(self, item: dict) -> bool:
        if not isinstance(item, dict):
            return False
        if str(item.get("ref_object_type", "") or "") != "st":
            return False
        # Strict event detection:
        # - Avoid misclassifying "ordinary ST structures" as CS events.
        # - A CS event must have a canonical event_ref_id that starts with "<prefix>::".
        #
        # Note:
        # - For HDB-backed events, ref_object_id is the HDB structure_id (st_*),
        #   while the canonical event_ref_id lives in ref_snapshot.content_signature or CS metadata.
        event_ref_id = self._extract_event_ref_id_from_state_item(item)
        return bool(event_ref_id and self._is_event_ref_id(event_ref_id))

    def _extract_event_ref_id_from_state_item(self, item: dict) -> str:
        """Return the canonical event_ref_id (cs_event::...) for a CS event state item."""
        prefix = f"{self._config.get('event_id_prefix', 'cs_event')}::"
        if not isinstance(item, dict):
            return ""
        ref_id = str(item.get("ref_object_id", "") or "")
        if ref_id.startswith(prefix):
            return ref_id

        meta_ext = (item.get("meta", {}) or {}).get("ext", {}) if isinstance((item.get("meta", {}) or {}).get("ext", {}), dict) else {}
        cs_meta = meta_ext.get("cognitive_stitching", {}) if isinstance(meta_ext.get("cognitive_stitching", {}), dict) else {}
        candidate = str(cs_meta.get("event_ref_id", "") or cs_meta.get("cs_event_ref_id", "") or "").strip()
        if candidate.startswith(prefix):
            return candidate

        ref_snapshot = item.get("ref_snapshot", {}) if isinstance(item.get("ref_snapshot", {}), dict) else {}
        snap_sig = str(ref_snapshot.get("content_signature", "") or "").strip()
        if snap_sig.startswith(prefix):
            return snap_sig
        structure_ext = ref_snapshot.get("structure_ext", {}) if isinstance(ref_snapshot.get("structure_ext", {}), dict) else {}
        cs_meta2 = structure_ext.get("cognitive_stitching", {}) if isinstance(structure_ext.get("cognitive_stitching", {}), dict) else {}
        candidate2 = str(cs_meta2.get("event_ref_id", "") or cs_meta2.get("cs_event_ref_id", "") or "").strip()
        if candidate2.startswith(prefix):
            return candidate2
        return ""

    @staticmethod
    def _extract_bound_numerical_attribute(state_item: dict, *, attribute_name: str) -> float:
        name = str(attribute_name or "").strip()
        if not name or not isinstance(state_item, dict):
            return 0.0
        try:
            attrs = list((state_item.get("ext", {}) or {}).get("bound_attributes", []) or [])
        except Exception:
            attrs = []
        for attr in attrs:
            if not isinstance(attr, dict):
                continue
            content = attr.get("content", {}) if isinstance(attr.get("content", {}), dict) else {}
            if str(content.get("attribute_name", "") or "").strip() != name:
                continue
            try:
                return round(float(content.get("attribute_value", 0.0) or 0.0), 8)
            except Exception:
                return 0.0
        return 0.0

    def _event_internal_balance_from_ledger(self, state_item: dict | None) -> float:
        """Compute internal balance from ES component ledger: 1 - mean(|er-ev|/(er+ev))."""
        if not isinstance(state_item, dict):
            return 0.0
        meta = state_item.get("meta", {}) if isinstance(state_item.get("meta", {}), dict) else {}
        meta_ext = meta.get("ext", {}) if isinstance(meta.get("ext", {}), dict) else {}
        cs_meta = meta_ext.get("cognitive_stitching") if isinstance(meta_ext.get("cognitive_stitching"), dict) else {}
        ledger = list((cs_meta or {}).get("component_ledger", []) or [])
        if not ledger:
            return 0.0
        norms: list[float] = []
        for entry in ledger:
            if not isinstance(entry, dict):
                continue
            er = max(0.0, float(entry.get("er", 0.0) or 0.0))
            ev = max(0.0, float(entry.get("ev", 0.0) or 0.0))
            total = er + ev
            if total <= 1e-9:
                norms.append(0.0)
                continue
            norms.append(abs(er - ev) / total)
        if not norms:
            return 0.0
        mean_norm = sum(norms) / float(len(norms))
        return self._clamp01(1.0 - mean_norm)

    def _compute_event_grasp(self, *, total_energy: float, balance: float, margin: float) -> float:
        w_e = float(self._config.get("event_grasp_energy_weight", 1.2) or 1.2)
        w_b = float(self._config.get("event_grasp_balance_weight", 1.3) or 1.3)
        w_m = float(self._config.get("event_grasp_margin_weight", 0.8) or 0.8)
        bias = float(self._config.get("event_grasp_bias", -1.0) or -1.0)
        temp = max(1e-6, float(self._config.get("event_grasp_sigmoid_temperature", 1.0) or 1.0))
        z = (
            bias
            + w_e * math.log1p(max(0.0, float(total_energy)))
            + w_b * self._clamp01(float(balance))
            + w_m * self._clamp01(float(margin))
        )
        # Sigmoid with temperature scaling.
        try:
            return self._clamp01(1.0 / (1.0 + math.exp(-float(z) / temp)))
        except OverflowError:
            return 1.0 if z > 0 else 0.0

    @staticmethod
    def _build_numerical_attribute_sa(
        *,
        attribute_name: str,
        attribute_value: float,
        target_item_id: str,
        target_ref_object_id: str,
        trace_id: str,
        tick_id: str,
        sub_type: str,
        display_prefix: str,
    ) -> dict:
        now_ms = int(time.time() * 1000)
        name = str(attribute_name or "").strip() or "attr"
        value = float(attribute_value or 0.0)
        # Stable per-target id (binding engine will also replace-by-name).
        attr_id = f"sa_attr_{name}_{target_item_id or target_ref_object_id or 'global'}"
        raw = f"{name}:{round(value, 6)}"
        display = f"{display_prefix}:{round(value, 3)}"
        return {
            "id": attr_id,
            "object_type": "sa",
            "sub_type": str(sub_type or "marker_attribute_presence"),
            "schema_version": "1.1",
            "content": {
                "raw": raw,
                "normalized": raw,
                "display": display,
                "value_type": "numerical",
                "attribute_name": name,
                "attribute_value": round(value, 8),
            },
            "stimulus": {"modality": "meta", "role": "attribute", "is_anchor": False, "group_index": 0, "position_in_group": 0, "global_sequence_index": 0},
            "energy": {
                "er": 0.0,
                "ev": 0.0,
                "ownership_level": "sa",
                "computed_from_children": False,
                "fatigue": 0.0,
                "recency_gain": 1.0,
                "salience_score": 0.0,
                "cognitive_pressure_delta": 0.0,
                "cognitive_pressure_abs": 0.0,
                "last_decay_tick": 0,
                "last_decay_at": now_ms,
            },
            "source": {
                "module": "cognitive_stitching",
                "interface": "run_event_grasp",
                "origin": "event_grasp_attribute_binding",
                "origin_id": tick_id,
                "parent_ids": [str(target_ref_object_id or "")] if str(target_ref_object_id or "") else [],
            },
            "trace_id": trace_id,
            "tick_id": tick_id,
            "created_at": now_ms,
            "updated_at": now_ms,
            "status": "active",
            "tags": ["cognitive_stitching", "attribute"],
            "ext": {"attribute_name": name, "target_ref_object_id": str(target_ref_object_id or "")},
            "meta": {"confidence": 0.7, "field_registry_version": "1.1", "debug": {}, "ext": {}},
        }

    def _is_event_ref_id(self, ref_id: str) -> bool:
        prefix = f"{self._config.get('event_id_prefix', 'cs_event')}::"
        return bool(ref_id) and str(ref_id).startswith(prefix)

    def _esdb_upsert_event(
        self,
        *,
        event_ref_id: str,
        components: list[str],
        parent_refs: list[str],
        tick_id: str,
        action_type: str,
    ) -> None:
        if not event_ref_id or not self._is_event_ref_id(event_ref_id):
            return
        now_ms = int(time.time() * 1000)
        entry = self._esdb.get(event_ref_id)
        if not isinstance(entry, dict):
            entry = {
                "event_ref_id": event_ref_id,
                "components": list(components),
                "parents": [],
                "delta_diff_table": [],
                "materialized": False,
                "materialized_diff_table": [],
                "runtime_cache": {},
                "created_at_ms": now_ms,
                "updated_at_ms": now_ms,
                "update_count": 0,
                "last_tick_id": "",
                "last_action_type": "",
            }
            self._esdb[event_ref_id] = entry

        prev_components = list(entry.get("components", []) or [])
        prev_parents = list(entry.get("parents", []) or [])

        entry["components"] = list(components)
        parents = list(entry.get("parents", []) or [])
        for parent in parent_refs or []:
            pid = str(parent or "").strip()
            if not pid:
                continue
            if pid not in parents:
                parents.append(pid)
        entry["parents"] = parents
        entry["update_count"] = int(entry.get("update_count", 0) or 0) + 1
        entry["last_tick_id"] = str(tick_id or "")
        entry["last_action_type"] = str(action_type or "")
        entry["updated_at_ms"] = now_ms
        # Parent chain / components change invalidates overlay cache.
        entry["runtime_cache"] = {}
        # If the event's components changed, a previously materialized overlay may become stale.
        # Prefer dropping materialization so future overlay opens can incorporate new tail knowledge.
        if prev_components != list(components):
            entry["materialized"] = False
            entry["materialized_diff_table"] = []

        # Keep delta clean: do not keep edges to already-in-event components.
        try:
            component_set = {str(x) for x in (components or []) if str(x)}
            delta = [d for d in list(entry.get("delta_diff_table", []) or []) if isinstance(d, dict) and str(d.get("target_id", "") or "") and str(d.get("target_id", "") or "") not in component_set]
            entry["delta_diff_table"] = delta
        except Exception:
            pass

        del prev_components, prev_parents

    def _esdb_parent_depth(self, event_ref_id: str, visited: set[str]) -> int:
        if not event_ref_id or event_ref_id in visited:
            return 0
        entry = self._esdb.get(event_ref_id)
        if not isinstance(entry, dict):
            return 0
        visited.add(event_ref_id)
        parents = [str(x) for x in (entry.get("parents", []) or []) if str(x)]
        if not parents:
            return 0
        depths = []
        for parent in parents:
            if self._is_event_ref_id(parent):
                depths.append(1 + self._esdb_parent_depth(parent, visited))
            else:
                depths.append(1)
        return int(max(depths)) if depths else 0

    def _esdb_open_overlay_top_diff_entries(self, *, event_ref_id: str, hdb) -> tuple[list[dict], float]:
        """Open ES overlay DB (parents+delta) and return top diff_table entries."""
        top_k = max(1, int(self._config.get("esdb_overlay_top_k", 16)))
        return self._esdb_open_overlay_diff_entries(
            event_ref_id=event_ref_id,
            hdb=hdb,
            top_k=top_k,
            visited=set(),
            use_cache=True,
        )

    def _esdb_materialize_diff_table(self, *, event_ref_id: str, hdb, top_n: int) -> list[dict]:
        entries, _ = self._esdb_open_overlay_diff_entries(
            event_ref_id=event_ref_id,
            hdb=hdb,
            top_k=max(1, int(top_n)),
            visited=set(),
            use_cache=False,
        )
        return list(entries)

    def _esdb_open_overlay_diff_entries(
        self,
        *,
        event_ref_id: str,
        hdb,
        top_k: int,
        visited: set[str],
        use_cache: bool,
    ) -> tuple[list[dict], float]:
        if not event_ref_id or not self._is_event_ref_id(event_ref_id):
            return [], 0.0
        entry = self._esdb.get(event_ref_id)
        if not isinstance(entry, dict):
            return [], 0.0

        top_k = max(1, int(top_k))
        if bool(entry.get("materialized", False)) and entry.get("materialized_diff_table"):
            # Materialized rows are a baseline; delta can continue to grow after consolidation.
            merged: dict[str, dict[str, Any]] = {}
            for r in list(entry.get("materialized_diff_table", []) or []):
                if not isinstance(r, dict):
                    continue
                target_id = str(r.get("target_id", "") or "")
                if not target_id:
                    continue
                w = max(0.0, float(r.get("base_weight", 0.0) or 0.0))
                merged[target_id] = {
                    "target_id": target_id,
                    "base_weight": round(w, 8),
                    "entry_type": str(r.get("entry_type", "structure_ref") or "structure_ref"),
                    "ext": dict(r.get("ext", {}) or {}),
                }

            # Merge delta on top of baseline (if any).
            beta = max(0.0, min(1.0, float(self._config.get("esdb_overlay_parent_beta", 0.35) or 0.35)))
            for d in list(entry.get("delta_diff_table", []) or []):
                if not isinstance(d, dict):
                    continue
                target_id = str(d.get("target_id", "") or "")
                if not target_id:
                    continue
                w = max(0.0, float(d.get("base_weight", 0.0) or 0.0))
                if w <= 0.0:
                    continue
                existing = merged.get(target_id)
                if existing is None:
                    merged[target_id] = {
                        "target_id": target_id,
                        "base_weight": round(w, 8),
                        "entry_type": str(d.get("entry_type", "structure_ref") or "structure_ref"),
                        "ext": {"support_hits": 1, "sources": ["delta"]},
                    }
                    continue
                old_w = float(existing.get("base_weight", 0.0) or 0.0)
                merged_w = max(old_w, w) + beta * min(old_w, w)
                existing["base_weight"] = round(float(merged_w), 8)
                ext = existing.get("ext") if isinstance(existing.get("ext"), dict) else {}
                sources = list(ext.get("sources", []) or [])
                if "delta" not in sources:
                    sources.append("delta")
                ext["sources"] = sources[:12]
                ext["support_hits"] = int(ext.get("support_hits", 1) or 1) + 1
                existing["ext"] = ext

            rows = list(merged.values())
            rows.sort(key=lambda r: float(r.get("base_weight", 0.0) or 0.0), reverse=True)
            picked = rows[:top_k]
            total = sum(max(0.0, float(r.get("base_weight", 0.0) or 0.0)) for r in picked)
            return picked, float(total)

        ttl_ms = max(0, int(self._config.get("esdb_overlay_cache_ttl_ms", 2500) or 2500))
        if use_cache and ttl_ms > 0:
            cache = entry.get("runtime_cache") if isinstance(entry.get("runtime_cache"), dict) else {}
            now_ms = int(time.time() * 1000)
            cached_at = int(cache.get("overlay_cached_at_ms", 0) or 0)
            cached_top_k = int(cache.get("overlay_cached_top_k", 0) or 0)
            cached_rows = cache.get("overlay_cached_rows")
            cached_total = cache.get("overlay_cached_total")
            if (
                isinstance(cached_rows, list)
                and isinstance(cached_total, (int, float))
                and cached_top_k >= top_k
                and cached_at > 0
                and (now_ms - cached_at) <= ttl_ms
            ):
                return list(cached_rows[:top_k]), float(cached_total)

        if event_ref_id in visited:
            return [], 0.0
        visited.add(event_ref_id)

        beta = max(0.0, min(1.0, float(self._config.get("esdb_overlay_parent_beta", 0.35) or 0.35)))
        merged: dict[str, dict[str, Any]] = {}

        # Start with delta table (kept empty in phase2, reserved for future).
        for d in list(entry.get("delta_diff_table", []) or []):
            if not isinstance(d, dict):
                continue
            target_id = str(d.get("target_id", "") or "")
            if not target_id:
                continue
            w = max(0.0, float(d.get("base_weight", 0.0) or 0.0))
            merged[target_id] = {
                "target_id": target_id,
                "base_weight": round(w, 8),
                "entry_type": str(d.get("entry_type", "structure_ref") or "structure_ref"),
                "ext": {"support_hits": 1, "sources": ["delta"]},
            }

        # Merge parents top-k (parents can be ST DB owners or other ES ids).
        structure_store = getattr(hdb, "_structure_store", None)
        for parent_ref in [str(x) for x in (entry.get("parents", []) or []) if str(x)]:
            parent_rows: list[dict] = []
            if self._is_event_ref_id(parent_ref):
                parent_rows, _ = self._esdb_open_overlay_diff_entries(
                    event_ref_id=parent_ref,
                    hdb=hdb,
                    top_k=top_k,
                    visited=visited,
                    use_cache=use_cache,
                )
            else:
                if structure_store is None:
                    continue
                parent_db = structure_store.get_db_by_owner(parent_ref) if hasattr(structure_store, "get_db_by_owner") else None
                if not isinstance(parent_db, dict):
                    continue
                diff_table = [r for r in list(parent_db.get("diff_table", []) or []) if isinstance(r, dict) and str(r.get("target_id", "") or "")]
                if not diff_table:
                    continue
                diff_table.sort(key=self._diff_entry_effective_weight, reverse=True)
                parent_rows = diff_table[:top_k]

            for pr in parent_rows:
                if not isinstance(pr, dict):
                    continue
                target_id = str(pr.get("target_id", "") or "")
                if not target_id:
                    continue
                w = max(0.0, float(self._diff_entry_effective_weight(pr)))
                if w <= 0.0:
                    continue
                existing = merged.get(target_id)
                if existing is None:
                    merged[target_id] = {
                        "target_id": target_id,
                        "base_weight": round(w, 8),
                        "entry_type": str(pr.get("entry_type", "structure_ref") or "structure_ref"),
                        "ext": {"support_hits": 1, "sources": [parent_ref]},
                    }
                    continue
                old_w = float(existing.get("base_weight", 0.0) or 0.0)
                merged_w = max(old_w, w) + beta * min(old_w, w)
                existing["base_weight"] = round(float(merged_w), 8)
                ext = existing.get("ext") if isinstance(existing.get("ext"), dict) else {}
                sources = list(ext.get("sources", []) or [])
                if parent_ref not in sources:
                    sources.append(parent_ref)
                ext["sources"] = sources[:12]
                ext["support_hits"] = int(ext.get("support_hits", 1) or 1) + 1
                existing["ext"] = ext

        rows = list(merged.values())
        rows.sort(key=lambda r: float(r.get("base_weight", 0.0) or 0.0), reverse=True)
        picked = rows[:top_k]
        total = sum(max(0.0, float(r.get("base_weight", 0.0) or 0.0)) for r in picked)

        if use_cache and ttl_ms > 0:
            cache = entry.get("runtime_cache") if isinstance(entry.get("runtime_cache"), dict) else {}
            cache["overlay_cached_at_ms"] = int(time.time() * 1000)
            cache["overlay_cached_top_k"] = int(top_k)
            cache["overlay_cached_rows"] = picked
            cache["overlay_cached_total"] = float(total)
            entry["runtime_cache"] = cache

        return picked, float(total)

    def _esdb_delta_upsert_edge(
        self,
        *,
        event_ref_id: str,
        target_id: str,
        base_weight: float,
        source_ref: str,
        tick_id: str,
        action_type: str,
        distance: int,
    ) -> None:
        if not event_ref_id or not self._is_event_ref_id(event_ref_id):
            return
        if not target_id:
            return
        if base_weight <= 0.0:
            return
        entry = self._esdb.get(event_ref_id)
        if not isinstance(entry, dict):
            return

        now_ms = int(time.time() * 1000)
        beta = max(0.0, min(1.0, float(self._config.get("esdb_delta_merge_beta", 0.25) or 0.25)))
        max_entries = max(8, int(self._config.get("esdb_delta_max_entries", 96)))

        delta = [d for d in list(entry.get("delta_diff_table", []) or []) if isinstance(d, dict)]
        found = None
        for d in delta:
            if str(d.get("target_id", "") or "") == target_id:
                found = d
                break
        if found is None:
            delta.append(
                {
                    "target_id": target_id,
                    "base_weight": round(float(base_weight), 8),
                    "entry_type": "structure_ref",
                    "recent_gain": 1.0,
                    "fatigue": 0.0,
                    "created_at_ms": now_ms,
                    "updated_at_ms": now_ms,
                    "ext": {
                        "support_hits": 1,
                        "sources": [source_ref] if str(source_ref or "") else [],
                        "action_type": str(action_type or ""),
                        "distance": int(distance),
                        "last_tick_id": str(tick_id or ""),
                    },
                }
            )
        else:
            old_w = max(0.0, float(found.get("base_weight", 0.0) or 0.0))
            merged_w = max(old_w, float(base_weight)) + beta * min(old_w, float(base_weight))
            found["base_weight"] = round(float(merged_w), 8)
            found["updated_at_ms"] = now_ms
            ext = found.get("ext") if isinstance(found.get("ext"), dict) else {}
            ext["support_hits"] = int(ext.get("support_hits", 1) or 1) + 1
            sources = list(ext.get("sources", []) or [])
            if str(source_ref or "") and str(source_ref) not in sources:
                sources.append(str(source_ref))
            ext["sources"] = sources[:12]
            ext["action_type"] = str(action_type or "") or ext.get("action_type", "")
            ext["distance"] = int(distance)
            ext["last_tick_id"] = str(tick_id or "")
            found["ext"] = ext

        # Keep delta ordered and bounded.
        delta = [d for d in delta if str(d.get("target_id", "") or "")]
        delta.sort(key=lambda r: float(r.get("base_weight", 0.0) or 0.0), reverse=True)
        entry["delta_diff_table"] = delta[:max_entries]
        # Delta change invalidates overlay cache (but does not force re-materialization).
        entry["runtime_cache"] = {}

    def _esdb_refresh_delta_from_tail_components(self, *, event_ref_id: str, components: list[str], hdb, tick_id: str, action_type: str) -> dict:
        """Import a small set of outgoing edges from tail component ST DBs into ES delta."""
        if not bool(self._config.get("enable_esdb_delta", True)):
            return {"enabled": False, "reason": "disabled_by_config", "imported_edge_count": 0}
        if not event_ref_id or not self._is_event_ref_id(event_ref_id):
            return {"enabled": False, "reason": "not_event", "imported_edge_count": 0}
        entry = self._esdb.get(event_ref_id)
        if not isinstance(entry, dict):
            return {"enabled": False, "reason": "missing_esdb_entry", "imported_edge_count": 0}

        structure_store = getattr(hdb, "_structure_store", None)
        if structure_store is None:
            return {"enabled": False, "reason": "no_structure_store", "imported_edge_count": 0}

        max_context_k = max(1, int(self._config.get("max_context_k", 2)))
        tail_refs = [str(x) for x in list(components or [])[-max_context_k:] if str(x)]
        if not tail_refs:
            return {"enabled": False, "reason": "empty_tail", "imported_edge_count": 0}

        import_top_k = max(1, int(self._config.get("esdb_delta_import_top_k_per_tail", 6)))
        distance_decay = max(0.0, min(1.0, float(self._config.get("esdb_delta_distance_decay", 0.75) or 0.75)))
        min_weight = max(0.0, float(self._config.get("esdb_delta_min_weight", 0.0001) or 0.0001))
        component_set = {str(x) for x in (components or []) if str(x)}

        imported = 0
        for distance, tail_ref in enumerate(reversed(tail_refs)):
            entries, _ = self._top_diff_entries(structure_store=structure_store, owner_ref_id=tail_ref)
            if not entries:
                continue
            scale = 1.0 if distance <= 0 else (distance_decay ** float(distance))
            for e in list(entries[:import_top_k]):
                if not isinstance(e, dict):
                    continue
                target_id = str(e.get("target_id", "") or "")
                if not target_id:
                    continue
                if target_id in component_set:
                    continue
                w = max(0.0, float(self._diff_entry_effective_weight(e))) * float(scale)
                if w < min_weight:
                    continue
                self._esdb_delta_upsert_edge(
                    event_ref_id=event_ref_id,
                    target_id=target_id,
                    base_weight=float(w),
                    source_ref=tail_ref,
                    tick_id=tick_id,
                    action_type=action_type,
                    distance=int(distance),
                )
                imported += 1

        # Keep delta clean: do not keep edges to already-in-event components.
        try:
            delta = [d for d in list(entry.get("delta_diff_table", []) or []) if isinstance(d, dict) and str(d.get("target_id", "") or "") and str(d.get("target_id", "") or "") not in component_set]
            entry["delta_diff_table"] = delta
        except Exception:
            pass

        return {"enabled": True, "reason": "ok", "imported_edge_count": int(imported)}

    @staticmethod
    def _diff_entry_effective_weight(entry: dict) -> float:
        try:
            base_weight = max(0.0, float(entry.get("base_weight", 0.0) or 0.0))
            recent_gain = max(1.0, float(entry.get("recent_gain", 1.0) or 1.0))
            fatigue = max(0.0, float(entry.get("fatigue", 0.0) or 0.0))
            return round(base_weight * recent_gain / (1.0 + fatigue), 8)
        except Exception:
            return 0.0

    @staticmethod
    def _safe_apply_energy_update(
        *,
        pool,
        item_id: str,
        delta_er: float,
        delta_ev: float,
        trace_id: str,
        tick_id: str,
        reason: str,
    ) -> None:
        if not item_id:
            return
        if abs(float(delta_er)) < 1e-9 and abs(float(delta_ev)) < 1e-9:
            return
        if not hasattr(pool, "apply_energy_update"):
            return
        pool.apply_energy_update(
            target_item_id=item_id,
            delta_er=float(delta_er),
            delta_ev=float(delta_ev),
            trace_id=trace_id,
            tick_id=tick_id,
            reason=reason,
            source_module="cognitive_stitching",
        )

    @staticmethod
    def _get_existing_state_item_by_ref(*, pool, ref_object_id: str) -> dict | None:
        store = getattr(pool, "_store", None)
        if store is None or not hasattr(store, "get_by_ref"):
            return None
        try:
            return store.get_by_ref(ref_object_id)
        except Exception:
            return None

    def _upsert_candidate(self, *, best_by_signature: dict[str, dict], candidate: dict) -> None:
        signature = str(candidate.get("candidate_signature", "") or "")
        existing = best_by_signature.get(signature)
        if existing is None or float(candidate.get("score", 0.0)) > float(existing.get("score", 0.0)):
            best_by_signature[signature] = candidate

    def _candidate_signature(self, *, action_type: str, new_components: list[str]) -> str:
        return f"{action_type}::{'|'.join(new_components)}"

    def _fatigue_scale(self, fatigue_before: float) -> float:
        floor_scale = max(0.0, min(1.0, float(self._config.get("same_pair_fatigue_floor_scale", 0.25))))
        fatigue_cap = max(0.01, float(self._config.get("same_pair_fatigue_cap", 1.6)))
        return max(
            floor_scale,
            1.0 - min(max(0.0, float(fatigue_before)), fatigue_cap) / fatigue_cap * (1.0 - floor_scale),
        )

    def _is_component_sequence_valid(self, components: list[str]) -> bool:
        if len(components) < 2:
            return False
        max_component_count = max(2, int(self._config.get("max_event_component_count", 8)))
        if len(components) > max_component_count:
            return False
        return all(str(item or "").strip() for item in components)

    def _merge_event_components(self, *, left: list[str], right: list[str]) -> list[str]:
        if not left or not right:
            return []
        overlap = self._boundary_overlap(left, right)
        if overlap == 0:
            shared = set(left) & set(right)
            if shared:
                return []
        merged = list(left) + list(right[overlap:])
        if len(merged) <= max(len(left), len(right)):
            return []
        return merged

    @staticmethod
    def _boundary_overlap(left: list[str], right: list[str]) -> int:
        max_overlap = min(len(left), len(right))
        for overlap in range(max_overlap, 0, -1):
            if left[-overlap:] == right[:overlap]:
                return overlap
        return 0

    def _event_ref_id_from_components(self, components: list[str]) -> str:
        prefix = str(self._config.get("event_id_prefix", "cs_event"))
        return f"{prefix}::" + "::".join(str(item) for item in components if str(item))

    def _parse_event_components(self, ref_object_id: str) -> list[str]:
        prefix = f"{self._config.get('event_id_prefix', 'cs_event')}::"
        if not ref_object_id.startswith(prefix):
            return []
        tail = ref_object_id[len(prefix) :]
        return [part for part in tail.split("::") if part]

    def _resolve_component_displays(self, *, components: list[str], structure_store) -> list[str]:
        if not components:
            return []
        displays: list[str] = []
        for component_ref in components:
            display = ""
            if structure_store is not None:
                structure_obj = structure_store.get(component_ref)
                if isinstance(structure_obj, dict):
                    display = self._structure_display(structure_obj)
            displays.append(display or str(component_ref))
        return displays

    def _event_display_from_components(self, component_displays: list[str]) -> str:
        if not component_displays:
            return ""
        return str(self._config.get("display_joiner", " -> ")).join(component_displays)

    @staticmethod
    def _candidate_preview(candidate: dict) -> dict[str, Any]:
        return {
            "action_type": candidate.get("action_type", ""),
            "source_display": candidate.get("source", {}).get("display", ""),
            "source_kind": candidate.get("source", {}).get("kind", ""),
            "target_display": candidate.get("target", {}).get("display", ""),
            "target_kind": candidate.get("target", {}).get("kind", ""),
            "edge_target_id": candidate.get("edge_target_id", ""),
            "match_mode": candidate.get("match_mode", ""),
            "score": round(float(candidate.get("score", 0.0)), 8),
            "edge_weight_ratio": round(float(candidate.get("edge_weight_ratio", 0.0)), 8),
            "match_strength": round(float(candidate.get("match_strength", 0.0)), 8),
            "context_k": int(candidate.get("context_hits", 1) or 1),
            "matched_span": int(candidate.get("matched_span", 1) or 1),
            "fatigue_before": round(float(candidate.get("fatigue_before", 0.0)), 8),
        }

    @staticmethod
    def _make_response(success: bool, code: str, message: str, data: dict, trace_id: str, tick_id: str, start_time: float) -> dict:
        return {
            "success": bool(success),
            "code": str(code),
            "message": str(message),
            "trace_id": trace_id,
            "tick_id": tick_id,
            "elapsed_ms": int((time.time() - start_time) * 1000),
            "data": data,
        }

    @staticmethod
    def _structure_display(structure_obj: dict) -> str:
        structure = structure_obj.get("structure", {}) if isinstance(structure_obj.get("structure", {}), dict) else {}
        for group in list(structure.get("sequence_groups", []) or []):
            if not isinstance(group, dict):
                continue
            if bool(group.get("order_sensitive", False)) and str(group.get("string_unit_kind", "") or "") == "char_sequence":
                text = str(group.get("string_token_text", "") or "").strip()
                if text:
                    return text
        flat_tokens = [str(token) for token in (structure.get("flat_tokens", []) or []) if str(token)]
        if flat_tokens:
            return "".join(flat_tokens)
        return str(structure.get("display_text", "") or structure_obj.get("id", ""))

    @staticmethod
    def _runtime_weight(*, hdb, structure_obj: dict) -> float:
        stats = dict(structure_obj.get("stats", {}) or {})
        try:
            weight_engine = getattr(hdb, "_weight", None)
            if weight_engine is not None and hasattr(weight_engine, "compute_runtime_weight"):
                return round(
                    float(
                        weight_engine.compute_runtime_weight(
                            base_weight=float(stats.get("base_weight", 1.0) or 1.0),
                            recent_gain=float(stats.get("recent_gain", 1.0) or 1.0),
                            fatigue=float(stats.get("fatigue", 0.0) or 0.0),
                        )
                    ),
                    8,
                )
        except Exception:
            pass
        base_weight = max(0.01, float(stats.get("base_weight", 1.0) or 1.0))
        recent_gain = max(1.0, float(stats.get("recent_gain", 1.0) or 1.0))
        fatigue = max(0.0, float(stats.get("fatigue", 0.0) or 0.0))
        return round(base_weight * recent_gain / (1.0 + fatigue), 8)

    @staticmethod
    def _energy_balance_ratio(a: float, b: float) -> float:
        x = max(0.0, float(a))
        y = max(0.0, float(b))
        if x <= 0.0 and y <= 0.0:
            return 0.0
        return round(min(x, y) / max(x, y, 1e-9), 8)

    @staticmethod
    def _get_structure(*, hdb, structure_id: str) -> dict | None:
        structure_store = getattr(hdb, "_structure_store", None)
        if structure_store is None or not structure_id:
            return None
        structure = structure_store.get(structure_id)
        return structure if isinstance(structure, dict) else None

    @staticmethod
    def _common_prefix_length(a_tokens: list[str], b_tokens: list[str]) -> int:
        limit = min(len(a_tokens), len(b_tokens))
        count = 0
        for index in range(limit):
            if a_tokens[index] != b_tokens[index]:
                break
            count += 1
        return count

    @staticmethod
    def _contiguous_subsequence_ratio(target_tokens: list[str], candidate_tokens: list[str]) -> float:
        if not target_tokens or not candidate_tokens or len(target_tokens) > len(candidate_tokens):
            return 0.0
        target = list(target_tokens)
        candidate = list(candidate_tokens)
        width = len(target)
        for start in range(0, len(candidate) - width + 1):
            if candidate[start : start + width] == target:
                return round(len(target) / max(1, len(candidate)), 8)
        return 0.0

    @staticmethod
    def _lcs_length(a_tokens: list[str], b_tokens: list[str]) -> int:
        if not a_tokens or not b_tokens:
            return 0
        rows = len(a_tokens) + 1
        cols = len(b_tokens) + 1
        dp = [[0] * cols for _ in range(rows)]
        for i in range(1, rows):
            for j in range(1, cols):
                if a_tokens[i - 1] == b_tokens[j - 1]:
                    dp[i][j] = dp[i - 1][j - 1] + 1
                else:
                    dp[i][j] = max(dp[i - 1][j], dp[i][j - 1])
        return int(dp[-1][-1])

    @classmethod
    def _lcs_ratio(cls, a_tokens: list[str], b_tokens: list[str]) -> float:
        lcs_len = cls._lcs_length(a_tokens, b_tokens)
        if lcs_len <= 0:
            return 0.0
        return round(float(lcs_len) / max(1, len(a_tokens)), 8)

    @staticmethod
    def _empty_report(*, enabled: bool, reason: str) -> dict[str, Any]:
        return {
            "enabled": bool(enabled),
            "stage": "phase2_contextual_event_stitching",
            "reason": str(reason),
            "seed_structure_count": 0,
            "seed_plain_structure_count": 0,
            "seed_event_count": 0,
            "candidate_count": 0,
            "action_count": 0,
            "created_count": 0,
            "extended_count": 0,
            "merged_count": 0,
            "reinforced_count": 0,
            "pair_fatigue_state_size": 0,
            "candidate_preview": [],
            "actions": [],
            "narrative_top_items": [],
        }
