# -*- coding: utf-8 -*-
"""
Structure-level retrieval-storage for HDB.
"""

from __future__ import annotations

import math
import time

from ._id_generator import next_id
from ._profile_restore import restore_profile
from ._sequence_display import format_sequence_groups


class StructureRetrievalEngine:
    def __init__(self, config: dict, weight_engine, logger, maintenance_engine):
        self._config = config
        self._weight = weight_engine
        self._logger = logger
        self._maintenance = maintenance_engine

    def update_config(self, config: dict) -> None:
        self._config = config

    def run(
        self,
        *,
        state_snapshot: dict,
        trace_id: str,
        tick_id: str,
        structure_store,
        group_store,
        pointer_index,
        cut_engine,
        episodic_store,
        attention_mode: str,
        top_n: int,
        enable_storage: bool,
        enable_new_group_creation: bool,
        max_rounds: int,
    ) -> dict:
        del enable_new_group_creation
        items = list(state_snapshot.get("top_items") or state_snapshot.get("items") or [])
        # 注意力模式（attention_mode）说明：
        # - top_n_stub: 旧版占位口径（兼容保留）
        # - cam_snapshot: 正式口径，state_snapshot 本身已经是 CAM（当前注意记忆体）快照
        if attention_mode not in {"top_n_stub", "cam_snapshot"}:
            return self._empty_result(code="NOT_IMPLEMENTED_ERROR", message="attention_mode not implemented")

        # 只消费结构（ST）。top_n 在这里是“安全上限”，避免误配置时 CAM 过大导致结构级展开爆炸。
        safe_cap = max(1, int(top_n or 1))
        st_items = [item for item in items if item.get("ref_object_type") == "st"][:safe_cap]
        if not st_items:
            return self._empty_result(code="OK", message="no structure items in cam")

        now_ms = int(time.time() * 1000)
        cam_items = self._collect_cam_items(st_items=st_items, structure_store=structure_store, now_ms=now_ms)
        if not cam_items:
            return self._empty_result(code="OK", message="no valid structure items in cam")

        cam_structure_ids = [item["structure_id"] for item in cam_items]
        budget_er_map = {item["structure_id"]: item["er"] for item in cam_items}
        budget_ev_map = {item["structure_id"]: item["ev"] for item in cam_items}
        debug_cam_items = [dict(item["debug"]) for item in cam_items]
        episodic_memory_id = ""
        if enable_storage:
            episodic = episodic_store.append(
                {
                    "event_summary": "structure-level runtime memory",
                    "structure_refs": list(dict.fromkeys(cam_structure_ids)),
                    "group_refs": [],
                    "origin": "structure_level_runtime_memory",
                    "meta": {
                        "confidence": 1.0,
                        "field_registry_version": 1,
                        "debug": {},
                        "ext": {
                            "trace_id": trace_id,
                            "tick_id": tick_id,
                        },
                    },
                },
                trace_id=trace_id,
                tick_id=tick_id,
            )
            episodic_memory_id = episodic.get("id", "")
            if episodic_memory_id:
                initial_runtime_profile = self._build_runtime_profile_from_cam(
                    cam_items=cam_items,
                    budget_er_map=budget_er_map,
                    budget_ev_map=budget_ev_map,
                    cut_engine=cut_engine,
                    origin_frame_id=tick_id,
                )
                episodic_meta = dict(episodic.get("meta", {}))
                episodic_ext = dict(episodic_meta.get("ext", {}))
                episodic_ext["display_text"] = format_sequence_groups(initial_runtime_profile.get("sequence_groups", [])) or initial_runtime_profile.get("display_text", "")
                episodic_ext["memory_material"] = self._build_structure_memory_material(profile=initial_runtime_profile)
                episodic_meta["ext"] = episodic_ext
                episodic["meta"] = episodic_meta
                episodic_store.update(episodic)

        matched_group_ids: list[str] = []
        new_group_ids: list[str] = []
        bias_structure_ids: list[str] = []
        bias_projections: list[dict] = []
        internal_fragments: list[dict] = []
        round_summaries: list[dict] = []
        debug_round_details: list[dict] = []
        debug_new_group_details: list[dict] = []
        fallback_used = False
        temp_anchor_fatigue: dict[str, float] = {}
        single_group_processed_ids: set[str] = set()
        min_budget_threshold = max(0.01, float(self._config.get("stimulus_residual_min_energy", 0.05)))

        for round_index in range(1, max_rounds + 1):
            if self._max_total_budget(cam_structure_ids, budget_er_map, budget_ev_map) < min_budget_threshold:
                break

            runtime_profile = self._build_runtime_profile_from_cam(
                cam_items=cam_items,
                budget_er_map=budget_er_map,
                budget_ev_map=budget_ev_map,
                cut_engine=cut_engine,
                origin_frame_id=tick_id,
            )
            budget_before = self._build_budget_snapshot(cam_structure_ids, budget_er_map, budget_ev_map)
            anchor_item = self._select_anchor_item(
                cam_items=cam_items,
                budget_er_map=budget_er_map,
                budget_ev_map=budget_ev_map,
                temp_anchor_fatigue=temp_anchor_fatigue,
                skip_structure_ids=single_group_processed_ids,
                now_ms=now_ms,
            )
            if not anchor_item:
                break

            lookup, best, candidate_details = self._resolve_anchor_chain_match(
                anchor_structure_id=anchor_item["structure_id"],
                runtime_profile=runtime_profile,
                budget_er_map=budget_er_map,
                budget_ev_map=budget_ev_map,
                structure_store=structure_store,
                group_store=group_store,
                pointer_index=pointer_index,
                cut_engine=cut_engine,
                trace_id=trace_id,
                tick_id=tick_id,
                round_index=round_index,
                now_ms=now_ms,
            )
            fallback_used = fallback_used or bool(lookup.get("used_fallback"))

            if not best:
                storage_summary = None
                selected_group = self._build_implicit_single_group_debug(
                    anchor_item=anchor_item,
                    structure_store=structure_store,
                    cut_engine=cut_engine,
                    runtime_profile=runtime_profile,
                    tick_id=tick_id,
                    round_index=round_index,
                )
                if enable_storage:
                    storage_summary = self._store_runtime_context(
                        owner_kind="st",
                        owner_id=anchor_item["structure_id"],
                        full_profile=runtime_profile,
                        covered_structure_ids=[anchor_item["structure_id"]],
                        full_energy_profile=self._profile_energy_map(runtime_profile),
                        structure_store=structure_store,
                        group_store=group_store,
                        pointer_index=pointer_index,
                        cut_engine=cut_engine,
                        trace_id=trace_id,
                        tick_id=tick_id,
                        round_index=round_index,
                        episodic_memory_id=episodic_memory_id,
                    )
                    if storage_summary:
                        for group_id in storage_summary.get("new_group_ids", []):
                            if group_id:
                                new_group_ids.append(group_id)
                        debug_new_group_details.extend(storage_summary.get("new_group_details", []))
                round_summaries.append(
                    {
                        "round_index": round_index,
                        "group_id": selected_group.get("group_id", ""),
                        "score": round(float(selected_group.get("score", 1.0)), 8),
                        "coverage_ratio": round(float(selected_group.get("coverage_ratio", 0.0)), 8),
                        "wave_similarity": round(float(selected_group.get("wave_similarity", 1.0)), 8),
                        "matched_er_total": round(float(anchor_item.get("er", 0.0)), 8),
                        "matched_ev_total": round(float(anchor_item.get("ev", 0.0)), 8),
                        "bias_structure_ids": [],
                        "internal_fragment_count": 0,
                        "synthetic": True,
                    }
                )
                debug_round_details.append(
                    {
                        "round_index": round_index,
                        "anchor": self._build_anchor_debug(anchor_item),
                        "budget_before": budget_before,
                        "budget_after": budget_before,
                        "candidate_groups": candidate_details,
                        "selected_group": selected_group,
                        "bias_projections": [],
                        "internal_fragments": [],
                        "storage_summary": storage_summary,
                        "chain_steps": list(lookup.get("chain_steps", [])),
                    }
                )
                single_group_processed_ids.add(anchor_item["structure_id"])
                temp_anchor_fatigue[anchor_item["structure_id"]] = round(
                    float(temp_anchor_fatigue.get(anchor_item["structure_id"], 0.0))
                    + max(
                        float(self._config.get("structure_anchor_temp_fatigue_step", 0.55)),
                        float(self._config.get("structure_single_anchor_temp_fatigue_step", 1.1)),
                    ),
                    8,
                )
                continue

            group_obj = group_store.get(best["group_id"])
            if not group_obj:
                break
            matched_group_ids.append(group_obj.get("id", ""))
            rho = round(max(0.0, min(1.0, float(best.get("competition_score", 0.0)))), 8)
            required_ids = list(best.get("required_ids", []))
            current_required_profile = self._normalize_energy(required_ids, budget_er_map, budget_ev_map)
            matched_er_total = round(sum(float(budget_er_map.get(structure_id, 0.0)) for structure_id in required_ids), 8)
            matched_ev_total = round(sum(float(budget_ev_map.get(structure_id, 0.0)) for structure_id in required_ids), 8)

            self._update_group_after_match(
                group_obj=group_obj,
                current_profile=current_required_profile,
                match_score=rho,
                matched_er_total=matched_er_total,
                matched_ev_total=matched_ev_total,
            )
            group_store.update(group_obj)
            self._mark_path_entries(
                best=best,
                structure_store=structure_store,
                group_store=group_store,
                transferred_er=round(matched_er_total * rho, 8),
                transferred_ev=round(matched_ev_total * rho, 8),
                match_score=rho,
            )

            round_bias_projections = self._build_bias_projections(
                group_obj=group_obj,
                required_ids=required_ids,
                matched_er_total=matched_er_total,
                matched_ev_total=matched_ev_total,
                rho=rho,
                structure_store=structure_store,
            )
            bias_projections.extend(round_bias_projections)
            bias_structure_ids.extend(
                projection.get("structure_id", "")
                for projection in round_bias_projections
                if projection.get("structure_id")
            )

            transferred_er_map: dict[str, float] = {}
            transferred_ev_map: dict[str, float] = {}
            for structure_id in cam_structure_ids:
                transferred_er = round(float(budget_er_map.get(structure_id, 0.0)) * rho, 8)
                transferred_ev = round(float(budget_ev_map.get(structure_id, 0.0)) * rho, 8)
                budget_er_map[structure_id] = round(float(budget_er_map.get(structure_id, 0.0)) - transferred_er, 8)
                budget_ev_map[structure_id] = round(float(budget_ev_map.get(structure_id, 0.0)) - transferred_ev, 8)
                if structure_id in required_ids:
                    continue
                transferred_er_map[structure_id] = transferred_er
                transferred_ev_map[structure_id] = transferred_ev

            residual_ids = [
                structure_id
                for structure_id in cam_structure_ids
                if float(transferred_er_map.get(structure_id, 0.0)) + float(transferred_ev_map.get(structure_id, 0.0)) > 0.0
            ]
            round_fragments = self._build_internal_fragments(
                source_group_id=group_obj.get("id", ""),
                source_phase="residual_round",
                structure_ids=residual_ids,
                transfer_er_map=transferred_er_map,
                transfer_ev_map=transferred_ev_map,
                structure_store=structure_store,
            )
            internal_fragments.extend(round_fragments)

            storage_summary = None
            if enable_storage:
                storage_summary = self._store_runtime_context(
                    owner_kind="sg",
                    owner_id=group_obj.get("id", ""),
                    full_profile=runtime_profile,
                    covered_structure_ids=required_ids,
                    full_energy_profile=self._profile_energy_map(runtime_profile),
                    structure_store=structure_store,
                    group_store=group_store,
                    pointer_index=pointer_index,
                    cut_engine=cut_engine,
                    trace_id=trace_id,
                    tick_id=tick_id,
                    round_index=round_index,
                    episodic_memory_id=episodic_memory_id,
                )
                if storage_summary:
                    for group_id in storage_summary.get("new_group_ids", []):
                        if group_id:
                            new_group_ids.append(group_id)
                    debug_new_group_details.extend(storage_summary.get("new_group_details", []))

            budget_after = self._build_budget_snapshot(cam_structure_ids, budget_er_map, budget_ev_map)
            selected_group = {
                **self._build_group_debug_payload(group_obj, structure_store, cut_engine),
                "score": round(float(best.get("competition_score", 0.0)), 8),
                "competition_score": round(float(best.get("competition_score", 0.0)), 8),
                "similarity": round(float(best.get("competition_score", 0.0)), 8),
                "base_similarity": round(float(best.get("base_similarity", 0.0)), 8),
                "coverage_ratio": round(float(best.get("coverage_ratio", 0.0)), 8),
                "structure_ratio": round(float(best.get("structure_ratio", 0.0)), 8),
                "wave_similarity": round(float(best.get("wave_similarity", 0.0)), 8),
                "path_strength": round(float(best.get("path_strength", 1.0)), 8),
                "runtime_weight": round(float(best.get("runtime_weight", 1.0)), 8),
                "entry_runtime_weight": round(float(best.get("entry_runtime_weight", 1.0)), 8),
                "chain_depth": int(best.get("chain_depth", 0)),
                "owner_kind": best.get("owner_kind", ""),
                "owner_id": best.get("owner_id", ""),
                "common_part": dict(best.get("common_part", {})),
            }
            debug_round_details.append(
                {
                    "round_index": round_index,
                    "anchor": self._build_anchor_debug(anchor_item),
                    "budget_before": budget_before,
                    "budget_after": budget_after,
                    "candidate_groups": candidate_details,
                    "selected_group": selected_group,
                    "bias_projections": list(round_bias_projections),
                    "internal_fragments": [
                        {
                            **fragment,
                            "display_text": " / ".join(fragment.get("flat_tokens", [])),
                            "sequence_groups": list(fragment.get("sequence_groups", [])),
                            "energy_hint": round(float(fragment.get("er_hint", 0.0)) + float(fragment.get("ev_hint", 0.0)), 8),
                        }
                        for fragment in round_fragments
                    ],
                    "storage_summary": storage_summary,
                    "chain_steps": list(lookup.get("chain_steps", [])),
                }
            )
            round_summaries.append(
                {
                    "round_index": round_index,
                    "group_id": group_obj.get("id", ""),
                    "score": round(float(best.get("competition_score", 0.0)), 8),
                    "coverage_ratio": round(float(best.get("coverage_ratio", 0.0)), 8),
                    "wave_similarity": round(float(best.get("wave_similarity", 0.0)), 8),
                    "matched_er_total": matched_er_total,
                    "matched_ev_total": matched_ev_total,
                    "bias_structure_ids": [projection.get("structure_id", "") for projection in round_bias_projections],
                    "internal_fragment_count": len(round_fragments),
                }
            )
            temp_anchor_fatigue[anchor_item["structure_id"]] = round(
                float(temp_anchor_fatigue.get(anchor_item["structure_id"], 0.0))
                + float(self._config.get("structure_anchor_temp_fatigue_step", 0.55))
                * (
                    float(self._config.get("structure_anchor_temp_fatigue_base", 0.7))
                    + float(self._config.get("structure_anchor_temp_fatigue_rho_gain", 0.6)) * rho
                ),
                8,
            )

        tail_ids = [
            structure_id
            for structure_id in cam_structure_ids
            if float(budget_er_map.get(structure_id, 0.0)) + float(budget_ev_map.get(structure_id, 0.0)) > 0.0
        ]
        tail_fragments = self._build_internal_fragments(
            source_group_id="",
            source_phase="tail_residual",
            structure_ids=tail_ids,
            transfer_er_map=budget_er_map,
            transfer_ev_map=budget_ev_map,
            structure_store=structure_store,
        )
        internal_fragments.extend(tail_fragments)
        internal_fragments = self._merge_internal_fragments(internal_fragments)

        if self._config.get("detail_log_dump_group_match_profile", True):
            self._logger.detail(
                trace_id=trace_id,
                tick_id=tick_id,
                step="structure_level_match_profile",
                message_zh="结构级查存一体轮次摘要",
                message_en="Structure-level retrieval round summaries",
                info={
                    "round_summaries": round_summaries,
                    "cam_structure_ids": cam_structure_ids,
                },
            )

        return {
            "code": "OK",
            "message": "Structure-level retrieval-storage completed",
            "cam_stub_count": len(cam_structure_ids),
            "round_count": len(round_summaries) if round_summaries else len(debug_round_details),
            "matched_group_ids": list(dict.fromkeys(matched_group_ids)),
            "new_group_ids": list(dict.fromkeys(new_group_ids)),
            "bias_structure_ids": list(dict.fromkeys(bias_structure_ids)),
            "bias_projections": bias_projections,
            "internal_stimulus_fragments": internal_fragments,
            "episodic_memory_id": episodic_memory_id,
            "fallback_used": fallback_used,
            "debug": {
                "cam_items": debug_cam_items,
                "round_details": debug_round_details,
                "new_group_details": list({item.get("group_id", ""): item for item in debug_new_group_details if item.get("group_id", "")}.values()),
            },
        }

    def _empty_result(self, *, code: str, message: str) -> dict:
        return {
            "code": code,
            "message": message,
            "cam_stub_count": 0,
            "round_count": 0,
            "matched_group_ids": [],
            "new_group_ids": [],
            "bias_structure_ids": [],
            "bias_projections": [],
            "internal_stimulus_fragments": [],
            "episodic_memory_id": "",
            "fallback_used": False,
            "debug": {"cam_items": [], "round_details": [], "new_group_details": []},
        }

    def _collect_cam_items(self, *, st_items: list[dict], structure_store, now_ms: int) -> list[dict]:
        cam_items = []
        for order_index, item in enumerate(st_items):
            structure_id = item.get("ref_object_id", "") or item.get("id", "")
            structure_obj = structure_store.get(structure_id)
            if not structure_id or not structure_obj:
                continue
            runtime_stats = self._preview_structure_stats(structure_obj, now_ms=now_ms)
            er = round(max(0.0, float(item.get("er", 0.0))), 8)
            ev = round(max(0.0, float(item.get("ev", 0.0))), 8)
            cam_items.append(
                {
                    "structure_id": structure_id,
                    "structure_obj": structure_obj,
                    "display_text": self._structure_display_text(structure_obj),
                    "er": er,
                    "ev": ev,
                    "order_index": order_index,
                    "runtime_weight": runtime_stats["runtime_weight"],
                    "debug": {
                        "structure_id": structure_id,
                        "display_text": self._structure_display_text(structure_obj),
                        "sequence_groups": list(structure_obj.get("structure", {}).get("sequence_groups", [])),
                        "er": er,
                        "ev": ev,
                        "total_energy": round(er + ev, 8),
                        "base_weight": runtime_stats["base_weight"],
                        "recent_gain": runtime_stats["recent_gain"],
                        "fatigue": runtime_stats["fatigue"],
                        "runtime_weight": runtime_stats["runtime_weight"],
                    },
                }
            )
        return cam_items

    def _preview_structure_stats(self, structure_obj: dict, *, now_ms: int) -> dict:
        preview = {"stats": dict(structure_obj.get("stats", {}))}
        self._weight.decay_structure(preview, now_ms=now_ms, round_step=1)
        stats = preview.get("stats", {})
        runtime_weight = self._weight.compute_runtime_weight(
            base_weight=float(stats.get("base_weight", 1.0)),
            recent_gain=float(stats.get("recent_gain", 1.0)),
            fatigue=float(stats.get("fatigue", 0.0)),
        )
        return {
            "base_weight": round(float(stats.get("base_weight", 1.0)), 8),
            "recent_gain": round(float(stats.get("recent_gain", 1.0)), 8),
            "fatigue": round(float(stats.get("fatigue", 0.0)), 8),
            "runtime_weight": round(float(runtime_weight), 8),
        }

    def _preview_group_stats(self, group_obj: dict, *, now_ms: int) -> dict:
        preview = {"stats": dict(group_obj.get("stats", {}))}
        self._weight.decay_group(preview, now_ms=now_ms, round_step=1)
        stats = preview.get("stats", {})
        runtime_weight = self._weight.compute_runtime_weight(
            base_weight=float(stats.get("base_weight", 1.0)),
            recent_gain=float(stats.get("recent_gain", 1.0)),
            fatigue=float(stats.get("fatigue", 0.0)),
        )
        return {
            "base_weight": round(float(stats.get("base_weight", 1.0)), 8),
            "recent_gain": round(float(stats.get("recent_gain", 1.0)), 8),
            "fatigue": round(float(stats.get("fatigue", 0.0)), 8),
            "runtime_weight": round(float(runtime_weight), 8),
        }

    def _preview_entry_stats(self, entry: dict, *, now_ms: int) -> dict:
        preview = dict(entry)
        self._weight.decay_entry(preview, now_ms=now_ms, round_step=1)
        runtime_weight = self._weight.entry_runtime_weight(preview)
        return {
            "base_weight": round(float(preview.get("base_weight", 1.0)), 8),
            "recent_gain": round(float(preview.get("recent_gain", 1.0)), 8),
            "fatigue": round(float(preview.get("fatigue", 0.0)), 8),
            "runtime_weight": round(float(runtime_weight), 8),
        }

    def _build_runtime_profile_from_cam(
        self,
        *,
        cam_items: list[dict],
        budget_er_map: dict[str, float],
        budget_ev_map: dict[str, float],
        cut_engine,
        origin_frame_id: str,
    ) -> dict:
        units = []
        for order_index, item in enumerate(cam_items):
            structure_id = item["structure_id"]
            structure_obj = item["structure_obj"]
            units.append(
                self._make_structure_unit(
                    structure_id=structure_id,
                    display_text=self._structure_display_text(structure_obj),
                    structure_obj=structure_obj,
                    er=float(budget_er_map.get(structure_id, 0.0)),
                    ev=float(budget_ev_map.get(structure_id, 0.0)),
                    order_index=order_index,
                    source_type="cam",
                    origin_frame_id=origin_frame_id,
                )
            )
        return self._profile_from_units(units=units, cut_engine=cut_engine, ext={"kind": "structure_level_runtime"})

    def _make_structure_unit(
        self,
        *,
        structure_id: str,
        display_text: str,
        structure_obj: dict | None = None,
        er: float,
        ev: float,
        order_index: int,
        source_type: str,
        origin_frame_id: str,
    ) -> dict:
        fuzzy_metadata = self._build_structure_fuzzy_metadata(structure_obj)
        return {
            "unit_id": structure_id,
            "object_type": "st",
            "token": fuzzy_metadata.get("grouped_display_text", "") or display_text or structure_id,
            "display_text": fuzzy_metadata.get("grouped_display_text", "") or display_text or structure_id,
            "unit_role": "feature",
            "unit_signature": f"ST:{structure_id}",
            "sequence_index": order_index,
            "group_index": order_index,
            "source_group_index": order_index,
            "source_type": source_type,
            "origin_frame_id": origin_frame_id,
            "er": round(max(0.0, float(er)), 8),
            "ev": round(max(0.0, float(ev)), 8),
            "total_energy": round(max(0.0, float(er)) + max(0.0, float(ev)), 8),
            "is_punctuation": False,
            "display_visible": True,
            "is_placeholder": False,
            "bundle_id": "",
            "bundle_anchor_unit_id": "",
            "bundle_anchor_signature": "",
            "bundle_signature": "",
            "bundle_member_unit_ids": [],
            "bundle_member_signatures": [],
            "structure_display_text": display_text or structure_id,
            "structure_grouped_display_text": fuzzy_metadata.get("grouped_display_text", "") or display_text or structure_id,
            "structure_sequence_groups": [dict(group) for group in (structure_obj or {}).get("structure", {}).get("sequence_groups", []) if isinstance(group, dict)],
            "structure_display_template": fuzzy_metadata.get("display_template", "") or display_text or structure_id,
            "structure_fuzzy_signature": fuzzy_metadata.get("fuzzy_signature", "") or f"ST:{structure_id}",
            "structure_numeric_slots": list(fuzzy_metadata.get("numeric_slots", [])),
        }

    # 结构级把 ST 当成“结构特征单元”比较时，不能只看 structure_id。
    # 这里显式抽取一个“数值可模糊匹配”的签名与显示模板，供 cut_engine
    # 在最大共同部分与结构组竞争时复用，避免 1.0 / 1.1 这种同类数值把本质相同的结构判成不同结构。
    def _build_structure_fuzzy_metadata(self, structure_obj: dict | None) -> dict:
        if not structure_obj:
            return {
                "fuzzy_signature": "",
                "numeric_slots": [],
                "grouped_display_text": "",
                "display_template": "",
            }
        structure = structure_obj.get("structure", {})
        sequence_groups = list(structure.get("sequence_groups", []))
        if not sequence_groups:
            return {
                "fuzzy_signature": "",
                "numeric_slots": [],
                "grouped_display_text": str(structure.get("display_text", structure_obj.get("id", ""))),
                "display_template": str(structure.get("display_text", structure_obj.get("id", ""))),
            }

        normalized_group_signatures: list[str] = []
        numeric_slots: list[dict] = []
        grouped_segments: list[str] = []
        template_segments: list[str] = []
        numeric_index = 0

        for group in sequence_groups:
            units = sorted(
                [dict(unit) for unit in group.get("units", []) if isinstance(unit, dict)],
                key=lambda item: int(item.get("sequence_index", 0)),
            )
            if not units:
                continue
            units_by_id = {str(unit.get("unit_id", "")): unit for unit in units if str(unit.get("unit_id", ""))}
            unit_signatures: list[str] = []
            bundle_signatures: list[str] = []
            covered_ids: set[str] = set()
            visible_segments: list[str] = []
            template_group_segments: list[str] = []

            for bundle in sorted(
                [dict(bundle) for bundle in group.get("csa_bundles", []) if isinstance(bundle, dict)],
                key=lambda item: int(units_by_id.get(str(item.get("anchor_unit_id", "")), {}).get("sequence_index", 0)),
            ):
                anchor_id = str(bundle.get("anchor_unit_id", ""))
                anchor_unit = units_by_id.get(anchor_id, {})
                if not anchor_unit:
                    continue
                anchor_token = str(anchor_unit.get("token", ""))
                if not anchor_token:
                    continue
                member_ids = [
                    str(member_id)
                    for member_id in bundle.get("member_unit_ids", [])
                    if str(member_id) in units_by_id
                ]
                attr_tokens: list[str] = []
                attr_template_tokens: list[str] = []
                attr_signatures: list[str] = []
                for member_id in member_ids:
                    if member_id == anchor_id:
                        continue
                    member = units_by_id.get(member_id, {})
                    normalized_signature, slot_value = self._normalize_structure_child_signature(member)
                    if not normalized_signature:
                        continue
                    attr_signatures.append(normalized_signature)
                    if slot_value is not None:
                        numeric_slots.append({"family": str(member.get("attribute_name", "")), "value": slot_value})
                        placeholder = f"{{{{NUM{numeric_index}}}}}"
                        numeric_index += 1
                        attr_tokens.append(str(member.get("token", "")))
                        attr_template_tokens.append(placeholder)
                    elif str(member.get("token", "")):
                        attr_tokens.append(str(member.get("token", "")))
                        attr_template_tokens.append(str(member.get("token", "")))
                if attr_signatures:
                    bundle_signatures.append(
                        f"CSA[{self._normalize_structure_child_signature(anchor_unit)[0]}=>{'|'.join(sorted(attr_signatures))}]"
                    )
                covered_ids.update(member_ids)
                segment_tokens = [anchor_token, *[token for token in attr_tokens if token]]
                template_tokens = [anchor_token, *[token for token in attr_template_tokens if token]]
                if segment_tokens:
                    visible_segments.append(f"({' + '.join(segment_tokens)})")
                if template_tokens:
                    template_group_segments.append(f"({' + '.join(template_tokens)})")

            for unit in units:
                unit_id = str(unit.get("unit_id", ""))
                normalized_signature, slot_value = self._normalize_structure_child_signature(unit)
                if normalized_signature:
                    unit_signatures.append(normalized_signature)
                if unit_id in covered_ids:
                    continue
                token = str(unit.get("token", ""))
                if not token:
                    continue
                if slot_value is not None:
                    numeric_slots.append({"family": str(unit.get("attribute_name", "")), "value": slot_value})
                    placeholder = f"{{{{NUM{numeric_index}}}}}"
                    numeric_index += 1
                    visible_segments.append(token)
                    template_group_segments.append(placeholder)
                else:
                    visible_segments.append(token)
                    template_group_segments.append(token)

            unit_signatures = sorted(signature for signature in unit_signatures if signature)
            bundle_signatures = sorted(signature for signature in bundle_signatures if signature)
            unit_part = "|".join(unit_signatures)
            bundle_part = "|".join(bundle_signatures)
            if unit_part and bundle_part:
                normalized_group_signatures.append(f"U[{unit_part}]#B[{bundle_part}]")
            elif bundle_part:
                normalized_group_signatures.append(f"B[{bundle_part}]")
            elif unit_part:
                normalized_group_signatures.append(f"U[{unit_part}]")
            if visible_segments:
                grouped_segments.append(f"{{{' + '.join(visible_segments)}}}")
            if template_group_segments:
                template_segments.append(f"{{{' + '.join(template_group_segments)}}}")

        grouped_display_text = " / ".join(segment for segment in grouped_segments if segment)
        display_template = " / ".join(segment for segment in template_segments if segment)
        return {
            "fuzzy_signature": "||".join(signature for signature in normalized_group_signatures if signature),
            "numeric_slots": numeric_slots,
            "grouped_display_text": grouped_display_text,
            "display_template": display_template or grouped_display_text,
        }

    @staticmethod
    def _normalize_structure_child_signature(unit: dict) -> tuple[str, float | None]:
        role = str(unit.get("unit_role", unit.get("role", "")) or "")
        attribute_name = str(unit.get("attribute_name", ""))
        attribute_value = unit.get("attribute_value")
        numeric_value = StructureRetrievalEngine._coerce_numeric(attribute_value)
        if role == "attribute" and attribute_name and numeric_value is not None:
            return f"A_NUM_FAMILY:{attribute_name}", float(numeric_value)
        signature = str(unit.get("unit_signature", ""))
        if signature:
            return signature, None
        token = str(unit.get("token", ""))
        prefix = "P" if bool(unit.get("is_placeholder")) else ("A" if role == "attribute" else "F")
        return (f"{prefix}:{token}" if token else "", None)

    @staticmethod
    def _coerce_numeric(value) -> float | None:
        if isinstance(value, bool):
            return None
        if isinstance(value, (int, float)):
            return float(value)
        text = str(value or "").strip()
        if not text:
            return None
        try:
            return float(text)
        except ValueError:
            return None

    def _make_placeholder_unit(self, *, placeholder_token: str, order_index: int, origin_frame_id: str) -> dict:
        return {
            "unit_id": f"placeholder::{placeholder_token}::{order_index}",
            "object_type": "st_placeholder",
            "token": placeholder_token,
            "display_text": placeholder_token,
            "unit_role": "placeholder",
            "unit_signature": f"P:{placeholder_token}",
            "sequence_index": order_index,
            "group_index": order_index,
            "source_group_index": order_index,
            "source_type": "structure_local",
            "origin_frame_id": origin_frame_id,
            "er": 0.0,
            "ev": 0.0,
            "total_energy": 0.0,
            "is_punctuation": False,
            "display_visible": True,
            "is_placeholder": True,
            "bundle_id": "",
            "bundle_anchor_unit_id": "",
            "bundle_anchor_signature": "",
            "bundle_signature": "",
            "bundle_member_unit_ids": [],
            "bundle_member_signatures": [],
        }

    def _profile_from_units(self, *, units: list[dict], cut_engine, ext: dict | None = None) -> dict:
        groups = []
        for order_index, unit in enumerate(units):
            groups.append(
                {
                    "group_index": order_index,
                    "source_type": unit.get("source_type", "structure_local"),
                    "origin_frame_id": unit.get("origin_frame_id", ""),
                    "units": [{**unit, "group_index": order_index, "sequence_index": order_index}],
                }
            )
        profile = cut_engine.build_sequence_profile_from_groups(groups)
        merged_ext = dict(profile.get("ext", {}))
        merged_ext.update(ext or {})
        profile["ext"] = merged_ext
        return profile

    def _select_anchor_item(
        self,
        *,
        cam_items: list[dict],
        budget_er_map: dict[str, float],
        budget_ev_map: dict[str, float],
        temp_anchor_fatigue: dict[str, float],
        skip_structure_ids: set[str] | None,
        now_ms: int,
    ) -> dict | None:
        ranked = []
        for item in cam_items:
            if skip_structure_ids and item["structure_id"] in skip_structure_ids:
                continue
            structure_obj = item["structure_obj"]
            runtime_stats = self._preview_structure_stats(structure_obj, now_ms=now_ms)
            total_energy = round(
                float(budget_er_map.get(item["structure_id"], 0.0)) + float(budget_ev_map.get(item["structure_id"], 0.0)),
                8,
            )
            temp_fatigue = float(temp_anchor_fatigue.get(item["structure_id"], 0.0))
            score = self._anchor_score(
                total_energy=total_energy,
                runtime_weight=float(runtime_stats["runtime_weight"]),
                temp_fatigue=temp_fatigue,
            )
            ranked.append((score, item["order_index"], temp_fatigue, runtime_stats, item))
        if not ranked:
            return None
        ranked.sort(key=lambda payload: (-payload[0], payload[1]))
        score, _, temp_fatigue, runtime_stats, item = ranked[0]
        return {
            **item,
            "anchor_score": round(float(score), 8),
            "temp_anchor_fatigue": round(float(temp_fatigue), 8),
            "runtime_weight": runtime_stats["runtime_weight"],
        }

    def _build_implicit_single_group_debug(
        self,
        *,
        anchor_item: dict,
        structure_store,
        cut_engine,
        runtime_profile: dict,
        tick_id: str,
        round_index: int,
    ) -> dict:
        structure_id = str(anchor_item.get("structure_id", ""))
        structure_obj = anchor_item.get("structure_obj") or structure_store.get(structure_id)
        display_text = self._structure_display_text(structure_obj) or structure_id
        single_profile = self._profile_from_units(
            units=[
                self._make_structure_unit(
                    structure_id=structure_id,
                    display_text=display_text,
                    structure_obj=structure_obj,
                    er=float(anchor_item.get("er", 0.0)),
                    ev=float(anchor_item.get("ev", 0.0)),
                    order_index=0,
                    source_type="structure_owner",
                    origin_frame_id=tick_id,
                )
            ],
            cut_engine=cut_engine,
            ext={"kind": "implicit_single_structure_group", "owner_id": structure_id},
        )
        owner_placeholder = self._owner_placeholder_token(
            owner_kind="st",
            owner_id=structure_id,
            owner_display_text=display_text,
        )
        residual_profile = self._build_relative_residual_profile(
            full_profile=runtime_profile,
            covered_structure_ids=[structure_id],
            owner_placeholder=owner_placeholder,
            cut_engine=cut_engine,
            origin_frame_id=f"{tick_id}:{round_index}:single:{structure_id}",
        ) or {"content_signature": "", "flat_tokens": [], "sequence_groups": []}
        runtime_total = max(1e-8, self._profile_total_energy(runtime_profile))
        anchor_total = round(float(anchor_item.get("er", 0.0)) + float(anchor_item.get("ev", 0.0)), 8)
        return {
            "group_id": self._implicit_single_group_id(structure_id),
            "group_kind": "implicit_single_st",
            "synthetic": True,
            "display_text": display_text,
            "required_structure_ids": [structure_id],
            "bias_structure_ids": [],
            "required_structures": self._build_structure_refs([structure_id], structure_store),
            "bias_structures": [],
            "avg_energy_profile": {structure_id: 1.0},
            "content_signature": single_profile.get("content_signature", ""),
            "temporal_signature": single_profile.get("content_signature", ""),
            "flat_tokens": list(single_profile.get("flat_tokens", [])),
            "sequence_groups": list(single_profile.get("sequence_groups", [])),
            "base_weight": round(float(anchor_item.get("runtime_weight", 1.0)), 8),
            "recent_gain": 1.0,
            "fatigue": 0.0,
            "runtime_weight": round(float(anchor_item.get("runtime_weight", 1.0)), 8),
            "score": 1.0,
            "competition_score": 1.0,
            "similarity": 1.0,
            "base_similarity": 1.0,
            "coverage_ratio": round(anchor_total / runtime_total, 8),
            "structure_ratio": 1.0,
            "wave_similarity": 1.0,
            "path_strength": 1.0,
            "entry_runtime_weight": round(float(anchor_item.get("runtime_weight", 1.0)), 8),
            "chain_depth": 0,
            "owner_kind": "st",
            "owner_id": structure_id,
            "common_part": {
                "common_tokens": list(single_profile.get("flat_tokens", [])),
                "common_length": int(single_profile.get("unit_count", 1)),
                "common_group_count": len(single_profile.get("sequence_groups", [])),
                "common_signature": single_profile.get("content_signature", ""),
                "common_display": single_profile.get("display_text", display_text),
                "common_groups": list(single_profile.get("sequence_groups", [])),
                "matched_pairs": [],
                "existing_range": [0, len(single_profile.get("sequence_groups", []))],
                "incoming_range": [0, len(single_profile.get("sequence_groups", []))],
                "matched_existing_group_indices": list(range(len(single_profile.get("sequence_groups", [])))),
                "matched_incoming_group_indices": list(range(len(single_profile.get("sequence_groups", [])))),
                "residual_existing_tokens": [],
                "residual_incoming_tokens": list(residual_profile.get("flat_tokens", [])),
                "residual_existing_groups": [],
                "residual_incoming_groups": list(residual_profile.get("sequence_groups", [])),
                "residual_existing_signature": "",
                "residual_incoming_signature": residual_profile.get("content_signature", ""),
            },
        }

    @staticmethod
    def _implicit_single_group_id(structure_id: str) -> str:
        return f"sg_single_{structure_id}"

    def _anchor_score(self, *, total_energy: float, runtime_weight: float, temp_fatigue: float) -> float:
        if total_energy <= 0.0:
            return 0.0
        runtime_signal = math.tanh(math.log(max(1e-6, float(runtime_weight))) / max(0.25, float(self._config.get("structure_anchor_runtime_scale", 1.35))))
        runtime_factor = math.exp(float(self._config.get("structure_anchor_runtime_gain", 0.22)) * runtime_signal)
        return round(float(total_energy) * runtime_factor / (1.0 + max(0.0, float(temp_fatigue))), 8)

    def _build_anchor_debug(self, anchor_item: dict) -> dict:
        return {
            "structure_id": anchor_item.get("structure_id", ""),
            "display_text": anchor_item.get("display_text", ""),
            "sequence_groups": list((anchor_item.get("structure_obj") or {}).get("structure", {}).get("sequence_groups", [])),
            "er": round(float(anchor_item.get("er", 0.0)), 8),
            "ev": round(float(anchor_item.get("ev", 0.0)), 8),
            "total_energy": round(float(anchor_item.get("er", 0.0)) + float(anchor_item.get("ev", 0.0)), 8),
            "runtime_weight": round(float(anchor_item.get("runtime_weight", 1.0)), 8),
            "temp_anchor_fatigue": round(float(anchor_item.get("temp_anchor_fatigue", 0.0)), 8),
            "anchor_score": round(float(anchor_item.get("anchor_score", 0.0)), 8),
        }

    def _resolve_anchor_chain_match(
        self,
        *,
        anchor_structure_id: str,
        runtime_profile: dict,
        budget_er_map: dict[str, float],
        budget_ev_map: dict[str, float],
        structure_store,
        group_store,
        pointer_index,
        cut_engine,
        trace_id: str,
        tick_id: str,
        round_index: int,
        now_ms: int,
    ) -> tuple[dict, dict | None, list[dict]]:
        candidate_details: list[dict] = []
        lookup = self._collect_local_group_candidates(
            owner_kind="st",
            owner_id=anchor_structure_id,
            structure_store=structure_store,
            group_store=group_store,
            pointer_index=pointer_index,
            cut_engine=cut_engine,
            trace_id=trace_id,
            tick_id=tick_id,
            now_ms=now_ms,
        )
        best, local_details = self._best_group_match(
            anchor_structure_id=anchor_structure_id,
            runtime_profile=runtime_profile,
            budget_er_map=budget_er_map,
            budget_ev_map=budget_ev_map,
            candidates=lookup.get("candidates", []),
            structure_store=structure_store,
            group_store=group_store,
            cut_engine=cut_engine,
            now_ms=now_ms,
            min_required_count=2,
            parent_match=None,
        )
        candidate_details = self._upsert_group_details(candidate_details, local_details)
        if not best:
            return {
                "candidate_source": "anchor_structure_chain",
                "used_fallback": bool(lookup.get("used_fallback")),
                "chain_steps": lookup.get("chain_steps", []),
            }, None, candidate_details

        max_depth = max(1, int(runtime_profile.get("unit_count", len(runtime_profile.get("flat_tokens", [])))))
        seen_group_ids = {best.get("group_id", "")}
        chain_steps = list(lookup.get("chain_steps", []))
        for depth in range(1, max_depth + 1):
            child_lookup = self._collect_local_group_candidates(
                owner_kind="sg",
                owner_id=best.get("group_id", ""),
                structure_store=structure_store,
                group_store=group_store,
                pointer_index=pointer_index,
                cut_engine=cut_engine,
                trace_id=trace_id,
                tick_id=tick_id,
                now_ms=now_ms,
            )
            chain_steps.append(
                {
                    "owner_kind": "sg",
                    "owner_id": best.get("group_id", ""),
                    "owner_display_text": best.get("display_text", ""),
                    "candidate_count": len(child_lookup.get("candidates", [])),
                    "round_index": round_index,
                    "depth": depth,
                }
            )
            child_best, child_details = self._best_group_match(
                anchor_structure_id=anchor_structure_id,
                runtime_profile=runtime_profile,
                budget_er_map=budget_er_map,
                budget_ev_map=budget_ev_map,
                candidates=[candidate for candidate in child_lookup.get("candidates", []) if candidate.get("group_id", "") not in seen_group_ids],
                structure_store=structure_store,
                group_store=group_store,
                cut_engine=cut_engine,
                now_ms=now_ms,
                min_required_count=max(len(best.get("required_ids", [])) + 1, 2),
                parent_match=best,
            )
            candidate_details = self._upsert_group_details(candidate_details, child_details)
            if not child_best:
                break
            best = child_best
            seen_group_ids.add(best.get("group_id", ""))
        return {
            "candidate_source": "anchor_structure_chain",
            "used_fallback": bool(lookup.get("used_fallback")),
            "chain_steps": chain_steps,
        }, best, candidate_details

    def _collect_local_group_candidates(
        self,
        *,
        owner_kind: str,
        owner_id: str,
        structure_store,
        group_store,
        pointer_index,
        cut_engine,
        trace_id: str,
        tick_id: str,
        now_ms: int,
    ) -> dict:
        owner_ctx = self._open_owner_context(
            owner_kind=owner_kind,
            owner_id=owner_id,
            structure_store=structure_store,
            group_store=group_store,
            pointer_index=pointer_index,
            cut_engine=cut_engine,
            trace_id=trace_id,
            tick_id=tick_id,
        )
        if not owner_ctx:
            return {"candidates": [], "used_fallback": False, "chain_steps": []}
        numeric_anchor = self._extract_numeric_atomic_structure(owner_ctx.get("structure_obj")) if owner_kind == "st" else None
        if numeric_anchor and pointer_index is not None:
            candidates = self._collect_numeric_bucket_group_candidates(
                numeric_anchor=numeric_anchor,
                pointer_index=pointer_index,
                structure_store=structure_store,
                group_store=group_store,
                cut_engine=cut_engine,
                trace_id=trace_id,
                tick_id=tick_id,
                now_ms=now_ms,
            )
        else:
            candidates = self._group_candidates_from_owner_ctx(
                owner_ctx=owner_ctx,
                structure_store=structure_store,
                group_store=group_store,
                cut_engine=cut_engine,
                now_ms=now_ms,
            )
        candidates.sort(
            key=lambda item: (
                -float(item.get("entry_runtime_weight", 0.0)),
                -float(item.get("group_runtime_weight", 0.0)),
                item.get("group_id", ""),
            )
        )
        return {
            "candidates": candidates,
            "used_fallback": bool(owner_ctx.get("used_fallback")),
            "chain_steps": [
                {
                    "owner_kind": owner_kind,
                    "owner_id": owner_id,
                    "owner_display_text": owner_ctx.get("owner_display_text", owner_id),
                    "candidate_count": len(candidates),
                }
            ],
        }

    def _group_candidates_from_owner_ctx(self, *, owner_ctx: dict, structure_store, group_store, cut_engine, now_ms: int) -> list[dict]:
        candidates = []
        owner_kind = str(owner_ctx.get("owner_kind", ""))
        owner_id = str(owner_ctx.get("owner_id", ""))
        owner_display_text = owner_ctx.get("owner_display_text", owner_id)
        for entry in list(owner_ctx.get("group_table", [])):
            self._ensure_group_entry_schema(entry)
            group_id = str(entry.get("group_id", ""))
            group_obj = group_store.get(group_id)
            if not group_obj:
                continue
            relative_profile = self._group_entry_relative_profile(
                owner_ctx=owner_ctx,
                entry=entry,
                group_obj=group_obj,
                cut_engine=cut_engine,
            )
            if not relative_profile:
                continue
            entry_stats = self._preview_entry_stats(entry, now_ms=now_ms)
            group_stats = self._preview_group_stats(group_obj, now_ms=now_ms)
            candidates.append(
                {
                    "owner_kind": owner_kind,
                    "owner_id": owner_id,
                    "owner_display_text": owner_display_text,
                    "entry_ref": entry,
                    "entry_id": entry.get("entry_id", ""),
                    "entry_runtime_weight": entry_stats["runtime_weight"],
                    "group_id": group_id,
                    "group_obj": group_obj,
                    "group_runtime_weight": group_stats["runtime_weight"],
                    "relative_profile": relative_profile,
                    "full_profile": self._group_full_profile(group_obj=group_obj, structure_store=structure_store, cut_engine=cut_engine),
                }
            )
        return candidates

    def _collect_numeric_bucket_group_candidates(
        self,
        *,
        numeric_anchor: dict,
        pointer_index,
        structure_store,
        group_store,
        cut_engine,
        trace_id: str,
        tick_id: str,
        now_ms: int,
    ) -> list[dict]:
        buckets = pointer_index.resolve_numeric_buckets(
            attribute_name=numeric_anchor.get("family", ""),
            value=numeric_anchor.get("value"),
            create_if_missing=True,
            neighbor_count=max(1, int(self._config.get("numeric_bucket_neighbor_count", 2))),
        )
        candidates = []
        seen_owner_ids: set[str] = set()
        for bucket in buckets:
            for bucket_owner_id in bucket.get("candidate_ids", []):
                bucket_owner_id = str(bucket_owner_id)
                if not bucket_owner_id or bucket_owner_id in seen_owner_ids:
                    continue
                seen_owner_ids.add(bucket_owner_id)
                bucket_owner_ctx = self._open_owner_context(
                    owner_kind="st",
                    owner_id=bucket_owner_id,
                    structure_store=structure_store,
                    group_store=group_store,
                    pointer_index=pointer_index,
                    cut_engine=cut_engine,
                    trace_id=trace_id,
                    tick_id=tick_id,
                )
                if not bucket_owner_ctx:
                    continue
                candidates.extend(
                    self._group_candidates_from_owner_ctx(
                        owner_ctx=bucket_owner_ctx,
                        structure_store=structure_store,
                        group_store=group_store,
                        cut_engine=cut_engine,
                        now_ms=now_ms,
                    )
                )
        return candidates

    def _extract_numeric_atomic_structure(self, structure_obj: dict | None) -> dict | None:
        if not structure_obj:
            return None
        sequence_groups = list(structure_obj.get("structure", {}).get("sequence_groups", []))
        units = [
            dict(unit)
            for group in sequence_groups
            for unit in group.get("units", [])
            if isinstance(unit, dict)
        ]
        if len(units) != 1:
            return None
        unit = units[0]
        family = str(unit.get("attribute_name", ""))
        numeric_value = self._coerce_numeric(unit.get("attribute_value"))
        if str(unit.get("unit_role", unit.get("role", "")) or "") != "attribute" or not family or numeric_value is None:
            return None
        return {"family": family, "value": numeric_value}

    def _best_group_match(
        self,
        *,
        anchor_structure_id: str,
        runtime_profile: dict,
        budget_er_map: dict[str, float],
        budget_ev_map: dict[str, float],
        candidates: list[dict],
        structure_store,
        group_store,
        cut_engine,
        now_ms: int,
        min_required_count: int,
        parent_match: dict | None,
    ) -> tuple[dict | None, list[dict]]:
        del group_store
        best = None
        candidate_details = []
        current_total_energy = max(1e-8, self._profile_total_energy(runtime_profile))
        for candidate in candidates:
            group_obj = candidate.get("group_obj") or {}
            group_profile = candidate.get("full_profile") or self._group_full_profile(group_obj=group_obj, structure_store=structure_store, cut_engine=cut_engine)
            common_part = cut_engine.maximum_common_part(
                group_profile.get("sequence_groups", []),
                runtime_profile.get("sequence_groups", []),
            )
            required_ids = list(group_obj.get("required_structure_ids", []))
            existing_length = max(1, len(required_ids) or int(group_profile.get("unit_count", 0)))
            matched_current_units = self._collect_matched_units_from_common_part(runtime_profile, common_part, use_existing_side=False)
            matched_current_ids = self._matched_structure_ids_from_units(matched_current_units)
            coverage_ratio = round(
                self._profile_total_energy_from_units(matched_current_units) / current_total_energy,
                8,
            ) if current_total_energy > 0 else 0.0
            structure_ratio = round(
                max(0.0, min(1.0, float(int(common_part.get("common_length", 0))) / max(1, existing_length))),
                8,
            )
            matched_existing_length = int(common_part.get("matched_existing_unit_count", 0))
            full_structure_included = bool(
                int(common_part.get("common_length", 0)) > 0
                and not common_part.get("residual_existing_signature", "")
                and matched_existing_length >= existing_length
                and bool(common_part.get("bundle_constraints_ok_existing_included", True))
            )
            contains_anchor = anchor_structure_id in matched_current_ids
            wave_similarity = self._wave_similarity(
                required_ids=required_ids,
                budget_er_map=budget_er_map,
                budget_ev_map=budget_ev_map,
                avg_energy_profile=group_obj.get("avg_energy_profile", {}),
            )
            path_strength = self._path_strength(
                group_runtime_weight=float(candidate.get("group_runtime_weight", 1.0)),
                entry_runtime_weight=float(candidate.get("entry_runtime_weight", 1.0)),
            )
            base_similarity = self._compose_group_match_score(
                coverage_ratio=coverage_ratio,
                structure_ratio=structure_ratio,
                wave_similarity=wave_similarity,
            )
            eligible = bool(
                full_structure_included
                and contains_anchor
                and len(required_ids) >= max(1, int(min_required_count))
            )
            competition_score = self._apply_runtime_modulation(
                base_similarity=base_similarity,
                path_strength=path_strength,
            ) if eligible else 0.0
            detail = {
                **self._build_group_debug_payload(group_obj, structure_store, cut_engine),
                "owner_kind": candidate.get("owner_kind", ""),
                "owner_id": candidate.get("owner_id", ""),
                "owner_display_text": candidate.get("owner_display_text", ""),
                "entry_id": candidate.get("entry_id", ""),
                "runtime_weight": round(float(candidate.get("group_runtime_weight", 1.0)), 8),
                "entry_runtime_weight": round(float(candidate.get("entry_runtime_weight", 1.0)), 8),
                "path_strength": round(float(path_strength), 8),
                "base_similarity": round(float(base_similarity), 8),
                "coverage_ratio": round(float(coverage_ratio), 8),
                "structure_ratio": round(float(structure_ratio), 8),
                "wave_similarity": round(float(wave_similarity), 8),
                "score": round(float(competition_score), 8),
                "competition_score": round(float(competition_score), 8),
                "similarity": round(float(competition_score), 8),
                "full_structure_included": full_structure_included,
                "contains_anchor": contains_anchor,
                "eligible": eligible,
                "common_part": common_part,
                "chain_depth": int(parent_match.get("chain_depth", 0) + 1 if parent_match else 1),
            }
            candidate_details.append(detail)
            if not eligible:
                continue
            if self._is_better_group_match(detail, best):
                best = {
                    "group_id": group_obj.get("id", ""),
                    "display_text": self._group_display_text(group_obj),
                    "required_ids": required_ids,
                    "competition_score": round(float(competition_score), 8),
                    "base_similarity": round(float(base_similarity), 8),
                    "coverage_ratio": round(float(coverage_ratio), 8),
                    "structure_ratio": round(float(structure_ratio), 8),
                    "wave_similarity": round(float(wave_similarity), 8),
                    "path_strength": round(float(path_strength), 8),
                    "runtime_weight": round(float(candidate.get("group_runtime_weight", 1.0)), 8),
                    "entry_runtime_weight": round(float(candidate.get("entry_runtime_weight", 1.0)), 8),
                    "common_part": common_part,
                    "chain_depth": int(parent_match.get("chain_depth", 0) + 1 if parent_match else 1),
                    "owner_kind": candidate.get("owner_kind", ""),
                    "owner_id": candidate.get("owner_id", ""),
                    "path_entries": list(parent_match.get("path_entries", [])) if parent_match else [],
                }
                best["path_entries"].append(
                    {
                        "owner_kind": candidate.get("owner_kind", ""),
                        "owner_id": candidate.get("owner_id", ""),
                        "entry_id": candidate.get("entry_id", ""),
                        "group_id": group_obj.get("id", ""),
                    }
                )
        candidate_details.sort(
            key=lambda item: (
                0 if item.get("eligible") else 1,
                -float(item.get("competition_score", 0.0)),
                -len(item.get("required_structure_ids", [])),
                -float(item.get("entry_runtime_weight", 0.0)),
                -float(item.get("runtime_weight", 0.0)),
            )
        )
        return best, candidate_details

    @staticmethod
    def _is_better_group_match(candidate: dict, current_best: dict | None) -> bool:
        if current_best is None:
            return True
        candidate_key = (
            float(candidate.get("competition_score", 0.0)),
            len(candidate.get("required_structure_ids", [])),
            float(candidate.get("entry_runtime_weight", 0.0)),
            float(candidate.get("runtime_weight", 0.0)),
        )
        current_key = (
            float(current_best.get("competition_score", 0.0)),
            len(current_best.get("required_ids", [])),
            float(current_best.get("entry_runtime_weight", 0.0)),
            float(current_best.get("runtime_weight", 0.0)),
        )
        return candidate_key > current_key

    def _path_strength(self, *, group_runtime_weight: float, entry_runtime_weight: float) -> float:
        return round(math.sqrt(max(1e-8, float(group_runtime_weight)) * max(1e-8, float(entry_runtime_weight))), 8)

    def _compose_group_match_score(self, *, coverage_ratio: float, structure_ratio: float, wave_similarity: float) -> float:
        joint_ratio = max(0.0, min(1.0, float(min(coverage_ratio, structure_ratio))))
        if joint_ratio >= 1.0 and wave_similarity >= 1.0:
            return 1.0
        coverage_curve = self._coverage_curve(joint_ratio)
        wave_floor = max(0.0, min(1.0, float(self._config.get("structure_wave_similarity_floor", 0.35))))
        shape_factor = wave_floor + (1.0 - wave_floor) * max(0.0, min(1.0, float(wave_similarity)))
        score = coverage_curve * shape_factor
        if score >= 1.0:
            return 1.0
        return round(max(0.0, score), 8)

    def _coverage_curve(self, raw_ratio: float) -> float:
        bounded = max(0.0, min(1.0, float(raw_ratio)))
        if bounded <= 0.0:
            return 0.0
        denoise = self._sigmoid(
            bounded,
            midpoint=float(self._config.get("structure_competition_noise_mid", 0.01)),
            slope=max(1e-6, float(self._config.get("structure_competition_noise_scale", 0.004))),
        )
        hill = self._hill_score(
            bounded,
            half_point=float(self._config.get("structure_competition_half_ratio", 0.1)),
            power=float(self._config.get("structure_competition_curve_power", 1.15)),
        )
        return round(max(0.0, hill * denoise), 8)

    @staticmethod
    def _sigmoid(value: float, *, midpoint: float, slope: float) -> float:
        safe_slope = max(1e-6, float(slope))
        try:
            result = 1.0 / (1.0 + math.exp(-(float(value) - float(midpoint)) / safe_slope))
        except OverflowError:
            result = 0.0 if value < midpoint else 1.0
        return round(max(0.0, min(1.0, result)), 8)

    @staticmethod
    def _hill_score(value: float, *, half_point: float, power: float) -> float:
        bounded = max(0.0, min(1.0, float(value)))
        if bounded <= 0.0:
            return 0.0
        safe_half = max(1e-6, min(1.0, float(half_point)))
        safe_power = max(0.2, float(power))
        numerator = pow(bounded, safe_power)
        denominator = numerator + pow(safe_half, safe_power)
        if denominator <= 0.0:
            return 0.0
        return round(max(0.0, min(1.0, numerator / denominator)), 8)

    def _apply_runtime_modulation(self, *, base_similarity: float, path_strength: float) -> float:
        base = max(0.0, min(1.0, float(base_similarity)))
        if base <= 0.0 or base >= 1.0:
            return round(base, 8)
        runtime_signal = math.tanh(
            math.log(max(1e-8, float(path_strength))) / max(0.25, float(self._config.get("structure_path_runtime_scale", 1.35)))
        )
        adjustment = float(self._config.get("structure_path_runtime_gain", 0.3)) * runtime_signal * base * (1.0 - base)
        adjusted = base + adjustment
        return round(max(0.0, min(1.0, adjusted)), 8)

    def _wave_similarity(
        self,
        *,
        required_ids: list[str],
        budget_er_map: dict[str, float],
        budget_ev_map: dict[str, float],
        avg_energy_profile: dict[str, float],
    ) -> float:
        if not required_ids:
            return 0.0
        current = [max(0.0, float(budget_er_map.get(structure_id, 0.0)) + float(budget_ev_map.get(structure_id, 0.0))) for structure_id in required_ids]
        history = [max(0.0, float(avg_energy_profile.get(structure_id, 0.0))) for structure_id in required_ids]
        current = self._normalize_vector(current)
        history = self._normalize_vector(history)
        l1_similarity = 1.0 - 0.5 * sum(abs(left - right) for left, right in zip(current, history))
        centered_cosine = self._centered_cosine_similarity(current, history)
        slope_similarity = self._slope_similarity(current, history)
        return round(max(0.0, min(1.0, (l1_similarity + centered_cosine + slope_similarity) / 3.0)), 8)

    @staticmethod
    def _normalize_vector(values: list[float]) -> list[float]:
        total = sum(max(0.0, float(value)) for value in values)
        if total <= 0.0:
            return [1.0 / max(1, len(values)) for _ in values]
        return [max(0.0, float(value)) / total for value in values]

    @staticmethod
    def _centered_cosine_similarity(left: list[float], right: list[float]) -> float:
        if not left or not right or len(left) != len(right):
            return 0.0
        mean_left = sum(left) / len(left)
        mean_right = sum(right) / len(right)
        left_centered = [value - mean_left for value in left]
        right_centered = [value - mean_right for value in right]
        norm_left = math.sqrt(sum(value * value for value in left_centered))
        norm_right = math.sqrt(sum(value * value for value in right_centered))
        if norm_left <= 1e-8 and norm_right <= 1e-8:
            return 1.0
        if norm_left <= 1e-8 or norm_right <= 1e-8:
            return 0.5
        cosine = sum(lv * rv for lv, rv in zip(left_centered, right_centered)) / (norm_left * norm_right)
        return max(0.0, min(1.0, 0.5 + 0.5 * cosine))

    @staticmethod
    def _slope_similarity(left: list[float], right: list[float]) -> float:
        if len(left) <= 1 or len(right) <= 1 or len(left) != len(right):
            return 1.0
        left_deltas = [left[index + 1] - left[index] for index in range(len(left) - 1)]
        right_deltas = [right[index + 1] - right[index] for index in range(len(right) - 1)]
        norm_left = sum(abs(value) for value in left_deltas)
        norm_right = sum(abs(value) for value in right_deltas)
        if norm_left > 0.0:
            left_deltas = [value / norm_left for value in left_deltas]
        if norm_right > 0.0:
            right_deltas = [value / norm_right for value in right_deltas]
        diff = sum(abs(lv - rv) for lv, rv in zip(left_deltas, right_deltas)) / max(1, len(left_deltas))
        return max(0.0, min(1.0, 1.0 - 0.5 * diff))

    def _collect_matched_units_from_common_part(self, profile: dict, common_part: dict, *, use_existing_side: bool) -> list[dict]:
        groups = list(profile.get("sequence_groups", []))
        matched_units = []
        group_key = "existing_group_index" if use_existing_side else "incoming_group_index"
        unit_key = "existing_unit_refs" if use_existing_side else "incoming_unit_refs"
        similarity_key = "matched_existing_unit_similarities" if use_existing_side else "matched_incoming_unit_similarities"
        global_similarity_map = {
            str(unit_id): float(similarity)
            for unit_id, similarity in common_part.get(similarity_key, {}).items()
            if str(unit_id)
        }
        for pair in common_part.get("matched_pairs", []):
            group_index = int(pair.get(group_key, -1))
            if group_index < 0 or group_index >= len(groups):
                continue
            needed_ids = {str(unit_id) for unit_id in pair.get(unit_key, []) if str(unit_id)}
            pair_similarity_map = {
                str(unit_id): float(similarity)
                for unit_id, similarity in pair.get(similarity_key, {}).items()
                if str(unit_id)
            }
            for unit in groups[group_index].get("units", []):
                unit_id = str(unit.get("unit_id", ""))
                if needed_ids and unit_id in needed_ids:
                    similarity = pair_similarity_map.get(unit_id, global_similarity_map.get(unit_id, 1.0))
                    matched_units.append({**dict(unit), "match_similarity": round(max(0.0, min(1.0, float(similarity))), 8)})
        return matched_units

    @staticmethod
    def _matched_structure_ids_from_units(units: list[dict]) -> list[str]:
        ordered = []
        seen = set()
        for unit in units:
            unit_id = str(unit.get("unit_id", ""))
            if not unit_id or unit.get("is_placeholder"):
                continue
            if unit_id in seen:
                continue
            seen.add(unit_id)
            ordered.append(unit_id)
        return ordered

    def _profile_total_energy(self, profile: dict) -> float:
        return self._profile_total_energy_from_units(self._collect_profile_units(profile))

    @staticmethod
    def _profile_unit_count(profile: dict) -> int:
        return int(profile.get("unit_count", profile.get("token_count", len(profile.get("flat_tokens", [])))))

    def _profiles_fuzzy_equivalent(self, *, left_profile: dict, right_profile: dict, cut_engine) -> bool:
        common_part = cut_engine.maximum_common_part(
            left_profile.get("sequence_groups", []),
            right_profile.get("sequence_groups", []),
        )
        return bool(
            int(common_part.get("common_length", 0)) > 0
            and not common_part.get("residual_existing_signature", "")
            and not common_part.get("residual_incoming_signature", "")
            and int(common_part.get("matched_existing_unit_count", 0)) >= self._profile_unit_count(left_profile)
            and int(common_part.get("matched_incoming_unit_count", 0)) >= self._profile_unit_count(right_profile)
            # CSA 门控：两侧 bundle 约束也必须完全满足，避免属性跨对象拼接导致“看起来相等”。
            and bool(common_part.get("bundle_constraints_ok_exact", True))
        )

    @staticmethod
    def _profile_total_energy_from_units(units: list[dict]) -> float:
        return round(
            sum(
                (
                    max(0.0, float(unit.get("er", 0.0))) + max(0.0, float(unit.get("ev", 0.0)))
                ) * max(0.0, min(1.0, float(unit.get("match_similarity", 1.0))))
                for unit in units
                if isinstance(unit, dict)
            ),
            8,
        )

    @staticmethod
    def _collect_profile_units(profile: dict) -> list[dict]:
        return [dict(unit) for group in profile.get("sequence_groups", []) for unit in group.get("units", []) if isinstance(unit, dict)]

    def _profile_energy_map(self, profile: dict) -> dict[str, float]:
        weights = {}
        for unit in self._collect_profile_units(profile):
            if unit.get("is_placeholder"):
                continue
            structure_id = str(unit.get("unit_id", ""))
            if not structure_id:
                continue
            weights[structure_id] = round(
                float(weights.get(structure_id, 0.0)) + max(0.0, float(unit.get("er", 0.0))) + max(0.0, float(unit.get("ev", 0.0))),
                8,
            )
        total = sum(max(0.0, value) for value in weights.values())
        if total <= 0.0:
            if not weights:
                return {}
            return {key: round(1.0 / len(weights), 8) for key in weights}
        return {key: round(max(0.0, value) / total, 8) for key, value in weights.items()}

    def _build_structure_memory_material(self, *, profile: dict) -> dict:
        sequence_groups = []
        structure_items: list[dict] = []
        ordered_structure_ids: list[str] = []
        seen_structure_ids: set[str] = set()

        for group in list(profile.get("sequence_groups", [])):
            cloned_group = {
                "group_index": int(group.get("group_index", 0)),
                "source_type": str(group.get("source_type", "")),
                "origin_frame_id": str(group.get("origin_frame_id", "")),
                "source_group_index": int(group.get("source_group_index", group.get("group_index", 0))),
                "units": [dict(unit) for unit in group.get("units", []) if isinstance(unit, dict)],
                "csa_bundles": [dict(bundle) for bundle in group.get("csa_bundles", []) if isinstance(bundle, dict)],
                "tokens": list(group.get("tokens", [])),
                "display_text": str(group.get("display_text", "")),
            }
            sequence_groups.append(cloned_group)
            for unit in cloned_group["units"]:
                structure_id = str(unit.get("unit_id", ""))
                if not structure_id or unit.get("is_placeholder"):
                    continue
                if structure_id in seen_structure_ids:
                    continue
                seen_structure_ids.add(structure_id)
                ordered_structure_ids.append(structure_id)
                structure_items.append(
                    {
                        "structure_id": structure_id,
                        "display_text": str(unit.get("structure_display_text", unit.get("display_text", unit.get("token", structure_id)))),
                        "grouped_display_text": str(unit.get("structure_grouped_display_text", unit.get("display_text", structure_id))),
                    }
                )

        return {
            "memory_kind": "structure_group",
            "storage_grain": "st",
            "grouped_display_text": format_sequence_groups(sequence_groups) or str(profile.get("display_text", "")),
            "sequence_groups": sequence_groups,
            "structure_refs": ordered_structure_ids,
            "structure_items": structure_items,
            "structure_energy_profile": self._profile_energy_map(profile),
        }

    def _normalize_energy(self, structure_ids: list[str], budget_er_map: dict[str, float], budget_ev_map: dict[str, float]) -> dict[str, float]:
        values = {
            structure_id: max(0.0, float(budget_er_map.get(structure_id, 0.0)) + float(budget_ev_map.get(structure_id, 0.0)))
            for structure_id in structure_ids
        }
        total = sum(values.values())
        if total <= 0.0:
            if not values:
                return {}
            return {structure_id: round(1.0 / len(values), 8) for structure_id in values}
        return {
            structure_id: round(value / total, 8)
            for structure_id, value in values.items()
        }

    def _update_group_after_match(
        self,
        *,
        group_obj: dict,
        current_profile: dict[str, float],
        match_score: float,
        matched_er_total: float,
        matched_ev_total: float,
    ) -> None:
        now_ms = int(time.time() * 1000)
        stats = group_obj.setdefault("stats", {})
        self._weight.decay_group(group_obj, now_ms=now_ms, round_step=1)
        er_gain = max(0.0, float(matched_er_total)) * max(0.0, float(match_score)) * float(self._config.get("base_weight_er_gain", 0.08))
        ev_wear = max(0.0, float(matched_ev_total)) * max(0.0, float(match_score)) * float(self._config.get("base_weight_ev_wear", 0.03))
        stats["base_weight"] = round(
            max(float(self._config.get("weight_floor", 0.05)), float(stats.get("base_weight", 1.0)) + er_gain - ev_wear),
            8,
        )
        self._weight.refresh_recent_state(stats, now_ms=now_ms, strength=max(float(self._config.get("recency_gain_refresh_floor", 0.45)), float(match_score)))
        self._weight.apply_match_fatigue(stats, strength=match_score)
        stats["last_matched_at"] = now_ms
        stats["match_count_total"] = int(stats.get("match_count_total", 0)) + 1
        group_obj["avg_energy_profile"] = self._smooth_profile_merge(
            existing=dict(group_obj.get("avg_energy_profile", {})),
            observed=current_profile,
            alpha=max(0.12, min(0.45, 0.18 + 0.32 * float(match_score))),
        )
        local_db = group_obj.setdefault("local_db", {})
        local_db.setdefault("group_table", [])
        local_db.setdefault("residual_table", [])
        local_db.setdefault("memory_table", [])

    def _mark_path_entries(self, *, best: dict, structure_store, group_store, transferred_er: float, transferred_ev: float, match_score: float) -> None:
        for path_entry in best.get("path_entries", []):
            owner_kind = str(path_entry.get("owner_kind", ""))
            owner_id = str(path_entry.get("owner_id", ""))
            entry_id = str(path_entry.get("entry_id", ""))
            if not owner_kind or not owner_id or not entry_id:
                continue
            owner_ctx = self._open_owner_context(
                owner_kind=owner_kind,
                owner_id=owner_id,
                structure_store=structure_store,
                group_store=group_store,
                pointer_index=None,
                cut_engine=None,
                trace_id="",
                tick_id="",
            )
            if not owner_ctx:
                continue
            updated = False
            for entry in owner_ctx.get("group_table", []):
                if str(entry.get("entry_id", "")) != entry_id:
                    continue
                self._mark_entry_weight(
                    entry,
                    delta_er=transferred_er,
                    delta_ev=transferred_ev,
                    match_score=match_score,
                )
                updated = True
                break
            if updated:
                self._persist_owner_context(owner_ctx, structure_store=structure_store, group_store=group_store)

    def _mark_entry_weight(self, entry: dict, *, delta_er: float, delta_ev: float, match_score: float) -> None:
        now_ms = int(time.time() * 1000)
        self._ensure_group_entry_schema(entry)
        self._weight.mark_entry_activation(
            entry,
            delta_er=max(0.0, float(delta_er)),
            delta_ev=max(0.0, float(delta_ev)),
            match_score=max(0.0, float(match_score)),
            now_ms=now_ms,
        )
        delta_weight = max(0.0, float(delta_er)) * float(self._config.get("base_weight_er_gain", 0.08)) - max(0.0, float(delta_ev)) * float(self._config.get("base_weight_ev_wear", 0.03))
        entry["base_weight"] = round(
            max(float(self._config.get("weight_floor", 0.05)), float(entry.get("base_weight", 1.0)) + delta_weight),
            8,
        )
        entry["last_updated_at"] = now_ms

    def _smooth_profile_merge(self, *, existing: dict[str, float], observed: dict[str, float], alpha: float) -> dict[str, float]:
        keys = set(existing.keys()) | set(observed.keys())
        if not keys:
            return {}
        merged = {}
        for key in keys:
            before = float(existing.get(key, 0.0))
            after = float(observed.get(key, 0.0))
            merged[key] = round((1.0 - float(alpha)) * before + float(alpha) * after, 8)
        total = sum(max(0.0, value) for value in merged.values())
        if total <= 0.0:
            return {key: round(1.0 / len(merged), 8) for key in merged}
        return {key: round(max(0.0, value) / total, 8) for key, value in merged.items()}

    def _store_runtime_context(
        self,
        *,
        owner_kind: str,
        owner_id: str,
        full_profile: dict,
        covered_structure_ids: list[str],
        full_energy_profile: dict[str, float],
        structure_store,
        group_store,
        pointer_index,
        cut_engine,
        trace_id: str,
        tick_id: str,
        round_index: int,
        episodic_memory_id: str,
    ) -> dict | None:
        owner_ctx = self._open_owner_context(
            owner_kind=owner_kind,
            owner_id=owner_id,
            structure_store=structure_store,
            group_store=group_store,
            pointer_index=pointer_index,
            cut_engine=cut_engine,
            trace_id=trace_id,
            tick_id=tick_id,
        )
        if not owner_ctx:
            return None
        summary = {
            "owner_kind": owner_kind,
            "owner_id": owner_id,
            "owner_display_text": owner_ctx.get("owner_display_text", owner_id),
            "resolved_db_id": owner_ctx.get("resolved_db_id", ""),
            "used_fallback": bool(owner_ctx.get("used_fallback", False)),
            "new_group_ids": [],
            "new_group_details": [],
            "actions": [],
        }
        if episodic_memory_id:
            self._append_memory_ref(
                owner_ctx=owner_ctx,
                memory_id=episodic_memory_id,
                content_signature=full_profile.get("content_signature", ""),
                round_index=round_index,
                event_kind="structure_runtime",
            )
        owner_placeholder = owner_ctx.get("owner_placeholder", "")
        residual_profile = self._build_relative_residual_profile(
            full_profile=full_profile,
            covered_structure_ids=covered_structure_ids,
            owner_placeholder=owner_placeholder,
            cut_engine=cut_engine,
            origin_frame_id=f"{tick_id}:{round_index}:{owner_id}",
        )
        if not residual_profile or not self._profile_has_non_placeholder_content(residual_profile, placeholder_token=owner_placeholder):
            self._persist_owner_context(owner_ctx, structure_store=structure_store, group_store=group_store)
            return summary
        self._normalize_owner_local_residual(
            owner_ctx=owner_ctx,
            residual_profile=residual_profile,
            full_energy_profile=full_energy_profile,
            structure_store=structure_store,
            group_store=group_store,
            pointer_index=pointer_index,
            cut_engine=cut_engine,
            trace_id=trace_id,
            tick_id=tick_id,
            round_index=round_index,
            episodic_memory_id=episodic_memory_id,
            summary=summary,
            depth=0,
        )
        self._persist_owner_context(owner_ctx, structure_store=structure_store, group_store=group_store)
        summary["new_group_ids"] = list(dict.fromkeys(summary.get("new_group_ids", [])))
        return summary

    def _normalize_owner_local_residual(
        self,
        *,
        owner_ctx: dict,
        residual_profile: dict,
        full_energy_profile: dict[str, float],
        structure_store,
        group_store,
        pointer_index,
        cut_engine,
        trace_id: str,
        tick_id: str,
        round_index: int,
        episodic_memory_id: str,
        summary: dict,
        depth: int,
    ) -> None:
        if depth >= 12:
            return
        owner_placeholder = owner_ctx.get("owner_placeholder", "")
        if not self._profile_has_non_placeholder_content(residual_profile, placeholder_token=owner_placeholder):
            return
        residual_signature = residual_profile.get("content_signature", "")
        if not residual_signature:
            return
        canonical_profile = self._canonicalize_local_profile(
            profile=residual_profile,
            structure_store=structure_store,
            group_store=group_store,
            cut_engine=cut_engine,
        )
        canonical_signature = canonical_profile.get("content_signature", "") or residual_signature

        local_items = self._list_local_storage_items(
            owner_ctx=owner_ctx,
            structure_store=structure_store,
            group_store=group_store,
            cut_engine=cut_engine,
        )
        exact_raw_item = next(
            (
                item
                for item in local_items
                if item.get("item_kind") == "raw_residual"
                and (
                    item.get("signature", "") == canonical_signature
                    or self._profiles_fuzzy_equivalent(
                        left_profile=item.get("canonical_profile", item.get("profile", {})),
                        right_profile=canonical_profile,
                        cut_engine=cut_engine,
                    )
                )
            ),
            None,
        )
        if exact_raw_item:
            self._reinforce_raw_residual_entry(
                entry=exact_raw_item["entry_ref"],
                residual_profile=canonical_profile,
                full_energy_profile=full_energy_profile,
                episodic_memory_id=episodic_memory_id,
                round_index=round_index,
                structure_store=structure_store,
                group_store=group_store,
                cut_engine=cut_engine,
            )
            summary["actions"].append(
                self._build_raw_residual_action(
                    action_type="reinforce_raw_residual",
                    owner_ctx=owner_ctx,
                    entry=exact_raw_item["entry_ref"],
                )
            )
            return

        exact_item = next(
            (
                item
                for item in local_items
                if item.get("item_kind") != "raw_residual"
                and (
                    item.get("signature", "") in {residual_signature, canonical_signature}
                    or self._profiles_fuzzy_equivalent(
                        left_profile=item.get("profile", {}),
                        right_profile=residual_profile,
                        cut_engine=cut_engine,
                    )
                )
            ),
            None,
        )
        if exact_item:
            self._mark_entry_weight(
                exact_item["entry_ref"],
                delta_er=self._profile_total_energy(residual_profile),
                delta_ev=0.0,
                match_score=1.0,
            )
            child_ctx = self._open_owner_context(
                owner_kind="sg",
                owner_id=exact_item.get("group_id", ""),
                structure_store=structure_store,
                group_store=group_store,
                pointer_index=pointer_index,
                cut_engine=cut_engine,
                trace_id=trace_id,
                tick_id=tick_id,
            )
            if child_ctx and episodic_memory_id:
                self._append_memory_ref(
                    owner_ctx=child_ctx,
                    memory_id=episodic_memory_id,
                    content_signature=residual_signature,
                    round_index=round_index,
                    event_kind="structure_exact_child",
                )
                self._persist_owner_context(child_ctx, structure_store=structure_store, group_store=group_store)
            summary["actions"].append({"type": "reinforce_child_group", "group_id": exact_item.get("group_id", "")})
            return

        parent_group = self._find_parent_group_candidate(
            owner_ctx=owner_ctx,
            residual_profile=residual_profile,
            local_items=local_items,
            cut_engine=cut_engine,
        )
        if parent_group:
            self._mark_entry_weight(
                parent_group["entry_ref"],
                delta_er=self._profile_total_energy(residual_profile),
                delta_ev=0.0,
                match_score=max(
                    float(self._config.get("structure_descend_match_floor", 0.35)),
                    float(parent_group.get("entry_runtime_weight", 1.0))
                    / max(1.0, float(parent_group.get("path_strength", 1.0))),
                ),
            )
            child_ctx = self._open_owner_context(
                owner_kind="sg",
                owner_id=parent_group.get("group_id", ""),
                structure_store=structure_store,
                group_store=group_store,
                pointer_index=pointer_index,
                cut_engine=cut_engine,
                trace_id=trace_id,
                tick_id=tick_id,
            )
            if child_ctx and episodic_memory_id:
                self._append_memory_ref(
                    owner_ctx=child_ctx,
                    memory_id=episodic_memory_id,
                    content_signature=residual_signature,
                    round_index=round_index,
                    event_kind="structure_parent_child",
                )
            child_profile = self._build_descend_relative_profile(
                full_profile=residual_profile,
                common_part=parent_group.get("common_part", {}),
                child_placeholder=child_ctx.get("owner_placeholder", "") if child_ctx else "",
                cut_engine=cut_engine,
                origin_frame_id=f"{tick_id}:{round_index}:descend:{parent_group.get('group_id', '')}",
            )
            if child_ctx and child_profile:
                self._normalize_owner_local_residual(
                    owner_ctx=child_ctx,
                    residual_profile=child_profile,
                    full_energy_profile=full_energy_profile,
                    structure_store=structure_store,
                    group_store=group_store,
                    pointer_index=pointer_index,
                    cut_engine=cut_engine,
                    trace_id=trace_id,
                    tick_id=tick_id,
                    round_index=round_index,
                    episodic_memory_id=episodic_memory_id,
                    summary=summary,
                    depth=depth + 1,
                )
                self._persist_owner_context(child_ctx, structure_store=structure_store, group_store=group_store)
            summary["actions"].append({"type": "descend_existing_group", "group_id": parent_group.get("group_id", "")})
            return

        overlap_item = self._find_best_overlap_candidate(
            owner_ctx=owner_ctx,
            residual_profile=residual_profile,
            local_items=local_items,
            cut_engine=cut_engine,
        )
        if overlap_item:
            common_part = overlap_item.get("common_part", {})
            common_relative_profile = overlap_item.get("common_relative_profile", {}) or self._profile_from_stored_groups(
                list(common_part.get("common_groups", [])),
                cut_engine=cut_engine,
                ext={
                    "kind": "structure_group_relative_common",
                    "owner_id": owner_ctx.get("owner_id", ""),
                    "owner_kind": owner_ctx.get("owner_kind", ""),
                },
            )
            common_full_profile = overlap_item.get("common_full_profile", {}) or self._expand_relative_profile(
                relative_profile=common_relative_profile,
                owner_profile=owner_ctx.get("owner_profile", {}),
                owner_placeholder=owner_placeholder,
                cut_engine=cut_engine,
            )
            if not self._common_overlap_beyond_owner(
                common_relative_profile=common_relative_profile,
                owner_placeholder=owner_placeholder,
            ) or not self._profile_fully_contains_subprofile(
                container_profile=common_full_profile,
                required_profile=owner_ctx.get("owner_profile", {}),
                cut_engine=cut_engine,
            ):
                overlap_item = None
        if overlap_item:
            common_part = overlap_item.get("common_part", {})
            common_relative_profile = overlap_item.get("common_relative_profile", {}) or common_relative_profile
            common_full_profile = overlap_item.get("common_full_profile", {}) or common_full_profile
            observed_maps = [dict(full_energy_profile)]
            if overlap_item.get("item_kind") == "raw_residual":
                observed_maps.append(dict(overlap_item.get("observed_energy_profile", {})))
            else:
                observed_maps.append(dict(overlap_item.get("group_obj", {}).get("avg_energy_profile", {})))
            avg_energy_profile = self._merge_observed_energy_profiles(
                profile_maps=observed_maps,
                required_ids=self._extract_structure_ids_from_profile(common_full_profile),
            )
            common_group_result = self._find_or_create_common_group(
                owner_ctx=owner_ctx,
                relative_profile=common_relative_profile,
                full_profile=common_full_profile,
                avg_energy_profile=avg_energy_profile,
                structure_store=structure_store,
                group_store=group_store,
                cut_engine=cut_engine,
                trace_id=trace_id,
                tick_id=tick_id,
            )
            common_group = common_group_result["group_obj"]
            common_group_id = common_group.get("id", "")
            if common_group_result.get("created") and common_group_id:
                summary["new_group_ids"].append(common_group_id)
                summary["new_group_details"].append(self._build_group_debug_payload(common_group, structure_store, cut_engine))
            self._remove_local_item(owner_ctx=owner_ctx, item=overlap_item)
            self._append_group_entry(
                owner_ctx=owner_ctx,
                group_obj=common_group,
                relative_profile=common_relative_profile,
                base_weight=max(
                    float(self._config.get("weight_floor", 0.05)),
                    float(overlap_item.get("base_weight", 1.0)) + self._profile_total_energy(residual_profile) * float(self._config.get("base_weight_er_gain", 0.08)),
                ),
            )

            common_ctx = self._open_owner_context(
                owner_kind="sg",
                owner_id=common_group_id,
                structure_store=structure_store,
                group_store=group_store,
                pointer_index=pointer_index,
                cut_engine=cut_engine,
                trace_id=trace_id,
                tick_id=tick_id,
            )
            if common_ctx and episodic_memory_id:
                self._append_memory_ref(
                    owner_ctx=common_ctx,
                    memory_id=episodic_memory_id,
                    content_signature=common_full_profile.get("content_signature", ""),
                    round_index=round_index,
                    event_kind="structure_common_group",
                )

            if overlap_item.get("item_kind") == "group_entry" and common_ctx:
                existing_child_profile = self._build_descend_relative_profile(
                    full_profile=overlap_item.get("relative_profile", {}),
                    common_part=common_part,
                    child_placeholder=common_ctx.get("owner_placeholder", ""),
                    cut_engine=cut_engine,
                    origin_frame_id=f"{tick_id}:{round_index}:existing:{common_group_id}",
                )
                if existing_child_profile and self._profile_has_non_placeholder_content(existing_child_profile, placeholder_token=common_ctx.get("owner_placeholder", "")):
                    self._append_group_entry(
                        owner_ctx=common_ctx,
                        group_obj=overlap_item.get("group_obj", {}),
                        relative_profile=existing_child_profile,
                        base_weight=max(float(self._config.get("weight_floor", 0.05)), float(overlap_item.get("base_weight", 1.0))),
                    )

            if overlap_item.get("item_kind") == "raw_residual" and common_ctx:
                existing_child_profile = self._build_descend_relative_profile(
                    full_profile=overlap_item.get("profile", {}),
                    common_part=common_part,
                    child_placeholder=common_ctx.get("owner_placeholder", ""),
                    cut_engine=cut_engine,
                    origin_frame_id=f"{tick_id}:{round_index}:existing_raw:{common_group_id}",
                )
                if existing_child_profile and self._profile_has_non_placeholder_content(existing_child_profile, placeholder_token=common_ctx.get("owner_placeholder", "")):
                    self._normalize_owner_local_residual(
                        owner_ctx=common_ctx,
                        residual_profile=existing_child_profile,
                        full_energy_profile=overlap_item.get("observed_energy_profile", {}),
                        structure_store=structure_store,
                        group_store=group_store,
                        pointer_index=pointer_index,
                        cut_engine=cut_engine,
                        trace_id=trace_id,
                        tick_id=tick_id,
                        round_index=round_index,
                        episodic_memory_id=episodic_memory_id,
                        summary=summary,
                        depth=depth + 1,
                    )

            incoming_child_profile = self._build_descend_relative_profile(
                full_profile=residual_profile,
                common_part=common_part,
                child_placeholder=common_ctx.get("owner_placeholder", "") if common_ctx else "",
                cut_engine=cut_engine,
                origin_frame_id=f"{tick_id}:{round_index}:incoming:{common_group_id}",
            )
            if common_ctx and incoming_child_profile:
                self._normalize_owner_local_residual(
                    owner_ctx=common_ctx,
                    residual_profile=incoming_child_profile,
                    full_energy_profile=full_energy_profile,
                    structure_store=structure_store,
                    group_store=group_store,
                    pointer_index=pointer_index,
                    cut_engine=cut_engine,
                    trace_id=trace_id,
                    tick_id=tick_id,
                    round_index=round_index,
                    episodic_memory_id=episodic_memory_id,
                    summary=summary,
                    depth=depth + 1,
                )
                self._persist_owner_context(common_ctx, structure_store=structure_store, group_store=group_store)
            summary["actions"].append({"type": "create_common_group", "group_id": common_group_id})
            return

        created_entry = self._append_raw_residual_entry(
            owner_ctx=owner_ctx,
            residual_profile=residual_profile,
            full_energy_profile=full_energy_profile,
            episodic_memory_id=episodic_memory_id,
            round_index=round_index,
            structure_store=structure_store,
            group_store=group_store,
            cut_engine=cut_engine,
        )
        summary["actions"].append(
            self._build_raw_residual_action(
                action_type="append_raw_residual",
                owner_ctx=owner_ctx,
                entry=created_entry,
            )
        )

    def _find_parent_group_candidate(self, *, owner_ctx: dict, residual_profile: dict, local_items: list[dict], cut_engine) -> dict | None:
        best = None
        for item in local_items:
            if item.get("item_kind") != "group_entry":
                continue
            existing_profile = item.get("relative_profile", {})
            common_part = cut_engine.maximum_common_part(
                existing_profile.get("sequence_groups", []),
                residual_profile.get("sequence_groups", []),
            )
            existing_length = self._profile_unit_count(existing_profile)
            if common_part.get("residual_existing_signature", ""):
                continue
            # CSA 门控：existing_profile 中的 CSA bundle 必须被 residual_profile 的某一个 bundle 完整覆盖。
            if not bool(common_part.get("bundle_constraints_ok_existing_included", True)):
                continue
            if int(common_part.get("matched_existing_unit_count", 0)) < max(1, existing_length):
                continue
            if self._profiles_fuzzy_equivalent(
                left_profile=existing_profile,
                right_profile=residual_profile,
                cut_engine=cut_engine,
            ):
                continue
            candidate_key = (
                len(item.get("group_obj", {}).get("required_structure_ids", [])),
                float(item.get("entry_runtime_weight", 0.0)),
            )
            current_key = (
                len(best.get("group_obj", {}).get("required_structure_ids", [])),
                float(best.get("entry_runtime_weight", 0.0)),
            ) if best else None
            if best is None or candidate_key > current_key:
                best = {**item, "common_part": common_part}
        return best

    def _find_best_overlap_candidate(self, *, owner_ctx: dict, residual_profile: dict, local_items: list[dict], cut_engine) -> dict | None:
        best = None
        for item in local_items:
            existing_profile = item.get("profile", {})
            common_part = cut_engine.maximum_common_part(
                existing_profile.get("sequence_groups", []),
                residual_profile.get("sequence_groups", []),
            )
            common_signature = common_part.get("common_signature", "")
            if not common_signature:
                continue
            validated_common = self._validate_owner_overlap_common_part(
                owner_ctx=owner_ctx,
                common_part=common_part,
                cut_engine=cut_engine,
            )
            if not validated_common:
                continue
            if self._profiles_fuzzy_equivalent(
                left_profile=existing_profile,
                right_profile=residual_profile,
                cut_engine=cut_engine,
            ):
                continue
            candidate_key = (
                int(common_part.get("common_length", 0)),
                int(common_part.get("common_group_count", 0)),
                float(item.get("entry_runtime_weight", 0.0)),
            )
            current_key = (
                int(best.get("common_part", {}).get("common_length", 0)),
                int(best.get("common_part", {}).get("common_group_count", 0)),
                float(best.get("entry_runtime_weight", 0.0)),
            ) if best else None
            if best is None or candidate_key > current_key:
                best = {
                    **item,
                    "common_part": common_part,
                    "common_relative_profile": validated_common.get("relative_profile", {}),
                    "common_full_profile": validated_common.get("full_profile", {}),
                }
        return best

    # 结构级残差只能在“共同部分仍然保留当前 owner，且 owner 外还有额外内容”时创建子共有结构组。
    # 否则就会把不包含当前组选中结构的内容错误地下沉到当前 owner 的本地库里。
    def _validate_owner_overlap_common_part(self, *, owner_ctx: dict, common_part: dict, cut_engine) -> dict | None:
        owner_placeholder = str(owner_ctx.get("owner_placeholder", ""))
        common_relative_profile = self._profile_from_stored_groups(
            list(common_part.get("common_groups", [])),
            cut_engine=cut_engine,
            ext={
                "kind": "structure_group_relative_common_candidate",
                "owner_id": owner_ctx.get("owner_id", ""),
                "owner_kind": owner_ctx.get("owner_kind", ""),
            },
        )
        if not self._common_overlap_beyond_owner(
            common_relative_profile=common_relative_profile,
            owner_placeholder=owner_placeholder,
        ):
            return None
        common_full_profile = self._expand_relative_profile(
            relative_profile=common_relative_profile,
            owner_profile=owner_ctx.get("owner_profile", {}),
            owner_placeholder=owner_placeholder,
            cut_engine=cut_engine,
        )
        if not self._profile_fully_contains_subprofile(
            container_profile=common_full_profile,
            required_profile=owner_ctx.get("owner_profile", {}),
            cut_engine=cut_engine,
        ):
            return None
        return {
            "relative_profile": common_relative_profile,
            "full_profile": common_full_profile,
        }

    def _common_overlap_beyond_owner(self, *, common_relative_profile: dict, owner_placeholder: str) -> bool:
        if not owner_placeholder:
            return False
        tokens = [
            str(unit.get("token", ""))
            for unit in self._collect_profile_units(common_relative_profile)
            if str(unit.get("token", ""))
        ]
        if owner_placeholder not in tokens:
            return False
        return self._profile_has_non_placeholder_content(common_relative_profile, placeholder_token=owner_placeholder)

    def _profile_fully_contains_subprofile(self, *, container_profile: dict, required_profile: dict, cut_engine) -> bool:
        if not container_profile or not required_profile:
            return False
        common_part = cut_engine.maximum_common_part(
            required_profile.get("sequence_groups", []),
            container_profile.get("sequence_groups", []),
        )
        return bool(
            int(common_part.get("common_length", 0)) > 0
            and not common_part.get("residual_existing_signature", "")
            and int(common_part.get("matched_existing_unit_count", 0)) >= self._profile_unit_count(required_profile)
            # CSA 门控：required_profile 中的 CSA bundle 必须被 container_profile 的某一个 bundle 完整覆盖。
            and bool(common_part.get("bundle_constraints_ok_existing_included", True))
        )

    def _list_local_storage_items(self, *, owner_ctx: dict, structure_store, group_store, cut_engine) -> list[dict]:
        items = []
        for entry in owner_ctx.get("residual_table", []):
            profiles = self._ensure_raw_residual_canonical_fields(
                entry=entry,
                structure_store=structure_store,
                group_store=group_store,
                cut_engine=cut_engine,
            )
            items.append(
                {
                    "item_kind": "raw_residual",
                    "entry_ref": entry,
                    "entry_id": entry.get("entry_id", ""),
                    "signature": entry.get("canonical_content_signature", ""),
                    "profile": profiles.get("raw_profile", {}),
                    "canonical_profile": profiles.get("canonical_profile", {}),
                    "raw_profile": profiles.get("raw_profile", {}),
                    "base_weight": float(entry.get("base_weight", 1.0)),
                    "entry_runtime_weight": self._weight.entry_runtime_weight(entry),
                    "observed_energy_profile": dict(entry.get("observed_energy_profile", {})),
                }
            )
        for entry in owner_ctx.get("group_table", []):
            self._ensure_group_entry_schema(entry)
            group_obj = group_store.get(entry.get("group_id", ""))
            if not group_obj:
                continue
            relative_profile = self._group_entry_relative_profile(owner_ctx=owner_ctx, entry=entry, group_obj=group_obj, cut_engine=cut_engine)
            if not relative_profile:
                continue
            items.append(
                {
                    "item_kind": "group_entry",
                    "entry_ref": entry,
                    "entry_id": entry.get("entry_id", ""),
                    "group_id": group_obj.get("id", ""),
                    "group_obj": group_obj,
                    "signature": relative_profile.get("content_signature", ""),
                    "profile": relative_profile,
                    "relative_profile": relative_profile,
                    "base_weight": float(entry.get("base_weight", 1.0)),
                    "entry_runtime_weight": self._weight.entry_runtime_weight(entry),
                    "path_strength": self._path_strength(
                        group_runtime_weight=self._preview_group_stats(group_obj, now_ms=int(time.time() * 1000))["runtime_weight"],
                        entry_runtime_weight=self._weight.entry_runtime_weight(entry),
                    ),
                }
            )
        items.sort(
            key=lambda item: (
                -float(item.get("entry_runtime_weight", 0.0)),
                -float(item.get("base_weight", 0.0)),
                item.get("entry_id", ""),
            )
        )
        return items

    def _reinforce_raw_residual_entry(
        self,
        *,
        entry: dict,
        residual_profile: dict,
        full_energy_profile: dict[str, float],
        episodic_memory_id: str,
        round_index: int,
        structure_store,
        group_store,
        cut_engine,
    ) -> None:
        now_ms = int(time.time() * 1000)
        self._ensure_raw_residual_schema(entry)
        existing_profiles = self._ensure_raw_residual_canonical_fields(
            entry=entry,
            structure_store=structure_store,
            group_store=group_store,
            cut_engine=cut_engine,
        )
        canonical_profile = self._canonicalize_local_profile(
            profile=residual_profile,
            structure_store=structure_store,
            group_store=group_store,
            cut_engine=cut_engine,
        )
        existing_canonical_profile = existing_profiles.get("canonical_profile", {})
        if existing_canonical_profile and self._profiles_fuzzy_equivalent(
            left_profile=existing_canonical_profile,
            right_profile=canonical_profile,
            cut_engine=cut_engine,
        ):
            common_part = cut_engine.maximum_common_part(
                existing_canonical_profile.get("sequence_groups", []),
                canonical_profile.get("sequence_groups", []),
            )
            canonical_profile = self._profile_from_stored_groups(
                list(common_part.get("common_groups", [])),
                cut_engine=cut_engine,
                ext={"kind": "structure_raw_residual_canonical_merged"},
            )
        self._mark_entry_weight(
            entry,
            delta_er=self._profile_total_energy(canonical_profile),
            delta_ev=0.0,
            match_score=1.0,
        )
        entry["canonical_content_signature"] = canonical_profile.get("content_signature", "")
        entry["canonical_display_text"] = canonical_profile.get("display_text", "")
        entry["canonical_flat_tokens"] = list(canonical_profile.get("flat_tokens", []))
        entry["canonical_sequence_groups"] = list(canonical_profile.get("sequence_groups", []))
        entry["observed_energy_profile"] = self._smooth_profile_merge(
            existing=dict(entry.get("observed_energy_profile", {})),
            observed=dict(full_energy_profile),
            alpha=float(self._config.get("structure_profile_merge_alpha", 0.22)),
        )
        entry["sample_count"] = int(entry.get("sample_count", 1)) + 1
        entry["last_updated_at"] = now_ms
        if episodic_memory_id:
            memory_refs = list(entry.get("memory_refs", []))
            if episodic_memory_id not in memory_refs:
                memory_refs.append(episodic_memory_id)
            entry["memory_refs"] = memory_refs
        entry.setdefault("ext", {})["last_round_index"] = round_index

    def _append_raw_residual_entry(
        self,
        *,
        owner_ctx: dict,
        residual_profile: dict,
        full_energy_profile: dict[str, float],
        episodic_memory_id: str,
        round_index: int,
        structure_store,
        group_store,
        cut_engine,
    ) -> dict:
        canonical_profile = self._canonicalize_local_profile(
            profile=residual_profile,
            structure_store=structure_store,
            group_store=group_store,
            cut_engine=cut_engine,
        )
        entry = {
            "entry_id": next_id("sgr"),
            "entry_type": "raw_residual",
            "content_signature": residual_profile.get("content_signature", ""),
            "display_text": residual_profile.get("display_text", ""),
            "flat_tokens": list(residual_profile.get("flat_tokens", [])),
            "sequence_groups": list(residual_profile.get("sequence_groups", [])),
            "canonical_content_signature": canonical_profile.get("content_signature", ""),
            "canonical_display_text": canonical_profile.get("display_text", ""),
            "canonical_flat_tokens": list(canonical_profile.get("flat_tokens", [])),
            "canonical_sequence_groups": list(canonical_profile.get("sequence_groups", [])),
            "base_weight": round(max(float(self._config.get("weight_floor", 0.05)), 1.0 + self._profile_total_energy(canonical_profile) * float(self._config.get("base_weight_er_gain", 0.08))), 8),
            "recent_gain": self._weight._target_recent_gain(strength=1.0),
            "fatigue": 0.0,
            "runtime_er": round(self._profile_total_energy(canonical_profile), 8),
            "runtime_ev": 0.0,
            "match_count_total": 0,
            "last_updated_at": int(time.time() * 1000),
            "last_matched_at": 0,
            "last_recency_refresh_at": int(time.time() * 1000),
            "recency_hold_rounds_remaining": int(self._config.get("recency_gain_hold_rounds", 2)),
            "observed_energy_profile": dict(full_energy_profile),
            "sample_count": 1,
            "memory_refs": [episodic_memory_id] if episodic_memory_id else [],
            "ext": {
                "relation_type": "structure_raw_residual",
                "owner_kind": owner_ctx.get("owner_kind", ""),
                "owner_id": owner_ctx.get("owner_id", ""),
                "round_index": round_index,
            },
        }
        owner_ctx.setdefault("residual_table", []).append(entry)
        return entry

    def _append_group_entry(self, *, owner_ctx: dict, group_obj: dict, relative_profile: dict, base_weight: float) -> dict:
        signature = relative_profile.get("content_signature", "")
        for existing in owner_ctx.get("group_table", []):
            self._ensure_group_entry_schema(existing)
            if existing.get("group_id", "") != group_obj.get("id", ""):
                continue
            if existing.get("relative_content_signature", "") != signature:
                continue
            existing["base_weight"] = round(
                max(
                    float(self._config.get("weight_floor", 0.05)),
                    float(existing.get("base_weight", 1.0))
                    + float(base_weight) * float(self._config.get("structure_group_entry_reinforce_ratio", 0.15)),
                ),
                8,
            )
            self._weight.refresh_recent_state(existing, now_ms=int(time.time() * 1000), strength=1.0)
            existing["last_updated_at"] = int(time.time() * 1000)
            return existing
        entry = {
            "entry_id": next_id("sge"),
            "group_id": group_obj.get("id", ""),
            "required_structure_ids": list(group_obj.get("required_structure_ids", [])),
            "avg_energy_profile": dict(group_obj.get("avg_energy_profile", {})),
            "content_signature": group_obj.get("group_structure", {}).get("content_signature", ""),
            "temporal_signature": group_obj.get("group_structure", {}).get("temporal_signature", ""),
            "display_text": self._group_display_text(group_obj),
            "relative_content_signature": signature,
            "relative_flat_tokens": list(relative_profile.get("flat_tokens", [])),
            "relative_sequence_groups": list(relative_profile.get("sequence_groups", [])),
            "base_weight": round(max(float(self._config.get("weight_floor", 0.05)), float(base_weight)), 8),
            "recent_gain": self._weight._target_recent_gain(strength=1.0),
            "fatigue": 0.0,
            "runtime_er": 0.0,
            "runtime_ev": 0.0,
            "match_count_total": 0,
            "last_updated_at": int(time.time() * 1000),
            "last_matched_at": 0,
            "last_recency_refresh_at": int(time.time() * 1000),
            "recency_hold_rounds_remaining": int(self._config.get("recency_gain_hold_rounds", 2)),
            "ext": {
                "relation_type": "structure_group_ref",
                "owner_kind": owner_ctx.get("owner_kind", ""),
                "owner_id": owner_ctx.get("owner_id", ""),
            },
        }
        owner_ctx.setdefault("group_table", []).append(entry)
        return entry

    def _find_or_create_common_group(
        self,
        *,
        owner_ctx: dict,
        relative_profile: dict,
        full_profile: dict,
        avg_energy_profile: dict[str, float],
        structure_store,
        group_store,
        cut_engine,
        trace_id: str,
        tick_id: str,
    ) -> dict:
        relative_signature = str(relative_profile.get("content_signature", ""))
        full_signature = str(full_profile.get("content_signature", ""))
        required_ids = self._extract_structure_ids_from_profile(full_profile)
        for entry in owner_ctx.get("group_table", []):
            self._ensure_group_entry_schema(entry)
            group_obj = group_store.get(str(entry.get("group_id", "")))
            if not group_obj:
                continue
            existing_relative_profile = self._group_entry_relative_profile(
                owner_ctx=owner_ctx,
                entry=entry,
                group_obj=group_obj,
                cut_engine=cut_engine,
            )
            existing_full_profile = self._group_full_profile(
                group_obj=group_obj,
                structure_store=structure_store,
                cut_engine=cut_engine,
            )
            if not existing_relative_profile or not existing_full_profile:
                continue
            if str(entry.get("relative_content_signature", "")) != relative_signature and not self._profiles_fuzzy_equivalent(
                left_profile=existing_relative_profile,
                right_profile=relative_profile,
                cut_engine=cut_engine,
            ):
                continue
            group_signature = str(group_obj.get("group_structure", {}).get("content_signature", ""))
            if group_signature and group_signature != full_signature and not self._profiles_fuzzy_equivalent(
                left_profile=existing_full_profile,
                right_profile=full_profile,
                cut_engine=cut_engine,
            ):
                continue
            group_obj.setdefault("local_db", {})
            group_obj["local_db"].setdefault("group_table", [])
            group_obj["local_db"].setdefault("residual_table", [])
            group_obj["local_db"].setdefault("memory_table", [])
            if required_ids:
                group_obj["required_structure_ids"] = list(required_ids)
            group_obj["avg_energy_profile"] = self._smooth_profile_merge(
                existing=dict(group_obj.get("avg_energy_profile", {})),
                observed=self._normalize_external_profile(avg_energy_profile, required_ids),
                alpha=float(self._config.get("structure_profile_merge_alpha", 0.22)),
            )
            group_obj["group_structure"] = {
                **full_profile,
                "temporal_signature": full_profile.get("content_signature", ""),
            }
            group_store.update(group_obj)
            return {"created": False, "group_obj": group_obj}

        for existing_group in group_store.iter_items():
            existing_signature = str(existing_group.get("group_structure", {}).get("content_signature", ""))
            existing_full_profile = self._group_full_profile(
                group_obj=existing_group,
                structure_store=structure_store,
                cut_engine=cut_engine,
            )
            if existing_signature and existing_signature != full_signature and not self._profiles_fuzzy_equivalent(
                left_profile=existing_full_profile,
                right_profile=full_profile,
                cut_engine=cut_engine,
            ):
                continue
            existing_group.setdefault("local_db", {})
            existing_group["local_db"].setdefault("group_table", [])
            existing_group["local_db"].setdefault("residual_table", [])
            existing_group["local_db"].setdefault("memory_table", [])
            if required_ids:
                existing_group["required_structure_ids"] = list(required_ids)
            existing_group["group_structure"] = {
                **full_profile,
                "temporal_signature": full_profile.get("content_signature", ""),
            }
            existing_group["avg_energy_profile"] = self._smooth_profile_merge(
                existing=dict(existing_group.get("avg_energy_profile", {})),
                observed=self._normalize_external_profile(avg_energy_profile, required_ids),
                alpha=float(self._config.get("structure_profile_merge_alpha", 0.22)),
            )
            group_store.update(existing_group)
            return {"created": False, "group_obj": existing_group}

        group_obj = group_store.create_group(
            required_structure_ids=required_ids,
            avg_energy_profile=self._normalize_external_profile(avg_energy_profile, required_ids),
            trace_id=trace_id,
            tick_id=tick_id,
            bias_structure_ids=[],
            origin="structure_local_common_group",
            origin_id=owner_ctx.get("owner_id", ""),
            metadata={
                "confidence": float(self._config.get("structure_common_group_confidence", 0.82)),
                "field_registry_version": 1,
                "debug": {},
                "ext": {
                    "owner_kind": owner_ctx.get("owner_kind", ""),
                    "owner_id": owner_ctx.get("owner_id", ""),
                    "relative_content_signature": relative_signature,
                    "group_signature": full_signature,
                },
            },
        )
        group_obj["group_structure"] = {
            **full_profile,
            "temporal_signature": full_profile.get("content_signature", ""),
        }
        group_obj.setdefault("local_db", {})
        group_obj["local_db"].setdefault("group_table", [])
        group_obj["local_db"].setdefault("residual_table", [])
        group_obj["local_db"].setdefault("memory_table", [])
        group_store.update(group_obj)
        return {"created": True, "group_obj": group_obj}

    def _expand_relative_profile(
        self,
        *,
        relative_profile: dict,
        owner_profile: dict,
        owner_placeholder: str,
        cut_engine,
    ) -> dict:
        expanded_units = []
        owner_units = self._collect_profile_units(owner_profile)
        for unit in self._collect_profile_units(relative_profile):
            if bool(unit.get("is_placeholder")) or str(unit.get("token", "")) == owner_placeholder:
                expanded_units.extend(dict(owner_unit) for owner_unit in owner_units)
                continue
            expanded_units.append(dict(unit))
        if not expanded_units:
            return self._profile_from_units(units=[], cut_engine=cut_engine, ext={"kind": "structure_expanded_profile"})
        return self._profile_from_units(
            units=expanded_units,
            cut_engine=cut_engine,
            ext={
                "kind": "structure_expanded_profile",
                "owner_placeholder": owner_placeholder,
            },
        )

    def _build_relative_residual_profile(
        self,
        *,
        full_profile: dict,
        covered_structure_ids: list[str],
        owner_placeholder: str,
        cut_engine,
        origin_frame_id: str,
    ) -> dict | None:
        covered_ids = {str(structure_id) for structure_id in covered_structure_ids if str(structure_id)}
        if not covered_ids:
            return dict(full_profile)
        residual_units = []
        placeholder_inserted = False
        placeholder_index = 0
        for unit in self._collect_profile_units(full_profile):
            unit_id = str(unit.get("unit_id", ""))
            if unit_id in covered_ids:
                if not placeholder_inserted:
                    residual_units.append(
                        self._make_placeholder_unit(
                            placeholder_token=owner_placeholder,
                            order_index=placeholder_index,
                            origin_frame_id=origin_frame_id,
                        )
                    )
                    placeholder_inserted = True
                    placeholder_index += 1
                continue
            residual_units.append(
                {
                    **dict(unit),
                    "origin_frame_id": origin_frame_id,
                    "source_type": unit.get("source_type", "structure_local"),
                }
            )
            placeholder_index += 1
        if not placeholder_inserted:
            return None
        return self._profile_from_units(
            units=residual_units,
            cut_engine=cut_engine,
            ext={
                "kind": "structure_relative_residual",
                "owner_placeholder": owner_placeholder,
            },
        )

    def _build_descend_relative_profile(
        self,
        *,
        full_profile: dict,
        common_part: dict,
        child_placeholder: str,
        cut_engine,
        origin_frame_id: str,
    ) -> dict | None:
        full_units = self._collect_profile_units(full_profile)
        if not full_units:
            return None
        existing_refs = {
            str(unit_id)
            for pair in common_part.get("matched_pairs", [])
            for unit_id in pair.get("existing_unit_refs", [])
            if str(unit_id)
        }
        incoming_refs = {
            str(unit_id)
            for pair in common_part.get("matched_pairs", [])
            for unit_id in pair.get("incoming_unit_refs", [])
            if str(unit_id)
        }
        full_unit_ids = {str(unit.get("unit_id", "")) for unit in full_units if str(unit.get("unit_id", ""))}
        existing_overlap = len(full_unit_ids & existing_refs)
        incoming_overlap = len(full_unit_ids & incoming_refs)
        matched_refs = incoming_refs if incoming_overlap >= existing_overlap else existing_refs
        if not matched_refs:
            return None
        child_units = []
        placeholder_inserted = False
        placeholder_index = 0
        for unit in full_units:
            unit_id = str(unit.get("unit_id", ""))
            if unit_id in matched_refs:
                if not placeholder_inserted:
                    child_units.append(
                        self._make_placeholder_unit(
                            placeholder_token=child_placeholder,
                            order_index=placeholder_index,
                            origin_frame_id=origin_frame_id,
                        )
                    )
                    placeholder_inserted = True
                    placeholder_index += 1
                continue
            child_units.append(
                {
                    **dict(unit),
                    "origin_frame_id": origin_frame_id,
                    "source_type": unit.get("source_type", "structure_local"),
                }
            )
            placeholder_index += 1
        if not placeholder_inserted:
            return None
        profile = self._profile_from_units(
            units=child_units,
            cut_engine=cut_engine,
            ext={
                "kind": "structure_descend_relative",
                "owner_placeholder": child_placeholder,
            },
        )
        if not self._profile_has_non_placeholder_content(profile, placeholder_token=child_placeholder):
            return None
        return profile

    @staticmethod
    def _profile_has_non_placeholder_content(profile: dict, *, placeholder_token: str) -> bool:
        return any(
            str(unit.get("token", "")) and str(unit.get("token", "")) != placeholder_token
            for group in profile.get("sequence_groups", [])
            for unit in group.get("units", [])
            if isinstance(unit, dict)
        )

    @staticmethod
    def _extract_structure_ids_from_profile(profile: dict) -> list[str]:
        ordered = []
        seen = set()
        for group in profile.get("sequence_groups", []):
            for unit in group.get("units", []):
                if not isinstance(unit, dict) or bool(unit.get("is_placeholder")):
                    continue
                unit_id = str(unit.get("unit_id", ""))
                object_type = str(unit.get("object_type", ""))
                if not unit_id:
                    continue
                if object_type != "st" and not unit_id.startswith("st_"):
                    continue
                if unit_id in seen:
                    continue
                seen.add(unit_id)
                ordered.append(unit_id)
        return ordered

    def _merge_observed_energy_profiles(self, *, profile_maps: list[dict[str, float]], required_ids: list[str]) -> dict[str, float]:
        normalized_maps = [
            self._normalize_external_profile(profile_map, required_ids)
            for profile_map in profile_maps
            if isinstance(profile_map, dict)
        ]
        if not normalized_maps:
            return self._normalize_external_profile({}, required_ids)
        merged = {structure_id: 0.0 for structure_id in required_ids}
        for profile_map in normalized_maps:
            for structure_id in required_ids:
                merged[structure_id] = round(float(merged.get(structure_id, 0.0)) + float(profile_map.get(structure_id, 0.0)), 8)
        divisor = float(len(normalized_maps))
        averaged = {
            structure_id: round(float(merged.get(structure_id, 0.0)) / divisor, 8)
            for structure_id in required_ids
        }
        return self._normalize_external_profile(averaged, required_ids)

    def _normalize_external_profile(self, profile_map: dict[str, float], required_ids: list[str]) -> dict[str, float]:
        keys = [str(structure_id) for structure_id in required_ids if str(structure_id)]
        if not keys:
            keys = [str(key) for key in profile_map.keys() if str(key)]
        if not keys:
            return {}
        values = {key: max(0.0, float(profile_map.get(key, 0.0))) for key in keys}
        total = sum(values.values())
        if total <= 0.0:
            return {key: round(1.0 / len(keys), 8) for key in keys}
        return {key: round(float(values.get(key, 0.0)) / total, 8) for key in keys}

    def _open_owner_context(
        self,
        *,
        owner_kind: str,
        owner_id: str,
        structure_store,
        group_store,
        pointer_index,
        cut_engine,
        trace_id: str,
        tick_id: str,
    ) -> dict | None:
        if owner_kind == "st":
            structure_obj = structure_store.get(owner_id)
            if not structure_obj:
                return None
            pointer_info = {"used_fallback": False, "resolved_db_id": ""}
            structure_db = structure_store.get_db_by_owner(owner_id)
            if not structure_db and pointer_index is not None:
                structure_db, pointer_info = pointer_index.resolve_db(
                    structure_obj=structure_obj,
                    structure_store=structure_store,
                    logger=self._logger,
                    trace_id=trace_id,
                    tick_id=tick_id,
                )
            if not structure_db:
                return None
            if pointer_index is not None:
                pointer_index.cache_structure_db(owner_id, structure_db)
            structure_db.setdefault("group_table", list(structure_db.get("group_table", [])))
            structure_db.setdefault("group_residual_table", [])
            structure_db.setdefault("group_memory_table", [])
            owner_display_text = self._structure_display_text(structure_obj)
            if cut_engine is not None:
                owner_profile = self._profile_from_units(
                    units=[
                        self._make_structure_unit(
                            structure_id=owner_id,
                            display_text=owner_display_text,
                            structure_obj=structure_obj,
                            er=0.0,
                            ev=0.0,
                            order_index=0,
                            source_type="structure_owner",
                            origin_frame_id=owner_id,
                        )
                    ],
                    cut_engine=cut_engine,
                    ext={"kind": "structure_owner_profile", "owner_id": owner_id},
                )
            else:
                owner_profile = {
                    "display_text": owner_display_text,
                    "flat_tokens": [owner_display_text] if owner_display_text else [owner_id],
                    "sequence_groups": [],
                    "content_signature": owner_id,
                }
            return {
                "owner_kind": "st",
                "owner_id": owner_id,
                "owner_display_text": owner_display_text,
                "owner_placeholder": self._owner_placeholder_token(owner_kind="st", owner_id=owner_id, owner_display_text=owner_display_text),
                "owner_profile": owner_profile,
                "structure_obj": structure_obj,
                "db_ref": structure_db,
                "group_table": structure_db.get("group_table", []),
                "residual_table": structure_db.get("group_residual_table", []),
                "memory_table": structure_db.get("group_memory_table", []),
                "used_fallback": bool(pointer_info.get("used_fallback")),
                "resolved_db_id": structure_db.get("structure_db_id", ""),
            }

        if owner_kind == "sg":
            group_obj = group_store.get(owner_id)
            if not group_obj:
                return None
            group_obj.setdefault("local_db", {})
            local_db = group_obj["local_db"]
            local_db.setdefault("group_table", [])
            local_db.setdefault("residual_table", [])
            local_db.setdefault("memory_table", [])
            owner_profile = self._group_full_profile(group_obj=group_obj, structure_store=structure_store, cut_engine=cut_engine)
            owner_display_text = self._group_display_text(group_obj)
            return {
                "owner_kind": "sg",
                "owner_id": owner_id,
                "owner_display_text": owner_display_text,
                "owner_placeholder": self._owner_placeholder_token(owner_kind="sg", owner_id=owner_id, owner_display_text=owner_display_text),
                "owner_profile": owner_profile,
                "group_obj": group_obj,
                "local_db": local_db,
                "group_table": local_db.get("group_table", []),
                "residual_table": local_db.get("residual_table", []),
                "memory_table": local_db.get("memory_table", []),
                "used_fallback": False,
                "resolved_db_id": owner_id,
            }
        return None

    def _persist_owner_context(self, owner_ctx: dict, *, structure_store, group_store) -> None:
        if not owner_ctx:
            return
        self._trim_owner_tables(owner_ctx)
        if owner_ctx.get("owner_kind") == "st":
            structure_db = owner_ctx.get("db_ref")
            if not structure_db:
                return
            structure_db["group_table"] = list(owner_ctx.get("group_table", []))
            structure_db["group_residual_table"] = list(owner_ctx.get("residual_table", []))
            structure_db["group_memory_table"] = list(owner_ctx.get("memory_table", []))
            structure_store.update_db(structure_db)
            return
        if owner_ctx.get("owner_kind") == "sg":
            group_obj = owner_ctx.get("group_obj")
            if not group_obj:
                return
            local_db = group_obj.setdefault("local_db", {})
            local_db["group_table"] = list(owner_ctx.get("group_table", []))
            local_db["residual_table"] = list(owner_ctx.get("residual_table", []))
            local_db["memory_table"] = list(owner_ctx.get("memory_table", []))
            group_store.update(group_obj)

    def _trim_owner_tables(self, owner_ctx: dict) -> None:
        group_limit = max(8, int(self._config.get("group_table_soft_limit", 128)))
        residual_limit = max(16, int(self._config.get("diff_table_soft_limit", 128)))
        memory_limit = max(16, int(self._config.get("structure_memory_table_soft_limit", 128)))
        for entry in owner_ctx.get("group_table", []):
            self._ensure_group_entry_schema(entry)
        for entry in owner_ctx.get("residual_table", []):
            self._ensure_raw_residual_schema(entry)
        owner_ctx["group_table"] = sorted(
            owner_ctx.get("group_table", []),
            key=lambda entry: (
                -float(self._weight.entry_runtime_weight(entry)),
                -float(entry.get("base_weight", 1.0)),
                str(entry.get("entry_id", "")),
            ),
        )[:group_limit]
        owner_ctx["residual_table"] = sorted(
            owner_ctx.get("residual_table", []),
            key=lambda entry: (
                -float(self._weight.entry_runtime_weight(entry)),
                -float(entry.get("base_weight", 1.0)),
                str(entry.get("entry_id", "")),
            ),
        )[:residual_limit]
        owner_ctx["memory_table"] = sorted(
            owner_ctx.get("memory_table", []),
            key=lambda entry: (
                -int(entry.get("last_updated_at", 0)),
                str(entry.get("memory_id", "")),
            ),
        )[:memory_limit]

    def _append_memory_ref(
        self,
        *,
        owner_ctx: dict,
        memory_id: str,
        content_signature: str,
        round_index: int,
        event_kind: str,
    ) -> None:
        if not memory_id:
            return
        now_ms = int(time.time() * 1000)
        for entry in owner_ctx.get("memory_table", []):
            if str(entry.get("memory_id", "")) != memory_id:
                continue
            if str(entry.get("content_signature", "")) != str(content_signature):
                continue
            entry["hit_count"] = int(entry.get("hit_count", 0)) + 1
            entry["last_updated_at"] = now_ms
            entry["round_index"] = round_index
            entry["event_kind"] = event_kind
            return
        owner_ctx.setdefault("memory_table", []).append(
            {
                "memory_id": memory_id,
                "content_signature": str(content_signature),
                "event_kind": event_kind,
                "round_index": int(round_index),
                "hit_count": 1,
                "last_updated_at": now_ms,
            }
        )

    def _ensure_group_entry_schema(self, entry: dict) -> None:
        now_ms = int(time.time() * 1000)
        entry.setdefault("entry_id", next_id("sge"))
        entry.setdefault("group_id", "")
        entry.setdefault("required_structure_ids", [])
        entry.setdefault("avg_energy_profile", {})
        entry.setdefault("content_signature", "")
        entry.setdefault("temporal_signature", entry.get("content_signature", ""))
        entry.setdefault("display_text", entry.get("group_id", ""))
        entry.setdefault("relative_content_signature", entry.get("content_signature", ""))
        entry.setdefault("relative_flat_tokens", [])
        entry.setdefault("relative_sequence_groups", [])
        entry.setdefault("base_weight", 1.0)
        entry.setdefault("recent_gain", self._weight._target_recent_gain(strength=1.0))
        entry.setdefault("fatigue", 0.0)
        entry.setdefault("runtime_er", 0.0)
        entry.setdefault("runtime_ev", 0.0)
        entry.setdefault("match_count_total", 0)
        entry.setdefault("last_updated_at", now_ms)
        entry.setdefault("last_matched_at", 0)
        entry.setdefault("last_recency_refresh_at", now_ms)
        entry.setdefault("recency_hold_rounds_remaining", int(self._config.get("recency_gain_hold_rounds", 2)))
        entry.setdefault("ext", {})

    def _ensure_raw_residual_schema(self, entry: dict) -> None:
        now_ms = int(time.time() * 1000)
        entry.setdefault("entry_id", next_id("sgr"))
        entry.setdefault("entry_type", "raw_residual")
        entry.setdefault("content_signature", "")
        entry.setdefault("display_text", "")
        entry.setdefault("flat_tokens", [])
        entry.setdefault("sequence_groups", [])
        entry.setdefault("canonical_content_signature", "")
        entry.setdefault("canonical_display_text", "")
        entry.setdefault("canonical_flat_tokens", [])
        entry.setdefault("canonical_sequence_groups", [])
        entry.setdefault("base_weight", 1.0)
        entry.setdefault("recent_gain", self._weight._target_recent_gain(strength=1.0))
        entry.setdefault("fatigue", 0.0)
        entry.setdefault("runtime_er", 0.0)
        entry.setdefault("runtime_ev", 0.0)
        entry.setdefault("match_count_total", 0)
        entry.setdefault("last_updated_at", now_ms)
        entry.setdefault("last_matched_at", 0)
        entry.setdefault("last_recency_refresh_at", now_ms)
        entry.setdefault("recency_hold_rounds_remaining", int(self._config.get("recency_gain_hold_rounds", 2)))
        entry.setdefault("observed_energy_profile", {})
        entry.setdefault("sample_count", 1)
        entry.setdefault("memory_refs", [])
        entry.setdefault("ext", {})

    def _remove_local_item(self, *, owner_ctx: dict, item: dict) -> None:
        entry_id = str(item.get("entry_id", ""))
        if not entry_id:
            return
        if item.get("item_kind") == "group_entry":
            owner_ctx["group_table"] = [
                entry for entry in owner_ctx.get("group_table", [])
                if str(entry.get("entry_id", "")) != entry_id
            ]
            return
        owner_ctx["residual_table"] = [
            entry for entry in owner_ctx.get("residual_table", [])
            if str(entry.get("entry_id", "")) != entry_id
        ]

    def _group_entry_relative_profile(self, *, owner_ctx: dict, entry: dict, group_obj: dict, cut_engine) -> dict | None:
        self._ensure_group_entry_schema(entry)
        relative_groups = list(entry.get("relative_sequence_groups", []))
        if relative_groups:
            return self._profile_from_stored_groups(
                relative_groups,
                cut_engine=cut_engine,
                ext={
                    "kind": "structure_group_relative",
                    "owner_kind": owner_ctx.get("owner_kind", ""),
                    "owner_id": owner_ctx.get("owner_id", ""),
                },
            )
        full_profile = self._group_full_profile(group_obj=group_obj, structure_store=None, cut_engine=cut_engine)
        owner_ids = set(self._extract_structure_ids_from_profile(owner_ctx.get("owner_profile", {})))
        if not owner_ids:
            return full_profile
        placeholder_token = owner_ctx.get("owner_placeholder", "")
        relative_units = []
        placeholder_inserted = False
        placeholder_index = 0
        for unit in self._collect_profile_units(full_profile):
            unit_id = str(unit.get("unit_id", ""))
            if unit_id in owner_ids:
                if not placeholder_inserted:
                    relative_units.append(
                        self._make_placeholder_unit(
                            placeholder_token=placeholder_token,
                            order_index=placeholder_index,
                            origin_frame_id=str(group_obj.get("id", "")),
                        )
                    )
                    placeholder_inserted = True
                    placeholder_index += 1
                continue
            relative_units.append(dict(unit))
            placeholder_index += 1
        if not placeholder_inserted:
            return None
        profile = self._profile_from_units(
            units=relative_units,
            cut_engine=cut_engine,
            ext={
                "kind": "structure_group_relative",
                "owner_kind": owner_ctx.get("owner_kind", ""),
                "owner_id": owner_ctx.get("owner_id", ""),
            },
        )
        entry["relative_content_signature"] = profile.get("content_signature", "")
        entry["relative_flat_tokens"] = list(profile.get("flat_tokens", []))
        entry["relative_sequence_groups"] = list(profile.get("sequence_groups", []))
        return profile

    def _profile_from_stored_groups(self, groups: list[dict], *, cut_engine, ext: dict | None = None) -> dict:
        if cut_engine is None:
            flat_tokens = [str(token) for group in groups for token in group.get("tokens", []) if str(token)]
            return {
                "display_text": format_sequence_groups(groups) or " / ".join(flat_tokens),
                "flat_tokens": flat_tokens,
                "sequence_groups": list(groups),
                "content_signature": "||".join(str(group.get("group_signature", "")) for group in groups if str(group.get("group_signature", ""))),
                "ext": dict(ext or {}),
            }
        profile = cut_engine.build_sequence_profile_from_groups(groups)
        merged_ext = dict(profile.get("ext", {}))
        merged_ext.update(ext or {})
        profile["ext"] = merged_ext
        return profile

    def _canonicalize_local_profile(self, *, profile: dict, structure_store, group_store, cut_engine) -> dict:
        restored = restore_profile(
            profile,
            cut_engine=cut_engine,
            structure_store=structure_store,
            group_store=group_store,
        )
        merged_ext = dict(restored.get("ext", {}))
        merged_ext.update(profile.get("ext", {}))
        restored["ext"] = merged_ext
        return restored

    def _ensure_raw_residual_canonical_fields(self, *, entry: dict, structure_store, group_store, cut_engine) -> dict:
        self._ensure_raw_residual_schema(entry)
        raw_profile = self._profile_from_stored_groups(
            entry.get("sequence_groups", []),
            cut_engine=cut_engine,
            ext={"kind": "structure_raw_residual"},
        )
        canonical_groups = list(entry.get("canonical_sequence_groups", []))
        if canonical_groups:
            canonical_profile = self._profile_from_stored_groups(
                canonical_groups,
                cut_engine=cut_engine,
                ext={"kind": "structure_raw_residual_canonical"},
            )
        else:
            canonical_profile = self._canonicalize_local_profile(
                profile=raw_profile,
                structure_store=structure_store,
                group_store=group_store,
                cut_engine=cut_engine,
            )
            entry["canonical_content_signature"] = canonical_profile.get("content_signature", "")
            entry["canonical_display_text"] = canonical_profile.get("display_text", "")
            entry["canonical_flat_tokens"] = list(canonical_profile.get("flat_tokens", []))
            entry["canonical_sequence_groups"] = list(canonical_profile.get("sequence_groups", []))
        return {"raw_profile": raw_profile, "canonical_profile": canonical_profile}

    def _build_raw_residual_action(
        self,
        *,
        action_type: str,
        owner_ctx: dict,
        entry: dict,
    ) -> dict:
        storage_table = "group_residual_table" if owner_ctx.get("owner_kind") == "st" else "local_db.residual_table"
        return {
            "type": action_type,
            "type_zh": "追加原始残差信息" if action_type == "append_raw_residual" else "强化原始残差信息",
            "owner_kind": owner_ctx.get("owner_kind", ""),
            "owner_id": owner_ctx.get("owner_id", ""),
            "resolved_db_id": owner_ctx.get("resolved_db_id", ""),
            "storage_table": storage_table,
            "storage_table_zh": "结构数据库.结构组残差表" if storage_table == "group_residual_table" else "结构组本地库.残差表",
            "entry_id": entry.get("entry_id", ""),
            "memory_id": (entry.get("memory_refs", []) or [""])[-1] if entry.get("memory_refs", []) else "",
            "raw_display_text": entry.get("display_text", ""),
            "raw_signature": entry.get("content_signature", ""),
            "raw_sequence_groups": list(entry.get("sequence_groups", [])),
            "canonical_display_text": entry.get("canonical_display_text", ""),
            "canonical_signature": entry.get("canonical_content_signature", ""),
            "canonical_sequence_groups": list(entry.get("canonical_sequence_groups", [])),
        }

    def _group_full_profile(self, *, group_obj: dict, structure_store, cut_engine) -> dict:
        group_structure = group_obj.get("group_structure", {})
        required_ids = [str(structure_id) for structure_id in group_obj.get("required_structure_ids", []) if str(structure_id)]
        if group_structure.get("sequence_groups"):
            stored_profile = self._profile_from_stored_groups(
                list(group_structure.get("sequence_groups", [])),
                cut_engine=cut_engine,
                ext={
                    "kind": "structure_group_full",
                    "group_id": group_obj.get("id", ""),
                },
            )
            stored_ids = self._extract_structure_ids_from_profile(stored_profile)
            if stored_ids == required_ids and len(stored_ids) == len(required_ids):
                return stored_profile
        units = []
        for order_index, structure_id in enumerate(required_ids):
            structure_obj = structure_store.get(structure_id) if structure_store is not None else None
            display_text = self._structure_display_text(structure_obj) if structure_obj else structure_id
            total = float(group_obj.get("avg_energy_profile", {}).get(structure_id, 1.0))
            units.append(
                self._make_structure_unit(
                    structure_id=structure_id,
                    display_text=display_text,
                    structure_obj=structure_obj,
                    er=total,
                    ev=0.0,
                    order_index=order_index,
                    source_type="group",
                    origin_frame_id=group_obj.get("id", ""),
                )
            )
        profile = self._profile_from_units(
            units=units,
            cut_engine=cut_engine,
            ext={
                "kind": "structure_group_full",
                "group_id": group_obj.get("id", ""),
            },
        ) if cut_engine is not None else {
            "display_text": " / ".join(str(unit.get("token", "")) for unit in units if str(unit.get("token", ""))),
            "flat_tokens": [str(unit.get("token", "")) for unit in units if str(unit.get("token", ""))],
            "sequence_groups": [
                {
                    "group_index": index,
                    "source_type": unit.get("source_type", "group"),
                    "origin_frame_id": unit.get("origin_frame_id", group_obj.get("id", "")),
                    "tokens": [unit.get("token", "")],
                    "units": [dict(unit)],
                }
                for index, unit in enumerate(units)
            ],
            "content_signature": "||".join(f"ST:{unit.get('unit_id', '')}" for unit in units),
        }
        display_text = group_structure.get("display_text", "") or self._group_display_text(group_obj)
        if display_text:
            profile["display_text"] = display_text
        return profile

    @staticmethod
    def _structure_display_text(structure_obj: dict | None) -> str:
        if not structure_obj:
            return ""
        return str(structure_obj.get("structure", {}).get("display_text", structure_obj.get("id", "")))

    @staticmethod
    def _group_display_text(group_obj: dict) -> str:
        group_structure = group_obj.get("group_structure", {})
        display_text = str(group_structure.get("display_text", ""))
        if display_text:
            return display_text
        grouped = format_sequence_groups(list(group_structure.get("sequence_groups", [])))
        if grouped:
            return grouped
        flat_tokens = [str(token) for token in group_structure.get("flat_tokens", []) if str(token)]
        if flat_tokens:
            return " / ".join(flat_tokens)
        return str(group_obj.get("id", ""))

    @staticmethod
    def _owner_placeholder_token(*, owner_kind: str, owner_id: str, owner_display_text: str) -> str:
        label = owner_display_text or owner_id
        return f"SELF[{owner_id}:{label}]" if owner_kind in {"st", "sg"} else f"SELF[{label}]"

    def _upsert_group_details(self, base_items: list[dict], new_items: list[dict]) -> list[dict]:
        merged = {}
        for item in list(base_items) + list(new_items):
            key = "|".join(
                [
                    str(item.get("owner_kind", "")),
                    str(item.get("owner_id", "")),
                    str(item.get("entry_id", "")),
                    str(item.get("group_id", "")),
                ]
            )
            current = merged.get(key)
            if current is None or self._is_better_group_detail(item, current):
                merged[key] = item
        return sorted(
            merged.values(),
            key=lambda item: (
                0 if item.get("eligible") else 1,
                -float(item.get("competition_score", 0.0)),
                -len(item.get("required_structure_ids", [])),
                -float(item.get("entry_runtime_weight", 0.0)),
                -float(item.get("runtime_weight", 0.0)),
            ),
        )

    @staticmethod
    def _is_better_group_detail(candidate: dict, current: dict) -> bool:
        candidate_key = (
            bool(candidate.get("eligible")),
            float(candidate.get("competition_score", 0.0)),
            len(candidate.get("required_structure_ids", [])),
            float(candidate.get("entry_runtime_weight", 0.0)),
            float(candidate.get("runtime_weight", 0.0)),
        )
        current_key = (
            bool(current.get("eligible")),
            float(current.get("competition_score", 0.0)),
            len(current.get("required_structure_ids", [])),
            float(current.get("entry_runtime_weight", 0.0)),
            float(current.get("runtime_weight", 0.0)),
        )
        return candidate_key > current_key

    def _build_bias_projections(
        self,
        *,
        group_obj: dict,
        required_ids: list[str],
        matched_er_total: float,
        matched_ev_total: float,
        rho: float,
        structure_store,
    ) -> list[dict]:
        target_ids = self._derive_bias_structures(group_obj=group_obj, required_ids=required_ids)
        if not target_ids:
            return []
        target_weights = self._derive_bias_weight_map(target_ids=target_ids, structure_store=structure_store)
        er_budget = max(0.0, float(matched_er_total)) * max(0.0, float(rho)) * float(self._config.get("structure_bias_er_ratio", 0.18))
        ev_budget = max(0.0, float(matched_ev_total)) * max(0.0, float(rho)) * float(self._config.get("structure_bias_ev_ratio", 0.28))
        projections = []
        for structure_id in target_ids:
            weight = float(target_weights.get(structure_id, 0.0))
            structure_obj = structure_store.get(structure_id)
            projections.append(
                {
                    "structure_id": structure_id,
                    "display_text": self._structure_display_text(structure_obj),
                    "er": round(er_budget * weight, 8),
                    "ev": round(ev_budget * weight, 8),
                    "reason": "structure_group_bias",
                    "source_group_id": group_obj.get("id", ""),
                }
            )
        return [item for item in projections if float(item.get("er", 0.0)) > 0.0 or float(item.get("ev", 0.0)) > 0.0]

    def _derive_bias_structures(self, *, group_obj: dict, required_ids: list[str]) -> list[str]:
        explicit = [str(structure_id) for structure_id in group_obj.get("bias_structure_ids", []) if str(structure_id)]
        if explicit:
            return list(dict.fromkeys(explicit))
        return self._derive_bias_from_required(required_ids)

    @staticmethod
    def _derive_bias_from_required(required_ids: list[str]) -> list[str]:
        return []

    def _derive_bias_weight_map(self, *, target_ids: list[str], structure_store) -> dict[str, float]:
        weights = {}
        now_ms = int(time.time() * 1000)
        for structure_id in target_ids:
            structure_obj = structure_store.get(structure_id)
            if not structure_obj:
                continue
            weights[structure_id] = float(self._preview_structure_stats(structure_obj, now_ms=now_ms).get("runtime_weight", 1.0))
        total = sum(max(0.0, value) for value in weights.values())
        if total <= 0.0:
            if not target_ids:
                return {}
            return {structure_id: round(1.0 / len(target_ids), 8) for structure_id in target_ids}
        return {structure_id: round(max(0.0, float(weights.get(structure_id, 0.0))) / total, 8) for structure_id in target_ids}

    def _build_internal_fragments(
        self,
        *,
        source_group_id: str,
        source_phase: str,
        structure_ids: list[str],
        transfer_er_map: dict[str, float],
        transfer_ev_map: dict[str, float],
        structure_store,
    ) -> list[dict]:
        fragments = []
        for structure_id in structure_ids:
            total_er = round(max(0.0, float(transfer_er_map.get(structure_id, 0.0))), 8)
            total_ev = round(max(0.0, float(transfer_ev_map.get(structure_id, 0.0))), 8)
            if total_er + total_ev <= 0.0:
                continue
            structure_obj = structure_store.get(structure_id)
            if not structure_obj:
                continue
            structure = structure_obj.get("structure", {})
            sequence_groups = list(structure.get("sequence_groups", []))
            if not sequence_groups:
                sequence_groups = [{"group_index": 0, "source_type": "internal", "origin_frame_id": structure_id, "tokens": list(structure.get("flat_tokens", []))}]
            fragments.append(
                {
                    "fragment_id": next_id("sif"),
                    "source_group_id": source_group_id,
                    "source_phase": source_phase,
                    "source_structure_id": structure_id,
                    "display_text": structure.get("display_text", structure_id),
                    "flat_tokens": list(structure.get("flat_tokens", [])),
                    "sequence_groups": sequence_groups,
                    "er_hint": total_er,
                    "ev_hint": total_ev,
                    "energy_hint": round(total_er + total_ev, 8),
                }
            )
        return fragments

    def _merge_internal_fragments(self, fragments: list[dict]) -> list[dict]:
        merged = {}
        for fragment in fragments:
            key = str(fragment.get("source_structure_id", "")) or str(fragment.get("display_text", "")) or str(fragment.get("fragment_id", ""))
            current = merged.get(key)
            if current is None:
                merged[key] = dict(fragment)
                continue
            current["er_hint"] = round(float(current.get("er_hint", 0.0)) + float(fragment.get("er_hint", 0.0)), 8)
            current["ev_hint"] = round(float(current.get("ev_hint", 0.0)) + float(fragment.get("ev_hint", 0.0)), 8)
            current["energy_hint"] = round(float(current.get("er_hint", 0.0)) + float(current.get("ev_hint", 0.0)), 8)
        return list(merged.values())

    @staticmethod
    def _max_total_budget(structure_ids: list[str], budget_er_map: dict[str, float], budget_ev_map: dict[str, float]) -> float:
        return round(
            sum(
                max(0.0, float(budget_er_map.get(structure_id, 0.0))) + max(0.0, float(budget_ev_map.get(structure_id, 0.0)))
                for structure_id in structure_ids
            ),
            8,
        )

    @staticmethod
    def _build_budget_snapshot(structure_ids: list[str], budget_er_map: dict[str, float], budget_ev_map: dict[str, float]) -> dict[str, dict[str, float]]:
        return {
            structure_id: {
                "er": round(max(0.0, float(budget_er_map.get(structure_id, 0.0))), 8),
                "ev": round(max(0.0, float(budget_ev_map.get(structure_id, 0.0))), 8),
                "total": round(max(0.0, float(budget_er_map.get(structure_id, 0.0))) + max(0.0, float(budget_ev_map.get(structure_id, 0.0))), 8),
            }
            for structure_id in structure_ids
        }

    def _build_structure_refs(self, structure_ids: list[str], structure_store) -> list[dict]:
        refs = []
        for structure_id in structure_ids or []:
            structure_obj = structure_store.get(structure_id)
            refs.append(
                {
                    "structure_id": structure_id,
                    "display_text": self._structure_display_text(structure_obj) or structure_id,
                    "content_signature": structure_obj.get("structure", {}).get("content_signature", "") if structure_obj else "",
                    "exists": bool(structure_obj),
                }
            )
        return refs

    def _build_group_debug_payload(self, group_obj: dict, structure_store, cut_engine) -> dict:
        now_ms = int(time.time() * 1000)
        stats = self._preview_group_stats(group_obj, now_ms=now_ms)
        profile = self._group_full_profile(group_obj=group_obj, structure_store=structure_store, cut_engine=cut_engine)
        group_structure = group_obj.get("group_structure", {})
        return {
            "group_id": group_obj.get("id", ""),
            "display_text": self._group_display_text(group_obj),
            "grouped_display_text": profile.get("display_text", self._group_display_text(group_obj)),
            "sequence_groups": list(profile.get("sequence_groups", [])),
            "required_structure_ids": list(group_obj.get("required_structure_ids", [])),
            "bias_structure_ids": list(group_obj.get("bias_structure_ids", [])),
            "required_structures": self._build_structure_refs(group_obj.get("required_structure_ids", []), structure_store),
            "bias_structures": self._build_structure_refs(group_obj.get("bias_structure_ids", []), structure_store),
            "avg_energy_profile": dict(group_obj.get("avg_energy_profile", {})),
            "content_signature": group_structure.get("content_signature", profile.get("content_signature", "")),
            "temporal_signature": group_structure.get("temporal_signature", group_structure.get("content_signature", profile.get("content_signature", ""))),
            "flat_tokens": list(profile.get("flat_tokens", [])),
            "base_weight": round(float(stats.get("base_weight", 1.0)), 8),
            "recent_gain": round(float(stats.get("recent_gain", 1.0)), 8),
            "fatigue": round(float(stats.get("fatigue", 0.0)), 8),
            "runtime_weight": round(float(stats.get("runtime_weight", 1.0)), 8),
        }



