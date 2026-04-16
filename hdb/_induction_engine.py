# -*- coding: utf-8 -*-
"""
Induction propagation engine for HDB.
"""

from __future__ import annotations

import time


class InductionEngine:
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
        episodic_store,
        pointer_index,
        cut_engine,
        max_source_items: int | None,
        enable_ev_propagation: bool,
        enable_er_induction: bool,
    ) -> dict:
        top_items = list(state_snapshot.get("top_items") or state_snapshot.get("items") or [])
        st_items = [item for item in top_items if item.get("ref_object_type") == "st"]
        if max_source_items:
            st_items = st_items[:max_source_items]

        now_ms = int(time.time() * 1000)
        source_item_count = 0
        propagated_target_count = 0
        induced_target_count = 0
        total_delta_ev = 0.0
        total_ev_consumed = 0.0
        updated_weight_count = 0
        fallback_used = False
        induction_targets: dict[tuple[str, str, str], dict] = {}
        source_details = []
        source_ev_consumptions = []

        # Ratios / 比例系数（对齐配置语义）
        # --------------------------------
        # 注意：这两个系数在理论与配置文档中明确存在：
        # - ev_propagation_ratio: 只消耗/传播“源 EV”的一部分，避免虚能量过度喷洒
        # - er_induction_ratio: 只使用“源 ER”的一部分作为“诱发 EV”的预算，避免 ER->EV 转换过猛
        #
        # 原型早期曾出现“配置存在但未使用”的情况，会导致 EV 过强、系统难以稳定。
        # 这里把它们作为明确的全局系数，便于调参与理论验收。
        try:
            ev_ratio = float(self._config.get("ev_propagation_ratio", 0.28) or 0.0)
        except Exception:
            ev_ratio = 0.28
        ev_ratio = max(0.0, min(1.0, float(ev_ratio)))

        try:
            er_ratio = float(self._config.get("er_induction_ratio", 0.22) or 0.0)
        except Exception:
            er_ratio = 0.22
        er_ratio = max(0.0, min(1.0, float(er_ratio)))

        for item in st_items:
            structure_id = item.get("ref_object_id", "") or item.get("id", "")
            structure_obj = structure_store.get(structure_id)
            if not structure_obj:
                continue
            structure_db, pointer_info = pointer_index.resolve_db(
                structure_obj=structure_obj,
                structure_store=structure_store,
                logger=self._logger,
                trace_id=trace_id,
                tick_id=tick_id,
            )
            fallback_used = fallback_used or bool(pointer_info.get("used_fallback"))
            if not structure_db:
                continue

            source_item_count += 1
            source_er = round(max(0.0, float(item.get("er", 0.0))), 8)
            source_ev = round(max(0.0, float(item.get("ev", 0.0))), 8)
            source_profile = cut_engine.build_sequence_profile_from_structure(structure_obj)
            aggregated_targets = self._aggregate_local_targets(
                structure_db=structure_db,
                structure_store=structure_store,
                episodic_store=episodic_store,
                cut_engine=cut_engine,
                now_ms=now_ms,
            )

            source_detail = {
                "source_structure_id": structure_id,
                "display_text": structure_obj.get("structure", {}).get("display_text", structure_id),
                "source_er": source_er,
                "source_ev": source_ev,
                "pointer_info": pointer_info,
                "candidate_entries": [],
            }

            if enable_ev_propagation and source_ev >= float(self._config.get("ev_propagation_threshold", 0.12)):
                total_weight = sum(float(target.get("runtime_weight", 0.0)) for target in aggregated_targets.values())
                if total_weight > 0.0:
                    # Consume only a fraction of source EV, as configured.
                    # 只消耗/传播源 EV 的一部分（ev_propagation_ratio）
                    consumed_ev = round(source_ev * ev_ratio, 8)
                    if consumed_ev <= 0.0:
                        consumed_ev = 0.0
                    total_ev_consumed += consumed_ev
                    source_ev_consumptions.append(
                        {
                            "source_structure_id": structure_id,
                            "source_item_id": item.get("item_id", item.get("id", "")),
                            "consumed_ev": consumed_ev,
                        }
                    )
                    for target in aggregated_targets.values():
                        target_runtime_weight = float(target.get("runtime_weight", 0.0))
                        delta_ev = round(consumed_ev * (target_runtime_weight / total_weight), 8)
                        if delta_ev <= 0.0:
                            continue
                        target_debug = self._build_target_debug_entry(
                            target=target,
                            total_weight=total_weight,
                            delta_ev=delta_ev,
                        )
                        self._mark_bucket_entries(
                            entry_items=target.get("entries", []),
                            total_weight=total_weight,
                            delta_er=0.0,
                            delta_ev=consumed_ev,
                            now_ms=now_ms,
                        )
                        updated_weight_count += len(target.get("entries", []))
                        total_delta_ev += delta_ev
                        propagated_target_count += 1
                        self._append_target_delta(
                            induction_targets=induction_targets,
                            projection_kind=target.get("projection_kind", "structure"),
                            memory_id=target.get("memory_id", ""),
                            target_id=target.get("target_id", ""),
                            backing_structure_id=target.get("backing_structure_id", ""),
                            target_display_text=target.get("display_text", target.get("target_id", "")),
                            mode="ev_propagation",
                            source_structure_id=structure_id,
                            delta_ev=delta_ev,
                            runtime_weight=target_runtime_weight,
                        )
                        source_detail["candidate_entries"].append(
                            {
                                "projection_kind": target.get("projection_kind", "structure"),
                                "memory_id": target.get("memory_id", ""),
                                "target_structure_id": target.get("target_id", ""),
                                "backing_structure_id": target.get("backing_structure_id", ""),
                                "target_display_text": target.get("display_text", target.get("target_id", "")),
                                "mode": "ev_propagation",
                                **target_debug,
                            }
                        )

            if enable_er_induction and source_er >= float(self._config.get("er_induction_threshold", 0.15)):
                induction_candidates = self._filter_full_inclusion_targets(
                    source_structure_id=structure_id,
                    source_profile=source_profile,
                    aggregated_targets=aggregated_targets,
                    cut_engine=cut_engine,
                )
                total_weight = sum(float(target.get("runtime_weight", 0.0)) for target in induction_candidates.values())
                if total_weight > 0.0:
                    # Induction budget: only use a fraction of source ER to generate EV.
                    # 诱发预算：只使用源 ER 的一部分作为“诱发 EV”的预算（er_induction_ratio）
                    induction_budget = round(source_er * er_ratio, 8)
                    if induction_budget <= 0.0:
                        induction_budget = 0.0
                    for target in induction_candidates.values():
                        target_runtime_weight = float(target.get("runtime_weight", 0.0))
                        delta_ev = round(induction_budget * (target_runtime_weight / total_weight), 8)
                        if delta_ev <= 0.0:
                            continue
                        target_debug = self._build_target_debug_entry(
                            target=target,
                            total_weight=total_weight,
                            delta_ev=delta_ev,
                        )
                        self._mark_bucket_entries(
                            entry_items=target.get("entries", []),
                            total_weight=total_weight,
                            delta_er=induction_budget,
                            delta_ev=induction_budget,
                            now_ms=now_ms,
                        )
                        updated_weight_count += len(target.get("entries", []))
                        total_delta_ev += delta_ev
                        induced_target_count += 1
                        self._append_target_delta(
                            induction_targets=induction_targets,
                            projection_kind=target.get("projection_kind", "structure"),
                            memory_id=target.get("memory_id", ""),
                            target_id=target.get("target_id", ""),
                            backing_structure_id=target.get("backing_structure_id", ""),
                            target_display_text=target.get("display_text", target.get("target_id", "")),
                            mode="er_induction",
                            source_structure_id=structure_id,
                            delta_ev=delta_ev,
                            runtime_weight=target_runtime_weight,
                        )
                        source_detail["candidate_entries"].append(
                            {
                                "projection_kind": target.get("projection_kind", "structure"),
                                "memory_id": target.get("memory_id", ""),
                                "target_structure_id": target.get("target_id", ""),
                                "backing_structure_id": target.get("backing_structure_id", ""),
                                "target_display_text": target.get("display_text", target.get("target_id", "")),
                                "mode": "er_induction",
                                **target_debug,
                            }
                        )

            self._maintenance.apply_structure_db_soft_limits(structure_db)
            structure_store.update_db(structure_db)
            source_detail["candidate_entries"].sort(
                key=lambda item: (
                    item.get("mode", ""),
                    -float(item.get("runtime_weight", 0.0)),
                    item.get("memory_id", "") or item.get("target_structure_id", ""),
                )
            )
            source_details.append(source_detail)

        target_list = []
        for payload in induction_targets.values():
            target_list.append(
                {
                    "projection_kind": payload.get("projection_kind", "structure"),
                    "memory_id": payload.get("memory_id", ""),
                    "target_structure_id": payload.get("target_structure_id", ""),
                    "backing_structure_id": payload.get("backing_structure_id", ""),
                    "target_display_text": payload.get("target_display_text", ""),
                    "delta_ev": round(float(payload.get("delta_ev", 0.0)), 8),
                    "sources": list(dict.fromkeys(payload.get("sources", []))),
                    "modes": [payload.get("mode", "")],
                    "runtime_weight": round(float(payload.get("runtime_weight", 0.0)), 8),
                }
            )

        self._logger.detail(
            trace_id=trace_id,
            tick_id=tick_id,
            step="induction_targets",
            message_zh="感应赋能目标摘要",
            message_en="Induction target summary",
            info={"targets": target_list[:16]},
        )

        return {
            "code": "OK",
            "message": "Induction propagation completed",
            "source_item_count": source_item_count,
            "propagated_target_count": propagated_target_count,
            "induced_target_count": induced_target_count,
            "total_delta_ev": round(total_delta_ev, 8),
            "total_ev_consumed": round(total_ev_consumed, 8),
            "updated_weight_count": updated_weight_count,
            "induction_targets": target_list,
            "source_ev_consumptions": source_ev_consumptions,
            "fallback_used": fallback_used,
            "debug": {
                "source_details": source_details,
            },
        }

    def _aggregate_local_targets(
        self,
        *,
        structure_db: dict,
        structure_store,
        episodic_store,
        cut_engine,
        now_ms: int,
    ) -> dict[tuple[str, str], dict]:
        targets: dict[tuple[str, str], dict] = {}
        for entry in structure_db.get("diff_table", []):
            self._weight.decay_entry(entry, now_ms=now_ms, round_step=1)
            entry_weight = self._weight.entry_runtime_weight(entry)
            if entry_weight <= 0.0:
                continue
            entry_ext = dict(entry.get("ext", {}) or {})
            relation_type = str(entry_ext.get("relation_type", ""))
            entry_type = str(entry.get("entry_type", "structure_ref"))
            target_id = str(entry.get("target_id", ""))

            # Raw residual entries point to episodic memories rather than structures.
            if entry_type == "raw_residual" and relation_type == "stimulus_raw_residual":
                memory_refs = [str(memory_id) for memory_id in entry.get("memory_refs", []) if str(memory_id)]
                if not memory_refs:
                    continue
                target_profile = self._resolve_entry_profile(
                    entry=entry,
                    cut_engine=cut_engine,
                    structure_store=structure_store,
                )
                if not target_profile:
                    continue
                shared_weight = entry_weight / max(1, len(memory_refs))
                for memory_id in memory_refs:
                    episodic_obj = episodic_store.get(memory_id)
                    if episodic_obj is None:
                        continue
                    bucket = targets.setdefault(
                        ("memory", memory_id),
                        {
                            "projection_kind": "memory",
                            "memory_id": memory_id,
                            "target_id": "",
                            "backing_structure_id": str(structure_db.get("owner_structure_id", "")),
                            "display_text": (
                                str(entry.get("canonical_display_text", ""))
                                or str(episodic_obj.get("meta", {}).get("ext", {}).get("display_text", ""))
                                or str(episodic_obj.get("event_summary", ""))
                                or memory_id
                            ),
                            "runtime_weight": 0.0,
                            "entry_count": 0,
                            "base_weight_weighted_sum": 0.0,
                            "recent_gain_weighted_sum": 0.0,
                            "fatigue_weighted_sum": 0.0,
                            "entries": [],
                            "structure_obj": None,
                            "target_profile": target_profile,
                            "relation_type": relation_type,
                            "owner_structure_id": str(structure_db.get("owner_structure_id", "")),
                        },
                    )
                    bucket["runtime_weight"] += shared_weight
                    bucket["entry_count"] += 1
                    bucket["base_weight_weighted_sum"] += float(entry.get("base_weight", 1.0)) * shared_weight
                    bucket["recent_gain_weighted_sum"] += float(entry.get("recent_gain", 1.0)) * shared_weight
                    bucket["fatigue_weighted_sum"] += float(entry.get("fatigue", 0.0)) * shared_weight
                    bucket["entries"].append(
                        {
                            "entry": entry,
                            "entry_runtime_weight": shared_weight,
                        }
                    )
                continue

            if not target_id:
                continue
            target_structure = structure_store.get(target_id)
            if not target_structure:
                continue
            memory_id = str(entry_ext.get("anchor_memory_id", ""))
            projection_kind = "memory" if memory_id else "structure"
            target_key = memory_id if memory_id else target_id
            bucket_key = (projection_kind, target_key)
            target_display_text = (
                str(entry_ext.get("canonical_display_text", ""))
                or str(target_structure.get("structure", {}).get("display_text", target_id))
                or target_id
            )
            bucket = targets.setdefault(
                bucket_key,
                {
                    "projection_kind": projection_kind,
                    "memory_id": memory_id,
                    "target_id": target_id,
                    "backing_structure_id": target_id,
                    "display_text": target_display_text,
                    "runtime_weight": 0.0,
                    "entry_count": 0,
                    "base_weight_weighted_sum": 0.0,
                    "recent_gain_weighted_sum": 0.0,
                    "fatigue_weighted_sum": 0.0,
                    "entries": [],
                    "structure_obj": target_structure,
                    "target_profile": cut_engine.build_sequence_profile_from_structure(target_structure),
                    "relation_type": relation_type,
                    "owner_structure_id": str(
                        entry_ext.get("owner_structure_id", "")
                        or target_structure.get("structure", {}).get("ext", {}).get("owner_structure_id", "")
                    ),
                },
            )
            bucket["runtime_weight"] += entry_weight
            bucket["entry_count"] += 1
            bucket["base_weight_weighted_sum"] += float(entry.get("base_weight", 1.0)) * entry_weight
            bucket["recent_gain_weighted_sum"] += float(entry.get("recent_gain", 1.0)) * entry_weight
            bucket["fatigue_weighted_sum"] += float(entry.get("fatigue", 0.0)) * entry_weight
            bucket["entries"].append(
                {
                    "entry": entry,
                    "entry_runtime_weight": entry_weight,
                }
            )
        return targets

    @staticmethod
    def _build_target_debug_entry(*, target: dict, total_weight: float, delta_ev: float) -> dict:
        runtime_weight = float(target.get("runtime_weight", 0.0))
        entry_count = int(target.get("entry_count", 0))
        safe_total_weight = max(1e-8, float(total_weight))
        safe_runtime_weight = max(1e-8, runtime_weight)
        return {
            "runtime_weight": round(runtime_weight, 8),
            "delta_ev": round(float(delta_ev), 8),
            "entry_count": entry_count,
            "normalized_share": round(runtime_weight / safe_total_weight, 8),
            "base_weight": round(float(target.get("base_weight_weighted_sum", 0.0)) / safe_runtime_weight, 8),
            "recent_gain": round(float(target.get("recent_gain_weighted_sum", 0.0)) / safe_runtime_weight, 8),
            "fatigue": round(float(target.get("fatigue_weighted_sum", 0.0)) / safe_runtime_weight, 8),
        }

    def _filter_full_inclusion_targets(
        self,
        *,
        source_structure_id: str,
        source_profile: dict,
        aggregated_targets: dict[str, dict],
        cut_engine,
    ) -> dict[str, dict]:
        eligible = {}
        source_signature = source_profile.get("content_signature", "")
        for target_key, payload in aggregated_targets.items():
            if str(payload.get("relation_type", "")) == "residual_context":
                if str(payload.get("owner_structure_id", "")) == str(source_structure_id):
                    eligible[target_key] = payload
                continue
            target_profile = payload.get("target_profile", {})
            if not target_profile:
                target_structure = payload.get("structure_obj")
                if not target_structure:
                    continue
                target_profile = cut_engine.build_sequence_profile_from_structure(target_structure)
            common_part = cut_engine.maximum_common_part(
                source_profile.get("sequence_groups", []),
                target_profile.get("sequence_groups", []),
            )
            if common_part.get("common_signature", "") != source_signature:
                continue
            if common_part.get("residual_existing_signature", ""):
                continue
            eligible[target_key] = payload
        return eligible

    def _mark_bucket_entries(
        self,
        *,
        entry_items: list[dict],
        total_weight: float,
        delta_er: float,
        delta_ev: float,
        now_ms: int,
    ) -> None:
        if total_weight <= 0.0:
            return
        for item in entry_items:
            entry = item.get("entry")
            entry_weight = float(item.get("entry_runtime_weight", 0.0))
            if not entry or entry_weight <= 0.0:
                continue
            ratio = entry_weight / total_weight
            self._weight.mark_entry_activation(
                entry,
                delta_er=round(delta_er * ratio, 8),
                delta_ev=round(delta_ev * ratio, 8),
                match_score=entry_weight,
                now_ms=now_ms,
            )

    @staticmethod
    def _append_target_delta(
        *,
        induction_targets: dict,
        projection_kind: str,
        memory_id: str,
        target_id: str,
        backing_structure_id: str,
        target_display_text: str,
        mode: str,
        source_structure_id: str,
        delta_ev: float,
        runtime_weight: float,
    ) -> None:
        stable_id = memory_id if projection_kind == "memory" and memory_id else target_id
        key = (projection_kind, stable_id, mode)
        payload = induction_targets.setdefault(
            key,
            {
                "projection_kind": projection_kind,
                "memory_id": memory_id,
                "target_structure_id": target_id,
                "backing_structure_id": backing_structure_id or target_id,
                "target_display_text": target_display_text,
                "delta_ev": 0.0,
                "sources": [],
                "mode": mode,
                "runtime_weight": runtime_weight,
            },
        )
        payload["delta_ev"] = round(float(payload.get("delta_ev", 0.0)) + float(delta_ev), 8)
        payload["sources"].append(source_structure_id)

    @staticmethod
    def _resolve_entry_profile(*, entry: dict, cut_engine, structure_store) -> dict:
        canonical_groups = list(entry.get("canonical_sequence_groups", []))
        if canonical_groups:
            return cut_engine.build_sequence_profile_from_groups(canonical_groups)
        display_text = str(entry.get("canonical_display_text", "") or entry.get("display_text", ""))
        sequence_groups = list(entry.get("sequence_groups", []))
        if not sequence_groups:
            return {}
        profile = cut_engine.build_sequence_profile_from_groups(sequence_groups)
        if display_text:
            profile["display_text"] = display_text
        return profile
