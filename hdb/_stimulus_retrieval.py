# -*- coding: utf-8 -*-
"""
Stimulus-level retrieval-storage for HDB.
"""

from __future__ import annotations

import math
import time
from collections import Counter

from ._id_generator import next_id
from ._profile_restore import restore_profile, restore_structure_profile
from ._sequence_display import (
    format_group_display,
    format_semantic_group_display,
    format_semantic_sequence_groups,
    format_sequence_groups,
)


class StimulusRetrievalEngine:
    def __init__(self, config: dict, weight_engine, logger, maintenance_engine):
        self._config = config
        self._weight = weight_engine
        self._logger = logger
        self._maintenance = maintenance_engine
        # Runtime-only caches scoped to a single `run()` invocation. This is reset
        # at the beginning of each run to avoid cross-tick semantic coupling.
        self._runtime_cache: dict | None = None

    def update_config(self, config: dict) -> None:
        self._config = config

    def run(
        self,
        *,
        stimulus_packet: dict,
        trace_id: str,
        tick_id: str,
        structure_store,
        pointer_index,
        cut_engine,
        episodic_store,
        enable_storage: bool,
        enable_new_structure_creation: bool,
        max_rounds: int,
    ) -> dict:
        self._runtime_cache = {
            "raw_residual_entry_profiles": {},
            "structure_profiles": {},
        }
        profile = cut_engine.build_sequence_profile_from_stimulus_packet(stimulus_packet)
        # 刺激级的“当前刺激组”必须以 cut engine 规范化后的整包分组为准，
        # 后续匹配、残差记忆、最大共同结构切割都在这个统一视图上进行。
        working_set = self._build_working_set(stimulus_packet, cut_engine=cut_engine)
        if not working_set["groups"]:
            episodic_memory_id = ""
            if enable_storage:
                episodic = episodic_store.append(
                    {
                        "event_summary": "stimulus-level retrieval-storage (empty packet)",
                        "structure_refs": [],
                        "origin": "stimulus_level_rs_empty",
                    },
                    trace_id=trace_id,
                    tick_id=tick_id,
                )
                episodic_memory_id = episodic.get("id", "")
            return {
                "code": "OK",
                "message": "no effective stimulus tokens",
                "round_count": 0,
                "matched_structure_ids": [],
                "new_structure_ids": [],
                "remaining_stimulus_sa_count": 0,
                "episodic_memory_id": episodic_memory_id,
                "storage_summary": {"written_index_count": 0, "cut_count": 0, "new_structure_count": 0},
                "runtime_projection_structures": [],
                "seeded_atomic_structure_ids": [],
                "fallback_used": False,
                "debug": {
                    "input_profile": {
                        "display_text": profile.get("display_text", ""),
                        "flat_tokens": list(profile.get("flat_tokens", [])),
                        "sequence_groups": list(profile.get("sequence_groups", [])),
                        "content_signature": profile.get("content_signature", ""),
                        "feature_units": [],
                    },
                    "round_details": [],
                },
            }

        matched_structure_ids: list[str] = []
        new_structure_ids: list[str] = []
        runtime_projection_structures: list[dict] = []
        written_index_count = 0
        cut_count = 0
        fallback_used = False
        round_count = 0
        debug_round_details: list[dict] = []
        seeded_atomic_structure_ids = self._ensure_atomic_structure_databases(
            working_groups=working_set["groups"],
            structure_store=structure_store,
            pointer_index=pointer_index,
            cut_engine=cut_engine,
            trace_id=trace_id,
            tick_id=tick_id,
            parent_ids=list(stimulus_packet.get("source", {}).get("parent_ids", [])),
            source_packet_id=stimulus_packet.get("id", ""),
        )
        episodic_memory_id = ""
        episodic_item = None
        if enable_storage:
            episodic_item = episodic_store.append(
                {
                    "event_summary": "stimulus-level retrieval-storage",
                    "structure_refs": [],
                    "origin": "stimulus_level_rs",
                    "origin_id": stimulus_packet.get("id", ""),
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
            episodic_memory_id = episodic_item.get("id", "")

        transfer_ratio = max(0.0, float(self._config.get("stimulus_match_transfer_ratio", 1.0)))
        residual_min_energy = max(0.0, float(self._config.get("stimulus_residual_min_energy", 0.05)))
        min_common_length = int(self._config.get("min_cut_common_length", 2))

        for round_index in range(1, max_rounds + 1):
            remaining_units = self._flatten_remaining_units(working_set["groups"])
            if not remaining_units:
                break

            remaining_total_er = round(sum(float(unit.get("er", 0.0)) for unit in remaining_units), 8)
            remaining_total_ev = round(sum(float(unit.get("ev", 0.0)) for unit in remaining_units), 8)
            if remaining_total_er + remaining_total_ev < residual_min_energy:
                break

            round_count = round_index
            anchor_unit = self._select_anchor_unit(remaining_units)
            if not anchor_unit:
                break

            focus_group = self._find_group(working_set["groups"], int(anchor_unit.get("group_index", 0)))
            if not focus_group:
                break

            groups_before = self._clone_working_groups(working_set["groups"])
            remaining_tokens_before = self._collect_remaining_tokens(groups_before)
            remaining_profile = cut_engine.build_sequence_profile_from_groups(groups_before)
            current_units_before = self._flatten_remaining_units(groups_before)
            focus_tokens_before = self._collect_remaining_tokens([focus_group])
            candidate_lookup, best, candidate_details = self._resolve_anchor_chain_match(
                anchor_unit=anchor_unit,
                focus_window_units=current_units_before,
                incoming_profile=remaining_profile,
                competition_units=remaining_units,
                structure_store=structure_store,
                pointer_index=pointer_index,
                cut_engine=cut_engine,
                trace_id=trace_id,
                tick_id=tick_id,
                source_packet_id=stimulus_packet.get("id", ""),
                parent_ids=list(stimulus_packet.get("source", {}).get("parent_ids", [])),
                round_index=round_index,
            )
            fallback_used = fallback_used or bool(candidate_lookup.get("used_recent_fallback"))

            created_common_structure = None
            created_residual_structure = None
            created_fresh_structure = None
            structure_stats_before = None
            structure_stats_after = None
            transferred_er = 0.0
            transferred_ev = 0.0
            covered_tokens: list[str] = []
            covered_range = [0, 0]
            transfer_similarity = 0.0

            if not best:
                debug_round_details.append(
                    self._build_round_debug(
                        round_index=round_index,
                        anchor_unit=anchor_unit,
                        focus_group=focus_group,
                        focus_tokens_before=focus_tokens_before,
                        remaining_tokens_before=remaining_tokens_before,
                        remaining_total_er=remaining_total_er,
                        remaining_total_ev=remaining_total_ev,
                        candidate_lookup=candidate_lookup,
                        candidate_details=candidate_details,
                        selected_match=None,
                        structure_stats_before=structure_stats_before,
                        structure_stats_after=structure_stats_after,
                        covered_range=covered_range,
                        covered_tokens=covered_tokens,
                        transfer_ratio=transfer_ratio,
                        transfer_similarity=transfer_similarity,
                        effective_transfer_fraction=0.0,
                        transferred_er=transferred_er,
                        transferred_ev=transferred_ev,
                        created_common_structure=created_common_structure,
                        created_residual_structure=created_residual_structure,
                        created_fresh_structure=created_fresh_structure,
                        groups_before=groups_before,
                        groups_after=working_set["groups"],
                    )
                )
                break

            structure_obj = structure_store.get(best["structure_id"])
            if not structure_obj:
                break

            common_part = best["common_part"]
            covered_units = self._collect_matched_units(groups_before, common_part)
            if not covered_units:
                break

            covered_tokens = [unit.get("token", "") for unit in covered_units if unit.get("token")]
            covered_range = list(common_part.get("incoming_range", [0, 0]))
            transfer_similarity = round(max(0.0, float(best.get("similarity_score", 0.0))), 8)
            effective_transfer_fraction = self._effective_transfer_fraction(transfer_ratio, transfer_similarity)
            transferred_er = round(sum(float(unit.get("er", 0.0)) for unit in covered_units) * effective_transfer_fraction, 8)
            transferred_ev = round(sum(float(unit.get("ev", 0.0)) for unit in covered_units) * effective_transfer_fraction, 8)

            structure_stats_before = self._capture_structure_stats(structure_obj)
            matched_structure_id = structure_obj.get("id", "")
            if matched_structure_id:
                matched_structure_ids.append(matched_structure_id)
            self._weight.mark_structure_match(
                structure_obj,
                match_score=best.get("match_score", 0.0),
                reality_support=transferred_er,
                virtual_support=transferred_ev,
                now_ms=int(time.time() * 1000),
            )
            structure_store.update_structure(structure_obj)
            self._mark_chain_entries(
                best=best,
                structure_store=structure_store,
                transferred_er=transferred_er,
                transferred_ev=transferred_ev,
            )
            structure_stats_after = self._capture_structure_stats(structure_obj)
            runtime_projection_structures.append(
                {
                    "structure_id": matched_structure_id,
                    "display_text": structure_obj.get("structure", {}).get("display_text", matched_structure_id),
                    "er": transferred_er,
                    "ev": transferred_ev,
                    "reason": "matched_structure",
                    "match_mode": best.get("match_mode", "candidate_match"),
                }
            )

            residual_store_result = None
            if enable_new_structure_creation:
                residual_store_result = self._store_residual_context_for_match(
                    owner_structure_id=matched_structure_id,
                    current_groups=groups_before,
                    current_profile=remaining_profile,
                    covered_units=covered_units,
                    matched_structure=structure_obj,
                    structure_store=structure_store,
                    pointer_index=pointer_index,
                    cut_engine=cut_engine,
                    trace_id=trace_id,
                    tick_id=tick_id,
                    source_packet_id=stimulus_packet.get("id", ""),
                    round_index=round_index,
                    min_common_length=min_common_length,
                    episodic_memory_id=episodic_memory_id,
                )
            if residual_store_result:
                written_index_count += int(residual_store_result.get("written_index_count", 0))
                cut_count += int(residual_store_result.get("cut_count", 0))
                # Propagate newly-created structures from residual normalization into the run summary.
                # 把“残差归一化/共同结构切割”过程中创建的新结构回填到 stimulus-level 的 new_structure_ids，
                # 否则前端/摘要会出现“新建结构=0，但逐轮日志里却出现新建共同结构”的误导。
                for sid in list(residual_store_result.get("new_structure_ids", []) or []):
                    sid = str(sid or "").strip()
                    if sid:
                        new_structure_ids.append(sid)
                if not created_common_structure and residual_store_result.get("common_structure"):
                    created_common_structure = residual_store_result.get("common_structure")
                if not created_residual_structure and residual_store_result.get("residual_structure"):
                    created_residual_structure = residual_store_result.get("residual_structure")

            self._apply_common_part_consumption(
                working_set["groups"],
                covered_units=covered_units,
                consume_fraction=effective_transfer_fraction,
                prune_threshold=max(1e-6, residual_min_energy * 0.02),
            )
            debug_round_details.append(
                self._build_round_debug(
                    round_index=round_index,
                    anchor_unit=anchor_unit,
                    focus_group=focus_group,
                    focus_tokens_before=focus_tokens_before,
                    remaining_tokens_before=remaining_tokens_before,
                    remaining_total_er=remaining_total_er,
                    remaining_total_ev=remaining_total_ev,
                    candidate_lookup=candidate_lookup,
                    candidate_details=candidate_details,
                    selected_match=best,
                    structure_stats_before=structure_stats_before,
                    structure_stats_after=structure_stats_after,
                    covered_range=covered_range,
                    covered_tokens=covered_tokens,
                    transfer_ratio=transfer_ratio,
                    transfer_similarity=transfer_similarity,
                    effective_transfer_fraction=effective_transfer_fraction,
                    transferred_er=transferred_er,
                    transferred_ev=transferred_ev,
                    created_common_structure=created_common_structure,
                    created_residual_structure=created_residual_structure,
                    created_fresh_structure=created_fresh_structure,
                    groups_before=groups_before,
                    groups_after=working_set["groups"],
                )
            )

        remaining_units = self._flatten_remaining_units(working_set["groups"])
        residual_packet = self._build_packet_from_working_groups(
            groups=working_set["groups"],
            trace_id=trace_id,
            tick_id=tick_id,
            source_packet_id=stimulus_packet.get("id", ""),
        )
        if enable_storage and episodic_item:
            meta = dict(episodic_item.get("meta", {}))
            ext = dict(meta.get("ext", {}))
            ext["display_text"] = format_sequence_groups(profile.get("sequence_groups", [])) or profile.get("display_text", "")
            ext["memory_material"] = self._build_stimulus_memory_material(profile=profile)
            meta["ext"] = ext
            episodic_item["meta"] = meta
            episodic_item["structure_refs"] = list(dict.fromkeys(matched_structure_ids + new_structure_ids))
            episodic_store.update(episodic_item)

        return {
            "code": "OK",
            "message": "Stimulus-level retrieval-storage completed",
            "round_count": round_count,
            "matched_structure_ids": list(dict.fromkeys(matched_structure_ids)),
            "new_structure_ids": list(dict.fromkeys(new_structure_ids)),
            "remaining_stimulus_sa_count": len(remaining_units),
            "episodic_memory_id": episodic_memory_id,
            "storage_summary": {
                "written_index_count": written_index_count,
                "cut_count": cut_count,
                "new_structure_count": len(list(dict.fromkeys(new_structure_ids))),
            },
            "residual_stimulus_packet": residual_packet,
            "runtime_projection_structures": runtime_projection_structures,
            "seeded_atomic_structure_ids": seeded_atomic_structure_ids,
            "fallback_used": fallback_used,
            "debug": {
                "input_profile": {
                    "display_text": profile.get("display_text", ""),
                    "flat_tokens": list(profile.get("flat_tokens", [])),
                    "sequence_groups": list(profile.get("sequence_groups", [])),
                    "content_signature": profile.get("content_signature", ""),
                    "feature_units": self._flatten_remaining_units(self._build_working_set(stimulus_packet, cut_engine=cut_engine)["groups"]),
                },
                "round_details": debug_round_details,
            },
        }

    def _ensure_atomic_structure_databases(
        self,
        *,
        working_groups: list[dict],
        structure_store,
        pointer_index,
        cut_engine,
        trace_id: str,
        tick_id: str,
        parent_ids: list[str],
        source_packet_id: str,
    ) -> list[str]:
        created_ids: list[str] = []
        seen_tokens: set[str] = set()
        for group in working_groups:
            for unit in sorted(group.get("units", []), key=lambda item: int(item.get("sequence_index", 0))):
                token = str(unit.get("token", ""))
                if not token or token in seen_tokens:
                    continue
                seen_tokens.add(token)
                result = self._find_or_create_structure_from_units(
                    units=[unit],
                    structure_store=structure_store,
                    pointer_index=pointer_index,
                    cut_engine=cut_engine,
                    trace_id=trace_id,
                    tick_id=tick_id,
                    confidence=float(self._config.get("stimulus_atomic_seed_confidence", 0.95)),
                    origin="stimulus_atomic_preseed",
                    origin_id=source_packet_id,
                    parent_ids=parent_ids,
                    ext={
                        "source_packet_id": source_packet_id,
                        "origin_group_index": unit.get("group_index", 0),
                        "origin_source_type": unit.get("source_type", ""),
                        "kind": "atomic_preseed",
                        "relation_type": "atomic_preseed",
                    },
                )
                if result.get("created") and result["structure"].get("id", ""):
                    created_ids.append(result["structure"]["id"])
        return created_ids

    def _build_working_set(self, stimulus_packet: dict, *, cut_engine) -> dict:
        sa_index = {
            item.get("id", ""): item
            for item in stimulus_packet.get("sa_items", [])
            if isinstance(item, dict) and item.get("id")
        }
        csa_index = {
            item.get("id", ""): item
            for item in stimulus_packet.get("csa_items", [])
            if isinstance(item, dict) and item.get("id")
        }

        groups = []
        for order_index, group in enumerate(stimulus_packet.get("grouped_sa_sequences", [])):
            source_type = group.get("source_type", "")
            origin_frame_id = group.get("origin_frame_id", "")
            source_group_index = int(group.get("source_group_index", group.get("group_index", order_index)))
            csa_members = [csa_index.get(csa_id) for csa_id in group.get("csa_ids", []) if csa_index.get(csa_id)]
            csa_members.sort(key=lambda item: item.get("ext", {}).get("packet_context", {}).get("sequence_index", 0))

            referenced_sa_ids = [str(sa_id) for sa_id in group.get("sa_ids", []) if str(sa_id)]
            for csa in csa_members:
                for member_id in csa.get("member_sa_ids", []):
                    member_text = str(member_id)
                    if member_text:
                        referenced_sa_ids.append(member_text)
            ordered_sa_ids = self._dedupe_preserve_order(referenced_sa_ids)
            sa_members = [sa_index.get(sa_id) for sa_id in ordered_sa_ids if sa_index.get(sa_id)]
            sa_members.sort(key=lambda item: item.get("ext", {}).get("packet_context", {}).get("sequence_index", 0))

            units = []
            for sa in sa_members:
                unit = self._make_unit_from_object(
                    obj=sa,
                    object_type="sa",
                    group_index=order_index,
                    source_group_index=source_group_index,
                    source_type=source_type,
                    origin_frame_id=origin_frame_id,
                )
                if unit:
                    units.append(unit)

            units_by_id = {str(unit.get("unit_id", "")): unit for unit in units if str(unit.get("unit_id", ""))}
            csa_bundles = []
            for csa in csa_members:
                anchor_unit_id = str(csa.get("anchor_sa_id", ""))
                member_ids = [
                    str(member_id)
                    for member_id in csa.get("member_sa_ids", [])
                    if str(member_id) in units_by_id
                ]
                if not anchor_unit_id or anchor_unit_id not in units_by_id or len(member_ids) < 2:
                    continue
                anchor_signature = str(units_by_id[anchor_unit_id].get("unit_signature", ""))
                member_signatures = [
                    str(units_by_id[member_id].get("unit_signature", ""))
                    for member_id in member_ids
                    if str(units_by_id[member_id].get("unit_signature", ""))
                ]
                csa_bundles.append(
                    {
                        "bundle_id": str(csa.get("id", "")),
                        "anchor_unit_id": anchor_unit_id,
                        "member_unit_ids": member_ids,
                        "anchor_unit_signature": anchor_signature,
                        "member_unit_signatures": member_signatures,
                        "bundle_signature": f"CSA[{anchor_signature}=>{'|'.join(sorted(member_signatures[1:]))}]",
                    }
                )

            if not units:
                continue

            groups.append(
                {
                    "group_index": order_index,
                    "source_group_index": source_group_index,
                    "source_type": source_type,
                    "origin_frame_id": origin_frame_id,
                    "units": units,
                    "csa_bundles": csa_bundles,
                }
            )

        # 统一交给 cut engine 做一次规范化，确保单位字段、CSA 绑定和时序分组
        # 在刺激级的所有后续流程里都保持一致。
        profile = cut_engine.build_sequence_profile_from_groups(groups)
        return {"groups": list(profile.get("sequence_groups", []))}

    def _make_unit_from_object(
        self,
        *,
        obj: dict,
        object_type: str,
        group_index: int,
        source_group_index: int,
        source_type: str,
        origin_frame_id: str,
    ) -> dict | None:
        token = str(obj.get("content", {}).get("display") or obj.get("content", {}).get("raw") or obj.get("id", ""))
        if not token:
            return None
        sequence_index = int(
            obj.get("ext", {}).get("packet_context", {}).get("sequence_index", obj.get("stimulus", {}).get("global_sequence_index", 0))
        )
        er = round(float(obj.get("energy", {}).get("er", 0.0)), 8)
        ev = round(float(obj.get("energy", {}).get("ev", 0.0)), 8)
        role = str(obj.get("stimulus", {}).get("role", "feature") or "feature")
        is_placeholder = token.startswith("SELF[")
        unit_signature_prefix = "P" if is_placeholder else ("A" if role == "attribute" else "F")
        attribute_name = str(obj.get("content", {}).get("attribute_name", ""))
        attribute_value = obj.get("content", {}).get("attribute_value")
        parent_ids = list((obj.get("source", {}) or {}).get("parent_ids", []))
        return {
            "unit_id": obj.get("id", ""),
            "object_type": object_type,
            "token": token,
            "display_text": token,
            "unit_role": "placeholder" if is_placeholder else role,
            "unit_signature": f"{unit_signature_prefix}:{token}",
            "sequence_index": sequence_index,
            "group_index": group_index,
            "source_group_index": source_group_index,
            "source_type": source_type,
            "origin_frame_id": origin_frame_id,
            "er": er,
            "ev": ev,
            "total_energy": round(er + ev, 8),
            "is_punctuation": self._is_punctuation_token(token),
            "display_visible": role != "attribute" or is_placeholder,
            "is_placeholder": is_placeholder,
            "attribute_name": attribute_name,
            "attribute_value": attribute_value,
            "bundle_id": "",
            "bundle_anchor_unit_id": str(parent_ids[0]) if role == "attribute" and parent_ids else "",
            "bundle_anchor_signature": "",
            "bundle_signature": "",
            "bundle_member_unit_ids": [],
            "bundle_member_signatures": [],
        }

    def _build_packet_from_working_groups(
        self,
        *,
        groups: list[dict],
        trace_id: str,
        tick_id: str,
        source_packet_id: str,
    ) -> dict:
        now_ms = int(time.time() * 1000)
        sa_items = []
        csa_items = []
        grouped_sequences = []
        total_er = 0.0
        total_ev = 0.0

        for group_index, group in enumerate(groups):
            units = sorted(group.get("units", []), key=lambda item: int(item.get("sequence_index", 0)))
            sa_ids = []
            group_csa_ids = []
            seen_bundle_ids = set()
            for seq_index, unit in enumerate(units):
                token = str(unit.get("token", ""))
                unit_id = str(unit.get("unit_id", ""))
                if not token or not unit_id:
                    continue
                er = round(max(0.0, float(unit.get("er", 0.0))), 8)
                ev = round(max(0.0, float(unit.get("ev", 0.0))), 8)
                if er + ev <= 0.0:
                    continue
                sa_ids.append(unit_id)
                total_er += er
                total_ev += ev
                sa_items.append(
                    {
                        "id": unit_id,
                        "object_type": "sa",
                        "content": {"raw": token, "display": token, "normalized": token, "value_type": "discrete"},
                        "stimulus": {"role": unit.get("unit_role", "feature"), "modality": unit.get("source_type", "text")},
                        "energy": {"er": er, "ev": ev},
                        "source": {
                            "module": "hdb",
                            "interface": "run_stimulus_level_retrieval_storage",
                            "origin": group.get("source_type", ""),
                            "origin_id": group.get("origin_frame_id", ""),
                            "parent_ids": [str(unit.get("bundle_anchor_unit_id", ""))] if str(unit.get("unit_role", "")) == "attribute" and str(unit.get("bundle_anchor_unit_id", "")) else [],
                        },
                        "ext": {
                            "packet_context": {
                                "group_index": group_index,
                                "source_group_index": int(group.get("source_group_index", group_index)),
                                "origin_frame_id": group.get("origin_frame_id", ""),
                                "source_type": group.get("source_type", ""),
                                "sequence_index": seq_index,
                            }
                        },
                        "created_at": now_ms,
                        "updated_at": now_ms,
                    }
                )
                bundle_id = str(unit.get("bundle_id", ""))
                member_ids = [str(member_id) for member_id in unit.get("bundle_member_unit_ids", []) if str(member_id)]
                if bundle_id and len(member_ids) >= 2 and bundle_id not in seen_bundle_ids:
                    seen_bundle_ids.add(bundle_id)
                    anchor_id = str(unit.get("bundle_anchor_unit_id", ""))
                    if anchor_id:
                        member_id_set = set(member_ids)
                        csa_items.append(
                            {
                                "id": bundle_id,
                                "object_type": "csa",
                                "anchor_sa_id": anchor_id,
                                "member_sa_ids": member_ids,
                                "content": {"display": f"CSA[{anchor_id}]", "raw": anchor_id},
                                "energy": {
                                    "er": round(sum(max(0.0, float(item.get("er", 0.0))) for item in units if str(item.get("unit_id", "")) in member_id_set), 8),
                                    "ev": round(sum(max(0.0, float(item.get("ev", 0.0))) for item in units if str(item.get("unit_id", "")) in member_id_set), 8),
                                },
                                "ext": {
                                    "packet_context": {
                                        "group_index": group_index,
                                        "source_group_index": int(group.get("source_group_index", group_index)),
                                        "origin_frame_id": group.get("origin_frame_id", ""),
                                        "source_type": group.get("source_type", ""),
                                        "sequence_index": len(group_csa_ids),
                                    }
                                },
                                "created_at": now_ms,
                                "updated_at": now_ms,
                            }
                        )
                        group_csa_ids.append(bundle_id)
            if not sa_ids:
                continue
            grouped_sequences.append(
                {
                    "group_index": group_index,
                    "source_type": group.get("source_type", ""),
                    "origin_frame_id": group.get("origin_frame_id", ""),
                    "sa_ids": sa_ids,
                    "csa_ids": group_csa_ids,
                    "source_group_index": int(group.get("source_group_index", group_index)),
                }
            )

        return {
            "id": f"spkt_residual_{int(time.time() * 1000)}",
            "object_type": "stimulus_packet",
            "sub_type": "stimulus_residual_packet",
            "packet_type": "residual_after_stimulus",
            "sa_items": sa_items,
            "csa_items": csa_items,
            "grouped_sa_sequences": grouped_sequences,
            "energy_summary": {
                "total_er": round(total_er, 8),
                "total_ev": round(total_ev, 8),
                "current_total_er": round(total_er, 8),
                "current_total_ev": round(total_ev, 8),
            },
            "trace_id": trace_id,
            "tick_id": tick_id,
            "source": {
                "module": "hdb",
                "interface": "run_stimulus_level_retrieval_storage",
                "origin": "residual_after_stimulus",
                "origin_id": source_packet_id,
                "parent_ids": [source_packet_id] if source_packet_id else [],
            },
            "created_at": now_ms,
            "updated_at": now_ms,
        }

    def _build_round_debug(
        self,
        *,
        round_index: int,
        anchor_unit: dict,
        focus_group: dict,
        focus_tokens_before: list[str],
        remaining_tokens_before: list[str],
        remaining_total_er: float,
        remaining_total_ev: float,
        candidate_lookup: dict,
        candidate_details: list[dict],
        selected_match: dict | None,
        structure_stats_before: dict | None,
        structure_stats_after: dict | None,
        covered_range: list[int],
        covered_tokens: list[str],
        transfer_ratio: float,
        transfer_similarity: float,
        effective_transfer_fraction: float,
        transferred_er: float,
        transferred_ev: float,
        created_common_structure: dict | None,
        created_residual_structure: dict | None,
        created_fresh_structure: dict | None,
        groups_before: list[dict],
        groups_after: list[dict],
    ) -> dict:
        remaining_groups_before = self._describe_runtime_groups(groups_before)
        remaining_groups_after = self._describe_runtime_groups(groups_after)
        remaining_units_after = self._flatten_remaining_units(groups_after)
        return {
            "round_index": round_index,
            "anchor_unit": anchor_unit,
            "focus_group_index": focus_group.get("group_index", 0),
            "focus_group_source_type": focus_group.get("source_type", ""),
            "focus_group_text_before": self._format_runtime_group_text(focus_group),
            "focus_group_sequence_groups_before": self._clone_working_groups([focus_group]),
            "remaining_tokens_before": remaining_tokens_before,
            "remaining_groups_before": remaining_groups_before,
            "remaining_grouped_text_before": self._format_runtime_group_texts(groups_before),
            "remaining_sequence_groups_before": self._clone_working_groups(groups_before),
            "remaining_total_er_before": remaining_total_er,
            "remaining_total_ev_before": remaining_total_ev,
            "candidate_lookup_source": candidate_lookup.get("candidate_source", "unknown"),
            "candidate_signature_hits": candidate_lookup.get("signature_hits", []),
            "chain_steps": list(candidate_lookup.get("chain_steps", [])),
            "candidate_details": candidate_details,
            "selected_match": selected_match,
            "structure_stats_before": structure_stats_before,
            "structure_stats_after": structure_stats_after,
            "covered_range": covered_range,
            "covered_tokens": covered_tokens,
            "transfer_ratio": transfer_ratio,
            "transfer_similarity": transfer_similarity,
            "effective_transfer_fraction": effective_transfer_fraction,
            "transferred_er": transferred_er,
            "transferred_ev": transferred_ev,
            "created_common_structure": created_common_structure,
            "created_residual_structure": created_residual_structure,
            "created_fresh_structure": created_fresh_structure,
            "remaining_tokens_after": self._collect_remaining_tokens(groups_after),
            "remaining_groups_after": remaining_groups_after,
            "remaining_grouped_text_after": self._format_runtime_group_texts(groups_after),
            "remaining_sequence_groups_after": self._clone_working_groups(groups_after),
            "remaining_total_er_after": round(sum(float(unit.get("er", 0.0)) for unit in remaining_units_after), 8),
            "remaining_total_ev_after": round(sum(float(unit.get("ev", 0.0)) for unit in remaining_units_after), 8),
        }

    def _resolve_anchor_chain_match(
        self,
        *,
        anchor_unit: dict,
        focus_window_units: list[dict],
        incoming_profile: dict,
        competition_units: list[dict] | None,
        structure_store,
        pointer_index,
        cut_engine,
        trace_id: str,
        tick_id: str,
        source_packet_id: str,
        parent_ids: list[str],
        round_index: int,
    ) -> tuple[dict, dict | None, list[dict]]:
        anchor_best, anchor_detail, _ = self._get_or_create_atomic_structure_for_unit(
            unit=anchor_unit,
            focus_units=focus_window_units,
            competition_units=competition_units,
            structure_store=structure_store,
            pointer_index=pointer_index,
            cut_engine=cut_engine,
            trace_id=trace_id,
            tick_id=tick_id,
            parent_ids=parent_ids,
            source_packet_id=source_packet_id,
            round_index=round_index,
        )
        candidate_details = []
        if anchor_detail:
            candidate_details = self._upsert_candidate_detail(candidate_details, anchor_detail)
        if not anchor_best:
            return {
                "candidate_source": "anchor_atomic_chain",
                "signature_hits": [{"signature": str(anchor_unit.get("token", "")), "candidate_count": 0}],
                "used_recent_fallback": False,
            }, None, candidate_details

        best = dict(anchor_best)
        best["path_entries"] = []
        chain_steps = []
        seen_ids = {best.get("structure_id", "")}
        max_chain_depth = max(1, int(incoming_profile.get("token_count", len(incoming_profile.get("flat_tokens", [])))))

        for depth in range(1, max_chain_depth + 1):
            local_lookup = self._collect_local_child_candidates(
                owner_match=best,
                structure_store=structure_store,
                seen_structure_ids=seen_ids,
            )
            chain_steps.append(
                {
                    "owner_structure_id": best.get("structure_id", ""),
                    "owner_display_text": best.get("display_text", ""),
                    "candidate_count": len(local_lookup.get("candidates", [])),
                }
            )
            if not local_lookup.get("candidates"):
                break
            local_best, local_details = self._best_structure_match(
                incoming_profile=incoming_profile,
                competition_units=competition_units,
                candidates=local_lookup.get("candidates", []),
                structure_store=structure_store,
                cut_engine=cut_engine,
                anchor_token=str(anchor_unit.get("token", "")),
                entry_lookup=local_lookup.get("entry_lookup", {}),
                min_existing_length=int(best.get("existing_length", 0)) + 1,
                chain_depth=depth,
                parent_match=best,
            )
            for detail in local_details:
                candidate_details = self._upsert_candidate_detail(candidate_details, detail)
            if not local_best:
                break
            best = local_best
            seen_ids.add(best.get("structure_id", ""))

        return {
            "candidate_source": "anchor_atomic_chain",
            "signature_hits": [
                {"signature": str(anchor_unit.get("token", "")), "candidate_count": 1 if best.get("structure_id", "") else 0},
                *[
                    {
                        "signature": f"local:{step.get('owner_display_text', '') or step.get('owner_structure_id', '')}",
                        "candidate_count": int(step.get("candidate_count", 0)),
                    }
                    for step in chain_steps
                ],
            ],
            "used_recent_fallback": False,
            "chain_steps": chain_steps,
        }, best, candidate_details

    def _collect_local_child_candidates(
        self,
        *,
        owner_match: dict,
        structure_store,
        seen_structure_ids: set[str] | None = None,
    ) -> dict:
        seen = set(seen_structure_ids or set())
        entry_lookup: dict[str, dict] = {}
        candidates: list[dict] = []
        structure_db = self._open_structure_db_from_match(owner_match, structure_store)
        if not structure_db:
            return {"candidates": candidates, "entry_lookup": entry_lookup}

        diff_table = sorted(
            structure_db.get("diff_table", []),
            key=lambda item: (
                -float(item.get("base_weight", 0.0)),
                -float(item.get("recent_gain", 1.0)),
                float(item.get("fatigue", 0.0)),
                str(item.get("entry_id", "")),
            ),
        )
        for entry in diff_table:
            if entry.get("entry_type", "structure_ref") != "structure_ref":
                continue
            if str(entry.get("ext", {}).get("relation_type", "")) == "residual_context":
                continue
            target_structure, target_db_id = self._resolve_diff_target(entry, structure_store)
            if not target_structure:
                continue
            target_id = target_structure.get("id", "")
            if not target_id or target_id in seen:
                continue
            seen.add(target_id)
            candidate = dict(target_structure)
            candidate.setdefault("_runtime_path", {})
            candidate["_runtime_path"] = {
                "entry_id": entry.get("entry_id", ""),
                "owner_structure_id": structure_db.get("owner_structure_id", ""),
                "owner_structure_db_id": structure_db.get("structure_db_id", ""),
                "target_db_id": target_db_id,
            }
            candidates.append(candidate)
            entry_lookup[target_id] = entry
        return {"candidates": candidates, "entry_lookup": entry_lookup, "structure_db_id": structure_db.get("structure_db_id", "")}

    def _open_structure_db_from_match(self, match: dict, structure_store) -> dict | None:
        structure_db_id = str(match.get("structure_db_id", ""))
        if structure_db_id:
            structure_db = structure_store.get_db(structure_db_id)
            if structure_db:
                return structure_db
        structure_id = str(match.get("structure_id", ""))
        if not structure_id:
            return None
        return structure_store.get_db_by_owner(structure_id)

    def _resolve_diff_target(self, entry: dict, structure_store) -> tuple[dict | None, str]:
        target_id = str(entry.get("target_id", ""))
        if not target_id:
            return None, ""
        target_structure = structure_store.get(target_id)
        target_db_id = str(entry.get("target_db_id", ""))
        if not target_db_id and target_structure:
            target_db_id = str(target_structure.get("db_pointer", {}).get("structure_db_id", ""))
            if target_db_id:
                entry["target_db_id"] = target_db_id
        return target_structure, target_db_id

    def _mark_chain_entries(
        self,
        *,
        best: dict,
        structure_store,
        transferred_er: float,
        transferred_ev: float,
    ) -> None:
        for path_entry in best.get("path_entries", []):
            owner_structure_id = str(path_entry.get("owner_structure_id", ""))
            entry_id = str(path_entry.get("entry_id", ""))
            if not owner_structure_id or not entry_id:
                continue
            structure_db = structure_store.get_db_by_owner(owner_structure_id)
            if not structure_db:
                continue
            updated = False
            for entry in structure_db.get("diff_table", []):
                if str(entry.get("entry_id", "")) != entry_id:
                    continue
                self._weight.mark_entry_activation(
                    entry,
                    delta_er=transferred_er,
                    delta_ev=transferred_ev,
                    match_score=float(best.get("match_score", 0.0)),
                    now_ms=int(time.time() * 1000),
                )
                updated = True
                break
            if updated:
                structure_store.update_db(structure_db)

    def _collect_candidates(
        self,
        *,
        signatures: list[str],
        structure_store,
        pointer_index,
        exclude_structure_ids: list[str] | None = None,
    ) -> dict:
        candidates = []
        signature_hits = []
        seen_ids = set(exclude_structure_ids or [])

        def append_candidate(candidate_id: str) -> None:
            if not candidate_id or candidate_id in seen_ids:
                return
            candidate = structure_store.get(candidate_id)
            if not candidate:
                return
            seen_ids.add(candidate_id)
            candidates.append(candidate)

        def append_children(owner_structure_id: str) -> None:
            structure_db = structure_store.get_db_by_owner(owner_structure_id)
            if not structure_db:
                return
            for entry in sorted(structure_db.get("diff_table", []), key=lambda item: float(item.get("base_weight", 0.0)), reverse=True):
                append_candidate(entry.get("target_id", ""))

        unique_signatures = []
        for signature in signatures:
            text = str(signature or "")
            if text and text not in unique_signatures:
                unique_signatures.append(text)

        for signature in unique_signatures:
            candidate_ids = pointer_index.query_candidates_by_signature(signature)
            signature_hits.append({"signature": signature, "candidate_count": len(candidate_ids)})
            direct_ids = []
            for candidate_id in candidate_ids:
                if candidate_id in seen_ids:
                    continue
                append_candidate(candidate_id)
                direct_ids.append(candidate_id)
            for candidate_id in direct_ids:
                append_children(candidate_id)

        used_recent_fallback = False
        candidate_source = "signature_index" if candidates else "recent_structure_fallback"
        if not candidates:
            used_recent_fallback = True
            for candidate in structure_store.get_recent_structures(
                limit=int(self._config.get("fallback_lookup_max_candidates", 32))
            ):
                candidate_id = candidate.get("id", "")
                if candidate_id and candidate_id not in seen_ids:
                    seen_ids.add(candidate_id)
                    candidates.append(candidate)

        return {
            "candidates": candidates[: int(self._config.get("fallback_lookup_max_candidates", 32))],
            "used_recent_fallback": used_recent_fallback,
            "candidate_source": candidate_source,
            "signature_hits": signature_hits,
        }

    def _best_structure_match(
        self,
        *,
        incoming_profile: dict,
        competition_units: list[dict] | None,
        candidates: list[dict],
        structure_store,
        cut_engine,
        anchor_token: str,
        entry_lookup: dict[str, dict] | None = None,
        min_existing_length: int = 1,
        chain_depth: int = 0,
        parent_match: dict | None = None,
    ) -> tuple[dict | None, list[dict]]:
        best = None
        candidate_details = []
        now_ms = int(time.time() * 1000)
        incoming_groups = list(incoming_profile.get("sequence_groups", []))
        incoming_length = int(incoming_profile.get("unit_count", incoming_profile.get("token_count", len(incoming_profile.get("flat_tokens", [])))))
        entry_lookup = entry_lookup or {}
        incoming_all_units = [
            dict(unit)
            for group in incoming_groups
            for unit in group.get("units", [])
        ]
        competition_all_units = [
            dict(unit)
            for unit in (competition_units or incoming_all_units)
            if isinstance(unit, dict)
        ]
        competition_length = len([unit for unit in competition_all_units if str(unit.get("token", ""))])

        for candidate in candidates:
            self._weight.decay_structure(candidate, now_ms=now_ms, round_step=1)
            entry = entry_lookup.get(candidate.get("id", ""))
            if entry is not None:
                self._weight.decay_entry(entry, now_ms=now_ms, round_step=1)
            existing_profile = self._build_structure_profile(
                structure_obj=candidate,
                structure_store=structure_store,
                cut_engine=cut_engine,
            )
            existing_groups = list(existing_profile.get("sequence_groups", []))
            if not existing_groups:
                continue

            existing_length = int(existing_profile.get("unit_count", existing_profile.get("token_count", len(existing_profile.get("flat_tokens", [])))))
            candidate_runtime_weight = self._weight.compute_runtime_weight(
                base_weight=float(candidate.get("stats", {}).get("base_weight", 1.0)),
                recent_gain=float(candidate.get("stats", {}).get("recent_gain", 1.0)),
                fatigue=float(candidate.get("stats", {}).get("fatigue", 0.0)),
            )
            entry_runtime_weight = self._weight.entry_runtime_weight(entry) if entry is not None else 1.0
            if existing_length > incoming_length or existing_length < max(1, int(min_existing_length)):
                candidate_details.append(
                    {
                        "structure_id": candidate.get("id", ""),
                        "display_text": candidate.get("structure", {}).get("display_text", candidate.get("id", "")),
                        "grouped_display_text": "",
                        "sequence_groups": list(existing_profile.get("sequence_groups", [])),
                        "runtime_weight": round(float(candidate_runtime_weight), 8),
                        "entry_runtime_weight": round(float(entry_runtime_weight), 8),
                        "match_score": 0.0,
                        "competition_score": 0.0,
                        "weighted_rank_score": 0.0,
                        "similarity_score": 0.0,
                        "exact_match": False,
                        "full_structure_included": False,
                        "coverage_ratio": 0.0,
                        "structure_match_ratio": 0.0,
                        "stimulus_match_ratio": 0.0,
                        "existing_length": existing_length,
                        "incoming_length": incoming_length,
                        "matched_existing_length": 0,
                        "matched_incoming_length": 0,
                        "contains_anchor": True,
                        "eligible": False,
                        "common_part": {"common_length": 0, "common_tokens": []},
                        "match_mode": "candidate_match",
                        "chain_depth": int(chain_depth),
                        "entry_id": entry.get("entry_id", "") if entry else "",
                        "owner_structure_id": str(candidate.get("_runtime_path", {}).get("owner_structure_id", "")),
                        "parent_structure_id": parent_match.get("structure_id", "") if parent_match else "",
                        "structure_db_id": str(candidate.get("_runtime_path", {}).get("target_db_id", ""))
                        or str(candidate.get("db_pointer", {}).get("structure_db_id", "")),
                        "structure_signature": existing_profile.get("content_signature", ""),
                        "stats": self._capture_structure_stats(candidate),
                    }
                )
                continue

            common_part = cut_engine.maximum_common_part(existing_groups, incoming_groups)
            common_length = int(common_part.get("common_length", 0))
            contains_anchor = anchor_token in list(common_part.get("common_tokens", [])) if anchor_token else True
            matched_incoming_units = self._collect_matched_units(incoming_groups, common_part)
            matched_existing_units = self._collect_matched_units(
                existing_groups,
                common_part,
                use_existing_side=True,
            )
            stimulus_match_ratio = self._energy_match_ratio(
                matched_units=matched_incoming_units,
                all_units=competition_all_units,
                fallback_numerator=common_length,
                fallback_denominator=max(1, competition_length),
            )
            structure_match_ratio = self._energy_match_ratio(
                matched_units=matched_existing_units,
                all_units=[
                    dict(unit)
                    for group in existing_groups
                    for unit in group.get("units", [])
                ],
                fallback_numerator=common_length,
                fallback_denominator=max(1, existing_length),
            )
            matched_existing_length = int(common_part.get("matched_existing_unit_count", 0))
            matched_incoming_length = int(common_part.get("matched_incoming_unit_count", 0))
            exact_match = (
                common_length > 0
                and not common_part.get("residual_existing_signature", "")
                and not common_part.get("residual_incoming_signature", "")
                and matched_existing_length >= existing_length
                and matched_incoming_length >= incoming_length
                # CSA 门控：要求双方 bundle 约束也完全满足，避免“跨对象拼接”造成假阳性。
                and bool(common_part.get("bundle_constraints_ok_exact", True))
            )
            full_structure_included = bool(
                common_length > 0
                and not common_part.get("residual_existing_signature", "")
                and matched_existing_length >= existing_length
                # CSA 门控：结构侧（existing）包含的 CSA 必须被刺激侧（incoming）某一个 bundle 完全覆盖。
                and bool(common_part.get("bundle_constraints_ok_existing_included", True))
            )
            match_score = self._compose_match_score(
                stimulus_match_ratio=stimulus_match_ratio,
                structure_match_ratio=structure_match_ratio,
            )
            similarity_score = match_score if common_length > 0 else 0.0
            eligible = bool(
                full_structure_included
                and contains_anchor
                and existing_length >= max(1, int(min_existing_length))
            )
            competition_score = round(float(match_score if eligible else 0.0), 8)

            detail = {
                "structure_id": candidate.get("id", ""),
                "display_text": candidate.get("structure", {}).get("display_text", candidate.get("id", "")),
                "grouped_display_text": self._format_runtime_group_texts(existing_groups),
                "sequence_groups": list(existing_profile.get("sequence_groups", [])),
                "runtime_weight": round(float(candidate_runtime_weight), 8),
                "entry_runtime_weight": round(float(entry_runtime_weight), 8),
                "match_score": round(float(match_score), 8),
                "competition_score": competition_score,
                "weighted_rank_score": competition_score,
                "similarity_score": round(float(similarity_score), 8),
                "exact_match": exact_match,
                "full_structure_included": full_structure_included,
                "coverage_ratio": round(float(stimulus_match_ratio), 8),
                "structure_match_ratio": round(float(structure_match_ratio), 8),
                "stimulus_match_ratio": round(float(stimulus_match_ratio), 8),
                "existing_length": existing_length,
                "incoming_length": incoming_length,
                "matched_existing_length": matched_existing_length,
                "matched_incoming_length": matched_incoming_length,
                "contains_anchor": contains_anchor,
                "eligible": eligible,
                "common_part": common_part,
                "match_mode": "candidate_match",
                "chain_depth": int(chain_depth),
                "entry_id": entry.get("entry_id", "") if entry else "",
                "owner_structure_id": str(candidate.get("_runtime_path", {}).get("owner_structure_id", "")),
                "parent_structure_id": parent_match.get("structure_id", "") if parent_match else "",
                "structure_db_id": str(candidate.get("_runtime_path", {}).get("target_db_id", "")) or str(candidate.get("db_pointer", {}).get("structure_db_id", "")),
                "structure_signature": existing_profile.get("content_signature", ""),
                "stats": self._capture_structure_stats(candidate),
            }
            candidate_details.append(detail)

            if not detail["eligible"]:
                continue

            if self._is_better_structure_match(detail, best):
                best = {
                    "structure_id": candidate.get("id", ""),
                    "display_text": candidate.get("structure", {}).get("display_text", candidate.get("id", "")),
                    "grouped_display_text": self._format_runtime_group_texts(existing_groups),
                    "sequence_groups": list(existing_profile.get("sequence_groups", [])),
                    "exact_match": exact_match,
                    "full_structure_included": full_structure_included,
                    "coverage_ratio": stimulus_match_ratio,
                    "match_score": round(float(match_score), 8),
                    "competition_score": round(float(match_score), 8),
                    "weighted_rank_score": competition_score,
                    "similarity_score": round(float(similarity_score), 8),
                    "structure_match_ratio": round(float(structure_match_ratio), 8),
                    "stimulus_match_ratio": round(float(stimulus_match_ratio), 8),
                    "existing_length": existing_length,
                    "matched_existing_length": matched_existing_length,
                    "matched_incoming_length": matched_incoming_length,
                    "runtime_weight": candidate_runtime_weight,
                    "entry_runtime_weight": entry_runtime_weight,
                    "common_part": common_part,
                    "incoming_range": list(common_part.get("incoming_range", [0, 0])),
                    "match_mode": "candidate_match",
                    "structure_signature": existing_profile.get("content_signature", ""),
                    "structure_db_id": str(candidate.get("_runtime_path", {}).get("target_db_id", "")) or str(candidate.get("db_pointer", {}).get("structure_db_id", "")),
                    "path_entries": list(parent_match.get("path_entries", [])) if parent_match else [],
                }
                if entry is not None:
                    best["path_entries"].append(
                        {
                            "entry_id": entry.get("entry_id", ""),
                            "owner_structure_id": str(candidate.get("_runtime_path", {}).get("owner_structure_id", "")),
                            "owner_structure_db_id": str(candidate.get("_runtime_path", {}).get("owner_structure_db_id", "")),
                            "target_structure_id": candidate.get("id", ""),
                            "target_db_id": str(candidate.get("_runtime_path", {}).get("target_db_id", "")),
                        }
                    )

        candidate_details.sort(
            key=lambda item: (
                0 if item.get("eligible") else 1,
                -float(item.get("competition_score", 0.0)),
                -int(item.get("existing_length", 0)),
                -float(item.get("entry_runtime_weight", 0.0)),
                -float(item.get("runtime_weight", 0.0)),
            )
        )
        return best, candidate_details

    def _get_or_create_focus_window_fallback(
        self,
        *,
        anchor_unit: dict,
        focus_units: list[dict],
        incoming_profile: dict,
        competition_units: list[dict] | None,
        structure_store,
        pointer_index,
        cut_engine,
        trace_id: str,
        tick_id: str,
        parent_ids: list[str],
        source_packet_id: str,
        round_index: int,
    ) -> tuple[dict | None, dict | None, dict | None]:
        if not focus_units:
            return None, None, None
        focus_profile = cut_engine.make_structure_payload_from_units(
            focus_units,
            confidence=float(self._config.get("stimulus_focus_seed_confidence", 0.9)),
        )
        focus_signature = focus_profile.get("content_signature", "")
        existing = self._find_exact_structure_by_signature(
            signature=focus_signature,
            structure_store=structure_store,
            pointer_index=pointer_index,
            cut_engine=cut_engine,
            expected_tokens=list(focus_profile.get("flat_tokens", [])),
            expected_sequence_groups=list(focus_profile.get("sequence_groups", [])),
        )
        created_structure = None
        if existing is None:
            kind = "atomic_anchor" if len(focus_units) == 1 else "focus_window_seed"
            result = self._find_or_create_structure_from_units(
                units=focus_units,
                structure_store=structure_store,
                pointer_index=pointer_index,
                cut_engine=cut_engine,
                trace_id=trace_id,
                tick_id=tick_id,
                confidence=float(self._config.get("stimulus_focus_seed_confidence", 0.9)),
                origin="stimulus_focus_window_seed",
                origin_id=source_packet_id,
                parent_ids=parent_ids,
                ext={
                    "origin_round": round_index,
                    "source_packet_id": source_packet_id,
                    "origin_group_index": anchor_unit.get("group_index", 0),
                    "origin_source_type": anchor_unit.get("source_type", ""),
                    "kind": kind,
                    "relation_type": kind,
                },
            )
            existing = result["structure"]
            created_structure = existing if result.get("created") else None
        if existing is None:
            return None, None, created_structure

        existing_profile = self._build_structure_profile(
            structure_obj=existing,
            structure_store=structure_store,
            cut_engine=cut_engine,
        )
        common_part = cut_engine.maximum_common_part(existing_profile.get("sequence_groups", []), incoming_profile.get("sequence_groups", []))
        common_length = int(common_part.get("common_length", 0))
        incoming_length = max(1, int(incoming_profile.get("token_count", len(incoming_profile.get("flat_tokens", [])))))
        existing_length = max(1, int(existing_profile.get("token_count", len(existing_profile.get("flat_tokens", [])))))
        focus_groups = self._units_to_groups(focus_units)
        matched_incoming_units = self._collect_matched_units(focus_groups, common_part)
        matched_existing_units = self._collect_matched_units(
            existing_profile.get("sequence_groups", []),
            common_part,
            use_existing_side=True,
        )
        stimulus_match_ratio = self._energy_match_ratio(
            matched_units=matched_incoming_units,
            all_units=[
                dict(unit)
                for unit in (competition_units or focus_units)
                if isinstance(unit, dict)
            ],
            fallback_numerator=common_length,
            fallback_denominator=max(
                1,
                len([unit for unit in (competition_units or focus_units) if str(unit.get("token", ""))]),
            ),
        )
        structure_match_ratio = self._energy_match_ratio(
            matched_units=matched_existing_units,
            all_units=[
                dict(unit)
                for group in existing_profile.get("sequence_groups", [])
                for unit in group.get("units", [])
            ],
            fallback_numerator=common_length,
            fallback_denominator=existing_length,
        )
        match_score = self._compose_match_score(
            stimulus_match_ratio=stimulus_match_ratio,
            structure_match_ratio=structure_match_ratio,
        )
        matched_existing_length = int(common_part.get("matched_existing_unit_count", 0))
        matched_incoming_length = int(common_part.get("matched_incoming_unit_count", 0))
        full_structure_included = bool(
            common_length > 0
            and not common_part.get("residual_existing_signature", "")
            and matched_existing_length >= existing_length
            and bool(common_part.get("bundle_constraints_ok_existing_included", True))
        )
        exact_match = bool(
            full_structure_included
            and not common_part.get("residual_incoming_signature", "")
            and matched_incoming_length >= incoming_length
            and bool(common_part.get("bundle_constraints_ok_exact", True))
        )
        fallback_best = {
            "structure_id": existing.get("id", ""),
            "display_text": existing.get("structure", {}).get("display_text", existing.get("id", "")),
            "grouped_display_text": self._format_runtime_group_texts(existing_profile.get("sequence_groups", [])),
            "sequence_groups": list(existing_profile.get("sequence_groups", [])),
            "exact_match": exact_match,
            "full_structure_included": full_structure_included,
            "coverage_ratio": stimulus_match_ratio,
            "match_score": match_score,
            "competition_score": match_score,
            "weighted_rank_score": match_score,
            "similarity_score": match_score,
            "structure_match_ratio": structure_match_ratio,
            "stimulus_match_ratio": stimulus_match_ratio,
            "existing_length": existing_length,
            "runtime_weight": 1.0,
            "common_part": common_part,
            "incoming_range": list(common_part.get("incoming_range", [0, 0])),
            "match_mode": "focus_window_fallback",
            "structure_signature": existing_profile.get("content_signature", focus_signature),
            "structure_db_id": str(existing.get("db_pointer", {}).get("structure_db_id", "")),
            "path_entries": [],
        }
        fallback_detail = {
            **fallback_best,
            "contains_anchor": True,
            "eligible": True,
            "existing_length": existing_length,
            "incoming_length": incoming_length,
            "stats": self._capture_structure_stats(existing),
        }
        return fallback_best, fallback_detail, created_structure

    @staticmethod
    def _is_numeric_attribute_unit(unit: dict) -> bool:
        if str(unit.get("unit_role", "")) != "attribute":
            return False
        if not str(unit.get("attribute_name", "")):
            return False
        value = unit.get("attribute_value")
        if isinstance(value, bool):
            return False
        if isinstance(value, (int, float)):
            return True
        try:
            return str(value or "").strip() != "" and float(str(value).strip()) == float(str(value).strip())
        except Exception:
            return False

    def _find_numeric_atomic_structure_candidate(
        self,
        *,
        unit: dict,
        structure_store,
        pointer_index,
        cut_engine,
    ) -> tuple[dict | None, dict | None]:
        if not self._is_numeric_attribute_unit(unit):
            return None, None
        family = str(unit.get("attribute_name", ""))
        value = unit.get("attribute_value")
        buckets = pointer_index.resolve_numeric_buckets(
            attribute_name=family,
            value=value,
            create_if_missing=True,
            neighbor_count=2,
        )
        candidate_ids: list[str] = []
        seen_ids: set[str] = set()
        for bucket in buckets:
            for candidate_id in bucket.get("candidate_ids", []):
                candidate_text = str(candidate_id)
                if not candidate_text or candidate_text in seen_ids:
                    continue
                seen_ids.add(candidate_text)
                candidate_ids.append(candidate_text)
        best = None
        best_match = None
        best_key = None
        for candidate_id in candidate_ids:
            candidate = structure_store.get(candidate_id)
            if not candidate:
                continue
            candidate_profile = self._build_structure_profile(
                structure_obj=candidate,
                structure_store=structure_store,
                cut_engine=cut_engine,
            )
            candidate_units = [
                dict(item)
                for group in candidate_profile.get("sequence_groups", [])
                for item in group.get("units", [])
                if isinstance(item, dict)
            ]
            if len(candidate_units) != 1:
                continue
            candidate_unit = candidate_units[0]
            if not self._is_numeric_attribute_unit(candidate_unit):
                continue
            if str(candidate_unit.get("attribute_name", "")) != family:
                continue
            numeric_match = pointer_index.describe_numeric_match(
                attribute_name=family,
                left_value=candidate_unit.get("attribute_value"),
                right_value=value,
            )
            if not numeric_match:
                continue
            candidate_key = (
                float(numeric_match.get("similarity", 0.0)),
                -float(numeric_match.get("distance", 0.0)),
                -float(candidate.get("stats", {}).get("base_weight", 1.0)),
            )
            if best_key is None or candidate_key > best_key:
                best_key = candidate_key
                best = candidate
                best_match = numeric_match
        return best, best_match

    def _get_or_create_atomic_structure_for_unit(
        self,
        *,
        unit: dict,
        focus_units: list[dict],
        competition_units: list[dict] | None,
        structure_store,
        pointer_index,
        cut_engine,
        trace_id: str,
        tick_id: str,
        parent_ids: list[str],
        source_packet_id: str,
        round_index: int,
    ) -> tuple[dict | None, dict | None, dict | None]:
        token = str(unit.get("token", ""))
        if not token:
            return None, None, None
        numeric_anchor_match = None
        existing = self._find_exact_structure_by_signature(
            signature=token,
            structure_store=structure_store,
            pointer_index=pointer_index,
            cut_engine=cut_engine,
            expected_tokens=[token],
            expected_sequence_groups=[{"group_index": 0, "tokens": [token], "source_type": unit.get("source_type", "")}],
        )
        if existing is None:
            existing, numeric_anchor_match = self._find_numeric_atomic_structure_candidate(
                unit=unit,
                structure_store=structure_store,
                pointer_index=pointer_index,
                cut_engine=cut_engine,
            )
        created_structure = None
        if existing is None:
            result = self._find_or_create_structure_from_units(
                units=[unit],
                structure_store=structure_store,
                pointer_index=pointer_index,
                cut_engine=cut_engine,
                trace_id=trace_id,
                tick_id=tick_id,
                confidence=float(self._config.get("stimulus_anchor_seed_confidence", 0.9)),
                origin="stimulus_atomic_anchor_seed",
                origin_id=source_packet_id,
                parent_ids=parent_ids,
                ext={
                    "origin_round": round_index,
                    "source_packet_id": source_packet_id,
                    "origin_group_index": unit.get("group_index", 0),
                    "origin_source_type": unit.get("source_type", ""),
                    "kind": "atomic_anchor",
                    "relation_type": "atomic_anchor",
                },
            )
            existing = result["structure"]
            created_structure = existing if result.get("created") else None
        if existing is None:
            return None, None, created_structure

        focus_groups = self._units_to_groups(focus_units)
        group_pos = self._find_group_position(focus_groups, int(unit.get("group_index", 0)))
        residual_units = self._subtract_units(focus_units, [unit])
        existing_profile = self._build_structure_profile(
            structure_obj=existing,
            structure_store=structure_store,
            cut_engine=cut_engine,
        )
        existing_atomic_units = [
            dict(item)
            for group in existing_profile.get("sequence_groups", [])
            for item in group.get("units", [])
            if isinstance(item, dict)
        ]
        common_unit = dict(unit)
        match_mode = "anchor_atomic_fallback"
        if numeric_anchor_match and existing_atomic_units:
            common_unit = cut_engine._generalize_numeric_common_unit(
                existing_unit=existing_atomic_units[0],
                incoming_unit=unit,
                numeric_match=numeric_anchor_match,
            )
            match_mode = "anchor_numeric_bucket"
        common_profile = cut_engine.build_sequence_profile_from_groups(
            [
                {
                    "group_index": 0,
                    "source_type": unit.get("source_type", ""),
                    "origin_frame_id": unit.get("origin_frame_id", ""),
                    "source_group_index": group_pos,
                    "units": [common_unit],
                }
            ]
        )
        common_groups = list(common_profile.get("sequence_groups", []))
        common_group = common_groups[0] if common_groups else {
            "group_index": 0,
            "source_type": unit.get("source_type", ""),
            "origin_frame_id": unit.get("origin_frame_id", ""),
            "tokens": [common_unit.get("token", token)],
            "units": [common_unit],
        }
        common_part = {
            "common_tokens": list(common_group.get("tokens", [])) or [common_unit.get("token", token)],
            "common_length": 1,
            "common_group_count": 1,
            "matched_existing_unit_count": 1,
            "matched_incoming_unit_count": 1,
            "common_signature": common_profile.get("content_signature", "") or token,
            "common_display": common_profile.get("display_text", "") or token,
            "common_groups": [common_group],
            "matched_pairs": [
                {
                    "existing_group_index": 0,
                    "incoming_group_index": group_pos,
                    "common_tokens": list(common_group.get("tokens", [])) or [common_unit.get("token", token)],
                    "existing_unit_refs": [str(existing_atomic_units[0].get("unit_id", ""))] if existing_atomic_units else [],
                    "incoming_unit_refs": [str(unit.get("unit_id", ""))] if str(unit.get("unit_id", "")) else [],
                }
            ],
            "existing_range": [0, 1],
            "incoming_range": [group_pos, group_pos + 1],
            "matched_existing_group_indices": [0],
            "matched_incoming_group_indices": [group_pos],
            "residual_existing_groups": [],
            "residual_incoming_groups": self._units_to_groups(residual_units),
            "residual_existing_tokens": [],
            "residual_incoming_tokens": [item.get("token", "") for item in residual_units if item.get("token")],
            "residual_existing_signature": "",
            "residual_incoming_signature": cut_engine.sequence_groups_to_signature(self._units_to_groups(residual_units)),
        }
        numeric_similarity = float(numeric_anchor_match.get("similarity", 1.0)) if numeric_anchor_match else 1.0
        stimulus_match_ratio = self._energy_match_ratio(
            matched_units=[{**dict(unit), "match_similarity": numeric_similarity}],
            all_units=[
                dict(item)
                for item in (competition_units or focus_units)
                if isinstance(item, dict)
            ],
            fallback_numerator=1.0,
            fallback_denominator=max(
                1,
                len([item for item in (competition_units or focus_units) if str(item.get("token", ""))]),
            ),
        )
        match_score = self._compose_match_score(
            stimulus_match_ratio=stimulus_match_ratio,
            structure_match_ratio=numeric_similarity,
        )
        exact_match = numeric_similarity >= 0.99999999
        fallback_best = {
            "structure_id": existing.get("id", ""),
            "display_text": existing.get("structure", {}).get("display_text", existing.get("id", "")),
            "grouped_display_text": self._format_runtime_group_texts(existing_profile.get("sequence_groups", [])),
            "exact_match": exact_match,
            "full_structure_included": True,
            "coverage_ratio": stimulus_match_ratio,
            "match_score": match_score,
            "competition_score": match_score,
            "weighted_rank_score": match_score,
            "similarity_score": match_score,
            "structure_match_ratio": 1.0,
            "stimulus_match_ratio": stimulus_match_ratio,
            "existing_length": 1,
            "runtime_weight": 1.0,
            "common_part": common_part,
            "incoming_range": list(common_part.get("incoming_range", [0, 0])),
            "match_mode": match_mode,
            "structure_signature": existing_profile.get("content_signature", token),
            "structure_db_id": str(existing.get("db_pointer", {}).get("structure_db_id", "")),
            "path_entries": [],
        }
        fallback_detail = {
            **fallback_best,
            "contains_anchor": True,
            "eligible": True,
            "existing_length": 1,
            "incoming_length": len([item for item in focus_units if item.get("token")]),
            "stats": self._capture_structure_stats(existing),
        }
        return fallback_best, fallback_detail, created_structure

    def _find_or_create_extension_structure(
        self,
        *,
        full_units: list[dict],
        matched_structure_id: str,
        common_part: dict,
        structure_store,
        pointer_index,
        cut_engine,
        trace_id: str,
        tick_id: str,
        source_packet_id: str,
        round_index: int,
    ) -> dict | None:
        if not full_units:
            return None
        result = self._find_or_create_structure_from_units(
            units=full_units,
            structure_store=structure_store,
            pointer_index=pointer_index,
            cut_engine=cut_engine,
            trace_id=trace_id,
            tick_id=tick_id,
            confidence=float(self._config.get("stimulus_extension_confidence", 0.74)),
            origin="stimulus_extension_create",
            origin_id=source_packet_id,
            parent_ids=[matched_structure_id],
            ext={
                "origin_round": round_index,
                "source_packet_id": source_packet_id,
                "kind": "incoming_extension",
                "relation_type": "incoming_extension",
                "parent_structure_id": matched_structure_id,
                "common_signature": common_part.get("common_signature", ""),
                "residual_incoming_signature": common_part.get("residual_incoming_signature", ""),
            },
        )
        structure_obj = result["structure"]
        structure_id = structure_obj.get("id", "")
        if structure_id and structure_id != matched_structure_id:
            structure_store.add_diff_entry(
                matched_structure_id,
                target_id=structure_id,
                content_signature=structure_obj.get("structure", {}).get("content_signature", ""),
                base_weight=float(self._config.get("stimulus_extension_link_base_weight", 0.75)),
                residual_existing_signature="",
                residual_incoming_signature=common_part.get("residual_incoming_signature", ""),
                ext={
                    "linked_from_parent": matched_structure_id,
                    "relation_type": "incoming_extension",
                    "source_packet_id": source_packet_id,
                },
            )
            structure_db = structure_store.get_db_by_owner(matched_structure_id)
            if structure_db:
                self._maintenance.apply_structure_db_soft_limits(structure_db)
                structure_store.update_db(structure_db)
            self._register_atomic_extension_paths(
                full_units=full_units,
                matched_structure_id=matched_structure_id,
                target_structure=structure_obj,
                structure_store=structure_store,
                pointer_index=pointer_index,
                cut_engine=cut_engine,
                source_packet_id=source_packet_id,
            )
        return result

    def _store_residual_context_for_match(
        self,
        *,
        owner_structure_id: str,
        current_groups: list[dict],
        current_profile: dict,
        covered_units: list[dict],
        matched_structure: dict,
        structure_store,
        pointer_index,
        cut_engine,
        trace_id: str,
        tick_id: str,
        source_packet_id: str,
        round_index: int,
        min_common_length: int,
        episodic_memory_id: str,
    ) -> dict | None:
        # 刺激级残差信息改为“owner 局部库 + raw residual entry + common structure”模型。
        # 这里不再走旧的 residual structure 递归树，而是始终基于“本轮当前刺激组”归一化。
        return self._store_owner_local_residual_context(
            owner_structure_id=owner_structure_id,
            current_groups=current_groups,
            current_profile=current_profile,
            covered_units=covered_units,
            matched_structure=matched_structure,
            structure_store=structure_store,
            pointer_index=pointer_index,
            cut_engine=cut_engine,
            trace_id=trace_id,
            tick_id=tick_id,
            source_packet_id=source_packet_id,
            round_index=round_index,
            min_common_length=min_common_length,
            episodic_memory_id=episodic_memory_id,
        )

    def _store_owner_local_residual_context(
        self,
        *,
        owner_structure_id: str,
        current_groups: list[dict],
        current_profile: dict,
        covered_units: list[dict],
        matched_structure: dict,
        structure_store,
        pointer_index,
        cut_engine,
        trace_id: str,
        tick_id: str,
        source_packet_id: str,
        round_index: int,
        min_common_length: int,
        episodic_memory_id: str,
    ) -> dict | None:
        del matched_structure
        owner_structure = structure_store.get(owner_structure_id)
        owner_db = structure_store.get_db_by_owner(owner_structure_id)
        if not owner_structure or not owner_db:
            return None

        owner_profile = self._build_structure_profile(
            structure_obj=owner_structure,
            structure_store=structure_store,
            cut_engine=cut_engine,
        )
        owner_placeholder = self._self_placeholder_token(owner_structure)
        canonical_profile = self._apply_grouped_display_to_profile(dict(current_profile or {}))
        residual_profile = self._build_relative_residual_profile_from_groups(
            full_groups=current_groups,
            covered_units=covered_units,
            owner_placeholder=owner_placeholder,
            cut_engine=cut_engine,
            origin_frame_id=f"{tick_id}:{round_index}:{owner_structure_id}",
            canonical_profile=canonical_profile,
        )
        if not residual_profile:
            return None
        if not self._profile_has_non_placeholder_tokens(residual_profile, placeholder_token=owner_placeholder):
            return None

        summary = {
            "written_index_count": 0,
            "cut_count": 0,
            "new_structure_ids": [],
            "common_structure": None,
            "residual_structure": None,
        }
        self._normalize_owner_local_residual(
            owner_structure_id=owner_structure_id,
            owner_db=owner_db,
            owner_profile=owner_profile,
            owner_placeholder=owner_placeholder,
            residual_profile=residual_profile,
            structure_store=structure_store,
            pointer_index=pointer_index,
            cut_engine=cut_engine,
            trace_id=trace_id,
            tick_id=tick_id,
            source_packet_id=source_packet_id,
            round_index=round_index,
            min_common_length=max(1, int(min_common_length)),
            episodic_memory_id=episodic_memory_id,
            summary=summary,
            depth=0,
        )
        self._maintenance.apply_structure_db_soft_limits(owner_db)
        structure_store.update_db(owner_db)
        if (
            summary["written_index_count"] <= 0
            and summary["cut_count"] <= 0
            and not summary["new_structure_ids"]
            and not summary["common_structure"]
            and not summary["residual_structure"]
        ):
            return None
        summary["new_structure_ids"] = list(dict.fromkeys(summary["new_structure_ids"]))
        return summary

    def _normalize_owner_local_residual(
        self,
        *,
        owner_structure_id: str,
        owner_db: dict,
        owner_profile: dict,
        owner_placeholder: str,
        residual_profile: dict,
        structure_store,
        pointer_index,
        cut_engine,
        trace_id: str,
        tick_id: str,
        source_packet_id: str,
        round_index: int,
        min_common_length: int,
        episodic_memory_id: str,
        summary: dict,
        depth: int,
    ) -> None:
        if depth >= 12:
            return

        canonical_profile = self._apply_grouped_display_to_profile(
            self._canonicalize_profile(
                residual_profile,
                structure_store=structure_store,
                cut_engine=cut_engine,
            )
        )
        if not self._profile_has_non_placeholder_tokens(canonical_profile, placeholder_token=owner_placeholder):
            return

        local_items = self._list_owner_local_residual_items(
            owner_db=owner_db,
            owner_structure_id=owner_structure_id,
            structure_store=structure_store,
            cut_engine=cut_engine,
        )
        now_ms = int(time.time() * 1000)

        canonical_signature = str(canonical_profile.get("content_signature", ""))
        exact_raw_item = next(
            (
                item
                for item in local_items
                if item.get("item_kind") == "raw_residual"
                and canonical_signature
                and str(item.get("signature", "")) == canonical_signature
            ),
            None,
        )
        if exact_raw_item is None:
            exact_raw_item = next(
                (
                    item
                    for item in local_items
                    if item.get("item_kind") == "raw_residual"
                    and self._profiles_fuzzy_equivalent(
                        left_profile=item.get("canonical_profile", {}),
                        right_profile=canonical_profile,
                        cut_engine=cut_engine,
                    )
                ),
                None,
            )
        if exact_raw_item:
            self._reinforce_raw_residual_entry(
                entry=exact_raw_item["entry_ref"],
                residual_profile=residual_profile,
                canonical_profile=canonical_profile,
                episodic_memory_id=episodic_memory_id,
                round_index=round_index,
                source_packet_id=source_packet_id,
                structure_store=structure_store,
                cut_engine=cut_engine,
                now_ms=now_ms,
            )
            summary["written_index_count"] += 1
            if not summary.get("residual_structure"):
                summary["residual_structure"] = self._build_raw_residual_debug(
                    entry=exact_raw_item["entry_ref"],
                    created=False,
                    fallback_memory_id=episodic_memory_id,
                    structure_store=structure_store,
                    cut_engine=cut_engine,
                )
            return

        exact_common_item = next(
            (
                item
                for item in local_items
                if item.get("item_kind") == "common_structure"
                and canonical_signature
                and str(item.get("profile", {}).get("content_signature", "")) == canonical_signature
            ),
            None,
        )
        if exact_common_item is None:
            exact_common_item = next(
                (
                    item
                    for item in local_items
                    if item.get("item_kind") == "common_structure"
                    and self._profiles_fuzzy_equivalent(
                        left_profile=item.get("profile", {}),
                        right_profile=canonical_profile,
                        cut_engine=cut_engine,
                    )
                ),
                None,
            )
        if exact_common_item:
            self._reinforce_common_structure_entry(
                entry=exact_common_item["entry_ref"],
                delta_profile=canonical_profile,
                episodic_memory_id=episodic_memory_id,
                round_index=round_index,
                source_packet_id=source_packet_id,
                now_ms=now_ms,
            )
            summary["written_index_count"] += 1
            if not summary.get("common_structure"):
                summary["common_structure"] = {
                    **self._build_structure_debug(exact_common_item.get("structure_obj", {})),
                    "created": False,
                    "relation_type": "residual_context_common",
                    "parent_structure_id": owner_structure_id,
                    "memory_id": episodic_memory_id,
                }
            return

        parent_candidate = self._find_parent_common_candidate(
            residual_profile=canonical_profile,
            common_items=[item for item in local_items if item.get("item_kind") == "common_structure"],
            cut_engine=cut_engine,
        )
        if parent_candidate:
            self._reinforce_common_structure_entry(
                entry=parent_candidate["entry_ref"],
                delta_profile=canonical_profile,
                episodic_memory_id=episodic_memory_id,
                round_index=round_index,
                source_packet_id=source_packet_id,
                now_ms=now_ms,
            )
            summary["written_index_count"] += 1
            if not summary.get("common_structure"):
                summary["common_structure"] = {
                    **self._build_structure_debug(parent_candidate.get("structure_obj", {})),
                    "created": False,
                    "relation_type": "residual_context_common",
                    "parent_structure_id": owner_structure_id,
                    "memory_id": episodic_memory_id,
                }
            child_structure = parent_candidate.get("structure_obj", {})
            child_structure_id = str(child_structure.get("id", ""))
            child_db = structure_store.get_db_by_owner(child_structure_id)
            if not child_db:
                return
            child_profile = self._build_descend_relative_profile_for_common(
                full_profile=canonical_profile,
                common_part=parent_candidate.get("common_part", {}),
                child_placeholder=self._self_placeholder_token(child_structure),
                cut_engine=cut_engine,
                origin_frame_id=f"{tick_id}:{round_index}:descend:{child_structure_id}",
            )
            if not child_profile:
                return
            self._normalize_owner_local_residual(
                owner_structure_id=child_structure_id,
                owner_db=child_db,
                owner_profile=self._build_structure_profile(
                    structure_obj=child_structure,
                    structure_store=structure_store,
                    cut_engine=cut_engine,
                ),
                owner_placeholder=self._self_placeholder_token(child_structure),
                residual_profile=child_profile,
                structure_store=structure_store,
                pointer_index=pointer_index,
                cut_engine=cut_engine,
                trace_id=trace_id,
                tick_id=tick_id,
                source_packet_id=source_packet_id,
                round_index=round_index,
                min_common_length=min_common_length,
                episodic_memory_id=episodic_memory_id,
                summary=summary,
                depth=depth + 1,
            )
            self._maintenance.apply_structure_db_soft_limits(child_db)
            structure_store.update_db(child_db)
            return

        overlap_candidate = self._find_best_raw_overlap_candidate(
            residual_profile=canonical_profile,
            raw_items=[item for item in local_items if item.get("item_kind") == "raw_residual"],
            owner_profile=owner_profile,
            cut_engine=cut_engine,
            min_common_length=max(1, int(min_common_length)),
        )
        if overlap_candidate:
            common_part = overlap_candidate.get("common_part", {})
            common_profile = overlap_candidate.get("common_profile", {}) or self._apply_grouped_display_to_profile(
                cut_engine.build_sequence_profile_from_groups(list(common_part.get("common_groups", [])))
            )
            if not self._common_overlap_beyond_owner(
                common_part=common_part,
                owner_profile=owner_profile,
                common_profile=common_profile,
                cut_engine=cut_engine,
            ):
                overlap_candidate = None
        if overlap_candidate:
            common_part = overlap_candidate.get("common_part", {})
            common_profile = overlap_candidate.get("common_profile", {}) or common_profile
            common_result = self._find_or_create_structure_from_profile(
                profile=common_profile,
                structure_store=structure_store,
                pointer_index=pointer_index,
                cut_engine=cut_engine,
                trace_id=trace_id,
                tick_id=tick_id,
                confidence=float(self._config.get("stimulus_residual_common_confidence", 0.78)),
                origin="stimulus_residual_common",
                origin_id=source_packet_id,
                parent_ids=[owner_structure_id],
                base_weight=max(
                    float(self._config.get("weight_floor", 0.05)),
                    float(overlap_candidate.get("base_weight", 1.0))
                    + self._profile_total_energy(canonical_profile) * float(self._config.get("base_weight_er_gain", 0.08)),
                ),
                ext={
                    "kind": "residual_context_common",
                    "relation_type": "residual_context_common",
                    "owner_structure_id": owner_structure_id,
                    "source_packet_id": source_packet_id,
                },
            )
            common_structure = common_result["structure"]
            common_structure_id = str(common_structure.get("id", ""))
            if common_result.get("created") and common_structure_id:
                summary["new_structure_ids"].append(common_structure_id)
            if not summary.get("common_structure"):
                summary["common_structure"] = {
                    **self._build_structure_debug(common_structure),
                    "created": bool(common_result.get("created")),
                    "relation_type": "residual_context_common",
                    "parent_structure_id": owner_structure_id,
                    "memory_id": episodic_memory_id,
                }

            removed_count = self._remove_owner_diff_entry(
                owner_db=owner_db,
                entry_id=str(overlap_candidate.get("entry_id", "")),
            )
            summary["cut_count"] += max(1, removed_count)
            common_entry = self._append_or_reinforce_common_structure_entry(
                owner_structure_id=owner_structure_id,
                owner_db=owner_db,
                common_structure=common_structure,
                common_profile=common_profile,
                structure_store=structure_store,
                base_weight=max(
                    float(self._config.get("weight_floor", 0.05)),
                    float(overlap_candidate.get("base_weight", 1.0))
                    + self._profile_total_energy(canonical_profile) * float(self._config.get("base_weight_er_gain", 0.08)),
                ),
                source_packet_id=source_packet_id,
                round_index=round_index,
                episodic_memory_id=episodic_memory_id,
                now_ms=now_ms,
            )
            summary["written_index_count"] += int(bool(common_entry))

            common_db = structure_store.get_db_by_owner(common_structure_id)
            if not common_db:
                return
            common_placeholder = self._self_placeholder_token(common_structure)
            common_owner_profile = self._build_structure_profile(
                structure_obj=common_structure,
                structure_store=structure_store,
                cut_engine=cut_engine,
            )
            existing_child_profile = self._build_descend_relative_profile_for_common(
                full_profile=overlap_candidate.get("canonical_profile", {}),
                common_part=common_part,
                child_placeholder=common_placeholder,
                cut_engine=cut_engine,
                origin_frame_id=f"{tick_id}:{round_index}:existing:{common_structure_id}",
            )
            incoming_child_profile = self._build_descend_relative_profile_for_common(
                full_profile=canonical_profile,
                common_part=common_part,
                child_placeholder=common_placeholder,
                cut_engine=cut_engine,
                origin_frame_id=f"{tick_id}:{round_index}:incoming:{common_structure_id}",
            )
            if incoming_child_profile:
                # Prefer surfacing the current tick branch in debug output.
                # The user-facing "new residual" should describe the current stimulus group
                # written this round, not the historical branch migrated from the old raw item.
                self._normalize_owner_local_residual(
                    owner_structure_id=common_structure_id,
                    owner_db=common_db,
                    owner_profile=common_owner_profile,
                    owner_placeholder=common_placeholder,
                    residual_profile=incoming_child_profile,
                    structure_store=structure_store,
                    pointer_index=pointer_index,
                    cut_engine=cut_engine,
                    trace_id=trace_id,
                    tick_id=tick_id,
                    source_packet_id=source_packet_id,
                    round_index=round_index,
                    min_common_length=min_common_length,
                    episodic_memory_id=episodic_memory_id,
                    summary=summary,
                    depth=depth + 1,
                )
            if existing_child_profile:
                self._normalize_owner_local_residual(
                    owner_structure_id=common_structure_id,
                    owner_db=common_db,
                    owner_profile=common_owner_profile,
                    owner_placeholder=common_placeholder,
                    residual_profile=existing_child_profile,
                    structure_store=structure_store,
                    pointer_index=pointer_index,
                    cut_engine=cut_engine,
                    trace_id=trace_id,
                    tick_id=tick_id,
                    source_packet_id=source_packet_id,
                    round_index=round_index,
                    min_common_length=min_common_length,
                    episodic_memory_id=episodic_memory_id,
                    summary=summary,
                    depth=depth + 1,
                )
            self._maintenance.apply_structure_db_soft_limits(common_db)
            structure_store.update_db(common_db)
            return

        created_entry = self._append_raw_residual_entry(
            owner_db=owner_db,
            residual_profile=residual_profile,
            canonical_profile=canonical_profile,
            episodic_memory_id=episodic_memory_id,
            round_index=round_index,
            source_packet_id=source_packet_id,
        )
        summary["written_index_count"] += 1
        if not summary.get("residual_structure"):
            summary["residual_structure"] = self._build_raw_residual_debug(
                entry=created_entry,
                created=True,
                fallback_memory_id=episodic_memory_id,
                structure_store=structure_store,
                cut_engine=cut_engine,
            )

    def _build_relative_residual_profile_from_groups(
        self,
        *,
        full_groups: list[dict],
        covered_units: list[dict],
        owner_placeholder: str,
        cut_engine,
        origin_frame_id: str,
        canonical_profile: dict,
    ) -> dict | None:
        covered_ids = {str(unit.get("unit_id", "")) for unit in covered_units if str(unit.get("unit_id", ""))}
        if not covered_ids:
            return None
        residual_groups = self._build_relative_groups_with_placeholder(
            full_groups=full_groups,
            matched_unit_ids=covered_ids,
            placeholder_token=owner_placeholder,
            origin_frame_id=origin_frame_id,
        )
        if not residual_groups:
            return None
        raw_profile = self._apply_grouped_display_to_profile(
            cut_engine.build_sequence_profile_from_groups(residual_groups)
        )
        canonical = self._apply_grouped_display_to_profile(dict(canonical_profile or {}))
        return self._attach_explicit_canonical_profile(raw_profile, canonical_profile=canonical)

    def _build_descend_relative_profile_for_common(
        self,
        *,
        full_profile: dict,
        common_part: dict,
        child_placeholder: str,
        cut_engine,
        origin_frame_id: str,
    ) -> dict | None:
        full_groups = list(full_profile.get("sequence_groups", []))
        if not full_groups:
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
        full_unit_ids = {
            str(unit.get("unit_id", ""))
            for group in full_groups
            for unit in group.get("units", [])
            if isinstance(unit, dict) and str(unit.get("unit_id", ""))
        }
        existing_overlap = len(full_unit_ids & existing_refs)
        incoming_overlap = len(full_unit_ids & incoming_refs)
        matched_refs = incoming_refs if incoming_overlap >= existing_overlap else existing_refs
        if not matched_refs:
            return None
        child_groups = self._build_relative_groups_with_placeholder(
            full_groups=full_groups,
            matched_unit_ids=matched_refs,
            placeholder_token=child_placeholder,
            origin_frame_id=origin_frame_id,
        )
        if not child_groups:
            return None
        child_profile = self._apply_grouped_display_to_profile(
            cut_engine.build_sequence_profile_from_groups(child_groups)
        )
        if not self._profile_has_non_placeholder_tokens(child_profile, placeholder_token=child_placeholder):
            return None
        return self._attach_explicit_canonical_profile(
            child_profile,
            canonical_profile=self._apply_grouped_display_to_profile(dict(full_profile or {})),
        )

    def _build_relative_groups_with_placeholder(
        self,
        *,
        full_groups: list[dict],
        matched_unit_ids: set[str],
        placeholder_token: str,
        origin_frame_id: str,
    ) -> list[dict]:
        if not matched_unit_ids:
            return []
        groups: list[dict] = []
        placeholder_inserted = False
        for group in full_groups:
            if not isinstance(group, dict):
                continue
            template_units = [
                dict(unit)
                for unit in sorted(
                    [item for item in group.get("units", []) if isinstance(item, dict)],
                    key=lambda item: int(item.get("sequence_index", 0)),
                )
            ]
            if not template_units:
                continue
            next_units: list[dict] = []
            for unit in template_units:
                unit_id = str(unit.get("unit_id", ""))
                if unit_id in matched_unit_ids:
                    if not placeholder_inserted:
                        next_units.append(
                            self._make_placeholder_unit_for_relative_profile(
                                placeholder_token=placeholder_token,
                                template_unit=unit,
                                origin_frame_id=origin_frame_id,
                            )
                        )
                        placeholder_inserted = True
                    continue
                next_units.append(
                    {
                        **dict(unit),
                        "origin_frame_id": str(group.get("origin_frame_id", unit.get("origin_frame_id", origin_frame_id))),
                        "source_type": str(group.get("source_type", unit.get("source_type", ""))),
                    }
                )
            if not next_units:
                continue
            groups.append(
                {
                    "group_index": len(groups),
                    "source_type": str(group.get("source_type", "")),
                    "origin_frame_id": str(group.get("origin_frame_id", origin_frame_id)),
                    "source_group_index": int(group.get("source_group_index", group.get("group_index", len(groups)))),
                    "source_sequence_index": int(group.get("source_sequence_index", 0)),
                    "units": next_units,
                }
            )
        return groups if placeholder_inserted else []

    @staticmethod
    def _make_placeholder_unit_for_relative_profile(
        *,
        placeholder_token: str,
        template_unit: dict,
        origin_frame_id: str,
    ) -> dict:
        return {
            "unit_id": f"placeholder::{placeholder_token}::{template_unit.get('unit_id', '')}",
            "object_type": "sa",
            "token": placeholder_token,
            "display_text": placeholder_token,
            "unit_role": "placeholder",
            "unit_signature": f"P:{placeholder_token}",
            "sequence_index": int(template_unit.get("sequence_index", 0)),
            "group_index": int(template_unit.get("group_index", 0)),
            "source_group_index": int(template_unit.get("source_group_index", template_unit.get("group_index", 0))),
            "source_type": str(template_unit.get("source_type", "")),
            "origin_frame_id": str(origin_frame_id),
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

    def _list_owner_local_residual_items(
        self,
        *,
        owner_db: dict,
        owner_structure_id: str,
        structure_store,
        cut_engine,
    ) -> list[dict]:
        items = []
        for entry in owner_db.get("diff_table", []):
            if entry.get("entry_type") == "raw_residual":
                profiles = self._ensure_raw_residual_entry_profiles(
                    entry=entry,
                    structure_store=structure_store,
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
                        "base_weight": float(entry.get("base_weight", 1.0)),
                        "entry_runtime_weight": self._weight.entry_runtime_weight(entry),
                    }
                )
                continue
            if entry.get("entry_type", "structure_ref") != "structure_ref":
                continue
            relation_type = str(entry.get("ext", {}).get("relation_type", ""))
            if relation_type != "residual_context_common":
                continue
            target_structure, _ = self._resolve_diff_target(entry, structure_store)
            if not target_structure:
                continue
            items.append(
                {
                    "item_kind": "common_structure",
                    "entry_ref": entry,
                    "entry_id": entry.get("entry_id", ""),
                    "structure_id": target_structure.get("id", ""),
                    "structure_obj": target_structure,
                    "profile": self._build_structure_profile(
                        structure_obj=target_structure,
                        structure_store=structure_store,
                        cut_engine=cut_engine,
                    ),
                    "base_weight": float(entry.get("base_weight", 1.0)),
                    "entry_runtime_weight": self._weight.entry_runtime_weight(entry),
                    "owner_structure_id": owner_structure_id,
                }
            )
        items.sort(
            key=lambda item: (
                -float(item.get("entry_runtime_weight", 0.0)),
                -float(item.get("base_weight", 0.0)),
                str(item.get("entry_id", "")),
            )
        )
        return items

    def _find_parent_common_candidate(
        self,
        *,
        residual_profile: dict,
        common_items: list[dict],
        cut_engine,
    ) -> dict | None:
        best = None
        residual_unit_count = max(0, self._profile_unit_count(residual_profile))
        for item in common_items:
            existing_profile = item.get("profile", {})
            existing_unit_count = max(0, self._profile_unit_count(existing_profile))
            if existing_unit_count <= 0 or existing_unit_count > residual_unit_count:
                continue
            common_part = cut_engine.maximum_common_part(
                existing_profile.get("sequence_groups", []),
                residual_profile.get("sequence_groups", []),
            )
            if common_part.get("residual_existing_signature", ""):
                continue
            if int(common_part.get("matched_existing_unit_count", 0)) < max(1, existing_unit_count):
                continue
            if self._profiles_fuzzy_equivalent(
                left_profile=existing_profile,
                right_profile=residual_profile,
                cut_engine=cut_engine,
                common_part=common_part,
            ):
                continue
            candidate_key = (
                self._profile_unit_count(existing_profile),
                float(item.get("entry_runtime_weight", 0.0)),
            )
            current_key = (
                self._profile_unit_count(best.get("profile", {})),
                float(best.get("entry_runtime_weight", 0.0)),
            ) if best else None
            if best is None or candidate_key > current_key:
                best = {**item, "common_part": common_part}
        return best

    def _find_best_raw_overlap_candidate(
        self,
        *,
        residual_profile: dict,
        raw_items: list[dict],
        owner_profile: dict,
        cut_engine,
        min_common_length: int,
    ) -> dict | None:
        best = None
        residual_unit_count = max(0, self._profile_unit_count(residual_profile))
        min_common_length = max(1, int(min_common_length))
        for item in raw_items:
            existing_profile = item.get("canonical_profile", {})
            existing_unit_count = max(0, self._profile_unit_count(existing_profile))
            if existing_unit_count <= 0 or min(existing_unit_count, residual_unit_count) < min_common_length:
                continue
            common_part = cut_engine.maximum_common_part(
                existing_profile.get("sequence_groups", []),
                residual_profile.get("sequence_groups", []),
            )
            common_signature = str(common_part.get("common_signature", ""))
            if not common_signature:
                continue
            if int(common_part.get("common_length", 0)) < min_common_length:
                continue
            validated_common = self._validate_owner_overlap_common_part(
                common_part=common_part,
                owner_profile=owner_profile,
                cut_engine=cut_engine,
            )
            if not validated_common:
                continue
            if self._profiles_fuzzy_equivalent(
                left_profile=existing_profile,
                right_profile=residual_profile,
                cut_engine=cut_engine,
                common_part=common_part,
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
                    "common_profile": validated_common.get("common_profile", {}),
                }
        return best

    def _ensure_raw_residual_entry_schema(self, entry: dict) -> None:
        now_ms = int(time.time() * 1000)
        entry.setdefault("entry_id", next_id("srr"))
        entry.setdefault("entry_type", "raw_residual")
        entry.setdefault("target_id", "")
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
        entry.setdefault("memory_refs", [])
        entry.setdefault("ext", {})

    def _profile_from_stored_groups(self, groups: list[dict], *, cut_engine, ext: dict | None = None) -> dict:
        profile = self._apply_grouped_display_to_profile(
            cut_engine.build_sequence_profile_from_groups(list(groups or []))
        )
        merged_ext = dict(profile.get("ext", {}))
        merged_ext.update(ext or {})
        profile["ext"] = merged_ext
        return profile

    def _ensure_raw_residual_entry_profiles(self, *, entry: dict, structure_store, cut_engine) -> dict:
        self._ensure_raw_residual_entry_schema(entry)
        cache = None
        if isinstance(self._runtime_cache, dict):
            cache = self._runtime_cache.get("raw_residual_entry_profiles")
        entry_id = str(entry.get("entry_id", ""))
        cache_key = (entry_id, int(entry.get("last_updated_at", 0)))
        if isinstance(cache, dict) and entry_id and cache_key in cache:
            return cache[cache_key]

        raw_profile = self._profile_from_stored_groups(
            entry.get("sequence_groups", []),
            cut_engine=cut_engine,
            ext={"kind": "stimulus_raw_residual"},
        )
        canonical_groups = list(entry.get("canonical_sequence_groups", []))
        if canonical_groups:
            canonical_profile = self._profile_from_stored_groups(
                canonical_groups,
                cut_engine=cut_engine,
                ext={"kind": "stimulus_raw_residual_canonical"},
            )
        else:
            canonical_profile = self._apply_grouped_display_to_profile(
                self._canonicalize_profile(
                    raw_profile,
                    structure_store=structure_store,
                    cut_engine=cut_engine,
                )
            )
            entry["canonical_content_signature"] = canonical_profile.get("content_signature", "")
            entry["canonical_display_text"] = canonical_profile.get("display_text", "")
            entry["canonical_flat_tokens"] = list(canonical_profile.get("flat_tokens", []))
            entry["canonical_sequence_groups"] = list(canonical_profile.get("sequence_groups", []))
        profiles = {"raw_profile": raw_profile, "canonical_profile": canonical_profile}
        if isinstance(cache, dict) and entry_id:
            cache[cache_key] = profiles
        return profiles

    def _append_raw_residual_entry(
        self,
        *,
        owner_db: dict,
        residual_profile: dict,
        canonical_profile: dict,
        episodic_memory_id: str,
        round_index: int,
        source_packet_id: str,
    ) -> dict:
        er, ev = self._residual_profile_energy(canonical_profile)
        entry = {
            "entry_id": next_id("srr"),
            "entry_type": "raw_residual",
            "target_id": "",
            "content_signature": residual_profile.get("content_signature", ""),
            "display_text": residual_profile.get("display_text", ""),
            "flat_tokens": list(residual_profile.get("flat_tokens", [])),
            "sequence_groups": list(residual_profile.get("sequence_groups", [])),
            "canonical_content_signature": canonical_profile.get("content_signature", ""),
            "canonical_display_text": canonical_profile.get("display_text", ""),
            "canonical_flat_tokens": list(canonical_profile.get("flat_tokens", [])),
            "canonical_sequence_groups": list(canonical_profile.get("sequence_groups", [])),
            "base_weight": self._residual_base_weight_from_profile(canonical_profile),
            "recent_gain": self._weight._target_recent_gain(strength=1.0),
            "fatigue": 0.0,
            "runtime_er": er,
            "runtime_ev": ev,
            "match_count_total": 0,
            "last_updated_at": int(time.time() * 1000),
            "last_matched_at": 0,
            "last_recency_refresh_at": int(time.time() * 1000),
            "recency_hold_rounds_remaining": int(self._config.get("recency_gain_hold_rounds", 2)),
            "memory_refs": [episodic_memory_id] if episodic_memory_id else [],
            "ext": {
                "relation_type": "stimulus_raw_residual",
                "source_packet_id": source_packet_id,
                "round_index": round_index,
            },
        }
        owner_db.setdefault("diff_table", []).append(entry)
        return entry

    def _reinforce_raw_residual_entry(
        self,
        *,
        entry: dict,
        residual_profile: dict,
        canonical_profile: dict,
        episodic_memory_id: str,
        round_index: int,
        source_packet_id: str,
        structure_store,
        cut_engine,
        now_ms: int,
    ) -> dict:
        self._ensure_raw_residual_entry_schema(entry)
        existing_profiles = self._ensure_raw_residual_entry_profiles(
            entry=entry,
            structure_store=structure_store,
            cut_engine=cut_engine,
        )
        merged_canonical = canonical_profile
        existing_canonical = existing_profiles.get("canonical_profile", {})
        common_part = None
        if existing_canonical:
            common_part = cut_engine.maximum_common_part(
                existing_canonical.get("sequence_groups", []),
                canonical_profile.get("sequence_groups", []),
            )
        if existing_canonical and common_part and self._profiles_fuzzy_equivalent(
            left_profile=existing_canonical,
            right_profile=canonical_profile,
            cut_engine=cut_engine,
            common_part=common_part,
        ):
            merged_canonical = self._profile_from_stored_groups(
                list(common_part.get("common_groups", [])),
                cut_engine=cut_engine,
                ext={"kind": "stimulus_raw_residual_canonical_merged"},
            )
        self._reinforce_residual_entry_from_profile(entry, merged_canonical, now_ms=now_ms)
        entry["canonical_content_signature"] = merged_canonical.get("content_signature", "")
        entry["canonical_display_text"] = merged_canonical.get("display_text", "")
        entry["canonical_flat_tokens"] = list(merged_canonical.get("flat_tokens", []))
        entry["canonical_sequence_groups"] = list(merged_canonical.get("sequence_groups", []))
        if episodic_memory_id:
            memory_refs = list(entry.get("memory_refs", []))
            if episodic_memory_id not in memory_refs:
                memory_refs.append(episodic_memory_id)
            entry["memory_refs"] = memory_refs
        entry.setdefault("ext", {})["round_index"] = round_index
        entry["ext"]["source_packet_id"] = source_packet_id
        entry["last_updated_at"] = now_ms
        return entry

    def _append_or_reinforce_common_structure_entry(
        self,
        *,
        owner_structure_id: str,
        owner_db: dict,
        common_structure: dict,
        common_profile: dict,
        structure_store,
        base_weight: float,
        source_packet_id: str,
        round_index: int,
        episodic_memory_id: str,
        now_ms: int,
    ) -> dict | None:
        common_structure_id = str(common_structure.get("id", ""))
        common_signature = str(common_profile.get("content_signature", ""))
        for entry in owner_db.get("diff_table", []):
            if entry.get("entry_type", "structure_ref") != "structure_ref":
                continue
            if str(entry.get("target_id", "")) != common_structure_id:
                continue
            if str(entry.get("ext", {}).get("relation_type", "")) != "residual_context_common":
                continue
            if str(entry.get("content_signature", "")) != common_signature:
                continue
            self._reinforce_common_structure_entry(
                entry=entry,
                delta_profile=common_profile,
                episodic_memory_id=episodic_memory_id,
                round_index=round_index,
                source_packet_id=source_packet_id,
                now_ms=now_ms,
            )
            return entry
        entry = structure_store.add_diff_entry(
            owner_structure_id,
            target_id=common_structure_id,
            content_signature=common_signature,
            base_weight=base_weight,
            residual_existing_signature="",
            residual_incoming_signature="",
            ext={
                "relation_type": "residual_context_common",
                "kind": "residual_context_common",
                "source_packet_id": source_packet_id,
                "round_index": round_index,
                "memory_refs": [episodic_memory_id] if episodic_memory_id else [],
                "grouped_display_text": common_profile.get("display_text", ""),
            },
        )
        if entry:
            self._reinforce_common_structure_entry(
                entry=entry,
                delta_profile=common_profile,
                episodic_memory_id=episodic_memory_id,
                round_index=round_index,
                source_packet_id=source_packet_id,
                now_ms=now_ms,
            )
        return entry

    def _reinforce_common_structure_entry(
        self,
        *,
        entry: dict,
        delta_profile: dict,
        episodic_memory_id: str,
        round_index: int,
        source_packet_id: str,
        now_ms: int,
    ) -> dict:
        self._reinforce_residual_entry_from_profile(entry, delta_profile, now_ms=now_ms)
        ext = dict(entry.get("ext", {}))
        memory_refs = list(ext.get("memory_refs", []))
        if episodic_memory_id and episodic_memory_id not in memory_refs:
            memory_refs.append(episodic_memory_id)
        ext["memory_refs"] = memory_refs
        ext["round_index"] = round_index
        ext["source_packet_id"] = source_packet_id
        entry["ext"] = ext
        return entry

    @staticmethod
    def _remove_owner_diff_entry(*, owner_db: dict, entry_id: str) -> int:
        if not entry_id:
            return 0
        before = len(owner_db.get("diff_table", []))
        owner_db["diff_table"] = [
            entry
            for entry in owner_db.get("diff_table", [])
            if str(entry.get("entry_id", "")) != entry_id
        ]
        return max(0, before - len(owner_db.get("diff_table", [])))

    def _profiles_fuzzy_equivalent(self, *, left_profile: dict, right_profile: dict, cut_engine, common_part: dict | None = None) -> bool:
        left_signature = str(left_profile.get("content_signature", ""))
        right_signature = str(right_profile.get("content_signature", ""))
        if left_signature and left_signature == right_signature:
            return True

        if common_part is None:
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
        )

    @staticmethod
    def _profile_unit_count(profile: dict) -> int:
        return int(profile.get("unit_count", profile.get("token_count", len(profile.get("flat_tokens", [])))))

    def _profile_total_energy(self, profile: dict) -> float:
        er, ev = self._residual_profile_energy(profile)
        return round(er + ev, 8)

    def _build_stimulus_memory_material(self, *, profile: dict) -> dict:
        sequence_groups = self._clone_working_groups(list(profile.get("sequence_groups", [])))
        grouped_display_text = format_sequence_groups(sequence_groups) or str(profile.get("display_text", ""))
        unit_weights: dict[str, float] = {}
        group_weights: dict[str, float] = {}
        total_weight = 0.0
        for group in sequence_groups:
            group_weight = 0.0
            for unit in group.get("units", []):
                unit_id = str(unit.get("unit_id", ""))
                if not unit_id:
                    continue
                unit_weight = round(max(0.0, float(unit.get("er", 0.0))) + max(0.0, float(unit.get("ev", 0.0))), 8)
                unit_weights[unit_id] = unit_weight
                group_weight += unit_weight
            group_key = str(group.get("group_index", len(group_weights)))
            group_weights[group_key] = round(group_weight, 8)
            total_weight += group_weight
        if total_weight > 0.0:
            unit_energy_profile = {
                unit_id: round(weight / total_weight, 8)
                for unit_id, weight in unit_weights.items()
                if weight > 0.0
            }
            group_energy_profile = {
                group_id: round(weight / total_weight, 8)
                for group_id, weight in group_weights.items()
                if weight > 0.0
            }
        else:
            ordered_units = [
                str(unit.get("unit_id", ""))
                for group in sequence_groups
                for unit in group.get("units", [])
                if str(unit.get("unit_id", ""))
            ]
            fallback_share = round(1.0 / len(ordered_units), 8) if ordered_units else 0.0
            unit_energy_profile = {unit_id: fallback_share for unit_id in ordered_units}
            group_energy_profile = {}
        return {
            "memory_kind": "stimulus_packet",
            "storage_grain": "sa",
            "grouped_display_text": grouped_display_text,
            "sequence_groups": sequence_groups,
            "unit_energy_profile": unit_energy_profile,
            "group_energy_profile": group_energy_profile,
        }

    def _apply_grouped_display_to_profile(self, profile: dict) -> dict:
        updated = dict(profile or {})
        grouped_display_text = format_sequence_groups(updated.get("sequence_groups", []))
        if grouped_display_text:
            updated["grouped_display_text"] = grouped_display_text
            updated["display_text"] = grouped_display_text
        return updated

    def _build_raw_residual_debug(
        self,
        *,
        entry: dict,
        created: bool,
        fallback_memory_id: str,
        structure_store,
        cut_engine,
    ) -> dict:
        profiles = self._ensure_raw_residual_entry_profiles(
            entry=entry,
            structure_store=structure_store,
            cut_engine=cut_engine,
        )
        raw_profile = profiles.get("raw_profile", {})
        canonical_profile = profiles.get("canonical_profile", {})
        return {
            "entry_id": entry.get("entry_id", ""),
            "memory_id": (entry.get("memory_refs", []) or [fallback_memory_id] or [""])[-1],
            "kind": "raw_residual_memory",
            "created": bool(created),
            "display_text": canonical_profile.get("display_text", "") or raw_profile.get("display_text", ""),
            "raw_display_text": raw_profile.get("display_text", ""),
            "canonical_display_text": canonical_profile.get("display_text", ""),
            "raw_grouped_display_text": raw_profile.get("display_text", ""),
            "canonical_grouped_display_text": canonical_profile.get("display_text", ""),
            "content_signature": canonical_profile.get("content_signature", ""),
            "stats": {
                "base_weight": round(float(entry.get("base_weight", 1.0)), 8),
                "recent_gain": round(float(entry.get("recent_gain", 1.0)), 8),
                "fatigue": round(float(entry.get("fatigue", 0.0)), 8),
                "runtime_er": round(float(entry.get("runtime_er", 0.0)), 8),
                "runtime_ev": round(float(entry.get("runtime_ev", 0.0)), 8),
                "match_count_total": int(entry.get("match_count_total", 0)),
            },
        }

    def _find_or_create_structure_from_profile(
        self,
        *,
        profile: dict,
        structure_store,
        pointer_index,
        cut_engine,
        trace_id: str,
        tick_id: str,
        confidence: float,
        origin: str,
        origin_id: str,
        parent_ids: list[str],
        base_weight: float | None = None,
        ext: dict | None = None,
    ) -> dict:
        canonical_profile = self._canonicalize_profile(
            profile,
            structure_store=structure_store,
            cut_engine=cut_engine,
        )
        payload = cut_engine.make_structure_payload_from_profile(canonical_profile, confidence=confidence, ext=ext)
        if base_weight is not None:
            payload["base_weight"] = round(float(base_weight), 8)
        signature = payload.get("content_signature", "")
        existing = self._find_exact_structure_by_signature(
            signature=signature,
            structure_store=structure_store,
            pointer_index=pointer_index,
            cut_engine=cut_engine,
            expected_tokens=list(payload.get("flat_tokens", [])),
            expected_sequence_groups=list(payload.get("sequence_groups", [])),
        )
        if existing:
            return {"created": False, "structure": existing}
        structure_obj, _ = structure_store.create_structure(
            structure_payload=payload,
            trace_id=trace_id,
            tick_id=tick_id,
            origin=origin,
            origin_id=origin_id,
            parent_ids=parent_ids,
        )
        pointer_index.register_structure(structure_obj)
        return {"created": True, "structure": structure_obj}

    @staticmethod
    def _self_placeholder_token(structure_obj: dict) -> str:
        structure_id = str(structure_obj.get("id", ""))
        display_text = str(structure_obj.get("structure", {}).get("display_text", structure_id))
        return f"SELF[{structure_id}:{display_text}]"

    def _build_structure_profile(self, *, structure_obj: dict, structure_store, cut_engine) -> dict:
        structure_id = str(structure_obj.get("id", ""))
        content_signature = str(structure_obj.get("structure", {}).get("content_signature", ""))
        cache_key = (structure_id, content_signature)
        cache = None
        if isinstance(self._runtime_cache, dict):
            cache = self._runtime_cache.get("structure_profiles")
        if isinstance(cache, dict) and cache_key in cache:
            return cache[cache_key]

        profile = restore_structure_profile(
            structure_obj,
            cut_engine=cut_engine,
            structure_store=structure_store,
            group_store=None,
        )
        if isinstance(cache, dict) and structure_id:
            cache[cache_key] = profile
        return profile

    @staticmethod
    def _attach_explicit_canonical_profile(profile: dict, *, canonical_profile: dict) -> dict:
        attached = dict(profile)
        attached["canonical_display_text"] = canonical_profile.get("display_text", "")
        attached["canonical_flat_tokens"] = list(canonical_profile.get("flat_tokens", []))
        attached["canonical_sequence_groups"] = list(canonical_profile.get("sequence_groups", []))
        return attached

    @staticmethod
    def _build_explicit_canonical_profile(profile: dict, *, cut_engine) -> dict | None:
        canonical_groups = list(profile.get("canonical_sequence_groups", []))
        if not canonical_groups:
            return None
        explicit = cut_engine.build_sequence_profile_from_groups(canonical_groups)
        explicit["display_text"] = str(profile.get("canonical_display_text", explicit.get("display_text", "")))
        explicit["flat_tokens"] = list(profile.get("canonical_flat_tokens", explicit.get("flat_tokens", [])))
        return explicit

    def _canonicalize_profile(self, profile: dict, *, structure_store, cut_engine) -> dict:
        explicit = self._build_explicit_canonical_profile(profile, cut_engine=cut_engine)
        if explicit is not None:
            merged_ext = dict(explicit.get("ext", {}))
            merged_ext.update(profile.get("ext", {}))
            merged_ext["restored_from_placeholder"] = True
            explicit["ext"] = merged_ext
            return explicit
        restored = restore_profile(
            profile,
            cut_engine=cut_engine,
            structure_store=structure_store,
            group_store=None,
        )
        if profile.get("display_text") and not restored.get("display_text"):
            restored["display_text"] = str(profile.get("display_text", ""))
        merged_ext = dict(restored.get("ext", {}))
        merged_ext.update(profile.get("ext", {}))
        restored["ext"] = merged_ext
        return restored

    @staticmethod
    def _build_residual_entry_ext(
        *,
        source_packet_id: str,
        round_index: int,
        episodic_memory_id: str,
        raw_profile: dict,
        canonical_profile: dict,
        raw_signature: str,
    ) -> dict:
        return {
            "relation_type": "residual_context",
            "source_packet_id": source_packet_id,
            "origin_round": round_index,
            "anchor_memory_id": episodic_memory_id,
            "raw_residual_signature": str(raw_signature or ""),
            "canonical_signature": str(canonical_profile.get("content_signature", "")),
            "raw_display_text": str(raw_profile.get("display_text", "")),
            "canonical_display_text": str(canonical_profile.get("display_text", "")),
        }

    @staticmethod
    def _profile_has_non_placeholder_tokens(profile: dict, *, placeholder_token: str) -> bool:
        return any(
            str(unit.get("token", "")) and str(unit.get("token", "")) != placeholder_token
            for group in profile.get("sequence_groups", [])
            for unit in group.get("units", [])
        )

    @staticmethod
    def _common_part_has_bidirectional_residuals(common_part: dict) -> bool:
        return bool(common_part.get("residual_existing_signature", "")) and bool(common_part.get("residual_incoming_signature", ""))

    # 刺激级共有结构必须完整保留当前命中的 owner 结构，并且 owner 外还要确实存在额外公共内容。
    # 如果共同部分已经不再包含当前 owner，就只能保留原始残差，不能错误地下沉为 owner 的子共有结构。
    def _validate_owner_overlap_common_part(self, *, common_part: dict, owner_profile: dict, cut_engine) -> dict | None:
        common_profile = self._apply_grouped_display_to_profile(
            cut_engine.build_sequence_profile_from_groups(list(common_part.get("common_groups", [])))
        )
        if not self._common_overlap_beyond_owner(
            common_part=common_part,
            owner_profile=owner_profile,
            common_profile=common_profile,
            cut_engine=cut_engine,
        ):
            return None
        return {"common_profile": common_profile}

    def _common_overlap_beyond_owner(self, *, common_part: dict, owner_profile: dict, common_profile: dict, cut_engine) -> bool:
        owner_unit_count = max(1, self._profile_unit_count(owner_profile))
        if int(common_part.get("common_length", 0)) <= owner_unit_count:
            return False
        return self._profile_fully_contains_subprofile(
            container_profile=common_profile,
            required_profile=owner_profile,
            cut_engine=cut_engine,
        )

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
        )

    @staticmethod
    def _subtract_tokens_preserve_order(tokens: list[str], tokens_to_remove: list[str]) -> list[str]:
        remove_counter = Counter(str(token) for token in tokens_to_remove if str(token))
        residual = []
        for token in tokens:
            text = str(token)
            if remove_counter.get(text, 0) > 0:
                remove_counter[text] -= 1
                continue
            residual.append(text)
        return residual

    def _register_atomic_extension_paths(
        self,
        *,
        full_units: list[dict],
        matched_structure_id: str,
        target_structure: dict,
        structure_store,
        pointer_index,
        cut_engine,
        source_packet_id: str,
    ) -> None:
        if not full_units:
            return
        target_id = target_structure.get("id", "")
        if not target_id:
            return
        target_signature = target_structure.get("structure", {}).get("content_signature", "")
        for unit in full_units:
            token = str(unit.get("token", ""))
            if not token:
                continue
            atomic_structure = self._find_exact_structure_by_signature(
                signature=token,
                structure_store=structure_store,
                pointer_index=pointer_index,
                cut_engine=cut_engine,
                expected_tokens=[token],
                expected_sequence_groups=[{"group_index": 0, "tokens": [token], "source_type": unit.get("source_type", "")}],
            )
            if not atomic_structure:
                continue
            owner_structure_id = atomic_structure.get("id", "")
            if not owner_structure_id or owner_structure_id == matched_structure_id:
                continue
            residual_units = self._subtract_units(full_units, [unit])
            residual_signature = cut_engine.sequence_groups_to_signature(self._units_to_groups(residual_units))
            structure_store.add_diff_entry(
                owner_structure_id,
                target_id=target_id,
                content_signature=target_signature,
                base_weight=float(self._config.get("stimulus_atomic_extension_link_base_weight", 0.72)),
                residual_existing_signature="",
                residual_incoming_signature=residual_signature,
                ext={
                    "linked_from_parent": owner_structure_id,
                    "relation_type": "incoming_extension",
                    "source_packet_id": source_packet_id,
                },
            )
            owner_db = structure_store.get_db_by_owner(owner_structure_id)
            if owner_db:
                self._maintenance.apply_structure_db_soft_limits(owner_db)
                structure_store.update_db(owner_db)

    def _find_or_create_structure_from_units(
        self,
        *,
        units: list[dict],
        structure_store,
        pointer_index,
        cut_engine,
        trace_id: str,
        tick_id: str,
        confidence: float,
        origin: str,
        origin_id: str,
        parent_ids: list[str],
        base_weight: float | None = None,
        ext: dict | None = None,
    ) -> dict:
        raw_profile = cut_engine.build_sequence_profile_from_groups(self._units_to_groups(units))
        canonical_profile = self._canonicalize_profile(
            raw_profile,
            structure_store=structure_store,
            cut_engine=cut_engine,
        )
        payload = cut_engine.make_structure_payload_from_profile(canonical_profile, confidence=confidence, ext=ext)
        if base_weight is not None:
            payload["base_weight"] = round(float(base_weight), 8)
        signature = payload.get("content_signature", "")
        existing = self._find_exact_structure_by_signature(
            signature=signature,
            structure_store=structure_store,
            pointer_index=pointer_index,
            cut_engine=cut_engine,
            expected_tokens=list(payload.get("flat_tokens", [])),
            expected_sequence_groups=list(payload.get("sequence_groups", [])),
        )
        if existing:
            return {"created": False, "structure": existing}
        structure_obj, _ = structure_store.create_structure(
            structure_payload=payload,
            trace_id=trace_id,
            tick_id=tick_id,
            origin=origin,
            origin_id=origin_id,
            parent_ids=parent_ids,
        )
        pointer_index.register_structure(structure_obj)
        return {"created": True, "structure": structure_obj}

    def _find_exact_structure_by_signature(
        self,
        *,
        signature: str,
        structure_store,
        pointer_index,
        cut_engine,
        expected_tokens: list[str] | None = None,
        expected_sequence_groups: list[dict] | None = None,
    ) -> dict | None:
        if not signature:
            return None
        signatures_to_try = [signature]
        if expected_tokens:
            legacy_signature = cut_engine.tokens_to_signature(expected_tokens)
            if legacy_signature and legacy_signature not in signatures_to_try:
                signatures_to_try.append(legacy_signature)
        seen_ids = set()
        for current_signature in signatures_to_try:
            for candidate_id in pointer_index.query_candidates_by_signature(current_signature):
                if candidate_id in seen_ids:
                    continue
                seen_ids.add(candidate_id)
                candidate = structure_store.get(candidate_id)
                if not candidate:
                    continue
                structure = candidate.get("structure", {})
                if expected_tokens is not None and list(structure.get("flat_tokens", [])) != list(expected_tokens):
                    continue
                if expected_sequence_groups is not None:
                    candidate_profile = self._build_structure_profile(
                        structure_obj=candidate,
                        structure_store=structure_store,
                        cut_engine=cut_engine,
                    )
                    if cut_engine.sequence_groups_to_signature(candidate_profile.get("sequence_groups", [])) != cut_engine.sequence_groups_to_signature(expected_sequence_groups):
                        continue
                return candidate
        return None

    @staticmethod
    def _upsert_candidate_detail(candidate_details: list[dict], detail: dict) -> list[dict]:
        detail_id = detail.get("structure_id", "")
        if not detail_id:
            return list(candidate_details)
        updated = [item for item in candidate_details if item.get("structure_id", "") != detail_id]
        updated.append(detail)
        updated.sort(
            key=lambda item: (
                0 if item.get("eligible") else 1,
                -float(item.get("competition_score", 0.0)),
                -int(item.get("existing_length", 0)),
                -float(item.get("entry_runtime_weight", 0.0)),
                -float(item.get("runtime_weight", 0.0)),
            )
        )
        return updated

    @staticmethod
    def _is_better_structure_match(candidate_detail: dict, current_best: dict | None) -> bool:
        if current_best is None:
            return True
        candidate_key = (
            1 if candidate_detail.get("eligible") else 0,
            float(candidate_detail.get("competition_score", 0.0)),
            int(candidate_detail.get("existing_length", 0)),
            float(candidate_detail.get("entry_runtime_weight", 0.0)),
            float(candidate_detail.get("runtime_weight", 0.0)),
        )
        current_key = (
            1 if current_best.get("eligible") else 0,
            float(current_best.get("competition_score", 0.0)),
            int(current_best.get("existing_length", 0)),
            float(current_best.get("entry_runtime_weight", 0.0)),
            float(current_best.get("runtime_weight", 0.0)),
        )
        return candidate_key > current_key

    @staticmethod
    def _unit_total_energy(unit: dict) -> float:
        return round(
            max(
                0.0,
                float(unit.get("total_energy", float(unit.get("er", 0.0)) + float(unit.get("ev", 0.0)))),
            ),
            8,
        )

    def _unit_competition_energy(self, unit: dict) -> float:
        base_energy = self._unit_total_energy(unit)
        if base_energy <= 0.0:
            return 0.0
        role = str(unit.get("unit_role", unit.get("role", "")) or "")
        if unit.get("is_placeholder"):
            scale = float(self._config.get("stimulus_placeholder_energy_scale", 1.0))
        elif role == "attribute":
            scale = float(self._config.get("stimulus_attribute_energy_scale", 0.22))
        else:
            scale = 1.0
        match_similarity = max(0.0, min(1.0, float(unit.get("match_similarity", 1.0))))
        return round(base_energy * max(0.0, scale) * match_similarity, 8)

    def _units_total_energy(self, units: list[dict]) -> float:
        return round(
            sum(self._unit_competition_energy(unit) for unit in units if isinstance(unit, dict)),
            8,
        )

    def _energy_match_ratio(
        self,
        *,
        matched_units: list[dict],
        all_units: list[dict],
        fallback_numerator: float,
        fallback_denominator: float,
    ) -> float:
        total_energy = self._units_total_energy(all_units)
        matched_energy = self._units_total_energy(matched_units)
        if total_energy > 0.0:
            return round(max(0.0, min(1.0, matched_energy / total_energy)), 8)
        if fallback_denominator <= 0:
            return 0.0
        return round(max(0.0, min(1.0, float(fallback_numerator) / float(fallback_denominator))), 8)

    def _residual_profile_energy(self, profile: dict) -> tuple[float, float]:
        er = 0.0
        ev = 0.0
        for group in profile.get("sequence_groups", []):
            for unit in group.get("units", []):
                if not isinstance(unit, dict):
                    continue
                er += max(0.0, float(unit.get("er", 0.0)))
                ev += max(0.0, float(unit.get("ev", 0.0)))
        return round(er, 8), round(ev, 8)

    def _residual_weight_delta(self, *, er: float, ev: float) -> float:
        return round(
            max(0.0, float(er)) * float(self._config.get("base_weight_er_gain", 0.08))
            - max(0.0, float(ev)) * float(self._config.get("base_weight_ev_wear", 0.03)),
            8,
        )

    def _residual_base_weight_from_profile(self, profile: dict, *, seed: float = 1.0) -> float:
        er, ev = self._residual_profile_energy(profile)
        return round(
            max(
                float(self._config.get("weight_floor", 0.05)),
                float(seed) + self._residual_weight_delta(er=er, ev=ev),
            ),
            8,
        )

    def _reinforce_residual_entry_from_profile(self, entry: dict, profile: dict, *, now_ms: int) -> dict:
        er, ev = self._residual_profile_energy(profile)
        self._weight.mark_entry_activation(
            entry,
            delta_er=er,
            delta_ev=ev,
            match_score=1.0,
            now_ms=now_ms,
        )
        entry["base_weight"] = round(
            max(
                float(self._config.get("weight_floor", 0.05)),
                float(entry.get("base_weight", 1.0)) + self._residual_weight_delta(er=er, ev=ev),
            ),
            8,
        )
        return entry

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

    @staticmethod
    def _sigmoid(value: float, *, midpoint: float, slope: float) -> float:
        safe_slope = max(1e-6, float(slope))
        try:
            result = 1.0 / (1.0 + math.exp(-(float(value) - float(midpoint)) / safe_slope))
        except OverflowError:
            result = 0.0 if value < midpoint else 1.0
        return round(max(0.0, min(1.0, result)), 8)

    def _compose_match_score(self, *, stimulus_match_ratio: float, structure_match_ratio: float) -> float:
        joint_ratio = max(0.0, min(1.0, float(min(stimulus_match_ratio, structure_match_ratio))))
        if joint_ratio >= 1.0:
            return 1.0
        denoise = self._sigmoid(
            joint_ratio,
            midpoint=float(self._config.get("stimulus_competition_noise_mid", 0.01)),
            slope=max(1e-6, float(self._config.get("stimulus_competition_noise_scale", 0.004))),
        )
        hill = self._hill_score(
            joint_ratio,
            half_point=float(self._config.get("stimulus_competition_half_ratio", 0.1)),
            power=float(self._config.get("stimulus_competition_curve_power", 1.2)),
        )
        score = hill * denoise
        return round(max(0.0, score), 8)

    @staticmethod
    def _effective_transfer_fraction(transfer_ratio: float, similarity_score: float) -> float:
        return round(
            max(0.0, min(1.0, max(0.0, float(transfer_ratio)) * max(0.0, min(1.0, float(similarity_score))))),
            8,
        )

    def _capture_structure_stats(self, structure_obj: dict) -> dict:
        stats = structure_obj.get("stats", {})
        return {
            "base_weight": round(float(stats.get("base_weight", 1.0)), 8),
            "recent_gain": round(float(stats.get("recent_gain", 1.0)), 8),
            "fatigue": round(float(stats.get("fatigue", 0.0)), 8),
            "runtime_er": round(float(stats.get("runtime_er", 0.0)), 8),
            "runtime_ev": round(float(stats.get("runtime_ev", 0.0)), 8),
            "match_count_total": int(stats.get("match_count_total", 0)),
            "verified_count_er": int(stats.get("verified_count_er", 0)),
            "worn_count_ev": int(stats.get("worn_count_ev", 0)),
        }

    def _build_structure_debug(self, structure_obj: dict) -> dict:
        sequence_groups = list(structure_obj.get("structure", {}).get("sequence_groups", []))
        return {
            "structure_id": structure_obj.get("id", ""),
            "display_text": structure_obj.get("structure", {}).get("display_text", structure_obj.get("id", "")),
            "flat_tokens": list(structure_obj.get("structure", {}).get("flat_tokens", [])),
            "sequence_groups": sequence_groups,
            "grouped_display_text": format_sequence_groups(sequence_groups),
            "content_signature": structure_obj.get("structure", {}).get("content_signature", ""),
            "ext": dict(structure_obj.get("structure", {}).get("ext", {})),
            "stats": self._capture_structure_stats(structure_obj),
        }

    @staticmethod
    def _clone_working_groups(groups: list[dict]) -> list[dict]:
        return [
            {
                **dict(group),
                "units": [dict(unit) for unit in group.get("units", []) if isinstance(unit, dict)],
                "csa_bundles": [dict(bundle) for bundle in group.get("csa_bundles", []) if isinstance(bundle, dict)],
            }
            for group in groups
            if isinstance(group, dict)
        ]

    @classmethod
    def _describe_runtime_groups(cls, groups: list[dict]) -> list[dict]:
        described = []
        for group in groups:
            if not isinstance(group, dict):
                continue
            units = sorted(
                [dict(unit) for unit in group.get("units", []) if isinstance(unit, dict)],
                key=lambda item: int(item.get("sequence_index", 0)),
            )
            described.append(
                {
                    "group_index": int(group.get("group_index", 0)),
                    "source_type": str(group.get("source_type", "")),
                    "origin_frame_id": str(group.get("origin_frame_id", "")),
                    "tokens": [str(unit.get("token", "")) for unit in units if str(unit.get("token", ""))],
                    "display_text": format_group_display(units, group.get("csa_bundles", [])),
                    "semantic_display_text": format_semantic_group_display(
                        {
                            **dict(group),
                            "units": [dict(unit) for unit in units if isinstance(unit, dict)],
                            "csa_bundles": [dict(bundle) for bundle in group.get("csa_bundles", []) if isinstance(bundle, dict)],
                        },
                        context="stimulus",
                    ),
                    "visible_text": "".join(
                        str(unit.get("token", ""))
                        for unit in units
                        if str(unit.get("token", "")) and (bool(unit.get("display_visible", False)) or bool(unit.get("is_placeholder", False)))
                    ),
                    "csa_bundles": cls._format_runtime_bundle_texts(group),
                    "csa_bundle_defs": [dict(bundle) for bundle in group.get("csa_bundles", []) if isinstance(bundle, dict)],
                    "units": [dict(unit) for unit in units if isinstance(unit, dict)],
                    "sequence_groups": [
                        {
                            **dict(group),
                            "units": [dict(unit) for unit in units if isinstance(unit, dict)],
                            "csa_bundles": [dict(bundle) for bundle in group.get("csa_bundles", []) if isinstance(bundle, dict)],
                        }
                    ],
                }
            )
        return described

    @classmethod
    def _format_runtime_group_texts(cls, groups: list[dict]) -> str:
        return format_sequence_groups(groups)

    @classmethod
    def _format_runtime_group_text(cls, group: dict) -> str:
        units = [dict(unit) for unit in group.get("units", []) if isinstance(unit, dict)]
        bundles = [dict(bundle) for bundle in group.get("csa_bundles", []) if isinstance(bundle, dict)]
        return format_group_display(units, bundles)

    @classmethod
    def _format_runtime_bundle_texts(cls, group: dict) -> list[str]:
        units_by_id = {
            str(unit.get("unit_id", "")): unit
            for unit in group.get("units", [])
            if isinstance(unit, dict) and str(unit.get("unit_id", ""))
        }
        displays = []
        for bundle in group.get("csa_bundles", []):
            if not isinstance(bundle, dict):
                continue
            member_tokens = [
                str(units_by_id.get(str(member_id), {}).get("token", ""))
                for member_id in bundle.get("member_unit_ids", [])
                if str(units_by_id.get(str(member_id), {}).get("token", ""))
            ]
            if member_tokens:
                displays.append(f"({' + '.join(member_tokens)})")
        return displays

    @staticmethod
    def _find_group(groups: list[dict], group_index: int) -> dict | None:
        for group in groups:
            if int(group.get("group_index", 0)) == int(group_index):
                return group
        return None

    @staticmethod
    def _flatten_remaining_units(groups: list[dict]) -> list[dict]:
        units = []
        for group in groups:
            for unit in group.get("units", []):
                units.append(dict(unit))
        return units

    @staticmethod
    def _collect_remaining_tokens(groups: list[dict]) -> list[str]:
        tokens = []
        for group in groups:
            for unit in sorted(group.get("units", []), key=lambda item: item.get("sequence_index", 0)):
                token = unit.get("token", "")
                if token:
                    tokens.append(token)
        return tokens

    def _collect_focus_window_groups(self, groups: list[dict], anchor_unit: dict) -> list[dict]:
        anchor_group = self._find_group(groups, int(anchor_unit.get("group_index", 0)))
        if not anchor_group:
            return []
        source_type = anchor_group.get("source_type", "")
        origin_frame_id = anchor_group.get("origin_frame_id", "")
        return [
            group
            for group in groups
            if group.get("source_type", "") == source_type and group.get("origin_frame_id", "") == origin_frame_id
        ]

    def _find_group_position(self, groups: list[dict], group_index: int) -> int:
        for position, group in enumerate(groups):
            if int(group.get("group_index", 0)) == int(group_index):
                return position
        return 0

    @staticmethod
    def _units_to_groups(units: list[dict]) -> list[dict]:
        grouped: dict[int, list[dict]] = {}
        order: list[int] = []
        for unit in units:
            key = int(unit.get("group_index", 0))
            if key not in grouped:
                grouped[key] = []
                order.append(key)
            grouped[key].append(dict(unit))
        return [
            {
                "group_index": key,
                "source_type": grouped[key][0].get("source_type", "") if grouped[key] else "",
                "origin_frame_id": grouped[key][0].get("origin_frame_id", "") if grouped[key] else "",
                "units": sorted(grouped[key], key=lambda item: int(item.get("sequence_index", 0))),
            }
            for key in order
        ]

    @staticmethod
    def _subtract_units(units: list[dict], matched_units: list[dict]) -> list[dict]:
        matched_ids = {str(unit.get("unit_id", "")) for unit in matched_units if str(unit.get("unit_id", ""))}
        return [dict(unit) for unit in units if str(unit.get("unit_id", "")) not in matched_ids]

    @staticmethod
    def _groups_in_span(groups: list[dict], span: list[int]) -> list[dict]:
        if not span or len(span) < 2:
            return []
        start = max(0, int(span[0]))
        end = max(start, min(len(groups), int(span[1])))
        return [groups[index] for index in range(start, end)]

    def _collect_matched_units(
        self,
        groups: list[dict],
        common_part: dict,
        *,
        use_existing_side: bool = False,
    ) -> list[dict]:
        matched_units = []
        group_index_key = "existing_group_index" if use_existing_side else "incoming_group_index"
        unit_refs_key = "existing_unit_refs" if use_existing_side else "incoming_unit_refs"
        similarity_map_key = "matched_existing_unit_similarities" if use_existing_side else "matched_incoming_unit_similarities"
        global_similarity_map = {
            str(unit_id): float(similarity)
            for unit_id, similarity in common_part.get(similarity_map_key, {}).items()
            if str(unit_id)
        }
        for pair in common_part.get("matched_pairs", []):
            group_index = int(pair.get(group_index_key, -1))
            if group_index < 0 or group_index >= len(groups):
                continue
            needed_ids = {str(unit_id) for unit_id in pair.get(unit_refs_key, []) if str(unit_id)}
            pair_similarity_map = {
                str(unit_id): float(similarity)
                for unit_id, similarity in pair.get(similarity_map_key, {}).items()
                if str(unit_id)
            }
            needed_tokens = Counter(str(token) for token in pair.get("common_tokens", []) if str(token))
            group_units = sorted(groups[group_index].get("units", []), key=lambda item: int(item.get("sequence_index", 0)))
            for unit in group_units:
                unit_id = str(unit.get("unit_id", ""))
                if needed_ids:
                    if unit_id in needed_ids:
                        similarity = pair_similarity_map.get(unit_id, global_similarity_map.get(unit_id, 1.0))
                        matched_units.append({**dict(unit), "match_similarity": round(max(0.0, min(1.0, float(similarity))), 8)})
                    continue
                token = str(unit.get("token", ""))
                if needed_tokens.get(token, 0) > 0:
                    needed_tokens[token] -= 1
                    similarity = global_similarity_map.get(unit_id, 1.0)
                    matched_units.append({**dict(unit), "match_similarity": round(max(0.0, min(1.0, float(similarity))), 8)})
        return matched_units

    def _apply_common_part_consumption(
        self,
        groups: list[dict],
        *,
        covered_units: list[dict],
        consume_fraction: float,
        prune_threshold: float,
    ) -> None:
        matched_ids = {str(unit.get("unit_id", "")) for unit in covered_units if str(unit.get("unit_id", ""))}
        effective_fraction = max(0.0, min(1.0, float(consume_fraction)))
        retained_groups = []
        for group in groups:
            retained_units = []
            for unit in group.get("units", []):
                unit_id = str(unit.get("unit_id", ""))
                cloned = dict(unit)
                if unit_id in matched_ids:
                    cloned["er"] = round(max(0.0, float(cloned.get("er", 0.0)) * (1.0 - effective_fraction)), 8)
                    cloned["ev"] = round(max(0.0, float(cloned.get("ev", 0.0)) * (1.0 - effective_fraction)), 8)
                    cloned["total_energy"] = round(float(cloned.get("er", 0.0)) + float(cloned.get("ev", 0.0)), 8)
                    if cloned["total_energy"] <= prune_threshold:
                        continue
                retained_units.append(cloned)
            if not retained_units:
                continue
            retained_groups.append({**group, "units": retained_units})
        groups[:] = retained_groups

    def _select_anchor_unit(self, remaining_units: list[dict]) -> dict | None:
        ranked = sorted(
            remaining_units,
            key=lambda unit: (-self._anchor_score(unit), int(unit.get("group_index", 0)), int(unit.get("sequence_index", 0))),
        )
        return dict(ranked[0]) if ranked else None

    def _anchor_score(self, unit: dict) -> float:
        er = float(unit.get("er", 0.0))
        ev = float(unit.get("ev", 0.0))
        score = (
            er * float(self._config.get("stimulus_anchor_er_weight", 1.25))
            + ev * float(self._config.get("stimulus_anchor_ev_weight", 0.9))
        )
        if unit.get("source_type") != "internal":
            score += float(self._config.get("stimulus_anchor_external_bonus", 0.08))
        if unit.get("is_punctuation"):
            score *= float(self._config.get("stimulus_anchor_punctuation_penalty", 0.35))
        else:
            score += float(self._config.get("stimulus_anchor_non_punctuation_bonus", 0.05))
        return round(score, 8)

    @staticmethod
    def _is_punctuation_token(token: str) -> bool:
        text = str(token or "").strip()
        if not text:
            return True
        for char in text:
            if char.isalnum() or "\u4e00" <= char <= "\u9fff":
                return False
        return True

    @staticmethod
    def _dedupe_preserve_order(items: list[str]) -> list[str]:
        seen = set()
        ordered = []
        for item in items:
            text = str(item)
            if not text or text in seen:
                continue
            seen.add(text)
            ordered.append(text)
        return ordered


