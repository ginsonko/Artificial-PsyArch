# -*- coding: utf-8 -*-
"""
Main entry for HDB.
"""

from __future__ import annotations

import os
import time
import traceback
from pathlib import Path
from typing import Any

from . import __module_name__, __schema_version__, __version__
from ._audit import AuditLogger
from ._cut_engine import CutEngine
from ._delete_engine import DeleteEngine
from ._episodic_store import EpisodicStore
from ._group_store import GroupStore
from ._logger import ModuleLogger
from ._maintenance import MaintenanceEngine
from ._memory_activation_store import MemoryActivationStore
from ._pointer_index import PointerIndex
from ._repair_engine import RepairEngine
from ._self_check import SelfCheckEngine
from ._snapshot_engine import SnapshotEngine
from ._induction_engine import InductionEngine
from ._stimulus_retrieval import StimulusRetrievalEngine
from ._storage_utils import ensure_dir, list_json_files, load_json_file, write_json_file
from ._structure_retrieval import StructureRetrievalEngine
from ._structure_store import StructureStore
from ._weight_engine import WeightEngine


def _parse_simple_yaml_scalar(raw: str) -> Any:
    text = raw.strip()
    if not text:
        return ""
    if len(text) >= 2 and text[0] == text[-1] and text[0] in {"\"", "'"}:
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



def _load_simple_yaml_config(path: str) -> dict:
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



def _load_yaml_config(path: str) -> dict:
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
    "data_dir": "",
    "stimulus_level_max_rounds": 6,
    "structure_level_max_rounds": 4,
    "top_n_attention_stub_default": 16,
    "stimulus_match_transfer_ratio": 1.0,
    "stimulus_competition_noise_mid": 0.01,
    "stimulus_competition_noise_scale": 0.004,
    "stimulus_competition_half_ratio": 0.1,
    "stimulus_competition_curve_power": 1.2,
    "stimulus_residual_min_energy": 0.05,
    "stimulus_attribute_energy_scale": 0.22,
    "stimulus_placeholder_energy_scale": 1.0,
    "stimulus_anchor_er_weight": 1.25,
    "stimulus_anchor_ev_weight": 0.9,
    "stimulus_anchor_external_bonus": 0.08,
    "stimulus_anchor_non_punctuation_bonus": 0.05,
    "stimulus_anchor_punctuation_penalty": 0.35,
    "stimulus_residual_projection_ratio": 0.35,
    "stimulus_atomic_seed_confidence": 0.95,
    "stimulus_anchor_seed_confidence": 0.9,
    "stimulus_focus_seed_confidence": 0.9,
    "stimulus_max_common_confidence": 0.86,
    "stimulus_overlap_residual_confidence": 0.68,
    "stimulus_residual_common_confidence": 0.78,
    "stimulus_residual_context_confidence": 0.7,
    "stimulus_extension_confidence": 0.74,
    "stimulus_overlap_residual_link_base_weight": 0.7,
    "stimulus_extension_link_base_weight": 0.75,
    "stimulus_atomic_extension_link_base_weight": 0.72,
    "structure_competition_noise_mid": 0.01,
    "structure_competition_noise_scale": 0.004,
    "structure_competition_half_ratio": 0.1,
    "structure_competition_curve_power": 1.15,
    "structure_path_runtime_scale": 1.35,
    "structure_path_runtime_gain": 0.3,
    "structure_anchor_runtime_scale": 1.35,
    "structure_anchor_runtime_gain": 0.22,
    "structure_anchor_temp_fatigue_step": 0.55,
    "structure_anchor_temp_fatigue_base": 0.7,
    "structure_anchor_temp_fatigue_rho_gain": 0.6,
    "structure_wave_similarity_floor": 0.35,
    "structure_descend_match_floor": 0.35,
    "structure_bias_er_ratio": 0.18,
    "structure_bias_ev_ratio": 0.28,
    "structure_profile_merge_alpha": 0.22,
    "structure_group_entry_reinforce_ratio": 0.15,
    "structure_group_entry_recent_gain_boost": 0.04,
    "structure_common_group_confidence": 0.82,
    "structure_memory_table_soft_limit": 128,
    "min_cut_common_length": 2,
    "diff_table_soft_limit": 128,
    "group_table_soft_limit": 128,
    "ev_propagation_threshold": 0.12,
    "er_induction_threshold": 0.15,
    "ev_propagation_ratio": 0.28,
    "er_induction_ratio": 0.22,
    "induction_target_top_k": 8,
    "memory_activation_decay_round_ratio_ev": 0.93,
    "memory_activation_prune_threshold_ev": 0.03,
    "memory_activation_event_history_limit": 24,
    "base_weight_er_gain": 0.08,
    "base_weight_ev_wear": 0.03,
    "weight_floor": 0.05,
    "recency_gain_boost": 0.08,
    "recency_gain_peak": 10.0,
    "recency_gain_hold_rounds": 2,
    "recency_gain_refresh_floor": 0.45,
    "recency_gain_decay_mode": "by_round",
    "recency_half_life_ms": 60000,
    "recency_gain_decay_ratio": 0.9999976974,
    "fatigue_cap": 1.5,
    "fatigue_increase_per_match": 0.08,
    "fatigue_decay_per_tick": 0.92,
    "fatigue_half_life_ms": 60000,
    "energy_decay_mode": "by_round",
    "energy_decay_round_ratio_er": 0.97,
    "energy_decay_round_ratio_ev": 0.93,
    "energy_decay_half_life_ms_er": 60000,
    "energy_decay_half_life_ms_ev": 30000,
    "enable_pointer_fallback": True,
    "fallback_lookup_max_candidates": 32,
    "fallback_scan_hard_limit": 200,
    "allow_global_scan_on_runtime_path": False,
    "lru_db_cache_size": 64,
    "numeric_bucket_max_per_family": 16,
    "numeric_bucket_neighbor_count": 2,
    "numeric_bucket_creation_abs_gap": 0.2,
    "numeric_bucket_creation_rel_gap": 0.35,
    "numeric_match_abs_tolerance": 0.2,
    "numeric_match_rel_tolerance": 0.35,
    "numeric_match_min_similarity": 0.4,
    "self_check_default_scope": "quick",
    "repair_batch_limit": 100,
    "repair_sleep_ms_between_batches": 10,
    "allow_delete_unrecoverable": True,
    "max_repair_runtime_ms": 30000,
    "enable_background_repair": True,
    "detail_log_dump_cut_summary": True,
    "detail_log_dump_group_match_profile": True,
    "detail_log_dump_pointer_fallback": True,
    "log_dir": "",
    "log_max_file_bytes": 5 * 1024 * 1024,
}


class HDB:
    def __init__(self, config_path: str = "", config_override: dict | None = None):
        self._config_path = config_path or os.path.join(os.path.dirname(__file__), "config", "hdb_config.yaml")
        self._config = self._build_config(config_override)
        self._paths = self._build_paths()

        self._logger = ModuleLogger(
            log_dir=self._config.get("log_dir", ""),
            max_file_bytes=int(self._config.get("log_max_file_bytes", 5 * 1024 * 1024)),
        )
        self._audit = AuditLogger(self._logger)
        self._weight = WeightEngine(self._config)
        self._cut = CutEngine()
        self._maintenance = MaintenanceEngine(self._config)
        self._structure_store = StructureStore(self._paths["structures"], self._paths["indexes"], self._config)
        self._group_store = GroupStore(self._paths["groups"], self._config)
        self._episodic_store = EpisodicStore(self._paths["episodic"])
        self._memory_activation_store = MemoryActivationStore(self._paths["memory_activation"], self._config)
        self._pointer_index = PointerIndex(self._config)
        self._cut.set_pointer_index(self._pointer_index)
        self._pointer_index.rebuild_from_store(self._structure_store)
        self._self_check = SelfCheckEngine(self._config)
        self._delete = DeleteEngine(self._config)
        self._repair = RepairEngine(self._config, self._paths["repair"], self._self_check)
        self._snapshot = SnapshotEngine(self._config)
        self._stimulus = StimulusRetrievalEngine(self._config, self._weight, self._logger, self._maintenance)
        self._structure_retrieval = StructureRetrievalEngine(self._config, self._weight, self._logger, self._maintenance)
        self._induction = InductionEngine(self._config, self._weight, self._logger, self._maintenance)

        self._issue_queue: list[dict] = self._load_issue_queue()
        self._repair.set_issue_callback(self._register_issue)
        self._load_repair_jobs()
        self._total_calls = 0

    def _build_config(self, config_override: dict | None) -> dict:
        config = dict(_DEFAULT_CONFIG)
        config.update(_load_yaml_config(self._config_path))
        if config_override:
            config.update(config_override)
        return config

    def _build_paths(self) -> dict[str, str]:
        data_dir = self._config.get("data_dir") or os.path.join(os.path.dirname(__file__), "data")
        data_dir = str(Path(data_dir))
        paths = {
            "data": data_dir,
            "episodic": os.path.join(data_dir, "episodic"),
            "structures": os.path.join(data_dir, "structures"),
            "groups": os.path.join(data_dir, "groups"),
            "indexes": os.path.join(data_dir, "indexes"),
            "repair": os.path.join(data_dir, "repair"),
            "cache": os.path.join(data_dir, "cache"),
            "memory_activation": os.path.join(data_dir, "memory_activation"),
        }
        for path in paths.values():
            ensure_dir(path)
        return paths

    def run_structure_level_retrieval_storage(
        self,
        *,
        state_snapshot: dict,
        trace_id: str,
        tick_id: str | None = None,
        attention_mode: str = "top_n_stub",
        top_n: int = 16,
        enable_storage: bool = True,
        enable_new_group_creation: bool = True,
        max_rounds: int | None = None,
        metadata: dict | None = None,
    ) -> dict:
        start_time = time.time()
        tick_id = tick_id or trace_id
        self._total_calls += 1
        err = self._validate_state_snapshot(state_snapshot, attention_mode, top_n)
        if err:
            return self._make_error_response("run_structure_level_retrieval_storage", err["code"], err["zh"], err["en"], trace_id, tick_id, start_time)
        try:
            result = self._structure_retrieval.run(
                state_snapshot=state_snapshot,
                trace_id=trace_id,
                tick_id=tick_id,
                structure_store=self._structure_store,
                group_store=self._group_store,
                pointer_index=self._pointer_index,
                cut_engine=self._cut,
                episodic_store=self._episodic_store,
                attention_mode=attention_mode,
                top_n=top_n,
                enable_storage=enable_storage,
                enable_new_group_creation=enable_new_group_creation,
                max_rounds=max_rounds or int(self._config.get("structure_level_max_rounds", 4)),
            )
            if result.get("fallback_used"):
                self._register_issue({"issue_type": "pointer_fallback_runtime", "target_id": "", "repair_suggestion": ["rebuild_pointer"]})
            self._logger.brief(
                trace_id=trace_id,
                tick_id=tick_id,
                interface="run_structure_level_retrieval_storage",
                success=result.get("code") == "OK",
                message_zh="结构级查存一体执行完成",
                message_en="Structure-level retrieval-storage completed",
                input_summary={"top_n": top_n, "attention_mode": attention_mode},
                output_summary={
                    "round_count": result.get("round_count", 0),
                    "matched_group_count": len(result.get("matched_group_ids", [])),
                    "new_group_count": len(result.get("new_group_ids", [])),
                    "bias_structure_count": len(result.get("bias_structure_ids", [])),
                },
            )
            return self._make_response(
                True,
                result.get("code", "OK"),
                "结构级查存一体执行成功 / Structure-level retrieval-storage completed successfully",
                data=result,
                trace_id=trace_id,
                tick_id=tick_id,
                elapsed_ms=self._elapsed_ms(start_time),
                interface="run_structure_level_retrieval_storage",
            )
        except Exception as exc:
            return self._make_exception_response("run_structure_level_retrieval_storage", exc, trace_id, tick_id, start_time)

    def run_stimulus_level_retrieval_storage(
        self,
        *,
        stimulus_packet: dict,
        trace_id: str,
        tick_id: str | None = None,
        top_n_attention_stub: int | None = None,
        source_module: str = "state_pool",
        enable_storage: bool = True,
        enable_new_structure_creation: bool = True,
        max_rounds: int | None = None,
        metadata: dict | None = None,
    ) -> dict:
        start_time = time.time()
        tick_id = tick_id or trace_id
        self._total_calls += 1
        err = self._validate_stimulus_packet(stimulus_packet, trace_id)
        if err:
            return self._make_error_response("run_stimulus_level_retrieval_storage", err["code"], err["zh"], err["en"], trace_id, tick_id, start_time)
        try:
            result = self._stimulus.run(
                stimulus_packet=stimulus_packet,
                trace_id=trace_id,
                tick_id=tick_id,
                structure_store=self._structure_store,
                pointer_index=self._pointer_index,
                cut_engine=self._cut,
                episodic_store=self._episodic_store,
                enable_storage=enable_storage,
                enable_new_structure_creation=enable_new_structure_creation,
                max_rounds=max_rounds or int(self._config.get("stimulus_level_max_rounds", 6)),
            )
            if result.get("fallback_used"):
                self._register_issue({"issue_type": "pointer_fallback_runtime", "target_id": "", "repair_suggestion": ["rebuild_pointer"]})
            self._logger.brief(
                trace_id=trace_id,
                tick_id=tick_id,
                interface="run_stimulus_level_retrieval_storage",
                success=result.get("code") == "OK",
                message_zh="刺激级查存一体执行完成",
                message_en="Stimulus-level retrieval-storage completed",
                input_summary={"packet_id": stimulus_packet.get("id", ""), "source_module": source_module},
                output_summary={
                    "round_count": result.get("round_count", 0),
                    "matched_structure_count": len(result.get("matched_structure_ids", [])),
                    "new_structure_count": len(result.get("new_structure_ids", [])),
                    "remaining_stimulus_sa_count": result.get("remaining_stimulus_sa_count", 0),
                },
            )
            return self._make_response(
                True,
                result.get("code", "OK"),
                "刺激级查存一体执行成功 / Stimulus-level retrieval-storage completed successfully",
                data=result,
                trace_id=trace_id,
                tick_id=tick_id,
                elapsed_ms=self._elapsed_ms(start_time),
                interface="run_stimulus_level_retrieval_storage",
            )
        except Exception as exc:
            return self._make_exception_response("run_stimulus_level_retrieval_storage", exc, trace_id, tick_id, start_time)

    def run_induction_propagation(
        self,
        *,
        state_snapshot: dict,
        trace_id: str,
        tick_id: str | None = None,
        max_source_items: int | None = None,
        enable_ev_propagation: bool = True,
        enable_er_induction: bool = True,
        metadata: dict | None = None,
    ) -> dict:
        start_time = time.time()
        tick_id = tick_id or trace_id
        self._total_calls += 1
        err = self._validate_state_snapshot(state_snapshot, "top_n_stub", 1)
        if err:
            return self._make_error_response("run_induction_propagation", err["code"], err["zh"], err["en"], trace_id, tick_id, start_time)
        if not enable_ev_propagation and not enable_er_induction:
            return self._make_error_response("run_induction_propagation", "INPUT_VALIDATION_ERROR", "至少启用一种感应赋能模式", "At least one induction mode must be enabled", trace_id, tick_id, start_time)
        try:
            result = self._induction.run(
                state_snapshot=state_snapshot,
                trace_id=trace_id,
                tick_id=tick_id,
                structure_store=self._structure_store,
                episodic_store=self._episodic_store,
                pointer_index=self._pointer_index,
                cut_engine=self._cut,
                max_source_items=max_source_items,
                enable_ev_propagation=enable_ev_propagation,
                enable_er_induction=enable_er_induction,
            )
            if result.get("fallback_used"):
                self._register_issue({"issue_type": "pointer_fallback_runtime", "target_id": "", "repair_suggestion": ["rebuild_pointer"]})
            self._logger.brief(
                trace_id=trace_id,
                tick_id=tick_id,
                interface="run_induction_propagation",
                success=result.get("code") == "OK",
                message_zh="感应赋能执行完成",
                message_en="Induction propagation completed",
                input_summary={"max_source_items": max_source_items, "enable_ev_propagation": enable_ev_propagation, "enable_er_induction": enable_er_induction},
                output_summary={
                    "source_item_count": result.get("source_item_count", 0),
                    "propagated_target_count": result.get("propagated_target_count", 0),
                    "induced_target_count": result.get("induced_target_count", 0),
                    "total_delta_ev": result.get("total_delta_ev", 0.0),
                },
            )
            return self._make_response(
                True,
                result.get("code", "OK"),
                "感应赋能执行成功 / Induction propagation completed successfully",
                data=result,
                trace_id=trace_id,
                tick_id=tick_id,
                elapsed_ms=self._elapsed_ms(start_time),
                interface="run_induction_propagation",
            )
        except Exception as exc:
            return self._make_exception_response("run_induction_propagation", exc, trace_id, tick_id, start_time)

    def query_structure_database(
        self,
        *,
        structure_id: str,
        trace_id: str,
        include_diff_table: bool = True,
        include_group_table: bool = True,
        limit: int | None = None,
    ) -> dict:
        start_time = time.time()
        self._total_calls += 1
        if not structure_id:
            return self._make_error_response("query_structure_database", "INPUT_VALIDATION_ERROR", "structure_id 不能为空", "structure_id is required", trace_id, "", start_time)
        structure_obj = self._structure_store.get(structure_id)
        if not structure_obj:
            return self._make_error_response("query_structure_database", "STATE_ERROR", f"结构不存在: {structure_id}", f"Structure not found: {structure_id}", trace_id, "", start_time)
        structure_db, pointer_info = self._pointer_index.resolve_db(structure_obj=structure_obj, structure_store=self._structure_store, logger=self._logger, trace_id=trace_id, tick_id="")
        if not structure_db:
            self._register_issue({"issue_type": "missing_primary_pointer", "target_id": structure_id, "repair_suggestion": ["rebuild_pointer"]})
            return self._make_error_response("query_structure_database", "STATE_ERROR", f"结构数据库不存在: {structure_id}", f"Structure database missing: {structure_id}", trace_id, "", start_time)
        payload = {
            "structure": structure_obj,
            "structure_db": {
                "structure_db_id": structure_db.get("structure_db_id", ""),
                "owner_structure_id": structure_db.get("owner_structure_id", ""),
                "integrity": structure_db.get("integrity", {}),
            },
            "pointer_info": pointer_info,
        }
        if include_diff_table:
            diff_table = []
            for entry in list(structure_db.get("diff_table", []))[: limit or None]:
                enriched_entry = dict(entry)
                target_id = enriched_entry.get("target_id", "")
                target_structure = self._structure_store.get(target_id) if target_id else None
                if target_structure:
                    enriched_entry["target_display_text"] = target_structure.get("structure", {}).get("display_text", target_id)
                    enriched_entry["target_signature"] = target_structure.get("structure", {}).get("content_signature", "")
                    enriched_entry["target_structure_stats"] = self._resolve_structure_ref(target_id)
                diff_table.append(enriched_entry)
            payload["structure_db"]["diff_table"] = diff_table
        if include_group_table:
            group_table = []
            for entry in list(structure_db.get("group_table", []))[: limit or None]:
                enriched_entry = dict(entry)
                group_id = enriched_entry.get("group_id", "")
                group_obj = self._group_store.get(group_id) if group_id else None
                if group_obj:
                    enriched_entry["group_stats"] = {
                        "base_weight": round(float(group_obj.get("stats", {}).get("base_weight", 1.0)), 8),
                        "recent_gain": round(float(group_obj.get("stats", {}).get("recent_gain", 1.0)), 8),
                        "fatigue": round(float(group_obj.get("stats", {}).get("fatigue", 0.0)), 8),
                    }
                    enriched_entry["required_structures"] = self._resolve_structure_refs(group_obj.get("required_structure_ids", []))
                    enriched_entry["bias_structures"] = self._resolve_structure_refs(group_obj.get("bias_structure_ids", []))
                group_table.append(enriched_entry)
            payload["structure_db"]["group_table"] = group_table
        return self._make_response(True, "OK", "结构数据库查询成功 / Structure database queried successfully", data=payload, trace_id=trace_id, elapsed_ms=self._elapsed_ms(start_time), interface="query_structure_database")

    def query_group(self, *, group_id: str, trace_id: str) -> dict:
        start_time = time.time()
        group_obj = self._group_store.get(group_id)
        if not group_obj:
            return self._make_error_response("query_group", "STATE_ERROR", f"结构组不存在: {group_id}", f"Group not found: {group_id}", trace_id, "", start_time)
        return self._make_response(
            True,
            "OK",
            "结构组查询成功 / Group queried successfully",
            data={
                "group": group_obj,
                "required_structures": self._resolve_structure_refs(group_obj.get("required_structure_ids", [])),
                "bias_structures": self._resolve_structure_refs(group_obj.get("bias_structure_ids", [])),
            },
            trace_id=trace_id,
            elapsed_ms=self._elapsed_ms(start_time),
            interface="query_group",
        )

    def append_episodic_memory(self, *, episodic_payload: dict, trace_id: str, tick_id: str | None = None) -> dict:
        start_time = time.time()
        tick_id = tick_id or trace_id
        event_summary = episodic_payload.get("event_summary", "")
        if not event_summary:
            return self._make_error_response("append_episodic_memory", "INPUT_VALIDATION_ERROR", "event_summary 不能为空", "event_summary is required", trace_id, tick_id, start_time)
        if not episodic_payload.get("timestamp_range") and not episodic_payload.get("created_at"):
            episodic_payload = dict(episodic_payload)
            episodic_payload["created_at"] = int(time.time() * 1000)
        item = self._episodic_store.append(episodic_payload, trace_id=trace_id, tick_id=tick_id)
        self._logger.brief(trace_id=trace_id, tick_id=tick_id, interface="append_episodic_memory", success=True, message_zh="情景记忆追加写成功", message_en="Episodic memory appended", input_summary={"event_summary": event_summary}, output_summary={"episodic_id": item.get("id", "")})
        return self._make_response(True, "OK", "情景记忆追加写成功 / Episodic memory appended successfully", data={"episodic_id": item.get("id", "")}, trace_id=trace_id, tick_id=tick_id, elapsed_ms=self._elapsed_ms(start_time), interface="append_episodic_memory")

    def delete_structure(self, *, structure_id: str, trace_id: str, delete_mode: str = "safe_detach", operator: str | None = None) -> dict:
        start_time = time.time()
        if delete_mode not in {"safe_detach", "force_delete"}:
            return self._make_error_response("delete_structure", "INPUT_VALIDATION_ERROR", f"delete_mode 不合法: {delete_mode}", f"Invalid delete_mode: {delete_mode}", trace_id, "", start_time)
        if delete_mode == "force_delete":
            self._audit.record(trace_id=trace_id, interface="delete_structure", action="force_delete_structure", reason="force_delete", operator=operator or "unknown", detail={"structure_id": structure_id})
        result = self._delete.delete_structure(
            structure_id=structure_id,
            delete_mode=delete_mode,
            structure_store=self._structure_store,
            group_store=self._group_store,
            pointer_index=self._pointer_index,
            issue_callback=self._register_issue,
        )
        return self._make_response(True, "OK", "结构删除执行完成 / Structure deletion completed", data=result, trace_id=trace_id, elapsed_ms=self._elapsed_ms(start_time), interface="delete_structure")

    def clear_hdb(self, *, trace_id: str, reason: str, operator: str | None = None, clear_mode: str = "full") -> dict:
        start_time = time.time()
        if clear_mode not in {"full", "episodic_only", "structures_only", "groups_only"}:
            return self._make_error_response("clear_hdb", "INPUT_VALIDATION_ERROR", f"clear_mode 不合法: {clear_mode}", f"Invalid clear_mode: {clear_mode}", trace_id, "", start_time)
        result = self._delete.clear_hdb(
            clear_mode=clear_mode,
            structure_store=self._structure_store,
            group_store=self._group_store,
            episodic_store=self._episodic_store,
            memory_activation_store=self._memory_activation_store,
            pointer_index=self._pointer_index,
            issue_queue=self._issue_queue,
            repair_jobs=self._repair.jobs,
            repair_dir=self._paths["repair"],
        )
        self._save_issue_queue()
        self._audit.record(trace_id=trace_id, interface="clear_hdb", action="clear_hdb", reason=reason, operator=operator or "unknown", detail={"clear_mode": clear_mode, **result})
        return self._make_response(True, "OK", "HDB 清空完成 / HDB cleared successfully", data=result, trace_id=trace_id, elapsed_ms=self._elapsed_ms(start_time), interface="clear_hdb")

    def self_check_hdb(
        self,
        *,
        trace_id: str,
        target_id: str | None = None,
        check_scope: str = "quick",
        max_items: int | None = None,
        include_orphans: bool = True,
        allow_fallback_scan: bool = False,
    ) -> dict:
        start_time = time.time()
        result = self._self_check.run(
            structure_store=self._structure_store,
            group_store=self._group_store,
            episodic_store=self._episodic_store,
            memory_activation_store=self._memory_activation_store,
            pointer_index=self._pointer_index,
            trace_id=trace_id,
            target_id=target_id,
            check_scope=check_scope,
            max_items=max_items,
            include_orphans=include_orphans,
        )
        return self._make_response(True, "OK", "HDB 自检完成 / HDB self-check completed", data=result, trace_id=trace_id, elapsed_ms=self._elapsed_ms(start_time), interface="self_check_hdb")

    def repair_hdb(
        self,
        *,
        trace_id: str,
        target_id: str | None = None,
        repair_scope: str = "targeted",
        repair_actions: list[str] | None = None,
        batch_limit: int = 100,
        allow_delete_unrecoverable: bool = True,
        background: bool = False,
    ) -> dict:
        start_time = time.time()
        if repair_scope not in {"targeted", "global_quick", "global_full"}:
            return self._make_error_response("repair_hdb", "INPUT_VALIDATION_ERROR", f"repair_scope 不合法: {repair_scope}", f"Invalid repair_scope: {repair_scope}", trace_id, "", start_time)
        if (repair_scope == "global_full" or allow_delete_unrecoverable) and background:
            self._audit.record(trace_id=trace_id, interface="repair_hdb", action="repair_hdb_background", reason=repair_scope, detail={"allow_delete_unrecoverable": allow_delete_unrecoverable})
        result = self._repair.start_or_run(
            trace_id=trace_id,
            structure_store=self._structure_store,
            group_store=self._group_store,
            episodic_store=self._episodic_store,
            memory_activation_store=self._memory_activation_store,
            pointer_index=self._pointer_index,
            delete_engine=self._delete,
            target_id=target_id,
            repair_scope=repair_scope,
            repair_actions=repair_actions,
            batch_limit=batch_limit,
            allow_delete_unrecoverable=allow_delete_unrecoverable,
            background=background,
        )
        message = "HDB 修复任务已提交 / HDB repair started"
        if not background or result.get("status") in {"completed", "stopped", "failed", "timeout"}:
            message = "HDB 修复执行完成 / HDB repair completed"
        return self._make_response(True, "OK", message, data=result, trace_id=trace_id, elapsed_ms=self._elapsed_ms(start_time), interface="repair_hdb")

    def stop_repair_job(self, *, repair_job_id: str, trace_id: str) -> dict:
        start_time = time.time()
        result = self._repair.stop_job(repair_job_id)
        if not result.get("success"):
            return self._make_error_response("stop_repair_job", result.get("code", "STATE_ERROR"), "修复任务不存在", "Repair job not found", trace_id, "", start_time)
        return self._make_response(True, "OK", "修复任务停止请求已发送 / Repair job stop requested", data=result, trace_id=trace_id, elapsed_ms=self._elapsed_ms(start_time), interface="stop_repair_job")

    def get_hdb_snapshot(
        self,
        *,
        trace_id: str,
        include_stats: bool = True,
        include_recent_structures: bool = True,
        include_recent_groups: bool = True,
        top_k: int = 10,
    ) -> dict:
        start_time = time.time()
        now_ms = int(time.time() * 1000)
        for item in self._structure_store.get_recent_structures(limit=top_k):
            self._weight.decay_structure(item, now_ms=now_ms, round_step=1)
            self._structure_store.update_structure(item)
        for item in self._group_store.get_recent(limit=top_k):
            self._weight.decay_group(item, now_ms=now_ms, round_step=1)
            self._group_store.update(item)
        snapshot = self._snapshot.build_hdb_snapshot(
            trace_id=trace_id,
            structure_store=self._structure_store,
            group_store=self._group_store,
            episodic_store=self._episodic_store,
            memory_activation_store=self._memory_activation_store,
            pointer_index=self._pointer_index,
            issue_queue=self._issue_queue,
            repair_jobs=self._repair.jobs,
            top_k=top_k,
            include_stats=include_stats,
            include_recent_structures=include_recent_structures,
            include_recent_groups=include_recent_groups,
        )
        return self._make_response(True, "OK", "HDB 快照获取成功 / HDB snapshot retrieved", data=snapshot, trace_id=trace_id, elapsed_ms=self._elapsed_ms(start_time), interface="get_hdb_snapshot")

    def get_recent_episodic(self, *, trace_id: str, limit: int = 10) -> dict:
        start_time = time.time()
        items = []
        for item in self._episodic_store.get_recent(limit=limit):
            enriched = dict(item)
            enriched["structure_ref_items"] = self._resolve_structure_refs(item.get("structure_refs", []))
            enriched["group_ref_items"] = self._resolve_group_refs(item.get("group_refs", []))
            items.append(enriched)
        return self._make_response(True, "OK", "最近情景记忆获取成功 / Recent episodic memories retrieved", data={"items": items}, trace_id=trace_id, elapsed_ms=self._elapsed_ms(start_time), interface="get_recent_episodic")

    def apply_memory_activation_targets(
        self,
        *,
        targets: list[dict],
        trace_id: str,
        tick_id: str | None = None,
    ) -> dict:
        start_time = time.time()
        tick_id = tick_id or trace_id
        result = self._memory_activation_store.apply_targets(
            targets=targets,
            episodic_store=self._episodic_store,
            trace_id=trace_id,
            tick_id=tick_id,
        )
        for item in result.get("items", []):
            item["structure_ref_items"] = self._resolve_structure_refs(item.get("structure_refs", []))
            item["group_ref_items"] = self._resolve_group_refs(item.get("group_refs", []))
        self._logger.brief(
            trace_id=trace_id,
            tick_id=tick_id,
            interface="apply_memory_activation_targets",
            success=True,
            message_zh="记忆赋能池更新完成",
            message_en="Memory activation pool updated",
            input_summary={"target_count": len(targets or [])},
            output_summary={
                "applied_count": result.get("applied_count", 0),
                "total_delta_er": result.get("total_delta_er", 0.0),
                "total_delta_ev": result.get("total_delta_ev", 0.0),
                "total_delta_energy": result.get("total_delta_energy", 0.0),
            },
        )
        return self._make_response(
            True,
            "OK",
            "记忆赋能池更新完成 / Memory activation pool updated",
            data=result,
            trace_id=trace_id,
            tick_id=tick_id,
            elapsed_ms=self._elapsed_ms(start_time),
            interface="apply_memory_activation_targets",
        )

    def tick_memory_activation_pool(self, *, trace_id: str, tick_id: str | None = None) -> dict:
        start_time = time.time()
        tick_id = tick_id or trace_id
        result = self._memory_activation_store.tick(trace_id=trace_id, tick_id=tick_id)
        return self._make_response(
            True,
            "OK",
            "记忆赋能池维护完成 / Memory activation pool maintenance completed",
            data=result,
            trace_id=trace_id,
            tick_id=tick_id,
            elapsed_ms=self._elapsed_ms(start_time),
            interface="tick_memory_activation_pool",
        )

    def get_memory_activation_snapshot(
        self,
        *,
        trace_id: str,
        limit: int = 16,
        sort_by: str = "energy_desc",
    ) -> dict:
        start_time = time.time()
        result = self._memory_activation_store.snapshot(
            episodic_store=self._episodic_store,
            limit=limit,
            sort_by=sort_by,
        )
        for item in result.get("items", []):
            item["structure_ref_items"] = self._resolve_structure_refs(item.get("structure_refs", []))
            item["group_ref_items"] = self._resolve_group_refs(item.get("group_refs", []))
        return self._make_response(
            True,
            "OK",
            "记忆赋能池快照获取成功 / Memory activation snapshot retrieved",
            data=result,
            trace_id=trace_id,
            elapsed_ms=self._elapsed_ms(start_time),
            interface="get_memory_activation_snapshot",
        )

    def query_memory_activation(self, *, memory_id: str, trace_id: str) -> dict:
        start_time = time.time()
        if not memory_id:
            return self._make_error_response(
                "query_memory_activation",
                "INPUT_VALIDATION_ERROR",
                "memory_id 不能为空",
                "memory_id is required",
                trace_id,
                "",
                start_time,
            )
        item = self._memory_activation_store.query(memory_id=memory_id, episodic_store=self._episodic_store)
        if item is None:
            return self._make_error_response(
                "query_memory_activation",
                "STATE_ERROR",
                f"记忆赋能条目不存在: {memory_id}",
                f"Memory activation entry not found: {memory_id}",
                trace_id,
                "",
                start_time,
            )
        item["structure_ref_items"] = self._resolve_structure_refs(item.get("structure_refs", []))
        item["group_ref_items"] = self._resolve_group_refs(item.get("group_refs", []))
        return self._make_response(
            True,
            "OK",
            "记忆赋能条目查询成功 / Memory activation entry queried successfully",
            data={"item": item},
            trace_id=trace_id,
            elapsed_ms=self._elapsed_ms(start_time),
            interface="query_memory_activation",
        )

    def record_memory_feedback(
        self,
        *,
        feedback_items: list[dict],
        trace_id: str,
        tick_id: str | None = None,
    ) -> dict:
        start_time = time.time()
        tick_id = tick_id or trace_id
        result = self._memory_activation_store.record_feedback(
            feedback_items=feedback_items,
            episodic_store=self._episodic_store,
            trace_id=trace_id,
            tick_id=tick_id,
        )
        for item in result.get("items", []):
            item["structure_ref_items"] = self._resolve_structure_refs(item.get("structure_refs", []))
            item["group_ref_items"] = self._resolve_group_refs(item.get("group_refs", []))
        return self._make_response(
            True,
            "OK",
            "记忆反哺事件记录完成 / Memory feedback events recorded",
            data=result,
            trace_id=trace_id,
            tick_id=tick_id,
            elapsed_ms=self._elapsed_ms(start_time),
            interface="record_memory_feedback",
        )

    def build_internal_stimulus_packet(self, fragments: list[dict], trace_id: str, tick_id: str | None = None) -> dict:
        return self._cut.build_internal_stimulus_packet(fragments, trace_id=trace_id, tick_id=tick_id or trace_id)

    def merge_stimulus_packets(self, external_packet: dict | None, internal_packet: dict | None, trace_id: str, tick_id: str | None = None) -> dict:
        return self._cut.merge_stimulus_packets(external_packet, internal_packet, trace_id=trace_id, tick_id=tick_id or trace_id)

    def make_runtime_structure_object(self, structure_id: str, er: float, ev: float, reason: str = "hdb_projection") -> dict | None:
        return self._structure_store.make_runtime_object(structure_id, er=er, ev=ev, reason=reason)

    def make_runtime_memory_object(
        self,
        memory_id: str,
        er: float,
        ev: float,
        reason: str = "hdb_memory_projection",
        display_text: str = "",
        backing_structure_id: str = "",
    ) -> dict | None:
        episodic_obj = self._episodic_store.get(memory_id)
        if episodic_obj is None:
            return None
        runtime_display = (
            str(display_text or "")
            or str(episodic_obj.get("meta", {}).get("ext", {}).get("display_text", ""))
            or str(episodic_obj.get("event_summary", ""))
            or str(memory_id)
        )
        return {
            "id": memory_id,
            "object_type": "em",
            "sub_type": episodic_obj.get("sub_type", "tick_episode"),
            "content": {
                "raw": runtime_display,
                "display": runtime_display,
                "normalized": runtime_display,
            },
            "energy": {
                "er": round(float(er), 6),
                "ev": round(float(ev), 6),
            },
            "memory": {
                "memory_id": memory_id,
                "event_summary": episodic_obj.get("event_summary", ""),
                "structure_refs": list(episodic_obj.get("structure_refs", [])),
                "group_refs": list(episodic_obj.get("group_refs", [])),
                "backing_structure_id": str(backing_structure_id or ""),
                "display_text": runtime_display,
            },
            "source": {
                "module": __module_name__,
                "interface": "make_runtime_memory_object",
                "origin": reason or "hdb_memory_projection",
                "origin_id": memory_id,
                "parent_ids": list(episodic_obj.get("structure_refs", [])),
            },
            "created_at": episodic_obj.get("created_at", int(time.time() * 1000)),
            "updated_at": int(time.time() * 1000),
        }

    def reload_config(self, *, trace_id: str, config_path: str | None = None, apply_partial: bool = True) -> dict:
        start_time = time.time()
        path = config_path or self._config_path
        raw = _load_yaml_config(path)
        if not raw:
            return self._make_error_response("reload_config", "CONFIG_ERROR", f"配置加载失败或为空: {path}", f"Config failed to load or empty: {path}", trace_id, "", start_time)
        applied = []
        rejected = []
        for key, value in raw.items():
            if key in _DEFAULT_CONFIG:
                self._config[key] = value
                applied.append(key)
            else:
                rejected.append(key)
        self._weight.update_config(self._config)
        self._pointer_index.update_config(self._config)
        self._maintenance.update_config(self._config)
        self._snapshot.update_config(self._config)
        self._stimulus.update_config(self._config)
        self._structure_retrieval.update_config(self._config)
        self._induction.update_config(self._config)
        self._structure_store.update_config(self._config)
        self._group_store.update_config(self._config)
        self._memory_activation_store.update_config(self._config)
        self._self_check.update_config(self._config)
        self._delete.update_config(self._config)
        self._repair.update_config(self._config)
        self._logger.update_config(log_dir=self._config.get("log_dir", ""), max_file_bytes=int(self._config.get("log_max_file_bytes", 0)))
        return self._make_response(True, "OK", "配置热加载完成 / Config hot reload done", data={"applied": applied, "rejected": rejected}, trace_id=trace_id, elapsed_ms=self._elapsed_ms(start_time), interface="reload_config")

    def close(self) -> None:
        self._save_issue_queue()
        self._logger.close()

    def _load_issue_queue(self) -> list[dict]:
        issues_path = os.path.join(self._paths["repair"], "issues.json")
        payload = load_json_file(issues_path, default=[])
        return payload if isinstance(payload, list) else []

    def _save_issue_queue(self) -> None:
        issues_path = os.path.join(self._paths["repair"], "issues.json")
        write_json_file(issues_path, self._issue_queue)

    def _load_repair_jobs(self) -> None:
        for path in list_json_files(self._paths["repair"]):
            payload = load_json_file(path, default=None)
            if isinstance(payload, dict) and payload.get("repair_job_id"):
                self._repair.jobs[payload["repair_job_id"]] = payload

    def _register_issue(self, issue: dict) -> dict:
        issue = dict(issue)
        issue.setdefault("issue_id", f"hdb_issue_{len(self._issue_queue) + 1:06d}")
        issue.setdefault("created_at", int(time.time() * 1000))
        self._issue_queue.append(issue)
        if len(self._issue_queue) > 5000:
            self._issue_queue = self._issue_queue[-5000:]
        self._save_issue_queue()
        return issue

    def _resolve_structure_ref(self, structure_id: str) -> dict:
        structure_obj = self._structure_store.get(structure_id)
        if not structure_obj:
            return {
                "structure_id": structure_id,
                "display_text": structure_id,
                "content_signature": "",
                "base_weight": 0.0,
                "recent_gain": 1.0,
                "fatigue": 0.0,
                "exists": False,
            }
        stats = structure_obj.get("stats", {})
        return {
            "structure_id": structure_id,
            "display_text": structure_obj.get("structure", {}).get("display_text", structure_id),
            "content_signature": structure_obj.get("structure", {}).get("content_signature", ""),
            "base_weight": round(float(stats.get("base_weight", 1.0)), 8),
            "recent_gain": round(float(stats.get("recent_gain", 1.0)), 8),
            "fatigue": round(float(stats.get("fatigue", 0.0)), 8),
            "exists": True,
        }

    def _resolve_structure_refs(self, structure_ids: list[str]) -> list[dict]:
        return [self._resolve_structure_ref(structure_id) for structure_id in structure_ids or []]

    def _resolve_group_refs(self, group_ids: list[str]) -> list[dict]:
        refs = []
        for group_id in group_ids or []:
            group_obj = self._group_store.get(group_id)
            if not group_obj:
                refs.append(
                    {
                        "group_id": group_id,
                        "required_structures": [],
                        "bias_structures": [],
                        "exists": False,
                    }
                )
                continue
            refs.append(
                {
                    "group_id": group_id,
                    "required_structures": self._resolve_structure_refs(group_obj.get("required_structure_ids", [])),
                    "bias_structures": self._resolve_structure_refs(group_obj.get("bias_structure_ids", [])),
                    "exists": True,
                }
            )
        return refs

    def _validate_stimulus_packet(self, stimulus_packet: Any, trace_id: str) -> dict | None:
        if not isinstance(stimulus_packet, dict):
            return {"code": "INPUT_VALIDATION_ERROR", "zh": "stimulus_packet 必须是 dict", "en": "stimulus_packet must be a dict"}
        if stimulus_packet.get("object_type") != "stimulus_packet":
            return {"code": "INPUT_VALIDATION_ERROR", "zh": "stimulus_packet.object_type 必须为 stimulus_packet", "en": "stimulus_packet.object_type must be stimulus_packet"}
        for field in ("sa_items", "csa_items", "grouped_sa_sequences"):
            if field not in stimulus_packet:
                return {"code": "INPUT_VALIDATION_ERROR", "zh": f"stimulus_packet 缺少字段: {field}", "en": f"stimulus_packet missing field: {field}"}
        if not trace_id:
            return {"code": "INPUT_VALIDATION_ERROR", "zh": "trace_id 不能为空", "en": "trace_id is required"}
        return None

    def _validate_state_snapshot(self, state_snapshot: Any, attention_mode: str, top_n: int) -> dict | None:
        if not isinstance(state_snapshot, dict):
            return {"code": "INPUT_VALIDATION_ERROR", "zh": "state_snapshot 必须是 dict", "en": "state_snapshot must be a dict"}
        if "summary" not in state_snapshot:
            return {"code": "INPUT_VALIDATION_ERROR", "zh": "state_snapshot 缺少 summary", "en": "state_snapshot missing summary"}
        if "top_items" not in state_snapshot and "items" not in state_snapshot:
            return {"code": "INPUT_VALIDATION_ERROR", "zh": "state_snapshot 缺少 top_items 或 items", "en": "state_snapshot missing top_items or items"}
        # 注意力模式（attention_mode）说明：
        # - top_n_stub: 旧版占位口径（仍保留兼容）
        # - cam_snapshot: 正式口径，表示 state_snapshot 本身就是 CAM（当前注意记忆体）的输出快照
        if attention_mode not in {"top_n_stub", "cam_snapshot"}:
            return {"code": "NOT_IMPLEMENTED_ERROR", "zh": f"attention_mode 尚未实现: {attention_mode}", "en": f"attention_mode not implemented: {attention_mode}"}
        if top_n <= 0:
            return {"code": "INPUT_VALIDATION_ERROR", "zh": "top_n 必须大于 0", "en": "top_n must be greater than 0"}
        return None

    def _make_exception_response(self, interface: str, exc: Exception, trace_id: str, tick_id: str, start_time: float) -> dict:
        self._logger.error(trace_id=trace_id, tick_id=tick_id, interface=interface, code="INTERNAL_ERROR", message_zh=f"内部异常: {exc}", message_en=f"Internal exception: {exc}", detail={"traceback": traceback.format_exc()})
        return self._make_response(False, "INTERNAL_ERROR", f"内部异常 / Internal exception: {exc}", error={"message": str(exc)}, trace_id=trace_id, tick_id=tick_id, elapsed_ms=self._elapsed_ms(start_time), interface=interface)

    def _make_error_response(self, interface: str, code: str, zh: str, en: str, trace_id: str, tick_id: str, start_time: float) -> dict:
        self._logger.error(trace_id=trace_id, tick_id=tick_id, interface=interface, code=code, message_zh=zh, message_en=en)
        return self._make_response(False, code, f"{zh} / {en}", error={"code": code, "message_zh": zh, "message_en": en}, trace_id=trace_id, tick_id=tick_id, elapsed_ms=self._elapsed_ms(start_time), interface=interface)

    @staticmethod
    def _elapsed_ms(start_time: float) -> int:
        return int((time.time() - start_time) * 1000)

    @staticmethod
    def _make_response(success: bool, code: str, message: str, *, data: Any = None, error: Any = None, trace_id: str = "", tick_id: str = "", elapsed_ms: int = 0, interface: str = "") -> dict:
        return {
            "success": success,
            "code": code,
            "message": message,
            "data": data,
            "error": error,
            "meta": {
                "module": __module_name__,
                "interface": interface,
                "trace_id": trace_id,
                "tick_id": tick_id,
                "elapsed_ms": elapsed_ms,
                "logged": True,
                "version": __version__,
                "schema_version": __schema_version__,
            },
        }






