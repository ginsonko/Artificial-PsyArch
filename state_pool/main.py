# -*- coding: utf-8 -*-
"""
AP 状态池模块（State Pool Module, SPM）— 主模块
=================================================
AP 运行态认知层核心中枢。负责维护当前活跃认知图景。

对外接口 (8个):
  1. apply_stimulus_packet()          — 接收刺激包
  2. apply_energy_update()            — 定向能量更新
  3. bind_attribute_node_to_object()  — 属性绑定
  4. insert_runtime_node()            — 手动插入运行态对象
  5. tick_maintain_state_pool()       — Tick 维护
  6. get_state_snapshot()             — 状态快照
  7. reload_config()                  — 热加载配置
  8. clear_state_pool()               — 清空状态池

职责边界:
  ✓ 接收刺激、维护对象、更新能量与认知压、衰减/中和/淘汰/合并
  ✓ 属性绑定、脚本检查抄送、快照输出、占位接口联调
  ✗ 不负责感受器残响、长期存储(HDB)、脚本判断、情绪更新、行动决策
"""

import os
import time
import traceback
import copy
from pathlib import Path
from typing import Any

# ---- 子模块 ----
from ._pool_store import PoolStore
from ._state_item_builder import build_state_item, SUPPORTED_REF_TYPES
from ._energy_engine import EnergyEngine
from ._neutralization_engine import NeutralizationEngine
from ._merge_engine import MergeEngine
from ._binding_engine_v2 import BindingEngine
from ._maintenance_engine import MaintenanceEngine
from ._snapshot_engine import SnapshotEngine
from ._history_window import HistoryWindow
from ._logger import ModuleLogger
from ._audit import AuditLogger
from . import __version__, __schema_version__, __module_name__


# ====================================================================== #
#                          配置加载工具                                    #
# ====================================================================== #

def _load_yaml_config(path: str) -> dict:
    """加载 YAML 配置文件。加载失败返回空 dict。"""
    try:
        import yaml
        with open(path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f)
        return data if isinstance(data, dict) else {}
    except ImportError:
        return {}
    except Exception:
        return {}


# ====================================================================== #
#                          默认配置                                       #
# ====================================================================== #

_DEFAULT_CONFIG = {
    "pool_max_items": 5000,
    "insert_zero_energy_object": True,
    "allow_negative_energy": False,
    "energy_update_floor_to_zero": True,
    "tick_time_floor_ms": 1,
    "recency_gain_peak": 10.0,
    "recency_gain_decay_ratio": 0.9999976974,
    "recency_gain_hold_ticks": 2,
    "fatigue_window_ticks": 12,
    "fatigue_threshold_count": 3,
    "fatigue_max_value": 1.0,
    "default_er_decay_ratio": 0.95,
    "default_ev_decay_ratio": 0.90,
    # soft_capacity_* / 状态池“软上限”衰减调制
    # 说明：当对象数量超过 soft_capacity_start_items 后，维护阶段衰减会变得更激进，
    # 以避免状态池规模无界增长（尤其是原型调试阶段）。
    # 算法：ratio' = ratio ** decay_power，decay_power 随对象数量线性从 1 -> soft_capacity_decay_power_max。
    "soft_capacity_enabled": True,
    "soft_capacity_start_items": 200,
    "soft_capacity_full_items": 400,
    # 经验值（验收口径）：在严重超载时，希望“每 tick 最多可衰减到约剩余 20%”（约 80% 衰减），
    # 以便状态池能在调试阶段快速自我收缩、避免对象数量无界增长。
    # 注意：由于 ER/EV 基础保留系数不同（默认 ER=0.95, EV=0.90），同一 power 下 EV 会更快衰减。
    "soft_capacity_decay_power_max": 30.0,
    "per_object_type_decay_override": {},
    "enable_neutralization": True,
    "neutralization_mode": "simple_min_cancel",
    "neutralization_apply_stage": "maintenance",
    "neutralization_min_effect_threshold": 0.01,
    "enable_priority_stimulus_neutralization": True,
    "priority_stimulus_target_ref_types": ["st"],
    "priority_neutralization_min_effect_threshold": 0.01,
    "er_elimination_threshold": 0.05,
    "ev_elimination_threshold": 0.05,
    "cp_elimination_ignore_below": 0.02,
    "prune_if_both_energy_low": True,
    "pool_overflow_strategy": "prune_lowest_then_reject",
    "enable_change_rate_tracking": True,
    "rate_window_mode": "last_update",
    "fast_cp_rise_threshold": 0.5,
    "fast_cp_drop_threshold": -0.5,
    "fast_er_rise_threshold": 0.5,
    "fast_ev_rise_threshold": 0.5,
    "rate_smoothing_alpha": 1.0,
    "merge_duplicate_items": True,
    "merge_only_same_ref_object": True,
    "enable_semantic_same_object_merge": True,
    "allow_weak_semantic_merge": False,
    "aggregate_same_semantic_incoming_objects": True,
    "sensor_input_reconcile_mode": "max",
    # 是否把 CSA（组合刺激元）作为独立 state_item 写入状态池。
    # 注意：理论层面 CSA 是“匹配约束单元”，工程上不一定要以独立对象存在于 SP。
    # 当前原型默认不写入 CSA，避免与 SA 同时存在导致展示混乱与维护成本上升。
    "insert_csa_as_state_item": False,
    # 是否把“属性 SA”（stimulus.role == attribute）作为独立 state_item 写入状态池。
    # 注意：理论层面属性刺激元通常绑定在 CSA（组合刺激元）内，运行态可以只把它们
    # 作为“约束/说明信息”挂在锚点对象上，而不必额外占用 SP 容量。
    # 当前原型默认不写入属性 SA，避免状态池回写出现大量类似 stimulus_intensity:1.1 的噪音对象。
    "insert_attribute_sa_as_state_item": False,
    # 绑定属性时是否自动创建“绑定型 CSA”state_item（synthetic）。
    # 当前原型默认关闭：CSA 主要在 packet/HDB 中承担匹配约束作用，运行态 SP 以 SA/ST 为主。
    "allow_auto_create_csa_on_attribute_bind": False,
    "attribute_bind_deduplicate_by_id": True,
    "attribute_bind_deduplicate_by_content": False,
    # 绑定属性时的“替换语义”（对齐理论：同一属性名在同一对象上应保持唯一）
    # - true: 若目标对象已存在同名 attribute_name，则替换旧属性（更贴近“额外约束信息”的语义）
    # - false: 允许同名属性重复绑定（更像“多次打标签”），可能导致 runtime_attrs 爆炸
    #
    # Attribute binding replace semantics:
    # - true: replace existing attribute with the same attribute_name
    # - false: allow duplicates (can blow up runtime_attrs)
    "attribute_bind_replace_by_attribute_name": True,
    "attribute_binding_supported_target_types": ["sa", "csa", "st"],
    "enable_script_broadcast": True,
    "script_broadcast_stage_after_apply": True,
    "script_broadcast_stage_after_maintenance": True,
    "script_broadcast_include_full_event_dump": True,
    "script_broadcast_top_k_items": 128,
    "script_broadcast_min_event_count": 1,
    "enable_placeholder_interfaces": True,
    "placeholder_hdb_enabled": True,
    "placeholder_script_enabled": True,
    "placeholder_attention_enabled": True,
    "placeholder_emotion_enabled": True,
    "placeholder_action_enabled": True,
    "detail_log_dump_full_object": True,
    "detail_log_dump_change_event": True,
    "history_window_max_events": 5000,
    "log_dir": "",
    "log_max_file_bytes": 5 * 1024 * 1024,
    "stdout_fallback_when_log_fail": True,
}


# ====================================================================== #
#                       StatePool 主类                                     #
# ====================================================================== #


class StatePool:
    """
    AP 状态池主类。

    使用示例:
        pool = StatePool()
        result = pool.apply_stimulus_packet(stimulus_packet=pkt, trace_id="tick_001")
    """

    def __init__(self, config_path: str = "", config_override: dict | None = None):
        """
        初始化状态池。

        参数:
            config_path: YAML 配置文件路径
            config_override: 直接传入配置 dict（优先级最高）
        """
        # 合并配置
        self._config_path = config_path or os.path.join(
            os.path.dirname(__file__), "config", "state_pool_config.yaml"
        )
        self._config = self._build_config(config_override)

        # 初始化子模块
        self._logger = ModuleLogger(
            log_dir=self._config.get("log_dir", ""),
            max_file_bytes=self._config.get("log_max_file_bytes", 5 * 1024 * 1024),
        )
        self._audit = AuditLogger(self._logger)
        self._store = PoolStore(self._config)
        self._energy = EnergyEngine(self._config)
        self._neutralization = NeutralizationEngine(self._config)
        self._merge = MergeEngine(self._config)
        self._binding = BindingEngine(self._config)
        self._maintenance = MaintenanceEngine(self._config)
        self._snapshot = SnapshotEngine(self._config)
        self._history = HistoryWindow(self._config)

        # 运行统计
        self._tick_counter: int = 0
        self._total_calls: int = 0
        self._total_items_created: int = 0

        # 占位接口引用（延迟导入）
        self._placeholder_interfaces: dict = {}

    # ================================================================== #
    #   接口一: apply_stimulus_packet — 接收刺激包                         #
    # ================================================================== #

    def apply_stimulus_packet(
        self,
        stimulus_packet: dict,
        trace_id: str,
        tick_id: str | None = None,
        source_module: str | None = None,
        apply_mode: str = "normal",
        enable_script_broadcast: bool = True,
        metadata: dict | None = None,
    ) -> dict:
        """
        接收刺激包并将其中对象写入或更新状态池。

        处理流程:
          1. 校验 packet
          2. 拆分合法/非法对象
          3. 映射为候选 state_item
          4. 查重、合并或新建
          5. 赋能并刷新动态指标
          6. 即时中和（若配置）
          7. 广播脚本检查
        """
        start_time = time.time()
        tick_id = tick_id or trace_id
        self._total_calls += 1
        self._tick_counter += 1
        tick_number = self._tick_counter

        # ---- Step 1: 参数校验 ----
        err = self._validate_stimulus_packet(stimulus_packet, trace_id, apply_mode)
        if err:
            self._logger.error(
                trace_id=trace_id, interface="apply_stimulus_packet",
                code=err["code"], message_zh=err["message_zh"], message_en=err["message_en"],
                tick_id=tick_id, detail=err.get("detail"),
            )
            return self._make_response(False, err["code"],
                f"{err['message_zh']} / {err['message_en']}",
                error=err, trace_id=trace_id, elapsed_ms=self._elapsed_ms(start_time))

        if apply_mode == "validation_only":
            return self._make_response(True, "OK",
                "校验通过（validation_only 模式不入池）/ Validation passed (not applied)",
                trace_id=trace_id, elapsed_ms=self._elapsed_ms(start_time))

        working_packet = self._clone_stimulus_packet(stimulus_packet)
        pre_neutralization_events: list[dict] = []
        priority_neutralized_item_count = 0
        if apply_mode == "normal" and self._config.get("enable_priority_stimulus_neutralization", True):
            neutralization_result = self._priority_neutralize_stimulus_packet(
                stimulus_packet=working_packet,
                tick_number=tick_number,
                trace_id=trace_id,
                tick_id=tick_id,
                source_module=source_module or "text_sensor",
            )
            working_packet = neutralization_result["residual_packet"]
            pre_neutralization_events = neutralization_result["events"]
            priority_neutralized_item_count = neutralization_result["neutralized_item_count"]

        # ---- Step 2: 拆分对象 ----
        # SA 是状态池的主要输入对象。
        # CSA/属性 SA 在理论中更像“绑定约束信息”，工程上不一定要作为独立 state_item 常驻于 SP。
        sa_items = list(working_packet.get("sa_items", []) or [])
        csa_items = list(working_packet.get("csa_items", []) or [])

        # 如需调试/观测 CSA 聚合视图，可通过 insert_csa_as_state_item 打开写入。
        store_csa = bool(self._config.get("insert_csa_as_state_item", False))
        # 如需调试/观测属性 SA（例如 stimulus_intensity:1.1），可通过 insert_attribute_sa_as_state_item 打开写入。
        store_attr = bool(self._config.get("insert_attribute_sa_as_state_item", False))

        feature_sa_items: list[dict] = []
        attribute_sa_items: list[dict] = []
        for sa in sa_items:
            if not isinstance(sa, dict):
                continue
            if str(sa.get("object_type", "")) != "sa":
                continue
            role = str(sa.get("stimulus", {}).get("role", "") or "")
            if role == "attribute":
                attribute_sa_items.append(sa)
            else:
                feature_sa_items.append(sa)

        # object_lookup 用于 build_state_item 生成语义签名与轻量快照；必须包含“完整对象视图”。
        # 注意：即使我们选择不把某类对象写入 SP，也应让它参与快照生成与绑定同步。
        object_lookup = {
            str(obj.get("id", "")): obj
            for obj in (list(sa_items) + list(csa_items))
            if isinstance(obj, dict) and str(obj.get("id", ""))
        }

        # 预先收集“锚点 -> 属性成员”映射：用于
        # 1) 当 store_attr=False 时，把属性 SA 的能量折叠进锚点 SA（避免能量丢失）
        # 2) 让锚点对象保存一份稳定的属性视图（便于观测；避免 SA/CSA 双份共存）
        attr_energy_by_anchor: dict[str, dict[str, float | int]] = {}
        attrs_by_anchor: dict[str, list[dict]] = {}
        for attr in attribute_sa_items:
            parent_ids = list(attr.get("source", {}).get("parent_ids", []) or [])
            anchor_id = str(parent_ids[0]) if parent_ids else ""
            if not anchor_id:
                continue
            attrs_by_anchor.setdefault(anchor_id, []).append(attr)
            energy = attr.get("energy", {}) or {}
            bucket = attr_energy_by_anchor.setdefault(anchor_id, {"er": 0.0, "ev": 0.0, "count": 0})
            bucket["er"] = float(bucket.get("er", 0.0)) + float(energy.get("er", 0.0) or 0.0)
            bucket["ev"] = float(bucket.get("ev", 0.0)) + float(energy.get("ev", 0.0) or 0.0)
            bucket["count"] = int(bucket.get("count", 0) or 0) + 1

        # 默认：只把“特征 SA”写入 SP；属性 SA/CSA 只做绑定约束信息（可通过开关打开写入）。
        all_objects = list(feature_sa_items)
        if store_attr:
            all_objects.extend(attribute_sa_items)
        if store_csa:
            all_objects.extend(list(csa_items))

        valid_objects: list[dict] = []
        rejected_count = 0
        for obj in all_objects:
            if not isinstance(obj, dict) or not obj.get("id") or not obj.get("energy"):
                rejected_count += 1
                continue
            valid_objects.append(obj)

        # ---- Step 3~5: 映射、查重、赋能 ----
        all_events: list[dict] = list(pre_neutralization_events)
        new_count = 0
        updated_count = 0
        merged_count = 0
        applied_ids = [
            event.get("target_item_id", "")
            for event in pre_neutralization_events
            if event.get("target_item_id", "")
        ]
        application_groups: dict[str, dict] = {}

        for obj in valid_objects:
            candidate = build_state_item(
                ref_object=obj, trace_id=trace_id, tick_id=tick_id,
                tick_number=tick_number, source_module=source_module or "text_sensor",
                source_interface="apply_stimulus_packet",
                origin="from_stimulus_packet",
                origin_id=working_packet.get("id", ""),
                object_lookup=object_lookup,
            )
            if candidate is None:
                rejected_count += 1
                continue

            packet_context = obj.get("ext", {}).get("packet_context", {})
            should_group_by_semantic = (
                bool(packet_context)
                and self._config.get("aggregate_same_semantic_incoming_objects", True)
                and bool(candidate.get("semantic_signature"))
            )

            if should_group_by_semantic:
                aggregate_key = f"semantic::{candidate['semantic_signature']}"
            else:
                aggregate_key = f"ref::{candidate.get('ref_object_id', '')}"

            group = application_groups.setdefault(
                aggregate_key,
                {
                    "entries": [],
                    "total_er": 0.0,
                    "total_ev": 0.0,
                    # Folded packet attributes (when attribute SA is not stored as state items).
                    # 折叠的属性 SA 统计（当属性 SA 不入池时）。
                    "folded_attribute_sa_count": 0,
                    "folded_attribute_total_er": 0.0,
                    "folded_attribute_total_ev": 0.0,
                    "has_packet_context": False,
                    "source_types": set(),
                },
            )
            group["entries"].append(
                {
                    "ref_object": obj,
                    "candidate": candidate,
                    "packet_context": packet_context,
                }
            )
            group["total_er"] += candidate["energy"]["er"]
            group["total_ev"] += candidate["energy"]["ev"]

            # Fold attribute SA energy into the anchor SA when we choose not to store attributes as state items.
            # 把属性 SA 的能量折叠进锚点 SA（当属性 SA 不入池时），避免能量丢失并减少噪音对象数量。
            if (not store_attr
                and str(obj.get("object_type", "")) == "sa"
                and str(obj.get("stimulus", {}).get("role", "") or "") != "attribute"):
                anchor_id = str(obj.get("id", "") or "")
                folded = attr_energy_by_anchor.get(anchor_id) or {}
                extra_er = float(folded.get("er", 0.0) or 0.0)
                extra_ev = float(folded.get("ev", 0.0) or 0.0)
                extra_count = int(folded.get("count", 0) or 0)
                if extra_count and (extra_er or extra_ev):
                    group["total_er"] += extra_er
                    group["total_ev"] += extra_ev
                    group["folded_attribute_sa_count"] += extra_count
                    group["folded_attribute_total_er"] += extra_er
                    group["folded_attribute_total_ev"] += extra_ev
            if packet_context:
                group["has_packet_context"] = True
                source_type = packet_context.get("source_type", "")
                if source_type:
                    group["source_types"].add(source_type)

        for group in application_groups.values():
            entries = group["entries"]
            representative = self._select_representative_candidate(entries)
            representative = self._synchronize_candidate_with_group(
                representative,
                entries=entries,
                total_er=group["total_er"],
                total_ev=group["total_ev"],
            )
            if group.get("folded_attribute_sa_count", 0):
                ext = representative.setdefault("ext", {})
                ext["incoming_packet_folded_attribute_sa_count"] = int(group.get("folded_attribute_sa_count", 0) or 0)
                ext["incoming_packet_folded_attribute_total_er"] = round(float(group.get("folded_attribute_total_er", 0.0) or 0.0), 8)
                ext["incoming_packet_folded_attribute_total_ev"] = round(float(group.get("folded_attribute_total_ev", 0.0) or 0.0), 8)

            existing = None
            matched_by_ref = False
            for entry in entries:
                candidate_ref_id = entry["candidate"].get("ref_object_id", "")
                existing = self._store.get_by_ref(candidate_ref_id)
                if existing is not None:
                    matched_by_ref = True
                    break
            if existing is None:
                existing = self._find_existing_item_for_candidate(representative)

            group_collapse_count = max(0, len(entries) - 1)

            if existing is not None:
                match_mode = "ref" if matched_by_ref else "semantic"

                if apply_mode == "dry_run":
                    updated_count += 1
                    merged_count += group_collapse_count + (1 if match_mode == "semantic" else 0)
                    continue

                if group["has_packet_context"] and self._config.get("sensor_input_reconcile_mode", "max") == "max":
                    event = self._reconcile_candidate_on_existing(
                        existing_item=existing,
                        candidate_item=representative,
                        incoming_er=group["total_er"],
                        incoming_ev=group["total_ev"],
                        tick_number=tick_number,
                        reason=f"stimulus_apply_{match_mode}_reconcile",
                        source_module=source_module or "text_sensor",
                        trace_id=trace_id,
                        tick_id=tick_id,
                    )
                    event["merge_mode"] = f"{match_mode}_reconcile"
                else:
                    event = self._merge_candidate_into_existing(
                        existing_item=existing,
                        candidate_item=representative,
                        merge_mode=match_mode,
                        tick_number=tick_number,
                        reason=(
                            "stimulus_apply_semantic_merge"
                            if match_mode == "semantic"
                            else "stimulus_apply_ref_hit"
                        ),
                        source_module=source_module or "text_sensor",
                        trace_id=trace_id,
                        tick_id=tick_id,
                    )

                event["incoming_member_count"] = len(entries)
                event["packet_source_types"] = sorted(group["source_types"])
                all_events.append(event)
                applied_ids.append(existing["id"])
                updated_count += 1
                merged_count += group_collapse_count + (1 if match_mode == "semantic" else 0)
                self._log_change_event(event, trace_id, tick_id)
                continue

            if apply_mode == "dry_run":
                new_count += 1
                merged_count += group_collapse_count
                continue

            # 检查零能量
            er = representative["energy"]["er"]
            ev = representative["energy"]["ev"]
            if er == 0 and ev == 0 and not self._config.get("insert_zero_energy_object", True):
                rejected_count += 1
                continue

            self._energy.seed_runtime_modulation(representative, tick_number)
            inserted = self._store.insert(representative)
            if inserted:
                self._total_items_created += 1
                applied_ids.append(representative["id"])
                new_count += 1
                merged_count += group_collapse_count
                event = {
                    "event_id": f"new_{representative['id']}",
                    "event_type": "created",
                    "target_item_id": representative["id"],
                    "trace_id": trace_id, "tick_id": tick_id,
                    "timestamp_ms": int(time.time() * 1000),
                    "before": {"er": 0.0, "ev": 0.0, "cp_delta": 0.0, "cp_abs": 0.0},
                    "after": {
                        "er": er,
                        "ev": ev,
                        "cp_delta": representative["energy"]["cognitive_pressure_delta"],
                        "cp_abs": representative["energy"]["cognitive_pressure_abs"],
                    },
                    "delta": {
                        "delta_er": er,
                        "delta_ev": ev,
                        "delta_cp_delta": representative["energy"]["cognitive_pressure_delta"],
                        "delta_cp_abs": representative["energy"]["cognitive_pressure_abs"],
                    },
                    "rate": {
                        "er_change_rate": er,
                        "ev_change_rate": ev,
                        "cp_delta_rate": representative["energy"]["cognitive_pressure_delta"],
                        "cp_abs_rate": representative["energy"]["cognitive_pressure_abs"],
                    },
                    "reason": "stimulus_apply_new_item",
                    "source_module": source_module or "text_sensor",
                    "semantic_signature": representative.get("semantic_signature", ""),
                    "incoming_member_count": len(entries),
                    "packet_source_types": sorted(group["source_types"]),
                }
                all_events.append(event)
                self._log_change_event(event, trace_id, tick_id)
            else:
                rejected_count += 1

        # ---- CSA / Attribute binding sync / CSA 与属性绑定关系同步 ----
        # 理论层面：CSA（组合刺激元）是“对象-属性绑定”的匹配约束单元（门控约束）。
        # 工程层面：当前原型默认不把 CSA/属性 SA 作为独立 state_item 写入 SP，避免 SA/CSA 双份共存导致展示混乱；
        # 但仍需要把“约束信息”融合回锚点对象，保证每个锚点只有一个 CSA 视图（融合规则）。
        if apply_mode == "normal" and (csa_items or attribute_sa_items):
            now_ms = int(time.time() * 1000)

            def _upsert_packet_attribute(*, anchor_item: dict, attribute_sa: dict) -> None:
                """把 packet 内的属性 SA 以“稳定字典”的形式挂到锚点对象上（按 attribute_name 覆写）。"""
                if not isinstance(anchor_item, dict) or not isinstance(attribute_sa, dict):
                    return
                if str(attribute_sa.get("object_type", "")) != "sa":
                    return
                if str(attribute_sa.get("stimulus", {}).get("role", "") or "") != "attribute":
                    return
                content = attribute_sa.get("content", {}) or {}
                attr_name = str(content.get("attribute_name", "") or "")
                if not attr_name:
                    raw = str(content.get("raw", "") or "")
                    if ":" in raw:
                        attr_name = raw.split(":", 1)[0].strip()
                    else:
                        attr_name = raw.strip()
                if not attr_name:
                    return

                display = str(content.get("display", "") or content.get("raw", "") or attribute_sa.get("id", ""))
                value = content.get("attribute_value")
                sa_id = str(attribute_sa.get("id", "") or "")

                binding_state = anchor_item.setdefault("binding_state", {})
                packet_attrs = binding_state.setdefault("packet_attribute_by_name", {})
                packet_attrs[attr_name] = {
                    "attribute_name": attr_name,
                    "attribute_value": value,
                    "display": display,
                    "sa_id": sa_id,
                    "updated_at": now_ms,
                }

                # 轻量快照：用于前端/报告解释（避免把完整 attribute_sa 挂到 ext 里导致膨胀）。
                ref_snapshot = anchor_item.setdefault("ref_snapshot", {})
                ordered = sorted(
                    list(packet_attrs.values()),
                    key=lambda row: str(row.get("attribute_name", "")),
                )
                ref_snapshot["attribute_displays"] = [
                    str(row.get("display", ""))
                    for row in ordered
                    if str(row.get("display", ""))
                ]

                detail_parts = []
                if ref_snapshot.get("attribute_displays"):
                    detail_parts.append(f"attrs={', '.join(ref_snapshot.get('attribute_displays', [])[:4])}")
                bound = list(ref_snapshot.get("bound_attribute_displays", []) or [])
                if bound:
                    detail_parts.append(f"runtime_attrs={', '.join(bound[:4])}")
                if detail_parts:
                    ref_snapshot["content_display_detail"] = " | ".join(detail_parts)

            # If CSA is inserted, keep the legacy link for compatibility.
            # 若选择写入 CSA，则保留旧的“anchor -> csa_item_id”链接，兼容历史逻辑。
            if store_csa:
                for obj in csa_items:
                    if not isinstance(obj, dict) or obj.get("object_type") != "csa":
                        continue
                    csa_ref_id = str(obj.get("id", "") or "")
                    anchor_ref_id = str(obj.get("anchor_sa_id", "") or "")
                    if not csa_ref_id or not anchor_ref_id:
                        continue
                    csa_item = self._store.get_by_ref(csa_ref_id)
                    anchor_item = self._store.get_by_ref(anchor_ref_id)
                    if not csa_item or not anchor_item:
                        continue
                    binding_state = anchor_item.setdefault("binding_state", {})
                    if not binding_state.get("bound_csa_item_id"):
                        binding_state["bound_csa_item_id"] = csa_item.get("id")

            # 1) Sync from attribute SA parent_ids mapping (works even if CSA output is disabled).
            # 1) 优先用 parent_ids 映射同步（即使感受器关闭了 CSA 输出也能工作）。
            for anchor_ref_id, attrs in attrs_by_anchor.items():
                anchor_item = self._store.get_by_ref(anchor_ref_id)
                if not anchor_item:
                    continue
                for attr in attrs:
                    _upsert_packet_attribute(anchor_item=anchor_item, attribute_sa=attr)
                anchor_item["updated_at"] = now_ms
                anchor_item.setdefault("lifecycle", {})["last_active_tick"] = tick_number

            # 2) Sync from CSA member ids (as a fallback/extra safety).
            # 2) 再从 CSA 的 member_ids 同步一次（兜底，避免某些 packet 缺 parent_ids 时丢失）。
            for csa in csa_items:
                if not isinstance(csa, dict) or csa.get("object_type") != "csa":
                    continue
                anchor_ref_id = str(csa.get("anchor_sa_id", "") or "")
                if not anchor_ref_id:
                    continue
                anchor_item = self._store.get_by_ref(anchor_ref_id)
                if not anchor_item:
                    continue
                for member_id in csa.get("member_sa_ids", []) or []:
                    mid = str(member_id or "")
                    if not mid or mid == anchor_ref_id:
                        continue
                    attr_obj = object_lookup.get(mid)
                    if attr_obj:
                        _upsert_packet_attribute(anchor_item=anchor_item, attribute_sa=attr_obj)
                anchor_item["updated_at"] = now_ms
                anchor_item.setdefault("lifecycle", {})["last_active_tick"] = tick_number

        # ---- Step 6: 即时中和 ----
        neut_stage = self._config.get("neutralization_apply_stage", "maintenance")
        neut_count = 0
        if neut_stage in ("immediate", "both") and apply_mode == "normal":
            for spi_id in applied_ids:
                item = self._store.get(spi_id)
                if item:
                    event = self._neutralization.neutralize(
                        item=item, tick_number=tick_number,
                        trace_id=trace_id, tick_id=tick_id,
                    )
                    if event:
                        all_events.append(event)
                        neut_count += 1
                        self._log_change_event(event, trace_id, tick_id)

        # ---- 记录事件到历史窗口 ----
        self._history.append_many(all_events)

        # ---- Step 7: 脚本广播 ----
        broadcast_sent = False
        if (enable_script_broadcast
            and self._config.get("enable_script_broadcast", True)
            and self._config.get("script_broadcast_stage_after_apply", True)
            and len(all_events) >= self._config.get("script_broadcast_min_event_count", 1)
            and apply_mode == "normal"):
            broadcast_sent = self._broadcast_script_check(all_events, trace_id, tick_id)

        # ---- 能量统计 ----
        total_delta_er = sum(e.get("delta", {}).get("delta_er", 0) for e in all_events if "delta" in e)
        total_delta_ev = sum(e.get("delta", {}).get("delta_ev", 0) for e in all_events if "delta" in e)
        high_cp = len(self._store.get_high_cp_items(0.5))

        elapsed = self._elapsed_ms(start_time)

        # Brief 日志
        self._logger.brief(
            trace_id=trace_id, interface="apply_stimulus_packet", success=True,
            message_zh="状态池应用刺激包成功", message_en="Stimulus packet applied successfully",
            tick_id=tick_id,
            input_summary={"packet_id": stimulus_packet.get("id", ""),
                           "sa_count": len(sa_items), "csa_count": len(csa_items)},
            output_summary={"new_item_count": new_count, "updated_item_count": updated_count,
                            "merged_item_count": merged_count, "rejected_object_count": rejected_count,
                            "priority_neutralized_item_count": priority_neutralized_item_count,
                            "residual_sa_count": len(working_packet.get("sa_items", [])),
                            "residual_csa_count": len(working_packet.get("csa_items", [])),
                            "active_item_count": self._store.size, "high_cp_item_count": high_cp,
                            "script_broadcast_sent": broadcast_sent},
        )

        return self._make_response(
            success=True, code="OK",
            message="状态池应用刺激包成功 / Stimulus packet applied successfully",
            data={
                "applied_state_item_ids": applied_ids,
                "new_item_count": new_count,
                "updated_item_count": updated_count,
                "merged_item_count": merged_count,
                "priority_neutralized_item_count": priority_neutralized_item_count,
                "neutralized_item_count": neut_count,
                "rejected_object_count": rejected_count,
                "script_broadcast_sent": broadcast_sent,
                "residual_stimulus_packet": working_packet,
                "state_delta_summary": {
                    "total_delta_er": round(total_delta_er, 6),
                    "total_delta_ev": round(total_delta_ev, 6),
                    "high_cp_item_count": high_cp,
                },
            },
            trace_id=trace_id, elapsed_ms=elapsed,
        )

    # ================================================================== #
    #   接口二: apply_energy_update — 定向能量更新                         #
    # ================================================================== #

    def apply_energy_update(
        self,
        target_item_id: str,
        delta_er: float,
        delta_ev: float,
        trace_id: str,
        tick_id: str | None = None,
        reason: str = "external_update",
        source_module: str = "unknown",
        allow_create_if_missing: bool = False,
        extra_context: dict | None = None,
    ) -> dict:
        """对状态池中已有对象执行定向能量更新。"""
        start_time = time.time()
        tick_id = tick_id or trace_id
        self._total_calls += 1

        # 校验
        if not target_item_id or not isinstance(target_item_id, str):
            return self._make_error_response("INPUT_VALIDATION_ERROR",
                "target_item_id 必填且必须为字符串", "target_item_id is required and must be a string",
                trace_id, start_time)

        if delta_er == 0.0 and delta_ev == 0.0:
            return self._make_error_response("INPUT_VALIDATION_ERROR",
                "delta_er 与 delta_ev 不允许同时为 0", "delta_er and delta_ev cannot both be 0",
                trace_id, start_time)

        item = self._store.get(target_item_id)
        if item is None:
            if not allow_create_if_missing:
                return self._make_error_response("STATE_ERROR",
                    f"目标对象不存在: {target_item_id}", f"Target item not found: {target_item_id}",
                    trace_id, start_time)
            # 允许创建时走 insert_runtime_node 逻辑
            return self._make_error_response("STATE_ERROR",
                f"目标对象不存在且未启用自动创建: {target_item_id}",
                f"Target not found and auto-create disabled: {target_item_id}",
                trace_id, start_time)

        # 执行更新
        before_er = item["energy"]["er"]
        before_ev = item["energy"]["ev"]

        event = self._energy.apply_energy_delta(
            item=item, delta_er=delta_er, delta_ev=delta_ev,
            tick_number=self._tick_counter, reason=reason,
            source_module=source_module, trace_id=trace_id, tick_id=tick_id,
        )
        self._history.append(event)
        self._log_change_event(event, trace_id, tick_id)

        after = item["energy"]

        self._logger.brief(
            trace_id=trace_id, interface="apply_energy_update", success=True,
            message_zh="状态池对象能量更新成功", message_en="State item energy updated successfully",
            tick_id=tick_id,
            input_summary={"target": target_item_id, "delta_er": delta_er, "delta_ev": delta_ev, "reason": reason},
            output_summary={"er": after["er"], "ev": after["ev"], "cp_abs": after["cognitive_pressure_abs"]},
        )

        return self._make_response(
            success=True, code="OK",
            message="状态池对象能量更新成功 / State item energy updated successfully",
            data={
                "target_item_id": target_item_id,
                "before": {"er": before_er, "ev": before_ev},
                "after": {"er": after["er"], "ev": after["ev"]},
                "delta": {"delta_er": delta_er, "delta_ev": delta_ev},
                "cp_change": {
                    "before_cp_abs": round(abs(before_er - before_ev), 8),
                    "after_cp_abs": after["cognitive_pressure_abs"],
                },
            },
            trace_id=trace_id, elapsed_ms=self._elapsed_ms(start_time),
        )

    # ================================================================== #
    #   接口三: bind_attribute_node_to_object — 属性绑定                   #
    # ================================================================== #

    def bind_attribute_node_to_object(
        self,
        target_item_id: str,
        attribute_sa: dict,
        trace_id: str,
        tick_id: str | None = None,
        bind_mode: str = "append_attribute",
        source_module: str = "unknown",
        reason: str = "attribute_binding",
    ) -> dict:
        """将属性刺激元绑定到已有 SA/CSA 上。"""
        start_time = time.time()
        tick_id = tick_id or trace_id
        self._total_calls += 1

        # 校验属性SA
        validation_err = self._binding.validate_attribute_sa(attribute_sa)
        if validation_err:
            return self._make_error_response("INPUT_VALIDATION_ERROR",
                validation_err, validation_err, trace_id, start_time)

        # 查找目标
        item = self._store.get(target_item_id)
        if item is None:
            return self._make_error_response("STATE_ERROR",
                f"目标对象不存在: {target_item_id}", f"Target item not found: {target_item_id}",
                trace_id, start_time)

        ref_type = item.get("ref_object_type", "")
        supported = self._config.get("attribute_binding_supported_target_types", ["sa", "st", "csa"])
        if ref_type not in supported:
            return self._make_error_response("NOT_IMPLEMENTED_ERROR",
                f"不支持对 {ref_type} 类型执行属性绑定",
                f"Attribute binding not supported for type: {ref_type}",
                trace_id, start_time)

        # 执行绑定
        if ref_type in {"sa", "st"}:
            # 原型阶段允许对 ST 绑定运行态属性（例如 CFS 元认知属性），用于观测与脚本触发。
            result = self._binding.bind_to_sa_item(
                target_item=item, attribute_sa=attribute_sa, pool_store=self._store,
                trace_id=trace_id, tick_id=tick_id, tick_number=self._tick_counter,
                source_module=source_module,
            )
        else:  # csa
            result = self._binding.bind_to_csa_item(
                target_item=item, attribute_sa=attribute_sa,
                trace_id=trace_id, tick_id=tick_id, tick_number=self._tick_counter,
            )

        self._logger.brief(
            trace_id=trace_id, interface="bind_attribute_node_to_object", success=True,
            message_zh="属性节点绑定成功", message_en="Attribute node bound successfully",
            tick_id=tick_id,
            input_summary={"target": target_item_id, "attr_id": attribute_sa.get("id", ""), "reason": reason},
            output_summary=result,
        )
        self._logger.detail(
            trace_id=trace_id, step="attribute_binding",
            message_zh="属性节点绑定详情", message_en="Attribute binding details",
            tick_id=tick_id,
            info={
                "target_item_id": target_item_id,
                "target_ref_object_id": item.get("ref_object_id", ""),
                "target_ref_object_type": ref_type,
                "attribute_sa_id": attribute_sa.get("id", ""),
                "attribute_display": attribute_sa.get("content", {}).get("display", attribute_sa.get("content", {}).get("raw", "")),
                "binding_result": result,
            },
        )

        return self._make_response(
            success=True, code="OK",
            message="属性节点绑定成功 / Attribute node bound successfully",
            data={"target_item_id": target_item_id, **result},
            trace_id=trace_id, elapsed_ms=self._elapsed_ms(start_time),
        )

    # ================================================================== #
    #   接口四: insert_runtime_node — 手动插入运行态对象                    #
    # ================================================================== #

    def insert_runtime_node(
        self,
        runtime_object: dict,
        trace_id: str,
        tick_id: str | None = None,
        allow_merge: bool = True,
        source_module: str = "unknown",
        reason: str = "runtime_insert",
    ) -> dict:
        """手动插入一个运行态对象到状态池。"""
        start_time = time.time()
        tick_id = tick_id or trace_id
        self._total_calls += 1

        # 校验
        if not isinstance(runtime_object, dict):
            return self._make_error_response("INPUT_VALIDATION_ERROR",
                "runtime_object 必须是 dict", "runtime_object must be a dict",
                trace_id, start_time)

        obj_id = runtime_object.get("id", "")
        obj_type = runtime_object.get("object_type", "")
        energy = runtime_object.get("energy")

        if not obj_id:
            return self._make_error_response("INPUT_VALIDATION_ERROR",
                "runtime_object 缺少 id", "runtime_object missing id", trace_id, start_time)
        if not energy or not isinstance(energy, dict):
            return self._make_error_response("INPUT_VALIDATION_ERROR",
                "runtime_object 缺少 energy", "runtime_object missing energy", trace_id, start_time)
        if obj_type not in SUPPORTED_REF_TYPES:
            return self._make_error_response("NOT_IMPLEMENTED_ERROR",
                f"不支持的对象类型: {obj_type}", f"Unsupported object type: {obj_type}",
                trace_id, start_time)

        # 构建 state_item
        item = build_state_item(
            ref_object=runtime_object, trace_id=trace_id, tick_id=tick_id,
            tick_number=self._tick_counter, source_module=source_module,
            source_interface="insert_runtime_node", origin=reason,
        )
        if item is None:
            return self._make_error_response("INPUT_VALIDATION_ERROR",
                "runtime_object 转换失败", "runtime_object conversion failed", trace_id, start_time)

        # 查重：优先精确 ref_id，其次尝试语义同一对象合并
        existing = self._store.get_by_ref(obj_id) or self._find_existing_item_for_candidate(item)
        if existing and allow_merge:
            merge_mode = "ref" if existing.get("ref_object_id") == obj_id or obj_id in existing.get("ref_alias_ids", []) else "semantic"
            self._merge_candidate_into_existing(
                existing_item=existing,
                candidate_item=item,
                merge_mode=merge_mode,
                tick_number=self._tick_counter,
                reason="merge_on_insert",
                source_module=source_module,
                trace_id=trace_id,
                tick_id=tick_id,
            )
            return self._make_response(True, "OK",
                "对象已合并到已有项 / Object merged into existing item",
                data={"merged": True, "target_item_id": existing["id"], "merge_mode": merge_mode},
                trace_id=trace_id, elapsed_ms=self._elapsed_ms(start_time))

        self._energy.seed_runtime_modulation(item, self._tick_counter)
        inserted = self._store.insert(item)
        if not inserted:
            return self._make_error_response("STATE_ERROR",
                "插入失败（池已满）", "Insert failed (pool full)", trace_id, start_time)

        self._total_items_created += 1

        self._logger.brief(
            trace_id=trace_id, interface="insert_runtime_node", success=True,
            message_zh="运行态对象插入成功", message_en="Runtime node inserted successfully",
            tick_id=tick_id,
            input_summary={"obj_type": obj_type, "obj_id": obj_id, "reason": reason},
            output_summary={"item_id": item["id"], "pool_size": self._store.size},
        )
        self._logger.detail(
            trace_id=trace_id, step="insert_runtime_node",
            message_zh="运行态对象插入详情", message_en="Runtime node insertion details",
            tick_id=tick_id,
            info={"item_id": item["id"], "state_item": item},
        )

        return self._make_response(True, "OK",
            "运行态对象插入成功 / Runtime node inserted successfully",
            data={"inserted": True, "item_id": item["id"], "pool_size": self._store.size},
            trace_id=trace_id, elapsed_ms=self._elapsed_ms(start_time))

    # ================================================================== #
    #   接口五: tick_maintain_state_pool — Tick 维护                       #
    # ================================================================== #

    def tick_maintain_state_pool(
        self,
        trace_id: str,
        tick_id: str | None = None,
        apply_decay: bool = True,
        apply_neutralization: bool = True,
        apply_prune: bool = True,
        apply_merge: bool = True,
        enable_script_broadcast: bool = True,
        emit_attention_snapshot: bool = False,
        metadata: dict | None = None,
    ) -> dict:
        """执行一次完整状态池维护周期。"""
        start_time = time.time()
        tick_id = tick_id or trace_id
        self._total_calls += 1
        self._tick_counter += 1
        tick_number = self._tick_counter

        result = self._maintenance.run_maintenance(
            pool_store=self._store,
            energy_engine=self._energy,
            neutralization_engine=self._neutralization,
            merge_engine=self._merge,
            tick_number=tick_number,
            trace_id=trace_id, tick_id=tick_id,
            apply_decay=apply_decay,
            apply_neutralization=apply_neutralization,
            apply_prune=apply_prune,
            apply_merge=apply_merge,
        )

        events = result["events"]
        summary = result["summary"]

        # 记录事件和日志
        self._history.append_many(events)
        for event in events:
            self._log_change_event(event, trace_id, tick_id)

        # 脚本广播
        broadcast_sent = False
        if (enable_script_broadcast
            and self._config.get("enable_script_broadcast", True)
            and self._config.get("script_broadcast_stage_after_maintenance", True)):
            broadcast_sent = self._broadcast_script_check(events, trace_id, tick_id)

        summary["script_broadcast_sent"] = broadcast_sent

        # 注意力快照（占位）
        if emit_attention_snapshot:
            self._emit_attention_snapshot(trace_id, tick_id)

        elapsed = self._elapsed_ms(start_time)

        self._logger.brief(
            trace_id=trace_id, interface="tick_maintain_state_pool", success=True,
            message_zh="状态池维护成功", message_en="State pool maintenance completed successfully",
            tick_id=tick_id,
            output_summary=summary,
        )

        return self._make_response(True, "OK",
            "状态池维护成功 / State pool maintenance completed successfully",
            data=summary, trace_id=trace_id, elapsed_ms=elapsed)

    # ================================================================== #
    #   接口六: get_state_snapshot — 状态快照                              #
    # ================================================================== #

    def get_state_snapshot(
        self,
        trace_id: str,
        tick_id: str | None = None,
        include_items: bool = True,
        include_history_window: bool = True,
        top_k: int | None = None,
        sort_by: str = "cp_abs",
    ) -> dict:
        """获取当前状态池快照。"""
        self._total_calls += 1
        snapshot = self._snapshot.build_state_snapshot(
            pool_store=self._store, history_window=self._history,
            trace_id=trace_id, tick_id=tick_id or "",
            include_items=include_items, include_history_window=include_history_window,
            top_k=top_k, sort_by=sort_by,
        )

        return self._make_response(True, "OK",
            "状态池快照 / State pool snapshot",
            data={
                "snapshot": snapshot,
                "pool_stats": {
                    "version": __version__,
                    "schema_version": __schema_version__,
                    "pool_size": self._store.size,
                    "tick_counter": self._tick_counter,
                    "total_calls": self._total_calls,
                    "total_items_created": self._total_items_created,
                    "history_window_size": self._history.size,
                },
            },
            trace_id=trace_id, elapsed_ms=0)

    # ================================================================== #
    #   接口七: reload_config — 热加载配置                                 #
    # ================================================================== #

    def reload_config(
        self,
        trace_id: str,
        config_path: str | None = None,
        apply_partial: bool = True,
    ) -> dict:
        """显式触发热加载配置。"""
        start_time = time.time()
        path = config_path or self._config_path
        self._total_calls += 1

        try:
            new_raw = _load_yaml_config(path)
            if not new_raw:
                return self._make_error_response("CONFIG_ERROR",
                    f"配置文件加载失败或为空: {path}",
                    f"Config file failed to load or empty: {path}",
                    trace_id, start_time)

            applied = []
            rejected = []
            for key, val in new_raw.items():
                if key in _DEFAULT_CONFIG:
                    expected_type = type(_DEFAULT_CONFIG[key])
                    if isinstance(val, expected_type) or (expected_type is float and isinstance(val, (int, float))):
                        self._config[key] = val
                        applied.append(key)
                    else:
                        rejected.append({"key": key, "reason": f"类型不匹配 / Type mismatch: expected {expected_type.__name__}, got {type(val).__name__}"})
                else:
                    rejected.append({"key": key, "reason": "未知配置项 / Unknown config key"})

            # 通知子模块
            self._store.update_config(self._config)
            self._energy.update_config(self._config)
            self._neutralization.update_config(self._config)
            self._merge.update_config(self._config)
            self._binding.update_config(self._config)
            self._maintenance.update_config(self._config)
            self._snapshot.update_config(self._config)
            self._history.update_config(self._config)
            self._logger.update_config(
                log_dir=self._config.get("log_dir", ""),
                max_file_bytes=self._config.get("log_max_file_bytes", 0),
            )

            self._logger.brief(
                trace_id=trace_id, interface="reload_config", success=True,
                message_zh="热加载完成", message_en="Hot reload done",
                input_summary={"path": path},
                output_summary={"applied": len(applied), "rejected": len(rejected)},
            )

            return self._make_response(True, "OK",
                f"热加载完成 / Hot reload done: {len(applied)} applied, {len(rejected)} rejected",
                data={"applied": applied, "rejected": rejected},
                trace_id=trace_id, elapsed_ms=self._elapsed_ms(start_time))

        except Exception as e:
            msg_zh = f"热加载异常: {e}"
            msg_en = f"Hot reload exception: {e}"
            self._logger.error(trace_id=trace_id, interface="reload_config",
                code="CONFIG_ERROR", message_zh=msg_zh, message_en=msg_en,
                detail={"traceback": traceback.format_exc()})
            return self._make_error_response("CONFIG_ERROR", msg_zh, msg_en, trace_id, start_time)

    # ================================================================== #
    #   接口八: clear_state_pool — 清空状态池                              #
    # ================================================================== #

    def clear_state_pool(
        self,
        trace_id: str,
        reason: str,
        operator: str | None = None,
    ) -> dict:
        """清空状态池。高风险操作，必须审计。"""
        self._total_calls += 1
        cleared = self._store.clear()
        history_cleared = self._history.clear()

        self._audit.record(
            trace_id=trace_id, interface="clear_state_pool",
            action="clear_state_pool", reason=reason,
            operator=operator or "unknown",
            detail={"cleared_item_count": cleared, "cleared_event_count": history_cleared},
        )

        return self._make_response(True, "OK",
            f"状态池已清空 / State pool cleared: {cleared} items removed",
            data={"cleared_item_count": cleared, "cleared_event_count": history_cleared},
            trace_id=trace_id, elapsed_ms=0)

    # ================================================================== #
    #                     内部辅助方法                                     #
    # ================================================================== #

    def _build_config(self, override: dict | None) -> dict:
        """构建最终配置: 默认值 → YAML → 代码覆盖。"""
        cfg = dict(_DEFAULT_CONFIG)
        file_cfg = _load_yaml_config(self._config_path)
        if file_cfg:
            cfg.update(file_cfg)
        if override:
            cfg.update(override)
        return cfg

    @staticmethod
    def _clone_stimulus_packet(stimulus_packet: dict) -> dict:
        """深拷贝 stimulus_packet，避免优先中和阶段污染调用方输入。"""
        return copy.deepcopy(stimulus_packet if isinstance(stimulus_packet, dict) else {})

    def _priority_neutralize_stimulus_packet(
        self,
        *,
        stimulus_packet: dict,
        tick_number: int,
        trace_id: str,
        tick_id: str,
        source_module: str,
    ) -> dict:
        """
        让完整刺激信号先对状态池中的高认知压结构做优先验证/中和，
        之后只把剩余刺激继续交给后续流程。
        """
        if not isinstance(stimulus_packet, dict):
            empty_packet = self._clone_stimulus_packet(
                {
                    "id": "",
                    "object_type": "stimulus_packet",
                    "sa_items": [],
                    "csa_items": [],
                    "grouped_sa_sequences": [],
                    "energy_summary": {"total_er": 0.0, "total_ev": 0.0},
                }
            )
            return {
                "residual_packet": empty_packet,
                "events": [],
                "neutralized_item_count": 0,
            }

        from hdb._cut_engine import CutEngine

        cut_engine = CutEngine()
        min_effect = max(
            0.0,
            float(self._config.get("priority_neutralization_min_effect_threshold", 0.01)),
        )
        target_ref_types = {
            str(ref_type)
            for ref_type in self._config.get("priority_stimulus_target_ref_types", ["st"])
            if str(ref_type)
        }
        if not list(stimulus_packet.get("grouped_sa_sequences", [])):
            return {
                "residual_packet": stimulus_packet,
                "events": [],
                "neutralized_item_count": 0,
            }

        candidate_items = []
        for ref_type in target_ref_types:
            candidate_items.extend(self._store.get_by_type(ref_type))
        candidate_items.sort(
            key=lambda item: (
                float(item.get("energy", {}).get("cognitive_pressure_abs", 0.0)),
                float(item.get("updated_at", 0.0)),
            ),
            reverse=True,
        )

        events: list[dict] = []
        diagnostics: list[dict] = []
        neutralized_item_count = 0
        consumed_any = False

        for item in candidate_items:
            cp_delta = float(item.get("energy", {}).get("cognitive_pressure_delta", 0.0))
            if abs(cp_delta) < min_effect:
                continue

            structure_groups = self._extract_sequence_groups_from_state_item(item)
            if not structure_groups:
                continue

            packet_groups = self._build_packet_groups_for_neutralization(
                stimulus_packet,
                cut_engine=cut_engine,
            )
            if not packet_groups:
                break

            common_part = cut_engine.maximum_common_part(structure_groups, packet_groups)
            structure_signature = cut_engine.sequence_groups_to_signature(structure_groups)
            if not structure_signature:
                continue
            if common_part.get("common_signature", "") != structure_signature:
                continue

            matched_units = self._collect_matched_units_from_common_part(
                packet_groups=packet_groups,
                common_part=common_part,
            )
            if not matched_units:
                continue

            if cp_delta < 0.0:
                required_amount = max(
                    0.0,
                    float(item.get("energy", {}).get("ev", 0.0))
                    - float(item.get("energy", {}).get("er", 0.0)),
                )
                available_amount = round(
                    sum(max(0.0, float(unit.get("er", 0.0))) for unit in matched_units),
                    8,
                )
                consumed_amount = self._consume_packet_unit_energy(
                    matched_units=matched_units,
                    energy_key="er",
                    amount=required_amount,
                )
                delta_er = consumed_amount
                delta_ev = 0.0
                reason = "priority_stimulus_real_verification"
            else:
                required_amount = max(
                    0.0,
                    float(item.get("energy", {}).get("er", 0.0))
                    - float(item.get("energy", {}).get("ev", 0.0)),
                )
                available_amount = round(
                    sum(max(0.0, float(unit.get("ev", 0.0))) for unit in matched_units),
                    8,
                )
                consumed_amount = self._consume_packet_unit_energy(
                    matched_units=matched_units,
                    energy_key="ev",
                    amount=required_amount,
                )
                delta_er = 0.0
                delta_ev = consumed_amount
                reason = "priority_stimulus_virtual_confirmation"

            diagnostics.append(
                {
                    "target_item_id": item.get("id", ""),
                    # 注意：这里的 target_display 必须包含“对象内容”，而不是只剩 ref_object_id，
                    # 否则观测台会出现 st_000695 这种“只有 ID 没有内容”的输出，无法验收。
                    # state_item.ref_snapshot 的标准字段为 content_display/content_display_detail（见 _state_item_builder）。
                    "target_ref_object_id": item.get("ref_object_id", ""),
                    "target_ref_object_type": item.get("ref_object_type", ""),
                    "target_display": (
                        (item.get("ref_snapshot", {}) or {}).get("content_display", "")
                        or (item.get("ref_snapshot", {}) or {}).get("content_display_detail", "")
                        or item.get("ref_object_id", "")
                        or item.get("id", "")
                    ),
                    "matched_structure_signature": structure_signature,
                    "required_energy_key": "er" if delta_er > 0.0 else "ev",
                    "required_amount": round(required_amount, 8),
                    "available_amount": round(available_amount, 8),
                    "consumed_amount": round(consumed_amount, 8),
                    "shortfall_amount": round(max(0.0, float(required_amount) - float(consumed_amount)), 8),
                    "matched_unit_count": len(matched_units),
                    "matched_tokens": [str(unit.get("token", "")) for unit in matched_units],
                }
            )

            if consumed_amount < min_effect:
                continue

            event = self._energy.apply_energy_delta(
                item=item,
                delta_er=delta_er,
                delta_ev=delta_ev,
                tick_number=tick_number,
                reason=reason,
                source_module=source_module,
                trace_id=trace_id,
                tick_id=tick_id,
            )
            event["event_type"] = "priority_stimulus_neutralization"
            event["matched_structure_signature"] = structure_signature
            event["matched_unit_count"] = len(matched_units)
            event["extra_context"] = {
                "consumed_energy_key": "er" if delta_er > 0.0 else "ev",
                "consumed_amount": round(consumed_amount, 8),
                "matched_unit_count": len(matched_units),
                "matched_tokens": [str(unit.get("token", "")) for unit in matched_units],
            }
            events.append(event)
            neutralized_item_count += 1
            consumed_any = True

        residual_packet = (
            self._prune_stimulus_packet_after_consumption(stimulus_packet)
            if consumed_any
            else stimulus_packet
        )
        return {
            "residual_packet": residual_packet,
            "events": events,
            "diagnostics": diagnostics,
            "neutralized_item_count": neutralized_item_count,
        }

    def _build_packet_groups_for_neutralization(self, stimulus_packet: dict, *, cut_engine) -> list[dict]:
        """把刺激包转成与 cut engine 一致的完整 SA/CSA 视图。"""
        profile = cut_engine.build_sequence_profile_from_stimulus_packet(stimulus_packet)
        energy_ref_by_id = {}
        for item in stimulus_packet.get("sa_items", []):
            if isinstance(item, dict) and item.get("id"):
                energy_ref_by_id[str(item.get("id", ""))] = item.setdefault("energy", {})

        packet_groups = []
        for group in profile.get("sequence_groups", []):
            units = []
            for unit in group.get("units", []):
                unit_id = str(unit.get("unit_id", ""))
                energy_ref = energy_ref_by_id.get(unit_id)
                if energy_ref is None:
                    energy_ref = {
                        "er": round(float(unit.get("er", 0.0)), 8),
                        "ev": round(float(unit.get("ev", 0.0)), 8),
                    }
                er = max(0.0, float(energy_ref.get("er", 0.0)))
                ev = max(0.0, float(energy_ref.get("ev", 0.0)))
                if er + ev <= 0.0:
                    continue
                units.append(
                    {
                        **dict(unit),
                        "unit_id": unit_id,
                        "unit_type": str(unit.get("object_type", "sa")),
                        "sequence_index": int(unit.get("sequence_index", 0)),
                        "er": round(er, 8),
                        "ev": round(ev, 8),
                        "total_energy": round(er + ev, 8),
                        "energy_ref": energy_ref,
                    }
                )
            if not units:
                continue
            packet_groups.append(
                {
                    "group_index": int(group.get("group_index", 0)),
                    "source_type": group.get("source_type", ""),
                    "origin_frame_id": group.get("origin_frame_id", ""),
                    "tokens": [str(unit.get("token", "")) for unit in units if str(unit.get("token", ""))],
                    "units": units,
                    "csa_bundles": [dict(bundle) for bundle in group.get("csa_bundles", [])],
                }
            )

        return packet_groups

    def _extract_sequence_groups_from_state_item(self, item: dict) -> list[dict]:
        """从状态池对象中提取稳定的结构时序分组。"""
        ref_snapshot = item.get("ref_snapshot", {})
        sequence_groups = list(ref_snapshot.get("sequence_groups", []))
        if sequence_groups:
            return copy.deepcopy(sequence_groups)

        flat_tokens = list(ref_snapshot.get("flat_tokens", []))
        if not flat_tokens:
            return []
        return [
            {
                "group_index": 0,
                "source_type": "state_pool",
                "origin_frame_id": item.get("ref_object_id", ""),
                "tokens": flat_tokens,
            }
        ]

    @staticmethod
    def _collect_matched_units_from_common_part(*, packet_groups: list[dict], common_part: dict) -> list[dict]:
        """根据 common_part 的 incoming_unit_refs 精确找出包内命中的单元。"""
        matched_units: list[dict] = []
        remaining_refs: dict[str, int] = {}
        for pair in common_part.get("matched_pairs", []):
            for unit_id in pair.get("incoming_unit_refs", []):
                text = str(unit_id)
                if text:
                    remaining_refs[text] = remaining_refs.get(text, 0) + 1

        if remaining_refs:
            for packet_group in packet_groups:
                for unit in packet_group.get("units", []):
                    unit_id = str(unit.get("unit_id", ""))
                    if remaining_refs.get(unit_id, 0) <= 0:
                        continue
                    matched_units.append(unit)
                    remaining_refs[unit_id] -= 1
            return matched_units

        for pair in common_part.get("matched_pairs", []):
            incoming_group_index = int(pair.get("incoming_group_index", -1))
            if incoming_group_index < 0 or incoming_group_index >= len(packet_groups):
                continue
            packet_group = packet_groups[incoming_group_index]
            remaining_tokens: dict[str, int] = {}
            for token in pair.get("common_tokens", []):
                key = str(token)
                remaining_tokens[key] = remaining_tokens.get(key, 0) + 1
            for unit in packet_group.get("units", []):
                token = str(unit.get("token", ""))
                if remaining_tokens.get(token, 0) <= 0:
                    continue
                matched_units.append(unit)
                remaining_tokens[token] -= 1
        return matched_units

    @staticmethod
    def _consume_packet_unit_energy(*, matched_units: list[dict], energy_key: str, amount: float) -> float:
        """从命中的刺激单元上消费指定侧能量，返回实际消费量。"""
        target_amount = max(0.0, float(amount))
        if target_amount <= 0.0:
            return 0.0

        remaining = target_amount
        for unit in matched_units:
            if remaining <= 0.0:
                break
            energy = unit.get("energy_ref", {})
            available = max(0.0, float(energy.get(energy_key, 0.0)))
            if available <= 0.0:
                continue
            consumed = min(available, remaining)
            energy[energy_key] = round(available - consumed, 8)
            remaining = round(remaining - consumed, 8)

        return round(target_amount - remaining, 8)

    def _prune_stimulus_packet_after_consumption(self, stimulus_packet: dict) -> dict:
        """清理被消费到零的刺激对象，并重建分组与能量摘要。"""
        pruned_packet = stimulus_packet

        kept_sa_items = [
            item
            for item in pruned_packet.get("sa_items", [])
            if max(0.0, float(item.get("energy", {}).get("er", 0.0)))
            + max(0.0, float(item.get("energy", {}).get("ev", 0.0)))
            > 0.0
        ]
        kept_sa_ids = {item.get("id", "") for item in kept_sa_items if item.get("id")}
        kept_sa_index = {
            item.get("id", ""): item
            for item in kept_sa_items
            if isinstance(item, dict) and item.get("id")
        }

        kept_csa_items = []
        kept_csa_ids: set[str] = set()
        for original_csa in pruned_packet.get("csa_items", []):
            if not isinstance(original_csa, dict):
                continue
            csa_id = str(original_csa.get("id", ""))
            anchor_id = str(original_csa.get("anchor_sa_id", ""))
            if not csa_id or anchor_id not in kept_sa_ids:
                continue

            member_sa_ids = []
            seen_member_ids: set[str] = set()
            for member_id in original_csa.get("member_sa_ids", []):
                member_text = str(member_id)
                if not member_text or member_text in seen_member_ids or member_text not in kept_sa_ids:
                    continue
                seen_member_ids.add(member_text)
                member_sa_ids.append(member_text)

            # CSA 只是 SA 的分组关系，只有锚点仍在且至少保留 2 个成员时才继续保留。
            if anchor_id not in member_sa_ids or len(member_sa_ids) < 2:
                continue

            display_total_er = round(
                sum(float(kept_sa_index.get(member_id, {}).get("energy", {}).get("er", 0.0)) for member_id in member_sa_ids),
                8,
            )
            display_total_ev = round(
                sum(float(kept_sa_index.get(member_id, {}).get("energy", {}).get("ev", 0.0)) for member_id in member_sa_ids),
                8,
            )
            rebuilt_csa = dict(original_csa)
            rebuilt_csa["anchor_sa_id"] = anchor_id
            rebuilt_csa["member_sa_ids"] = member_sa_ids
            ownership_map = [
                {
                    "sa_id": member_id,
                    "er": round(float(kept_sa_index.get(member_id, {}).get("energy", {}).get("er", 0.0)), 8),
                    "ev": round(float(kept_sa_index.get(member_id, {}).get("energy", {}).get("ev", 0.0)), 8),
                }
                for member_id in member_sa_ids
            ]
            cp_delta = round(display_total_er - display_total_ev, 8)
            cp_abs = round(abs(cp_delta), 8)
            rebuilt_csa["energy_ownership_map"] = ownership_map
            rebuilt_csa["energy"] = {
                "er": display_total_er,
                "ev": display_total_ev,
                "ownership_level": "aggregated_from_sa",
                "computed_from_children": True,
                "fatigue": float(rebuilt_csa.get("energy", {}).get("fatigue", 0.0) or 0.0),
                "recency_gain": float(rebuilt_csa.get("energy", {}).get("recency_gain", 1.0) or 1.0),
                "salience_score": round(max(display_total_er, display_total_ev), 8),
                "cognitive_pressure_delta": cp_delta,
                "cognitive_pressure_abs": cp_abs,
                "last_decay_tick": int(rebuilt_csa.get("energy", {}).get("last_decay_tick", 0) or 0),
                "last_decay_at": int(time.time() * 1000),
            }
            rebuilt_csa["bundle_summary"] = {
                "member_count": len(member_sa_ids),
                "display_total_er": display_total_er,
                "display_total_ev": display_total_ev,
            }
            kept_csa_items.append(rebuilt_csa)
            kept_csa_ids.add(csa_id)

        rebuilt_groups = []
        for group in pruned_packet.get("grouped_sa_sequences", []):
            group_sa_ids = [sa_id for sa_id in group.get("sa_ids", []) if sa_id in kept_sa_ids]
            group_csa_ids = [csa_id for csa_id in group.get("csa_ids", []) if csa_id in kept_csa_ids]

            if not group_sa_ids and not group_csa_ids:
                continue

            rebuilt_group = dict(group)
            rebuilt_group["group_index"] = len(rebuilt_groups)
            rebuilt_group["sa_ids"] = group_sa_ids
            rebuilt_group["csa_ids"] = group_csa_ids
            rebuilt_groups.append(rebuilt_group)

        total_er = round(
            sum(float(item.get("energy", {}).get("er", 0.0)) for item in kept_sa_items),
            8,
        )
        total_ev = round(
            sum(float(item.get("energy", {}).get("ev", 0.0)) for item in kept_sa_items),
            8,
        )
        echo_total_er = round(
            sum(
                float(item.get("energy", {}).get("er", 0.0))
                for item in kept_sa_items
                if item.get("ext", {}).get("packet_context", {}).get("source_type", "") == "echo"
            ),
            8,
        )
        echo_total_ev = round(
            sum(
                float(item.get("energy", {}).get("ev", 0.0))
                for item in kept_sa_items
                if item.get("ext", {}).get("packet_context", {}).get("source_type", "") == "echo"
            ),
            8,
        )

        pruned_packet["sa_items"] = kept_sa_items
        pruned_packet["csa_items"] = kept_csa_items
        pruned_packet["grouped_sa_sequences"] = rebuilt_groups
        energy_summary = pruned_packet.setdefault("energy_summary", {})
        energy_summary["total_er"] = total_er
        energy_summary["total_ev"] = total_ev
        energy_summary["current_total_er"] = total_er
        energy_summary["current_total_ev"] = total_ev
        energy_summary["echo_total_er"] = echo_total_er
        energy_summary["echo_total_ev"] = echo_total_ev
        energy_summary["combined_context_er"] = total_er
        energy_summary["combined_context_ev"] = total_ev
        pruned_packet["updated_at"] = int(time.time() * 1000)
        return pruned_packet

    def _find_existing_item_for_candidate(self, candidate_item: dict | None) -> dict | None:
        """
        根据候选 state_item 查找是否存在“语义同一对象”。

        当前策略：
        1. 仍然优先使用 ref_id 精确命中；
        2. ref_id 未命中时，再用 semantic_signature 做稳定合并；
        3. 这是“同一对象跨轮次重新进入状态池”的主入口，而不是模糊近义词匹配。
        """
        if not candidate_item:
            return None
        if not self._config.get("enable_semantic_same_object_merge", True):
            return None

        semantic_signature = candidate_item.get("semantic_signature", "")
        if not semantic_signature:
            return None

        return self._store.get_by_semantic_signature(semantic_signature)

    def _merge_candidate_into_existing(
        self,
        existing_item: dict,
        candidate_item: dict,
        merge_mode: str,
        tick_number: int,
        reason: str,
        source_module: str,
        trace_id: str,
        tick_id: str,
    ) -> dict:
        """把候选对象并入已有对象，并返回标准变化事件。"""
        merge_info = self._merge.merge_items(existing_item, candidate_item)
        self._refresh_existing_item_from_candidate(existing_item, candidate_item)
        # Identity promotion (SA -> ST) / 身份提升（SA -> ST）
        # ---------------------------------------------------
        # 对齐理论核心：SA（基础刺激元）可视为最小 ST（结构）。
        # 当语义合并发生且候选对象是 ST 时，优先把该运行态对象“锚定”为 ST，
        # 这样：
        #   1) 状态池中不会长期出现“同内容 SA 与 ST 并存”
        #   2) 下游模块（注意力/结构级查存/HDB）按 ref_object_type=="st" 过滤时不会漏掉对象
        #   3) 观测台显示更符合直觉：结构对象用 st_* 作为主 ID，SA 作为别名来源
        self._maybe_promote_existing_item_to_structure(existing_item, candidate_item)

        event = self._energy.apply_energy_delta(
            item=existing_item,
            delta_er=merge_info["delta_er"],
            delta_ev=merge_info["delta_ev"],
            tick_number=tick_number,
            reason=reason,
            source_module=source_module,
            trace_id=trace_id,
            tick_id=tick_id,
        )
        event["merge_mode"] = merge_mode
        event["merge_source_ref_id"] = candidate_item.get("ref_object_id", "")
        event["semantic_signature"] = candidate_item.get("semantic_signature", "")
        return event

    def _reconcile_candidate_on_existing(
        self,
        existing_item: dict,
        candidate_item: dict,
        incoming_er: float,
        incoming_ev: float,
        tick_number: int,
        reason: str,
        source_module: str,
        trace_id: str,
        tick_id: str,
    ) -> dict:
        """
        按“当前感受器包的存在度”去对齐已有状态项，而不是盲目叠加。

        当前默认策略为 max：
        - 若本轮 packet 聚合后的能量高于现有运行态，则抬升到该值；
        - 若本轮只是较弱残响，则不再把旧刺激层层叠加放大。
        """
        self._refresh_existing_item_from_candidate(existing_item, candidate_item)
        self._maybe_promote_existing_item_to_structure(existing_item, candidate_item)

        current_er = existing_item["energy"]["er"]
        current_ev = existing_item["energy"]["ev"]
        reconcile_mode = self._config.get("sensor_input_reconcile_mode", "max")

        if reconcile_mode == "add":
            target_er = current_er + incoming_er
            target_ev = current_ev + incoming_ev
        else:
            target_er = max(current_er, incoming_er)
            target_ev = max(current_ev, incoming_ev)

        event = self._energy.apply_energy_delta(
            item=existing_item,
            delta_er=target_er - current_er,
            delta_ev=target_ev - current_ev,
            tick_number=tick_number,
            reason=reason,
            source_module=source_module,
            trace_id=trace_id,
            tick_id=tick_id,
        )
        event["incoming_packet_er"] = round(incoming_er, 8)
        event["incoming_packet_ev"] = round(incoming_ev, 8)
        event["reconcile_mode"] = reconcile_mode
        event["semantic_signature"] = candidate_item.get("semantic_signature", "")
        return event

    def _maybe_promote_existing_item_to_structure(self, existing_item: dict, candidate_item: dict) -> None:
        """
        Promote an existing merged item to "structure" (ST) identity when possible.
        当语义合并把 SA 与 ST 合并到一起时，尽量让运行态对象以 ST 作为主身份。

        Why / 为什么要做这个提升：
          - 状态池对象唯一性：同一个概念不应同时以 SA 与 ST 两种身份存在；
          - 下游模块常按 ref_object_type=="st" 做结构级输入过滤；
          - 前端展示更直观：用 st_* 作为主 id，更像“结构对象”。

        Safety / 安全性：
          - 只在 candidate 是 ST 且 existing 不是 ST 时提升；
          - 不会删除旧 ref_id：旧 SA id 仍保留在 ref_alias_ids 并被 PoolStore 索引；
          - 仅修改 existing_item 的 ref_object_id/type 与 type_index，避免破坏能量与绑定状态。
        """
        try:
            cand_type = str(candidate_item.get("ref_object_type", "") or "").strip()
            if cand_type != "st":
                return
            old_type = str(existing_item.get("ref_object_type", "") or "").strip()
            if old_type == "st":
                return

            cand_ref_id = str(candidate_item.get("ref_object_id", "") or "").strip()
            if not cand_ref_id:
                return

            spi_id = str(existing_item.get("id", "") or "").strip()
            if not spi_id:
                return

            # Update primary identity fields / 更新主身份字段
            existing_item["ref_object_type"] = "st"
            existing_item["ref_object_id"] = cand_ref_id
            # Keep sub_type aligned for observability (optional)
            # 同步 sub_type（用于观测，不影响语义）
            existing_item["sub_type"] = "st_runtime_item"

            # Update PoolStore type index / 更新类型索引
            try:
                # Remove from old type set
                if old_type and hasattr(self._store, "_type_index") and isinstance(self._store._type_index, dict):
                    if old_type in self._store._type_index:
                        self._store._type_index[old_type].discard(spi_id)
                # Add to ST set
                if hasattr(self._store, "_type_index") and isinstance(self._store._type_index, dict):
                    self._store._type_index.setdefault("st", set()).add(spi_id)
            except Exception:
                # Fallback: rebuild all indexes (best-effort, should be rare).
                try:
                    self._store.rebuild_index()
                except Exception:
                    pass
        except Exception:
            # Best-effort: never crash merges.
            return

    def _refresh_existing_item_from_candidate(self, existing_item: dict, candidate_item: dict):
        """
        在语义合并时回写候选对象的非能量信息。

        这里不会覆盖运行态字段（如 binding_state、bound runtime attrs），
        只补充 ref alias、语义签名和更完整的静态快照信息。
        """
        candidate_ref_ids = candidate_item.get("ref_alias_ids") or [candidate_item.get("ref_object_id", "")]
        for candidate_ref_id in candidate_ref_ids:
            if candidate_ref_id:
                self._store.bind_ref_alias(existing_item["id"], candidate_ref_id)

        if candidate_item.get("semantic_signature") and not existing_item.get("semantic_signature"):
            existing_item["semantic_signature"] = candidate_item["semantic_signature"]

        # Merge learned/packet-side attributes from candidate.
        # 合并“记忆/结构侧属性”（packet 属性）：
        # - 这些属性来自 stimulus_packet/memory_feedback 或结构投影本身；
        # - 它们是期待/压力等规则的关键输入（IESM selector.scope=packet）。
        #
        # 注意：这里仍然不覆盖 runtime 绑定属性（bound_attribute_by_name），只合并 packet 属性映射。
        try:
            cand_bs = candidate_item.get("binding_state", {}) if isinstance(candidate_item.get("binding_state", {}), dict) else {}
            cand_packet = cand_bs.get("packet_attribute_by_name", {})
            if isinstance(cand_packet, dict) and cand_packet:
                ex_bs = existing_item.setdefault("binding_state", {})
                if not isinstance(ex_bs, dict):
                    ex_bs = {}
                    existing_item["binding_state"] = ex_bs
                ex_packet = ex_bs.setdefault("packet_attribute_by_name", {})
                if not isinstance(ex_packet, dict):
                    ex_packet = {}
                    ex_bs["packet_attribute_by_name"] = ex_packet
                for name, row in cand_packet.items():
                    key = str(name or "").strip()
                    if not key or not isinstance(row, dict):
                        continue
                    if key not in ex_packet:
                        ex_packet[key] = dict(row)
                        continue
                    # Prefer the fresher record if updated_at exists.
                    try:
                        old_u = int((ex_packet.get(key) or {}).get("updated_at", 0) or 0)
                        new_u = int(row.get("updated_at", 0) or 0)
                        if new_u > old_u:
                            ex_packet[key] = dict(row)
                    except Exception:
                        # If parsing fails, keep the existing one (stability).
                        pass
        except Exception:
            pass

        existing_ext = existing_item.setdefault("ext", {})
        candidate_ext = candidate_item.get("ext", {})
        if candidate_ext.get("semantic_labels") and not existing_ext.get("semantic_labels"):
            existing_ext["semantic_labels"] = candidate_ext["semantic_labels"]

        existing_snapshot = existing_item.setdefault("ref_snapshot", {})
        candidate_snapshot = candidate_item.get("ref_snapshot", {})
        list_fields = {"attribute_displays", "feature_displays", "bound_attribute_displays", "member_summaries"}

        for key, value in candidate_snapshot.items():
            if key in list_fields:
                merged_list = list(existing_snapshot.get(key, []))
                for entry in value or []:
                    if entry not in merged_list:
                        merged_list.append(entry)
                if merged_list:
                    existing_snapshot[key] = merged_list
                continue

            if key not in existing_snapshot or existing_snapshot.get(key) in ("", None, [], {}):
                if value not in ("", None, [], {}):
                    existing_snapshot[key] = value

        existing_item["updated_at"] = max(
            existing_item.get("updated_at", 0),
            candidate_item.get("updated_at", 0),
        )

    @staticmethod
    def _select_representative_candidate(entries: list[dict]) -> dict:
        """在同一输入组内选择最适合承载静态快照的候选对象。优先当前刺激，其次能量更高者。"""
        ordered = sorted(
            entries,
            key=lambda entry: (
                entry.get("packet_context", {}).get("source_type") != "current",
                -entry.get("candidate", {}).get("energy", {}).get("er", 0.0),
            ),
        )
        return ordered[0]["candidate"]

    def _synchronize_candidate_with_group(
        self,
        candidate_item: dict,
        *,
        entries: list[dict],
        total_er: float,
        total_ev: float,
    ) -> dict:
        """
        把同组输入聚合后的能量与别名回写到代表候选项上。

        这样“当前刺激 + 多轮残响”的组合，进入状态池前先成为一个更完整的本轮输入投影。
        """
        ref_alias_ids: list[str] = []
        for entry in entries:
            ref_id = entry["candidate"].get("ref_object_id", "")
            if ref_id and ref_id not in ref_alias_ids:
                ref_alias_ids.append(ref_id)

        candidate_item["ref_alias_ids"] = ref_alias_ids or candidate_item.get("ref_alias_ids", [])

        energy = candidate_item["energy"]
        dynamics = candidate_item["dynamics"]
        cp_delta = round(total_er - total_ev, 8)
        cp_abs = round(abs(cp_delta), 8)

        energy["er"] = round(total_er, 8)
        energy["ev"] = round(total_ev, 8)
        energy["salience_score"] = round(max(total_er, total_ev), 8)
        energy["cognitive_pressure_delta"] = cp_delta
        energy["cognitive_pressure_abs"] = cp_abs

        dynamics["delta_er"] = round(total_er, 8)
        dynamics["delta_ev"] = round(total_ev, 8)
        dynamics["er_change_rate"] = round(total_er, 6)
        dynamics["ev_change_rate"] = round(total_ev, 6)
        dynamics["delta_cp_delta"] = cp_delta
        dynamics["delta_cp_abs"] = cp_abs
        dynamics["cp_delta_rate"] = round(cp_delta, 6)
        dynamics["cp_abs_rate"] = round(cp_abs, 6)

        ext = candidate_item.setdefault("ext", {})
        ext["incoming_packet_member_count"] = len(entries)
        ext["incoming_packet_source_types"] = sorted(
            {
                entry.get("packet_context", {}).get("source_type", "")
                for entry in entries
                if entry.get("packet_context")
            }
        )

        return candidate_item

    def _validate_stimulus_packet(self, pkt: Any, trace_id: str, apply_mode: str) -> dict | None:
        """校验刺激包。返回 None 表示通过。"""
        if not isinstance(pkt, dict):
            return {"code": "INPUT_VALIDATION_ERROR",
                    "message_zh": "stimulus_packet 必须是 dict",
                    "message_en": "stimulus_packet must be a dict"}

        if pkt.get("object_type") != "stimulus_packet":
            return {"code": "INPUT_VALIDATION_ERROR",
                    "message_zh": "stimulus_packet.object_type 必须为 'stimulus_packet'",
                    "message_en": "stimulus_packet.object_type must be 'stimulus_packet'"}

        for field in ("id", "sa_items", "csa_items"):
            if field not in pkt:
                return {"code": "INPUT_VALIDATION_ERROR",
                        "message_zh": f"stimulus_packet 缺少字段: {field}",
                        "message_en": f"stimulus_packet missing field: {field}"}

        valid_modes = ("normal", "validation_only", "dry_run")
        if apply_mode not in valid_modes:
            return {"code": "INPUT_VALIDATION_ERROR",
                    "message_zh": f"apply_mode 不合法: {apply_mode}",
                    "message_en": f"Invalid apply_mode: {apply_mode}"}
        return None

    def _broadcast_script_check(self, events: list[dict], trace_id: str, tick_id: str) -> bool:
        """生成脚本检查抄送包并调用占位接口。"""
        try:
            packet = self._snapshot.build_script_check_packet(
                events=events, pool_store=self._store,
                trace_id=trace_id, tick_id=tick_id,
            )

            self._logger.detail(
                trace_id=trace_id, step="broadcast_state_window_for_script_check",
                message_zh="已向先天脚本检查接口抄送状态变化窗口",
                message_en="State change window broadcast to innate script checker",
                tick_id=tick_id,
                info={"packet_id": packet.get("packet_id", ""),
                      "summary": packet.get("summary", {}),
                      "candidate_count": len(packet.get("candidate_triggers", []))},
            )

            # 调用占位接口
            if self._config.get("enable_placeholder_interfaces", True) and self._config.get("placeholder_script_enabled", True):
                try:
                    from interfaces.innate_script.placeholder_innate_script_api import check_state_window
                    result = check_state_window(packet)
                    self._logger.detail(
                        trace_id=trace_id, step="placeholder_script_response",
                        message_zh="占位脚本接口返回", message_en="Placeholder script interface responded",
                        tick_id=tick_id,
                        info={"placeholder_code": result.get("code", ""), "success": result.get("success", False)},
                    )
                except ImportError:
                    self._logger.detail(
                        trace_id=trace_id, step="placeholder_script_unavailable",
                        message_zh="占位脚本接口不可用（模块未找到）",
                        message_en="Placeholder script interface unavailable (module not found)",
                        tick_id=tick_id,
                    )
            return True
        except Exception as e:
            self._logger.error(
                trace_id=trace_id, interface="broadcast_script_check",
                code="OUTPUT_ERROR", message_zh=f"脚本广播失败: {e}",
                message_en=f"Script broadcast failed: {e}", tick_id=tick_id,
            )
            return False

    def _emit_attention_snapshot(self, trace_id: str, tick_id: str):
        """生成并输出注意力快照（占位）。"""
        try:
            snapshot = self._snapshot.build_attention_snapshot(
                pool_store=self._store, trace_id=trace_id, tick_id=tick_id,
            )
            if self._config.get("enable_placeholder_interfaces", True) and self._config.get("placeholder_attention_enabled", True):
                try:
                    from interfaces.attention.placeholder_attention_api import receive_state_snapshot
                    receive_state_snapshot(snapshot)
                except ImportError:
                    pass
        except Exception:
            pass

    def _log_change_event(self, event: dict, trace_id: str, tick_id: str):
        """将变化事件写入 detail 日志。"""
        if self._config.get("detail_log_dump_change_event", True):
            self._logger.detail(
                trace_id=trace_id, step=event.get("event_type", "unknown_event"),
                message_zh=f"对象状态变化: {event.get('target_item_id', '')}",
                message_en=f"State item changed: {event.get('target_item_id', '')}",
                tick_id=tick_id, info=event,
            )

    def _make_error_response(self, code: str, msg_zh: str, msg_en: str, trace_id: str, start_time: float) -> dict:
        """构建错误响应并记录日志。"""
        self._logger.error(
            trace_id=trace_id, interface="", code=code,
            message_zh=msg_zh, message_en=msg_en,
        )
        return self._make_response(False, code, f"{msg_zh} / {msg_en}",
            trace_id=trace_id, elapsed_ms=self._elapsed_ms(start_time))

    @staticmethod
    def _elapsed_ms(start: float) -> int:
        return int((time.time() - start) * 1000)

    @staticmethod
    def _make_response(success: bool, code: str, message: str,
                       data: Any = None, error: Any = None,
                       trace_id: str = "", elapsed_ms: int = 0) -> dict:
        """构建 AP 标准统一返回结构。"""
        return {
            "success": success, "code": code, "message": message,
            "data": data, "error": error,
            "meta": {
                "module": __module_name__, "interface": "",
                "trace_id": trace_id, "elapsed_ms": elapsed_ms, "logged": True,
            },
        }
