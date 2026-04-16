# -*- coding: utf-8 -*-
"""
AP 状态池模块 — 快照引擎
==========================
生成状态池的各种快照结构：
  - state_snapshot（供调试/测试/前端展示）
  - script_check_packet（供脚本检查抄送）
  - attention_snapshot（供注意力过滤器）
"""

import time
from ._id_generator import next_id
from . import __schema_version__


class SnapshotEngine:
    """
    快照引擎。

    负责从当前池状态和事件窗口中导出各类快照结构。
    """

    def __init__(self, config: dict):
        self._config = config

    def build_state_snapshot(
        self,
        pool_store,
        history_window,
        trace_id: str,
        tick_id: str = "",
        include_items: bool = True,
        include_history_window: bool = True,
        top_k: int | None = None,
        sort_by: str = "cp_abs",
    ) -> dict:
        """生成状态池快照。"""
        all_items = pool_store.get_all()

        # 统计
        high_er_count = sum(1 for i in all_items if i["energy"]["er"] >= 0.5)
        high_ev_count = sum(1 for i in all_items if i["energy"]["ev"] >= 0.5)
        high_cp_count = sum(1 for i in all_items if i["energy"]["cognitive_pressure_abs"] >= 0.5)
        type_counts: dict[str, int] = {}
        bound_attribute_item_count = 0
        binding_csa_item_count = 0

        for item in all_items:
            ref_type = item.get("ref_object_type", "unknown")
            type_counts[ref_type] = type_counts.get(ref_type, 0) + 1
            if item.get("binding_state", {}).get("bound_attribute_sa_ids"):
                bound_attribute_item_count += 1
            if item.get("sub_type") == "csa_binding_item":
                binding_csa_item_count += 1

        summary = {
            "active_item_count": len(all_items),
            "high_er_item_count": high_er_count,
            "high_ev_item_count": high_ev_count,
            "high_cp_item_count": high_cp_count,
            "object_type_counts": type_counts,
            "bound_attribute_item_count": bound_attribute_item_count,
            "binding_csa_item_count": binding_csa_item_count,
        }

        # top items
        top_items = []
        if include_items:
            sorted_items = pool_store.get_sorted(sort_by=sort_by, top_k=top_k)
            for item in sorted_items:
                top_items.append(self._build_top_item_summary(item))

        snapshot = {
            "snapshot_id": next_id("sps"),
            "object_type": "runtime_snapshot",
            "sub_type": "state_pool_snapshot",
            "schema_version": __schema_version__,
            "trace_id": trace_id,
            "tick_id": tick_id,
            "timestamp_ms": int(time.time() * 1000),
            "summary": summary,
            "top_items": top_items,
        }

        if include_history_window and history_window:
            snapshot["history_window_ref"] = history_window.get_summary()

        return snapshot

    def build_script_check_packet(
        self,
        events: list[dict],
        pool_store,
        trace_id: str,
        tick_id: str = "",
    ) -> dict:
        """
        生成脚本检查抄送包。

        包含:
          - 本窗口事件列表
          - 变化统计
          - 候选触发摘要（认知压快速上升/下降的对象）
        """
        now_ms = int(time.time() * 1000)
        all_items = pool_store.get_all()

        # 事件起止时间
        window_start = events[0]["timestamp_ms"] if events else now_ms
        window_end = events[-1]["timestamp_ms"] if events else now_ms

        # 统计
        new_count = sum(1 for e in events if e.get("event_type") == "created")
        update_count = sum(1 for e in events if e.get("event_type") == "energy_update")

        # 候选触发
        fast_cp_rise = self._config.get("fast_cp_rise_threshold", 0.5)
        fast_cp_drop = self._config.get("fast_cp_drop_threshold", -0.5)
        candidates = []

        for item in all_items:
            d = item.get("dynamics", {})
            delta_cp_abs = d.get("delta_cp_abs", 0)
            if delta_cp_abs >= fast_cp_rise:
                candidates.append({
                    "item_id": item["id"],
                    "trigger_hint": "cp_abs_rise_fast",
                    "value": round(delta_cp_abs, 6),
                    "display": item.get("ref_snapshot", {}).get("content_display", ""),
                })
            elif delta_cp_abs <= fast_cp_drop:
                candidates.append({
                    "item_id": item["id"],
                    "trigger_hint": "cp_abs_drop_fast",
                    "value": round(delta_cp_abs, 6),
                    "display": item.get("ref_snapshot", {}).get("content_display", ""),
                })

        high_cp_count = sum(1 for i in all_items if i["energy"]["cognitive_pressure_abs"] >= 0.5)
        fast_rise_count = sum(1 for c in candidates if c["trigger_hint"] == "cp_abs_rise_fast")
        fast_drop_count = sum(1 for c in candidates if c["trigger_hint"] == "cp_abs_drop_fast")

        packet = {
            "packet_id": next_id("scp"),
            "object_type": "runtime_snapshot",
            "sub_type": "state_change_window_packet",
            "schema_version": __schema_version__,
            "trace_id": trace_id,
            "tick_id": tick_id,
            "window_start_ms": window_start,
            "window_end_ms": window_end,
            "summary": {
                "active_item_count": len(all_items),
                "new_item_count": new_count,
                "updated_item_count": update_count,
                "high_cp_item_count": high_cp_count,
                "fast_cp_rise_item_count": fast_rise_count,
                "fast_cp_drop_item_count": fast_drop_count,
            },
            "candidate_triggers": candidates,
        }

        # 包含完整事件列表（可配置）
        if self._config.get("script_broadcast_include_full_event_dump", True):
            packet["events"] = events
        else:
            packet["event_count"] = len(events)

        return packet

    def build_attention_snapshot(
        self,
        pool_store,
        trace_id: str,
        tick_id: str = "",
        top_k: int = 64,
    ) -> dict:
        """生成供注意力过滤器使用的摘要快照。"""
        sorted_items = pool_store.get_sorted(sort_by="cp_abs", top_k=top_k)
        items_summary = []
        for item in sorted_items:
            items_summary.append({
                "item_id": item["id"],
                "ref_object_id": item.get("ref_object_id", ""),
                "ref_object_type": item.get("ref_object_type", ""),
                "display": item.get("ref_snapshot", {}).get("content_display", ""),
                "er": item["energy"]["er"],
                "ev": item["energy"]["ev"],
                "cp_abs": item["energy"]["cognitive_pressure_abs"],
                "salience": item["energy"].get("salience_score", 0),
            })
        return {
            "snapshot_type": "attention_input",
            "trace_id": trace_id,
            "tick_id": tick_id,
            "total_pool_size": pool_store.size,
            "top_k": top_k,
            "items": items_summary,
        }

    def update_config(self, config: dict):
        self._config = config

    def _build_top_item_summary(self, item: dict) -> dict:
        """构建适合调试、测试和交互演示的状态池项摘要。"""
        ref_snapshot = item.get("ref_snapshot", {})
        energy = item.get("energy", {})
        dynamics = item.get("dynamics", {})
        binding_state = item.get("binding_state", {})

        # Extract time-feeling bucket meta (best-effort) for rules/actions.
        # 从绑定属性中提取“时间感受”的桶元信息（尽力而为），供 IESM/行动参数透传使用：
        # - 避免必须把时间桶节点常驻入池才能拿到时间间隔参数（对齐理论 4.2.6~4.2.7）。
        time_bucket_ref_object_id = ""
        time_bucket_id = ""
        time_bucket_label_zh = ""
        time_bucket_unit = ""
        time_basis = ""
        time_bucket_center_sec: float | None = None
        try:
            for attr in item.get("ext", {}).get("bound_attributes", []) or []:
                if not isinstance(attr, dict):
                    continue
                content = attr.get("content", {}) if isinstance(attr.get("content", {}), dict) else {}
                attr_name = str(content.get("attribute_name", "") or "").strip()
                if not attr_name:
                    raw = str(content.get("raw", "") or "")
                    if ":" in raw:
                        attr_name = raw.split(":", 1)[0].strip()
                if attr_name != "时间感受":
                    continue
                ext_meta = attr.get("meta", {}).get("ext", {}) if isinstance(attr.get("meta", {}).get("ext", {}), dict) else {}
                time_bucket_ref_object_id = str(ext_meta.get("time_bucket_ref_object_id", "") or "").strip()
                time_bucket_id = str(ext_meta.get("time_bucket_id", "") or "").strip()
                time_bucket_label_zh = str(ext_meta.get("time_bucket_label_zh", "") or "").strip()
                time_bucket_unit = str(ext_meta.get("time_bucket_unit", "") or "").strip()
                time_basis = str(ext_meta.get("time_basis", "") or "").strip()
                try:
                    if ext_meta.get("time_bucket_center_sec", None) is not None:
                        time_bucket_center_sec = float(ext_meta.get("time_bucket_center_sec"))
                except Exception:
                    time_bucket_center_sec = None
                break
        except Exception:
            time_bucket_ref_object_id = ""
            time_bucket_id = ""
            time_bucket_label_zh = ""
            time_bucket_unit = ""
            time_basis = ""
            time_bucket_center_sec = None

        bound_attributes = [
            attr.get("content", {}).get("display", attr.get("content", {}).get("raw", attr.get("id", "")))
            for attr in item.get("ext", {}).get("bound_attributes", [])
            if isinstance(attr, dict)
        ]
        # attribute_names（属性名稳定键）
        # ------------------------------------------------
        # 理论对齐点：
        # - CFS（认知感受信号）、奖励/惩罚、时间感受等都应作为“属性刺激元（attribute SA）”进入系统匹配与记忆闭环。
        #
        # 工程对齐点：
        # - 目前属性有两条入口：
        #   1) packet 属性（来自感受器/回忆反哺 stimulus_packet）：binding_state.packet_attribute_by_name
        #   2) runtime 绑定属性（来自 IESM/time_sensor 等运行态绑定）：binding_state.bound_attribute_by_name / ext.bound_attributes
        #
        # 这三组字段的用途：
        # - packet_attribute_names: 用于检视“记忆/结构侧”属性是否真正进入刺激流与结构形成（验收要求）。
        # - runtime_attribute_names: 用于检视“运行态绑定”是否生效（IESM/时间感受器等）。
        # - all_attribute_names: 规则引擎与前端推荐使用的统一口径（避免仅靠 display contains_text 导致易碎）。
        binding_state = item.get("binding_state", {}) if isinstance(item.get("binding_state", {}), dict) else {}
        packet_by_name = binding_state.get("packet_attribute_by_name", {})
        packet_attribute_names = (
            sorted([str(k) for k in packet_by_name.keys() if str(k)]) if isinstance(packet_by_name, dict) else []
        )
        runtime_by_name = binding_state.get("bound_attribute_by_name", {})
        runtime_attribute_names = (
            sorted([str(k) for k in runtime_by_name.keys() if str(k)]) if isinstance(runtime_by_name, dict) else []
        )

        # Backward-compatible fallback: ext.bound_attributes -> runtime_attribute_names
        # 兼容兜底：如果没有 bound_attribute_by_name，则从 ext.bound_attributes 推断 runtime 属性名。
        if not runtime_attribute_names:
            inferred: list[str] = []
            for attr in item.get("ext", {}).get("bound_attributes", []) or []:
                if not isinstance(attr, dict):
                    continue
                content = attr.get("content", {}) if isinstance(attr.get("content", {}), dict) else {}
                name = str(content.get("attribute_name", "") or "").strip()
                if not name:
                    raw = str(content.get("raw", "") or "")
                    if ":" in raw:
                        name = raw.split(":", 1)[0].strip()
                    else:
                        name = raw.strip()
                if name:
                    inferred.append(name)
            runtime_attribute_names = sorted(set(inferred))

        # all_attribute_names: union (dedupe, keep stable order: packet -> runtime)
        seen_names: set[str] = set()
        all_attribute_names: list[str] = []
        for name in [*packet_attribute_names, *runtime_attribute_names]:
            if not name or name in seen_names:
                continue
            seen_names.add(name)
            all_attribute_names.append(name)

        # Backward compatibility: keep existing field name for older UI/logic.
        # 向后兼容：保留历史字段 bound_attribute_names（旧 UI/逻辑仍可能读取）。
        bound_attribute_names = list(runtime_attribute_names)

        return {
            "item_id": item["id"],
            "ref_object_id": item.get("ref_object_id", ""),
            "ref_object_type": item.get("ref_object_type", ""),
            # ref_alias_ids: 同一“语义对象”在不同模块/阶段可能拥有不同 ref_id（例如 sa_* 与 st_*）。
            # 在状态池里它们会被语义合并为同一运行态对象；为保证观测台与规则引擎能正确解析目标对象，
            # 这里显式输出别名列表，避免前端只剩 st_000xxx 这种 ID 看不到内容的问题。
            #
            # ref_alias_ids: one semantic object may have multiple ref ids across modules/phases
            # (e.g. sa_* and st_*). We expose aliases for UI resolution and rule-engine targeting.
            "ref_alias_ids": list(item.get("ref_alias_ids", []) or []),
            "display": ref_snapshot.get("content_display", ""),
            "display_detail": self._build_display_detail(item, bound_attributes),
            "anchor_display": ref_snapshot.get("anchor_display", ""),
            "semantic_signature": str(item.get("semantic_signature", "") or ""),
            "attribute_displays": list(ref_snapshot.get("attribute_displays", [])),
            # feature_displays / 特征展示（例如 CSA 的非属性成员摘要）
            # 用途：先天规则 metric 选择器 contains_text 可用来匹配“包含某特征”的对象。
            "feature_displays": list(ref_snapshot.get("feature_displays", [])),
            "bound_attribute_displays": list(ref_snapshot.get("bound_attribute_displays", [])),
            # time_bucket_*: 时间桶元信息（仅当存在“时间感受”绑定属性时才有值）
            "time_bucket_ref_object_id": time_bucket_ref_object_id,
            "time_bucket_id": time_bucket_id,
            "time_bucket_label_zh": time_bucket_label_zh,
            "time_bucket_unit": time_bucket_unit,
            "time_basis": time_basis,
            "time_bucket_center_sec": time_bucket_center_sec,
            # packet_attribute_names: packet 属性名（来自刺激包/回忆反哺）
            "packet_attribute_names": list(packet_attribute_names),
            # runtime_attribute_names: 运行态绑定属性名（来自 IESM/time_sensor 等）
            "runtime_attribute_names": list(runtime_attribute_names),
            # all_attribute_names: 推荐规则引擎/前端统一口径
            "all_attribute_names": list(all_attribute_names),
            "bound_attribute_names": list(bound_attribute_names),
            "member_count": ref_snapshot.get("member_count", 0),
            "er": energy.get("er", 0),
            "ev": energy.get("ev", 0),
            "cp_delta": energy.get("cognitive_pressure_delta", 0),
            "cp_abs": energy.get("cognitive_pressure_abs", 0),
            "salience_score": energy.get("salience_score", 0),
            "fatigue": energy.get("fatigue", 0),
            "recency_gain": energy.get("recency_gain", 0),
            "delta_er": dynamics.get("delta_er", 0),
            "delta_ev": dynamics.get("delta_ev", 0),
            # 说明：
            # - delta_cp_delta / cp_delta_rate 在能量引擎里会更新，但旧版摘要里没暴露；
            # - IESM 的 metric 条件（获得认知压/变化率）会用到它们作为早期 tick 的 fallback。
            "delta_cp_delta": dynamics.get("delta_cp_delta", 0),
            "delta_cp_abs": dynamics.get("delta_cp_abs", 0),
            "er_change_rate": dynamics.get("er_change_rate", 0),
            "ev_change_rate": dynamics.get("ev_change_rate", 0),
            "cp_delta_rate": dynamics.get("cp_delta_rate", 0),
            "cp_abs_rate": dynamics.get("cp_abs_rate", 0),
            "update_count": dynamics.get("update_count", 0),
            "last_update_tick": dynamics.get("last_update_tick", 0),
            "bound_attribute_count": len(binding_state.get("bound_attribute_sa_ids", [])),
            "bound_csa_item_id": binding_state.get("bound_csa_item_id"),
            "status": item.get("status", "active"),
            "updated_at": item.get("updated_at", 0),
            "created_at": item.get("created_at", 0),
        }

    @staticmethod
    def _build_display_detail(item: dict, bound_attributes: list[str]) -> str:
        """生成适合交互演示的人类可读解释摘要。"""
        ref_snapshot = item.get("ref_snapshot", {})
        detail = ref_snapshot.get("content_display_detail")
        if detail:
            return detail

        parts = []
        if ref_snapshot.get("anchor_display"):
            parts.append(f"anchor={ref_snapshot.get('anchor_display')}")
        if ref_snapshot.get("attribute_displays"):
            parts.append(f"attrs={', '.join(ref_snapshot.get('attribute_displays', [])[:4])}")
        if bound_attributes:
            parts.append(f"runtime_attrs={', '.join(bound_attributes[:4])}")
        if ref_snapshot.get("member_count"):
            parts.append(f"members={ref_snapshot.get('member_count')}")
        return " | ".join(parts)
