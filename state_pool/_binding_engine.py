# -*- coding: utf-8 -*-
"""
AP 状态池模块 — 绑定引擎
==========================
负责将属性刺激元（如认知感受节点、奖惩节点）绑定到已有 SA/CSA 上。
若目标是独立 SA 且尚无 CSA，自动创建绑定型 CSA。
"""

import time
from ._id_generator import next_id
from . import __schema_version__


class BindingEngine:
    """
    绑定引擎。

    核心逻辑:
      1. 校验 attribute_sa 的 role 必须为 attribute
      2. 校验目标对象存在且类型支持
      3. 若目标是 SA → 自动创建绑定型 CSA
      4. 若目标是 CSA → 直接附加属性
      5. 去重：按 id 或 content 去重
    """

    def __init__(self, config: dict):
        self._config = config

    def validate_attribute_sa(self, attribute_sa: dict) -> str | None:
        """
        校验属性 SA 格式。

        返回:
            None 表示校验通过；否则返回错误消息
        """
        if not isinstance(attribute_sa, dict):
            return "attribute_sa 不是 dict / attribute_sa is not a dict"

        obj_type = attribute_sa.get("object_type", "")
        if obj_type != "sa":
            return f"attribute_sa.object_type 必须为 'sa'，实际为 '{obj_type}' / must be 'sa', got '{obj_type}'"

        role = attribute_sa.get("stimulus", {}).get("role", "")
        if role != "attribute":
            return f"attribute_sa.stimulus.role 必须为 'attribute'，实际为 '{role}' / must be 'attribute', got '{role}'"

        if not attribute_sa.get("id"):
            return "attribute_sa 缺少 id / attribute_sa missing id"

        return None

    def bind_to_sa_item(
        self,
        target_item: dict,
        attribute_sa: dict,
        pool_store,
        trace_id: str,
        tick_id: str,
        tick_number: int,
        source_module: str = "unknown",
    ) -> dict:
        """
        将属性SA绑定到目标SA类型的state_item。
        自动创建绑定型 CSA state_item。

        返回:
            绑定结果 dict
        """
        now_ms = int(time.time() * 1000)
        attr_id = attribute_sa["id"]

        # 去重检查
        if self._should_deduplicate(target_item, attr_id, attribute_sa):
            return {
                "created_new_csa": False,
                "deduplicated": True,
                "bound_attribute_sa_id": attr_id,
            }

        # 将属性SA记录到绑定状态
        target_item["binding_state"]["bound_attribute_sa_ids"].append(attr_id)
        self._append_bound_attribute_snapshot(target_item, attribute_sa)
        target_item["updated_at"] = now_ms

        if self._config.get("allow_auto_create_csa_on_attribute_bind", True):
            # 创建绑定型 CSA state_item
            csa_spi_id = next_id("spi")
            target_ref_id = target_item.get("ref_object_id", "")
            target_display = self._extract_target_display(target_item)
            attribute_display = self._extract_attribute_display(attribute_sa)

            csa_item = {
                "id": csa_spi_id,
                "object_type": "state_item",
                "sub_type": "csa_binding_item",
                "schema_version": __schema_version__,
                "ref_object_type": "csa",
                "ref_object_id": f"csa_bind_{target_ref_id}_{attr_id}",
                "ref_snapshot": {
                    "content_display": f"BindingCSA[{target_display or target_ref_id}]",
                    "content_display_detail": f"anchor={target_display or target_ref_id} | attrs={attribute_display}",
                    "source_module": source_module,
                    "anchor_sa_ref": target_ref_id,
                    "attribute_sa_id": attr_id,
                    "anchor_display": target_display,
                    "attribute_displays": [attribute_display],
                    "bound_attribute_displays": [attribute_display],
                    "member_count": 2,
                },
                "energy": {
                    "er": target_item["energy"]["er"],
                    "ev": target_item["energy"]["ev"],
                    "ownership_level": "aggregated_from_sa",
                    "computed_from_children": True,
                    "fatigue": 0.0,
                    "recency_gain": 1.0,
                    "salience_score": max(target_item["energy"]["er"], target_item["energy"]["ev"]),
                    "cognitive_pressure_delta": target_item["energy"]["cognitive_pressure_delta"],
                    "cognitive_pressure_abs": target_item["energy"]["cognitive_pressure_abs"],
                    "last_decay_tick": 0,
                    "last_decay_at": now_ms,
                },
                "dynamics": {
                    "prev_er": 0.0, "prev_ev": 0.0,
                    "delta_er": target_item["energy"]["er"], "delta_ev": target_item["energy"]["ev"],
                    "er_change_rate": 0.0, "ev_change_rate": 0.0,
                    "prev_cp_delta": 0.0, "prev_cp_abs": 0.0,
                    "delta_cp_delta": target_item["energy"]["cognitive_pressure_delta"],
                    "delta_cp_abs": target_item["energy"]["cognitive_pressure_abs"],
                    "cp_delta_rate": 0.0, "cp_abs_rate": 0.0,
                    "last_update_tick": tick_number, "last_update_at": now_ms,
                    "update_count": 1,
                },
                "binding_state": {
                    "bound_csa_item_id": None,
                    "bound_attribute_sa_ids": [attr_id],
                },
                "lifecycle": {
                    "created_in_tick": tick_number,
                    "last_active_tick": tick_number,
                    "elimination_candidate": False,
                },
                "source": {
                    "module": "state_pool",
                    "interface": "bind_attribute_node_to_object",
                    "origin": "attribute_binding",
                    "origin_id": target_item["id"],
                    "parent_ids": [target_item["id"], attr_id],
                },
                "trace_id": trace_id,
                "tick_id": tick_id,
                "created_at": now_ms,
                "updated_at": now_ms,
                "status": "active",
                "ext": {"attribute_sa_snapshot": attribute_sa},
                "meta": {
                    "confidence": 1.0,
                    "field_registry_version": __schema_version__,
                    "debug": {},
                    "ext": {},
                },
            }

            pool_store.insert(csa_item)
            target_item["binding_state"]["bound_csa_item_id"] = csa_spi_id

            return {
                "created_new_csa": True,
                "deduplicated": False,
                "bound_csa_item_id": csa_spi_id,
                "bound_attribute_sa_id": attr_id,
            }

        return {
            "created_new_csa": False,
            "deduplicated": False,
            "bound_attribute_sa_id": attr_id,
        }

    def bind_to_csa_item(
        self,
        target_item: dict,
        attribute_sa: dict,
        trace_id: str,
        tick_id: str,
        tick_number: int,
    ) -> dict:
        """将属性SA绑定到CSA类型的state_item。直接附加。"""
        now_ms = int(time.time() * 1000)
        attr_id = attribute_sa["id"]

        if self._should_deduplicate(target_item, attr_id, attribute_sa):
            return {"created_new_csa": False, "deduplicated": True, "bound_attribute_sa_id": attr_id}

        target_item["binding_state"]["bound_attribute_sa_ids"].append(attr_id)
        target_item["updated_at"] = now_ms
        target_item["lifecycle"]["last_active_tick"] = tick_number

        # 将属性SA快照存入ext
        ext_attrs = target_item.get("ext", {}).get("bound_attributes", [])
        ext_attrs.append(attribute_sa)
        target_item.setdefault("ext", {})["bound_attributes"] = ext_attrs
        self._append_bound_attribute_snapshot(target_item, attribute_sa)

        return {
            "created_new_csa": False,
            "deduplicated": False,
            "bound_csa_item_id": target_item["id"],
            "bound_attribute_sa_id": attr_id,
        }

    def _should_deduplicate(self, target_item: dict, attr_id: str, attribute_sa: dict) -> bool:
        """检查是否应去重。"""
        existing_ids = target_item.get("binding_state", {}).get("bound_attribute_sa_ids", [])

        if self._config.get("attribute_bind_deduplicate_by_id", True):
            if attr_id in existing_ids:
                return True

        if self._config.get("attribute_bind_deduplicate_by_content", False):
            # 按内容去重（比较 content.raw）
            raw = attribute_sa.get("content", {}).get("raw", "")
            ext_attrs = target_item.get("ext", {}).get("bound_attributes", [])
            for existing_attr in ext_attrs:
                if existing_attr.get("content", {}).get("raw", "") == raw:
                    return True

        return False

    def _append_bound_attribute_snapshot(self, target_item: dict, attribute_sa: dict):
        """把绑定属性写回目标对象，便于快照与交互解释。"""
        attribute_display = self._extract_attribute_display(attribute_sa)
        content = attribute_sa.get("content", {}) if isinstance(attribute_sa.get("content", {}), dict) else {}
        attr_name = str(content.get("attribute_name", "") or "").strip()
        if not attr_name:
            raw = str(content.get("raw", "") or "")
            if ":" in raw:
                attr_name = raw.split(":", 1)[0].strip()

        ext = target_item.setdefault("ext", {})
        ext_attrs = list(ext.get("bound_attributes", []) or [])
        # Replace-by-name (recommended) / 按属性名替换（推荐）
        # 目的：避免同一个目标对象出现大量重复 runtime_attrs，导致前端难以验收。
        # 例如：时间感受、违和感、奖励信号等通常希望“同名只保留最新一条”。
        #
        # 默认策略：若 attribute_name 可解析，则同名属性会被替换为最新绑定。
        replace_by_name = bool(self._config.get("attribute_bind_replace_by_attribute_name", True))
        removed_ids: list[str] = []
        if replace_by_name and attr_name:
            kept = []
            for existing in ext_attrs:
                if not isinstance(existing, dict):
                    continue
                ex_content = existing.get("content", {}) if isinstance(existing.get("content", {}), dict) else {}
                ex_name = str(ex_content.get("attribute_name", "") or "").strip()
                if not ex_name:
                    ex_raw = str(ex_content.get("raw", "") or "")
                    if ":" in ex_raw:
                        ex_name = ex_raw.split(":", 1)[0].strip()
                if ex_name and ex_name == attr_name:
                    ex_id = str(existing.get("id", "") or "")
                    if ex_id:
                        removed_ids.append(ex_id)
                    continue
                kept.append(existing)
            ext_attrs = kept

        # Upsert by id / 按 id 去重追加（保持原有行为）
        attr_id = str(attribute_sa.get("id", "") or "")
        if not any(isinstance(existing, dict) and str(existing.get("id", "") or "") == attr_id for existing in ext_attrs):
            ext_attrs.append(attribute_sa)
        ext["bound_attributes"] = ext_attrs

        # Keep binding_state ids in sync (so it won't grow forever).
        # 同步 binding_state.bound_attribute_sa_ids（防止无限增长）。
        bs = target_item.setdefault("binding_state", {})
        if isinstance(bs, dict):
            bs_ids = [str(x) for x in (bs.get("bound_attribute_sa_ids", []) or []) if str(x)]
            # Drop removed ids + keep only ids that still exist in ext_attrs.
            alive_ids = {str(a.get("id", "") or "") for a in ext_attrs if isinstance(a, dict) and str(a.get("id", "") or "")}
            bs_ids = [x for x in bs_ids if x in alive_ids and x not in set(removed_ids)]
            # Ensure current attr id is present.
            if attr_id and attr_id in alive_ids and attr_id not in bs_ids:
                bs_ids.append(attr_id)
            bs["bound_attribute_sa_ids"] = bs_ids

        ref_snapshot = target_item.setdefault("ref_snapshot", {})
        # Rebuild display list from ext_attrs so "replace_by_name" can remove old displays.
        bound_displays = []
        for ex in ext_attrs:
            if not isinstance(ex, dict):
                continue
            disp = self._extract_attribute_display(ex)
            if disp and disp not in bound_displays:
                bound_displays.append(disp)
        ref_snapshot["bound_attribute_displays"] = bound_displays

        detail_parts = []
        if ref_snapshot.get("anchor_display"):
            detail_parts.append(f"anchor={ref_snapshot.get('anchor_display')}")
        if ref_snapshot.get("attribute_displays"):
            detail_parts.append(f"attrs={', '.join(ref_snapshot.get('attribute_displays', [])[:4])}")
        if bound_displays:
            detail_parts.append(f"runtime_attrs={', '.join(bound_displays[:4])}")
        if detail_parts:
            ref_snapshot["content_display_detail"] = " | ".join(detail_parts)

    @staticmethod
    def _extract_attribute_display(attribute_sa: dict) -> str:
        content = attribute_sa.get("content", {})
        return str(content.get("display", content.get("raw", attribute_sa.get("id", ""))))

    @staticmethod
    def _extract_target_display(target_item: dict) -> str:
        ref_snapshot = target_item.get("ref_snapshot", {})
        return str(
            ref_snapshot.get(
                "content_display",
                target_item.get("ref_object_id", target_item.get("id", "")),
            )
        )

    def update_config(self, config: dict):
        self._config = config
