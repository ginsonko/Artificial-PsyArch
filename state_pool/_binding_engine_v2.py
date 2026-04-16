# -*- coding: utf-8 -*-
"""
AP 状态池模块 - 属性绑定引擎（清晰实现版）
========================================

旧版文件中存在较重的编码痕迹，本实现保持相同类名与主要行为，
但把运行时属性绑定、展示字段回写和自动创建绑定型 CSA 的逻辑整理为可维护版本。
"""

from __future__ import annotations

import time

from . import __schema_version__
from ._id_generator import next_id


class BindingEngine:
    """负责把属性 SA 绑定到 SA / CSA 类型的状态池对象。"""

    def __init__(self, config: dict):
        self._config = config

    def validate_attribute_sa(self, attribute_sa: dict) -> str | None:
        if not isinstance(attribute_sa, dict):
            return "attribute_sa 不是 dict / attribute_sa is not a dict"

        if attribute_sa.get("object_type") != "sa":
            actual = attribute_sa.get("object_type", "")
            return (
                f"attribute_sa.object_type 必须为 'sa'，实际为 '{actual}' / "
                f"must be 'sa', got '{actual}'"
            )

        role = attribute_sa.get("stimulus", {}).get("role", "")
        if role != "attribute":
            return (
                f"attribute_sa.stimulus.role 必须为 'attribute'，实际为 '{role}' / "
                f"must be 'attribute', got '{role}'"
            )

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
        now_ms = int(time.time() * 1000)
        attr_id = attribute_sa["id"]

        # Dedup-by-id should still allow update semantics, otherwise dynamic attributes
        # (e.g. 违和感强度、期待强度) would become "write once then stale".
        # 按 ID 去重时仍要允许覆盖更新，否则动态属性会变成“一次写入后永远不变”，影响可观测性。
        existing_ids = target_item.get("binding_state", {}).get("bound_attribute_sa_ids", [])
        if (
            self._config.get("attribute_bind_deduplicate_by_id", True)
            and isinstance(existing_ids, list)
            and attr_id in existing_ids
        ):
            target_item["updated_at"] = now_ms
            target_item.setdefault("lifecycle", {})["last_active_tick"] = tick_number
            self._append_bound_attribute_snapshot(target_item, attribute_sa, now_ms=now_ms)
            return {
                "created_new_csa": False,
                "deduplicated": True,
                "updated_existing": True,
                "bound_attribute_sa_id": attr_id,
                "bound_csa_item_id": target_item.get("binding_state", {}).get("bound_csa_item_id"),
            }

        if self._config.get("attribute_bind_deduplicate_by_content", False) and self._should_deduplicate(target_item, attr_id, attribute_sa):
            return {
                "created_new_csa": False,
                "deduplicated": True,
                "updated_existing": False,
                "bound_attribute_sa_id": attr_id,
                "bound_csa_item_id": target_item.get("binding_state", {}).get("bound_csa_item_id"),
            }

        target_item.setdefault("binding_state", {}).setdefault("bound_attribute_sa_ids", []).append(attr_id)
        target_item["updated_at"] = now_ms
        target_item.setdefault("lifecycle", {})["last_active_tick"] = tick_number
        self._append_bound_attribute_snapshot(target_item, attribute_sa, now_ms=now_ms)

        # 默认不自动创建“绑定型 CSA”：CSA 主要承担匹配约束作用，不一定要以独立对象存在于 SP。
        allow_auto = bool(self._config.get("allow_auto_create_csa_on_attribute_bind", False))
        bound_csa_item_id = target_item.get("binding_state", {}).get("bound_csa_item_id")
        created_new_csa = False
        if allow_auto:
            existing = pool_store.get(bound_csa_item_id) if bound_csa_item_id else None
            if existing is None:
                # 创建绑定型 CSA state_item（synthetic），用于观测与后续脚本/匹配消费。
                anchor_ref_id = str(target_item.get("ref_object_id", "") or "")
                anchor_display = self._extract_target_display(target_item)
                attribute_display = self._extract_attribute_display(attribute_sa)
                csa_spi_id = next_id("spi")
                csa_item = {
                    "id": csa_spi_id,
                    "object_type": "state_item",
                    "sub_type": "csa_binding_item",
                    "schema_version": __schema_version__,
                    "ref_object_type": "csa",
                    "ref_object_id": f"csa_bind_{anchor_ref_id}_{attr_id}",
                    "ref_alias_ids": [],
                    "ref_snapshot": {
                        "content_display": f"BindingCSA[{anchor_display or anchor_ref_id}]",
                        "content_display_detail": f"anchor={anchor_display or anchor_ref_id} | attrs={attribute_display}",
                        "source_module": source_module,
                        "anchor_sa_ref": anchor_ref_id,
                        "attribute_sa_id": attr_id,
                        "anchor_display": anchor_display,
                        "attribute_displays": [attribute_display] if attribute_display else [],
                        "bound_attribute_displays": [attribute_display] if attribute_display else [],
                        "member_count": 2,
                    },
                    "semantic_signature": "",
                    "energy": {
                        "er": float(target_item.get("energy", {}).get("er", 0.0) or 0.0),
                        "ev": float(target_item.get("energy", {}).get("ev", 0.0) or 0.0),
                        "ownership_level": "aggregated_from_sa",
                        "computed_from_children": True,
                        "fatigue": 0.0,
                        "recency_gain": 1.0,
                        "salience_score": float(target_item.get("energy", {}).get("salience_score", 0.0) or 0.0),
                        "cognitive_pressure_delta": float(target_item.get("energy", {}).get("cognitive_pressure_delta", 0.0) or 0.0),
                        "cognitive_pressure_abs": float(target_item.get("energy", {}).get("cognitive_pressure_abs", 0.0) or 0.0),
                        "last_decay_tick": 0,
                        "last_decay_at": now_ms,
                    },
                    "dynamics": {
                        "prev_er": 0.0,
                        "prev_ev": 0.0,
                        "delta_er": float(target_item.get("energy", {}).get("er", 0.0) or 0.0),
                        "delta_ev": float(target_item.get("energy", {}).get("ev", 0.0) or 0.0),
                        "er_change_rate": 0.0,
                        "ev_change_rate": 0.0,
                        "prev_cp_delta": 0.0,
                        "prev_cp_abs": 0.0,
                        "delta_cp_delta": float(target_item.get("energy", {}).get("cognitive_pressure_delta", 0.0) or 0.0),
                        "delta_cp_abs": float(target_item.get("energy", {}).get("cognitive_pressure_abs", 0.0) or 0.0),
                        "cp_delta_rate": 0.0,
                        "cp_abs_rate": 0.0,
                        "last_update_tick": tick_number,
                        "last_update_at": now_ms,
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
                        "origin_id": target_item.get("id", ""),
                        "parent_ids": [target_item.get("id", ""), attr_id],
                    },
                    "trace_id": trace_id,
                    "tick_id": tick_id,
                    "created_at": now_ms,
                    "updated_at": now_ms,
                    "status": "active",
                    "ext": {"bound_attributes": [attribute_sa]},
                    "meta": {
                        "confidence": 1.0,
                        "field_registry_version": __schema_version__,
                        "debug": {},
                        "ext": {},
                    },
                }
                pool_store.insert(csa_item)
                target_item["binding_state"]["bound_csa_item_id"] = csa_spi_id
                bound_csa_item_id = csa_spi_id
                created_new_csa = True
            else:
                # 若已有绑定型 CSA，则同步把该属性写入其展示快照，便于观测。
                existing.setdefault("binding_state", {}).setdefault("bound_attribute_sa_ids", []).append(attr_id)
                existing["updated_at"] = now_ms
                existing.setdefault("lifecycle", {})["last_active_tick"] = tick_number
                self._append_bound_attribute_snapshot(existing, attribute_sa)

        return {
            "created_new_csa": created_new_csa,
            "deduplicated": False,
            "bound_attribute_sa_id": attr_id,
            "bound_csa_item_id": bound_csa_item_id,
        }

    def bind_to_csa_item(
        self,
        target_item: dict,
        attribute_sa: dict,
        trace_id: str,
        tick_id: str,
        tick_number: int,
    ) -> dict:
        now_ms = int(time.time() * 1000)
        attr_id = attribute_sa["id"]

        existing_ids = target_item.get("binding_state", {}).get("bound_attribute_sa_ids", [])
        if (
            self._config.get("attribute_bind_deduplicate_by_id", True)
            and isinstance(existing_ids, list)
            and attr_id in existing_ids
        ):
            target_item["updated_at"] = now_ms
            target_item.setdefault("lifecycle", {})["last_active_tick"] = tick_number
            self._append_bound_attribute_snapshot(target_item, attribute_sa, now_ms=now_ms)
            return {
                "created_new_csa": False,
                "deduplicated": True,
                "updated_existing": True,
                "bound_attribute_sa_id": attr_id,
            }

        if self._config.get("attribute_bind_deduplicate_by_content", False) and self._should_deduplicate(target_item, attr_id, attribute_sa):
            return {
                "created_new_csa": False,
                "deduplicated": True,
                "updated_existing": False,
                "bound_attribute_sa_id": attr_id,
            }

        target_item.setdefault("binding_state", {}).setdefault("bound_attribute_sa_ids", []).append(attr_id)
        target_item["updated_at"] = now_ms
        target_item.setdefault("lifecycle", {})["last_active_tick"] = tick_number
        self._append_bound_attribute_snapshot(target_item, attribute_sa, now_ms=now_ms)

        return {
            "created_new_csa": False,
            "deduplicated": False,
            "bound_csa_item_id": target_item["id"],
            "bound_attribute_sa_id": attr_id,
        }

    def _should_deduplicate(self, target_item: dict, attr_id: str, attribute_sa: dict) -> bool:
        existing_ids = target_item.get("binding_state", {}).get("bound_attribute_sa_ids", [])
        if self._config.get("attribute_bind_deduplicate_by_id", True) and attr_id in existing_ids:
            return True

        if self._config.get("attribute_bind_deduplicate_by_content", False):
            raw = attribute_sa.get("content", {}).get("raw", "")
            ext_attrs = target_item.get("ext", {}).get("bound_attributes", [])
            for existing_attr in ext_attrs:
                if isinstance(existing_attr, dict) and existing_attr.get("content", {}).get("raw", "") == raw:
                    return True

        return False

    def _append_bound_attribute_snapshot(self, target_item: dict, attribute_sa: dict, *, now_ms: int | None = None):
        """
        Record a lightweight runtime-attribute view on the target item.
        在目标对象上记录一份轻量的运行态属性视图（用于观测与规则匹配）。

        Key design / 关键设计：
        - 按 attribute_id 支持覆盖更新（避免动态属性“写一次就过期”）。
        - 按 attribute_name 维护稳定的展示列表（避免 display 值变化导致 bound_attribute_displays 无限增长）。
        """
        now_ms = int(now_ms or (time.time() * 1000))
        attribute_display = self._extract_attribute_display(attribute_sa)

        # ---- ext.bound_attributes: keep the newest snapshot per id ----
        ext = target_item.setdefault("ext", {})
        ext_attrs = list(ext.get("bound_attributes", []) or [])
        replaced = False
        for i, existing in enumerate(ext_attrs):
            if isinstance(existing, dict) and existing.get("id") == attribute_sa.get("id"):
                ext_attrs[i] = attribute_sa
                replaced = True
                break
        if not replaced:
            ext_attrs.append(attribute_sa)
        ext["bound_attributes"] = ext_attrs

        # ---- binding_state.bound_attribute_by_name: stable mapping ----
        content = attribute_sa.get("content", {}) or {}
        attr_name = str(content.get("attribute_name", "") or "").strip()
        if not attr_name:
            raw = str(content.get("raw", "") or "")
            if ":" in raw:
                attr_name = raw.split(":", 1)[0].strip()
            else:
                attr_name = raw.strip()
        if attr_name:
            binding_state = target_item.setdefault("binding_state", {})
            by_name = binding_state.setdefault("bound_attribute_by_name", {})
            if isinstance(by_name, dict):
                by_name[attr_name] = {
                    "attribute_name": attr_name,
                    "display": attribute_display,
                    "sa_id": str(attribute_sa.get("id", "") or ""),
                    "updated_at": now_ms,
                }

        ref_snapshot = target_item.setdefault("ref_snapshot", {})
        # 对绑定型 CSA（以及真实 CSA state_item）同步更新 attribute_displays，便于快照解释与测试。
        if target_item.get("ref_object_type") == "csa" or target_item.get("sub_type") == "csa_binding_item":
            attr_displays = list(ref_snapshot.get("attribute_displays", []))
            if attribute_display and attribute_display not in attr_displays:
                attr_displays.append(attribute_display)
            ref_snapshot["attribute_displays"] = attr_displays

        # ---- ref_snapshot.bound_attribute_displays: stable ordered view ----
        by_name = target_item.get("binding_state", {}).get("bound_attribute_by_name", {})
        if isinstance(by_name, dict) and by_name:
            ordered = sorted(list(by_name.values()), key=lambda row: str(row.get("attribute_name", "")))
            bound_displays = [str(row.get("display", "")) for row in ordered if str(row.get("display", ""))]
            ref_snapshot["bound_attribute_displays"] = bound_displays
        else:
            bound_displays = list(ref_snapshot.get("bound_attribute_displays", []))
            if attribute_display and attribute_display not in bound_displays:
                bound_displays.append(attribute_display)
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
        return str(ref_snapshot.get("content_display", target_item.get("ref_object_id", target_item.get("id", ""))))

    def update_config(self, config: dict):
        self._config = config
