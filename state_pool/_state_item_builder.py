# -*- coding: utf-8 -*-
"""
AP 状态池模块 — 状态池项构建器
================================
将 SA / CSA / ST / action_node / cfs_signal 等对象转换为标准 state_item。
state_item 是状态池的核心运行态对象，包含完整的能量、动态指标、绑定状态和生命周期信息。
"""

import copy
import time
import re
from typing import Any

from . import __schema_version__, __module_name__
from ._id_generator import next_id
from hdb._sequence_display import format_sequence_groups


# ====================================================================== #
#                     支持的引用对象类型                                    #
# ====================================================================== #

SUPPORTED_REF_TYPES = {"sa", "csa", "st", "sg", "em", "cfs_signal", "action_node"}

# 属性名兜底提取：用于在缺失 attribute_name 字段时，从 token 中解析稳定键。
# 例如： "惩罚信号:存在（punish_signal）" -> punish_signal
_ATTR_NAME_IN_PAREN_RE = re.compile(r"（\\s*([A-Za-z0-9_]+)\\s*）")


def build_state_item(
    ref_object: dict,
    trace_id: str,
    tick_id: str,
    tick_number: int = 0,
    source_module: str = "",
    source_interface: str = "apply_stimulus_packet",
    origin: str = "from_stimulus_packet",
    origin_id: str = "",
    object_lookup: dict[str, dict] | None = None,
) -> dict | None:
    """
    将一个原始对象（SA/CSA/ST等）转换为标准 state_item。

    参数:
        ref_object: 原始对象 dict，必须含 id、object_type、energy
        trace_id: 调用链追踪 ID
        tick_id: 认知滴答 ID
        tick_number: 当前 Tick 编号
        source_module: 来源模块名
        source_interface: 来源接口名
        origin: 来源分类
        origin_id: 来源 ID

    返回:
        state_item dict，若输入非法则返回 None
    """
    # ---- 校验 ----
    obj_type = ref_object.get("object_type", "")
    obj_id = ref_object.get("id", "")
    energy = ref_object.get("energy")

    if not obj_id:
        return None
    if obj_type not in SUPPORTED_REF_TYPES:
        return None
    if not isinstance(energy, dict):
        return None

    # ---- 提取能量 ----
    er = _safe_float(energy.get("er", 0.0))
    ev = _safe_float(energy.get("ev", 0.0))

    now_ms = int(time.time() * 1000)
    spi_id = next_id("spi")

    # ---- 认知压 ----
    cp_delta = er - ev
    cp_abs = abs(cp_delta)

    # ---- 轻量快照 ----
    ref_snapshot = _build_ref_snapshot(ref_object, source_module, object_lookup=object_lookup)
    semantic_signature = _build_semantic_signature(ref_object, object_lookup=object_lookup)
    semantic_labels = _build_semantic_labels(ref_object, object_lookup=object_lookup)

    # ---- sub_type 映射 ----
    sub_type_map = {
        "sa": "sa_runtime_item",
        "csa": "csa_runtime_item",
        "st": "st_runtime_item",
        "sg": "sg_runtime_item",
        "em": "em_runtime_item",
        "cfs_signal": "cfs_runtime_item",
        "action_node": "action_runtime_item",
    }
    sub_type = sub_type_map.get(obj_type, f"{obj_type}_runtime_item")

    ownership_level = str(energy.get("ownership_level") or "runtime_projection")
    computed_from_children = bool(energy.get("computed_from_children", False))
    if obj_type == "csa":
        # CSA 是成员 SA 的聚合视图，默认应标记为 derived-from-children。
        ownership_level = str(energy.get("ownership_level") or "aggregated_from_sa")
        computed_from_children = bool(energy.get("computed_from_children", True))

    # ---- 绑定状态（可扩展） ----
    # 说明：
    # - bound_attribute_*: 运行态绑定属性（IESM/time_sensor 等）
    # - packet_attribute_*: 记忆/结构侧属性（来自 stimulus_packet / memory_feedback / 结构投影）
    #
    # 为什么要区分：
    # - “期待/压力”等认知感受在理论上应来自“学到的结构属性”，而不是脚本当下贴标签；
    # - 因此规则引擎需要能明确选择 scope=packet 或 scope=runtime。
    binding_state: dict[str, Any] = {
        "bound_csa_item_id": None,
        "bound_attribute_sa_ids": [],
    }

    # ---- ST（结构）内嵌属性抽取（学到的属性） ----
    # 重要：这不是“运行态绑定”，而是结构内容本身携带的属性单元（unit_role=attribute）。
    # 将其挂到 binding_state.packet_attribute_by_name，方便规则引擎用 scope=packet 稳定检索。
    if obj_type == "st":
        structure_block = ref_object.get("structure", {}) if isinstance(ref_object.get("structure", {}), dict) else {}
        packet_attr_map = _extract_structure_packet_attributes(structure_block, now_ms=now_ms)
        if packet_attr_map:
            binding_state["packet_attribute_by_name"] = packet_attr_map
            # 同步一份轻量展示列表（供观测台/日志解释），避免前端只能看到 st_000xxx 的裸ID。
            try:
                ordered = sorted(list(packet_attr_map.values()), key=lambda row: str(row.get("attribute_name", "")))
                ref_snapshot["attribute_displays"] = [
                    str(row.get("display", ""))
                    for row in ordered
                    if str(row.get("display", ""))
                ]
            except Exception:
                pass

    return {
        "id": spi_id,
        "object_type": "state_item",
        "sub_type": sub_type,
        "schema_version": __schema_version__,

        # ---- 引用信息 ----
        "ref_object_type": obj_type,
        "ref_object_id": obj_id,
        "ref_alias_ids": [obj_id],
        "ref_snapshot": ref_snapshot,
        "semantic_signature": semantic_signature,

        # ---- 能量 ----
        "energy": {
            "er": er,
            "ev": ev,
            "ownership_level": ownership_level,
            "computed_from_children": computed_from_children,
            "fatigue": 0.0,
            "recency_gain": 1.0,
            "salience_score": max(er, ev),
            "cognitive_pressure_delta": cp_delta,
            "cognitive_pressure_abs": cp_abs,
            "last_decay_tick": 0,
            "last_decay_at": now_ms,
        },

        # ---- 动态指标 ----
        "dynamics": {
            "prev_er": 0.0,
            "prev_ev": 0.0,
            "delta_er": er,
            "delta_ev": ev,
            "er_change_rate": er,    # 初始时视为瞬间从0到er
            "ev_change_rate": ev,
            "prev_cp_delta": 0.0,
            "prev_cp_abs": 0.0,
            "delta_cp_delta": cp_delta,
            "delta_cp_abs": cp_abs,
            "cp_delta_rate": cp_delta,
            "cp_abs_rate": cp_abs,
            "last_update_tick": tick_number,
            "last_update_at": now_ms,
            "update_count": 1,
        },

        # ---- 绑定状态 ----
        "binding_state": binding_state,

        # ---- 生命周期 ----
        "lifecycle": {
            "created_in_tick": tick_number,
            "last_active_tick": tick_number,
            "elimination_candidate": False,
        },

        # ---- 来源 ----
        "source": {
            "module": source_module or __module_name__,
            "interface": source_interface,
            "origin": origin,
            "origin_id": origin_id,
            "parent_ids": [obj_id],
        },

        # ---- 时间和追踪 ----
        "trace_id": trace_id,
        "tick_id": tick_id,
        "created_at": now_ms,
        "updated_at": now_ms,
        "status": "active",
        "ext": {
            "semantic_labels": semantic_labels,
        },
        "meta": {
            "confidence": 1.0,
            "field_registry_version": __schema_version__,
            "debug": {},
            "ext": _build_runtime_meta_ext(ref_object),
        },
    }


def _extract_structure_packet_attributes(structure: dict, *, now_ms: int) -> dict[str, dict]:
    """
    Extract attribute-units from a structure (ST) and convert into packet_attribute_by_name mapping.
    从结构（ST）的 sequence_groups.units 中抽取属性单元（unit_role=attribute），并转成 packet_attribute_by_name 映射。

    Why / 为什么要做这个：
    - 理论对齐：期待/压力应来自“学到的结构属性”（记忆/结构侧），而不是脚本当下贴标签；
    - 工程对齐：StatePool 对外输出 pool_items 摘要时，会把 packet_attribute_names 暴露出来，
      IESM 规则可用 selector.scope=packet 做稳定检测。

    Notes / 注意：
    - 这里的 “packet” 更接近“非运行态绑定”的意思：包括 memory_feedback 带回来的属性，
      也包括结构投影（HDB 结构本身携带的属性单元）。
    - 若 unit 缺失 attribute_name，会尝试从 token 末尾的 “（attr_name）” 兜底解析。
    """
    if not isinstance(structure, dict):
        return {}

    groups = structure.get("sequence_groups", [])
    if not isinstance(groups, list) or not groups:
        return {}

    out: dict[str, dict] = {}
    for group in groups:
        if not isinstance(group, dict):
            continue
        units = group.get("units", [])
        if not isinstance(units, list) or not units:
            continue
        for unit in units:
            if not isinstance(unit, dict):
                continue
            role = str(unit.get("unit_role", unit.get("role", "")) or "").strip()
            if role != "attribute":
                continue

            token = str(unit.get("token", unit.get("display_text", "")) or "").strip()
            attr_name = str(unit.get("attribute_name", "") or "").strip()
            attr_value = unit.get("attribute_value")
            unit_id = str(unit.get("unit_id", unit.get("id", "")) or "").strip()

            if not attr_name and token:
                m = _ATTR_NAME_IN_PAREN_RE.search(token)
                if m:
                    attr_name = str(m.group(1) or "").strip()
            if not attr_name:
                continue

            out[attr_name] = {
                "attribute_name": attr_name,
                "attribute_value": attr_value,
                "display": token or attr_name,
                "sa_id": unit_id,
                "updated_at": int(now_ms),
            }
    return out


def _build_ref_snapshot(
    ref_object: dict,
    source_module: str,
    object_lookup: dict[str, dict] | None = None,
) -> dict:
    """构建原始对象的轻量快照，用于日志和调试。"""
    obj_type = ref_object.get("object_type", "")
    snapshot = {
        "source_module": source_module,
    }

    if obj_type == "sa":
        content = ref_object.get("content", {})
        stimulus = ref_object.get("stimulus", {})
        # Prefer human-friendly display, but never leave it blank.
        # 优先人类可读 display，但不要留空（否则前端会退化显示 runtime_attrs/id）。
        display = (
            content.get("display")
            or content.get("normalized")
            or content.get("raw")
            or ref_object.get("id", "")
        )
        snapshot["content_display"] = display
        snapshot["content_display_detail"] = content.get("normalized") or display
        snapshot["role"] = stimulus.get("role", "")
        snapshot["value_type"] = content.get("value_type", "")
        parent_obj = _extract_parent_feature_object(ref_object, object_lookup=object_lookup)
        if snapshot["role"] == "attribute":
            snapshot["attribute_name"] = content.get("attribute_name", "")
            snapshot["attribute_value"] = content.get("attribute_value")
            if parent_obj:
                snapshot["parent_display"] = _extract_ref_object_display(parent_obj)
                if snapshot["parent_display"]:
                    snapshot["content_display_detail"] = (
                        f"{snapshot['content_display']} | parent={snapshot['parent_display']}"
                    )
    elif obj_type == "csa":
        member_sa_ids = ref_object.get("member_sa_ids", [])
        anchor_sa_id = ref_object.get("anchor_sa_id", "")
        snapshot["anchor_sa_id"] = anchor_sa_id
        snapshot["member_count"] = len(member_sa_ids)
        content = ref_object.get("content", {})
        anchor_obj = object_lookup.get(anchor_sa_id) if object_lookup else None
        anchor_display = _extract_ref_object_display(anchor_obj) if anchor_obj else ""

        member_summaries = []
        attribute_displays = []
        feature_displays = []

        for member_id in member_sa_ids:
            member_obj = object_lookup.get(member_id) if object_lookup else None
            role = member_obj.get("stimulus", {}).get("role", "") if member_obj else ""
            display = _extract_ref_object_display(member_obj) if member_obj else member_id
            member_summaries.append({
                "id": member_id,
                "role": role or "unknown",
                "display": display,
            })
            if role == "attribute":
                attribute_displays.append(display)
            else:
                feature_displays.append(display)

        base_display = content.get("display")
        if not base_display:
            base_anchor = anchor_display or (feature_displays[0] if feature_displays else "")
            base_display = f"CSA[{base_anchor or snapshot['member_count']}]"

        detail_parts = []
        if anchor_display:
            detail_parts.append(f"anchor={anchor_display}")
        if attribute_displays:
            detail_parts.append(f"attrs={', '.join(attribute_displays[:4])}")
        if snapshot["member_count"]:
            detail_parts.append(f"members={snapshot['member_count']}")

        snapshot["content_display"] = base_display
        snapshot["content_display_detail"] = " | ".join(detail_parts) if detail_parts else base_display
        snapshot["anchor_display"] = anchor_display or anchor_sa_id
        snapshot["attribute_displays"] = attribute_displays
        snapshot["feature_displays"] = feature_displays
        snapshot["member_summaries"] = member_summaries[:8]
    elif obj_type == "st":
        content = ref_object.get("content", {})
        structure = ref_object.get("structure", {})
        structured_display = format_sequence_groups(structure.get("sequence_groups", []))
        display = (
            structured_display
            or content.get("display")
            or content.get("normalized")
            or content.get("raw")
            or structure.get("display_text")
            or ref_object.get("id", "")
        )
        snapshot["content_display"] = display
        snapshot["content_display_detail"] = (
            structured_display
            or structure.get("display_text")
            or content.get("normalized")
            or display
        )
        snapshot["token_count"] = int(structure.get("token_count", len(structure.get("flat_tokens", []))))
        snapshot["content_signature"] = structure.get("content_signature", "")
        snapshot["flat_tokens"] = list(structure.get("flat_tokens", []))
        snapshot["sequence_groups"] = list(structure.get("sequence_groups", []))
        snapshot["member_refs"] = list(structure.get("member_refs", []))
        snapshot["structure_ext"] = copy.deepcopy(structure.get("ext", {})) if isinstance(structure.get("ext", {}), dict) else {}
    elif obj_type == "sg":
        content = ref_object.get("content", {})
        group_structure = ref_object.get("group_structure", {})
        if not isinstance(group_structure, dict):
            group_structure = {}
        structured_display = format_sequence_groups(group_structure.get("sequence_groups", []))
        display = (
            structured_display
            or content.get("display")
            or content.get("normalized")
            or content.get("raw")
            or group_structure.get("display_text")
            or ref_object.get("id", "")
        )
        snapshot["content_display"] = display
        snapshot["content_display_detail"] = (
            structured_display
            or group_structure.get("display_text")
            or content.get("normalized")
            or display
        )
        snapshot["token_count"] = int(group_structure.get("token_count", len(group_structure.get("flat_tokens", []))))
        snapshot["content_signature"] = group_structure.get("content_signature", "")
        snapshot["flat_tokens"] = list(group_structure.get("flat_tokens", []))
        snapshot["sequence_groups"] = list(group_structure.get("sequence_groups", []))
        snapshot["member_refs"] = list(group_structure.get("member_refs", []))
        group_obj = ref_object.get("group", {}) if isinstance(ref_object.get("group", {}), dict) else {}
        snapshot["required_structure_ids"] = list(group_obj.get("required_structure_ids", ref_object.get("required_structure_ids", [])) or [])
        snapshot["bias_structure_ids"] = list(group_obj.get("bias_structure_ids", ref_object.get("bias_structure_ids", [])) or [])
        snapshot["group_ext"] = copy.deepcopy(group_structure.get("ext", {})) if isinstance(group_structure.get("ext", {}), dict) else {}
    elif obj_type == "em":
        content = ref_object.get("content", {})
        memory = ref_object.get("memory", {})
        structured_display = format_sequence_groups(memory.get("sequence_groups", []))
        display = (
            memory.get("semantic_grouped_display_text")
            or memory.get("grouped_display_text")
            or structured_display
            or content.get("display")
            or memory.get("display_text")
            or memory.get("event_summary")
            or ref_object.get("id", "")
        )
        snapshot["content_display"] = display
        snapshot["content_display_detail"] = (
            memory.get("semantic_grouped_display_text")
            or memory.get("grouped_display_text")
            or structured_display
            or memory.get("event_summary", display)
        )
        snapshot["memory_id"] = memory.get("memory_id", ref_object.get("id", ""))
        snapshot["structure_refs"] = list(memory.get("structure_refs", []))
        snapshot["group_refs"] = list(memory.get("group_refs", []))
        snapshot["sequence_groups"] = list(memory.get("sequence_groups", []))
        snapshot["backing_structure_id"] = memory.get("backing_structure_id", "")
    elif obj_type == "cfs_signal":
        content = ref_object.get("content", {})
        snapshot["content_display"] = content.get("display", content.get("raw", ""))
        snapshot["signal_type"] = ref_object.get("sub_type", "cfs")
    elif obj_type == "action_node":
        content = ref_object.get("content", {})
        snapshot["content_display"] = content.get("display", content.get("raw", ""))
        snapshot["action_type"] = ref_object.get("sub_type", "action")
    else:
        snapshot["content_display"] = str(ref_object.get("id", ""))

    return snapshot


def _build_runtime_meta_ext(ref_object: dict) -> dict:
    """Preserve lightweight runtime metadata needed by later modules."""
    result: dict[str, Any] = {}

    meta = ref_object.get("meta", {})
    if isinstance(meta, dict):
        meta_ext = meta.get("ext", {})
        if isinstance(meta_ext, dict):
            result.update(copy.deepcopy(meta_ext))

    structure = ref_object.get("structure", {})
    if isinstance(structure, dict):
        structure_ext = structure.get("ext", {})
        if isinstance(structure_ext, dict):
            if "cognitive_stitching" in structure_ext and isinstance(structure_ext.get("cognitive_stitching"), dict):
                result.setdefault("cognitive_stitching", copy.deepcopy(structure_ext.get("cognitive_stitching", {})))

    return result


def _extract_ref_object_display(ref_object: dict | None) -> str:
    """从 SA / CSA / 运行态对象中提取最稳定的展示文本。"""
    if not ref_object:
        return ""

    if ref_object.get("object_type") == "state_item":
        ref_snapshot = ref_object.get("ref_snapshot", {})
        return ref_snapshot.get("content_display", ref_object.get("ref_object_id", ""))

    content = ref_object.get("content", {})
    if isinstance(content, dict):
        display = content.get("display") or content.get("raw") or content.get("normalized")
        if display:
            return str(display)

    return str(ref_object.get("id", ""))


def _build_semantic_signature(
    ref_object: dict,
    object_lookup: dict[str, dict] | None = None,
) -> str:
    """
    为运行态对象构建稳定的“语义签名（semantic_signature）”。

    作用 / Why:
      - 用于“同一对象”跨轮次进入状态池时的稳定合并（去重）。
      - 这是状态池“对象唯一性（Object Uniqueness）”的核心 Key。

    重要设计取舍（对齐理论核心的工程化落地）:
      - 在理论上：SA（基础刺激元）可以视作最小粒度的 ST（结构）。
        因此“特征信息完全相同”的 SA 与 ST，应被视作同一对象。
      - 在工程上：我们不强制把 SA 改写为 ST（避免牵连太多模块），
        但会让 **特征 SA 与 ST 使用同一套签名规则**，从而在状态池层面合并为一个运行态对象。
      - 属性 SA（role=attribute）仍保持“绑定约束信息”的语义，不与特征对象混为一谈。

    注 / Note:
      - 这里的签名以“内容签名”为主，不包含来源(sub_type)与包来源(memfb/internal/current)差异，
        避免同一个概念因为来源不同而在状态池中重复出现（用户验收中常见问题）。
    """
    obj_type = ref_object.get("object_type", "")

    if obj_type == "sa":
        return _build_sa_semantic_signature(ref_object, object_lookup=object_lookup)
    if obj_type == "csa":
        return _build_csa_semantic_signature(ref_object, object_lookup=object_lookup)
    if obj_type == "st":
        # ST（结构）的身份签名需要与 SA（特征刺激元）对齐，否则会出现：
        # 同样是“你”，在状态池里同时存在 sa_* 与 st_* 两个对象（用户验收阻塞点）。
        #
        # 注意：HDB 的 content_signature 是 “U[F:你]#B[...]” 这种结构化签名，
        # 它非常适合做检索/去重，但不适合拿来当“状态池同一对象”的 key。
        # 因此这里改用“结构的特征 token 序列”做身份：
        #   - 若结构只包含 1 个 feature token，则直接用该 token（可与 SA 统一）。
        #   - 若包含多个 feature token，则按序拼接（作为更大粒度 ST 的身份）。
        structure = ref_object.get("structure", {}) or {}
        identity_text = _extract_structure_feature_identity(structure)
        if not identity_text:
            # Fallback: keep previous behavior (best-effort).
            # 兜底：沿用旧逻辑，避免某些残缺结构对象丢失身份。
            content_signature = str(
                structure.get("content_signature")
                or structure.get("semantic_signature")
                or ""
            ).strip()
            if not content_signature:
                content = ref_object.get("content", {}) or {}
                content_signature = str(
                    content.get("normalized") or content.get("display") or content.get("raw") or ref_object.get("id", "")
                ).strip()
            identity_text = content_signature

        return _build_feature_identity_signature(identity_text)
    if obj_type == "em":
        memory = ref_object.get("memory", {})
        memory_id = str(memory.get("memory_id", "") or ref_object.get("id", ""))
        return f"em|{memory_id}"
    if obj_type == "cfs_signal":
        return f"cfs|{_extract_object_content_token(ref_object)}"
    if obj_type == "action_node":
        return f"action|{_extract_object_content_token(ref_object)}"
    return f"{obj_type}|{ref_object.get('id', '')}"


def _build_semantic_labels(
    ref_object: dict,
    object_lookup: dict[str, dict] | None = None,
) -> dict:
    """返回便于日志与调试展示的语义标签。"""
    obj_type = ref_object.get("object_type", "")
    labels = {"object_type": obj_type}

    if obj_type == "sa":
        stimulus = ref_object.get("stimulus", {})
        content = ref_object.get("content", {})
        labels["role"] = stimulus.get("role", "")
        labels["content"] = _extract_object_content_token(ref_object)
        parent_obj = _extract_parent_feature_object(ref_object, object_lookup=object_lookup)
        if parent_obj:
            labels["parent"] = _extract_object_content_token(parent_obj)
    elif obj_type == "csa":
        anchor_obj = object_lookup.get(ref_object.get("anchor_sa_id", "")) if object_lookup else None
        labels["anchor"] = _extract_object_content_token(anchor_obj) if anchor_obj else ""
        labels["attributes"] = _collect_csa_attribute_tokens(ref_object, object_lookup=object_lookup)
    elif obj_type == "em":
        memory = ref_object.get("memory", {})
        labels["memory_id"] = str(memory.get("memory_id", "") or ref_object.get("id", ""))
        labels["content"] = _extract_object_content_token(ref_object)
        labels["backing_structure_id"] = str(memory.get("backing_structure_id", ""))
    else:
        labels["content"] = _extract_object_content_token(ref_object)

    return labels


def _build_sa_semantic_signature(
    ref_object: dict,
    object_lookup: dict[str, dict] | None = None,
) -> str:
    stimulus = ref_object.get("stimulus", {})
    content = ref_object.get("content", {})
    role = stimulus.get("role", "")
    modality = stimulus.get("modality", "")
    sub_type = ref_object.get("sub_type", "")
    value_type = content.get("value_type", "")
    content_token = _extract_object_content_token(ref_object)

    if role == "attribute":
        attribute_name = content.get("attribute_name")
        if not attribute_name:
            raw = str(content.get("normalized") or content.get("display") or content.get("raw") or "")
            attribute_name = raw.split(":", 1)[0] if ":" in raw else raw
        if value_type == "numerical":
            value_token = "numerical"
        else:
            value_token = content_token

        parent_obj = _extract_parent_feature_object(ref_object, object_lookup=object_lookup)
        parent_token = _build_feature_semantic_token(parent_obj) if parent_obj else ""
        return "|".join(
            [
                "sa",
                role or "unknown",
                modality or "unknown",
                sub_type or "unknown",
                _normalize_text_fragment(attribute_name),
                value_token,
                parent_token,
            ]
        )

    # 特征 SA（非 attribute）在理论上可以视作“最小结构 ST”。
    # 因此它的身份签名应与 ST 的 content_signature 对齐，从而在状态池层面合并。
    #
    # 注意：这里故意 **不包含 sub_type**（例如 txt/memfb/internal），
    # 否则会导致同一个“你好”因为来源不同而在状态池里出现多个对象（用户验收阻塞点）。
    del modality, sub_type, value_type  # keep signature stable, ignore source differences
    return _build_feature_identity_signature(content_token)


def _build_feature_identity_signature(content_signature: Any) -> str:
    """
    统一“特征对象”的身份签名。

    设计目标 / Goal:
      - SA(特征) 与 ST(结构) 在状态池中视作同一对象时，能稳定命中同一 key；
      - 内容相同 -> 同一对象；内容不同 -> 不同对象。
    """
    token = _normalize_text_fragment(content_signature)
    return f"obj|{token}" if token else "obj|"


def _build_csa_semantic_signature(
    ref_object: dict,
    object_lookup: dict[str, dict] | None = None,
) -> str:
    sub_type = ref_object.get("sub_type", "")
    anchor_obj = object_lookup.get(ref_object.get("anchor_sa_id", "")) if object_lookup else None
    anchor_token = _build_feature_semantic_token(anchor_obj) if anchor_obj else ""
    attribute_tokens = _collect_csa_attribute_tokens(ref_object, object_lookup=object_lookup)
    feature_tokens = _collect_csa_feature_tokens(ref_object, object_lookup=object_lookup)

    return "|".join(
        [
            "csa",
            sub_type or "unknown",
            f"anchor={anchor_token}",
            f"attrs={','.join(sorted(set(attribute_tokens)))}",
            f"features={','.join(sorted(set(feature_tokens)))}",
        ]
    )


def _extract_parent_feature_object(
    ref_object: dict,
    object_lookup: dict[str, dict] | None = None,
) -> dict | None:
    if not object_lookup:
        return None
    parent_ids = ref_object.get("source", {}).get("parent_ids", [])
    if not parent_ids:
        return None
    return object_lookup.get(parent_ids[0])


def _collect_csa_attribute_tokens(
    ref_object: dict,
    object_lookup: dict[str, dict] | None = None,
) -> list[str]:
    tokens: list[str] = []
    for member_id in ref_object.get("member_sa_ids", []):
        member_obj = object_lookup.get(member_id) if object_lookup else None
        if member_obj and member_obj.get("stimulus", {}).get("role") == "attribute":
            tokens.append(_build_sa_semantic_signature(member_obj, object_lookup=object_lookup))
    return tokens


def _collect_csa_feature_tokens(
    ref_object: dict,
    object_lookup: dict[str, dict] | None = None,
) -> list[str]:
    tokens: list[str] = []
    for member_id in ref_object.get("member_sa_ids", []):
        member_obj = object_lookup.get(member_id) if object_lookup else None
        if member_obj and member_obj.get("stimulus", {}).get("role") != "attribute":
            tokens.append(_build_feature_semantic_token(member_obj))
    return tokens


def _build_feature_semantic_token(ref_object: dict | None) -> str:
    if not ref_object:
        return ""
    stimulus = ref_object.get("stimulus", {})
    modality = stimulus.get("modality", "")
    sub_type = ref_object.get("sub_type", "")
    content_token = _extract_object_content_token(ref_object)
    return "|".join(
        [
            "feature",
            modality or "unknown",
            sub_type or "unknown",
            content_token,
        ]
    )


def _extract_object_content_token(ref_object: dict | None) -> str:
    if not ref_object:
        return ""
    content = ref_object.get("content", {})
    if isinstance(content, dict):
        return _normalize_text_fragment(
            content.get("normalized") or content.get("display") or content.get("raw") or ref_object.get("id", "")
        )
    return _normalize_text_fragment(ref_object.get("id", ""))


def _normalize_text_fragment(value: Any) -> str:
    text = str(value if value is not None else "").strip()
    return text.replace("|", "/").replace("\n", " ")


def _extract_structure_feature_identity(structure: dict) -> str:
    """
    Extract a stable identity text for ST objects, aligned with SA tokens.
    提取结构（ST）的“特征身份文本”，用于与 SA 对齐的语义签名。

    Rule / 规则：
      - Prefer sequence_groups.units where unit_role != "attribute".
        优先使用 sequence_groups.units 中 unit_role != attribute 的 token。
      - Fallback to flat_tokens (best-effort).
        若缺少 units，则退化使用 flat_tokens。

    Note / 注意：
      - 这里“刻意忽略 attribute token”，因为属性应作为“约束信息”绑定在锚点对象上，
        不应让锚点对象因为属性出现而变成另一个“全新对象”。
    """
    if not isinstance(structure, dict):
        return ""

    tokens: list[str] = []
    groups = structure.get("sequence_groups", [])
    if isinstance(groups, list) and groups:
        for group in groups:
            if not isinstance(group, dict):
                continue
            units = group.get("units", [])
            if isinstance(units, list) and units:
                for unit in units:
                    if not isinstance(unit, dict):
                        continue
                    role = str(unit.get("unit_role", "") or "")
                    if role == "attribute":
                        continue
                    token = str(unit.get("token", "") or "")
                    if token:
                        tokens.append(token)
                continue

            # Fallback for legacy groups without units: treat tokens as features.
            # 旧格式兜底：没有 units 时，tokens 视作特征序列。
            for raw in group.get("tokens", []) or []:
                t = str(raw or "")
                if t:
                    tokens.append(t)

    if not tokens:
        for raw in structure.get("flat_tokens", []) or []:
            t = str(raw or "")
            if t:
                tokens.append(t)

    normalized = [_normalize_text_fragment(t) for t in tokens]
    normalized = [t for t in normalized if t]
    if not normalized:
        return ""
    if len(normalized) == 1:
        return normalized[0]
    # Keep order and duplicates; join with a space for readability & stability.
    # 保留顺序与重复项；用空格拼接，兼顾可读性与稳定性。
    return " ".join(normalized)


def _safe_float(val: Any) -> float:
    """安全转换为浮点数，失败返回 0.0。"""
    try:
        return float(val)
    except (TypeError, ValueError):
        return 0.0
