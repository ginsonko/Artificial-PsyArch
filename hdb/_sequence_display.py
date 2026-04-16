# -*- coding: utf-8 -*-
"""
Unified sequence-group display helpers.

Display convention:
  - Temporal group: {...}
  - CSA bundle: (...)
  - Same-group separator: +
  - Cross-group separator: /

These helpers are intentionally display-only. They must not change matching,
energy ownership, or structural signatures.
"""

from __future__ import annotations


def _unit_token(unit: dict) -> str:
    return str(unit.get("token", "") or unit.get("display_text", "") or "").strip()


def format_group_display(units: list[dict], bundles: list[dict] | None = None) -> str:
    ordered_units = sorted(
        [dict(unit) for unit in units or [] if isinstance(unit, dict)],
        key=lambda item: (
            int(item.get("sequence_index", 0)),
            str(item.get("unit_id", "")),
            str(item.get("unit_signature", "")),
        ),
    )
    if not ordered_units:
        return ""

    units_by_id = {
        str(unit.get("unit_id", "")): unit
        for unit in ordered_units
        if str(unit.get("unit_id", ""))
    }
    ordered_bundles = sorted(
        [dict(bundle) for bundle in bundles or [] if isinstance(bundle, dict)],
        key=lambda item: (
            int(units_by_id.get(str(item.get("anchor_unit_id", "")), {}).get("sequence_index", 0)),
            str(item.get("bundle_id", "")),
        ),
    )
    bundle_by_id = {
        str(bundle.get("bundle_id", "")): bundle
        for bundle in ordered_bundles
        if str(bundle.get("bundle_id", ""))
    }
    emitted_bundle_ids: set[str] = set()
    covered_unit_ids: set[str] = set()
    segments: list[str] = []

    for unit in ordered_units:
        unit_id = str(unit.get("unit_id", ""))
        if unit_id in covered_unit_ids:
            continue

        bundle_id = str(unit.get("bundle_id", ""))
        bundle = bundle_by_id.get(bundle_id) if bundle_id else None
        if bundle and bundle_id not in emitted_bundle_ids:
            anchor_id = str(bundle.get("anchor_unit_id", ""))
            if unit_id == anchor_id:
                member_tokens = [
                    _unit_token(units_by_id.get(str(member_id), {}))
                    for member_id in bundle.get("member_unit_ids", [])
                ]
                member_tokens = [token for token in member_tokens if token]
                if member_tokens:
                    segments.append(f"({' + '.join(member_tokens)})")
                    emitted_bundle_ids.add(bundle_id)
                    covered_unit_ids.update(
                        str(member_id)
                        for member_id in bundle.get("member_unit_ids", [])
                        if str(member_id)
                    )
                    continue

        token = _unit_token(unit)
        if not token:
            continue
        covered_unit_ids.add(unit_id)
        segments.append(token)

    return f"{{{' + '.join(segments)}}}" if segments else ""


def format_sequence_groups(groups: list[dict]) -> str:
    parts: list[str] = []
    for group in groups or []:
        if not isinstance(group, dict):
            continue
        units = list(group.get("units", []))
        bundles = list(group.get("csa_bundles", []))
        if units:
            text = format_group_display(units, bundles)
        else:
            raw_tokens = [str(token).strip() for token in group.get("tokens", []) if str(token).strip()]
            text = f"{{{' + '.join(raw_tokens)}}}" if raw_tokens else ""
        if text:
            parts.append(text)
    return " / ".join(parts)


def _ordered_units(units: list[dict] | None) -> list[dict]:
    return sorted(
        [dict(unit) for unit in units or [] if isinstance(unit, dict)],
        key=lambda item: (
            int(item.get("sequence_index", 0)),
            str(item.get("unit_id", "")),
            str(item.get("unit_signature", "")),
        ),
    )


def _ordered_bundles(bundles: list[dict] | None, units_by_id: dict[str, dict]) -> list[dict]:
    return sorted(
        [dict(bundle) for bundle in bundles or [] if isinstance(bundle, dict)],
        key=lambda item: (
            int(units_by_id.get(str(item.get("anchor_unit_id", "")), {}).get("sequence_index", 0)),
            str(item.get("bundle_id", "")),
            str(item.get("bundle_signature", "")),
        ),
    )


def _is_structure_unit(unit: dict) -> bool:
    object_type = str(unit.get("object_type", ""))
    signature = str(unit.get("unit_signature", ""))
    return object_type == "st" or signature.startswith("ST:")


def _is_placeholder_unit(unit: dict) -> bool:
    return bool(unit.get("is_placeholder")) or str(unit.get("object_type", "")) == "st_placeholder"


def _structure_fallback_text(unit: dict) -> str:
    return (
        str(unit.get("structure_grouped_display_text", "") or "")
        or str(unit.get("structure_display_text", "") or "")
        or str(unit.get("display_text", "") or "")
        or str(unit.get("token", "") or "")
        or str(unit.get("unit_id", "") or "")
    )


def _render_bundle_signature(bundle: dict, units_by_id: dict[str, dict]) -> str:
    member_tokens = [
        _unit_token(units_by_id.get(str(member_id), {}))
        for member_id in bundle.get("member_unit_ids", [])
    ]
    member_tokens = [token for token in member_tokens if token]
    if member_tokens:
        return f"({' + '.join(member_tokens)})"
    signature = str(bundle.get("bundle_signature", "") or "")
    return f"({signature})" if signature else ""


def _render_stimulus_group_semantic(group: dict) -> str:
    units = _ordered_units(group.get("units", []))
    unit_tokens = [_unit_token(unit) for unit in units]
    unit_tokens = [token for token in unit_tokens if token]
    units_by_id = {
        str(unit.get("unit_id", "")): unit
        for unit in units
        if str(unit.get("unit_id", ""))
    }
    bundle_tokens = [
        _render_bundle_signature(bundle, units_by_id)
        for bundle in _ordered_bundles(group.get("csa_bundles", []), units_by_id)
    ]
    bundle_tokens = [token for token in bundle_tokens if token]
    if not unit_tokens and not bundle_tokens:
        raw_tokens = [str(token).strip() for token in group.get("tokens", []) if str(token).strip()]
        unit_tokens = raw_tokens
    parts = [*unit_tokens, *bundle_tokens]
    return f"{{{' + '.join(parts)}}}" if parts else ""


def _render_structure_unit_semantic(unit: dict) -> str:
    if _is_placeholder_unit(unit):
        return _unit_token(unit)
    nested_groups = [dict(group) for group in unit.get("structure_sequence_groups", []) if isinstance(group, dict)]
    if nested_groups:
        inner = format_semantic_sequence_groups(nested_groups, context="stimulus")
    else:
        inner = _structure_fallback_text(unit)
    return f"[{inner}]" if inner else "[]"


def _render_structure_group_semantic(group: dict) -> str:
    units = _ordered_units(group.get("units", []))
    members = []
    for unit in units:
        text = _render_structure_unit_semantic(unit)
        if text:
            members.append(text)
    if not members:
        raw_tokens = [str(token).strip() for token in group.get("tokens", []) if str(token).strip()]
        members = raw_tokens
    return f"{{{' / '.join(members)}}}" if members else ""


def detect_semantic_context(groups: list[dict] | None) -> str:
    for group in groups or []:
        if not isinstance(group, dict):
            continue
        for unit in group.get("units", []) or []:
            if not isinstance(unit, dict):
                continue
            if _is_structure_unit(unit) or _is_placeholder_unit(unit):
                return "structure"
    return "stimulus"


def format_semantic_group_display(group: dict, *, context: str = "auto") -> str:
    if not isinstance(group, dict):
        return str(group)
    resolved_context = context
    if resolved_context == "auto":
        resolved_context = detect_semantic_context([group])
    if resolved_context == "structure":
        return _render_structure_group_semantic(group)
    return _render_stimulus_group_semantic(group)


def format_semantic_sequence_groups(groups: list[dict], *, context: str = "auto") -> str:
    resolved_context = context
    if resolved_context == "auto":
        resolved_context = detect_semantic_context(groups)
    parts: list[str] = []
    for group in groups or []:
        if not isinstance(group, dict):
            continue
        text = format_semantic_group_display(group, context=resolved_context)
        if text:
            parts.append(text)
    return " || ".join(parts)


def semantic_notation_legend() -> list[dict[str, str]]:
    return [
        {"symbol": "()", "meaning": "CSA 组关系，只表示同一时序组里的共现绑定。"},
        {"symbol": "+", "meaning": "同一内容片段里的基础单元连接。"},
        {"symbol": "[]", "meaning": "一个未打散的结构 ST。"},
        {"symbol": "{}", "meaning": "一个时序组。"},
        {"symbol": "/", "meaning": "同一时序组里多个结构并列、无先后。"},
        {"symbol": "||", "meaning": "不同时序组之间存在先后顺序。"},
    ]


def semantic_notation_examples() -> list[dict[str, str]]:
    return [
        {
            "title": "刺激流",
            "example": "{你好 + (时间感受:约1秒)} || {呀} || {!}",
            "explanation": "这是打散后的 SA/CSA 工作流，所以没有 []；前后两个时序组用 || 分开。",
        },
        {
            "title": "结构 ST",
            "example": "[{你好 + (时间感受:约1秒)} || {呀} || {!}]",
            "explanation": "这是一个未打散结构，[] 包住整个 ST，内部仍保留自己的时序组。",
        },
        {
            "title": "结构组",
            "example": "{[{谁}] / [{是}]} || {[{银子}]}",
            "explanation": "第一个时序点并列出现两个结构，第二个时序点随后出现另一个结构。",
        },
    ]
