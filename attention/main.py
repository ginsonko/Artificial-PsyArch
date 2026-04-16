# -*- coding: utf-8 -*-
"""
AP 注意力模块（Attention Filter, AF）— 主模块
============================================
本模块负责在预算约束下，对状态池（SP）中的运行态对象做筛选/调制，
输出 CAM（当前注意记忆体）。

原型阶段 MVP：
  - 从状态池快照中选 Top-N 候选
  - （可选）按比例从 SP 扣能，形成 CAM 预算能量（记账式转移，不复制能量）
  - 输出 cam_snapshot（runtime_snapshot），可直接作为结构级查存一体输入

职责边界（与理论一致）：
  ✓ 选择哪些对象进入 CAM
  ✓ 为进入 CAM 的对象分配/划拨可被消耗的注意预算（预算能量）
  ✓ 输出可审计的选择原因与预算记账
  ✗ 不直接写 HDB（结构级/刺激级查存一体属于 HDB）
  ✗ 不直接生成内源刺激（内源刺激在结构级阶段按 ρ_k 划拨残差产生）
  ✗ 不直接决策最终行动（行动在 Drive 竞争后触发）
"""

from __future__ import annotations

import os
import math
import time
import traceback
from typing import Any

from . import __module_name__, __schema_version__, __version__
from ._logger import ModuleLogger


def _load_yaml_config(path: str) -> dict:
    """加载 YAML 配置文件。加载失败返回空 dict。"""
    try:
        import yaml
        with open(path, "r", encoding="utf-8") as fh:
            data = yaml.safe_load(fh)
        return data if isinstance(data, dict) else {}
    except ImportError:
        return {}
    except Exception:
        return {}


_DEFAULT_CONFIG: dict[str, Any] = {
    # ---- 注意力资源预算（以“对象数量上限”为主）----
    #
    # 重要澄清（非常关键，避免误读）：
    # - 这里的 top_n / max_cam_items 不是“永远取前 N 个”的硬编码逻辑；
    # - 它仅表示 CAM（当前注意记忆体）允许保留的对象数量上限（cap），实际入选数量由“抑制阈值 + 能量分布”
    #   动态决定，常常会 < cap，从而更像“只保留波峰”的拟人注意过程。
    #
    # Compatibility / 兼容性：
    # - 旧配置使用 top_n；新配置优先读取 max_cam_items。
    "top_n": 16,           # 兼容字段：CAM 上限（旧名）
    "max_cam_items": 16,   # 新名：CAM 上限（cap）
    "min_cam_items": 2,    # 最少保留多少个（防止 CAM 为空导致后续查存一体失去输入）

    # ---- 动态抑制阈值（Dynamic cutoff）----
    # 思路：以“波峰优先级分数 * 比例”为阈值，低于阈值的对象会被注意力抑制，不进入 CAM。
    # 该比例会随“能量/优先级分布集中度”动态变化：越集中 -> 阈值越高 -> CAM 越短；越分散 -> 阈值越低 -> CAM 越长。
    "keep_score_ratio_base": 0.28,
    "keep_score_ratio_concentration_gain": 0.22,
    "keep_score_ratio_min": 0.18,
    "keep_score_ratio_max": 0.72,
    "score_entropy_eps": 1e-9,

    # ---- 记账式能量划拨（抽取）----
    "consume_energy": True,
    "memory_energy_ratio": 0.5,
    "exclude_ref_object_types": ["em"],

    # ---- 候选过滤 ----
    "min_total_energy": 0.0,

    # ---- 排序权重（MVP 默认与 observatory stub 保持一致）----
    "priority_weight_total_energy": 1.25,
    "priority_weight_cp_abs": 0.35,
    "priority_weight_salience": 0.15,
    "priority_weight_updated_at": 1e-12,
    # 预留：疲劳/近因进入注意力的调制位（默认 0，不改变当前语义）
    "priority_weight_fatigue": 0.0,
    "priority_weight_recency_gain": 0.0,

    # ---- 聚焦指令 / Focus Directives ----
    # focus_boost_weight: directives 提供的 focus_boost 会再乘该权重后计入 priority 分数
    "focus_boost_weight": 1.0,

    # ---- 日志 ----
    "log_dir": "",
    "log_max_file_bytes": 5 * 1024 * 1024,
    "stdout_fallback_when_log_fail": True,
}


class AttentionFilter:
    """
    AP 注意力模块主类。

    使用示例:
        from attention import AttentionFilter
        af = AttentionFilter()
        resp = af.build_cam_from_pool(pool, trace_id="tick_001", tick_id="tick_001")
        cam_snapshot = resp["data"]["cam_snapshot"]
    """

    def __init__(self, config_path: str = "", config_override: dict | None = None):
        self._config_path = config_path or os.path.join(os.path.dirname(__file__), "config", "attention_config.yaml")
        self._config = self._build_config(config_override)
        self._logger = ModuleLogger(
            log_dir=self._config.get("log_dir", ""),
            max_file_bytes=int(self._config.get("log_max_file_bytes", 5 * 1024 * 1024)),
            enable_stdout_fallback=bool(self._config.get("stdout_fallback_when_log_fail", True)),
        )
        self._total_calls = 0

    # ================================================================== #
    # 接口一：build_cam_from_pool                                          #
    # ================================================================== #

    def build_cam_from_pool(
        self,
        pool: Any,
        *,
        trace_id: str,
        tick_id: str | None = None,
        top_n: int | None = None,
        consume_energy: bool | None = None,
        memory_energy_ratio: float | None = None,
        focus_directives: list[dict] | None = None,
        modulation: dict | None = None,
        metadata: dict | None = None,
    ) -> dict:
        """
        从状态池中构建 CAM（当前注意记忆体）。

        参数:
            pool: StatePool 实例（需要提供 get_state_snapshot/apply_energy_update）
            trace_id/tick_id: 追踪标识
            top_n: CAM（当前注意记忆体）对象数量上限（cap，上限不保证填满；真实入选由抑制阈值动态决定）
            consume_energy: 是否真实从 SP 扣能（默认取配置）
            memory_energy_ratio: 抽取比例（默认取配置，范围 [0,1]）
            focus_directives: IESM 输出的注意力聚焦指令（本 tick 生效的输入）
            modulation: EMgr 输出的调制包（本 tick 生效的输入；仅覆盖少量权重/阈值，不改变语义）
            metadata: 预留扩展字段
        """
        start_time = time.time()
        tick_id = tick_id or trace_id
        self._total_calls += 1

        if not trace_id:
            return self._make_response(
                success=False,
                code="VALIDATION_ERROR",
                message="trace_id 不能为空 / trace_id is required",
                error={"code": "trace_id_required"},
                trace_id=trace_id,
                elapsed_ms=self._elapsed_ms(start_time),
            )

        if pool is None or not hasattr(pool, "get_state_snapshot"):
            return self._make_response(
                success=False,
                code="VALIDATION_ERROR",
                message="pool 必须提供 get_state_snapshot 接口 / pool must implement get_state_snapshot",
                error={"code": "pool_invalid"},
                trace_id=trace_id,
                elapsed_ms=self._elapsed_ms(start_time),
            )

        # ---- 读取参数（注意力预算口径）----
        # top_n 在这里被解释为“CAM 上限（cap）”，而不是“永远填满 Top-N”。
        # 真实入选数量会受动态阈值抑制影响，从而更贴近理论中的“只维持波峰”。
        modulation = modulation or {}

        base_cap = int(self._config.get("max_cam_items", self._config.get("top_n", 16)) or 16)
        mod_cap = modulation.get("max_cam_items")
        if mod_cap is None:
            # 兼容：情绪调制沿用 top_n 字段
            mod_cap = modulation.get("top_n")
        resolved_top_n = int(top_n) if top_n is not None else (int(mod_cap) if mod_cap is not None else base_cap)
        resolved_top_n = max(1, int(resolved_top_n))

        base_min_keep = int(self._config.get("min_cam_items", 2) or 2)
        mod_min_keep = modulation.get("min_cam_items")
        resolved_min_keep = int(mod_min_keep) if mod_min_keep is not None else base_min_keep
        resolved_min_keep = max(1, min(resolved_top_n, int(resolved_min_keep)))
        resolved_consume = bool(consume_energy) if consume_energy is not None else bool(self._config.get("consume_energy", True))
        ratio_raw = float(memory_energy_ratio) if memory_energy_ratio is not None else float(self._config.get("memory_energy_ratio", 0.5))
        resolved_ratio = max(0.0, min(1.0, ratio_raw))

        # ---- 调制覆盖（仅影响排序权重/阈值，不改变“Top-N + 抽取扣能”语义）----
        effective_weights = {
            "priority_weight_total_energy": float(modulation.get("priority_weight_total_energy", self._config.get("priority_weight_total_energy", 1.25))),
            "priority_weight_cp_abs": float(modulation.get("priority_weight_cp_abs", self._config.get("priority_weight_cp_abs", 0.35))),
            "priority_weight_salience": float(modulation.get("priority_weight_salience", self._config.get("priority_weight_salience", 0.15))),
            "priority_weight_updated_at": float(modulation.get("priority_weight_updated_at", self._config.get("priority_weight_updated_at", 1e-12))),
            "priority_weight_fatigue": float(modulation.get("priority_weight_fatigue", self._config.get("priority_weight_fatigue", 0.0))),
            "priority_weight_recency_gain": float(modulation.get("priority_weight_recency_gain", self._config.get("priority_weight_recency_gain", 0.0))),
            "min_total_energy": float(modulation.get("min_total_energy", self._config.get("min_total_energy", 0.0))),
            "focus_boost_weight": float(modulation.get("focus_boost_weight", self._config.get("focus_boost_weight", 1.0))),
        }
        effective_cutoff = {
            "keep_score_ratio_base": float(modulation.get("keep_score_ratio_base", self._config.get("keep_score_ratio_base", 0.28))),
            "keep_score_ratio_concentration_gain": float(modulation.get("keep_score_ratio_concentration_gain", self._config.get("keep_score_ratio_concentration_gain", 0.22))),
            "keep_score_ratio_min": float(modulation.get("keep_score_ratio_min", self._config.get("keep_score_ratio_min", 0.18))),
            "keep_score_ratio_max": float(modulation.get("keep_score_ratio_max", self._config.get("keep_score_ratio_max", 0.72))),
            "score_entropy_eps": float(modulation.get("score_entropy_eps", self._config.get("score_entropy_eps", 1e-9))),
        }

        # ---- 取状态池快照 ----
        try:
            snapshot_result = pool.get_state_snapshot(
                trace_id=f"{trace_id}_attention_source",
                tick_id=tick_id,
                top_k=None,
            )
            if not snapshot_result.get("success"):
                raise RuntimeError(snapshot_result.get("message", "state_pool snapshot failed"))
            source_snapshot = snapshot_result.get("data", {}).get("snapshot", {}) or {}
        except Exception as e:
            self._logger.error(
                trace_id=trace_id,
                tick_id=tick_id,
                interface="build_cam_from_pool",
                code="STATE_SNAPSHOT_ERROR",
                message=f"获取状态池快照失败: {e}",
                detail={"traceback": traceback.format_exc()},
            )
            return self._make_response(
                success=False,
                code="STATE_SNAPSHOT_ERROR",
                message=f"获取状态池快照失败 / failed to get state snapshot: {e}",
                error={"code": "state_snapshot_error", "message": str(e)},
                trace_id=trace_id,
                elapsed_ms=self._elapsed_ms(start_time),
            )

        all_items = list(source_snapshot.get("top_items", []))
        excluded = set(str(x) for x in (self._config.get("exclude_ref_object_types") or []))
        eligible_items = [
            item for item in all_items
            if str(item.get("ref_object_type", "")) not in excluded
        ]

        # ---- Step 2: compute priority once / 计算优先级（只算一次，避免重复算导致解释不一致）----
        scored_items: list[dict] = []
        for item in eligible_items:
            try:
                score = self._priority_score(item, weights=effective_weights, focus_directives=focus_directives)
            except Exception:
                score = 0.0
            copied = dict(item)
            copied["attention_priority"] = round(float(score), 8)
            copied["focus_boost"] = round(float(self._compute_focus_boost(item, focus_directives)), 8)
            scored_items.append(copied)

        scored_items.sort(key=lambda it: float(it.get("attention_priority", 0.0) or 0.0), reverse=True)

        # ---- Step 3: selection policy / 选择策略（对齐理论：波峰 + 抑制）----
        # 1) 优先保留 focus_directives 指向的对象（带参聚焦的效果必须可见）；
        # 2) 再用“动态阈值”抑制掉低于波峰一定比例的对象，使 CAM 通常不会太长；
        # 3) 最后保证至少 min_cam_items（避免空 CAM）。
        consume_events: list[dict] = []
        consumed_total_er = 0.0
        consumed_total_ev = 0.0
        min_total_energy = max(0.0, float(effective_weights.get("min_total_energy", 0.0)))

        # Focus target set / 聚焦目标集合
        focus_ref_set: set[str] = set()
        focus_item_set: set[str] = set()
        for directive in focus_directives or []:
            if not isinstance(directive, dict):
                continue
            rid = str(directive.get("target_ref_object_id", "") or "").strip()
            rtype = str(directive.get("target_ref_object_type", "") or "").strip()
            iid = str(directive.get("target_item_id", "") or "").strip()
            if iid:
                focus_item_set.add(iid)
            if rid:
                # allow both "st:xxx" and raw "xxx" for compatibility
                # 同时兼容 "st:xxx" 与 "xxx" 两种写法（因为部分指令可能没带 type）。
                focus_ref_set.add(rid)
                if rtype:
                    focus_ref_set.add(f"{rtype}:{rid}")

        def _is_focus_target(it: dict) -> bool:
            iid = str(it.get("item_id", "") or "").strip()
            rid = str(it.get("ref_object_id", "") or "").strip()
            rtype = str(it.get("ref_object_type", "") or "").strip()
            if iid and iid in focus_item_set:
                return True
            if rid and (rid in focus_ref_set or f"{rtype}:{rid}" in focus_ref_set):
                return True
            return False

        # ---- Dynamic cutoff / 动态阈值（按优先级分布集中度自适应）----
        score_eps = max(0.0, float(effective_cutoff.get("score_entropy_eps", 1e-9) or 1e-9))
        score_weights: list[float] = []
        for it in scored_items:
            before_er = float(it.get("er", 0.0) or 0.0)
            before_ev = float(it.get("ev", 0.0) or 0.0)
            if before_er + before_ev <= min_total_energy:
                continue
            v = float(it.get("attention_priority", 0.0) or 0.0)
            if v > score_eps:
                score_weights.append(v)

        peak_score = float(score_weights[0]) if score_weights else (float(scored_items[0].get("attention_priority", 0.0) or 0.0) if scored_items else 0.0)
        peak_score = max(0.0, float(peak_score))

        entropy = 0.0
        concentration = 0.0
        if len(score_weights) >= 2:
            total_w = float(sum(score_weights))
            if total_w > score_eps:
                probs = [w / total_w for w in score_weights if w > score_eps]
                if probs:
                    raw = -sum(p * math.log(max(p, 1e-12)) for p in probs)
                    denom = max(1e-6, math.log(float(len(probs))))
                    entropy = max(0.0, min(1.0, raw / denom))
                    concentration = max(0.0, min(1.0, 1.0 - entropy))

        ratio_base = float(effective_cutoff.get("keep_score_ratio_base", 0.28) or 0.28)
        ratio_gain = float(effective_cutoff.get("keep_score_ratio_concentration_gain", 0.22) or 0.22)
        ratio_min = float(effective_cutoff.get("keep_score_ratio_min", 0.18) or 0.18)
        ratio_max = float(effective_cutoff.get("keep_score_ratio_max", 0.72) or 0.72)
        keep_ratio = ratio_base + concentration * ratio_gain
        keep_ratio = max(ratio_min, min(ratio_max, float(keep_ratio)))
        cutoff_score = float(peak_score) * float(keep_ratio)

        selected_candidates: list[dict] = []
        selected_item_ids: set[str] = set()

        def _select(it: dict, why: str) -> None:
            iid = str(it.get("item_id", "") or "").strip()
            if not iid:
                return
            if iid in selected_item_ids:
                return
            selected_item_ids.add(iid)
            selected_candidates.append({**it, "selected_by": why})

        # 1) Focus-first / 聚焦优先
        for it in scored_items:
            if len(selected_candidates) >= resolved_top_n:
                break
            if not _is_focus_target(it):
                continue
            total_energy = float(it.get("er", 0.0) or 0.0) + float(it.get("ev", 0.0) or 0.0)
            if total_energy <= min_total_energy:
                continue
            _select(it, "focus_directive")

        # 2) Cutoff selection / 阈值筛选（保留波峰）
        for it in scored_items:
            if len(selected_candidates) >= resolved_top_n:
                break
            iid = str(it.get("item_id", "") or "").strip()
            if iid and iid in selected_item_ids:
                continue
            total_energy = float(it.get("er", 0.0) or 0.0) + float(it.get("ev", 0.0) or 0.0)
            if total_energy <= min_total_energy:
                continue
            score = float(it.get("attention_priority", 0.0) or 0.0)
            if score < cutoff_score:
                continue
            _select(it, "cutoff")

        # 3) Min-keep fill / 最少保留数兜底
        for it in scored_items:
            if len(selected_candidates) >= resolved_top_n:
                break
            if len(selected_candidates) >= resolved_min_keep:
                break
            iid = str(it.get("item_id", "") or "").strip()
            if iid and iid in selected_item_ids:
                continue
            total_energy = float(it.get("er", 0.0) or 0.0) + float(it.get("ev", 0.0) or 0.0)
            if total_energy <= min_total_energy:
                continue
            _select(it, "min_keep")

        # 4) Extraction / 记账式能量划拨（抽取）
        selected_items: list[dict] = []
        for item in selected_candidates:
            before_er = round(float(item.get("er", 0.0)), 8)
            before_ev = round(float(item.get("ev", 0.0)), 8)
            total_energy = before_er + before_ev
            if total_energy <= min_total_energy:
                continue

            memory_er = round(before_er * resolved_ratio, 8) if resolved_consume else before_er
            memory_ev = round(before_ev * resolved_ratio, 8) if resolved_consume else before_ev
            pool_after_er = before_er
            pool_after_ev = before_ev

            if resolved_consume and (memory_er > 0.0 or memory_ev > 0.0) and hasattr(pool, "apply_energy_update"):
                try:
                    update_result = pool.apply_energy_update(
                        target_item_id=item.get("item_id", ""),
                        delta_er=-memory_er,
                        delta_ev=-memory_ev,
                        trace_id=f"{trace_id}_attention_extract",
                        tick_id=tick_id,
                        reason="attention_memory_extract",
                        source_module="attention",
                    )
                    update_data = update_result.get("data", {}) if update_result.get("success") else {}
                    pool_after_er = round(float(update_data.get("after", {}).get("er", max(0.0, before_er - memory_er))), 8)
                    pool_after_ev = round(float(update_data.get("after", {}).get("ev", max(0.0, before_ev - memory_ev))), 8)
                except Exception:
                    # 扣能失败不应中断 CAM 构建；记录日志并回退为“观测态不扣能”
                    self._logger.error(
                        trace_id=trace_id,
                        tick_id=tick_id,
                        interface="apply_energy_update",
                        code="ENERGY_UPDATE_ERROR",
                        message="注意力预算扣能失败，已降级为不扣能继续",
                        detail={"item_id": item.get("item_id", ""), "traceback": traceback.format_exc()},
                    )
                    pool_after_er = before_er
                    pool_after_ev = before_ev

            selected_items.append(
                {
                    **item,
                    "memory_er": memory_er,
                    "memory_ev": memory_ev,
                    "memory_total": round(memory_er + memory_ev, 8),
                    "pool_before_er": before_er,
                    "pool_before_ev": before_ev,
                    "pool_before_total": round(before_er + before_ev, 8),
                    "pool_after_er": pool_after_er,
                    "pool_after_ev": pool_after_ev,
                    "pool_after_total": round(pool_after_er + pool_after_ev, 8),
                    "attention_extract_ratio": resolved_ratio if resolved_consume else 0.0,
                    "attention_cost_applied": resolved_consume,
                }
            )
            consume_events.append(
                {
                    "item_id": item.get("item_id", ""),
                    "ref_object_id": item.get("ref_object_id", ""),
                    "ref_object_type": item.get("ref_object_type", ""),
                    "display": item.get("display", ""),
                    "memory_er": memory_er,
                    "memory_ev": memory_ev,
                    "memory_total": round(memory_er + memory_ev, 8),
                    "pool_before_er": before_er,
                    "pool_before_ev": before_ev,
                    "pool_after_er": pool_after_er,
                    "pool_after_ev": pool_after_ev,
                    "attention_priority": round(float(item.get("attention_priority", 0.0) or 0.0), 8),
                    "focus_boost": round(float(item.get("focus_boost", 0.0) or 0.0), 8),
                    "selected_by": item.get("selected_by", ""),
                }
            )

            consumed_total_er += memory_er
            consumed_total_ev += memory_ev

        cam_snapshot = self._make_cam_snapshot(selected_items=selected_items, trace_id=trace_id, tick_id=tick_id)
        structure_items = [item for item in selected_items if item.get("ref_object_type") == "st"]

        selected_by_counts: dict[str, int] = {}
        for it in selected_items:
            k = str(it.get("selected_by", "") or "unknown")
            selected_by_counts[k] = int(selected_by_counts.get(k, 0) or 0) + 1

        energy_eligible_count = 0
        for it in scored_items:
            total_energy = float(it.get("er", 0.0) or 0.0) + float(it.get("ev", 0.0) or 0.0)
            if total_energy > min_total_energy:
                energy_eligible_count += 1

        report = {
            "selection_basis": "注意力滤波：聚焦优先 + 动态阈值抑制 + 最少保留数兜底；入选对象再按比例抽取能量形成 CAM（有代价）。",
            # top_n 为兼容字段：表示 CAM 上限（cap），不保证填满。
            "top_n": resolved_top_n,
            "cam_item_cap": resolved_top_n,
            "min_cam_items": resolved_min_keep,
            "consume_enabled": resolved_consume,
            "consume_ratio": round(resolved_ratio, 8),
            "modulation_applied": dict(modulation) if isinstance(modulation, dict) else {},
            "effective_priority_weights": dict(effective_weights),
            "effective_cutoff_params": dict(effective_cutoff),
            "dynamic_cutoff": {
                "peak_score": round(float(peak_score), 8),
                "keep_ratio": round(float(keep_ratio), 8),
                "cutoff_score": round(float(cutoff_score), 8),
                "score_entropy": round(float(entropy), 8),
                "score_concentration": round(float(concentration), 8),
            },
            "focus_directive_count": len(focus_directives or []),
            "focus_directives": [dict(item) for item in (focus_directives or [])[:16] if isinstance(item, dict)],
            "selected_by_counts": selected_by_counts,
            "top_item_count": len(selected_items),
            "memory_item_count": len(selected_items),
            "top_items": selected_items,
            "structure_items": structure_items,
            "consume_events": consume_events,
            "consumed_total_er": round(consumed_total_er, 8),
            "consumed_total_ev": round(consumed_total_ev, 8),
            "consumed_total_energy": round(consumed_total_er + consumed_total_ev, 8),
            "memory_total_er": round(sum(float(item.get("memory_er", 0.0)) for item in selected_items), 8),
            "memory_total_ev": round(sum(float(item.get("memory_ev", 0.0)) for item in selected_items), 8),
            "memory_total_cp": round(sum(abs(float(item.get("memory_er", 0.0)) - float(item.get("memory_ev", 0.0))) for item in selected_items), 8),
            "state_pool_candidate_count": len(eligible_items),
            "state_pool_energy_eligible_count": int(energy_eligible_count),
            "skipped_memory_item_count": max(0, len(all_items) - len(eligible_items)),
            "source_pool_summary": source_snapshot.get("summary", {}),
            "cam_snapshot_summary": cam_snapshot.get("summary", {}),
        }

        self._logger.brief(
            trace_id=trace_id,
            tick_id=tick_id,
            interface="build_cam_from_pool",
            success=True,
            input_summary={
                "cam_item_cap": resolved_top_n,
                "min_cam_items": resolved_min_keep,
                "cutoff_score": round(float(cutoff_score), 8),
                "consume_enabled": resolved_consume,
                "consume_ratio": round(resolved_ratio, 8),
                "source_candidate_count": len(eligible_items),
                "focus_directive_count": len(focus_directives or []),
            },
            output_summary={
                "cam_item_count": len(selected_items),
                "cam_structure_count": len(structure_items),
                "consumed_total_energy": round(consumed_total_er + consumed_total_ev, 8),
            },
            message="CAM built",
        )

        return self._make_response(
            success=True,
            code="OK",
            message="CAM 构建完成 / CAM built",
            data={
                "cam_snapshot": cam_snapshot,
                "attention_report": report,
                "meta": {
                    "version": __version__,
                    "schema_version": __schema_version__,
                    "config": dict(self._config),
                    "metadata": metadata or {},
                },
            },
            trace_id=trace_id,
            elapsed_ms=self._elapsed_ms(start_time),
        )

    # ================================================================== #
    # 接口二：get_runtime_snapshot                                         #
    # ================================================================== #

    def get_runtime_snapshot(self, *, trace_id: str = "attention_snapshot") -> dict:
        start_time = time.time()
        return self._make_response(
            success=True,
            code="OK",
            message="attention runtime snapshot",
            data={
                "module": __module_name__,
                "version": __version__,
                "schema_version": __schema_version__,
                "config_summary": dict(self._config),
                "stats": {
                    "total_calls": int(self._total_calls),
                },
            },
            trace_id=trace_id,
            elapsed_ms=self._elapsed_ms(start_time),
        )

    # ================================================================== #
    # 接口三：reload_config                                                #
    # ================================================================== #

    def reload_config(
        self,
        *,
        trace_id: str,
        config_path: str | None = None,
        apply_partial: bool = True,
    ) -> dict:
        start_time = time.time()
        path = config_path or self._config_path

        try:
            new_raw = _load_yaml_config(path)
            if not new_raw:
                return self._make_response(
                    success=False,
                    code="CONFIG_ERROR",
                    message=f"配置文件加载失败或为空 / Config file failed to load or empty: {path}",
                    trace_id=trace_id,
                    elapsed_ms=self._elapsed_ms(start_time),
                )

            applied: list[str] = []
            rejected: list[dict] = []
            for key, val in new_raw.items():
                if key not in _DEFAULT_CONFIG:
                    rejected.append({"key": key, "reason": "未知配置项 / Unknown config key"})
                    continue
                expected_type = type(_DEFAULT_CONFIG[key])
                if isinstance(val, expected_type) or (expected_type is float and isinstance(val, (int, float))):
                    self._config[key] = val
                    applied.append(key)
                else:
                    rejected.append({
                        "key": key,
                        "reason": f"类型不匹配 / Type mismatch: expected {expected_type.__name__}, got {type(val).__name__}",
                    })

            self._logger.update_config(
                log_dir=str(self._config.get("log_dir", "")),
                max_file_bytes=int(self._config.get("log_max_file_bytes", 0) or 0),
            )

            self._logger.brief(
                trace_id=trace_id,
                interface="reload_config",
                success=True,
                input_summary={"path": path},
                output_summary={"applied_count": len(applied), "rejected_count": len(rejected)},
                message="hot reload done",
            )

            if rejected and not apply_partial:
                return self._make_response(
                    success=False,
                    code="CONFIG_ERROR",
                    message=f"部分配置项被拒绝 / Some config items rejected: {len(rejected)}",
                    data={"applied": applied, "rejected": rejected},
                    trace_id=trace_id,
                    elapsed_ms=self._elapsed_ms(start_time),
                )

            return self._make_response(
                success=True,
                code="OK",
                message=f"热加载完成 / Hot reload done: {len(applied)} applied, {len(rejected)} rejected",
                data={"applied": applied, "rejected": rejected},
                trace_id=trace_id,
                elapsed_ms=self._elapsed_ms(start_time),
            )
        except Exception as e:
            self._logger.error(
                trace_id=trace_id,
                interface="reload_config",
                code="CONFIG_ERROR",
                message=f"热加载失败: {e}",
                detail={"traceback": traceback.format_exc()},
            )
            return self._make_response(
                success=False,
                code="CONFIG_ERROR",
                message=f"热加载失败 / Hot reload failed: {e}",
                error={"code": "config_error", "message": str(e)},
                trace_id=trace_id,
                elapsed_ms=self._elapsed_ms(start_time),
            )

    def close(self) -> None:
        try:
            self._logger.close()
        except Exception:
            pass

    # ================================================================== #
    # 内部工具                                                            #
    # ================================================================== #

    def _build_config(self, config_override: dict | None) -> dict:
        config = dict(_DEFAULT_CONFIG)
        config.update(_load_yaml_config(self._config_path))
        if config_override:
            config.update(config_override)
        return config

    def _priority_score(self, item: dict, *, weights: dict | None = None, focus_directives: list[dict] | None = None) -> float:
        weights = weights or self._config
        er = float(item.get("er", 0.0))
        ev = float(item.get("ev", 0.0))
        total_energy = er + ev
        cp_abs = float(item.get("cp_abs", 0.0))
        salience = float(item.get("salience_score", 0.0))
        updated_at = float(item.get("updated_at", 0.0))
        fatigue = float(item.get("fatigue", 0.0))
        recency_gain = float(item.get("recency_gain", 0.0))

        score = (
            total_energy * float(weights.get("priority_weight_total_energy", 1.25))
            + cp_abs * float(weights.get("priority_weight_cp_abs", 0.35))
            + salience * float(weights.get("priority_weight_salience", 0.15))
            + updated_at * float(weights.get("priority_weight_updated_at", 1e-12))
            - fatigue * float(weights.get("priority_weight_fatigue", 0.0))
            + recency_gain * float(weights.get("priority_weight_recency_gain", 0.0))
        )

        focus_boost = self._compute_focus_boost(item, focus_directives)
        if focus_boost > 0.0:
            score += focus_boost * float(weights.get("focus_boost_weight", 1.0))
        return round(float(score), 12)

    @staticmethod
    def _compute_focus_boost(item: dict, focus_directives: list[dict] | None) -> float:
        """
        计算聚焦增益（不改语义，只在排序时做可解释的加成）。

        当前实现：严格按 target_ref_object_id/target_item_id 命中才加成，取最大值。
        boost = directive.focus_boost * directive.strength
        """
        if not focus_directives:
            return 0.0

        ref_id = str(item.get("ref_object_id", "") or "")
        ref_type = str(item.get("ref_object_type", "") or "")
        item_id = str(item.get("item_id", "") or "")

        best = 0.0
        for directive in focus_directives:
            if not isinstance(directive, dict):
                continue
            target_ref_id = str(directive.get("target_ref_object_id", "") or "")
            target_ref_type = str(directive.get("target_ref_object_type", "") or "")
            target_item_id = str(directive.get("target_item_id", "") or "")

            ref_match = bool(target_ref_id) and target_ref_id == ref_id and (not target_ref_type or target_ref_type == ref_type)
            item_match = bool(target_item_id) and target_item_id == item_id
            if not ref_match and not item_match:
                continue

            strength = float(directive.get("strength", 0.0) or 0.0)
            focus_boost = float(directive.get("focus_boost", 0.0) or 0.0)
            best = max(best, max(0.0, strength) * max(0.0, focus_boost))

        return float(best)

    def _make_cam_snapshot(self, *, selected_items: list[dict], trace_id: str, tick_id: str) -> dict:
        top_items: list[dict] = []
        type_counts: dict[str, int] = {}
        high_er = 0
        high_ev = 0
        high_cp = 0

        for item in selected_items:
            memory_er = round(float(item.get("memory_er", 0.0)), 8)
            memory_ev = round(float(item.get("memory_ev", 0.0)), 8)
            cp_delta = round(memory_er - memory_ev, 8)
            cp_abs = round(abs(cp_delta), 8)
            copied = dict(item)
            copied["er"] = memory_er
            copied["ev"] = memory_ev
            copied["cp_delta"] = cp_delta
            copied["cp_abs"] = cp_abs
            copied["salience_score"] = round(max(memory_er, memory_ev), 8)
            top_items.append(copied)

            ref_type = copied.get("ref_object_type", "unknown")
            type_counts[ref_type] = type_counts.get(ref_type, 0) + 1
            if memory_er >= 0.5:
                high_er += 1
            if memory_ev >= 0.5:
                high_ev += 1
            if cp_abs >= 0.5:
                high_cp += 1

        return {
            "snapshot_id": f"{trace_id}_cam",
            "object_type": "runtime_snapshot",
            "sub_type": "cam_snapshot",
            "schema_version": __schema_version__,
            "trace_id": trace_id,
            "tick_id": tick_id,
            "summary": {
                "active_item_count": len(top_items),
                "high_er_item_count": high_er,
                "high_ev_item_count": high_ev,
                "high_cp_item_count": high_cp,
                "object_type_counts": type_counts,
            },
            "top_items": top_items,
        }

    @staticmethod
    def _elapsed_ms(start: float) -> int:
        return int((time.time() - start) * 1000)

    @staticmethod
    def _make_response(
        success: bool,
        code: str,
        message: str,
        *,
        data: Any = None,
        error: Any = None,
        trace_id: str = "",
        elapsed_ms: int = 0,
    ) -> dict:
        return {
            "success": bool(success),
            "code": str(code),
            "message": str(message),
            "data": data,
            "error": error,
            "meta": {
                "module": __module_name__,
                "interface": "",
                "trace_id": trace_id,
                "elapsed_ms": int(elapsed_ms),
                "logged": True,
            },
        }
