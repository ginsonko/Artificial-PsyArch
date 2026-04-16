# -*- coding: utf-8 -*-
"""
AP 行动管理模块（Action/Drive）— 主模块
=====================================

对齐理论核心（3.12 Step 9 / 4.2.*）的原型落地目标：
  - 维护行动节点（Action Node, 行动意图）
  - 每 tick 更新 Drive（驱动力）并衰减
  - 当 Drive 超过阈值时，按阈值“消耗”并尝试触发行动

当前 MVP 范围（可运行优先）：
  - 仅实现“内在行动器”：
    1) 注意力聚焦（带参 focus_directives）
    2) 注意力发散/聚焦模式（通过 modulation.top_n 影响下一 tick）
    3) 回忆行动（从记忆赋能池 MAP 选一个候选，生成聚焦指令）
  - 不调用外部工具，不调用外部大模型，不生成真实语言输出

术语与缩写 / Glossary
--------------------
  - 行动节点（Action Node, AN）
  - 驱动力（Drive）
  - 状态池（StatePool, SP）
  - 认知感受信号（CFS, Cognitive Feeling Signals）
  - 情绪递质管理器（EMgr/NT）
  - 先天编码脚本管理器（IESM）
  - 记忆赋能池（MAP, Memory Activation Pool）
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
    try:
        import yaml  # type: ignore

        with open(path, "r", encoding="utf-8") as fh:
            data = yaml.safe_load(fh)
        return data if isinstance(data, dict) else {}
    except ImportError:
        return {}
    except Exception:
        return {}


_DEFAULT_CONFIG: dict[str, Any] = {
    "enabled": True,
    # ---- Drive (驱动力) ----
    "drive_decay_ratio": 0.85,
    "drive_max": 3.0,
    "node_idle_prune_ticks": 18,
    "max_action_nodes": 64,
    # ---- Execution limits / 执行配额（对齐理论 4.2.1.3 行动预算）----
    # 理论口径：每个行动器（Action Executor）有独立行动预算，
    # 在同一个 tick 内不应无限执行同类行动，否则会造成“无穷聚焦/无穷回忆”的失控。
    #
    # 工程落地口径（当前原型）：
    # - 以 action_kind 作为“行动器”的粗粒度标识（例如 attention_focus / recall）。
    # - 默认每个 action_kind 每 tick 允许执行多个行动（安全上限），
    #   但“是否冲突”主要由 mutex_key/mutex_keys（互斥资源 key）决定：
    #     - 冲突：同一 mutex_key 同 tick 只能 1 个胜出
    #     - 不冲突：不同 mutex_key 可并行执行
    #
    # 为什么不把每 kind 默认限制为 1？
    # - 你在验收口径中明确提出：同一行动器内“不冲突的行动可以同时执行”；冲突的行动不能同时执行。
    # - 因此默认应允许并行（由互斥域裁决），再用全局 cap 防止爆炸。
    # - 仍保留一个全局上限，避免未来加入更多 action_kind 后出现意外爆发。
    # 每个 action_kind 每 tick 最大可执行数（默认）。
    # 说明：对齐你最新验收口径：默认应尽量“像人”而不是“一次执行很多”。
    # - 冲突仲裁主要靠 mutex_key/mutex_keys（互斥资源 key）
    # - 这里的数量上限主要用于“安全刹车”，避免未来把一个 action_kind 细分成多个不冲突域后单 tick 过载
    "max_actions_per_kind_per_tick_default": 1,
    "max_actions_per_kind_per_tick": {},
    "max_total_actions_per_tick": 8,
    # ---- Conflict / 冲突仲裁（对齐你提出的“同一行动器：冲突不能同时执行”）----
    # 说明（工程口径）：
    # - “是否冲突”由行动器注册表给出默认口径（default_mutex_keys_by_action_kind）。
    # - 同时允许规则/触发源在 params 中显式指定 mutex_key/mutex_keys 来细分冲突域：
    #     例如：attention_focus 的 mutex_key=focus:text 与 focus:vision 可视为不冲突并行。
    # - 冲突仲裁发生在同一 tick 的执行阶段：同一个 mutex_key 只允许 1 个行动胜出。
    "default_mutex_keys_by_action_kind": {
        # 注意力聚焦：默认互斥（同 tick 只能一个“聚焦类行动”胜出）
        "attention_focus": ["attention_focus"],
        # 注意力模式：聚焦/发散属于同一资源（attention_mode），因此互斥
        "attention_focus_mode": ["attention_mode"],
        "attention_diverge_mode": ["attention_mode"],
        # 回忆：默认互斥（同 tick 只能一个回忆行动）
        "recall": ["recall"],
        # 停止/取消：默认互斥（防止 stop_action 自己被刷屏）
        "stop_action": ["stop_action"],
    },
    # ---- Threshold modulation / 行动阈值调制（对齐理论 4.2.1.4）----
    # 说明：
    # - base_threshold：规则/触发源给出的“先天基准阈值”
    # - effective_threshold：本 tick 实际用于判定 drive>=threshold 的阈值
    # - effective_threshold = base_threshold * threshold_scale
    #
    # threshold_scale 的来源（原型实现）：
    # 1) 情绪递质 NT（例如 DA 降低阈值、COR 提高阈值）
    # 2) 行动疲劳（重复执行会提高阈值，防止死循环）
    "threshold_scale_by_nt": {
        # key: NT channel code; value: linear coefficient.
        # 线性系数：scale = 1 + Σ(nt[ch] * coef)
        "DA": -0.25,   # 多巴胺（奖励驱动）偏冲动：阈值降低
        "ADR": -0.06,  # 肾上腺素（警觉唤醒）轻微降低阈值（更容易启动行动）
        "OXY": -0.05,  # 催产素（亲和连接）轻微降低阈值（更愿意互动/回应）
        "SER": +0.06,  # 血清素（稳定满足）轻微提高阈值（更稳重）
        "END": -0.03,  # 内啡肽（舒缓镇痛）轻微降低阈值（更容易采取缓解行动）
        "COR": +0.30,  # 皮质醇（长期警戒）提高阈值（更保守）
    },
    "threshold_scale_min": 0.55,
    "threshold_scale_max": 1.75,
    # ---- Action fatigue (local) / 行动疲劳（模块内局部实现）----
    "action_fatigue_enabled": True,
    "action_fatigue_decay_ratio": 0.92,
    "action_fatigue_increase_on_execute": 0.35,
    "action_fatigue_threshold_gain": 0.55,  # effective_threshold *= (1 + fatigue*gain)
    # ---- Attention focus directives (注意力聚焦，带参) ----
    "focus_threshold": 0.30,
    "focus_gain_base": 1.00,
    # ---- Attention focus/diverge mode (注意力聚焦/发散模式) ----
    "mode_threshold": 0.55,
    "mode_drive_gain": 0.60,
    "focus_mode_complexity_threshold": 0.65,
    "diverge_mode_complexity_threshold": 0.25,
    "mode_focus_top_n_scale": 0.70,
    "mode_diverge_top_n_scale": 1.30,
    "mode_cooldown_ticks": 2,
    # ---- Recall (回忆行动) ----
    "recall_threshold": 0.40,
    "recall_gain_base": 0.90,
    "recall_trigger_kinds": ["expectation", "pressure"],
    "recall_min_strength": 0.45,
    "recall_focus_boost": 0.65,
    "recall_ttl_ticks": 2,
    # Recall candidate competition (parameterized recall) / 回忆候选竞争（带参回忆，偏理论口径）
    # - 疲劳：短期内被选中过的记忆会被惩罚，促进多次回忆命中更多不同记忆
    "recall_memory_fatigue_window_ticks": 4,
    "recall_memory_fatigue_penalty": 0.60,
    # - 时间/新鲜度影响：更接近目标时间、且记忆本身更“新”的候选更占优
    "recall_recency_scale_sec": 30.0,
    # recall -> MAP（记忆赋能池）的赋能量（MVP 映射）
    #
    # 说明（重要）：
    # - Drive（驱动力）不是 ER/EV（双能量），二者单位不同。
    # - 但为了让“回忆行动”能在原型阶段形成闭环（对齐理论 4.2.7.2：回忆->记忆进入 MAP->记忆反哺回到状态池），
    #   我们需要一个“Drive 消耗 -> MAP 能量赋能”的映射。
    # - 这里用一个可配置的线性映射：delta_ev ≈ effective_threshold * per_threshold * strength
    #   并设置 min/max，避免回忆一触发就把 MAP/状态池灌爆。
    "recall_map_delta_ev_per_threshold": 0.90,
    "recall_map_delta_ev_min": 0.08,
    "recall_map_delta_ev_max": 0.85,
    "recall_map_mode_tag": "recall_action",
    # ---- Built-in triggers (legacy fallback) / 内置触发源（旧逻辑回退）----
    # 说明：
    # - 为了满足验收口径：“先天行动触发规则应可在 IESM 先天规则里观察与编辑”，
    #   复杂度/回忆这类触发源建议由 IESM 通过 action_trigger 输出，而不是行动模块内部硬编码。
    # - 因此默认关闭内置触发；如需快速验证或回退，可手动打开。
    "enable_builtin_triggers_complexity": False,
    "enable_builtin_triggers_recall": False,
    # ---- Observability / 可观测性 ----
    # executed_history_keep / 最近执行历史保留条数
    # 作用：前端“行动监控”页需要看到最近执行过哪些行动、来源是先天触发还是内驱触发。
    # 说明：仅用于观测与调试；不会影响行动逻辑。
    "executed_history_keep": 200,
    # ---- 日志 ----
    "log_dir": "",
    "log_max_file_bytes": 5 * 1024 * 1024,
    "stdout_fallback_when_log_fail": True,
}


class ActionManager:
    """
    行动管理器（Action Manager）。

    说明：
      - 本模块维护的 drive 是“行动触发资源”，不是 ER/EV（双能量）。
      - 但 drive 与 ER/EV 的关系可以由未来脚本化公式对齐（例如 drive_gain 受 NT/CFS 调制）。
    """

    def __init__(self, config_path: str = "", config_override: dict | None = None):
        self._config_path = config_path or os.path.join(os.path.dirname(__file__), "config", "action_config.yaml")
        self._config = self._build_config(config_override)
        self._logger = ModuleLogger(
            log_dir=str(self._config.get("log_dir", "")),
            max_file_bytes=int(self._config.get("log_max_file_bytes", 0) or 0),
            enable_stdout_fallback=bool(self._config.get("stdout_fallback_when_log_fail", True)),
        )

        self._tick_counter = 0
        # action_id -> node dict
        self._nodes: dict[str, dict[str, Any]] = {}
        # Built-in executor registry (for observability) / 内置行动器注册表（用于观测与审计）
        self._executor_registry: list[dict[str, Any]] = self._build_executor_registry()
        # Recent executed actions ring buffer / 最近执行行动环形缓冲（用于前端观测）
        self._executed_history: list[dict[str, Any]] = []
        # Recall memory fatigue (short-term) / 回忆记忆疲劳（短期）
        # - key: memory_id
        # - value: last picked tick_index (from run_action_cycle input), used for diversification
        self._recall_memory_last_picked_tick: dict[str, int] = {}

    def _build_executor_registry(self) -> list[dict[str, Any]]:
        """
        行动器注册表（内置）。

        目的：
          - 让前端观测台能“看到系统到底注册了哪些行动接口”
          - 便于把 IESM（先天规则）的 action_trigger 与行动器能力对齐
        """
        return [
            {
                "action_kind": "attention_focus",
                "title_zh": "注意力聚焦（内在行动器，带参）",
                "desc_zh": "输出 focus_directive，下一 tick 会影响注意力筛选排序。该行动不会直接修改状态池能量。",
                "params_schema": {
                    "focus_directive": {
                        "directive_type": "attention_focus",
                        "target_ref_object_id": "st_* 等",
                        "target_ref_object_type": "st/sa/cfs_signal 等",
                        "strength": "0~1",
                        "focus_boost": ">=0",
                        "ttl_ticks": ">=1",
                    }
                },
                "sources_zh": ["先天规则 IESM focus_directives", "先天规则 IESM action_trigger(kind=attention_focus)"],
            },
            {
                "action_kind": "attention_focus_mode",
                "title_zh": "注意力模式切换：聚焦模式（不带参）",
                "desc_zh": "输出 modulation_out.attention.top_n（更聚焦：更少对象进入 CAM）。",
                "params_schema": {"complexity": "0~1"},
                "sources_zh": ["认知感受 CFS: complexity（繁）", "先天规则 IESM action_trigger"],
            },
            {
                "action_kind": "attention_diverge_mode",
                "title_zh": "注意力模式切换：发散模式（不带参）",
                "desc_zh": "输出 modulation_out.attention.top_n（更发散：更多对象进入 CAM）。",
                "params_schema": {"complexity": "0~1"},
                "sources_zh": ["认知感受 CFS: complexity（简）", "先天规则 IESM action_trigger"],
            },
            {
                "action_kind": "recall",
                "title_zh": "回忆行动（内在行动器）",
                "desc_zh": "从记忆赋能池（MAP）选一个候选，生成聚焦指令，引导下一 tick 的注意力回到相关记忆结构。",
                "params_schema": {
                    "trigger_kind": "expectation/pressure 等",
                    "trigger_strength": "0~1",
                    "trigger_target": "可选：目标对象信息",
                },
                "sources_zh": ["认知感受 CFS: expectation/pressure", "先天规则 IESM action_trigger(kind=recall)"],
            },
        ]

    def close(self) -> None:
        try:
            self._logger.close()
        except Exception:
            pass

    # ================================================================== #
    # Main interface                                                      #
    # ================================================================== #

    def run_action_cycle(
        self,
        *,
        trace_id: str,
        tick_id: str,
        tick_index: int,
        cfs_signals: list[dict] | None = None,
        emotion_state: dict | None = None,
        innate_focus_directives: list[dict] | None = None,
        innate_action_triggers: list[dict] | None = None,
        memory_activation_snapshot: dict | None = None,
    ) -> dict:
        """
        主入口：更新驱动力、竞争并尝试触发行动。

        输入说明（原型口径）：
          - cfs_signals：来自 CFS 的结构化信号列表（含强度、目标）
          - emotion_state：来自 EMgr 的输出（含 modulation/nt_state）
          - innate_focus_directives：IESM 生成的 focus_directives（当前实现把它视为行动触发源）
          - memory_activation_snapshot：HDB 记忆赋能池快照（用于回忆行动选目标）
        """
        start_time = time.time()
        self._tick_counter += 1
        tick_number = self._tick_counter

        cfs_signals = list(cfs_signals or [])
        emotion_state = emotion_state or {}
        innate_focus_directives = list(innate_focus_directives or [])
        innate_action_triggers = list(innate_action_triggers or [])
        memory_activation_snapshot = memory_activation_snapshot or {}

        if not self._config.get("enabled", True):
            return self._make_response(
                True,
                "OK_DISABLED",
                "行动模块已禁用 / Action module disabled",
                data={
                    "executed_actions": [],
                    "focus_directives_out": [],
                    "modulation_out": {},
                    "nodes": [],
                    "triggers": [],
                },
                trace_id=trace_id,
                tick_id=tick_id,
                elapsed_ms=self._elapsed_ms(start_time),
            )

        # ---- Step 1: build triggers / 构造触发源 ----
        triggers: list[dict[str, Any]] = []
        triggers.extend(self._triggers_from_focus_directives(innate_focus_directives))
        triggers.extend(self._triggers_from_action_triggers(innate_action_triggers))
        # NOTE:
        # - 复杂度/回忆等触发源推荐由 IESM 规则通过 action_trigger 输出（便于观察与编辑）。
        # - 此处保留内置触发作为“旧逻辑回退”，默认关闭。
        if bool(self._config.get("enable_builtin_triggers_complexity", False)):
            triggers.extend(self._triggers_from_complexity(cfs_signals))
        if bool(self._config.get("enable_builtin_triggers_recall", False)):
            triggers.extend(self._triggers_from_recall(cfs_signals, memory_activation_snapshot))

        # ---- Step 2: decay drives / 驱动力衰减 ----
        decay = float(self._config.get("drive_decay_ratio", 0.85))
        drive_max = float(self._config.get("drive_max", 3.0))
        for node in self._nodes.values():
            node["drive"] = max(0.0, min(drive_max, float(node.get("drive", 0.0)) * decay))
            node["last_update_tick"] = tick_number
            # Reset per-tick trigger summary.
            node["tick_gain_total"] = 0.0
            node["tick_gain_by_source_kind"] = {}
            node["tick_sources"] = []

            # Local fatigue decay (if enabled) / 行动疲劳衰减（若启用）
            if bool(self._config.get("action_fatigue_enabled", True)):
                fr = float(self._config.get("action_fatigue_decay_ratio", 0.92))
                node["fatigue"] = max(0.0, min(1.0, float(node.get("fatigue", 0.0) or 0.0) * fr))

        # ---- Step 3: apply triggers / 应用触发增益（驱动力增加） ----
        for trig in triggers:
            self._apply_trigger(trig, tick_number=tick_number)

        # ---- Step 4: prune nodes / 淘汰长期闲置节点 ----
        self._prune_idle_nodes(tick_number=tick_number)

        # ---- Step 5: competition + execution / 竞争与执行 ----
        executed: list[dict[str, Any]] = []
        focus_out: list[dict[str, Any]] = []
        modulation_out: dict[str, Any] = {}
        # recall_requests_out：当“回忆”行动被执行时，输出一个结构化请求，
        # 由上层观测台（Observatory）负责实际执行“记忆检索 -> MAP 赋能 -> 记忆反哺”副作用，
        # 从而保持 ActionManager 本身不直接操作 HDB/StatePool（便于审计与测试）。
        recall_requests_out: list[dict[str, Any]] = []
        nt = (emotion_state.get("modulation", {}) or {}).get("attention", {}) or {}

        # Compute effective thresholds for all nodes once per tick.
        # 为本 tick 的所有行动节点计算一次“实时阈值”（用于排序与执行判定）。
        for node in self._nodes.values():
            eff = self._compute_effective_threshold(node=node, emotion_state=emotion_state)
            node["base_threshold"] = float(eff.get("base_threshold", node.get("threshold", 1.0) or 1.0))
            node["threshold_scale"] = float(eff.get("threshold_scale", 1.0))
            node["effective_threshold"] = float(eff.get("effective_threshold", node.get("threshold", 1.0) or 1.0))
            node["threshold_components"] = eff.get("components", {})

        # Candidate selection (budget + conflict arbitration) / 候选选择（预算 + 冲突仲裁）
        #
        # 对齐理论 + 你的验收口径：
        # 1) 行动器预算（Budget）：同一行动器（粗粒度用 action_kind 表示）在同一个 tick 内的执行数量应有限，
        #    默认每 tick 只执行 1 个（可配置上调）。
        # 2) 冲突仲裁（Conflict）：同一行动器内“不冲突”的行动可以并行执行；“冲突”的行动同 tick 只能一个胜出。
        #    - 冲突域用 mutex_key/mutex_keys 表示（互斥资源 key）。
        #    - 默认由行动器注册表（default_mutex_keys_by_action_kind）给出；
        #    - 也允许触发源/规则在 params 中覆盖 mutex_key/mutex_keys 以细分冲突域。
        #
        # 竞争排序（用于挑胜者）：
        #   1) 本 tick 增益 tick_gain_total（可近似理解为“本轮赋能更强”）
        #   2) 驱动力 drive（行动意图更强）
        #   3) 规则优先级 rule_priority（安全/终止类规则可更高）
        #   4) action_id（稳定打破平局）

        def _max_actions_per_kind(kind: str) -> int:
            """Get per-kind execution budget for this tick / 获取每类行动器的本 tick 执行上限。"""
            default_n = int(self._config.get("max_actions_per_kind_per_tick_default", 1) or 1)
            override = self._config.get("max_actions_per_kind_per_tick", {}) or {}
            if isinstance(override, dict) and str(kind or "") in override:
                try:
                    return max(0, min(64, int(override.get(str(kind or ""), default_n) or 0)))
                except Exception:
                    return max(0, min(64, default_n))
            return max(0, min(64, default_n))

        def _tick_max_rule_priority(node: dict[str, Any]) -> int:
            """Best-effort: read the max rule_priority from this tick sources / 取本 tick 触发源中最大的 rule_priority。"""
            best = 0
            for src in (node.get("tick_sources", []) or []):
                if not isinstance(src, dict):
                    continue
                try:
                    best = max(best, int(src.get("rule_priority", src.get("priority", 0)) or 0))
                except Exception:
                    continue
            return int(best)

        def _rank_key(node: dict[str, Any]) -> tuple[float, float, int, str]:
            """Sort key for competition within the same action_kind / 同一行动器内竞争排序键。"""
            energy_score = float(node.get("tick_gain_total", 0.0) or 0.0)
            drive_score = float(node.get("drive", 0.0) or 0.0)
            pri_score = _tick_max_rule_priority(node)
            aid = str(node.get("action_id", "") or "")
            # Descending for first three fields; action_id ascending for stable tie-break.
            return (energy_score, drive_score, pri_score, aid)

        def _node_mutex_keys(node: dict[str, Any]) -> list[str]:
            """Compute mutex keys for a node / 计算行动节点的互斥资源 key 列表。"""
            kind2 = str(node.get("action_kind", "") or "").strip()
            params2 = node.get("params") if isinstance(node.get("params"), dict) else {}

            # 1) Explicit override from params / 优先使用规则/触发源显式指定的互斥 key
            raw_keys = None
            for k in ("mutex_keys", "mutex_key", "conflict_keys", "conflict_key"):
                if k in params2:
                    raw_keys = params2.get(k)
                    break
            if raw_keys is None and "mutex_keys" in node:
                raw_keys = node.get("mutex_keys")
            if raw_keys is None and "mutex_key" in node:
                raw_keys = node.get("mutex_key")

            keys: list[str] = []
            if isinstance(raw_keys, str) and raw_keys.strip():
                keys = [raw_keys.strip()]
            elif isinstance(raw_keys, list):
                keys = [str(x).strip() for x in raw_keys if str(x).strip()]

            # 2) Default registration by action_kind / 默认用行动器注册表口径
            if not keys:
                dm = self._config.get("default_mutex_keys_by_action_kind", {}) or {}
                if isinstance(dm, dict) and kind2 in dm:
                    raw = dm.get(kind2) or []
                    if isinstance(raw, str) and raw.strip():
                        keys = [raw.strip()]
                    elif isinstance(raw, list):
                        keys = [str(x).strip() for x in raw if str(x).strip()]

            # 3) Fallback to action_kind itself / 兜底：以 action_kind 作为互斥域
            if not keys and kind2:
                keys = [kind2]

            # Dedupe while keeping order / 去重（保持顺序）
            deduped: list[str] = []
            seen = set()
            for key in keys:
                if not key or key in seen:
                    continue
                seen.add(key)
                deduped.append(key)
            return deduped

        # Build executable candidates / 生成可执行候选（已过阈值 + 冷却条件满足）
        candidates: list[dict[str, Any]] = []
        for node in self._nodes.values():
            if not self._should_execute(node, tick_number=tick_number):
                continue
            if not str(node.get("action_kind", "") or "").strip():
                continue
            candidates.append(node)

        # Greedy selection with mutex + per-kind budget / 互斥资源 + 行动器预算的贪婪选择
        exec_cap = int(self._config.get("max_total_actions_per_tick", 8) or 8)
        exec_cap = max(0, min(128, exec_cap))
        kind_counts: dict[str, int] = {}
        used_mutex_keys: set[str] = set()
        ranked = sorted(candidates, key=_rank_key, reverse=True)

        selected: list[dict[str, Any]] = []
        for node in ranked:
            if len(selected) >= exec_cap:
                break
            kind2 = str(node.get("action_kind", "") or "").strip()
            limit2 = _max_actions_per_kind(kind2)
            if limit2 <= 0:
                continue
            if int(kind_counts.get(kind2, 0) or 0) >= int(limit2):
                continue

            mutex_keys = _node_mutex_keys(node)
            if mutex_keys and any(k in used_mutex_keys for k in mutex_keys):
                continue

            selected.append(node)
            kind_counts[kind2] = int(kind_counts.get(kind2, 0) or 0) + 1
            for k in mutex_keys:
                used_mutex_keys.add(k)

        for node in selected:
            kind = str(node.get("action_kind", "") or "")
            ok = True
            produced = {"focus_directives": [], "modulation": {}}
            failure_reason = ""

            if kind == "attention_focus":
                params = (node.get("params", {}) or {}) if isinstance(node.get("params", {}), dict) else {}
                directive = dict(params.get("focus_directive", {}) or {}) if isinstance(params.get("focus_directive", {}), dict) else {}
                if not directive:
                    # Convenience path:
                    # If the trigger provides "target_*" fields directly, build a standard focus_directive.
                    #
                    # 便捷路径：
                    # 若触发只给了 target_* 字段（而没给完整 focus_directive），这里自动补齐为标准指令格式，
                    # 让 IESM 的 action_trigger 更易写、更易读（减少冗长 JSON）。
                    target_ref_id = str(params.get("target_ref_object_id", "") or params.get("ref_object_id", "") or "").strip()
                    target_ref_type = str(params.get("target_ref_object_type", "") or params.get("ref_object_type", "") or "").strip()
                    target_item_id = str(params.get("target_item_id", "") or params.get("item_id", "") or "").strip()
                    target_display = str(params.get("target_display", "") or params.get("display", "") or target_ref_id or target_item_id).strip()
                    if target_ref_id or target_item_id:
                        try:
                            strength = self._clamp01(float(params.get("strength", params.get("match_value", 1.0)) or 1.0))
                        except Exception:
                            strength = 1.0
                        try:
                            focus_boost = float(params.get("focus_boost", 0.9) or 0.9)
                        except Exception:
                            focus_boost = 0.9
                        try:
                            ttl_ticks = int(params.get("ttl_ticks", 2) or 2)
                        except Exception:
                            ttl_ticks = 2
                        ttl_ticks = max(1, min(64, ttl_ticks))

                        now_ms = int(time.time() * 1000)
                        directive = {
                            "directive_id": f"focus_action_{node.get('action_id', 'unknown')}_{tick_id}",
                            "directive_type": "attention_focus",
                            "source_kind": str(params.get("source_kind", "action_trigger") or "action_trigger"),
                            "strength": round(float(strength), 6),
                            "focus_boost": round(max(0.0, float(focus_boost)), 6),
                            "ttl_ticks": int(ttl_ticks),
                            "target_ref_object_id": target_ref_id,
                            "target_ref_object_type": target_ref_type,
                            "target_item_id": target_item_id,
                            "target_display": target_display,
                            "created_at": now_ms,
                            "reasons": [
                                "action:attention_focus",
                                f"action_id:{node.get('action_id', '')}",
                                "from:action_trigger_params",
                            ],
                        }

                if directive:
                    produced["focus_directives"].append(directive)
            elif kind in {"attention_focus_mode", "attention_diverge_mode"}:
                base_top_n = int(nt.get("top_n", 16) or 16)
                if kind == "attention_focus_mode":
                    scale = float(self._config.get("mode_focus_top_n_scale", 0.70))
                else:
                    scale = float(self._config.get("mode_diverge_top_n_scale", 1.30))
                top_n = int(round(base_top_n * max(0.1, scale)))
                top_n = max(4, min(64, top_n))
                produced["modulation"] = {"attention": {"top_n": top_n, "reason": kind}}
            elif kind == "recall":
                directive = self._build_recall_focus_directive(
                    node=node,
                    tick_id=tick_id,
                    tick_index=int(tick_index),
                    now_ms=int(time.time() * 1000),
                    memory_activation_snapshot=memory_activation_snapshot,
                )
                if directive:
                    produced["focus_directives"].append(directive)
                else:
                    ok = False
                    failure_reason = "no_recall_candidate"
            else:
                ok = False
                failure_reason = "unknown_action_kind"

            # Always record an "attempt" for observability (even when failed).
            # 无论成功与否，都记录一次“尝试”，便于前端观测：看见“尝试了什么、为什么失败、来源是先天还是内驱”。
            eff_threshold = float(node.get("effective_threshold", node.get("threshold", 0.0) or 0.0) or 0.0)
            drive_before = round(float(node.get("drive", 0.0) or 0.0), 8)
            node["last_attempt_tick"] = tick_number

            if ok:
                # Consume drive by threshold (not clear).
                # 按“实时阈值”消耗 drive（不是清零）。
                node["drive"] = max(0.0, float(node.get("drive", 0.0)) - max(0.0, eff_threshold))
                node["last_trigger_tick"] = tick_number
                # Fatigue bump on execute / 执行后疲劳上升（避免无限循环）
                if bool(self._config.get("action_fatigue_enabled", True)):
                    inc = float(self._config.get("action_fatigue_increase_on_execute", 0.35))
                    node["fatigue"] = max(0.0, min(1.0, float(node.get("fatigue", 0.0) or 0.0) + max(0.0, inc)))

            # Derive origin tags (passive vs active) from current + historical sources.
            # 从触发源推断“被动/主动”标签（用于观测；不影响逻辑）。
            source_kinds = [str(s.get("kind", "") or "") for s in (node.get("tick_sources", []) or []) if isinstance(s, dict)]
            passive = any(k.startswith("iesm_") for k in source_kinds)
            active = any((k and not k.startswith("iesm_")) for k in source_kinds)

            rec = {
                "action_id": node.get("action_id", ""),
                "action_kind": kind,
                "attempted": True,
                "success": bool(ok),
                "drive_before": drive_before,
                "drive_after": round(float(node.get("drive", 0.0)), 8),
                "base_threshold": round(float(node.get("base_threshold", node.get("threshold", 0.0) or 0.0)), 8),
                "threshold_scale": round(float(node.get("threshold_scale", 1.0) or 1.0), 8),
                "effective_threshold": round(float(eff_threshold), 8),
                "fatigue": round(float(node.get("fatigue", 0.0) or 0.0), 8),
                "produced": produced,
                "trigger_sources": list(node.get("trigger_sources", []) or [])[:6],
                "tick_gain_total": round(float(node.get("tick_gain_total", 0.0) or 0.0), 8),
                "tick_gain_by_source_kind": dict(node.get("tick_gain_by_source_kind", {}) or {}),
                "origin": {
                    "passive_iesm": bool(passive),
                    "active_internal": bool(active),
                },
            }

            # 若回忆行动成功执行，则生成“回忆请求”，交由上层执行。
            # 说明：对齐理论 4.2.7.2 回忆流程：回忆不是只输出聚焦指令，
            # 而是要把检索到的记忆赋能进入 MAP，并走默认的记忆反哺（memory_feedback）回到状态池。
            if ok and kind == "recall":
                req = self._build_recall_request(
                    node=node,
                    tick_id=tick_id,
                    tick_index=int(tick_index),
                    now_ms=int(time.time() * 1000),
                    drive_before=float(drive_before),
                    effective_threshold=float(eff_threshold),
                    memory_activation_snapshot=memory_activation_snapshot,
                )
                produced["recall_request"] = dict(req)
                rec["recall_request"] = dict(req)
                recall_requests_out.append(dict(req))

            if not ok and failure_reason:
                rec["failure_reason"] = str(failure_reason)
            executed.append(rec)

            if ok:
                focus_out.extend(produced.get("focus_directives", []) or [])
                modulation_out = self._merge_modulation(modulation_out, produced.get("modulation", {}) or {})

        focus_out = self._dedup_focus_directives(focus_out)

        # Node snapshot for UI / 节点快照（供前端展示）
        # 注意：这里应展示“当前最强的一批节点”，而不是仅展示本 tick 被选中竞争/执行的节点。
        nodes_ranked_for_snapshot = sorted(
            list(self._nodes.values()),
            key=lambda n: float(n.get("drive", 0.0) or 0.0),
            reverse=True,
        )
        nodes_snapshot = [
            {
                "action_id": node.get("action_id", ""),
                "action_kind": node.get("action_kind", ""),
                "drive": round(float(node.get("drive", 0.0)), 8),
                "base_threshold": round(float(node.get("base_threshold", node.get("threshold", 0.0) or 0.0)), 8),
                "threshold_scale": round(float(node.get("threshold_scale", 1.0) or 1.0), 8),
                "effective_threshold": round(float(node.get("effective_threshold", node.get("threshold", 0.0) or 0.0)), 8),
                "fatigue": round(float(node.get("fatigue", 0.0) or 0.0), 8),
                "cooldown_ticks": int(node.get("cooldown_ticks", 0) or 0),
                "last_trigger_tick": int(node.get("last_trigger_tick", -1) or -1),
                "last_update_tick": int(node.get("last_update_tick", -1) or -1),
                "tick_gain_total": round(float(node.get("tick_gain_total", 0.0) or 0.0), 8),
            }
            for node in nodes_ranked_for_snapshot[:24]
        ]

        self._logger.brief(
            trace_id=trace_id,
            tick_id=tick_id,
            interface="run_action_cycle",
            success=True,
            message="行动模块已计算 Drive 并尝试执行动作 / Drive updated and actions attempted",
            input_summary={
                "tick_index": int(tick_index),
                "cfs_signal_count": len(cfs_signals),
                "innate_focus_directive_count": len(innate_focus_directives),
                "innate_action_trigger_count": len(innate_action_triggers),
                "memory_activation_item_count": len(memory_activation_snapshot.get("items", []) or []),
            },
            output_summary={
                "trigger_count": len(triggers),
                "node_count": len(self._nodes),
                "executed_action_count": len(executed),
                "focus_directives_out": len(focus_out),
            },
        )
        self._logger.detail(
            trace_id=trace_id,
            tick_id=tick_id,
            step="action_cycle_detail",
            info={
                "tick_number": tick_number,
                "triggers": triggers,
                "executed": executed,
                "modulation_out": modulation_out,
                "focus_directives_out": focus_out,
                "nodes": nodes_snapshot,
            },
        )

        # Record executed history for the observatory UI (ring buffer).
        # 记录最近执行历史（环形缓冲），供观测台“行动监控”页面实时查看。
        self._append_executed_history(tick_id=tick_id, tick_number=tick_number, executed=executed)

        return self._make_response(
            True,
            "OK",
            "行动模块执行完成 / Action cycle finished",
            data={
                "executed_actions": executed,
                "focus_directives_out": focus_out,
                "modulation_out": modulation_out,
                "recall_requests_out": recall_requests_out,
                "nodes": nodes_snapshot,
                "triggers": triggers[:64],
                "executors_registry": list(self._executor_registry),
                "threshold_modulation": {
                    "threshold_scale_by_nt": dict(self._config.get("threshold_scale_by_nt", {}) or {}),
                    "threshold_scale_min": float(self._config.get("threshold_scale_min", 0.55) or 0.55),
                    "threshold_scale_max": float(self._config.get("threshold_scale_max", 1.75) or 1.75),
                    "action_fatigue_enabled": bool(self._config.get("action_fatigue_enabled", True)),
                },
                "meta": {
                    "version": __version__,
                    "schema_version": __schema_version__,
                    "tick_number": tick_number,
                },
            },
            trace_id=trace_id,
            tick_id=tick_id,
            elapsed_ms=self._elapsed_ms(start_time),
        )

    def _append_executed_history(self, *, tick_id: str, tick_number: int, executed: list[dict[str, Any]]) -> None:
        """Append executed actions into a bounded history buffer.

        追加“已执行行动”到一个有界历史缓冲：
        - 仅用于观测与调试（不会反过来影响行动逻辑）。
        - 让前端能看到“最近执行了哪些行动、来源是先天触发还是内驱触发”。
        """
        keep = int(self._config.get("executed_history_keep", 200) or 200)
        keep = max(0, min(5000, keep))
        if keep <= 0:
            return
        if not executed:
            return

        now_ms = int(time.time() * 1000)
        for row in executed:
            if not isinstance(row, dict):
                continue
            rec = dict(row)
            rec["tick_id"] = str(tick_id or "")
            rec["tick_number"] = int(tick_number)
            rec["recorded_at_ms"] = int(now_ms)
            self._executed_history.append(rec)

        if len(self._executed_history) > keep:
            self._executed_history = self._executed_history[-keep:]

    # ================================================================== #
    # Triggers / 触发源                                                   #
    # ================================================================== #

    def _triggers_from_focus_directives(self, directives: list[dict]) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        gain_base = float(self._config.get("focus_gain_base", 0.70))
        threshold = float(self._config.get("focus_threshold", 0.40))
        for d in directives:
            if not isinstance(d, dict):
                continue
            strength = self._clamp01(float(d.get("strength", 1.0) or 1.0))
            gain = max(0.0, gain_base) * strength
            action_id = self._focus_action_id(d)
            if not action_id:
                continue
            out.append(
                {
                    "action_id": action_id,
                    "action_kind": "attention_focus",
                    "gain": round(gain, 8),
                    "threshold": threshold,
                    "cooldown_ticks": 0,
                    "params": {"focus_directive": dict(d)},
                    "source": {"kind": "iesm_focus_directive", "strength": strength},
                }
            )
        return out

    def _triggers_from_action_triggers(self, items: list[dict]) -> list[dict[str, Any]]:
        """Convert IESM action_triggers into internal trigger records.

        将 IESM（先天规则）输出的 action_triggers 转为行动模块内部触发源记录。

        说明（原型约定 / MVP contract）:
        - IESM 的 action_trigger 是“结构化触发”，不直接执行代码。
        - 这里把它映射为与本模块一致的 trigger schema:
          {action_id, action_kind, gain, threshold, cooldown_ticks, params, source}
        - 未识别/缺少字段的触发会被忽略（保持安全与可审计）。

        支持字段（兼容写法）:
        - action_id 或 id: 必填
        - action_kind 或 kind: 选填（默认 custom）
        - gain 或 drive_gain: 选填（默认 0）
        - threshold: 选填（默认 1.0）
        - cooldown_ticks: 选填（默认 0）
        - params: 选填（dict）
        - rule_id: 由规则引擎补充，用于审计
        """
        out: list[dict[str, Any]] = []
        for item in items:
            if not isinstance(item, dict):
                continue
            action_id = str(item.get("action_id", "") or item.get("id", "") or "").strip()
            if not action_id:
                continue

            action_kind = str(item.get("action_kind", "") or item.get("kind", "") or "custom").strip() or "custom"
            try:
                gain = float(item.get("gain", item.get("drive_gain", 0.0)) or 0.0)
            except Exception:
                gain = 0.0
            try:
                threshold = float(item.get("threshold", 1.0) or 1.0)
            except Exception:
                threshold = 1.0
            try:
                cooldown = int(item.get("cooldown_ticks", 0) or 0)
            except Exception:
                cooldown = 0

            params = item.get("params") or {}
            if not isinstance(params, dict):
                params = {"raw": params}
            # Allow conflict keys to be declared at the top level for readability.
            # 允许把互斥 key（mutex_key/mutex_keys）写在 action_trigger 顶层（更直观），
            # 这里统一折叠到 params 中，供 ActionManager 的冲突仲裁读取。
            if "mutex_key" not in params and isinstance(item.get("mutex_key"), str) and str(item.get("mutex_key") or "").strip():
                params["mutex_key"] = str(item.get("mutex_key") or "").strip()
            if "mutex_keys" not in params and isinstance(item.get("mutex_keys"), list):
                params["mutex_keys"] = [str(x).strip() for x in (item.get("mutex_keys") or []) if str(x).strip()]

            out.append(
                {
                    "action_id": action_id,
                    "action_kind": action_kind,
                    "gain": round(max(0.0, gain), 8),
                    "threshold": float(threshold),
                    "cooldown_ticks": int(cooldown),
                    "params": dict(params),
                    "source": {
                        "kind": "iesm_action_trigger",
                        "rule_id": str(item.get("rule_id", "") or ""),
                        "rule_title": str(item.get("rule_title", "") or ""),
                        "rule_phase": str(item.get("rule_phase", "") or ""),
                        "rule_priority": int(item.get("rule_priority", item.get("rule_pri", item.get("priority", 0))) or 0),
                    },
                }
            )
        return out

    def _triggers_from_complexity(self, cfs_signals: list[dict]) -> list[dict[str, Any]]:
        complexity = self._best_global_signal_strength(cfs_signals, kind="complexity")
        if complexity <= 0.0:
            return []

        out: list[dict[str, Any]] = []
        gain_base = float(self._config.get("mode_drive_gain", 0.60))
        threshold = float(self._config.get("mode_threshold", 0.55))
        cooldown = int(self._config.get("mode_cooldown_ticks", 2) or 0)
        focus_th = float(self._config.get("focus_mode_complexity_threshold", 0.65))
        diverge_th = float(self._config.get("diverge_mode_complexity_threshold", 0.25))

        if complexity >= focus_th:
            out.append(
                {
                    "action_id": "attention_focus_mode",
                    "action_kind": "attention_focus_mode",
                    "gain": round(gain_base * complexity, 8),
                    "threshold": threshold,
                    "cooldown_ticks": cooldown,
                    "params": {"complexity": complexity},
                    "source": {"kind": "cfs_complexity", "strength": complexity},
                }
            )
        elif complexity <= diverge_th:
            # When complexity is very low ("simple"), try to diverge.
            # 复杂度很低（很“简”）时，尝试发散。
            score = self._clamp01((diverge_th - complexity) / max(1e-6, diverge_th))
            out.append(
                {
                    "action_id": "attention_diverge_mode",
                    "action_kind": "attention_diverge_mode",
                    "gain": round(gain_base * score, 8),
                    "threshold": threshold,
                    "cooldown_ticks": cooldown,
                    "params": {"complexity": complexity},
                    "source": {"kind": "cfs_complexity", "strength": score},
                }
            )
        return out

    def _triggers_from_recall(self, cfs_signals: list[dict], memory_activation_snapshot: dict) -> list[dict[str, Any]]:
        items = memory_activation_snapshot.get("items", []) or []
        if not items:
            return []

        kinds = [str(x) for x in (self._config.get("recall_trigger_kinds") or []) if str(x)]
        min_strength = float(self._config.get("recall_min_strength", 0.45))
        best = 0.0
        best_kind = ""
        best_target = {}
        for sig in cfs_signals:
            if not isinstance(sig, dict):
                continue
            if str(sig.get("scope", "")) != "object":
                continue
            kind = str(sig.get("kind", "") or "")
            if kinds and kind not in kinds:
                continue
            strength = float(sig.get("strength", 0.0) or 0.0)
            if strength > best:
                best = strength
                best_kind = kind
                best_target = dict(sig.get("target", {}) or {}) if isinstance(sig.get("target", {}), dict) else {}

        if best < min_strength:
            return []

        gain_base = float(self._config.get("recall_gain_base", 0.55))
        threshold = float(self._config.get("recall_threshold", 0.55))
        return [
            {
                "action_id": "recall_top_memory",
                "action_kind": "recall",
                "gain": round(gain_base * self._clamp01(best), 8),
                "threshold": threshold,
                "cooldown_ticks": 0,
                "params": {"trigger_kind": best_kind, "trigger_strength": best, "trigger_target": best_target},
                "source": {"kind": f"cfs_{best_kind}", "strength": best},
            }
        ]

    # ================================================================== #
    # Node ops / 节点维护                                                 #
    # ================================================================== #

    def _apply_trigger(self, trig: dict[str, Any], *, tick_number: int) -> None:
        action_id = str(trig.get("action_id", "") or "")
        if not action_id:
            return
        action_kind = str(trig.get("action_kind", "") or "")
        gain = max(0.0, float(trig.get("gain", 0.0) or 0.0))
        threshold = float(trig.get("threshold", 1.0) or 1.0)
        cooldown = int(trig.get("cooldown_ticks", 0) or 0)
        drive_max = float(self._config.get("drive_max", 3.0))

        node = self._nodes.get(action_id)
        if node is None:
            if len(self._nodes) >= int(self._config.get("max_action_nodes", 64) or 64):
                return
            node = {
                "action_id": action_id,
                "action_kind": action_kind,
                "drive": 0.0,
                "threshold": float(threshold),
                "cooldown_ticks": cooldown,
                "params": {},
                "trigger_sources": [],
                "created_at": int(time.time() * 1000),
                "last_update_tick": tick_number,
                "last_trigger_tick": -999999,
            }
            self._nodes[action_id] = node

        node["action_kind"] = action_kind or node.get("action_kind", "")
        # Store baseline threshold on node; effective threshold is computed per tick.
        # 节点内存放“基准阈值”，实时阈值在每 tick 根据调制再计算。
        node["threshold"] = float(threshold)
        node["cooldown_ticks"] = cooldown
        node["params"] = dict(trig.get("params", {}) or {})
        node["drive"] = max(0.0, min(drive_max, float(node.get("drive", 0.0)) + gain))
        node["last_update_tick"] = tick_number
        node.setdefault("trigger_sources", []).append(trig.get("source", {}))
        # Per-tick trigger summary (for observability).
        # 本 tick 触发源摘要（用于观测“被动/主动原因”）。
        node["tick_gain_total"] = float(node.get("tick_gain_total", 0.0) or 0.0) + float(gain)
        sk = str((trig.get("source", {}) or {}).get("kind", "") or "unknown")
        by = node.get("tick_gain_by_source_kind", {}) if isinstance(node.get("tick_gain_by_source_kind", {}), dict) else {}
        by[sk] = round(float(by.get(sk, 0.0) or 0.0) + float(gain), 8)
        node["tick_gain_by_source_kind"] = by
        node.setdefault("tick_sources", []).append(trig.get("source", {}) or {})
        # Trim sources to keep memory bounded.
        if len(node["trigger_sources"]) > 12:
            node["trigger_sources"] = node["trigger_sources"][-12:]

    def _should_execute(self, node: dict, *, tick_number: int) -> bool:
        # Stop/hold gate: when a node is explicitly stopped, prevent execution for a while.
        # 停止门控：当节点被显式停止后，在 stop_until_tick 之前禁止执行。
        try:
            stop_until = int(node.get("stop_until_tick", -1) or -1)
        except Exception:
            stop_until = -1
        if stop_until >= 0 and tick_number <= stop_until:
            return False
        drive = float(node.get("drive", 0.0) or 0.0)
        threshold = float(node.get("effective_threshold", node.get("threshold", 1.0) or 1.0) or 1.0)
        if drive < threshold:
            return False
        cooldown = int(node.get("cooldown_ticks", 0) or 0)
        if cooldown <= 0:
            return True
        last = int(node.get("last_trigger_tick", -999999) or -999999)
        return (tick_number - last) > cooldown

    def _compute_effective_threshold(self, *, node: dict[str, Any], emotion_state: dict) -> dict[str, Any]:
        """
        计算“实时阈值”（effective_threshold）。

        对齐理论核心 4.2.1.4：
          - 先天基准阈值（base_threshold）
          - 情绪调制因子（这里用 NT 线性缩放做 MVP）
          - 行动近因/疲劳（这里用模块内 fatigue 做 MVP）

        返回：
          {
            base_threshold,
            threshold_scale,
            effective_threshold,
            components: {nt_scale, fatigue_scale, nt_snapshot, ...}
          }
        """
        base_threshold = float(node.get("threshold", 1.0) or 1.0)
        # 1) NT scaling
        nt = {}
        # emotion_state 来自 EmotionManager.update_emotion_state 的 data 字段
        # 常见字段：nt_state_after / nt_state_snapshot / modulation
        if isinstance(emotion_state.get("nt_state_after"), dict):
            nt = dict(emotion_state.get("nt_state_after") or {})
        elif isinstance(emotion_state.get("nt_state_before"), dict):
            nt = dict(emotion_state.get("nt_state_before") or {})
        elif isinstance(emotion_state.get("nt_state_snapshot"), dict):
            channels = (emotion_state.get("nt_state_snapshot", {}) or {}).get("channels", {})
            if isinstance(channels, dict):
                nt = {k: (v.get("value") if isinstance(v, dict) else v) for k, v in channels.items()}

        nt_scale_map = self._config.get("threshold_scale_by_nt", {}) or {}
        nt_scale = 1.0
        if isinstance(nt_scale_map, dict) and isinstance(nt, dict):
            for ch, coef in nt_scale_map.items():
                try:
                    nt_scale += float(nt.get(ch, 0.0) or 0.0) * float(coef or 0.0)
                except Exception:
                    continue

        zmin = float(self._config.get("threshold_scale_min", 0.55) or 0.55)
        zmax = float(self._config.get("threshold_scale_max", 1.75) or 1.75)
        nt_scale_clamped = max(zmin, min(zmax, float(nt_scale)))

        # 2) Local action fatigue scaling
        fatigue = float(node.get("fatigue", 0.0) or 0.0)
        fatigue_gain = float(self._config.get("action_fatigue_threshold_gain", 0.55) or 0.55)
        fatigue_scale = 1.0 + max(0.0, min(1.0, fatigue)) * max(0.0, fatigue_gain)

        threshold_scale = float(nt_scale_clamped) * float(fatigue_scale)
        effective = base_threshold * threshold_scale
        return {
            "base_threshold": float(base_threshold),
            "threshold_scale": float(threshold_scale),
            "effective_threshold": float(effective),
            "components": {
                "nt_scale_raw": float(nt_scale),
                "nt_scale_clamped": float(nt_scale_clamped),
                "fatigue_scale": float(fatigue_scale),
                "fatigue": float(fatigue),
                "nt_snapshot": {str(k): float(v or 0.0) for k, v in (nt or {}).items() if str(k)},
            },
        }

    def _prune_idle_nodes(self, *, tick_number: int) -> None:
        idle = int(self._config.get("node_idle_prune_ticks", 18) or 0)
        if idle <= 0:
            return
        to_delete = []
        for action_id, node in self._nodes.items():
            last_update = int(node.get("last_update_tick", tick_number) or tick_number)
            last_trigger = int(node.get("last_trigger_tick", -999999) or -999999)
            if (tick_number - max(last_update, last_trigger)) > idle:
                to_delete.append(action_id)
        for action_id in to_delete:
            self._nodes.pop(action_id, None)

    # ================================================================== #
    # Builders / 构造器                                                   #
    # ================================================================== #

    @staticmethod
    def _focus_action_id(directive: dict) -> str:
        ref_id = str(directive.get("target_ref_object_id", "") or "")
        ref_type = str(directive.get("target_ref_object_type", "") or "")
        item_id = str(directive.get("target_item_id", "") or "")
        if ref_id:
            return f"focus::{ref_type or 'ref'}::{ref_id}"
        if item_id:
            return f"focus::item::{item_id}"
        return ""

    def _build_recall_focus_directive(
        self,
        *,
        node: dict,
        tick_id: str,
        tick_index: int,
        now_ms: int,
        memory_activation_snapshot: dict,
    ) -> dict | None:
        """
        从记忆赋能池里挑一个目标，生成 focus directive。
        注意：这只是 MVP 的“回忆行动”落地方式，后续可扩展为更复杂的记忆检索与计划。
        """
        params = (node.get("params", {}) or {}) if isinstance(node.get("params", {}), dict) else {}

        trigger_kind = str(params.get("trigger_kind", params.get("kind", "")) or "").strip()
        trigger_target_ref = str(params.get("trigger_target_ref", params.get("trigger_target", "")) or "").strip()
        anchor_ref_object_type = ""
        anchor_ref_object_id = ""
        if ":" in trigger_target_ref:
            anchor_ref_object_type, anchor_ref_object_id = [x.strip() for x in trigger_target_ref.split(":", 1)]
        else:
            anchor_ref_object_id = trigger_target_ref

        time_basis = str(params.get("time_basis", params.get("time_base", "wallclock")) or "wallclock").strip().lower() or "wallclock"
        if time_basis in {"tick", "ticks"}:
            time_basis = "tick"
        else:
            time_basis = "wallclock"

        target_ts_ms: int | None = None
        target_tick_index: int | None = None
        try:
            if time_basis == "tick":
                iv = None
                if "target_interval_ticks" in params and params.get("target_interval_ticks") not in (None, "", "null"):
                    iv = float(params.get("target_interval_ticks"))
                elif "target_interval_sec" in params and params.get("target_interval_sec") not in (None, "", "null"):
                    iv = float(params.get("target_interval_sec"))
                if iv is not None and float(iv) > 0:
                    iv_int = max(1, int(round(float(iv))))
                    target_tick_index = int(tick_index) - int(iv_int)
            else:
                if "target_interval_sec" in params and params.get("target_interval_sec") not in (None, "", "null"):
                    iv = float(params.get("target_interval_sec"))
                    if iv > 0:
                        target_ts_ms = int(now_ms - iv * 1000.0)
                elif str(params.get("time_bucket_ref_object_id", "") or "").strip():
                    center = self._parse_time_bucket_center_sec(str(params.get("time_bucket_ref_object_id", "") or ""))
                    if center is not None and center > 0:
                        target_ts_ms = int(now_ms - float(center) * 1000.0)
        except Exception:
            target_ts_ms = None
            target_tick_index = None

        require_anchor = bool(trigger_kind == "time_feeling" and anchor_ref_object_type == "st" and anchor_ref_object_id.startswith("st_"))
        item = self._pick_memory_activation_item(
            memory_activation_snapshot,
            now_ms=int(now_ms),
            current_tick_index=int(tick_index),
            target_ts_ms=target_ts_ms,
            target_tick_index=target_tick_index,
            anchor_ref_object_id=anchor_ref_object_id if require_anchor else "",
            require_anchor=bool(require_anchor),
        )
        if not item:
            return None

        # Prefer first structure ref.
        # 优先聚焦到该记忆关联的第一个结构引用。
        structure_id = ""
        display = ""
        refs = list(item.get("structure_ref_items", []) or [])
        if refs:
            structure_id = str(refs[0].get("structure_id", "") or "")
            display = str(refs[0].get("display_text", "") or "")
        if not structure_id:
            structure_refs = list(item.get("structure_refs", []) or [])
            if structure_refs:
                structure_id = str(structure_refs[0])
        if not structure_id:
            return None

        boost = float(self._config.get("recall_focus_boost", 0.65))
        ttl = int(self._config.get("recall_ttl_ticks", 2) or 2)
        strength = self._clamp01(float(params.get("trigger_strength", 0.6) or 0.6))

        reasons = [f"tick:{tick_id}", "action:recall"]
        if target_ts_ms is not None:
            reasons.append(f"bias:time_target_ts:{target_ts_ms}")
        if target_tick_index is not None:
            reasons.append(f"bias:time_target_tick:{target_tick_index}")
        if str(params.get("time_bucket_ref_object_id", "") or ""):
            reasons.append(f"time_bucket:{str(params.get('time_bucket_ref_object_id', '') or '')}")
        if require_anchor and anchor_ref_object_id:
            reasons.append(f"anchor:{anchor_ref_object_id}")

        return {
            "directive_id": f"recall_focus_{structure_id}_{now_ms}",
            "directive_type": "attention_focus",
            "source_kind": "recall",
            "strength": round(strength, 6),
            "focus_boost": round(max(0.0, boost), 6),
            "ttl_ticks": int(max(1, ttl)),
            "target_ref_object_id": structure_id,
            "target_ref_object_type": "st",
            "target_item_id": "",
            "target_display": display or structure_id,
            "created_at": int(now_ms),
            "reasons": reasons,
        }

    def _build_recall_request(
        self,
        *,
        node: dict,
        tick_id: str,
        tick_index: int,
        now_ms: int,
        drive_before: float,
        effective_threshold: float,
        memory_activation_snapshot: dict,
    ) -> dict[str, Any]:
        """
        Build a structured recall request for the upper layer (Observatory).
        构造“回忆请求”（结构化数据），交由上层（观测台/主流程）执行副作用。

        为什么需要这个请求？
        - 对齐理论核心 4.2.7.2：回忆行动执行后应把检索到的记忆赋能进入 MAP（记忆赋能池），并走默认记忆反哺回到 SP（状态池）。
        - 但 ActionManager 自身不应直接操作 HDB/SP（可审计 + 可测试 + 解耦）。
        - 因此这里输出一个“可执行请求”，上层收到后：
            1) 调用 HDB.apply_memory_activation_targets() 把记忆写入 MAP
            2) 调用默认 memory_feedback 把记忆内容投影回 SP

        返回字段说明（MVP）：
          - map_targets: 直接可喂给 HDB.apply_memory_activation_targets 的 targets 列表
          - selected_memory: 本次挑选的 MAP 条目摘要（用于观测）
          - target_interval_sec/target_ts_ms: 时间桶偏置（若有）
          - source: 来自 IESM 规则的最相关来源（rule_id/rule_title 等）
        """
        params = (node.get("params", {}) or {}) if isinstance(node.get("params", {}), dict) else {}
        memory_activation_snapshot = memory_activation_snapshot or {}

        trigger_kind = str(params.get("trigger_kind", params.get("kind", "")) or "").strip()
        trigger_target_ref = str(params.get("trigger_target_ref", params.get("trigger_target", "")) or "").strip()
        anchor_ref_object_type = ""
        anchor_ref_object_id = ""
        if ":" in trigger_target_ref:
            anchor_ref_object_type, anchor_ref_object_id = [x.strip() for x in trigger_target_ref.split(":", 1)]
        else:
            anchor_ref_object_id = trigger_target_ref

        # ---- 1) Resolve time target (optional) / 解析时间目标（可选） ----
        time_basis = str(params.get("time_basis", params.get("time_base", "wallclock")) or "wallclock").strip().lower() or "wallclock"
        if time_basis in {"tick", "ticks"}:
            time_basis = "tick"
        else:
            time_basis = "wallclock"

        time_bucket_ref_object_id = str(params.get("time_bucket_ref_object_id", "") or "").strip()
        target_interval_sec: float | None = None
        target_ts_ms: int | None = None
        target_interval_ticks: float | None = None
        target_tick_index: int | None = None
        try:
            if time_basis == "tick":
                # Tick-based recall: use target_interval_ticks if provided.
                if "target_interval_ticks" in params and params.get("target_interval_ticks") not in (None, "", "null"):
                    iv = float(params.get("target_interval_ticks"))
                    if iv > 0:
                        target_interval_ticks = float(iv)
                # Best-effort fallback: allow reusing target_interval_sec field as ticks (caller may pass only one).
                if target_interval_ticks is None and "target_interval_sec" in params and params.get("target_interval_sec") not in (None, "", "null"):
                    iv = float(params.get("target_interval_sec"))
                    if iv > 0:
                        target_interval_ticks = float(iv)
                if target_interval_ticks is not None and float(target_interval_ticks) > 0:
                    # Clamp: at least 1 tick back.
                    iv_int = max(1, int(round(float(target_interval_ticks))))
                    target_interval_ticks = float(iv_int)
                    target_tick_index = int(tick_index) - int(iv_int)
            else:
                if "target_interval_sec" in params and params.get("target_interval_sec") not in (None, "", "null"):
                    iv = float(params.get("target_interval_sec"))
                    if iv > 0:
                        target_interval_sec = float(iv)
                        target_ts_ms = int(now_ms - target_interval_sec * 1000.0)
                elif time_bucket_ref_object_id:
                    center = self._parse_time_bucket_center_sec(time_bucket_ref_object_id)
                    if center is not None and float(center) > 0:
                        target_interval_sec = float(center)
                        target_ts_ms = int(now_ms - float(center) * 1000.0)
        except Exception:
            target_interval_sec = None
            target_ts_ms = None
            target_interval_ticks = None
            target_tick_index = None

        # ---- 2) Pick a memory candidate (competition) / 从 MAP（记忆赋能池）挑一个候选（竞争） ----
        #
        # Theory alignment (4.2.7):
        # - Parameterized recall from time-feeling attributes should only consider memories that contain the anchor.
        # 对齐理论（4.2.7）：
        # - 由“时间感受属性”触发的带参回忆：候选必须包含该时间感受绑定的锚点。
        require_anchor = bool(trigger_kind == "time_feeling" and anchor_ref_object_type == "st" and anchor_ref_object_id.startswith("st_"))
        picked = self._pick_memory_activation_item(
            memory_activation_snapshot,
            now_ms=int(now_ms),
            current_tick_index=int(tick_index),
            target_ts_ms=target_ts_ms,
            target_tick_index=target_tick_index,
            anchor_ref_object_id=anchor_ref_object_id if require_anchor else "",
            require_anchor=bool(require_anchor),
        )
        memory_id = str((picked or {}).get("memory_id", (picked or {}).get("id", "")) or "").strip()
        display_text = str((picked or {}).get("display_text", "") or (picked or {}).get("event_summary", "") or "") or memory_id
        try:
            # Prefer episodic memory timestamp if present in MAP snapshot.
            # 优先使用“记忆本身时间戳”（对齐理论 4.2.7.2 的按时间定位）。
            created_at = int((picked or {}).get("memory_created_at", (picked or {}).get("created_at", 0)) or 0)
        except Exception:
            created_at = 0
        try:
            memory_tick_index = int((picked or {}).get("memory_tick_index", 0) or 0)
        except Exception:
            memory_tick_index = 0
        try:
            total_energy = float((picked or {}).get("total_energy", 0.0) or 0.0)
        except Exception:
            total_energy = 0.0

        # Try best-effort structure_id for observability / 尝试提取结构 id（便于观测）
        structure_id = ""
        try:
            refs = list((picked or {}).get("structure_ref_items", []) or [])
            if refs:
                structure_id = str((refs[0] or {}).get("structure_id", "") or "")
            if not structure_id:
                srefs = list((picked or {}).get("structure_refs", []) or [])
                if srefs:
                    structure_id = str(srefs[0] or "")
        except Exception:
            structure_id = ""

        # ---- 3) Map Drive consumption to MAP activation delta (MVP) / Drive->MAP 赋能映射（MVP） ----
        # 说明：Drive 单位与 ER/EV 不同，这里只是工程闭环映射，后续可脚本化升级。
        strength = self._clamp01(float(params.get("trigger_strength", 0.6) or 0.6))
        per_th = float(self._config.get("recall_map_delta_ev_per_threshold", 0.90) or 0.90)
        min_ev = float(self._config.get("recall_map_delta_ev_min", 0.08) or 0.08)
        max_ev = float(self._config.get("recall_map_delta_ev_max", 0.85) or 0.85)
        raw_ev = max(0.0, float(effective_threshold)) * max(0.0, per_th) * max(0.25, float(strength))
        # clamp + round
        delta_ev = 0.0
        if raw_ev > 0.0:
            delta_ev = float(max(min_ev, min(max_ev, raw_ev)))
        delta_ev = round(float(delta_ev), 8)

        mode_tag = str(self._config.get("recall_map_mode_tag", "recall_action") or "recall_action").strip() or "recall_action"

        map_targets: list[dict[str, Any]] = []
        if memory_id and delta_ev > 0.0:
            # Register short-term fatigue for diversification (best-effort).
            # 记忆疲劳（短期）：记录本次选择，便于后续多次回忆命中更多不同记忆。
            self._recall_memory_last_picked_tick[str(memory_id)] = int(tick_index)
            map_targets.append(
                {
                    "projection_kind": "memory",
                    "memory_id": memory_id,
                    "backing_structure_id": structure_id,
                    "target_display_text": display_text or memory_id,
                    # Recall is mostly “virtual” activation in our current semantics.
                    # 原型语义：回忆主要是“虚能量（EV）赋能”，让记忆作为候选回到 SP。
                    "delta_er": 0.0,
                    "delta_ev": float(delta_ev),
                    "sources": [structure_id] if structure_id else [],
                    "modes": [mode_tag],
                }
            )

        # ---- 4) Best-effort rule source info for UI / 尽力提供规则来源信息（用于前端显示“因为什么规则”） ----
        best_src: dict[str, Any] = {}
        try:
            srcs = [s for s in (node.get("tick_sources", []) or []) if isinstance(s, dict)]
            # Prefer IESM sources that contain rule_id; pick the highest rule_priority.
            scored = []
            for s in srcs:
                try:
                    pri = int(s.get("rule_priority", s.get("priority", 0)) or 0)
                except Exception:
                    pri = 0
                rid = str(s.get("rule_id", "") or "").strip()
                kind = str(s.get("kind", "") or "").strip()
                # IESM sources first; then any kind; then stable.
                is_iesm = 1 if kind.startswith("iesm_") else 0
                scored.append((is_iesm, pri, rid, s))
            if scored:
                scored.sort(key=lambda x: (x[0], x[1], x[2]), reverse=True)
                best_src = dict(scored[0][3] or {})
        except Exception:
            best_src = {}

        return {
            "request_id": f"recall_req_{str(node.get('action_id','recall'))}_{tick_id}_{now_ms}",
            "request_kind": "recall_request",
            "created_at": int(now_ms),
            "tick_id": str(tick_id or ""),
            "action_id": str(node.get("action_id", "") or ""),
            "action_kind": "recall",
            "drive_before": round(float(drive_before), 8),
            "effective_threshold": round(float(effective_threshold), 8),
            "trigger_strength": round(float(strength), 6),
            "time_bucket_ref_object_id": time_bucket_ref_object_id,
            "target_interval_sec": None if target_interval_sec is None else round(float(target_interval_sec), 6),
            "target_ts_ms": None if target_ts_ms is None else int(target_ts_ms),
            "target_interval_ticks": None if target_interval_ticks is None else round(float(target_interval_ticks), 6),
            "target_tick_index": None if target_tick_index is None else int(target_tick_index),
            "anchor_ref_object_type": anchor_ref_object_type,
            "anchor_ref_object_id": anchor_ref_object_id,
            "selected_memory": {
                "memory_id": memory_id,
                "display_text": display_text,
                "created_at": int(created_at),
                "tick_id": str((picked or {}).get("memory_tick_id", "") or (picked or {}).get("tick_id", "") or ""),
                "tick_index": int(memory_tick_index),
                "total_energy": round(float(total_energy), 8),
                "structure_id": structure_id,
            },
            "map_targets": map_targets,
            "source": {
                "kind": str(best_src.get("kind", "") or ""),
                "rule_id": str(best_src.get("rule_id", "") or ""),
                "rule_title": str(best_src.get("rule_title", "") or ""),
                "rule_phase": str(best_src.get("rule_phase", "") or ""),
                "rule_priority": int(best_src.get("rule_priority", 0) or 0),
            },
            # Keep a small tail for audit; UI can choose to show/hide.
            # 保留少量来源尾巴用于审计；前端可选择折叠显示。
            "tick_sources": [dict(s) for s in (node.get("tick_sources", []) or []) if isinstance(s, dict)][:8],
        }

    def _pick_memory_activation_item(
        self,
        snapshot: dict,
        *,
        now_ms: int,
        current_tick_index: int,
        target_ts_ms: int | None = None,
        target_tick_index: int | None = None,
        anchor_ref_object_id: str = "",
        require_anchor: bool = False,
    ) -> dict | None:
        """
        Pick ONE recall candidate from MAP (memory_activation_snapshot).
        从 MAP（记忆赋能池）里挑 1 条回忆候选（候选竞争，返回 1 条）。

        Theory alignment / 对齐理论核心 4.2.7（带参回忆口径）：
          - 若 require_anchor=True：候选记忆必须包含 anchor_ref_object_id（通常是 st_* 锚点）。
          - 候选之间需要竞争：综合 “目标时间接近度 + 记忆新近度 + 能量” 评分。
          - 短期疲劳：最近被选中过的记忆会被惩罚，促进多次回忆命中不同记忆，避免对大量记忆赋能。
        """
        items = snapshot.get("items", []) or []
        rows = [it for it in items if isinstance(it, dict)]
        if not rows:
            return None

        anchor_id = str(anchor_ref_object_id or "").strip()
        require_anchor = bool(require_anchor and anchor_id)

        def _has_anchor(it: dict) -> bool:
            if not anchor_id:
                return False
            # Prefer explicit structure_refs (episodic enriched fields).
            try:
                srefs = list(it.get("structure_refs", []) or [])
            except Exception:
                srefs = []
            if anchor_id in [str(x) for x in srefs if str(x)]:
                return True
            # Optional richer refs (if present).
            try:
                sitems = list(it.get("structure_ref_items", []) or [])
            except Exception:
                sitems = []
            for si in sitems:
                if not isinstance(si, dict):
                    continue
                if str(si.get("structure_id", "") or "") == anchor_id:
                    return True
                if str(si.get("ref_object_id", "") or "") == anchor_id and str(si.get("ref_object_type", "") or "").lower() == "st":
                    return True
            return False

        if require_anchor:
            rows = [it for it in rows if _has_anchor(it)]
            if not rows:
                return None

        # Config knobs
        try:
            recency_scale_sec = float(self._config.get("recall_recency_scale_sec", 30.0) or 30.0)
        except Exception:
            recency_scale_sec = 30.0
        recency_scale_sec = max(0.1, min(3600.0, float(recency_scale_sec)))

        try:
            fatigue_window_ticks = int(self._config.get("recall_memory_fatigue_window_ticks", 4) or 4)
        except Exception:
            fatigue_window_ticks = 4
        fatigue_window_ticks = max(0, min(10_000, int(fatigue_window_ticks)))

        try:
            fatigue_penalty = float(self._config.get("recall_memory_fatigue_penalty", 0.60) or 0.60)
        except Exception:
            fatigue_penalty = 0.60
        fatigue_penalty = max(0.0, min(0.95, float(fatigue_penalty)))

        def _parse_int_suffix(text: str) -> int:
            s = str(text or "")
            if not s:
                return 0
            digits = ""
            for ch in reversed(s):
                if ch.isdigit():
                    digits = ch + digits
                elif digits:
                    break
            try:
                return int(digits) if digits else 0
            except Exception:
                return 0

        def _get_memory_id(it: dict) -> str:
            return str(it.get("memory_id", it.get("id", "")) or "").strip()

        def _get_memory_created_at(it: dict) -> int:
            try:
                v = int(it.get("memory_created_at", it.get("created_at", 0)) or 0)
            except Exception:
                v = 0
            return int(v)

        def _get_memory_fresh_at(it: dict) -> int:
            """A 'freshness' timestamp used for recency bias.

            Prefer MAP update time when present, because it reflects recent activations/re-contacts.
            优先使用 MAP 的更新时间（last_updated_at），因为它更贴近“最近被重新接触/被激活”的语义。
            """
            mem_ts = _get_memory_created_at(it)
            try:
                upd_ts = int(it.get("last_updated_at", 0) or 0)
            except Exception:
                upd_ts = 0
            return int(max(int(mem_ts), int(upd_ts)))

        def _get_memory_tick_index(it: dict) -> int:
            try:
                v = int(it.get("memory_tick_index", 0) or 0)
            except Exception:
                v = 0
            if v > 0:
                return int(v)
            tid = str(it.get("memory_tick_id", it.get("tick_id", "")) or "")
            return int(_parse_int_suffix(tid))

        def _target_score(it: dict) -> float:
            # Tick target has higher precedence when provided.
            if target_tick_index is not None:
                mem_tick = _get_memory_tick_index(it)
                if mem_tick > 0:
                    dist = abs(int(mem_tick) - int(target_tick_index))
                    interval = abs(int(current_tick_index) - int(target_tick_index))
                    interval = max(1, int(interval))
                    ratio = float(dist) / float(interval)
                    return float(math.exp(-ratio))
                return 0.0

            if target_ts_ms is not None:
                mem_ts = _get_memory_created_at(it)
                if mem_ts > 0:
                    dist_sec = abs(int(mem_ts) - int(target_ts_ms)) / 1000.0
                    interval_sec = abs(int(now_ms) - int(target_ts_ms)) / 1000.0
                    interval_sec = max(1.0, float(interval_sec))
                    ratio = float(dist_sec) / float(interval_sec)
                    return float(math.exp(-ratio))
                return 0.0

            return 0.0

        def _recency_score(it: dict) -> float:
            fresh_ts = _get_memory_fresh_at(it)
            if fresh_ts <= 0:
                return 0.0
            age_sec = max(0.0, float(int(now_ms) - int(fresh_ts)) / 1000.0)
            return float(math.exp(-age_sec / float(recency_scale_sec)))

        def _energy_term(it: dict) -> float:
            try:
                e = float(it.get("total_energy", 0.0) or 0.0)
            except Exception:
                e = 0.0
            e = max(0.0, float(e))
            # log1p keeps this term bounded and less dominant than time/recency.
            return float(math.log1p(e))

        best: dict | None = None
        best_key: tuple[float, float, float, int, str] | None = None

        for it in rows:
            memory_id = _get_memory_id(it)
            if not memory_id:
                continue

            tscore = _target_score(it)
            rscore = _recency_score(it)
            eterm = _energy_term(it)

            # Weighted score (MVP): time target > recency > energy.
            score = 1.20 * float(tscore) + 0.55 * float(rscore) + 0.15 * float(eterm)

            # Short-term fatigue (diversification): penalize recently picked memories.
            if fatigue_window_ticks > 0:
                last_picked = self._recall_memory_last_picked_tick.get(str(memory_id))
                if last_picked is not None:
                    try:
                        if int(current_tick_index) - int(last_picked) <= int(fatigue_window_ticks):
                            score *= max(0.0, 1.0 - float(fatigue_penalty))
                    except Exception:
                        pass

            mem_created_at = _get_memory_created_at(it)
            try:
                te = float(it.get("total_energy", 0.0) or 0.0)
            except Exception:
                te = 0.0

            # Sort key: score desc, then energy desc, then recency desc.
            key = (float(score), float(te), float(rscore), int(mem_created_at), str(memory_id))
            if best_key is None or key > best_key:
                best_key = key
                best = it

        return best

    @staticmethod
    def _parse_time_bucket_center_sec(ref_object_id: str) -> float | None:
        """
        Parse time bucket center seconds from a StatePool time bucket ref id.
        从时间桶节点 ref_object_id 解析中心秒数（MVP 用于回忆偏置）。

        Expected examples / 期望格式示例：
          - "sa_time_bucket_0_25s" -> 0.25
          - "sa_time_bucket_37_5s" -> 37.5
          - "sa_time_bucket_86400s" -> 86400.0
        """
        s = str(ref_object_id or "").strip()
        if not s:
            return None
        # strip common prefix
        for p in ("sa_time_bucket_", "time_bucket_", "sa_time_", "tb_"):
            if s.startswith(p):
                s = s[len(p):]
                break
        # strip suffix
        if s.endswith("s"):
            s = s[:-1]
        if not s:
            return None
        # "0_25" -> "0.25"
        # NOTE: bucket ids may contain multiple "_" (e.g. "3_25"); join as a single decimal is safe for our presets.
        if "_" in s and s.count("_") >= 1:
            parts = s.split("_")
            if len(parts) == 2 and parts[0].isdigit() and parts[1].isdigit():
                s2 = f"{parts[0]}.{parts[1]}"
            else:
                # fallback: treat last "_" as decimal point, keep others
                head = "_".join(parts[:-1])
                tail = parts[-1]
                s2 = f"{head}.{tail}".replace("_", "")
            s = s2
        try:
            return float(s)
        except Exception:
            return None

    # ================================================================== #
    # Helpers / 工具函数                                                   #
    # ================================================================== #

    def _build_config(self, config_override: dict | None) -> dict:
        cfg = dict(_DEFAULT_CONFIG)
        cfg.update(_load_yaml_config(self._config_path))
        if config_override:
            cfg.update(config_override)
        return cfg

    @staticmethod
    def _merge_modulation(left: dict, right: dict) -> dict:
        if not isinstance(left, dict):
            left = {}
        if not isinstance(right, dict):
            return dict(left)
        merged = dict(left)
        for k, v in right.items():
            if isinstance(v, dict) and isinstance(merged.get(k), dict):
                merged[k] = ActionManager._merge_modulation(dict(merged.get(k)), v)
            else:
                merged[k] = v
        return merged

    @staticmethod
    def _dedup_focus_directives(items: list[dict]) -> list[dict]:
        """按 directive_id 去重（保留最后一个），再按 target 去重（保留最后一个）。"""
        by_id: dict[str, dict] = {}
        for d in items:
            if not isinstance(d, dict):
                continue
            did = str(d.get("directive_id", "") or "")
            if not did:
                continue
            by_id[did] = d
        deduped = list(by_id.values())

        by_target: dict[str, dict] = {}
        for d in deduped:
            key = str(d.get("target_ref_object_id", "") or "") or str(d.get("target_item_id", "") or "")
            if not key:
                continue
            by_target[key] = d
        return list(by_target.values())

    @staticmethod
    def _best_global_signal_strength(cfs_signals: list[dict], *, kind: str) -> float:
        best = 0.0
        for sig in cfs_signals:
            if not isinstance(sig, dict):
                continue
            if str(sig.get("scope", "")) != "global":
                continue
            if str(sig.get("kind", "")) != str(kind):
                continue
            best = max(best, float(sig.get("strength", 0.0) or 0.0))
        return best

    @staticmethod
    def _clamp01(v: float) -> float:
        try:
            x = float(v)
        except (TypeError, ValueError):
            return 0.0
        if x < 0.0:
            return 0.0
        if x > 1.0:
            return 1.0
        return x

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
        error: dict | None = None,
        trace_id: str = "",
        tick_id: str = "",
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
                "version": __version__,
                "schema_version": __schema_version__,
                "trace_id": trace_id,
                "tick_id": tick_id,
                "elapsed_ms": int(elapsed_ms),
            },
        }

    # ================================================================== #
    # reload / snapshot                                                   #
    # ================================================================== #

    def stop_actions(
        self,
        *,
        trace_id: str,
        mode: str,
        value: Any = None,
        hold_ticks: int = 2,
        reason: str = "manual_stop",
    ) -> dict:
        """
        Stop/cancel action nodes.
        行动停止/取消接口（对齐理论 4.2.1.1 中“停止/取消类行动”概念）。

        说明：
          - 原型阶段的“行动”多数是瞬时产物（focus_directive / modulation），并不存在长时间 running 进程；
            因此 stop 的主要作用是：
              1) 把某些行动节点 drive 清零（停止其意图维持）
              2) 在若干 tick 内门控其执行（stop_until_tick）
          - 未来若接入外部行动器（LLM/工具协议/机器人），stop 应被下游执行器实现为“真实取消”。

        参数：
          - mode:
              - "action_id": 按 action_id 停止
              - "action_kind": 按 action_kind（行动器类型）停止
              - "all": 停止全部行动节点
          - value: 对应 mode 的值（字符串或字符串列表）
          - hold_ticks: 停止后门控的 tick 数（默认 2）
        """
        start_time = time.time()
        mode = str(mode or "").strip().lower() or "action_id"
        hold_ticks = max(0, min(10_000, int(hold_ticks or 0)))
        tick_number = int(self._tick_counter)

        # Normalize value to a set of strings / 归一化 value 为 set[str]
        values: set[str] = set()
        if isinstance(value, str) and value.strip():
            values.add(value.strip())
        elif isinstance(value, list):
            for it in value:
                s = str(it or "").strip()
                if s:
                    values.add(s)

        stopped_ids: list[str] = []
        now_ms = int(time.time() * 1000)

        if mode not in {"action_id", "action_kind", "all"}:
            return self._make_response(
                False,
                "INPUT_VALIDATION_ERROR",
                f"Unknown stop mode: {mode}",
                data={"supported_modes": ["action_id", "action_kind", "all"]},
                trace_id=trace_id,
                tick_id=trace_id,
                elapsed_ms=self._elapsed_ms(start_time),
            )

        for action_id, node in list(self._nodes.items()):
            if not isinstance(node, dict):
                continue

            if mode == "all":
                matched = True
            elif mode == "action_kind":
                matched = (str(node.get("action_kind", "") or "").strip() in values) if values else False
            else:
                matched = (str(action_id or "").strip() in values) if values else False

            if not matched:
                continue

            node["drive"] = 0.0
            node["tick_gain_total"] = 0.0
            node["tick_gain_by_source_kind"] = {}
            node["tick_sources"] = []
            node["last_stop_tick"] = tick_number
            node["stop_until_tick"] = tick_number + int(hold_ticks)
            node["last_stop_reason"] = str(reason or "manual_stop")
            node["last_stop_at_ms"] = int(now_ms)
            stopped_ids.append(str(action_id))

        stopped_ids = sorted(list(dict.fromkeys([s for s in stopped_ids if s])))
        self._logger.brief(
            trace_id=trace_id,
            tick_id=trace_id,
            interface="stop_actions",
            success=True,
            message="行动节点已停止 / action nodes stopped",
            input_summary={"mode": mode, "value_count": len(values), "hold_ticks": hold_ticks},
            output_summary={"stopped_count": len(stopped_ids)},
        )

        return self._make_response(
            True,
            "OK",
            "行动节点已停止 / action nodes stopped",
            data={
                "mode": mode,
                "values": sorted(list(values)),
                "hold_ticks": int(hold_ticks),
                "reason": str(reason or ""),
                "tick_counter": tick_number,
                "stopped_count": len(stopped_ids),
                "stopped_action_ids": stopped_ids,
            },
            trace_id=trace_id,
            tick_id=trace_id,
            elapsed_ms=self._elapsed_ms(start_time),
        )

    def get_runtime_snapshot(self, *, trace_id: str = "action_runtime") -> dict:
        start_time = time.time()

        # Provide a detailed node snapshot for real-time observability.
        # 提供更完整的行动节点快照，便于前端“实时监控行动器/行动接口状态”。
        nodes = list(self._nodes.values())
        nodes.sort(key=lambda n: float(n.get("drive", 0.0) or 0.0), reverse=True)
        nodes_snapshot = [
            {
                "action_id": str(n.get("action_id", "") or ""),
                "action_kind": str(n.get("action_kind", "") or ""),
                "drive": round(float(n.get("drive", 0.0) or 0.0), 8),
                "base_threshold": round(float(n.get("base_threshold", n.get("threshold", 0.0) or 0.0) or 0.0), 8),
                "threshold_scale": round(float(n.get("threshold_scale", 1.0) or 1.0), 8),
                "effective_threshold": round(float(n.get("effective_threshold", n.get("threshold", 0.0) or 0.0) or 0.0), 8),
                "threshold_components": dict(n.get("threshold_components", {}) or {}) if isinstance(n.get("threshold_components", {}), dict) else {},
                "fatigue": round(float(n.get("fatigue", 0.0) or 0.0), 8),
                "cooldown_ticks": int(n.get("cooldown_ticks", 0) or 0),
                "last_attempt_tick": int(n.get("last_attempt_tick", -1) or -1),
                "last_trigger_tick": int(n.get("last_trigger_tick", -1) or -1),
                "last_update_tick": int(n.get("last_update_tick", -1) or -1),
                "tick_gain_total": round(float(n.get("tick_gain_total", 0.0) or 0.0), 8),
                "tick_gain_by_source_kind": dict(n.get("tick_gain_by_source_kind", {}) or {}) if isinstance(n.get("tick_gain_by_source_kind", {}), dict) else {},
                "trigger_sources": list(n.get("trigger_sources", []) or [])[:8],
                "tick_sources": list(n.get("tick_sources", []) or [])[:8],
                "last_stop_tick": int(n.get("last_stop_tick", -1) or -1),
                "stop_until_tick": int(n.get("stop_until_tick", -1) or -1),
                "last_stop_reason": str(n.get("last_stop_reason", "") or ""),
                "created_at": int(n.get("created_at", 0) or 0),
            }
            for n in nodes[:64]
        ]
        return self._make_response(
            True,
            "OK",
            "行动模块运行态快照 / action runtime snapshot",
            data={
                "module": __module_name__,
                "version": __version__,
                "schema_version": __schema_version__,
                "config_summary": dict(self._config),
                "executors_registry": list(self._executor_registry),
                "stats": {
                    "tick_counter": int(self._tick_counter),
                    "node_count": len(self._nodes),
                    "executed_history_count": len(self._executed_history),
                },
                "nodes": nodes_snapshot,
                # Recent executed actions (flattened) / 最近执行行动（扁平记录）
                "recent_executed_actions": list(self._executed_history)[-80:],
                "stop_interface": {
                    "supported_modes": ["action_id", "action_kind", "all"],
                    "default_hold_ticks": 2,
                },
            },
            trace_id=trace_id,
            tick_id=trace_id,
            elapsed_ms=self._elapsed_ms(start_time),
        )

    def reload_config(self, *, trace_id: str, config_path: str | None = None, apply_partial: bool = True) -> dict:
        start_time = time.time()
        path = config_path or self._config_path
        try:
            new_raw = _load_yaml_config(path)
            if not new_raw:
                return self._make_response(False, "CONFIG_ERROR", f"Config empty: {path}", trace_id=trace_id, tick_id=trace_id, elapsed_ms=self._elapsed_ms(start_time))
            applied, rejected = [], []
            for key, val in new_raw.items():
                if key not in _DEFAULT_CONFIG:
                    rejected.append({"key": key, "reason": "unknown key"})
                    continue
                expected = type(_DEFAULT_CONFIG[key])
                if isinstance(val, expected) or (expected is float and isinstance(val, (int, float))):
                    self._config[key] = val
                    applied.append(key)
                else:
                    rejected.append({"key": key, "reason": f"type mismatch expected {expected.__name__}, got {type(val).__name__}"})
            self._logger.update_config(
                log_dir=str(self._config.get("log_dir", "")),
                max_file_bytes=int(self._config.get("log_max_file_bytes", 0) or 0),
            )
            if rejected and not apply_partial:
                return self._make_response(False, "CONFIG_ERROR", "Some items rejected", data={"applied": applied, "rejected": rejected}, trace_id=trace_id, tick_id=trace_id, elapsed_ms=self._elapsed_ms(start_time))
            return self._make_response(True, "OK", "hot reload done", data={"applied": applied, "rejected": rejected}, trace_id=trace_id, tick_id=trace_id, elapsed_ms=self._elapsed_ms(start_time))
        except Exception as exc:
            self._logger.error(
                trace_id=trace_id,
                tick_id=trace_id,
                interface="reload_config",
                code="CONFIG_ERROR",
                message=str(exc),
                detail={"traceback": traceback.format_exc()},
            )
            return self._make_response(False, "CONFIG_ERROR", f"Hot reload failed: {exc}", trace_id=trace_id, tick_id=trace_id, elapsed_ms=self._elapsed_ms(start_time))
