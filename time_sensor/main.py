# -*- coding: utf-8 -*-
"""
AP 时间感受器模块（Time Sensor）— 主模块
=======================================

目标（MVP，先跑通再逐步对齐理论的完整版本）：
  1) 时间差（time delta）：当前时间戳 - 记忆时间戳
  2) 时间桶（time buckets）：用有限数量的区间覆盖连续时间尺度
  3) 双桶赋能（dual-bucket energization）：把一个时间差 t 的能量分配给最接近的两个桶
  4) 输出形式（可配置，可同时启用）：
     - bucket_nodes（时间桶节点层）：
         把“时间桶节点”写入状态池（SP=StatePool/状态池）为有限数量的稳定节点（可匹配、可门控、可触发行动）。
         对齐理论核心 4.2.6 的“基础结构表分段 + 双表赋能/匹配”。
     - bind_attribute（属性绑定层）：
         把“时间感受/时间间隔”作为运行态属性刺激元绑定到具体锚点对象上（例如记忆反哺投影的能量波峰对象），
         便于形成结构与提升可解释性（例如节奏结构示例中的【咚 + 时间间隔：约 1 秒】）。
     注意：二者并不冲突，它们应共享同一套桶体系（bucket_id），只是“表达层级”不同。
     后续由 IESM（先天编码脚本管理器）通过 metric 条件观察“时间桶节点能量/变化”并触发行动（例如 recall 回忆）。

对齐理论核心的对应章节：
  - 4.2.6 时间感受的数值刺激元设计（基础结构表分段 + 双表赋能）
  - 4.2.7 回忆行动与时间感受器（时间感受节点能量超过阈值 -> 触发回忆行动）

实现约束（结合你当前的产品验收口径）：
  - 中文优先；缩写要能在注释里找到中文全称
  - 绝不能让状态池出现“每 tick 新增成百上千对象”的噪音：
    时间桶节点数量必须是固定有限桶数，且 ref_object_id 稳定可合并。
"""

from __future__ import annotations

import os
import re
import time
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


TIME_SENSOR_DEFAULT_CONFIG: dict[str, Any] = {
    "enabled": True,
    # time_basis / 时间基准开关
    # - wallclock: 使用真实时间戳差（单位：秒）
    # - tick: 使用 tick_id 解析出的 tick_index 差（单位：tick）
    "time_basis": "wallclock",
    # tick_interval_sec / tick 的近似秒数（仅用于展示/兼容，不参与 tick 模式的计算）
    "tick_interval_sec": 1.0,
    "buckets": [
        {"id": "0_25s", "label_zh": "约 0.5 秒内", "min_sec": 0.0, "max_sec": 0.5, "center_sec": 0.25},
        {"id": "1s", "label_zh": "约 1 秒", "min_sec": 0.5, "max_sec": 1.5, "center_sec": 1.0},
        {"id": "3_25s", "label_zh": "约 3 秒", "min_sec": 1.5, "max_sec": 5.0, "center_sec": 3.25},
        {"id": "10s", "label_zh": "约 10 秒", "min_sec": 5.0, "max_sec": 15.0, "center_sec": 10.0},
        {"id": "37_5s", "label_zh": "约 30~60 秒", "min_sec": 15.0, "max_sec": 60.0, "center_sec": 37.5},
        {"id": "180s", "label_zh": "约 3 分钟", "min_sec": 60.0, "max_sec": 300.0, "center_sec": 180.0},
        {"id": "1050s", "label_zh": "约 15 分钟", "min_sec": 300.0, "max_sec": 1800.0, "center_sec": 1050.0},
        {"id": "3600s", "label_zh": "约 1 小时", "min_sec": 1800.0, "max_sec": 7200.0, "center_sec": 3600.0},
        {"id": "21600s", "label_zh": "约 6 小时", "min_sec": 7200.0, "max_sec": 86400.0, "center_sec": 21600.0},
        {"id": "86400s", "label_zh": "约 1 天", "min_sec": 86400.0, "max_sec": 172800.0, "center_sec": 86400.0},
    ],
    # tick_buckets / tick 时间桶（用于 time_basis=tick）
    # 说明：字段名沿用 *_sec 是为了减少结构变更；在 tick 模式下它们表示 “tick 单位”。
    "tick_buckets": [
        {"id": "0_5t", "label_zh": "约 1 tick 内", "min_sec": 0.0, "max_sec": 1.0, "center_sec": 0.5},
        {"id": "1_5t", "label_zh": "约 2 tick", "min_sec": 1.0, "max_sec": 2.0, "center_sec": 1.5},
        {"id": "3t", "label_zh": "约 3 tick", "min_sec": 2.0, "max_sec": 4.0, "center_sec": 3.0},
        {"id": "6t", "label_zh": "约 6 tick", "min_sec": 4.0, "max_sec": 8.0, "center_sec": 6.0},
        {"id": "12t", "label_zh": "约 12 tick", "min_sec": 8.0, "max_sec": 16.0, "center_sec": 12.0},
        {"id": "24t", "label_zh": "约 24 tick", "min_sec": 16.0, "max_sec": 32.0, "center_sec": 24.0},
        {"id": "48t", "label_zh": "约 48 tick", "min_sec": 32.0, "max_sec": 64.0, "center_sec": 48.0},
        {"id": "96t", "label_zh": "约 96 tick", "min_sec": 64.0, "max_sec": 128.0, "center_sec": 96.0},
    ],
    "source_mode": "memory_activation_snapshot",
    "memory_top_k": 16,
    "energy_gain_ratio": 0.18,
    # base_energy_source / 时间感受赋能的能量来源口径
    # - total_energy: 使用 MAP 条目的当前总能量（更“粘”，可能导致时间感受每 tick 都持续出现）
    # - last_delta_energy: 使用 MAP 条目的最近增量（更贴近“被重新接触/被赋能”的语义，且更不易形成回忆正反馈）
    "base_energy_source": "last_delta_energy",
    "energy_key": "ev",  # "ev" (虚能量) or "er" (实能量)
    "min_bucket_energy": 0.02,
    # ---- delayed energization tasks (theory 4.2.8) / 延迟赋能任务表（理论 4.2.8）----
    "enable_delayed_tasks": False,
    "delayed_task_capacity": 48,
    "delayed_task_register_min_delta_energy": 0.20,
    "delayed_task_fatigue_ticks": 2,
    "delayed_task_fatigue_ms": 800,
    "delayed_task_min_interval_sec": 0.5,
    "delayed_task_min_interval_ticks": 1,
    "delayed_task_due_tolerance_sec": 0.15,
    "delayed_task_due_tolerance_ticks": 0,
    "delayed_task_energy_key": "ev",
    "delayed_task_energy_ratio": 0.80,
    "delayed_task_energy_min": 0.06,
    "delayed_task_energy_max": 0.85,
    # enable_bucket_nodes / 是否写入“时间桶节点”（桶节点层）
    # Chinese: true 表示把固定数量的时间桶作为稳定 SA 写入状态池，并给其赋能（双桶分配）。
    # English: When true, write stable bucket nodes into StatePool and energize them (dual-bucket distribution).
    "enable_bucket_nodes": None,
    # enable_bind_attribute / 是否执行“时间感受属性绑定”（属性绑定层）
    # Chinese: true 表示把时间感受作为属性刺激元绑定到具体锚点对象上（更贴近“约束/标记”语义）。
    # English: When true, bind time-feeling as an attribute SA to peak target objects for interpretability.
    "enable_bind_attribute": None,
    # output_mode / 旧版兼容字段（deprecated）
    # - bucket_nodes: 仅写入时间桶节点
    # - bind_attribute: 仅执行属性绑定
    # - both: 二者都启用
    # 说明：当 enable_bucket_nodes/enable_bind_attribute 其中任一被显式设置为 bool 时，
    #      本字段将仅作为“默认回退”使用。
    "output_mode": "bind_attribute",
    # attribute_name / 绑定到对象上的属性名（用于 contains_text 触发、前端展示）
    "attribute_name": "时间感受",
    # max_bind_targets_per_memory / 每条记忆最多绑定到几个“能量波峰对象”
    "max_bind_targets_per_memory": 2,
    # peak_keep_ratio / 波峰保留比例（>= max_delta * ratio 的目标会被视为“同一波峰”）
    "peak_keep_ratio": 0.72,
    # max_total_bindings / 单 tick 最大绑定条数（安全刹车，防止 MAP 爆炸时刷屏）
    "max_total_bindings": 12,
    "node_id_prefix": "sa_time_bucket_",
    "node_display_prefix": "时间感受",
    # ---- logging ----
    "log_dir": "",
    "log_max_file_bytes": 5 * 1024 * 1024,
    "stdout_fallback_when_log_fail": True,
}


class TimeSensor:
    """
    时间感受器（Time Sensor）。

    说明：
      - 当前原型只负责“生成时间感受并写入状态池/绑定到对象”，不直接触发行动；
        触发回忆等行动应由 IESM（先天规则）显式描述并输出 action_trigger。
      - 未来扩展（理论 4.2.8）：延迟赋能任务表/任务式生物钟能力，可在本模块内实现。
    """

    def __init__(self, config_path: str = "", config_override: dict | None = None):
        self._config_path = config_path or os.path.join(os.path.dirname(__file__), "config", "time_sensor_config.yaml")
        self._config = self._build_config(config_override)
        self._logger = ModuleLogger(
            log_dir=str(self._config.get("log_dir", "")),
            max_file_bytes=int(self._config.get("log_max_file_bytes", 0) or 0),
            enable_stdout_fallback=bool(self._config.get("stdout_fallback_when_log_fail", True)),
        )
        self._tick_counter = 0
        self._last_tick_report: dict[str, Any] | None = None
        # Delayed energization tasks (theory 4.2.8) / 延迟赋能任务表（理论 4.2.8）
        # - key: target_item_id (anchor object id in StatePool)
        self._delayed_tasks: dict[str, dict[str, Any]] = {}
        # Short-term fatigue after execution (avoid immediate re-trigger) / 执行后短时疲劳（避免立刻重复触发）
        self._task_fatigue_until_tick: dict[str, int] = {}
        self._task_fatigue_until_ms: dict[str, int] = {}

    def close(self) -> None:
        try:
            self._logger.close()
        except Exception:
            pass

    def _build_config(self, config_override: dict | None = None) -> dict[str, Any]:
        config = dict(TIME_SENSOR_DEFAULT_CONFIG)
        config.update(_load_yaml_config(self._config_path))
        if config_override:
            config.update(config_override)
        return config

    def reload_config(self, trace_id: str = "") -> dict[str, Any]:
        start = time.time()
        self._config = self._build_config(config_override=None)
        self._logger.update_config(
            log_dir=str(self._config.get("log_dir", "")),
            max_file_bytes=int(self._config.get("log_max_file_bytes", 0) or 0),
        )
        return self._make_response(
            True,
            "OK",
            "时间感受器配置已重载 / Time sensor config reloaded",
            data={"config_path": self._config_path, "enabled": bool(self._config.get("enabled", True))},
            trace_id=trace_id,
            elapsed_ms=self._elapsed_ms(start),
        )

    def get_runtime_snapshot(self, trace_id: str = "") -> dict[str, Any]:
        return self._make_response(
            True,
            "OK",
            "时间感受器运行态快照 / Time sensor runtime snapshot",
            data={
                "module": __module_name__,
                "version": __version__,
                "schema_version": __schema_version__,
                "tick_counter": self._tick_counter,
                "config_path": self._config_path,
                "config": dict(self._config),
                "last_tick_report": self._last_tick_report or {},
            },
            trace_id=trace_id,
            elapsed_ms=0,
        )

    # ================================================================== #
    # Main tick interface                                                 #
    # ================================================================== #

    def run_time_feeling_tick(
        self,
        *,
        pool: Any,
        trace_id: str,
        tick_id: str,
        now_ms: int | None = None,
        memory_activation_snapshot: dict | None = None,
        memory_feedback_result: dict | None = None,
    ) -> dict[str, Any]:
        """
        主入口：根据“被重新接触到的记忆”生成时间感受桶，并对状态池节点赋能。

        参数：
          - pool: 状态池对象（StatePool, SP），需要提供：
              - _store.get_by_ref(ref_id) -> state_item | None
              - apply_energy_update(...)
              - insert_runtime_node(...)
          - memory_activation_snapshot: HDB 记忆赋能池（MAP）快照（items 内含 created_at/total_energy）

        返回：
          - time_feelings: 时间桶赋能详情（供观测台展示）
          - pool_events: 对状态池的写入摘要（insert/update）
        """
        start = time.time()
        self._tick_counter += 1
        now_ms = int(now_ms or (time.time() * 1000))

        if not bool(self._config.get("enabled", True)):
            out = {
                "now_ms": now_ms,
                "enabled": False,
                "bucket_updates": [],
                "pool_events": [],
                "note": "时间感受器已禁用 / disabled",
            }
            self._last_tick_report = out
            return self._make_response(True, "OK_DISABLED", "时间感受器已禁用 / disabled", data=out, trace_id=trace_id, elapsed_ms=self._elapsed_ms(start))

        # ---- Resolve time basis (wallclock vs tick) / 解析时间基准开关 ----
        time_basis = str(self._config.get("time_basis", "wallclock") or "wallclock").strip().lower() or "wallclock"
        if time_basis in {"tick", "ticks"}:
            time_basis = "tick"
        else:
            time_basis = "wallclock"

        tick_index = self._parse_tick_index(tick_id)
        if time_basis == "tick" and tick_index is None:
            # Fallback to wallclock when tick_index cannot be parsed.
            # tick_id 无法解析 tick_index 时回退到 wallclock，避免混乱输出。
            time_basis = "wallclock"

        bucket_key = "tick_buckets" if time_basis == "tick" else "buckets"
        buckets = self._normalized_buckets(bucket_key)
        if not buckets:
            self._logger.error(trace_id=trace_id, tick_id=tick_id, interface="run_time_feeling_tick", code="CONFIG_ERROR", message="时间桶 buckets 为空", detail={})
            return self._make_response(False, "CONFIG_ERROR", "时间桶 buckets 为空 / empty buckets", data={}, trace_id=trace_id, elapsed_ms=self._elapsed_ms(start))

        # ---- Step 1: execute due delayed tasks (theory 4.2.8) / 执行到期的延迟赋能任务 ----
        delayed_report = self._execute_due_delayed_tasks(
            pool=pool,
            trace_id=trace_id,
            tick_id=tick_id,
            now_ms=now_ms,
            time_basis=time_basis,
            tick_index=int(tick_index) if tick_index is not None else None,
        )

        # ---- Step 2: collect memory candidates / 收集记忆候选 ----
        memory_activation_snapshot = memory_activation_snapshot or {}
        items = list(memory_activation_snapshot.get("items", []) or [])
        items = [it for it in items if isinstance(it, dict)]
        items.sort(key=lambda x: float(x.get("total_energy", 0.0) or 0.0), reverse=True)
        top_k = int(self._config.get("memory_top_k", 16) or 16)
        top_k = max(1, min(128, top_k))
        items = items[:top_k]

        gain_ratio = float(self._config.get("energy_gain_ratio", 0.18) or 0.18)
        gain_ratio = max(0.0, min(2.0, gain_ratio))
        base_energy_source = str(self._config.get("base_energy_source", "total_energy") or "total_energy").strip().lower() or "total_energy"
        energy_key = str(self._config.get("energy_key", "ev") or "ev").strip().lower() or "ev"
        energy_key = "er" if energy_key == "er" else "ev"
        min_bucket_energy = float(self._config.get("min_bucket_energy", 0.02) or 0.02)
        min_bucket_energy = max(0.0, min(10.0, min_bucket_energy))

        def _resolve_source_energy(it: dict) -> float:
            """Pick the energy base used for time-feeling generation / 选择时间感受赋能的能量基准口径。"""
            if base_energy_source in {"last_delta_energy", "last_delta", "delta"}:
                try:
                    de = float(it.get("last_delta_er", 0.0) or 0.0) + float(it.get("last_delta_ev", 0.0) or 0.0)
                except Exception:
                    de = 0.0
                return max(0.0, float(de))
            try:
                te = float(it.get("total_energy", 0.0) or 0.0)
            except Exception:
                te = 0.0
            return max(0.0, float(te))

        # ---- Step 3: accumulate bucket energies / 汇总每个桶的能量 ----
        bucket_energy: dict[str, float] = {b["id"]: 0.0 for b in buckets}
        mem_rows: list[dict[str, Any]] = []

        for it in items:
            # Prefer the episodic memory timestamp if available.
            # 优先使用“记忆本身时间戳”（对齐理论 4.2.6.3），否则回退到 MAP 条目时间戳。
            memory_created_at = int(it.get("memory_created_at", it.get("created_at", 0)) or 0)
            map_created_at = int(it.get("created_at", 0) or 0)
            if memory_created_at <= 0 and time_basis == "wallclock":
                continue

            dt_value: float | None = None
            dt_unit = "s"
            if time_basis == "tick":
                # tick delta: current_tick_index - memory_tick_index
                dt_unit = "tick"
                cur_tick = int(tick_index or 0)
                mem_tick = int(it.get("memory_tick_index", 0) or 0)
                if mem_tick <= 0:
                    # Fallback: parse from memory_tick_id if provided.
                    mem_tick_id = str(it.get("memory_tick_id", "") or "")
                    parsed = self._parse_tick_index(mem_tick_id)
                    mem_tick = int(parsed or 0)
                if mem_tick <= 0 and memory_created_at > 0:
                    # Best-effort fallback to wallclock delta when tick data is missing.
                    # tick 信息缺失时尽力回退到 wallclock（避免完全无输出）。
                    dt_unit = "s"
                    dt_value = max(0.0, float(now_ms - memory_created_at) / 1000.0)
                else:
                    dt_value = max(0.0, float(cur_tick - mem_tick))
            else:
                dt_unit = "s"
                dt_value = max(0.0, float(now_ms - memory_created_at) / 1000.0)

            if dt_value is None:
                continue
            src_energy = _resolve_source_energy(it)
            base = max(0.0, float(src_energy)) * gain_ratio
            if base <= 0.0:
                continue

            b1, w1, b2, w2 = self._dual_bucket_weights(buckets, float(dt_value))
            if b1:
                bucket_energy[b1] = float(bucket_energy.get(b1, 0.0) or 0.0) + float(base) * float(w1)
            if b2 and b2 != b1:
                bucket_energy[b2] = float(bucket_energy.get(b2, 0.0) or 0.0) + float(base) * float(w2)

            mem_rows.append(
                {
                    "memory_id": str(it.get("memory_id", it.get("id", "")) or ""),
                    "display_text": str(it.get("display_text", "") or it.get("event_summary", "") or ""),
                    # Keep both for audit / 同时保留两套时间戳便于审计
                    "created_at": memory_created_at,
                    "memory_created_at": memory_created_at,
                    "map_created_at": map_created_at,
                    "delta_unit": dt_unit,
                    # Backward compatible field name: delta_sec (may actually be tick delta when time_basis=tick).
                    # 向后兼容字段名：delta_sec（time_basis=tick 时它表示 tick 差值）。
                    "delta_sec": round(float(dt_value), 3),
                    "delta_value": round(float(dt_value), 3),
                    "total_energy": round(float(it.get("total_energy", 0.0) or 0.0), 6),
                    "base_energy_source": base_energy_source,
                    "source_energy": round(float(src_energy), 6),
                    "time_feeling_energy": round(base, 6),
                    "bucket_1": b1,
                    "w1": round(float(w1), 4),
                    "bucket_2": b2,
                    "w2": round(float(w2), 4),
                }
            )

        # ---- Step 4: output to StatePool / 输出到状态池 ----
        # 说明（对齐理论核心 4.2.6~4.2.9）：
        # - 时间桶节点层（bucket_nodes）：有限桶 + 双桶赋能/匹配，是行动门控与脚本条件的稳定入口；
        # - 属性绑定层（bind_attribute）：把时间感受挂到具体锚点对象上，便于结构形成与可解释性；
        # 二者可同时启用，且应共享同一套桶体系（bucket_id）。
        out_flags = self._resolve_output_flags()
        enable_bucket_nodes = bool(out_flags.get("enable_bucket_nodes", False))
        enable_bind_attribute = bool(out_flags.get("enable_bind_attribute", False))
        output_mode = str(out_flags.get("output_mode", "") or "")

        node_prefix = str(self._config.get("node_id_prefix", "sa_time_bucket_") or "sa_time_bucket_")
        display_prefix = str(self._config.get("node_display_prefix", "时间感受") or "时间感受").strip() or "时间感受"
        attr_name = str(self._config.get("attribute_name", "时间感受") or "时间感受").strip() or "时间感受"

        pool_events: list[dict[str, Any]] = []
        bucket_updates: list[dict[str, Any]] = []
        attribute_bindings: list[dict[str, Any]] = []
        # Include delayed-task execution events in pool_events for unified audit.
        # 把延迟任务执行事件合并进 pool_events（统一审计口径）。
        for ev in list((delayed_report.get("pool_events", []) or [])):
            if isinstance(ev, dict):
                pool_events.append(ev)

        bucket_by_id = {str(b.get("id", "") or ""): dict(b) for b in buckets if str(b.get("id", "") or "")}

        # (A) Always emit bucket summary for observability / 无论输出模式都输出桶能量汇总（便于验收）
        for b in buckets:
            bid = str(b.get("id", "") or "")
            if not bid:
                continue
            e = float(bucket_energy.get(bid, 0.0) or 0.0)
            if e < min_bucket_energy:
                continue
            label_zh = str(b.get("label_zh", bid) or bid)
            center_sec = float(b.get("center_sec", 0.0) or 0.0)
            min_sec = float(b.get("min_sec", 0.0) or 0.0)
            max_sec = float(b.get("max_sec", 0.0) or 0.0)
            bucket_updates.append(
                {
                    "bucket_id": bid,
                    "label_zh": label_zh,
                    "center_sec": center_sec,
                    "range_sec": [min_sec, max_sec],
                    "unit": "tick" if time_basis == "tick" else "s",
                    "assigned_energy": round(e, 6),
                    "energy_key": energy_key,
                }
            )

        if enable_bucket_nodes:
            # ------------------------------------------------------------
            # Mode 1: bucket_nodes / 时间桶节点写入 SP
            # ------------------------------------------------------------
            for row in bucket_updates:
                bid = str(row.get("bucket_id", "") or "")
                e = float(row.get("assigned_energy", 0.0) or 0.0)
                if not bid or e < min_bucket_energy:
                    continue

                b = bucket_by_id.get(bid, {})
                ref_id = f"{node_prefix}{bid}"
                label_zh = str(b.get("label_zh", bid) or bid)
                center_sec = float(b.get("center_sec", 0.0) or 0.0)
                min_sec = float(b.get("min_sec", 0.0) or 0.0)
                max_sec = float(b.get("max_sec", 0.0) or 0.0)

                runtime_sa = {
                    "id": ref_id,
                    "object_type": "sa",
                    "schema_version": __schema_version__,
                    "content": {
                        "raw": f"{display_prefix}:{label_zh}",
                        "display": f"{display_prefix}：【{label_zh}】",
                        "normalized": f"{display_prefix}|{label_zh}|center_sec={center_sec}|range_sec={min_sec}~{max_sec}",
                        "value_type": "numerical",
                    },
                    "stimulus": {
                        "role": "time_feeling",
                    },
                    "meta": {
                        "ext": {
                            "time_bucket": {"id": bid, "center_sec": center_sec, "min_sec": min_sec, "max_sec": max_sec},
                        }
                    },
                    "energy": {"er": 0.0, "ev": 0.0},
                }

                delta_er = float(e) if energy_key == "er" else 0.0
                delta_ev = float(e) if energy_key == "ev" else 0.0
                runtime_sa["energy"]["er"] = round(delta_er, 8)
                runtime_sa["energy"]["ev"] = round(delta_ev, 8)

                existing = None
                try:
                    existing = pool._store.get_by_ref(ref_id)  # type: ignore[attr-defined]
                except Exception:
                    existing = None

                if existing and isinstance(existing, dict) and str(existing.get("id", "")):
                    try:
                        res = pool.apply_energy_update(  # type: ignore[attr-defined]
                            target_item_id=str(existing.get("id", "")),
                            delta_er=float(delta_er),
                            delta_ev=float(delta_ev),
                            trace_id=trace_id,
                            tick_id=tick_id,
                            reason="time_feeling_bucket_energy",
                            source_module=__module_name__,
                            allow_create_if_missing=False,
                            extra_context={"time_bucket_id": bid, "time_bucket_label_zh": label_zh, "center_sec": center_sec},
                        )
                        pool_events.append(
                            {
                                "op": "update",
                                "ref_id": ref_id,
                                "target_item_id": str(existing.get("id", "")),
                                "delta_er": round(delta_er, 6),
                                "delta_ev": round(delta_ev, 6),
                                "code": res.get("code", ""),
                            }
                        )
                    except Exception as exc:
                        pool_events.append({"op": "update", "ref_id": ref_id, "error": str(exc)})
                else:
                    try:
                        res = pool.insert_runtime_node(  # type: ignore[attr-defined]
                            runtime_object=runtime_sa,
                            trace_id=trace_id,
                            tick_id=tick_id,
                            allow_merge=True,
                            source_module=__module_name__,
                            reason="time_feeling_bucket_energy",
                        )
                        data = res.get("data", {}) if isinstance(res, dict) else {}
                        pool_events.append(
                            {
                                "op": "insert",
                                "ref_id": ref_id,
                                "delta_er": round(delta_er, 6),
                                "delta_ev": round(delta_ev, 6),
                                "inserted": bool(data.get("inserted", False)),
                                "merged": bool(data.get("merged", False)),
                                "target_item_id": str(data.get("item_id", data.get("target_item_id", "")) or ""),
                                "code": res.get("code", "") if isinstance(res, dict) else "",
                            }
                        )
                    except Exception as exc:
                        pool_events.append({"op": "insert", "ref_id": ref_id, "error": str(exc)})

                # Carry ref_id for UI when in bucket_nodes mode.
                row["ref_object_id"] = ref_id

        if enable_bind_attribute:
            # ------------------------------------------------------------
            # Mode 2: bind_attribute / 绑定到能量波峰对象
            # ------------------------------------------------------------
            # 关键点：
            # - 时间桶节点（bucket_nodes）是“有限域的数值刺激元承载层”（用于匹配与门控）；
            # - 属性绑定（bind_attribute）是“把时间感受作为约束/标记挂到具体对象上”（用于结构与解释）；
            # 二者是互补的表达层，不冲突。

            # ---- Build per-memory bucket summary / 每条记忆的桶摘要（用于属性绑定展示） ----
            # 说明：
            # - 时间感受本质是“数值刺激元”，理论要求双桶赋能/匹配；
            # - 绑定到具体锚点对象上时，为了可读性，默认展示“主桶”，并在 meta 中保留双桶信息。
            mem_time: dict[str, dict[str, Any]] = {}
            for it in items:
                memory_id = str(it.get("memory_id", it.get("id", "")) or "").strip()
                if not memory_id:
                    continue
                memory_created_at = int(it.get("memory_created_at", it.get("created_at", 0)) or 0)
                if memory_created_at <= 0:
                    continue

                dt_value: float | None = None
                dt_unit = "s"
                if time_basis == "tick":
                    dt_unit = "tick"
                    cur_tick = int(tick_index or 0)
                    mem_tick = int(it.get("memory_tick_index", 0) or 0)
                    if mem_tick <= 0:
                        mem_tick_id = str(it.get("memory_tick_id", "") or "")
                        parsed = self._parse_tick_index(mem_tick_id)
                        mem_tick = int(parsed or 0)
                    if mem_tick <= 0:
                        # Fallback to wallclock if tick is missing.
                        dt_unit = "s"
                        dt_value = max(0.0, float(now_ms - memory_created_at) / 1000.0)
                    else:
                        dt_value = max(0.0, float(cur_tick - mem_tick))
                else:
                    dt_unit = "s"
                    dt_value = max(0.0, float(now_ms - memory_created_at) / 1000.0)

                if dt_value is None:
                    continue
                src_energy = _resolve_source_energy(it)
                base = max(0.0, float(src_energy)) * gain_ratio
                if base <= 0.0:
                    continue
                b1, w1, b2, w2 = self._dual_bucket_weights(buckets, float(dt_value))

                # Determine primary/secondary bucket for display.
                # 选择“主桶/副桶”：用于展示与属性绑定，但仍保留双桶信息。
                primary_id = str(b1 or "")
                primary_w = float(w1 or 0.0)
                secondary_id = str(b2 or "")
                secondary_w = float(w2 or 0.0)
                if secondary_id and secondary_w > primary_w:
                    primary_id, secondary_id = secondary_id, primary_id
                    primary_w, secondary_w = secondary_w, primary_w
                if secondary_id == primary_id:
                    secondary_id = ""
                    secondary_w = 0.0

                pmeta = bucket_by_id.get(primary_id, {}) if primary_id else {}
                smeta = bucket_by_id.get(secondary_id, {}) if secondary_id else {}
                mem_time[memory_id] = {
                    "memory_id": memory_id,
                    "display_text": str(it.get("display_text", "") or it.get("event_summary", "") or ""),
                    "created_at": memory_created_at,
                    "delta_unit": dt_unit,
                    # Backward compatible: delta_sec may represent tick delta when time_basis=tick.
                    "delta_sec": round(float(dt_value), 3),
                    "delta_value": round(float(dt_value), 3),
                    "base_energy_source": base_energy_source,
                    "source_energy": round(float(src_energy), 6),
                    "time_feeling_energy": round(float(base), 6),
                    # Primary bucket for display / 主桶（用于展示）
                    "bucket_id": primary_id,
                    "bucket_label_zh": str(pmeta.get("label_zh", primary_id) or primary_id or ""),
                    "bucket_center_sec": float(pmeta.get("center_sec", 0.0) or 0.0),
                    "bucket_weight": round(float(primary_w), 4),
                    # Secondary bucket (kept for audit) / 副桶（用于审计/调试）
                    "bucket_secondary_id": secondary_id,
                    "bucket_secondary_label_zh": str(smeta.get("label_zh", secondary_id) or secondary_id or ""),
                    "bucket_secondary_center_sec": float(smeta.get("center_sec", 0.0) or 0.0),
                    "bucket_secondary_weight": round(float(secondary_w), 4),
                    # Raw dual-bucket result / 原始双桶结果（与 mem_rows 口径对齐）
                    "bucket_1": str(b1 or ""),
                    "w1": round(float(w1 or 0.0), 4),
                    "bucket_2": str(b2 or ""),
                    "w2": round(float(w2 or 0.0), 4),
                    "time_basis": time_basis,
                }

            # ---- Parse memory feedback result -> per-memory peak targets ----
            # 输入来自 observatory._apply_memory_feedback() 的返回结构（items 内含 events/projections）。
            fb = memory_feedback_result or {}
            fb_items = list(fb.get("items", []) or [])
            fb_items = [x for x in fb_items if isinstance(x, dict)]
            score_by_mem: dict[str, dict[str, float]] = {}

            for fbi in fb_items:
                mid = str(fbi.get("memory_id", "") or "").strip()
                if not mid:
                    continue
                kind = str(fbi.get("memory_kind", "") or "").strip()
                score_by_mem.setdefault(mid, {})

                if kind == "stimulus_packet":
                    for ev in list(fbi.get("events", []) or []):
                        if not isinstance(ev, dict):
                            continue
                        tid = str(ev.get("target_item_id", "") or "").strip()
                        if not tid:
                            continue
                        d = ev.get("delta", {}) if isinstance(ev.get("delta", {}), dict) else {}
                        de = max(0.0, float(d.get("delta_er", 0.0) or 0.0)) + max(0.0, float(d.get("delta_ev", 0.0) or 0.0))
                        if de <= 0.0:
                            continue
                        score_by_mem[mid][tid] = float(score_by_mem[mid].get(tid, 0.0) or 0.0) + float(de)

                elif kind == "structure_group":
                    for pr in list(fbi.get("projections", []) or []):
                        if not isinstance(pr, dict):
                            continue
                        tid = str(pr.get("target_item_id", "") or "").strip()
                        if not tid:
                            continue
                        de = max(0.0, float(pr.get("er", 0.0) or 0.0)) + max(0.0, float(pr.get("ev", 0.0) or 0.0))
                        if de <= 0.0:
                            continue
                        score_by_mem[mid][tid] = float(score_by_mem[mid].get(tid, 0.0) or 0.0) + float(de)

            # ---- Select peak targets (per memory) / 每条记忆选取波峰目标 ----
            max_targets = int(self._config.get("max_bind_targets_per_memory", 2) or 2)
            max_targets = max(1, min(8, max_targets))
            keep_ratio = float(self._config.get("peak_keep_ratio", 0.72) or 0.72)
            keep_ratio = max(0.0, min(1.0, keep_ratio))
            max_total = int(self._config.get("max_total_bindings", 12) or 12)
            max_total = max(1, min(64, max_total))

            candidates: list[dict[str, Any]] = []
            for mid, scores in score_by_mem.items():
                mt = mem_time.get(mid)
                if not mt:
                    continue
                pairs = [(tid, float(v or 0.0)) for tid, v in (scores or {}).items() if str(tid) and float(v or 0.0) > 0.0]
                if not pairs:
                    continue
                pairs.sort(key=lambda p: p[1], reverse=True)
                max_score = float(pairs[0][1] or 0.0)
                picked = [(tid, sc) for (tid, sc) in pairs if sc >= max_score * keep_ratio][:max_targets]
                if not picked:
                    picked = [pairs[0]]
                for tid, sc in picked:
                    candidates.append(
                        {
                            "memory_id": mid,
                            "memory_display_text": mt.get("display_text", ""),
                            "delta_unit": mt.get("delta_unit", "s"),
                            "delta_sec": mt.get("delta_sec", 0.0),
                            "delta_value": mt.get("delta_value", mt.get("delta_sec", 0.0)),
                            "time_basis": mt.get("time_basis", time_basis),
                            "bucket_id": mt.get("bucket_id", ""),
                            "bucket_label_zh": mt.get("bucket_label_zh", ""),
                            "bucket_center_sec": mt.get("bucket_center_sec", 0.0),
                            "bucket_weight": mt.get("bucket_weight", 0.0),
                            "bucket_secondary_id": mt.get("bucket_secondary_id", ""),
                            "bucket_secondary_label_zh": mt.get("bucket_secondary_label_zh", ""),
                            "bucket_secondary_center_sec": mt.get("bucket_secondary_center_sec", 0.0),
                            "bucket_secondary_weight": mt.get("bucket_secondary_weight", 0.0),
                            # Raw dual buckets (for audit) / 原始双桶结果（审计用）
                            "bucket_1": mt.get("bucket_1", ""),
                            "w1": mt.get("w1", 0.0),
                            "bucket_2": mt.get("bucket_2", ""),
                            "w2": mt.get("w2", 0.0),
                            "bucket_ref_object_id": f"{node_prefix}{str(mt.get('bucket_id', '') or '')}" if str(mt.get("bucket_id", "") or "") else "",
                            "bucket_secondary_ref_object_id": f"{node_prefix}{str(mt.get('bucket_secondary_id', '') or '')}" if str(mt.get("bucket_secondary_id", "") or "") else "",
                            "time_feeling_energy": mt.get("time_feeling_energy", 0.0),
                            "target_item_id": tid,
                            "target_delta_energy": round(float(sc), 8),
                        }
                    )

            # Reduce duplicates by target_item_id: keep strongest.
            best_by_target: dict[str, dict[str, Any]] = {}
            for c in candidates:
                tid = str(c.get("target_item_id", "") or "").strip()
                if not tid:
                    continue
                if tid not in best_by_target or float(c.get("target_delta_energy", 0.0) or 0.0) > float(best_by_target[tid].get("target_delta_energy", 0.0) or 0.0):
                    best_by_target[tid] = c
            selected = list(best_by_target.values())
            selected.sort(key=lambda r: float(r.get("target_delta_energy", 0.0) or 0.0), reverse=True)
            selected = selected[:max_total]

            # ---- Bind runtime attribute to selected targets ----
            for c in selected:
                tid = str(c.get("target_item_id", "") or "").strip()
                bid = str(c.get("bucket_id", "") or "").strip()
                if not tid or not bid:
                    continue
                label_zh = str(c.get("bucket_label_zh", bid) or bid)
                center_sec = float(c.get("bucket_center_sec", 0.0) or 0.0)
                bucket_w = float(c.get("bucket_weight", 0.0) or 0.0)
                sec_id = str(c.get("bucket_secondary_id", "") or "").strip()
                sec_label_zh = str(c.get("bucket_secondary_label_zh", sec_id) or sec_id)
                sec_center_sec = float(c.get("bucket_secondary_center_sec", 0.0) or 0.0)
                sec_w = float(c.get("bucket_secondary_weight", 0.0) or 0.0)
                primary_ref_id = str(c.get("bucket_ref_object_id", "") or "").strip()
                secondary_ref_id = str(c.get("bucket_secondary_ref_object_id", "") or "").strip()

                # Attribute SA (not inserted as standalone state item).
                # 属性 SA（不会作为独立 state_item 入池）。
                # 重要：attribute_sa.id 必须尽量稳定，否则 ext.bound_attributes 会随 bucket 变化而膨胀。
                # 因此这里使用 “按目标对象稳定”的 id（每个目标对象 1 条时间感受属性）。
                attr_id = f"sa_time_attr_{tid}"
                attribute_sa = {
                    "id": attr_id,
                    "object_type": "sa",
                    "content": {
                        "raw": f"{attr_name}:{bid}",
                        "display": f"{attr_name}：【{label_zh}】",
                        "value_type": "numerical",
                        "attribute_name": attr_name,
                        "attribute_value": center_sec,
                    },
                    "stimulus": {"role": "attribute", "modality": "internal"},
                    "energy": {"er": 0.0, "ev": 0.0},
                    "meta": {
                        "ext": {
                            "time_basis": str(c.get("time_basis", time_basis) or time_basis),
                            "time_unit": str(c.get("delta_unit", "s") or "s"),
                            "time_bucket_id": bid,
                            "time_bucket_label_zh": label_zh,
                            "time_bucket_center_sec": center_sec,
                            "time_bucket_center_value": center_sec,
                            "time_bucket_unit": "tick" if time_basis == "tick" else "s",
                            "time_bucket_weight": round(bucket_w, 6),
                            "time_bucket_ref_object_id": primary_ref_id or f"{node_prefix}{bid}",
                            "time_bucket_secondary_id": sec_id,
                            "time_bucket_secondary_label_zh": sec_label_zh,
                            "time_bucket_secondary_center_sec": sec_center_sec,
                            "time_bucket_secondary_center_value": sec_center_sec,
                            "time_bucket_secondary_weight": round(sec_w, 6),
                            "time_bucket_secondary_ref_object_id": secondary_ref_id or (f"{node_prefix}{sec_id}" if sec_id else ""),
                            "memory_id": str(c.get("memory_id", "") or ""),
                            "delta_sec": float(c.get("delta_sec", 0.0) or 0.0),
                            "delta_value": float(c.get("delta_value", c.get("delta_sec", 0.0)) or 0.0),
                            "time_feeling_energy": float(c.get("time_feeling_energy", 0.0) or 0.0),
                            # Keep raw dual-bucket result for audit / 保留原始双桶结果用于审计
                            "dual_bucket_1": str(c.get("bucket_1", "") or ""),
                            "dual_bucket_w1": float(c.get("w1", 0.0) or 0.0),
                            "dual_bucket_2": str(c.get("bucket_2", "") or ""),
                            "dual_bucket_w2": float(c.get("w2", 0.0) or 0.0),
                        }
                    },
                }

                try:
                    res = pool.bind_attribute_node_to_object(  # type: ignore[attr-defined]
                        target_item_id=tid,
                        attribute_sa=attribute_sa,
                        trace_id=trace_id,
                        tick_id=tick_id,
                        source_module=__module_name__,
                        reason="time_feeling_bind_attribute",
                    )
                    pool_events.append(
                        {
                            "op": "bind_attribute",
                            "target_item_id": tid,
                            "attribute_sa_id": attr_id,
                            "bucket_id": bid,
                            "bucket_secondary_id": sec_id,
                            "code": res.get("code", "") if isinstance(res, dict) else "",
                            "success": bool(res.get("success", False)) if isinstance(res, dict) else True,
                        }
                    )
                except Exception as exc:
                    pool_events.append({"op": "bind_attribute", "target_item_id": tid, "attribute_sa_id": attr_id, "bucket_id": bid, "bucket_secondary_id": sec_id, "error": str(exc)})

                # Best-effort target display snapshot (after binding).
                target_display = ""
                target_ref_id = ""
                target_ref_type = ""
                try:
                    target_item = pool._store.get(tid)  # type: ignore[attr-defined]
                    if isinstance(target_item, dict):
                        rs = target_item.get("ref_snapshot", {}) if isinstance(target_item.get("ref_snapshot", {}), dict) else {}
                        target_display = str(rs.get("content_display", "") or target_item.get("ref_object_id", "") or tid)
                        target_ref_id = str(target_item.get("ref_object_id", "") or "")
                        target_ref_type = str(target_item.get("ref_object_type", "") or "")
                except Exception:
                    pass

                attribute_bindings.append(
                    {
                        **dict(c),
                        "attribute_name": attr_name,
                        "attribute_sa_id": attr_id,
                        "attribute_display": attribute_sa.get("content", {}).get("display", ""),
                        "target_ref_object_id": target_ref_id,
                        "target_ref_object_type": target_ref_type,
                        "target_display": target_display,
                    }
                )

        # ---- Step 5: register delayed tasks from attribute time-feelings (theory 4.2.8) ----
        delayed_register = self._register_delayed_tasks_from_bindings(
            attribute_bindings=attribute_bindings,
            now_ms=now_ms,
            time_basis=time_basis,
            tick_index=int(tick_index) if tick_index is not None else None,
        )

        out = {
            "now_ms": now_ms,
            "enabled": True,
            "time_basis": time_basis,
            "tick_index": int(tick_index) if tick_index is not None else None,
            "source_mode": str(self._config.get("source_mode", "")),
            "enabled_bucket_nodes": bool(enable_bucket_nodes),
            "enabled_bind_attribute": bool(enable_bind_attribute),
            "output_mode": output_mode,
            "memory_used_count": len(items),
            "memory_rows": mem_rows[:24],
            "bucket_updates": bucket_updates,
            "attribute_bindings": attribute_bindings[:64],
            "pool_events": pool_events,
            "delayed_tasks": {
                **dict(delayed_report),
                "registered": dict(delayed_register),
                "table_size": len(self._delayed_tasks),
            },
        }
        self._last_tick_report = out
        self._logger.brief(
            trace_id=trace_id,
            tick_id=tick_id,
            interface="run_time_feeling_tick",
            success=True,
            message="时间感受已计算并输出到状态池 / time feelings output to StatePool",
            input_summary={"memory_item_count": len(items), "gain_ratio": gain_ratio, "energy_key": energy_key},
            output_summary={
                "output_mode": output_mode,
                "bucket_nodes": bool(enable_bucket_nodes),
                "bind_attribute": bool(enable_bind_attribute),
                "bucket_update_count": len(bucket_updates),
                "attr_bind_count": len(attribute_bindings),
                "pool_event_count": len(pool_events),
            },
        )
        return self._make_response(True, "OK", "时间感受器执行成功 / Time sensor tick OK", data=out, trace_id=trace_id, elapsed_ms=self._elapsed_ms(start))

    # ================================================================== #
    # Helpers                                                             #
    # ================================================================== #

    def _resolve_output_flags(self) -> dict[str, Any]:
        """
        Resolve effective output switches.
        解析“最终生效”的输出开关。

        Why / 为什么需要：
          - 旧版配置用 output_mode（二选一）。
          - 新版理论口径允许 bucket_nodes 与 bind_attribute 同时存在，因此需要两开关。
          - 为了兼容旧配置，我们允许：
              - 若 enable_bucket_nodes / enable_bind_attribute 任何一个被显式设为 bool，
                则以两开关为准（未显式设置的那个从 output_mode 回退）。
              - 若两者都未显式设置（None/缺失），则完全按 output_mode。
        """
        cfg = self._config or {}
        raw_bucket = cfg.get("enable_bucket_nodes", None)
        raw_bind = cfg.get("enable_bind_attribute", None)
        legacy_mode = str(cfg.get("output_mode", "bind_attribute") or "bind_attribute").strip().lower() or "bind_attribute"

        # Normalize legacy mode / 兼容旧字段
        legacy_bucket = False
        legacy_bind = True
        if legacy_mode in {"bucket_nodes", "bucket", "buckets"}:
            legacy_bucket, legacy_bind = True, False
        elif legacy_mode in {"bind_attribute", "bind", "attribute"}:
            legacy_bucket, legacy_bind = False, True
        elif legacy_mode in {"both", "all", "bucket_and_bind", "bucket+bind"}:
            legacy_bucket, legacy_bind = True, True

        enable_bucket = legacy_bucket
        enable_bind = legacy_bind

        # If user explicitly sets booleans, honor them; fallback unspecified to legacy mode.
        # 若用户显式配置了布尔值，则以其为准；未显式配置者回退到 legacy_mode。
        if isinstance(raw_bucket, bool):
            enable_bucket = raw_bucket
        if isinstance(raw_bind, bool):
            enable_bind = raw_bind

        # Effective label for UI / 输出模式标签（供前端展示）
        if enable_bucket and enable_bind:
            effective_mode = "both"
        elif enable_bucket:
            effective_mode = "bucket_nodes"
        elif enable_bind:
            effective_mode = "bind_attribute"
        else:
            effective_mode = "disabled"

        return {
            "enable_bucket_nodes": bool(enable_bucket),
            "enable_bind_attribute": bool(enable_bind),
            "output_mode": effective_mode,
            "legacy_output_mode": legacy_mode,
        }

    def _normalized_buckets(self, key: str = "buckets") -> list[dict[str, Any]]:
        raw = list(self._config.get(key, []) or [])
        if not raw and key != "buckets":
            # Fallback to wallclock buckets if tick_buckets is missing / tick_buckets 缺失时回退到秒桶
            raw = list(self._config.get("buckets", []) or [])
        out: list[dict[str, Any]] = []
        for b in raw:
            if not isinstance(b, dict):
                continue
            bid = str(b.get("id", "") or "").strip()
            if not bid:
                continue
            try:
                center = float(b.get("center_sec", 0.0) or 0.0)
            except Exception:
                center = 0.0
            out.append(
                {
                    "id": bid,
                    "label_zh": str(b.get("label_zh", bid) or bid),
                    "min_sec": float(b.get("min_sec", 0.0) or 0.0),
                    "max_sec": float(b.get("max_sec", 0.0) or 0.0),
                    "center_sec": float(center),
                }
            )
        out.sort(key=lambda x: float(x.get("center_sec", 0.0) or 0.0))
        return out

    @staticmethod
    def _parse_tick_index(tick_id: str) -> int | None:
        """Parse numeric tick_index from tick_id tail (e.g. 'cycle_0003' -> 3)."""
        s = str(tick_id or "").strip()
        if not s:
            return None
        m = re.search(r"(\d+)$", s)
        if not m:
            return None
        try:
            return int(m.group(1))
        except Exception:
            return None

    # ================================================================== #
    # Delayed Task Table (theory 4.2.8) / 延迟赋能任务表（理论 4.2.8）       #
    # ================================================================== #

    def _task_in_fatigue(
        self,
        *,
        target_item_id: str,
        now_ms: int,
        time_basis: str,
        tick_index: int | None,
    ) -> bool:
        if not target_item_id:
            return False
        if str(time_basis) == "tick":
            until = int(self._task_fatigue_until_tick.get(target_item_id, 0) or 0)
            if tick_index is None:
                return False
            return int(tick_index) < until
        until_ms = int(self._task_fatigue_until_ms.get(target_item_id, 0) or 0)
        return int(now_ms) < until_ms

    def _execute_due_delayed_tasks(
        self,
        *,
        pool: Any,
        trace_id: str,
        tick_id: str,
        now_ms: int,
        time_basis: str,
        tick_index: int | None,
    ) -> dict[str, Any]:
        """
        Execute due delayed tasks and energize their anchor targets.
        执行到期任务，并对锚点对象赋能（理论 4.2.8 的“到点再点亮”）。
        """
        enabled = bool(self._config.get("enable_delayed_tasks", False))
        if not enabled:
            return {"enabled": False, "executed_count": 0, "pool_events": [], "executed": []}

        tasks = dict(self._delayed_tasks or {})
        if not tasks:
            return {"enabled": True, "executed_count": 0, "pool_events": [], "executed": []}

        # Config
        energy_key = str(self._config.get("delayed_task_energy_key", "ev") or "ev").strip().lower() or "ev"
        energy_key = "er" if energy_key == "er" else "ev"
        ratio = float(self._config.get("delayed_task_energy_ratio", 0.80) or 0.80)
        ratio = max(0.0, min(10.0, ratio))
        e_min = float(self._config.get("delayed_task_energy_min", 0.06) or 0.06)
        e_max = float(self._config.get("delayed_task_energy_max", 0.85) or 0.85)
        e_min = max(0.0, min(e_max, e_min))
        e_max = max(e_min, e_max)
        tol_sec = float(self._config.get("delayed_task_due_tolerance_sec", 0.15) or 0.15)
        tol_sec = max(0.0, min(3600.0, tol_sec))
        tol_ticks = int(self._config.get("delayed_task_due_tolerance_ticks", 0) or 0)
        tol_ticks = max(0, min(10_000, tol_ticks))
        fatigue_ticks = int(self._config.get("delayed_task_fatigue_ticks", 2) or 2)
        fatigue_ticks = max(0, min(10_000, fatigue_ticks))
        fatigue_ms = int(self._config.get("delayed_task_fatigue_ms", 800) or 800)
        fatigue_ms = max(0, min(86_400_000, fatigue_ms))

        executed: list[dict[str, Any]] = []
        pool_events: list[dict[str, Any]] = []
        kept: dict[str, dict[str, Any]] = {}

        for target_item_id, task in tasks.items():
            tid = str(target_item_id or "").strip()
            if not tid or not isinstance(task, dict):
                continue

            # Skip if in fatigue window.
            if self._task_in_fatigue(target_item_id=tid, now_ms=now_ms, time_basis=time_basis, tick_index=tick_index):
                kept[tid] = task
                continue

            due = False
            due_reason = ""
            try:
                if str(time_basis) == "tick":
                    if tick_index is None:
                        due = False
                    else:
                        due_tick = int(task.get("due_tick", 0) or 0)
                        due = int(tick_index) >= (due_tick - tol_ticks)
                        due_reason = f"tick>=due({tick_index}>={due_tick}-tol{tol_ticks})"
                else:
                    due_at = int(task.get("due_at_ms", 0) or 0)
                    due = int(now_ms) >= int(due_at - int(tol_sec * 1000.0))
                    due_reason = f"ms>=due({now_ms}>={due_at}-tol{int(tol_sec*1000.0)})"
            except Exception:
                due = False

            if not due:
                kept[tid] = task
                continue

            # Apply energization
            try:
                weight = max(0.0, float(task.get("weight", 0.0) or 0.0))
            except Exception:
                weight = 0.0
            raw = weight * ratio
            delta_energy = 0.0
            if raw > 0.0:
                delta_energy = max(e_min, min(e_max, raw))
            delta_energy = float(round(delta_energy, 8))

            if delta_energy <= 0.0:
                # Nothing to apply; drop task silently.
                executed.append({"target_item_id": tid, "ok": False, "reason": "delta_energy_zero"})
            else:
                delta_er = float(delta_energy) if energy_key == "er" else 0.0
                delta_ev = float(delta_energy) if energy_key == "ev" else 0.0
                try:
                    res = pool.apply_energy_update(  # type: ignore[attr-defined]
                        target_item_id=tid,
                        delta_er=delta_er,
                        delta_ev=delta_ev,
                        trace_id=f"{trace_id}_time_sensor_task",
                        tick_id=tick_id,
                        reason="time_sensor_delayed_task_due",
                        source_module=__module_name__,
                    )
                    pool_events.append(
                        {
                            "op": "delayed_task_execute",
                            "target_item_id": tid,
                            "delta_er": round(delta_er, 6),
                            "delta_ev": round(delta_ev, 6),
                            "time_basis": time_basis,
                            "due_reason": due_reason,
                            "success": bool(res.get("success", False)) if isinstance(res, dict) else True,
                            "code": res.get("code", "") if isinstance(res, dict) else "",
                        }
                    )
                    executed.append(
                        {
                            "target_item_id": tid,
                            "target_display": str(task.get("target_display", "") or ""),
                            "weight": round(weight, 6),
                            "delta_energy": round(delta_energy, 6),
                            "energy_key": energy_key,
                            "time_basis": time_basis,
                            "due_reason": due_reason,
                            "ok": bool(res.get("success", False)) if isinstance(res, dict) else True,
                        }
                    )
                except Exception as exc:
                    pool_events.append({"op": "delayed_task_execute", "target_item_id": tid, "error": str(exc)})
                    executed.append({"target_item_id": tid, "ok": False, "error": str(exc)})

            # Apply post-exec fatigue window (avoid immediate re-registration/re-trigger)
            if str(time_basis) == "tick" and tick_index is not None and fatigue_ticks > 0:
                self._task_fatigue_until_tick[tid] = int(tick_index) + int(fatigue_ticks)
            elif str(time_basis) != "tick" and fatigue_ms > 0:
                self._task_fatigue_until_ms[tid] = int(now_ms) + int(fatigue_ms)

        # Keep only future tasks.
        self._delayed_tasks = kept
        return {
            "enabled": True,
            "executed_count": len([x for x in executed if isinstance(x, dict) and bool(x.get("ok", False))]),
            "executed": executed[:16],
            "pool_events": pool_events,
        }

    def _register_delayed_tasks_from_bindings(
        self,
        *,
        attribute_bindings: list[dict[str, Any]],
        now_ms: int,
        time_basis: str,
        tick_index: int | None,
    ) -> dict[str, Any]:
        """
        Register (or update) delayed tasks from time-feeling attribute bindings.
        从“时间感受属性绑定”注册/更新延迟赋能任务（理论 4.2.8）。
        """
        enabled = bool(self._config.get("enable_delayed_tasks", False))
        if not enabled:
            return {"enabled": False, "registered_count": 0, "updated_count": 0, "skipped": {}}

        capacity = int(self._config.get("delayed_task_capacity", 48) or 48)
        capacity = max(1, min(512, capacity))
        min_delta = float(self._config.get("delayed_task_register_min_delta_energy", 0.20) or 0.20)
        min_delta = max(0.0, min(100.0, min_delta))
        min_interval_sec = float(self._config.get("delayed_task_min_interval_sec", 0.5) or 0.5)
        min_interval_sec = max(0.0, min(3600.0, min_interval_sec))
        min_interval_ticks = int(self._config.get("delayed_task_min_interval_ticks", 1) or 1)
        min_interval_ticks = max(1, min(10_000, min_interval_ticks))

        registered = 0
        updated = 0
        skipped_small = 0
        skipped_fatigue = 0
        skipped_bad = 0

        for b in list(attribute_bindings or [])[:128]:
            if not isinstance(b, dict):
                continue
            target_item_id = str(b.get("target_item_id", "") or "").strip()
            if not target_item_id:
                skipped_bad += 1
                continue
            try:
                delta = float(b.get("target_delta_energy", 0.0) or 0.0)
            except Exception:
                delta = 0.0
            if delta < min_delta:
                skipped_small += 1
                continue

            # Enforce "attribute time-feeling only": bindings already satisfy this by construction.
            # 这里不再额外检查 attribute_name，避免前端字段变化导致漏注册。

            if self._task_in_fatigue(target_item_id=target_item_id, now_ms=now_ms, time_basis=time_basis, tick_index=tick_index):
                skipped_fatigue += 1
                continue

            # Interval value comes from bucket center.
            try:
                interval_value = float(b.get("bucket_center_sec", 0.0) or 0.0)
            except Exception:
                interval_value = 0.0
            if interval_value <= 0.0:
                skipped_bad += 1
                continue

            task: dict[str, Any] | None = self._delayed_tasks.get(target_item_id)
            target_display = str(b.get("target_display", "") or "")
            target_ref_object_id = str(b.get("target_ref_object_id", "") or "")
            target_ref_object_type = str(b.get("target_ref_object_type", "") or "")

            # Compute due
            due_tick: int | None = None
            due_at_ms: int | None = None
            if str(time_basis) == "tick" and tick_index is not None:
                interval_ticks = max(min_interval_ticks, int(round(float(interval_value))))
                due_tick = int(tick_index) + int(interval_ticks)
            else:
                interval_sec = max(min_interval_sec, float(interval_value))
                due_at_ms = int(now_ms) + int(round(interval_sec * 1000.0))

            if task:
                try:
                    task["weight"] = round(max(0.0, float(task.get("weight", 0.0) or 0.0)) + max(0.0, float(delta)), 8)
                except Exception:
                    task["weight"] = round(max(0.0, float(delta)), 8)
                task["updated_at"] = int(now_ms)
                task["register_count"] = int(task.get("register_count", 0) or 0) + 1
                if due_tick is not None:
                    task["due_tick"] = int(due_tick)
                if due_at_ms is not None:
                    task["due_at_ms"] = int(due_at_ms)
                # Keep display fresh
                if target_display:
                    task["target_display"] = target_display
                if target_ref_object_id:
                    task["target_ref_object_id"] = target_ref_object_id
                if target_ref_object_type:
                    task["target_ref_object_type"] = target_ref_object_type
                updated += 1
            else:
                task = {
                    "task_id": f"ts_task_{target_item_id}",
                    "target_item_id": target_item_id,
                    "target_ref_object_id": target_ref_object_id,
                    "target_ref_object_type": target_ref_object_type,
                    "target_display": target_display,
                    "time_basis": str(time_basis),
                    "interval_value": float(interval_value),
                    "weight": round(max(0.0, float(delta)), 8),
                    "created_at": int(now_ms),
                    "updated_at": int(now_ms),
                    "register_count": 1,
                }
                if due_tick is not None:
                    task["due_tick"] = int(due_tick)
                if due_at_ms is not None:
                    task["due_at_ms"] = int(due_at_ms)
                self._delayed_tasks[target_item_id] = task
                registered += 1

        pruned = self._prune_delayed_tasks(capacity=capacity)
        table_size = len(self._delayed_tasks)

        # For UI: show a stable top list (earliest due first).
        rows = list(self._delayed_tasks.values())
        if str(time_basis) == "tick":
            rows.sort(key=lambda t: (int(t.get("due_tick", 0) or 0), -float(t.get("weight", 0.0) or 0.0)))
        else:
            rows.sort(key=lambda t: (int(t.get("due_at_ms", 0) or 0), -float(t.get("weight", 0.0) or 0.0)))

        return {
            "enabled": True,
            "registered_count": registered,
            "updated_count": updated,
            "pruned_count": pruned,
            "table_size": table_size,
            "skipped": {"small_delta": skipped_small, "fatigue": skipped_fatigue, "bad": skipped_bad},
            "tasks": rows[:16],
        }

    def _prune_delayed_tasks(self, *, capacity: int) -> int:
        """
        Prune the delayed task table when exceeding capacity.
        当任务表超过容量时裁剪（对齐理论 4.2.8.2：从较旧任务中淘汰权重较低者）。
        """
        try:
            capacity = int(capacity)
        except Exception:
            capacity = 48
        capacity = max(1, min(512, capacity))
        tasks = dict(self._delayed_tasks or {})
        if len(tasks) <= capacity:
            return 0

        # Sort by updated_at asc (oldest first).
        rows = list(tasks.values())
        rows = [r for r in rows if isinstance(r, dict)]
        rows.sort(key=lambda r: int(r.get("updated_at", r.get("created_at", 0)) or 0))

        over = len(rows) - capacity
        if over <= 0:
            return 0

        # Candidate pool = oldest 1/4 (at least 1).
        cand_size = max(1, int(len(rows) / 4))
        candidates = rows[:cand_size]
        # Drop lowest weight among candidates.
        candidates.sort(key=lambda r: float(r.get("weight", 0.0) or 0.0))

        to_drop = candidates[:over]
        dropped = 0
        drop_ids = {str(r.get("target_item_id", "") or "") for r in to_drop if str(r.get("target_item_id", "") or "")}
        for tid in drop_ids:
            if tid in tasks:
                tasks.pop(tid, None)
                dropped += 1

        # If still over (candidate pool too small), drop remaining oldest by weight.
        while len(tasks) > capacity:
            rest = list(tasks.values())
            rest.sort(key=lambda r: (int(r.get("updated_at", r.get("created_at", 0)) or 0), float(r.get("weight", 0.0) or 0.0)))
            victim = rest[0] if rest else None
            if not isinstance(victim, dict):
                break
            vid = str(victim.get("target_item_id", "") or "")
            if not vid:
                break
            tasks.pop(vid, None)
            dropped += 1

        self._delayed_tasks = tasks
        return dropped

    @staticmethod
    def _dual_bucket_weights(buckets: list[dict[str, Any]], t_sec: float) -> tuple[str, float, str, float]:
        """
        Dual-bucket interpolation.
        双桶插值：找到 t 在中心点序列中的相邻两桶，并按距离线性分配权重。

        返回：
          (bucket_id_1, weight_1, bucket_id_2, weight_2)
        """
        if not buckets:
            return "", 0.0, "", 0.0
        centers = [float(b.get("center_sec", 0.0) or 0.0) for b in buckets]
        ids = [str(b.get("id", "") or "") for b in buckets]

        # Clamp to edges / 边界钳制
        if t_sec <= centers[0]:
            return ids[0], 1.0, ids[0], 0.0
        if t_sec >= centers[-1]:
            return ids[-1], 1.0, ids[-1], 0.0

        # Find neighbors / 找相邻中心点
        for i in range(len(centers) - 1):
            c1 = centers[i]
            c2 = centers[i + 1]
            if c1 <= t_sec <= c2:
                span = max(1e-9, float(c2 - c1))
                w2 = float(t_sec - c1) / span
                w2 = max(0.0, min(1.0, w2))
                w1 = 1.0 - w2
                return ids[i], w1, ids[i + 1], w2

        # Fallback / 回退
        return ids[0], 1.0, ids[0], 0.0

    @staticmethod
    def _elapsed_ms(start_time: float) -> int:
        return int((time.time() - start_time) * 1000)

    @staticmethod
    def _make_response(success: bool, code: str, message: str, *, data: dict, trace_id: str, elapsed_ms: int) -> dict[str, Any]:
        return {
            "success": bool(success),
            "code": str(code),
            "message": str(message),
            "data": data,
            "trace_id": trace_id,
            "elapsed_ms": int(elapsed_ms),
        }
