# -*- coding: utf-8 -*-
"""
AP 原型观测台（Observatory）应用
=============================

本模块是原型测试与可视化的“入口壳层”：
  - 提供 web/终端两种运行方式
  - 驱动一次 Tick（认知滴答）闭环
  - 汇总并导出报告（HTML/JSON）
  - 提供配置编辑、热加载，以及先天规则编辑/校验/模拟接口

English (short):
  Local observatory application for AP prototype testing and monitoring.
"""

from __future__ import annotations

import json
import os
import shlex
import sys
import time
import webbrowser
from pathlib import Path
from typing import Any

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from attention import AttentionFilter
from attention.main import _DEFAULT_CONFIG as ATTENTION_DEFAULT_CONFIG
from cognitive_feeling import CognitiveFeelingSystem
from cognitive_feeling.main import _DEFAULT_CONFIG as CFS_DEFAULT_CONFIG
from emotion import EmotionManager
from emotion.main import _DEFAULT_CONFIG as EMOTION_DEFAULT_CONFIG
from hdb import HDB
from hdb.main import _DEFAULT_CONFIG as HDB_DEFAULT_CONFIG
from hdb._cut_engine import CutEngine
from hdb._id_generator import next_id
from hdb._sequence_display import (
    format_group_display,
    format_semantic_group_display,
    format_semantic_sequence_groups,
    format_sequence_groups,
)
from state_pool.main import StatePool, _DEFAULT_CONFIG as STATE_POOL_DEFAULT_CONFIG
from text_sensor import TextSensor
from text_sensor.main import _DEFAULT_CONFIG as TEXT_SENSOR_DEFAULT_CONFIG
from time_sensor import TimeSensor
from time_sensor.main import TIME_SENSOR_DEFAULT_CONFIG
from innate_script import InnateScriptManager
from innate_script.main import _DEFAULT_CONFIG as IESM_DEFAULT_CONFIG
from action import ActionManager
from action.main import _DEFAULT_CONFIG as ACTION_DEFAULT_CONFIG
from energy_balance import EnergyBalanceController
from energy_balance.main import _DEFAULT_CONFIG as ENERGY_BALANCE_DEFAULT_CONFIG

from ._config_layout import build_config_view, coerce_updates_by_defaults, load_yaml_dict, save_annotated_config
from ._render_html import export_cycle_html
from ._render_terminal import (
    format_help,
    render_check_report,
    render_cycle_report,
    render_group_report,
    render_hdb_snapshot,
    render_header,
    render_repair_report,
    render_state_snapshot,
    render_structure_report,
    render_episodic_report,
)


DEFAULT_CONFIG = {
    "attention_top_n": 16,
    "attention_stub_consume_energy": True,
    "attention_memory_energy_ratio": 0.5,
    "snapshot_top_k": 24,
    # cfs_source_mode / 认知感受信号（CFS）来源模式
    # - iesm: 由 IESM 规则（phase=cfs）生成（推荐，可观测/可编辑）
    # - legacy: 由旧版 CFS 模块生成（过渡/对照）
    "cfs_source_mode": "iesm",
    "export_html": True,
    "export_json": True,
    "auto_open_html_report": False,
    "history_limit": 24,
    "default_launch_mode": "web",
    "web_host": "127.0.0.1",
    "web_port": 8765,
    "web_auto_open_browser": True,
    "sensor_default_mode": "advanced",
    "sensor_tokenizer_backend": "jieba",
    "sensor_enable_token_output": True,
    "sensor_enable_char_output": False,
    "sensor_enable_echo": True,
    "sensor_include_echoes_in_packet": True,
    "state_pool_enable_placeholder_interfaces": False,
    "state_pool_enable_script_broadcast": False,
    "hdb_enable_background_repair": True,
}

OBSERVATORY_CONFIG_SCHEMA = {
    "title": "观测台",
    "description": "控制前端观测台自身的启动模式、导出与历史记录。",
    "groups": [
        {
            "title": "基础",
            "fields": {
                "attention_top_n": "Top-N 占位注意力大小。",
                "attention_stub_consume_energy": "形成注意记忆体时是否从状态池真实扣减能量。",
                "attention_memory_energy_ratio": "形成注意记忆体时，从入选对象抽取的能量比例。",
                "snapshot_top_k": "默认展示的状态池对象数量。",
                "cfs_source_mode": "认知感受信号（CFS）来源模式：iesm（推荐，规则化）/ legacy（旧版硬编码对照）。",
                "history_limit": "前端最近轮次历史保留数量。",
                "export_html": "是否导出 HTML 报告。",
                "export_json": "是否导出 JSON 报告。",
                "auto_open_html_report": "每轮完成后是否自动打开 HTML 报告。",
            },
        },
        {
            "title": "Web 启动",
            "fields": {
                "default_launch_mode": "默认启动模式，建议使用 web。",
                "web_host": "本地 Web 服务监听地址。",
                "web_port": "本地 Web 服务端口。",
                "web_auto_open_browser": "启动 Web 观测台时是否自动打开浏览器。",
            },
        },
        {
            "title": "运行时覆盖",
            "fields": {
                "sensor_default_mode": "观测台启动时覆盖 TextSensor 的默认模式。",
                "sensor_tokenizer_backend": "观测台启动时覆盖分词后端。",
                "sensor_enable_token_output": "观测台启动时是否强制输出 token SA。",
                "sensor_enable_char_output": "观测台启动时是否强制输出字符级 SA。",
                "sensor_enable_echo": "观测台启动时是否强制启用 echo。",
                "sensor_include_echoes_in_packet": "观测台启动时是否强制把 echo 混入 stimulus_packet。",
                "state_pool_enable_placeholder_interfaces": "观测台启动时是否启用状态池占位接口。",
                "state_pool_enable_script_broadcast": "观测台启动时是否启用脚本广播。",
                "hdb_enable_background_repair": "观测台启动时是否允许 HDB 后台修复。",
            },
        },
    ],
}

TEXT_SENSOR_CONFIG_SCHEMA = {
    "title": "文本感受器",
    "description": "控制分词、重要性评分、残响与输入刺激生成策略。",
    "groups": [
        {
            "title": "输入模式",
            "fields": {
                "default_mode": "默认切分模式：simple / advanced / hybrid。",
                "enable_char_output": "是否输出字符级 SA。",
                "enable_token_output": "是否输出词元级 SA。",
                "enable_csa_output": "是否输出 CSA。",
                "tokenizer_backend": "分词器后端。",
                "tokenizer_fallback_to_char": "分词失败时是否回退到字符级。",
            },
        },
        {
            "title": "刺激量",
            "fields": {
                "char_base_er": "字符基础实能量。",
                "token_base_er": "词元基础实能量。",
                "enable_stimulus_intensity_attribute_sa": "是否生成 stimulus_intensity 数值属性刺激元（属性 SA）。默认关闭以提升可读性。",
                "attribute_er_ratio": "属性 SA 的 ER 比例。",
                "attribute_ev_ratio": "属性 SA 的 EV 比例。",
            },
        },
        {
            "title": "残响",
            "fields": {
                "enable_echo": "是否启用 echo 残响。",
                "echo_decay_mode": "残响衰减模式。",
                "echo_round_decay_factor": "轮次衰减因子。",
                "echo_min_energy_threshold": "低于该阈值的残响被淘汰。",
                "echo_pool_max_frames": "残响池最大帧数。",
                "include_echoes_in_stimulus_packet_objects": "是否将历史 echo 混入 stimulus_packet。",
            },
        },
        {
            "title": "刺激疲劳",
            "fields": {
                "enable_stimulus_fatigue": "是否启用重复输入的刺激疲劳。",
                "stimulus_fatigue_window_rounds": "刺激疲劳统计窗口轮次。",
                "stimulus_fatigue_threshold_count": "窗口内出现次数达到该值后开始疲劳。",
                "stimulus_fatigue_max_suppression": "刺激疲劳的最大 ER 抑制比例。",
            },
        },
    ],
}

STATE_POOL_CONFIG_SCHEMA = {
    "title": "状态池",
    "description": "控制运行态对象的衰减、中和、合并、淘汰与广播。",
    "groups": [
        {
            "title": "容量与衰减",
            "fields": {
                "pool_max_items": "状态池对象总上限。",
                "default_er_decay_ratio": "每 Tick ER 衰减比例。",
                "default_ev_decay_ratio": "每 Tick EV 衰减比例。",
                "er_elimination_threshold": "ER 淘汰阈值。",
                "ev_elimination_threshold": "EV 淘汰阈值。",
            },
        },
        {
            "title": "近因增益与疲劳",
            "fields": {
                "recency_gain_peak": "新建或重新激活时写入的近因峰值；默认配置为 10x。",
                "recency_gain_hold_ticks": "近因增益在峰值段保持的 Tick 数。",
                "recency_gain_decay_ratio": "保持期后每 Tick 的近因增益保留系数；默认值约对应 10x 在 100 万 Tick 量级回到 1x。",
                "fatigue_window_ticks": "统计短时重复激活的疲劳窗口。",
                "fatigue_threshold_count": "窗口内激活次数达到该值后开始疲劳。",
                "fatigue_max_value": "状态池运行疲劳的上限。",
            },
        },
        {
            "title": "中和与合并",
            "fields": {
                "enable_neutralization": "是否启用中和逻辑。",
                "neutralization_mode": "中和模式。",
                "merge_duplicate_items": "是否合并重复对象。",
                "enable_semantic_same_object_merge": "是否启用语义同一对象合并。",
                "aggregate_same_semantic_incoming_objects": "同包内语义同一对象是否先聚合。",
            },
        },
        {
            "title": "广播与占位",
            "fields": {
                "enable_script_broadcast": "是否广播状态变化到脚本占位接口。",
                "enable_placeholder_interfaces": "是否启用各类占位接口。",
            },
        },
    ],
}

HDB_CONFIG_SCHEMA = {
    "title": "HDB",
    "description": "控制结构级与刺激级查存、赋能、指针 fallback、自检与修复。",
    "groups": [
        {
            "title": "查存一体",
            "fields": {
                "stimulus_level_max_rounds": "刺激级查存最大轮次。",
                "structure_level_max_rounds": "结构级查存最大轮次。",
                "top_n_attention_stub_default": "Top-N 占位默认大小。",
                "stimulus_match_transfer_ratio": "刺激级命中后，从被覆盖刺激转移到结构的能量比例。",
                "stimulus_residual_min_energy": "剩余刺激总能量低于该值时停止下一轮刺激级查存。",
                "min_cut_common_length": "最小共同切割长度。",
                "diff_table_soft_limit": "diff_table 软上限。",
                "group_table_soft_limit": "group_table 软上限。",
            },
        },
        {
            "title": "感应赋能",
            "fields": {
                "ev_propagation_threshold": "虚能量传播阈值。",
                "er_induction_threshold": "实能量诱发阈值。",
                "ev_propagation_ratio": "EV 传播预算比例。",
                "er_induction_ratio": "ER 诱发预算比例。",
                "induction_target_top_k": "赋能目标 Top-K。",
                "memory_activation_decay_round_ratio_ev": "记忆赋能池每轮 EV 衰减比例。",
                "memory_activation_prune_threshold_ev": "记忆赋能池裁剪阈值。",
                "memory_activation_event_history_limit": "每个记忆赋能条目保留的事件历史数量。",
            },
        },
        {
            "title": "权重与保护",
            "fields": {
                "base_weight_er_gain": "实验证增强基础权重。",
                "base_weight_ev_wear": "虚循环磨损基础权重。",
                "recency_gain_boost": "近因增益提升量。",
                "recency_gain_peak": "近因增益峰值上限；默认配置为 10x。",
                "recency_gain_hold_rounds": "近因峰值保持的轮数。",
                "recency_gain_refresh_floor": "弱命中刷新近因时使用的最低强度地板。",
                "recency_gain_decay_ratio": "按轮次衰减时的近因保留系数；默认值约对应 10x 在 100 万 Tick 量级回到 1x。",
                "fatigue_cap": "疲劳上限。",
                "fatigue_increase_per_match": "每次命中增加的疲劳。",
                "fatigue_decay_per_tick": "每 Tick 的疲劳保留系数。",
                "enable_pointer_fallback": "是否启用指针 fallback。",
                "fallback_lookup_max_candidates": "fallback 候选上限。",
            },
        },
        {
            "title": "修复治理",
            "fields": {
                "self_check_default_scope": "默认自检范围。",
                "repair_batch_limit": "每批修复处理上限。",
                "repair_sleep_ms_between_batches": "修复批次间休眠。",
                "allow_delete_unrecoverable": "是否允许删除不可恢复数据。",
                "enable_background_repair": "是否启用后台修复。",
            },
        },
    ],
}


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
    return load_yaml_dict(path)


def _serialize_simple_yaml_scalar(value: Any) -> str:
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return str(value)
    text = str(value)
    escaped = text.replace("\\", "\\\\").replace('"', '\\"')
    return f"\"{escaped}\""


def _dump_simple_yaml(data: dict[str, Any], indent: int = 0) -> str:
    lines: list[str] = []
    prefix = " " * indent
    for key, value in data.items():
        if isinstance(value, dict):
            lines.append(f"{prefix}{key}:")
            if value:
                lines.append(_dump_simple_yaml(value, indent + 2))
            else:
                lines.append(f"{prefix}  {{}}")
        elif isinstance(value, list):
            if not value:
                lines.append(f"{prefix}{key}: []")
                continue
            lines.append(f"{prefix}{key}:")
            for item in value:
                if isinstance(item, dict):
                    lines.append(f"{prefix}  -")
                    lines.append(_dump_simple_yaml(item, indent + 4))
                else:
                    lines.append(f"{prefix}  - {_serialize_simple_yaml_scalar(item)}")
        else:
            lines.append(f"{prefix}{key}: {_serialize_simple_yaml_scalar(value)}")
    return "\n".join(lines)


def _write_yaml_config(path: str, data: dict[str, Any]) -> None:
    try:
        import yaml

        with open(path, "w", encoding="utf-8") as fh:
            yaml.safe_dump(data, fh, allow_unicode=True, sort_keys=False)
        return
    except ImportError:
        pass

    content = _dump_simple_yaml(data).strip() + "\n"
    with open(path, "w", encoding="utf-8") as fh:
        fh.write(content)


class ObservatoryApp:
    def __init__(self, config_path: str = "", config_override: dict | None = None):
        self._config_path = config_path or os.path.join(os.path.dirname(__file__), "config", "observatory_config.yaml")
        self._config = self._build_config(config_override)
        self.output_dir = Path(__file__).resolve().parent / "outputs"
        self.output_dir.mkdir(parents=True, exist_ok=True)

        self.sensor = TextSensor(
            config_override=self._sensor_config_override()
        )
        # 时间感受器（Time Sensor）
        # 对齐理论 4.2.6~4.2.8：生成“时间桶节点”，供 IESM 规则触发回忆等行动。
        self.time_sensor = TimeSensor()
        self.pool = StatePool(
            config_override=self._state_pool_config_override()
        )
        self.hdb = HDB(config_override=self._hdb_config_override())
        self.attention = AttentionFilter(
            config_override=self._attention_config_override()
        )
        self.cfs = CognitiveFeelingSystem()
        self.emotion = EmotionManager()
        self.iesm = InnateScriptManager()
        # Step 9 行动管理模块（Action/Drive, 驱动力）
        # 注意：行动模块不是“直接回答”，而是对齐理论中的 Drive 竞争与消耗机制，
        # 用于把注意力聚焦/发散/回忆等“内在行动器”纳入可解释闭环。
        self.action = ActionManager()
        # 实虚能量平衡控制器（EBC）
        # 对齐你提出的“ER:EV 是否可稳定收敛到 1:1”的验收需求：
        # - 这是一个可插拔闭环模块，默认可关闭；
        # - 它读取本 tick 的全局 ER_total/EV_total，并输出下一 tick 的 HDB 调制 scale；
        # - scale 会与 EMgr/Action 的 scale 以“相乘”的方式合并（不是互相覆盖）。
        self.energy_balance = EnergyBalanceController()
        self.cut_engine = CutEngine()
        self.tick_counter = 0
        self._last_report: dict[str, Any] | None = None
        self._report_history: list[dict[str, Any]] = []
        self._started_at = int(time.time() * 1000)
        self._pending_focus_directives: list[dict[str, Any]] = []
        self._last_modulation: dict[str, Any] = {}
        # HDB 基准配置快照（用于“情绪调制 -> HDB 参数”时避免累计漂移）
        # 说明：每 tick 会按该基准值 * scale 计算 effective 配置值。
        self._hdb_config_base: dict[str, Any] = dict(self.hdb._config)
        self._silence_jieba_logs()

    def close(self) -> None:
        self.sensor._logger.close()
        self.time_sensor.close()
        self.pool._logger.close()
        self.hdb.close()
        self.attention.close()
        self.cfs.close()
        self.emotion.close()
        self.iesm.close()
        self.action.close()
        self.energy_balance.close()

    def next_trace(self, prefix: str = "cycle") -> str:
        self.tick_counter += 1
        return f"{prefix}_{self.tick_counter:04d}"

    def print_header(self) -> None:
        print(render_header())
        print(format_help())

    def run_cycle(self, text: str | None = None, *, labels: dict[str, Any] | None = None) -> dict:
        trace_id = self.next_trace("cycle")
        tick_id = trace_id
        # Timing / 耗时统计（用于“找茬式验收”与性能排查）
        # - 只做观测，不影响逻辑。
        # - 统计口径：ms（毫秒）
        cycle_t0 = time.perf_counter()
        timing_steps_ms: dict[str, int] = {}
        report: dict[str, Any] = {
            "trace_id": trace_id,
            # tick_id：用于跨模块/前端对齐“本轮 tick”的统一标识。
            # 说明：当前原型里 tick_id 与 trace_id 一致，但仍保留字段，便于未来做并行/多子流程时扩展。
            "tick_id": tick_id,
            "started_at": int(time.time() * 1000),
            "observatory": {
                "module": "observatory",
                "config": dict(self._config),
                "output_dir": str(self.output_dir),
            },
        }

        # Optional per-tick labels (experiment/teacher signals).
        # 说明：
        # - 默认 None，不影响原有观测台调用口径；
        # - 实验跑批可通过 labels 注入“教师信号/外置奖惩/离线评估标签”等。
        tick_labels = labels if isinstance(labels, dict) else {}
        report["tick_labels"] = dict(tick_labels) if tick_labels else {}

        external_packet = None
        sensor_result = None
        if text is not None:
            t0 = time.perf_counter()
            sensor_result = self.sensor.ingest_text(text=text, trace_id=trace_id, tick_id=tick_id)
            # 注意：文本感受器可能因输入校验失败而返回 success=False（例如空字符串）。
            # 为了让“空输入”也能作为一个 tick 被观测与验收，这里做防御式处理：
            # - 传感器成功：取出 stimulus_packet 作为 external_packet
            # - 传感器失败：external_packet 置空，后续按“空 Tick”继续跑闭环，但 report 会保留错误信息
            try:
                if isinstance(sensor_result, dict) and bool(sensor_result.get("success", False)):
                    data = sensor_result.get("data", {}) if isinstance(sensor_result.get("data", {}), dict) else {}
                    if isinstance(data.get("stimulus_packet"), dict):
                        external_packet = data.get("stimulus_packet")
            except Exception:
                external_packet = None
            report["sensor"] = self._build_sensor_report(text, sensor_result)
            timing_steps_ms["sensor_ms"] = int((time.perf_counter() - t0) * 1000)
        else:
            report["sensor"] = {}
            timing_steps_ms["sensor_ms"] = 0

        t0 = time.perf_counter()
        report["maintenance"] = self._run_state_pool_maintenance(trace_id, tick_id)
        timing_steps_ms["maintenance_ms"] = int((time.perf_counter() - t0) * 1000)
        report["memory_activation"] = {
            "maintenance": self.hdb.tick_memory_activation_pool(trace_id=trace_id, tick_id=tick_id)["data"],
            "apply_result": {
                "applied_count": 0,
                "total_delta_er": 0.0,
                "total_delta_ev": 0.0,
                "total_delta_energy": 0.0,
                "items": [],
            },
            "seed_targets": [],
            "feedback_result": {
                "applied_count": 0,
                "total_feedback_er": 0.0,
                "total_feedback_ev": 0.0,
                "total_feedback_energy": 0.0,
                "items": [],
                "record_result": {
                    "recorded_count": 0,
                    "total_feedback_er": 0.0,
                    "total_feedback_ev": 0.0,
                    "total_feedback_energy": 0.0,
                    "items": [],
                },
            },
            "snapshot": {
                "summary": {"count": 0, "total_er": 0.0, "total_ev": 0.0, "total_energy": 0.0, "top_total_energy": 0.0},
                "items": [],
                "sort_by": "energy_desc",
            },
        }

        # 上一 tick 的调制/聚焦指令输入（本 tick 生效）
        modulation_in = self._last_modulation.get("attention", {}) if isinstance(self._last_modulation, dict) else {}
        focus_directives_in: list[dict[str, Any]] = []
        for directive in self._pending_focus_directives:
            if not isinstance(directive, dict):
                continue
            ttl = int(directive.get("ttl_ticks", 0) or 0)
            if ttl > 0:
                focus_directives_in.append(directive)
        report["modulation_inputs"] = {
            "attention": dict(modulation_in) if isinstance(modulation_in, dict) else {},
            "focus_directives": [dict(item) for item in focus_directives_in[:16]],
        }

        # HDB 调制输入（上一 tick 生效到本 tick）
        # 对齐理论 3.9.2：情绪递质不仅调制注意力，也应调制学习力度与能量传播系数等 HDB 参数。
        hdb_mod_in = self._last_modulation.get("hdb", {}) if isinstance(self._last_modulation, dict) else {}
        hdb_mod_apply = self._apply_hdb_modulation_for_tick(
            modulation=hdb_mod_in if isinstance(hdb_mod_in, dict) else {},
            trace_id=trace_id,
            tick_id=tick_id,
        )
        report["modulation_inputs"]["hdb"] = dict(hdb_mod_in) if isinstance(hdb_mod_in, dict) else {}
        report["modulation_applied"] = {"hdb": hdb_mod_apply}

        t0 = time.perf_counter()
        attention_snapshot, attention_report = self._build_attention_memory_stub(
            trace_id,
            tick_id,
            focus_directives=focus_directives_in,
            modulation=modulation_in,
        )
        report["attention"] = attention_report
        timing_steps_ms["attention_ms"] = int((time.perf_counter() - t0) * 1000)

        # 指令衰减：被消费过的 focus directives TTL-1（新生成指令在本 tick 末尾加入）
        decayed: list[dict[str, Any]] = []
        for directive in self._pending_focus_directives:
            if not isinstance(directive, dict):
                continue
            ttl = int(directive.get("ttl_ticks", 0) or 0)
            ttl -= 1
            if ttl <= 0:
                continue
            decayed.append({**directive, "ttl_ticks": ttl})
        self._pending_focus_directives = decayed

        t0 = time.perf_counter()
        structure_result = self.hdb.run_structure_level_retrieval_storage(
            state_snapshot=attention_snapshot,
            trace_id=trace_id,
            tick_id=tick_id,
            # 结构级输入应以“当前 CAM（注意力记忆体）快照”为准，而不是固化配置的 Top-N。
            # 这里把 top_n 作为安全上限：默认使用 CAM 内结构数量（ST count）。
            attention_mode="cam_snapshot",
            top_n=max(
                1,
                sum(1 for it in (attention_snapshot.get("top_items", []) or []) if str(it.get("ref_object_type", "")) == "st"),
            ),
        )
        structure_data = structure_result["data"]
        internal_packet = self.hdb.build_internal_stimulus_packet(
            structure_data.get("internal_stimulus_fragments", []),
            trace_id=trace_id,
            tick_id=tick_id,
        )
        combined_packet = self.hdb.merge_stimulus_packets(external_packet, internal_packet, trace_id=trace_id, tick_id=tick_id)
        report["structure_level"] = {"result": structure_data}
        report["merged_stimulus"] = self._describe_stimulus_packet(combined_packet)
        timing_steps_ms["structure_level_ms"] = int((time.perf_counter() - t0) * 1000)

        t0 = time.perf_counter()
        structure_bias_projection = self._project_runtime_structures(
            structure_result["data"].get("bias_projections", []),
            trace_id=trace_id,
            tick_id=tick_id,
        )
        cache_neutralization = self._neutralize_packet_against_pool(combined_packet, trace_id, tick_id)
        residual_packet = cache_neutralization["residual_packet_raw"]
        report["cache_neutralization"] = {
            "input_packet": cache_neutralization["input_packet"],
            "residual_packet": cache_neutralization["residual_packet"],
            "priority_events": cache_neutralization["priority_events"],
            "priority_diagnostics": cache_neutralization.get("priority_diagnostics", []),
            "priority_summary": cache_neutralization["priority_summary"],
        }
        report["pool_apply"] = {
            "apply_result": {},
            "events": [],
            "priority_events": cache_neutralization["priority_events"],
            "priority_diagnostics": cache_neutralization.get("priority_diagnostics", []),
            "bias_projection": structure_bias_projection,
            "input_packet": cache_neutralization["input_packet"],
            "residual_packet": cache_neutralization["residual_packet"],
            "priority_summary": dict(cache_neutralization["priority_summary"]),
        }
        timing_steps_ms["cache_neutralization_ms"] = int((time.perf_counter() - t0) * 1000)

        t0 = time.perf_counter()
        stimulus_result = self.hdb.run_stimulus_level_retrieval_storage(
            stimulus_packet=residual_packet,
            trace_id=trace_id,
            tick_id=tick_id,
        )
        stimulus_data = stimulus_result["data"]
        report["stimulus_level"] = {"result": stimulus_data}
        timing_steps_ms["stimulus_level_ms"] = int((time.perf_counter() - t0) * 1000)

        landing_packet = stimulus_data.get("residual_stimulus_packet", residual_packet)
        t0 = time.perf_counter()
        apply_result, apply_events, landed_packet = self._apply_packet_to_pool(
            landing_packet,
            trace_id,
            tick_id,
            disable_priority_neutralization=True,
        )
        runtime_projection = self._project_runtime_structures(
            stimulus_data.get("runtime_projection_structures", []),
            trace_id=trace_id,
            tick_id=tick_id,
        )
        report["pool_apply"]["apply_result"] = apply_result
        report["pool_apply"]["events"] = apply_events
        report["pool_apply"]["landed_packet"] = self._describe_stimulus_packet(landed_packet)
        report["pool_apply"]["runtime_projection"] = runtime_projection
        timing_steps_ms["pool_apply_ms"] = int((time.perf_counter() - t0) * 1000)

        t0 = time.perf_counter()
        induction_snapshot = self.pool.get_state_snapshot(
            trace_id=f"{trace_id}_induction_snapshot",
            tick_id=tick_id,
            top_k=int(self._config["snapshot_top_k"]),
        )["data"]["snapshot"]
        induction_result = self.hdb.run_induction_propagation(
            state_snapshot=induction_snapshot,
            trace_id=trace_id,
            tick_id=tick_id,
            max_source_items=8,
        )
        induction_data = induction_result["data"]
        source_ev_events = self._apply_induction_source_consumptions(
            induction_data.get("source_ev_consumptions", []),
            trace_id,
            tick_id,
        )
        induction_targets = list(induction_data.get("induction_targets", []))
        structure_targets = [
            item for item in induction_targets if str(item.get("projection_kind", "structure")) != "memory"
        ]
        memory_targets = [
            item for item in induction_targets if str(item.get("projection_kind", "structure")) == "memory"
        ]
        memory_seed_targets = self._collect_memory_activation_seed_targets(report)
        combined_memory_targets = memory_targets + memory_seed_targets
        applied_targets = self._apply_induction_targets(structure_targets, trace_id, tick_id)
        memory_apply_result = self.hdb.apply_memory_activation_targets(
            targets=combined_memory_targets,
            trace_id=trace_id,
            tick_id=tick_id,
        )["data"]
        memory_feedback_result = self._apply_memory_feedback(
            memory_items=memory_apply_result.get("items", []),
            trace_id=trace_id,
            tick_id=tick_id,
        )
        memory_snapshot = self.hdb.get_memory_activation_snapshot(
            trace_id=f"{trace_id}_memory_activation_snapshot",
            limit=24,
            sort_by="energy_desc",
        )["data"]
        report["induction"] = {
            "result": induction_data,
            "source_ev_events": source_ev_events,
            "applied_targets": applied_targets,
            "structure_target_count": len(structure_targets),
            "memory_target_count": len(memory_targets),
            "memory_seed_target_count": len(memory_seed_targets),
            "memory_target_total_count": len(combined_memory_targets),
        }
        report["memory_activation"]["apply_result"] = memory_apply_result
        report["memory_activation"]["seed_targets"] = memory_seed_targets
        report["memory_activation"]["feedback_result"] = memory_feedback_result
        report["memory_activation"]["snapshot"] = memory_snapshot
        report["memory_feedback"] = memory_feedback_result
        timing_steps_ms["induction_and_memory_ms"] = int((time.perf_counter() - t0) * 1000)

        # =============================================================== #
        # Time Sensor（时间感受器）: 生成时间感受（桶节点 + 属性绑定）并写入状态池（SP） #
        # =============================================================== #
        # 对齐理论 4.2.6~4.2.7：
        # - 当记忆被重新接触（MAP 赋能）时，依据“当前时间戳 - 记忆时间戳”生成时间感受；
        # - 用有限数量的时间桶承载连续时间尺度（双桶赋能/匹配）；
        # - 同时可把时间感受作为“属性刺激元”绑定到能量波峰对象上，便于形成结构与解释；
        # - 后续由 IESM（先天规则）观察“时间桶节点获得能量/变化”并触发回忆行动（recall）。
        t0 = time.perf_counter()
        try:
            ts_res = self.time_sensor.run_time_feeling_tick(
                pool=self.pool,
                trace_id=trace_id,
                tick_id=tick_id,
                now_ms=int(report.get("started_at", 0) or 0) or None,
                memory_activation_snapshot=memory_snapshot,
                memory_feedback_result=memory_feedback_result,
            )
            report["time_sensor"] = ts_res.get("data", {}) if isinstance(ts_res, dict) else {}
        except Exception as exc:
            report["time_sensor"] = {"error": str(exc)}
        timing_steps_ms["time_sensor_ms"] = int((time.perf_counter() - t0) * 1000)

        # =============================================================== #
        # Teacher Feedback（教师信号/外置奖惩）                              #
        # =============================================================== #
        # 说明：
        # - 用于论文实验与元学习验证：数据集可在每个 tick 附带 labels.teacher_rwd/pun 等字段；
        # - 教师信号既应进入“状态池可审计的属性绑定”（供记忆材料补全），
        #   也应进入 EMgr 的 rwd/pun 汇总（供递质通道调制），但不应破坏原有闭环。
        t0 = time.perf_counter()
        try:
            report["teacher_feedback"] = self._apply_teacher_feedback(
                labels=tick_labels,
                report=report,
                trace_id=trace_id,
                tick_id=tick_id,
            )
        except Exception as exc:
            report["teacher_feedback"] = {"ok": False, "code": "EXCEPTION", "message": f"teacher_feedback failed: {exc}"}
        timing_steps_ms["teacher_feedback_ms"] = int((time.perf_counter() - t0) * 1000)

        # =============================================================== #
        # Step 7/8/IESM: CFS（认知感受信号）-> IESM（先天规则）-> Emotion    #
        # =============================================================== #
        # 说明：对齐理论（3.10/3.12）中“先天脚本可管理情绪脚本（NT）”的口径。
        # 因此这里把 IESM（先天规则）放在 EMgr（情绪递质）之前，
        # 让 emotion_update 能在同一 tick 生效（而不是拖到下一 tick）。
        #
        # 重要：认知感受信号（CFS）来源可切换（observatory_config.yaml: cfs_source_mode）：
        # - iesm（推荐）：由 IESM 规则（phase=cfs）生成，规则可观测、可编辑（避免硬编码散落）。
        # - legacy：沿用旧版 CFS 模块的硬编码计算（仅作为过渡/对照实验）。

        cfs_source_mode = str(self._config.get("cfs_source_mode", "iesm") or "iesm").strip().lower() or "iesm"
        t0 = time.perf_counter()
        cfs_data: dict[str, Any] = {}
        cfs_signals: list[dict[str, Any]] = []

        if cfs_source_mode in {"legacy", "module", "cfs"}:
            # Legacy CFS module path (transition / comparison only).
            # 旧版 CFS 模块路径（仅过渡/对照用）：其内部包含硬编码触发逻辑。
            cfs_snapshot = self.pool.get_state_snapshot(
                trace_id=f"{trace_id}_cfs_snapshot",
                tick_id=tick_id,
                top_k=int(self._config.get("snapshot_top_k", 24)),
            )["data"]["snapshot"]
            cfs_result = self.cfs.run_cfs(
                pool=self.pool,
                state_snapshot=cfs_snapshot,
                cam_snapshot=attention_snapshot,
                attention_report=report.get("attention", {}),
                trace_id=trace_id,
                tick_id=tick_id,
                context={
                    "structure_level": report.get("structure_level", {}).get("result", {}),
                    "stimulus_level": report.get("stimulus_level", {}).get("result", {}),
                    "induction": report.get("induction", {}).get("result", {}),
                    "cache_neutralization": report.get("cache_neutralization", {}),
                },
            )
            cfs_data = cfs_result.get("data", {}) or {}
            report["cognitive_feeling"] = cfs_data
            cfs_signals = list(cfs_data.get("cfs_signals", []) or [])
        else:
            # Preferred path: IESM rules generate CFS signals.
            # 推荐路径：由 IESM 规则生成认知感受信号（CFS），这里先放一个占位，后面用 IESM 输出回填。
            cfs_data = {
                "cfs_signals": [],
                "writes": {"runtime_nodes": [], "attribute_bindings": []},
                "meta": {"tick_number": int(self.tick_counter), "source_mode": "iesm_rules"},
            }
            report["cognitive_feeling"] = cfs_data
            cfs_signals = []

        timing_steps_ms["cfs_ms"] = int((time.perf_counter() - t0) * 1000)
        t_iesm0 = time.perf_counter()

        innate_script_report: dict[str, Any] = {
            "active_scripts": self.iesm.get_active_scripts(trace_id=f"{trace_id}_iesm_scripts").get("data", {}),
            "state_window_checks": [],
            "focus": {},
        }
        maint_packet: dict[str, Any] = {}
        apply_packet: dict[str, Any] = {}
        try:
            # 维护窗口
            maint_packet = self.pool._snapshot.build_script_check_packet(
                events=report.get("maintenance", {}).get("events", []),
                pool_store=self.pool._store,
                trace_id=f"{trace_id}_iesm_maint",
                tick_id=tick_id,
            )
            maint_check = self.iesm.check_state_window(maint_packet, trace_id=trace_id).get("data", {})
            innate_script_report["state_window_checks"].append(
                {"stage": "maintenance", "packet_summary": maint_packet.get("summary", {}), "check": maint_check}
            )
        except Exception:
            innate_script_report["state_window_checks"].append({"stage": "maintenance", "error": "packet_build_failed"})

        try:
            # 落地窗口（刺激回写）
            apply_events = report.get("pool_apply", {}).get("events", [])
            apply_packet = self.pool._snapshot.build_script_check_packet(
                events=apply_events,
                pool_store=self.pool._store,
                trace_id=f"{trace_id}_iesm_apply",
                tick_id=tick_id,
            )
            apply_check = self.iesm.check_state_window(apply_packet, trace_id=trace_id).get("data", {})
            innate_script_report["state_window_checks"].append(
                {"stage": "pool_apply", "packet_summary": apply_packet.get("summary", {}), "check": apply_check}
            )
        except Exception:
            innate_script_report["state_window_checks"].append({"stage": "pool_apply", "error": "packet_build_failed"})

        # 构造 IESM 规则引擎所需的运行态上下文（供 metric 条件使用）。
        # Build runtime context for IESM metric predicates.
        innate_rules_context = self._build_innate_rules_context(
            report=report,
            pool_snapshot=None,  # use live StatePool store
            emotion_state=None,  # IESM runs before EMgr update; use current snapshot + CFS-derived rwd/pun
            cfs_signals=cfs_signals,
            trace_id=trace_id,
            tick_id=tick_id,
        )

        # 规则引擎：同时接入 CFS + 两个状态窗口（maintenance / pool_apply），允许组合触发条件（any/all）。
        tick_rules_result = self.iesm.run_tick_rules(
            trace_id=trace_id,
            tick_id=tick_id,
            tick_index=int(self.tick_counter),
            cfs_signals=cfs_signals,
            state_windows=[
                {"stage": "maintenance", "packet": maint_packet},
                {"stage": "pool_apply", "packet": apply_packet},
            ],
            context=innate_rules_context,
            dry_run=False,
        )
        tick_rules_data = tick_rules_result.get("data", {}) or {}
        directives = tick_rules_data.get("directives", {}) or {}

        # If CFS is sourced from IESM rules, treat directives.cfs_signals as canonical.
        # 如果认知感受信号来源选择为 IESM（推荐），则以规则引擎输出的 cfs_signals 作为本 tick 的“官方”CFS 列表，
        # 并回填到 report["cognitive_feeling"]，供后续 EMgr/Action/前端展示使用。
        if cfs_source_mode not in {"legacy", "module", "cfs"}:
            cfs_signals = list(directives.get("cfs_signals", []) or [])
            report["cognitive_feeling"] = {
                "cfs_signals": cfs_signals,
                "writes": {"runtime_nodes": [], "attribute_bindings": []},
                "meta": {"tick_number": int(self.tick_counter), "source_mode": "iesm_rules"},
            }
        pool_effects = list(directives.get("pool_effects", []) or [])
        pool_effect_apply = {}
        if pool_effects:
            # Apply pool effects immediately so the same tick can affect later steps/snapshots.
            # 立即应用状态池效果：让同一 tick 的后续步骤/快照能看到变化。
            pool_effect_apply = self._apply_innate_pool_effects(
                effects=pool_effects,
                context=innate_rules_context,
                trace_id=trace_id,
                tick_id=tick_id,
            )

        # Enrich episodic memory material with runtime-bound attributes (CFS/time-feeling/rwd/pun tags).
        # 把“运行态绑定属性（CFS/时间感受/奖惩信号等）”补写进本 tick 的情景记忆材料：
        # - 让属性刺激元能进入记忆并被回忆反哺带回 SP，从而支持“期待/压力”等通道成立。
        try:
            enrich_res = self._enrich_tick_episodic_memory_with_bound_attributes(report=report, trace_id=trace_id, tick_id=tick_id)
        except Exception as exc:
            enrich_res = {"ok": False, "code": "EXCEPTION", "message": f"enrich episodic memory failed: {exc}"}
        try:
            stim_res = (report.get("stimulus_level", {}) or {}).get("result", {})
            if isinstance(stim_res, dict):
                stim_res["episodic_memory_enrichment"] = enrich_res
        except Exception:
            pass
        focus_data = {
            # 规则引擎输出的“运行态 CFS 信号列表”（包含输入 + 本 tick 新生成）。
            # Note: This is the rule-engine output list, not necessarily the legacy CFS module output.
            "cfs_signals": list(directives.get("cfs_signals", []) or []),
            "focus_directives": list(directives.get("focus_directives", []) or []),
            "emotion_updates": dict(directives.get("emotion_updates", {}) or {}),
            "action_triggers": list(directives.get("action_triggers", []) or []),
            "pool_effects": pool_effects,
            "pool_effect_apply": pool_effect_apply,
            "episodic_memory_enrichment": enrich_res,
            "audit": tick_rules_data.get("audit", {}) or {},
            "triggered_rules": list(tick_rules_data.get("triggered_rules", []) or []),
            "triggered_scripts": list(tick_rules_data.get("triggered_scripts", []) or []),
        }
        innate_script_report["focus"] = focus_data
        innate_script_report["tick_rules"] = {
            "code": tick_rules_result.get("code", ""),
            "triggered_rule_count": len(focus_data.get("triggered_rules", []) or []),
            "focus_directive_count": len(focus_data.get("focus_directives", []) or []),
            "emotion_update_key_count": len((focus_data.get("emotion_updates") or {}).keys()),
            "action_trigger_count": len(focus_data.get("action_triggers", []) or []),
            "pool_effect_count": len(focus_data.get("pool_effects", []) or []),
        }
        new_directives = list(focus_data.get("focus_directives", []) or [])
        new_action_triggers = list(focus_data.get("action_triggers", []) or [])
        if new_directives:
            # 说明：在理论中，IESM（先天脚本）应当“触发行动节点”而非直接强制修改注意力。
            # 因此当行动模块启用时，IESM 的 focus_directives 会作为 Step 9 的行动触发源；
            # 只有在行动模块禁用时，才直接把指令加入 pending（下一 tick 生效）。
            action_enabled = bool(getattr(self, "action", None) and getattr(self.action, "_config", {}).get("enabled", True))
            if not action_enabled:
                # 合并到 pending（下一 tick 生效）；按 directive_id 去重
                existing_by_id = {
                    str(item.get("directive_id", "")): item
                    for item in self._pending_focus_directives
                    if isinstance(item, dict) and str(item.get("directive_id", ""))
                }
                for directive in new_directives:
                    if not isinstance(directive, dict):
                        continue
                    did = str(directive.get("directive_id", ""))
                    if not did:
                        continue
                    existing_by_id[did] = directive
                self._pending_focus_directives = list(existing_by_id.values())

        report["innate_script"] = innate_script_report
        timing_steps_ms["iesm_ms"] = int((time.perf_counter() - t_iesm0) * 1000)

        # =============================================================== #
        # Step 8: EMgr（情绪管理器/递质通道 NT）                            #
        # =============================================================== #
        # 输入：CFS + IESM 的 emotion_update（脚本化增量）
        t0 = time.perf_counter()
        # Compute rwd/pun override from the *current* pool (after IESM/time-sensor binding).
        # 从“当前状态池（已包含时间感受/IESM绑定属性）”计算本 tick 的奖惩汇总，传给 EMgr：
        # - 这样 EMgr 前端卡片里显示的 rwd/pun 就是“自然汇总”的结果，而不是旧版 CFS 硬映射。
        rwd_pun_override = None
        try:
            rows = []
            for item in list(self.pool._store.get_all()):  # type: ignore[attr-defined]
                if not isinstance(item, dict):
                    continue
                row = self.pool._snapshot._build_top_item_summary(item)  # type: ignore[attr-defined]
                if isinstance(row, dict):
                    rows.append(row)
            rwd_pun_override = self._estimate_rwd_pun_from_pool_items(rows, trace_id=trace_id, tick_id=tick_id)
        except Exception:
            rwd_pun_override = None

        # External teacher reward/punish can add on top of pool aggregation.
        # 外置奖惩注入：在不破坏“池内自然汇总”的前提下，把教师信号作为附加分量叠加进 rwd/pun。
        try:
            tfb = report.get("teacher_feedback", {}) if isinstance(report.get("teacher_feedback", {}), dict) else {}
            teacher_rwd = float(tfb.get("teacher_rwd", 0.0) or 0.0)
            teacher_pun = float(tfb.get("teacher_pun", 0.0) or 0.0)
            if teacher_rwd > 0.0 or teacher_pun > 0.0:
                base = dict(rwd_pun_override or {})
                base_rwd = float(base.get("rwd", 0.0) or 0.0)
                base_pun = float(base.get("pun", 0.0) or 0.0)
                merged_rwd = self._clamp01(base_rwd + max(0.0, teacher_rwd))
                merged_pun = self._clamp01(base_pun + max(0.0, teacher_pun))
                detail = dict(base.get("detail", {}) or {}) if isinstance(base.get("detail", {}), dict) else {}
                detail.update(
                    {
                        "teacher_rwd": round(float(max(0.0, teacher_rwd)), 8),
                        "teacher_pun": round(float(max(0.0, teacher_pun)), 8),
                        "teacher_mode": str(tfb.get("mode", "") or ""),
                        "teacher_anchor": str(tfb.get("anchor", "") or ""),
                    }
                )
                rwd_pun_override = {
                    **base,
                    "rwd": round(float(merged_rwd), 8),
                    "pun": round(float(merged_pun), 8),
                    "source": f"{str(base.get('source', '') or 'pool_items')}+teacher",
                    "detail": detail,
                }
        except Exception:
            pass
        emotion_result = self.emotion.update_emotion_state(
            {
                "cfs_signals": cfs_signals,
                "tick_id": tick_id,
                "emotion_updates": focus_data.get("emotion_updates", {}),
                "rwd_pun_override": rwd_pun_override or {},
            },
            trace_id=trace_id,
            tick_id=tick_id,
        )
        emotion_data = emotion_result.get("data", {}) or {}
        report["emotion"] = emotion_data
        timing_steps_ms["emotion_ms"] = int((time.perf_counter() - t0) * 1000)
        # 下一 tick 的调制输入（本 tick 末尾确定）：
        # 先取 EMgr（情绪管理器）的调制，再在 Step 9 叠加行动模块（Action/Drive）输出。
        next_modulation: dict[str, Any] = dict(emotion_data.get("modulation", {}) or {})

        # =============================================================== #
        # Step 9: Action/Drive（行动模块：驱动力竞争与消耗）                 #
        # =============================================================== #

        # 行动模块输入：
        #  - CFS（认知感受信号）
        #  - EMgr（情绪递质）输出（用于未来调制 drive/threshold；当前先透传）
        #  - IESM focus_directives（作为“先天触发源”）
        #  - MAP（记忆赋能池）快照（用于回忆行动）
        t0 = time.perf_counter()
        action_result = self.action.run_action_cycle(
            trace_id=trace_id,
            tick_id=tick_id,
            tick_index=int(self.tick_counter),
            cfs_signals=cfs_signals,
            emotion_state=emotion_data,
            innate_focus_directives=new_directives,
            innate_action_triggers=new_action_triggers,
            memory_activation_snapshot=memory_snapshot,
        )
        action_data = action_result.get("data", {}) or {}
        report["action"] = action_data
        timing_steps_ms["action_ms"] = int((time.perf_counter() - t0) * 1000)

        # Step 9 输出 1：行动模块可生成“下一 tick 生效”的注意力聚焦指令。
        focus_directives_out = list(action_data.get("focus_directives_out", []) or [])
        if focus_directives_out:
            # 合并到 pending（下一 tick 生效）；按 directive_id 去重
            existing_by_id = {
                str(item.get("directive_id", "")): item
                for item in self._pending_focus_directives
                if isinstance(item, dict) and str(item.get("directive_id", ""))
            }
            for directive in focus_directives_out:
                if not isinstance(directive, dict):
                    continue
                did = str(directive.get("directive_id", ""))
                if not did:
                    continue
                existing_by_id[did] = directive
            self._pending_focus_directives = list(existing_by_id.values())

        # Step 9 输出 2：行动模块可输出注意力调制（如 top_n），与 EMgr 的调制合并后作为下一 tick 输入。
        action_mod_out = action_data.get("modulation_out", {}) or {}
        if isinstance(action_mod_out, dict):
            for key, value in action_mod_out.items():
                if isinstance(value, dict) and isinstance(next_modulation.get(key), dict):
                    next_modulation[key] = {**dict(next_modulation.get(key) or {}), **dict(value)}
                else:
                    next_modulation[key] = value
        # 注意：此处先不要立刻写入 self._last_modulation。
        # 我们会在本 tick 结束时（final snapshot 之后）再叠加 EBC（能量平衡控制器）的输出，
        # 并一次性写入“下一 tick 的调制包”（避免遗漏与覆盖问题）。

        # =============================================================== #
        # Step 9.5: Recall Side Effects（回忆副作用闭环）                    #
        # =============================================================== #
        # 对齐理论核心 4.2.7.2 回忆流程：
        # - 回忆行动执行后，应把命中的记忆赋能进入 MAP（记忆赋能池，Memory Activation Pool）
        # - 并执行默认的记忆反哺（memory_feedback），把记忆内容以结构/刺激元形式投影回 SP（状态池）。
        #
        # 设计约束：
        # - ActionManager（行动模块）只输出“recall_requests_out”（结构化请求），不直接操作 HDB/SP，
        #   这样更可审计、更容易测试，也避免模块之间循环依赖。
        # - 因此这里由主流程统一执行副作用，并把结果写入 report 供前端观测。
        t0 = time.perf_counter()
        recall_requests = [x for x in (action_data.get("recall_requests_out", []) or []) if isinstance(x, dict)]
        recall_apply_results: list[dict[str, Any]] = []
        recall_feedback_results: list[dict[str, Any]] = []
        recall_total_target_count = 0

        for req in recall_requests:
            targets = list(req.get("map_targets", req.get("targets", [])) or [])
            targets = [t for t in targets if isinstance(t, dict)]
            if not targets:
                continue
            recall_total_target_count += len(targets)

            apply_data = self.hdb.apply_memory_activation_targets(
                targets=targets,
                trace_id=f"{trace_id}_recall_map",
                tick_id=tick_id,
            ).get("data", {}) or {}
            recall_apply_results.append(apply_data)

            # NOTE:
            # - 这里复用主流程的默认记忆反哺逻辑，保证回忆结果能以“可观察的状态池写入”形式出现。
            # - 如果未来你希望“回忆”只投影结构而不生成 SA/CSA，可在 memory_feedback 侧做更精细的策略配置。
            fb_data = self._apply_memory_feedback(
                memory_items=list(apply_data.get("items", []) or []),
                trace_id=f"{trace_id}_recall_feedback",
                tick_id=tick_id,
            )
            recall_feedback_results.append(fb_data)

        recall_memory_snapshot_after: dict[str, Any] = {}
        if recall_apply_results:
            try:
                recall_memory_snapshot_after = self.hdb.get_memory_activation_snapshot(
                    trace_id=f"{trace_id}_recall_map_snapshot",
                    limit=16,
                    sort_by="energy_desc",
                ).get("data", {}) or {}
            except Exception:
                recall_memory_snapshot_after = {}

        if recall_requests:
            action_data["recall_side_effects"] = {
                "request_count": len(recall_requests),
                "target_count": int(recall_total_target_count),
                "apply_results": recall_apply_results,
                "feedback_results": recall_feedback_results,
                "memory_snapshot_after": recall_memory_snapshot_after,
            }
            # 同时把“回忆后”的 MAP 快照也挂到 memory_activation 下面，便于前端对比。
            report.setdefault("memory_activation", {})
            report["memory_activation"]["snapshot_after_action"] = recall_memory_snapshot_after

        timing_steps_ms["action_recall_side_effect_ms"] = int((time.perf_counter() - t0) * 1000)

        t0 = time.perf_counter()
        final_state_snapshot = self.pool.get_state_snapshot(
            trace_id=f"{trace_id}_final_snapshot",
            tick_id=tick_id,
            top_k=None,
        )["data"]["snapshot"]
        hdb_snapshot = self.hdb.get_hdb_snapshot(trace_id=f"{trace_id}_hdb_snapshot", top_k=12)["data"]
        report["final_state"] = {
            "state_snapshot": final_state_snapshot,
            "state_energy_summary": self._summarize_state_snapshot(final_state_snapshot),
            "hdb_snapshot": hdb_snapshot,
        }
        timing_steps_ms["final_snapshot_ms"] = int((time.perf_counter() - t0) * 1000)

        # =============================================================== #
        # Step 9.6: Energy Balance Controller（EBC 实虚能量平衡控制器）      #
        # =============================================================== #
        # 目的：回答“系统能否在任意 ER 输入频次下，长期稳定收敛到 EV:ER≈1:1？”的验收问题。
        # - EBC 读取本 tick 的全局 ER_total/EV_total（最终快照口径）
        # - 输出下一 tick 的 HDB scale（如 ev_propagation_ratio_scale / er_induction_ratio_scale）
        # - 与 EMgr/Action 的 HDB scale 以“相乘”方式合并（避免互相覆盖）
        t0 = time.perf_counter()
        ebc_data: dict[str, Any] = {}
        try:
            es = report.get("final_state", {}).get("state_energy_summary", {}) or {}
            total_er = float(es.get("total_er", 0.0) or 0.0)
            total_ev = float(es.get("total_ev", 0.0) or 0.0)
            ebc_res = self.energy_balance.update_from_energy_summary(
                trace_id=f"{trace_id}_ebc",
                tick_id=tick_id,
                tick_index=int(self.tick_counter),
                total_er=total_er,
                total_ev=total_ev,
            )
            if isinstance(ebc_res, dict):
                ebc_data = ebc_res.get("data", {}) or {}
        except Exception as exc:
            ebc_data = {"error": str(exc)}
        report["energy_balance"] = ebc_data
        timing_steps_ms["energy_balance_ms"] = int((time.perf_counter() - t0) * 1000)

        # Merge EBC HDB scales into next_modulation (multiplicative).
        # 把 EBC 的 HDB scales 合并进 next_modulation（相乘合并，避免覆盖）。
        try:
            hdb_scales = ebc_data.get("hdb_scales_out", {}) if isinstance(ebc_data, dict) else {}
            if isinstance(hdb_scales, dict) and hdb_scales:
                hdb_mod = next_modulation.get("hdb", {}) if isinstance(next_modulation.get("hdb", {}), dict) else {}
                hdb_mod = dict(hdb_mod)
                for k, v in hdb_scales.items():
                    key = str(k or "").strip()
                    if not key:
                        continue
                    try:
                        scale = float(v or 1.0)
                    except Exception:
                        scale = 1.0
                    if not (scale > 0.0):
                        scale = 1.0
                    try:
                        existing = float(hdb_mod.get(key, 1.0) or 1.0)
                    except Exception:
                        existing = 1.0
                    hdb_mod[key] = round(float(existing) * float(scale), 8)
                next_modulation["hdb"] = hdb_mod
        except Exception:
            pass

        # Commit the final merged modulation for the next tick.
        # 最终写入下一 tick 调制包（已包含 Emotion + Action + EBC 的合并结果）。
        self._last_modulation = dict(next_modulation)

        # 总耗时（不含导出）：让导出的 JSON/HTML 报告也能携带“过程耗时”信息，便于找性能问题。
        total_logic_ms = int((time.perf_counter() - cycle_t0) * 1000)
        timing_steps_ms["total_logic_ms"] = int(total_logic_ms)
        report["timing"] = {
            "total_logic_ms": int(total_logic_ms),
            "steps_ms": dict(timing_steps_ms),
        }
        report["finished_at"] = int(time.time() * 1000)
        report["exports"] = self._export_report(trace_id, report)
        self._last_report = report
        self._report_history.append(report)
        history_limit = max(1, int(self._config.get("history_limit", 24)))
        if len(self._report_history) > history_limit:
            self._report_history = self._report_history[-history_limit:]
        if self._config.get("auto_open_html_report", False):
            self.open_report(trace_id, open_browser=True)
        return report

    def show_state_snapshot(self, top_k: str | int | None = None) -> str:
        snapshot = self.pool.get_state_snapshot(trace_id="cmd_snap", top_k=None if top_k == "all" else top_k)["data"]["snapshot"]
        return render_state_snapshot(snapshot, None if top_k == "all" else top_k)

    def show_hdb_snapshot(self) -> str:
        snapshot = self.hdb.get_hdb_snapshot(trace_id="cmd_hdb", top_k=12)["data"]
        return render_hdb_snapshot(snapshot)

    def show_structure(self, structure_id: str) -> str:
        result = self.hdb.query_structure_database(structure_id=structure_id, trace_id="cmd_st")
        if not result["success"]:
            return result["message"]
        return render_structure_report(result["data"])

    def show_group(self, group_id: str) -> str:
        result = self.hdb.query_group(group_id=group_id, trace_id="cmd_sg")
        if not result["success"]:
            return result["message"]
        return render_group_report(result["data"])

    def show_episodic(self, limit: int = 10) -> str:
        result = self.hdb.get_recent_episodic(trace_id="cmd_em", limit=limit)
        return render_episodic_report(result["data"])

    def open_report(self, target: str = "latest", *, open_browser: bool = True) -> str:
        html_path = self.output_dir / ("latest.html" if target in {"", "latest"} else f"{target}.html")
        if not html_path.exists():
            return f"报告不存在 / Report not found: {html_path}"
        opened = False
        if open_browser:
            try:
                opened = webbrowser.open(html_path.resolve().as_uri())
            except Exception:
                opened = False
        return json.dumps(
            {
                "html_path": str(html_path),
                "opened": opened,
            },
            ensure_ascii=False,
            indent=2,
        )

    def run_tick_cycles(self, count: int = 1) -> list[dict]:
        reports = []
        for _ in range(max(1, int(count))):
            reports.append(self.run_cycle(text=None))
        return reports

    def get_last_report(self) -> dict[str, Any] | None:
        return self._last_report

    def get_report(self, trace_id: str = "latest") -> dict[str, Any] | None:
        if trace_id in {"", "latest"}:
            return self._last_report
        report_path = self.output_dir / f"{trace_id}.json"
        if not report_path.exists():
            return None
        try:
            return json.loads(report_path.read_text(encoding="utf-8"))
        except Exception:
            return None

    def get_recent_cycle_summaries(self, limit: int | None = None) -> list[dict]:
        items = self._report_history[-(limit or len(self._report_history)) :]
        summaries = []
        for report in reversed(items):
            matched_structure_ids = list(report.get("stimulus_level", {}).get("result", {}).get("matched_structure_ids", []))
            new_structure_ids = list(report.get("stimulus_level", {}).get("result", {}).get("new_structure_ids", []))
            matched_group_ids = list(report.get("structure_level", {}).get("result", {}).get("matched_group_ids", []))
            new_group_ids = list(report.get("structure_level", {}).get("result", {}).get("new_group_ids", []))
            summaries.append(
                {
                    "trace_id": report.get("trace_id", ""),
                    "started_at": report.get("started_at", 0),
                    "finished_at": report.get("finished_at", 0),
                    "input_text": report.get("sensor", {}).get("input_text", ""),
                    "sensor_mode": report.get("sensor", {}).get("mode", ""),
                    "structure_rounds": report.get("structure_level", {}).get("result", {}).get("round_count", 0),
                    "stimulus_rounds": report.get("stimulus_level", {}).get("result", {}).get("round_count", 0),
                    "attention_memory_count": report.get("attention", {}).get("memory_item_count", 0),
                    "attention_consumed_total": report.get("attention", {}).get("consumed_total_energy", 0.0),
                    "matched_structures": matched_structure_ids,
                    "new_structures": new_structure_ids,
                    "matched_groups": matched_group_ids,
                    "new_groups": new_group_ids,
                    "matched_structure_refs": self._build_cycle_structure_refs(matched_structure_ids),
                    "new_structure_refs": self._build_cycle_structure_refs(new_structure_ids),
                    "matched_group_refs": self._build_cycle_group_refs(matched_group_ids),
                    "new_group_refs": self._build_cycle_group_refs(new_group_ids),
                    "total_delta_ev": report.get("induction", {}).get("result", {}).get("total_delta_ev", 0.0),
                    "memory_activation_applied_count": report.get("memory_activation", {}).get("apply_result", {}).get("applied_count", 0),
                    "memory_feedback_applied_count": report.get("memory_activation", {}).get("feedback_result", {}).get("applied_count", 0),
                    "memory_feedback_total_er": report.get("memory_activation", {}).get("feedback_result", {}).get("total_feedback_er", 0.0),
                    "memory_feedback_total_ev": report.get("memory_activation", {}).get("feedback_result", {}).get("total_feedback_ev", 0.0),
                    "memory_activation_total_er": report.get("memory_activation", {}).get("snapshot", {}).get("summary", {}).get("total_er", 0.0),
                    "memory_activation_total_ev": report.get("memory_activation", {}).get("snapshot", {}).get("summary", {}).get("total_ev", 0.0),
                    "cfs_signal_count": len(report.get("cognitive_feeling", {}).get("cfs_signals", []) or []),
                    "nt_state": dict(report.get("emotion", {}).get("nt_state_after", {}) or {}),
                }
            )
        return summaries

    def _build_cycle_structure_refs(self, structure_ids: list[str]) -> list[dict[str, Any]]:
        refs: list[dict[str, Any]] = []
        for structure_id in list(dict.fromkeys(structure_ids)):
            if not structure_id:
                continue
            structure_obj = self.hdb._structure_store.get(structure_id)
            display_text = structure_id
            signature = ""
            flat_tokens: list[str] = []
            if structure_obj:
                payload = structure_obj.get("structure", {})
                display_text = payload.get("display_text", structure_id)
                signature = payload.get("content_signature", "")
                flat_tokens = list(payload.get("flat_tokens", []))
            refs.append(
                {
                    "structure_id": structure_id,
                    "display_text": display_text,
                    "content_signature": signature,
                    "flat_tokens": flat_tokens,
                }
            )
        return refs

    def _build_cycle_group_refs(self, group_ids: list[str]) -> list[dict[str, Any]]:
        refs: list[dict[str, Any]] = []
        for group_id in list(dict.fromkeys(group_ids)):
            if not group_id:
                continue
            if group_id.startswith("sg_single_"):
                structure_id = group_id.removeprefix("sg_single_")
                refs.append(
                    {
                        "group_id": group_id,
                        "synthetic": True,
                        "required_structures": self._build_cycle_structure_refs([structure_id] if structure_id else []),
                        "bias_structures": [],
                    }
                )
                continue
            group_obj = self.hdb._group_store.get(group_id)
            required_ids = list(group_obj.get("required_structure_ids", [])) if group_obj else []
            bias_ids = list(group_obj.get("bias_structure_ids", [])) if group_obj else []
            refs.append(
                {
                    "group_id": group_id,
                    "required_structures": self._build_cycle_structure_refs(required_ids),
                    "bias_structures": self._build_cycle_structure_refs(bias_ids),
                }
            )
        return refs

    def get_dashboard_data(self) -> dict[str, Any]:
        snapshot_top_k = int(self._config.get("snapshot_top_k", 24))
        state_snapshot = self.pool.get_state_snapshot(
            trace_id="dashboard_state",
            top_k=snapshot_top_k,
        )["data"]["snapshot"]
        hdb_snapshot = self.hdb.get_hdb_snapshot(trace_id="dashboard_hdb", top_k=snapshot_top_k)["data"]
        sensor_runtime = self.sensor.get_runtime_snapshot(trace_id="dashboard_sensor")["data"]
        time_sensor_runtime = self.time_sensor.get_runtime_snapshot(trace_id="dashboard_time_sensor")["data"]
        # EBC runtime snapshot / 实虚能量平衡控制器运行态（用于前端实时展示）
        energy_balance_runtime = {}
        try:
            energy_balance_runtime = self.energy_balance.get_runtime_snapshot(trace_id="dashboard_energy_balance")  # type: ignore[attr-defined]
        except Exception:
            energy_balance_runtime = {}
        return {
            "meta": {
                "started_at": self._started_at,
                "tick_counter": self.tick_counter,
                "last_cycle_id": self._last_report.get("trace_id", "") if self._last_report else "",
                "output_dir": str(self.output_dir),
            },
            "last_report": self._last_report,
            "recent_cycles": self.get_recent_cycle_summaries(limit=int(self._config.get("history_limit", 24))),
            "state_snapshot": state_snapshot,
            "state_energy_summary": self._summarize_state_snapshot(state_snapshot),
            "hdb_snapshot": hdb_snapshot,
            "sensor_runtime": sensor_runtime,
            "time_sensor_runtime": time_sensor_runtime,
            "energy_balance_runtime": energy_balance_runtime,
            "module_configs": self.get_config_bundle(),
            "placeholder_modules": self.get_placeholder_modules(),
        }

    def get_state_snapshot_data(self, top_k: int | None = None) -> dict[str, Any]:
        snapshot = self.pool.get_state_snapshot(
            trace_id="api_state_snapshot",
            top_k=top_k,
        )["data"]["snapshot"]
        return {
            "snapshot": snapshot,
            "energy_summary": self._summarize_state_snapshot(snapshot),
        }

    def get_hdb_snapshot_data(self, top_k: int = 12) -> dict[str, Any]:
        return self.hdb.get_hdb_snapshot(trace_id="api_hdb_snapshot", top_k=top_k)["data"]

    def get_action_runtime_data(self) -> dict[str, Any]:
        """
        Action runtime snapshot for real-time monitoring.
        行动模块运行态快照：用于前端实时监控“已注册行动器/行动节点/阈值/驱动力”等。
        """
        if not getattr(self, "action", None):
            return {"enabled": False, "message": "行动模块未初始化 / action module not initialized", "data": {}}
        return self.action.get_runtime_snapshot(trace_id="api_action_runtime")["data"]

    def stop_action_nodes(
        self,
        *,
        mode: str,
        value: Any = None,
        hold_ticks: int = 2,
        reason: str = "manual_stop",
        trace_id: str = "api_action_stop",
    ) -> dict[str, Any]:
        """
        Stop/cancel action nodes (exposed to Web UI).
        行动停止/取消接口（供前端观测台调用）。
        """
        if not getattr(self, "action", None):
            return {"success": False, "code": "STATE_ERROR", "message": "行动模块未初始化 / action not initialized", "data": {}}
        res = self.action.stop_actions(
            trace_id=trace_id,
            mode=str(mode or ""),
            value=value,
            hold_ticks=int(hold_ticks or 0),
            reason=str(reason or "manual_stop"),
        )
        return res

    def get_structure_data(self, structure_id: str) -> dict[str, Any]:
        result = self.hdb.query_structure_database(structure_id=structure_id, trace_id="api_structure")
        if not result["success"]:
            raise ValueError(result["message"])
        return result["data"]

    def get_group_data(self, group_id: str) -> dict[str, Any]:
        if group_id.startswith("sg_single_"):
            structure_id = group_id.removeprefix("sg_single_")
            return {
                "group": {
                    "id": group_id,
                    "synthetic": True,
                    "group_kind": "implicit_single_st",
                    "required_structure_ids": [structure_id] if structure_id else [],
                    "bias_structure_ids": [],
                    "avg_energy_profile": {structure_id: 1.0} if structure_id else {},
                },
                "required_structures": self._build_cycle_structure_refs([structure_id] if structure_id else []),
                "bias_structures": [],
            }
        result = self.hdb.query_group(group_id=group_id, trace_id="api_group")
        if not result["success"]:
            raise ValueError(result["message"])
        return result["data"]

    def get_episodic_data(self, limit: int = 10) -> dict[str, Any]:
        return self.hdb.get_recent_episodic(trace_id="api_episodic", limit=limit)["data"]

    def run_check(self, target: str | None = None) -> str:
        result = self.hdb.self_check_hdb(trace_id="cmd_check", target_id=target)
        return render_check_report(result["data"])

    def run_repair(self, target: str) -> str:
        result = self.hdb.repair_hdb(
            trace_id="cmd_repair",
            target_id=target,
            repair_scope="targeted",
            background=False,
        )
        return render_repair_report(result["data"])

    def run_repair_all(self) -> str:
        result = self.hdb.repair_hdb(
            trace_id="cmd_repair_all",
            repair_scope="global_quick",
            background=True,
        )
        return render_repair_report(result["data"])

    def stop_repair(self, job_id: str) -> str:
        result = self.hdb.stop_repair_job(repair_job_id=job_id, trace_id="cmd_stop_repair")
        if not result["success"]:
            return result["message"]
        return render_repair_report(result["data"])

    def clear_hdb(self) -> str:
        result = self.hdb.clear_hdb(trace_id="cmd_clear_hdb", reason="interactive_reset", operator="researcher")
        return json.dumps(result["data"], ensure_ascii=False, indent=2)

    def clear_all(self) -> str:
        self.sensor.clear_echo_pool(trace_id="cmd_clear_sensor")
        self.pool.clear_state_pool(trace_id="cmd_clear_pool", reason="interactive_reset", operator="researcher")
        result = self.hdb.clear_hdb(trace_id="cmd_clear_all", reason="interactive_reset", operator="researcher")
        self._last_report = None
        self._report_history = []
        return json.dumps(result["data"], ensure_ascii=False, indent=2)

    def show_config(self) -> str:
        payload = {
            "sensor_backend": self.sensor.get_runtime_snapshot()["data"]["config_summary"]["tokenizer_backend"],
            "sensor_tokenizer_available": self.sensor.get_runtime_snapshot()["data"]["config_summary"]["tokenizer_available"],
            "hdb_core": {
                key: self.hdb._config[key]
                for key in [
                    "stimulus_level_max_rounds",
                    "structure_level_max_rounds",
                    "ev_propagation_threshold",
                    "er_induction_threshold",
                    "fallback_lookup_max_candidates",
                ]
            },
            "observatory": dict(self._config),
            "observatory_config_path": self._config_path,
            "output_dir": str(self.output_dir),
        }
        return json.dumps(payload, ensure_ascii=False, indent=2)

    def reload_all(self) -> str:
        self._config = self._build_config()
        payload = {
            "observatory": "OK",
            "text_sensor": self.sensor.reload_config(trace_id="cmd_reload_sensor")["code"],
            "time_sensor": self.time_sensor.reload_config(trace_id="cmd_reload_time_sensor")["code"],
            "state_pool": self.pool.reload_config(trace_id="cmd_reload_pool")["code"],
            "hdb": self.hdb.reload_config(trace_id="cmd_reload_hdb")["code"],
            "attention": self.attention.reload_config(trace_id="cmd_reload_attention")["code"],
            "cognitive_feeling": self.cfs.reload_config(trace_id="cmd_reload_cfs")["code"],
            "emotion": self.emotion.reload_config(trace_id="cmd_reload_emotion")["code"],
            "innate_script": self.iesm.reload_config(trace_id="cmd_reload_iesm")["code"],
            "action": self.action.reload_config(trace_id="cmd_reload_action")["code"],
        }
        self._apply_runtime_overrides()
        return json.dumps(payload, ensure_ascii=False, indent=2)

    def loop(self) -> None:
        self.print_header()
        try:
            while True:
                line = input("\nAP-OBS> ").strip().lstrip("\ufeff\ufffe")
                if not line:
                    continue
                if line in {"quit", "exit"}:
                    break
                if line == "help":
                    print(format_help())
                    continue
                if line.startswith("text "):
                    print(render_cycle_report(self.run_cycle(text=line[5:])))
                    continue

                parts = shlex.split(line)
                cmd = parts[0]
                if cmd == "tick":
                    count = int(parts[1]) if len(parts) > 1 else 1
                    for _ in range(max(1, count)):
                        print(render_cycle_report(self.run_cycle(text=None)))
                elif cmd == "snap":
                    arg = parts[1] if len(parts) > 1 else str(self._config["snapshot_top_k"])
                    top_k: str | int | None = "all" if arg == "all" else int(arg)
                    print(self.show_state_snapshot(top_k))
                elif cmd == "hdb":
                    print(self.show_hdb_snapshot())
                elif cmd == "st" and len(parts) > 1:
                    print(self.show_structure(parts[1]))
                elif cmd == "sg" and len(parts) > 1:
                    print(self.show_group(parts[1]))
                elif cmd == "em":
                    limit = int(parts[1]) if len(parts) > 1 else 10
                    print(self.show_episodic(limit))
                elif cmd == "check":
                    target = parts[1] if len(parts) > 1 else None
                    print(self.run_check(target))
                elif cmd == "repair" and len(parts) > 1:
                    print(self.run_repair(parts[1]))
                elif cmd == "repair_all":
                    print(self.run_repair_all())
                elif cmd == "stop_repair" and len(parts) > 1:
                    print(self.stop_repair(parts[1]))
                elif cmd == "clear_hdb":
                    print(self.clear_hdb())
                elif cmd == "clear_all":
                    print(self.clear_all())
                elif cmd == "config":
                    print(self.show_config())
                elif cmd == "reload":
                    print(self.reload_all())
                elif cmd == "open_report":
                    target = parts[1] if len(parts) > 1 else "latest"
                    print(self.open_report(target))
                else:
                    print(render_cycle_report(self.run_cycle(text=line)))
        finally:
            self.close()

    def _build_config(self, config_override: dict | None = None) -> dict:
        config = dict(DEFAULT_CONFIG)
        config.update(_load_yaml_config(self._config_path))
        if config_override:
            config.update(config_override)
        return config

    def _sensor_config_override(self) -> dict[str, Any]:
        return {
            "default_mode": self._config.get("sensor_default_mode", "advanced"),
            "tokenizer_backend": self._config.get("sensor_tokenizer_backend", "jieba"),
            "enable_token_output": bool(self._config.get("sensor_enable_token_output", True)),
            "enable_char_output": bool(self._config.get("sensor_enable_char_output", False)),
            "enable_echo": bool(self._config.get("sensor_enable_echo", True)),
            "include_echoes_in_stimulus_packet_objects": bool(self._config.get("sensor_include_echoes_in_packet", True)),
        }

    def _state_pool_config_override(self) -> dict[str, Any]:
        return {
            "enable_placeholder_interfaces": bool(self._config.get("state_pool_enable_placeholder_interfaces", False)),
            "enable_script_broadcast": bool(self._config.get("state_pool_enable_script_broadcast", False)),
        }

    def _hdb_config_override(self) -> dict[str, Any]:
        override = {
            "enable_background_repair": bool(self._config.get("hdb_enable_background_repair", True)),
        }
        if self._config.get("hdb_data_dir"):
            override["data_dir"] = self._config.get("hdb_data_dir")
        return override

    def _apply_hdb_modulation_for_tick(
        self,
        *,
        modulation: dict[str, Any] | None,
        trace_id: str,
        tick_id: str,
    ) -> dict[str, Any]:
        """
        Apply HDB modulation scales for the current tick (best-effort).
        应用本 tick 的 HDB 调制缩放系数（尽力而为，不影响主流程）。

        Design / 设计要点：
        - modulation 来自上一 tick 的 EMgr/Action 输出（self._last_modulation["hdb"]）。
        - 为避免“缩放累积漂移”，我们会先把目标字段重置回基准值（self._hdb_config_base），
          然后再按 scale 计算 effective 值。
        - 返回 applied 详情供前端审计/验收（你可以在观测台直接看到 base/scale/effective）。
        """
        mod = modulation if isinstance(modulation, dict) else {}
        base = getattr(self, "_hdb_config_base", None)
        if not isinstance(base, dict) or not base:
            base = dict(getattr(self.hdb, "_config", {}) or {})
            self._hdb_config_base = dict(base)

        applied: dict[str, Any] = {}

        def apply_scale(scale_key: str, cfg_key: str, *, min_value: float | None = None) -> None:
            # Reset to baseline first (avoid drift).
            try:
                base_val = float(base.get(cfg_key, self.hdb._config.get(cfg_key, 0.0) or 0.0) or 0.0)
            except Exception:
                base_val = float(self.hdb._config.get(cfg_key, 0.0) or 0.0)
            self.hdb._config[cfg_key] = base_val

            try:
                scale = float(mod.get(scale_key, 1.0) or 1.0)
            except Exception:
                scale = 1.0
            if not (scale > 0.0):
                scale = 1.0

            eff = float(base_val) * float(scale)
            if min_value is not None:
                eff = max(float(min_value), float(eff))
            self.hdb._config[cfg_key] = float(eff)

            # Only record when non-trivial (still reset even if trivial).
            if abs(float(scale) - 1.0) > 1e-9:
                applied[cfg_key] = {
                    "base": round(float(base_val), 8),
                    "scale": round(float(scale), 8),
                    "effective": round(float(eff), 8),
                    "scale_key": scale_key,
                }

        # Weight / 学习力度
        apply_scale("base_weight_er_gain_scale", "base_weight_er_gain", min_value=0.0)
        apply_scale("base_weight_ev_wear_scale", "base_weight_ev_wear", min_value=0.0)
        # Induction / 传播与诱发
        apply_scale("ev_propagation_threshold_scale", "ev_propagation_threshold", min_value=0.0)
        apply_scale("ev_propagation_ratio_scale", "ev_propagation_ratio", min_value=0.0)
        apply_scale("er_induction_ratio_scale", "er_induction_ratio", min_value=0.0)

        try:
            # Update only the affected engines (fast enough for prototype).
            # 只更新受影响的引擎（原型阶段足够快）。
            self.hdb._weight.update_config(self.hdb._config)
            self.hdb._stimulus.update_config(self.hdb._config)
            self.hdb._structure_retrieval.update_config(self.hdb._config)
            self.hdb._induction.update_config(self.hdb._config)
        except Exception as exc:
            return {"error": str(exc), "applied": applied}

        return {"applied": applied, "base_refreshed": True, "tick_id": tick_id, "trace_id": trace_id}

    def _attention_config_override(self) -> dict[str, Any]:
        """
        观测台对注意力模块的运行时覆盖。

        注意：观测台历史配置中仍保留 attention_stub_* 命名，本函数将其映射为正式模块字段。
        """
        return {
            "top_n": int(self._config.get("attention_top_n", 16)),
            "consume_energy": bool(self._config.get("attention_stub_consume_energy", True)),
            "memory_energy_ratio": float(self._config.get("attention_memory_energy_ratio", 0.5)),
        }

    def _apply_runtime_overrides(self) -> None:
        sensor_override = self._sensor_config_override()
        self.sensor._config.update(sensor_override)
        self.sensor._normalizer.update_config(self.sensor._config)
        self.sensor._segmenter.update_config(self.sensor._config)
        self.sensor._scorer.update_config(self.sensor._config)
        self.sensor._echo_mgr.update_config(self.sensor._config)
        self.sensor._logger.update_config(
            log_dir=self.sensor._config.get("log_dir", ""),
            max_file_bytes=self.sensor._config.get("log_max_file_bytes", 0),
        )

        pool_override = self._state_pool_config_override()
        self.pool._config.update(pool_override)
        self.pool._store.update_config(self.pool._config)
        self.pool._energy.update_config(self.pool._config)
        self.pool._neutralization.update_config(self.pool._config)
        self.pool._merge.update_config(self.pool._config)
        self.pool._binding.update_config(self.pool._config)
        self.pool._maintenance.update_config(self.pool._config)
        self.pool._snapshot.update_config(self.pool._config)
        self.pool._history.update_config(self.pool._config)
        self.pool._logger.update_config(
            log_dir=self.pool._config.get("log_dir", ""),
            max_file_bytes=self.pool._config.get("log_max_file_bytes", 0),
        )

        hdb_override = self._hdb_config_override()
        self.hdb._config.update(hdb_override)
        self.hdb._weight.update_config(self.hdb._config)
        self.hdb._pointer_index.update_config(self.hdb._config)
        self.hdb._maintenance.update_config(self.hdb._config)
        self.hdb._snapshot.update_config(self.hdb._config)
        self.hdb._stimulus.update_config(self.hdb._config)
        self.hdb._structure_retrieval.update_config(self.hdb._config)
        self.hdb._induction.update_config(self.hdb._config)
        self.hdb._memory_activation_store.update_config(self.hdb._config)
        self.hdb._self_check.update_config(self.hdb._config)
        self.hdb._delete.update_config(self.hdb._config)
        self.hdb._repair.update_config(self.hdb._config)
        self.hdb._logger.update_config(
            log_dir=self.hdb._config.get("log_dir", ""),
            max_file_bytes=int(self.hdb._config.get("log_max_file_bytes", 0)),
        )
        # Refresh baseline after config changes (avoid drift in per-tick modulation).
        # 配置发生变化后刷新 HDB 基准配置（避免 per-tick 调制出现累计漂移）。
        self._hdb_config_base = dict(self.hdb._config)

        attention_override = self._attention_config_override()
        self.attention._config.update(attention_override)
        self.attention._logger.update_config(
            log_dir=self.attention._config.get("log_dir", ""),
            max_file_bytes=int(self.attention._config.get("log_max_file_bytes", 0)),
        )

    def _module_config_specs(self) -> dict[str, dict[str, Any]]:
        return {
            "observatory": {
                "path": self._config_path,
                "defaults": dict(DEFAULT_CONFIG),
                "effective": lambda: dict(self._config),
                "runtime_override": lambda: {},
            },
            "text_sensor": {
                "path": self.sensor._config_path,
                "defaults": dict(TEXT_SENSOR_DEFAULT_CONFIG),
                "effective": lambda: dict(self.sensor._config),
                "runtime_override": self._sensor_config_override,
            },
            "time_sensor": {
                "path": self.time_sensor._config_path,
                "defaults": dict(TIME_SENSOR_DEFAULT_CONFIG),
                "effective": lambda: dict(self.time_sensor._config),
                "runtime_override": lambda: {},
            },
            "state_pool": {
                "path": self.pool._config_path,
                "defaults": dict(STATE_POOL_DEFAULT_CONFIG),
                "effective": lambda: dict(self.pool._config),
                "runtime_override": self._state_pool_config_override,
            },
            "hdb": {
                "path": self.hdb._config_path,
                "defaults": dict(HDB_DEFAULT_CONFIG),
                "effective": lambda: dict(self.hdb._config),
                "runtime_override": self._hdb_config_override,
            },
            "attention": {
                "path": self.attention._config_path,
                "defaults": dict(ATTENTION_DEFAULT_CONFIG),
                "effective": lambda: dict(self.attention._config),
                "runtime_override": self._attention_config_override,
            },
            "cognitive_feeling": {
                "path": self.cfs._config_path,
                "defaults": dict(CFS_DEFAULT_CONFIG),
                "effective": lambda: dict(self.cfs._config),
                "runtime_override": lambda: {},
            },
            "emotion": {
                "path": self.emotion._config_path,
                "defaults": dict(EMOTION_DEFAULT_CONFIG),
                "effective": lambda: dict(self.emotion._config),
                "runtime_override": lambda: {},
            },
            "innate_script": {
                "path": self.iesm._config_path,
                "defaults": dict(IESM_DEFAULT_CONFIG),
                "effective": lambda: dict(self.iesm._config),
                "runtime_override": lambda: {},
            },
            "action": {
                "path": self.action._config_path,
                "defaults": dict(ACTION_DEFAULT_CONFIG),
                "effective": lambda: dict(self.action._config),
                "runtime_override": lambda: {},
            },
            "energy_balance": {
                "path": self.energy_balance._config_path,
                "defaults": dict(ENERGY_BALANCE_DEFAULT_CONFIG),
                "effective": lambda: dict(self.energy_balance._config),
                "runtime_override": lambda: {},
            },
        }

    def get_config_bundle(self) -> dict[str, Any]:
        bundle: dict[str, Any] = {}
        for module_name, spec in self._module_config_specs().items():
            bundle[module_name] = build_config_view(
                module_name=module_name,
                path=spec["path"],
                defaults=spec["defaults"],
                file_values=load_yaml_dict(spec["path"]),
                effective=spec["effective"](),
                runtime_override=spec["runtime_override"](),
            )
        return bundle

    def save_module_config(self, module_name: str, values: dict[str, Any]) -> dict[str, Any]:
        normalized_name = str(module_name).strip().lower()
        specs = self._module_config_specs()
        if normalized_name not in specs:
            raise ValueError(f"unsupported module_name: {module_name}")
        spec = specs[normalized_name]
        coerced, rejected = coerce_updates_by_defaults(spec["defaults"], values or {})
        merged = save_annotated_config(
            path=spec["path"],
            defaults=spec["defaults"],
            updates=coerced,
        )
        self.reload_all()
        return {
            "module": normalized_name,
            "path": spec["path"],
            "saved_values": coerced,
            "rejected_values": rejected,
            "file_values": merged,
            "config_bundle": self.get_config_bundle(),
        }

    # =============================================================== #
    # Innate Rules UI API / 先天规则前端 API                           #
    # =============================================================== #

    def get_innate_rules_data(self) -> dict[str, Any]:
        """Expose IESM rules bundle for the web UI / 给前端展示规则文件信息。"""
        return self.iesm.get_rules_bundle(trace_id="api_innate_rules", include_file_yaml=True)["data"]

    def validate_innate_rules(self, *, doc: dict[str, Any] | None = None, yaml_text: str | None = None) -> dict[str, Any]:
        """Validate doc/yaml and return normalized preview / 校验并返回规范化预览。"""
        result = self.iesm.validate_rules(trace_id="api_innate_rules_validate", doc=doc, yaml_text=yaml_text)
        data = result.get("data", {}) or {}
        return {
            "valid": bool(result.get("success", False)),
            "code": result.get("code", ""),
            "message": result.get("message", ""),
            "errors": list(data.get("errors", []) or []),
            "warnings": list(data.get("warnings", []) or []),
            "normalized_doc": data.get("normalized_doc", {}) or {},
            "yaml_preview": str(data.get("yaml_preview", "") or ""),
        }

    def save_innate_rules(self, *, doc: dict[str, Any] | None = None, yaml_text: str | None = None) -> dict[str, Any]:
        """Validate + save + reload / 校验+保存+热加载。"""
        result = self.iesm.save_rules(trace_id="api_innate_rules_save", doc=doc, yaml_text=yaml_text)
        return {
            "saved": bool(result.get("success", False)),
            "code": result.get("code", ""),
            "message": result.get("message", ""),
            "data": result.get("data", {}) or {},
            "error": result.get("error", {}) or {},
        }

    def reload_innate_rules(self) -> dict[str, Any]:
        """Reload rules from disk / 从磁盘热加载规则文件。"""
        result = self.iesm.reload_rules(trace_id="api_innate_rules_reload")
        return {
            "reloaded_ok": bool(result.get("success", False)),
            "code": result.get("code", ""),
            "message": result.get("message", ""),
            "data": result.get("data", {}) or {},
        }

    def simulate_innate_rules(self) -> dict[str, Any]:
        """
        Simulate rules on the last report context (dry-run).
        用最近一轮 report 上下文做规则模拟（dry-run，不修改冷却记账）。
        """
        if not self._last_report:
            return {"ok": False, "message": "no last report yet / 暂无最近轮次报告"}
        trace_id = str(self._last_report.get("trace_id", "latest") or "latest")
        tick_id = trace_id

        cfs_signals = list((self._last_report.get("cognitive_feeling", {}) or {}).get("cfs_signals", []) or [])
        maint_events = list((self._last_report.get("maintenance", {}) or {}).get("events", []) or [])
        apply_events = list((self._last_report.get("pool_apply", {}) or {}).get("events", []) or [])

        try:
            maint_packet = self.pool._snapshot.build_script_check_packet(
                events=maint_events,
                pool_store=self.pool._store,
                trace_id=f"{trace_id}_sim_maint",
                tick_id=tick_id,
            )
        except Exception:
            maint_packet = {}
        try:
            apply_packet = self.pool._snapshot.build_script_check_packet(
                events=apply_events,
                pool_store=self.pool._store,
                trace_id=f"{trace_id}_sim_apply",
                tick_id=tick_id,
            )
        except Exception:
            apply_packet = {}

        # Build context from the last report (prefer report snapshots), so metric predicates can work in simulate.
        # 用最近一轮 report 构造上下文（优先使用 report 快照），以便 metric 条件在模拟里也能工作。
        pool_snapshot = (self._last_report.get("final_state", {}) or {}).get("state_snapshot") or {}
        emotion_state = self._last_report.get("emotion", {}) or {}
        sim_context = self._build_innate_rules_context(
            report=self._last_report,
            pool_snapshot=pool_snapshot if isinstance(pool_snapshot, dict) else None,
            emotion_state=emotion_state if isinstance(emotion_state, dict) else None,
            cfs_signals=cfs_signals,
            trace_id=trace_id,
            tick_id=tick_id,
        )

        sim = self.iesm.run_tick_rules(
            trace_id=trace_id,
            tick_id=tick_id,
            # Provide a real tick_index so delta/avg_rate metrics can use history (dry-run won't mutate runtime_state).
            # 提供真实 tick_index：让 delta/avg_rate 指标能使用历史（dry-run 不会修改运行态记账）。
            tick_index=int(self.tick_counter),
            cfs_signals=cfs_signals,
            state_windows=[
                {"stage": "maintenance", "packet": maint_packet},
                {"stage": "pool_apply", "packet": apply_packet},
            ],
            context=sim_context,
            dry_run=True,
        )
        return {"ok": bool(sim.get("success", False)), "code": sim.get("code", ""), "message": sim.get("message", ""), "data": sim.get("data", {}) or {}}

    # ================================================================== #
    # Innate Rules Context + Pool Effects                                 #
    # 先天规则：上下文构造 + 状态池效果落地                                 #
    # ================================================================== #

    def _build_innate_rules_context(
        self,
        *,
        report: dict[str, Any] | None,
        pool_snapshot: dict[str, Any] | None,
        emotion_state: dict[str, Any] | None,
        cfs_signals: list[dict] | None,
        trace_id: str,
        tick_id: str,
    ) -> dict[str, Any]:
        """
        Build the runtime context for IESM metric predicates.
        构造 IESM metric 条件所需的运行态上下文。

        Context shape (subset, MVP) / 上下文结构（子集，原型阶段）：
          - pool: total_er/total_ev/total_cp_delta/total_cp_abs/energy_concentration/effective_peak_count/complexity_score
          - pool_items: list of item summaries (selectors use display/attrs/etc.)
          - cam: size/energy_concentration (当前注意记忆体摘要)
          - memory_activation: item_count/total_ev (记忆赋能池摘要)
          - emotion: {nt:{}, rwd, pun}
          - stimulus: {residual_ratio}
          - retrieval: {stimulus:{best_match_score, grasp_score}}

        Notes / 注意：
        - IESM runs before EMgr update in run_cycle, so emotion_state may be None.
          在 run_cycle 中 IESM 早于 EMgr 更新，因此 emotion_state 可能为空；此时使用“上一轮情绪快照”
          + “基于本 tick CFS 的 rwd/pun 即时估计”。
        """
        report = report if isinstance(report, dict) else {}
        cfs_signals = list(cfs_signals or [])

        # ---- pool_items ----
        pool_items: list[dict[str, Any]] = []
        if isinstance(pool_snapshot, dict) and isinstance(pool_snapshot.get("top_items"), list):
            for row in pool_snapshot.get("top_items", []) or []:
                if isinstance(row, dict):
                    pool_items.append(dict(row))
        else:
            # Use live pool store (no sort) to avoid heavy snapshots during tick runtime.
            # 使用 live store（不排序），避免 tick 运行时做重快照。
            try:
                all_items = list(self.pool._store.get_all())
            except Exception:
                all_items = []
            for item in all_items:
                if not isinstance(item, dict):
                    continue
                try:
                    summary = self.pool._snapshot._build_top_item_summary(item)  # type: ignore[attr-defined]
                    if isinstance(summary, dict):
                        pool_items.append(summary)
                except Exception:
                    continue

        # Ensure total_energy is available for selector.top_n.
        # 确保 total_energy 存在，供 selector.top_n 使用。
        for row in pool_items:
            try:
                er = float(row.get("er", 0.0) or 0.0)
                ev = float(row.get("ev", 0.0) or 0.0)
                row["total_energy"] = round(max(0.0, er) + max(0.0, ev), 8)
            except Exception:
                row["total_energy"] = 0.0

        total_er = round(sum(float(r.get("er", 0.0) or 0.0) for r in pool_items), 8)
        total_ev = round(sum(float(r.get("ev", 0.0) or 0.0) for r in pool_items), 8)
        total_cp_delta = round(sum(float(r.get("cp_delta", 0.0) or 0.0) for r in pool_items), 8)
        total_cp_abs = round(sum(float(r.get("cp_abs", 0.0) or 0.0) for r in pool_items), 8)

        # Energy concentration (Herfindahl index on (er+ev)).
        # 能量聚集度（Herfindahl 指数，基于 er+ev 归一化平方和）：
        # - 越接近 1：能量越集中于少数对象（波峰更少）
        # - 越接近 1/N：能量越均匀分散（波峰更多）
        energies = [max(0.0, float(r.get("total_energy", 0.0) or 0.0)) for r in pool_items]
        e_sum = float(sum(energies))
        if e_sum > 1e-12:
            energy_concentration = round(sum((e / e_sum) ** 2 for e in energies if e > 1e-12), 8)
        else:
            energy_concentration = 0.0

        # Effective peak count (inverse Herfindahl), roughly interpretable as "number of peaks".
        # 有效波峰数量（Herfindahl 逆）：可粗略理解为“波峰个数”。
        #
        # - energy_concentration ≈ 1   => effective_peak_count ≈ 1（能量集中在极少数对象）
        # - energy_concentration ≈ 1/N => effective_peak_count ≈ N（能量更均匀分散）
        if float(energy_concentration) > 1e-12:
            effective_peak_count = float(round(1.0 / float(energy_concentration), 8))
        else:
            effective_peak_count = 0.0

        # ---- CAM (Current Attention Memory) ----
        # CAM 指标（当前注意记忆体）：用于“繁/简（复杂度）”等规则触发与行动调制。
        cam_size = 0
        cam_concentration = 0.0
        try:
            att = report.get("attention", {}) if isinstance(report.get("attention", {}), dict) else {}
            cam_size = int(att.get("memory_item_count", 0) or 0)
            cam_items = list(att.get("top_items", []) or [])
            cam_energies = []
            for it in cam_items:
                if not isinstance(it, dict):
                    continue
                # Prefer extracted memory energy if available; fall back to current er/ev.
                # 优先用“抽取到 CAM 的能量”字段；否则用当前 er/ev。
                er = float(it.get("memory_er", it.get("er", 0.0)) or 0.0)
                ev = float(it.get("memory_ev", it.get("ev", 0.0)) or 0.0)
                cam_energies.append(max(0.0, er) + max(0.0, ev))
            s = float(sum(cam_energies))
            if s > 1e-12:
                cam_concentration = float(round(sum((e / s) ** 2 for e in cam_energies if e > 1e-12), 8))
            else:
                cam_concentration = 0.0
        except Exception:
            cam_size = 0
            cam_concentration = 0.0

        # ---- Derived: complexity_score (繁/简综合复杂度，0~1) ----
        # 对齐理论 3.8.3：
        # - “繁/简”不仅与 CAM（当前注意记忆体）大小有关，也与状态池能量波峰数量有关；
        # - 因此这里构造一个可解释的综合分：complexity_score ∈ [0,1]
        #   - size_norm：CAM size 归一化（默认 6~24）
        #   - peak_norm：有效波峰数量（1/Herfindahl）归一化（默认 1~12）
        #
        # 注意：
        # - 这里是“指标构造”，阈值与触发策略仍由 IESM 规则文件决定；
        # - 数值范围选取与归一化口径可在后续按验收数据再调参；
        # - effective_peak_count 来源于 pool_items（通常是状态池 top_k 摘要），属于“可解释近似”。
        try:
            size_min = 6.0
            size_max = 24.0
            if size_max <= size_min:
                size_max = size_min + 1.0
            size_norm = (float(cam_size) - size_min) / (size_max - size_min)
            size_norm = max(0.0, min(1.0, float(size_norm)))

            peak_min = 1.0
            peak_max = 12.0
            if peak_max <= peak_min:
                peak_max = peak_min + 1.0
            peak_norm = (float(effective_peak_count) - peak_min) / (peak_max - peak_min)
            peak_norm = max(0.0, min(1.0, float(peak_norm)))

            # Weighted sum / 加权求和（可解释、可调参）
            complexity_score = 0.55 * size_norm + 0.45 * peak_norm
            complexity_score = max(0.0, min(1.0, float(complexity_score)))
            complexity_score = float(round(complexity_score, 8))
        except Exception:
            complexity_score = 0.0

        # ---- Memory Activation Pool (MAP) ----
        # MAP 指标（记忆赋能池）：用于回忆行动等规则触发的“有无候选”门控。
        map_item_count = 0
        map_total_ev = 0.0
        try:
            snap = (report.get("memory_activation", {}) or {}).get("snapshot", {}) or {}
            items = list(snap.get("items", []) or [])
            map_item_count = len([x for x in items if isinstance(x, dict)])
            map_total_ev = float(((snap.get("summary", {}) or {}).get("total_ev", 0.0) or 0.0))
        except Exception:
            map_item_count = 0
            map_total_ev = 0.0

        # ---- stimulus metrics ----
        # Residual ratio: (after stimulus retrieval) / (before stimulus retrieval).
        # 刺激级剩余能量比例：(刺激级查存结束后残余) / (刺激级查存开始前残余)。
        residual_ratio = 0.0
        try:
            before = report.get("cache_neutralization", {}).get("residual_packet", {}) or {}
            after = report.get("pool_apply", {}).get("landed_packet", {}) or {}
            before_total = float(before.get("total_er", 0.0) or 0.0) + float(before.get("total_ev", 0.0) or 0.0)
            after_total = float(after.get("total_er", 0.0) or 0.0) + float(after.get("total_ev", 0.0) or 0.0)
            residual_ratio = float(after_total / before_total) if before_total > 1e-12 else 0.0
        except Exception:
            residual_ratio = 0.0

        best_match_score = 0.0
        match_scores: dict[str, float] = {}
        best_match_target_id = ""
        best_match_target_display = ""
        match_displays: dict[str, str] = {}
        try:
            rounds = list(
                (report.get("stimulus_level", {}) or {})
                .get("result", {})
                .get("debug", {})
                .get("round_details", [])
                or []
            )
            for rd in rounds:
                if not isinstance(rd, dict):
                    continue
                sm = rd.get("selected_match") or {}
                if not isinstance(sm, dict):
                    continue
                score = float(sm.get("match_score", 0.0) or 0.0)
                best_match_score = max(best_match_score, score)

                # Per-target match score map (best-effort).
                # 匹配分数（按目标映射，尽力而为）：用于“查存一体过程匹配分数（带目标对象）”类条件。
                sid = str(
                    sm.get("structure_id", "")
                    or sm.get("structure_db_id", "")
                    or sm.get("structure_signature", "")
                    or ""
                ).strip()
                if sid:
                    match_scores[sid] = max(float(match_scores.get(sid, 0.0) or 0.0), float(score))
            best_match_score = round(float(best_match_score), 8)
            if match_scores:
                best_match_target_id = max(match_scores.items(), key=lambda kv: float(kv[1] or 0.0))[0]

            # Best-effort: resolve display text for retrieval targets (st_*).
            # 尽力补全“检索目标”的可读展示（主要是 st_* 结构ID）。
            try:
                # Helper: structure_id -> display_text
                def _st_display(sid: str) -> str:
                    if not sid or not str(sid).startswith("st_"):
                        return ""
                    st_obj = self.hdb._structure_store.get(str(sid))  # type: ignore[attr-defined]
                    if not isinstance(st_obj, dict):
                        return ""
                    block = st_obj.get("structure", {}) if isinstance(st_obj.get("structure", {}), dict) else {}
                    return str(block.get("display_text", "") or sid)

                if best_match_target_id:
                    best_match_target_display = _st_display(best_match_target_id) or str(best_match_target_id)
                for sid in list(match_scores.keys()):
                    disp = _st_display(sid)
                    if disp:
                        match_displays[str(sid)] = disp
            except Exception:
                best_match_target_display = best_match_target_display or ""
                match_displays = match_displays or {}
        except Exception:
            best_match_score = 0.0
            match_scores = {}
            best_match_target_id = ""
            best_match_target_display = ""
            match_displays = {}

        # ---- structure-level retrieval metrics ----
        # 结构级查存一体匹配分数（按“结构组 group_id”统计）
        #
        # 说明：
        # - 刺激级查存的目标通常是 structure_id（st_*）
        # - 结构级查存的目标通常是 group_id（sg_*）
        # - 两者都支持作为 IESM metric 条件的输入（例如“置信度/把握感”）。
        structure_best_match_score = 0.0
        structure_match_scores: dict[str, float] = {}
        structure_best_match_target_id = ""
        structure_best_match_target_display = ""
        structure_match_displays: dict[str, str] = {}
        try:
            rounds = list(
                (report.get("structure_level", {}) or {})
                .get("result", {})
                .get("debug", {})
                .get("round_details", [])
                or []
            )
            for rd in rounds:
                if not isinstance(rd, dict):
                    continue
                # Current HDB structure-level debug uses "selected_group" as the main selected record.
                # 当前 HDB 结构级 debug 使用 selected_group 作为“本轮命中结果”的主要字段。
                sel = rd.get("selected_group") or rd.get("selected_match") or {}
                if not isinstance(sel, dict):
                    continue
                try:
                    score = float(
                        sel.get("score", sel.get("competition_score", sel.get("match_score", 0.0)))
                        or 0.0
                    )
                except Exception:
                    score = 0.0
                structure_best_match_score = max(structure_best_match_score, score)

                gid = str(sel.get("group_id", "") or sel.get("id", "") or "").strip()
                if gid:
                    structure_match_scores[gid] = max(float(structure_match_scores.get(gid, 0.0) or 0.0), float(score))
            structure_best_match_score = round(float(structure_best_match_score), 8)
            if structure_match_scores:
                structure_best_match_target_id = max(structure_match_scores.items(), key=lambda kv: float(kv[1] or 0.0))[0]

            # Best-effort display for group ids (sg_*). GroupStore has no direct display_text,
            # so we keep a readable fallback: "sg_xxx" (future: derive from required structures).
            # 结构组显示兜底：GroupStore 没有直接的 display_text，因此先用可读 fallback（后续可由 required_structures 派生）。
            if structure_best_match_target_id:
                structure_best_match_target_display = str(structure_best_match_target_id)
            for gid in list(structure_match_scores.keys()):
                if str(gid):
                    structure_match_displays[str(gid)] = str(gid)
        except Exception:
            structure_best_match_score = 0.0
            structure_match_scores = {}
            structure_best_match_target_id = ""
            structure_best_match_target_display = ""
            structure_match_displays = {}

        # ---- Derived: grasp_score（把握感/置信度综合得分，0~1） ----
        # 对齐理论 3.8.3（把握感/置信度口径要点）：
        # - 虚能量稳定性（EV 稳定）：预测图景是否稳定不乱跳
        # - 关键结构的虚能量覆盖程度（EV 覆盖）：预测是否集中在关键对象上（而不是过度发散）
        # - 认知压下降趋势（|CP| 下降）：偏差是否在收敛（更“对得上”）
        # - 匹配质量与残余比例：过程层面的直接证据
        #
        # 原型阶段落地为一个“可解释、可调参”的组合分数：
        # - match_norm：刺激级 best_match_score 归一化
        # - residual_complement：1 - residual_ratio（残余越少，表示本轮越“对得上”）
        # - structure_norm：结构级 best_match_score 归一化（若结构级未跑起来，不作为惩罚项）
        # - ev_stability：关键对象 EV 相对变化率越小越稳定
        # - ev_coverage：关键对象 EV / pool_total_ev（越高越集中）
        # - cp_relief：关键对象 |CP| 下降速度越快越好（用 cp_abs_rate 的负值近似）
        try:
            # best_match_score 当前实现通常在 0~1（越高越匹配）。
            # 为避免“几乎总是 1.0”的饱和，先做一个可调参的线性归一化区间。
            m_lo = 0.40
            m_hi = 0.95
            if m_hi <= m_lo:
                m_hi = m_lo + 1e-6
            match_norm = (float(best_match_score) - float(m_lo)) / (float(m_hi) - float(m_lo))
            match_norm = max(0.0, min(1.0, float(match_norm)))

            rr = float(residual_ratio)
            rr = max(0.0, min(1.0, rr))
            residual_complement = 1.0 - rr

            # 结构级匹配分数：如果当前没有结构级匹配候选，就不把它当作惩罚项，
            # 避免“结构级流程尚未跑起来”时把 grasp 全部压低。
            #
            # 若未来结构级匹配常态化，可进一步提高其权重。
            has_structure = bool(structure_match_scores) or float(structure_best_match_score) > 1e-9
            if has_structure:
                s_lo = 0.20
                s_hi = 0.90
                if s_hi <= s_lo:
                    s_hi = s_lo + 1e-6
                structure_norm = (float(structure_best_match_score) - float(s_lo)) / (float(s_hi) - float(s_lo))
                structure_norm = max(0.0, min(1.0, float(structure_norm)))
            else:
                structure_norm = 0.0

            # ---- 关键对象（best_match_target）上的 EV 稳定性 / 覆盖 / CP 下降 ----
            # 说明：
            # - best_match_target_id 典型是 st_*（结构ID）；但状态池对象可能是 SA/ST 合并后以 st_* 为主身份，
            #   因此这里用 ref_alias_ids 做一次兜底匹配。
            best_row: dict[str, Any] | None = None
            if best_match_target_id:
                for row in pool_items:
                    if not isinstance(row, dict):
                        continue
                    rid = str(row.get("ref_object_id", "") or "").strip()
                    if rid and rid == str(best_match_target_id):
                        best_row = row
                        break
                    aliases = row.get("ref_alias_ids", [])
                    if isinstance(aliases, list) and str(best_match_target_id) in {str(x) for x in aliases if str(x)}:
                        best_row = row
                        break

            ev_stability = 0.0
            ev_coverage = 0.0
            cp_relief = 0.0
            if best_row:
                # EV stability / 虚能量稳定性：相对变化率越小越稳定
                best_ev = float(best_row.get("ev", 0.0) or 0.0)
                best_ev_rate = float(best_row.get("ev_change_rate", best_row.get("delta_ev", 0.0)) or 0.0)
                rel = abs(best_ev_rate) / max(1e-6, abs(best_ev))
                k_rel = 0.35  # 半衰尺度：rel=k 时稳定性≈0.5（可后续按验收数据调参）
                ev_stability = 1.0 / (1.0 + (rel / max(1e-9, k_rel)))
                ev_stability = max(0.0, min(1.0, float(ev_stability)))

                # EV coverage / 覆盖度：关键对象 EV 在全局 EV 中的占比（越高越集中）
                ev_sum = max(1e-9, float(total_ev))
                ev_coverage = float(best_ev) / float(ev_sum)
                ev_coverage = max(0.0, min(1.0, float(ev_coverage)))

                # CP relief / 认知压下降趋势：cp_abs_rate 为负代表 |CP| 在下降
                cp_abs_rate = float(best_row.get("cp_abs_rate", best_row.get("delta_cp_abs", 0.0)) or 0.0)
                relief = max(0.0, -cp_abs_rate)
                # Softcap to (0,1): relief/(relief+k)
                k_relief = 0.30
                cp_relief = float(relief / (relief + max(1e-9, k_relief))) if relief > 0.0 else 0.0
                cp_relief = max(0.0, min(1.0, float(cp_relief)))

            # ---- Final weighted score / 最终加权 ----
            # 权重设计原则：
            # - 过程证据（match/residual）占主导
            # - 理论新增项（EV稳定/覆盖/CP下降）提供“额外可解释支撑”，避免过度简化
            if has_structure:
                grasp_score = (
                    0.30 * match_norm
                    + 0.25 * residual_complement
                    + 0.15 * structure_norm
                    + 0.15 * ev_stability
                    + 0.10 * ev_coverage
                    + 0.05 * cp_relief
                )
            else:
                grasp_score = (
                    0.35 * match_norm
                    + 0.30 * residual_complement
                    + 0.15 * ev_stability
                    + 0.10 * ev_coverage
                    + 0.10 * cp_relief
                )

            grasp_score = max(0.0, min(1.0, float(grasp_score)))
            grasp_score = float(round(grasp_score, 8))
        except Exception:
            grasp_score = 0.0

        # Also store these metrics into report for observability (best-effort).
        # 同时把这些指标写回 report，便于前端观测（尽力而为，不影响主流程）。
        try:
            stim_res = (report.get("stimulus_level", {}) or {}).get("result", {})
            if isinstance(stim_res, dict):
                metrics = stim_res.setdefault("metrics", {})
                if isinstance(metrics, dict):
                    metrics["residual_ratio"] = round(float(residual_ratio), 8)
                    metrics["best_match_score"] = round(float(best_match_score), 8)
                    metrics["grasp_score"] = round(float(grasp_score), 8)
                    metrics["match_score_target_count"] = len(match_scores)
                    metrics["best_match_target_id"] = str(best_match_target_id or "")
        except Exception:
            pass

        # Also store structure-level match metrics for observability (best-effort).
        # 同时把结构级匹配指标写回 report，便于前端观测（尽力而为，不影响主流程）。
        try:
            st_res = (report.get("structure_level", {}) or {}).get("result", {})
            if isinstance(st_res, dict):
                metrics = st_res.setdefault("metrics", {})
                if isinstance(metrics, dict):
                    metrics["best_match_score"] = round(float(structure_best_match_score), 8)
                    metrics["match_score_target_count"] = len(structure_match_scores)
                    metrics["best_match_target_id"] = str(structure_best_match_target_id or "")
        except Exception:
            pass

        # ---- emotion ----
        nt_state: dict[str, float] = {}
        if isinstance(emotion_state, dict) and isinstance(emotion_state.get("nt_state_after"), dict):
            nt_state = {str(k): float(v) for k, v in (emotion_state.get("nt_state_after") or {}).items() if str(k)}
        else:
            # Snapshot from EMgr (previous tick state).
            try:
                snap = self.emotion.get_emotion_snapshot(trace_id=f"{trace_id}_emotion_snapshot_for_rules").get("data", {}) or {}
                channels = (snap.get("nt_state_snapshot", {}) or {}).get("channels", {}) or {}
                if isinstance(channels, dict):
                    for ch, row in channels.items():
                        if not str(ch):
                            continue
                        if isinstance(row, dict) and "value" in row:
                            nt_state[str(ch)] = float(row.get("value", 0.0) or 0.0)
            except Exception:
                nt_state = {}

        # Expand NT aliases for readability (Chinese-first).
        # 扩展递质通道别名（中文优先可读性）：
        # - 允许在 IESM metric 里写 emotion.nt.多巴胺 / emotion.nt.多巴胺（DA）等
        # - 仍保留稳定缩写 key（DA/ADR/...），便于与行动模块阈值调制对齐
        try:
            snap = self.emotion.get_emotion_snapshot(trace_id=f"{trace_id}_emotion_labels_for_rules").get("data", {}) or {}
            labels = snap.get("nt_channel_labels", {}) if isinstance(snap.get("nt_channel_labels", {}), dict) else {}
            if labels:
                for ch, v in list(nt_state.items()):
                    lab = str(labels.get(ch, "") or "").strip()
                    if not lab:
                        continue
                    # Full label (e.g. "多巴胺（DA）")
                    if lab not in nt_state:
                        nt_state[lab] = float(v)
                    # Short Chinese label (e.g. "多巴胺")
                    short = lab.split("（", 1)[0].strip()
                    if short and short not in nt_state:
                        nt_state[short] = float(v)
        except Exception:
            pass

        # Reward/Punish（Rwd/Pun）/ 奖励-惩罚信号（全局汇总）
        # ----------------------------------------------------
        # 理论核心口径（3.8.2/3.9）：
        # - reward_signal / punish_signal 作为“属性刺激元”绑定在对象（CSA 锚点）上；
        # - 当这些对象具有预测能量（EV）时，形成期待/压力（CFS）；
        # - 同时，系统需要一个全局汇总信号（Rwd/Pun）供 EMgr/Drive 进一步调制。
        #
        # 推荐实现：从状态池对象的绑定属性 + EV/ER 关系自然汇总（更贴近理论），
        # 而不是只依赖 “CFS -> rwd/pun” 的硬映射（那更像过渡/对照）。
        rwd_pun_pool = self._estimate_rwd_pun_from_pool_items(pool_items, trace_id=trace_id, tick_id=tick_id)
        rwd = float(rwd_pun_pool.get("rwd", 0.0) or 0.0)
        pun = float(rwd_pun_pool.get("pun", 0.0) or 0.0)

        return {
            "pool": {
                "total_er": total_er,
                "total_ev": total_ev,
                "total_energy": round(float(total_er) + float(total_ev), 8),
                "item_count": len(pool_items),
                "total_cp_delta": total_cp_delta,
                "total_cp_abs": total_cp_abs,
                "energy_concentration": float(energy_concentration),
                "effective_peak_count": float(effective_peak_count),
                "complexity_score": float(complexity_score),
            },
            "pool_items": pool_items,
            "cam": {"size": int(cam_size), "energy_concentration": float(cam_concentration)},
            "memory_activation": {"item_count": int(map_item_count), "total_ev": float(map_total_ev)},
            "emotion": {
                "nt": nt_state,
                "rwd": float(rwd),
                "pun": float(pun),
                "rwd_pun_source": str(rwd_pun_pool.get("source", "") or "pool_items"),
                "rwd_pun_detail": dict(rwd_pun_pool.get("detail", {}) or {}),
            },
            "stimulus": {"residual_ratio": round(float(residual_ratio), 8)},
            "retrieval": {
                "stimulus": {
                    "best_match_score": round(float(best_match_score), 8),
                    "grasp_score": round(float(grasp_score), 8),
                    "best_match_target_id": str(best_match_target_id or ""),
                    "best_match_target_display": str(best_match_target_display or ""),
                    # match_scores: {target_id -> score}. Target id is typically structure_id/st_*.
                    # match_scores: {目标ID -> 分数}。目标ID通常为 structure_id/st_*。
                    "match_scores": dict(match_scores),
                    "match_displays": dict(match_displays),
                }
                ,
                "structure": {
                    "best_match_score": round(float(structure_best_match_score), 8),
                    "best_match_target_id": str(structure_best_match_target_id or ""),
                    "best_match_target_display": str(structure_best_match_target_display or ""),
                    # match_scores: {group_id -> score}. Target id is typically sg_*.
                    # match_scores: {结构组ID -> 分数}。目标ID通常为 sg_*。
                    "match_scores": dict(structure_match_scores),
                    "match_displays": dict(structure_match_displays),
                },
            },
            "meta": {"trace_id": trace_id, "tick_id": tick_id, "built_at_ms": int(time.time() * 1000)},
        }

    # ================================================================== #
    # Reward/Punish Aggregation                                            #
    # 奖励/惩罚（Rwd/Pun）汇总估计                                           #
    # ================================================================== #

    @staticmethod
    def _clamp01(x: float) -> float:
        try:
            v = float(x)
        except Exception:
            v = 0.0
        return max(0.0, min(1.0, v))

    @staticmethod
    def _softcap(x: float, *, k: float) -> float:
        """
        Soft-saturating mapping x -> x/(x+k).
        软饱和映射：把无界正数映射到 (0,1)（越大越接近 1，但永远到不了 1）。
        """
        try:
            v = float(x)
        except Exception:
            v = 0.0
        k = max(1e-9, float(k))
        if v <= 0.0:
            return 0.0
        return float(v / (v + k))

    @staticmethod
    def _row_has_bound_attribute(row: dict[str, Any], attr_name: str) -> bool:
        """Check stable key list first; fall back to display text. / 先查稳定键，再兜底文本。"""
        if not isinstance(row, dict) or not attr_name:
            return False
        names = row.get("bound_attribute_names", [])
        if isinstance(names, list) and attr_name in {str(x) for x in names if str(x)}:
            return True
        hay = " ".join(str(x) for x in (row.get("bound_attribute_displays", []) or []) if str(x))
        return attr_name in hay

    # ================================================================== #
    # Teacher Feedback (External Reward/Punish)                           #
    # 教师信号/外置奖惩（实验输入）                                         #
    # ================================================================== #

    def _apply_teacher_feedback(
        self,
        *,
        labels: dict[str, Any] | None,
        report: dict[str, Any] | None,
        trace_id: str,
        tick_id: str,
    ) -> dict[str, Any]:
        """
        Apply external teacher feedback to the runtime StatePool.

        Goals / 目标：
        - 让“外置奖惩”能够被实验数据集注入（labels），并以“属性刺激元绑定”的形式进入可审计运行态；
        - 不强依赖特定 action/tool，实现上尽量保守且不影响原有闭环；
        - 允许空 tick 也能注入（例如奖励/惩罚在下一 tick 才到达）。

        Supported label keys / 支持的字段（最小口径，未来可扩展）：
        - teacher_rwd / teacher_pun: float in [0,1]
        - teacher_anchor: cam_top1 | pool_top1_total | pool_top1_total_any | specific_item | specific_ref | none
        - teacher_anchor_item_id / teacher_anchor_ref_object_id / teacher_anchor_ref_object_type
        - teacher_anchor_ref_object_types: ['st', 'sa', ...] (default ['st'])
        - tool_feedback_rwd / tool_feedback_pun: aliases for teacher_rwd/pun (for tool experiments)
        """
        labels = labels if isinstance(labels, dict) else {}
        report = report if isinstance(report, dict) else {}

        # Allow a nested "teacher" dict, but keep top-level keys as the stable protocol.
        teacher = labels.get("teacher") if isinstance(labels.get("teacher"), dict) else {}

        def _pick(keys: list[str], *, default: Any = None) -> Any:
            for k in keys:
                if k in teacher:
                    return teacher.get(k)
                if k in labels:
                    return labels.get(k)
            return default

        def _as_float(v: Any) -> float:
            try:
                return float(v)
            except Exception:
                return 0.0

        teacher_rwd = self._clamp01(_as_float(_pick(["teacher_rwd", "tool_feedback_rwd", "rwd"], default=0.0)))
        teacher_pun = self._clamp01(_as_float(_pick(["teacher_pun", "tool_feedback_pun", "pun"], default=0.0)))
        mode = str(_pick(["teacher_mode", "mode"], default="bind_attribute") or "bind_attribute").strip() or "bind_attribute"

        anchor = str(_pick(["teacher_anchor", "anchor"], default="pool_top1_total") or "pool_top1_total").strip() or "pool_top1_total"
        if anchor == "pool_top1":
            anchor = "pool_top1_total"
        if anchor == "pool_top1_any":
            anchor = "pool_top1_total_any"

        allow_types = _pick(["teacher_anchor_ref_object_types", "ref_object_types"], default=None)
        ref_object_types: list[str] = []
        if isinstance(allow_types, list):
            ref_object_types = [str(x) for x in allow_types if str(x).strip()]
        if not ref_object_types:
            ref_object_types = ["st"]

        explicit_item_id = str(_pick(["teacher_anchor_item_id", "item_id"], default="") or "").strip()
        explicit_ref_id = str(_pick(["teacher_anchor_ref_object_id", "ref_object_id"], default="") or "").strip()
        explicit_ref_type = str(_pick(["teacher_anchor_ref_object_type", "ref_object_type"], default="") or "").strip()
        contains_text = str(_pick(["teacher_anchor_contains_text", "contains_text"], default="") or "").strip()
        note = str(_pick(["teacher_note", "note", "teacher_reason", "reason"], default="") or "").strip()

        # Early exit: no feedback provided.
        if teacher_rwd <= 0.0 and teacher_pun <= 0.0:
            return {
                "ok": True,
                "mode": mode,
                "anchor": anchor,
                "teacher_rwd": 0.0,
                "teacher_pun": 0.0,
                "applied_count": 0,
                "applied": [],
                "message": "no teacher feedback on this tick",
            }

        if anchor in {"none", "off", "disabled"}:
            return {
                "ok": True,
                "mode": mode,
                "anchor": anchor,
                "teacher_rwd": round(float(teacher_rwd), 8),
                "teacher_pun": round(float(teacher_pun), 8),
                "applied_count": 0,
                "applied": [],
                "message": "teacher feedback ignored by anchor policy",
            }

        # ---- Resolve target ----
        target_item_id = ""
        target_row: dict[str, Any] = {}
        resolve_reason = ""

        # (1) Specific item id
        if explicit_item_id:
            it = self.pool._store.get(explicit_item_id)  # type: ignore[attr-defined]
            if isinstance(it, dict):
                target_item_id = explicit_item_id
                try:
                    target_row = self.pool._snapshot._build_top_item_summary(it)  # type: ignore[attr-defined]
                except Exception:
                    target_row = {}
                resolve_reason = "specific_item"

        # (2) Specific ref id
        if not target_item_id and explicit_ref_id:
            it = self.pool._store.get_by_ref(explicit_ref_id)  # type: ignore[attr-defined]
            if isinstance(it, dict):
                if explicit_ref_type and str(it.get("ref_object_type", "")) != explicit_ref_type:
                    pass
                else:
                    target_item_id = str(it.get("id", "") or "")
                    try:
                        target_row = self.pool._snapshot._build_top_item_summary(it)  # type: ignore[attr-defined]
                    except Exception:
                        target_row = {}
                    resolve_reason = "specific_ref"

        # (3) CAM top1
        if not target_item_id and anchor == "cam_top1":
            att = report.get("attention", {}) if isinstance(report.get("attention", {}), dict) else {}
            top_items = list(att.get("top_items", []) or [])
            for r in top_items:
                if not isinstance(r, dict):
                    continue
                if ref_object_types and str(r.get("ref_object_type", "")) not in set(ref_object_types):
                    continue
                iid = str(r.get("item_id", "") or "").strip()
                if iid:
                    target_item_id = iid
                    target_row = dict(r)
                    resolve_reason = "cam_top1"
                    break

        # (4) Contains text
        if not target_item_id and (contains_text or anchor.startswith("contains_text")):
            needle = contains_text
            if not needle and ":" in anchor:
                needle = anchor.split(":", 1)[1].strip()
            if needle:
                # Use a cheap scan on the live pool store.
                try:
                    all_items = list(self.pool._store.get_all())  # type: ignore[attr-defined]
                except Exception:
                    all_items = []
                for it in all_items:
                    if not isinstance(it, dict):
                        continue
                    try:
                        row = self.pool._snapshot._build_top_item_summary(it)  # type: ignore[attr-defined]
                    except Exception:
                        continue
                    if not isinstance(row, dict):
                        continue
                    if ref_object_types and str(row.get("ref_object_type", "")) not in set(ref_object_types):
                        continue
                    hay = " ".join(
                        [
                            str(row.get("display", "") or ""),
                            str(row.get("display_detail", "") or ""),
                            " ".join(str(x) for x in (row.get("attribute_displays", []) or []) if str(x)),
                            " ".join(str(x) for x in (row.get("feature_displays", []) or []) if str(x)),
                            " ".join(str(x) for x in (row.get("bound_attribute_displays", []) or []) if str(x)),
                        ]
                    )
                    if needle in hay or needle.lower() in hay.lower():
                        target_item_id = str(row.get("item_id", "") or "")
                        target_row = dict(row)
                        resolve_reason = f"contains_text:{needle}"
                        break

        # (5) Default: pool top1 by total_energy
        if not target_item_id and anchor in {"pool_top1_total", "pool_top1_total_any"}:
            prefer_any = anchor == "pool_top1_total_any"
            try:
                all_items = list(self.pool._store.get_all())  # type: ignore[attr-defined]
            except Exception:
                all_items = []
            best_it: dict[str, Any] | None = None
            best_total = -1.0
            allow = set(ref_object_types)
            for it in all_items:
                if not isinstance(it, dict):
                    continue
                if (not prefer_any) and allow and str(it.get("ref_object_type", "")) not in allow:
                    continue
                e = it.get("energy", {}) if isinstance(it.get("energy", {}), dict) else {}
                try:
                    total = float(e.get("er", 0.0) or 0.0) + float(e.get("ev", 0.0) or 0.0)
                except Exception:
                    total = 0.0
                if total > best_total:
                    best_total = total
                    best_it = it
            if best_it is not None:
                target_item_id = str(best_it.get("id", "") or "")
                try:
                    target_row = self.pool._snapshot._build_top_item_summary(best_it)  # type: ignore[attr-defined]
                except Exception:
                    target_row = {}
                resolve_reason = f"{anchor}:top_by_total_energy"

        if not target_item_id:
            return {
                "ok": False,
                "mode": mode,
                "anchor": anchor,
                "teacher_rwd": round(float(teacher_rwd), 8),
                "teacher_pun": round(float(teacher_pun), 8),
                "ref_object_types": list(ref_object_types),
                "applied_count": 0,
                "applied": [],
                "message": "teacher feedback provided but no anchor target found",
                "resolve_reason": resolve_reason or "no_target",
            }

        # ---- Apply bindings ----
        applied: list[dict[str, Any]] = []

        def bind_attr(*, attr_name: str, attr_value: float, display: str) -> None:
            target_ref_id = str(target_row.get("ref_object_id", "") or target_item_id)
            attr_id = f"sa_teacher_attr_{attr_name}_{target_ref_id}"
            attribute_sa = {
                "id": attr_id,
                "object_type": "sa",
                "content": {
                    "raw": f"{attr_name}:{round(float(attr_value), 8)}",
                    "display": display,
                    "value_type": "numerical",
                    "attribute_name": attr_name,
                    "attribute_value": round(float(attr_value), 8),
                },
                "stimulus": {"role": "attribute", "modality": "external"},
                "energy": {"er": 0.0, "ev": 0.0},
                "meta": {
                    "ext": {
                        "bound_from": "teacher_feedback",
                        "trace_id": trace_id,
                        "tick_id": tick_id,
                        "mode": mode,
                        "anchor": anchor,
                        "resolve_reason": resolve_reason,
                        "note": note,
                    }
                },
            }
            res = self.pool.bind_attribute_node_to_object(
                target_item_id=target_item_id,
                attribute_sa=attribute_sa,
                trace_id=f"{trace_id}_teacher_bind_attr",
                tick_id=tick_id,
                source_module="teacher_feedback",
                reason=f"teacher_feedback:{attr_name}",
            )
            applied.append(
                {
                    "attribute_name": attr_name,
                    "attribute_sa_id": attr_id,
                    "target_item_id": target_item_id,
                    "success": bool(res.get("success", False)),
                    "code": str(res.get("code", "") or ""),
                    "data": res.get("data", {}) if isinstance(res.get("data", {}), dict) else {},
                }
            )

        if teacher_rwd > 0.0:
            bind_attr(attr_name="teacher_reward_signal", attr_value=teacher_rwd, display="外置奖励信号:教师")
        if teacher_pun > 0.0:
            bind_attr(attr_name="teacher_punish_signal", attr_value=teacher_pun, display="外置惩罚信号:教师")

        return {
            "ok": True,
            "mode": mode,
            "anchor": anchor,
            "teacher_rwd": round(float(teacher_rwd), 8),
            "teacher_pun": round(float(teacher_pun), 8),
            "ref_object_types": list(ref_object_types),
            "resolve_reason": resolve_reason,
            "target": {
                "item_id": target_item_id,
                "ref_object_id": str(target_row.get("ref_object_id", "") or ""),
                "ref_object_type": str(target_row.get("ref_object_type", "") or ""),
                "display": str(target_row.get("display", "") or ""),
            },
            "applied_count": len(applied),
            "applied": applied[:8],
        }

    def _estimate_rwd_pun_from_pool_items(self, pool_items: list[dict[str, Any]], *, trace_id: str, tick_id: str) -> dict[str, Any]:
        """
        Estimate global reward/punish signals from pool_items.
        从状态池对象（pool_items 摘要）估计全局奖励/惩罚信号（Rwd/Pun）。

        对齐理论核心（3.8.2/3.9）的直观口径：
        - reward_signal/punish_signal 是绑定在“对象（CSA 锚点）”上的属性刺激元；
        - 当这些对象的 EV（虚能量/预测能量）高，代表它们“被预测/被期待/被担忧”；这应贡献全局 rwd/pun 状态；
        - 当这些对象获得 ER（实能量）上升，代表“预测被现实验证”（奖励/惩罚体验被证实）；这也应贡献全局 rwd/pun。

        返回值：
        - rwd/pun: 0~1 的软饱和值（便于 UI 与后续调制接口）
        - detail: 中间统计，便于验收与找茬
        """
        cfg = getattr(self.emotion, "_config", {}) or {}
        agg = cfg.get("rwd_pun_pool_aggregation", {}) if isinstance(cfg.get("rwd_pun_pool_aggregation", {}), dict) else {}

        reward_attr = str(agg.get("reward_attr_name", "reward_signal") or "reward_signal").strip() or "reward_signal"
        punish_attr = str(agg.get("punish_attr_name", "punish_signal") or "punish_signal").strip() or "punish_signal"
        ev_min = float(agg.get("ev_min", 0.0) or 0.0)

        rwd_pred_ev = 0.0
        pun_pred_ev = 0.0
        rwd_got_er = 0.0
        pun_got_er = 0.0

        for row in pool_items or []:
            if not isinstance(row, dict):
                continue
            ev = float(row.get("ev", 0.0) or 0.0)
            der = float(row.get("delta_er", 0.0) or 0.0)
            if self._row_has_bound_attribute(row, reward_attr):
                if ev >= ev_min:
                    rwd_pred_ev += max(0.0, ev)
                if der > 0.0:
                    rwd_got_er += der
            if self._row_has_bound_attribute(row, punish_attr):
                if ev >= ev_min:
                    pun_pred_ev += max(0.0, ev)
                if der > 0.0:
                    pun_got_er += der

        k_pred = float(agg.get("k_pred", 1.0) or 1.0)
        k_got = float(agg.get("k_got", 0.5) or 0.5)
        w_pred = float(agg.get("w_pred", 0.7) or 0.7)
        w_got = float(agg.get("w_got", 0.3) or 0.3)
        w_sum = max(1e-9, abs(w_pred) + abs(w_got))
        w_pred = w_pred / w_sum
        w_got = w_got / w_sum

        rwd = self._clamp01(w_pred * self._softcap(rwd_pred_ev, k=k_pred) + w_got * self._softcap(rwd_got_er, k=k_got))
        pun = self._clamp01(w_pred * self._softcap(pun_pred_ev, k=k_pred) + w_got * self._softcap(pun_got_er, k=k_got))

        return {
            "rwd": round(float(rwd), 8),
            "pun": round(float(pun), 8),
            "source": "pool_items",
            "detail": {
                "reward_attr_name": reward_attr,
                "punish_attr_name": punish_attr,
                "ev_min": ev_min,
                "rwd_pred_ev_sum": round(float(rwd_pred_ev), 8),
                "pun_pred_ev_sum": round(float(pun_pred_ev), 8),
                "rwd_got_er_sum": round(float(rwd_got_er), 8),
                "pun_got_er_sum": round(float(pun_got_er), 8),
                "k_pred": k_pred,
                "k_got": k_got,
                "w_pred": round(float(w_pred), 6),
                "w_got": round(float(w_got), 6),
            },
        }

    # ================================================================== #
    # Episodic Memory Enrichment                                          #
    # 回合记忆材料补全：把运行态绑定属性写入 episodic memory_material         #
    # ================================================================== #

    def _enrich_tick_episodic_memory_with_bound_attributes(self, *, report: dict[str, Any], trace_id: str, tick_id: str) -> dict[str, Any]:
        """
        Enrich current tick episodic memory material with runtime bound attributes.
        把本 tick 的运行态绑定属性（CFS/时间感受/奖惩信号等）补写进该 tick 的情景记忆材料。
        """
        try:
            stim_res = (report.get("stimulus_level", {}) or {}).get("result", {}) or {}
            memory_id = str(stim_res.get("episodic_memory_id", "") or "").strip()
        except Exception:
            memory_id = ""
        if not memory_id:
            return {"ok": False, "code": "NO_MEMORY_ID", "message": "本 tick 无 stimulus_level episodic_memory_id。"}

        try:
            episodic_obj = self.hdb._episodic_store.get(memory_id)  # type: ignore[attr-defined]
        except Exception:
            episodic_obj = None
        if not isinstance(episodic_obj, dict):
            return {"ok": False, "code": "MEMORY_NOT_FOUND", "message": f"未找到 episodic memory: {memory_id}"}

        meta = episodic_obj.get("meta", {}) if isinstance(episodic_obj.get("meta", {}), dict) else {}
        ext = meta.get("ext", {}) if isinstance(meta.get("ext", {}), dict) else {}
        mm = ext.get("memory_material", {}) if isinstance(ext.get("memory_material", {}), dict) else {}
        if str(mm.get("memory_kind", "")) != "stimulus_packet":
            return {"ok": False, "code": "SKIP_KIND", "message": f"跳过：memory_kind={mm.get('memory_kind')}"}

        enrich_meta = mm.get("runtime_enrichment", {}) if isinstance(mm.get("runtime_enrichment", {}), dict) else {}
        if enrich_meta.get("bound_attributes_included") is True:
            return {"ok": True, "code": "OK_ALREADY", "message": "已补写（runtime_enrichment 标记已存在）。", "data": enrich_meta}

        seq_groups = list(mm.get("sequence_groups", []) or [])
        if not seq_groups:
            return {"ok": False, "code": "EMPTY_MATERIAL", "message": "memory_material.sequence_groups 为空。"}

        include_exact = {"reward_signal", "punish_signal", "时间感受"}
        max_attrs_per_anchor = 6
        added_unit_count = 0
        added_bundle_count = 0
        anchor_hit_count = 0

        for group in seq_groups:
            if not isinstance(group, dict):
                continue
            units = list(group.get("units", []) or [])
            if not units:
                continue
            existing_unit_ids = {str(u.get("unit_id", "")) for u in units if isinstance(u, dict) and str(u.get("unit_id", ""))}
            try:
                next_si = max(int(u.get("sequence_index", 0) or 0) for u in units if isinstance(u, dict)) + 1
            except Exception:
                next_si = 0

            bundles = group.get("csa_bundles", [])
            if not isinstance(bundles, list):
                bundles = []
            existing_bundle_keys = {str(b.get("bundle_id", "") or "") for b in bundles if isinstance(b, dict) and str(b.get("bundle_id", ""))}

            for u in list(units):
                if not isinstance(u, dict):
                    continue
                role = str(u.get("unit_role", u.get("role", "feature")) or "feature").strip() or "feature"
                if role == "attribute":
                    continue
                anchor_unit_id = str(u.get("unit_id", "") or "").strip()
                if not anchor_unit_id:
                    continue
                try:
                    st_item = self.pool._store.get_by_ref(anchor_unit_id)  # type: ignore[attr-defined]
                except Exception:
                    st_item = None
                if not isinstance(st_item, dict):
                    continue
                bound_attrs = (st_item.get("ext", {}) or {}).get("bound_attributes", [])
                if not isinstance(bound_attrs, list) or not bound_attrs:
                    continue

                selected_attrs: list[dict] = []
                for attr in bound_attrs:
                    if not isinstance(attr, dict):
                        continue
                    content = attr.get("content", {}) if isinstance(attr.get("content", {}), dict) else {}
                    attr_name = str(content.get("attribute_name", "") or "").strip()
                    raw = str(content.get("raw", "") or "")
                    if not attr_name:
                        if ":" in raw:
                            attr_name = raw.split(":", 1)[0].strip()
                        else:
                            attr_name = raw.strip()
                    if not attr_name:
                        continue
                    if attr_name in include_exact or attr_name.startswith("cfs_"):
                        selected_attrs.append(attr)

                if not selected_attrs:
                    continue

                anchor_hit_count += 1
                member_unit_ids = [anchor_unit_id]
                for attr in selected_attrs[:max_attrs_per_anchor]:
                    attr_unit_id = str(attr.get("id", "") or "").strip()
                    if not attr_unit_id or attr_unit_id in existing_unit_ids:
                        continue
                    content = attr.get("content", {}) if isinstance(attr.get("content", {}), dict) else {}
                    attr_name = str(content.get("attribute_name", "") or "").strip()
                    raw = str(content.get("raw", "") or "")
                    if not attr_name:
                        if ":" in raw:
                            attr_name = raw.split(":", 1)[0].strip()
                        else:
                            attr_name = raw.strip()
                    display = str(content.get("display", "") or raw or attr_unit_id)
                    token = display
                    if attr_name and attr_name not in token:
                        token = f"{token}（{attr_name}）"
                    attribute_value = content.get("attribute_value")
                    value_type = str(content.get("value_type", "numerical" if attribute_value is not None else "discrete") or "discrete")
                    units.append(
                        {
                            "object_type": "sa",
                            "unit_id": attr_unit_id,
                            "token": token,
                            "display_text": token,
                            "unit_role": "attribute",
                            "attribute_name": attr_name,
                            "attribute_value": attribute_value,
                            "value_type": value_type,
                            "sequence_index": int(next_si),
                            "group_index": int(group.get("group_index", 0) or 0),
                            "origin_frame_id": memory_id,
                            "source_type": "runtime_enrichment",
                        }
                    )
                    existing_unit_ids.add(attr_unit_id)
                    member_unit_ids.append(attr_unit_id)
                    next_si += 1
                    added_unit_count += 1

                if len(member_unit_ids) >= 2:
                    bundle_id = f"enrich::{anchor_unit_id}"
                    if bundle_id in existing_bundle_keys:
                        continue
                    bundles.append({"bundle_id": bundle_id, "anchor_unit_id": anchor_unit_id, "member_unit_ids": member_unit_ids})
                    existing_bundle_keys.add(bundle_id)
                    added_bundle_count += 1

            group["units"] = units
            group["csa_bundles"] = bundles
            # Keep tokens in sync (best-effort).
            try:
                group["tokens"] = [str(x.get("token", "")) for x in units if isinstance(x, dict) and str(x.get("token", ""))]
            except Exception:
                pass

        mm["sequence_groups"] = seq_groups
        mm["runtime_enrichment"] = {
            "bound_attributes_included": True,
            "memory_id": memory_id,
            "tick_id": tick_id,
            "trace_id": trace_id,
            "added_unit_count": int(added_unit_count),
            "added_bundle_count": int(added_bundle_count),
            "anchor_hit_count": int(anchor_hit_count),
            "include_exact": sorted(list(include_exact)),
            "max_attrs_per_anchor": int(max_attrs_per_anchor),
            "built_at_ms": int(time.time() * 1000),
        }
        ext["memory_material"] = mm
        meta["ext"] = ext
        episodic_obj["meta"] = meta
        try:
            self.hdb._episodic_store.update(episodic_obj)  # type: ignore[attr-defined]
        except Exception as exc:
            return {"ok": False, "code": "UPDATE_FAILED", "message": f"episodic_store.update 失败: {exc}"}

        return {"ok": True, "code": "OK", "message": "已补写运行态绑定属性到情景记忆材料。", "data": dict(mm.get("runtime_enrichment", {}) or {})}

    def _apply_innate_pool_effects(
        self,
        *,
        effects: list[dict[str, Any]],
        context: dict[str, Any],
        trace_id: str,
        tick_id: str,
    ) -> dict[str, Any]:
        """
        Apply IESM pool_effects to StatePool (safe executor).
        将 IESM 输出的 pool_effects 通过“安全执行器”落地到 StatePool。

        Safety / 安全策略：
        - 只支持白名单 effect_type（pool_energy / pool_bind_attribute）
        - 不执行任意代码
        - 单 tick 设上限，避免规则误配置导致大量写入
        """
        effects = [e for e in (effects or []) if isinstance(e, dict)]
        pool_items = list(context.get("pool_items", []) or [])
        pool_items = [it for it in pool_items if isinstance(it, dict)]

        def select_items(selector: dict[str, Any] | None) -> list[dict[str, Any]]:
            if not selector or not isinstance(selector, dict):
                return list(pool_items)
            mode = str(selector.get("mode", "all") or "all").strip()
            rows = list(pool_items)

            ref_types = selector.get("ref_object_types")
            if isinstance(ref_types, list):
                allow = {str(x) for x in ref_types if str(x)}
                if allow:
                    rows = [r for r in rows if str(r.get("ref_object_type", "")) in allow]

            if mode in {"all", "any"}:
                return rows
            if mode == "specific_item":
                iid = str(selector.get("item_id", "") or "").strip()
                return [r for r in rows if str(r.get("item_id", "")) == iid] if iid else []
            if mode == "specific_ref":
                rid = str(selector.get("ref_object_id", "") or "").strip()
                rtype = str(selector.get("ref_object_type", "") or "").strip()
                out = [r for r in rows if str(r.get("ref_object_id", "")) == rid] if rid else []
                if rtype:
                    out = [r for r in out if str(r.get("ref_object_type", "")) == rtype]
                return out
            if mode == "contains_text":
                needle = str(selector.get("contains_text", "") or "").strip()
                if not needle:
                    return []
                needle_low = needle.lower()
                out: list[dict[str, Any]] = []
                for r in rows:
                    hay = " ".join(
                        [
                            str(r.get("display", "") or ""),
                            str(r.get("display_detail", "") or ""),
                            " ".join(str(x) for x in (r.get("attribute_displays", []) or []) if str(x)),
                            " ".join(str(x) for x in (r.get("feature_displays", []) or []) if str(x)),
                            " ".join(str(x) for x in (r.get("bound_attribute_displays", []) or []) if str(x)),
                        ]
                    )
                    if needle in hay or needle_low in hay.lower():
                        out.append(r)
                return out
            if mode == "top_n":
                try:
                    n = int(selector.get("top_n", 8) or 8)
                except Exception:
                    n = 8
                n = max(1, min(512, n))
                rows.sort(key=lambda r: float(r.get("total_energy", 0.0) or 0.0), reverse=True)
                return rows[:n]
            return rows

        def coerce_float(v: Any, default: float = 0.0) -> float:
            try:
                if v is None or v == "":
                    return float(default)
                return float(v)
            except Exception:
                return float(default)

        applied: list[dict[str, Any]] = []
        skipped: list[dict[str, Any]] = []

        cap = 64  # single tick cap / 单 tick 上限
        for eff in effects[:cap]:
            et = str(eff.get("effect_type", "") or "")
            spec = eff.get("spec") if isinstance(eff.get("spec"), dict) else {}
            rule_id = str(eff.get("rule_id", "") or "")
            effect_id = str(eff.get("effect_id", "") or "")

            if et == "pool_energy":
                delta_er = coerce_float(spec.get("delta_er", spec.get("er", 0.0)), 0.0)
                delta_ev = coerce_float(spec.get("delta_ev", spec.get("ev", 0.0)), 0.0)
                if abs(delta_er) < 1e-12 and abs(delta_ev) < 1e-12:
                    skipped.append({"effect_id": effect_id, "effect_type": et, "rule_id": rule_id, "reason": "delta_both_zero"})
                    continue

                # Resolve targets
                targets: list[dict[str, Any]] = []
                selector = spec.get("selector") if isinstance(spec.get("selector"), dict) else None
                if selector:
                    targets = select_items(selector)
                else:
                    item_id = str(spec.get("target_item_id", "") or spec.get("item_id", "") or "").strip()
                    if item_id:
                        targets = [r for r in pool_items if str(r.get("item_id", "")) == item_id]
                    if not targets:
                        ref_id = str(spec.get("ref_object_id", "") or spec.get("target_ref_object_id", "") or "").strip()
                        ref_type = str(spec.get("ref_object_type", "") or spec.get("target_ref_object_type", "") or "").strip()
                        if ref_id:
                            targets = [r for r in pool_items if str(r.get("ref_object_id", "")) == ref_id and (not ref_type or str(r.get("ref_object_type", "")) == ref_type)]

                # Optional create-if-missing (only for specific ref targets)
                if not targets and bool(spec.get("create_if_missing", False)):
                    ref_id = str(spec.get("ref_object_id", "") or spec.get("target_ref_object_id", "") or "").strip()
                    if ref_id and (max(0.0, delta_er) > 0.0 or max(0.0, delta_ev) > 0.0):
                        obj_type = str(spec.get("create_ref_object_type", "") or spec.get("ref_object_type", "") or "sa").strip() or "sa"
                        display = str(spec.get("create_display", "") or spec.get("display", "") or ref_id)
                        runtime_obj = {
                            "id": ref_id,
                            "object_type": obj_type,
                            "content": {"raw": display, "display": display, "value_type": "discrete"},
                            "energy": {"er": round(max(0.0, delta_er), 8), "ev": round(max(0.0, delta_ev), 8)},
                        }
                        insert = self.pool.insert_runtime_node(
                            runtime_object=runtime_obj,
                            trace_id=f"{trace_id}_iesm_pool_energy_create",
                            tick_id=tick_id,
                            allow_merge=True,
                            source_module="innate_script",
                            reason=f"iesm_pool_energy_create:{rule_id or 'rule'}",
                        )
                        applied.append(
                            {
                                "effect_id": effect_id,
                                "effect_type": et,
                                "rule_id": rule_id,
                                "op": "create",
                                "ref_object_id": ref_id,
                                "ref_object_type": obj_type,
                                "success": bool(insert.get("success", False)),
                                "code": insert.get("code", ""),
                                "data": insert.get("data", {}) or {},
                            }
                        )
                        # After create, refresh pool_items list for later effects (best-effort).
                        try:
                            new_item = self.pool._store.get_by_ref(ref_id)
                            if new_item and isinstance(new_item, dict):
                                summary = self.pool._snapshot._build_top_item_summary(new_item)  # type: ignore[attr-defined]
                                if isinstance(summary, dict):
                                    summary["total_energy"] = round(max(0.0, float(summary.get("er", 0.0) or 0.0)) + max(0.0, float(summary.get("ev", 0.0) or 0.0)), 8)
                                    pool_items.append(summary)
                        except Exception:
                            pass
                        continue

                if not targets:
                    skipped.append({"effect_id": effect_id, "effect_type": et, "rule_id": rule_id, "reason": "no_targets"})
                    continue

                # Apply to each target (cap per effect).
                reason = str(spec.get("reason", "") or f"iesm_pool_energy:{rule_id}").strip()
                for t in targets[:24]:
                    tid = str(t.get("item_id", "") or "")
                    if not tid:
                        continue
                    res = self.pool.apply_energy_update(
                        target_item_id=tid,
                        delta_er=float(delta_er),
                        delta_ev=float(delta_ev),
                        trace_id=f"{trace_id}_iesm_pool_energy",
                        tick_id=tick_id,
                        reason=reason,
                        source_module="innate_script",
                    )
                    applied.append(
                        {
                            "effect_id": effect_id,
                            "effect_type": et,
                            "rule_id": rule_id,
                            "op": "update",
                            "target_item_id": tid,
                            "success": bool(res.get("success", False)),
                            "code": res.get("code", ""),
                            "data": res.get("data", {}) or {},
                        }
                    )
                continue

            if et == "pool_bind_attribute":
                selector = spec.get("selector") if isinstance(spec.get("selector"), dict) else None
                targets = select_items(selector) if selector else []

                if not targets:
                    item_id = str(spec.get("target_item_id", "") or spec.get("item_id", "") or "").strip()
                    if item_id:
                        targets = [r for r in pool_items if str(r.get("item_id", "")) == item_id]
                if not targets:
                    ref_id = str(spec.get("ref_object_id", "") or spec.get("target_ref_object_id", "") or "").strip()
                    ref_type = str(spec.get("ref_object_type", "") or spec.get("target_ref_object_type", "") or "").strip()
                    if ref_id:
                        targets = [r for r in pool_items if str(r.get("ref_object_id", "")) == ref_id and (not ref_type or str(r.get("ref_object_type", "")) == ref_type)]

                if not targets:
                    skipped.append({"effect_id": effect_id, "effect_type": et, "rule_id": rule_id, "reason": "no_targets"})
                    continue

                attr = spec.get("attribute") if isinstance(spec.get("attribute"), dict) else spec
                raw = str(attr.get("raw", "") or attr.get("attribute_raw", "") or "").strip()
                display = str(attr.get("display", "") or attr.get("attribute_display", "") or raw or "attribute")
                value_type = str(attr.get("value_type", "") or ("numerical" if isinstance(attr.get("attribute_value"), (int, float)) else "discrete"))
                attr_name = str(attr.get("attribute_name", "") or "").strip()
                attr_value = attr.get("attribute_value")
                if not attr_name:
                    # best-effort infer from raw "name:value"
                    if ":" in raw:
                        attr_name = raw.split(":", 1)[0].strip()
                    else:
                        attr_name = raw.strip() or "attribute"

                modality = str(attr.get("modality", "") or "internal")
                er = coerce_float(attr.get("er", 0.0), 0.0)
                ev = coerce_float(attr.get("ev", 0.0), 0.0)
                reason = str(spec.get("reason", "") or f"iesm_pool_bind_attribute:{rule_id}").strip()

                for t in targets[:24]:
                    tid = str(t.get("item_id", "") or "")
                    if not tid:
                        continue
                    # Stable attribute id for deduplication on the same target+name.
                    # 同一目标+同名属性用稳定 id，便于去重（避免 runtime_attrs 爆炸）。
                    target_ref_id = str(t.get("ref_object_id", "") or tid)
                    attr_id = f"sa_iesm_attr_{attr_name}_{target_ref_id}"
                    attribute_sa = {
                        "id": attr_id,
                        "object_type": "sa",
                        "content": {
                            "raw": raw or f"{attr_name}:{attr_value}",
                            "display": display,
                            "value_type": value_type,
                            "attribute_name": attr_name,
                            "attribute_value": attr_value,
                        },
                        "stimulus": {"role": "attribute", "modality": modality},
                        "energy": {"er": float(er), "ev": float(ev)},
                        # meta.ext: keep minimal provenance for observability and downstream memory enrichment.
                        # meta.ext：保留最小溯源信息，便于前端解释与“记忆材料补全”（把属性随记忆写入 HDB）。
                        "meta": {
                            "ext": {
                                "bound_from": "iesm_pool_bind_attribute",
                                "rule_id": rule_id,
                                "rule_title": str(eff.get("rule_title", "") or ""),
                                "rule_phase": str(eff.get("rule_phase", "") or ""),
                                "rule_priority": int(eff.get("rule_priority", 0) or 0),
                                "reason": reason,
                                "trace_id": trace_id,
                                "tick_id": tick_id,
                            }
                        },
                    }
                    res = self.pool.bind_attribute_node_to_object(
                        target_item_id=tid,
                        attribute_sa=attribute_sa,
                        trace_id=f"{trace_id}_iesm_bind_attr",
                        tick_id=tick_id,
                        source_module="innate_script",
                        reason=reason,
                    )
                    applied.append(
                        {
                            "effect_id": effect_id,
                            "effect_type": et,
                            "rule_id": rule_id,
                            "target_item_id": tid,
                            "attribute_sa_id": attr_id,
                            "success": bool(res.get("success", False)),
                            "code": res.get("code", ""),
                            "data": res.get("data", {}) or {},
                        }
                    )
                continue

            skipped.append({"effect_id": effect_id, "effect_type": et, "rule_id": rule_id, "reason": "unsupported_effect_type"})

        return {
            "applied_count": len(applied),
            "skipped_count": len(skipped),
            "applied": applied[:256],
            "skipped": skipped[:256],
        }

    def get_placeholder_modules(self) -> list[dict[str, Any]]:
        return [
            {
                "module": "attention",
                "title": "注意力模块",
                "status": "MVP 已接入",
                "description": "已接入注意力过滤器（AF）生成注意力记忆体（CAM）；当前策略仍为 Top-N（前 N）+ 预算扣能，后续将补齐聚焦/调制接口。",
            },
            {
                "module": "cognitive_feeling",
                "title": "认知感受模块",
                "status": "规则化实现（推荐）",
                "description": "默认由先天规则（IESM，phase=cfs）生成认知感受信号（违和/正确/期待/压力/置信度等），规则可在「先天规则」页面直接观测与编辑。旧版硬编码 CFS 模块仅用于对照实验（observatory_config.yaml: cfs_source_mode=legacy）。",
            },
            {
                "module": "emotion",
                "title": "情绪模块",
                "status": "已接入",
                "description": "已接入情绪递质管理器（NT 递质通道）维护递质通道，并输出调制参数（当前先对注意力过滤器（AF）生效）。",
            },
            {
                "module": "innate_script",
                "title": "先天脚本管理模块",
                "status": "已接入",
                "description": "已接入先天编码脚本管理器（IESM）执行状态窗口检查，并通过声明式规则系统输出认知感受信号（phase=cfs）、聚焦指令、情绪增量与行动触发（下一步进入行动模块竞争）。",
            },
            {
                "module": "action",
                "title": "行动模块",
                "status": "MVP 已接入",
                "description": "已接入行动管理模块（Action/Drive，驱动力）用于驱动力竞争与消耗；当前先落地内在行动器（注意力聚焦/发散模式、回忆行动）。",
            },
        ]

    def _build_sensor_report(self, text: str, sensor_result: dict) -> dict:
        # 防御式：感受器可能返回 success=False（例如空字符串输入），此时 data 可能为空/None。
        # 观测台要能把“失败”也作为可验收的输出展示出来，而不是直接崩溃。
        if not isinstance(sensor_result, dict):
            return {"input_text": text, "success": False, "code": "SENSOR_RESULT_INVALID", "message": "sensor_result 非 dict"}
        if not bool(sensor_result.get("success", False)):
            return {
                "input_text": text,
                "success": False,
                "code": str(sensor_result.get("code", "") or ""),
                "message": str(sensor_result.get("message", "") or ""),
                "error": sensor_result.get("error", {}) if isinstance(sensor_result.get("error", {}), dict) else {},
                "note": "文本感受器校验失败时，本轮按“空 Tick”继续跑闭环（便于验收其它模块）。",
            }

        data = sensor_result.get("data", {}) if isinstance(sensor_result.get("data", {}), dict) else {}
        packet = data.get("stimulus_packet", {}) if isinstance(data.get("stimulus_packet", {}), dict) else {}
        sensor_frame = data.get("sensor_frame", {}) if isinstance(data.get("sensor_frame", {}), dict) else {}
        trace_id = str(((sensor_result.get("meta", {}) or {}).get("trace_id", "")) or "")
        runtime_snapshot = self.sensor.get_runtime_snapshot(trace_id=f"{trace_id}_sensor_runtime")["data"]
        unit_rows = self._describe_packet_units(packet)
        groups = self._describe_packet_groups(packet)
        # 重要口径说明（避免前端误解）：
        # - packet.csa_items 是“CSA item 列表”（可能包含仅有锚点成员的 trivial CSA）。
        # - cut_engine 会把 CSA 归一化为 csa_bundles（只保留“包含属性成员”的有效 bundle）。
        #   因此“包里 CSA item 数”与“有效 bundle 数”可能不同（例如关闭 stimulus_intensity 属性 SA 时）。
        csa_bundle_count = sum(int(g.get("csa_count", 0) or 0) for g in groups if isinstance(g, dict))
        return {
            "input_text": text,
            "success": True,
            "normalized_text": sensor_frame.get("normalized_text", text),
            "mode": data.get("tokenization_summary", {}).get("mode", ""),
            "tokenizer_backend": runtime_snapshot["config_summary"]["tokenizer_backend"],
            "tokenizer_available": runtime_snapshot["config_summary"]["tokenizer_available"],
            "tokenizer_fallback": data.get("tokenization_summary", {}).get("tokenizer_fallback", False),
            "sa_count": len(packet.get("sa_items", [])),
            "csa_count": len(packet.get("csa_items", [])),
            "csa_bundle_count": csa_bundle_count,
            "groups": groups,
            "units": unit_rows,
            "feature_units": unit_rows,
            "echo_frames_used": list(data.get("echo_frames_used", [])),
            "echo_decay_summary": data.get("echo_decay_summary", {}),
            "fatigue_summary": data.get("fatigue_summary", {}),
        }

    def _run_state_pool_maintenance(self, trace_id: str, tick_id: str) -> dict:
        before_snapshot = self.pool.get_state_snapshot(trace_id=f"{trace_id}_maint_before", tick_id=tick_id, top_k=None)["data"]["snapshot"]
        before_count = self.pool._history.size
        start_ms = int(time.time() * 1000)
        result = self.pool.tick_maintain_state_pool(
            trace_id=f"{trace_id}_maint",
            tick_id=tick_id,
            apply_decay=True,
            apply_neutralization=True,
            apply_prune=True,
            apply_merge=True,
            enable_script_broadcast=False,
        )
        after_snapshot = self.pool.get_state_snapshot(trace_id=f"{trace_id}_maint_after", tick_id=tick_id, top_k=None)["data"]["snapshot"]
        return {
            "summary": result["data"],
            "before_summary": before_snapshot.get("summary", {}),
            "after_summary": after_snapshot.get("summary", {}),
            "events": self._collect_history_events(before_count, start_ms),
        }

    def _build_attention_memory_stub(self, trace_id: str, tick_id: str, *, focus_directives: list[dict] | None = None, modulation: dict | None = None) -> tuple[dict, dict]:
        modulation = modulation or {}
        base_top_n = int(self._config.get("attention_top_n", 16))
        effective_top_n = int(modulation.get("top_n", base_top_n) or base_top_n)
        result = self.attention.build_cam_from_pool(
            self.pool,
            trace_id=trace_id,
            tick_id=tick_id,
            top_n=effective_top_n,
            consume_energy=bool(self._config.get("attention_stub_consume_energy", True)),
            memory_energy_ratio=float(self._config.get("attention_memory_energy_ratio", 0.5)),
            focus_directives=focus_directives,
            modulation=modulation,
        )
        if not result.get("success"):
            return (
                {
                    "snapshot_id": f"{trace_id}_cam",
                    "object_type": "runtime_snapshot",
                    "sub_type": "cam_snapshot_error_fallback",
                    "schema_version": "1.1",
                    "trace_id": trace_id,
                    "tick_id": tick_id,
                    "summary": {"active_item_count": 0},
                    "top_items": [],
                },
                {"top_items": [], "structure_items": []},
            )

        data = result.get("data", {}) or {}
        cam_snapshot = data.get("cam_snapshot", {}) or {}
        attention_report = data.get("attention_report", {}) or {}
        if "memory_snapshot_summary" not in attention_report and "cam_snapshot_summary" in attention_report:
            attention_report["memory_snapshot_summary"] = attention_report.get("cam_snapshot_summary", {})
        return cam_snapshot, attention_report

    def _make_attention_memory_snapshot(self, *, selected_items: list[dict], trace_id: str, tick_id: str) -> dict:
        top_items = []
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
            "snapshot_id": f"{trace_id}_attention_memory",
            "object_type": "runtime_snapshot",
            "sub_type": "attention_memory_stub_snapshot",
            "schema_version": "1.1",
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
    def _attention_priority(item: dict) -> float:
        total_energy = float(item.get("er", 0.0)) + float(item.get("ev", 0.0))
        cp_abs = float(item.get("cp_abs", 0.0))
        salience = float(item.get("salience_score", 0.0))
        updated_at = float(item.get("updated_at", 0.0))
        return round(total_energy * 1.25 + cp_abs * 0.35 + salience * 0.15 + updated_at * 1e-12, 12)

    def _neutralize_packet_against_pool(self, packet: dict, trace_id: str, tick_id: str) -> dict:
        input_packet_summary = self._describe_stimulus_packet(packet)
        if not packet.get("sa_items") and not packet.get("csa_items"):
            return {
                "input_packet": input_packet_summary,
                "residual_packet": input_packet_summary,
                "residual_packet_raw": packet,
                "priority_events": [],
                "priority_diagnostics": [],
                "priority_summary": {
                    "priority_neutralized_item_count": 0,
                    "priority_event_count": 0,
                    "priority_diagnostic_count": 0,
                    "input_total_er": round(float(input_packet_summary.get("total_er", 0.0)), 8),
                    "input_total_ev": round(float(input_packet_summary.get("total_ev", 0.0)), 8),
                    "residual_total_er": round(float(input_packet_summary.get("total_er", 0.0)), 8),
                    "residual_total_ev": round(float(input_packet_summary.get("total_ev", 0.0)), 8),
                    "consumed_er": 0.0,
                    "consumed_ev": 0.0,
                    "input_flat_token_count": len(input_packet_summary.get("flat_tokens", [])),
                    "residual_flat_token_count": len(input_packet_summary.get("flat_tokens", [])),
                },
            }

        before_count = self.pool._history.size
        start_ms = int(time.time() * 1000)
        neutralization_result = self.pool._priority_neutralize_stimulus_packet(
            stimulus_packet=packet,
            tick_number=self.pool._tick_counter + 1,
            trace_id=f"{trace_id}_cache_neutralize",
            tick_id=tick_id,
            source_module="observatory",
        )
        priority_events = self._collect_history_events(before_count, start_ms)
        if not priority_events:
            priority_events = [
                self._enrich_history_event(event)
                for event in neutralization_result.get("events", [])
            ]
        residual_packet = neutralization_result.get("residual_packet", packet)
        residual_packet_summary = self._describe_stimulus_packet(residual_packet)
        priority_diagnostics = list(neutralization_result.get("diagnostics", []))
        return {
            "input_packet": input_packet_summary,
            "residual_packet": residual_packet_summary,
            "residual_packet_raw": residual_packet,
            "priority_events": priority_events,
            "priority_diagnostics": priority_diagnostics,
            "priority_summary": {
                "priority_neutralized_item_count": int(neutralization_result.get("neutralized_item_count", 0)),
                "priority_event_count": len(priority_events),
                "priority_diagnostic_count": len(priority_diagnostics),
                "input_total_er": round(float(input_packet_summary.get("total_er", 0.0)), 8),
                "input_total_ev": round(float(input_packet_summary.get("total_ev", 0.0)), 8),
                "residual_total_er": round(float(residual_packet_summary.get("total_er", 0.0)), 8),
                "residual_total_ev": round(float(residual_packet_summary.get("total_ev", 0.0)), 8),
                "consumed_er": round(float(input_packet_summary.get("total_er", 0.0)) - float(residual_packet_summary.get("total_er", 0.0)), 8),
                "consumed_ev": round(float(input_packet_summary.get("total_ev", 0.0)) - float(residual_packet_summary.get("total_ev", 0.0)), 8),
                "input_flat_token_count": len(input_packet_summary.get("flat_tokens", [])),
                "residual_flat_token_count": len(residual_packet_summary.get("flat_tokens", [])),
            },
        }

    def _apply_packet_to_pool(
        self,
        packet: dict,
        trace_id: str,
        tick_id: str,
        disable_priority_neutralization: bool = False,
    ) -> tuple[dict, list[dict], dict]:
        if not packet.get("sa_items") and not packet.get("csa_items"):
            return {}, [], {
                "id": "",
                "object_type": "stimulus_packet",
                "sa_items": [],
                "csa_items": [],
                "grouped_sa_sequences": [],
                "energy_summary": {"total_er": 0.0, "total_ev": 0.0},
            }
        before_count = self.pool._history.size
        start_ms = int(time.time() * 1000)
        original_priority_flag = bool(self.pool._config.get("enable_priority_stimulus_neutralization", True))
        if disable_priority_neutralization:
            self.pool._config["enable_priority_stimulus_neutralization"] = False
        try:
            result = self.pool.apply_stimulus_packet(
                stimulus_packet=packet,
                trace_id=f"{trace_id}_pool_apply",
                tick_id=tick_id,
                source_module="observatory",
            )
        finally:
            if disable_priority_neutralization:
                self.pool._config["enable_priority_stimulus_neutralization"] = original_priority_flag
        return (
            result.get("data", {}),
            self._collect_history_events(before_count, start_ms),
            result.get("data", {}).get("residual_stimulus_packet", packet),
        )

    def _project_runtime_structures(self, projections: list[dict], trace_id: str, tick_id: str) -> list[dict]:
        def _is_attribute_only_structure(structure_block: dict) -> bool:
            """
            Detect attribute-only structures (should NOT become standalone StatePool objects).
            检测“纯属性结构”（不应作为独立对象进入状态池）。

            Why / 为什么：
              用户验收口径要求：属性刺激元/属性结构应作为“约束信息”绑定在锚点对象上，
              而不是在 SP（状态池）里出现大量类似 {stimulus_intensity:1.1} 的噪音对象。

            Rule / 规则（MVP）：
              - 若结构的 sequence_groups.units 中存在任意 unit_role != attribute 的 token，则认为不是纯属性结构；
              - 若存在 token 且全部为 attribute，则视作纯属性结构。
            """
            if not isinstance(structure_block, dict):
                return False
            groups = structure_block.get("sequence_groups", [])
            tokens_seen = 0
            feature_seen = 0
            if isinstance(groups, list) and groups:
                for g in groups:
                    if not isinstance(g, dict):
                        continue
                    units = g.get("units", [])
                    if isinstance(units, list) and units:
                        for u in units:
                            if not isinstance(u, dict):
                                continue
                            tok = str(u.get("token", "") or "")
                            if not tok:
                                continue
                            tokens_seen += 1
                            role = str(u.get("unit_role", "") or "")
                            if role != "attribute":
                                feature_seen += 1
                    else:
                        # Legacy fallback: if no units, treat tokens as features (cannot decide attribute-only).
                        # 旧格式兜底：没有 units 时无法可靠判断，默认不当作纯属性。
                        return False
            else:
                # No groups: fallback to flat_tokens, assume not attribute-only.
                # 没有分组信息：无法可靠判断，默认不当作纯属性。
                return False
            return tokens_seen > 0 and feature_seen == 0

        results = []
        for item in projections:
            projection_kind = str(item.get("projection_kind", "structure") or "structure")
            memory_id = str(item.get("memory_id", ""))
            structure_id = str(item.get("structure_id", ""))
            backing_structure_id = str(item.get("backing_structure_id", structure_id))
            if projection_kind == "memory" and memory_id:
                results.append(
                    {
                        "projection_kind": projection_kind,
                        "memory_id": memory_id,
                        "structure_id": structure_id,
                        "display_text": str(item.get("display_text", memory_id)),
                        "er": round(float(item.get("er", 0.0)), 8),
                        "ev": round(float(item.get("ev", 0.0)), 8),
                        "reason": item.get("reason", ""),
                        "result": "memory_projection_skipped_state_pool",
                    }
                )
                continue

            # Skip attribute-only structures to keep StatePool clean.
            # 跳过“纯属性结构”，避免状态池出现 {stimulus_intensity:1.1} 这类噪音对象。
            try:
                st_obj = self.hdb._structure_store.get(structure_id)  # type: ignore[attr-defined]
                st_block = (st_obj or {}).get("structure", {}) if isinstance(st_obj, dict) else {}
                if _is_attribute_only_structure(st_block):
                    results.append(
                        {
                            "projection_kind": projection_kind,
                            "memory_id": memory_id,
                            "structure_id": structure_id,
                            "target_item_id": "",
                            "target_ref_object_id": structure_id,
                            "target_ref_object_type": "st",
                            "display_text": (st_block.get("display_text") if isinstance(st_block, dict) else "") or structure_id,
                            "er": round(float(item.get("er", 0.0)), 8),
                            "ev": round(float(item.get("ev", 0.0)), 8),
                            "reason": item.get("reason", ""),
                            "result": "skipped_attribute_only_structure",
                        }
                    )
                    continue
            except Exception:
                # Best-effort: if detection fails, do not block projection.
                # 尽力而为：检测失败时不阻断投影。
                pass

            runtime_object = self.hdb.make_runtime_structure_object(
                structure_id,
                er=float(item.get("er", 0.0)),
                ev=float(item.get("ev", 0.0)),
                reason=item.get("reason", "hdb_projection"),
            )
            if not runtime_object:
                continue
            insert_result = self.pool.insert_runtime_node(
                runtime_object=runtime_object,
                trace_id=f"{trace_id}_projection",
                tick_id=tick_id,
                source_module="hdb",
                reason=item.get("reason", "hdb_projection"),
            )
            # For observability: carry the resulting StatePool item_id so other modules
            # (e.g. TimeSensor binding, IESM scripts) can reference the exact runtime anchor.
            # 可观测性：带回 StatePool 的 item_id，便于后续模块（例如时间感受绑定、IESM 规则）精确引用锚点。
            ir_data = insert_result.get("data", {}) if isinstance(insert_result, dict) else {}
            target_item_id = ""
            if isinstance(ir_data, dict):
                target_item_id = str(ir_data.get("item_id", "") or ir_data.get("target_item_id", "") or "")
            results.append(
                {
                    "projection_kind": projection_kind,
                    "memory_id": memory_id,
                    "structure_id": structure_id,
                    "target_item_id": target_item_id,
                    "target_ref_object_id": structure_id,
                    "target_ref_object_type": "st",
                    "display_text": runtime_object.get("content", {}).get("display", structure_id),
                    "er": round(float(item.get("er", 0.0)), 8),
                    "ev": round(float(item.get("ev", 0.0)), 8),
                    "reason": item.get("reason", ""),
                    "result": insert_result.get("message", ""),
                }
            )
        return results

    def _collect_memory_activation_seed_targets(self, report: dict) -> list[dict]:
        """
        Seed memory activation directly from newly written residual-memory records.

        This keeps fresh episodic memories visible even in cycles where the current
        induction source energy in StatePool is too small to cross the induction
        threshold, while still preserving the separate memory pool design.
        """
        er_ratio = max(0.0, float(self.hdb._config.get("er_induction_ratio", 0.22)))
        ev_ratio = max(0.0, float(self.hdb._config.get("ev_propagation_ratio", 0.28)))
        seed_targets: list[dict] = []

        stimulus_rounds = list(
            report.get("stimulus_level", {}).get("result", {}).get("debug", {}).get("round_details", [])
        )
        for round_detail in stimulus_rounds:
            residual = dict(round_detail.get("created_residual_structure", {}) or {})
            memory_id = str(residual.get("memory_id", ""))
            if not memory_id:
                continue
            delta_ev = round(
                max(
                    0.0,
                    float(round_detail.get("transferred_er", 0.0)) * er_ratio
                    + float(round_detail.get("transferred_ev", 0.0)) * ev_ratio,
                ),
                8,
            )
            if delta_ev <= 0.0:
                continue
            matched_structure_id = str((round_detail.get("selected_match") or {}).get("structure_id", ""))
            target_display_text = (
                str(residual.get("canonical_grouped_display_text", ""))
                or str(residual.get("canonical_display_text", ""))
                or memory_id
            )
            seed_targets.append(
                {
                    "projection_kind": "memory",
                    "memory_id": memory_id,
                    "backing_structure_id": matched_structure_id,
                    "target_display_text": target_display_text,
                    "delta_ev": delta_ev,
                    "sources": [matched_structure_id] if matched_structure_id else [],
                    "modes": ["residual_storage_seed"],
                }
            )

        structure_rounds = list(
            report.get("structure_level", {}).get("result", {}).get("debug", {}).get("round_details", [])
        )
        structure_summaries = {
            int(item.get("round_index", 0)): dict(item)
            for item in report.get("structure_level", {}).get("result", {}).get("round_summaries", [])
            if int(item.get("round_index", 0)) > 0
        }
        for round_detail in structure_rounds:
            storage_summary = dict(round_detail.get("storage_summary", {}) or {})
            actions = list(storage_summary.get("actions", []) or [])
            if not actions:
                continue
            round_summary = structure_summaries.get(int(round_detail.get("round_index", 0)), {})
            delta_ev = round(
                max(
                    0.0,
                    float(round_summary.get("matched_er_total", 0.0)) * er_ratio
                    + float(round_summary.get("matched_ev_total", 0.0)) * ev_ratio,
                ),
                8,
            )
            if delta_ev <= 0.0:
                continue
            selected_group = dict(round_detail.get("selected_group", {}) or {})
            source_ids = [
                str(item.get("structure_id", ""))
                for item in selected_group.get("required_structures", [])
                if str(item.get("structure_id", ""))
            ]
            for action in actions:
                if str(action.get("type", "")) != "append_raw_residual":
                    continue
                memory_id = str(action.get("memory_id", ""))
                if not memory_id:
                    continue
                target_display_text = (
                    str(action.get("canonical_grouped_display_text", ""))
                    or str(action.get("canonical_display_text", ""))
                    or memory_id
                )
                seed_targets.append(
                    {
                        "projection_kind": "memory",
                        "memory_id": memory_id,
                        "backing_structure_id": str(storage_summary.get("owner_id", "")),
                        "target_display_text": target_display_text,
                        "delta_ev": delta_ev,
                        "sources": list(dict.fromkeys(source_ids)),
                        "modes": ["residual_storage_seed"],
                    }
                )
        return seed_targets

    def _apply_memory_feedback(self, *, memory_items: list[dict], trace_id: str, tick_id: str) -> dict:
        feedback_items: list[dict] = []
        feedback_results: list[dict] = []

        for item in memory_items or []:
            memory_id = str(item.get("memory_id", ""))
            if not memory_id:
                continue
            # Only the newly assigned activation delta of this round may feed back.
            # The pool's retained live energy is not a per-round replay budget.
            delta_er = round(max(0.0, float(item.get("last_delta_er", 0.0))), 8)
            delta_ev = round(max(0.0, float(item.get("last_delta_ev", 0.0))), 8)
            if delta_er <= 0.0 and delta_ev <= 0.0:
                continue
            episodic_obj = self.hdb._episodic_store.get(memory_id)
            if not episodic_obj:
                continue
            memory_material = dict(episodic_obj.get("meta", {}).get("ext", {}).get("memory_material", {}) or {})
            memory_kind = str(memory_material.get("memory_kind", ""))
            if memory_kind == "stimulus_packet":
                packet_result = self._build_memory_feedback_stimulus_packet(
                    memory_id=memory_id,
                    memory_material=memory_material,
                    total_er=delta_er,
                    total_ev=delta_ev,
                    trace_id=trace_id,
                    tick_id=tick_id,
                )
                packet = packet_result.get("packet")
                if not packet:
                    continue
                apply_result, events, landed_packet = self._apply_packet_to_pool(
                    packet,
                    trace_id=f"{trace_id}_memory_feedback",
                    tick_id=tick_id,
                    disable_priority_neutralization=True,
                )
                target_texts = list(packet_result.get("target_display_texts", []))
                feedback_items.append(
                    {
                        "memory_id": memory_id,
                        "delta_er": delta_er,
                        "delta_ev": delta_ev,
                        "feedback_kind": "stimulus_packet",
                        "target_count": len(target_texts),
                        "grouped_display_text": str(memory_material.get("grouped_display_text", "")),
                        "target_display_texts": target_texts,
                    }
                )
                feedback_results.append(
                    {
                        "memory_id": memory_id,
                        "memory_kind": "stimulus_packet",
                        "display_text": str(item.get("display_text", memory_id)),
                        "grouped_display_text": str(memory_material.get("grouped_display_text", "")),
                        "delta_er": delta_er,
                        "delta_ev": delta_ev,
                        "target_count": len(target_texts),
                        "target_display_texts": target_texts,
                        "packet": self._describe_stimulus_packet(packet),
                        "landed_packet": self._describe_stimulus_packet(landed_packet),
                        "apply_result": apply_result,
                        "events": events,
                    }
                )
                continue

            if memory_kind == "structure_group":
                projections = self._build_memory_feedback_structure_projections(
                    memory_id=memory_id,
                    memory_material=memory_material,
                    total_er=delta_er,
                    total_ev=delta_ev,
                )
                if not projections:
                    continue
                projection_results = self._project_runtime_structures(
                    projections,
                    trace_id=f"{trace_id}_memory_feedback",
                    tick_id=tick_id,
                )
                target_texts = [
                    str(projection.get("display_text", projection.get("structure_id", "")))
                    for projection in projections
                    if str(projection.get("structure_id", ""))
                ]
                feedback_items.append(
                    {
                        "memory_id": memory_id,
                        "delta_er": delta_er,
                        "delta_ev": delta_ev,
                        "feedback_kind": "structure_group",
                        "target_count": len(target_texts),
                        "grouped_display_text": str(memory_material.get("grouped_display_text", "")),
                        "target_display_texts": target_texts,
                    }
                )
                feedback_results.append(
                    {
                        "memory_id": memory_id,
                        "memory_kind": "structure_group",
                        "display_text": str(item.get("display_text", memory_id)),
                        "grouped_display_text": str(memory_material.get("grouped_display_text", "")),
                        "delta_er": delta_er,
                        "delta_ev": delta_ev,
                        "target_count": len(target_texts),
                        "target_display_texts": target_texts,
                        "projections": projection_results,
                    }
                )

        record_result = self.hdb.record_memory_feedback(
            feedback_items=feedback_items,
            trace_id=trace_id,
            tick_id=tick_id,
        )["data"]
        total_feedback_er = round(sum(float(item.get("delta_er", 0.0)) for item in feedback_results), 8)
        total_feedback_ev = round(sum(float(item.get("delta_ev", 0.0)) for item in feedback_results), 8)
        return {
            "applied_count": len(feedback_results),
            "total_feedback_er": total_feedback_er,
            "total_feedback_ev": total_feedback_ev,
            "total_feedback_energy": round(total_feedback_er + total_feedback_ev, 8),
            "items": feedback_results,
            "record_result": record_result,
        }

    def _build_memory_feedback_structure_projections(
        self,
        *,
        memory_id: str,
        memory_material: dict,
        total_er: float,
        total_ev: float,
    ) -> list[dict]:
        structure_items = list(memory_material.get("structure_items", []))
        ordered_structure_ids = [
            str(item.get("structure_id", ""))
            for item in structure_items
            if str(item.get("structure_id", ""))
        ]
        if not ordered_structure_ids:
            ordered_structure_ids = [
                str(structure_id)
                for structure_id in memory_material.get("structure_refs", [])
                if str(structure_id)
            ]
        if not ordered_structure_ids:
            return []
        er_allocations = self._allocate_weighted_values(
            keys=ordered_structure_ids,
            raw_weights=dict(memory_material.get("structure_energy_profile", {}) or {}),
            total_value=total_er,
        )
        ev_allocations = self._allocate_weighted_values(
            keys=ordered_structure_ids,
            raw_weights=dict(memory_material.get("structure_energy_profile", {}) or {}),
            total_value=total_ev,
        )
        display_lookup = {
            str(item.get("structure_id", "")): str(item.get("display_text", item.get("grouped_display_text", item.get("structure_id", ""))))
            for item in structure_items
            if str(item.get("structure_id", ""))
        }
        return [
            {
                "projection_kind": "structure",
                "memory_id": memory_id,
                "structure_id": structure_id,
                "display_text": display_lookup.get(structure_id, structure_id),
                "er": round(float(er_allocations.get(structure_id, 0.0)), 8),
                "ev": round(float(ev_allocations.get(structure_id, 0.0)), 8),
                "reason": "memory_feedback",
            }
            for structure_id in ordered_structure_ids
            if float(er_allocations.get(structure_id, 0.0)) > 0.0 or float(ev_allocations.get(structure_id, 0.0)) > 0.0
        ]

    def _build_memory_feedback_stimulus_packet(
        self,
        *,
        memory_id: str,
        memory_material: dict,
        total_er: float,
        total_ev: float,
        trace_id: str,
        tick_id: str,
    ) -> dict:
        sequence_groups = list(memory_material.get("sequence_groups", []))
        if not sequence_groups or (total_er <= 0.0 and total_ev <= 0.0):
            return {"packet": None, "target_display_texts": []}

        ordered_unit_ids = [
            str(unit.get("unit_id", ""))
            for group in sequence_groups
            for unit in group.get("units", [])
            if str(unit.get("unit_id", ""))
        ]
        er_allocations = self._allocate_weighted_values(
            keys=ordered_unit_ids,
            raw_weights=dict(memory_material.get("unit_energy_profile", {}) or {}),
            total_value=total_er,
        )
        ev_allocations = self._allocate_weighted_values(
            keys=ordered_unit_ids,
            raw_weights=dict(memory_material.get("unit_energy_profile", {}) or {}),
            total_value=total_ev,
        )

        now_ms = int(time.time() * 1000)
        packet_id = next_id("mfpkt")
        sa_items: list[dict] = []
        csa_items: list[dict] = []
        grouped_sequences: list[dict] = []
        packet_sequence_index = 0

        for group_order, group in enumerate(sequence_groups):
            units = sorted(
                [dict(unit) for unit in group.get("units", []) if isinstance(unit, dict)],
                key=lambda item: int(item.get("sequence_index", 0)),
            )
            if not units:
                continue
            packet_group_index = len(grouped_sequences)
            source_group_index = int(group.get("source_group_index", group.get("group_index", packet_group_index)))
            origin_frame_id = str(group.get("origin_frame_id", memory_id)) or memory_id
            group_unit_id_map: dict[str, str] = {}
            created_sa_by_id: dict[str, dict] = {}
            group_sa_ids: list[str] = []
            group_csa_ids: list[str] = []

            for unit in units:
                original_unit_id = str(unit.get("unit_id", ""))
                if not original_unit_id:
                    continue
                sa_id = next_id("sa_memfb")
                token = str(unit.get("token", unit.get("display_text", "")))
                unit_role = str(unit.get("unit_role", unit.get("role", "feature")))
                attribute_name = str(unit.get("attribute_name", ""))
                attribute_value = unit.get("attribute_value")
                if attribute_name:
                    content = {
                        "raw": token,
                        "display": token,
                        "normalized": token,
                        "value_type": "numerical" if attribute_value is not None else str(unit.get("value_type", "discrete") or "discrete"),
                        "attribute_name": attribute_name,
                        "attribute_value": attribute_value,
                    }
                else:
                    content = {
                        "raw": token,
                        "display": token,
                        "normalized": token,
                        "value_type": str(unit.get("value_type", "discrete") or "discrete"),
                    }
                packet_context = {
                    "source_type": "memory_feedback",
                    "group_index": packet_group_index,
                    "source_group_index": source_group_index,
                    "origin_frame_id": origin_frame_id,
                    "echo_depth": 0,
                    "round_created": 0,
                    "decay_count": 0,
                    "sequence_index": packet_sequence_index,
                }
                sa_obj = {
                    "id": sa_id,
                    "object_type": "sa",
                    "content": content,
                    "stimulus": {
                        "role": unit_role,
                        "modality": "memory_feedback",
                    },
                    "energy": {
                        "er": round(float(er_allocations.get(original_unit_id, 0.0)), 8),
                        "ev": round(float(ev_allocations.get(original_unit_id, 0.0)), 8),
                    },
                    "source": {
                        "module": "observatory",
                        "interface": "memory_feedback",
                        "origin": "episodic_memory_feedback",
                        "origin_id": memory_id,
                        "parent_ids": [],
                    },
                    "ext": {
                        "packet_context": packet_context,
                    },
                    "created_at": now_ms,
                    "updated_at": now_ms,
                }
                group_unit_id_map[original_unit_id] = sa_id
                created_sa_by_id[sa_id] = sa_obj
                sa_items.append(sa_obj)
                group_sa_ids.append(sa_id)
                packet_sequence_index += 1

            for bundle in group.get("csa_bundles", []):
                anchor_id = group_unit_id_map.get(str(bundle.get("anchor_unit_id", "")), "")
                member_ids = [
                    group_unit_id_map.get(str(member_id), "")
                    for member_id in bundle.get("member_unit_ids", [])
                    if group_unit_id_map.get(str(member_id), "")
                ]
                member_ids = list(dict.fromkeys(member_ids))
                if not anchor_id or len(member_ids) < 2:
                    continue
                csa_id = next_id("csa_memfb")
                csa_obj = {
                    "id": csa_id,
                    "object_type": "csa",
                    "anchor_sa_id": anchor_id,
                    "member_sa_ids": member_ids,
                    "content": {
                        "display": created_sa_by_id.get(anchor_id, {}).get("content", {}).get("display", ""),
                        "raw": created_sa_by_id.get(anchor_id, {}).get("content", {}).get("raw", ""),
                    },
                    "bundle_summary": {
                        "member_count": len(member_ids),
                        "display_total_er": round(
                            sum(float(created_sa_by_id.get(member_id, {}).get("energy", {}).get("er", 0.0)) for member_id in member_ids),
                            6,
                        ),
                        "display_total_ev": round(
                            sum(float(created_sa_by_id.get(member_id, {}).get("energy", {}).get("ev", 0.0)) for member_id in member_ids),
                            6,
                        ),
                    },
                    "ext": {
                        "packet_context": {
                            "group_index": packet_group_index,
                            "source_group_index": source_group_index,
                            "origin_frame_id": origin_frame_id,
                            "source_type": "memory_feedback",
                            "sequence_index": int(
                                created_sa_by_id.get(anchor_id, {}).get("ext", {}).get("packet_context", {}).get("sequence_index", 0)
                            ),
                        }
                    },
                    "created_at": now_ms,
                    "updated_at": now_ms,
                }
                csa_items.append(csa_obj)
                group_csa_ids.append(csa_id)
                for member_id in member_ids:
                    sa_obj = created_sa_by_id.get(member_id)
                    if not sa_obj:
                        continue
                    if member_id != anchor_id and sa_obj.get("stimulus", {}).get("role") == "attribute":
                        sa_obj.setdefault("source", {}).setdefault("parent_ids", [])
                        sa_obj["source"]["parent_ids"] = [anchor_id]

            grouped_sequences.append(
                {
                    "group_index": packet_group_index,
                    "source_type": "memory_feedback",
                    "origin_frame_id": origin_frame_id,
                    "sa_ids": group_sa_ids,
                    "csa_ids": group_csa_ids,
                    "source_group_index": source_group_index,
                }
            )

        total_packet_er = round(sum(float(item.get("energy", {}).get("er", 0.0)) for item in sa_items), 6)
        total_packet_ev = round(sum(float(item.get("energy", {}).get("ev", 0.0)) for item in sa_items), 6)
        packet = {
            "id": packet_id,
            "object_type": "stimulus_packet",
            "sub_type": "memory_feedback_stimulus_packet",
            "schema_version": "1.1",
            "packet_type": "memory_feedback",
            "current_frame_id": packet_id,
            "echo_frame_ids": [],
            "sa_items": sa_items,
            "csa_items": csa_items,
            "echo_frames": [],
            "grouped_sa_sequences": grouped_sequences,
            "energy_summary": {
                "total_er": total_packet_er,
                "total_ev": total_packet_ev,
                "current_total_er": total_packet_er,
                "current_total_ev": total_packet_ev,
                "echo_total_er": 0.0,
                "echo_total_ev": 0.0,
                "combined_context_er": total_packet_er,
                "combined_context_ev": total_packet_ev,
                "ownership_level": "sa",
                "echo_merged_into_objects": False,
            },
            "trace_id": trace_id,
            "tick_id": tick_id or trace_id,
            "created_at": now_ms,
            "updated_at": now_ms,
            "source": {
                "module": "observatory",
                "interface": "memory_feedback",
                "origin": "episodic_memory_feedback",
                "origin_id": memory_id,
                "parent_ids": [memory_id],
            },
            "status": "active",
            "ext": {
                "memory_id": memory_id,
                "grouped_display_text": str(memory_material.get("grouped_display_text", "")),
            },
            "meta": {"confidence": 0.7, "field_registry_version": "1.1", "debug": {}, "ext": {}},
        }
        target_display_texts = [
            str(unit.get("content", {}).get("display", ""))
            for unit in sa_items
            if str(unit.get("content", {}).get("display", ""))
        ]
        return {"packet": packet, "target_display_texts": target_display_texts}

    @staticmethod
    def _allocate_weighted_values(*, keys: list[str], raw_weights: dict[str, float], total_value: float) -> dict[str, float]:
        ordered_keys = [str(key) for key in keys if str(key)]
        if not ordered_keys:
            return {}
        total_value = round(max(0.0, float(total_value)), 8)
        if total_value <= 0.0:
            return {key: 0.0 for key in ordered_keys}
        positive_weights = {
            key: max(0.0, float(raw_weights.get(key, 0.0)))
            for key in ordered_keys
        }
        total_weight = sum(positive_weights.values())
        if total_weight <= 0.0:
            positive_weights = {key: 1.0 for key in ordered_keys}
            total_weight = float(len(ordered_keys))
        allocations: dict[str, float] = {}
        remaining = total_value
        for index, key in enumerate(ordered_keys):
            if index == len(ordered_keys) - 1:
                allocations[key] = round(max(0.0, remaining), 8)
                continue
            value = round(total_value * positive_weights[key] / total_weight, 8)
            allocations[key] = value
            remaining = round(remaining - value, 8)
        return allocations

    def _project_structure_ids(self, structure_ids: list[str], trace_id: str, tick_id: str, *, er: float, ev: float, reason: str) -> list[dict]:
        projections = []
        for structure_id in structure_ids:
            projections.append({"structure_id": structure_id, "er": er, "ev": ev, "reason": reason})
        return self._project_runtime_structures(projections, trace_id, tick_id)

    def _apply_induction_targets(self, targets: list[dict], trace_id: str, tick_id: str) -> list[dict]:
        projections = []
        for target in targets:
            projections.append(
                {
                    "projection_kind": target.get("projection_kind", "structure"),
                    "memory_id": target.get("memory_id", ""),
                    "structure_id": target.get("target_structure_id", ""),
                    "backing_structure_id": target.get("backing_structure_id", target.get("target_structure_id", "")),
                    "display_text": target.get("target_display_text", ""),
                    "er": 0.0,
                    "ev": float(target.get("delta_ev", 0.0)),
                    "reason": "induction_target",
                }
            )
        return self._project_runtime_structures(projections, trace_id, tick_id)

    def _apply_induction_source_consumptions(self, consumptions: list[dict], trace_id: str, tick_id: str) -> list[dict]:
        results = []
        for item in consumptions:
            structure_id = str(item.get("source_structure_id", ""))
            consumed_ev = max(0.0, float(item.get("consumed_ev", 0.0)))
            if not structure_id or consumed_ev <= 0.0:
                continue
            state_item = self.pool._store.get_by_ref(structure_id)
            if not state_item:
                continue
            available_ev = max(0.0, float(state_item.get("energy", {}).get("ev", 0.0)))
            delta_ev = -min(consumed_ev, available_ev)
            if delta_ev >= 0.0:
                continue
            result = self.pool.apply_energy_update(
                target_item_id=state_item.get("id", ""),
                delta_er=0.0,
                delta_ev=delta_ev,
                trace_id=f"{trace_id}_induction_source",
                tick_id=tick_id,
                reason="induction_source_ev_consumed",
            )
            results.append(
                {
                    "source_structure_id": structure_id,
                    "target_item_id": state_item.get("id", ""),
                    "delta_ev": round(delta_ev, 8),
                    "result": result.get("message", ""),
                }
            )
        return results

    def _collect_history_events(self, before_count: int, since_ms: int) -> list[dict]:
        recent_count = max(0, self.pool._history.size - before_count)
        events = self.pool._history.get_recent(recent_count)
        enriched = []
        for event in events:
            if event.get("timestamp_ms", 0) < since_ms:
                continue
            enriched.append(self._enrich_history_event(event))
        return enriched

    def _describe_stimulus_packet(self, packet: dict) -> dict:
        profile = self.cut_engine.build_sequence_profile_from_stimulus_packet(packet)
        unit_rows = self._describe_packet_units(packet, profile=profile)
        groups = self._describe_packet_groups(packet, profile=profile)
        flat_tokens = [str(unit.get("display", "")) for unit in unit_rows if str(unit.get("display", ""))]
        total_er = round(sum(float(unit.get("er", 0.0)) for unit in unit_rows), 8)
        total_ev = round(sum(float(unit.get("ev", 0.0)) for unit in unit_rows), 8)
        semantic_display_text = format_semantic_sequence_groups(list(profile.get("sequence_groups", [])), context="stimulus")
        return {
            "packet_id": packet.get("id", ""),
            "display_text": " / ".join(group.get("display_text", "") for group in groups if group.get("display_text", "")),
            "grouped_display_text": " / ".join(group.get("display_text", "") for group in groups if group.get("display_text", "")),
            "semantic_display_text": semantic_display_text,
            "semantic_grouped_display_text": semantic_display_text,
            "visible_text": profile.get("display_text", ""),
            "flat_tokens": flat_tokens,
            "sequence_groups": [
                {
                    **dict(group),
                    "units": [dict(unit) for unit in group.get("units", []) if isinstance(unit, dict)],
                    "csa_bundles": [dict(bundle) for bundle in group.get("csa_bundles", []) if isinstance(bundle, dict)],
                }
                for group in profile.get("sequence_groups", [])
                if isinstance(group, dict)
            ],
            "groups": groups,
            "units": unit_rows,
            "feature_units": unit_rows,
            "total_er": total_er,
            "total_ev": total_ev,
        }

    def _describe_packet_groups(self, packet: dict, *, profile: dict | None = None) -> list[dict]:
        profile = profile or self.cut_engine.build_sequence_profile_from_stimulus_packet(packet)
        groups = []
        for group in profile.get("sequence_groups", []):
            units = sorted(group.get("units", []), key=lambda item: int(item.get("sequence_index", 0)))
            total_er = round(sum(float(item.get("er", 0.0)) for item in units), 8)
            total_ev = round(sum(float(item.get("ev", 0.0)) for item in units), 8)
            all_tokens = [str(unit.get("token", "")) for unit in units if str(unit.get("token", ""))]
            visible_tokens = [
                str(unit.get("token", ""))
                for unit in units
                if str(unit.get("token", "")) and (bool(unit.get("display_visible", False)) or bool(unit.get("is_placeholder", False)))
            ]
            bundle_displays = self._describe_group_bundles(group)
            cloned_group = {
                **dict(group),
                "units": [dict(unit) for unit in group.get("units", []) if isinstance(unit, dict)],
                "csa_bundles": [dict(bundle) for bundle in group.get("csa_bundles", []) if isinstance(bundle, dict)],
            }
            groups.append(
                {
                    "group_index": group.get("group_index", 0),
                    "source_type": group.get("source_type", ""),
                    "origin_frame_id": group.get("origin_frame_id", ""),
                    "display_text": self._format_group_display(group),
                    "semantic_display_text": format_semantic_group_display(cloned_group, context="stimulus"),
                    "token_text": " / ".join(all_tokens),
                    "visible_text": "".join(visible_tokens),
                    "tokens": all_tokens,
                    "visible_tokens": visible_tokens,
                    "sa_count": len(units),
                    "csa_count": len(bundle_displays),
                    "unit_count": len(units),
                    "csa_bundles": bundle_displays,
                    "csa_bundle_defs": [dict(bundle) for bundle in group.get("csa_bundles", []) if isinstance(bundle, dict)],
                    "units": [dict(unit) for unit in units if isinstance(unit, dict)],
                    "sequence_groups": [cloned_group],
                    "total_er": total_er,
                    "total_ev": total_ev,
                    "total_energy": round(total_er + total_ev, 8),
                }
            )
        return groups

    def _describe_packet_units(self, packet: dict, *, profile: dict | None = None) -> list[dict]:
        profile = profile or self.cut_engine.build_sequence_profile_from_stimulus_packet(packet)
        rows = []
        for group in profile.get("sequence_groups", []):
            bundle_by_unit = self._map_group_unit_bundles(group)
            for unit in sorted(group.get("units", []), key=lambda item: int(item.get("sequence_index", 0))):
                rows.append(
                    {
                        "id": unit.get("unit_id", ""),
                        "display": unit.get("token", ""),
                        "role": unit.get("unit_role", ""),
                        "unit_kind": unit.get("object_type", "sa"),
                        "source_type": unit.get("source_type", "current"),
                        "group_index": unit.get("group_index", group.get("group_index", 0)),
                        "sequence_index": unit.get("sequence_index", 0),
                        "attribute_name": unit.get("attribute_name", ""),
                        "attribute_value": unit.get("attribute_value"),
                        "bundle_display": bundle_by_unit.get(str(unit.get("unit_id", "")), ""),
                        "display_visible": bool(unit.get("display_visible", False)),
                        "er": round(float(unit.get("er", 0.0)), 8),
                        "ev": round(float(unit.get("ev", 0.0)), 8),
                        "total_energy": round(float(unit.get("total_energy", 0.0)), 8),
                        "fatigue": round(float(unit.get("fatigue", 0.0)), 8),
                        "suppression_ratio": round(float(unit.get("suppression_ratio", 0.0)), 6),
                        "er_before_fatigue": round(float(unit.get("er_before_fatigue", unit.get("er", 0.0))), 8),
                        "er_after_fatigue": round(float(unit.get("er_after_fatigue", unit.get("er", 0.0))), 8),
                        "window_count": int(unit.get("window_count", 0) or 0),
                        "threshold_count": int(unit.get("threshold_count", 0) or 0),
                        "window_rounds": int(unit.get("window_rounds", 0) or 0),
                        "sensor_round": int(unit.get("sensor_round", 0) or 0),
                        "sensor_fatigue": dict(unit.get("sensor_fatigue", {}) or {}),
                    }
                )
        return rows

    def _describe_feature_units(self, packet: dict) -> list[dict]:
        return self._describe_packet_units(packet)

    def _describe_group_bundles(self, group: dict) -> list[str]:
        units_by_id = {
            str(unit.get("unit_id", "")): unit
            for unit in group.get("units", [])
            if str(unit.get("unit_id", ""))
        }
        displays = []
        for bundle in group.get("csa_bundles", []):
            member_tokens = [
                str(units_by_id.get(str(member_id), {}).get("token", ""))
                for member_id in bundle.get("member_unit_ids", [])
                if str(units_by_id.get(str(member_id), {}).get("token", ""))
            ]
            if member_tokens:
                displays.append(f"({' + '.join(member_tokens)})")
            else:
                displays.append(str(bundle.get("bundle_signature", "")))
        return displays

    def _format_group_display(self, group: dict) -> str:
        return format_group_display(group.get("units", []), group.get("csa_bundles", []))

    def _map_group_unit_bundles(self, group: dict) -> dict[str, str]:
        bundle_map: dict[str, str] = {}
        bundle_displays = self._describe_group_bundles(group)
        for bundle, display in zip(group.get("csa_bundles", []), bundle_displays):
            for member_id in bundle.get("member_unit_ids", []):
                member_text = str(member_id)
                if member_text:
                    bundle_map[member_text] = display
        return bundle_map

    def _export_report(self, trace_id: str, report: dict) -> dict:
        json_path = self.output_dir / f"{trace_id}.json"
        html_path = self.output_dir / f"{trace_id}.html"
        latest_json = self.output_dir / "latest.json"
        latest_html = self.output_dir / "latest.html"
        if self._config.get("export_json", True):
            payload = json.dumps(report, ensure_ascii=False, indent=2)
            json_path.write_text(payload, encoding="utf-8")
            latest_json.write_text(payload, encoding="utf-8")
        if self._config.get("export_html", True):
            export_cycle_html(report, html_path)
            export_cycle_html(report, latest_html)
        return {
            "json_path": str(json_path),
            "html_path": str(html_path),
            "latest_json_path": str(latest_json),
            "latest_html_path": str(latest_html),
        }

    def _silence_jieba_logs(self) -> None:
        try:
            import jieba

            jieba.setLogLevel(60)
        except Exception:
            pass

    def _summarize_state_snapshot(self, snapshot: dict) -> dict:
        items = list(snapshot.get("top_items", []))
        total_er = sum(float(item.get("er", 0.0)) for item in items)
        total_ev = sum(float(item.get("ev", 0.0)) for item in items)
        total_cp = sum(float(item.get("cp_abs", 0.0)) for item in items)
        energy_by_type: dict[str, dict[str, float]] = {}
        for item in items:
            ref_type = item.get("ref_object_type", "unknown")
            bucket = energy_by_type.setdefault(ref_type, {"count": 0, "total_er": 0.0, "total_ev": 0.0, "total_cp": 0.0})
            bucket["count"] += 1
            bucket["total_er"] += float(item.get("er", 0.0))
            bucket["total_ev"] += float(item.get("ev", 0.0))
            bucket["total_cp"] += float(item.get("cp_abs", 0.0))
        for bucket in energy_by_type.values():
            bucket["total_er"] = round(bucket["total_er"], 8)
            bucket["total_ev"] = round(bucket["total_ev"], 8)
            bucket["total_cp"] = round(bucket["total_cp"], 8)
        return {
            "total_er": round(total_er, 8),
            "total_ev": round(total_ev, 8),
            "total_cp": round(total_cp, 8),
            "energy_by_type": energy_by_type,
            "top_er_items": sorted(items, key=lambda item: item.get("er", 0.0), reverse=True)[:8],
            "top_ev_items": sorted(items, key=lambda item: item.get("ev", 0.0), reverse=True)[:8],
            "top_cp_items": sorted(items, key=lambda item: item.get("cp_abs", 0.0), reverse=True)[:8],
        }

    def _enrich_history_event(self, event: dict) -> dict:
        enriched = dict(event)
        target_item = self.pool._store.get(event.get("target_item_id", ""))
        if target_item:
            ref_snapshot = target_item.get("ref_snapshot", {})
            enriched["target_display"] = ref_snapshot.get("content_display", event.get("target_item_id", ""))
            enriched["target_detail"] = ref_snapshot.get("content_display_detail", "")
            enriched["target_ref_object_id"] = target_item.get("ref_object_id", "")
            enriched["target_ref_object_type"] = target_item.get("ref_object_type", "")
        else:
            enriched["target_display"] = event.get("target_item_id", "")
            enriched["target_detail"] = ""
            enriched["target_ref_object_id"] = ""
            enriched["target_ref_object_type"] = ""
        return enriched



