# -*- coding: utf-8 -*-
"""
Parameter Catalog (Auto-Tuner)
==============================

This module builds an explicit, auditable catalog of "tunable parameters" across
the prototype:
- module configs (*/config/*_config.yaml and observatory/config/observatory_config.yaml)
- IESM rules (innate_script/config/innate_rules.yaml) as "rule parameters"

Why:
- Users want a complete "参数-影响对应表" (parameter -> observable impacts).
- The adaptive AutoTuner should be able to tune "almost every" numeric knob,
  but in a safe, explainable way (bounds, small steps, audit logs).

Notes:
- We intentionally avoid semantic hacks. We include most numeric params, but we
  mark high-risk params (strings/enums/large structural lists like time buckets)
  as not-auto-tunable by default.
- This is not a perfect causal model; the mapping is a practical engineering
  index used to pick candidate knobs when a metric drifts.
"""

from __future__ import annotations

import dataclasses
import json
import math
import re
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

from . import io, storage


# ------------------------------
# Param identifiers / helpers
# ------------------------------


_LIST_INDEX_RE = re.compile(r"^\[(\d+)\]$")


def _path_tokens_to_str(tokens: list[Any]) -> str:
    out: list[str] = []
    for t in tokens:
        if isinstance(t, int):
            if not out:
                out.append(f"[{t}]")
            else:
                out[-1] = f"{out[-1]}[{t}]"
            continue
        s = str(t)
        if not out:
            out.append(s)
        else:
            out.append(s)
    return ".".join(out)


def _is_scalar(v: Any) -> bool:
    return v is None or isinstance(v, (bool, int, float, str))


def _is_number(v: Any) -> bool:
    return isinstance(v, (int, float)) and not isinstance(v, bool)


def _safe_float(v: Any, default: float = 0.0) -> float:
    try:
        return float(v)
    except Exception:
        return float(default)


DEFAULT_LONG_METRIC_LIBRARY: list[dict[str, Any]] = [
    {
        "key": "timing_total_logic_ms",
        "title": "单 Tick 总逻辑耗时",
        "group": "运行效率",
        "unit": "ms",
        "expected_min": 0.0,
        "expected_max": 8000.0,
        "ideal": 4500.0,
        "min_std": 200.0,
        "description": "整轮主逻辑耗时。过高说明预算、轮次或残响堆积没有被软约束住。",
    },
    {
        "key": "timing_structure_level_ms",
        "title": "结构级查存耗时",
        "group": "运行效率",
        "unit": "ms",
        "expected_min": 0.0,
        "expected_max": 1400.0,
        "ideal": 450.0,
        "min_std": 60.0,
        "description": "结构级查存与内源分辨率的主要成本。",
    },
    {
        "key": "timing_stimulus_level_ms",
        "title": "刺激级查存耗时",
        "group": "运行效率",
        "unit": "ms",
        "expected_min": 0.0,
        "expected_max": 4200.0,
        "ideal": 2200.0,
        "min_std": 120.0,
        "description": "刺激级查存一体的主耗时，常与 flat token 规模和轮次直接耦合。",
    },
    {
        "key": "timing_cache_neutralization_ms",
        "title": "缓存中和耗时",
        "group": "运行效率",
        "unit": "ms",
        "expected_min": 0.0,
        "expected_max": 2400.0,
        "ideal": 1200.0,
        "min_std": 80.0,
        "description": "残响与状态池优先中和的成本，过高通常说明历史残响或中和空间膨胀。",
    },
    {
        "key": "timing_cognitive_stitching_ms",
        "title": "认知拼接耗时（CS）",
        "group": "运行效率",
        "unit": "ms",
        "expected_min": 0.0,
        "expected_max": 1200.0,
        "ideal": 180.0,
        "min_std": 25.0,
        "description": "认知拼接模块（CS）的运行耗时。若持续偏高，通常意味着候选空间过大或 ESDB overlay/弱命中过热。",
    },
    {
        "key": "timing_event_grasp_ms",
        "title": "事件把握感耗时（Event Grasp）",
        "group": "运行效率",
        "unit": "ms",
        "expected_min": 0.0,
        "expected_max": 260.0,
        "ideal": 18.0,
        "min_std": 6.0,
        "description": "对进入 CAM 的 ES 绑定事件把握感（event_grasp）的耗时。应保持很轻，否则会挤压主链路预算。",
    },
    {
        "key": "internal_resolution_raw_unit_count",
        "title": "内源原始分辨率单位数",
        "group": "预算控制",
        "unit": "count",
        "expected_min": 0.0,
        "expected_max": 350.0,
        "ideal": 160.0,
        "min_std": 10.0,
        "description": "结构级内源残差的原始规模。若长期远超预算，说明先生成后截断，成本已发生。",
    },
    {
        "key": "internal_resolution_selected_unit_count",
        "title": "内源入选分辨率单位数",
        "group": "预算控制",
        "unit": "count",
        "expected_min": 0.0,
        "expected_max": 250.0,
        "ideal": 135.0,
        "min_std": 8.0,
        "description": "在预算内真正进入本轮的分辨率单位数。",
    },
    {
        "key": "internal_sa_count",
        "title": "内源刺激元数量",
        "group": "内源主导",
        "unit": "count",
        "expected_min": 64.0,
        "expected_max": 260.0,
        "ideal": 140.0,
        "min_std": 8.0,
        "description": "结构级内源包真正生成的内源 SA 数量，是判断内源内容是否占主导的主要口径。",
    },
    {
        "key": "internal_to_external_sa_ratio",
        "title": "内源/外源刺激比",
        "group": "内源主导",
        "unit": "ratio",
        "expected_min": 1.25,
        "expected_max": 6.0,
        "ideal": 2.2,
        "min_std": 0.08,
        "description": "内源 SA 数量与外源 SA 数量的比值。若长期低于 1，通常意味着内源回路被压瘦了。",
    },
    {
        "key": "internal_resolution_structure_count_selected",
        "title": "内源入选结构来源数",
        "group": "内源主导",
        "unit": "count",
        "expected_min": 3.0,
        "expected_max": 12.0,
        "ideal": 5.0,
        "min_std": 0.4,
        "description": "真正进入内源分辨率分配的结构来源数量。若长期只剩 1 到 2 个，内源内容通常会被压得过薄。",
    },
    {
        "key": "merged_flat_token_count",
        "title": "合流后 flat token 数",
        "group": "刺激负载",
        "unit": "count",
        "expected_min": 0.0,
        "expected_max": 240.0,
        "ideal": 140.0,
        "min_std": 8.0,
        "description": "外源与内源合流后的扁平 token 规模，是刺激级成本的核心上游量。",
    },
    {
        "key": "cache_residual_flat_token_count",
        "title": "中和后 flat token 数",
        "group": "刺激负载",
        "unit": "count",
        "expected_min": 0.0,
        "expected_max": 220.0,
        "ideal": 120.0,
        "min_std": 8.0,
        "description": "中和之后仍需进入刺激级查存的 token 规模。",
    },
    {
        "key": "landed_flat_token_count",
        "title": "落地 flat token 数",
        "group": "刺激负载",
        "unit": "count",
        "expected_min": 0.0,
        "expected_max": 220.0,
        "ideal": 120.0,
        "min_std": 8.0,
        "description": "刺激级查存后落地到状态池的 token 规模。",
    },
    {
        "key": "sensor_echo_pool_size",
        "title": "文本残响池大小",
        "group": "短时上下文",
        "unit": "count",
        "expected_min": 0.0,
        "expected_max": 36.0,
        "ideal": 12.0,
        "min_std": 2.0,
        "description": "文本残响池帧数，过高说明短时上下文残响在堆积。",
    },
    {
        "key": "sensor_echo_frames_used_count",
        "title": "本 Tick 使用的残响帧数",
        "group": "短时上下文",
        "unit": "count",
        "expected_min": 0.0,
        "expected_max": 12.0,
        "ideal": 4.0,
        "min_std": 1.0,
        "description": "本轮真正混入刺激包的历史残响帧数。",
    },
    {
        "key": "pool_active_item_count",
        "title": "状态池活跃条目数",
        "group": "状态池稳态",
        "unit": "count",
        "expected_min": 0.0,
        "expected_max": 120.0,
        "ideal": 85.0,
        "min_std": 5.0,
        "description": "状态池的总体活跃规模。过高说明衰减、淘汰、软容量或合并不够。",
    },
    {
        "key": "pool_high_cp_item_count",
        "title": "高认知压条目数",
        "group": "状态池稳态",
        "unit": "count",
        "expected_min": 0.0,
        "expected_max": 48.0,
        "ideal": 24.0,
        "min_std": 3.0,
        "description": "长期高位意味着冲突长期得不到中和。",
    },
    {
        "key": "pool_total_cp",
        "title": "状态池认知压总量",
        "group": "状态池稳态",
        "unit": "number",
        "expected_min": 0.0,
        "expected_max": 92.0,
        "ideal": 68.0,
        "min_std": 4.0,
        "description": "总体认知压。理论上应随预测-现实中和而起伏，而非长期单边抬升。",
    },
    {
        "key": "cam_item_count",
        "title": "当前注意记忆体条目数",
        "group": "注意力负载",
        "unit": "count",
        "expected_min": 1.0,
        "expected_max": 18.0,
        "ideal": 6.0,
        "min_std": 1.0,
        "description": "注意力真正带走的对象数，过低会抽空，过高会放大后续成本。",
    },
    {
        "key": "attention_state_pool_candidate_count",
        "title": "注意力候选条目数",
        "group": "注意力负载",
        "unit": "count",
        "expected_min": 1.0,
        "expected_max": 64.0,
        "ideal": 24.0,
        "min_std": 2.0,
        "description": "进入注意力竞争的候选规模，过大说明前级筛选太松。",
    },
    {
        "key": "attention_consumed_total_energy",
        "title": "注意力抽取消耗",
        "group": "注意力负载",
        "unit": "number",
        "expected_min": 0.0,
        "expected_max": 20.0,
        "ideal": 12.0,
        "min_std": 1.0,
        "description": "形成 CAM 时从状态池真实抽走的能量。",
    },
    {
        "key": "cfs_dissonance_max",
        "title": "违和感峰值",
        "group": "认知感受",
        "unit": "number",
        "expected_min": 0.0,
        "expected_max": 0.45,
        "ideal": 0.18,
        "min_std": 0.03,
        "high_band_threshold": 0.50,
        "high_band_max_ratio": 0.18,
        "high_band_soft_p95": 0.68,
        "high_band_max_run": 3,
        "description": "违和应存在但不应常驻爆炸。常态宜落在低于半量程的波动带内，0.5 以上高位应只占较小比例，主要留给明确冲突和强外界事件。",
    },
    {
        "key": "cfs_pressure_max",
        "title": "压力峰值",
        "group": "认知感受",
        "unit": "number",
        "expected_min": 0.0,
        "expected_max": 0.55,
        "ideal": 0.22,
        "min_std": 0.03,
        "high_band_threshold": 0.50,
        "high_band_max_ratio": 0.22,
        "high_band_soft_p95": 0.72,
        "high_band_max_run": 4,
        "description": "压力应更多由惩罚预测结构驱动，而不是脚本噪声常驻。常态建议保持在半量程以下，仅在强惩罚、强违和或明显失败事件下短时抬高。",
    },
    {
        "key": "cfs_expectation_max",
        "title": "期待峰值",
        "group": "认知感受",
        "unit": "number",
        "expected_min": 0.0,
        "expected_max": 0.55,
        "ideal": 0.22,
        "min_std": 0.03,
        "high_band_threshold": 0.50,
        "high_band_max_ratio": 0.22,
        "high_band_soft_p95": 0.72,
        "high_band_max_run": 4,
        "description": "期待应来自奖励预测而不是到处贴标签。默认哲学不是把它压成固定点值，而是让常态落在低到中位带，高位主要留给强验证和明确奖励窗口。",
    },
    {
        "key": "cfs_correct_event_count",
        "title": "正确事件计数",
        "group": "认知感受",
        "unit": "count",
        "expected_min": 0.0,
        "expected_max": 0.35,
        "ideal": 0.10,
        "min_std": 0.02,
        "description": "正确事件应依托违和显著下降而出现，不应早期密集、后期麻木。",
    },
    {
        "key": "cs_narrative_top_grasp",
        "title": "事件把握感（主叙事 ES）",
        "group": "认知拼接",
        "unit": "number",
        "expected_min": 0.0,
        "expected_max": 0.70,
        "ideal": 0.32,
        "min_std": 0.05,
        "high_band_threshold": 0.50,
        "high_band_max_ratio": 0.25,
        "high_band_soft_p95": 0.78,
        "high_band_max_run": 4,
        "description": "对进入 CAM 的主叙事 ES 绑定的 event_grasp 数值。常态应处在低到中位带，高位主要保留给强验证、组分中和后的一致性提升与明显领先优势的短时峰值。",
    },
    {
        "key": "rwd_pun_rwd",
        "title": "系统奖励信号",
        "group": "奖惩调制",
        "unit": "number",
        "expected_min": 0.0,
        "expected_max": 0.60,
        "ideal": 0.24,
        "min_std": 0.03,
        "high_band_threshold": 0.50,
        "high_band_max_ratio": 0.20,
        "high_band_soft_p95": 0.72,
        "high_band_max_run": 3,
        "description": "系统内部奖励汇总。大多数时候应停留在低于半量程的常态带内，0.5 以上的高位主要保留给外界明确奖励、教师强化和强验证后的短时峰值。",
    },
    {
        "key": "rwd_pun_pun",
        "title": "系统惩罚信号",
        "group": "奖惩调制",
        "unit": "number",
        "expected_min": 0.0,
        "expected_max": 0.60,
        "ideal": 0.20,
        "min_std": 0.03,
        "high_band_threshold": 0.50,
        "high_band_max_ratio": 0.18,
        "high_band_soft_p95": 0.70,
        "high_band_max_run": 3,
        "description": "系统内部惩罚汇总。默认应保持稀疏而可分层，不应长期贴在高位；高位区主要保留给明显错误、失败反馈和强违和事件。",
    },
    {
        "key": "nt_DA",
        "title": "多巴胺通道强度",
        "group": "情绪递质",
        "unit": "number",
        "expected_min": 0.0,
        "expected_max": 0.70,
        "ideal": 0.28,
        "min_std": 0.03,
        "high_band_threshold": 0.50,
        "high_band_max_ratio": 0.30,
        "high_band_soft_p95": 0.80,
        "high_band_max_run": 5,
        "description": "DA 允许比纯奖惩信号稍宽的活动带，但常态仍宜低于半量程。高位区应更多服务于明显正向驱动、强验证和高显著性机会。", 
    },
    {
        "key": "nt_ADR",
        "title": "肾上腺素通道强度",
        "group": "情绪递质",
        "unit": "number",
        "expected_min": 0.0,
        "expected_max": 0.55,
        "ideal": 0.18,
        "min_std": 0.03,
        "high_band_threshold": 0.50,
        "high_band_max_ratio": 0.16,
        "high_band_soft_p95": 0.68,
        "high_band_max_run": 3,
        "description": "ADR 适合低基线、较短脉冲。常态应明显低于半量程，仅在突发外界压力、紧迫行动或教师性强刺激下短时抬升。",
    },
    {
        "key": "nt_COR",
        "title": "皮质醇通道强度",
        "group": "情绪递质",
        "unit": "number",
        "expected_min": 0.0,
        "expected_max": 0.55,
        "ideal": 0.16,
        "min_std": 0.03,
        "high_band_threshold": 0.50,
        "high_band_max_ratio": 0.16,
        "high_band_soft_p95": 0.68,
        "high_band_max_run": 3,
        "description": "COR 主要反映压力和耗竭侧背景。默认不应长期高位，否则会压缩系统区分一般波动与强事件的能力。",
    },
    {
        "key": "nt_SER",
        "title": "血清素通道强度",
        "group": "情绪递质",
        "unit": "number",
        "expected_min": 0.0,
        "expected_max": 0.70,
        "ideal": 0.30,
        "min_std": 0.03,
        "high_band_threshold": 0.50,
        "high_band_max_ratio": 0.35,
        "high_band_soft_p95": 0.82,
        "high_band_max_run": 6,
        "description": "SER 可允许比压力型通道更宽、更慢的稳态带，但仍不建议把高位常态化；高位区应保留给明显稳定、修复或满足后的持续窗口。",
    },
    {
        "key": "nt_OXY",
        "title": "催产素通道强度",
        "group": "情绪递质",
        "unit": "number",
        "expected_min": 0.0,
        "expected_max": 0.70,
        "ideal": 0.26,
        "min_std": 0.03,
        "high_band_threshold": 0.50,
        "high_band_max_ratio": 0.28,
        "high_band_soft_p95": 0.80,
        "high_band_max_run": 5,
        "description": "OXY 代表亲和和联结侧增强。默认应在低到中位带活动，高位区适合留给强关系线索、信任验证和高质量互动回合。",
    },
    {
        "key": "nt_END",
        "title": "内啡肽通道强度",
        "group": "情绪递质",
        "unit": "number",
        "expected_min": 0.0,
        "expected_max": 0.65,
        "ideal": 0.22,
        "min_std": 0.03,
        "high_band_threshold": 0.50,
        "high_band_max_ratio": 0.22,
        "high_band_soft_p95": 0.76,
        "high_band_max_run": 4,
        "description": "END 更适合作为中低基线、有限持续的舒缓与缓冲信号。若长期高位，往往意味着缓和链路过宽，削弱系统对真实强刺激的分辨。",
    },
    {
        "key": "action_executed_recall",
        "title": "回忆动作执行率",
        "group": "行动驱动力",
        "unit": "count",
        "expected_min": 0.0,
        "expected_max": 0.40,
        "ideal": 0.10,
        "min_std": 0.02,
        "description": "回忆动作应稀疏且有意义，而不是一触即发。",
    },
    {
        "key": "action_executed_attention_focus",
        "title": "聚焦动作执行率",
        "group": "行动驱动力",
        "unit": "count",
        "expected_min": 0.0,
        "expected_max": 0.80,
        "ideal": 0.35,
        "min_std": 0.03,
        "description": "聚焦动作过少会抽空 CAM，过多会造成模式僵死。",
    },
    {
        "key": "action_drive_max",
        "title": "行动驱动力峰值",
        "group": "行动驱动力",
        "unit": "number",
        "expected_min": 0.0,
        "expected_max": 1.60,
        "ideal": 0.80,
        "min_std": 0.05,
        "description": "驱动力峰值过高说明触发太猛，过低说明动作总被压住。",
    },
    {
        "key": "action_drive_active_count",
        "title": "活跃行动节点数",
        "group": "行动驱动力",
        "unit": "count",
        "expected_min": 0.0,
        "expected_max": 10.0,
        "ideal": 4.0,
        "min_std": 1.0,
        "description": "并存行动意图数。过多会造成驱动竞争混乱。",
    },
    {
        "key": "time_sensor_bucket_energy_sum",
        "title": "时间感受桶总能量",
        "group": "时间感受",
        "unit": "number",
        "expected_min": 0.0,
        "expected_max": 1.50,
        "ideal": 0.70,
        "min_std": 0.08,
        "description": "时间感受整体强度，过高易误触回忆，过低则时间信息失真。",
    },
    {
        "key": "time_sensor_attribute_binding_count",
        "title": "时间感受属性绑定数",
        "group": "时间感受",
        "unit": "count",
        "expected_min": 0.0,
        "expected_max": 12.0,
        "ideal": 4.0,
        "min_std": 1.0,
        "description": "本 tick 把时间感受绑定到多少个锚点对象上。",
    },
    {
        "key": "time_sensor_delayed_task_table_size",
        "title": "时间感受延迟任务表大小",
        "group": "时间感受",
        "unit": "count",
        "expected_min": 0.0,
        "expected_max": 24.0,
        "ideal": 8.0,
        "min_std": 1.0,
        "description": "时间感受派生的延迟任务规模。",
    },
    {
        "key": "time_sensor_delayed_task_executed_count",
        "title": "时间感受延迟任务执行数",
        "group": "时间感受",
        "unit": "count",
        "expected_min": 0.0,
        "expected_max": 0.40,
        "ideal": 0.08,
        "min_std": 0.02,
        "description": "到期延迟任务的执行频率，过高说明节奏控制过松。",
    },
    {
        "key": "map_count",
        "title": "记忆赋能池条目数",
        "group": "记忆回响",
        "unit": "count",
        "expected_min": 0.0,
        "expected_max": 56.0,
        "ideal": 28.0,
        "min_std": 2.0,
        "description": "记忆赋能池当前规模。长期抬升通常与回忆触发过密有关。",
    },
    {
        "key": "map_feedback_total_ev",
        "title": "记忆反哺总虚能量",
        "group": "记忆回响",
        "unit": "number",
        "expected_min": 0.0,
        "expected_max": 12.0,
        "ideal": 4.5,
        "min_std": 0.4,
        "description": "记忆反哺给回状态池的总虚能量。",
    },
    {
        "key": "hdb_structure_count",
        "title": "结构总数",
        "group": "结构增长",
        "unit": "count",
        "expected_min": 0.0,
        "expected_max": 260.0,
        "ideal": 150.0,
        "min_std": 4.0,
        "description": "HDB 结构数量，用于观测是否在稳定积累而不是无序膨胀。",
    },
    {
        "key": "hdb_episodic_count",
        "title": "情节记忆总数",
        "group": "结构增长",
        "unit": "count",
        "expected_min": 0.0,
        "expected_max": 140.0,
        "ideal": 78.0,
        "min_std": 3.0,
        "description": "情节记忆数量，用于观察是否形成真正积累。",
    },
    {
        "key": "stimulus_round_count",
        "title": "刺激级查存轮次",
        "group": "检索复杂度",
        "unit": "count",
        "expected_min": 0.0,
        "expected_max": 5.0,
        "ideal": 3.0,
        "min_std": 0.2,
        "description": "刺激级轮次长期跑满，通常说明停止条件和预算没有真正生效。",
    },
    {
        "key": "structure_round_count",
        "title": "结构级查存轮次",
        "group": "检索复杂度",
        "unit": "count",
        "expected_min": 0.0,
        "expected_max": 3.0,
        "ideal": 2.0,
        "min_std": 0.2,
        "description": "结构级轮次长期跑满，通常说明候选空间过大或阈值过松。",
    },
]


def list_metric_definitions() -> list[dict[str, Any]]:
    return [dict(item) for item in DEFAULT_LONG_METRIC_LIBRARY]


# ------------------------------
# Data structures
# ------------------------------


@dataclass(frozen=True)
class ParamBound:
    min_value: float
    max_value: float
    max_step_abs: float
    quantum: float = 0.0


@dataclass(frozen=True)
class ParamSpec:
    param_id: str
    source_kind: str  # module_config | observatory_config | iesm_rule
    module: str
    # For module configs: tokens point to a leaf, like ["threshold_scale_by_nt", "DA"]
    # For IESM rules: tokens point within a single rule dict.
    path_tokens: list[Any]
    value: Any
    value_type: str  # int | float | bool | str | none | other
    auto_tune_allowed: bool
    tags: list[str]
    impacts: list[str]
    note: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "param_id": self.param_id,
            "source_kind": self.source_kind,
            "module": self.module,
            "path": _path_tokens_to_str(self.path_tokens),
            "path_tokens": list(self.path_tokens),
            "value": self.value,
            "value_type": self.value_type,
            "auto_tune_allowed": bool(self.auto_tune_allowed),
            "tags": list(self.tags),
            "impacts": list(self.impacts),
            "note": self.note,
        }


# ------------------------------
# Catalog builders
# ------------------------------


def _flatten_yaml_tree(*, node: Any, prefix: list[Any] | None = None) -> Iterable[tuple[list[Any], Any]]:
    """Yield (path_tokens, leaf_value) for scalar leaves in a YAML structure."""
    prefix = list(prefix or [])
    if _is_scalar(node):
        yield (prefix, node)
        return
    if isinstance(node, dict):
        for k, v in node.items():
            if not isinstance(k, str):
                k = str(k)
            yield from _flatten_yaml_tree(node=v, prefix=prefix + [k])
        return
    if isinstance(node, list):
        for idx, v in enumerate(node):
            yield from _flatten_yaml_tree(node=v, prefix=prefix + [idx])
        return
    # Unknown object: do not recurse
    yield (prefix, node)


def _value_type(v: Any) -> str:
    if v is None:
        return "none"
    if isinstance(v, bool):
        return "bool"
    if isinstance(v, int) and not isinstance(v, bool):
        return "int"
    if isinstance(v, float):
        return "float"
    if isinstance(v, str):
        return "str"
    return "other"


def _default_ignore_paths_for_module(module: str) -> list[str]:
    """Top-level keys we do not auto-tune (still catalogued)."""
    m = str(module or "").strip().lower()
    if m in {"time_sensor"}:
        # Buckets define the discretization lattice; auto-tuning them is too risky.
        return ["buckets", "tick_buckets"]
    if m in {"observatory"}:
        # Web host/port etc should never be auto tuned.
        return [
            "web_host",
            "web_port",
            "default_launch_mode",
            "snapshot_top_k",
            "history_limit",
            "export_html",
            "export_json",
            "auto_open_html_report",
            "web_auto_open_browser",
        ]
    if m in {"text_sensor"}:
        # Tokenizer backends and paths are semantic/dependency knobs.
        return ["tokenizer_backend", "custom_tokenizer_module_path", "user_dict_path", "importance_backend"]
    return []


def _guess_tags(*, module: str, path_tokens: list[Any], source_kind: str) -> list[str]:
    m = str(module or "").strip().lower()
    leaf = str(path_tokens[-1]) if path_tokens else ""
    top = str(path_tokens[0]) if path_tokens else ""
    name = f"{top}.{leaf}".lower() if top else leaf.lower()

    tags: list[str] = []
    if source_kind == "iesm_rule":
        tags.append("iesm")
        if str(path_tokens[0] if path_tokens else "").startswith("cfs_") or "cfs" in name:
            tags.append("cfs_rules")
        if "action" in name or "action_trigger" in name:
            tags.append("action_rules")
    else:
        tags.append(m)

    # Common patterns
    if "echo" in name:
        tags.append("echo")
    if "fatigue" in name or "habituation" in name:
        tags.append("fatigue")
    if "decay" in name or "half_life" in name or "retention" in name:
        tags.append("decay")
    if any(x in name for x in ["top_n", "max_", "cap", "budget", "round", "window", "history", "capacity"]):
        tags.append("performance")
    if "threshold" in name or name.endswith("_min") or name.endswith("_max"):
        tags.append("gating")
    return sorted(set(tags))


def _guess_impacts(*, module: str, param_id: str, tags: list[str], source_kind: str) -> list[str]:
    """Return a richer list of metric keys likely impacted (best-effort)."""
    m = str(module or "").strip().lower()
    pid = str(param_id or "")
    name = pid.lower()
    impacts: set[str] = set()

    def _add(*keys: str) -> None:
        for key in keys:
            k = str(key or "").strip()
            if k:
                impacts.add(k)

    # IESM rules: try map by rule id name.
    if source_kind == "iesm_rule":
        if "cfs_dissonance" in name:
            _add("cfs_dissonance_max", "cfs_dissonance_count", "pool_high_cp_item_count", "rwd_pun_pun")
        if "cfs_pressure" in name:
            _add("cfs_pressure_max", "cfs_pressure_count", "rwd_pun_pun", "action_drive_max")
        if "cfs_expectation" in name:
            _add("cfs_expectation_max", "cfs_expectation_count", "rwd_pun_rwd", "action_drive_max")
        if "cfs_correct_event" in name:
            _add("cfs_correct_event_count", "cfs_correct_event_max", "rwd_pun_rwd")
        if "surprise" in name:
            _add("cfs_surprise_max", "cfs_signal_count", "pool_total_er")
        if "complexity" in name:
            _add("cfs_complexity_max", "action_executed_focus_mode", "action_executed_diverge_mode")
        if "action_recall" in name:
            _add(
                "action_executed_recall",
                "action_drive_max",
                "action_drive_active_count",
                "map_count",
                "map_feedback_total_ev",
                "timing_total_logic_ms",
                "time_sensor_bucket_energy_sum",
            )
        if "action_attention_focus" in name:
            _add("action_executed_attention_focus", "action_drive_max", "cam_item_count", "attention_consumed_total_energy")
        if "attention_focus_mode" in name or "attention_diverge_mode" in name:
            _add("action_executed_focus_mode", "action_executed_diverge_mode", "cam_item_count")
        if not impacts:
            _add("cfs_signal_count")
        return sorted(impacts)

    # Module configs
    if m == "hdb":
        _add(
            "internal_sa_count",
            "internal_to_external_sa_ratio",
            "internal_resolution_structure_count_selected",
            "internal_resolution_raw_unit_count",
            "internal_resolution_selected_unit_count",
            "structure_round_count",
            "stimulus_round_count",
            "timing_structure_level_ms",
            "timing_stimulus_level_ms",
        )
        if any(x in name for x in ["internal_resolution", "flat_unit", "detail_budget", "structures_per_tick", "resolution"]):
            _add(
                "internal_resolution_detail_budget",
                "internal_sa_count",
                "internal_flat_token_count",
                "internal_to_external_sa_ratio",
                "internal_resolution_structure_count_selected",
                "timing_total_logic_ms",
                "merged_flat_token_count",
            )
        if any(x in name for x in ["stimulus_level", "stimulus_match", "cut", "diff_table", "group_table", "residual"]):
            _add("landed_flat_token_count", "cache_residual_flat_token_count", "pool_apply_merged_item_count")
        if any(x in name for x in ["structure_level", "group", "fallback"]):
            _add("hdb_group_count", "timing_structure_level_ms")
        if any(x in name for x in ["memory_activation", "map", "feedback", "induction"]):
            _add("map_count", "map_feedback_total_ev", "hdb_episodic_count")
        return sorted(impacts)
    if m == "cognitive_stitching":
        _add(
            "cs_candidate_count",
            "cs_action_count",
            "cs_created_count",
            "cs_extended_count",
            "cs_merged_count",
            "cs_reinforced_count",
            "stimulus_new_structure_count",
            "timing_cognitive_stitching_ms",
            "timing_event_grasp_ms",
        )
        if any(x in name for x in ["min_candidate_score", "min_seed_total_energy", "min_event_total_energy", "event_grasp_min_total_energy"]):
            _add(
                "cs_candidate_count",
                "cs_action_count",
                "cs_created_count",
                "cs_extended_count",
                "cs_merged_count",
                "cs_reinforced_count",
                "stimulus_new_structure_count",
            )
        if any(x in name for x in ["weight", "penalty", "scale", "temperature", "bias"]):
            _add(
                "cs_candidate_count",
                "cs_action_count",
                "cs_created_count",
                "cs_extended_count",
                "cs_merged_count",
                "cs_reinforced_count",
                "timing_cognitive_stitching_ms",
            )
        if any(x in name for x in ["max_seed_items", "max_outgoing_edges_per_seed", "max_events_per_tick", "max_context_k", "top_k", "overlay"]):
            _add(
                "cs_candidate_count",
                "cs_action_count",
                "timing_cognitive_stitching_ms",
                "timing_event_grasp_ms",
            )
        return sorted(impacts)
    if m == "attention" or pid.startswith("observatory.attention_"):
        _add(
            "cam_item_count",
            "attention_memory_item_count",
            "attention_consumed_total_energy",
            "attention_state_pool_candidate_count",
            "timing_attention_ms",
        )
        if any(x in name for x in ["top_n", "max_cam_items", "cap"]):
            _add("attention_cam_item_cap", "cam_item_count", "attention_skipped_memory_item_count")
        if any(x in name for x in ["ratio", "consume_energy", "memory_energy"]):
            _add("pool_total_er", "pool_total_ev", "attention_consumed_total_energy")
        if any(x in name for x in ["threshold", "suppression", "gate", "keep"]):
            _add("cam_item_count", "attention_skipped_memory_item_count")
        return sorted(impacts)
    if m == "state_pool":
        _add(
            "pool_active_item_count",
            "pool_high_cp_item_count",
            "pool_total_cp",
            "timing_maintenance_ms",
            "maintenance_delta_active_item_count",
        )
        if any(x in name for x in ["pool_max_items", "soft_capacity", "overflow", "prune"]):
            _add("timing_total_logic_ms", "maintenance_after_active_item_count", "maintenance_delta_active_item_count")
        if any(x in name for x in ["default_er_decay", "er_elimination", "recency_gain"]):
            _add("pool_total_er", "pool_total_cp")
        if any(x in name for x in ["default_ev_decay", "ev_elimination"]):
            _add("pool_total_ev", "pool_total_cp", "cfs_expectation_max", "cfs_pressure_max")
        if any(x in name for x in ["fatigue", "cp_elimination", "fast_cp", "rate_smoothing"]):
            _add("pool_high_cp_item_count", "cfs_dissonance_max", "pool_total_cp")
        if "neutralization" in name:
            _add("cache_residual_flat_token_count", "cache_priority_consumed_er", "cache_priority_consumed_ev", "pool_apply_total_delta_cp")
        if "merge" in name or "semantic_same_object" in name:
            _add("pool_apply_merged_item_count", "pool_active_item_count", "timing_maintenance_ms")
        if "attribute_bind" in name:
            _add("time_sensor_attribute_binding_count", "pool_active_item_count")
        return sorted(impacts)
    if m == "text_sensor":
        _add(
            "external_sa_count",
            "merged_flat_token_count",
            "timing_sensor_ms",
            "sensor_echo_pool_size",
            "sensor_echo_current_round",
        )
        if "echo" in name:
            _add("sensor_echo_frames_used_count", "timing_cache_neutralization_ms", "cache_residual_flat_token_count")
        if any(x in name for x in ["char_output", "token_output", "csa_output", "tokenizer", "importance"]):
            _add("sensor_feature_sa_count", "sensor_attribute_sa_count", "sensor_csa_bundle_count", "external_sa_count")
        if any(x in name for x in ["base_er", "attribute_er_ratio", "attribute_ev_ratio", "importance"]):
            _add("pool_total_er", "pool_total_ev", "external_sa_count")
        if "fatigue" in name:
            _add("external_sa_count", "merged_flat_token_count", "pool_total_er")
        return sorted(impacts)
    if m == "time_sensor":
        _add(
            "timing_time_sensor_ms",
            "time_sensor_bucket_update_count",
            "time_sensor_bucket_energy_sum",
            "time_sensor_bucket_energy_max",
            "time_sensor_attribute_binding_count",
        )
        if any(x in name for x in ["memory_top_k", "source_mode", "time_basis", "tick_interval"]):
            _add("time_sensor_memory_used_count", "time_sensor_bucket_energy_sum", "action_executed_recall")
        if any(x in name for x in ["energy_gain_ratio", "base_energy_source", "energy_key", "min_bucket_energy"]):
            _add("time_sensor_bucket_energy_sum", "time_sensor_bucket_energy_max", "time_sensor_attribute_binding_count")
        if any(x in name for x in ["bind", "attribute_name", "peak_keep_ratio", "max_total_bindings"]):
            _add("time_sensor_attribute_binding_count", "action_executed_recall")
        if "delayed_task" in name:
            _add(
                "time_sensor_delayed_task_table_size",
                "time_sensor_delayed_task_registered_count",
                "time_sensor_delayed_task_executed_count",
                "timing_time_sensor_ms",
            )
        return sorted(impacts)
    if m == "action":
        _add(
            "action_executed_count",
            "action_node_count",
            "action_drive_max",
            "action_drive_mean",
            "action_drive_active_count",
            "timing_action_ms",
        )
        if any(x in name for x in ["drive_decay", "drive_max", "max_action_nodes", "node_idle"]):
            _add("action_node_count", "action_drive_active_count", "timing_action_ms")
        if any(x in name for x in ["threshold_scale", "fatigue"]):
            _add("action_executed_count", "action_drive_max", "action_drive_active_count")
        if any(x in name for x in ["focus_threshold", "focus_gain", "attention_focus"]):
            _add("action_executed_attention_focus", "cam_item_count", "attention_consumed_total_energy")
        if any(x in name for x in ["mode_threshold", "mode_drive_gain", "focus_mode", "diverge_mode", "attention_mode"]):
            _add("action_executed_focus_mode", "action_executed_diverge_mode", "cam_item_count")
        if "recall" in name:
            _add("action_executed_recall", "map_count", "map_feedback_total_ev", "time_sensor_bucket_energy_sum")
        return sorted(impacts)
    if m == "emotion":
        _add("rwd_pun_rwd", "rwd_pun_pun", "nt_DA", "nt_COR", "nt_ADR", "nt_SER")
        if "da" in name:
            _add("nt_DA", "action_drive_max", "cfs_expectation_max")
        if "cor" in name:
            _add("nt_COR", "cfs_pressure_max", "timing_total_logic_ms")
        if "adr" in name:
            _add("nt_ADR", "action_drive_max", "cfs_pressure_max")
        if "ser" in name:
            _add("nt_SER", "action_executed_count")
        if "oxy" in name:
            _add("nt_OXY", "action_executed_attention_focus")
        if "end" in name:
            _add("nt_END", "action_executed_recall")
        return sorted(impacts)
    if m == "cognitive_feeling":
        _add("cfs_signal_count", "cfs_dissonance_max", "cfs_pressure_max", "cfs_expectation_max")
        if "dissonance" in name:
            _add("cfs_dissonance_max", "rwd_pun_pun")
        if "pressure" in name:
            _add("cfs_pressure_max", "rwd_pun_pun")
        if "expectation" in name:
            _add("cfs_expectation_max", "rwd_pun_rwd")
        if "correct" in name:
            _add("cfs_correct_event_count", "rwd_pun_rwd")
        if "complexity" in name:
            _add("cfs_complexity_max", "action_executed_focus_mode")
        if "grasp" in name:
            _add("cfs_grasp_max", "action_executed_attention_focus")
        return sorted(impacts)
    if m == "energy_balance":
        _add("pool_total_er", "pool_total_ev", "pool_total_cp", "rwd_pun_rwd", "rwd_pun_pun")
        return sorted(impacts)
    if m == "observatory":
        _add("timing_total_logic_ms")
        if "attention_" in name:
            _add("cam_item_count", "attention_consumed_total_energy")
        if "sensor_" in name:
            _add("external_sa_count", "merged_flat_token_count", "sensor_echo_pool_size")
        if "state_pool_" in name:
            _add("pool_active_item_count", "pool_high_cp_item_count")
        if "hdb_" in name:
            _add("internal_resolution_raw_unit_count", "stimulus_round_count", "structure_round_count")
        return sorted(impacts)
    if m == "innate_script":
        _add("cfs_signal_count", "action_drive_max", "action_executed_count")
        return sorted(impacts)
    return sorted(impacts)


def _is_auto_tunable_scalar(*, module: str, path_tokens: list[Any], value: Any) -> bool:
    # Numeric scalars only (by default). Bool/string are kept in the catalog but not tuned automatically.
    if not _is_number(value):
        return False

    # Exclude risky structural lattice knobs
    ignore_top = set(_default_ignore_paths_for_module(module))
    if path_tokens and isinstance(path_tokens[0], str) and str(path_tokens[0]) in ignore_top:
        return False

    return True


def build_module_param_specs(*, module: str, yaml_path: Path, runtime_config: dict[str, Any] | None, source_kind: str) -> list[ParamSpec]:
    raw = {}
    try:
        raw = io.load_yaml_file(yaml_path) if yaml_path.exists() else {}
    except Exception:
        raw = {}

    # Prefer runtime_config values if provided (effective config).
    root = runtime_config if isinstance(runtime_config, dict) and runtime_config else raw
    if not isinstance(root, dict):
        root = raw if isinstance(raw, dict) else {}

    specs: list[ParamSpec] = []
    for path_tokens, leaf in _flatten_yaml_tree(node=root, prefix=[]):
        # Skip empty path (root)
        if not path_tokens:
            continue
        pid = f"{module}.{_path_tokens_to_str(path_tokens)}"
        vtype = _value_type(leaf)
        tags = _guess_tags(module=module, path_tokens=path_tokens, source_kind=source_kind)
        impacts = _guess_impacts(module=module, param_id=pid, tags=tags, source_kind=source_kind)
        allow = _is_auto_tunable_scalar(module=module, path_tokens=path_tokens, value=leaf)
        specs.append(
            ParamSpec(
                param_id=pid,
                source_kind=source_kind,
                module=module,
                path_tokens=list(path_tokens),
                value=leaf,
                value_type=vtype,
                auto_tune_allowed=bool(allow),
                tags=tags,
                impacts=impacts,
            )
        )
    return specs


def build_iesm_rule_param_specs(*, rules_doc: dict[str, Any]) -> list[ParamSpec]:
    """Extract numeric tunables from IESM rules doc (best-effort)."""
    rules = rules_doc.get("rules") if isinstance(rules_doc, dict) else None
    rules = rules if isinstance(rules, list) else []

    specs: list[ParamSpec] = []
    for rule in rules:
        if not isinstance(rule, dict):
            continue
        rule_id = str(rule.get("id", "") or "").strip()
        if not rule_id:
            continue

        # We only index a conservative subset of numeric leaves to avoid tuning UI text, IDs, etc.
        # Allowed leaf keys (heuristic).
        allowed_leaf_names = {
            "priority",
            "cooldown_ticks",
            "window_ticks",
            "top_n",
            "value",
            "min_strength",
            "max_signals",
            "max_triggers",
            "gain",
            "threshold",
            "min_delta",
            "min_interval_ticks",
            "out_min",
            "out_max",
            "min",
            "max",
            "attribute_value",
            "er",
            "ev",
        }

        def _walk(node: Any, prefix: list[Any]) -> None:
            if isinstance(node, dict):
                for k, v in node.items():
                    kk = str(k) if not isinstance(k, str) else k
                    if kk in {"ui", "note", "title", "id", "display", "raw", "message"}:
                        continue
                    _walk(v, prefix + [kk])
                return
            if isinstance(node, list):
                for idx, v in enumerate(node):
                    _walk(v, prefix + [idx])
                return
            # leaf
            if not _is_number(node):
                return
            leaf_name = str(prefix[-1]) if prefix else ""
            if leaf_name not in allowed_leaf_names:
                return

            path_tokens = list(prefix)
            pid = f"iesm.rules.{rule_id}.{_path_tokens_to_str(path_tokens)}"
            tags = _guess_tags(module="iesm", path_tokens=[rule_id] + path_tokens, source_kind="iesm_rule")
            impacts = _guess_impacts(module="iesm", param_id=pid, tags=tags, source_kind="iesm_rule")
            specs.append(
                ParamSpec(
                    param_id=pid,
                    source_kind="iesm_rule",
                    module="iesm",
                    path_tokens=[rule_id] + path_tokens,  # first token is rule_id for patching
                    value=node,
                    value_type=_value_type(node),
                    auto_tune_allowed=True,
                    tags=tags,
                    impacts=impacts,
                    note="auto-indexed from innate_rules.yaml",
                )
            )

        _walk(rule, [])

    return specs


def build_param_catalog(*, app: Any | None = None) -> list[ParamSpec]:
    root = storage.repo_root()

    # Module configs
    configs: list[tuple[str, Path, str]] = [
        ("observatory", root / "observatory" / "config" / "observatory_config.yaml", "observatory_config"),
        ("action", root / "action" / "config" / "action_config.yaml", "module_config"),
        ("attention", root / "attention" / "config" / "attention_config.yaml", "module_config"),
        ("cognitive_feeling", root / "cognitive_feeling" / "config" / "cognitive_feeling_config.yaml", "module_config"),
        ("emotion", root / "emotion" / "config" / "emotion_config.yaml", "module_config"),
        ("energy_balance", root / "energy_balance" / "config" / "energy_balance_config.yaml", "module_config"),
        ("hdb", root / "hdb" / "config" / "hdb_config.yaml", "module_config"),
        ("innate_script", root / "innate_script" / "config" / "innate_script_config.yaml", "module_config"),
        ("state_pool", root / "state_pool" / "config" / "state_pool_config.yaml", "module_config"),
        ("text_sensor", root / "text_sensor" / "config" / "text_sensor_config.yaml", "module_config"),
        ("time_sensor", root / "time_sensor" / "config" / "time_sensor_config.yaml", "module_config"),
    ]

    def _runtime_cfg(mod: str) -> dict[str, Any] | None:
        if app is None:
            return None
        try:
            if mod == "observatory":
                return dict(getattr(app, "_config", {}) or {})
            if mod == "action":
                return dict(getattr(getattr(app, "action", None), "_config", {}) or {})
            if mod == "attention":
                return dict(getattr(getattr(app, "attention", None), "_config", {}) or {})
            if mod == "cognitive_feeling":
                return dict(getattr(getattr(app, "cfs", None), "_config", {}) or {})
            if mod == "emotion":
                return dict(getattr(getattr(app, "emotion", None), "_config", {}) or {})
            if mod == "energy_balance":
                return dict(getattr(getattr(app, "energy_balance", None), "_config", {}) or {})
            if mod == "hdb":
                return dict(getattr(getattr(app, "hdb", None), "_config", {}) or {})
            if mod == "innate_script":
                return dict(getattr(getattr(app, "iesm", None), "_config", {}) or {})
            if mod == "state_pool":
                return dict(getattr(getattr(app, "pool", None), "_config", {}) or {})
            if mod == "text_sensor":
                return dict(getattr(getattr(app, "sensor", None), "_config", {}) or {})
            if mod == "time_sensor":
                return dict(getattr(getattr(app, "time_sensor", None), "_config", {}) or {})
        except Exception:
            return None
        return None

    specs: list[ParamSpec] = []
    for mod, path, kind in configs:
        runtime = _runtime_cfg(mod)
        specs.extend(build_module_param_specs(module=mod, yaml_path=path, runtime_config=runtime, source_kind=kind))

    # IESM rules doc:
    # Prefer the *effective* runtime rules path when an app is provided, so the catalog
    # matches the actual rule graph currently in use (e.g. persisted overrides).
    rules_path = root / "innate_script" / "config" / "innate_rules.yaml"
    try:
        if app is not None:
            p = getattr(getattr(app, "iesm", None), "_rules_path", None)
            if isinstance(p, str) and p.strip():
                candidate = Path(p).resolve()
                if candidate.exists():
                    rules_path = candidate
    except Exception:
        pass

    try:
        rules_doc = io.load_yaml_file(rules_path)
    except Exception:
        rules_doc = {}
    if isinstance(rules_doc, dict):
        specs.extend(build_iesm_rule_param_specs(rules_doc=rules_doc))

    # De-duplicate by param_id (keep first)
    seen: set[str] = set()
    out: list[ParamSpec] = []
    for s in specs:
        if s.param_id in seen:
            continue
        seen.add(s.param_id)
        out.append(s)
    return out


# ------------------------------
# Bounds heuristics
# ------------------------------


def guess_bound_for_param(spec: ParamSpec) -> ParamBound | None:
    """Best-effort bounds. Users can override via outputs/auto_tuner/config.json."""
    if not spec.auto_tune_allowed:
        return None
    if not _is_number(spec.value):
        return None

    name = spec.param_id.lower()
    v = float(spec.value)

    # Integer-like leaves
    is_int_like = spec.value_type == "int"

    # Heuristics by name patterns
    if name.endswith("_ms") or "timeout" in name or "sleep_ms" in name:
        lo, hi = 0.0, max(10.0, v * 4.0 + 100.0)
        step = max(10.0, abs(v) * 0.05)
        return ParamBound(lo, hi, step, quantum=1.0 if is_int_like else 1.0)

    if any(k in name for k in ["_ticks", "_rounds", "window", "top_n", "max_", "capacity", "history_keep", "max_items"]):
        if is_int_like:
            base = max(1.0, float(v))
            lo, hi = 0.0, max(8.0, math.ceil(base * 3.0 + 8.0))
            step = 1.0
            return ParamBound(lo, hi, step, quantum=1.0)
        base = max(0.0, float(v))
        lo, hi = 0.0, max(1.0, base * 3.0 + 1.0)
        step = max(0.05, abs(base) * 0.08)
        return ParamBound(lo, hi, step, quantum=0.01)

    if "decay_ratio" in name or "retention" in name:
        # Ratios close to 1.0 are extremely sensitive; use tiny steps.
        lo = 0.0
        hi = 0.9999999 if v > 0.99 else 1.2
        if v > 0.99:
            return ParamBound(lo, hi, max_step_abs=5e-6, quantum=1e-6)
        return ParamBound(lo, hi, max_step_abs=0.01, quantum=0.001)

    if "half_life" in name:
        lo, hi = 0.05, max(1.0, v * 5.0 + 1.0)
        step = 0.2 if v < 3.0 else 0.5
        return ParamBound(lo, hi, step, quantum=0.05)

    if "threshold" in name or name.endswith("_min") or name.endswith("_max"):
        lo, hi = 0.0, max(1.0, v * 2.5 + 0.2)
        step = 0.02 if v <= 2.0 else 0.05
        return ParamBound(lo, hi, step, quantum=0.01)

    if "ratio" in name or "scale" in name or "gain" in name:
        lo, hi = 0.0, max(1.5, v * 3.0 + 0.3)
        step = 0.02 if v < 1.0 else 0.05
        return ParamBound(lo, hi, step, quantum=0.01)

    # Generic numeric
    lo, hi = (0.0, max(1.0, v * 3.0 + 1.0)) if v >= 0.0 else (v * 3.0 - 1.0, max(0.0, v * -1.0 + 1.0))
    step = max(0.01, abs(v) * 0.05)
    quantum = 1.0 if is_int_like else 0.01
    return ParamBound(lo, hi, step, quantum=quantum)


def build_default_param_bounds(specs: Iterable[ParamSpec]) -> dict[str, ParamBound]:
    out: dict[str, ParamBound] = {}
    for spec in specs:
        b = guess_bound_for_param(spec)
        if b is None:
            continue
        out[spec.param_id] = b
    return out


# ------------------------------
# Output helpers (gitignored)
# ------------------------------


def auto_tuner_dir() -> Path:
    return storage.repo_root() / "observatory" / "outputs" / "auto_tuner"


def write_catalog_outputs(*, specs: Iterable[ParamSpec], bounds: dict[str, ParamBound] | None = None) -> dict[str, Any]:
    """
    Write catalog + bounds summary under outputs/auto_tuner/ (gitignored).
    Returns a small summary dict for auditing.
    """
    specs_list = list(specs)
    bounds = bounds or {}

    out_dir = auto_tuner_dir()
    out_dir.mkdir(parents=True, exist_ok=True)

    catalog_path = out_dir / "param_catalog.json"
    bounds_path = out_dir / "param_bounds.guessed.json"
    md_path = out_dir / "param_impact_table.md"

    catalog_payload = {
        "generated_at_ms": int(time.time() * 1000),
        "count": len(specs_list),
        "params": [s.to_dict() for s in specs_list],
    }
    catalog_path.write_text(json.dumps(catalog_payload, ensure_ascii=False, indent=2), encoding="utf-8")

    bounds_payload = {
        "generated_at_ms": int(time.time() * 1000),
        "count": len(bounds),
        "bounds": {k: dataclasses.asdict(v) for k, v in bounds.items()},
    }
    bounds_path.write_text(json.dumps(bounds_payload, ensure_ascii=False, indent=2), encoding="utf-8")

    # Human-friendly table (compact but searchable)
    lines: list[str] = []
    lines.append("# AutoTuner 参数-影响对应表（自动生成）")
    lines.append("")
    lines.append("说明：这是一个工程级“索引表”。`impacts` 是基于模块/命名启发式推断的“可能影响的长期指标”，用于调参器选参，不是论文级因果证明。")
    lines.append("")
    lines.append("| param_id | type | auto_tune | tags | impacts | bound |")
    lines.append("|---|---:|:---:|---|---|---|")
    for s in specs_list:
        b = bounds.get(s.param_id)
        bound_s = "-"
        if b is not None:
            bound_s = f"[{b.min_value:g},{b.max_value:g}] step<= {b.max_step_abs:g}"
        lines.append(
            f"| `{s.param_id}` | {s.value_type} | {'Y' if s.auto_tune_allowed else 'N'} | {', '.join(s.tags) or '-'} | {', '.join(s.impacts) or '-'} | {bound_s} |"
        )
    md_path.write_text("\n".join(lines) + "\n", encoding="utf-8")

    return {
        "catalog_path": str(catalog_path),
        "bounds_path": str(bounds_path),
        "impact_table_path": str(md_path),
        "param_count": len(specs_list),
        "bound_count": len(bounds),
    }
