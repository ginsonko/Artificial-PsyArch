# -*- coding: utf-8 -*-
"""
LLM Review (Post-Run)
====================

This module provides an OpenAI-compatible "reviewer" pipeline:
- Read a completed experiment run (manifest + metrics + dataset)
- Inject AP theory core text
- Ask an external LLM to produce a rigorous, nitpicky, actionable report

Notes:
- The Observatory is local-first; we store secrets under `observatory/outputs/`
  (gitignored) instead of committing them into repo config files.
- We intentionally keep the request shape compatible with OpenAI-style
  `/v1/chat/completions` endpoints so users can point to OpenAI or any
  compatible proxy.
"""

from __future__ import annotations

import json
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

from . import storage


@dataclass(frozen=True)
class LLMReviewConfig:
    enabled: bool = False
    auto_analyze_on_completion: bool = False
    base_url: str = "https://api.openai.com"
    api_key: str = ""
    model: str = ""
    temperature: float = 0.2
    max_prompt_chars: int = 900_000
    timeout_sec: int = 240
    max_completion_tokens: int = 4096

    def to_public_dict(self) -> dict[str, Any]:
        """Safe for front-end: do not include api_key."""
        return {
            "enabled": bool(self.enabled),
            "auto_analyze_on_completion": bool(self.auto_analyze_on_completion),
            "base_url": str(self.base_url or ""),
            "api_key_masked": mask_api_key(self.api_key),
            "model": str(self.model or ""),
            "temperature": float(self.temperature),
            "max_prompt_chars": int(self.max_prompt_chars),
            "timeout_sec": int(self.timeout_sec),
            "max_completion_tokens": int(self.max_completion_tokens),
        }


def mask_api_key(value: str) -> str:
    v = str(value or "").strip()
    if not v:
        return ""
    if len(v) <= 8:
        return "*" * len(v)
    return f"{v[:3]}...{v[-4:]}"


def _config_path() -> Path:
    # Store secrets in outputs/ (gitignored).
    return storage.repo_root() / "observatory" / "outputs" / "llm_review_config.json"


def load_review_config() -> LLMReviewConfig:
    path = _config_path()
    cfg = LLMReviewConfig()
    if not path.exists():
        return cfg
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return cfg
    if not isinstance(raw, dict):
        return cfg

    def _b(key: str, default: bool) -> bool:
        try:
            return bool(raw.get(key, default))
        except Exception:
            return bool(default)

    def _s(key: str, default: str) -> str:
        try:
            return str(raw.get(key, default) or "").strip() or str(default or "")
        except Exception:
            return str(default or "")

    def _f(key: str, default: float) -> float:
        try:
            return float(raw.get(key, default))
        except Exception:
            return float(default)

    def _i(key: str, default: int) -> int:
        try:
            return int(raw.get(key, default))
        except Exception:
            return int(default)

    return LLMReviewConfig(
        enabled=_b("enabled", cfg.enabled),
        auto_analyze_on_completion=_b("auto_analyze_on_completion", cfg.auto_analyze_on_completion),
        base_url=_s("base_url", cfg.base_url),
        api_key=_s("api_key", cfg.api_key),
        model=_s("model", cfg.model),
        temperature=_f("temperature", cfg.temperature),
        max_prompt_chars=_i("max_prompt_chars", cfg.max_prompt_chars),
        timeout_sec=_i("timeout_sec", cfg.timeout_sec),
        max_completion_tokens=_i("max_completion_tokens", cfg.max_completion_tokens),
    )


def save_review_config(updates: dict[str, Any]) -> LLMReviewConfig:
    """Persist review config under outputs/. api_key can be omitted to keep current."""
    current = load_review_config()
    updates = updates if isinstance(updates, dict) else {}

    def _pick_bool(key: str, default: bool) -> bool:
        if key not in updates:
            return bool(default)
        try:
            return bool(updates.get(key))
        except Exception:
            return bool(default)

    def _pick_str(key: str, default: str) -> str:
        if key not in updates:
            return str(default or "")
        try:
            return str(updates.get(key) or "").strip()
        except Exception:
            return str(default or "")

    def _pick_float(key: str, default: float) -> float:
        if key not in updates:
            return float(default)
        try:
            return float(updates.get(key))
        except Exception:
            return float(default)

    def _pick_int(key: str, default: int) -> int:
        if key not in updates:
            return int(default)
        try:
            return int(updates.get(key))
        except Exception:
            return int(default)

    new_api_key = current.api_key
    if "api_key" in updates:
        candidate = _pick_str("api_key", "")
        # Empty means "keep existing" (front-end can submit blank to avoid retyping).
        if candidate:
            new_api_key = candidate

    merged = LLMReviewConfig(
        enabled=_pick_bool("enabled", current.enabled),
        auto_analyze_on_completion=_pick_bool("auto_analyze_on_completion", current.auto_analyze_on_completion),
        base_url=_pick_str("base_url", current.base_url),
        api_key=str(new_api_key or ""),
        model=_pick_str("model", current.model),
        temperature=_pick_float("temperature", current.temperature),
        max_prompt_chars=_pick_int("max_prompt_chars", current.max_prompt_chars),
        timeout_sec=_pick_int("timeout_sec", current.timeout_sec),
        max_completion_tokens=_pick_int("max_completion_tokens", current.max_completion_tokens),
    )

    path = _config_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "enabled": bool(merged.enabled),
        "auto_analyze_on_completion": bool(merged.auto_analyze_on_completion),
        "base_url": str(merged.base_url or ""),
        "api_key": str(merged.api_key or ""),
        "model": str(merged.model or ""),
        "temperature": float(merged.temperature),
        "max_prompt_chars": int(merged.max_prompt_chars),
        "timeout_sec": int(merged.timeout_sec),
        "max_completion_tokens": int(merged.max_completion_tokens),
        "updated_at_ms": int(time.time() * 1000),
    }
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return merged


def _safe_read_text(path: Path, *, max_chars: int | None = None) -> str:
    try:
        text = path.read_text(encoding="utf-8", errors="replace")
    except Exception:
        try:
            text = path.read_text(encoding="utf-8", errors="ignore")
        except Exception:
            return ""
    if max_chars is not None and max_chars > 0 and len(text) > int(max_chars):
        return text[: int(max_chars)]
    return text


def _join_base_url(base_url: str, path: str) -> str:
    base = str(base_url or "").strip()
    if not base:
        base = "https://api.openai.com"
    base = base.rstrip("/")
    p = "/" + str(path or "").lstrip("/")
    return base + p


def _build_system_prompt() -> str:
    return (
        "你是一名严格、客观、略带找茬风格的审稿人（peer reviewer）。\n"
        "你要评审的是：一个本地运行的 Artificial PsyArch (AP) 原型系统，在一个数据集上的实验运行结果。\n"
        "\n"
        "总目标：帮助研究者通过调参、修改规则配置、调整模块参数，让系统表现更贴近 AP 理论核心文本的预期。\n"
        "注意：你的建议必须尽可能可执行，避免空话。\n"
        "\n"
        "重要背景（请务必基于本段理解当前原型的“流程口径”，否则会产生系统性误判）：\n"
        "0) 本原型支持两条“结构层闭环”的实验路径：\n"
        "   - Legacy 结构级查存一体（Structure-level Retrieval-Storage，简称 SLR-S）：以 HDB 的结构组/模板匹配为主，输出结构级轮次与组匹配等。\n"
        "   - 认知拼接（Cognitive Stitching，简称 CS-first）：用 CS 模块承担“事件认知/叙事化想法”的结构整合职责，从而可在实验中替代 Legacy SLR-S。\n"
        "   你需要先从 manifest/config/额外上下文中判断：本次 run 处于哪条路径。\n"
        "\n"
        "1) 当 `enable_structure_level_retrieval_storage=false`（结构级查存一体关闭）时：\n"
        "   - metrics 中 `structure_round_count` 很可能长期为 0，这在 CS-first 路径下是预期现象，不应直接判定为失败或“理论主干缺位”。\n"
        "   - 结构级阶段并非“消失”，而是会进入 CAM-only 内源刺激构造模式（mode 通常标记为 `cam_internal_stimulus_only`）：\n"
        "     输入：注意力模块输出的 CAM snapshot（当前注意记忆体）；\n"
        "     处理：把 CAM 条目转换为内源共现 stimulus fragments；\n"
        "     约束：仍会应用 DARL+PARS 内源分辨率预算（`internal_resolution_*`），用于压缩超长残差；\n"
        "     输出：internal_stimulus 与 merged_stimulus，然后继续 cache neutralization + 刺激级查存一体。\n"
        "   - 因为 DARL+PARS 是“预算式压缩”，`internal_sa_count / internal_resolution_selected_unit_count` 可能出现接近预算上限的平顶/flatline，这并不必然是统计 bug；\n"
        "     但如果多个指标长期“完全相等且无波动”，你可以提出“统计口径复用/重影”的排错建议（给出如何验证）。\n"
        "\n"
        "2) CS-first 路径下，“结构级整体认知”的对照口径是 CS 模块：\n"
        "   - CS 在每 tick 里会基于状态池活动对象与 HDB 边做：事件新建/扩展/合并/强化，并可能触发事件退化（组分淘汰）。\n"
        "   - CS 事件可被持久化入 HDB（sub_type=`cognitive_stitching_event_structure`），并通过 diff_table 链式关系参与刺激级查存一体的候选扩展。\n"
        "   - 因此当结构级关闭时，请用这些指标评估“结构层能力是否被 CS 替代并且运行健康”：\n"
        "     `cs_candidate_count / cs_action_count / cs_created_count / cs_extended_count / cs_merged_count / cs_reinforced_count`\n"
        "     以及 `cs_narrative_top_total_energy / cs_narrative_top_grasp` 与 `timing_cognitive_stitching_ms`。\n"
        "\n"
        "3) 结构级关闭时，HDB 的 `group_count` 可能长期为 0 也是预期（Legacy 结构组模板创建/匹配未启用）。\n"
        "   不要用 “group_count==0” 直接判定系统失败；应结合 CS 事件结构、刺激级链式扩展、以及行动闭环效果做判断。\n"
        "\n"
        "4) 实验跑批可能插入 synthetic ticks（例如 expectation contract 生成的教师反馈 tick）。\n"
        "   metrics 中：\n"
        "   - `tick_index` 是严格单调递增的“执行 tick 序号”（用于画图与趋势分析）；\n"
        "   - `dataset_tick_index` 保留数据集原序号（用于审计/回溯）。\n"
        "   请不要把两者混用来判断时间序列是否倒退。\n"
        "\n"
        "5) 期望契约（Expectation Contracts）在当前实现里有一个非常关键的口径：\n"
        "   - 合约注册来自“source tick”（数据集原始 tick）；\n"
        "   - 合约窗口的计数与满足判定，也只在后续 source tick 上进行；\n"
        "   - synthetic feedback ticks 不会消耗窗口、也不会被计入“满足条件”。\n"
        "   因此当你看到“合约全失败但系统似乎有行动执行迹象”时，要优先怀疑：\n"
        "   action 的异步延迟（async_delay_ticks）把执行落到了 synthetic tick 上，导致合约看不见。\n"
        "   你的建议应包含：如何用对照实验（sync vs async、within_ticks 变化）验证这一点。\n"
        "\n"
        "6) CFS（认知感受）同时存在两类观测口径：\n"
        "   - `cfs_*_max / cfs_*_count` 代表本 tick 输出的“感受信号事件峰值/次数”（可能被 emit_gate 抑制频率）；\n"
        "   - `cfs_*_live_total_*`（来自状态池 runtime bound attributes 汇总）代表“持续态实时能量总量（会按半衰期衰减）”。\n"
        "   你在评估“是否维持/是否回落”时，应优先使用 live_total 口径，而不是只盯 max 峰值。\n"
        "\n"
        "7) 行动闭环诊断口径：\n"
        "   - 行动记录至少要区分 attempted / scheduled / executed；不要只用“总体 action 数”推断某个 action_kind 没发生。\n"
        "   - 当你看到 `expectation_contract` 全失败时，不要立刻断言“行动闭环失败”；请先提出一套可证伪的排错顺序：\n"
        "     规则是否命中 -> action node 是否创建 -> 是否被延迟调度 -> 是否真正 executed -> 合约口径是否可见（source tick vs synthetic tick）。\n"
        "\n"
        "8) 时间感受器（TimeSensor）/延迟任务（Delayed Tasks）常会插入内部 tick 与后台开销：\n"
        "   - 若你要批评 time_sensor 过热，请明确引用相应指标（例如 bucket_energy_sum、delayed_task_executed_count）并给出对照实验（禁用 delayed_tasks、降低 top_k/gain、加入冷却窗）。\n"
        "   - 不要在未看到 config 目标阈值时直接宣称“expected_max=某值必然超标”；应把“阈值”作为可调参线索而不是定罪依据。\n"
        "\n"
        "写作要求：\n"
        "1) 结构化输出：摘要、符合理论之处、不符合理论之处、可能原因、可操作建议、建议的对照实验与观测指标。\n"
        "2) 尽量引用具体证据：引用 tick 区间、峰值、趋势、异常点（比如耗时尖峰、CFS 长期高位、奖励/惩罚失衡）。\n"
        "3) 允许提出你认为当前原型在工程实现上最可能的 bug/逻辑缺口，但要给出理由与如何验证。\n"
        "4) 不要假设你能访问本地文件系统；你只能使用我提供的上下文文本。\n"
    )


def build_review_prompt(
    *,
    run_id: str,
    config: LLMReviewConfig,
    theory_core_text: str,
    manifest_text: str,
    dataset_text: str,
    metrics_text: str,
    metrics_note: str,
    extra_context: str,
) -> str:
    # Keep prompt plain and auditable. No fancy tool calls.
    parts: list[str] = []
    parts.append("以下是 AP 理论核心文本（尽可能完整提供）：")
    parts.append("```text")
    parts.append(theory_core_text.strip())
    parts.append("```")
    parts.append("")

    parts.append("以下是本次实验运行的 manifest（运行元信息）：")
    parts.append("```json")
    parts.append(manifest_text.strip())
    parts.append("```")
    parts.append("")

    if dataset_text.strip():
        parts.append("以下是本次实验使用的数据集内容（可能是 normalized YAML 或 source 文件）：")
        parts.append("```text")
        parts.append(dataset_text.strip())
        parts.append("```")
        parts.append("")

    if extra_context.strip():
        parts.append("以下是额外上下文（模块配置、规则文件等）：")
        parts.append("```text")
        parts.append(extra_context.strip())
        parts.append("```")
        parts.append("")

    parts.append("以下是本次运行的 metrics.jsonl（逐 tick 指标记录）。")
    parts.append(metrics_note.strip())
    parts.append("```jsonl")
    parts.append(metrics_text.strip())
    parts.append("```")
    parts.append("")

    parts.append(
        "请你基于以上材料，输出一份严谨、客观、找茬式的审稿报告。"
        "重点回答：\n"
        "1) 哪些模块/图表维度与理论吻合，证据是什么？\n"
        "2) 哪些模块/图表维度与理论不吻合，证据是什么？\n"
        "3) 你认为最优先的 3-8 个改进点是什么？每个改进点都要给出：改哪里、怎么改、为什么、风险与验证方式。\n"
        "4) 如果你只能让研究者改 5 个参数（不改代码），你建议改哪 5 个？给出建议范围与预期影响。\n"
        "5) 如果允许改代码，你建议增加哪些观测字段/保护机制/资源预算，来让系统更可控、可审计？\n"
    )

    return "\n".join(parts).strip() + "\n"


def _read_run_artifacts(*, run_dir: Path, max_prompt_chars: int) -> tuple[str, str, str, str, str]:
    manifest_path = run_dir / "manifest.json"
    metrics_path = run_dir / "metrics.jsonl"
    dataset_norm = run_dir / "dataset.normalized.yaml"

    manifest_text = _safe_read_text(manifest_path, max_chars=200_000)

    dataset_text = ""
    if dataset_norm.exists():
        dataset_text = _safe_read_text(dataset_norm, max_chars=200_000)
    else:
        # fallback: copy of source dataset (runner writes dataset.source.*)
        for p in sorted(run_dir.glob("dataset.source*")):
            dataset_text = _safe_read_text(p, max_chars=200_000)
            if dataset_text.strip():
                break

    metrics_note = ""
    metrics_text = ""
    if metrics_path.exists():
        try:
            size = int(metrics_path.stat().st_size)
        except Exception:
            size = -1

        # Try to include as much as possible, but keep prompt bounded.
        # Strategy:
        # - If the file size is within max_prompt_chars, include it fully.
        # - Otherwise, provide a head+tail excerpt (so the reviewer sees both early and late dynamics).
        if size >= 0 and size <= int(max_prompt_chars):
            metrics_text = _safe_read_text(metrics_path, max_chars=None)
            metrics_note = "(metrics.jsonl 已全量包含。)"
        else:
            head = []
            tail = []
            tail_keep = 120
            head_keep = 240
            try:
                with metrics_path.open("r", encoding="utf-8", errors="replace") as fh:
                    for i, line in enumerate(fh):
                        if i < head_keep:
                            head.append(line.rstrip("\n"))
                        if line.strip():
                            tail.append(line.rstrip("\n"))
                            if len(tail) > tail_keep:
                                tail = tail[-tail_keep:]
            except Exception:
                head = []
                tail = []
            metrics_text = "\n".join(head + ["", "# ...(middle omitted for context budget)...", ""] + tail).strip()
            metrics_note = (
                f"(metrics.jsonl 体量较大，已按“头 {head_keep} 行 + 尾 {tail_keep} 行”提供；"
                "如需更长上下文，可在前端提高 max_prompt_chars 或分段分析。)"
            )

    return (manifest_text, dataset_text, metrics_text, metrics_note, str(metrics_path))


def _read_ap_theory_core_text(*, max_chars: int) -> str:
    # Repo root: try the known file first.
    root = storage.repo_root()
    candidates = [
        root / "txt版本的理论核心.txt",
        root / "AP理论核心.txt",
    ]
    for p in candidates:
        if p.exists() and p.is_file():
            return _safe_read_text(p, max_chars=max_chars)
    # fallback empty
    return ""


def _read_extra_context(*, max_chars: int) -> str:
    # Provide key config/rules sources to help the reviewer propose actionable tuning.
    root = storage.repo_root()
    paths = [
        root / "observatory" / "config" / "observatory_config.yaml",
        root / "cognitive_stitching" / "config" / "cognitive_stitching_config.yaml",
        root / "hdb" / "config" / "hdb_config.yaml",
        root / "state_pool" / "config" / "state_pool_config.yaml",
        root / "attention" / "config" / "attention_config.yaml",
        root / "time_sensor" / "config" / "time_sensor_config.yaml",
        root / "energy_balance" / "config" / "energy_balance_config.yaml",
        root / "action" / "config" / "action_config.yaml",
        root / "innate_script" / "config" / "innate_script_config.yaml",
        root / "innate_script" / "config" / "innate_rules.yaml",
    ]
    chunks: list[str] = []
    for p in paths:
        if not p.exists() or not p.is_file():
            continue
        text = _safe_read_text(p, max_chars=max_chars // max(1, len(paths)))
        if not text.strip():
            continue
        chunks.append(f"[FILE] {p}\n{text}".strip())
    return "\n\n".join(chunks).strip()


def call_openai_chat_completions(
    *,
    config: LLMReviewConfig,
    system_prompt: str,
    user_prompt: str,
) -> dict[str, Any]:
    if not config.model.strip():
        raise ValueError("LLM model is empty")
    if not config.base_url.strip():
        raise ValueError("LLM base_url is empty")

    url = _join_base_url(config.base_url, "/v1/chat/completions")
    headers = {
        "Content-Type": "application/json",
        "User-Agent": "ap-observatory-llm-review/0.1",
    }
    if config.api_key.strip():
        headers["Authorization"] = f"Bearer {config.api_key.strip()}"

    body = {
        "model": str(config.model),
        "temperature": float(config.temperature),
        "max_tokens": int(config.max_completion_tokens),
        "messages": [
            {"role": "system", "content": str(system_prompt or "")},
            {"role": "user", "content": str(user_prompt or "")},
        ],
    }
    data = json.dumps(body, ensure_ascii=False).encode("utf-8")

    req = urllib.request.Request(url, data=data, headers=headers, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=int(config.timeout_sec)) as resp:
            raw = resp.read().decode("utf-8", errors="replace")
            parsed = json.loads(raw)
            if not isinstance(parsed, dict):
                return {"success": False, "error": "invalid_json_response", "raw": raw}
            return {"success": True, "data": parsed}
    except urllib.error.HTTPError as exc:
        raw = ""
        try:
            raw = exc.read().decode("utf-8", errors="replace")
        except Exception:
            raw = ""
        return {
            "success": False,
            "error": f"http_error:{exc.code}",
            "message": str(exc),
            "raw": raw,
        }
    except Exception as exc:
        return {"success": False, "error": "request_failed", "message": str(exc)}

def _extract_delta_from_stream_event(payload: dict[str, Any]) -> str:
    """
    Best-effort extractor for OpenAI-compatible streaming events.

    Common shapes:
      - choices[0].delta.content
      - choices[0].message.content (some proxies)
    """
    try:
        choices = payload.get("choices")
        if not isinstance(choices, list) or not choices:
            return ""
        c0 = choices[0] if isinstance(choices[0], dict) else {}
        delta = c0.get("delta") if isinstance(c0.get("delta"), dict) else {}
        if isinstance(delta, dict) and str(delta.get("content", "") or ""):
            return str(delta.get("content") or "")
        msg = c0.get("message") if isinstance(c0.get("message"), dict) else {}
        if isinstance(msg, dict) and str(msg.get("content", "") or ""):
            return str(msg.get("content") or "")
    except Exception:
        return ""
    return ""


def call_openai_chat_completions_stream(
    *,
    config: LLMReviewConfig,
    system_prompt: str,
    user_prompt: str,
    on_delta: Callable[[str], None] | None = None,
) -> dict[str, Any]:
    """
    OpenAI-compatible chat.completions with optional SSE streaming.

    Return:
      - success True: {"text": "...", "stream": bool, "data": final_payload_if_any}
      - success False: {"error": "...", "message": "...", "raw": "...", "url": "..."}
    """
    if not config.model.strip():
        raise ValueError("LLM model is empty")
    if not config.base_url.strip():
        raise ValueError("LLM base_url is empty")

    url = _join_base_url(config.base_url, "/v1/chat/completions")
    headers = {
        "Content-Type": "application/json",
        "Accept": "text/event-stream",
        "User-Agent": "ap-observatory-llm-review/0.2",
    }
    if config.api_key.strip():
        headers["Authorization"] = f"Bearer {config.api_key.strip()}"

    body = {
        "model": str(config.model),
        "temperature": float(config.temperature),
        "max_tokens": int(config.max_completion_tokens),
        "stream": True,
        "messages": [
            {"role": "system", "content": str(system_prompt or "")},
            {"role": "user", "content": str(user_prompt or "")},
        ],
    }
    data = json.dumps(body, ensure_ascii=False).encode("utf-8")

    req = urllib.request.Request(url, data=data, headers=headers, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=int(config.timeout_sec)) as resp:
            # Some proxies ignore stream=True and return a normal JSON payload.
            content_type = str(getattr(resp, "headers", {}).get("Content-Type", "") or "").lower()
            is_sse = "text/event-stream" in content_type or "event-stream" in content_type
            if not is_sse:
                raw = resp.read().decode("utf-8", errors="replace")
                try:
                    parsed = json.loads(raw)
                except Exception:
                    return {"success": False, "error": "invalid_json_response", "raw": raw, "url": url}
                text = ""
                if isinstance(parsed, dict):
                    text = _extract_text_from_chat_completions(parsed)
                return {"success": True, "stream": False, "text": str(text or ""), "data": parsed, "url": url}

            chunks: list[str] = []
            while True:
                raw_line = resp.readline()
                if not raw_line:
                    break
                line = raw_line.decode("utf-8", errors="replace").strip()
                if not line:
                    continue
                if not line.startswith("data:"):
                    continue
                payload_text = line[len("data:") :].strip()
                if payload_text == "[DONE]":
                    break
                try:
                    payload = json.loads(payload_text)
                except Exception:
                    continue
                if not isinstance(payload, dict):
                    continue
                delta = _extract_delta_from_stream_event(payload)
                if not delta:
                    continue
                chunks.append(delta)
                if on_delta is not None:
                    try:
                        on_delta(delta)
                    except Exception:
                        pass

            return {"success": True, "stream": True, "text": "".join(chunks), "data": None, "url": url}
    except urllib.error.HTTPError as exc:
        raw = ""
        try:
            raw = exc.read().decode("utf-8", errors="replace")
        except Exception:
            raw = ""
        return {
            "success": False,
            "error": f"http_error:{exc.code}",
            "message": str(exc),
            "raw": raw,
            "url": url,
        }
    except Exception as exc:
        return {"success": False, "error": "request_failed", "message": str(exc), "raw": "", "url": url}


def _extract_text_from_chat_completions(payload: dict[str, Any]) -> str:
    # OpenAI chat.completions: choices[0].message.content
    try:
        choices = payload.get("choices")
        if not isinstance(choices, list) or not choices:
            return ""
        msg = choices[0].get("message") if isinstance(choices[0], dict) else None
        if not isinstance(msg, dict):
            return ""
        return str(msg.get("content", "") or "")
    except Exception:
        return ""


def review_run_with_llm(*, run_id: str, config: LLMReviewConfig | None = None) -> dict[str, Any]:
    cfg = config or load_review_config()
    run_dir = storage.resolve_run_dir(run_id)
    if not run_dir.exists():
        return {"success": False, "error": f"run not found: {run_id}"}

    if not cfg.enabled:
        return {"success": False, "error": "llm_review_disabled"}
    if not cfg.model.strip():
        return {"success": False, "error": "llm_model_missing"}
    if not cfg.base_url.strip():
        return {"success": False, "error": "llm_base_url_missing"}

    started_at_ms = int(time.time() * 1000)
    status_path = run_dir / "llm_review.status.json"
    out_path = run_dir / "llm_review.report.md"
    raw_path = run_dir / "llm_review.raw.json"
    err_path = run_dir / "llm_review.error.txt"

    def _write_status(status: str, extra: dict[str, Any] | None = None) -> None:
        payload = {
            "run_id": str(run_id),
            "status": str(status),
            "started_at_ms": int(started_at_ms),
            "updated_at_ms": int(time.time() * 1000),
            "model": str(cfg.model),
            "base_url": str(cfg.base_url),
        }
        if extra:
            payload.update(extra)
        try:
            status_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        except Exception:
            pass

    _write_status("running", {"stage": "building_prompt"})

    # Build prompt (long, but bounded by config.max_prompt_chars).
    max_chars = int(max(50_000, cfg.max_prompt_chars))
    theory = _read_ap_theory_core_text(max_chars=max_chars)
    extra_context = _read_extra_context(max_chars=max_chars)
    manifest_text, dataset_text, metrics_text, metrics_note, metrics_path_str = _read_run_artifacts(
        run_dir=run_dir, max_prompt_chars=max_chars
    )

    system_prompt = _build_system_prompt()
    user_prompt = build_review_prompt(
        run_id=run_id,
        config=cfg,
        theory_core_text=theory,
        manifest_text=manifest_text,
        dataset_text=dataset_text,
        metrics_text=metrics_text,
        metrics_note=metrics_note,
        extra_context=extra_context,
    )
    # Trim final prompt if still too long (safety net).
    if len(user_prompt) > max_chars:
        user_prompt = user_prompt[:max_chars] + "\n\n[TRUNCATED: prompt exceeds max_prompt_chars]\n"

    _write_status(
        "running",
        {
            "stage": "calling_llm",
            "prompt_chars": int(len(user_prompt)),
            "theory_chars": int(len(theory)),
            "metrics_path": metrics_path_str,
            "received_chars": 0,
            "streaming": True,
        },
    )

    # Ensure report path exists early so the UI can show streaming progress.
    try:
        out_path.write_text("", encoding="utf-8")
    except Exception:
        pass

    received_chars = 0
    pending_chunks: list[str] = []
    last_flush_ms = int(time.time() * 1000)

    def _flush_partial(force: bool = False) -> None:
        nonlocal pending_chunks, last_flush_ms
        if not pending_chunks:
            return
        now_ms = int(time.time() * 1000)
        if not force and now_ms - last_flush_ms < 650 and sum(len(x) for x in pending_chunks) < 4096:
            return
        try:
            with out_path.open("a", encoding="utf-8") as fh:
                fh.write("".join(pending_chunks))
        except Exception:
            pass
        pending_chunks = []
        last_flush_ms = now_ms
        _write_status(
            "running",
            {
                "stage": "streaming",
                "received_chars": int(received_chars),
                "updated_at_ms": now_ms,
            },
        )

    def _on_delta(delta: str) -> None:
        nonlocal received_chars, pending_chunks
        if not delta:
            return
        pending_chunks.append(str(delta))
        received_chars += len(delta)
        _flush_partial(force=False)

    res = call_openai_chat_completions_stream(config=cfg, system_prompt=system_prompt, user_prompt=user_prompt, on_delta=_on_delta)
    _flush_partial(force=True)
    if not res.get("success", False):
        # Persist raw error for audit.
        raw_err = str(res.get("raw", "") or "")
        try:
            err_path.write_text(raw_err, encoding="utf-8")
        except Exception:
            pass
        # Also surface the error inside the report file so the UI can display it
        # without needing a dedicated "raw error" endpoint.
        try:
            with out_path.open("a", encoding="utf-8") as fh:
                fh.write("\n\n---\n\n")
                fh.write("# LLM Review Failed\n\n")
                fh.write(f"- error: {res.get('error', '')}\n")
                fh.write(f"- message: {res.get('message', '')}\n")
                fh.write(f"- url: {res.get('url', '')}\n\n")
                if raw_err.strip():
                    fh.write("```text\n")
                    fh.write(raw_err[:8000])
                    fh.write("\n```\n")
        except Exception:
            pass
        _write_status(
            "failed",
            {
                "stage": "failed",
                "error": res.get("error", ""),
                "message": res.get("message", ""),
                "url": res.get("url", ""),
                "error_path": str(err_path),
                "error_preview": raw_err[:8000],
                "finished_at_ms": int(time.time() * 1000),
            },
        )
        try:
            raw_path.write_text(json.dumps(res, ensure_ascii=False, indent=2), encoding="utf-8")
        except Exception:
            pass
        return {"success": False, "error": res.get("error", "failed"), "message": res.get("message", "")}

    text = str(res.get("text", "") or "")
    finished_at_ms = int(time.time() * 1000)
    try:
        # Append any buffered output (if any) and ensure newline at end.
        if text:
            out_path.write_text(str(text or "").strip() + "\n", encoding="utf-8")
    except Exception:
        pass
    try:
        raw_path.write_text(json.dumps(res, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception:
        pass

    _write_status(
        "completed",
        {
            "stage": "completed",
            "finished_at_ms": finished_at_ms,
            "report_path": str(out_path),
            "raw_path": str(raw_path),
            "received_chars": int(len(text or "")),
            "streaming": bool(res.get("stream", False)),
            "url": res.get("url", ""),
        },
    )
    return {
        "success": True,
        "run_id": run_id,
        "status": "completed",
        "report_path": str(out_path),
        "raw_path": str(raw_path),
        "prompt_chars": int(len(user_prompt)),
        "finished_at_ms": finished_at_ms,
    }


def read_review_status(*, run_id: str) -> dict[str, Any]:
    run_dir = storage.resolve_run_dir(run_id)
    status_path = run_dir / "llm_review.status.json"
    if not status_path.exists():
        return {"run_id": run_id, "status": "not_started"}
    try:
        raw = json.loads(status_path.read_text(encoding="utf-8"))
        return raw if isinstance(raw, dict) else {"run_id": run_id, "status": "unknown"}
    except Exception:
        return {"run_id": run_id, "status": "unknown"}


def read_review_report(*, run_id: str, max_chars: int = 800_000) -> dict[str, Any]:
    run_dir = storage.resolve_run_dir(run_id)
    report_path = run_dir / "llm_review.report.md"
    if not report_path.exists():
        return {"run_id": run_id, "exists": False, "text": ""}
    text = _safe_read_text(report_path, max_chars=max_chars)
    return {"run_id": run_id, "exists": True, "text": text, "path": str(report_path)}
