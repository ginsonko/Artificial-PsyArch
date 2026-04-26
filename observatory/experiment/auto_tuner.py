# -*- coding: utf-8 -*-
"""
Adaptive Auto-Tuner (MVP)
=========================

This module implements the "自适应调参器" idea from the AP theory core:
- monitor long-running metrics (per tick)
- define an expected normal range + ideal value per metric
- apply small, auditable parameter nudges:
  - short-term mode: trigger when metrics drift out of normal range
  - long-term mode: trigger at run completion (or fixed period) toward ideal values

Design constraints (from the repo philosophy):
- local-first, auditable, and reversible
- do not auto-edit tracked repo config files
  -> all overrides are written under `observatory/outputs/auto_tuner/` (gitignored)
- no semantic "hacks" (no stopword/regex shortcuts); prefer budget coupling, fatigue, thresholds
"""

from __future__ import annotations

import json
import math
import time
import copy
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable, Optional

from . import io, storage, param_catalog
from .llm_analysis import (
    LLMReviewConfig,
    call_openai_chat_completions,
    load_review_config,
    mask_api_key,
)


# ------------------------------
# Storage layout (gitignored)
# ------------------------------


def _auto_tuner_dir() -> Path:
    return storage.repo_root() / "observatory" / "outputs" / "auto_tuner"


def _overrides_dir() -> Path:
    return _auto_tuner_dir() / "overrides"


def _state_path() -> Path:
    return _auto_tuner_dir() / "state.json"


def _config_path() -> Path:
    return _auto_tuner_dir() / "config.json"


def _rules_path() -> Path:
    return _auto_tuner_dir() / "rules.json"


def _rollback_points_path() -> Path:
    return _auto_tuner_dir() / "rollback_points.json"


def _global_audit_path() -> Path:
    return _auto_tuner_dir() / "audit.global.jsonl"


def _llm_config_path() -> Path:
    return _auto_tuner_dir() / "llm_config.json"


def _llm_suggestions_dir() -> Path:
    return _auto_tuner_dir() / "llm_suggestions"


def _now_ms() -> int:
    return int(time.time() * 1000)


def _load_json_dict(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return raw if isinstance(raw, dict) else {}


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _load_jsonl(path: Path, *, limit: int = 200) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    try:
        with path.open("r", encoding="utf-8") as fh:
            for line in fh:
                text = str(line or "").strip()
                if not text:
                    continue
                try:
                    item = json.loads(text)
                except Exception:
                    continue
                if isinstance(item, dict):
                    rows.append(item)
    except Exception:
        return []
    if limit > 0:
        return rows[-int(limit) :]
    return rows


def _load_rollback_points() -> list[dict[str, Any]]:
    raw = _load_json_dict(_rollback_points_path())
    items = raw.get("points")
    return list(items) if isinstance(items, list) else []


def _save_rollback_points(points: list[dict[str, Any]]) -> None:
    cleaned = [dict(p) for p in points if isinstance(p, dict)]
    cleaned.sort(key=lambda item: int(item.get("created_at_ms", 0) or 0), reverse=True)
    _write_json(
        _rollback_points_path(),
        {
            "schema_version": "v1",
            "updated_at_ms": _now_ms(),
            "points": cleaned[:200],
        },
    )


def _append_rollback_point(point: dict[str, Any]) -> None:
    points = _load_rollback_points()
    points.insert(0, dict(point))
    _save_rollback_points(points)


def _default_state_payload() -> dict[str, Any]:
    return {
        "schema_version": "v3",
        "persisted_params": {},
        "rule_health": {},
        "active_trials": [],
        "trial_history": [],
        "last_applied_updates": [],
        "rule_observations": [],
        "observation_history": [],
        "observation_review_history": [],
        "last_observation_review": {},
    }


# ------------------------------
# Config + state
# ------------------------------


@dataclass(frozen=True)
class MetricTarget:
    key: str
    expected_min: float
    expected_max: float
    ideal: float
    # A soft "do not overfit into a flat line" hint:
    # if std becomes too small while still in-range, avoid further tightening.
    min_std: float = 0.0
    weight: float = 1.0
    # Optional distribution-guided envelope for "strong-feeling" metrics.
    # This is intentionally a soft philosophy guide rather than a hard ban:
    # - values above `high_band_threshold` may happen,
    # - but they should not dominate normal operation.
    high_band_threshold: float | None = None
    high_band_max_ratio: float | None = None
    high_band_soft_p95: float | None = None
    high_band_max_run: int | None = None


@dataclass(frozen=True)
class ParamBound:
    min_value: float
    max_value: float
    max_step_abs: float
    # Optional: minimal resolution for rounding (e.g. 0.01).
    quantum: float = 0.0


@dataclass(frozen=True)
class AutoTunerConfig:
    enabled: bool = False
    enable_short_term: bool = True
    enable_long_term: bool = True
    short_window_ticks: int = 10
    long_window_ticks: int = 40
    decision_cooldown_ticks: int = 2
    max_param_updates_per_tick: int = 4
    persist_overrides: bool = True
    # When a metric stays out-of-range for too long and local heuristics fail,
    # we can optionally ask an external LLM for suggestions (disabled by default).
    llm_assist_enabled: bool = False
    llm_assist_trigger_windows: int = 6
    # Observation-zone auto validation:
    # LLM-generated rules can enter an observation zone first, then be reviewed
    # across multiple runs before being solidified or reverted.
    llm_auto_validation_enabled: bool = False
    llm_auto_validation_min_runs: int = 2
    llm_auto_validation_max_observations_per_review: int = 4
    llm_auto_validation_review_every_run: bool = True
    # Backoff / anti-thrash:
    # - When a parameter repeatedly hits bounds or yields no improvement, apply exponential cooldown.
    # - Keeps short-term auto-tuning bounded and prevents "infinite push" loops.
    param_backoff_enabled: bool = True
    param_backoff_base_cooldown_ticks: int = 6
    param_backoff_max_cooldown_ticks: int = 120
    param_backoff_failure_penalty: float = 1.0
    param_backoff_neutral_penalty: float = 0.35

    def to_public_dict(self) -> dict[str, Any]:
        return {
            "enabled": bool(self.enabled),
            "enable_short_term": bool(self.enable_short_term),
            "enable_long_term": bool(self.enable_long_term),
            "short_window_ticks": int(self.short_window_ticks),
            "long_window_ticks": int(self.long_window_ticks),
            "decision_cooldown_ticks": int(self.decision_cooldown_ticks),
            "max_param_updates_per_tick": int(self.max_param_updates_per_tick),
            "persist_overrides": bool(self.persist_overrides),
            "llm_assist_enabled": bool(self.llm_assist_enabled),
            "llm_assist_trigger_windows": int(self.llm_assist_trigger_windows),
            "llm_auto_validation_enabled": bool(self.llm_auto_validation_enabled),
            "llm_auto_validation_min_runs": int(self.llm_auto_validation_min_runs),
            "llm_auto_validation_max_observations_per_review": int(self.llm_auto_validation_max_observations_per_review),
            "llm_auto_validation_review_every_run": bool(self.llm_auto_validation_review_every_run),
            "param_backoff_enabled": bool(self.param_backoff_enabled),
            "param_backoff_base_cooldown_ticks": int(self.param_backoff_base_cooldown_ticks),
            "param_backoff_max_cooldown_ticks": int(self.param_backoff_max_cooldown_ticks),
            "param_backoff_failure_penalty": float(self.param_backoff_failure_penalty),
            "param_backoff_neutral_penalty": float(self.param_backoff_neutral_penalty),
        }


def load_auto_tuner_config() -> AutoTunerConfig:
    path = _config_path()
    cfg = AutoTunerConfig()
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

    def _i(key: str, default: int) -> int:
        try:
            return int(raw.get(key, default))
        except Exception:
            return int(default)

    return AutoTunerConfig(
        enabled=_b("enabled", cfg.enabled),
        enable_short_term=_b("enable_short_term", cfg.enable_short_term),
        enable_long_term=_b("enable_long_term", cfg.enable_long_term),
        short_window_ticks=max(3, _i("short_window_ticks", cfg.short_window_ticks)),
        long_window_ticks=max(10, _i("long_window_ticks", cfg.long_window_ticks)),
        decision_cooldown_ticks=max(0, _i("decision_cooldown_ticks", cfg.decision_cooldown_ticks)),
        max_param_updates_per_tick=max(1, _i("max_param_updates_per_tick", cfg.max_param_updates_per_tick)),
        persist_overrides=_b("persist_overrides", cfg.persist_overrides),
        llm_assist_enabled=_b("llm_assist_enabled", cfg.llm_assist_enabled),
        llm_assist_trigger_windows=max(2, _i("llm_assist_trigger_windows", cfg.llm_assist_trigger_windows)),
        llm_auto_validation_enabled=_b("llm_auto_validation_enabled", cfg.llm_auto_validation_enabled),
        llm_auto_validation_min_runs=max(1, _i("llm_auto_validation_min_runs", cfg.llm_auto_validation_min_runs)),
        llm_auto_validation_max_observations_per_review=max(
            1,
            _i("llm_auto_validation_max_observations_per_review", cfg.llm_auto_validation_max_observations_per_review),
        ),
        llm_auto_validation_review_every_run=_b(
            "llm_auto_validation_review_every_run",
            cfg.llm_auto_validation_review_every_run,
        ),
        param_backoff_enabled=_b("param_backoff_enabled", cfg.param_backoff_enabled),
        param_backoff_base_cooldown_ticks=max(2, _i("param_backoff_base_cooldown_ticks", cfg.param_backoff_base_cooldown_ticks)),
        param_backoff_max_cooldown_ticks=max(10, _i("param_backoff_max_cooldown_ticks", cfg.param_backoff_max_cooldown_ticks)),
        param_backoff_failure_penalty=float(raw.get("param_backoff_failure_penalty", cfg.param_backoff_failure_penalty)),
        param_backoff_neutral_penalty=float(raw.get("param_backoff_neutral_penalty", cfg.param_backoff_neutral_penalty)),
    )


def _load_raw_config_dict() -> dict[str, Any]:
    path = _config_path()
    if not path.exists():
        return {}
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return raw if isinstance(raw, dict) else {}


def save_auto_tuner_config(updates: dict[str, Any]) -> AutoTunerConfig:
    current = load_auto_tuner_config()
    updates = updates if isinstance(updates, dict) else {}

    def _pick_bool(key: str, default: bool) -> bool:
        if key not in updates:
            return bool(default)
        try:
            return bool(updates.get(key))
        except Exception:
            return bool(default)

    def _pick_int(key: str, default: int) -> int:
        if key not in updates:
            return int(default)
        try:
            return int(updates.get(key))
        except Exception:
            return int(default)

    merged = AutoTunerConfig(
        enabled=_pick_bool("enabled", current.enabled),
        enable_short_term=_pick_bool("enable_short_term", current.enable_short_term),
        enable_long_term=_pick_bool("enable_long_term", current.enable_long_term),
        short_window_ticks=max(3, _pick_int("short_window_ticks", current.short_window_ticks)),
        long_window_ticks=max(10, _pick_int("long_window_ticks", current.long_window_ticks)),
        decision_cooldown_ticks=max(0, _pick_int("decision_cooldown_ticks", current.decision_cooldown_ticks)),
        max_param_updates_per_tick=max(1, _pick_int("max_param_updates_per_tick", current.max_param_updates_per_tick)),
        persist_overrides=_pick_bool("persist_overrides", current.persist_overrides),
        llm_assist_enabled=_pick_bool("llm_assist_enabled", current.llm_assist_enabled),
        llm_assist_trigger_windows=max(2, _pick_int("llm_assist_trigger_windows", current.llm_assist_trigger_windows)),
        llm_auto_validation_enabled=_pick_bool("llm_auto_validation_enabled", current.llm_auto_validation_enabled),
        llm_auto_validation_min_runs=max(1, _pick_int("llm_auto_validation_min_runs", current.llm_auto_validation_min_runs)),
        llm_auto_validation_max_observations_per_review=max(
            1,
            _pick_int(
                "llm_auto_validation_max_observations_per_review",
                current.llm_auto_validation_max_observations_per_review,
            ),
        ),
        llm_auto_validation_review_every_run=_pick_bool(
            "llm_auto_validation_review_every_run",
            current.llm_auto_validation_review_every_run,
        ),
        param_backoff_enabled=_pick_bool("param_backoff_enabled", current.param_backoff_enabled),
        param_backoff_base_cooldown_ticks=max(
            2,
            _pick_int("param_backoff_base_cooldown_ticks", current.param_backoff_base_cooldown_ticks),
        ),
        param_backoff_max_cooldown_ticks=max(
            10,
            _pick_int("param_backoff_max_cooldown_ticks", current.param_backoff_max_cooldown_ticks),
        ),
        param_backoff_failure_penalty=float(updates.get("param_backoff_failure_penalty", current.param_backoff_failure_penalty)),
        param_backoff_neutral_penalty=float(updates.get("param_backoff_neutral_penalty", current.param_backoff_neutral_penalty)),
    )

    path = _config_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {**merged.to_public_dict(), "updated_at_ms": int(time.time() * 1000)}
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return merged


def _normalize_metric_target_item(item: dict[str, Any]) -> dict[str, Any] | None:
    if not isinstance(item, dict):
        return None
    key = str(item.get("key", "") or "").strip()
    if not key:
        return None
    out = {
        "key": key,
        "expected_min": float(item.get("expected_min", 0.0)),
        "expected_max": float(item.get("expected_max", 0.0)),
        "ideal": float(item.get("ideal", 0.0)),
        "min_std": float(item.get("min_std", 0.0)),
        "weight": float(item.get("weight", 1.0)),
    }
    if "high_band_threshold" in item:
        out["high_band_threshold"] = _safe_float_or_none(item.get("high_band_threshold"))
    if "high_band_max_ratio" in item:
        out["high_band_max_ratio"] = _safe_float_or_none(item.get("high_band_max_ratio"))
    if "high_band_soft_p95" in item:
        out["high_band_soft_p95"] = _safe_float_or_none(item.get("high_band_soft_p95"))
    if "high_band_max_run" in item:
        out["high_band_max_run"] = _safe_int_or_none(item.get("high_band_max_run"))
    return out


def _normalize_param_bound_item(value: dict[str, Any]) -> dict[str, Any] | None:
    if not isinstance(value, dict):
        return None
    try:
        return {
            "min_value": float(value.get("min_value", 0.0)),
            "max_value": float(value.get("max_value", 0.0)),
            "max_step_abs": float(value.get("max_step_abs", 0.0)),
            "quantum": float(value.get("quantum", 0.0)),
        }
    except Exception:
        return None


def _merge_metric_target_defs() -> list[dict[str, Any]]:
    meta_by_key = {str(item.get("key", "")): dict(item) for item in param_catalog.list_metric_definitions()}
    raw_cfg = _load_raw_config_dict()
    overrides = raw_cfg.get("metric_targets")
    if isinstance(overrides, list):
        for item in overrides:
            norm = _normalize_metric_target_item(item)
            if not norm:
                continue
            merged = dict(meta_by_key.get(norm["key"], {}))
            merged.update(norm)
            meta_by_key[norm["key"]] = merged
    out = list(meta_by_key.values())
    out.sort(key=lambda item: (str(item.get("group", "")), str(item.get("key", ""))))
    return out


def load_auto_tuner_public_config() -> dict[str, Any]:
    raw_cfg = _load_raw_config_dict()
    metric_targets = _merge_metric_target_defs()
    param_bounds = raw_cfg.get("param_bounds")
    if not isinstance(param_bounds, dict):
        param_bounds = {}
    return {
        "config": load_auto_tuner_config().to_public_dict(),
        "metric_targets": metric_targets,
        "param_bounds": dict(param_bounds),
        "config_path": str(_config_path()),
        "rules_path": str(_rules_path()),
        "rollback_points_path": str(_rollback_points_path()),
    }


def save_auto_tuner_public_config(updates: dict[str, Any]) -> dict[str, Any]:
    updates = updates if isinstance(updates, dict) else {}
    raw_cfg = _load_raw_config_dict()
    current = load_auto_tuner_config()

    def _pick_bool(key: str, default: bool) -> bool:
        if key not in updates:
            return bool(default)
        try:
            return bool(updates.get(key))
        except Exception:
            return bool(default)

    def _pick_int(key: str, default: int) -> int:
        if key not in updates:
            return int(default)
        try:
            return int(updates.get(key))
        except Exception:
            return int(default)

    merged_cfg = AutoTunerConfig(
        enabled=_pick_bool("enabled", current.enabled),
        enable_short_term=_pick_bool("enable_short_term", current.enable_short_term),
        enable_long_term=_pick_bool("enable_long_term", current.enable_long_term),
        short_window_ticks=max(3, _pick_int("short_window_ticks", current.short_window_ticks)),
        long_window_ticks=max(10, _pick_int("long_window_ticks", current.long_window_ticks)),
        decision_cooldown_ticks=max(0, _pick_int("decision_cooldown_ticks", current.decision_cooldown_ticks)),
        max_param_updates_per_tick=max(1, _pick_int("max_param_updates_per_tick", current.max_param_updates_per_tick)),
        persist_overrides=_pick_bool("persist_overrides", current.persist_overrides),
        llm_assist_enabled=_pick_bool("llm_assist_enabled", current.llm_assist_enabled),
        llm_assist_trigger_windows=max(2, _pick_int("llm_assist_trigger_windows", current.llm_assist_trigger_windows)),
        llm_auto_validation_enabled=_pick_bool("llm_auto_validation_enabled", current.llm_auto_validation_enabled),
        llm_auto_validation_min_runs=max(1, _pick_int("llm_auto_validation_min_runs", current.llm_auto_validation_min_runs)),
        llm_auto_validation_max_observations_per_review=max(
            1,
            _pick_int(
                "llm_auto_validation_max_observations_per_review",
                current.llm_auto_validation_max_observations_per_review,
            ),
        ),
        llm_auto_validation_review_every_run=_pick_bool(
            "llm_auto_validation_review_every_run",
            current.llm_auto_validation_review_every_run,
        ),
        param_backoff_enabled=_pick_bool("param_backoff_enabled", current.param_backoff_enabled),
        param_backoff_base_cooldown_ticks=max(
            2,
            _pick_int("param_backoff_base_cooldown_ticks", current.param_backoff_base_cooldown_ticks),
        ),
        param_backoff_max_cooldown_ticks=max(
            10,
            _pick_int("param_backoff_max_cooldown_ticks", current.param_backoff_max_cooldown_ticks),
        ),
        param_backoff_failure_penalty=float(updates.get("param_backoff_failure_penalty", current.param_backoff_failure_penalty)),
        param_backoff_neutral_penalty=float(updates.get("param_backoff_neutral_penalty", current.param_backoff_neutral_penalty)),
    )

    metric_targets: list[dict[str, Any]] = []
    items = updates.get("metric_targets")
    if isinstance(items, list):
        for item in items:
            norm = _normalize_metric_target_item(item)
            if norm:
                metric_targets.append(norm)
    else:
        for item in raw_cfg.get("metric_targets", []):
            norm = _normalize_metric_target_item(item) if isinstance(item, dict) else None
            if norm:
                metric_targets.append(norm)

    param_bounds: dict[str, Any] = {}
    bound_updates = updates.get("param_bounds")
    if isinstance(bound_updates, dict):
        for pid, value in bound_updates.items():
            if not isinstance(pid, str):
                continue
            norm = _normalize_param_bound_item(value) if isinstance(value, dict) else None
            if norm:
                param_bounds[pid] = norm
    else:
        current_bounds = raw_cfg.get("param_bounds")
        if isinstance(current_bounds, dict):
            for pid, value in current_bounds.items():
                if not isinstance(pid, str):
                    continue
                norm = _normalize_param_bound_item(value) if isinstance(value, dict) else None
                if norm:
                    param_bounds[pid] = norm

    payload = {**raw_cfg, **merged_cfg.to_public_dict(), "updated_at_ms": _now_ms()}
    if metric_targets:
        payload["metric_targets"] = metric_targets
    else:
        payload.pop("metric_targets", None)
    if param_bounds:
        payload["param_bounds"] = param_bounds
    else:
        payload.pop("param_bounds", None)
    _write_json(_config_path(), payload)
    return load_auto_tuner_public_config()


def _default_rules_payload() -> dict[str, Any]:
    return {
        "schema_version": "v1",
        "updated_at_ms": _now_ms(),
        "disabled_rule_ids": [],
        "protected_rule_ids": [],
        "custom_rules": [],
    }


def _normalize_custom_rule(item: dict[str, Any]) -> dict[str, Any] | None:
    if not isinstance(item, dict):
        return None
    rule_id = str(item.get("id", "") or "").strip()
    metric_key = str(item.get("metric_key", "") or "").strip()
    issue_mode = str(item.get("issue_mode", "") or "").strip().lower()
    param_id = str(item.get("param_id", "") or "").strip()
    if not rule_id or not metric_key or issue_mode not in {"high", "low", "flatline"} or not param_id:
        return None
    direction = int(item.get("direction", 0) or 0)
    if direction not in {-1, 1}:
        return None
    return {
        "id": rule_id,
        "title": str(item.get("title", "") or rule_id),
        "description": str(item.get("description", "") or ""),
        "enabled": bool(item.get("enabled", True)),
        "metric_key": metric_key,
        "issue_mode": issue_mode,
        "param_id": param_id,
        "direction": direction,
        "step_scale": float(item.get("step_scale", 0.6)),
        "min_severity": float(item.get("min_severity", 0.05)),
        "cooldown_ticks": int(item.get("cooldown_ticks", 0) or 0),
        "protect_from_llm": bool(item.get("protect_from_llm", False)),
        "origin": str(item.get("origin", "") or "").strip(),
        "status": str(item.get("status", "") or "").strip(),
        "source_suggestion_path": str(item.get("source_suggestion_path", "") or "").strip(),
        "applied_at_ms": int(item.get("applied_at_ms", 0) or 0),
        "evaluation_runs": int(item.get("evaluation_runs", 0) or 0),
        "llm_confidence": float(item.get("llm_confidence", 0.0) or 0.0),
        "notes": [str(x) for x in (item.get("notes") or []) if str(x).strip()],
    }


def load_auto_tuner_rules() -> dict[str, Any]:
    raw = _load_json_dict(_rules_path())
    if not raw:
        raw = _default_rules_payload()

    disabled = raw.get("disabled_rule_ids")
    protected = raw.get("protected_rule_ids")
    custom = raw.get("custom_rules")

    disabled_ids = sorted({str(x).strip() for x in (disabled or []) if str(x).strip()})
    protected_ids = sorted({str(x).strip() for x in (protected or []) if str(x).strip()})
    custom_rules: list[dict[str, Any]] = []
    if isinstance(custom, list):
        for item in custom:
            norm = _normalize_custom_rule(item) if isinstance(item, dict) else None
            if norm:
                custom_rules.append(norm)
                if bool(norm.get("protect_from_llm", False)):
                    protected_ids.append(str(norm["id"]))

    return {
        "schema_version": "v1",
        "updated_at_ms": int(raw.get("updated_at_ms", 0) or 0),
        "disabled_rule_ids": sorted(set(disabled_ids)),
        "protected_rule_ids": sorted(set(protected_ids)),
        "custom_rules": custom_rules,
    }


def save_auto_tuner_rules(updates: dict[str, Any]) -> dict[str, Any]:
    current = load_auto_tuner_rules()
    updates = updates if isinstance(updates, dict) else {}

    disabled_ids = current["disabled_rule_ids"]
    if "disabled_rule_ids" in updates:
        disabled_ids = sorted({str(x).strip() for x in (updates.get("disabled_rule_ids") or []) if str(x).strip()})

    protected_ids = current["protected_rule_ids"]
    if "protected_rule_ids" in updates:
        protected_ids = sorted({str(x).strip() for x in (updates.get("protected_rule_ids") or []) if str(x).strip()})

    custom_rules = current["custom_rules"]
    if "custom_rules" in updates:
        custom_rules = []
        raw_items = updates.get("custom_rules")
        if isinstance(raw_items, list):
            for item in raw_items:
                norm = _normalize_custom_rule(item) if isinstance(item, dict) else None
                if norm:
                    custom_rules.append(norm)
                    if bool(norm.get("protect_from_llm", False)):
                        protected_ids.append(str(norm["id"]))

    payload = {
        "schema_version": "v1",
        "updated_at_ms": _now_ms(),
        "disabled_rule_ids": sorted(set(disabled_ids)),
        "protected_rule_ids": sorted(set(protected_ids)),
        "custom_rules": custom_rules,
    }
    _write_json(_rules_path(), payload)
    return load_auto_tuner_rules()


def _default_metric_targets() -> list[MetricTarget]:
    """
    Defaults are intentionally conservative; users can refine expected ranges per environment.

    Note:
    - These are *control* targets, not "paper truth". The idea is to keep the system
      within a stable envelope to avoid runaway (resource spikes / always-on feelings).
    """

    return [
        # Performance / resource
        MetricTarget(key="timing_total_logic_ms", expected_min=0.0, expected_max=8000.0, ideal=4500.0, min_std=200.0, weight=1.0),
        MetricTarget(key="internal_resolution_raw_unit_count", expected_min=0.0, expected_max=350.0, ideal=160.0, min_std=10.0, weight=1.0),
        MetricTarget(key="internal_sa_count", expected_min=64.0, expected_max=260.0, ideal=140.0, min_std=8.0, weight=1.0),
        MetricTarget(key="internal_to_external_sa_ratio", expected_min=1.25, expected_max=6.0, ideal=2.2, min_std=0.08, weight=1.0),
        MetricTarget(
            key="internal_resolution_structure_count_selected",
            expected_min=3.0,
            expected_max=12.0,
            ideal=5.0,
            min_std=0.4,
            weight=0.9,
        ),
        MetricTarget(key="merged_flat_token_count", expected_min=0.0, expected_max=240.0, ideal=140.0, min_std=0.0, weight=0.9),
        MetricTarget(key="sensor_echo_pool_size", expected_min=0.0, expected_max=24.0, ideal=6.0, min_std=0.0, weight=0.35),
        MetricTarget(key="pool_active_item_count", expected_min=40.0, expected_max=260.0, ideal=150.0, min_std=0.0, weight=0.75),
        MetricTarget(key="cam_item_count", expected_min=3.0, expected_max=18.0, ideal=8.0, min_std=0.0, weight=0.7),
        MetricTarget(key="pool_total_er", expected_min=60.0, expected_max=260.0, ideal=130.0, min_std=3.0, weight=0.7),
        MetricTarget(key="pool_total_ev", expected_min=60.0, expected_max=320.0, ideal=150.0, min_std=3.0, weight=0.95),
        MetricTarget(key="pool_total_cp", expected_min=40.0, expected_max=260.0, ideal=120.0, min_std=3.0, weight=0.7),
        # CFS dynamics
        # Philosophy note:
        # - strong-feeling metrics are not forced to a single point value like 0.30
        # - instead we keep a low-to-mid normal band and reserve >0.5 for notable events
        MetricTarget(
            key="cfs_dissonance_max",
            expected_min=0.0,
            expected_max=0.45,
            ideal=0.18,
            min_std=0.03,
            weight=1.0,
            high_band_threshold=0.50,
            high_band_max_ratio=0.18,
            high_band_soft_p95=0.68,
            high_band_max_run=3,
        ),
        MetricTarget(
            key="cfs_pressure_max",
            expected_min=0.0,
            expected_max=0.55,
            ideal=0.22,
            min_std=0.03,
            weight=0.7,
            high_band_threshold=0.50,
            high_band_max_ratio=0.22,
            high_band_soft_p95=0.72,
            high_band_max_run=4,
        ),
        MetricTarget(
            key="cfs_expectation_max",
            expected_min=0.0,
            expected_max=0.55,
            ideal=0.22,
            min_std=0.03,
            weight=0.6,
            high_band_threshold=0.50,
            high_band_max_ratio=0.22,
            high_band_soft_p95=0.72,
            high_band_max_run=4,
        ),
        MetricTarget(
            key="rwd_pun_rwd",
            expected_min=0.0,
            expected_max=0.75,
            ideal=0.30,
            min_std=0.03,
            weight=0.6,
            high_band_threshold=0.50,
            high_band_max_ratio=0.20,
            high_band_soft_p95=0.72,
            high_band_max_run=3,
        ),
        MetricTarget(
            key="rwd_pun_pun",
            expected_min=0.0,
            expected_max=0.60,
            ideal=0.24,
            min_std=0.03,
            weight=0.7,
            high_band_threshold=0.50,
            high_band_max_ratio=0.18,
            high_band_soft_p95=0.70,
            high_band_max_run=3,
        ),
        MetricTarget(
            key="nt_DA",
            expected_min=0.0,
            expected_max=0.80,
            ideal=0.38,
            min_std=0.03,
            weight=0.45,
            high_band_threshold=0.50,
            high_band_max_ratio=0.30,
            high_band_soft_p95=0.80,
            high_band_max_run=5,
        ),
        MetricTarget(
            key="nt_ADR",
            expected_min=0.0,
            expected_max=0.75,
            ideal=0.32,
            min_std=0.03,
            weight=0.45,
            high_band_threshold=0.50,
            high_band_max_ratio=0.16,
            high_band_soft_p95=0.68,
            high_band_max_run=3,
        ),
        MetricTarget(
            key="nt_COR",
            expected_min=0.0,
            expected_max=0.75,
            ideal=0.32,
            min_std=0.03,
            weight=0.45,
            high_band_threshold=0.50,
            high_band_max_ratio=0.16,
            high_band_soft_p95=0.68,
            high_band_max_run=3,
        ),
        MetricTarget(
            key="nt_SER",
            expected_min=0.0,
            expected_max=0.75,
            ideal=0.38,
            min_std=0.03,
            weight=0.4,
            high_band_threshold=0.50,
            high_band_max_ratio=0.35,
            high_band_soft_p95=0.82,
            high_band_max_run=6,
        ),
        MetricTarget(
            key="nt_OXY",
            expected_min=0.0,
            expected_max=0.75,
            ideal=0.34,
            min_std=0.03,
            weight=0.4,
            high_band_threshold=0.50,
            high_band_max_ratio=0.28,
            high_band_soft_p95=0.80,
            high_band_max_run=5,
        ),
        MetricTarget(
            key="nt_END",
            expected_min=0.0,
            expected_max=0.70,
            ideal=0.26,
            min_std=0.03,
            weight=0.4,
            high_band_threshold=0.50,
            high_band_max_ratio=0.22,
            high_band_soft_p95=0.76,
            high_band_max_run=4,
        ),
        MetricTarget(key="time_sensor_bucket_energy_sum", expected_min=2.0, expected_max=40.0, ideal=16.0, min_std=1.5, weight=0.45),
        MetricTarget(key="time_sensor_attribute_binding_count", expected_min=0.0, expected_max=12.0, ideal=4.0, min_std=1.0, weight=0.4),
        # Action rates (we derive window rates from these per-tick counts)
        MetricTarget(key="action_executed_recall", expected_min=0.0, expected_max=0.25, ideal=0.06, min_std=0.015, weight=0.8),
        MetricTarget(key="action_executed_attention_focus", expected_min=0.05, expected_max=0.65, ideal=0.28, min_std=0.03, weight=0.45),
        MetricTarget(key="action_drive_max", expected_min=0.10, expected_max=1.40, ideal=0.72, min_std=0.05, weight=0.55),
        MetricTarget(key="action_drive_active_count", expected_min=1.0, expected_max=16.0, ideal=6.0, min_std=1.0, weight=0.45),
        # Cognitive stitching / string-mode structure output
        MetricTarget(key="cs_candidate_count", expected_min=0.8, expected_max=10.0, ideal=3.5, min_std=0.2, weight=0.65),
        MetricTarget(key="cs_action_count", expected_min=0.12, expected_max=4.0, ideal=0.8, min_std=0.08, weight=0.75),
        MetricTarget(key="stimulus_new_structure_count", expected_min=0.12, expected_max=6.0, ideal=1.0, min_std=0.05, weight=0.6),
        MetricTarget(key="timing_cognitive_stitching_ms", expected_min=0.0, expected_max=1200.0, ideal=180.0, min_std=25.0, weight=0.4),
    ]


def _default_param_bounds() -> dict[str, ParamBound]:
    """
    A small set of hand-tuned param bounds used as "seed overrides" on top of the
    catalog-guessed bounds.

    Most bounds are guessed automatically from the config catalog. We only keep a
    few conservative overrides here for critical safety fuses.
    """

    return {
        # HDB resource fuses (hard protection)
        "hdb.internal_resolution_flat_unit_cap_per_structure": ParamBound(128.0, 800.0, 20.0, quantum=1.0),
        "hdb.internal_resolution_max_structures_per_tick": ParamBound(3.0, 12.0, 1.0, quantum=1.0),
        "hdb.internal_resolution_detail_budget_base": ParamBound(96.0, 512.0, 12.0, quantum=1.0),
        "hdb.stimulus_level_max_rounds": ParamBound(4.0, 20.0, 1.0, quantum=1.0),
        "hdb.structure_level_max_rounds": ParamBound(3.0, 10.0, 1.0, quantum=1.0),
        # StatePool decay ratios must stay <= 1.0 (ratio=1 means no decay).
        "state_pool.default_er_decay_ratio": ParamBound(0.93, 0.99, 0.005, quantum=0.001),
        "state_pool.default_ev_decay_ratio": ParamBound(0.94, 0.995, 0.005, quantum=0.001),
        # StatePool soft-cap philosophy fuses (keep within a sane envelope).
        # - start/full define when the pool starts self-tightening
        # - decay_power_max controls how strong the self-tightening can get
        "state_pool.soft_capacity_start_items": ParamBound(80.0, 1200.0, 10.0, quantum=1.0),
        "state_pool.soft_capacity_full_items": ParamBound(160.0, 2400.0, 10.0, quantum=1.0),
        "state_pool.soft_capacity_decay_power_max": ParamBound(4.0, 60.0, 2.0, quantum=0.1),
        # Hard capacity (avoid infinite growth in short-term tuning).
        "state_pool.pool_max_items": ParamBound(400.0, 12000.0, 250.0, quantum=1.0),
        # Snapshot payload fuse (avoid thrash in UI/metrics).
        "state_pool.snapshot_bound_attribute_energy_top_n": ParamBound(16.0, 128.0, 8.0, quantum=1.0),
        # Attention capacity fuses (avoid explosion; keep within UX envelope).
        "attention.max_cam_items": ParamBound(4.0, 32.0, 2.0, quantum=1.0),
        "attention.min_cam_items": ParamBound(0.0, 8.0, 1.0, quantum=1.0),
        # Cognitive stitching: keep threshold tuning bounded and conservative.
        "cognitive_stitching.min_candidate_score": ParamBound(0.05, 0.60, 0.03, quantum=0.005),
        "cognitive_stitching.min_seed_total_energy": ParamBound(0.10, 0.90, 0.04, quantum=0.005),
        "cognitive_stitching.min_event_total_energy": ParamBound(0.03, 0.40, 0.02, quantum=0.005),
        "cognitive_stitching.event_grasp_min_total_energy": ParamBound(0.05, 0.60, 0.03, quantum=0.005),
        "cognitive_stitching.edge_ratio_weight": ParamBound(0.05, 1.20, 0.06, quantum=0.01),
        "cognitive_stitching.match_strength_weight": ParamBound(0.05, 1.20, 0.06, quantum=0.01),
        "cognitive_stitching.context_support_weight": ParamBound(0.00, 0.90, 0.04, quantum=0.01),
        "cognitive_stitching.anchor_distance_penalty": ParamBound(0.00, 0.80, 0.04, quantum=0.01),
        # Emotion / EV shaping: keep within [0, 1]-style normalized envelope.
        "emotion.subjective_modulators.ev_propagation_ratio.base": ParamBound(0.20, 1.00, 0.05, quantum=0.01),
        "emotion.subjective_modulators.er_induction_ratio.base": ParamBound(0.20, 1.00, 0.05, quantum=0.01),
        "state_pool.default_ev_decay_ratio": ParamBound(0.94, 0.995, 0.005, quantum=0.001),
    }


ENDOGENOUS_RECOVERY_PARAM_IDS: set[str] = {
    "hdb.internal_resolution_max_structures_per_tick",
    "hdb.internal_resolution_flat_unit_cap_per_structure",
    "hdb.internal_resolution_detail_budget_base",
    "hdb.stimulus_level_max_rounds",
    "hdb.structure_level_max_rounds",
}


def _clamp(v: float, lo: float, hi: float) -> float:
    return max(float(lo), min(float(hi), float(v)))


def _round_quantum(v: float, quantum: float) -> float:
    q = float(quantum or 0.0)
    if q <= 0.0:
        return float(v)
    return round(float(v) / q) * q


def _safe_float(x: Any, default: float = 0.0) -> float:
    try:
        return float(x)
    except Exception:
        return float(default)


def _safe_float_or_none(x: Any) -> float | None:
    try:
        return float(x)
    except Exception:
        return None


def _safe_int_or_none(x: Any) -> int | None:
    try:
        if x is None:
            return None
        return int(x)
    except Exception:
        return None


def _mean(values: list[float]) -> float:
    if not values:
        return 0.0
    return float(sum(values)) / float(len(values))


def _std(values: list[float]) -> float:
    if len(values) < 2:
        return 0.0
    m = _mean(values)
    var = _mean([(v - m) * (v - m) for v in values])
    return float(math.sqrt(max(0.0, var)))


def _percentile(values: list[float], q: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(float(v) for v in values)
    qq = max(0.0, min(1.0, float(q)))
    if len(ordered) == 1:
        return float(ordered[0])
    pos = qq * float(len(ordered) - 1)
    lo = int(math.floor(pos))
    hi = int(math.ceil(pos))
    if lo == hi:
        return float(ordered[lo])
    frac = pos - float(lo)
    return float(ordered[lo] * (1.0 - frac) + ordered[hi] * frac)


def _max_consecutive(values: list[float], *, predicate) -> int:
    best = 0
    cur = 0
    for value in values:
        if bool(predicate(value)):
            cur += 1
            if cur > best:
                best = cur
        else:
            cur = 0
    return int(best)


def _recent_window(rows: list[dict[str, Any]], key: str, n: int) -> list[float]:
    out: list[float] = []
    for r in rows[-max(1, int(n)) :]:
        if not isinstance(r, dict):
            continue
        if key not in r:
            continue
        out.append(_safe_float(r.get(key, 0.0)))
    return out


def _band_distribution_summary(*, values: list[float], target: MetricTarget | None) -> dict[str, Any]:
    if not values or target is None:
        return {}
    threshold = _safe_float_or_none(getattr(target, "high_band_threshold", None))
    max_ratio = _safe_float_or_none(getattr(target, "high_band_max_ratio", None))
    soft_p95 = _safe_float_or_none(getattr(target, "high_band_soft_p95", None))
    max_run = getattr(target, "high_band_max_run", None)
    if threshold is None and max_ratio is None and soft_p95 is None and max_run is None:
        return {}
    thr = 0.5 if threshold is None else float(threshold)
    occupancy_ratio = _mean([1.0 if float(v) > thr else 0.0 for v in values])
    p95 = _percentile(values, 0.95)
    max_high_run = _max_consecutive(values, predicate=lambda v: float(v) > thr)
    occupancy_over = 0.0
    if max_ratio is not None and float(max_ratio) > 0.0:
        occupancy_over = max(0.0, (occupancy_ratio - float(max_ratio)) / max(1e-6, float(max_ratio)))
    p95_over = 0.0
    if soft_p95 is not None and float(soft_p95) > 0.0:
        p95_over = max(0.0, (float(p95) - float(soft_p95)) / max(1e-6, float(soft_p95)))
    run_over = 0.0
    if max_run is not None and int(max_run) > 0:
        run_over = max(0.0, (float(max_high_run) - float(int(max_run))) / float(max(1, int(max_run))))
    return {
        "threshold": thr,
        "occupancy_ratio": round(float(occupancy_ratio), 8),
        "max_ratio": float(max_ratio) if max_ratio is not None else None,
        "p95": round(float(p95), 8),
        "soft_p95": float(soft_p95) if soft_p95 is not None else None,
        "max_high_run": int(max_high_run),
        "max_run": int(max_run) if max_run is not None else None,
        "occupancy_over_ratio": round(float(occupancy_over), 8),
        "p95_over_ratio": round(float(p95_over), 8),
        "run_over_ratio": round(float(run_over), 8),
    }


# ------------------------------
# YAML patch helpers
# ------------------------------


def _write_yaml_patch(path: Path, patch: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    # Use PyYAML through experiment.io (explicit dep).
    path.write_text(io.dump_yaml(patch).strip() + "\n", encoding="utf-8")


def _load_yaml_mapping(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        raw = io.load_yaml_file(path)
    except Exception:
        return {}
    return raw if isinstance(raw, dict) else {}


def _load_rules_doc(path: Path) -> dict[str, Any]:
    try:
        raw = io.load_yaml_file(path)
    except Exception:
        return {}
    return raw if isinstance(raw, dict) else {}


def _dump_rules_doc(doc: dict[str, Any]) -> str:
    return io.dump_yaml(doc).strip() + "\n"


def _get_rule(doc: dict[str, Any], rule_id: str) -> Optional[dict[str, Any]]:
    rules = doc.get("rules")
    if not isinstance(rules, list):
        return None
    rid = str(rule_id or "").strip()
    for item in rules:
        if not isinstance(item, dict):
            continue
        if str(item.get("id", "")).strip() == rid:
            return item
    return None


def _find_then_step(rule: dict[str, Any], step_key: str) -> Optional[dict[str, Any]]:
    steps = rule.get("then")
    if not isinstance(steps, list):
        return None
    for step in steps:
        if not isinstance(step, dict):
            continue
        if step_key in step and isinstance(step.get(step_key), dict):
            return step.get(step_key)
    return None


# ------------------------------
# Auto tuner core
# ------------------------------


class AutoTuner:
    """
    Apply small parameter nudges based on recent metrics.

    MVP scope:
    - tune IESM rule thresholds/cooldowns to make reward/punish derived signals sparse but non-zero
    - tune time-feeling recall trigger to be drive-based and not "energy enough => recall"
    - tune HDB internal resolution fuses when raw/timing spikes
    """

    def __init__(
        self,
        *,
        app: Any,
        run_dir: Path,
        enabled: bool,
        enable_short_term: bool,
        enable_long_term: bool,
    ) -> None:
        self.app = app
        self.run_dir = Path(run_dir)
        self.cfg = load_auto_tuner_config()
        raw_cfg = _load_raw_config_dict()
        # RunOptions override config file (so UI toggles work without editing disk config).
        self.enabled = bool(enabled)
        self.enable_short_term = bool(enable_short_term)
        self.enable_long_term = bool(enable_long_term)

        # Metric targets and param bounds can be overridden via outputs/auto_tuner/config.json
        self.metric_targets = {t.key: t for t in _default_metric_targets()}
        try:
            items = raw_cfg.get("metric_targets")
            if isinstance(items, list) and items:
                parsed: dict[str, MetricTarget] = {}
                for it in items:
                    if not isinstance(it, dict):
                        continue
                    norm = _normalize_metric_target_item(it)
                    if not norm:
                        continue
                    key = str(norm.get("key", "") or "").strip()
                    if not key:
                        continue
                    base = self.metric_targets.get(key)
                    parsed[key] = MetricTarget(
                        key=key,
                        expected_min=float(norm.get("expected_min", 0.0)),
                        expected_max=float(norm.get("expected_max", 0.0)),
                        ideal=float(norm.get("ideal", 0.0)),
                        min_std=float(norm.get("min_std", 0.0)),
                        weight=float(norm.get("weight", 1.0)),
                        high_band_threshold=(
                            _safe_float_or_none(norm.get("high_band_threshold"))
                            if "high_band_threshold" in norm
                            else getattr(base, "high_band_threshold", None)
                        ),
                        high_band_max_ratio=(
                            _safe_float_or_none(norm.get("high_band_max_ratio"))
                            if "high_band_max_ratio" in norm
                            else getattr(base, "high_band_max_ratio", None)
                        ),
                        high_band_soft_p95=(
                            _safe_float_or_none(norm.get("high_band_soft_p95"))
                            if "high_band_soft_p95" in norm
                            else getattr(base, "high_band_soft_p95", None)
                        ),
                        high_band_max_run=(
                            _safe_int_or_none(norm.get("high_band_max_run"))
                            if "high_band_max_run" in norm
                            else getattr(base, "high_band_max_run", None)
                        ),
                    )
                if parsed:
                    self.metric_targets.update(parsed)
        except Exception:
            pass

        # ------------------------------
        # Parameter catalog + bounds
        # ------------------------------
        #
        # The catalog enumerates (almost) all numeric knobs from configs + IESM rules.
        # Bounds are guessed heuristically but can be overridden in config.json.
        try:
            self.catalog_specs = list(param_catalog.build_param_catalog(app=self.app))
        except Exception:
            self.catalog_specs = []
        self.spec_by_id: dict[str, param_catalog.ParamSpec] = {s.param_id: s for s in self.catalog_specs if isinstance(s, param_catalog.ParamSpec)}

        # 1) Guessed bounds from catalog
        try:
            guessed = param_catalog.build_default_param_bounds(self.catalog_specs)
        except Exception:
            guessed = {}
        self.param_bounds = {
            pid: ParamBound(min_value=b.min_value, max_value=b.max_value, max_step_abs=b.max_step_abs, quantum=b.quantum)
            for pid, b in guessed.items()
        }

        # 2) Seed overrides (hard safety fuses / conservative steps)
        self.param_bounds.update(_default_param_bounds())

        # 3) User overrides from outputs/auto_tuner/config.json
        try:
            bounds = raw_cfg.get("param_bounds")
            if isinstance(bounds, dict) and bounds:
                for pid, b in bounds.items():
                    if not isinstance(pid, str) or not isinstance(b, dict):
                        continue
                    self.param_bounds[pid] = ParamBound(
                        min_value=float(b.get("min_value", 0.0)),
                        max_value=float(b.get("max_value", 0.0)),
                        max_step_abs=float(b.get("max_step_abs", 0.0)),
                        quantum=float(b.get("quantum", 0.0)),
                    )
        except Exception:
            pass

        self.history: list[dict[str, Any]] = []
        self.last_decision_tick: int = -10_000

        self.state: dict[str, Any] = self._load_state()
        # Persisted params are for "next run" (long-term tuning).
        self.persisted_params: dict[str, float] = self.state.get("persisted_params", {}) if isinstance(self.state.get("persisted_params"), dict) else {}
        # Runtime params are for "this run" (short-term tuning), initialized from persisted ones.
        self.runtime_params: dict[str, float] = copy.deepcopy(self.persisted_params)
        # If we tightened the catalog safety envelope, sanitize can clamp invalid persisted values.
        # Persist the clamped values back to state.json immediately so we don't keep starting from
        # already-invalid numbers.
        if self._sanitize_param_stores():
            try:
                self._save_state()
            except Exception:
                pass
        self.rule_health: dict[str, Any] = self.state.get("rule_health", {}) if isinstance(self.state.get("rule_health"), dict) else {}
        self.active_trials: list[dict[str, Any]] = list(self.state.get("active_trials", [])) if isinstance(self.state.get("active_trials"), list) else []
        self.trial_history: list[dict[str, Any]] = list(self.state.get("trial_history", [])) if isinstance(self.state.get("trial_history"), list) else []
        self.last_applied_updates: list[dict[str, Any]] = list(self.state.get("last_applied_updates", [])) if isinstance(self.state.get("last_applied_updates"), list) else []
        self.rule_observations: list[dict[str, Any]] = list(self.state.get("rule_observations", [])) if isinstance(self.state.get("rule_observations"), list) else []
        self.observation_history: list[dict[str, Any]] = list(self.state.get("observation_history", [])) if isinstance(self.state.get("observation_history"), list) else []
        self.observation_review_history: list[dict[str, Any]] = list(self.state.get("observation_review_history", [])) if isinstance(self.state.get("observation_review_history"), list) else []
        self.last_observation_review: dict[str, Any] = (
            dict(self.state.get("last_observation_review", {}))
            if isinstance(self.state.get("last_observation_review"), dict)
            else {}
        )
        self.rules_cfg = load_auto_tuner_rules()
        self.disabled_rule_ids: set[str] = set(self.rules_cfg.get("disabled_rule_ids", []))
        self.protected_rule_ids: set[str] = set(self.rules_cfg.get("protected_rule_ids", []))
        self.custom_rules: list[dict[str, Any]] = list(self.rules_cfg.get("custom_rules", []))
        # Per-run cooldown memory (do NOT persist across runs; tick_index resets).
        self.last_param_tick: dict[str, int] = {}
        # Exponential backoff per param (avoid repeated no-op / ineffective nudges).
        # Keys are canonical param ids.
        self.param_backoff: dict[str, dict[str, Any]] = {}

        self.audit_path = self.run_dir / "auto_tuner.audit.jsonl"

        # Base configs (captured after applying persisted params at prepare time).
        self._base_module_configs: dict[str, dict[str, Any]] = {}

        # Persistent overrides (cross-run) live under outputs/auto_tuner/overrides/
        self.persist_rules_path = _overrides_dir() / "innate_rules.persisted.yaml"

        # Runtime overrides (per-run) live under the run_dir, to avoid short-term tuning
        # permanently affecting the next run.
        self.runtime_rules_path = self.run_dir / "auto_tuner.innate_rules.runtime.yaml"
        self.runtime_iesm_config_path = self.run_dir / "auto_tuner.innate_script_config.runtime.yaml"

        # Per-module runtime patch files (YAML patches) + persisted copies for audit.
        self.runtime_module_patch_paths: dict[str, Path] = {
            "observatory": self.run_dir / "auto_tuner.observatory_config.runtime.yaml",
            "action": self.run_dir / "auto_tuner.action_config.runtime.yaml",
            "attention": self.run_dir / "auto_tuner.attention_config.runtime.yaml",
            "cognitive_feeling": self.run_dir / "auto_tuner.cognitive_feeling_config.runtime.yaml",
            "emotion": self.run_dir / "auto_tuner.emotion_config.runtime.yaml",
            "energy_balance": self.run_dir / "auto_tuner.energy_balance_config.runtime.yaml",
            "hdb": self.run_dir / "auto_tuner.hdb_config.runtime.yaml",
            "innate_script": self.runtime_iesm_config_path,
            "state_pool": self.run_dir / "auto_tuner.state_pool_config.runtime.yaml",
            "text_sensor": self.run_dir / "auto_tuner.text_sensor_config.runtime.yaml",
            "time_sensor": self.run_dir / "auto_tuner.time_sensor_config.runtime.yaml",
        }
        self.persist_module_patch_paths: dict[str, Path] = {
            "observatory": _overrides_dir() / "observatory_config.persisted.yaml",
            "action": _overrides_dir() / "action_config.persisted.yaml",
            "attention": _overrides_dir() / "attention_config.persisted.yaml",
            "cognitive_feeling": _overrides_dir() / "cognitive_feeling_config.persisted.yaml",
            "emotion": _overrides_dir() / "emotion_config.persisted.yaml",
            "energy_balance": _overrides_dir() / "energy_balance_config.persisted.yaml",
            "hdb": _overrides_dir() / "hdb_config.persisted.yaml",
            "innate_script": _overrides_dir() / "innate_script_config.persisted.yaml",
            "state_pool": _overrides_dir() / "state_pool_config.persisted.yaml",
            "text_sensor": _overrides_dir() / "text_sensor_config.persisted.yaml",
            "time_sensor": _overrides_dir() / "time_sensor_config.persisted.yaml",
        }
        self._ensure_candidate_observations()

    # --------------------------
    # Persistence
    # --------------------------

    def _load_state(self) -> dict[str, Any]:
        p = _state_path()
        if not p.exists():
            return _default_state_payload()
        try:
            raw = json.loads(p.read_text(encoding="utf-8"))
        except Exception:
            return _default_state_payload()

        if not isinstance(raw, dict):
            return _default_state_payload()

        schema = str(raw.get("schema_version", "") or "").strip() or "v0"
        if schema in {"v1", "v2", "v3"}:
            persisted_params = raw.get("persisted_params", {})
            rule_health = raw.get("rule_health", {})
            return {
                "schema_version": "v3",
                "persisted_params": dict(persisted_params) if isinstance(persisted_params, dict) else {},
                "rule_health": dict(rule_health) if isinstance(rule_health, dict) else {},
                "active_trials": list(raw.get("active_trials", [])) if isinstance(raw.get("active_trials"), list) else [],
                "trial_history": list(raw.get("trial_history", [])) if isinstance(raw.get("trial_history"), list) else [],
                "last_applied_updates": list(raw.get("last_applied_updates", [])) if isinstance(raw.get("last_applied_updates"), list) else [],
                "rule_observations": list(raw.get("rule_observations", [])) if isinstance(raw.get("rule_observations"), list) else [],
                "observation_history": list(raw.get("observation_history", [])) if isinstance(raw.get("observation_history"), list) else [],
                "observation_review_history": list(raw.get("observation_review_history", []))
                if isinstance(raw.get("observation_review_history"), list)
                else [],
                "last_observation_review": dict(raw.get("last_observation_review", {}))
                if isinstance(raw.get("last_observation_review"), dict)
                else {},
            }

        # v0 migration: overrides -> persisted_params
        overrides = raw.get("overrides", {})
        overrides = overrides if isinstance(overrides, dict) else {}
        persisted_params: dict[str, Any] = {}

        # HDB patch used to be stored as a plain dict of key->value under overrides["hdb_config"].
        hdb_patch = overrides.get("hdb_config", {})
        if isinstance(hdb_patch, dict):
            for k, v in hdb_patch.items():
                if not isinstance(k, str):
                    continue
                persisted_params[f"hdb.{k}"] = v

        # IESM rules used to be stored as param_id->value under overrides["iesm_rules"].
        # We keep those keys as-is; a later catalog-based migration will map legacy aliases
        # (iesm.xxx) into canonical rule paths (iesm.rules.<rule_id>...).
        iesm_patch = overrides.get("iesm_rules", {})
        if isinstance(iesm_patch, dict):
            for k, v in iesm_patch.items():
                if not isinstance(k, str):
                    continue
                persisted_params[str(k)] = v

        rule_health = raw.get("rule_health", {})
        payload = _default_state_payload()
        payload.update(
            {
                "persisted_params": dict(persisted_params),
                "rule_health": dict(rule_health) if isinstance(rule_health, dict) else {},
            }
        )
        return payload

    def _save_state(self) -> None:
        p = _state_path()
        p.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "schema_version": "v3",
            "updated_at_ms": _now_ms(),
            "persisted_params": self.persisted_params,
            "rule_health": self.rule_health,
            "active_trials": self.active_trials[-120:],
            "trial_history": self.trial_history[-400:],
            "last_applied_updates": self.last_applied_updates[-120:],
            "rule_observations": self.rule_observations[-160:],
            "observation_history": self.observation_history[-400:],
            "observation_review_history": self.observation_review_history[-200:],
            "last_observation_review": self.last_observation_review,
        }
        p.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    def _audit(self, event: dict[str, Any]) -> None:
        if not isinstance(event, dict):
            return
        payload = dict(event)
        payload.setdefault("ts_ms", _now_ms())
        try:
            self.audit_path.parent.mkdir(parents=True, exist_ok=True)
            with self.audit_path.open("a", encoding="utf-8") as f:
                f.write(json.dumps(payload, ensure_ascii=False))
                f.write("\n")
        except Exception:
            pass
        try:
            _global_audit_path().parent.mkdir(parents=True, exist_ok=True)
            with _global_audit_path().open("a", encoding="utf-8") as f:
                f.write(json.dumps(payload, ensure_ascii=False))
                f.write("\n")
        except Exception:
            pass

    # --------------------------
    # Override application
    # --------------------------

    def prepare_and_apply_overrides(self, *, trace_id: str) -> dict[str, Any]:
        """
        Best-effort: prepare per-run runtime override files and apply persisted baseline overrides.

        Returns an audit dict (safe for manifest).
        """
        if not self.enabled:
            return {"enabled": False}

        _overrides_dir().mkdir(parents=True, exist_ok=True)

        # 0) Best-effort: migrate legacy IESM param aliases into canonical iesm.rules.<rule_id> paths.
        mig = self._migrate_legacy_param_ids_inplace()
        sanitized_param_stores = self._sanitize_param_stores()

        # 1) Ensure persisted rules file exists and stays in-sync with the repo baseline.
        #
        # Why: the project evolves quickly (new IESM rules are added over time). If we treat
        # outputs/auto_tuner/overrides/innate_rules.persisted.yaml as a frozen full copy, then
        # newer baseline rules (e.g. action stubs / CFS bind-energy templates) can silently vanish
        # from long-run runs, causing systematic mis-evaluation and "all zeros" live totals.
        try:
            baseline_rules_path = Path(str(getattr(getattr(self.app, "iesm", None), "_rules_path", "") or "")).resolve()
        except Exception:
            baseline_rules_path = Path(storage.repo_root() / "innate_script" / "config" / "innate_rules.yaml").resolve()

        baseline_text = ""
        baseline_doc: dict[str, Any] | None = None
        try:
            baseline_text = baseline_rules_path.read_text(encoding="utf-8")
            baseline_doc = _load_rules_doc(baseline_rules_path)
        except Exception:
            baseline_text = ""
            baseline_doc = None

        def _rule_ids(doc: dict[str, Any] | None) -> set[str]:
            if not isinstance(doc, dict):
                return set()
            rules = doc.get("rules")
            if not isinstance(rules, list):
                return set()
            out: set[str] = set()
            for r in rules:
                if not isinstance(r, dict):
                    continue
                rid = str(r.get("id", "") or "").strip()
                if rid:
                    out.add(rid)
            return out

        def _should_refresh_persisted_rules(*, baseline: dict[str, Any] | None, persisted: dict[str, Any] | None) -> bool:
            # If baseline is unreadable, keep current persisted file (best-effort).
            if not isinstance(baseline, dict):
                return False
            if not isinstance(persisted, dict):
                return True
            base_ids = _rule_ids(baseline)
            pers_ids = _rule_ids(persisted)
            if not base_ids:
                return False
            # Missing any baseline rule ids => stale persisted copy.
            if not pers_ids or not base_ids.issubset(pers_ids):
                return True
            # If baseline version increased, refresh (but only when baseline parsed cleanly).
            base_ver = str(baseline.get("rules_version", "") or "").strip()
            pers_ver = str(persisted.get("rules_version", "") or "").strip()
            if base_ver and pers_ver and base_ver != pers_ver:
                return True
            return False

        persisted_doc: dict[str, Any] | None = None
        if self.persist_rules_path.exists():
            try:
                persisted_doc = _load_rules_doc(self.persist_rules_path)
            except Exception:
                persisted_doc = None

        if (not self.persist_rules_path.exists()) or _should_refresh_persisted_rules(baseline=baseline_doc, persisted=persisted_doc):
            try:
                if baseline_text:
                    self.persist_rules_path.write_text(baseline_text, encoding="utf-8")
                else:
                    # If copy fails, at least create a minimal doc so reload does not crash.
                    self.persist_rules_path.write_text("rules_schema_version: '1.0'\nrules: []\n", encoding="utf-8")
            except Exception:
                pass

        # 2) Apply persisted IESM rule params into the persisted rules file, then copy to runtime rules file.
        try:
            doc = _load_rules_doc(self.persist_rules_path)
            if doc:
                iesm_params = {k: v for k, v in (self.persisted_params or {}).items() if isinstance(k, str) and k.startswith("iesm.rules.")}
                if iesm_params and self._apply_iesm_rule_param_values(doc, iesm_params):
                    self.persist_rules_path.write_text(_dump_rules_doc(doc), encoding="utf-8")
        except Exception:
            pass

        # 3) Create runtime rules file for *this run* (copy persisted baseline).
        try:
            self.runtime_rules_path.parent.mkdir(parents=True, exist_ok=True)
            self.runtime_rules_path.write_text(self.persist_rules_path.read_text(encoding="utf-8"), encoding="utf-8")
        except Exception:
            self.runtime_rules_path.write_text("rules_schema_version: '1.0'\nrules: []\n", encoding="utf-8")

        applied: dict[str, Any] = {
            "persist_rules_path": str(self.persist_rules_path),
            "runtime_rules_path": str(self.runtime_rules_path),
            "migrate": mig,
            "sanitized_param_stores": bool(sanitized_param_stores),
        }

        # 4) Reload innate_script (IESM) config: always point rules_path to runtime file, and also apply persisted
        #    numeric knobs from innate_script_config.yaml if present.
        try:
            patch = {"rules_path": str(self.runtime_rules_path)}
            patch.update(self._materialize_module_patch(module="innate_script", params=self.persisted_params, base=self._get_runtime_module_config("innate_script")))
            _write_yaml_patch(self.runtime_iesm_config_path, patch)
            res = self.app.iesm.reload_config(trace_id=f"{trace_id}_auto_tuner", config_path=str(self.runtime_iesm_config_path))  # type: ignore[attr-defined]
            applied["innate_script_reload_config"] = res.get("code", "")
        except Exception as exc:
            applied["innate_script_reload_config_error"] = str(exc)

        # 5) Apply persisted module params (runtime baseline) via per-module runtime patch files.
        module_results: dict[str, Any] = {}
        for module in sorted(self.runtime_module_patch_paths.keys()):
            if module == "innate_script":
                # already handled above (also carries rules_path)
                continue
            try:
                patch = self._materialize_module_patch(module=module, params=self.persisted_params, base=self._get_runtime_module_config(module))
                if not patch:
                    continue
                path = self.runtime_module_patch_paths[module]
                _write_yaml_patch(path, patch)
                res = self._reload_module_config(module=module, trace_id=f"{trace_id}_auto_tuner_{module}", config_path=path)
                module_results[module] = {"patch_path": str(path), "code": (res or {}).get("code", "")}
            except Exception as exc:
                module_results[module] = {"error": str(exc)}
        if module_results:
            applied["module_reload"] = module_results

        # 6) Capture base configs after applying persisted baselines (used for safe deep-updates later).
        try:
            self._capture_base_module_configs()
        except Exception:
            pass

        if sanitized_param_stores:
            try:
                self._apply_persisted_overrides_to_persist_files()
            except Exception:
                pass
            self._save_state()

        # 7) Refresh catalog once more so rule params match the *effective* runtime rules path/config.
        try:
            self.catalog_specs = list(param_catalog.build_param_catalog(app=self.app))
            self.spec_by_id = {s.param_id: s for s in self.catalog_specs if isinstance(s, param_catalog.ParamSpec)}
        except Exception:
            pass

        # 8) Write catalog outputs (gitignored) for auditing / the "参数-影响对应表" requirement.
        try:
            guessed_bounds = {k: param_catalog.ParamBound(v.min_value, v.max_value, v.max_step_abs, quantum=v.quantum) for k, v in self.param_bounds.items()}
            param_catalog.write_catalog_outputs(specs=self.catalog_specs, bounds=guessed_bounds)
        except Exception:
            pass

        # If sanitize clamped values, persist immediately so the next run doesn't start from
        # already-invalid numbers that were only corrected in-memory.
        if sanitized_param_stores:
            try:
                self._save_state()
            except Exception:
                pass

        self._audit({"ts_ms": int(time.time() * 1000), "kind": "prepare", "applied": applied})
        return {"enabled": True, "applied": applied}

    # --------------------------
    # Catalog helpers
    # --------------------------

    def _migrate_legacy_param_ids_inplace(self) -> dict[str, Any]:
        """
        Best-effort migration of legacy param aliases into canonical catalog ids.

        Currently we only migrate a small set of known IESM aliases like:
        - iesm.cfs_dissonance.threshold  -> iesm.rules.cfs_dissonance_from_cp_abs.when.metric.value

        This keeps old state.json compatible after we switched to catalog-style ids.
        """
        moved: list[dict[str, str]] = []

        def _migrate_one(container: dict[str, float]) -> None:
            for legacy in list(container.keys()):
                if not isinstance(legacy, str):
                    continue
                if not legacy.startswith("iesm.") or legacy.startswith("iesm.rules."):
                    continue
                canonical = self._resolve_legacy_iesm_alias(legacy)
                if not canonical or canonical == legacy:
                    continue
                if canonical in container:
                    # Prefer canonical if explicitly present.
                    del container[legacy]
                    continue
                container[canonical] = container[legacy]
                del container[legacy]
                moved.append({"from": legacy, "to": canonical})

        _migrate_one(self.persisted_params)
        _migrate_one(self.runtime_params)

        return {"moved_count": len(moved), "moved": moved[:64]}

    def _sanitize_param_stores(self) -> bool:
        """
        Drop params that are no longer present / no longer auto-tunable in the current catalog,
        and clamp surviving values back into the current safety bounds.

        This prevents old state.json entries from surviving after we tighten the catalog policy.
        """
        allowed: set[str] = set(self.param_bounds.keys())
        if not allowed:
            changed = bool(self.persisted_params) or bool(self.runtime_params)
            self.persisted_params = {}
            self.runtime_params = {}
            return changed

        def _clean(container: dict[str, float]) -> tuple[dict[str, float], bool]:
            out: dict[str, float] = {}
            changed_local = False
            for k, v in (container or {}).items():
                if not isinstance(k, str):
                    changed_local = True
                    continue
                pid = self._canonicalize_param_id(k)
                if pid not in allowed:
                    changed_local = True
                    continue
                try:
                    num = float(v)
                except Exception:
                    changed_local = True
                    continue
                bound = self.param_bounds.get(pid)
                if bound is None:
                    out[pid] = num
                    continue
                clamped = _round_quantum(_clamp(num, bound.min_value, bound.max_value), bound.quantum)
                if pid != k or abs(clamped - num) > 1e-12:
                    changed_local = True
                out[pid] = clamped
            return out, changed_local

        persisted, persisted_changed = _clean(self.persisted_params)
        runtime, runtime_changed = _clean(self.runtime_params)
        changed = persisted_changed or runtime_changed or persisted != self.persisted_params or runtime != self.runtime_params
        self.persisted_params = persisted
        self.runtime_params = runtime
        return changed

    def _resolve_legacy_iesm_alias(self, legacy_param_id: str) -> str | None:
        """
        Map a handful of legacy `iesm.*` friendly ids to canonical `iesm.rules.<rule_id>...` ids.

        We do this by searching the catalog for the corresponding rule + leaf key.
        """
        legacy = str(legacy_param_id or "").strip()
        if not legacy.startswith("iesm.") or legacy.startswith("iesm.rules."):
            return None

        # (legacy_id) -> (rule_id, leaf_name, required_substrings)
        mapping: dict[str, tuple[str, str, list[str]]] = {
            "iesm.cfs_dissonance.threshold": ("cfs_dissonance_from_cp_abs", "value", ["when", "metric"]),
            "iesm.cfs_dissonance.cooldown_ticks": ("cfs_dissonance_from_cp_abs", "cooldown_ticks", []),
            "iesm.cfs_dissonance.max_signals": ("cfs_dissonance_from_cp_abs", "max_signals", ["cfs_emit"]),
            "iesm.cfs_punish_signal.value": ("cfs_dissonance_from_cp_abs", "attribute_value", ["pool_bind_attribute"]),
            "iesm.cfs_correct_event.cooldown_ticks": ("cfs_correct_event_from_cp_abs_drop", "cooldown_ticks", []),
            "iesm.cfs_reward_signal.value": ("cfs_correct_event_from_cp_abs_drop", "attribute_value", ["pool_bind_attribute"]),
            "iesm.cfs_pressure.ev_threshold": ("cfs_pressure_from_punish_pred", "value", ["when", "metric"]),
            "iesm.cfs_expectation.ev_threshold": ("cfs_expectation_from_reward_pred", "value", ["when", "metric"]),
            "iesm.action_recall_time.bucket_energy_threshold": ("innate_action_recall_from_time_feeling_bucket_gain", "value", ["when", "metric"]),
            "iesm.action_recall_time.gain": ("innate_action_recall_from_time_feeling_bucket_gain", "gain", ["action_trigger"]),
            "iesm.action_recall_time.threshold": ("innate_action_recall_from_time_feeling_bucket_gain", "threshold", ["action_trigger"]),
        }
        if legacy not in mapping:
            return None
        rule_id, leaf_name, must_have = mapping[legacy]

        # Search the current catalog for the best matching leaf.
        candidates: list[str] = []
        for pid, spec in self.spec_by_id.items():
            if spec.source_kind != "iesm_rule":
                continue
            if not pid.startswith(f"iesm.rules.{rule_id}."):
                continue
            if not spec.path_tokens:
                continue
            if str(spec.path_tokens[-1]) != leaf_name:
                continue
            if must_have and not all(s in pid for s in must_have):
                continue
            candidates.append(pid)

        # Prefer the shortest path when multiple matches exist.
        if not candidates:
            return None
        candidates.sort(key=lambda s: (len(s), s))
        return candidates[0]

    # --------------------------
    # Patch materialization + hot reload
    # --------------------------

    def _get_runtime_module_config(self, module: str) -> dict[str, Any]:
        """Return the module's current effective config dict (best-effort)."""
        m = str(module or "").strip().lower()
        try:
            if m == "observatory":
                return dict(getattr(self.app, "_config", {}) or {})
            if m == "action":
                return dict(getattr(getattr(self.app, "action", None), "_config", {}) or {})
            if m == "attention":
                return dict(getattr(getattr(self.app, "attention", None), "_config", {}) or {})
            if m == "cognitive_feeling":
                return dict(getattr(getattr(self.app, "cfs", None), "_config", {}) or {})
            if m == "emotion":
                return dict(getattr(getattr(self.app, "emotion", None), "_config", {}) or {})
            if m == "energy_balance":
                return dict(getattr(getattr(self.app, "energy_balance", None), "_config", {}) or {})
            if m == "hdb":
                return dict(getattr(getattr(self.app, "hdb", None), "_config", {}) or {})
            if m == "innate_script":
                return dict(getattr(getattr(self.app, "iesm", None), "_config", {}) or {})
            if m == "state_pool":
                return dict(getattr(getattr(self.app, "pool", None), "_config", {}) or {})
            if m == "text_sensor":
                return dict(getattr(getattr(self.app, "sensor", None), "_config", {}) or {})
            if m == "time_sensor":
                return dict(getattr(getattr(self.app, "time_sensor", None), "_config", {}) or {})
        except Exception:
            return {}
        return {}

    def _reload_module_config(self, *, module: str, trace_id: str, config_path: Path) -> dict[str, Any]:
        """Hot reload a module from a patch file (best-effort)."""
        m = str(module or "").strip().lower()
        p = str(config_path)
        if m == "observatory":
            patch = _load_yaml_mapping(config_path)
            if isinstance(patch, dict) and patch:
                cfg = getattr(self.app, "_config", None)
                if isinstance(cfg, dict):
                    cfg.update(patch)
                return {"success": True, "code": "OK", "data": {"applied": list(patch.keys())}}
            return {"success": False, "code": "CONFIG_EMPTY", "data": {"path": p}}
        if m == "action":
            return self.app.action.reload_config(trace_id=trace_id, config_path=p)  # type: ignore[attr-defined]
        if m == "attention":
            return self.app.attention.reload_config(trace_id=trace_id, config_path=p)  # type: ignore[attr-defined]
        if m == "cognitive_feeling":
            return self.app.cfs.reload_config(trace_id=trace_id, config_path=p)  # type: ignore[attr-defined]
        if m == "emotion":
            return self.app.emotion.reload_config(trace_id=trace_id, config_path=p)  # type: ignore[attr-defined]
        if m == "energy_balance":
            return self.app.energy_balance.reload_config(trace_id=trace_id, config_path=p)  # type: ignore[attr-defined]
        if m == "hdb":
            return self.app.hdb.reload_config(trace_id=trace_id, config_path=p)  # type: ignore[attr-defined]
        if m == "innate_script":
            return self.app.iesm.reload_config(trace_id=trace_id, config_path=p)  # type: ignore[attr-defined]
        if m == "state_pool":
            return self.app.pool.reload_config(trace_id=trace_id, config_path=p)  # type: ignore[attr-defined]
        if m == "text_sensor":
            return self.app.sensor.reload_config(trace_id=trace_id, config_path=p)  # type: ignore[attr-defined]
        if m == "time_sensor":
            return self.app.time_sensor.reload_config(trace_id=trace_id, config_path=p)  # type: ignore[attr-defined]
        return {}

    def _capture_base_module_configs(self) -> None:
        """Capture base configs after persisted overrides are applied (so later deep patches are safe)."""
        base: dict[str, dict[str, Any]] = {}
        for m in {"observatory", *self.runtime_module_patch_paths.keys()}:
            try:
                base[m] = self._get_runtime_module_config(m)
            except Exception:
                base[m] = {}
        self._base_module_configs = base

    def _coerce_param_value(self, *, spec: param_catalog.ParamSpec, value: float) -> Any:
        """Coerce numeric value into int/float based on catalog value_type."""
        if spec.value_type == "int":
            return int(round(float(value)))
        # Keep floats as floats (even if integer-like) to avoid surprising YAML diffs.
        return float(value)

    def _deep_set(self, obj: Any, tokens: list[Any], value: Any) -> bool:
        """
        Set a nested value in-place.
        Tokens may contain strings (dict keys) and ints (list indices).
        Returns True if successful.
        """
        if not tokens:
            return False
        cur = obj
        for t in tokens[:-1]:
            if isinstance(t, int):
                if not isinstance(cur, list):
                    return False
                if t < 0 or t >= len(cur):
                    return False
                cur = cur[t]
                continue
            # dict key
            if not isinstance(cur, dict):
                return False
            key = str(t)
            if key not in cur:
                return False
            cur = cur[key]
        last = tokens[-1]
        if isinstance(last, int):
            if not isinstance(cur, list):
                return False
            if last < 0 or last >= len(cur):
                return False
            cur[last] = value
            return True
        if not isinstance(cur, dict):
            return False
        cur[str(last)] = value
        return True

    def _deep_get(self, obj: Any, tokens: list[Any]) -> Any:
        """Best-effort nested read by tokens; returns None when missing."""
        cur = obj
        for t in tokens:
            if isinstance(t, int):
                if not isinstance(cur, list):
                    return None
                if t < 0 or t >= len(cur):
                    return None
                cur = cur[t]
                continue
            if not isinstance(cur, dict):
                return None
            key = str(t)
            if key not in cur:
                return None
            cur = cur[key]
        return cur

    def _materialize_module_patch(self, *, module: str, params: dict[str, float], base: dict[str, Any]) -> dict[str, Any]:
        """
        Build a reloadable YAML patch for a module based on tuned param values.

        Important: module reload_config only merges at the *top-level keys*. If a param is nested,
        we materialize the full top-level object value by deep-updating a copy of the base config.
        """
        m = str(module or "").strip().lower()
        if not isinstance(params, dict) or not params:
            return {}
        if not isinstance(base, dict):
            base = {}

        # Collect leaf updates for this module
        leaf_updates: list[tuple[param_catalog.ParamSpec, float]] = []
        prefix = f"{m}."
        for pid, v in params.items():
            if not isinstance(pid, str) or not pid.startswith(prefix):
                continue
            if pid not in self.spec_by_id:
                continue
            spec = self.spec_by_id[pid]
            if not spec.auto_tune_allowed:
                continue
            if spec.source_kind not in {"module_config", "observatory_config"}:
                continue
            if str(spec.module).strip().lower() != m:
                continue
            try:
                leaf_updates.append((spec, float(v)))
            except Exception:
                continue
        if not leaf_updates:
            return {}

        patch: dict[str, Any] = {}
        for spec, v in leaf_updates:
            tokens = list(spec.path_tokens or [])
            if not tokens:
                continue
            top_key = tokens[0]
            if not isinstance(top_key, str):
                continue

            # Start from already-built patch value for this key, otherwise from base config value.
            if top_key in patch:
                top_obj = copy.deepcopy(patch.get(top_key))
            else:
                top_obj = copy.deepcopy(base.get(top_key))

            # Apply update
            if len(tokens) == 1:
                patch[top_key] = self._coerce_param_value(spec=spec, value=v)
                continue

            # Nested: deep-update the top-level object.
            if top_obj is None:
                # Best-effort initialize container when missing.
                top_obj = {} if isinstance(tokens[1], str) else []
            ok = self._deep_set(top_obj, tokens[1:], self._coerce_param_value(spec=spec, value=v))
            if not ok:
                continue
            patch[top_key] = top_obj

        return patch

    # --------------------------
    # IESM rules patching
    # --------------------------

    def _apply_iesm_rule_param_values(self, doc: dict[str, Any], params: dict[str, Any]) -> bool:
        """
        Apply canonical `iesm.rules.<rule_id>.<path>` scalar updates into a loaded rules doc.
        """
        if not isinstance(doc, dict):
            return False
        rules = doc.get("rules")
        if not isinstance(rules, list) or not rules:
            return False

        by_id: dict[str, dict[str, Any]] = {}
        for r in rules:
            if not isinstance(r, dict):
                continue
            rid = str(r.get("id", "") or "").strip()
            if rid:
                by_id[rid] = r

        changed = False
        for pid, v in (params or {}).items():
            if not isinstance(pid, str) or not pid.startswith("iesm.rules."):
                continue
            if pid not in self.spec_by_id:
                continue
            spec = self.spec_by_id[pid]
            if spec.source_kind != "iesm_rule":
                continue
            tokens = list(spec.path_tokens or [])
            if not tokens:
                continue
            rule_id = str(tokens[0])
            inner = tokens[1:]
            if not inner:
                continue
            if rule_id not in by_id:
                continue
            try:
                nv = float(v)
            except Exception:
                continue
            coerced = self._coerce_param_value(spec=spec, value=nv)
            ok = self._deep_set(by_id[rule_id], inner, coerced)
            changed = changed or ok
        return changed

    def _rule_enabled(self, rule_id: str) -> bool:
        rid = str(rule_id or "").strip()
        if not rid:
            return True
        return rid not in self.disabled_rule_ids

    def _ensure_rule_health_entry(self, rule_id: str) -> dict[str, Any]:
        rid = str(rule_id or "").strip() or "unknown"
        row = self.rule_health.get(rid)
        if not isinstance(row, dict):
            row = {
                "rule_id": rid,
                "hit_count": 0,
                "success_count": 0,
                "failure_count": 0,
                "rollback_count": 0,
                "neutral_count": 0,
                "avg_improvement": 0.0,
                "last_result": "",
                "last_applied_at_ms": 0,
                "last_evaluated_at_ms": 0,
            }
            self.rule_health[rid] = row
        return row

    def _record_rule_hit(self, rule_id: str) -> None:
        row = self._ensure_rule_health_entry(rule_id)
        row["hit_count"] = int(row.get("hit_count", 0) or 0) + 1
        row["last_applied_at_ms"] = _now_ms()

    def _record_rule_result(self, *, rule_id: str, result: str, improvement: float, rolled_back: bool = False) -> None:
        row = self._ensure_rule_health_entry(rule_id)
        if result == "success":
            row["success_count"] = int(row.get("success_count", 0) or 0) + 1
        elif result == "failure":
            row["failure_count"] = int(row.get("failure_count", 0) or 0) + 1
        else:
            row["neutral_count"] = int(row.get("neutral_count", 0) or 0) + 1
        if rolled_back:
            row["rollback_count"] = int(row.get("rollback_count", 0) or 0) + 1
        prev_avg = float(row.get("avg_improvement", 0.0) or 0.0)
        prev_n = int(row.get("success_count", 0) or 0) + int(row.get("failure_count", 0) or 0) + int(row.get("neutral_count", 0) or 0) - 1
        row["avg_improvement"] = (prev_avg * max(0, prev_n) + float(improvement)) / float(max(1, prev_n + 1))
        row["last_result"] = result
        row["last_evaluated_at_ms"] = _now_ms()

    def _backoff_cooldown_ticks(self, *, pid: str, base: int) -> int:
        if not bool(self.cfg.param_backoff_enabled):
            return int(base)
        cap = max(int(base), int(self.cfg.param_backoff_max_cooldown_ticks))
        row = self.param_backoff.get(pid)
        if not isinstance(row, dict):
            return int(base)
        cd = int(row.get("cooldown_ticks", 0) or 0)
        if cd <= 0:
            return int(base)
        return int(max(int(base), min(int(cap), int(cd))))

    def _mark_param_backoff(
        self,
        *,
        pid: str,
        tick_index: int,
        reason: str,
        penalty: float,
    ) -> None:
        if not bool(self.cfg.param_backoff_enabled):
            return
        base = max(2, int(self.cfg.param_backoff_base_cooldown_ticks))
        cap = max(base, int(self.cfg.param_backoff_max_cooldown_ticks))
        penalty = max(0.0, float(penalty))
        row = self.param_backoff.get(pid)
        if not isinstance(row, dict):
            row = {"cooldown_ticks": base, "score": 0.0, "last_tick": -10_000_000, "last_reason": ""}
        # score accumulates; cooldown grows exponentially but is bounded.
        score = float(row.get("score", 0.0) or 0.0) + penalty
        # 2^(floor(score)) gives discrete jumps; mild penalties accumulate gradually.
        try:
            exp = int(max(0.0, math.floor(score)))
        except Exception:
            exp = 0
        cd = int(min(cap, max(base, int(round(base * (2 ** exp))))))
        row.update({"cooldown_ticks": cd, "score": score, "last_tick": int(tick_index), "last_reason": str(reason or "")})
        self.param_backoff[pid] = row
        self._audit(
            {
                "kind": "param_backoff",
                "param": pid,
                "tick_index": int(tick_index),
                "cooldown_ticks": int(cd),
                "score": float(score),
                "reason": str(reason or ""),
            }
        )

    def _register_trial(
        self,
        *,
        update: dict[str, Any],
        persist: bool,
        current_tick: int | None,
        recent_rows: list[dict[str, Any]] | None,
    ) -> None:
        metric_key = str(update.get("metric_key", "") or "").strip()
        if not metric_key:
            return
        values = _recent_window(recent_rows or [], metric_key, max(1, len(recent_rows or [])))
        baseline_mean = _mean(values)
        baseline_std = _std(values)
        trial = {
            "trial_id": f"trial_{_now_ms()}_{len(self.trial_history) + len(self.active_trials)}",
            "rule_id": str(update.get("rule_id", "") or "manual"),
            "param": str(update.get("param", "") or ""),
            "metric_key": metric_key,
            "issue_mode": str(update.get("issue_mode", "") or ""),
            "persist": bool(persist),
            "status": "pending",
            "started_at_ms": _now_ms(),
            "started_tick": int(current_tick) if current_tick is not None else None,
            "evaluate_after_tick": (int(current_tick) + max(3, int(self.cfg.short_window_ticks))) if current_tick is not None else None,
            "baseline_mean": baseline_mean,
            "baseline_std": baseline_std,
            "baseline_latest": values[-1] if values else None,
            "from": _safe_float(update.get("from", 0.0), 0.0),
            "to": _safe_float(update.get("to", 0.0), 0.0),
            "reason": str(update.get("reason", "") or ""),
        }
        self.active_trials.append(trial)
        self.last_applied_updates.append(
            {
                "ts_ms": _now_ms(),
                "rule_id": trial["rule_id"],
                "param": trial["param"],
                "metric_key": metric_key,
                "issue_mode": trial["issue_mode"],
                "persist": bool(persist),
                "from": trial["from"],
                "to": trial["to"],
                "reason": trial["reason"],
            }
        )
        self._save_state()

    def _evaluate_trial_improvement(self, *, trial: dict[str, Any], rows: list[dict[str, Any]]) -> tuple[str, float, dict[str, Any]]:
        metric_key = str(trial.get("metric_key", "") or "").strip()
        values = _recent_window(rows, metric_key, max(1, len(rows)))
        if not values:
            return ("neutral", 0.0, {"current_mean": 0.0, "current_std": 0.0, "current_latest": None})
        current_mean = _mean(values)
        current_std = _std(values)
        baseline_mean = _safe_float(trial.get("baseline_mean", 0.0), 0.0)
        baseline_std = _safe_float(trial.get("baseline_std", 0.0), 0.0)
        issue_mode = str(trial.get("issue_mode", "") or "").strip()
        if issue_mode == "high":
            improvement = baseline_mean - current_mean
        elif issue_mode == "low":
            improvement = current_mean - baseline_mean
        elif issue_mode == "flatline":
            improvement = current_std - baseline_std
        else:
            improvement = baseline_mean - current_mean
        denom = max(0.05, abs(baseline_mean), abs(current_mean), abs(baseline_std), abs(current_std))
        ratio = float(improvement) / float(denom)
        if ratio > 0.05:
            result = "success"
        elif ratio < -0.04:
            result = "failure"
        else:
            result = "neutral"
        return (
            result,
            float(improvement),
            {
                "current_mean": current_mean,
                "current_std": current_std,
                "current_latest": values[-1] if values else None,
                "ratio": ratio,
            },
        )

    def _rollback_param_value(self, *, param: str, value: float, persist: bool, trace_id: str) -> bool:
        pid = self._canonicalize_param_id(param)
        if not pid or pid not in self.param_bounds:
            return False
        bound = self.param_bounds[pid]
        next_v = _round_quantum(_clamp(float(value), bound.min_value, bound.max_value), bound.quantum)
        self.runtime_params[pid] = float(next_v)
        if persist:
            self.persisted_params[pid] = float(next_v)
            self._apply_persisted_overrides_to_persist_files()
        self._apply_overrides_to_runtime(trace_id=trace_id)
        self._save_state()
        return True

    def _evaluate_pending_trials(self, *, current_tick: int, recent_rows: list[dict[str, Any]]) -> None:
        remaining: list[dict[str, Any]] = []
        for trial in list(self.active_trials):
            if not isinstance(trial, dict):
                continue
            if bool(trial.get("persist", False)):
                remaining.append(trial)
                continue
            evaluate_after_tick = trial.get("evaluate_after_tick")
            if evaluate_after_tick is not None and int(current_tick) < int(evaluate_after_tick):
                remaining.append(trial)
                continue
            result, improvement, stats = self._evaluate_trial_improvement(trial=trial, rows=recent_rows)
            rolled_back = False
            if result == "failure":
                rolled_back = self._rollback_param_value(
                    param=str(trial.get("param", "") or ""),
                    value=_safe_float(trial.get("from", 0.0), 0.0),
                    persist=False,
                    trace_id="auto_tuner_trial_rollback",
                )
            # Update per-param backoff. We do this regardless of rollback, because a repeated
            # failure/neutral means "this param doesn't help for this metric right now".
            pid = self._canonicalize_param_id(str(trial.get("param", "") or "").strip())
            if pid:
                penalty = 0.0
                if result == "failure":
                    penalty = float(self.cfg.param_backoff_failure_penalty)
                elif result == "neutral":
                    penalty = float(self.cfg.param_backoff_neutral_penalty)
                if penalty > 0.0:
                    self._mark_param_backoff(
                        pid=pid,
                        tick_index=int(current_tick),
                        reason=f"trial_{result} metric={str(trial.get('metric_key',''))}",
                        penalty=penalty,
                    )
            trial["status"] = "evaluated"
            trial["result"] = result
            trial["improvement"] = float(improvement)
            trial["evaluation"] = stats
            trial["finished_at_ms"] = _now_ms()
            self.trial_history.append(trial)
            self._record_rule_result(rule_id=str(trial.get("rule_id", "") or "manual"), result=result, improvement=float(improvement), rolled_back=rolled_back)
            self._audit(
                {
                    "kind": "trial_evaluated",
                    "trial_id": trial.get("trial_id"),
                    "rule_id": trial.get("rule_id"),
                    "param": trial.get("param"),
                    "metric_key": trial.get("metric_key"),
                    "result": result,
                    "improvement": float(improvement),
                    "rolled_back": bool(rolled_back),
                    "evaluation": stats,
                }
            )
        self.active_trials = remaining[-120:]
        self._save_state()

    def _evaluate_persisted_trials(self, *, recent_rows: list[dict[str, Any]], trace_id: str) -> list[dict[str, Any]]:
        evaluated: list[dict[str, Any]] = []
        remaining: list[dict[str, Any]] = []
        for trial in list(self.active_trials):
            if not isinstance(trial, dict) or not bool(trial.get("persist", False)):
                remaining.append(trial)
                continue
            if str(trial.get("status", "") or "") not in {"pending", ""}:
                remaining.append(trial)
                continue
            result, improvement, stats = self._evaluate_trial_improvement(trial=trial, rows=recent_rows)
            rolled_back = False
            if result == "failure":
                rolled_back = self._rollback_param_value(
                    param=str(trial.get("param", "") or ""),
                    value=_safe_float(trial.get("from", 0.0), 0.0),
                    persist=True,
                    trace_id=f"{trace_id}_persisted_rollback",
                )
            trial["status"] = "evaluated"
            trial["result"] = result
            trial["improvement"] = float(improvement)
            trial["evaluation"] = stats
            trial["finished_at_ms"] = _now_ms()
            self.trial_history.append(trial)
            self._record_rule_result(rule_id=str(trial.get("rule_id", "") or "manual"), result=result, improvement=float(improvement), rolled_back=rolled_back)
            evaluated.append({"trial_id": trial.get("trial_id"), "result": result, "rolled_back": rolled_back, "metric_key": trial.get("metric_key")})
            self._audit(
                {
                    "kind": "persisted_trial_evaluated",
                    "trial_id": trial.get("trial_id"),
                    "rule_id": trial.get("rule_id"),
                    "param": trial.get("param"),
                    "metric_key": trial.get("metric_key"),
                    "result": result,
                    "improvement": float(improvement),
                    "rolled_back": bool(rolled_back),
                    "evaluation": stats,
                }
            )
        self.active_trials = remaining[-120:]
        self._save_state()
        return evaluated

    def _create_rollback_point(self, *, reason: str, summary: dict[str, Any] | None = None) -> dict[str, Any]:
        point = {
            "point_id": f"rb_{_now_ms()}_{len(_load_rollback_points())}",
            "created_at_ms": _now_ms(),
            "reason": str(reason or ""),
            "summary": dict(summary or {}),
            "persisted_params": copy.deepcopy(self.persisted_params),
        }
        _append_rollback_point(point)
        return point

    def _snapshot_rule_state(
        self,
        *,
        rule_id: str,
        rules_cfg: dict[str, Any] | None = None,
        custom_rules: list[dict[str, Any]] | None = None,
        disabled_ids: set[str] | None = None,
        protected_ids: set[str] | None = None,
    ) -> dict[str, Any]:
        cfg = rules_cfg if isinstance(rules_cfg, dict) else self.rules_cfg
        rows = custom_rules if isinstance(custom_rules, list) else list(cfg.get("custom_rules", []) or [])
        disabled = disabled_ids if isinstance(disabled_ids, set) else set(cfg.get("disabled_rule_ids", []))
        protected = protected_ids if isinstance(protected_ids, set) else set(cfg.get("protected_rule_ids", []))
        rid = str(rule_id or "").strip()
        custom_rule = next((dict(row) for row in rows if isinstance(row, dict) and str(row.get("id", "") or "").strip() == rid), None)
        return {
            "rule_id": rid,
            "exists": custom_rule is not None or rid in disabled or rid in protected,
            "disabled": rid in disabled,
            "protected": rid in protected,
            "custom_rule": custom_rule,
        }

    def _resolve_rule_metric_key(self, *, rule_id: str, snapshot: dict[str, Any] | None = None) -> str:
        snap = snapshot if isinstance(snapshot, dict) else self._snapshot_rule_state(rule_id=rule_id)
        custom_rule = snap.get("custom_rule") if isinstance(snap.get("custom_rule"), dict) else {}
        metric_key = str(custom_rule.get("metric_key", "") or "").strip()
        if metric_key:
            return metric_key
        if rule_id.startswith("builtin.") or rule_id.startswith("generated."):
            tokens = [tok for tok in str(rule_id).split(".") if tok]
            for token in tokens:
                if token in self.metric_targets:
                    return token
        return ""

    def _metric_summary(self, *, rows: list[dict[str, Any]], metric_key: str) -> dict[str, Any]:
        key = str(metric_key or "").strip()
        if not key:
            return {}
        values = _recent_window(rows, key, max(1, len(rows)))
        if not values:
            return {
                "metric_key": key,
                "count": 0,
                "mean": 0.0,
                "std": 0.0,
                "min": 0.0,
                "max": 0.0,
                "latest": None,
                "expected_min": None,
                "expected_max": None,
                "ideal": None,
                "outside_expected": False,
            }
        target = self.metric_targets.get(key)
        latest = values[-1] if values else None
        mean_v = _mean(values)
        summary = {
            "metric_key": key,
            "count": len(values),
            "mean": mean_v,
            "std": _std(values),
            "min": min(values),
            "max": max(values),
            "latest": latest,
            "expected_min": float(target.expected_min) if target else None,
            "expected_max": float(target.expected_max) if target else None,
            "ideal": float(target.ideal) if target else None,
            "outside_expected": False,
        }
        band = _band_distribution_summary(values=values, target=target)
        if band:
            summary["band"] = band
        if target and latest is not None:
            summary["outside_expected"] = bool(latest < target.expected_min or latest > target.expected_max)
        return summary

    def _observation_effect_summary(
        self,
        *,
        baseline: dict[str, Any],
        current: dict[str, Any],
        issue_mode: str,
    ) -> dict[str, Any]:
        if not baseline or not current:
            return {"improvement": 0.0, "ratio": 0.0, "result": "neutral"}
        baseline_mean = _safe_float(baseline.get("mean", 0.0), 0.0)
        baseline_std = _safe_float(baseline.get("std", 0.0), 0.0)
        current_mean = _safe_float(current.get("mean", 0.0), 0.0)
        current_std = _safe_float(current.get("std", 0.0), 0.0)
        mode = str(issue_mode or "").strip()
        if mode == "high":
            improvement = baseline_mean - current_mean
        elif mode == "low":
            improvement = current_mean - baseline_mean
        elif mode == "flatline":
            improvement = current_std - baseline_std
        else:
            improvement = baseline_mean - current_mean
        denom = max(0.05, abs(baseline_mean), abs(current_mean), abs(baseline_std), abs(current_std))
        ratio = float(improvement) / float(denom)
        if ratio > 0.05:
            result = "better"
        elif ratio < -0.04:
            result = "worse"
        else:
            result = "mixed"
        return {
            "improvement": float(improvement),
            "ratio": float(ratio),
            "result": result,
        }

    def _ensure_candidate_observations(self) -> None:
        active_by_rule = {
            str(item.get("rule_id", "") or "").strip()
            for item in self.rule_observations
            if isinstance(item, dict) and str(item.get("status", "") or "observing") == "observing"
        }
        changed = False
        for rule in self.custom_rules:
            if not isinstance(rule, dict):
                continue
            if str(rule.get("origin", "") or "").strip() != "llm_auto":
                continue
            if str(rule.get("status", "") or "candidate") not in {"candidate", "observing"}:
                continue
            rule_id = str(rule.get("id", "") or "").strip()
            if not rule_id or rule_id in active_by_rule:
                continue
            observation = {
                "observation_id": f"obs_{_now_ms()}_{len(self.rule_observations) + len(self.observation_history)}",
                "rule_id": rule_id,
                "title": str(rule.get("title", "") or rule_id),
                "status": "observing",
                "source_kind": "candidate_migration",
                "target_kind": "custom_rule",
                "action": "candidate_rule",
                "reason": "migrated_existing_llm_candidate",
                "source_run_id": "",
                "source_suggestion_path": str(rule.get("source_suggestion_path", "") or "").strip(),
                "metric_key": str(rule.get("metric_key", "") or "").strip(),
                "issue_mode": str(rule.get("issue_mode", "") or "").strip(),
                "param_id": str(rule.get("param_id", "") or "").strip(),
                "focus_metrics": [str(rule.get("metric_key", "") or "").strip()] if str(rule.get("metric_key", "") or "").strip() else [],
                "created_at_ms": _now_ms(),
                "revision": 0,
                "before_rule_snapshot": {},
                "current_rule_snapshot": self._snapshot_rule_state(rule_id=rule_id),
                "baseline_metric_summary": {},
                "baseline_focus_summaries": {},
                "observed_runs": [],
                "review_count": 0,
                "last_review_result": {},
                "last_review_at_ms": 0,
            }
            self.rule_observations.append(observation)
            active_by_rule.add(rule_id)
            changed = True
        if changed:
            self._save_state()

    def _create_observation(
        self,
        *,
        rule_id: str,
        title: str,
        source_kind: str,
        target_kind: str,
        action: str,
        reason: str,
        run_id: str,
        suggestion_path: str,
        metric_key: str,
        issue_mode: str,
        param_id: str,
        focus_metrics: list[str],
        before_snapshot: dict[str, Any],
        after_snapshot: dict[str, Any],
        recent_rows: list[dict[str, Any]],
    ) -> dict[str, Any]:
        metric_keys = [str(x).strip() for x in focus_metrics if str(x).strip()]
        if metric_key and metric_key not in metric_keys:
            metric_keys.insert(0, metric_key)
        baseline_metric_summary = self._metric_summary(rows=recent_rows, metric_key=metric_key)
        baseline_focus_summaries = {
            key: self._metric_summary(rows=recent_rows, metric_key=key)
            for key in metric_keys[:6]
            if key
        }
        return {
            "observation_id": f"obs_{_now_ms()}_{len(self.rule_observations) + len(self.observation_history)}",
            "rule_id": str(rule_id or "").strip(),
            "title": str(title or rule_id or "observation"),
            "status": "observing",
            "source_kind": str(source_kind or "").strip(),
            "target_kind": str(target_kind or "").strip(),
            "action": str(action or "").strip(),
            "reason": str(reason or "").strip(),
            "source_run_id": str(run_id or "").strip(),
            "source_suggestion_path": str(suggestion_path or "").strip(),
            "metric_key": str(metric_key or "").strip(),
            "issue_mode": str(issue_mode or "").strip(),
            "param_id": str(param_id or "").strip(),
            "focus_metrics": metric_keys[:6],
            "created_at_ms": _now_ms(),
            "revision": 0,
            "before_rule_snapshot": dict(before_snapshot or {}),
            "current_rule_snapshot": dict(after_snapshot or {}),
            "baseline_metric_summary": baseline_metric_summary,
            "baseline_focus_summaries": baseline_focus_summaries,
            "observed_runs": [],
            "review_count": 0,
            "last_review_result": {},
            "last_review_at_ms": 0,
        }

    def _append_observation(self, observation: dict[str, Any]) -> None:
        if not isinstance(observation, dict):
            return
        self.rule_observations = [
            item
            for item in self.rule_observations
            if not (
                isinstance(item, dict)
                and str(item.get("observation_id", "") or "").strip() == str(observation.get("observation_id", "") or "").strip()
            )
        ]
        self.rule_observations.append(observation)
        self.rule_observations = self.rule_observations[-160:]
        self._save_state()

    def _record_observation_run(self, *, observation: dict[str, Any], recent_rows: list[dict[str, Any]], run_id: str, trace_id: str) -> dict[str, Any]:
        metric_key = str(observation.get("metric_key", "") or "").strip()
        focus_metrics = [str(x).strip() for x in (observation.get("focus_metrics") or []) if str(x).strip()]
        if metric_key and metric_key not in focus_metrics:
            focus_metrics.insert(0, metric_key)
        primary_summary = self._metric_summary(rows=recent_rows, metric_key=metric_key)
        focus_summaries = {key: self._metric_summary(rows=recent_rows, metric_key=key) for key in focus_metrics[:6] if key}
        effect = self._observation_effect_summary(
            baseline=observation.get("baseline_metric_summary") if isinstance(observation.get("baseline_metric_summary"), dict) else {},
            current=primary_summary,
            issue_mode=str(observation.get("issue_mode", "") or "").strip(),
        )
        rule_id = str(observation.get("rule_id", "") or "").strip()
        health = self.rule_health.get(rule_id, {}) if isinstance(self.rule_health.get(rule_id), dict) else {}
        run_record = {
            "run_id": str(run_id or "").strip(),
            "trace_id": str(trace_id or "").strip(),
            "observed_at_ms": _now_ms(),
            "metric_summary": primary_summary,
            "focus_summaries": focus_summaries,
            "effect": effect,
            "rule_health": {
                "hit_count": int(health.get("hit_count", 0) or 0),
                "success_count": int(health.get("success_count", 0) or 0),
                "failure_count": int(health.get("failure_count", 0) or 0),
                "rollback_count": int(health.get("rollback_count", 0) or 0),
                "avg_improvement": float(health.get("avg_improvement", 0.0) or 0.0),
                "last_result": str(health.get("last_result", "") or ""),
            },
        }
        observed_runs = [dict(item) for item in (observation.get("observed_runs") or []) if isinstance(item, dict)]
        observed_runs = [item for item in observed_runs if str(item.get("run_id", "") or "").strip() != str(run_id or "").strip()]
        observed_runs.append(run_record)
        observation["observed_runs"] = observed_runs[-12:]
        return run_record

    def _record_observation_run_results(self, *, recent_rows: list[dict[str, Any]], run_id: str, trace_id: str) -> list[dict[str, Any]]:
        changed = False
        recorded: list[dict[str, Any]] = []
        for observation in self.rule_observations:
            if not isinstance(observation, dict):
                continue
            if str(observation.get("status", "") or "observing") != "observing":
                continue
            # Rules added or adjusted during this run only start affecting the next run.
            if str(observation.get("source_run_id", "") or "").strip() == str(run_id or "").strip() and not list(observation.get("observed_runs") or []):
                continue
            run_record = self._record_observation_run(observation=observation, recent_rows=recent_rows, run_id=run_id, trace_id=trace_id)
            recorded.append(
                {
                    "observation_id": str(observation.get("observation_id", "") or ""),
                    "rule_id": str(observation.get("rule_id", "") or ""),
                    "run_id": str(run_id or ""),
                    "effect": run_record.get("effect", {}),
                }
            )
            changed = True
        if changed:
            self._save_state()
            self._audit({"kind": "observation_run_recorded", "run_id": run_id, "items": recorded[:24]})
        return recorded

    def _observation_is_reviewable(self, observation: dict[str, Any]) -> bool:
        if not isinstance(observation, dict):
            return False
        if str(observation.get("status", "") or "observing") != "observing":
            return False
        min_runs = max(1, int(self.cfg.llm_auto_validation_min_runs))
        observed_runs = [item for item in (observation.get("observed_runs") or []) if isinstance(item, dict)]
        return len(observed_runs) >= min_runs

    def _summarize_unhealthy_state(self, *, recent_rows: list[dict[str, Any]]) -> dict[str, Any]:
        issues: list[dict[str, Any]] = []
        severe_count = 0
        for metric_key in self.metric_targets.keys():
            issue = self._metric_issue_snapshot(rows=recent_rows, metric_key=metric_key)
            if not issue:
                continue
            severity = max(float(issue.get("high_ratio", 0.0)), float(issue.get("low_ratio", 0.0)), 1.0 if bool(issue.get("flatline", False)) else 0.0)
            item = {
                "metric_key": metric_key,
                "mean": float(issue.get("mean", 0.0)),
                "std": float(issue.get("std", 0.0)),
                "latest": float(issue.get("latest", 0.0)),
                "severity": float(severity),
                "high_ratio": float(issue.get("high_ratio", 0.0)),
                "low_ratio": float(issue.get("low_ratio", 0.0)),
                "flatline": bool(issue.get("flatline", False)),
            }
            issues.append(item)
            if severity >= 0.18:
                severe_count += 1

        unhealthy_rules: list[dict[str, Any]] = []
        rollback_count = 0
        failure_count = 0
        for rule_id, row in (self.rule_health or {}).items():
            if not isinstance(row, dict):
                continue
            failures = int(row.get("failure_count", 0) or 0)
            rollbacks = int(row.get("rollback_count", 0) or 0)
            successes = int(row.get("success_count", 0) or 0)
            if failures <= 0 and rollbacks <= 0:
                continue
            failure_count += failures
            rollback_count += rollbacks
            unhealthy_rules.append(
                {
                    "rule_id": str(rule_id),
                    "failure_count": failures,
                    "rollback_count": rollbacks,
                    "success_count": successes,
                    "avg_improvement": float(row.get("avg_improvement", 0.0) or 0.0),
                    "last_result": str(row.get("last_result", "") or ""),
                }
            )

        unhealthy_rules.sort(key=lambda item: (item["rollback_count"], item["failure_count"]), reverse=True)
        issues.sort(key=lambda item: item["severity"], reverse=True)
        score = severe_count + failure_count + rollback_count
        return {
            "issue_count": len(issues),
            "severe_issue_count": severe_count,
            "failure_count": failure_count,
            "rollback_count": rollback_count,
            "unhealthy_score": score,
            "issues": issues[:24],
            "unhealthy_rules": unhealthy_rules[:24],
        }

    def _should_trigger_llm_auto_loop(self, *, recent_rows: list[dict[str, Any]]) -> tuple[bool, dict[str, Any]]:
        summary = self._summarize_unhealthy_state(recent_rows=recent_rows)
        llm_cfg = _load_auto_tuner_llm_cfg_from_path(_llm_config_path(), fallback=load_review_config())
        if not bool(self.cfg.llm_assist_enabled):
            summary["reason"] = "cfg.llm_assist_enabled=false"
            return (False, summary)
        if not bool(llm_cfg.enabled) or not bool(llm_cfg.auto_analyze_on_completion):
            summary["reason"] = "llm auto analyze disabled"
            return (False, summary)
        trigger_score = max(2, int(self.cfg.llm_assist_trigger_windows))
        summary["trigger_score"] = trigger_score
        summary["reason"] = "score_below_threshold"
        if int(summary["unhealthy_score"]) >= trigger_score:
            summary["reason"] = "unhealthy_score_trigger"
            return (True, summary)
        if int(summary["rollback_count"]) >= 2:
            summary["reason"] = "rollback_trigger"
            return (True, summary)
        if int(summary["failure_count"]) >= 3:
            summary["reason"] = "failure_trigger"
            return (True, summary)
        return (False, summary)

    def _build_llm_auto_prompt(self, *, summary: dict[str, Any]) -> str:
        parts = [
            "请优先分析以下自动闭环上下文：",
            json.dumps(summary, ensure_ascii=False, indent=2),
            "",
            "请给出尽量少、但足够针对性的规则调整与候选实验。",
            "如果某条规则已经被证明健康或与当前问题无关，不要动它。",
            "如果问题看起来是环境变化，请优先给出候选实验，不要直接永久禁用大量规则。",
        ]
        return "\n".join(parts)

    def _apply_llm_suggestion_result(self, *, run_id: str, suggestion_path: str, recent_rows: list[dict[str, Any]]) -> dict[str, Any]:
        raw = _load_json_dict(Path(suggestion_path))
        parsed = _normalize_llm_suggestion_payload(raw.get("parsed_json") if isinstance(raw.get("parsed_json"), dict) else {})
        if not parsed:
            return {"success": False, "error": "parsed_json_missing"}

        rules_cfg = load_auto_tuner_rules()
        disabled_ids = set(rules_cfg.get("disabled_rule_ids", []))
        protected_ids = set(rules_cfg.get("protected_rule_ids", []))
        custom_rules = [dict(item) for item in (rules_cfg.get("custom_rules", []) or []) if isinstance(item, dict)]
        applied_rule_changes: list[dict[str, Any]] = []
        skipped_changes: list[dict[str, Any]] = []
        added_experiments: list[dict[str, Any]] = []
        created_observations: list[dict[str, Any]] = []
        rollback_point: dict[str, Any] | None = None
        custom_rule_ids = {str(item.get("id", "") or "").strip() for item in custom_rules if isinstance(item, dict)}
        suggestion_focus_metrics = [str(x).strip() for x in (raw.get("focus_metrics") or []) if str(x).strip()]
        suggestion_metric_findings = [dict(item) for item in (parsed.get("metric_findings") or []) if isinstance(item, dict)]

        def _has_enough_evidence_for_direct_rule_change(rule_id: str) -> bool:
            if not rule_id:
                return False
            if rule_id.startswith("llm_auto::") or rule_id in custom_rule_ids:
                return True
            health = self.rule_health.get(rule_id, {}) if isinstance(self.rule_health.get(rule_id), dict) else {}
            rollback_count = int(health.get("rollback_count", 0) or 0)
            failure_count = int(health.get("failure_count", 0) or 0)
            hit_count = int(health.get("hit_count", 0) or 0)
            avg_improvement = float(health.get("avg_improvement", 0.0) or 0.0)
            return rollback_count >= 1 or failure_count >= 2 or (hit_count >= 3 and avg_improvement < 0.0)

        pending_observations: list[dict[str, Any]] = []

        for change in parsed.get("suggested_rule_changes", []):
            rule_id = str(change.get("rule_id", "") or "").strip()
            action = str(change.get("action", "") or "").strip()
            payload = change.get("payload") if isinstance(change.get("payload"), dict) else {}
            if rule_id in self.protected_rule_ids or rule_id in protected_ids:
                skipped_changes.append({**change, "skip_reason": "protected"})
                continue
            if action in {"disable", "protect"} and not _has_enough_evidence_for_direct_rule_change(rule_id):
                skipped_changes.append({**change, "skip_reason": "insufficient_rule_evidence_for_auto_apply"})
                continue
            if action == "disable":
                before_snapshot = self._snapshot_rule_state(
                    rule_id=rule_id,
                    custom_rules=custom_rules,
                    disabled_ids=disabled_ids,
                    protected_ids=protected_ids,
                )
                disabled_ids.add(rule_id)
                applied_rule_changes.append(change)
                after_snapshot = self._snapshot_rule_state(
                    rule_id=rule_id,
                    custom_rules=custom_rules,
                    disabled_ids=disabled_ids,
                    protected_ids=protected_ids,
                )
                pending_observations.append(
                    {
                        "rule_id": rule_id,
                        "title": f"LLM 规则变更：{action}",
                        "source_kind": "llm_rule_change",
                        "target_kind": "rule_state",
                        "action": action,
                        "reason": str(change.get("reason", "") or ""),
                        "metric_key": self._resolve_rule_metric_key(rule_id=rule_id, snapshot=after_snapshot),
                        "issue_mode": "",
                        "param_id": "",
                        "focus_metrics": suggestion_focus_metrics,
                        "before_snapshot": before_snapshot,
                        "after_snapshot": after_snapshot,
                    }
                )
            elif action == "enable":
                before_snapshot = self._snapshot_rule_state(
                    rule_id=rule_id,
                    custom_rules=custom_rules,
                    disabled_ids=disabled_ids,
                    protected_ids=protected_ids,
                )
                disabled_ids.discard(rule_id)
                applied_rule_changes.append(change)
                after_snapshot = self._snapshot_rule_state(
                    rule_id=rule_id,
                    custom_rules=custom_rules,
                    disabled_ids=disabled_ids,
                    protected_ids=protected_ids,
                )
                pending_observations.append(
                    {
                        "rule_id": rule_id,
                        "title": f"LLM 规则变更：{action}",
                        "source_kind": "llm_rule_change",
                        "target_kind": "rule_state",
                        "action": action,
                        "reason": str(change.get("reason", "") or ""),
                        "metric_key": self._resolve_rule_metric_key(rule_id=rule_id, snapshot=after_snapshot),
                        "issue_mode": "",
                        "param_id": "",
                        "focus_metrics": suggestion_focus_metrics,
                        "before_snapshot": before_snapshot,
                        "after_snapshot": after_snapshot,
                    }
                )
            elif action == "protect":
                before_snapshot = self._snapshot_rule_state(
                    rule_id=rule_id,
                    custom_rules=custom_rules,
                    disabled_ids=disabled_ids,
                    protected_ids=protected_ids,
                )
                protected_ids.add(rule_id)
                applied_rule_changes.append(change)
                after_snapshot = self._snapshot_rule_state(
                    rule_id=rule_id,
                    custom_rules=custom_rules,
                    disabled_ids=disabled_ids,
                    protected_ids=protected_ids,
                )
                pending_observations.append(
                    {
                        "rule_id": rule_id,
                        "title": f"LLM 规则变更：{action}",
                        "source_kind": "llm_rule_change",
                        "target_kind": "rule_state",
                        "action": action,
                        "reason": str(change.get("reason", "") or ""),
                        "metric_key": self._resolve_rule_metric_key(rule_id=rule_id, snapshot=after_snapshot),
                        "issue_mode": "",
                        "param_id": "",
                        "focus_metrics": suggestion_focus_metrics,
                        "before_snapshot": before_snapshot,
                        "after_snapshot": after_snapshot,
                    }
                )
            elif action == "unprotect":
                before_snapshot = self._snapshot_rule_state(
                    rule_id=rule_id,
                    custom_rules=custom_rules,
                    disabled_ids=disabled_ids,
                    protected_ids=protected_ids,
                )
                protected_ids.discard(rule_id)
                applied_rule_changes.append(change)
                after_snapshot = self._snapshot_rule_state(
                    rule_id=rule_id,
                    custom_rules=custom_rules,
                    disabled_ids=disabled_ids,
                    protected_ids=protected_ids,
                )
                pending_observations.append(
                    {
                        "rule_id": rule_id,
                        "title": f"LLM 规则变更：{action}",
                        "source_kind": "llm_rule_change",
                        "target_kind": "rule_state",
                        "action": action,
                        "reason": str(change.get("reason", "") or ""),
                        "metric_key": self._resolve_rule_metric_key(rule_id=rule_id, snapshot=after_snapshot),
                        "issue_mode": "",
                        "param_id": "",
                        "focus_metrics": suggestion_focus_metrics,
                        "before_snapshot": before_snapshot,
                        "after_snapshot": after_snapshot,
                    }
                )
            elif action == "update_custom_rule":
                norm = _normalize_custom_rule(payload)
                if not norm:
                    skipped_changes.append({**change, "skip_reason": "invalid_custom_rule_payload"})
                    continue
                if str(norm.get("id", "")) in self.protected_rule_ids or str(norm.get("id", "")) in protected_ids:
                    skipped_changes.append({**change, "skip_reason": "payload_rule_protected"})
                    continue
                before_snapshot = self._snapshot_rule_state(
                    rule_id=str(norm.get("id", "") or ""),
                    custom_rules=custom_rules,
                    disabled_ids=disabled_ids,
                    protected_ids=protected_ids,
                )
                replaced = False
                for idx, row in enumerate(custom_rules):
                    if str(row.get("id", "") or "") == str(norm.get("id", "") or ""):
                        custom_rules[idx] = norm
                        replaced = True
                        break
                if not replaced:
                    custom_rules.append(norm)
                applied_rule_changes.append(change)
                after_snapshot = self._snapshot_rule_state(
                    rule_id=str(norm.get("id", "") or ""),
                    custom_rules=custom_rules,
                    disabled_ids=disabled_ids,
                    protected_ids=protected_ids,
                )
                pending_observations.append(
                    {
                        "rule_id": str(norm.get("id", "") or ""),
                        "title": str(norm.get("title", "") or "LLM 更新自定义规则"),
                        "source_kind": "llm_rule_change",
                        "target_kind": "custom_rule",
                        "action": action,
                        "reason": str(change.get("reason", "") or ""),
                        "metric_key": str(norm.get("metric_key", "") or ""),
                        "issue_mode": str(norm.get("issue_mode", "") or ""),
                        "param_id": str(norm.get("param_id", "") or ""),
                        "focus_metrics": suggestion_focus_metrics,
                        "before_snapshot": before_snapshot,
                        "after_snapshot": after_snapshot,
                    }
                )

        for idx, exp_item in enumerate(sorted(parsed.get("suggested_experiments", []), key=lambda item: int(item.get("priority", 99) or 99))[:6]):
            pid = self._canonicalize_param_id(str(exp_item.get("param_id", "") or "").strip())
            if not pid or pid not in self.param_bounds:
                skipped_changes.append({**exp_item, "skip_reason": "unknown_param"})
                continue
            rule_id = str(exp_item.get("experiment_id", "") or "").strip() or f"llm_auto::{storage.safe_slug(run_id or 'global')}::{_now_ms()}::{idx}"
            if rule_id in self.protected_rule_ids or rule_id in protected_ids:
                skipped_changes.append({**exp_item, "skip_reason": "experiment_rule_protected"})
                continue
            direction = int(exp_item.get("direction", 0) or 0)
            if direction not in {-1, 1}:
                skipped_changes.append({**exp_item, "skip_reason": "direction_zero"})
                continue
            before_snapshot = self._snapshot_rule_state(
                rule_id=rule_id,
                custom_rules=custom_rules,
                disabled_ids=disabled_ids,
                protected_ids=protected_ids,
            )
            custom_rule = {
                "id": rule_id,
                "title": str(exp_item.get("title", "") or f"LLM 候选实验 {idx + 1}"),
                "description": str(exp_item.get("reason", "") or ""),
                "enabled": True,
                "metric_key": str(exp_item.get("metric_key", "") or ""),
                "issue_mode": str(exp_item.get("issue_mode", "") or ""),
                "param_id": pid,
                "direction": direction,
                "step_scale": float(exp_item.get("step_scale", 0.35) or 0.35),
                "min_severity": float(exp_item.get("min_severity", 0.05) or 0.05),
                "cooldown_ticks": int(exp_item.get("cooldown_ticks", 0) or 0),
                "protect_from_llm": False,
                "origin": "llm_auto",
                "status": "candidate",
                "source_suggestion_path": str(suggestion_path),
                "applied_at_ms": _now_ms(),
                "evaluation_runs": 0,
                "llm_confidence": float(exp_item.get("confidence", 0.0) or 0.0),
                "notes": [f"priority={int(exp_item.get('priority', 1) or 1)}", f"run_id={run_id}"],
            }
            existing_idx = None
            for j, row in enumerate(custom_rules):
                if str(row.get("id", "") or "") == rule_id:
                    existing_idx = j
                    break
            if existing_idx is None:
                custom_rules.append(custom_rule)
            else:
                custom_rules[existing_idx] = custom_rule
            added_experiments.append(custom_rule)
            after_snapshot = self._snapshot_rule_state(
                rule_id=rule_id,
                custom_rules=custom_rules,
                disabled_ids=disabled_ids,
                protected_ids=protected_ids,
            )
            pending_observations.append(
                {
                    "rule_id": rule_id,
                    "title": str(custom_rule.get("title", "") or rule_id),
                    "source_kind": "llm_experiment",
                    "target_kind": "custom_rule",
                    "action": "candidate_rule",
                    "reason": str(exp_item.get("reason", "") or ""),
                    "metric_key": str(exp_item.get("metric_key", "") or ""),
                    "issue_mode": str(exp_item.get("issue_mode", "") or ""),
                    "param_id": pid,
                    "focus_metrics": suggestion_focus_metrics,
                    "before_snapshot": before_snapshot,
                    "after_snapshot": after_snapshot,
                }
            )

        changed = bool(applied_rule_changes or added_experiments)
        if changed:
            rollback_point = self._create_rollback_point(
                reason="llm_auto_loop_before_apply",
                summary={"run_id": run_id, "suggestion_path": suggestion_path, "summary": parsed.get("summary", "")},
            )

            save_auto_tuner_rules(
                {
                    "disabled_rule_ids": sorted(disabled_ids),
                    "protected_rule_ids": sorted(protected_ids),
                    "custom_rules": custom_rules,
                }
            )

        apply_result = {
            "success": True,
            "changed": changed,
            "rollback_point": rollback_point,
            "applied_rule_changes": applied_rule_changes,
            "added_experiments": [{"id": item["id"], "metric_key": item["metric_key"], "param_id": item["param_id"]} for item in added_experiments],
            "skipped_changes": skipped_changes,
        }
        if created_observations:
            apply_result["created_observations"] = created_observations
        if changed:
            self.rules_cfg = load_auto_tuner_rules()
            self.disabled_rule_ids = set(self.rules_cfg.get("disabled_rule_ids", []))
            self.protected_rule_ids = set(self.rules_cfg.get("protected_rule_ids", []))
            self.custom_rules = list(self.rules_cfg.get("custom_rules", []))
            for item in pending_observations:
                if not isinstance(item, dict):
                    continue
                resolved_metric_key = str(item.get("metric_key", "") or "").strip()
                if not resolved_metric_key and suggestion_metric_findings:
                    resolved_metric_key = str(suggestion_metric_findings[0].get("metric_key", "") or "").strip()
                issue_mode = str(item.get("issue_mode", "") or "").strip()
                if not issue_mode and suggestion_metric_findings:
                    issue_mode = str(suggestion_metric_findings[0].get("status", "") or "").strip()
                    if issue_mode == "ok":
                        issue_mode = ""
                observation = self._create_observation(
                    rule_id=str(item.get("rule_id", "") or ""),
                    title=str(item.get("title", "") or ""),
                    source_kind=str(item.get("source_kind", "") or ""),
                    target_kind=str(item.get("target_kind", "") or ""),
                    action=str(item.get("action", "") or ""),
                    reason=str(item.get("reason", "") or ""),
                    run_id=run_id,
                    suggestion_path=str(suggestion_path or ""),
                    metric_key=resolved_metric_key,
                    issue_mode=issue_mode,
                    param_id=str(item.get("param_id", "") or ""),
                    focus_metrics=[resolved_metric_key] + list(item.get("focus_metrics", []) or []),
                    before_snapshot=dict(item.get("before_snapshot", {}) or {}),
                    after_snapshot=self._snapshot_rule_state(rule_id=str(item.get("rule_id", "") or "")),
                    recent_rows=recent_rows,
                )
                self._append_observation(observation)
                created_observations.append(
                    {
                        "observation_id": observation.get("observation_id", ""),
                        "rule_id": observation.get("rule_id", ""),
                        "metric_key": observation.get("metric_key", ""),
                    }
                )
        self._audit({"kind": "llm_auto_apply", "run_id": run_id, "suggestion_path": suggestion_path, "result": apply_result})
        try:
            raw["auto_apply_result"] = apply_result
            raw["auto_applied_at_ms"] = _now_ms()
            raw["auto_apply_run_id"] = str(run_id or "")
            Path(suggestion_path).write_text(json.dumps(raw, ensure_ascii=False, indent=2), encoding="utf-8")
        except Exception:
            pass
        return apply_result

    def _maintain_llm_candidate_rules(self) -> dict[str, Any]:
        rules_cfg = load_auto_tuner_rules()
        custom_rules = [dict(item) for item in (rules_cfg.get("custom_rules", []) or []) if isinstance(item, dict)]
        changed = False
        updated: list[dict[str, Any]] = []
        observation_rule_ids = {
            str(item.get("rule_id", "") or "").strip()
            for item in self.rule_observations
            if isinstance(item, dict) and str(item.get("status", "") or "observing") == "observing"
        }
        for idx, rule in enumerate(custom_rules):
            if str(rule.get("origin", "") or "").strip() != "llm_auto":
                continue
            rule_id = str(rule.get("id", "") or "").strip()
            if rule_id in observation_rule_ids:
                custom_rules[idx] = rule
                continue
            health = self.rule_health.get(rule_id, {}) if isinstance(self.rule_health.get(rule_id), dict) else {}
            status = str(rule.get("status", "") or "candidate")
            rule["evaluation_runs"] = int(rule.get("evaluation_runs", 0) or 0) + 1
            if status == "candidate":
                success_count = int(health.get("success_count", 0) or 0)
                failure_count = int(health.get("failure_count", 0) or 0)
                rollback_count = int(health.get("rollback_count", 0) or 0)
                avg_improvement = float(health.get("avg_improvement", 0.0) or 0.0)
                if success_count >= 2 and avg_improvement > 0.0 and failure_count == 0:
                    rule["status"] = "solidified"
                    changed = True
                    updated.append({"rule_id": rule_id, "status": "solidified"})
                elif rollback_count >= 1 or (failure_count >= 2 and success_count == 0):
                    rule["status"] = "rejected"
                    rule["enabled"] = False
                    changed = True
                    updated.append({"rule_id": rule_id, "status": "rejected"})
            custom_rules[idx] = rule
        if changed:
            save_auto_tuner_rules(
                {
                    "disabled_rule_ids": rules_cfg.get("disabled_rule_ids", []),
                    "protected_rule_ids": rules_cfg.get("protected_rule_ids", []),
                    "custom_rules": custom_rules,
                }
            )
            self.rules_cfg = load_auto_tuner_rules()
            self.disabled_rule_ids = set(self.rules_cfg.get("disabled_rule_ids", []))
            self.protected_rule_ids = set(self.rules_cfg.get("protected_rule_ids", []))
            self.custom_rules = list(self.rules_cfg.get("custom_rules", []))
            self._audit({"kind": "llm_candidate_maintenance", "updated": updated})
        return {"changed": changed, "updated": updated}

    def _maybe_run_llm_auto_loop(self, *, recent_rows: list[dict[str, Any]], run_id: str) -> dict[str, Any]:
        should_run, summary = self._should_trigger_llm_auto_loop(recent_rows=recent_rows)
        maintenance = self._maintain_llm_candidate_rules()
        if not should_run:
            return {"triggered": False, "summary": summary, "maintenance": maintenance}
        user_prompt = self._build_llm_auto_prompt(summary=summary)
        res = analyze_auto_tuner_with_llm(app=self.app, run_id=run_id, user_prompt=user_prompt, focus_metrics=[item["metric_key"] for item in summary.get("issues", [])[:8]])
        if not res.get("success", False):
            self._audit({"kind": "llm_auto_loop_failed", "run_id": run_id, "summary": summary, "error": res.get("error", ""), "message": res.get("message", "")})
            return {"triggered": True, "analysis": res, "summary": summary, "maintenance": maintenance}
        apply_res = self._apply_llm_suggestion_result(
            run_id=run_id,
            suggestion_path=str(res.get("suggestion_path", "") or ""),
            recent_rows=recent_rows,
        )
        return {"triggered": True, "analysis": res, "apply_result": apply_res, "summary": summary, "maintenance": maintenance}

    def _build_llm_observation_prompt(self, *, review_items: list[dict[str, Any]], recent_rows: list[dict[str, Any]], run_id: str) -> str:
        recent_summary = self._summarize_unhealthy_state(recent_rows=recent_rows)
        active_snapshots = []
        for item in review_items:
            if not isinstance(item, dict):
                continue
            active_snapshots.append(
                {
                    "observation_id": str(item.get("observation_id", "") or ""),
                    "rule_id": str(item.get("rule_id", "") or ""),
                    "title": str(item.get("title", "") or ""),
                    "source_kind": str(item.get("source_kind", "") or ""),
                    "target_kind": str(item.get("target_kind", "") or ""),
                    "action": str(item.get("action", "") or ""),
                    "reason": str(item.get("reason", "") or ""),
                    "metric_key": str(item.get("metric_key", "") or ""),
                    "issue_mode": str(item.get("issue_mode", "") or ""),
                    "param_id": str(item.get("param_id", "") or ""),
                    "focus_metrics": [str(x).strip() for x in (item.get("focus_metrics") or []) if str(x).strip()],
                    "baseline_metric_summary": dict(item.get("baseline_metric_summary", {}) or {}),
                    "baseline_focus_summaries": dict(item.get("baseline_focus_summaries", {}) or {}),
                    "observed_runs": list(item.get("observed_runs", []) or [])[-6:],
                    "review_count": int(item.get("review_count", 0) or 0),
                    "before_rule_snapshot": dict(item.get("before_rule_snapshot", {}) or {}),
                    "current_rule_snapshot": dict(item.get("current_rule_snapshot", {}) or {}),
                }
            )

        parts = [
            "你是 AP 原型自适应调参器中的“自动验收审稿器”。",
            "你的职责不是激进改规则，而是谨慎判断观察区中的规则是否已经证明有效、证明无效，还是应该继续观察。",
            "请优先保持可审计、小步、可回滚、符合理论预期的风格。",
            "只有在证据充分时才固化或回退；如果证据不够，就继续观察。",
            "",
            "[当前运行]",
            json.dumps({"run_id": run_id, "recent_unhealthy_summary": recent_summary}, ensure_ascii=False, indent=2),
            "",
            "[观察区候选]",
            json.dumps(active_snapshots, ensure_ascii=False, indent=2),
            "",
            "[受保护规则]",
            json.dumps(sorted(self.protected_rule_ids), ensure_ascii=False, indent=2),
            "",
            "请输出两部分：",
            "1. 先用中文给出谨慎、具体的验收结论。",
            "2. 再输出一个 ```json``` 代码块，格式如下：",
            "```json",
            json.dumps(
                {
                    "summary": "",
                    "observation_reviews": [
                        {
                            "observation_id": "",
                            "rule_id": "",
                            "action": "keep_observing|solidify|reject_disable|adjust_rule|revert_change|remove_custom_rule",
                            "reason": "",
                            "confidence": 0.0,
                            "payload": {
                                "custom_rule": {},
                                "set_disabled": None,
                                "set_protected": None,
                            },
                        }
                    ],
                    "notes": [],
                },
                ensure_ascii=False,
                indent=2,
            ),
            "```",
            "注意：",
            "1. protected 规则不能被修改。",
            "2. 如果只是证据不足，请用 keep_observing。",
            "3. adjust_rule 只允许做小步修改，不要大幅改写规则目标。",
            "4. 如果观察前后没有明确改善，优先考虑 revert_change 或 remove_custom_rule，而不是强行固化。",
        ]
        return "\n".join(parts)

    def _apply_snapshot_to_rule_state(
        self,
        *,
        rule_id: str,
        snapshot: dict[str, Any],
        custom_rules: list[dict[str, Any]],
        disabled_ids: set[str],
        protected_ids: set[str],
    ) -> bool:
        rid = str(rule_id or "").strip()
        if not rid:
            return False
        before_rule = snapshot.get("custom_rule") if isinstance(snapshot.get("custom_rule"), dict) else None
        existing_idx = next((idx for idx, row in enumerate(custom_rules) if str(row.get("id", "") or "").strip() == rid), None)
        changed = False
        if before_rule is None:
            if existing_idx is not None:
                custom_rules.pop(int(existing_idx))
                changed = True
        else:
            norm = _normalize_custom_rule(before_rule)
            if norm:
                if existing_idx is None:
                    custom_rules.append(norm)
                else:
                    custom_rules[int(existing_idx)] = norm
                changed = True
        should_disable = bool(snapshot.get("disabled", False))
        if should_disable and rid not in disabled_ids:
            disabled_ids.add(rid)
            changed = True
        if not should_disable and rid in disabled_ids:
            disabled_ids.discard(rid)
            changed = True
        should_protect = bool(snapshot.get("protected", False))
        if should_protect and rid not in protected_ids:
            protected_ids.add(rid)
            changed = True
        if not should_protect and rid in protected_ids:
            protected_ids.discard(rid)
            changed = True
        return changed

    def _apply_llm_observation_review_result(
        self,
        *,
        run_id: str,
        recent_rows: list[dict[str, Any]],
        review_payload: dict[str, Any],
    ) -> dict[str, Any]:
        parsed = _normalize_llm_observation_review_payload(review_payload)
        if not parsed:
            return {"success": False, "error": "invalid_observation_review_payload"}

        rules_cfg = load_auto_tuner_rules()
        custom_rules = [dict(item) for item in (rules_cfg.get("custom_rules", []) or []) if isinstance(item, dict)]
        disabled_ids = set(rules_cfg.get("disabled_rule_ids", []))
        protected_ids = set(rules_cfg.get("protected_rule_ids", []))
        active_by_id = {
            str(item.get("observation_id", "") or "").strip(): dict(item)
            for item in self.rule_observations
            if isinstance(item, dict)
        }
        changed_rules = False
        applied: list[dict[str, Any]] = []
        skipped: list[dict[str, Any]] = []
        remaining: list[dict[str, Any]] = []
        history_additions: list[dict[str, Any]] = []

        review_items_by_id = {
            str(item.get("observation_id", "") or "").strip(): item
            for item in parsed.get("observation_reviews", [])
            if isinstance(item, dict) and str(item.get("observation_id", "") or "").strip()
        }

        for current in self.rule_observations:
            if not isinstance(current, dict):
                continue
            obs_id = str(current.get("observation_id", "") or "").strip()
            review_item = review_items_by_id.get(obs_id)
            if not review_item:
                remaining.append(current)
                continue

            rule_id = str(current.get("rule_id", "") or "").strip()
            action = str(review_item.get("action", "") or "").strip()
            payload = review_item.get("payload") if isinstance(review_item.get("payload"), dict) else {}
            confidence = float(review_item.get("confidence", 0.0) or 0.0)
            reason = str(review_item.get("reason", "") or "").strip()

            current["review_count"] = int(current.get("review_count", 0) or 0) + 1
            current["last_review_at_ms"] = _now_ms()
            current["last_review_result"] = {
                "run_id": run_id,
                "action": action,
                "reason": reason,
                "confidence": confidence,
            }

            if rule_id in self.protected_rule_ids or rule_id in protected_ids:
                if action not in {"keep_observing", "solidify"}:
                    skipped.append({"observation_id": obs_id, "rule_id": rule_id, "action": action, "reason": "protected"})
                    remaining.append(current)
                    continue

            terminal_status = ""
            if action == "keep_observing":
                remaining.append(current)
                applied.append({"observation_id": obs_id, "rule_id": rule_id, "action": action})
                continue

            if action == "solidify":
                for idx, row in enumerate(custom_rules):
                    if str(row.get("id", "") or "").strip() == rule_id:
                        row["status"] = "solidified"
                        row["evaluation_runs"] = max(int(row.get("evaluation_runs", 0) or 0), len(current.get("observed_runs", []) or []))
                        custom_rules[idx] = row
                        changed_rules = True
                        break
                terminal_status = "solidified"
            elif action == "reject_disable":
                existing_idx = next((idx for idx, row in enumerate(custom_rules) if str(row.get("id", "") or "").strip() == rule_id), None)
                if existing_idx is not None:
                    custom_rules[int(existing_idx)]["enabled"] = False
                    custom_rules[int(existing_idx)]["status"] = "rejected"
                    changed_rules = True
                else:
                    disabled_ids.add(rule_id)
                    changed_rules = True
                terminal_status = "rejected"
            elif action == "remove_custom_rule":
                before_len = len(custom_rules)
                custom_rules = [row for row in custom_rules if str(row.get("id", "") or "").strip() != rule_id]
                changed_rules = changed_rules or len(custom_rules) != before_len
                terminal_status = "removed"
            elif action == "revert_change":
                changed_rules = self._apply_snapshot_to_rule_state(
                    rule_id=rule_id,
                    snapshot=current.get("before_rule_snapshot") if isinstance(current.get("before_rule_snapshot"), dict) else {},
                    custom_rules=custom_rules,
                    disabled_ids=disabled_ids,
                    protected_ids=protected_ids,
                ) or changed_rules
                terminal_status = "reverted"
            elif action == "adjust_rule":
                next_snapshot = self._snapshot_rule_state(
                    rule_id=rule_id,
                    custom_rules=custom_rules,
                    disabled_ids=disabled_ids,
                    protected_ids=protected_ids,
                )
                custom_rule_payload = payload.get("custom_rule") if isinstance(payload.get("custom_rule"), dict) else None
                if custom_rule_payload is not None:
                    norm = _normalize_custom_rule(custom_rule_payload)
                    if not norm or str(norm.get("id", "") or "").strip() != rule_id:
                        skipped.append({"observation_id": obs_id, "rule_id": rule_id, "action": action, "reason": "invalid_custom_rule_payload"})
                        remaining.append(current)
                        continue
                    existing_idx = next((idx for idx, row in enumerate(custom_rules) if str(row.get("id", "") or "").strip() == rule_id), None)
                    if existing_idx is None:
                        custom_rules.append(norm)
                    else:
                        custom_rules[int(existing_idx)] = norm
                    changed_rules = True
                if "set_disabled" in payload:
                    should_disable = bool(payload.get("set_disabled"))
                    if should_disable:
                        disabled_ids.add(rule_id)
                    else:
                        disabled_ids.discard(rule_id)
                    changed_rules = True
                if "set_protected" in payload:
                    should_protect = bool(payload.get("set_protected"))
                    if should_protect:
                        protected_ids.add(rule_id)
                    else:
                        protected_ids.discard(rule_id)
                    changed_rules = True
                current["revision"] = int(current.get("revision", 0) or 0) + 1
                current["before_rule_snapshot"] = next_snapshot
                current["current_rule_snapshot"] = self._snapshot_rule_state(
                    rule_id=rule_id,
                    custom_rules=custom_rules,
                    disabled_ids=disabled_ids,
                    protected_ids=protected_ids,
                )
                current["baseline_metric_summary"] = self._metric_summary(
                    rows=recent_rows,
                    metric_key=str(current.get("metric_key", "") or "").strip(),
                )
                current["baseline_focus_summaries"] = {
                    key: self._metric_summary(rows=recent_rows, metric_key=key)
                    for key in [str(x).strip() for x in (current.get("focus_metrics") or []) if str(x).strip()][:6]
                }
                current["source_run_id"] = str(run_id or "")
                current["observed_runs"] = []
                current["status"] = "observing"
                remaining.append(current)
                applied.append({"observation_id": obs_id, "rule_id": rule_id, "action": action, "revision": current["revision"]})
                continue
            else:
                skipped.append({"observation_id": obs_id, "rule_id": rule_id, "action": action, "reason": "unsupported_action"})
                remaining.append(current)
                continue

            closed = dict(current)
            closed["status"] = terminal_status
            closed["resolved_at_ms"] = _now_ms()
            closed["last_review_result"] = {
                "run_id": run_id,
                "action": action,
                "reason": reason,
                "confidence": confidence,
            }
            history_additions.append(closed)
            applied.append({"observation_id": obs_id, "rule_id": rule_id, "action": action, "status": terminal_status})

        if changed_rules:
            save_auto_tuner_rules(
                {
                    "disabled_rule_ids": sorted(disabled_ids),
                    "protected_rule_ids": sorted(protected_ids),
                    "custom_rules": custom_rules,
                }
            )
            self.rules_cfg = load_auto_tuner_rules()
            self.disabled_rule_ids = set(self.rules_cfg.get("disabled_rule_ids", []))
            self.protected_rule_ids = set(self.rules_cfg.get("protected_rule_ids", []))
            self.custom_rules = list(self.rules_cfg.get("custom_rules", []))

        self.rule_observations = remaining[-160:]
        self.observation_history.extend(history_additions)
        self.observation_history = self.observation_history[-400:]

        review_log = {
            "review_id": f"observation_review_{_now_ms()}",
            "run_id": str(run_id or ""),
            "reviewed_at_ms": _now_ms(),
            "summary": str(parsed.get("summary", "") or ""),
            "decisions": applied,
            "skipped": skipped,
            "notes": list(parsed.get("notes", []) or []),
        }
        self.last_observation_review = review_log
        self.observation_review_history.append(review_log)
        self.observation_review_history = self.observation_review_history[-200:]
        self._save_state()
        self._audit({"kind": "llm_observation_review_applied", "run_id": run_id, "review": review_log})
        return {
            "success": True,
            "changed_rules": changed_rules,
            "applied": applied,
            "skipped": skipped,
            "review_log": review_log,
        }

    def _maybe_run_llm_observation_review(self, *, recent_rows: list[dict[str, Any]], run_id: str) -> dict[str, Any]:
        if not bool(self.cfg.llm_auto_validation_enabled):
            return {"triggered": False, "reason": "cfg.llm_auto_validation_enabled=false"}
        llm_cfg = _load_auto_tuner_llm_cfg_from_path(_llm_config_path(), fallback=load_review_config())
        if not bool(llm_cfg.enabled):
            return {"triggered": False, "reason": "llm_disabled"}
        if not bool(self.cfg.llm_auto_validation_review_every_run) and self.last_observation_review:
            last_run_id = str(self.last_observation_review.get("run_id", "") or "")
            if last_run_id and last_run_id == str(run_id or ""):
                return {"triggered": False, "reason": "already_reviewed_this_run"}
        review_items = [dict(item) for item in self.rule_observations if self._observation_is_reviewable(item)]
        if not review_items:
            return {"triggered": False, "reason": "no_reviewable_observations"}
        limit = max(1, int(self.cfg.llm_auto_validation_max_observations_per_review))
        review_items = sorted(
            review_items,
            key=lambda item: (
                -len([x for x in (item.get("observed_runs") or []) if isinstance(x, dict)]),
                -int(item.get("review_count", 0) or 0),
                int(item.get("created_at_ms", 0) or 0),
            ),
        )[:limit]

        cfg = _load_auto_tuner_llm_cfg_from_path(_llm_config_path(), fallback=load_review_config())
        cautious_cfg = LLMReviewConfig(
            enabled=cfg.enabled,
            auto_analyze_on_completion=cfg.auto_analyze_on_completion,
            base_url=cfg.base_url,
            api_key=cfg.api_key,
            model=cfg.model,
            temperature=min(0.15, float(cfg.temperature)),
            max_prompt_chars=cfg.max_prompt_chars,
            timeout_sec=cfg.timeout_sec,
            max_completion_tokens=cfg.max_completion_tokens,
        )
        system_prompt = (
            "你是 AP 原型系统中的谨慎型自动验收员。"
            "你的首要原则是：证据不足时不要激进修改；"
            "优先保证可解释、可回滚、符合理论核心的稳态改进。"
        )
        user_prompt = self._build_llm_observation_prompt(review_items=review_items, recent_rows=recent_rows, run_id=run_id)
        if len(user_prompt) > int(cautious_cfg.max_prompt_chars):
            user_prompt = user_prompt[: int(cautious_cfg.max_prompt_chars)] + "\n\n[TRUNCATED]\n"
        res = call_openai_chat_completions(config=cautious_cfg, system_prompt=system_prompt, user_prompt=user_prompt)
        if not res.get("success", False):
            self._audit(
                {
                    "kind": "llm_observation_review_failed",
                    "run_id": run_id,
                    "error": res.get("error", ""),
                    "message": res.get("message", ""),
                    "reviewable_count": len(review_items),
                }
            )
            return {"triggered": True, "success": False, "error": res.get("error", ""), "message": res.get("message", "")}
        payload = res.get("data", {})
        text = _extract_chat_text(payload if isinstance(payload, dict) else {})
        parsed = _normalize_llm_observation_review_payload(_extract_json_code_block(text))
        apply_res = self._apply_llm_observation_review_result(run_id=run_id, recent_rows=recent_rows, review_payload=parsed)
        report_excerpt = str(text or "")[:2400]
        apply_res["report_text"] = text
        if isinstance(self.last_observation_review, dict):
            self.last_observation_review["report_excerpt"] = report_excerpt
        if self.observation_review_history and isinstance(self.observation_review_history[-1], dict):
            self.observation_review_history[-1]["report_excerpt"] = report_excerpt
        self._save_state()
        apply_res["reviewed_observation_count"] = len(review_items)
        return {"triggered": True, "success": True, "result": apply_res}

    # --------------------------
    # Decision loop
    # --------------------------

    def on_tick(self, *, metrics: dict[str, Any]) -> dict[str, Any]:
        """
        Decide and apply a short-term tuning step based on recent window metrics.
        """
        if not self.enabled or not self.enable_short_term:
            return {"enabled": bool(self.enabled), "applied": False}

        tick_index = int(_safe_float(metrics.get("tick_index", 0), 0.0))
        if tick_index - self.last_decision_tick < int(self.cfg.decision_cooldown_ticks):
            self.history.append(metrics)
            return {"enabled": True, "applied": False, "reason": "decision_cooldown"}

        self.history.append(metrics)
        window_n = int(self.cfg.short_window_ticks)
        recent = self.history[-window_n:]
        self._evaluate_pending_trials(current_tick=int(tick_index), recent_rows=recent)

        # Avoid overreacting at the very beginning of a run.
        # We require a few samples before short-term tuning kicks in.
        min_samples = min(window_n, 5)
        if len(self.history) < min_samples:
            return {"enabled": True, "applied": False, "reason": f"warmup<{min_samples}"}

        decisions: list[dict[str, Any]] = []
        endogenous_balance = self._endogenous_balance_snapshot(rows=recent)
        cs_snapshot = self._cognitive_stitching_snapshot(rows=recent)
        ev_balance = self._ev_balance_snapshot(rows=recent)

        if endogenous_balance.get("needs_recovery", False):
            decisions.extend(self._decide_endogenous_recovery(balance=endogenous_balance, long_term=False))
        decisions.extend(self._decide_structure_supply_nudges(balance=endogenous_balance, long_term=False))
        decisions.extend(self._decide_attention_overheat_nudges(balance=endogenous_balance, long_term=False))
        decisions.extend(self._decide_ev_balance_nudges(snapshot=ev_balance, long_term=False))
        decisions.extend(self._decide_cognitive_stitching_nudges(snapshot=cs_snapshot, long_term=False))

        # 1) Resource runaway guard (fast path): internal_resolution_raw_unit_count and timing_total_logic_ms
        timing_vals = _recent_window(recent, "timing_total_logic_ms", window_n)
        raw_vals = _recent_window(recent, "internal_resolution_raw_unit_count", window_n)
        timing_mean = _mean(timing_vals)
        raw_mean = _mean(raw_vals)
        timing_max = max(timing_vals) if timing_vals else 0.0
        raw_max = max(raw_vals) if raw_vals else 0.0

        if timing_max > self.metric_targets["timing_total_logic_ms"].expected_max or raw_max > self.metric_targets["internal_resolution_raw_unit_count"].expected_max:
            # Tighten HDB fuses slightly (small steps; multiple rounds).
            decisions.extend(self._decide_hdb_fuse_nudge(timing_max=timing_max, raw_max=raw_max))

        # 2) Feelings: keep the normal band mostly below half-range,
        # while still allowing occasional strong peaks for real events.
        dis_vals = _recent_window(recent, "cfs_dissonance_max", window_n)
        dis_mean = _mean(dis_vals)
        dis_std = _std(dis_vals)
        dis_issue = self._metric_issue_snapshot(rows=recent, metric_key="cfs_dissonance_max") or {}
        dis_band = dict(dis_issue.get("band", {})) if isinstance(dis_issue.get("band", {}), dict) else {}
        dis_high_band_rate = _safe_float(dis_band.get("occupancy_ratio", 0.0), 0.0)
        dis_high_band_limit = _safe_float(dis_band.get("max_ratio", 0.0), 0.0)

        # Dissonance too often in the strong band or too high on average => tighten.
        if dis_mean > self.metric_targets["cfs_dissonance_max"].expected_max or (
            dis_high_band_limit > 0.0 and dis_high_band_rate > dis_high_band_limit
        ):
            decisions.extend(
                self._decide_dissonance_tighten(
                    dis_rate=dis_high_band_rate,
                    dis_mean=dis_mean,
                    dis_std=dis_std,
                )
            )

        # Pressure too high but dissonance not high => likely punish_signal leak / EV threshold too low.
        pressure_vals = _recent_window(recent, "cfs_pressure_max", window_n)
        pressure_mean = _mean(pressure_vals)
        pressure_issue = self._metric_issue_snapshot(rows=recent, metric_key="cfs_pressure_max") or {}
        pressure_band = dict(pressure_issue.get("band", {})) if isinstance(pressure_issue.get("band", {}), dict) else {}
        pressure_high_band_rate = _safe_float(pressure_band.get("occupancy_ratio", 0.0), 0.0)
        pressure_high_band_limit = _safe_float(pressure_band.get("max_ratio", 0.0), 0.0)
        if (
            pressure_mean > self.metric_targets["cfs_pressure_max"].expected_max
            or (pressure_high_band_limit > 0.0 and pressure_high_band_rate > pressure_high_band_limit)
        ) and dis_mean <= self.metric_targets["cfs_dissonance_max"].expected_max:
            decisions.extend(self._decide_pressure_tighten())

        # Expectation too often above the strong band => tighten reward/expectation gating.
        expect_vals = _recent_window(recent, "cfs_expectation_max", window_n)
        expect_mean = _mean(expect_vals)
        expect_issue = self._metric_issue_snapshot(rows=recent, metric_key="cfs_expectation_max") or {}
        expect_band = dict(expect_issue.get("band", {})) if isinstance(expect_issue.get("band", {}), dict) else {}
        expect_high_band_rate = _safe_float(expect_band.get("occupancy_ratio", 0.0), 0.0)
        expect_high_band_limit = _safe_float(expect_band.get("max_ratio", 0.0), 0.0)
        if expect_mean > self.metric_targets["cfs_expectation_max"].expected_max or (
            expect_high_band_limit > 0.0 and expect_high_band_rate > expect_high_band_limit
        ):
            decisions.extend(self._decide_expectation_tighten())

        # Action recall rate too high => tighten time-recall trigger (drive threshold/gain) first.
        recall_vals = _recent_window(recent, "action_executed_recall", window_n)
        recall_rate = _mean([1.0 if v > 0.0 else 0.0 for v in recall_vals]) if recall_vals else 0.0
        if recall_rate > 0.25:
            decisions.extend(self._decide_recall_tighten())

        decisions.extend(self._decide_custom_rule_nudges(recent=recent, long_term=False))

        # 3) Generic catalog-driven nudges:
        # use impacts/tags to synthesize targeted rules for the current environment.
        decisions.extend(self._decide_catalog_nudges(recent=recent, long_term=False))
        decisions = self._filter_endogenous_conflicts(decisions=decisions, balance=endogenous_balance)

        # No decisions
        if not decisions:
            return {"enabled": True, "applied": False}

        # Apply at most N updates per tick (avoid thrash).
        # Also apply a per-param cooldown (avoid "hammering" the same knob repeatedly).
        param_cooldown = max(4, int(self.cfg.decision_cooldown_ticks) * 2 + 2)
        filtered: list[dict[str, Any]] = []
        seen_this_tick: set[str] = set()
        for d in decisions:
            pid = self._canonicalize_param_id(str(d.get("param", "") or "").strip())
            if not pid:
                continue
            if pid in seen_this_tick:
                continue
            last_t = int(self.last_param_tick.get(pid, -10_000_000))
            cooldown_eff = self._backoff_cooldown_ticks(pid=pid, base=param_cooldown)
            if int(tick_index) - last_t < cooldown_eff:
                continue
            d["param"] = pid
            filtered.append(d)
            seen_this_tick.add(pid)
        decisions = filtered[: int(self.cfg.max_param_updates_per_tick)]

        applied: list[dict[str, Any]] = []
        for d in decisions:
            try:
                d["tick_index"] = int(tick_index)
                ok = self._apply_param_update(d, persist=False)
                if ok:
                    self._record_rule_hit(str(d.get("rule_id", "") or "manual"))
                    self._register_trial(update=d, persist=False, current_tick=int(tick_index), recent_rows=recent)
                    applied.append(d)
            except Exception as exc:
                self._audit({"ts_ms": int(time.time() * 1000), "kind": "apply_error", "error": str(exc), "update": d})

        if applied:
            for d in applied:
                pid = self._canonicalize_param_id(str(d.get("param", "") or "").strip())
                if pid:
                    self.last_param_tick[pid] = int(tick_index)
            self.last_decision_tick = int(tick_index)
            self._audit(
                {
                    "ts_ms": int(time.time() * 1000),
                    "kind": "short_term_update",
                    "tick_index": int(tick_index),
                    "window": {"n": window_n, "timing_mean": timing_mean, "timing_max": timing_max, "raw_mean": raw_mean, "raw_max": raw_max},
                    "endogenous_balance": endogenous_balance,
                    "applied": applied,
                }
            )
            return {"enabled": True, "applied": True, "applied_count": len(applied), "applied_updates": applied}

        return {"enabled": True, "applied": False, "reason": "all_updates_rejected"}

    def on_run_complete(self, *, all_metrics: Iterable[dict[str, Any]], trace_id: str) -> dict[str, Any]:
        """
        Long-term mode: after run completion, nudge parameters toward ideal values (very small steps).
        """
        if not self.enabled:
            return {"enabled": False, "applied": False}

        rows = [m for m in all_metrics if isinstance(m, dict)]
        if not rows:
            return {"enabled": True, "applied": False, "reason": "no_metrics"}
        run_id = str(self.run_dir.name or "")

        n = max(10, int(self.cfg.long_window_ticks))
        recent = rows[-n:]
        trial_eval = self._evaluate_persisted_trials(recent_rows=recent, trace_id=trace_id)
        observation_record = self._record_observation_run_results(recent_rows=recent, run_id=run_id, trace_id=trace_id)
        endogenous_balance = self._endogenous_balance_snapshot(rows=recent)
        cs_snapshot = self._cognitive_stitching_snapshot(rows=recent)
        ev_balance = self._ev_balance_snapshot(rows=recent)

        if not self.enable_long_term:
            llm_loop = self._maybe_run_llm_auto_loop(recent_rows=recent, run_id=run_id)
            observation_review = self._maybe_run_llm_observation_review(recent_rows=recent, run_id=run_id)
            return {
                "enabled": True,
                "applied": False,
                "reason": "long_term_disabled",
                "evaluated_trials": trial_eval,
                "observation_record": observation_record,
                "llm_auto_loop": llm_loop,
                "observation_review": observation_review,
            }

        # Example long-term: if mean timing is significantly above ideal, tighten HDB fuses very slightly.
        timing_vals = _recent_window(recent, "timing_total_logic_ms", n)
        raw_vals = _recent_window(recent, "internal_resolution_raw_unit_count", n)
        timing_mean = _mean(timing_vals)
        raw_mean = _mean(raw_vals)

        updates: list[dict[str, Any]] = []
        if endogenous_balance.get("needs_recovery", False):
            updates.extend(self._decide_endogenous_recovery(balance=endogenous_balance, long_term=True))
        updates.extend(self._decide_structure_supply_nudges(balance=endogenous_balance, long_term=True))
        updates.extend(self._decide_attention_overheat_nudges(balance=endogenous_balance, long_term=True))
        updates.extend(self._decide_ev_balance_nudges(snapshot=ev_balance, long_term=True))
        updates.extend(self._decide_cognitive_stitching_nudges(snapshot=cs_snapshot, long_term=True))
        if timing_mean > self.metric_targets["timing_total_logic_ms"].ideal * 1.25 or raw_mean > self.metric_targets["internal_resolution_raw_unit_count"].ideal * 1.25:
            # a smaller step than short-term
            updates.extend(self._decide_hdb_fuse_nudge(timing_max=timing_mean, raw_max=raw_mean, long_term=True))

        # If dissonance is near-zero and too flat, relax slightly (avoid "perfect straight line").
        dis_vals = _recent_window(recent, "cfs_dissonance_max", n)
        dis_mean = _mean(dis_vals)
        dis_std = _std(dis_vals)
        if dis_mean < 0.05 and dis_std < self.metric_targets["cfs_dissonance_max"].min_std:
            rule_id = "builtin.long_term.dissonance.relax_flatline"
            if self._rule_enabled(rule_id):
                updates.append({"rule_id": rule_id, "param": "iesm.cfs_dissonance.threshold", "delta": -0.02, "reason": "avoid_flatline", "metric_key": "cfs_dissonance_max", "issue_mode": "flatline"})

        # Generic long-term catalog rules: much more conservative than short-term.
        updates.extend(self._decide_custom_rule_nudges(recent=recent, long_term=True))
        updates.extend(self._decide_catalog_nudges(recent=recent, long_term=True))
        updates = self._filter_endogenous_conflicts(decisions=updates, balance=endogenous_balance)

        if not updates:
            return {"enabled": True, "applied": False}

        # Canonicalize + keep the first update per param for this long-term pass.
        deduped: list[dict[str, Any]] = []
        seen_long: set[str] = set()
        for upd in updates:
            pid = self._canonicalize_param_id(str(upd.get("param", "") or "").strip())
            if not pid or pid in seen_long:
                continue
            upd["param"] = pid
            seen_long.add(pid)
            deduped.append(upd)
        updates = deduped

        applied: list[dict[str, Any]] = []
        rollback_point: dict[str, Any] | None = None
        if updates:
            rollback_point = self._create_rollback_point(
                reason="long_term_before_update",
                summary={"trace_id": trace_id, "timing_mean": timing_mean, "raw_mean": raw_mean, "evaluated_trials": trial_eval},
            )
        for upd in updates[: int(self.cfg.max_param_updates_per_tick)]:
            try:
                upd["tick_index"] = int(rows[-1].get("tick_index", 0) or 0) if rows else -1
                ok = self._apply_param_update(upd, persist=True)
                if ok:
                    self._record_rule_hit(str(upd.get("rule_id", "") or "manual"))
                    self._register_trial(update=upd, persist=True, current_tick=None, recent_rows=recent)
                    applied.append(upd)
            except Exception:
                continue

        if applied:
            # Materialize persisted overrides so the next run starts closer to the ideal envelope.
            try:
                self._apply_persisted_overrides_to_persist_files()
            except Exception:
                pass
            self._save_state()
            self._audit(
                {
                    "ts_ms": int(time.time() * 1000),
                    "kind": "long_term_update",
                    "window": {"n": n, "timing_mean": timing_mean, "raw_mean": raw_mean, "dissonance_mean": dis_mean, "dissonance_std": dis_std},
                    "endogenous_balance": endogenous_balance,
                    "applied": applied,
                    "rollback_point": rollback_point,
                    "evaluated_trials": trial_eval,
                }
            )
            # Apply to runtime too (best-effort).
            try:
                self._apply_overrides_to_runtime(trace_id=trace_id)
            except Exception:
                pass
            llm_loop = self._maybe_run_llm_auto_loop(recent_rows=recent, run_id=run_id)
            observation_review = self._maybe_run_llm_observation_review(recent_rows=recent, run_id=run_id)
            return {
                "enabled": True,
                "applied": True,
                "applied_count": len(applied),
                "rollback_point": rollback_point,
                "evaluated_trials": trial_eval,
                "observation_record": observation_record,
                "llm_auto_loop": llm_loop,
                "observation_review": observation_review,
            }

        llm_loop = self._maybe_run_llm_auto_loop(recent_rows=recent, run_id=run_id)
        observation_review = self._maybe_run_llm_observation_review(recent_rows=recent, run_id=run_id)
        return {
            "enabled": True,
            "applied": False,
            "evaluated_trials": trial_eval,
            "observation_record": observation_record,
            "llm_auto_loop": llm_loop,
            "observation_review": observation_review,
        }

    # --------------------------
    # Heuristics (decisions)
    # --------------------------

    def _decide_hdb_fuse_nudge(self, *, timing_max: float, raw_max: float, long_term: bool = False) -> list[dict[str, Any]]:
        step_scale = 0.5 if long_term else 1.0
        updates: list[dict[str, Any]] = []
        # Lower flat cap if raw spikes; lower max structures if timing spikes.
        if raw_max > self.metric_targets["internal_resolution_raw_unit_count"].expected_max:
            rule_id = "builtin.resource.hdb_fuse.raw_spike"
            if self._rule_enabled(rule_id):
                updates.append(
                    {
                        "rule_id": rule_id,
                        "param": "hdb.internal_resolution_flat_unit_cap_per_structure",
                        "delta": -float(self.param_bounds["hdb.internal_resolution_flat_unit_cap_per_structure"].max_step_abs) * step_scale,
                        "reason": "raw_unit_spike",
                        "metric_key": "internal_resolution_raw_unit_count",
                        "issue_mode": "high",
                    }
                )
        if timing_max > self.metric_targets["timing_total_logic_ms"].expected_max:
            rule_id = "builtin.resource.hdb_fuse.timing_spike"
            if self._rule_enabled(rule_id):
                updates.append(
                    {
                        "rule_id": rule_id,
                        "param": "hdb.internal_resolution_max_structures_per_tick",
                        "delta": -float(self.param_bounds["hdb.internal_resolution_max_structures_per_tick"].max_step_abs) * step_scale,
                        "reason": "timing_spike",
                        "metric_key": "timing_total_logic_ms",
                        "issue_mode": "high",
                    }
                )
        return updates

    def _decide_dissonance_tighten(self, *, dis_rate: float, dis_mean: float, dis_std: float) -> list[dict[str, Any]]:
        updates: list[dict[str, Any]] = []
        # Tighten threshold first; only if it's already high do we raise cooldown.
        rule_id = "builtin.cfs.dissonance.tighten"
        if not self._rule_enabled(rule_id):
            return updates
        updates.append({"rule_id": rule_id, "param": "iesm.cfs_dissonance.threshold", "delta": +0.03, "reason": f"dissonance_high rate={dis_rate:.2f} mean={dis_mean:.3f}", "metric_key": "cfs_dissonance_max", "issue_mode": "high"})
        if dis_std > 0.15:
            updates.append({"rule_id": rule_id, "param": "iesm.cfs_dissonance.cooldown_ticks", "delta": +1.0, "reason": "dissonance_spiky", "metric_key": "cfs_dissonance_max", "issue_mode": "high"})
        updates.append({"rule_id": rule_id, "param": "iesm.cfs_dissonance.max_signals", "delta": -1.0, "reason": "reduce_dissonance_spread", "metric_key": "cfs_dissonance_max", "issue_mode": "high"})
        # Also reduce punish_signal value slightly to keep pressure channel from saturating.
        updates.append({"rule_id": rule_id, "param": "iesm.cfs_punish_signal.value", "delta": -0.05, "reason": "punish_signal_sparse", "metric_key": "cfs_pressure_max", "issue_mode": "high"})
        return updates

    def _decide_pressure_tighten(self) -> list[dict[str, Any]]:
        rule_id = "builtin.cfs.pressure.tighten"
        if not self._rule_enabled(rule_id):
            return []
        return [
            {"rule_id": rule_id, "param": "iesm.cfs_pressure.ev_threshold", "delta": +0.03, "reason": "pressure_high_without_dissonance", "metric_key": "cfs_pressure_max", "issue_mode": "high"},
            {"rule_id": rule_id, "param": "iesm.cfs_punish_signal.value", "delta": -0.05, "reason": "punish_signal_overhang", "metric_key": "cfs_pressure_max", "issue_mode": "high"},
        ]

    def _decide_expectation_tighten(self) -> list[dict[str, Any]]:
        rule_id = "builtin.cfs.expectation.tighten"
        if not self._rule_enabled(rule_id):
            return []
        return [
            {"rule_id": rule_id, "param": "iesm.cfs_expectation.ev_threshold", "delta": +0.03, "reason": "expectation_high", "metric_key": "cfs_expectation_max", "issue_mode": "high"},
            {"rule_id": rule_id, "param": "iesm.cfs_reward_signal.value", "delta": -0.05, "reason": "reward_signal_overhang", "metric_key": "cfs_expectation_max", "issue_mode": "high"},
            {"rule_id": rule_id, "param": "iesm.cfs_correct_event.cooldown_ticks", "delta": +2.0, "reason": "correct_event_too_frequent", "metric_key": "cfs_expectation_max", "issue_mode": "high"},
        ]

    def _decide_recall_tighten(self) -> list[dict[str, Any]]:
        rule_id = "builtin.action.recall.tighten"
        if not self._rule_enabled(rule_id):
            return []
        return [
            {"rule_id": rule_id, "param": "iesm.action_recall_time.threshold", "delta": +0.08, "reason": "recall_too_frequent", "metric_key": "action_executed_recall", "issue_mode": "high"},
            {"rule_id": rule_id, "param": "iesm.action_recall_time.gain", "delta": -0.05, "reason": "recall_too_frequent", "metric_key": "action_drive_max", "issue_mode": "high"},
            {"rule_id": rule_id, "param": "iesm.action_recall_time.bucket_energy_threshold", "delta": +0.03, "reason": "recall_too_frequent", "metric_key": "time_sensor_bucket_energy_sum", "issue_mode": "high"},
        ]

    def _cognitive_stitching_snapshot(self, *, rows: list[dict[str, Any]]) -> dict[str, Any]:
        if not rows:
            return {}

        n = len(rows)
        candidate_vals = _recent_window(rows, "cs_candidate_count", n)
        action_vals = _recent_window(rows, "cs_action_count", n)
        created_vals = _recent_window(rows, "cs_created_count", n)
        extended_vals = _recent_window(rows, "cs_extended_count", n)
        merged_vals = _recent_window(rows, "cs_merged_count", n)
        reinforced_vals = _recent_window(rows, "cs_reinforced_count", n)
        stimulus_new_structure_vals = _recent_window(rows, "stimulus_new_structure_count", n)
        timing_vals = _recent_window(rows, "timing_cognitive_stitching_ms", n)

        mean_candidates = _mean(candidate_vals)
        mean_actions = _mean(action_vals)
        mean_created = _mean(created_vals)
        mean_extended = _mean(extended_vals)
        mean_merged = _mean(merged_vals)
        mean_reinforced = _mean(reinforced_vals)
        mean_stimulus_new_structures = _mean(stimulus_new_structure_vals)
        mean_timing = _mean(timing_vals)

        latest_candidates = candidate_vals[-1] if candidate_vals else 0.0
        latest_actions = action_vals[-1] if action_vals else 0.0
        latest_stimulus_new_structures = stimulus_new_structure_vals[-1] if stimulus_new_structure_vals else 0.0

        candidate_to_action_ratio = 0.0
        if mean_candidates > 1e-6:
            candidate_to_action_ratio = mean_actions / mean_candidates

        output_total = mean_created + mean_extended + mean_merged + mean_reinforced
        candidate_rich_but_action_starved = bool(mean_candidates >= 0.8 and mean_actions <= 0.05)
        upstream_structure_alive = bool(mean_stimulus_new_structures >= 0.12 or latest_stimulus_new_structures > 0.0)
        timing_hot = bool(mean_timing > float(self.metric_targets.get("timing_cognitive_stitching_ms", MetricTarget("",0,0,0,0,0)).expected_max or 1200.0) * 0.85)

        return {
            "mean_candidates": mean_candidates,
            "latest_candidates": latest_candidates,
            "mean_actions": mean_actions,
            "latest_actions": latest_actions,
            "mean_created": mean_created,
            "mean_extended": mean_extended,
            "mean_merged": mean_merged,
            "mean_reinforced": mean_reinforced,
            "mean_stimulus_new_structures": mean_stimulus_new_structures,
            "latest_stimulus_new_structures": latest_stimulus_new_structures,
            "mean_timing": mean_timing,
            "candidate_to_action_ratio": candidate_to_action_ratio,
            "candidate_rich_but_action_starved": candidate_rich_but_action_starved,
            "upstream_structure_alive": upstream_structure_alive,
            "timing_hot": timing_hot,
            "output_total": output_total,
        }

    def _decide_cognitive_stitching_nudges(self, *, snapshot: dict[str, Any], long_term: bool = False) -> list[dict[str, Any]]:
        if not isinstance(snapshot, dict) or not snapshot:
            return []

        updates: list[dict[str, Any]] = []
        step_scale = 0.45 if long_term else 1.0

        mean_candidates = float(snapshot.get("mean_candidates", 0.0) or 0.0)
        mean_actions = float(snapshot.get("mean_actions", 0.0) or 0.0)
        mean_timing = float(snapshot.get("mean_timing", 0.0) or 0.0)
        candidate_to_action_ratio = float(snapshot.get("candidate_to_action_ratio", 0.0) or 0.0)
        candidate_rich_but_action_starved = bool(snapshot.get("candidate_rich_but_action_starved", False))
        upstream_structure_alive = bool(snapshot.get("upstream_structure_alive", False))
        timing_hot = bool(snapshot.get("timing_hot", False))

        def _append(rule_id: str, param: str, direction: float, reason: str, metric_key: str, issue_mode: str, weight: float = 1.0) -> None:
            pid = self._canonicalize_param_id(param)
            if pid not in self.param_bounds:
                return
            if not self._rule_enabled(rule_id):
                return
            bound = self.param_bounds[pid]
            delta = float(direction) * float(bound.max_step_abs) * step_scale * max(0.35, min(1.0, weight))
            if abs(delta) < 1e-12:
                return
            updates.append(
                {
                    "rule_id": rule_id,
                    "param": pid,
                    "delta": delta,
                    "reason": reason,
                    "metric_key": metric_key,
                    "issue_mode": issue_mode,
                }
            )

        if candidate_rich_but_action_starved and upstream_structure_alive and not timing_hot:
            _append(
                "builtin.cs.open_gate.thresholds",
                "cognitive_stitching.min_candidate_score",
                -1.0,
                f"cs_candidates_present_but_actions_flat mean_candidates={mean_candidates:.3f} mean_actions={mean_actions:.3f}",
                "cs_action_count",
                "low",
                1.0,
            )
            _append(
                "builtin.cs.open_gate.seed_energy",
                "cognitive_stitching.min_seed_total_energy",
                -1.0,
                "cs_seed_gate_may_be_too_strict",
                "cs_action_count",
                "low",
                0.8,
            )
            _append(
                "builtin.cs.open_gate.event_energy",
                "cognitive_stitching.min_event_total_energy",
                -1.0,
                "cs_event_gate_may_be_too_strict",
                "cs_action_count",
                "low",
                0.7,
            )
            _append(
                "builtin.cs.open_gate.event_grasp",
                "cognitive_stitching.event_grasp_min_total_energy",
                -1.0,
                "event_grasp_gate_may_be_too_strict_for_string_mode_bootstrap",
                "cs_action_count",
                "low",
                0.55,
            )

        if mean_candidates >= 6.0 and mean_actions >= 2.5:
            _append(
                "builtin.cs.tighten.thresholds",
                "cognitive_stitching.min_candidate_score",
                +1.0,
                f"cs_actions_too_dense mean_candidates={mean_candidates:.3f} mean_actions={mean_actions:.3f}",
                "cs_action_count",
                "high",
                0.9,
            )
            _append(
                "builtin.cs.tighten.anchor_penalty",
                "cognitive_stitching.anchor_distance_penalty",
                +1.0,
                "cs_branching_too_dense_raise_distance_penalty",
                "cs_candidate_count",
                "high",
                0.6,
            )

        if mean_candidates >= 3.0 and candidate_to_action_ratio < 0.12 and upstream_structure_alive and not timing_hot:
            _append(
                "builtin.cs.rebalance.match_strength",
                "cognitive_stitching.match_strength_weight",
                +1.0,
                "cs_candidates_exist_but_conversion_low_raise_match_strength_weight",
                "cs_action_count",
                "low",
                0.55,
            )
            _append(
                "builtin.cs.rebalance.anchor_penalty",
                "cognitive_stitching.anchor_distance_penalty",
                -1.0,
                "cs_candidates_exist_but_conversion_low_reduce_anchor_distance_penalty",
                "cs_action_count",
                "low",
                0.45,
            )

        if timing_hot or mean_timing >= 900.0:
            _append(
                "builtin.cs.performance.tighten_context",
                "cognitive_stitching.context_support_weight",
                -1.0,
                f"cs_timing_hot mean_timing={mean_timing:.3f}",
                "timing_cognitive_stitching_ms",
                "high",
                0.7,
            )
            _append(
                "builtin.cs.performance.tighten_threshold",
                "cognitive_stitching.min_candidate_score",
                +1.0,
                "cs_timing_hot_reduce_candidate_fanout",
                "timing_cognitive_stitching_ms",
                "high",
                0.7,
            )

        return updates

    def _decide_structure_supply_nudges(self, *, balance: dict[str, Any], long_term: bool = False) -> list[dict[str, Any]]:
        if not isinstance(balance, dict) or not balance:
            return []
        updates: list[dict[str, Any]] = []
        step_scale = 0.5 if long_term else 1.0

        mean_cam_items = float(balance.get("mean_cam_items", 0.0) or 0.0)
        mean_selected_structures = float(balance.get("mean_selected_structures", 0.0) or 0.0)
        latest_selected_structures = float(balance.get("latest_selected_structures", 0.0) or 0.0)
        mean_pool_items = float(balance.get("mean_pool_items", 0.0) or 0.0)
        budget_not_binding = bool(
            (float(balance.get("latest_raw_units", 0.0) or 0.0) > 0.0 and float(balance.get("latest_selected_units", 0.0) or 0.0) >= float(balance.get("latest_raw_units", 0.0) or 0.0) * 0.98)
            or (float(balance.get("latest_detail_budget", 0.0) or 0.0) > 0.0 and float(balance.get("latest_selected_units", 0.0) or 0.0) <= float(balance.get("latest_detail_budget", 0.0) or 0.0) * 0.75)
        )

        def _append(rule_id: str, param: str, direction: float, reason: str, metric_key: str, issue_mode: str, weight: float = 1.0) -> None:
            pid = self._canonicalize_param_id(param)
            if pid not in self.param_bounds:
                return
            if not self._rule_enabled(rule_id):
                return
            bound = self.param_bounds[pid]
            delta = float(direction) * float(bound.max_step_abs) * step_scale * max(0.35, min(1.0, weight))
            if abs(delta) < 1e-12:
                return
            updates.append({
                "rule_id": rule_id,
                "param": pid,
                "delta": delta,
                "reason": reason,
                "metric_key": metric_key,
                "issue_mode": issue_mode,
            })

        if mean_cam_items < 3.0 and mean_pool_items >= 80.0:
            _append(
                "builtin.structure_supply.raise_cam_cap",
                "attention.max_cam_items",
                +1.0,
                f"cam_items_too_low mean_cam_items={mean_cam_items:.3f}",
                "cam_item_count",
                "low",
                0.8,
            )
            _append(
                "builtin.structure_supply.raise_cam_floor",
                "attention.min_cam_items",
                +1.0,
                "cam_floor_too_low_for_structure_supply",
                "cam_item_count",
                "low",
                0.45,
            )

        if mean_selected_structures < 3.0 and latest_selected_structures < 3.0:
            _append(
                "builtin.structure_supply.raise_structure_cap",
                "hdb.internal_resolution_max_structures_per_tick",
                +1.0,
                f"selected_structures_too_low mean={mean_selected_structures:.3f}",
                "internal_resolution_structure_count_selected",
                "low",
                0.75,
            )
            if budget_not_binding:
                _append(
                    "builtin.structure_supply.keep_pool_alive",
                    "state_pool.default_er_decay_ratio",
                    +1.0,
                    "selected_structures_low_prefer_retention_first",
                    "internal_resolution_structure_count_selected",
                    "low",
                    0.45,
                )

        return updates

    def _ev_balance_snapshot(self, *, rows: list[dict[str, Any]]) -> dict[str, Any]:
        if not rows:
            return {}
        n = len(rows)
        er_vals = _recent_window(rows, "pool_total_er", n)
        ev_vals = _recent_window(rows, "pool_total_ev", n)
        if not er_vals and not ev_vals:
            return {}
        mean_er = _mean(er_vals)
        mean_ev = _mean(ev_vals)
        latest_er = er_vals[-1] if er_vals else 0.0
        latest_ev = ev_vals[-1] if ev_vals else 0.0
        mean_ratio = (mean_ev / mean_er) if mean_er > 1e-6 else 0.0
        latest_ratio = (latest_ev / latest_er) if latest_er > 1e-6 else 0.0
        severely_ev_starved = bool(mean_er >= 20.0 and mean_ratio < 0.85)
        return {
            "mean_er": mean_er,
            "mean_ev": mean_ev,
            "latest_er": latest_er,
            "latest_ev": latest_ev,
            "mean_ev_to_er_ratio": mean_ratio,
            "latest_ev_to_er_ratio": latest_ratio,
            "severely_ev_starved": severely_ev_starved,
        }

    def _decide_ev_balance_nudges(self, *, snapshot: dict[str, Any], long_term: bool = False) -> list[dict[str, Any]]:
        if not isinstance(snapshot, dict) or not snapshot:
            return []
        if not bool(snapshot.get("severely_ev_starved", False)):
            return []
        step_scale = 0.45 if long_term else 1.0
        updates: list[dict[str, Any]] = []

        def _append(rule_id: str, param: str, direction: float, reason: str, weight: float = 1.0) -> None:
            pid = self._canonicalize_param_id(param)
            if pid not in self.param_bounds:
                return
            if not self._rule_enabled(rule_id):
                return
            bound = self.param_bounds[pid]
            delta = float(direction) * float(bound.max_step_abs) * step_scale * max(0.35, min(1.0, weight))
            if abs(delta) < 1e-12:
                return
            updates.append({
                "rule_id": rule_id,
                "param": pid,
                "delta": delta,
                "reason": reason,
                "metric_key": "pool_total_ev",
                "issue_mode": "low",
            })

        mean_ratio = float(snapshot.get("mean_ev_to_er_ratio", 0.0) or 0.0)
        _append(
            "builtin.ev_balance.raise_induction",
            "emotion.subjective_modulators.er_induction_ratio.base",
            +1.0,
            f"ev_starved raise_er_induction_ratio mean_ev_to_er_ratio={mean_ratio:.3f}",
            0.9,
        )
        _append(
            "builtin.ev_balance.raise_propagation",
            "emotion.subjective_modulators.ev_propagation_ratio.base",
            +1.0,
            "ev_starved raise_ev_propagation_ratio",
            1.0,
        )
        _append(
            "builtin.ev_balance.slow_ev_decay",
            "state_pool.default_ev_decay_ratio",
            +1.0,
            "ev_starved slow_default_ev_decay",
            0.55,
        )
        if float(snapshot.get("mean_er", 0.0) or 0.0) > 180.0:
            _append(
                "builtin.ev_balance.soften_er_retention",
                "state_pool.default_er_decay_ratio",
                -1.0,
                "ev_starved_and_er_overfull soften_default_er_decay",
                0.35,
            )
        return updates

    def _decide_attention_overheat_nudges(self, *, balance: dict[str, Any], long_term: bool = False) -> list[dict[str, Any]]:
        if not isinstance(balance, dict) or not balance:
            return []
        mean_cam_items = float(balance.get("mean_cam_items", 0.0) or 0.0)
        if mean_cam_items <= 16.0:
            return []
        step_scale = 0.5 if long_term else 1.0
        updates: list[dict[str, Any]] = []
        pid = self._canonicalize_param_id("attention.max_cam_items")
        if pid in self.param_bounds and self._rule_enabled("builtin.attention.tighten.cam_cap"):
            bound = self.param_bounds[pid]
            updates.append({
                "rule_id": "builtin.attention.tighten.cam_cap",
                "param": pid,
                "delta": -float(bound.max_step_abs) * step_scale * 0.8,
                "reason": f"cam_overheat mean_cam_items={mean_cam_items:.3f}",
                "metric_key": "cam_item_count",
                "issue_mode": "high",
            })
        return updates

    def _decide_custom_rule_nudges(self, *, recent: list[dict[str, Any]], long_term: bool = False) -> list[dict[str, Any]]:
        updates: list[dict[str, Any]] = []
        if not recent:
            return updates
        for rule in self.custom_rules:
            if not isinstance(rule, dict) or not bool(rule.get("enabled", True)):
                continue
            rule_id = str(rule.get("id", "") or "").strip()
            if not rule_id or not self._rule_enabled(rule_id):
                continue
            metric_key = str(rule.get("metric_key", "") or "").strip()
            issue_mode = str(rule.get("issue_mode", "") or "").strip()
            issue = self._metric_issue_snapshot(rows=recent, metric_key=metric_key)
            if not issue:
                continue
            severity = float(issue.get("high_ratio", 0.0) if issue_mode == "high" else issue.get("low_ratio", 0.0))
            if issue_mode == "flatline":
                severity = 1.0 if bool(issue.get("flatline", False)) else 0.0
            if severity < float(rule.get("min_severity", 0.05)):
                continue
            pid = self._canonicalize_param_id(str(rule.get("param_id", "") or "").strip())
            if pid not in self.param_bounds:
                continue
            bound = self.param_bounds[pid]
            direction = int(rule.get("direction", 0) or 0)
            scale = float(rule.get("step_scale", 0.35 if long_term else 0.6) or 0.0)
            delta = float(direction) * float(bound.max_step_abs) * max(0.1, min(1.0, scale))
            updates.append(
                {
                    "rule_id": rule_id,
                    "param": pid,
                    "delta": delta,
                    "reason": str(rule.get("description", "") or f"custom_rule {rule_id}"),
                    "metric_key": metric_key,
                    "issue_mode": issue_mode,
                    "severity": severity,
                }
            )
        return updates

    def _metric_issue_snapshot(self, *, rows: list[dict[str, Any]], metric_key: str) -> dict[str, Any] | None:
        vals = _recent_window(rows, metric_key, len(rows))
        if not vals:
            return None
        target = self.metric_targets.get(metric_key)
        if target is None:
            return None
        mean_v = _mean(vals)
        std_v = _std(vals)
        max_v = max(vals)
        min_v = min(vals)
        latest_v = vals[-1]

        high_ratio = 0.0
        if float(target.expected_max) > 0.0:
            high_ratio = max(0.0, (max(mean_v, latest_v, max_v) - float(target.expected_max)) / max(1e-6, float(target.expected_max)))
        elif max(mean_v, latest_v, max_v) > 0.0:
            high_ratio = max(mean_v, latest_v, max_v)

        low_ratio = 0.0
        if mean_v < float(target.expected_min):
            denom = max(1e-6, abs(float(target.expected_min)) if abs(float(target.expected_min)) > 1e-9 else abs(float(target.ideal)) + 1.0)
            low_ratio = max(0.0, (float(target.expected_min) - mean_v) / denom)

        band = _band_distribution_summary(values=vals, target=target)
        if band:
            high_ratio = max(
                float(high_ratio),
                float(band.get("occupancy_over_ratio", 0.0) or 0.0),
                float(band.get("p95_over_ratio", 0.0) or 0.0),
                float(band.get("run_over_ratio", 0.0) or 0.0),
            )

        flatline = bool(
            target.min_std > 0.0
            and std_v < float(target.min_std)
            and float(target.expected_min) <= mean_v <= float(target.expected_max)
        )

        if high_ratio <= 0.0 and low_ratio <= 0.0 and not flatline:
            return None
        return {
            "metric_key": metric_key,
            "mean": mean_v,
            "std": std_v,
            "max": max_v,
            "min": min_v,
            "latest": latest_v,
            "target": target,
            "high_ratio": high_ratio,
            "low_ratio": low_ratio,
            "flatline": flatline,
            "band": band,
        }

    def _endogenous_balance_snapshot(self, *, rows: list[dict[str, Any]]) -> dict[str, Any]:
        internal_vals = _recent_window(rows, "internal_sa_count", len(rows))
        ratio_vals = _recent_window(rows, "internal_to_external_sa_ratio", len(rows))
        selected_vals = _recent_window(rows, "internal_resolution_structure_count_selected", len(rows))
        external_vals = _recent_window(rows, "external_sa_count", len(rows))
        raw_unit_vals = _recent_window(rows, "internal_resolution_raw_unit_count", len(rows))
        selected_unit_vals = _recent_window(rows, "internal_resolution_selected_unit_count", len(rows))
        budget_vals = _recent_window(rows, "internal_resolution_detail_budget", len(rows))
        cam_vals = _recent_window(rows, "cam_item_count", len(rows))
        pool_vals = _recent_window(rows, "pool_active_item_count", len(rows))

        target_internal = self.metric_targets.get("internal_sa_count")
        target_ratio = self.metric_targets.get("internal_to_external_sa_ratio")
        target_selected = self.metric_targets.get("internal_resolution_structure_count_selected")

        mean_internal = _mean(internal_vals)
        mean_ratio = _mean(ratio_vals)
        mean_selected = _mean(selected_vals)
        mean_external = _mean(external_vals)
        mean_raw_units = _mean(raw_unit_vals)
        mean_selected_units = _mean(selected_unit_vals)
        mean_budget = _mean(budget_vals)
        mean_cam_items = _mean(cam_vals)
        mean_pool_items = _mean(pool_vals)

        latest_internal = internal_vals[-1] if internal_vals else 0.0
        latest_ratio = ratio_vals[-1] if ratio_vals else 0.0
        latest_selected = selected_vals[-1] if selected_vals else 0.0
        latest_external = external_vals[-1] if external_vals else 0.0
        latest_raw_units = raw_unit_vals[-1] if raw_unit_vals else 0.0
        latest_selected_units = selected_unit_vals[-1] if selected_unit_vals else 0.0
        latest_budget = budget_vals[-1] if budget_vals else 0.0
        latest_cam_items = cam_vals[-1] if cam_vals else 0.0
        latest_pool_items = pool_vals[-1] if pool_vals else 0.0
        current_structure_cap = self._get_current_param_value(
            "hdb.internal_resolution_max_structures_per_tick",
            source="runtime",
        )

        internal_min = float(target_internal.expected_min) if isinstance(target_internal, MetricTarget) else 0.0
        ratio_min = float(target_ratio.expected_min) if isinstance(target_ratio, MetricTarget) else 0.0
        selected_min = float(target_selected.expected_min) if isinstance(target_selected, MetricTarget) else 0.0

        internal_deficit = 0.0
        if internal_min > 0.0:
            internal_deficit = max(
                0.0,
                (internal_min - min(mean_internal, latest_internal if latest_internal > 0.0 else mean_internal)) / internal_min,
            )

        ratio_deficit = 0.0
        if ratio_min > 0.0:
            ratio_deficit = max(
                0.0,
                (ratio_min - min(mean_ratio, latest_ratio if latest_ratio > 0.0 else mean_ratio)) / ratio_min,
            )

        selected_deficit = 0.0
        if selected_min > 0.0:
            selected_deficit = max(
                0.0,
                (selected_min - min(mean_selected, latest_selected if latest_selected > 0.0 else mean_selected)) / selected_min,
            )

        dominance_lost = False
        if internal_vals and external_vals:
            dominance_lost = bool(mean_internal <= mean_external or latest_internal <= latest_external)

        severity = max(internal_deficit, ratio_deficit, selected_deficit)
        if dominance_lost:
            severity = max(severity, 0.2)

        return {
            "needs_recovery": bool(severity > 0.0 or dominance_lost),
            "severity": round(float(severity), 6),
            "mean_internal_sa": mean_internal,
            "latest_internal_sa": latest_internal,
            "mean_external_sa": mean_external,
            "latest_external_sa": latest_external,
            "mean_ratio": mean_ratio,
            "latest_ratio": latest_ratio,
            "mean_selected_structures": mean_selected,
            "latest_selected_structures": latest_selected,
            "current_structure_cap": current_structure_cap,
            "mean_raw_units": mean_raw_units,
            "latest_raw_units": latest_raw_units,
            "mean_selected_units": mean_selected_units,
            "latest_selected_units": latest_selected_units,
            "mean_detail_budget": mean_budget,
            "latest_detail_budget": latest_budget,
            "mean_cam_items": mean_cam_items,
            "latest_cam_items": latest_cam_items,
            "mean_pool_items": mean_pool_items,
            "latest_pool_items": latest_pool_items,
            "internal_deficit": round(float(internal_deficit), 6),
            "ratio_deficit": round(float(ratio_deficit), 6),
            "selected_deficit": round(float(selected_deficit), 6),
            "dominance_lost": dominance_lost,
        }

    def _decide_endogenous_recovery(self, *, balance: dict[str, Any], long_term: bool = False) -> list[dict[str, Any]]:
        if not isinstance(balance, dict) or not bool(balance.get("needs_recovery", False)):
            return []

        severity = max(0.18, min(1.0, float(balance.get("severity", 0.0) or 0.0)))
        step_scale = 0.45 if long_term else 1.0
        updates: list[dict[str, Any]] = []

        # If the internal-resolution budget is not binding (everything is already selected),
        # raising the budget won't help. In that case we should prefer upstream levers:
        # - keep more items alive in StatePool (half-life / decay)
        # - allow CAM to carry a bit more structure sources (attention cap)
        raw_units = float(balance.get("latest_raw_units", 0.0) or 0.0)
        selected_units = float(balance.get("latest_selected_units", 0.0) or 0.0)
        detail_budget = float(balance.get("latest_detail_budget", 0.0) or 0.0)
        latest_selected_structures = float(balance.get("latest_selected_structures", 0.0) or 0.0)
        current_structure_cap = float(balance.get("current_structure_cap", 0.0) or 0.0)
        # 判断“预算是否在真正限制内源展开”：
        # - 如果 raw≈selected（几乎全入选），说明当前瓶颈不在预算，而在候选来源/上游残差规模；
        # - 如果 selected 明显小于 budget（budget 远大于实际入选），说明预算也不紧；
        # 两种情况都不该继续推高 budget_base，否则会出现“无效调参一直加预算”的循环。
        budget_not_binding = bool(
            (raw_units > 0.0 and selected_units >= raw_units * 0.98)
            or (detail_budget > 0.0 and selected_units <= detail_budget * 0.75)
        )
        structure_cap_binding = bool(
            current_structure_cap > 0.0
            and latest_selected_structures >= current_structure_cap * 0.85
        )

        def _append(param: str, *, metric_key: str, reason: str, weight: float = 1.0) -> None:
            pid = self._canonicalize_param_id(param)
            if pid not in self.param_bounds:
                return
            rule_id = f"builtin.endogenous.recover::{pid}"
            if not self._rule_enabled(rule_id):
                return
            bound = self.param_bounds[pid]
            delta = float(bound.max_step_abs) * step_scale * max(0.3, min(1.0, (0.4 + severity * 0.6) * weight))
            if abs(delta) < 1e-12:
                return
            updates.append(
                {
                    "rule_id": rule_id,
                    "param": pid,
                    "delta": delta,
                    "reason": reason,
                    "metric_key": metric_key,
                    "issue_mode": "low",
                    "severity": round(float(severity), 6),
                }
            )

        if float(balance.get("selected_deficit", 0.0) or 0.0) > 0.0 or bool(balance.get("dominance_lost", False)):
            if structure_cap_binding:
                _append(
                    "hdb.internal_resolution_max_structures_per_tick",
                    metric_key="internal_resolution_structure_count_selected",
                    reason="recover_endogenous_sources",
                    weight=1.0,
                )
            _append(
                "hdb.structure_level_max_rounds",
                metric_key="internal_resolution_structure_count_selected",
                reason="recover_endogenous_structure_rounds",
                weight=0.45,
            )

        if float(balance.get("internal_deficit", 0.0) or 0.0) > 0.0 or float(balance.get("ratio_deficit", 0.0) or 0.0) > 0.0:
            if budget_not_binding:
                _append(
                    "state_pool.default_er_decay_ratio",
                    metric_key="internal_sa_count",
                    reason="recover_endogenous_retention",
                    weight=0.85,
                )
                _append(
                    "state_pool.default_ev_decay_ratio",
                    metric_key="internal_sa_count",
                    reason="recover_endogenous_retention",
                    weight=0.85,
                )
                _append(
                    "state_pool.soft_capacity_start_items",
                    metric_key="pool_active_item_count",
                    reason="recover_endogenous_softcap",
                    weight=0.35,
                )
                _append(
                    "state_pool.soft_capacity_full_items",
                    metric_key="pool_active_item_count",
                    reason="recover_endogenous_softcap",
                    weight=0.35,
                )
                _append(
                    "attention.max_cam_items",
                    metric_key="internal_resolution_structure_count_selected",
                    reason="recover_endogenous_cam_capacity",
                    weight=0.35,
                )
                _append(
                    "attention.min_cam_items",
                    metric_key="internal_resolution_structure_count_selected",
                    reason="recover_endogenous_cam_capacity",
                    weight=0.2,
                )
            else:
                _append(
                    "hdb.internal_resolution_flat_unit_cap_per_structure",
                    metric_key="internal_sa_count",
                    reason="recover_endogenous_capacity",
                    weight=1.0,
                )
                _append(
                    "hdb.internal_resolution_detail_budget_base",
                    metric_key="internal_sa_count",
                    reason="recover_endogenous_detail_budget",
                    weight=0.7,
                )
                _append(
                    "hdb.stimulus_level_max_rounds",
                    metric_key="internal_to_external_sa_ratio",
                    reason="recover_endogenous_followthrough",
                    weight=0.5,
                )

        return updates

    def _filter_endogenous_conflicts(self, *, decisions: list[dict[str, Any]], balance: dict[str, Any]) -> list[dict[str, Any]]:
        if not decisions or not isinstance(balance, dict) or not bool(balance.get("needs_recovery", False)):
            return decisions
        filtered: list[dict[str, Any]] = []
        blocked: list[dict[str, Any]] = []
        for item in decisions:
            pid = self._canonicalize_param_id(str(item.get("param", "") or "").strip())
            delta = _safe_float(item.get("delta", 0.0), 0.0)
            if pid in ENDOGENOUS_RECOVERY_PARAM_IDS and delta < 0.0:
                blocked.append(
                    {
                        "rule_id": str(item.get("rule_id", "") or ""),
                        "param": pid,
                        "delta": delta,
                        "reason": str(item.get("reason", "") or ""),
                    }
                )
                continue
            filtered.append(item)
        if blocked:
            self._audit(
                {
                    "kind": "endogenous_guard_blocked",
                    "balance": balance,
                    "blocked_updates": blocked[:32],
                }
            )
        return filtered

    def _score_spec_for_metric(self, *, metric_key: str, spec: param_catalog.ParamSpec) -> float:
        score = 0.0
        pid = str(spec.param_id or "").lower()
        tags = set(spec.tags or [])
        module = str(spec.module or "").lower()

        if metric_key in (spec.impacts or []):
            score += 2.0
        if "performance" in tags and metric_key in {"timing_total_logic_ms", "internal_resolution_raw_unit_count", "merged_flat_token_count", "pool_active_item_count"}:
            score += 2.5
        if "gating" in tags and metric_key.startswith("cfs_"):
            score += 2.0
        if "action_rules" in tags and metric_key.startswith("action_executed_"):
            score += 2.0
        if metric_key.startswith("action_drive") and (module == "action" or "action_rules" in tags):
            score += 2.5
        if "echo" in tags and metric_key.startswith("sensor_echo"):
            score += 3.0
        if module == "time_sensor" and metric_key.startswith("time_sensor"):
            score += 3.0
        if module == "text_sensor" and metric_key in {"merged_flat_token_count", "external_sa_count", "sensor_echo_pool_size"}:
            score += 2.0
        if module == "hdb" and metric_key in {"timing_total_logic_ms", "internal_resolution_raw_unit_count", "merged_flat_token_count"}:
            score += 2.0
        if module == "attention" and metric_key in {"cam_item_count", "action_executed_attention_focus", "timing_total_logic_ms"}:
            score += 1.5
        if module == "cognitive_stitching" and metric_key in {"cs_candidate_count", "cs_action_count", "stimulus_new_structure_count", "timing_cognitive_stitching_ms"}:
            score += 3.0
        if module == "state_pool" and metric_key in {"pool_active_item_count", "pool_high_cp_item_count", "timing_total_logic_ms"}:
            score += 1.5
        if metric_key.startswith("cfs_dissonance") and "dissonance" in pid:
            score += 3.0
        if metric_key.startswith("cfs_pressure") and "pressure" in pid:
            score += 3.0
        if metric_key.startswith("cfs_expectation") and "expectation" in pid:
            score += 3.0
        if metric_key.startswith("action_executed_recall") and ("recall" in pid or "time_feeling" in pid):
            score += 3.0
        if metric_key.startswith("cs_") and module == "cognitive_stitching":
            score += 3.5
        if metric_key == "stimulus_new_structure_count" and module == "cognitive_stitching":
            score += 3.0
        if metric_key == "timing_cognitive_stitching_ms" and module == "cognitive_stitching":
            score += 3.0
        return score

    def _infer_adjustment_direction(self, *, metric_key: str, issue_mode: str, spec: param_catalog.ParamSpec) -> int:
        """
        Return +1 / -1 / 0:
        - +1 means raise the param value
        - -1 means lower the param value
        """
        pid = str(spec.param_id or "").lower()
        tags = set(spec.tags or [])

        increase_when_metric_high = any(
            token in pid
            for token in [
                "threshold",
                "cooldown",
                "window_ticks",
                "hold_ticks",
                "soft_capacity_start_items",
                "soft_capacity_full_items",
                "soft_capacity_decay_power_max",
                "fatigue_threshold_count",
                "er_elimination_threshold",
                "ev_elimination_threshold",
                "cp_elimination_ignore_below",
                "min_strength",
                "min_delta",
                "min_candidate_score",
                "min_seed_total_energy",
                "min_event_total_energy",
                "event_grasp_min_total_energy",
                "tick_time_floor_ms",
            ]
        )
        decrease_when_metric_high = any(
            token in pid
            for token in [
                "max_",
                "top_n",
                "budget",
                "capacity",
                "pool_max_items",
                "memory_top_k",
                "gain",
                "boost",
                "ratio",
                "retention",
                "half_life",
                "round",
                "flat_unit_cap",
                "structures_per_tick",
                "ttl_ticks",
                "focus_boost",
                "energy_gain_ratio",
                "bucket_energy_threshold",
                "weight",
                "scale",
            ]
        )
        if metric_key in {"cs_action_count", "cs_candidate_count", "stimulus_new_structure_count"} and "anchor_distance_penalty" in pid:
            decrease_when_metric_high = True
        if "decay" in tags:
            decrease_when_metric_high = True
        if "gating" in tags and not decrease_when_metric_high:
            increase_when_metric_high = True

        sign = 0
        if issue_mode == "high":
            if increase_when_metric_high:
                sign = +1
            elif decrease_when_metric_high:
                sign = -1
            elif "performance" in tags:
                sign = -1
        elif issue_mode == "low":
            if increase_when_metric_high:
                sign = -1
            elif decrease_when_metric_high:
                sign = +1
            elif "performance" in tags:
                sign = +1
        elif issue_mode == "flatline":
            # We want to reintroduce natural movement, not create explosions.
            if increase_when_metric_high:
                sign = -1
            elif decrease_when_metric_high:
                sign = +1
            elif "gating" in tags:
                sign = -1
            elif "performance" in tags:
                sign = +1
        return sign

    def _decide_catalog_nudges(self, *, recent: list[dict[str, Any]], long_term: bool = False) -> list[dict[str, Any]]:
        """
        Table-driven generic rule generator.

        Instead of hard-coding hundreds of `if metric X then tweak param Y`, we use the
        catalog's `impacts/tags` + a small set of adjustment templates to synthesize many
        targeted rules on demand. This keeps the system auditable while scaling to most
        numeric parameters.
        """
        if not recent:
            return []

        updates: list[dict[str, Any]] = []
        seen_params: set[str] = set()
        issue_modes = ("high", "low", "flatline")
        scale_base = 0.35 if long_term else 0.6

        for metric_key in self.metric_targets.keys():
            issue = self._metric_issue_snapshot(rows=recent, metric_key=metric_key)
            if not issue:
                continue

            target = issue["target"]
            candidates = [s for s in self.catalog_specs if s.auto_tune_allowed and metric_key in (s.impacts or [])]
            if not candidates:
                continue
            candidates = sorted(candidates, key=lambda s: self._score_spec_for_metric(metric_key=metric_key, spec=s), reverse=True)

            for issue_mode in issue_modes:
                severity = 0.0
                if issue_mode == "high":
                    severity = float(issue["high_ratio"])
                elif issue_mode == "low":
                    severity = float(issue["low_ratio"])
                elif issue_mode == "flatline":
                    severity = 1.0 if bool(issue["flatline"]) else 0.0
                if severity <= 0.0:
                    continue

                picked = 0
                for spec in candidates:
                    if picked >= (1 if long_term else 2):
                        break
                    pid = self._canonicalize_param_id(spec.param_id)
                    if pid in seen_params:
                        continue
                    if pid not in self.param_bounds:
                        continue
                    direction = self._infer_adjustment_direction(metric_key=metric_key, issue_mode=issue_mode, spec=spec)
                    if direction == 0:
                        continue
                    rule_id = f"catalog::{metric_key}::{issue_mode}::{pid}"
                    if not self._rule_enabled(rule_id):
                        continue
                    bound = self.param_bounds[pid]
                    step_scale = min(1.0, max(0.15, scale_base + min(1.0, severity) * 0.5))
                    delta = float(direction) * float(bound.max_step_abs) * step_scale
                    if abs(delta) < 1e-12:
                        continue
                    updates.append(
                        {
                            "rule_id": rule_id,
                            "param": pid,
                            "delta": delta,
                            "reason": f"catalog_rule metric={metric_key} mode={issue_mode} mean={float(issue['mean']):.4f} std={float(issue['std']):.4f}",
                            "metric_key": metric_key,
                            "issue_mode": issue_mode,
                            "severity": round(float(severity), 6),
                            "rule_weight": float(target.weight),
                        }
                    )
                    seen_params.add(pid)
                    picked += 1

        return updates

    # --------------------------
    # Apply parameter updates
    # --------------------------

    def _apply_param_update(self, update: dict[str, Any], *, persist: bool) -> bool:
        """
        Update a parameter in the unified param store, then hot reload runtime modules.
        """
        requested_param = str(update.get("param", "") or "").strip()
        param = self._canonicalize_param_id(requested_param)
        if not param or param not in self.param_bounds:
            return False
        bound = self.param_bounds[param]
        delta = _safe_float(update.get("delta", 0.0), 0.0)
        if not (abs(delta) > 1e-12):
            return False
        delta = _clamp(delta, -bound.max_step_abs, +bound.max_step_abs)

        # Load current value from overrides (or use a reasonable baseline).
        current = self._get_current_param_value(param, source=("persisted" if persist else "runtime"))
        next_v = _clamp(current + delta, bound.min_value, bound.max_value)
        next_v = _round_quantum(next_v, bound.quantum)

        if abs(next_v - current) < 1e-12:
            # No-op due to bounds/quantum. Mark backoff so we don't keep trying.
            pid = self._canonicalize_param_id(param)
            if pid:
                # Also stamp last_param_tick so the cooldown/backoff can actually take effect.
                try:
                    t = int(update.get("tick_index", -1) or -1)
                except Exception:
                    t = -1
                if t >= 0:
                    self.last_param_tick[pid] = t
                self._mark_param_backoff(
                    pid=pid,
                    tick_index=int(update.get("tick_index", -1) or -1),
                    reason="no_op_clamped_or_quantized",
                    penalty=float(self.cfg.param_backoff_failure_penalty),
                )
            return False

        # Update param stores first (auditable even if hot reload fails).
        self._set_param_value(param, next_v, persist=persist)

        # Apply to runtime now (best-effort).
        self._apply_overrides_to_runtime(trace_id="auto_tuner_tick")
        if persist:
            self._apply_persisted_overrides_to_persist_files()
        update["from"] = current
        update["to"] = next_v
        if requested_param != param:
            update["requested_param"] = requested_param
            update["param"] = param
        self._save_state()
        return True

    def _canonicalize_param_id(self, param: str) -> str:
        """Resolve legacy aliases to canonical catalog ids when possible."""
        pid = str(param or "").strip()
        if not pid:
            return ""
        if pid in self.param_bounds or pid in self.spec_by_id:
            return pid
        if pid.startswith("iesm.") and not pid.startswith("iesm.rules."):
            resolved = self._resolve_legacy_iesm_alias(pid)
            if resolved:
                return resolved
        return pid

    def _get_current_param_value(self, param: str, *, source: str) -> float:
        # source: "runtime" | "persisted"
        pid = self._canonicalize_param_id(param)
        params = self.runtime_params if source == "runtime" else self.persisted_params
        if pid in params:
            return _safe_float(params.get(pid, 0.0), 0.0)

        spec = self.spec_by_id.get(pid)
        if spec is None:
            return 0.0

        # Rule param: read from current rules file.
        if spec.source_kind == "iesm_rule":
            try:
                rules_path = self.runtime_rules_path if source == "runtime" else self.persist_rules_path
                if not rules_path.exists():
                    rules_path = Path(str(getattr(self.app.iesm, "_rules_path", "")) or "")  # type: ignore[attr-defined]
                doc = _load_rules_doc(rules_path)
                rules = doc.get("rules") if isinstance(doc, dict) else None
                rules = rules if isinstance(rules, list) else []
                target_rule_id = str(spec.path_tokens[0]) if spec.path_tokens else ""
                for rule in rules:
                    if not isinstance(rule, dict):
                        continue
                    if str(rule.get("id", "") or "") != target_rule_id:
                        continue
                    v = self._deep_get(rule, list(spec.path_tokens[1:]))
                    return _safe_float(v, _safe_float(spec.value, 0.0))
            except Exception:
                return _safe_float(spec.value, 0.0)

        # Module / observatory config param: read from current runtime config.
        try:
            cfg = self._get_runtime_module_config(spec.module)
            v = self._deep_get(cfg, list(spec.path_tokens))
            if v is not None:
                return _safe_float(v, _safe_float(spec.value, 0.0))
        except Exception:
            pass
        return _safe_float(spec.value, 0.0)

    def _set_param_value(self, param: str, value: float, *, persist: bool) -> None:
        """
        Store absolute param values in the unified runtime/persisted param maps.
        """
        pid = self._canonicalize_param_id(param)
        self.runtime_params[pid] = float(value)
        if persist:
            self.persisted_params[pid] = float(value)

    def _apply_overrides_to_runtime(self, *, trace_id: str) -> None:
        """
        Apply overrides to runtime modules (hot reload).

        Runtime flow:
        1. Patch runtime rules file from runtime_params (IESM rule leaves) and reload rules.
        2. For each module config, materialize a patch from runtime_params and hot reload that module.
        """
        if not self.enabled:
            return

        # Ensure runtime files exist / runtime baseline prepared.
        if not self.runtime_rules_path.exists():
            self.prepare_and_apply_overrides(trace_id=trace_id)

        # 1) IESM rules runtime patch
        iesm_rule_params = {
            k: v
            for k, v in (self.runtime_params or {}).items()
            if isinstance(k, str) and k.startswith("iesm.rules.")
        }
        if iesm_rule_params:
            doc = _load_rules_doc(self.runtime_rules_path)
            if doc and self._apply_iesm_rule_param_values(doc, iesm_rule_params):
                self.runtime_rules_path.write_text(_dump_rules_doc(doc), encoding="utf-8")
                try:
                    self.app.iesm.reload_rules(trace_id=f"{trace_id}_iesm_rules")  # type: ignore[attr-defined]
                except Exception:
                    pass

        # 2) Module config runtime patches
        for module, path in self.runtime_module_patch_paths.items():
            # innate_script config itself is special: it needs rules_path + any config tunables.
            try:
                base = copy.deepcopy(self._base_module_configs.get(module, self._get_runtime_module_config(module)))
                patch = self._materialize_module_patch(module=module, params=self.runtime_params, base=base)
                if module == "innate_script":
                    patch = dict(patch)
                    patch["rules_path"] = str(self.runtime_rules_path)
                if not patch:
                    continue
                _write_yaml_patch(path, patch)
                self._reload_module_config(module=module, trace_id=f"{trace_id}_{module}", config_path=path)
            except Exception:
                continue

    def _apply_persisted_overrides_to_persist_files(self) -> None:
        """
        Materialize persisted params into files under outputs/auto_tuner/overrides/.
        This is what makes long-term tuning affect the next run.
        """
        _overrides_dir().mkdir(parents=True, exist_ok=True)

        # 1) Persisted IESM rules file
        if not self.persist_rules_path.exists() and self.runtime_rules_path.exists():
            try:
                self.persist_rules_path.write_text(self.runtime_rules_path.read_text(encoding="utf-8"), encoding="utf-8")
            except Exception:
                pass
        iesm_params = {
            k: v
            for k, v in (self.persisted_params or {}).items()
            if isinstance(k, str) and k.startswith("iesm.rules.")
        }
        if iesm_params and self.persist_rules_path.exists():
            try:
                doc = _load_rules_doc(self.persist_rules_path)
                if doc and self._apply_iesm_rule_param_values(doc, iesm_params):
                    self.persist_rules_path.write_text(_dump_rules_doc(doc), encoding="utf-8")
            except Exception:
                pass

        # 2) Persisted module patch files for audit / next-run baseline preparation
        for module, path in self.persist_module_patch_paths.items():
            try:
                base = self._base_module_configs.get(module, self._get_runtime_module_config(module))
                patch = self._materialize_module_patch(module=module, params=self.persisted_params, base=copy.deepcopy(base))
                if module == "innate_script":
                    patch = dict(patch)
                    patch["rules_path"] = str(self.persist_rules_path)
                if not patch:
                    continue
                _write_yaml_patch(path, patch)
            except Exception:
                continue


def _make_preview_tuner(*, app: Any | None = None) -> AutoTuner:
    return AutoTuner(
        app=app,
        run_dir=_auto_tuner_dir() / "_preview_session",
        enabled=False,
        enable_short_term=False,
        enable_long_term=False,
    )


def read_auto_tuner_catalog(*, app: Any | None = None) -> dict[str, Any]:
    tuner = _make_preview_tuner(app=app)
    guessed_bounds = {
        pid: {
            "min_value": float(bound.min_value),
            "max_value": float(bound.max_value),
            "max_step_abs": float(bound.max_step_abs),
            "quantum": float(bound.quantum),
        }
        for pid, bound in tuner.param_bounds.items()
    }
    params = [spec.to_dict() for spec in tuner.catalog_specs]
    module_counts: dict[str, int] = {}
    for item in params:
        module = str(item.get("module", "") or "unknown")
        module_counts[module] = int(module_counts.get(module, 0)) + 1
    return {
        "metric_library": _merge_metric_target_defs(),
        "params": params,
        "param_bounds": guessed_bounds,
        "summary": {
            "param_count": len(params),
            "auto_tune_allowed_count": sum(1 for spec in tuner.catalog_specs if spec.auto_tune_allowed),
            "module_counts": module_counts,
        },
        "paths": {
            "catalog_path": str(param_catalog.auto_tuner_dir() / "param_catalog.json"),
            "bounds_path": str(param_catalog.auto_tuner_dir() / "param_bounds.guessed.json"),
            "impact_table_path": str(param_catalog.auto_tuner_dir() / "param_impact_table.md"),
        },
    }


def build_auto_tuner_rule_catalog(*, app: Any | None = None) -> dict[str, Any]:
    tuner = _make_preview_tuner(app=app)
    rules_cfg = load_auto_tuner_rules()
    disabled = set(rules_cfg.get("disabled_rule_ids", []))
    protected = set(rules_cfg.get("protected_rule_ids", []))
    metrics = _merge_metric_target_defs()

    builtin_rules = [
        {
            "rule_id": "builtin.resource.hdb_fuse.raw_spike",
            "source": "builtin",
            "title": "原始分辨率暴涨时收紧单结构平铺上限",
            "metric_key": "internal_resolution_raw_unit_count",
            "issue_mode": "high",
            "param_id": "hdb.internal_resolution_flat_unit_cap_per_structure",
        },
        {
            "rule_id": "builtin.resource.hdb_fuse.timing_spike",
            "source": "builtin",
            "title": "总耗时暴涨时收紧每 Tick 参与结构数",
            "metric_key": "timing_total_logic_ms",
            "issue_mode": "high",
            "param_id": "hdb.internal_resolution_max_structures_per_tick",
        },
        {
            "rule_id": "builtin.cfs.dissonance.tighten",
            "source": "builtin",
            "title": "违和感长期过高时整体收紧违和与惩罚派生",
            "metric_key": "cfs_dissonance_max",
            "issue_mode": "high",
            "param_id": "iesm.cfs_dissonance.threshold",
        },
        {
            "rule_id": "builtin.cfs.pressure.tighten",
            "source": "builtin",
            "title": "压力高而违和不高时优先压缩惩罚残留",
            "metric_key": "cfs_pressure_max",
            "issue_mode": "high",
            "param_id": "iesm.cfs_pressure.ev_threshold",
        },
        {
            "rule_id": "builtin.cfs.expectation.tighten",
            "source": "builtin",
            "title": "期待长期高位时收紧奖励阈值与正确事件节奏",
            "metric_key": "cfs_expectation_max",
            "issue_mode": "high",
            "param_id": "iesm.cfs_expectation.ev_threshold",
        },
        {
            "rule_id": "builtin.action.recall.tighten",
            "source": "builtin",
            "title": "回忆行动过于频繁时优先压缩时间感受驱动力",
            "metric_key": "action_executed_recall",
            "issue_mode": "high",
            "param_id": "iesm.action_recall_time.threshold",
        },
        {
            "rule_id": "builtin.long_term.dissonance.relax_flatline",
            "source": "builtin",
            "title": "违和感过平时微量放松阈值，避免僵直直线",
            "metric_key": "cfs_dissonance_max",
            "issue_mode": "flatline",
            "param_id": "iesm.cfs_dissonance.threshold",
        },
        {
            "rule_id": "builtin.cs.open_gate.thresholds",
            "source": "builtin",
            "title": "认知拼接有候选但无动作时，温和放松候选阈值",
            "metric_key": "cs_action_count",
            "issue_mode": "low",
            "param_id": "cognitive_stitching.min_candidate_score",
        },
        {
            "rule_id": "builtin.cs.open_gate.seed_energy",
            "source": "builtin",
            "title": "认知拼接启动力不足时，温和放松种子能量门槛",
            "metric_key": "cs_action_count",
            "issue_mode": "low",
            "param_id": "cognitive_stitching.min_seed_total_energy",
        },
        {
            "rule_id": "builtin.cs.open_gate.event_energy",
            "source": "builtin",
            "title": "认知拼接事件门槛偏高时，温和放松事件能量阈值",
            "metric_key": "cs_action_count",
            "issue_mode": "low",
            "param_id": "cognitive_stitching.min_event_total_energy",
        },
        {
            "rule_id": "builtin.cs.tighten.thresholds",
            "source": "builtin",
            "title": "认知拼接过密时，回收候选阈值避免泛滥",
            "metric_key": "cs_action_count",
            "issue_mode": "high",
            "param_id": "cognitive_stitching.min_candidate_score",
        },
        {
            "rule_id": "builtin.cs.performance.tighten_threshold",
            "source": "builtin",
            "title": "认知拼接耗时过热时，抬高候选阈值压缩候选扇出",
            "metric_key": "timing_cognitive_stitching_ms",
            "issue_mode": "high",
            "param_id": "cognitive_stitching.min_candidate_score",
        },
        {
            "rule_id": "builtin.structure_supply.raise_cam_cap",
            "source": "builtin",
            "title": "工作集过瘦时，提高注意力上限补足结构候选来源",
            "metric_key": "cam_item_count",
            "issue_mode": "low",
            "param_id": "attention.max_cam_items",
        },
        {
            "rule_id": "builtin.structure_supply.raise_structure_cap",
            "source": "builtin",
            "title": "内源入选结构过少时，提高每 Tick 结构来源上限",
            "metric_key": "internal_resolution_structure_count_selected",
            "issue_mode": "low",
            "param_id": "hdb.internal_resolution_max_structures_per_tick",
        },
        {
            "rule_id": "builtin.ev_balance.raise_induction",
            "source": "builtin",
            "title": "虚能量明显偏低时，提高实能量诱发虚能量的基线系数",
            "metric_key": "pool_total_ev",
            "issue_mode": "low",
            "param_id": "emotion.subjective_modulators.er_induction_ratio.base",
        },
        {
            "rule_id": "builtin.ev_balance.raise_propagation",
            "source": "builtin",
            "title": "虚能量明显偏低时，提高虚能量传播基线系数",
            "metric_key": "pool_total_ev",
            "issue_mode": "low",
            "param_id": "emotion.subjective_modulators.ev_propagation_ratio.base",
        },
        {
            "rule_id": "builtin.ev_balance.slow_ev_decay",
            "source": "builtin",
            "title": "虚能量明显偏低时，适度减缓虚能量默认衰减",
            "metric_key": "pool_total_ev",
            "issue_mode": "low",
            "param_id": "state_pool.default_ev_decay_ratio",
        },
        {
            "rule_id": "builtin.ev_balance.soften_er_retention",
            "source": "builtin",
            "title": "实能量显著过满且虚能量偏低时，适度回收实能量默认保留",
            "metric_key": "pool_total_er",
            "issue_mode": "high",
            "param_id": "state_pool.default_er_decay_ratio",
        },
        {
            "rule_id": "builtin.attention.tighten.cam_cap",
            "source": "builtin",
            "title": "工作集长期过热时，回收注意力上限避免后续链路膨胀",
            "metric_key": "cam_item_count",
            "issue_mode": "high",
            "param_id": "attention.max_cam_items",
        },
    ]
    for item in builtin_rules:
        item["enabled"] = item["rule_id"] not in disabled
        item["protected"] = item["rule_id"] in protected

    generated: list[dict[str, Any]] = []
    for metric in metrics:
        metric_key = str(metric.get("key", "") or "").strip()
        if not metric_key:
            continue
        candidates = [spec for spec in tuner.catalog_specs if spec.auto_tune_allowed and metric_key in (spec.impacts or [])]
        candidates = sorted(candidates, key=lambda spec: tuner._score_spec_for_metric(metric_key=metric_key, spec=spec), reverse=True)
        for issue_mode in ("high", "low", "flatline"):
            for spec in candidates[:12]:
                direction = tuner._infer_adjustment_direction(metric_key=metric_key, issue_mode=issue_mode, spec=spec)
                if direction == 0:
                    continue
                rule_id = f"catalog::{metric_key}::{issue_mode}::{spec.param_id}"
                generated.append(
                    {
                        "rule_id": rule_id,
                        "source": "catalog",
                        "title": f"{metric.get('title', metric_key)} · {issue_mode} · {spec.param_id}",
                        "metric_key": metric_key,
                        "issue_mode": issue_mode,
                        "param_id": spec.param_id,
                        "module": spec.module,
                        "direction": direction,
                        "score": round(float(tuner._score_spec_for_metric(metric_key=metric_key, spec=spec)), 4),
                        "enabled": rule_id not in disabled,
                        "protected": rule_id in protected,
                        "tags": list(spec.tags),
                        "impacts": list(spec.impacts),
                    }
                )

    custom_rules = []
    for rule in rules_cfg.get("custom_rules", []):
        item = dict(rule)
        item["source"] = "custom"
        item["enabled"] = bool(item.get("enabled", True))
        item["protected"] = str(item.get("id", "") or "") in protected
        custom_rules.append(item)

    return {
        "builtin_rules": builtin_rules,
        "generated_rules": generated,
        "custom_rules": custom_rules,
        "summary": {
            "builtin_count": len(builtin_rules),
            "generated_count": len(generated),
            "custom_count": len(custom_rules),
            "disabled_count": len(disabled),
            "protected_count": len(protected),
        },
    }


def read_auto_tuner_state(*, app: Any | None = None) -> dict[str, Any]:
    raw_before = _load_json_dict(_state_path())
    tuner = _make_preview_tuner(app=app)
    needs_save = (
        dict(raw_before.get("persisted_params", {})) != dict(tuner.persisted_params)
        or dict(raw_before.get("rule_health", {})) != dict(tuner.rule_health)
        or list(raw_before.get("active_trials", [])) != list(tuner.active_trials)
        or list(raw_before.get("trial_history", [])) != list(tuner.trial_history)
        or list(raw_before.get("last_applied_updates", [])) != list(tuner.last_applied_updates)
        or list(raw_before.get("rule_observations", [])) != list(tuner.rule_observations)
        or list(raw_before.get("observation_history", [])) != list(tuner.observation_history)
        or list(raw_before.get("observation_review_history", [])) != list(tuner.observation_review_history)
        or dict(raw_before.get("last_observation_review", {})) != dict(tuner.last_observation_review)
        or str(raw_before.get("schema_version", "") or "") != "v3"
    )
    if needs_save:
        tuner._save_state()
        state = _load_json_dict(_state_path())
    else:
        state = raw_before
    rules = load_auto_tuner_rules()
    suggestions = _list_recent_llm_suggestions(limit=12)
    llm_custom_rules = [
        dict(item)
        for item in (rules.get("custom_rules", []) or [])
        if isinstance(item, dict) and str(item.get("origin", "") or "").strip() == "llm_auto"
    ]
    active_observations = [
        dict(item)
        for item in (state.get("rule_observations", []) or [])
        if isinstance(item, dict) and str(item.get("status", "") or "observing") == "observing"
    ]
    reviewable_observation_count = sum(1 for item in active_observations if tuner._observation_is_reviewable(item))
    return {
        "state": state,
        "recent_llm_suggestions": suggestions,
        "summary": {
            "persisted_param_count": len(tuner.persisted_params),
            "runtime_param_count": len(tuner.runtime_params),
            "active_trial_count": len(tuner.active_trials),
            "trial_history_count": len(tuner.trial_history),
            "rule_health_count": len(tuner.rule_health),
            "disabled_rule_count": len(rules.get("disabled_rule_ids", [])),
            "protected_rule_count": len(rules.get("protected_rule_ids", [])),
            "llm_suggestion_count": len(suggestions),
            "llm_candidate_rule_count": len([x for x in llm_custom_rules if str(x.get("status", "") or "candidate") == "candidate"]),
            "llm_solidified_rule_count": len([x for x in llm_custom_rules if str(x.get("status", "") or "") == "solidified"]),
            "llm_rejected_rule_count": len([x for x in llm_custom_rules if str(x.get("status", "") or "") == "rejected"]),
            "observation_active_count": len(active_observations),
            "observation_history_count": len([x for x in (state.get("observation_history", []) or []) if isinstance(x, dict)]),
            "observation_reviewable_count": reviewable_observation_count,
            "observation_review_history_count": len([x for x in (state.get("observation_review_history", []) or []) if isinstance(x, dict)]),
            "last_observation_review_action_count": len(
                [
                    x
                    for x in (
                        (state.get("last_observation_review", {}) or {}).get("decisions", [])
                        if isinstance(state.get("last_observation_review", {}), dict)
                        else []
                    )
                    if isinstance(x, dict)
                ]
            ),
        },
        "paths": {
            "state_path": str(_state_path()),
            "global_audit_path": str(_global_audit_path()),
        },
    }


def read_auto_tuner_audit(*, limit: int = 200) -> dict[str, Any]:
    rows = _load_jsonl(_global_audit_path(), limit=max(1, int(limit)))
    rows.reverse()
    return {
        "items": rows,
        "count": len(rows),
        "path": str(_global_audit_path()),
    }


def list_rollback_points(*, limit: int = 50) -> dict[str, Any]:
    points = _load_rollback_points()[: max(1, int(limit))]
    return {
        "points": points,
        "count": len(points),
        "path": str(_rollback_points_path()),
    }


def rollback_to_point(*, point_id: str, app: Any | None = None) -> dict[str, Any]:
    pid = str(point_id or "").strip()
    if not pid:
        raise ValueError("point_id is required")
    points = _load_rollback_points()
    point = next((item for item in points if str(item.get("point_id", "") or "") == pid), None)
    if not isinstance(point, dict):
        raise ValueError(f"rollback point not found: {pid}")
    tuner = _make_preview_tuner(app=app)
    persisted_params = point.get("persisted_params")
    tuner.persisted_params = dict(persisted_params) if isinstance(persisted_params, dict) else {}
    tuner.runtime_params = copy.deepcopy(tuner.persisted_params)
    tuner._sanitize_param_stores()
    tuner._apply_persisted_overrides_to_persist_files()
    try:
        tuner._apply_overrides_to_runtime(trace_id=f"auto_tuner_rollback_{storage.safe_slug(pid)}")
    except Exception:
        pass
    tuner.last_applied_updates.append(
        {
            "ts_ms": _now_ms(),
            "rule_id": "manual.rollback",
            "param": "*",
            "metric_key": "",
            "issue_mode": "",
            "persist": True,
            "from": None,
            "to": None,
            "reason": f"rollback_to:{pid}",
        }
    )
    tuner._save_state()
    tuner._audit({"kind": "manual_rollback", "point_id": pid, "summary": point.get("summary", {})})
    return {"success": True, "point": point, "state": read_auto_tuner_state(app=app)}


def _load_auto_tuner_llm_cfg_from_path(path: Path, fallback: LLMReviewConfig | None = None) -> LLMReviewConfig:
    raw = _load_json_dict(path)
    if not raw and fallback is not None:
        return fallback
    base = fallback or LLMReviewConfig()
    return LLMReviewConfig(
        enabled=bool(raw.get("enabled", base.enabled)),
        auto_analyze_on_completion=bool(raw.get("auto_analyze_on_completion", base.auto_analyze_on_completion)),
        base_url=str(raw.get("base_url", base.base_url) or "").strip() or base.base_url,
        api_key=str(raw.get("api_key", base.api_key) or "").strip() or base.api_key,
        model=str(raw.get("model", base.model) or "").strip() or base.model,
        temperature=float(raw.get("temperature", base.temperature)),
        max_prompt_chars=int(raw.get("max_prompt_chars", base.max_prompt_chars)),
        timeout_sec=int(raw.get("timeout_sec", base.timeout_sec)),
        max_completion_tokens=int(raw.get("max_completion_tokens", base.max_completion_tokens)),
    )


def load_auto_tuner_llm_config() -> dict[str, Any]:
    fallback = load_review_config()
    cfg = _load_auto_tuner_llm_cfg_from_path(_llm_config_path(), fallback=fallback)
    source = "auto_tuner"
    if not _llm_config_path().exists():
        source = "llm_review_fallback"
    return {
        "config": cfg.to_public_dict(),
        "source": source,
        "path": str(_llm_config_path()),
    }


def save_auto_tuner_llm_config(updates: dict[str, Any]) -> dict[str, Any]:
    current = _load_auto_tuner_llm_cfg_from_path(_llm_config_path(), fallback=load_review_config())
    updates = updates if isinstance(updates, dict) else {}
    api_key = str(current.api_key or "")
    if "api_key" in updates:
        candidate = str(updates.get("api_key", "") or "").strip()
        if candidate:
            api_key = candidate
    cfg = LLMReviewConfig(
        enabled=bool(updates.get("enabled", current.enabled)),
        auto_analyze_on_completion=bool(updates.get("auto_analyze_on_completion", current.auto_analyze_on_completion)),
        base_url=str(updates.get("base_url", current.base_url) or "").strip() or current.base_url,
        api_key=api_key,
        model=str(updates.get("model", current.model) or "").strip() or current.model,
        temperature=float(updates.get("temperature", current.temperature)),
        max_prompt_chars=int(updates.get("max_prompt_chars", current.max_prompt_chars)),
        timeout_sec=int(updates.get("timeout_sec", current.timeout_sec)),
        max_completion_tokens=int(updates.get("max_completion_tokens", current.max_completion_tokens)),
    )
    _write_json(
        _llm_config_path(),
        {
            "enabled": bool(cfg.enabled),
            "auto_analyze_on_completion": bool(cfg.auto_analyze_on_completion),
            "base_url": str(cfg.base_url or ""),
            "api_key": str(cfg.api_key or ""),
            "model": str(cfg.model or ""),
            "temperature": float(cfg.temperature),
            "max_prompt_chars": int(cfg.max_prompt_chars),
            "timeout_sec": int(cfg.timeout_sec),
            "max_completion_tokens": int(cfg.max_completion_tokens),
            "updated_at_ms": _now_ms(),
        },
    )
    return load_auto_tuner_llm_config()


def _extract_chat_text(payload: dict[str, Any]) -> str:
    try:
        choices = payload.get("choices")
        if not isinstance(choices, list) or not choices:
            return ""
        message = choices[0].get("message") if isinstance(choices[0], dict) else {}
        if not isinstance(message, dict):
            return ""
        return str(message.get("content", "") or "")
    except Exception:
        return ""


def _read_run_excerpt(run_id: str, *, max_prompt_chars: int) -> dict[str, Any]:
    run_dir = storage.resolve_run_dir(run_id)
    manifest = _load_json_dict(run_dir / "manifest.json")
    metrics_path = run_dir / "metrics.jsonl"
    head: list[str] = []
    tail: list[str] = []
    if metrics_path.exists():
        try:
            with metrics_path.open("r", encoding="utf-8", errors="replace") as fh:
                for idx, line in enumerate(fh):
                    if idx < 160:
                        head.append(line.rstrip("\n"))
                    if line.strip():
                        tail.append(line.rstrip("\n"))
                        if len(tail) > 120:
                            tail = tail[-120:]
        except Exception:
            pass
    metrics_text = "\n".join(head + ["", "# ...(middle omitted)...", ""] + tail).strip()
    if len(metrics_text) > max_prompt_chars:
        metrics_text = metrics_text[: max_prompt_chars]
    return {
        "run_id": run_id,
        "manifest": manifest,
        "metrics_excerpt": metrics_text,
    }


def _extract_json_code_block(text: str) -> dict[str, Any]:
    raw = str(text or "")
    candidates: list[str] = []
    marker = "```json"
    start = raw.find(marker)
    while start >= 0:
        end = raw.find("```", start + len(marker))
        if end > start:
            candidates.append(raw[start + len(marker) : end].strip())
        start = raw.find(marker, start + len(marker))
    candidates.append(raw.strip())
    for candidate in candidates:
        if not candidate:
            continue
        try:
            payload = json.loads(candidate)
        except Exception:
            continue
        if isinstance(payload, dict):
            return payload
    return {}


def _normalize_llm_rule_change(item: dict[str, Any]) -> dict[str, Any] | None:
    if not isinstance(item, dict):
        return None
    action = str(item.get("action", "") or "").strip()
    rule_id = str(item.get("rule_id", "") or "").strip()
    if action not in {"disable", "enable", "protect", "unprotect", "update_custom_rule"}:
        return None
    if not rule_id and action != "update_custom_rule":
        return None
    payload = item.get("payload") if isinstance(item.get("payload"), dict) else {}
    return {
        "rule_id": rule_id,
        "action": action,
        "reason": str(item.get("reason", "") or ""),
        "payload": dict(payload or {}),
    }


def _normalize_llm_experiment(item: dict[str, Any]) -> dict[str, Any] | None:
    if not isinstance(item, dict):
        return None
    metric_key = str(item.get("metric_key", "") or "").strip()
    issue_mode = str(item.get("issue_mode", "") or "").strip().lower()
    param_id = str(item.get("param_id", "") or "").strip()
    direction_text = str(item.get("direction", "") or "").strip().lower()
    if not metric_key or issue_mode not in {"high", "low", "flatline"} or not param_id:
        return None
    direction = 0
    if direction_text == "increase":
        direction = 1
    elif direction_text == "decrease":
        direction = -1
    elif direction_text == "keep":
        direction = 0
    else:
        return None
    step_scale = float(item.get("step_scale", 0.35) or 0.35)
    min_severity = float(item.get("min_severity", 0.05) or 0.05)
    cooldown_ticks = int(item.get("cooldown_ticks", 0) or 0)
    priority = int(item.get("priority", 1) or 1)
    confidence = float(item.get("confidence", 0.0) or 0.0)
    return {
        "experiment_id": str(item.get("experiment_id", "") or "").strip(),
        "metric_key": metric_key,
        "issue_mode": issue_mode,
        "param_id": param_id,
        "direction": direction,
        "direction_text": direction_text,
        "step_scale": max(0.05, min(0.6, step_scale)),
        "min_severity": max(0.01, min(1.0, min_severity)),
        "cooldown_ticks": max(0, min(200, cooldown_ticks)),
        "priority": max(1, min(10, priority)),
        "reason": str(item.get("reason", "") or ""),
        "confidence": max(0.0, min(1.0, confidence)),
        "title": str(item.get("title", "") or ""),
    }


def _normalize_llm_suggestion_payload(payload: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(payload, dict):
        return {}
    metric_findings = [dict(item) for item in (payload.get("metric_findings") or []) if isinstance(item, dict)]
    rule_changes = []
    for item in payload.get("suggested_rule_changes", []) or []:
        norm = _normalize_llm_rule_change(item) if isinstance(item, dict) else None
        if norm:
            rule_changes.append(norm)
    experiments = []
    for item in payload.get("suggested_experiments", []) or []:
        norm = _normalize_llm_experiment(item) if isinstance(item, dict) else None
        if norm:
            experiments.append(norm)
    return {
        "summary": str(payload.get("summary", "") or ""),
        "metric_findings": metric_findings,
        "suggested_rule_changes": rule_changes,
        "suggested_experiments": experiments,
        "notes": [str(x) for x in (payload.get("notes") or []) if str(x).strip()],
    }


def _normalize_llm_observation_review_item(item: dict[str, Any]) -> dict[str, Any] | None:
    if not isinstance(item, dict):
        return None
    observation_id = str(item.get("observation_id", "") or "").strip()
    rule_id = str(item.get("rule_id", "") or "").strip()
    action = str(item.get("action", "") or "").strip()
    if not observation_id or action not in {
        "keep_observing",
        "solidify",
        "reject_disable",
        "adjust_rule",
        "revert_change",
        "remove_custom_rule",
    }:
        return None
    payload = item.get("payload") if isinstance(item.get("payload"), dict) else {}
    return {
        "observation_id": observation_id,
        "rule_id": rule_id,
        "action": action,
        "reason": str(item.get("reason", "") or ""),
        "confidence": max(0.0, min(1.0, float(item.get("confidence", 0.0) or 0.0))),
        "payload": dict(payload or {}),
    }


def _normalize_llm_observation_review_payload(payload: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(payload, dict):
        return {}
    reviews = []
    for item in payload.get("observation_reviews", []) or []:
        norm = _normalize_llm_observation_review_item(item) if isinstance(item, dict) else None
        if norm:
            reviews.append(norm)
    return {
        "summary": str(payload.get("summary", "") or ""),
        "observation_reviews": reviews,
        "notes": [str(x) for x in (payload.get("notes") or []) if str(x).strip()],
    }


def _list_recent_llm_suggestions(*, limit: int = 40) -> list[dict[str, Any]]:
    out_dir = _llm_suggestions_dir()
    if not out_dir.exists():
        return []
    items: list[dict[str, Any]] = []
    for path in sorted(out_dir.glob("suggestion_*.json"), reverse=True):
        raw = _load_json_dict(path)
        if not raw:
            continue
        parsed = _normalize_llm_suggestion_payload(raw.get("parsed_json") if isinstance(raw.get("parsed_json"), dict) else {})
        report_text = str(raw.get("report_text", "") or "")
        auto_apply_result = raw.get("auto_apply_result") if isinstance(raw.get("auto_apply_result"), dict) else {}
        items.append(
            {
                "_path": str(path),
                "created_at_ms": int(raw.get("created_at_ms", 0) or 0),
                "run_id": str(raw.get("run_id", "") or "").strip(),
                "focus_metrics": [str(x).strip() for x in (raw.get("focus_metrics") or []) if str(x).strip()],
                "config": dict(raw.get("config", {}) or {}),
                "parsed_json": parsed,
                "counts": {
                    "metric_findings": len(parsed.get("metric_findings", [])),
                    "rule_changes": len(parsed.get("suggested_rule_changes", [])),
                    "experiments": len(parsed.get("suggested_experiments", [])),
                },
                "auto_apply_result": auto_apply_result,
                "auto_applied_at_ms": int(raw.get("auto_applied_at_ms", 0) or 0),
                "auto_apply_run_id": str(raw.get("auto_apply_run_id", "") or "").strip(),
                "report_excerpt": report_text[:1200],
            }
        )
        if len(items) >= int(limit):
            break
    return items


def analyze_auto_tuner_with_llm(
    *,
    app: Any | None = None,
    run_id: str = "",
    user_prompt: str = "",
    focus_metrics: list[str] | None = None,
) -> dict[str, Any]:
    cfg = _load_auto_tuner_llm_cfg_from_path(_llm_config_path(), fallback=load_review_config())
    if not cfg.enabled:
        return {"success": False, "error": "auto_tuner_llm_disabled"}
    if not cfg.model.strip():
        return {"success": False, "error": "auto_tuner_llm_model_missing"}
    if not cfg.base_url.strip():
        return {"success": False, "error": "auto_tuner_llm_base_url_missing"}

    state = read_auto_tuner_state(app=app)
    catalog = read_auto_tuner_catalog(app=app)
    rules = load_auto_tuner_rules()
    rule_catalog = build_auto_tuner_rule_catalog(app=app)
    focus_metrics = [str(x).strip() for x in (focus_metrics or []) if str(x).strip()]
    run_excerpt = _read_run_excerpt(run_id, max_prompt_chars=max(80_000, int(cfg.max_prompt_chars // 2))) if str(run_id or "").strip() else {}

    system_prompt = (
        "你是 AP 原型系统的自适应调参审稿器。"
        "你的任务不是泛泛而谈，而是基于长期指标、参数目录、规则目录、规则健康度、回滚点与近期运行数据，"
        "给出一份严谨、可审计、符合 AP 理论核心哲学的调参建议。"
        "请避免改动受保护规则（protected_rule_ids）以及与问题无关的参数。"
        "输出必须尽量具体，且最终必须包含一个 JSON 建议块。"
    )

    prompt_parts = [
        "以下是当前自适应调参器的关键上下文。",
        "请先判断哪些长期指标真正偏离了理论预期，再判断应该收紧、放松还是保持当前参数。",
        "如果某个问题更像数据环境变化而不是规则错误，请明确指出，不要盲目收缩全部参数。",
        "",
        "[当前配置]",
        json.dumps(load_auto_tuner_public_config(), ensure_ascii=False, indent=2),
        "",
        "[规则配置]",
        json.dumps(rules, ensure_ascii=False, indent=2),
        "",
        "[规则目录摘要]",
        json.dumps(rule_catalog.get("summary", {}), ensure_ascii=False, indent=2),
        "",
        "[调参器状态]",
        json.dumps(state, ensure_ascii=False, indent=2),
        "",
        "[参数目录摘要]",
        json.dumps(catalog.get("summary", {}), ensure_ascii=False, indent=2),
        "",
    ]
    if focus_metrics:
        prompt_parts.extend(["[重点关注指标]", json.dumps(focus_metrics, ensure_ascii=False)])
    if run_excerpt:
        prompt_parts.extend(
            [
                "",
                "[关联运行 manifest]",
                json.dumps(run_excerpt.get("manifest", {}), ensure_ascii=False, indent=2),
                "",
                "[关联运行 metrics 节选]",
                "```jsonl",
                str(run_excerpt.get("metrics_excerpt", "") or ""),
                "```",
            ]
        )
    if str(user_prompt or "").strip():
        prompt_parts.extend(["", "[研究者补充要求]", str(user_prompt).strip()])
    prompt_parts.extend(
        [
            "",
            "请输出两部分：",
            "1. 先写中文分析报告，说明你如何理解当前调参器状态、最值得处理的异常、建议的调参方向。",
            "2. 再输出一个 ```json``` 代码块，格式如下：",
            "```json",
            json.dumps(
                {
                    "summary": "",
                    "metric_findings": [{"metric_key": "", "status": "high|low|flatline|ok", "reason": "", "confidence": 0.0}],
                    "suggested_rule_changes": [{"rule_id": "", "action": "disable|enable|protect|unprotect|update_custom_rule", "reason": "", "payload": {}}],
                    "suggested_experiments": [
                        {
                            "experiment_id": "",
                            "title": "",
                            "metric_key": "",
                            "issue_mode": "high|low|flatline",
                            "param_id": "",
                            "direction": "increase|decrease",
                            "step_scale": 0.35,
                            "min_severity": 0.05,
                            "cooldown_ticks": 0,
                            "priority": 1,
                            "confidence": 0.0,
                            "reason": "",
                        }
                    ],
                    "notes": [],
                },
                ensure_ascii=False,
                indent=2,
            ),
            "```",
            "请注意：",
            "1. 只有在你非常确定时才建议 disable / protect 既有规则。",
            "2. 如果更适合做小步试验，请优先给 suggested_experiments，而不是直接永久修改。",
            "3. 不要建议修改 protected_rule_ids 中的规则。",
        ]
    )
    user_text = "\n".join(prompt_parts)
    if len(user_text) > int(cfg.max_prompt_chars):
        user_text = user_text[: int(cfg.max_prompt_chars)] + "\n\n[TRUNCATED]\n"

    res = call_openai_chat_completions(config=cfg, system_prompt=system_prompt, user_prompt=user_text)
    if not res.get("success", False):
        return {"success": False, "error": res.get("error", "request_failed"), "message": res.get("message", ""), "raw": res.get("raw", "")}
    payload = res.get("data", {})
    text = _extract_chat_text(payload if isinstance(payload, dict) else {})
    parsed_json = _normalize_llm_suggestion_payload(_extract_json_code_block(text))
    suggestion = {
        "created_at_ms": _now_ms(),
        "run_id": str(run_id or "").strip(),
        "focus_metrics": focus_metrics,
        "config": {"base_url": cfg.base_url, "model": cfg.model, "api_key_masked": mask_api_key(cfg.api_key)},
        "report_text": text,
        "parsed_json": parsed_json,
        "raw_response": payload,
        "protected_rule_ids": list(rules.get("protected_rule_ids", [])),
    }
    out_dir = _llm_suggestions_dir()
    out_dir.mkdir(parents=True, exist_ok=True)
    file_name = f"suggestion_{_now_ms()}_{storage.safe_slug(run_id or 'global')}.json"
    out_path = out_dir / file_name
    out_path.write_text(json.dumps(suggestion, ensure_ascii=False, indent=2), encoding="utf-8")
    return {"success": True, "suggestion_path": str(out_path), "report_text": text, "parsed_json": parsed_json, "config": {"model": cfg.model, "base_url": cfg.base_url}}
