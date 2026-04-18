# -*- coding: utf-8 -*-
"""
Episode Dataset Protocol (YAML) + Expander (-> JSONL)
=====================================================

This module defines a small, strict dataset protocol that is:
- deterministic
- auditable
- easy to version-control

It is shared by:
- CLI tools in `tools/`
- the Observatory web UI experiment panel (future)
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable


class DatasetValidationError(ValueError):
    pass


@dataclass(frozen=True)
class DatasetMeta:
    dataset_id: str
    seed: int
    time_basis: str
    tick_dt_ms: int | None


def _as_str(v: Any) -> str:
    return str(v) if v is not None else ""


def _as_int(v: Any, *, where: str) -> int:
    try:
        return int(v)
    except Exception as exc:
        raise DatasetValidationError(f"{where} must be int, got: {v!r}") from exc


def _ensure_list(v: Any, *, where: str) -> list:
    if v is None:
        return []
    if isinstance(v, list):
        return v
    raise DatasetValidationError(f"{where} must be a list, got: {type(v).__name__}")


def validate_meta(raw: dict[str, Any]) -> DatasetMeta:
    dataset_id = _as_str(raw.get("dataset_id", "")).strip()
    if not dataset_id:
        raise DatasetValidationError("dataset_id is required.")

    seed = _as_int(raw.get("seed", 0), where="seed")
    time_basis = _as_str(raw.get("time_basis", "")).strip().lower()
    if time_basis not in {"tick", "wallclock"}:
        raise DatasetValidationError("time_basis must be 'tick' or 'wallclock'.")

    tick_dt_ms: int | None = None
    if time_basis == "tick":
        if raw.get("tick_dt_ms", None) is None:
            raise DatasetValidationError("tick_dt_ms is required when time_basis=tick.")
        tick_dt_ms = _as_int(raw.get("tick_dt_ms"), where="tick_dt_ms")
        if tick_dt_ms <= 0:
            raise DatasetValidationError("tick_dt_ms must be > 0.")

    return DatasetMeta(dataset_id=dataset_id, seed=seed, time_basis=time_basis, tick_dt_ms=tick_dt_ms)


def normalize_tick(tick: Any, *, where: str) -> dict[str, Any]:
    # Allow a shortcut: `- \"hello\"` means `{\"text\": \"hello\"}`
    if isinstance(tick, str):
        return {"text": tick}
    if not isinstance(tick, dict):
        raise DatasetValidationError(f"{where} must be a mapping (dict) or string, got: {type(tick).__name__}")

    has_text = "text" in tick
    has_empty = bool(tick.get("empty", False))
    if has_text and has_empty:
        raise DatasetValidationError(f"{where} has both text and empty=true. Choose one.")
    if not has_text and not has_empty:
        raise DatasetValidationError(f"{where} must have 'text' or 'empty: true'.")

    out = dict(tick)
    # Optional per-tick repeat (for compact datasets, especially long empty gaps).
    # 说明：episode.repeat 是“整段 ticks 模板”重复；tick.repeat 是“单个 tick”重复。
    if "repeat" in out:
        rep = _as_int(out.get("repeat", 1), where=f"{where}.repeat")
        if rep <= 0:
            raise DatasetValidationError(f"{where}.repeat must be >= 1.")
        out["repeat"] = rep
    if has_empty:
        out["text"] = ""
        out["empty"] = True
    else:
        out["text"] = _as_str(out.get("text", ""))
        out.pop("empty", None)
    return out


def normalize_episode(ep: Any, *, index: int) -> dict[str, Any]:
    where = f"episodes[{index}]"
    if not isinstance(ep, dict):
        raise DatasetValidationError(f"{where} must be a mapping (dict).")

    ep_id = _as_str(ep.get("id", "")).strip()
    if not ep_id:
        raise DatasetValidationError(f"{where}.id is required.")

    repeat = int(ep.get("repeat", 1) or 1)
    if repeat <= 0:
        raise DatasetValidationError(f"{where}.repeat must be >= 1.")

    tags_raw = ep.get("tags", [])
    tags = []
    if tags_raw is not None:
        if not isinstance(tags_raw, list):
            raise DatasetValidationError(f"{where}.tags must be a list.")
        tags = [str(x) for x in tags_raw if str(x).strip()]

    ticks_raw = ep.get("ticks", None)
    if ticks_raw is None:
        raise DatasetValidationError(f"{where}.ticks is required.")
    ticks_list = _ensure_list(ticks_raw, where=f"{where}.ticks")
    if not ticks_list:
        raise DatasetValidationError(f"{where}.ticks must not be empty.")

    ticks_norm: list[dict[str, Any]] = []
    for j, t in enumerate(ticks_list):
        ticks_norm.append(normalize_tick(t, where=f"{where}.ticks[{j}]"))

    out = dict(ep)
    out["id"] = ep_id
    out["repeat"] = repeat
    out["tags"] = tags
    out["ticks"] = ticks_norm
    return out


def validate_and_normalize_dataset(raw: dict[str, Any]) -> dict[str, Any]:
    meta = validate_meta(raw)
    episodes_raw = raw.get("episodes", None)
    if episodes_raw is None:
        raise DatasetValidationError("episodes is required.")
    episodes_list = _ensure_list(episodes_raw, where="episodes")
    if not episodes_list:
        raise DatasetValidationError("episodes must not be empty.")

    episodes_norm: list[dict[str, Any]] = []
    for i, ep in enumerate(episodes_list):
        episodes_norm.append(normalize_episode(ep, index=i))

    out = dict(raw)
    out["_meta"] = {
        "dataset_id": meta.dataset_id,
        "seed": meta.seed,
        "time_basis": meta.time_basis,
        "tick_dt_ms": meta.tick_dt_ms,
    }
    out["dataset_id"] = meta.dataset_id
    out["seed"] = meta.seed
    out["time_basis"] = meta.time_basis
    if meta.time_basis == "tick":
        out["tick_dt_ms"] = meta.tick_dt_ms
    out["episodes"] = episodes_norm
    return out


def estimate_total_ticks(dataset: dict[str, Any]) -> int:
    episodes = dataset.get("episodes", [])
    total = 0
    if not isinstance(episodes, list):
        return 0
    for ep in episodes:
        if not isinstance(ep, dict):
            continue
        repeat = int(ep.get("repeat", 1) or 1)
        ticks = ep.get("ticks", [])
        if not isinstance(ticks, list):
            continue
        per_ep = 0
        for t in ticks:
            if isinstance(t, dict):
                try:
                    per_ep += max(1, int(t.get("repeat", 1) or 1))
                except Exception:
                    per_ep += 1
            else:
                per_ep += 1
        total += max(0, repeat) * int(per_ep)
    return int(total)


def expand_dataset(dataset: dict[str, Any]) -> Iterable[dict[str, Any]]:
    meta = dataset.get("_meta", {})
    dataset_id = str(meta.get("dataset_id", "") or dataset.get("dataset_id", ""))
    seed = int(meta.get("seed", 0) or dataset.get("seed", 0) or 0)
    time_basis = str(meta.get("time_basis", "") or dataset.get("time_basis", "")).strip().lower()
    tick_dt_ms = meta.get("tick_dt_ms", dataset.get("tick_dt_ms", None))
    tick_dt_ms = int(tick_dt_ms) if tick_dt_ms is not None else None

    episodes = dataset.get("episodes", [])
    tick_index = 0
    for ep in episodes:
        ep_id = str(ep.get("id", ""))
        repeat = int(ep.get("repeat", 1) or 1)
        tags = ep.get("tags", [])
        ticks = ep.get("ticks", [])
        for rep_i in range(repeat):
            for j, t in enumerate(ticks):
                tick_repeat = 1
                try:
                    tick_repeat = max(1, int(t.get("repeat", 1) or 1))
                except Exception:
                    tick_repeat = 1

                for rep_j in range(tick_repeat):
                    text = str(t.get("text", "") or "")
                    is_empty = bool(t.get("empty", False)) or (text == "")
                    item: dict[str, Any] = {
                        "dataset_id": dataset_id,
                        "seed": seed,
                        "time_basis": time_basis,
                        "tick_dt_ms": tick_dt_ms,
                        "tick_index": tick_index,
                        "episode_id": ep_id,
                        "episode_repeat_index": rep_i,
                        "tick_in_episode_index": j,
                        "tick_repeat_index": rep_j,
                        "tags": list(tags) if isinstance(tags, list) else [],
                        "input_text": "" if is_empty else text,
                        "input_is_empty": is_empty,
                    }
                    # Optional labels pass-through (future use).
                    labels = t.get("labels")
                    if isinstance(labels, dict) and labels:
                        item["labels"] = labels

                    yield item
                    tick_index += 1

