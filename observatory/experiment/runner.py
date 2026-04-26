# -*- coding: utf-8 -*-
"""
Headless Experiment Runner
=========================

Runs a dataset (YAML episode template or expanded JSONL ticks) against an
`ObservatoryApp` instance and writes paper-friendly metrics to disk.
"""

from __future__ import annotations

import json
import os
import sys
import time
import contextlib
from collections import deque
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Iterable

from . import dataset as ds
from .expectation_contracts import ExpectationContractEngine, ExpectationContractError
from .io import ExperimentIOError, iter_jsonl, load_yaml_file, sha256_file, write_jsonl
from .metrics import extract_tick_metrics
from .storage import DatasetFileRef, ExperimentStorageError, make_run_dir, resolve_dataset_file, safe_slug
from .auto_tuner import AutoTuner


class ExperimentRunnerError(RuntimeError):
    pass


@dataclass(frozen=True)
class RunOptions:
    reset_mode: str = "keep"  # keep | clear_runtime | clear_all
    export_json: bool = False
    export_html: bool = False
    # Adaptive auto tuner (self-adaptive parameter tuning)
    auto_tune_enabled: bool = False
    auto_tune_short_term: bool = True
    auto_tune_long_term: bool = True
    # time sensor override during the run (None means keep current runtime config)
    time_sensor_time_basis: str | None = None  # tick | wallclock | None
    tick_interval_sec: float | None = None  # for display only (time_sensor config field)
    max_ticks: int | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "reset_mode": self.reset_mode,
            "export_json": bool(self.export_json),
            "export_html": bool(self.export_html),
            "auto_tune_enabled": bool(self.auto_tune_enabled),
            "auto_tune_short_term": bool(self.auto_tune_short_term),
            "auto_tune_long_term": bool(self.auto_tune_long_term),
            "time_sensor_time_basis": self.time_sensor_time_basis,
            "tick_interval_sec": self.tick_interval_sec,
            "max_ticks": self.max_ticks,
        }


def _now_ms() -> int:
    return int(time.time() * 1000)


def _read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def make_run_id(*, dataset_id: str) -> str:
    # Example: exp_smoke_100_v0_20260418_123456_1a2b
    base = safe_slug(dataset_id or "dataset", fallback="dataset")
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    # small random suffix (deterministic not required; just to avoid collisions)
    suffix = f"{os.getpid() % 10000:04d}"
    return f"exp_{base}_{stamp}_{suffix}"


def _count_jsonl_lines(path: Path) -> int:
    n = 0
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            if line.strip():
                n += 1
    return n


def _compact_auto_tuner_tick_result(result: dict[str, Any] | None) -> dict[str, Any] | None:
    if not isinstance(result, dict):
        return None
    compact: dict[str, Any] = {
        "enabled": bool(result.get("enabled", False)),
        "applied": bool(result.get("applied", False)),
        "reason": str(result.get("reason", "") or ""),
        "applied_count": int(result.get("applied_count", 0) or 0),
    }
    updates = []
    for item in (result.get("applied_updates") or []):
        if not isinstance(item, dict):
            continue
        updates.append(
            {
                "rule_id": str(item.get("rule_id", "") or ""),
                "param": str(item.get("param", "") or ""),
                "metric_key": str(item.get("metric_key", "") or ""),
                "issue_mode": str(item.get("issue_mode", "") or ""),
                "reason": str(item.get("reason", "") or ""),
                "from": item.get("from"),
                "to": item.get("to"),
            }
        )
    if updates:
        compact["applied_updates"] = updates[:8]
    return compact


def _resolve_time_sensor_runtime_overrides(
    *,
    normalized_doc: dict[str, Any] | None,
    options: RunOptions,
) -> tuple[str | None, float | None]:
    """
    Resolve the effective time-sensor runtime override for this run.

    Priority:
    1. Explicit RunOptions override
    2. Dataset-declared time basis / tick interval
    3. Keep current runtime config (None)
    """

    basis = options.time_sensor_time_basis
    tick_interval_sec = options.tick_interval_sec

    meta_basis = None
    meta_tick_dt_ms = None
    if isinstance(normalized_doc, dict):
        meta_basis = str(normalized_doc.get("time_basis", "") or "").strip().lower() or None
        try:
            raw_tick_dt_ms = normalized_doc.get("tick_dt_ms", None)
            if raw_tick_dt_ms is not None:
                meta_tick_dt_ms = int(raw_tick_dt_ms)
        except Exception:
            meta_tick_dt_ms = None

    if basis not in {"tick", "wallclock"}:
        if meta_basis in {"tick", "wallclock"}:
            basis = meta_basis
        else:
            basis = None

    if tick_interval_sec is None and meta_tick_dt_ms is not None and meta_tick_dt_ms > 0:
        tick_interval_sec = float(meta_tick_dt_ms) / 1000.0

    return basis, tick_interval_sec


def load_dataset_ticks(
    *,
    dataset_ref: DatasetFileRef,
    preview_limit: int | None = None,
) -> tuple[str, str, dict[str, Any] | None, Iterable[dict[str, Any]], int | None]:
    """Load dataset ticks generator.

    Returns:
      (dataset_id, dataset_sha256, normalized_dataset_doc, ticks_iter, total_ticks)
    - For YAML: normalized_dataset_doc is returned and total_ticks is exact.
    - For JSONL: normalized_dataset_doc is None; total_ticks is best-effort (counts lines).
    """

    path = resolve_dataset_file(dataset_ref)
    if not path.exists() or not path.is_file():
        raise ExperimentRunnerError(f"Dataset file not found: {path}")

    digest = sha256_file(path)
    suffix = path.suffix.lower()
    if suffix in {".yaml", ".yml"}:
        raw = load_yaml_file(path)
        normalized = ds.validate_and_normalize_dataset(raw)
        dataset_id = str(normalized.get("dataset_id", "") or "").strip() or path.stem
        total = ds.estimate_total_ticks(normalized)
        ticks = ds.expand_dataset(normalized)
        if preview_limit is not None and preview_limit > 0:
            ticks = list(ticks)[: int(preview_limit)]
        return dataset_id, digest, normalized, ticks, total

    if suffix == ".jsonl":
        # JSONL can be either "expanded ticks" output of the expander,
        # or a user-provided per-tick stream with at least input_text/input_is_empty.
        total = _count_jsonl_lines(path)
        ticks = iter_jsonl(path)
        if preview_limit is not None and preview_limit > 0:
            ticks = list(ticks)[: int(preview_limit)]
        # Derive dataset_id from first item (if any), else filename stem.
        derived_id = path.stem
        try:
            first = None
            if isinstance(ticks, list):
                first = ticks[0] if ticks else None
            else:
                # preview_limit is None; do not consume generator here
                first = None
            if isinstance(first, dict) and str(first.get("dataset_id", "") or "").strip():
                derived_id = str(first.get("dataset_id") or "").strip()
        except Exception:
            derived_id = path.stem
        return derived_id, digest, None, ticks, total

    raise ExperimentRunnerError(f"Unsupported dataset file type: {path.suffix} (expected .yaml/.yml/.jsonl)")


def run_dataset(
    *,
    app,
    app_lock: Any | None = None,
    dataset_ref: DatasetFileRef,
    options: RunOptions,
    run_id: str | None = None,
    progress_cb: Callable[[dict[str, Any]], None] | None = None,
    cancel_cb: Callable[[], bool] | None = None,
) -> dict[str, Any]:
    """Run a dataset against the provided ObservatoryApp.

    This function is designed to be called in a background thread from the web server.
    It is intentionally synchronous and writes results to disk incrementally.
    """

    progress_cb = progress_cb or (lambda _: None)
    cancel_cb = cancel_cb or (lambda: False)
    lock_ctx = app_lock if app_lock is not None else contextlib.nullcontext()

    dataset_id, dataset_sha, normalized_doc, ticks_iter, total_ticks = load_dataset_ticks(dataset_ref=dataset_ref)
    if run_id is None:
        run_id = make_run_id(dataset_id=dataset_id)

    run_dir = make_run_dir(run_id)
    started_wall_ms = _now_ms()
    manifest_path = run_dir / "manifest.json"
    metrics_path = run_dir / "metrics.jsonl"
    expectation_events_path = run_dir / "expectation_contract_events.jsonl"
    normalized_path = run_dir / "dataset.normalized.yaml"
    dataset_copy_path = run_dir / f"dataset.source{Path(resolve_dataset_file(dataset_ref)).suffix.lower()}"

    # Copy source dataset for audit (small, safe: outputs/ is gitignored).
    try:
        dataset_copy_path.write_text(_read_text(resolve_dataset_file(dataset_ref)), encoding="utf-8")
    except Exception:
        # Non-fatal.
        pass

    if normalized_doc is not None:
        try:
            from .io import dump_yaml

            normalized_path.write_text(dump_yaml(normalized_doc), encoding="utf-8")
        except Exception:
            pass

    # Prepare initial manifest
    tick_summary = ds.summarize_tick_counts(normalized_doc) if isinstance(normalized_doc, dict) else {}
    planned_tick_summary = dict(tick_summary) if tick_summary else {}
    if isinstance(normalized_doc, dict) and options.max_ticks is not None and int(options.max_ticks) > 0:
        try:
            planned_items = []
            limit = max(0, int(options.max_ticks))
            for idx, item in enumerate(ds.expand_dataset(normalized_doc)):
                if idx >= limit:
                    break
                planned_items.append(item)
            planned_tick_summary = ds.summarize_expanded_tick_items(planned_items)
        except Exception:
            planned_tick_summary = dict(tick_summary) if tick_summary else {}
    manifest: dict[str, Any] = {
        "run_id": run_id,
        "status": "running",
        "dataset": {
            "dataset_id": dataset_id,
            "dataset_sha256": dataset_sha,
            "dataset_ref": dataset_ref.to_dict(),
            "dataset_path": str(resolve_dataset_file(dataset_ref)),
            "total_ticks": int(total_ticks) if total_ticks is not None else None,
            "effective_text_ticks": planned_tick_summary.get("effective_text_ticks") if planned_tick_summary else None,
            "empty_ticks": planned_tick_summary.get("empty_ticks") if planned_tick_summary else None,
            "labeled_ticks": planned_tick_summary.get("labeled_ticks") if planned_tick_summary else None,
        },
        "options": options.to_dict(),
        "runtime": {
            "python": sys.version,
            "platform": sys.platform,
            "pid": os.getpid(),
        },
        "started_at_ms": int(started_wall_ms),
        "finished_at_ms": 0,
        "tick_done": 0,
        "source_tick_done": 0,
        "synthetic_tick_done": 0,
        "executed_tick_done_total": 0,
        "tick_planned": int(total_ticks) if total_ticks is not None else None,
        "errors": [],
        "expectation_contracts": {
            "registered_count": 0,
            "success_count": 0,
            "failure_count": 0,
            "synthetic_tick_count": 0,
            "pending_count": 0,
            "events_path": str(expectation_events_path),
        },
    }

    # Adaptive tuner (optional)
    tuner: AutoTuner | None = None
    if bool(options.auto_tune_enabled):
        tuner = AutoTuner(
            app=app,
            run_dir=run_dir,
            enabled=True,
            enable_short_term=bool(options.auto_tune_short_term),
            enable_long_term=bool(options.auto_tune_long_term),
        )
        try:
            with lock_ctx:
                applied = tuner.prepare_and_apply_overrides(trace_id="exp_prepare")
            manifest["auto_tuner"] = {"enabled": True, "prepare": applied}
        except Exception as exc:
            manifest["auto_tuner"] = {"enabled": True, "prepare_error": str(exc)}
    else:
        manifest["auto_tuner"] = {"enabled": False}

    effective_time_sensor_basis, effective_tick_interval_sec = _resolve_time_sensor_runtime_overrides(
        normalized_doc=normalized_doc,
        options=options,
    )
    manifest["time_sensor_runtime_override"] = {
        "time_basis": effective_time_sensor_basis,
        "tick_interval_sec": effective_tick_interval_sec,
    }

    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")

    # Apply pre-run options (save old values, restore on exit)
    old_config = {}
    old_time_sensor = {}
    try:
        with lock_ctx:
            try:
                old_config = dict(getattr(app, "_config", {}) or {})
            except Exception:
                old_config = {}
            try:
                old_time_sensor = dict(getattr(app, "time_sensor")._config or {})  # type: ignore[attr-defined]
            except Exception:
                old_time_sensor = {}

        # Ensure experiment runs follow the current string-mode theory path when the dataset
        # is intended for companion / dialogue style runs. Without this, HDB stimulus-level
        # string projections may never be seeded, causing `stimulus_new_structure_count` to
        # stay at 0 even though the frontend/user observes long string objects elsewhere.
        with lock_ctx:
            try:
                app._config["enable_goal_b_char_sa_string_mode"] = True  # type: ignore[attr-defined]
                sensor_override = app._sensor_config_override() if hasattr(app, "_sensor_config_override") else {}  # type: ignore[attr-defined]
                app.sensor._config.update(sensor_override)  # type: ignore[attr-defined]
                app.sensor._normalizer.update_config(app.sensor._config)  # type: ignore[attr-defined]
                app.sensor._segmenter.update_config(app.sensor._config)  # type: ignore[attr-defined]
                app.sensor._scorer.update_config(app.sensor._config)  # type: ignore[attr-defined]
                app.sensor._echo_mgr.update_config(app.sensor._config)  # type: ignore[attr-defined]
                hdb_override = app._hdb_config_override() if hasattr(app, "_hdb_config_override") else {}  # type: ignore[attr-defined]
                app.hdb._config.update(hdb_override)  # type: ignore[attr-defined]
                app.hdb._stimulus.update_config(app.hdb._config)  # type: ignore[attr-defined]
                app.hdb._cut.update_config(app.hdb._config)  # type: ignore[attr-defined]
            except Exception:
                pass

        # Reset modes (mutates app state)
        with lock_ctx:
            if options.reset_mode == "clear_all":
                if hasattr(app, "_clear_runtime_modules"):
                    app._clear_runtime_modules(  # type: ignore[attr-defined]
                        clear_hdb=True,
                        trace_prefix="exp_clear_all",
                        reason="experiment_reset",
                        operator="researcher",
                    )
                else:
                    app.sensor.clear_echo_pool(trace_id="exp_clear_sensor")  # type: ignore[attr-defined]
                    if hasattr(getattr(app, "time_sensor", None), "clear_runtime_state"):
                        app.time_sensor.clear_runtime_state(trace_id="exp_clear_time_sensor", reason="experiment_reset")  # type: ignore[attr-defined]
                    if hasattr(getattr(app, "action", None), "clear_runtime_state"):
                        app.action.clear_runtime_state(trace_id="exp_clear_action", reason="experiment_reset")  # type: ignore[attr-defined]
                    if hasattr(getattr(app, "cognitive_stitching", None), "clear_runtime_state"):
                        app.cognitive_stitching.clear_runtime_state(trace_id="exp_clear_cs", reason="experiment_reset")  # type: ignore[attr-defined]
                    if hasattr(getattr(app, "attention", None), "clear_runtime_state"):
                        app.attention.clear_runtime_state(trace_id="exp_clear_attention", reason="experiment_reset")  # type: ignore[attr-defined]
                    app.pool.clear_state_pool(trace_id="exp_clear_pool", reason="experiment_reset", operator="researcher")  # type: ignore[attr-defined]
                    app.hdb.clear_hdb(trace_id="exp_clear_hdb", reason="experiment_reset", operator="researcher")  # type: ignore[attr-defined]
                    app._last_report = None  # type: ignore[attr-defined]
                    app._report_history = []  # type: ignore[attr-defined]
            elif options.reset_mode == "clear_runtime":
                if hasattr(app, "_clear_runtime_modules"):
                    app._clear_runtime_modules(  # type: ignore[attr-defined]
                        clear_hdb=False,
                        trace_prefix="exp_clear_runtime",
                        reason="experiment_reset",
                        operator="researcher",
                    )
                else:
                    app.sensor.clear_echo_pool(trace_id="exp_clear_sensor")  # type: ignore[attr-defined]
                    if hasattr(getattr(app, "time_sensor", None), "clear_runtime_state"):
                        app.time_sensor.clear_runtime_state(trace_id="exp_clear_time_sensor", reason="experiment_reset")  # type: ignore[attr-defined]
                    if hasattr(getattr(app, "action", None), "clear_runtime_state"):
                        app.action.clear_runtime_state(trace_id="exp_clear_action", reason="experiment_reset")  # type: ignore[attr-defined]
                    if hasattr(getattr(app, "cognitive_stitching", None), "clear_runtime_state"):
                        app.cognitive_stitching.clear_runtime_state(trace_id="exp_clear_cs", reason="experiment_reset")  # type: ignore[attr-defined]
                    if hasattr(getattr(app, "attention", None), "clear_runtime_state"):
                        app.attention.clear_runtime_state(trace_id="exp_clear_attention", reason="experiment_reset")  # type: ignore[attr-defined]
                    if hasattr(getattr(app, "hdb", None), "clear_runtime_state"):
                        app.hdb.clear_runtime_state(trace_id="exp_clear_hdb_runtime", reason="experiment_reset")  # type: ignore[attr-defined]
                    app.pool.clear_state_pool(trace_id="exp_clear_pool", reason="experiment_reset", operator="researcher")  # type: ignore[attr-defined]
                    app._last_report = None  # type: ignore[attr-defined]
                    app._report_history = []  # type: ignore[attr-defined]
            elif options.reset_mode == "keep":
                pass
            else:
                raise ExperimentRunnerError(f"Unknown reset_mode: {options.reset_mode}")

        # Disable per-tick exports by default for long runs (overrideable)
        # Note: we mutate app._config in-place for the duration of this run.
        with lock_ctx:
            try:
                app._config["export_json"] = bool(options.export_json)  # type: ignore[attr-defined]
                app._config["export_html"] = bool(options.export_html)  # type: ignore[attr-defined]
                app._config["auto_open_html_report"] = False  # type: ignore[attr-defined]
            except Exception:
                pass

        # time_sensor override (runtime-only)
        with lock_ctx:
            if effective_time_sensor_basis in {"tick", "wallclock"}:
                try:
                    app.time_sensor._config["time_basis"] = str(effective_time_sensor_basis)  # type: ignore[attr-defined]
                except Exception:
                    pass
            if effective_tick_interval_sec is not None:
                try:
                    app.time_sensor._config["tick_interval_sec"] = float(effective_tick_interval_sec)  # type: ignore[attr-defined]
                except Exception:
                    pass

        # Main loop
        source_tick_done = 0
        synthetic_tick_done = 0
        executed_tick_done_total = 0
        max_ticks = options.max_ticks
        if max_ticks is not None:
            max_ticks = max(1, int(max_ticks))

        expectation_engine = ExpectationContractEngine()
        synthetic_queue: deque[dict[str, Any]] = deque()
        source_iter = iter(ticks_iter)
        source_exhausted = False

        with metrics_path.open("w", encoding="utf-8") as mf, expectation_events_path.open("w", encoding="utf-8") as ef:
            while True:
                if cancel_cb():
                    manifest["status"] = "cancelled"
                    break

                tick: dict[str, Any] | None = None
                tick_is_synthetic = False

                if synthetic_queue:
                    tick = synthetic_queue.popleft()
                    tick_is_synthetic = True
                else:
                    if not source_exhausted:
                        if max_ticks is not None and source_tick_done >= max_ticks:
                            manifest["status"] = "stopped_max_ticks"
                            source_exhausted = True
                        else:
                            try:
                                tick = next(source_iter)
                            except StopIteration:
                                source_exhausted = True
                    if tick is None and source_exhausted:
                        settle_res = expectation_engine.settle_on_run_end()
                        for event in settle_res.get("events", []) or []:
                            ef.write(json.dumps(event, ensure_ascii=False))
                            ef.write("\n")
                        for synthetic_tick in settle_res.get("synthetic_ticks", []) or []:
                            if isinstance(synthetic_tick, dict):
                                synthetic_queue.append(synthetic_tick)
                        if synthetic_queue:
                            try:
                                ef.flush()
                            except Exception:
                                pass
                            continue
                        break

                if not isinstance(tick, dict):
                    continue

                text = str(tick.get("input_text", "") or "")
                is_empty = bool(tick.get("input_is_empty", False)) or (text == "")
                labels = tick.get("labels") if isinstance(tick.get("labels"), dict) else None
                with lock_ctx:
                    report = app.run_cycle(text=None if is_empty else text, labels=labels)  # type: ignore[attr-defined]
                metrics = extract_tick_metrics(report=report, dataset_tick=tick)
                mf.write(json.dumps(metrics, ensure_ascii=False))
                mf.write("\n")
                executed_tick_done_total += 1

                if tick_is_synthetic:
                    synthetic_tick_done += 1
                else:
                    source_tick_done += 1
                    try:
                        contract_res = expectation_engine.on_source_tick(
                            tick=tick,
                            report=report,
                            metrics=metrics,
                            source_tick_cursor=source_tick_done,
                        )
                    except ExpectationContractError as exc:
                        raise ExperimentRunnerError(f"Expectation contract error: {exc}") from exc
                    for event in contract_res.get("events", []) or []:
                        ef.write(json.dumps(event, ensure_ascii=False))
                        ef.write("\n")
                    for synthetic_tick in contract_res.get("synthetic_ticks", []) or []:
                        if isinstance(synthetic_tick, dict):
                            synthetic_queue.append(synthetic_tick)

                manifest["tick_done"] = int(source_tick_done)
                manifest["source_tick_done"] = int(source_tick_done)
                manifest["synthetic_tick_done"] = int(synthetic_tick_done)
                manifest["executed_tick_done_total"] = int(executed_tick_done_total)
                manifest["expectation_contracts"] = {
                    **dict(manifest.get("expectation_contracts", {}) or {}),
                    **expectation_engine.snapshot(),
                    "events_path": str(expectation_events_path),
                }

                # Short-term auto tuning (best-effort, should never crash the run)
                short_term_res: dict[str, Any] | None = None
                if tuner is not None:
                    try:
                        # Keep tuning within the same app lock to avoid races with run_cycle/web reads.
                        with lock_ctx:
                            short_term_res = tuner.on_tick(metrics=metrics)
                    except Exception:
                        short_term_res = {"enabled": True, "applied": False, "reason": "short_term_error"}

                progress_cb(
                    {
                        "run_id": run_id,
                        "status": manifest.get("status", "running"),
                        "tick_done": source_tick_done,
                        "source_tick_done": source_tick_done,
                        "synthetic_tick_done": synthetic_tick_done,
                        "executed_tick_done_total": executed_tick_done_total,
                        "tick_planned": total_ticks,
                        "tick_index": int(metrics.get("tick_index", source_tick_done - 1) or (source_tick_done - 1)),
                        "tick_source": str(metrics.get("tick_source", "dataset") or "dataset"),
                        "auto_tuner_short_term": _compact_auto_tuner_tick_result(short_term_res),
                    }
                )
                try:
                    mf.flush()
                    ef.flush()
                except Exception:
                    pass

        manifest["tick_done"] = int(source_tick_done)
        manifest["source_tick_done"] = int(source_tick_done)
        manifest["synthetic_tick_done"] = int(synthetic_tick_done)
        manifest["executed_tick_done_total"] = int(executed_tick_done_total)
        manifest["expectation_contracts"] = {
            **dict(manifest.get("expectation_contracts", {}) or {}),
            **expectation_engine.snapshot(),
            "events_path": str(expectation_events_path),
        }
        if manifest.get("status") == "running":
            manifest["status"] = "completed"
        manifest["finished_at_ms"] = _now_ms()

        # Long-term tuning at completion
        if tuner is not None:
            try:
                # Re-read metrics.jsonl (bounded by run length; acceptable for paper runs).
                from .io import iter_jsonl

                all_rows = list(iter_jsonl(metrics_path))
                with lock_ctx:
                    long_res = tuner.on_run_complete(all_metrics=all_rows, trace_id="exp_complete")
                manifest.setdefault("auto_tuner", {})
                manifest["auto_tuner"]["long_term"] = long_res
            except Exception as exc:
                manifest.setdefault("auto_tuner", {})
                manifest["auto_tuner"]["long_term_error"] = str(exc)

        # Idle-time consolidation (best-effort): keep long-run storage/runtime from drifting too far.
        # - Safe to run after a dataset completes (or is stopped/cancelled) to support acceptance runs.
        idle_cons: dict[str, Any] = {}
        if hasattr(app, "hdb") and hasattr(app.hdb, "idle_consolidate_hdb"):
            try:
                with lock_ctx:
                    idle_cons["hdb"] = app.hdb.idle_consolidate_hdb(
                        trace_id="exp_idle_consolidate",
                        reason="experiment_run_completed",
                        rebuild_pointer_index=True,
                        apply_soft_limits=True,
                    )
            except Exception as exc:
                idle_cons["hdb_error"] = str(exc)

        if hasattr(app, "cognitive_stitching") and hasattr(app.cognitive_stitching, "idle_consolidate"):
            try:
                with lock_ctx:
                    idle_cons["cognitive_stitching"] = app.cognitive_stitching.idle_consolidate(
                        hdb=app.hdb,
                        trace_id="exp_idle_consolidate_cs",
                        tick_id="exp_idle_consolidate",
                        reason="experiment_run_completed",
                    )
            except Exception as exc:
                idle_cons["cognitive_stitching_error"] = str(exc)

        if idle_cons:
            manifest["idle_consolidation"] = idle_cons

        manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
        progress_cb(
            {
                "run_id": run_id,
                "status": manifest.get("status"),
                "tick_done": manifest.get("tick_done"),
                "source_tick_done": manifest.get("source_tick_done"),
                "synthetic_tick_done": manifest.get("synthetic_tick_done"),
                "executed_tick_done_total": manifest.get("executed_tick_done_total"),
                "tick_planned": manifest.get("tick_planned"),
            }
        )
        return {"success": True, "run_id": run_id, "run_dir": str(run_dir), "manifest": manifest}

    except (ExperimentIOError, ExperimentStorageError, ds.DatasetValidationError, ExperimentRunnerError) as exc:
        manifest["status"] = "failed"
        manifest["finished_at_ms"] = _now_ms()
        manifest.setdefault("errors", [])
        manifest["errors"].append({"type": type(exc).__name__, "message": str(exc)})
        try:
            manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
        except Exception:
            pass
        progress_cb({"run_id": run_id, "status": "failed", "error": str(exc)})
        return {"success": False, "run_id": run_id, "run_dir": str(run_dir), "error": str(exc)}

    except Exception as exc:  # pragma: no cover (unexpected)
        manifest["status"] = "failed"
        manifest["finished_at_ms"] = _now_ms()
        manifest.setdefault("errors", [])
        manifest["errors"].append({"type": type(exc).__name__, "message": str(exc)})
        try:
            manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
        except Exception:
            pass
        progress_cb({"run_id": run_id, "status": "failed", "error": str(exc)})
        return {"success": False, "run_id": run_id, "run_dir": str(run_dir), "error": str(exc)}

    finally:
        # Restore configs (best-effort)
        with lock_ctx:
            try:
                if old_config:
                    app._config.clear()  # type: ignore[attr-defined]
                    app._config.update(old_config)  # type: ignore[attr-defined]
            except Exception:
                pass
            try:
                if old_time_sensor:
                    app.time_sensor._config.clear()  # type: ignore[attr-defined]
                    app.time_sensor._config.update(old_time_sensor)  # type: ignore[attr-defined]
            except Exception:
                pass


def export_expanded_ticks(
    *,
    dataset_ref: DatasetFileRef,
    out_path: str | Path,
) -> dict[str, Any]:
    """Expand a YAML dataset into JSONL expanded ticks.

    This is shared by web UI and CLI tools.
    """
    path = resolve_dataset_file(dataset_ref)
    if path.suffix.lower() not in {".yaml", ".yml"}:
        raise ExperimentRunnerError("Only YAML datasets can be expanded via this API.")
    raw = load_yaml_file(path)
    normalized = ds.validate_and_normalize_dataset(raw)
    items = list(ds.expand_dataset(normalized))
    out_p = Path(out_path)
    n = write_jsonl(out_p, items)
    return {
        "success": True,
        "dataset_id": str(normalized.get("dataset_id", "") or ""),
        "tick_count": int(n),
        "out_path": str(out_p),
    }
