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
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Iterable

from . import dataset as ds
from .io import ExperimentIOError, iter_jsonl, load_yaml_file, sha256_file, write_jsonl
from .metrics import extract_tick_metrics
from .storage import DatasetFileRef, ExperimentStorageError, make_run_dir, resolve_dataset_file, safe_slug


class ExperimentRunnerError(RuntimeError):
    pass


@dataclass(frozen=True)
class RunOptions:
    reset_mode: str = "keep"  # keep | clear_runtime | clear_all
    export_json: bool = False
    export_html: bool = False
    # time sensor override during the run (None means keep current runtime config)
    time_sensor_time_basis: str | None = None  # tick | wallclock | None
    tick_interval_sec: float | None = None  # for display only (time_sensor config field)
    max_ticks: int | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "reset_mode": self.reset_mode,
            "export_json": bool(self.export_json),
            "export_html": bool(self.export_html),
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
    manifest: dict[str, Any] = {
        "run_id": run_id,
        "status": "running",
        "dataset": {
            "dataset_id": dataset_id,
            "dataset_sha256": dataset_sha,
            "dataset_ref": dataset_ref.to_dict(),
            "dataset_path": str(resolve_dataset_file(dataset_ref)),
            "total_ticks": int(total_ticks) if total_ticks is not None else None,
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
        "tick_planned": int(total_ticks) if total_ticks is not None else None,
        "errors": [],
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

        # Reset modes (mutates app state)
        with lock_ctx:
            if options.reset_mode == "clear_all":
                app.sensor.clear_echo_pool(trace_id="exp_clear_sensor")  # type: ignore[attr-defined]
                app.pool.clear_state_pool(trace_id="exp_clear_pool", reason="experiment_reset", operator="researcher")  # type: ignore[attr-defined]
                app.hdb.clear_hdb(trace_id="exp_clear_hdb", reason="experiment_reset", operator="researcher")  # type: ignore[attr-defined]
                app._last_report = None  # type: ignore[attr-defined]
                app._report_history = []  # type: ignore[attr-defined]
            elif options.reset_mode == "clear_runtime":
                app.sensor.clear_echo_pool(trace_id="exp_clear_sensor")  # type: ignore[attr-defined]
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
            if options.time_sensor_time_basis in {"tick", "wallclock"}:
                try:
                    app.time_sensor._config["time_basis"] = str(options.time_sensor_time_basis)  # type: ignore[attr-defined]
                except Exception:
                    pass
            if options.tick_interval_sec is not None:
                try:
                    app.time_sensor._config["tick_interval_sec"] = float(options.tick_interval_sec)  # type: ignore[attr-defined]
                except Exception:
                    pass

        # Main loop
        tick_done = 0
        max_ticks = options.max_ticks
        if max_ticks is not None:
            max_ticks = max(1, int(max_ticks))

        with metrics_path.open("w", encoding="utf-8") as mf:
            for tick in ticks_iter:
                if cancel_cb():
                    manifest["status"] = "cancelled"
                    break
                if not isinstance(tick, dict):
                    continue

                if max_ticks is not None and tick_done >= max_ticks:
                    manifest["status"] = "stopped_max_ticks"
                    break

                text = str(tick.get("input_text", "") or "")
                is_empty = bool(tick.get("input_is_empty", False)) or (text == "")
                labels = tick.get("labels") if isinstance(tick.get("labels"), dict) else None
                with lock_ctx:
                    report = app.run_cycle(text=None if is_empty else text, labels=labels)  # type: ignore[attr-defined]
                metrics = extract_tick_metrics(report=report, dataset_tick=tick)
                mf.write(json.dumps(metrics, ensure_ascii=False))
                mf.write("\n")
                tick_done += 1

                # Update progress (cheap, but do not flush too often)
                if tick_done <= 5 or tick_done % 10 == 0:
                    progress_cb(
                        {
                            "run_id": run_id,
                            "status": manifest.get("status", "running"),
                            "tick_done": tick_done,
                            "tick_planned": total_ticks,
                        }
                    )
                    try:
                        mf.flush()
                    except Exception:
                        pass

        manifest["tick_done"] = int(tick_done)
        if manifest.get("status") == "running":
            manifest["status"] = "completed"
        manifest["finished_at_ms"] = _now_ms()
        manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
        progress_cb(
            {
                "run_id": run_id,
                "status": manifest.get("status"),
                "tick_done": manifest.get("tick_done"),
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
