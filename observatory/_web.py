# -*- coding: utf-8 -*-
"""
AP 原型观测台 Web 服务（本地）
===========================

说明：
  - 本文件提供一个最小本地 HTTP 服务，用于观测台前端页面与 API 调用。
  - 目标是“可用、可读、可审计”，而非生产级别的高并发 Web 框架。

English (short):
  Local web server for the AP observatory.
"""

from __future__ import annotations

import json
import mimetypes
import threading
import time
import urllib.parse
import webbrowser
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
import traceback

from ._app import ObservatoryApp
from . import experiment as exp


class ObservatoryWebServer(ThreadingHTTPServer):
    daemon_threads = True

    def __init__(self, host: str, port: int, app: ObservatoryApp):
        self.app = app
        self.app_lock = threading.RLock()
        # Background experiment jobs (in-memory, non-persistent).
        self.experiment_jobs: dict[str, dict[str, Any]] = {}
        self.experiment_jobs_lock = threading.RLock()
        # Background LLM review jobs (in-memory, non-persistent; status is persisted under run_dir).
        self.llm_review_jobs: dict[str, dict[str, Any]] = {}
        self.llm_review_jobs_lock = threading.RLock()
        # Background auto-tuner LLM analysis jobs.
        self.auto_tuner_llm_jobs: dict[str, dict[str, Any]] = {}
        self.auto_tuner_llm_jobs_lock = threading.RLock()
        # Background maintenance jobs (idle consolidation etc.).
        self.maintenance_jobs: dict[str, dict[str, Any]] = {}
        self.maintenance_jobs_lock = threading.RLock()
        self.static_dir = Path(__file__).resolve().parent / "web_static"
        self.started_at = app._started_at
        super().__init__((host, port), _build_handler())


def _build_handler():
    class ObservatoryHandler(BaseHTTPRequestHandler):
        server: ObservatoryWebServer

        def do_GET(self) -> None:
            parsed = urllib.parse.urlparse(self.path)
            if parsed.path.startswith("/api/"):
                self._handle_api_get(parsed)
                return
            self._serve_static(parsed.path)

        def do_POST(self) -> None:
            parsed = urllib.parse.urlparse(self.path)
            if not parsed.path.startswith("/api/"):
                self._send_json({"success": False, "message": "Not found"}, status=HTTPStatus.NOT_FOUND)
                return
            self._handle_api_post(parsed)

        def log_message(self, format: str, *args: Any) -> None:
            return

        def _handle_api_get(self, parsed: urllib.parse.ParseResult) -> None:
            query = urllib.parse.parse_qs(parsed.query)
            try:
                if parsed.path == "/api/health":
                    self._send_json({"success": True, "data": {"status": "ok"}})
                    return
                if parsed.path == "/api/dashboard":
                    with self.server.app_lock:
                        payload = self.server.app.get_dashboard_data()
                    # The raw per-tick report can become extremely large during long runs.
                    # Keep the default dashboard payload compact for UI responsiveness.
                    # Use `?full=1` when a deep offline inspection is needed.
                    full = str(query.get("full", ["0"])[0] or "0").strip().lower() in {"1", "true", "yes"}
                    if not full:
                        try:
                            payload = dict(payload)
                            payload["last_report"] = _compact_report_for_web(payload.get("last_report"))
                        except Exception:
                            pass
                    self._send_json({"success": True, "data": payload})
                    return
                if parsed.path == "/api/idle_consolidate_status":
                    job_id = (query.get("job_id", [""])[0] or "").strip()
                    if not job_id:
                        raise ValueError("job_id is required")
                    with self.server.maintenance_jobs_lock:
                        job = self.server.maintenance_jobs.get(job_id)
                    if not job:
                        self._send_json({"success": False, "message": f"job not found: {job_id}"}, status=HTTPStatus.NOT_FOUND)
                        return
                    self._send_json({"success": True, "data": job})
                    return
                if parsed.path == "/api/state":
                    top_k = _maybe_int(query.get("top_k", [None])[0])
                    with self.server.app_lock:
                        payload = self.server.app.get_state_snapshot_data(top_k=top_k)
                    self._send_json({"success": True, "data": payload})
                    return
                if parsed.path == "/api/hdb":
                    top_k = _maybe_int(query.get("top_k", [12])[0]) or 12
                    with self.server.app_lock:
                        payload = self.server.app.get_hdb_snapshot_data(top_k=top_k)
                    self._send_json({"success": True, "data": payload})
                    return
                if parsed.path == "/api/action_runtime":
                    with self.server.app_lock:
                        payload = self.server.app.get_action_runtime_data()
                    self._send_json({"success": True, "data": payload})
                    return
                if parsed.path == "/api/episodic":
                    limit = _maybe_int(query.get("limit", [10])[0]) or 10
                    with self.server.app_lock:
                        payload = self.server.app.get_episodic_data(limit=limit)
                    self._send_json({"success": True, "data": payload})
                    return
                if parsed.path == "/api/structure":
                    structure_id = (query.get("structure_id", [""])[0] or "").strip()
                    with self.server.app_lock:
                        payload = self.server.app.get_structure_data(structure_id)
                    self._send_json({"success": True, "data": payload})
                    return
                if parsed.path == "/api/group":
                    group_id = (query.get("group_id", [""])[0] or "").strip()
                    with self.server.app_lock:
                        payload = self.server.app.get_group_data(group_id)
                    self._send_json({"success": True, "data": payload})
                    return
                if parsed.path == "/api/report":
                    trace_id = (query.get("trace_id", ["latest"])[0] or "latest").strip()
                    with self.server.app_lock:
                        payload = self.server.app.get_report(trace_id)
                    if payload is None:
                        self._send_json({"success": False, "message": f"report not found: {trace_id}"}, status=HTTPStatus.NOT_FOUND)
                        return
                    full = str(query.get("full", ["0"])[0] or "0").strip().lower() in {"1", "true", "yes"}
                    if not full:
                        try:
                            payload = _compact_report_for_web(payload)
                        except Exception:
                            pass
                    self._send_json({"success": True, "data": payload})
                    return
                if parsed.path == "/api/config":
                    with self.server.app_lock:
                        payload = self.server.app.get_config_bundle()
                    self._send_json({"success": True, "data": payload})
                    return
                if parsed.path == "/api/innate_rules":
                    with self.server.app_lock:
                        payload = self.server.app.get_innate_rules_data()
                    self._send_json({"success": True, "data": payload})
                    return
                if parsed.path == "/api/experiment/datasets":
                    # List built-in and imported dataset files.
                    items = []
                    for ref in exp.list_dataset_files():
                        meta = {
                            "dataset_id": "",
                            "title": "",
                            "description": "",
                            "experiment_goal": "",
                            "time_basis": "",
                            "tick_dt_ms": None,
                            "estimated_ticks": None,
                            "effective_text_ticks": None,
                            "empty_ticks": None,
                            "labeled_ticks": None,
                            "evaluation_dimensions": [],
                            "notes": [],
                            "dataset_kind": "",
                        }
                        try:
                            p = exp.storage.resolve_dataset_file(ref)  # type: ignore[attr-defined]
                            if p.suffix.lower() in {".yaml", ".yml"}:
                                raw = exp.io.load_yaml_file(p)  # type: ignore[attr-defined]
                                norm = exp.validate_and_normalize_dataset(raw)
                                meta.update(exp.dataset_overview(norm))
                                meta["dataset_kind"] = "yaml_episode_template"
                            elif p.suffix.lower() == ".jsonl":
                                summary = exp.summarize_expanded_tick_items(exp.io.iter_jsonl(p))  # type: ignore[attr-defined]
                                meta.update(
                                    {
                                        "dataset_id": summary.get("dataset_id", "") or p.stem,
                                        "time_basis": summary.get("time_basis", ""),
                                        "tick_dt_ms": summary.get("tick_dt_ms", None),
                                        "estimated_ticks": summary.get("total_ticks", 0),
                                        "effective_text_ticks": summary.get("effective_text_ticks", 0),
                                        "empty_ticks": summary.get("empty_ticks", 0),
                                        "labeled_ticks": summary.get("labeled_ticks", 0),
                                        "dataset_kind": "jsonl_tick_stream",
                                    }
                                )
                        except Exception:
                            pass
                        items.append(
                            {
                                "source": ref.source,
                                "rel_path": ref.rel_path,
                                "meta": meta,
                            }
                        )
                    self._send_json({"success": True, "data": {"datasets": items}})
                    return
                if parsed.path == "/api/experiment/dataset_protocol":
                    self._send_json({"success": True, "data": exp.dataset_protocol_doc()})
                    return
                if parsed.path == "/api/experiment/runs":
                    limit = _maybe_int(query.get("limit", [32])[0]) or 32
                    run_ids = exp.list_runs(limit=limit)
                    run_items = exp.list_run_infos(limit=limit)
                    self._send_json({"success": True, "data": {"runs": run_ids, "items": run_items}})
                    return
                if parsed.path == "/api/experiment/llm_review/config":
                    cfg = exp.load_review_config()
                    self._send_json({"success": True, "data": {"config": cfg.to_public_dict()}})
                    return
                if parsed.path == "/api/experiment/run/llm_review_status":
                    run_id = (query.get("run_id", [""])[0] or "").strip()
                    if not run_id:
                        raise ValueError("run_id is required")
                    payload = exp.read_review_status(run_id=run_id)
                    self._send_json({"success": True, "data": payload})
                    return
                if parsed.path == "/api/experiment/run/llm_review_report":
                    run_id = (query.get("run_id", [""])[0] or "").strip()
                    if not run_id:
                        raise ValueError("run_id is required")
                    payload = exp.read_review_report(run_id=run_id)
                    self._send_json({"success": True, "data": payload})
                    return
                if parsed.path == "/api/experiment/run/manifest":
                    run_id = (query.get("run_id", [""])[0] or "").strip()
                    if not run_id:
                        raise ValueError("run_id is required")
                    base = exp.storage.experiment_runs_dir()  # type: ignore[attr-defined]
                    run_dir = (base / exp.storage.safe_slug(run_id)).resolve()  # type: ignore[attr-defined]
                    try:
                        run_dir.relative_to(base.resolve())
                    except ValueError:
                        raise ValueError("invalid run_id")
                    manifest_path = run_dir / "manifest.json"
                    if not manifest_path.exists():
                        self._send_json({"success": False, "message": f"manifest not found: {run_id}"}, status=HTTPStatus.NOT_FOUND)
                        return
                    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
                    self._send_json({"success": True, "data": payload})
                    return
                if parsed.path == "/api/experiment/run/metrics":
                    run_id = (query.get("run_id", [""])[0] or "").strip()
                    if not run_id:
                        raise ValueError("run_id is required")
                    every = _maybe_int(query.get("every", [1])[0]) or 1
                    limit = _maybe_int(query.get("limit", [0])[0]) or 0
                    offset = _maybe_int(query.get("offset", [0])[0]) or 0
                    every = max(1, min(1000, int(every)))
                    limit = max(0, min(50000, int(limit)))
                    offset = max(0, min(50000, int(offset)))

                    base = exp.storage.experiment_runs_dir()  # type: ignore[attr-defined]
                    run_dir = (base / exp.storage.safe_slug(run_id)).resolve()  # type: ignore[attr-defined]
                    try:
                        run_dir.relative_to(base.resolve())
                    except ValueError:
                        raise ValueError("invalid run_id")
                    metrics_path = run_dir / "metrics.jsonl"
                    if not metrics_path.exists():
                        self._send_json({"success": False, "message": f"metrics not found: {run_id}"}, status=HTTPStatus.NOT_FOUND)
                        return
                    rows = []
                    i = 0
                    kept = 0
                    for row in exp.io.iter_jsonl(metrics_path):  # type: ignore[attr-defined]
                        if i % every != 0:
                            i += 1
                            continue
                        if kept < offset:
                            kept += 1
                            i += 1
                            continue
                        rows.append(row)
                        kept += 1
                        i += 1
                        if limit and len(rows) >= limit:
                            break
                    self._send_json(
                        {
                            "success": True,
                            "data": {
                                "run_id": run_id,
                                "every": every,
                                "offset": offset,
                                "next_offset": offset + len(rows),
                                "rows": rows,
                            },
                        }
                    )
                    return
                if parsed.path == "/api/experiment/jobs":
                    job_id = (query.get("job_id", [""])[0] or "").strip()
                    with self.server.experiment_jobs_lock:
                        if job_id:
                            job = self.server.experiment_jobs.get(job_id)
                            if not job:
                                self._send_json({"success": False, "message": f"job not found: {job_id}"}, status=HTTPStatus.NOT_FOUND)
                                return
                            self._send_json({"success": True, "data": job})
                            return
                        # list jobs (recent)
                        jobs = list(self.server.experiment_jobs.values())
                        jobs.sort(key=lambda j: int(j.get("created_at_ms", 0) or 0), reverse=True)
                        self._send_json({"success": True, "data": {"jobs": jobs[:40]}})
                        return
                if parsed.path == "/api/experiment/llm_review/jobs":
                    job_id = (query.get("job_id", [""])[0] or "").strip()
                    with self.server.llm_review_jobs_lock:
                        if job_id:
                            job = self.server.llm_review_jobs.get(job_id)
                            if not job:
                                self._send_json({"success": False, "message": f"job not found: {job_id}"}, status=HTTPStatus.NOT_FOUND)
                                return
                            self._send_json({"success": True, "data": job})
                            return
                        jobs = list(self.server.llm_review_jobs.values())
                        jobs.sort(key=lambda j: int(j.get("created_at_ms", 0) or 0), reverse=True)
                        self._send_json({"success": True, "data": {"jobs": jobs[:40]}})
                        return
                if parsed.path == "/api/experiment/auto_tuner/config":
                    self._send_json({"success": True, "data": exp.load_auto_tuner_public_config()})
                    return
                if parsed.path == "/api/experiment/auto_tuner/catalog":
                    with self.server.app_lock:
                        payload = exp.read_auto_tuner_catalog(app=self.server.app)
                    self._send_json({"success": True, "data": payload})
                    return
                if parsed.path == "/api/experiment/auto_tuner/state":
                    with self.server.app_lock:
                        payload = exp.read_auto_tuner_state(app=self.server.app)
                    self._send_json({"success": True, "data": payload})
                    return
                if parsed.path == "/api/experiment/auto_tuner/audit":
                    limit = _maybe_int(query.get("limit", [200])[0]) or 200
                    payload = exp.read_auto_tuner_audit(limit=max(1, min(2000, limit)))
                    self._send_json({"success": True, "data": payload})
                    return
                if parsed.path == "/api/experiment/auto_tuner/rules":
                    with self.server.app_lock:
                        payload = {
                            "rules": exp.load_auto_tuner_rules(),
                            "catalog": exp.build_auto_tuner_rule_catalog(app=self.server.app),
                        }
                    self._send_json({"success": True, "data": payload})
                    return
                if parsed.path == "/api/experiment/auto_tuner/rollback_points":
                    limit = _maybe_int(query.get("limit", [80])[0]) or 80
                    self._send_json({"success": True, "data": exp.list_rollback_points(limit=max(1, min(200, limit)))})
                    return
                if parsed.path == "/api/experiment/auto_tuner/llm/config":
                    self._send_json({"success": True, "data": exp.load_auto_tuner_llm_config()})
                    return
                if parsed.path == "/api/experiment/auto_tuner/llm/jobs":
                    job_id = (query.get("job_id", [""])[0] or "").strip()
                    with self.server.auto_tuner_llm_jobs_lock:
                        if job_id:
                            job = self.server.auto_tuner_llm_jobs.get(job_id)
                            if not job:
                                self._send_json({"success": False, "message": f"job not found: {job_id}"}, status=HTTPStatus.NOT_FOUND)
                                return
                            self._send_json({"success": True, "data": job})
                            return
                        jobs = list(self.server.auto_tuner_llm_jobs.values())
                        jobs.sort(key=lambda j: int(j.get("created_at_ms", 0) or 0), reverse=True)
                        self._send_json({"success": True, "data": {"jobs": jobs[:40]}})
                        return
                self._send_json({"success": False, "message": "Unknown API path"}, status=HTTPStatus.NOT_FOUND)
            except Exception as exc:
                self._send_json({"success": False, "message": str(exc)}, status=HTTPStatus.BAD_REQUEST)

        def _handle_api_post(self, parsed: urllib.parse.ParseResult) -> None:
            payload = self._read_json_body()
            try:
                if parsed.path == "/api/cycle":
                    text = payload.get("text")
                    try:
                        with self.server.app_lock:
                            report = self.server.app.run_cycle(text=text)
                    except Exception as exc:
                        self._send_json(
                            {
                                "success": False,
                                "message": str(exc),
                                "error": str(exc),
                                "error_type": type(exc).__name__,
                                "traceback": traceback.format_exc(limit=12),
                            },
                            status=HTTPStatus.INTERNAL_SERVER_ERROR,
                        )
                        return
                    self._send_json({"success": True, "data": report})
                    return
                if parsed.path == "/api/tick":
                    count = max(1, int(payload.get("count", 1)))
                    with self.server.app_lock:
                        reports = self.server.app.run_tick_cycles(count=count)
                    self._send_json({"success": True, "data": reports})
                    return
                if parsed.path == "/api/check":
                    target = payload.get("target")
                    with self.server.app_lock:
                        result = self.server.app.hdb.self_check_hdb(trace_id="web_check", target_id=target)
                    self._send_json(result)
                    return
                if parsed.path == "/api/repair":
                    target = str(payload.get("target", "")).strip()
                    if not target:
                        raise ValueError("target is required")
                    with self.server.app_lock:
                        result = self.server.app.hdb.repair_hdb(
                            trace_id="web_repair",
                            target_id=target,
                            repair_scope="targeted",
                            background=False,
                        )
                    self._send_json(result)
                    return
                if parsed.path == "/api/repair_all":
                    with self.server.app_lock:
                        result = self.server.app.hdb.repair_hdb(
                            trace_id="web_repair_all",
                            repair_scope="global_quick",
                            background=True,
                        )
                    self._send_json(result)
                    return
                if parsed.path == "/api/idle_consolidate":
                    rebuild = payload.get("rebuild_pointer_index", True)
                    apply_limits = payload.get("apply_soft_limits", True)
                    reason = str(payload.get("reason", "") or "").strip() or "web_manual_trigger"
                    background = bool(payload.get("background", False))
                    max_cs_events = payload.get("max_cs_events", None)
                    try:
                        max_cs_events = int(max_cs_events) if max_cs_events is not None else None
                    except Exception:
                        max_cs_events = None

                    def _run_idle_consolidation() -> dict:
                        with self.server.app_lock:
                            data = {}
                            try:
                                data["hdb"] = self.server.app.hdb.idle_consolidate_hdb(
                                    trace_id="web_idle_consolidate",
                                    reason=reason,
                                    rebuild_pointer_index=bool(rebuild),
                                    apply_soft_limits=bool(apply_limits),
                                )
                            except Exception as exc:
                                data["hdb_error"] = str(exc)

                            if hasattr(self.server.app, "cognitive_stitching") and hasattr(self.server.app.cognitive_stitching, "idle_consolidate"):
                                try:
                                    data["cognitive_stitching"] = self.server.app.cognitive_stitching.idle_consolidate(
                                        hdb=self.server.app.hdb,
                                        trace_id="web_idle_consolidate_cs",
                                        tick_id="web_idle_consolidate",
                                        reason=reason,
                                        max_events=max_cs_events,
                                    )
                                except Exception as exc:
                                    data["cognitive_stitching_error"] = str(exc)
                        return data

                    if background:
                        now_ms = int(time.time() * 1000)
                        with self.server.maintenance_jobs_lock:
                            seq = int(self.server.maintenance_jobs.get("_seq", 0) or 0) + 1
                            self.server.maintenance_jobs["_seq"] = seq
                            job_id = f"idle_cons_{now_ms}_{seq:04d}"
                            job = {
                                "job_id": job_id,
                                "job_type": "idle_consolidation",
                                "status": "queued",
                                "created_at_ms": now_ms,
                                "started_at_ms": 0,
                                "finished_at_ms": 0,
                                "request": {
                                    "reason": reason,
                                    "rebuild_pointer_index": bool(rebuild),
                                    "apply_soft_limits": bool(apply_limits),
                                    "max_cs_events": max_cs_events,
                                },
                                "data": None,
                                "error": "",
                            }
                            self.server.maintenance_jobs[job_id] = job
                            # Prevent unbounded growth (in-memory only).
                            try:
                                items = [
                                    (jid, j)
                                    for jid, j in self.server.maintenance_jobs.items()
                                    if isinstance(j, dict) and jid and jid != "_seq"
                                ]
                                if len(items) > 80:
                                    items.sort(key=lambda it: int((it[1] or {}).get("created_at_ms", 0) or 0))
                                    for jid, _ in items[: max(0, len(items) - 60)]:
                                        self.server.maintenance_jobs.pop(jid, None)
                            except Exception:
                                pass

                        def worker() -> None:
                            with self.server.maintenance_jobs_lock:
                                j = self.server.maintenance_jobs.get(job_id) or {}
                                j["status"] = "running"
                                j["started_at_ms"] = int(time.time() * 1000)
                                self.server.maintenance_jobs[job_id] = j
                            try:
                                data = _run_idle_consolidation()
                                with self.server.maintenance_jobs_lock:
                                    j = self.server.maintenance_jobs.get(job_id) or {}
                                    j["status"] = "completed"
                                    j["finished_at_ms"] = int(time.time() * 1000)
                                    j["data"] = data
                                    self.server.maintenance_jobs[job_id] = j
                            except Exception as exc:
                                with self.server.maintenance_jobs_lock:
                                    j = self.server.maintenance_jobs.get(job_id) or {}
                                    j["status"] = "failed"
                                    j["finished_at_ms"] = int(time.time() * 1000)
                                    j["error"] = str(exc)
                                    self.server.maintenance_jobs[job_id] = j

                        threading.Thread(target=worker, daemon=True).start()
                        self._send_json({"success": True, "code": "OK", "message": "idle consolidation job queued", "data": job})
                        return

                    data = _run_idle_consolidation()
                    self._send_json({"success": True, "code": "OK", "message": "idle consolidation completed", "data": data})
                    return
                if parsed.path == "/api/stop_repair":
                    job_id = str(payload.get("repair_job_id", "")).strip()
                    if not job_id:
                        raise ValueError("repair_job_id is required")
                    with self.server.app_lock:
                        result = self.server.app.hdb.stop_repair_job(repair_job_id=job_id, trace_id="web_stop_repair")
                    self._send_json(result)
                    return
                if parsed.path == "/api/clear_hdb":
                    with self.server.app_lock:
                        result = self.server.app.hdb.clear_hdb(trace_id="web_clear_hdb", reason="web_reset", operator="researcher")
                    self._send_json(result)
                    return
                if parsed.path == "/api/clear_all":
                    with self.server.app_lock:
                        if hasattr(self.server.app, "_clear_runtime_modules"):
                            result = self.server.app._clear_runtime_modules(
                                clear_hdb=True,
                                trace_prefix="web_clear_all",
                                reason="web_reset",
                                operator="researcher",
                            )
                        else:
                            self.server.app.sensor.clear_echo_pool(trace_id="web_clear_sensor")
                            self.server.app.pool.clear_state_pool(trace_id="web_clear_pool", reason="web_reset", operator="researcher")
                            result = self.server.app.hdb.clear_hdb(trace_id="web_clear_all", reason="web_reset", operator="researcher")
                            self.server.app._last_report = None
                            self.server.app._report_history = []
                    self._send_json({"success": True, "data": result})
                    return
                if parsed.path == "/api/clear_runtime":
                    with self.server.app_lock:
                        if hasattr(self.server.app, "_clear_runtime_modules"):
                            result = self.server.app._clear_runtime_modules(
                                clear_hdb=False,
                                trace_prefix="web_clear_runtime",
                                reason="web_reset",
                                operator="researcher",
                            )
                        else:
                            self.server.app.sensor.clear_echo_pool(trace_id="web_clear_sensor")
                            self.server.app.pool.clear_state_pool(trace_id="web_clear_pool", reason="web_reset", operator="researcher")
                            self.server.app._last_report = None
                            self.server.app._report_history = []
                            result = {"cleared": True}
                    self._send_json({"success": True, "data": result})
                    return
                if parsed.path == "/api/experiment/datasets/import":
                    # Import a dataset file by uploading text content (no multipart).
                    # Stored under observatory/outputs/datasets_imported (gitignored).
                    content = str(payload.get("content", "") or "")
                    if not content.strip():
                        raise ValueError("content is required")
                    fmt = str(payload.get("format", "yaml") or "yaml").strip().lower()
                    if fmt not in {"yaml", "yml", "jsonl"}:
                        raise ValueError("format must be yaml/yml/jsonl")
                    filename = str(payload.get("filename", "") or "").strip() or "imported_dataset"
                    safe_name = exp.storage.safe_slug(filename, fallback="imported_dataset")  # type: ignore[attr-defined]
                    ext = ".jsonl" if fmt == "jsonl" else ".yaml"
                    out_dir = exp.storage.imported_datasets_dir()  # type: ignore[attr-defined]
                    out_dir.mkdir(parents=True, exist_ok=True)
                    out_path = (out_dir / f"{safe_name}{ext}").resolve()
                    try:
                        out_path.relative_to(out_dir.resolve())
                    except ValueError:
                        raise ValueError("invalid filename")

                    # Validate basic parse for YAML to fail fast.
                    if ext in {".yaml", ".yml"}:
                        try:
                            raw = exp.io.load_yaml_text(content)  # type: ignore[attr-defined]
                            norm = exp.validate_and_normalize_dataset(raw)
                            summary = exp.dataset_overview(norm)
                        except Exception as exc:
                            raise ValueError(f"YAML dataset validation failed: {exc}")
                    else:
                        try:
                            summary = exp.validate_and_summarize_jsonl_text(content)
                        except Exception as exc:
                            raise ValueError(f"JSONL 数据集校验失败: {exc}")
                    out_path.write_text(content, encoding="utf-8")
                    self._send_json(
                        {
                            "success": True,
                            "data": {
                                "source": "imported",
                                "rel_path": out_path.relative_to(out_dir).as_posix(),
                                "summary": summary,
                            },
                        }
                    )
                    return
                if parsed.path == "/api/experiment/datasets/preview":
                    ref = payload.get("dataset_ref") or {}
                    if not isinstance(ref, dict):
                        raise ValueError("dataset_ref must be an object")
                    source = str(ref.get("source", "") or "").strip()
                    rel_path = str(ref.get("rel_path", "") or "").strip()
                    if not source or not rel_path:
                        raise ValueError("dataset_ref.source and dataset_ref.rel_path are required")
                    limit = int(payload.get("limit", 24) or 24)
                    limit = max(1, min(200, limit))
                    dataset_ref = exp.DatasetFileRef(source=source, rel_path=rel_path)
                    dataset_id, digest, normalized_doc, ticks_iter, total_ticks = exp.load_dataset_ticks(dataset_ref=dataset_ref, preview_limit=limit)
                    tick_summary = exp.summarize_tick_counts(normalized_doc) if isinstance(normalized_doc, dict) else {}
                    overview = exp.dataset_overview(normalized_doc) if isinstance(normalized_doc, dict) else {}
                    # ticks_iter is a list when preview_limit is set
                    ticks_list = list(ticks_iter) if not isinstance(ticks_iter, list) else ticks_iter
                    if not isinstance(normalized_doc, dict):
                        p = exp.storage.resolve_dataset_file(dataset_ref)  # type: ignore[attr-defined]
                        try:
                            jsonl_summary = exp.summarize_expanded_tick_items(exp.io.iter_jsonl(p))  # type: ignore[attr-defined]
                        except Exception:
                            jsonl_summary = {}
                        tick_summary = jsonl_summary
                        overview = {
                            "dataset_id": jsonl_summary.get("dataset_id", dataset_id),
                            "title": "",
                            "description": "",
                            "experiment_goal": "",
                            "time_basis": jsonl_summary.get("time_basis", ""),
                            "tick_dt_ms": jsonl_summary.get("tick_dt_ms", None),
                            "estimated_ticks": jsonl_summary.get("total_ticks", total_ticks),
                            "effective_text_ticks": jsonl_summary.get("effective_text_ticks", None),
                            "empty_ticks": jsonl_summary.get("empty_ticks", None),
                            "labeled_ticks": jsonl_summary.get("labeled_ticks", None),
                            "evaluation_dimensions": [],
                            "notes": [],
                        }
                    self._send_json(
                        {
                            "success": True,
                            "data": {
                                "dataset_id": dataset_id,
                                "dataset_sha256": digest,
                                "total_ticks": total_ticks,
                                "effective_text_ticks": tick_summary.get("effective_text_ticks") if tick_summary else None,
                                "empty_ticks": tick_summary.get("empty_ticks") if tick_summary else None,
                                "labeled_ticks": tick_summary.get("labeled_ticks") if tick_summary else None,
                                "preview_limit": limit,
                                "preview_ticks": ticks_list,
                                "normalized_meta": (normalized_doc or {}).get("_meta", {}) if isinstance(normalized_doc, dict) else {},
                                "overview": overview,
                            },
                        }
                    )
                    return
                if parsed.path == "/api/experiment/datasets/expand":
                    ref = payload.get("dataset_ref") or {}
                    if not isinstance(ref, dict):
                        raise ValueError("dataset_ref must be an object")
                    source = str(ref.get("source", "") or "").strip()
                    rel_path = str(ref.get("rel_path", "") or "").strip()
                    if not source or not rel_path:
                        raise ValueError("dataset_ref.source and dataset_ref.rel_path are required")
                    dataset_ref = exp.DatasetFileRef(source=source, rel_path=rel_path)
                    # Expand to observatory/outputs/datasets/<dataset_id>/expanded_ticks.jsonl
                    dataset_id, _, normalized_doc, _, _ = exp.load_dataset_ticks(dataset_ref=dataset_ref, preview_limit=1)
                    if not dataset_id:
                        dataset_id = Path(rel_path).stem
                    out_dir = Path(__file__).resolve().parent / "outputs" / "datasets" / exp.storage.safe_slug(dataset_id)  # type: ignore[attr-defined]
                    out_path = out_dir / "expanded_ticks.jsonl"
                    result = exp.export_expanded_ticks(dataset_ref=dataset_ref, out_path=out_path)
                    self._send_json({"success": True, "data": result})
                    return
                if parsed.path == "/api/experiment/run/start":
                    ref = payload.get("dataset_ref") or {}
                    if not isinstance(ref, dict):
                        raise ValueError("dataset_ref must be an object")
                    source = str(ref.get("source", "") or "").strip()
                    rel_path = str(ref.get("rel_path", "") or "").strip()
                    if not source or not rel_path:
                        raise ValueError("dataset_ref.source and dataset_ref.rel_path are required")
                    dataset_ref = exp.DatasetFileRef(source=source, rel_path=rel_path)

                    opt_raw = payload.get("options") or {}
                    if not isinstance(opt_raw, dict):
                        opt_raw = {}
                    options = exp.RunOptions(
                        reset_mode=str(opt_raw.get("reset_mode", "keep") or "keep").strip(),
                        export_json=bool(opt_raw.get("export_json", False)),
                        export_html=bool(opt_raw.get("export_html", False)),
                        auto_tune_enabled=bool(opt_raw.get("auto_tune_enabled", False)),
                        auto_tune_short_term=bool(opt_raw.get("auto_tune_short_term", True)),
                        auto_tune_long_term=bool(opt_raw.get("auto_tune_long_term", True)),
                        time_sensor_time_basis=(str(opt_raw.get("time_sensor_time_basis")).strip() if opt_raw.get("time_sensor_time_basis") is not None else None),
                        tick_interval_sec=(float(opt_raw.get("tick_interval_sec")) if opt_raw.get("tick_interval_sec") is not None else None),
                        max_ticks=(int(opt_raw.get("max_ticks")) if opt_raw.get("max_ticks") is not None else None),
                    )

                    # Prepare job record
                    job_id = f"exp_job_{int(time.time() * 1000)}_{threading.get_ident()}"
                    # Resolve dataset_id early (for UI display) without consuming too much work.
                    try:
                        dataset_id, _, _, _, total_ticks = exp.load_dataset_ticks(dataset_ref=dataset_ref, preview_limit=1)
                    except Exception:
                        dataset_id, total_ticks = Path(rel_path).stem, None
                    run_id = str(payload.get("run_id", "") or "").strip() or exp.make_run_id(dataset_id=dataset_id)

                    job = {
                        "job_id": job_id,
                        "run_id": run_id,
                        "dataset_ref": dataset_ref.to_dict(),
                        "dataset_id": dataset_id,
                        "status": "queued",
                        "tick_done": 0,
                        "tick_planned": total_ticks,
                        "created_at_ms": int(time.time() * 1000),
                        "started_at_ms": 0,
                        "finished_at_ms": 0,
                        "cancelled": False,
                        "error": "",
                        "auto_tuner_run_options": {
                            "enabled": bool(options.auto_tune_enabled),
                            "short_term": bool(options.auto_tune_short_term),
                            "long_term": bool(options.auto_tune_long_term),
                        },
                        "auto_tuner_last_tick": {},
                        "auto_tuner_recent_events": [],
                    }
                    with self.server.experiment_jobs_lock:
                        self.server.experiment_jobs[job_id] = job

                    def progress_cb(update: dict[str, Any]) -> None:
                        with self.server.experiment_jobs_lock:
                            j = self.server.experiment_jobs.get(job_id) or {}
                            if not j:
                                return
                            j["tick_done"] = int(update.get("tick_done", j.get("tick_done", 0)) or 0)
                            if update.get("tick_planned") is not None:
                                j["tick_planned"] = int(update.get("tick_planned") or 0)
                            if update.get("status"):
                                j["status"] = str(update.get("status") or j.get("status") or "")
                            if update.get("error"):
                                j["error"] = str(update.get("error") or "")
                            if update.get("tick_index") is not None:
                                try:
                                    j["last_tick_index"] = int(update.get("tick_index") or 0)
                                except Exception:
                                    pass
                            short_term = update.get("auto_tuner_short_term")
                            if isinstance(short_term, dict):
                                row = {
                                    "tick_index": j.get("last_tick_index"),
                                    "enabled": bool(short_term.get("enabled", False)),
                                    "applied": bool(short_term.get("applied", False)),
                                    "reason": str(short_term.get("reason", "") or ""),
                                    "applied_count": int(short_term.get("applied_count", 0) or 0),
                                    "applied_updates": [dict(item) for item in (short_term.get("applied_updates") or []) if isinstance(item, dict)][:8],
                                }
                                j["auto_tuner_last_tick"] = row
                                history = list(j.get("auto_tuner_recent_events", [])) if isinstance(j.get("auto_tuner_recent_events"), list) else []
                                prev = history[-1] if history else {}
                                should_append = bool(row.get("applied")) or str(row.get("reason", "") or "") != str(prev.get("reason", "") or "")
                                if should_append:
                                    history.append(row)
                                elif history:
                                    history[-1] = row
                                else:
                                    history.append(row)
                                j["auto_tuner_recent_events"] = history[-40:]
                            self.server.experiment_jobs[job_id] = j

                    def cancel_cb() -> bool:
                        with self.server.experiment_jobs_lock:
                            j = self.server.experiment_jobs.get(job_id) or {}
                            return bool(j.get("cancelled", False))

                    def worker() -> None:
                        with self.server.experiment_jobs_lock:
                            j = self.server.experiment_jobs.get(job_id) or {}
                            j["status"] = "running"
                            j["started_at_ms"] = int(time.time() * 1000)
                            self.server.experiment_jobs[job_id] = j
                        try:
                            # Important: do NOT hold app_lock for the entire run.
                            # We lock per-tick inside exp.run_dataset(app_lock=...),
                            # so the existing observatory UI can still refresh between ticks.
                            res = exp.run_dataset(
                                app=self.server.app,
                                app_lock=self.server.app_lock,
                                dataset_ref=dataset_ref,
                                options=options,
                                run_id=run_id,
                                progress_cb=progress_cb,
                                cancel_cb=cancel_cb,
                            )
                        except Exception as exc:
                            res = {"success": False, "error": str(exc), "run_id": run_id}
                        with self.server.experiment_jobs_lock:
                            j = self.server.experiment_jobs.get(job_id) or {}
                            j["finished_at_ms"] = int(time.time() * 1000)
                            if not res.get("success", False):
                                j["status"] = "failed"
                                j["error"] = str(res.get("error", "") or "")
                            else:
                                j["status"] = str(res.get("manifest", {}).get("status", "completed") or "completed")
                            self.server.experiment_jobs[job_id] = j

                        # Optional: auto-run LLM review after completion.
                        try:
                            cfg = exp.load_review_config()
                            status = str(res.get("manifest", {}).get("status", "") or "")
                            if bool(cfg.enabled) and bool(cfg.auto_analyze_on_completion) and status in {"completed", "stopped_max_ticks"}:
                                _start_llm_review_job(server=self.server, run_id=str(res.get("run_id", run_id) or run_id), force=False)
                        except Exception:
                            pass

                    threading.Thread(target=worker, daemon=True).start()
                    self._send_json({"success": True, "data": {"job_id": job_id, "run_id": run_id}})
                    return
                if parsed.path == "/api/experiment/llm_review/config/save":
                    values = payload.get("config") if isinstance(payload.get("config"), dict) else (payload if isinstance(payload, dict) else {})
                    cfg = exp.save_review_config(values if isinstance(values, dict) else {})
                    self._send_json({"success": True, "data": {"config": cfg.to_public_dict()}})
                    return
                if parsed.path == "/api/experiment/auto_tuner/config/save":
                    values = payload.get("config") if isinstance(payload.get("config"), dict) else (payload if isinstance(payload, dict) else {})
                    data = exp.save_auto_tuner_public_config(values if isinstance(values, dict) else {})
                    self._send_json({"success": True, "data": data})
                    return
                if parsed.path == "/api/experiment/auto_tuner/rules/save":
                    values = payload.get("rules") if isinstance(payload.get("rules"), dict) else (payload if isinstance(payload, dict) else {})
                    saved = exp.save_auto_tuner_rules(values if isinstance(values, dict) else {})
                    with self.server.app_lock:
                        catalog = exp.build_auto_tuner_rule_catalog(app=self.server.app)
                    self._send_json({"success": True, "data": {"rules": saved, "catalog": catalog}})
                    return
                if parsed.path == "/api/experiment/auto_tuner/rollback":
                    point_id = str(payload.get("point_id", "") or "").strip()
                    if not point_id:
                        raise ValueError("point_id is required")
                    with self.server.app_lock:
                        data = exp.rollback_to_point(point_id=point_id, app=self.server.app)
                    self._send_json({"success": True, "data": data})
                    return
                if parsed.path == "/api/experiment/auto_tuner/llm/config/save":
                    values = payload.get("config") if isinstance(payload.get("config"), dict) else (payload if isinstance(payload, dict) else {})
                    data = exp.save_auto_tuner_llm_config(values if isinstance(values, dict) else {})
                    self._send_json({"success": True, "data": data})
                    return
                if parsed.path == "/api/experiment/auto_tuner/llm/analyze":
                    run_id = str(payload.get("run_id", "") or "").strip()
                    prompt = str(payload.get("user_prompt", "") or "").strip()
                    focus_metrics = payload.get("focus_metrics") if isinstance(payload.get("focus_metrics"), list) else []
                    job = _start_auto_tuner_llm_job(server=self.server, run_id=run_id, user_prompt=prompt, focus_metrics=focus_metrics)
                    self._send_json({"success": True, "data": job})
                    return
                if parsed.path == "/api/experiment/run/llm_review/start":
                    run_id = str(payload.get("run_id", "") or "").strip()
                    if not run_id:
                        raise ValueError("run_id is required")
                    force = bool(payload.get("force", False))
                    job = _start_llm_review_job(server=self.server, run_id=run_id, force=force)
                    self._send_json({"success": True, "data": job})
                    return
                if parsed.path == "/api/experiment/run/stop":
                    job_id = str(payload.get("job_id", "") or "").strip()
                    if not job_id:
                        raise ValueError("job_id is required")
                    with self.server.experiment_jobs_lock:
                        job = self.server.experiment_jobs.get(job_id)
                        if not job:
                            raise ValueError(f"job not found: {job_id}")
                        job["cancelled"] = True
                        job["status"] = "cancelling"
                        self.server.experiment_jobs[job_id] = job
                    self._send_json({"success": True, "data": {"job_id": job_id, "cancelled": True}})
                    return
                if parsed.path == "/api/experiment/run/delete":
                    run_id = str(payload.get("run_id", "") or "").strip()
                    if not run_id:
                        raise ValueError("run_id is required")
                    with self.server.experiment_jobs_lock:
                        active = [
                            j for j in self.server.experiment_jobs.values()
                            if str(j.get("run_id", "") or "") == run_id
                            and str(j.get("status", "") or "") in {"queued", "running", "cancelling"}
                        ]
                    if active:
                        raise ValueError("该运行任务仍在进行中，不能删除。请先停止任务。")
                    result = exp.delete_run(run_id)
                    self._send_json({"success": True, "data": result})
                    return
                if parsed.path == "/api/experiment/runs/clear":
                    with self.server.experiment_jobs_lock:
                        keep_run_ids = {
                            str(j.get("run_id", "") or "")
                            for j in self.server.experiment_jobs.values()
                            if str(j.get("status", "") or "") in {"queued", "running", "cancelling"}
                        }
                    result = exp.clear_runs(keep_run_ids=keep_run_ids)
                    self._send_json({"success": True, "data": result})
                    return
                if parsed.path == "/api/reload":
                    with self.server.app_lock:
                        result = json.loads(self.server.app.reload_all())
                    self._send_json({"success": True, "data": result})
                    return
                if parsed.path == "/api/config/save":
                    module_name = str(payload.get("module", "")).strip()
                    values = payload.get("values", {}) or {}
                    with self.server.app_lock:
                        result = self.server.app.save_module_config(module_name, values)
                    self._send_json({"success": True, "data": result})
                    return
                if parsed.path == "/api/innate_rules/validate":
                    doc = payload.get("doc")
                    yaml_text = payload.get("yaml")
                    with self.server.app_lock:
                        result = self.server.app.validate_innate_rules(doc=doc if isinstance(doc, dict) else None, yaml_text=str(yaml_text) if yaml_text is not None else None)
                    self._send_json({"success": True, "data": result})
                    return
                if parsed.path == "/api/innate_rules/save":
                    doc = payload.get("doc")
                    yaml_text = payload.get("yaml")
                    with self.server.app_lock:
                        result = self.server.app.save_innate_rules(doc=doc if isinstance(doc, dict) else None, yaml_text=str(yaml_text) if yaml_text is not None else None)
                    self._send_json({"success": True, "data": result})
                    return
                if parsed.path == "/api/innate_rules/reload":
                    with self.server.app_lock:
                        result = self.server.app.reload_innate_rules()
                    self._send_json({"success": True, "data": result})
                    return
                if parsed.path == "/api/innate_rules/simulate":
                    with self.server.app_lock:
                        result = self.server.app.simulate_innate_rules()
                    self._send_json({"success": True, "data": result})
                    return
                if parsed.path == "/api/action_stop":
                    # Stop/cancel action nodes.
                    # 行动停止/取消接口：用于验收“必须有行动停止接口”的要求。
                    mode = str(payload.get("mode", "") or "action_id")
                    value = payload.get("value")
                    hold_ticks = int(payload.get("hold_ticks", 2) or 0)
                    reason = str(payload.get("reason", "manual_stop") or "manual_stop")
                    with self.server.app_lock:
                        result = self.server.app.stop_action_nodes(
                            mode=mode,
                            value=value,
                            hold_ticks=hold_ticks,
                            reason=reason,
                            trace_id="web_action_stop",
                        )
                    # stop_action_nodes already returns a {success, code, message, data} style payload.
                    self._send_json(result)
                    return
                if parsed.path == "/api/open_report":
                    trace_id = str(payload.get("trace_id", "latest") or "latest")
                    with self.server.app_lock:
                        result = json.loads(self.server.app.open_report(trace_id, open_browser=True))
                    self._send_json({"success": True, "data": result})
                    return
                if parsed.path == "/api/shutdown":
                    self._send_json({"success": True, "data": {"message": "server shutting down"}})
                    threading.Thread(target=self.server.shutdown, daemon=True).start()
                    return
                self._send_json({"success": False, "message": "Unknown API path"}, status=HTTPStatus.NOT_FOUND)
            except Exception as exc:
                self._send_json({"success": False, "message": str(exc)}, status=HTTPStatus.BAD_REQUEST)

        def _serve_static(self, path: str) -> None:
            if path in {"", "/"}:
                relative = Path("index.html")
            else:
                relative = Path(path.lstrip("/"))
            file_path = (self.server.static_dir / relative).resolve()
            try:
                file_path.relative_to(self.server.static_dir.resolve())
            except ValueError:
                self._send_json({"success": False, "message": "Forbidden"}, status=HTTPStatus.FORBIDDEN)
                return
            if not file_path.exists() or not file_path.is_file():
                self._send_json({"success": False, "message": "Not found"}, status=HTTPStatus.NOT_FOUND)
                return
            content = file_path.read_bytes()
            mime_type, _ = mimetypes.guess_type(str(file_path))
            content_type = mime_type or "application/octet-stream"
            # Make UTF-8 explicit for text-like assets to avoid garbled Chinese UI strings.
            if "charset=" not in content_type:
                if content_type.startswith("text/") or content_type in {"application/javascript", "application/json"} or content_type.endswith("+xml"):
                    content_type = f"{content_type}; charset=utf-8"
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", content_type)
            # Disable caching for static assets during rapid prototype iteration.
            # 原型迭代阶段强制禁用静态资源缓存：避免浏览器缓存旧版 app.js/styles.css，
            # 导致“明明修了但前端看不到”的错觉（例如图形编辑器的删除/缩放按钮）。
            #
            # 说明：
            # - no-store: 浏览器不应缓存任何内容
            # - no-cache + must-revalidate: 即使缓存也必须每次向服务器确认
            # - max-age=0: 立即过期
            self.send_header("Cache-Control", "no-store, no-cache, must-revalidate, max-age=0")
            self.send_header("Pragma", "no-cache")
            self.send_header("Expires", "0")
            self.send_header("Content-Length", str(len(content)))
            self.end_headers()
            self.wfile.write(content)

        def _read_json_body(self) -> dict[str, Any]:
            length = int(self.headers.get("Content-Length", "0") or "0")
            if length <= 0:
                return {}
            raw = self.rfile.read(length)
            if not raw:
                return {}
            return json.loads(raw.decode("utf-8"))

        def _send_json(self, payload: dict[str, Any], status: HTTPStatus = HTTPStatus.OK) -> None:
            # NOTE:
            # Browsers may abort connections (tab refresh, navigation, devtools) while the backend
            # is writing a JSON payload. On Windows this commonly surfaces as WinError 10053.
            # This is not a server bug; suppress these noisy exceptions to keep logs clean.
            try:
                body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
                self.send_response(status)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                # Disable caching for API responses as well.
                # API 响应也禁用缓存，避免前端因缓存看到旧快照。
                self.send_header("Cache-Control", "no-store, no-cache, must-revalidate, max-age=0")
                self.send_header("Pragma", "no-cache")
                self.send_header("Expires", "0")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
            except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError):
                return

    return ObservatoryHandler


def run_observatory_web(app: ObservatoryApp, *, host: str, port: int, open_browser: bool = True) -> None:
    server = ObservatoryWebServer(host, port, app)
    url = f"http://{host}:{port}/"
    print(f"AP Observatory Web UI: {url}")
    if open_browser:
        try:
            webbrowser.open(url)
        except Exception:
            pass
    try:
        server.serve_forever()
    finally:
        server.server_close()
        app.close()


def _maybe_int(raw: Any) -> int | None:
    try:
        if raw in {None, "", "null"}:
            return None
        return int(raw)
    except (TypeError, ValueError):
        return None


def _start_llm_review_job(*, server: ObservatoryWebServer, run_id: str, force: bool = False) -> dict[str, Any]:
    run_id = str(run_id or "").strip()
    if not run_id:
        return {"job_id": "", "run_id": "", "success": False, "error": "run_id is empty"}

    cfg = exp.load_review_config()
    if not bool(cfg.enabled):
        return {"job_id": "", "run_id": run_id, "success": False, "error": "LLM review is disabled (config.enabled=false)"}

    # Avoid duplicate concurrent review jobs for the same run unless forced.
    status = exp.read_review_status(run_id=run_id)
    st = str(status.get("status", "") or "")
    if not force and st in {"running", "completed"}:
        return {"job_id": "", "run_id": run_id, "success": True, "skipped": True, "status": st}

    job_id = f"llm_job_{int(time.time() * 1000)}_{threading.get_ident()}"
    job = {
        "job_id": job_id,
        "run_id": run_id,
        "status": "queued",
        "created_at_ms": int(time.time() * 1000),
        "started_at_ms": 0,
        "finished_at_ms": 0,
        "error": "",
        "config": cfg.to_public_dict(),
        "force": bool(force),
    }
    with server.llm_review_jobs_lock:
        server.llm_review_jobs[job_id] = job

    def worker() -> None:
        with server.llm_review_jobs_lock:
            j = server.llm_review_jobs.get(job_id) or {}
            j["status"] = "running"
            j["started_at_ms"] = int(time.time() * 1000)
            server.llm_review_jobs[job_id] = j
        try:
            res = exp.review_run_with_llm(run_id=run_id)
        except Exception as exc:
            res = {"success": False, "error": str(exc)}
        with server.llm_review_jobs_lock:
            j = server.llm_review_jobs.get(job_id) or {}
            j["finished_at_ms"] = int(time.time() * 1000)
            if not res.get("success", False):
                j["status"] = "failed"
                j["error"] = str(res.get("error", "") or res.get("message", "") or "failed")
            else:
                j["status"] = "completed"
                j["error"] = ""
            server.llm_review_jobs[job_id] = j

    threading.Thread(target=worker, daemon=True).start()
    return {"job_id": job_id, "run_id": run_id, "success": True, "status": "queued"}


def _start_auto_tuner_llm_job(
    *,
    server: ObservatoryWebServer,
    run_id: str = "",
    user_prompt: str = "",
    focus_metrics: list[str] | None = None,
) -> dict[str, Any]:
    cfg_info = exp.load_auto_tuner_llm_config()
    cfg_public = cfg_info.get("config", {}) if isinstance(cfg_info, dict) else {}
    if not bool(cfg_public.get("enabled", False)):
        return {"job_id": "", "success": False, "error": "auto_tuner_llm_disabled"}

    job_id = f"auto_tuner_llm_job_{int(time.time() * 1000)}_{threading.get_ident()}"
    job = {
        "job_id": job_id,
        "run_id": str(run_id or "").strip(),
        "status": "queued",
        "created_at_ms": int(time.time() * 1000),
        "started_at_ms": 0,
        "finished_at_ms": 0,
        "error": "",
        "user_prompt": str(user_prompt or ""),
        "focus_metrics": list(focus_metrics or []),
        "config": cfg_public,
    }
    with server.auto_tuner_llm_jobs_lock:
        server.auto_tuner_llm_jobs[job_id] = job

    def worker() -> None:
        with server.auto_tuner_llm_jobs_lock:
            j = server.auto_tuner_llm_jobs.get(job_id) or {}
            j["status"] = "running"
            j["started_at_ms"] = int(time.time() * 1000)
            server.auto_tuner_llm_jobs[job_id] = j
        try:
            res = exp.analyze_auto_tuner_with_llm(
                app=server.app,
                run_id=str(run_id or "").strip(),
                user_prompt=str(user_prompt or ""),
                focus_metrics=list(focus_metrics or []),
            )
        except Exception as exc:
            res = {"success": False, "error": str(exc)}
        with server.auto_tuner_llm_jobs_lock:
            j = server.auto_tuner_llm_jobs.get(job_id) or {}
            j["finished_at_ms"] = int(time.time() * 1000)
            j["result"] = res
            if not res.get("success", False):
                j["status"] = "failed"
                j["error"] = str(res.get("error", "") or res.get("message", "") or "failed")
            else:
                j["status"] = "completed"
                j["error"] = ""
            server.auto_tuner_llm_jobs[job_id] = j

    threading.Thread(target=worker, daemon=True).start()
    return {"job_id": job_id, "success": True, "status": "queued"}


def _compact_report_for_web(report: Any) -> Any:
    """
    Compact a report payload for the Web UI.

    Why: In long runs, a single tick report can grow into tens of MB due to
    debug payloads and verbose per-round details. Serializing such payloads on
    every UI refresh will freeze the browser and waste CPU.

    Scope: This is presentation-only. It must not mutate the in-memory report.
    """

    # Tunables (safe defaults for a browser UI).
    max_depth = 8
    max_list_items_default = 180
    max_str_len_default = 2400

    # Per-key tighter caps for known heavy fields.
    max_list_items_by_key = {
        # raw unit/token lists
        "flat_tokens": 260,
        "tokens": 260,
        "units": 140,
        "feature_units": 140,
        "groups": 80,
        "sequence_groups": 60,
        # deep debug + candidates
        "debug": 0,
        "round_details": 8,
        "candidate_details": 12,
        "cam_items": 32,
        # memory feedback items
        "items": 24,
        "events": 24,
        "target_display_texts": 24,
        # snapshots
        "top_items": 40,
        "memory_item_count": 40,
    }
    max_str_len_by_key = {
        "display_text": 1600,
        "grouped_display_text": 1600,
        "semantic_display_text": 1200,
        "semantic_grouped_display_text": 1200,
        "visible_text": 2000,
        "raw": 2000,
        "normalized": 2000,
        "message": 1600,
    }

    def _is_primitive(x: Any) -> bool:
        return x is None or isinstance(x, (bool, int, float, str))

    def _trim_str(s: str, limit: int) -> str:
        if len(s) <= limit:
            return s
        head = s[: max(0, limit - 120)]
        tail = s[-100:] if limit >= 200 else ""
        return f"{head}…(truncated,len={len(s)})…{tail}"

    def _compact(obj: Any, *, depth: int, key: str | None) -> Any:
        if _is_primitive(obj):
            if isinstance(obj, str):
                limit = max_str_len_by_key.get(str(key or ""), max_str_len_default)
                return _trim_str(obj, int(limit))
            return obj
        if depth <= 0:
            return {"_omitted": True, "reason": "max_depth"}

        if isinstance(obj, list):
            limit = max_list_items_by_key.get(str(key or ""), max_list_items_default)
            if limit <= 0:
                return []
            if len(obj) > limit:
                kept = obj[: int(limit)]
                # Marker element so the UI can hint truncation without breaking type.
                # (If the UI doesn't render it, no harm.)
                marker = f"…(truncated {len(obj) - int(limit)} items)…"
                if kept and isinstance(kept[-1], str):
                    kept = list(kept)
                    kept.append(marker)
                return [_compact(x, depth=depth - 1, key=None) for x in kept]
            return [_compact(x, depth=depth - 1, key=None) for x in obj]

        if isinstance(obj, dict):
            out: dict[str, Any] = {}
            for k, v in obj.items():
                kk = str(k)
                if kk == "debug":
                    # Keep compacted round-level debug details for structure/stimulus panels.
                    # These are the primary observability payloads users inspect in the web UI.
                    out[kk] = _compact(v, depth=depth - 1, key=kk)
                    continue
                out[kk] = _compact(v, depth=depth - 1, key=kk)
            return out

        # Fallback for unknown types (shouldn't happen in JSON payloads).
        return str(obj)

    return _compact(report, depth=max_depth, key=None)
