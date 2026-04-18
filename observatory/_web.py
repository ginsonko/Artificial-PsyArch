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
                    self._send_json({"success": True, "data": payload})
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
                        meta = {"dataset_id": "", "time_basis": "", "tick_dt_ms": None, "estimated_ticks": None}
                        try:
                            # Best-effort parse meta for YAML datasets.
                            p = exp.storage.resolve_dataset_file(ref)  # type: ignore[attr-defined]
                            if p.suffix.lower() in {".yaml", ".yml"}:
                                raw = exp.io.load_yaml_file(p)  # type: ignore[attr-defined]
                                norm = exp.validate_and_normalize_dataset(raw)
                                meta["dataset_id"] = str(norm.get("dataset_id", "") or "")
                                meta["time_basis"] = str(norm.get("time_basis", "") or "")
                                meta["tick_dt_ms"] = norm.get("tick_dt_ms", None)
                                meta["estimated_ticks"] = exp.estimate_total_ticks(norm)
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
                if parsed.path == "/api/experiment/runs":
                    limit = _maybe_int(query.get("limit", [32])[0]) or 32
                    run_ids = exp.list_runs(limit=limit)
                    self._send_json({"success": True, "data": {"runs": run_ids}})
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
                    every = max(1, min(1000, int(every)))
                    limit = max(0, min(50000, int(limit)))

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
                    for row in exp.io.iter_jsonl(metrics_path):  # type: ignore[attr-defined]
                        if i % every != 0:
                            i += 1
                            continue
                        rows.append(row)
                        i += 1
                        if limit and len(rows) >= limit:
                            break
                    self._send_json({"success": True, "data": {"run_id": run_id, "every": every, "rows": rows}})
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
                self._send_json({"success": False, "message": "Unknown API path"}, status=HTTPStatus.NOT_FOUND)
            except Exception as exc:
                self._send_json({"success": False, "message": str(exc)}, status=HTTPStatus.BAD_REQUEST)

        def _handle_api_post(self, parsed: urllib.parse.ParseResult) -> None:
            payload = self._read_json_body()
            try:
                if parsed.path == "/api/cycle":
                    text = payload.get("text")
                    with self.server.app_lock:
                        report = self.server.app.run_cycle(text=text)
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
                        self.server.app.sensor.clear_echo_pool(trace_id="web_clear_sensor")
                        self.server.app.pool.clear_state_pool(trace_id="web_clear_pool", reason="web_reset", operator="researcher")
                        result = self.server.app.hdb.clear_hdb(trace_id="web_clear_all", reason="web_reset", operator="researcher")
                        self.server.app._last_report = None
                        self.server.app._report_history = []
                    self._send_json(result)
                    return
                if parsed.path == "/api/clear_runtime":
                    with self.server.app_lock:
                        self.server.app.sensor.clear_echo_pool(trace_id="web_clear_sensor")
                        self.server.app.pool.clear_state_pool(trace_id="web_clear_pool", reason="web_reset", operator="researcher")
                        self.server.app._last_report = None
                        self.server.app._report_history = []
                    self._send_json({"success": True, "data": {"cleared": True}})
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
                            exp.validate_and_normalize_dataset(raw)
                        except Exception as exc:
                            raise ValueError(f"YAML dataset validation failed: {exc}")
                    out_path.write_text(content, encoding="utf-8")
                    self._send_json({"success": True, "data": {"source": "imported", "rel_path": out_path.relative_to(out_dir).as_posix()}})
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
                    # ticks_iter is a list when preview_limit is set
                    ticks_list = list(ticks_iter) if not isinstance(ticks_iter, list) else ticks_iter
                    self._send_json(
                        {
                            "success": True,
                            "data": {
                                "dataset_id": dataset_id,
                                "dataset_sha256": digest,
                                "total_ticks": total_ticks,
                                "preview_limit": limit,
                                "preview_ticks": ticks_list,
                                "normalized_meta": (normalized_doc or {}).get("_meta", {}) if isinstance(normalized_doc, dict) else {},
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

                    threading.Thread(target=worker, daemon=True).start()
                    self._send_json({"success": True, "data": {"job_id": job_id, "run_id": run_id}})
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
