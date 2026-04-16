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
import urllib.parse
import webbrowser
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

from ._app import ObservatoryApp


class ObservatoryWebServer(ThreadingHTTPServer):
    daemon_threads = True

    def __init__(self, host: str, port: int, app: ObservatoryApp):
        self.app = app
        self.app_lock = threading.RLock()
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
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", mime_type or "application/octet-stream")
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
