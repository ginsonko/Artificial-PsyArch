# -*- coding: utf-8 -*-
"""
Module logger for HDB.
"""

from __future__ import annotations

import json
import os
import sys
import time
from pathlib import Path
from typing import Any


LEVEL_ERROR = "ERROR"
LEVEL_BRIEF = "BRIEF"
LEVEL_DETAIL = "DETAIL"

_MODULE_NAME = "hdb"


class ModuleLogger:
    def __init__(
        self,
        log_dir: str = "",
        max_file_bytes: int = 5 * 1024 * 1024,
        enable_stdout_fallback: bool = True,
    ):
        self._max_bytes = max_file_bytes
        self._stdout_fallback = enable_stdout_fallback

        if not log_dir:
            log_dir = os.path.join(os.path.dirname(__file__), "logs")
        self._base_dir = Path(log_dir)
        self._dirs: dict[str, Path | None] = {}
        self._handles: dict[str, Any] = {}

        for level in ("error", "brief", "detail"):
            directory = self._base_dir / level
            try:
                directory.mkdir(parents=True, exist_ok=True)
                self._dirs[level] = directory
            except OSError:
                self._dirs[level] = None

    def error(
        self,
        trace_id: str,
        interface: str,
        code: str,
        message_zh: str,
        message_en: str,
        tick_id: str = "",
        detail: dict | None = None,
    ) -> None:
        entry = self._build_entry(
            level=LEVEL_ERROR,
            trace_id=trace_id,
            tick_id=tick_id,
            interface=interface,
            code=code,
            message_zh=message_zh,
            message_en=message_en,
            detail=detail,
        )
        self._write("error", entry)
        self._write("brief", entry)
        self._write("detail", entry)

    def brief(
        self,
        trace_id: str,
        interface: str,
        success: bool,
        message_zh: str = "",
        message_en: str = "",
        tick_id: str = "",
        input_summary: dict | None = None,
        output_summary: dict | None = None,
    ) -> None:
        entry = self._build_entry(
            level=LEVEL_BRIEF,
            trace_id=trace_id,
            tick_id=tick_id,
            interface=interface,
            code="OK" if success else "FAIL",
            message_zh=message_zh,
            message_en=message_en,
            detail={
                "success": success,
                "input_summary": input_summary or {},
                "output_summary": output_summary or {},
            },
        )
        self._write("brief", entry)
        self._write("detail", entry)

    def detail(
        self,
        trace_id: str,
        step: str,
        info: dict | None = None,
        tick_id: str = "",
        message_zh: str = "",
        message_en: str = "",
    ) -> None:
        entry = self._build_entry(
            level=LEVEL_DETAIL,
            trace_id=trace_id,
            tick_id=tick_id,
            interface=step,
            code="",
            message_zh=message_zh,
            message_en=message_en,
            detail=info or {},
        )
        self._write("detail", entry)

    def update_config(self, log_dir: str = "", max_file_bytes: int = 0) -> bool:
        changed = False
        if max_file_bytes > 0 and max_file_bytes != self._max_bytes:
            self._max_bytes = max_file_bytes
            changed = True
        if log_dir and log_dir != str(self._base_dir):
            self.close()
            self._base_dir = Path(log_dir)
            for level in ("error", "brief", "detail"):
                directory = self._base_dir / level
                try:
                    directory.mkdir(parents=True, exist_ok=True)
                    self._dirs[level] = directory
                except OSError:
                    self._dirs[level] = None
            changed = True
        return changed

    def close(self) -> None:
        for handle in self._handles.values():
            try:
                if handle and not handle.closed:
                    handle.close()
            except OSError:
                pass
        self._handles.clear()

    def _build_entry(
        self,
        level: str,
        trace_id: str,
        tick_id: str,
        interface: str,
        code: str,
        message_zh: str,
        message_en: str,
        detail: dict | None,
    ) -> str:
        record = {
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
            "timestamp_ms": int(time.time() * 1000),
            "module": _MODULE_NAME,
            "level": level,
            "trace_id": trace_id,
            "tick_id": tick_id,
            "interface": interface,
            "code": code,
            "message_zh": message_zh,
            "message_en": message_en,
        }
        if detail:
            record["detail"] = detail
        try:
            return json.dumps(record, ensure_ascii=False)
        except (TypeError, ValueError):
            record["detail"] = str(detail)
            return json.dumps(record, ensure_ascii=False)

    def _write(self, level_key: str, line: str) -> None:
        target_dir = self._dirs.get(level_key)
        if target_dir is None:
            if self._stdout_fallback:
                print(line, file=sys.stderr)
            return
        try:
            handle = self._get_or_open(level_key, target_dir)
            handle.write(line + "\n")
            handle.flush()
            if handle.tell() >= self._max_bytes:
                self._rotate(level_key, target_dir)
        except OSError:
            if self._stdout_fallback:
                print(line, file=sys.stderr)

    def _get_or_open(self, level_key: str, target_dir: Path):
        handle = self._handles.get(level_key)
        if handle is None or handle.closed:
            path = target_dir / f"{level_key}_current.log"
            handle = open(path, "a", encoding="utf-8")
            self._handles[level_key] = handle
        return handle

    def _rotate(self, level_key: str, target_dir: Path) -> None:
        try:
            current = self._handles.pop(level_key, None)
            if current and not current.closed:
                current.close()
            current_path = target_dir / f"{level_key}_current.log"
            if current_path.exists():
                suffix = time.strftime("%Y-%m-%d_%H-%M-%S")
                current_path.rename(target_dir / f"{level_key}_{suffix}.log")
        except OSError:
            if self._stdout_fallback:
                print(f"[{_MODULE_NAME}] log rotation failed: {level_key}", file=sys.stderr)
