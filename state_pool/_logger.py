# -*- coding: utf-8 -*-
"""
AP 状态池模块 — 模块专属日志管理器
====================================
增强版三层日志器，基于 text_sensor 日志器模式，扩展以下能力：
  1. 日志字段含 message_zh / message_en 分离（设计文档 13.3 节）
  2. 日志字段含 tick_id（状态池核心标识）
  3. error 事件同时写入 error + brief + detail 三层
  4. 日志不可用时降级到 stdout，绝不阻塞主流程
  5. 按文件大小自动轮转
"""

import json
import os
import sys
import time
import traceback
from pathlib import Path
from typing import Any

# ----- 日志级别常量 -----
LEVEL_ERROR = "ERROR"
LEVEL_BRIEF = "BRIEF"
LEVEL_DETAIL = "DETAIL"

_MODULE_NAME = "state_pool"


class ModuleLogger:
    """
    状态池模块专属日志器。

    增强点（相对于 text_sensor 版本）：
      - 每条日志含 message_zh + message_en 双语字段
      - 支持 tick_id 字段
      - detail 日志支持高粒度状态变化记录
    """

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
        for level in ("error", "brief", "detail"):
            d = self._base_dir / level
            try:
                d.mkdir(parents=True, exist_ok=True)
                self._dirs[level] = d
            except OSError:
                self._dirs[level] = None

        self._handles: dict[str, Any] = {}

    # ------------------------------------------------------------------ #
    #                         公共写入方法                                 #
    # ------------------------------------------------------------------ #

    def error(
        self,
        trace_id: str,
        interface: str,
        code: str,
        message_zh: str,
        message_en: str,
        tick_id: str = "",
        detail: dict | None = None,
    ):
        """
        写入错误日志。错误事件同时记录到 error + brief + detail 三层。
        """
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
    ):
        """
        写入精简运行日志。普通调用记录到 brief + detail 两层。
        """
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
    ):
        """
        写入详细运行日志。仅写 detail 层。
        """
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

    # ------------------------------------------------------------------ #
    #                         内部实现                                     #
    # ------------------------------------------------------------------ #

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
        """构建一条结构化日志行（JSON 单行格式）。"""
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

    def _write(self, level_key: str, line: str):
        """向对应级别的日志文件追加一行。"""
        target_dir = self._dirs.get(level_key)
        if target_dir is None:
            if self._stdout_fallback:
                print(line, file=sys.stderr)
            return
        try:
            fh = self._get_or_open(level_key, target_dir)
            fh.write(line + "\n")
            fh.flush()
            if fh.tell() >= self._max_bytes:
                self._rotate(level_key, target_dir)
        except OSError:
            if self._stdout_fallback:
                print(line, file=sys.stderr)

    def _get_or_open(self, level_key: str, target_dir: Path):
        """获取或延迟打开日志文件句柄。"""
        fh = self._handles.get(level_key)
        if fh is None or fh.closed:
            filepath = target_dir / f"{level_key}_current.log"
            fh = open(filepath, "a", encoding="utf-8")
            self._handles[level_key] = fh
        return fh

    def _rotate(self, level_key: str, target_dir: Path):
        """日志轮转：关闭当前文件，重命名为带时间戳归档名。"""
        try:
            old_fh = self._handles.pop(level_key, None)
            if old_fh and not old_fh.closed:
                old_fh.close()
            current_path = target_dir / f"{level_key}_current.log"
            if current_path.exists():
                ts = time.strftime("%Y-%m-%d_%H-%M-%S")
                archive_path = target_dir / f"{level_key}_{ts}_{os.getpid()}_{time.time_ns()}.log"
                current_path.rename(archive_path)
        except OSError:
            # Do not spam stderr on transient rotation failures.
            # Fall back to reopening/continuing the current file so logging stays non-blocking.
            try:
                filepath = target_dir / f"{level_key}_current.log"
                self._handles[level_key] = open(filepath, "a", encoding="utf-8")
            except OSError:
                pass

    def close(self):
        """关闭所有打开的日志文件句柄。"""
        for fh in self._handles.values():
            try:
                if fh and not fh.closed:
                    fh.close()
            except OSError:
                pass
        self._handles.clear()

    def update_config(self, log_dir: str = "", max_file_bytes: int = 0):
        """热加载时更新日志配置。"""
        changed = False
        if max_file_bytes > 0 and max_file_bytes != self._max_bytes:
            self._max_bytes = max_file_bytes
            changed = True
        if log_dir and log_dir != str(self._base_dir):
            self.close()
            self._base_dir = Path(log_dir)
            for level in ("error", "brief", "detail"):
                d = self._base_dir / level
                try:
                    d.mkdir(parents=True, exist_ok=True)
                    self._dirs[level] = d
                except OSError:
                    self._dirs[level] = None
            changed = True
        return changed
