# -*- coding: utf-8 -*-
"""
AP 文本感受器 — 模块专属日志管理器
==================================
实现 error / brief / detail 三层日志，每层独立文件、按大小轮转。
设计原则：
  - 日志不可用时降级到 stdout，绝不让日志问题阻塞主流程
  - 每条日志必含 trace_id、模块名、时间戳
  - 文件大小达到阈值自动轮转
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

_MODULE_NAME = "text_sensor"


class ModuleLogger:
    """
    文本感受器模块专属日志器。

    支持:
      - error / brief / detail 三层独立文件
      - 按文件大小自动轮转
      - 日志目录不可写时降级到 stdout
      - 结构化日志条目（含 trace_id）
    """

    def __init__(
        self,
        log_dir: str = "",
        max_file_bytes: int = 5 * 1024 * 1024,  # 默认 5MB
        enable_stdout_fallback: bool = True,
    ):
        """
        参数:
            log_dir: 日志根目录路径；为空则使用模块默认 logs/ 目录
            max_file_bytes: 单个日志文件大小上限（字节）
            enable_stdout_fallback: 文件写入失败时是否降级到 stdout
        """
        self._max_bytes = max_file_bytes
        self._stdout_fallback = enable_stdout_fallback

        # 确定日志目录
        if not log_dir:
            log_dir = os.path.join(os.path.dirname(__file__), "logs")
        self._base_dir = Path(log_dir)

        # 为每种级别创建子目录
        self._dirs: dict[str, Path] = {}
        for level in ("error", "brief", "detail"):
            d = self._base_dir / level
            try:
                d.mkdir(parents=True, exist_ok=True)
                self._dirs[level] = d
            except OSError:
                # 目录创建失败时标记为 None，后续写入降级到 stdout
                self._dirs[level] = None  # type: ignore

        # 当前活跃文件句柄（延迟打开）
        self._handles: dict[str, Any] = {}

    # ------------------------------------------------------------------ #
    #                         公共写入方法                                 #
    # ------------------------------------------------------------------ #

    def error(
        self,
        trace_id: str,
        interface: str,
        code: str,
        message: str,
        detail: dict | None = None,
    ):
        """
        写入错误日志。错误事件同时记录到 error + brief + detail 三层。
        """
        entry = self._build_entry(
            level=LEVEL_ERROR,
            trace_id=trace_id,
            interface=interface,
            code=code,
            message=message,
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
        input_summary: dict | None = None,
        output_summary: dict | None = None,
        message: str = "",
    ):
        """
        写入精简运行日志。普通调用记录到 brief + detail 两层。
        """
        entry = self._build_entry(
            level=LEVEL_BRIEF,
            trace_id=trace_id,
            interface=interface,
            code="OK" if success else "FAIL",
            message=message,
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
    ):
        """
        写入详细运行日志。仅写 detail 层。
        """
        entry = self._build_entry(
            level=LEVEL_DETAIL,
            trace_id=trace_id,
            interface=step,
            code="",
            message="",
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
        interface: str,
        code: str,
        message: str,
        detail: dict | None,
    ) -> str:
        """构建一条结构化日志行（JSON 单行格式，便于后续解析）。"""
        record = {
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
            "timestamp_ms": int(time.time() * 1000),
            "module": _MODULE_NAME,
            "level": level,
            "trace_id": trace_id,
            "interface": interface,
            "code": code,
            "message": message,
        }
        if detail:
            record["detail"] = detail
        try:
            return json.dumps(record, ensure_ascii=False)
        except (TypeError, ValueError):
            # 序列化失败时做安全回退
            record["detail"] = str(detail)
            return json.dumps(record, ensure_ascii=False)

    def _write(self, level_key: str, line: str):
        """
        向对应级别的日志文件追加一行。
        若文件写入失败则降级到 stdout。
        """
        target_dir = self._dirs.get(level_key)
        if target_dir is None:
            # 目录不可用，降级
            if self._stdout_fallback:
                print(line, file=sys.stderr)
            return

        try:
            fh = self._get_or_open(level_key, target_dir)
            fh.write(line + "\n")
            fh.flush()

            # 检查是否需要轮转
            if fh.tell() >= self._max_bytes:
                self._rotate(level_key, target_dir)
        except OSError:
            # 写入失败，降级到 stdout
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
        """
        日志轮转：关闭当前文件，重命名为带时间戳的归档名，打开新文件。
        """
        try:
            old_fh = self._handles.pop(level_key, None)
            if old_fh and not old_fh.closed:
                old_fh.close()

            current_path = target_dir / f"{level_key}_current.log"
            if current_path.exists():
                ts = time.strftime("%Y-%m-%d_%H-%M-%S")
                archive_name = f"{level_key}_{ts}.log"
                archive_path = target_dir / archive_name
                current_path.rename(archive_path)
        except OSError:
            # 轮转失败不应阻塞后续日志写入
            if self._stdout_fallback:
                print(
                    f"[{_MODULE_NAME}] 日志轮转失败: {level_key}",
                    file=sys.stderr,
                )

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
        """
        热加载时更新日志配置。
        仅在参数有效时生效；无效参数保留旧值。
        """
        changed = False
        if max_file_bytes > 0 and max_file_bytes != self._max_bytes:
            self._max_bytes = max_file_bytes
            changed = True

        if log_dir and log_dir != str(self._base_dir):
            # 关闭旧句柄，切换目录
            self.close()
            self._base_dir = Path(log_dir)
            for level in ("error", "brief", "detail"):
                d = self._base_dir / level
                try:
                    d.mkdir(parents=True, exist_ok=True)
                    self._dirs[level] = d
                except OSError:
                    self._dirs[level] = None  # type: ignore
            changed = True

        return changed
