# -*- coding: utf-8 -*-
"""
Filesystem helpers for the HDB prototype.
"""

from __future__ import annotations

import json
import os
import time
from pathlib import Path
from typing import Any


def ensure_dir(path: str | Path) -> Path:
    target = Path(path)
    target.mkdir(parents=True, exist_ok=True)
    return target


def load_json_file(path: str | Path, default: Any = None) -> Any:
    target = Path(path)
    if not target.exists():
        return default
    try:
        with open(target, "r", encoding="utf-8") as fh:
            return json.load(fh)
    except Exception:
        return default


def write_json_file(path: str | Path, payload: Any) -> None:
    target = Path(path)
    ensure_dir(target.parent)
    tmp_path = target.with_suffix(target.suffix + ".tmp")
    used_orjson = False
    try:
        import orjson

        option = orjson.OPT_INDENT_2 | orjson.OPT_SORT_KEYS
        data = orjson.dumps(payload, option=option)
        with open(tmp_path, "wb") as fh:
            fh.write(data)
        used_orjson = True
    except Exception:
        # Keep a safe fallback path if orjson is missing or payload contains
        # non-serializable types.
        with open(tmp_path, "w", encoding="utf-8") as fh:
            json.dump(payload, fh, ensure_ascii=False, indent=2, sort_keys=True)

    # Windows may transiently lock files (e.g., antivirus / log tailers).
    # Retry a few times before giving up.
    for attempt in range(6):
        try:
            os.replace(tmp_path, target)
            return
        except PermissionError:
            if attempt >= 5:
                raise
            time.sleep(0.005 * (attempt + 1))
        except OSError:
            # If the target dir is on a slow filesystem, os.replace can
            # sporadically fail. Retrying keeps the behaviour robust.
            if attempt >= 5:
                raise
            time.sleep(0.005 * (attempt + 1))

    # Defensive: should not reach here, but keep the behaviour explicit.
    if not used_orjson:
        os.replace(tmp_path, target)


def list_json_files(path: str | Path) -> list[Path]:
    target = Path(path)
    if not target.exists():
        return []
    return sorted(p for p in target.glob("*.json") if p.is_file())


def remove_file(path: str | Path) -> bool:
    target = Path(path)
    if not target.exists():
        return False
    try:
        target.unlink()
        return True
    except OSError:
        return False
