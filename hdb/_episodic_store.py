# -*- coding: utf-8 -*-
"""
Episodic memory store for HDB.
"""

from __future__ import annotations

import time
from pathlib import Path

from . import __schema_version__, __module_name__
from ._id_generator import ensure_counter, next_id
from ._storage_utils import list_json_files, load_json_file, remove_file, write_json_file


class EpisodicStore:
    def __init__(self, base_dir: str | Path):
        self._base_dir = Path(base_dir)
        self._items: dict[str, dict] = {}
        self._load()

    @property
    def size(self) -> int:
        return len(self._items)

    def append(self, episodic_payload: dict, trace_id: str, tick_id: str = "") -> dict:
        now_ms = int(time.time() * 1000)
        em_id = next_id("em")
        item = {
            "id": em_id,
            "object_type": "em",
            "sub_type": episodic_payload.get("sub_type", "tick_episode"),
            "schema_version": __schema_version__,
            "event_summary": episodic_payload.get("event_summary", ""),
            "structure_refs": list(episodic_payload.get("structure_refs", [])),
            "group_refs": list(episodic_payload.get("group_refs", [])),
            "timestamp_range": episodic_payload.get("timestamp_range"),
            "created_at": episodic_payload.get("created_at", now_ms),
            "updated_at": now_ms,
            "trace_id": trace_id,
            "tick_id": tick_id or trace_id,
            "source": episodic_payload.get(
                "source",
                {
                    "module": __module_name__,
                    "interface": "append_episodic_memory",
                    "origin": episodic_payload.get("origin", "runtime_event"),
                    "origin_id": episodic_payload.get("origin_id", ""),
                    "parent_ids": list(episodic_payload.get("parent_ids", [])),
                },
            ),
            "status": "active",
            "meta": episodic_payload.get(
                "meta",
                {
                    "confidence": 1.0,
                    "field_registry_version": __schema_version__,
                    "debug": {},
                    "ext": {},
                },
            ),
        }
        self._items[em_id] = item
        self._persist_item(item)
        return item

    def get(self, episodic_id: str) -> dict | None:
        return self._items.get(episodic_id)

    def update(self, item: dict) -> None:
        episodic_id = str(item.get("id", ""))
        if not episodic_id:
            return
        item["updated_at"] = int(time.time() * 1000)
        self._items[episodic_id] = item
        self._persist_item(item)

    def get_recent(self, limit: int = 10) -> list[dict]:
        if limit <= 0:
            return []
        items = sorted(self._items.values(), key=lambda x: x.get("created_at", 0), reverse=True)
        return items[:limit]

    def iter_items(self) -> list[dict]:
        return list(self._items.values())

    def delete(self, episodic_id: str) -> bool:
        item = self._items.pop(episodic_id, None)
        if item is None:
            return False
        return remove_file(self._file_path(episodic_id))

    def clear(self) -> int:
        count = len(self._items)
        for episodic_id in list(self._items):
            self.delete(episodic_id)
        self._items.clear()
        return count

    def _file_path(self, episodic_id: str) -> Path:
        return self._base_dir / f"{episodic_id}.json"

    def _persist_item(self, item: dict) -> None:
        write_json_file(self._file_path(item["id"]), item)

    def _load(self) -> None:
        for path in list_json_files(self._base_dir):
            payload = load_json_file(path, default=None)
            if not isinstance(payload, dict) or not payload.get("id"):
                continue
            episodic_id = payload["id"]
            self._items[episodic_id] = payload
            numeric_tail = episodic_id.rsplit("_", 1)[-1]
            if numeric_tail.isdigit():
                ensure_counter("em", int(numeric_tail))
