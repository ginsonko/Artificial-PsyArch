# -*- coding: utf-8 -*-
"""
Structure group store for HDB.
"""

from __future__ import annotations

import time
from pathlib import Path

from . import __schema_version__, __module_name__
from ._id_generator import ensure_counter, next_id
from ._storage_utils import list_json_files, load_json_file, remove_file, write_json_file


class GroupStore:
    def __init__(self, base_dir: str | Path, config: dict | None = None):
        self._base_dir = Path(base_dir)
        self._config = config or {}
        self._items: dict[str, dict] = {}
        self._load()

    @property
    def size(self) -> int:
        return len(self._items)

    def _recency_peak(self) -> float:
        return max(1.0, float(self._config.get("recency_gain_peak", 10.0)))

    def _recency_hold_rounds(self) -> int:
        return max(0, int(self._config.get("recency_gain_hold_rounds", 2)))

    def _new_recent_gain(self) -> float:
        return round(self._recency_peak(), 8)

    def create_group(
        self,
        required_structure_ids: list[str],
        avg_energy_profile: dict[str, float],
        trace_id: str,
        tick_id: str = "",
        *,
        bias_structure_ids: list[str] | None = None,
        source_interface: str = "run_structure_level_retrieval_storage",
        origin: str = "learned_from_cam",
        origin_id: str = "",
        metadata: dict | None = None,
    ) -> dict:
        now_ms = int(time.time() * 1000)
        group_id = next_id("sg")
        item = {
            "id": group_id,
            "object_type": "sg",
            "sub_type": "event_template_group",
            "schema_version": __schema_version__,
            "required_structure_ids": list(dict.fromkeys(required_structure_ids)),
            "bias_structure_ids": list(dict.fromkeys(bias_structure_ids or [])),
            "avg_energy_profile": dict(avg_energy_profile),
            "stats": {
                "base_weight": 1.0,
                "recent_gain": self._new_recent_gain(),
                "fatigue": 0.0,
                "match_count_total": 0,
                "last_matched_at": 0,
                "last_recency_refresh_at": now_ms,
                "recency_hold_rounds_remaining": self._recency_hold_rounds(),
                "created_from_structure_count": len(required_structure_ids),
            },
            "source": {
                "module": __module_name__,
                "interface": source_interface,
                "origin": origin,
                "origin_id": origin_id,
                "parent_ids": list(required_structure_ids),
            },
            "trace_id": trace_id,
            "tick_id": tick_id or trace_id,
            "created_at": now_ms,
            "updated_at": now_ms,
            "status": "active",
            "meta": metadata
            or {
                "confidence": 0.75,
                "field_registry_version": __schema_version__,
                "debug": {},
                "ext": {},
            },
        }
        self._items[group_id] = item
        self._persist_item(item)
        return item

    def get(self, group_id: str) -> dict | None:
        return self._items.get(group_id)

    def iter_items(self) -> list[dict]:
        return list(self._items.values())

    def update(self, item: dict) -> None:
        if not item.get("id"):
            return
        item["updated_at"] = int(time.time() * 1000)
        self._items[item["id"]] = item
        self._persist_item(item)

    def update_config(self, config: dict) -> None:
        self._config = config or {}

    def delete(self, group_id: str) -> bool:
        item = self._items.pop(group_id, None)
        if item is None:
            return False
        return remove_file(self._file_path(group_id))

    def clear(self) -> int:
        count = len(self._items)
        for group_id in list(self._items):
            self.delete(group_id)
        self._items.clear()
        return count

    def get_recent(self, limit: int = 10) -> list[dict]:
        if limit <= 0:
            return []
        return sorted(
            self._items.values(),
            key=lambda x: x.get("created_at", 0),
            reverse=True,
        )[:limit]

    def _file_path(self, group_id: str) -> Path:
        return self._base_dir / f"{group_id}.json"

    def _persist_item(self, item: dict) -> None:
        write_json_file(self._file_path(item["id"]), item)

    def _load(self) -> None:
        for path in list_json_files(self._base_dir):
            payload = load_json_file(path, default=None)
            if not isinstance(payload, dict) or not payload.get("id"):
                continue
            group_id = payload["id"]
            self._items[group_id] = payload
            numeric_tail = group_id.rsplit("_", 1)[-1]
            if numeric_tail.isdigit():
                ensure_counter("sg", int(numeric_tail))
