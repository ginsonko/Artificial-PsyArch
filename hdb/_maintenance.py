# -*- coding: utf-8 -*-
"""
Maintenance helpers for HDB soft limits and index refresh.
"""

from __future__ import annotations


class MaintenanceEngine:
    def __init__(self, config: dict):
        self._config = config

    def update_config(self, config: dict) -> None:
        self._config = config

    def apply_structure_db_soft_limits(self, structure_db: dict) -> dict:
        diff_limit = int(self._config.get("diff_table_soft_limit", 128))
        group_limit = int(self._config.get("group_table_soft_limit", 128))
        structure_db["diff_table"] = self._trim_table(structure_db.get("diff_table", []), diff_limit)
        structure_db["group_table"] = self._trim_table(structure_db.get("group_table", []), group_limit)
        return structure_db

    def _trim_table(self, entries: list[dict], limit: int) -> list[dict]:
        if limit <= 0 or len(entries) <= limit:
            return list(entries)
        scored = sorted(
            entries,
            key=lambda item: (
                float(item.get("base_weight", 0.0)) * float(item.get("recent_gain", 1.0)) / (1.0 + float(item.get("fatigue", 0.0))),
                int(item.get("last_updated_at", item.get("last_matched_at", 0))),
            ),
            reverse=True,
        )
        return scored[:limit]
