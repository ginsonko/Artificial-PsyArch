# -*- coding: utf-8 -*-
"""
Structure object and structure database storage for HDB.
"""

from __future__ import annotations

import time
from pathlib import Path

from . import __module_name__, __schema_version__
from ._id_generator import ensure_counter, next_id
from ._storage_utils import list_json_files, load_json_file, remove_file, write_json_file


class StructureStore:
    def __init__(self, structures_dir: str | Path, indexes_dir: str | Path, config: dict | None = None):
        self._structures_dir = Path(structures_dir)
        self._indexes_dir = Path(indexes_dir)
        self._config = config or {}
        self._structures: dict[str, dict] = {}
        self._structure_dbs: dict[str, dict] = {}
        self._owner_to_db: dict[str, str] = {}
        self._load()

    @property
    def structure_count(self) -> int:
        return len(self._structures)

    @property
    def structure_db_count(self) -> int:
        return len(self._structure_dbs)

    def _recency_peak(self) -> float:
        return max(1.0, float(self._config.get("recency_gain_peak", 10.0)))

    def _recency_hold_rounds(self) -> int:
        return max(0, int(self._config.get("recency_gain_hold_rounds", 2)))

    def _recency_refresh_floor(self) -> float:
        return max(0.0, min(1.0, float(self._config.get("recency_gain_refresh_floor", 0.45))))

    def _new_recent_gain(self) -> float:
        return round(self._recency_peak(), 8)

    def _refresh_recent_gain(self, current: float, *, strength: float = 1.0) -> float:
        bounded_strength = max(self._recency_refresh_floor(), min(1.0, float(strength)))
        peak = self._recency_peak()
        return round(min(peak, max(float(current), 1.0 + (peak - 1.0) * bounded_strength)), 8)

    def create_structure(
        self,
        *,
        structure_payload: dict,
        trace_id: str,
        tick_id: str = "",
        source_interface: str = "run_stimulus_level_retrieval_storage",
        origin: str = "direct_store",
        origin_id: str = "",
        parent_ids: list[str] | None = None,
    ) -> tuple[dict, dict]:
        now_ms = int(time.time() * 1000)
        structure_id = next_id("st")
        structure_db_id = next_id("sdb")

        structure_obj = {
            "id": structure_id,
            "object_type": "st",
            "sub_type": structure_payload.get("sub_type", "stimulus_sequence_structure"),
            "schema_version": __schema_version__,
            "structure": {
                "unit_type": structure_payload.get("unit_type", "sa_csa_sequence"),
                "display_text": structure_payload.get("display_text", structure_id),
                "member_refs": list(structure_payload.get("member_refs", [])),
                "sequence_groups": list(structure_payload.get("sequence_groups", [])),
                "flat_tokens": list(structure_payload.get("flat_tokens", [])),
                "content_signature": structure_payload.get("content_signature", structure_id),
                "semantic_signature": structure_payload.get(
                    "semantic_signature",
                    structure_payload.get("content_signature", structure_id),
                ),
                "token_count": len(structure_payload.get("flat_tokens", [])),
                "ext": dict(structure_payload.get("ext", {})),
            },
            "db_pointer": {
                "structure_db_id": structure_db_id,
                "pointer_status": "ok",
                "fallback_index_key": structure_id,
                "last_known_parent_db": structure_payload.get("last_known_parent_db", ""),
            },
            "stats": {
                "base_weight": structure_payload.get("base_weight", 1.0),
                "recent_gain": structure_payload.get("recent_gain", self._new_recent_gain()),
                "fatigue": structure_payload.get("fatigue", 0.0),
                "runtime_er": structure_payload.get("runtime_er", 0.0),
                "runtime_ev": structure_payload.get("runtime_ev", 0.0),
                "last_runtime_energy_at": now_ms,
                "last_matched_at": structure_payload.get("last_matched_at", 0),
                "last_recency_refresh_at": structure_payload.get("last_recency_refresh_at", now_ms),
                "recency_hold_rounds_remaining": structure_payload.get("recency_hold_rounds_remaining", self._recency_hold_rounds()),
                "last_verified_by_er_at": structure_payload.get("last_verified_by_er_at", 0),
                "last_worn_by_ev_at": structure_payload.get("last_worn_by_ev_at", 0),
                "match_count_total": structure_payload.get("match_count_total", 0),
                "verified_count_er": structure_payload.get("verified_count_er", 0),
                "worn_count_ev": structure_payload.get("worn_count_ev", 0),
            },
            "source": {
                "module": __module_name__,
                "interface": source_interface,
                "origin": origin,
                "origin_id": origin_id,
                "parent_ids": list(parent_ids or []),
            },
            "trace_id": trace_id,
            "tick_id": tick_id or trace_id,
            "created_at": now_ms,
            "updated_at": now_ms,
            "status": "active",
            "meta": structure_payload.get(
                "meta",
                {
                    "confidence": structure_payload.get("confidence", 0.8),
                    "field_registry_version": __schema_version__,
                    "debug": {},
                    "ext": structure_payload.get("ext", {}),
                },
            ),
        }

        structure_db = {
            "structure_db_id": structure_db_id,
            "owner_structure_id": structure_id,
            "diff_table": list(structure_payload.get("diff_table", [])),
            "group_table": list(structure_payload.get("group_table", [])),
            "integrity": {
                "pointer_ok": True,
                "last_check_at": 0,
                "issue_count": 0,
            },
            "created_at": now_ms,
            "updated_at": now_ms,
        }

        self._structures[structure_id] = structure_obj
        self._structure_dbs[structure_db_id] = structure_db
        self._owner_to_db[structure_id] = structure_db_id
        self._persist_structure(structure_obj)
        self._persist_db(structure_db)
        return structure_obj, structure_db

    def get(self, structure_id: str) -> dict | None:
        return self._structures.get(structure_id)

    def iter_structures(self) -> list[dict]:
        return list(self._structures.values())

    def iter_structure_dbs(self) -> list[dict]:
        return list(self._structure_dbs.values())

    def get_db(self, structure_db_id: str) -> dict | None:
        return self._structure_dbs.get(structure_db_id)

    def get_db_by_owner(self, structure_id: str) -> dict | None:
        structure_db_id = self._owner_to_db.get(structure_id)
        if not structure_db_id:
            return None
        return self._structure_dbs.get(structure_db_id)

    def update_structure(self, structure_obj: dict) -> None:
        if not structure_obj.get("id"):
            return
        structure_obj["updated_at"] = int(time.time() * 1000)
        self._structures[structure_obj["id"]] = structure_obj
        self._persist_structure(structure_obj)

    def update_db(self, structure_db: dict) -> None:
        structure_db_id = structure_db.get("structure_db_id")
        if not structure_db_id:
            return
        structure_db["updated_at"] = int(time.time() * 1000)
        self._structure_dbs[structure_db_id] = structure_db
        owner_id = structure_db.get("owner_structure_id", "")
        if owner_id:
            self._owner_to_db[owner_id] = structure_db_id
        self._persist_db(structure_db)

    def update_config(self, config: dict) -> None:
        self._config = config or {}

    def add_diff_entry(
        self,
        owner_structure_id: str,
        *,
        target_id: str,
        content_signature: str,
        base_weight: float,
        entry_type: str = "structure_ref",
        residual_existing_signature: str = "",
        residual_incoming_signature: str = "",
        ext: dict | None = None,
    ) -> dict | None:
        structure_db = self.get_db_by_owner(owner_structure_id)
        if structure_db is None:
            return None
        now_ms = int(time.time() * 1000)
        target_db_id = ""
        target_structure = self.get(target_id)
        if target_structure:
            target_db_id = str(target_structure.get("db_pointer", {}).get("structure_db_id", ""))
        relation_type = str((ext or {}).get("relation_type", ""))
        for existing in structure_db.setdefault("diff_table", []):
            if existing.get("entry_type", "structure_ref") != entry_type:
                continue
            if existing.get("target_id", "") != target_id:
                continue
            if existing.get("content_signature", "") != content_signature:
                continue
            if existing.get("residual_existing_signature", "") != residual_existing_signature:
                continue
            if existing.get("residual_incoming_signature", "") != residual_incoming_signature:
                continue
            existing_relation_type = str(existing.get("ext", {}).get("relation_type", ""))
            if existing_relation_type != relation_type:
                continue
            existing["base_weight"] = round(float(existing.get("base_weight", 0.0)) + max(0.01, float(base_weight) * 0.2), 6)
            existing["recent_gain"] = self._refresh_recent_gain(float(existing.get("recent_gain", 1.0)))
            existing["match_count_total"] = int(existing.get("match_count_total", 0)) + 1
            existing["last_updated_at"] = now_ms
            existing["last_recency_refresh_at"] = now_ms
            existing["recency_hold_rounds_remaining"] = self._recency_hold_rounds()
            if target_db_id:
                existing["target_db_id"] = target_db_id
            merged_ext = dict(existing.get("ext", {}))
            merged_ext.update(ext or {})
            existing["ext"] = merged_ext
            self.update_db(structure_db)
            return existing
        entry = {
            "entry_id": next_id("diff"),
            "entry_type": entry_type,
            "target_id": target_id,
            "target_db_id": target_db_id,
            "content_signature": content_signature,
            "base_weight": round(float(base_weight), 6),
            "runtime_er": 0.0,
            "runtime_ev": 0.0,
            "recent_gain": self._new_recent_gain(),
            "fatigue": 0.0,
            "match_count_total": 0,
            "last_updated_at": now_ms,
            "last_matched_at": 0,
            "last_recency_refresh_at": now_ms,
            "recency_hold_rounds_remaining": self._recency_hold_rounds(),
            "path_stats": {
                "verified_count_er": 0,
                "worn_count_ev": 0,
            },
            "residual_existing_signature": residual_existing_signature,
            "residual_incoming_signature": residual_incoming_signature,
            "ext": ext or {},
        }
        structure_db.setdefault("diff_table", []).append(entry)
        self.update_db(structure_db)
        return entry

    def add_group_table_entry(
        self,
        owner_structure_id: str,
        *,
        group_id: str,
        required_structure_ids: list[str],
        avg_energy_profile: dict[str, float],
        base_weight: float,
    ) -> dict | None:
        structure_db = self.get_db_by_owner(owner_structure_id)
        if structure_db is None:
            return None
        now_ms = int(time.time() * 1000)
        entry = {
            "group_id": group_id,
            "required_structure_ids": list(required_structure_ids),
            "avg_energy_profile": dict(avg_energy_profile),
            "base_weight": round(float(base_weight), 6),
            "recent_gain": self._new_recent_gain(),
            "fatigue": 0.0,
            "last_matched_at": 0,
            "last_recency_refresh_at": now_ms,
            "recency_hold_rounds_remaining": self._recency_hold_rounds(),
            "match_count_total": 0,
            "last_updated_at": now_ms,
        }
        group_table = structure_db.setdefault("group_table", [])
        if not any(existing.get("group_id") == group_id for existing in group_table):
            group_table.append(entry)
            self.update_db(structure_db)
        return entry

    def remove_diff_entries(
        self,
        owner_structure_id: str,
        *,
        predicate=None,
        entry_ids: list[str] | None = None,
    ) -> int:
        structure_db = self.get_db_by_owner(owner_structure_id)
        if structure_db is None:
            return 0
        entry_id_set = {str(entry_id) for entry_id in (entry_ids or []) if str(entry_id)}
        removed = 0
        retained = []
        for entry in structure_db.get("diff_table", []):
            should_remove = False
            if entry_id_set and str(entry.get("entry_id", "")) in entry_id_set:
                should_remove = True
            elif predicate is not None:
                try:
                    should_remove = bool(predicate(entry))
                except Exception:
                    should_remove = False
            if should_remove:
                removed += 1
                continue
            retained.append(entry)
        if removed:
            structure_db["diff_table"] = retained
            self.update_db(structure_db)
        return removed

    def delete_structure(self, structure_id: str) -> dict:
        structure_obj = self._structures.pop(structure_id, None)
        if structure_obj is None:
            return {"deleted": False, "db_deleted": False}
        structure_deleted = remove_file(self._structure_file_path(structure_id))
        db_deleted = False
        structure_db_id = structure_obj.get("db_pointer", {}).get("structure_db_id", "")
        if structure_db_id:
            self._structure_dbs.pop(structure_db_id, None)
            self._owner_to_db.pop(structure_id, None)
            db_deleted = remove_file(self._db_file_path(structure_db_id))
        return {"deleted": structure_deleted, "db_deleted": db_deleted, "structure_db_id": structure_db_id}

    def clear_structures(self) -> dict:
        structure_count = len(self._structures)
        db_count = len(self._structure_dbs)
        for structure_id in list(self._structures):
            self.delete_structure(structure_id)
        orphan_db_count = 0
        for path in list_json_files(self._indexes_dir):
            if remove_file(path):
                orphan_db_count += 1
        self._structures.clear()
        self._structure_dbs.clear()
        self._owner_to_db.clear()
        return {
            "structure_count": structure_count,
            "structure_db_count": db_count + orphan_db_count,
            "orphan_structure_db_count": orphan_db_count,
        }

    def get_recent_structures(self, limit: int = 10) -> list[dict]:
        if limit <= 0:
            return []
        return sorted(
            self._structures.values(),
            key=lambda x: x.get("created_at", 0),
            reverse=True,
        )[:limit]

    def _structure_file_path(self, structure_id: str) -> Path:
        return self._structures_dir / f"{structure_id}.json"

    def _db_file_path(self, structure_db_id: str) -> Path:
        return self._indexes_dir / f"{structure_db_id}.json"

    def _persist_structure(self, structure_obj: dict) -> None:
        write_json_file(self._structure_file_path(structure_obj["id"]), structure_obj)

    def _persist_db(self, structure_db: dict) -> None:
        write_json_file(self._db_file_path(structure_db["structure_db_id"]), structure_db)

    def _load(self) -> None:
        referenced_db_ids: set[str] = set()
        for path in list_json_files(self._structures_dir):
            payload = load_json_file(path, default=None)
            if not isinstance(payload, dict) or not payload.get("id"):
                continue
            structure_id = payload["id"]
            self._structures[structure_id] = payload
            structure_db_id = str(payload.get("db_pointer", {}).get("structure_db_id", ""))
            if structure_db_id:
                referenced_db_ids.add(structure_db_id)
            numeric_tail = structure_id.rsplit("_", 1)[-1]
            if numeric_tail.isdigit():
                ensure_counter("st", int(numeric_tail))

        for path in list_json_files(self._indexes_dir):
            numeric_tail = path.stem.rsplit("_", 1)[-1]
            if numeric_tail.isdigit():
                ensure_counter("sdb", int(numeric_tail))
            if referenced_db_ids and path.stem not in referenced_db_ids:
                continue
            if self._structures and not referenced_db_ids:
                # Legacy fallback: structure files without db pointers are invalid for
                # current storage, so avoid loading every orphan DB into memory.
                continue
            if not self._structures:
                continue
            payload = load_json_file(path, default=None)
            if not isinstance(payload, dict) or not payload.get("structure_db_id"):
                continue
            structure_db_id = payload["structure_db_id"]
            if referenced_db_ids and structure_db_id not in referenced_db_ids:
                continue
            owner_id = payload.get("owner_structure_id", "")
            if owner_id and owner_id not in self._structures:
                continue
            self._structure_dbs[structure_db_id] = payload
            if owner_id:
                self._owner_to_db[owner_id] = structure_db_id

        for structure_id, structure_obj in self._structures.items():
            structure_db_id = structure_obj.get("db_pointer", {}).get("structure_db_id", "")
            if structure_db_id and structure_db_id in self._structure_dbs:
                self._owner_to_db.setdefault(structure_id, structure_db_id)

    def make_runtime_object(self, structure_id: str, er: float, ev: float, reason: str = "") -> dict | None:
        structure_obj = self.get(structure_id)
        if structure_obj is None:
            return None
        structure = structure_obj.get("structure", {}) if isinstance(structure_obj.get("structure", {}), dict) else {}
        display_text = str(structure.get("display_text", structure_id) or structure_id)
        flat_tokens = [str(token) for token in (structure.get("flat_tokens", []) or []) if str(token)]
        plain_text = "".join(flat_tokens) if flat_tokens else ""
        if not plain_text and isinstance(structure.get("sequence_groups", []), list):
            plain_parts = []
            for group in structure.get("sequence_groups", []):
                if not isinstance(group, dict):
                    continue
                if bool(group.get("order_sensitive", False)) and str(group.get("string_unit_kind", "") or "") == "char_sequence":
                    text_part = str(group.get("string_token_text", "") or "")
                    if text_part:
                        plain_parts.append(text_part)
            plain_text = "".join(part for part in plain_parts if part)
        canonical_text = plain_text or structure_id
        return {
            "id": structure_id,
            "object_type": "st",
            "sub_type": structure_obj.get("sub_type", "stimulus_sequence_structure"),
            "content": {
                "raw": canonical_text,
                "display": display_text,
                "normalized": canonical_text,
            },
            "energy": {
                "er": round(float(er), 6),
                "ev": round(float(ev), 6),
            },
            "structure": structure_obj.get("structure", {}),
            "db_pointer": structure_obj.get("db_pointer", {}),
            "source": {
                "module": __module_name__,
                "interface": "make_runtime_object",
                "origin": reason or "hdb_projection",
                "origin_id": structure_id,
                "parent_ids": list(structure_obj.get("source", {}).get("parent_ids", [])),
            },
            "created_at": structure_obj.get("created_at", int(time.time() * 1000)),
            "updated_at": int(time.time() * 1000),
        }





