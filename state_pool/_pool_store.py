# -*- coding: utf-8 -*-
"""
AP 状态池模块 — 主存储与索引
==============================
维护 state_item 的主存储字典和辅助索引。
支持按 id、ref_object_id、ref_object_type 快速查找，
以及容量管理和溢出淘汰。
"""

from typing import Any


class PoolStore:
    """
    状态池主存储。

    内部结构:
      _items: dict[spi_id -> state_item]         主存储
      _ref_index: dict[ref_obj_id -> spi_id]     引用对象ID → 池项ID 索引
      _semantic_index: dict[signature -> spi_id] 语义签名 → 池项ID 索引
      _type_index: dict[ref_obj_type -> set[spi_id]]  按对象类型分类索引
    """

    def __init__(self, config: dict):
        self._config = config
        self._items: dict[str, dict] = {}
        self._ref_index: dict[str, str] = {}
        self._semantic_index: dict[str, str] = {}
        self._type_index: dict[str, set[str]] = {}

    # ================================================================== #
    #                       基础操作                                       #
    # ================================================================== #

    @property
    def size(self) -> int:
        """当前池内对象数量。"""
        return len(self._items)

    @property
    def max_items(self) -> int:
        return self._config.get("pool_max_items", 5000)

    def get(self, spi_id: str) -> dict | None:
        """按 state_item ID 查找。"""
        return self._items.get(spi_id)

    def get_by_ref(self, ref_object_id: str) -> dict | None:
        """按引用对象 ID 查找。"""
        spi_id = self._ref_index.get(ref_object_id)
        if spi_id:
            return self._items.get(spi_id)
        return None

    def get_by_semantic_signature(self, semantic_signature: str) -> dict | None:
        """按语义签名查找。"""
        spi_id = self._semantic_index.get(semantic_signature)
        if spi_id:
            return self._items.get(spi_id)
        return None

    def get_by_type(self, ref_object_type: str) -> list[dict]:
        """按引用对象类型查找所有匹配项。"""
        spi_ids = self._type_index.get(ref_object_type, set())
        return [self._items[sid] for sid in spi_ids if sid in self._items]

    def get_all(self) -> list[dict]:
        """返回所有活跃对象列表。"""
        return list(self._items.values())

    def contains_ref(self, ref_object_id: str) -> bool:
        """检查是否已有该引用对象。"""
        return ref_object_id in self._ref_index

    # ================================================================== #
    #                       写入操作                                       #
    # ================================================================== #

    def insert(self, item: dict) -> bool:
        """
        插入一个新的 state_item。

        返回:
            True 表示成功插入，False 表示容量已满且淘汰策略拒绝。
        """
        spi_id = item["id"]
        ref_id = item.get("ref_object_id", "")
        ref_type = item.get("ref_object_type", "")

        # 容量检查
        if self.size >= self.max_items:
            strategy = self._config.get("pool_overflow_strategy", "prune_lowest_then_reject")
            if strategy == "reject_new":
                return False
            elif strategy in ("prune_lowest_then_reject", "prune_lowest_then_insert"):
                # 淘汰最低能量对象腾出空间
                pruned = self._prune_lowest_energy(count=1)
                if not pruned and strategy == "prune_lowest_then_reject":
                    return False

        # 写入主存储
        self._items[spi_id] = item

        # 更新引用索引
        alias_ids = item.get("ref_alias_ids") or ([ref_id] if ref_id else [])
        for alias_id in alias_ids:
            if alias_id:
                self._ref_index[alias_id] = spi_id

        semantic_signature = item.get("semantic_signature", "")
        if semantic_signature:
            self._semantic_index[semantic_signature] = spi_id

        # 更新类型索引
        if ref_type:
            if ref_type not in self._type_index:
                self._type_index[ref_type] = set()
            self._type_index[ref_type].add(spi_id)

        return True

    def update(self, spi_id: str, item: dict):
        """原地更新一个已有对象。"""
        self._items[spi_id] = item

    def remove(self, spi_id: str) -> dict | None:
        """移除并返回一个对象。"""
        item = self._items.pop(spi_id, None)
        if item:
            ref_id = item.get("ref_object_id", "")
            ref_type = item.get("ref_object_type", "")
            alias_ids = item.get("ref_alias_ids") or ([ref_id] if ref_id else [])
            for alias_id in alias_ids:
                if alias_id and self._ref_index.get(alias_id) == spi_id:
                    del self._ref_index[alias_id]
            semantic_signature = item.get("semantic_signature", "")
            if semantic_signature and self._semantic_index.get(semantic_signature) == spi_id:
                del self._semantic_index[semantic_signature]
            if ref_type and ref_type in self._type_index:
                self._type_index[ref_type].discard(spi_id)
        return item

    def clear(self) -> int:
        """清空全部对象，返回清除数量。"""
        count = len(self._items)
        self._items.clear()
        self._ref_index.clear()
        self._semantic_index.clear()
        self._type_index.clear()
        return count

    def bind_ref_alias(self, spi_id: str, ref_object_id: str):
        """把新的 ref_object_id 绑定到已有对象上，支持语义同一对象跨轮次对齐。"""
        if not ref_object_id:
            return
        item = self._items.get(spi_id)
        if not item:
            return
        alias_ids = item.setdefault("ref_alias_ids", [])
        if ref_object_id not in alias_ids:
            alias_ids.append(ref_object_id)
        self._ref_index[ref_object_id] = spi_id

    # ================================================================== #
    #                     排序和查询                                       #
    # ================================================================== #

    def get_sorted(
        self,
        sort_by: str = "cp_abs",
        top_k: int | None = None,
        descending: bool = True,
    ) -> list[dict]:
        """
        返回排序后的对象列表。

        sort_by: cp_abs | er | ev | updated_at
        top_k: 返回前 K 个（None=全部）
        """
        key_map = {
            "cp_abs": lambda x: x.get("energy", {}).get("cognitive_pressure_abs", 0),
            "er": lambda x: x.get("energy", {}).get("er", 0),
            "ev": lambda x: x.get("energy", {}).get("ev", 0),
            "updated_at": lambda x: x.get("updated_at", 0),
        }
        key_fn = key_map.get(sort_by, key_map["cp_abs"])
        items = sorted(self._items.values(), key=key_fn, reverse=descending)
        if top_k is not None:
            items = items[:top_k]
        return items

    def get_high_cp_items(self, threshold: float = 0.5) -> list[dict]:
        """获取认知压幅值高于阈值的对象。"""
        return [
            item for item in self._items.values()
            if item.get("energy", {}).get("cognitive_pressure_abs", 0) >= threshold
        ]

    # ================================================================== #
    #                     内部淘汰                                         #
    # ================================================================== #

    def _prune_lowest_energy(self, count: int = 1) -> int:
        """淘汰最低能量的对象以腾出空间。"""
        if not self._items:
            return 0
        # 按 er+ev 排序，淘汰最低的
        sorted_ids = sorted(
            self._items.keys(),
            key=lambda sid: (
                self._items[sid].get("energy", {}).get("er", 0)
                + self._items[sid].get("energy", {}).get("ev", 0)
            ),
        )
        pruned = 0
        for sid in sorted_ids[:count]:
            self.remove(sid)
            pruned += 1
        return pruned

    def update_config(self, config: dict):
        """更新配置。"""
        self._config = config

    def rebuild_index(self):
        """重建索引（用于故障恢复）。"""
        self._ref_index.clear()
        self._semantic_index.clear()
        self._type_index.clear()
        for spi_id, item in self._items.items():
            ref_id = item.get("ref_object_id", "")
            ref_type = item.get("ref_object_type", "")
            alias_ids = item.get("ref_alias_ids") or ([ref_id] if ref_id else [])
            for alias_id in alias_ids:
                if alias_id:
                    self._ref_index[alias_id] = spi_id
            semantic_signature = item.get("semantic_signature", "")
            if semantic_signature:
                self._semantic_index[semantic_signature] = spi_id
            if ref_type:
                if ref_type not in self._type_index:
                    self._type_index[ref_type] = set()
                self._type_index[ref_type].add(spi_id)
