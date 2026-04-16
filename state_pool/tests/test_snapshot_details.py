# -*- coding: utf-8 -*-
"""
状态池快照增强测试
==================

重点覆盖：
1. CSA 快照必须能展示锚点和属性摘要。
2. 属性绑定后，快照里必须能看到运行时绑定属性。
3. Tick 维护必须推进内部 tick 计数，并刷新疲劳等维护字段。
"""

import os
import sys
import time

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from state_pool.main import StatePool
from state_pool._id_generator import reset_id_generator


@pytest.fixture
def pool():
    reset_id_generator()
    instance = StatePool(
        config_override={
            "pool_max_items": 100,
            "enable_placeholder_interfaces": False,
            "enable_script_broadcast": False,
        }
    )
    yield instance
    instance._logger.close()


def build_packet_with_csa_details():
    now_ms = int(time.time() * 1000)
    feature_sa = {
        "id": "sa_feature_001",
        "object_type": "sa",
        "content": {"raw": "你", "display": "你", "value_type": "discrete"},
        "stimulus": {"role": "feature", "modality": "text"},
        "energy": {"er": 1.0, "ev": 0.0},
        "created_at": now_ms,
        "updated_at": now_ms,
    }
    attribute_sa = {
        "id": "sa_attr_001",
        "object_type": "sa",
        "content": {
            "raw": "stimulus_intensity:1.0",
            "display": "stimulus_intensity:1.0",
            "value_type": "numerical",
        },
        "stimulus": {"role": "attribute", "modality": "text"},
        "energy": {"er": 0.0, "ev": 0.0},
        "created_at": now_ms,
        "updated_at": now_ms,
    }
    csa = {
        "id": "csa_feature_001",
        "object_type": "csa",
        "anchor_sa_id": feature_sa["id"],
        "member_sa_ids": [feature_sa["id"], attribute_sa["id"]],
        "content": {"display": "CSA[你]"},
        "energy": {"er": 1.0, "ev": 0.0},
        "created_at": now_ms,
        "updated_at": now_ms,
    }
    return {
        "id": "spkt_detail_001",
        "object_type": "stimulus_packet",
        "sa_items": [feature_sa, attribute_sa],
        "csa_items": [csa],
        "trace_id": "detail_trace",
    }


def test_csa_snapshot_contains_anchor_and_attribute_summary(pool):
    packet = build_packet_with_csa_details()
    result = pool.apply_stimulus_packet(packet, trace_id="detail_trace")
    assert result["success"] is True

    snapshot = pool.get_state_snapshot("snap_detail", top_k=10)["data"]["snapshot"]
    # 默认配置下 CSA 不作为独立 state_item 入池；但 CSA/属性信息应折叠到锚点 SA 的快照里，
    # 以便前端可读、避免出现大量 "CSA[...]" 噪音对象。
    sa_items = [item for item in snapshot["top_items"] if item["ref_object_type"] == "sa" and item["display"] == "你"]
    assert sa_items

    anchor_summary = sa_items[0]
    assert "stimulus_intensity:1.0" in anchor_summary["attribute_displays"]
    assert "attrs=stimulus_intensity:1.0" in anchor_summary["display_detail"]


def test_binding_updates_runtime_attribute_summary(pool):
    packet = build_packet_with_csa_details()
    pool.apply_stimulus_packet(packet, trace_id="bind_trace")

    snapshot = pool.get_state_snapshot("snap_bind", top_k=10)["data"]["snapshot"]
    sa_items = [item for item in snapshot["top_items"] if item["ref_object_type"] == "sa" and item["display"] == "你"]
    assert sa_items

    attribute_sa = {
        "id": "sa_attr_correctness_001",
        "object_type": "sa",
        "content": {"raw": "correctness:high", "display": "correctness:high", "value_type": "discrete"},
        "stimulus": {"role": "attribute", "modality": "internal"},
        "energy": {"er": 0.0, "ev": 0.0},
    }
    result = pool.bind_attribute_node_to_object(
        target_item_id=sa_items[0]["item_id"],
        attribute_sa=attribute_sa,
        trace_id="bind_trace_2",
        source_module="pytest",
    )
    assert result["success"] is True

    after_snapshot = pool.get_state_snapshot("snap_bind_after", top_k=20)["data"]["snapshot"]
    bound_sa = next(item for item in after_snapshot["top_items"] if item["item_id"] == sa_items[0]["item_id"])
    assert "correctness:high" in bound_sa["bound_attribute_displays"]
    # 默认不自动创建“绑定型 CSA”state_item；只更新锚点对象的运行态绑定快照即可。
    assert result["data"]["created_new_csa"] is False
    assert result["data"]["bound_csa_item_id"] in (None, "")
    assert "runtime_attrs=correctness:high" in bound_sa["display_detail"]


def test_tick_maintenance_advances_tick_counter_and_fatigue(pool):
    packet = build_packet_with_csa_details()
    pool.apply_stimulus_packet(packet, trace_id="tick_trace")
    before_tick = pool._tick_counter

    result = pool.tick_maintain_state_pool(trace_id="tick_maintain_trace")
    assert result["success"] is True
    assert pool._tick_counter == before_tick + 1

    snapshot = pool.get_state_snapshot("tick_snapshot", top_k=10)["data"]["snapshot"]
    top_item = snapshot["top_items"][0]
    assert top_item["recency_gain"] > 1.0
    assert top_item["fatigue"] == pytest.approx(0.0)


