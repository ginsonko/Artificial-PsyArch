# -*- coding: utf-8 -*-

from __future__ import annotations

import shutil
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from observatory._config_layout import build_config_view, coerce_updates_by_defaults, load_yaml_dict, save_annotated_config
from observatory._app import DEFAULT_CONFIG
from text_sensor.main import _DEFAULT_CONFIG as TEXT_SENSOR_DEFAULT_CONFIG
from state_pool.main import _DEFAULT_CONFIG as STATE_POOL_DEFAULT_CONFIG
from hdb.main import _DEFAULT_CONFIG as HDB_DEFAULT_CONFIG


ROOT = Path(__file__).resolve().parents[2]


def test_build_config_view_covers_all_current_fields():
    module_specs = {
        "observatory": (
            ROOT / "observatory" / "config" / "observatory_config.yaml",
            DEFAULT_CONFIG,
            {},
        ),
        "text_sensor": (
            ROOT / "text_sensor" / "config" / "text_sensor_config.yaml",
            TEXT_SENSOR_DEFAULT_CONFIG,
            {},
        ),
        "state_pool": (
            ROOT / "state_pool" / "config" / "state_pool_config.yaml",
            STATE_POOL_DEFAULT_CONFIG,
            {},
        ),
        "hdb": (
            ROOT / "hdb" / "config" / "hdb_config.yaml",
            HDB_DEFAULT_CONFIG,
            {},
        ),
    }

    for module_name, (path, defaults, runtime_override) in module_specs.items():
        view = build_config_view(
            module_name=module_name,
            path=str(path),
            defaults=defaults,
            file_values=load_yaml_dict(path),
            effective=dict(defaults),
            runtime_override=runtime_override,
        )
        field_count = sum(len(section["fields"]) for section in view["sections"])
        assert field_count == len(defaults)


def test_save_annotated_config_preserves_comments_and_updates_value(tmp_path):
    source = ROOT / "hdb" / "config" / "hdb_config.yaml"
    target = tmp_path / "hdb_config.yaml"
    shutil.copyfile(source, target)

    save_annotated_config(
        path=str(target),
        defaults=HDB_DEFAULT_CONFIG,
        updates={
            "recency_gain_peak": 10.0,
            "recency_gain_decay_ratio": 0.9999976974,
        },
    )

    text = target.read_text(encoding="utf-8")
    assert "recency_gain_peak / 近因增益上限" in text
    assert "recency_gain_peak: 10.0" in text
    assert "recency_gain_decay_ratio / 近因增益每 Tick 保留系数" in text


def test_coerce_updates_by_defaults_handles_list_and_dict_text_payloads():
    updates, rejected = coerce_updates_by_defaults(
        STATE_POOL_DEFAULT_CONFIG,
        {
            "priority_stimulus_target_ref_types": '["st", "sa"]',
            "per_object_type_decay_override": '{"sa": {"er": 0.9, "ev": 0.8}}',
        },
    )

    assert rejected == []
    assert updates["priority_stimulus_target_ref_types"] == ["st", "sa"]
    assert updates["per_object_type_decay_override"] == {"sa": {"er": 0.9, "ev": 0.8}}
