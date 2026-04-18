# -*- coding: utf-8 -*-

from __future__ import annotations

import json
from pathlib import Path

import pytest

from tools.expand_episode_dataset import DatasetValidationError, validate_and_normalize_dataset, expand_dataset


def test_validate_rejects_missing_dataset_id():
    with pytest.raises(DatasetValidationError):
        validate_and_normalize_dataset({"seed": 1, "time_basis": "tick", "tick_dt_ms": 100, "episodes": []})


def test_expand_produces_stable_tick_indices_and_empty_text():
    raw = {
        "dataset_id": "unit_test_ds",
        "seed": 123,
        "time_basis": "tick",
        "tick_dt_ms": 50,
        "episodes": [
            {"id": "ep1", "repeat": 2, "ticks": [{"text": "A"}, {"empty": True}]},
            {"id": "ep2", "ticks": ["B"]},
        ],
    }
    ds = validate_and_normalize_dataset(raw)
    items = list(expand_dataset(ds))

    assert [it["tick_index"] for it in items] == list(range(len(items)))
    assert items[1]["input_is_empty"] is True
    assert items[1]["input_text"] == ""
    assert items[-1]["episode_id"] == "ep2"


def test_smoke_100_yaml_expands_to_100_ticks(tmp_path: Path):
    yaml_path = Path("datasets/smoke_100_v0.yaml").resolve()
    assert yaml_path.exists()

    import yaml  # type: ignore

    raw = yaml.safe_load(yaml_path.read_text(encoding="utf-8"))
    ds = validate_and_normalize_dataset(raw)
    items = list(expand_dataset(ds))
    assert len(items) == 100

    # Ensure the JSON we will write later is valid and contains required keys.
    sample = items[0]
    for k in ("dataset_id", "seed", "time_basis", "tick_index", "episode_id", "input_text", "input_is_empty"):
        assert k in sample
    json.dumps(sample, ensure_ascii=False)

