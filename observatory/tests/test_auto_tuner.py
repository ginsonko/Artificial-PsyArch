# -*- coding: utf-8 -*-

from __future__ import annotations

from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from observatory.experiment import auto_tuner, param_catalog


def test_default_metric_targets_include_distribution_envelopes_for_reward_punish_and_nt_channels():
    targets = {item.key: item for item in auto_tuner._default_metric_targets()}

    for key in ["rwd_pun_rwd", "rwd_pun_pun", "nt_DA", "nt_ADR", "nt_COR", "nt_SER", "nt_OXY", "nt_END"]:
        target = targets[key]
        assert target.high_band_threshold is not None
        assert target.high_band_max_ratio is not None
        assert target.high_band_soft_p95 is not None
        assert target.high_band_max_run is not None


def test_normalize_metric_target_item_accepts_distribution_fields_and_tolerates_bad_run_cap():
    norm = auto_tuner._normalize_metric_target_item(
        {
            "key": "rwd_pun_rwd",
            "expected_min": 0.0,
            "expected_max": 0.55,
            "ideal": 0.24,
            "min_std": 0.03,
            "weight": 0.6,
            "high_band_threshold": "0.5",
            "high_band_max_ratio": "0.2",
            "high_band_soft_p95": "0.72",
            "high_band_max_run": "bad-value",
        }
    )

    assert norm is not None
    assert norm["high_band_threshold"] == 0.5
    assert norm["high_band_max_ratio"] == 0.2
    assert norm["high_band_soft_p95"] == 0.72
    assert norm["high_band_max_run"] is None


def test_merge_metric_target_defs_keeps_catalog_distribution_defaults_when_override_omits_optional_fields(monkeypatch):
    monkeypatch.setattr(
        auto_tuner,
        "_load_raw_config_dict",
        lambda: {
            "metric_targets": [
                {
                    "key": "rwd_pun_rwd",
                    "expected_min": 0.0,
                    "expected_max": 0.55,
                    "ideal": 0.25,
                    "min_std": 0.02,
                    "weight": 0.55,
                }
            ]
        },
    )

    merged = {item["key"]: item for item in auto_tuner._merge_metric_target_defs()}
    reward_def = merged["rwd_pun_rwd"]

    assert reward_def["expected_max"] == 0.55
    assert reward_def["ideal"] == 0.25
    assert reward_def["high_band_threshold"] == 0.50
    assert reward_def["high_band_max_ratio"] == 0.20
    assert reward_def["high_band_soft_p95"] == 0.72
    assert reward_def["high_band_max_run"] == 3


def test_metric_issue_snapshot_uses_high_band_occupancy_even_when_mean_is_not_above_expected_max():
    target = auto_tuner.MetricTarget(
        key="cfs_dissonance_max",
        expected_min=0.0,
        expected_max=0.55,
        ideal=0.18,
        min_std=0.03,
        high_band_threshold=0.50,
        high_band_max_ratio=0.20,
        high_band_soft_p95=0.68,
        high_band_max_run=2,
    )
    tuner = auto_tuner.AutoTuner.__new__(auto_tuner.AutoTuner)
    tuner.metric_targets = {target.key: target}

    rows = [{"cfs_dissonance_max": value} for value in [0.12, 0.18, 0.20, 0.16, 0.74, 0.76, 0.78, 0.14, 0.18, 0.20]]
    issue = auto_tuner.AutoTuner._metric_issue_snapshot(tuner, rows=rows, metric_key="cfs_dissonance_max")

    assert issue is not None
    assert issue["mean"] < target.expected_max
    assert issue["band"]["occupancy_ratio"] == 0.3
    assert issue["band"]["occupancy_over_ratio"] > 0.0
    assert issue["high_ratio"] > 0.0


def test_auto_tuner_constructor_preserves_default_distribution_caps_for_partial_metric_overrides(monkeypatch, tmp_path):
    monkeypatch.setattr(auto_tuner, "load_auto_tuner_config", lambda: auto_tuner.AutoTunerConfig())
    monkeypatch.setattr(
        auto_tuner,
        "_load_raw_config_dict",
        lambda: {
            "metric_targets": [
                {
                    "key": "rwd_pun_rwd",
                    "expected_min": 0.0,
                    "expected_max": 0.55,
                    "ideal": 0.25,
                    "min_std": 0.02,
                    "weight": 0.55,
                }
            ]
        },
    )
    monkeypatch.setattr(auto_tuner, "load_auto_tuner_rules", lambda: auto_tuner._default_rules_payload())
    monkeypatch.setattr(auto_tuner.AutoTuner, "_load_state", lambda self: auto_tuner._default_state_payload())
    monkeypatch.setattr(auto_tuner.AutoTuner, "_ensure_candidate_observations", lambda self: None)
    monkeypatch.setattr(auto_tuner.param_catalog, "build_param_catalog", lambda app=None: [])
    monkeypatch.setattr(auto_tuner.param_catalog, "build_default_param_bounds", lambda specs: {})

    tuner = auto_tuner.AutoTuner(
        app=None,
        run_dir=tmp_path / "run",
        enabled=False,
        enable_short_term=False,
        enable_long_term=False,
    )

    reward_target = tuner.metric_targets["rwd_pun_rwd"]
    assert reward_target.expected_max == 0.55
    assert reward_target.ideal == 0.25
    assert reward_target.high_band_threshold == 0.50
    assert reward_target.high_band_max_ratio == 0.20
    assert reward_target.high_band_soft_p95 == 0.72
    assert reward_target.high_band_max_run == 3


def test_param_catalog_exposes_distribution_metadata_for_reward_punish_and_nt_metrics():
    definitions = {item["key"]: item for item in param_catalog.list_metric_definitions()}

    for key in ["cfs_dissonance_max", "cfs_pressure_max", "cfs_expectation_max", "rwd_pun_rwd", "rwd_pun_pun", "nt_DA", "nt_ADR", "nt_COR", "nt_SER", "nt_OXY", "nt_END"]:
        item = definitions[key]
        assert item["high_band_threshold"] == 0.50
        assert "description" in item and item["description"]

    assert definitions["rwd_pun_rwd"]["expected_max"] == 0.60
    assert definitions["rwd_pun_rwd"]["ideal"] == 0.24
    assert "半量程" in definitions["rwd_pun_rwd"]["description"]
