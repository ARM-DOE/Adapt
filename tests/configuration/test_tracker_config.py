# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Tracker v2 parameters: defaults, overrides reach the tracker, legacy keys fail loudly."""

import pytest

from adapt.configuration.schemas.errors import ConfigError
from adapt.configuration.schemas.param import ParamConfig
from adapt.execution.nodes.tracking import TrackingModule

pytestmark = pytest.mark.unit


def test_physical_limits_and_rule_defaults():
    t = ParamConfig().tracker
    assert (t.max_speed_ms, t.max_acceleration_ms2) == (40.0, 0.04)
    assert (t.growth_max_km, t.growth_size_km) == (2.0, 12.0)
    assert (t.residual_weight, t.heading_weight) == (0.25, 1.0)
    assert (t.latent_scans, t.identity_intensity_weight, t.identity_score_margin) == (2, 1.0, 0.33)
    assert (t.cell_threshold, t.polarity) == (30.0, 1)


def test_rule_thresholds_are_in_their_valid_ranges():
    t = ParamConfig().tracker
    assert 0.0 < t.max_overlap_mismatch <= 1.0
    assert t.max_link_cost > 0.0
    assert 0.0 < t.split_overlap_threshold <= 1.0
    assert 0.0 < t.merge_overlap_threshold <= 1.0


def test_override_reaches_the_tracker(make_config):
    cfg = TrackingModule.build_config(
        make_config(tracker={"max_link_cost": 1.5, "latent_scans": 3, "polarity": -1})
    )
    assert (cfg.max_link_cost, cfg.latent_scans, cfg.polarity) == (1.5, 3, -1)


@pytest.mark.parametrize(
    ("legacy", "replacement"),
    [
        ("minimum_candidate_overlap", "max_overlap_mismatch"),
        ("projected_hull_buffer_km", "growth_max_km"),
        ("max_speed_multiplier", "max_acceleration_ms2"),
        ("heading_change_penalty_weight", "heading_weight"),
        ("length_scale", "residual_weight"),
    ],
)
def test_legacy_key_fails_naming_its_replacement(make_config, legacy, replacement):
    with pytest.raises(ConfigError, match=replacement):
        make_config(tracker={legacy: 1.0})


def test_polarity_must_be_a_sign(make_config):
    with pytest.raises(ConfigError):
        make_config(tracker={"polarity": 2})


def test_negative_latency_is_rejected(make_config):
    with pytest.raises(ConfigError):
        make_config(tracker={"latent_scans": -1})
