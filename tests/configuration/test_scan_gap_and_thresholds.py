# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""One scan-gap bound, and independent split/merge thresholds."""

import pytest

from adapt.configuration.schemas.param import ParamConfig
from adapt.execution.nodes.projection import ProjectionModule
from adapt.execution.nodes.tracking import TrackingModule

pytestmark = pytest.mark.unit


def test_scan_gap_lives_only_on_global():
    """The per-module gap fields are gone, so the stages cannot disagree."""
    param = ParamConfig()
    assert param.global_.max_scan_gap_minutes == 10.0
    assert not hasattr(param.projector, "max_time_interval_minutes")
    assert not hasattr(param.tracker, "max_tracking_gap_minutes")


def test_every_stage_reads_the_same_gap(make_config):
    cfg = make_config(**{"global": {"max_scan_gap_minutes": 7.5}})
    assert cfg.global_.max_scan_gap_minutes == 7.5
    assert ProjectionModule.build_config(cfg).max_time_interval_minutes == 7.5
    assert TrackingModule.build_config(cfg).max_tracking_gap_minutes == 7.5


def test_split_and_merge_thresholds_are_independent(make_config):
    param = ParamConfig()
    assert param.tracker.split_overlap_threshold == 0.65
    assert param.tracker.merge_overlap_threshold == 0.7

    cfg = make_config(tracker={"split_overlap_threshold": 0.5})
    tracking = TrackingModule.build_config(cfg)
    assert tracking.split_overlap == 0.5
    assert tracking.merge_overlap == 0.7
