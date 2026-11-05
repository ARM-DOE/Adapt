# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Seed-carry configuration: off by default, bounded, and needs projection."""

import pytest
from pydantic import ValidationError

from adapt.configuration.schemas.param import SegmenterConfig
from adapt.configuration.schemas.user import UserSegmenterConfig
from adapt.execution.nodes.detection import DetectModule

pytestmark = pytest.mark.unit


def test_seed_carry_defaults_to_one_frame():
    """One integer governs the carry: 1 means only from the preceding frame."""
    seg = SegmenterConfig()
    assert seg.seed_carry_frames == 1
    assert seg.seed_carry_min_separation == 5
    assert seg.carried_exempt_size_filter is False


def test_seed_carry_can_be_disabled():
    assert SegmenterConfig(seed_carry_frames=0).seed_carry_frames == 0


@pytest.mark.parametrize("field", ["seed_carry_frames", "seed_carry_min_separation"])
def test_seed_carry_bounds_below_zero_are_rejected(field):
    with pytest.raises(ValidationError):
        SegmenterConfig(**{field: -1})


def test_seed_carry_min_separation_below_one_is_rejected():
    with pytest.raises(ValidationError):
        SegmenterConfig(seed_carry_min_separation=0)


def test_legacy_seed_carry_pair_is_folded_into_the_integer():
    """The retired bool + max_frames pair still loads, keeping its meaning."""
    assert UserSegmenterConfig(seed_carry=False).seed_carry_frames == 0
    assert UserSegmenterConfig(seed_carry=True).seed_carry_frames == 2
    assert UserSegmenterConfig(seed_carry=True, seed_carry_max_frames=4).seed_carry_frames == 4


def test_build_config_passes_seed_carry_fields(make_config):
    cfg = make_config(
        segmenter=UserSegmenterConfig(seed_carry_frames=4, seed_carry_min_separation=5)
    )
    det = DetectModule.build_config(cfg)
    assert det.seed_carry_frames == 4
    assert det.seed_carry_min_separation == 5


@pytest.mark.parametrize(
    "selection",
    [
        {"exclude_modules": ["projection"]},
        {"only_modules": ["ingest", "detection"]},
        {"modules": ["ingest", "detection", "analysis"]},
    ],
)
def test_build_config_raises_when_seed_carry_is_on_without_projection(make_config, selection):
    cfg = make_config(segmenter=UserSegmenterConfig(seed_carry_frames=1)).model_copy(
        update=selection
    )
    with pytest.raises(ValueError, match="projection"):
        DetectModule.build_config(cfg)


def test_build_config_accepts_seed_carry_when_projection_is_enabled(make_config):
    cfg = make_config(segmenter=UserSegmenterConfig(seed_carry_frames=1))
    assert DetectModule.build_config(cfg).seed_carry_frames == 1


def test_legacy_closing_kernel_folds_to_a_disk_radius():
    """closing_kernel [w, h] is retired; it maps to closing_radius = max(w, h) // 2."""
    assert UserSegmenterConfig(closing_kernel=(1, 1)).closing_radius == 0  # was a no-op
    assert UserSegmenterConfig(closing_kernel=(5, 5)).closing_radius == 2
    assert UserSegmenterConfig(closing_kernel=(3, 7)).closing_radius == 3
    assert UserSegmenterConfig(closing_radius=1, closing_kernel=(9, 9)).closing_radius == 1
