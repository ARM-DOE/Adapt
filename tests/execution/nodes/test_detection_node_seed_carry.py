# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""DetectModule hands the previous scan's advected labels to the segmenter.

Same synthetic field as the segmenter tests: peak A at (5, 3) is a real
h-maxima seed; bump B at (5, 8) is too shallow to seed on its own. The prior
scan had two cells (A = label 1, B = label 2), so B's core is the one lost.
"""

from datetime import UTC, datetime

import numpy as np
import pytest
import xarray as xr

from adapt.configuration.schemas.user import UserSegmenterConfig
from adapt.contracts import ContractViolation, check_prior_scan, check_seed_carry
from adapt.execution.nodes.detection import DetectModule

pytestmark = pytest.mark.unit

PEAK_A = (5, 3)
BUMP_B = (5, 8)
T0 = datetime(2024, 1, 1, 0, 0, tzinfo=UTC)


def _plateau_ds() -> xr.Dataset:
    field = np.full((12, 12), 10.0, dtype=np.float32)
    field[3:9, 2:10] = 40.0
    field[PEAK_A] = 48.0
    field[BUMP_B] = 43.0
    return xr.Dataset(
        {"reflectivity": (("y", "x"), field)},
        coords={"y": np.arange(12), "x": np.arange(12)},
    )


def _advected(*blocks: tuple[int, int]) -> np.ndarray:
    out = np.zeros((12, 12), dtype=np.int32)
    for lab, (r, c) in enumerate(blocks, start=1):
        out[r - 1 : r + 2, c - 1 : c + 2] = lab
    return out


def _projected_ds(advected: np.ndarray) -> xr.Dataset:
    """A projected_ds whose frame_offset=1 slice is ``advected``."""
    stack = np.stack([np.zeros_like(advected), advected], axis=0)
    return xr.Dataset(
        {"cell_projections": (("frame_offset", "y", "x"), stack)},
        coords={"frame_offset": [0, 1], "y": np.arange(12), "x": np.arange(12)},
    )


def _prior(advected: np.ndarray | None, ages: tuple[int, ...]) -> dict:
    prior = {"scan_time": T0, "seed_carry": ages, "num_cells": len(ages)}
    if advected is not None:
        prior["projected_ds"] = _projected_ds(advected)
    return prior


def _run(make_detection_config, prior, **segmenter_kw) -> dict:
    kw = {"filter_by_size": False, "h_maxima": 5.0, "seed_carry": True, **segmenter_kw}
    ctx = {
        "grid_ds_2d": _plateau_ds(),
        "grid_ds": None,
        "prior_scan": prior,
        "detection_config": make_detection_config(
            threshold=35.0, segmenter=UserSegmenterConfig(**kw)
        ),
    }
    return DetectModule().run(ctx)


def test_node_declares_prior_scan_input_and_seed_carry_output():
    assert "prior_scan" in DetectModule.inputs
    assert "seed_carry" in DetectModule.outputs
    assert DetectModule.input_contracts["prior_scan"] is check_prior_scan
    assert DetectModule.output_contracts["seed_carry"] is check_seed_carry


def test_seed_carry_is_empty_with_the_flag_off(make_detection_config):
    out = _run(make_detection_config, _prior(_advected(PEAK_A, BUMP_B), (0, 0)), seed_carry=False)
    assert out["seed_carry"] == ()
    assert out["num_cells"] == 1


def test_no_carry_when_prior_scan_is_none(make_detection_config):
    out = _run(make_detection_config, None)
    assert out["num_cells"] == 1
    assert out["seed_carry"] == (0,)


def test_no_carry_when_prior_has_no_motion_field(make_detection_config):
    out = _run(make_detection_config, _prior(None, (0, 0)))
    assert out["num_cells"] == 1
    assert out["seed_carry"] == (0,)


def test_lost_core_of_a_second_prior_cell_is_rescued(make_detection_config):
    out = _run(make_detection_config, _prior(_advected(PEAK_A, BUMP_B), (0, 0)))
    labels = out["segmented_ds"]["cell_labels"].values
    assert out["num_cells"] == 2
    assert out["seed_carry"][labels[BUMP_B] - 1] == 1
    assert out["seed_carry"][labels[PEAK_A] - 1] == 0


def test_single_prior_cell_never_splits_its_own_core(make_detection_config):
    own_footprint_4px_off = (PEAK_A[0], PEAK_A[1] + 4)
    out = _run(make_detection_config, _prior(_advected(own_footprint_4px_off), (0,)))
    assert out["num_cells"] == 1


def test_label_at_the_carry_bound_is_not_offered(make_detection_config):
    out = _run(
        make_detection_config,
        _prior(_advected(PEAK_A, BUMP_B), (0, 2)),
        seed_carry_max_frames=2,
    )
    assert out["num_cells"] == 1


def test_seed_carry_length_must_match_prior_cell_count(make_detection_config):
    prior = _prior(_advected(PEAK_A, BUMP_B), (0, 0))
    prior["num_cells"] = 1
    with pytest.raises(ContractViolation):
        _run(make_detection_config, prior)


def test_centroid_outside_a_hollow_footprint_is_snapped_onto_it(make_detection_config):
    advected = _advected(PEAK_A, BUMP_B)
    advected[BUMP_B] = 0  # B's footprint is a ring; its centroid is the empty centre
    out = _run(make_detection_config, _prior(advected, (0, 0)))
    assert out["num_cells"] == 2
