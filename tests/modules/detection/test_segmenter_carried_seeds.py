# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Carried seeds: the previous frame's advected labels as extra watershed markers.

The field below is one 35 dBZ blob with a strong peak A at (5, 3) and a
shallow bump B at (5, 8) that rises only 3 dBZ above the plateau — below the
h-maxima height of 5 — so on its own the operator finds one cell.

A carried seed is admitted only when its connected component has *lost* a
core: with k independent seeds and n prior cells landing in the component,
at most n − k carried seeds are added, farthest from a seed first. So the
previous frame's own footprint, mispositioned by advection error, never
splits a healthy core; a second prior cell whose core dropped below h does
get rescued.
"""

import numpy as np
import pytest
import xarray as xr

from adapt.configuration.schemas.user import UserSegmenterConfig
from adapt.contracts import ContractViolation
from adapt.modules.detection.module import CarriedLabels, RadarCellSegmenter

pytestmark = pytest.mark.unit

PEAK_A = (5, 3)
BUMP_B = (5, 8)


def _plateau_ds() -> xr.Dataset:
    field = np.full((12, 12), 10.0, dtype=np.float32)
    field[3:9, 2:10] = 40.0
    field[PEAK_A] = 48.0
    field[BUMP_B] = 43.0
    return xr.Dataset(
        {"reflectivity": (("y", "x"), field)},
        coords={"y": np.arange(12), "x": np.arange(12)},
    )


def _raster(*blocks: tuple[int, int]) -> np.ndarray:
    """Advected-label raster with one 3x3 footprint per block centre, labels 1..n."""
    out = np.zeros((12, 12), dtype=np.int32)
    for lab, (r, c) in enumerate(blocks, start=1):
        out[r - 1 : r + 2, c - 1 : c + 2] = lab
    return out


def _carried(*blocks: tuple[int, int], ages: tuple[int, ...] | None = None) -> CarriedLabels:
    return CarriedLabels(labels=_raster(*blocks), ages=ages or (0,) * len(blocks))


def _segmenter(make_detection_config, **segmenter_kw) -> RadarCellSegmenter:
    kw = {"filter_by_size": False, "h_maxima": 5.0, "seed_carry": True, **segmenter_kw}
    return RadarCellSegmenter(
        make_detection_config(threshold=35.0, segmenter=UserSegmenterConfig(**kw))
    )


def test_labels_identical_with_no_carried_labels(make_detection_config):
    seg = _segmenter(make_detection_config)
    plain = seg.segment(_plateau_ds())["cell_labels"].values
    none = seg.segment(_plateau_ds(), carried=None)["cell_labels"].values
    assert np.array_equal(plain, none)
    assert int(plain.max()) == 1


def test_carry_age_is_all_zero_when_nothing_is_offered(make_detection_config):
    out = _segmenter(make_detection_config).segment(_plateau_ds())
    assert tuple(out["cell_labels"].attrs["carry_age"]) == (0,)


def test_carry_age_is_absent_when_the_flag_is_off(make_detection_config):
    out = _segmenter(make_detection_config, seed_carry=False).segment(_plateau_ds())
    assert "carry_age" not in out["cell_labels"].attrs


def test_carried_labels_with_the_flag_off_raise(make_detection_config):
    seg = _segmenter(make_detection_config, seed_carry=False)
    with pytest.raises(ContractViolation):
        seg.segment(_plateau_ds(), carried=_carried(BUMP_B))


def test_footprint_outside_the_mask_is_ignored(make_detection_config):
    out = _segmenter(make_detection_config).segment(_plateau_ds(), carried=_carried((1, 1)))
    assert int(out["cell_labels"].values.max()) == 1
    assert tuple(out["cell_labels"].attrs["carry_age"]) == (0,)


def test_own_footprint_mispositioned_by_advection_does_not_split_the_core(make_detection_config):
    """One prior cell, one seed: nothing was lost, so nothing is added."""
    own_footprint_4px_off = (PEAK_A[0], PEAK_A[1] + 4)
    out = _segmenter(make_detection_config).segment(
        _plateau_ds(), carried=_carried(own_footprint_4px_off)
    )
    assert int(out["cell_labels"].values.max()) == 1
    assert tuple(out["cell_labels"].attrs["carry_age"]) == (0,)


def test_second_prior_cell_in_a_one_seed_component_is_rescued(make_detection_config):
    out = _segmenter(make_detection_config).segment(
        _plateau_ds(), carried=_carried(PEAK_A, BUMP_B, ages=(0, 1))
    )
    labels = out["cell_labels"].values
    ages = tuple(out["cell_labels"].attrs["carry_age"])

    assert int(labels.max()) == 2
    assert labels[PEAK_A] != labels[BUMP_B]
    assert ages[labels[BUMP_B] - 1] == 2  # carried one more frame
    assert ages[labels[PEAK_A] - 1] == 0  # independently detected


def test_only_the_deficit_is_admitted_farthest_from_a_seed_first(make_detection_config):
    """Two prior cells besides A's own, one seed → deficit 2, but the second
    candidate sits within min_separation of the first admitted marker."""
    near_b = (BUMP_B[0] + 1, BUMP_B[1] - 1)
    out = _segmenter(make_detection_config).segment(
        _plateau_ds(), carried=_carried(PEAK_A, near_b, BUMP_B)
    )
    labels = out["cell_labels"].values
    assert int(labels.max()) == 2
    assert labels[BUMP_B] != labels[PEAK_A]  # the farther candidate (B) was the one admitted


def test_prior_cell_at_the_carry_bound_is_not_offered(make_detection_config):
    out = _segmenter(make_detection_config, seed_carry_max_frames=2).segment(
        _plateau_ds(), carried=_carried(PEAK_A, BUMP_B, ages=(0, 2))
    )
    assert int(out["cell_labels"].values.max()) == 1


def test_labels_stay_contiguous_and_size_ranked_with_carried_markers(make_detection_config):
    labels = (
        _segmenter(make_detection_config)
        .segment(_plateau_ds(), carried=_carried(PEAK_A, BUMP_B))["cell_labels"]
        .values
    )
    present = sorted(set(labels.flatten()) - {0})
    assert present == [1, 2]
    assert (labels == 1).sum() >= (labels == 2).sum()
