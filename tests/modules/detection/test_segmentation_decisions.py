# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""The segmenter records every seed and carried claim and what became of it.

Same field as the carried-seeds tests: one 48-px blob, peak A at (5, 3) and a
shallow bump B at (5, 8).
"""

import numpy as np
import pytest
import xarray as xr

from adapt.configuration.schemas.user import UserSegmenterConfig
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


def _carried(*blocks: tuple[int, int], ages=None) -> CarriedLabels:
    raster = np.zeros((12, 12), dtype=np.int32)
    for lab, (r, c) in enumerate(blocks, start=1):
        raster[r - 1 : r + 2, c - 1 : c + 2] = lab
    return CarriedLabels(labels=raster, ages=ages or (0,) * len(blocks))


def _segmenter(make_detection_config, **kw) -> RadarCellSegmenter:
    # closing_radius=0: these tests count basin pixels on synthetic squares
    seg_kw = {
        "filter_by_size": False,
        "h_maxima": 5.0,
        "seed_carry": True,
        "closing_radius": 0,
        **kw,
    }
    return RadarCellSegmenter(
        make_detection_config(threshold=35.0, segmenter=UserSegmenterConfig(**seg_kw))
    )


def test_independent_seed_is_recorded_with_its_peak_and_basin(make_detection_config):
    seg = _segmenter(make_detection_config)
    seg.segment(_plateau_ds())
    d = seg.decisions()
    (seed,) = d.seeds
    assert (seed.origin, seed.marker_id, (seed.row, seed.col)) == ("HMAXIMA", 1, PEAK_A)
    assert (seed.peak_value, seed.plateau_px, seed.component_id) == (48.0, 1, 1)
    assert (seed.basin_px, seed.final_label) == (48, 1)
    assert (d.frame.n_components, d.frame.n_independent_seeds, d.frame.n_cells) == (1, 1, 1)
    assert d.frame.mask_px == 48


def test_carried_claims_record_survivor_and_rescue(make_detection_config):
    seg = _segmenter(make_detection_config)
    seg.segment(_plateau_ds(), carried=_carried(PEAK_A, BUMP_B, ages=(0, 1)))
    d = seg.decisions()
    by_prior = {s.prior_label: s for s in d.seeds if s.origin == "CARRIED"}
    assert by_prior[1].decision == "SURVIVOR" and by_prior[1].footprint_holds_seed is True
    assert by_prior[2].decision == "SEEDED" and by_prior[2].marker_id == 2
    assert (
        by_prior[2].component_claims,
        by_prior[2].component_seeds,
        by_prior[2].component_deficit,
    ) == (2, 1, 1)
    assert by_prior[2].final_label == 2 and by_prior[2].basin_px > 0
    (component,) = d.components
    assert (
        component.n_seeds,
        component.n_claims,
        component.n_admitted,
        component.n_final_labels,
    ) == (1, 2, 1, 2)
    assert (d.frame.n_carried_claims, d.frame.n_carried_admitted) == (2, 1)


def test_expired_and_outside_claims_are_recorded_but_do_not_claim(make_detection_config):
    seg = _segmenter(make_detection_config, seed_carry_max_frames=2)
    seg.segment(_plateau_ds(), carried=_carried((1, 1), BUMP_B, ages=(0, 2)))
    d = seg.decisions()
    decisions = {s.prior_label: s.decision for s in d.seeds if s.origin == "CARRIED"}
    assert decisions == {1: "OUTSIDE_MASK", 2: "EXPIRED"}
    assert all(s.component_id is None for s in d.seeds if s.origin == "CARRIED")
    assert d.frame.n_carried_claims == 0


def test_own_footprint_without_deficit_is_recorded_as_no_deficit(make_detection_config):
    seg = _segmenter(make_detection_config)
    seg.segment(_plateau_ds(), carried=_carried((PEAK_A[0], PEAK_A[1] + 4)))
    (claim,) = [s for s in seg.decisions().seeds if s.origin == "CARRIED"]
    assert claim.decision == "NO_DEFICIT" and claim.marker_id is None
    assert claim.clearance_px == pytest.approx(4.0)


def test_size_filter_drop_is_recorded_on_the_seed_and_the_frame(make_detection_config):
    seg = _segmenter(make_detection_config, filter_by_size=True, min_cellsize_gridpoint=60)
    seg.segment(_plateau_ds())
    d = seg.decisions()
    (seed,) = d.seeds
    assert (seed.basin_px, seed.final_label) == (48, 0)
    assert (d.frame.n_basins, d.frame.n_dropped_small, d.frame.n_cells) == (1, 1, 0)


def test_empty_mask_yields_an_empty_record(make_detection_config):
    seg = _segmenter(make_detection_config)
    seg.segment(_plateau_ds().assign(reflectivity=lambda ds: ds.reflectivity * 0))
    d = seg.decisions()
    assert d.seeds == () and d.components == ()
    assert (d.frame.mask_px, d.frame.n_cells) == (0, 0)
