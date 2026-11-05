# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Carried-seed basins are spared by the minimum-size filter.

A carried seed is only ever admitted where a previous cell lost its core, so
its basin is small by construction. Deleting it undoes the rescue and leaves
the previous cell with no successor, which the tracker then reports as a
termination — the filter silently cancelling the carry. Only the minimum-size
test is waived; the maximum still applies.
"""

import numpy as np
import pytest

from adapt.configuration.schemas.user import UserSegmenterConfig
from adapt.modules.detection.module import RadarCellSegmenter

pytestmark = pytest.mark.unit


@pytest.fixture
def basins():
    """One basin below the size floor (2 px) and one above it (9 px)."""
    out = np.zeros((10, 10), dtype=np.int32)
    out[0, 0:2] = 1
    out[5:8, 5:8] = 2
    return out


def _segmenter(make_detection_config, **kw):
    return RadarCellSegmenter(
        make_detection_config(
            threshold=35.0,
            segmenter=UserSegmenterConfig(filter_by_size=True, min_cellsize_gridpoint=5, **kw),
        )
    )


def test_a_small_basin_is_dropped_without_an_exemption(make_detection_config, basins):
    seg = _segmenter(make_detection_config)
    out = seg._filter_and_relabel(basins)
    assert sorted(np.unique(out[out > 0])) == [1]
    assert int((out == 1).sum()) == 9  # only the large basin survived


def test_an_exempt_basin_survives_the_size_floor(make_detection_config, basins):
    seg = _segmenter(make_detection_config)
    out = seg._filter_and_relabel(basins, frozenset({1}))
    kept = sorted(np.unique(out[out > 0]))
    assert kept == [1, 2]
    # labels are still size-ranked, so the 9 px basin takes rank 1
    assert int((out == 1).sum()) == 9
    assert int((out == 2).sum()) == 2


def test_the_maximum_size_filter_still_applies_to_exempt_basins(make_detection_config, basins):
    seg = RadarCellSegmenter(
        make_detection_config(
            threshold=35.0,
            segmenter=UserSegmenterConfig(
                filter_by_size=True, min_cellsize_gridpoint=5, max_cellsize_gridpoint=4
            ),
        )
    )
    out = seg._filter_and_relabel(basins, frozenset({1, 2}))
    # basin 2 (9 px) exceeds the maximum and goes, basin 1 (2 px) is spared
    assert sorted(np.unique(out[out > 0])) == [1]
    assert int((out == 1).sum()) == 2
