# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Size ranking must not depend on label values being contiguous from 0."""

import numpy as np
import pytest
import xarray as xr

from adapt.configuration.schemas.user import UserSegmenterConfig
from adapt.modules.detection.module import RadarCellSegmenter

pytestmark = pytest.mark.unit


def _all_convective_two_dome_ds() -> xr.Dataset:
    """12x12 field entirely above threshold with two domes of different size.

    Base 40 dBZ; a tall narrow dome at (3, 3) and a broader one at (8, 8).
    With threshold 35 the convective mask covers every pixel, so the label
    image contains no background (no 0), which is the case a positional
    label lookup gets wrong.
    """
    yy, xx = np.mgrid[0:12, 0:12]
    field = (
        40.0
        + 18.0 * np.exp(-((yy - 3) ** 2 + (xx - 3) ** 2) / 4.0)
        + 14.0 * np.exp(-((yy - 8) ** 2 + (xx - 8) ** 2) / 12.0)
    ).astype(np.float32)
    return xr.Dataset(
        {"reflectivity": (("y", "x"), field)},
        coords={"y": np.arange(12), "x": np.arange(12)},
    )


def test_size_ranking_when_every_pixel_is_convective(make_detection_config):
    """Both domes are labelled, nothing is dropped, and the larger one is label 1."""
    config = make_detection_config(
        threshold=35.0, segmenter=UserSegmenterConfig(filter_by_size=False)
    )
    labels = RadarCellSegmenter(config).segment(_all_convective_two_dome_ds())["cell_labels"].values

    assert int(labels.max()) == 2
    assert int((labels == 0).sum()) == 0
    assert (labels == 1).sum() >= (labels == 2).sum()
