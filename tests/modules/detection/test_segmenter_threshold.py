"""Test RadarCellSegmenter threshold-based segmentation."""

import numpy as np
import pytest

from adapt.contracts import ContractViolation
from adapt.modules.detection.module import RadarCellSegmenter

pytestmark = pytest.mark.unit


def test_threshold_filters_all(simple_2d_ds, make_detection_config):
    """Threshold higher than max value results in no cells."""
    config = make_detection_config(threshold=50)  # Higher than 40 in simple_2d_ds
    seg = RadarCellSegmenter(config)

    out = seg.segment(simple_2d_ds)

    assert "cell_labels" in out
    labels = out["cell_labels"].values

    # No cells should exist
    assert labels.max() == 0
    assert np.count_nonzero(labels) == 0


def test_threshold_creates_at_least_one_cell(simple_2d_ds, make_detection_config):
    """Threshold below max value creates cells."""
    config = make_detection_config(threshold=30, min_cellsize_gridpoint=2)
    seg = RadarCellSegmenter(config)

    out = seg.segment(simple_2d_ds)

    assert "cell_labels" in out
    labels = out["cell_labels"].values

    assert labels.max() >= 1
    assert np.count_nonzero(labels) > 0


def test_no_cells_below_threshold(empty_2d_ds, detection_module_config):
    """Empty dataset (all zeros) produces no cells."""
    seg = RadarCellSegmenter(detection_module_config)

    out = seg.segment(empty_2d_ds)
    labels = out["cell_labels"].values

    assert labels.max() == 0


def test__multiple_cells(large_multi_cell_ds, make_detection_config):
    """Multiple distinct cells are detected and labeled."""
    # Don't filter by size for this test
    from adapt.configuration.schemas.user import UserSegmenterConfig

    config = make_detection_config(
        threshold=30, segmenter=UserSegmenterConfig(filter_by_size=False)
    )
    seg = RadarCellSegmenter(config)

    out = seg.segment(large_multi_cell_ds)
    labels = out["cell_labels"].values

    # Expect four distinct cells
    assert labels.max() == 4


def test_detection_raises_naming_config_when_field_missing(simple_2d_ds, make_detection_config):
    # The module boundary owns the "is my configured field present" check
    # (the executor-bound grid contract is structural only).
    ds = simple_2d_ds.rename({"reflectivity": "pressure"})
    seg = RadarCellSegmenter(make_detection_config())

    with pytest.raises(ContractViolation, match="tracking field"):
        seg.segment(ds)
