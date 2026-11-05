"""Test RadarCellSegmenter morphological operations."""

import pytest

from adapt.modules.detection.module import RadarCellSegmenter

pytestmark = pytest.mark.unit


def test_close_cells_without_closing(close_cells_ds, make_detection_config):
    """Without morphological closing, nearby cells remain separate."""
    from adapt.configuration.schemas.user import UserSegmenterConfig

    config = make_detection_config(
        threshold=30, segmenter=UserSegmenterConfig(filter_by_size=False)
    )
    seg = RadarCellSegmenter(config)

    labels = seg.segment(close_cells_ds)["cell_labels"].values

    # Two cells separated by gap should remain separate
    assert labels.max() == 2


def test_close_cells_with_closing(close_cells_ds, make_detection_config):
    """Closing fills the gap but maxtree still resolves two distinct intensity peaks."""
    from adapt.configuration.schemas.user import UserSegmenterConfig

    config = make_detection_config(
        threshold=30,
        segmenter=UserSegmenterConfig(filter_by_size=False, closing_radius=1),
    )
    seg = RadarCellSegmenter(config)

    labels = seg.segment(close_cells_ds)["cell_labels"].values

    # Closing merges the binary mask, but the two reflectivity peaks remain
    # distinct (separated by a 0-value gap in the original field), so maxtree
    # seeds two watershed regions and correctly labels them as two cells.
    assert labels.max() == 2
