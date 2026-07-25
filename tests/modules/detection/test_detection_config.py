# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Tests for DetectionConfig — the detection module's own config schema.

Holds exactly the fields the RadarCellSegmenter consumes. Frozen.
"""

import pytest

pytestmark = pytest.mark.unit

from adapt.modules.detection.config import DetectionConfig  # noqa: E402


def _make() -> DetectionConfig:
    return DetectionConfig(
        method="threshold",
        method_params={"threshold": 30.0},
        closing_kernel=(3, 3),
        filter_by_size=True,
        min_cellsize_gridpoint=5,
        max_cellsize_gridpoint=None,
        h_maxima=3.0,
        reflectivity_var="reflectivity",
        labels_var="cell_labels",
        z_level=2000.0,
    )


class TestDetectionConfig:
    def test_holds_all_required_fields(self):
        cfg = _make()
        assert cfg.method == "threshold"
        assert cfg.method_params == {"threshold": 30.0}
        assert cfg.closing_kernel == (3, 3)
        assert cfg.min_cellsize_gridpoint == 5
        assert cfg.max_cellsize_gridpoint is None
        assert cfg.h_maxima == 3.0
        assert cfg.reflectivity_var == "reflectivity"
        assert cfg.labels_var == "cell_labels"
        assert cfg.z_level == 2000.0

    def test_is_frozen(self):
        cfg = _make()
        with pytest.raises((TypeError, ValueError)):
            cfg.method = "conv_strat_raut"
