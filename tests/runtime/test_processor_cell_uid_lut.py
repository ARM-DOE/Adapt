"""Processor attaches a cell_label -> cell_uid lookup to the analysis dataset.

The 2D ``cell_labels`` segmentation carries only local labels; before the
analysis NetCDF is written the processor adds a 1D ``cell_uid`` variable so the
global uid travels inside the file.
"""

import numpy as np
import pandas as pd
import pytest
import xarray as xr

from adapt.runtime.processor import RadarProcessor

pytestmark = [pytest.mark.unit, pytest.mark.pipeline]


def _ds_with_labels(max_label: int) -> xr.Dataset:
    labels = np.zeros((4, 4), dtype=int)
    flat = labels.reshape(-1)
    flat[:max_label] = np.arange(1, max_label + 1)
    return xr.Dataset(
        {
            "reflectivity": (("y", "x"), np.ones((4, 4))),
            "cell_labels": (("y", "x"), labels),
        },
        coords={"x": np.arange(4), "y": np.arange(4)},
    )


def test_attaches_cell_uid_indexed_by_label():
    ds = _ds_with_labels(max_label=2)
    tracked = pd.DataFrame({"cell_label": [1, 2], "cell_uid": ["a1", "b2"]})

    out = RadarProcessor._attach_cell_uid_lut(ds, tracked)

    assert out["cell_uid"].dims == ("cell_label",)
    assert list(out["cell_uid"].values.astype(str)) == ["NONE", "a1", "b2"]
    assert list(out["cell_label"].values) == [0, 1, 2]


def test_index_zero_is_none_background():
    ds = _ds_with_labels(max_label=1)
    tracked = pd.DataFrame({"cell_label": [1], "cell_uid": ["a1"]})

    out = RadarProcessor._attach_cell_uid_lut(ds, tracked)

    assert str(out["cell_uid"].values[0]) == "NONE"


def test_no_variable_added_when_tracked_cells_empty():
    ds = _ds_with_labels(max_label=0)
    tracked = pd.DataFrame(columns=["cell_label", "cell_uid"])

    out = RadarProcessor._attach_cell_uid_lut(ds, tracked)

    assert "cell_uid" not in out.data_vars


def test_raises_when_label_in_grid_has_no_uid():
    ds = _ds_with_labels(max_label=3)
    tracked = pd.DataFrame({"cell_label": [1, 2], "cell_uid": ["a1", "b2"]})

    with pytest.raises(ValueError, match="3"):
        RadarProcessor._attach_cell_uid_lut(ds, tracked)
