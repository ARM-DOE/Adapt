# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Cell-uid LUT attachment — tracking-owned science, pure functions.

``attach_cell_uid_lut`` resolves the current scan's local labels to global
uids; ``attach_registration_uid_lut`` resolves the *previous* scan's labels
(carried by ``registration_minutes``). Both return a new dataset and never
mutate their input.
"""

import numpy as np
import pandas as pd
import pytest
import xarray as xr

from adapt.modules.tracking.lut import attach_cell_uid_lut, attach_registration_uid_lut

pytestmark = pytest.mark.unit


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


class TestAttachCellUidLut:
    def test_attaches_cell_uid_indexed_by_label(self):
        ds = _ds_with_labels(max_label=2)
        tracked = pd.DataFrame({"cell_label": [1, 2], "cell_uid": ["a1", "b2"]})

        out = attach_cell_uid_lut(ds, tracked)

        assert out["cell_uid"].dims == ("cell_label",)
        assert list(out["cell_uid"].values.astype(str)) == ["NONE", "a1", "b2"]
        assert list(out["cell_label"].values) == [0, 1, 2]

    def test_index_zero_is_none_background(self):
        ds = _ds_with_labels(max_label=1)
        tracked = pd.DataFrame({"cell_label": [1], "cell_uid": ["a1"]})

        out = attach_cell_uid_lut(ds, tracked)

        assert str(out["cell_uid"].values[0]) == "NONE"

    def test_no_variable_added_when_tracked_cells_empty(self):
        ds = _ds_with_labels(max_label=0)
        tracked = pd.DataFrame(columns=["cell_label", "cell_uid"])

        out = attach_cell_uid_lut(ds, tracked)

        assert "cell_uid" not in out.data_vars

    def test_raises_when_label_in_grid_has_no_uid(self):
        ds = _ds_with_labels(max_label=3)
        tracked = pd.DataFrame({"cell_label": [1, 2], "cell_uid": ["a1", "b2"]})

        with pytest.raises(ValueError, match="3"):
            attach_cell_uid_lut(ds, tracked)

    def test_input_dataset_is_not_mutated(self):
        ds = _ds_with_labels(max_label=1)
        tracked = pd.DataFrame({"cell_label": [1], "cell_uid": ["a1"]})

        out = attach_cell_uid_lut(ds, tracked)

        assert "cell_uid" not in ds.data_vars
        assert "cell_uid" in out.data_vars


def _projected_ds() -> xr.Dataset:
    labels = np.zeros((4, 4), dtype=np.int32)
    labels[0, 0] = 1
    reg_minutes = np.zeros((2, 4, 4), dtype=np.int32)
    reg_minutes[:, 1, 1] = 1
    return xr.Dataset(
        {
            "reflectivity": (("y", "x"), np.ones((4, 4), dtype=np.float32)),
            "cell_labels": (("y", "x"), labels),
            "registration_minutes": (("minute", "y", "x"), reg_minutes),
        },
        coords={
            "x": np.arange(4),
            "y": np.arange(4),
            "minute": np.array(["2024-05-18T19:01", "2024-05-18T19:02"], dtype="datetime64[ns]"),
        },
    )


class TestAttachRegistrationUidLut:
    def test_attaches_registration_uid_for_previous_labels(self):
        prev_tracked = pd.DataFrame({"cell_label": [1, 2], "cell_uid": ["prev1", "prev2"]})

        out = attach_registration_uid_lut(_projected_ds(), prev_tracked)

        assert out["registration_cell_uid"].dims == ("registration_cell_label",)
        assert list(out["registration_cell_uid"].values.astype(str)) == ["NONE", "prev1", "prev2"]

    def test_no_variable_when_no_previous_tracking(self):
        out = attach_registration_uid_lut(_projected_ds(), None)

        assert "registration_cell_uid" not in out.data_vars

    def test_input_dataset_is_not_mutated(self):
        ds = _projected_ds()
        prev_tracked = pd.DataFrame({"cell_label": [1], "cell_uid": ["prev1"]})

        out = attach_registration_uid_lut(ds, prev_tracked)

        assert "registration_cell_uid" not in ds.data_vars
        assert "registration_cell_uid" in out.data_vars
