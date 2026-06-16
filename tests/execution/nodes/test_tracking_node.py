# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""TrackingModule emits analysis_ds with the uid LUTs and remembers state.

``registration_minutes`` carries the *previous* scan's labels advected forward,
so ``analysis_ds`` needs a second LUT (``registration_cell_uid``) indexed by
previous-scan label. The tracking node builds it from the previous scan's
tracked_cells, which it remembers across run() calls (module instances persist
across files).
"""

import numpy as np
import pandas as pd
import pytest
import xarray as xr

from adapt.execution.nodes.tracking import TrackingModule

pytestmark = [pytest.mark.unit, pytest.mark.pipeline]


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


class _FakeTracker:
    """Returns a preset (tracked_cells, cell_events) pair per call."""

    def __init__(self, results):
        self._results = list(results)

    def track(self, ds_projected, cell_stats_df):
        return self._results.pop(0)


def _run_module(module, tracked_per_scan):
    module._tracker = _FakeTracker([(t, pd.DataFrame()) for t in tracked_per_scan])
    outputs = []
    for _ in tracked_per_scan:
        outputs.append(
            module.run(
                {
                    "tracking_config": None,
                    "projected_ds": _projected_ds(),
                    "cell_stats": pd.DataFrame({"cell_label": [1]}),
                }
            )
        )
    return outputs


def test_analysis_ds_uses_previous_scan_tracking_for_registration_lut():
    """The LUT on scan t's analysis_ds maps scan t-1's labels (remembered state)."""
    t1 = pd.DataFrame({"cell_label": [1], "cell_uid": ["uid-t1"]})
    t2 = pd.DataFrame({"cell_label": [1], "cell_uid": ["uid-t2"]})

    out1, out2 = _run_module(TrackingModule(), [t1, t2])

    # Scan t1: no previous tracking yet -> no registration LUT.
    assert "registration_cell_uid" not in out1["analysis_ds"].data_vars
    assert list(out1["analysis_ds"]["cell_uid"].values.astype(str)) == ["NONE", "uid-t1"]

    # Scan t2: registration LUT must map t1's labels; cell_uid maps t2's.
    assert list(out2["analysis_ds"]["registration_cell_uid"].values.astype(str)) == [
        "NONE",
        "uid-t1",
    ]
    assert list(out2["analysis_ds"]["cell_uid"].values.astype(str)) == ["NONE", "uid-t2"]


def test_empty_tracking_does_not_advance_registration_state():
    """An empty scan neither adds LUTs nor overwrites the remembered mapping."""
    t1 = pd.DataFrame({"cell_label": [1], "cell_uid": ["uid-t1"]})
    empty = pd.DataFrame(columns=["cell_label", "cell_uid"])
    t3 = pd.DataFrame({"cell_label": [1], "cell_uid": ["uid-t3"]})

    _, out2, out3 = _run_module(TrackingModule(), [t1, empty, t3])

    assert "cell_uid" not in out2["analysis_ds"].data_vars
    # State still maps t1 (the empty scan did not overwrite it).
    assert list(out3["analysis_ds"]["registration_cell_uid"].values.astype(str)) == [
        "NONE",
        "uid-t1",
    ]


def test_input_projected_ds_not_mutated():
    """analysis_ds is a copy; the dataset in scan history keeps no LUT vars."""
    module = TrackingModule()
    module._tracker = _FakeTracker(
        [(pd.DataFrame({"cell_label": [1], "cell_uid": ["u1"]}), pd.DataFrame())]
    )
    ds = _projected_ds()
    out = module.run(
        {
            "tracking_config": None,
            "projected_ds": ds,
            "cell_stats": pd.DataFrame({"cell_label": [1]}),
        }
    )
    assert "cell_uid" not in ds.data_vars
    assert "cell_uid" in out["analysis_ds"].data_vars
