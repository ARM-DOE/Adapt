# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Processor attaches a previous-scan label -> cell_uid lookup.

``registration_minutes`` carries the *previous* scan's labels advected forward,
so the analysis NetCDF needs a second LUT (``registration_cell_uid``) indexed
by previous-scan label. The processor builds it from the previous scan's
tracked_cells, which it remembers across _save_results calls.
"""

import numpy as np
import pandas as pd
import pytest
import xarray as xr

from adapt.runtime import processor as processor_module
from adapt.runtime.processor import RadarProcessor

pytestmark = [pytest.mark.unit, pytest.mark.pipeline]


def _projected_ds() -> xr.Dataset:
    labels = np.zeros((4, 4), dtype=np.int32)
    labels[0, 0] = 1
    reg_minutes = np.zeros((2, 4, 4), dtype=np.int32)
    reg_minutes[:, 1, 1] = 1
    ds = xr.Dataset(
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
    return ds


def test_attaches_registration_uid_for_previous_labels():
    prev_tracked = pd.DataFrame({"cell_label": [1, 2], "cell_uid": ["prev1", "prev2"]})

    out = RadarProcessor._attach_registration_uid_lut(_projected_ds(), prev_tracked)

    assert out["registration_cell_uid"].dims == ("registration_cell_label",)
    assert list(out["registration_cell_uid"].values.astype(str)) == ["NONE", "prev1", "prev2"]


def test_no_variable_when_no_previous_tracking():
    out = RadarProcessor._attach_registration_uid_lut(_projected_ds(), None)

    assert "registration_cell_uid" not in out.data_vars


def test_save_results_uses_previous_scan_tracking(
    monkeypatch, pipeline_config, pipeline_output_dirs, test_repository
):
    """The LUT written for scan t maps scan t-1's labels (remembered state)."""
    import queue

    proc = RadarProcessor(
        queue.Queue(), pipeline_config, pipeline_output_dirs, repository=test_repository
    )
    captured: list[xr.Dataset] = []
    monkeypatch.setattr(
        proc, "_save_analysis_netcdf", lambda ds, filepath, scan_time: captured.append(ds)
    )

    class FakeTrackStore:
        def __init__(self, _db_path):
            pass

        def write_scan(self, **kwargs):
            pass

    monkeypatch.setattr(processor_module, "TrackStore", FakeTrackStore)

    t1_tracked = pd.DataFrame({"cell_label": [1], "cell_uid": ["uid-t1"]})
    stats = pd.DataFrame({"cell_label": [1]})

    # Scan t1: no previous tracking yet -> no registration LUT.
    proc._save_results(
        {
            "projected_ds": _projected_ds(),
            "tracked_cells": t1_tracked,
            "cell_stats": stats,
            "cell_adjacency": pd.DataFrame(),
        },
        scan_time=pd.Timestamp("2024-05-18T19:07:00", tz="UTC"),
    )
    assert "registration_cell_uid" not in captured[0].data_vars

    # Scan t2: registration LUT must map t1's labels.
    proc._save_results(
        {
            "projected_ds": _projected_ds(),
            "tracked_cells": pd.DataFrame({"cell_label": [1], "cell_uid": ["uid-t2"]}),
            "cell_stats": stats,
            "cell_adjacency": pd.DataFrame(),
        },
        scan_time=pd.Timestamp("2024-05-18T19:14:00", tz="UTC"),
    )
    assert list(captured[1]["registration_cell_uid"].values.astype(str)) == ["NONE", "uid-t1"]
