# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""The tracker is field-generic: it reads stats via stat_column(field_var).

Matching already runs on ds[field_var]; these tests pin that the per-cell
stat READS follow the configured field too — tomorrow's pressure-tracked
run needs zero tracker edits — and that a missing stat column fails loudly
naming the analyzer whitelist.
"""

import numpy as np
import pandas as pd
import pytest
import xarray as xr

from adapt.modules.tracking.module import CellTracker

pytestmark = pytest.mark.unit

_T1 = np.datetime64("2024-01-01T12:00:00")
_SCAN1 = "abc123def4567890"


def _scan(time, labels, field_name):
    y = np.arange(labels.shape[0], dtype=float) * 1000.0
    x = np.arange(labels.shape[1], dtype=float) * 1000.0
    field = np.where(labels > 0, 900.0, 0.0)
    return xr.Dataset(
        {
            field_name: (("y", "x"), field),
            "cell_labels": (("y", "x"), labels),
        },
        coords={"y": y, "x": x, "time": time},
    )


def _stats(time, cell_label, field):
    return pd.DataFrame(
        [
            {
                "time": time,
                "time_volume_start": time,
                "cell_label": cell_label,
                "cell_area_sqkm": 4.0,
                "cell_centroid_geom_x": 2.5,
                "cell_centroid_geom_y": 2.5,
                "cell_centroid_mass_lat": 35.0,
                "cell_centroid_mass_lon": -97.0,
                f"radar_{field}_mean": 850.0,
                f"radar_{field}_max": 900.0,
            }
        ]
    )


def _labels():
    labels = np.zeros((6, 6), dtype=np.int32)
    labels[2:4, 2:4] = 1
    return labels


def test_tracker_runs_on_a_non_reflectivity_field(make_tracking_config):
    config = make_tracking_config(
        **{"global": {"tracking_field": "pressure"}},
        radar_variables=["pressure"],
    )
    tracker = CellTracker(config)

    tracked, _events = tracker.track(
        _scan(_T1, _labels(), "pressure"), _stats(_T1, 1, "pressure"), scan_id=_SCAN1
    )

    assert len(tracked) == 1
    # Role columns carry the tracked field's stats, whatever the field is.
    assert tracked.loc[0, "max_reflectivity"] == 900.0
    assert tracked.loc[0, "mean_reflectivity"] == 850.0


def test_tracker_raises_naming_whitelist_when_stat_columns_missing(make_tracking_config):
    config = make_tracking_config(
        **{"global": {"tracking_field": "pressure"}},
        radar_variables=["pressure"],
    )
    tracker = CellTracker(config)
    stats_without_pressure = _stats(_T1, 1, "reflectivity")

    with pytest.raises(ValueError, match="radar_variables"):
        tracker.track(_scan(_T1, _labels(), "pressure"), stats_without_pressure, scan_id=_SCAN1)


def test_empty_scan_needs_no_stat_columns(make_tracking_config):
    # Zero-cell scans hand the analyzer's empty structural stub to the
    # tracker; the stat-column check must not fire on an empty frame.
    config = make_tracking_config(
        **{"global": {"tracking_field": "pressure"}},
        radar_variables=["pressure"],
    )
    tracker = CellTracker(config)
    empty = pd.DataFrame(
        columns=[
            "cell_label",
            "cell_area_sqkm",
            "time",
            "time_volume_start",
            "cell_centroid_mass_lat",
            "cell_centroid_mass_lon",
        ]
    )

    tracked, events = tracker.track(
        _scan(_T1, np.zeros((6, 6), dtype=np.int32), "pressure"), empty, scan_id=_SCAN1
    )
    assert tracked.empty
