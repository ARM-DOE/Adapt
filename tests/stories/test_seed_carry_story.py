# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Story: a core that loses prominence for one frame survives with seed_carry.

Given two convective cores, a strong one (A) fixed in place and a weaker one
(B) approaching it over three scans, until at scan 3 the field between them
rises so far that B is no longer an h-maximum of its own.

When the pipeline segments scan 3 with the flag off, B is absorbed into A.
When the flag is on, B's labels from scan 2 — advected by the projection
module — seed the watershed at scan 3 and B is kept as its own cell.
"""

from datetime import UTC, datetime, timedelta

import numpy as np
import pytest
import xarray as xr

from adapt.configuration.schemas.user import UserSegmenterConfig
from adapt.execution.nodes.detection import DetectModule
from adapt.execution.nodes.projection import ProjectionModule

pytestmark = pytest.mark.unit

T0 = datetime(2024, 6, 17, 0, 0, tzinfo=UTC)
A_COL = 6
B_COLS = (22, 16, 12)  # B walks toward A; at 12 it is no longer a separate h-maximum


def _frame(b_col: int, t: datetime) -> xr.Dataset:
    yy, xx = np.mgrid[0:21, 0:30]
    field = (
        30.0
        + 25.0 * np.exp(-((yy - 10) ** 2 + (xx - A_COL) ** 2) / 8.0)
        + 15.0 * np.exp(-((yy - 10) ** 2 + (xx - b_col) ** 2) / 18.0)
    ).astype(np.float32)
    time = np.datetime64(t.replace(tzinfo=None))
    return xr.Dataset(
        {"reflectivity": (("y", "x"), field)},
        coords={"y": np.arange(21), "x": np.arange(30), "time": time},
    )


def _cells_per_scan(make_detection_config, make_projection_config, seed_carry: bool) -> list[int]:
    detect, project = DetectModule(), ProjectionModule()
    detection_config = make_detection_config(
        threshold=35.0,
        segmenter=UserSegmenterConfig(seed_carry=seed_carry, filter_by_size=False, h_maxima=5.0),
    )
    projection_config = make_projection_config()

    completed: list[dict] = []
    counts: list[int] = []
    for i, b_col in enumerate(B_COLS):
        t = T0 + timedelta(minutes=5 * i)
        ctx = {
            "grid_ds_2d": _frame(b_col, t),
            "grid_ds": None,
            "scan_time": t,
            "prior_scan": completed[-1] if completed else None,
            "detection_config": detection_config,
            "projection_config": projection_config,
        }
        result = detect.run(ctx)
        entry = {**result, "scan_time": t}
        if completed:
            history = [completed[-1], entry]
            entry.update(project.run({**ctx, **result, "scan_history": history}))
        completed.append(entry)
        counts.append(result["num_cells"])
    return counts


def test_weak_core_is_absorbed_with_the_flag_off(make_detection_config, make_projection_config):
    assert _cells_per_scan(make_detection_config, make_projection_config, False) == [2, 2, 1]


def test_weak_core_survives_with_the_flag_on(make_detection_config, make_projection_config):
    assert _cells_per_scan(make_detection_config, make_projection_config, True) == [2, 2, 2]
