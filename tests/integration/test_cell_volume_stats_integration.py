# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Integration: full enrich context -> run -> write -> read back, with join key check."""

import sqlite3
from datetime import datetime

import numpy as np
import pandas as pd
import pytest
import xarray as xr

pytestmark = pytest.mark.integration


def _build_context(internal_config):
    nz, ny, nx = 8, 6, 6
    dbz = np.full((nz, ny, nx), np.nan)
    # two cells with different vertical extents
    dbz[:5, 1:3, 1:3] = 50.0
    dbz[:2, 3:5, 3:5] = 35.0
    grid_3d = xr.Dataset(
        {"reflectivity": (["z", "y", "x"], dbz)},
        coords={
            "z": np.arange(nz) * 500.0,
            "y": np.arange(ny) * 1000.0,
            "x": np.arange(nx) * 1000.0,
        },
    )
    labels = np.zeros((ny, nx), dtype=np.int32)
    labels[1:3, 1:3] = 1
    labels[3:5, 3:5] = 2
    segmented = xr.Dataset(
        {"cell_labels": (["y", "x"], labels)},
        coords={"y": np.arange(ny) * 1000.0, "x": np.arange(nx) * 1000.0},
    )
    tracked = pd.DataFrame(
        [
            {"cell_label": 1, "cell_uid": "uid-1"},
            {"cell_label": 2, "cell_uid": "uid-2"},
        ]
    )
    from adapt.execution.nodes.cell_volume_stats import CellVolumeStatsModule

    return {
        "cell_volume_stats_config": CellVolumeStatsModule.build_config(internal_config),
        "grid_ds_3d": grid_3d,
        "segmented_ds": segmented,
        "tracked_cells": tracked,
        "run_id": "RUN1",
        "scan_time": datetime(2024, 5, 15, 18, 30, 0),  # naive, NEXRAD-style
    }


def test_end_to_end_run_write_read(internal_config, tmp_path):
    from adapt.execution.nodes.cell_volume_stats import CellVolumeStatsModule
    from adapt.persistence.module_output import ModuleOutputWriter
    from adapt.utils.time import to_scan_iso

    ctx = _build_context(internal_config)
    out = CellVolumeStatsModule().run(ctx)
    df = out["cell_volume_stats_rows"]

    assert len(df) == 2
    for col in (
        "run_id",
        "scan_time",
        "cell_uid",
        "cell_label",
        "cell_area_km2",
        "cell_volume_km3",
        "dbz_max",
        "dbz_mean",
    ):
        assert col in df.columns
    assert set(df["cell_uid"]) == {"uid-1", "uid-2"}

    db = tmp_path / "catalog.db"
    ModuleOutputWriter(db, CellVolumeStatsModule.persistence[0]).write(df)

    conn = sqlite3.connect(str(db))
    try:
        rows = conn.execute(
            "SELECT cell_uid, scan_time, scan_time_unix, cell_volume_km3 "
            "FROM cell_volume_stats ORDER BY cell_uid"
        ).fetchall()
    finally:
        conn.close()

    assert len(rows) == 2
    expected_iso = to_scan_iso(ctx["scan_time"])
    for _cell_uid, scan_iso, scan_unix, vol in rows:
        assert scan_iso == expected_iso  # joins to cells_by_scan
        assert scan_unix == int(scan_unix)  # machine-readable present
        assert vol > 0
    # cell 1 (taller, stronger) has more volume than cell 2
    vols = {r[0]: r[3] for r in rows}
    assert vols["uid-1"] > vols["uid-2"]
