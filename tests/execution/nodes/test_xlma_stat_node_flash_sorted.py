# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""XlmaStatModule.run() on flash-sorted NetCDF input + injected minute masks.

End-to-end through the node: synthetic flash-sorted file + minute masks ->
both extension-table frames, attributed to the cell at the radar origin.
"""

import numpy as np
import pandas as pd
import pytest

from adapt.execution.nodes.xlma_stat import XlmaStatModule
from adapt.modules.xlma_stat.config import XlmaStatConfig
from tests.helpers.lma import write_flash_sorted_nc

pytestmark = pytest.mark.unit

pytest.importorskip("pyproj")  # geo projection of flash lon/lat onto the grid


def _minute_mask(minute: str) -> dict:
    """21x21 km grid centred on the radar; one cell covering the centre block."""
    coords = np.linspace(-10_000.0, 10_000.0, 21)
    labels = np.zeros((21, 21), dtype=int)
    labels[8:13, 8:13] = 1
    return {
        "minute_time": pd.Timestamp(minute),
        "cell_labels": labels,
        "x": coords,
        "y": coords,
        "cell_uid_lut": np.array(["NONE", "CELL_A"]),
        "source_scan_time": pd.Timestamp("2024-05-18T11:58:00"),
        "target_scan_time": pd.Timestamp("2024-05-18T12:01:00"),
        "interpolation_fraction": 0.5,
    }


def _run_context(input_dir) -> dict:
    return {
        "xlma_stat_config": XlmaStatConfig(input_dir=str(input_dir)),
        "minute_masks": [_minute_mask("2024-05-18T12:00:00")],
        "radar_origin": (40.0, -88.0),
        "run_id": "testrun1",
    }


def test_run_on_flash_sorted_netcdf_produces_both_tables(tmp_path):
    # 3 flashes 10 s apart from 12:00:00 — all inside the 12:00 minute mask
    write_flash_sorted_nc(tmp_path / "LYLOUT_240518_120000_3600_map.nc", "2024-05-18T12:00:00", 3)

    result = XlmaStatModule().run(_run_context(tmp_path))

    minutes = result["xlma_stat_minutes_rows"]
    scan = result["xlma_stat_scan_rows"]
    assert (minutes["cell_uid"] == "CELL_A").all()
    assert int(minutes["flash_count"].sum()) == 3
    assert (scan["cell_uid"] == "CELL_A").all()
    assert pd.Timestamp(scan.iloc[0]["scan_time"]) == pd.Timestamp("2024-05-18T12:01:00")
    # run_id is stamped on every row of both extension tables
    assert (minutes["run_id"] == "testrun1").all()
    assert (scan["run_id"] == "testrun1").all()


def test_non_netcdf_files_are_ignored(tmp_path):
    write_flash_sorted_nc(tmp_path / "a.nc", "2024-05-18T12:00:00", 2)
    (tmp_path / "LYLOUT_240518_120000.dat").write_text("ascii sources", encoding="utf-8")

    result = XlmaStatModule().run(_run_context(tmp_path))

    assert int(result["xlma_stat_minutes_rows"]["flash_count"].sum()) == 2


def test_directory_without_netcdf_raises(tmp_path):
    (tmp_path / "LYLOUT_240518_120000.dat").write_text("ascii sources", encoding="utf-8")

    with pytest.raises(ValueError, match="No flash-sorted NetCDF"):
        XlmaStatModule().run(_run_context(tmp_path))


def test_missing_minute_masks_fails_loudly(tmp_path):
    write_flash_sorted_nc(tmp_path / "a.nc", "2024-05-18T12:00:00", 1)
    context = _run_context(tmp_path)
    context["minute_masks"] = []

    with pytest.raises(ValueError, match="reprocess"):
        XlmaStatModule().run(context)
