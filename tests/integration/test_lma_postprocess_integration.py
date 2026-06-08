# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""End-to-end: `adapt postprocess --module lma` over a small repository.

Exercises the whole chain — PostProcessor discovery/resolution, scan-mask
injection, PyXLMA clustering + flash stats, initiation-point attribution,
1-minute binning, aggregation, and multi-table persistence — on a tiny repo.
ASCII parsing is bypassed by injecting a synthetic in-memory event dataset; the
rest is real. Requires scikit-learn (pyXLMA clustering).
"""

import shutil
import sqlite3
import tempfile
from datetime import UTC, datetime
from pathlib import Path

import numpy as np
import pytest
import xarray as xr

pytest.importorskip("sklearn")

from pyxlma.lmalib.io import cf_netcdf  # noqa: E402

from adapt.persistence import DataRepository, ProductType  # noqa: E402
from adapt.runtime.postprocessor import PostProcessor  # noqa: E402

pytestmark = [pytest.mark.integration]

LAT0, LON0 = 40.0, -88.0


@pytest.fixture
def repo():
    d = Path(tempfile.mkdtemp())
    r = DataRepository(run_id="LMAINT1", base_dir=d, radar="TEST_RADAR")
    yield r
    r.close()
    r.registry.close()
    shutil.rmtree(d, ignore_errors=True)


def _write_analysis(repo):
    coords = (np.arange(11) - 5) * 200.0  # metres, radar origin at index 5
    labels = np.zeros((11, 11), dtype=np.int32)
    labels[5, 5] = 1
    ds = xr.Dataset(
        {
            "cell_labels": (("y", "x"), labels),
            "cell_uid": ("cell_label", np.array(["NONE", "uid-A"], dtype=np.str_)),
        },
        coords={"x": coords, "y": coords, "cell_label": [0, 1]},
    )
    repo.write_netcdf(
        ds=ds,
        product_type=ProductType.ANALYSIS_NC,
        scan_time=datetime(2024, 5, 18, 12, 0, 0, tzinfo=UTC),
        producer="test",
    )


def _synthetic_events():
    ds = cf_netcdf.new_dataset(events=6)
    base = np.datetime64("2024-05-18T12:00:00")
    secs = np.array([0.0, 0.02, 0.04, 30.0, 30.02, 30.04])
    ds["event_time"][:] = base + (secs * 1e9).astype("timedelta64[ns]")
    # all sources essentially at the radar origin -> inside cell uid-A
    ds["event_latitude"][:] = LAT0
    ds["event_longitude"][:] = LON0
    ds["event_altitude"][:] = np.array([5000, 6000, 7000, 5000, 6000, 7000.0])
    ds["event_power"][:] = -20.0
    ds["event_chi2"][:] = 1.0
    ds["event_stations"][:] = 7
    ds["event_id"][:] = np.arange(6)
    ds["network_center_latitude"].data = np.float64(LAT0)
    ds["network_center_longitude"].data = np.float64(LON0)
    ds["network_center_altitude"].data = np.float64(0.0)
    return ds


def test_postprocess_lma_writes_both_extension_tables(repo, make_config, monkeypatch):
    _write_analysis(repo)
    repo.registry.ensure_radar_location("TEST_RADAR", LAT0, LON0)

    import adapt.execution.nodes.lma as lma_node

    monkeypatch.setattr(lma_node, "read_event_dataset", lambda files: _synthetic_events())

    config = make_config(module_params={"lma": {"input_dir": str(repo.base_dir)}})

    # lma auto-registers via postprocess_defaults.yaml (idempotent import).
    PostProcessor(repo, config).run(modules=["lma"])

    conn = sqlite3.connect(repo.catalog.db_path)
    try:
        stats = conn.execute(
            "SELECT cell_uid, flash_count, source_count FROM lma_cell_stats"
        ).fetchall()
        attr = conn.execute("SELECT cell_uid FROM lma_flash_attribution").fetchall()
    finally:
        conn.close()

    # two flashes, all sources at the cell -> attributed to uid-A
    assert ("uid-A", 2, 6) in stats
    assert {row[0] for row in attr} == {"uid-A"}
