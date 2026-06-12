# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""End-to-end: `adapt postprocess --module xlma_stat` over a small repository.

Exercises the whole chain — PostProcessor discovery/resolution, minute-mask
injection (read_minute_masks over the analysis NetCDFs), flash-sorted NetCDF
reading, exact minute-bin attribution, and multi-table persistence — fully
synthetic. The key fixture is a *moving* cell: the flash sits at the cell's
projected mid-gap position, where both real scan masks are out of attribution
range — only the minute-resolution geometry attributes it.
"""

import shutil
import sqlite3
import tempfile
from datetime import UTC, datetime
from pathlib import Path

import pytest

from adapt.persistence import DataRepository, ProductType
from adapt.persistence.tables import CORE_TABLES
from adapt.runtime.postprocessor import PostProcessor
from tests.helpers.analysis_nc import cell_block, make_analysis_ds
from tests.helpers.lma import write_flash_sorted_nc

pytestmark = [pytest.mark.unit, pytest.mark.pipeline]

pytest.importorskip("pyproj")

LAT0, LON0 = 40.0, -88.0


@pytest.fixture
def repo():
    d = Path(tempfile.mkdtemp())
    r = DataRepository(run_id="XLMAE2E1", base_dir=d, radar="TEST_RADAR")
    yield r
    r.close()
    r.registry.close()
    shutil.rmtree(d, ignore_errors=True)


def _write(repo, ds, scan_time: str):
    repo.write_netcdf(
        ds=ds,
        product_type=ProductType.ANALYSIS_NC,
        scan_time=datetime.fromisoformat(scan_time).replace(tzinfo=UTC),
        producer="test",
    )


def _build_moving_cell_repo(repo):
    """Scans 19:00/19:03/19:06; the cell crosses the grid centre at minute 19:05.

    At 19:03 the cell is far west (cols 5-7), at 19:06 far east (cols 11-13);
    only the advected 19:05 mask (cols 9-11) covers the radar origin.
    """
    _write(
        repo,
        make_analysis_ds(
            "2024-05-18T19:03:00",
            "2024-05-18T19:00:00",
            cell_labels=cell_block(col=5),
            cell_uids=["uid-A"],
            minute_labels={
                "2024-05-18T19:01:00": cell_block(col=3),
                "2024-05-18T19:02:00": cell_block(col=4),
                "2024-05-18T19:03:00": cell_block(col=5),
            },
            registration_uids=None,  # first pair: previous scan never tracked
        ),
        "2024-05-18T19:03:00",
    )
    _write(
        repo,
        make_analysis_ds(
            "2024-05-18T19:06:00",
            "2024-05-18T19:03:00",
            cell_labels=cell_block(col=11),
            cell_uids=["uid-A"],
            minute_labels={
                "2024-05-18T19:04:00": cell_block(col=7),
                "2024-05-18T19:05:00": cell_block(col=9),
                "2024-05-18T19:06:00": cell_block(col=11),
            },
            registration_uids=["uid-A"],
        ),
        "2024-05-18T19:06:00",
    )


def _run_postprocess(repo, make_config, tmp_path) -> None:
    repo.registry.ensure_radar_location("TEST_RADAR", LAT0, LON0)
    lma_dir = tmp_path / "lma"
    lma_dir.mkdir(exist_ok=True)
    # two flashes in minute 19:05 at the radar origin — the cell's mid-gap position
    write_flash_sorted_nc(lma_dir / "LYLOUT_240518_190000_3600_map.nc", "2024-05-18T19:05:10", 2)
    (lma_dir / "LYLOUT_240518_190000.dat").write_text("raw ascii — ignored")
    config = make_config(module_params={"xlma_stat": {"input_dir": str(lma_dir)}})
    PostProcessor(repo, config).run(modules=["xlma_stat"])


def test_postprocess_xlma_writes_both_extension_tables(repo, make_config, tmp_path):
    _build_moving_cell_repo(repo)
    _run_postprocess(repo, make_config, tmp_path)

    conn = sqlite3.connect(repo.catalog.db_path)
    conn.row_factory = sqlite3.Row
    try:
        minutes = conn.execute(
            "SELECT run_id, cell_uid, time, source_scan_time, target_scan_time, "
            "interpolation_fraction, flash_count, lightning_source_count "
            "FROM xlma_stat_minutes"
        ).fetchall()
        scans = conn.execute(
            "SELECT run_id, cell_uid, scan_time, scan_time_unix, n_minutes, "
            "n_lightning_minutes, flash_count FROM xlma_stat_scan"
        ).fetchall()
        minutes_pk = [
            r["name"] for r in conn.execute("PRAGMA table_info('xlma_stat_minutes')") if r["pk"] > 0
        ]
        scan_pk = [
            r["name"] for r in conn.execute("PRAGMA table_info('xlma_stat_scan')") if r["pk"] > 0
        ]
        science_tables = {"cells_by_scan", "cell_events", "cell_tracks"} & CORE_TABLES
        core_counts = {
            t: conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0] for t in science_tables
        }
    finally:
        conn.close()

    # The flash at 19:05 attributes to the moving cell via its minute mask —
    # both real scan masks are out of range at that moment.
    assert len(minutes) == 1
    m = minutes[0]
    assert m["cell_uid"] == "uid-A"
    assert m["time"] == "2024-05-18T19:05:00Z"
    assert m["source_scan_time"] == "2024-05-18T19:03:00Z"
    assert m["target_scan_time"] == "2024-05-18T19:06:00Z"
    assert m["interpolation_fraction"] == pytest.approx(2 / 3)
    assert (m["flash_count"], m["lightning_source_count"]) == (2, 4)

    assert len(scans) == 1
    s = scans[0]
    assert s["cell_uid"] == "uid-A"
    assert s["scan_time"] == "2024-05-18T19:06:00Z"
    assert s["scan_time_unix"] is not None
    assert s["flash_count"] == 2  # equals the sum of its member minute rows
    assert s["n_minutes"] == 3  # 19:04, 19:05 advected + 19:06 real mask
    assert s["n_lightning_minutes"] == 1

    assert set(minutes_pk) == {"run_id", "time", "cell_uid"}
    assert set(scan_pk) == {"run_id", "scan_time", "cell_uid"}
    assert all(r["run_id"] == "XLMAE2E1" for r in [*minutes, *scans])
    # post-processing only adds extension tables; science core tables untouched
    assert all(count == 0 for count in core_counts.values())


def test_postprocess_xlma_rerun_is_idempotent(repo, make_config, tmp_path):
    _build_moving_cell_repo(repo)
    _run_postprocess(repo, make_config, tmp_path)
    _run_postprocess(repo, make_config, tmp_path)

    conn = sqlite3.connect(repo.catalog.db_path)
    try:
        n_minutes = conn.execute("SELECT COUNT(*) FROM xlma_stat_minutes").fetchone()[0]
        n_scans = conn.execute("SELECT COUNT(*) FROM xlma_stat_scan").fetchone()[0]
    finally:
        conn.close()

    assert n_minutes == 1
    assert n_scans == 1
