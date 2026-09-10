# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""End-to-end: `adapt postprocess --module xlma_stat` over a small stored run.

Exercises the whole chain — PostProcessor discovery/resolution, minute-mask
injection (read_minute_masks over the analysis NetCDFs), flash-sorted NetCDF
reading, exact minute-bin attribution, and multi-table persistence — fully
synthetic. The key fixture is a *moving* cell: the flash sits at the cell's
projected mid-gap position, where both real scan masks are out of attribution
range — only the minute-resolution geometry attributes it.
"""

import sqlite3
from datetime import UTC, datetime

import pytest

from adapt.persistence.objects import ArtifactMeta, ObjectStore
from adapt.runtime.postprocessor import PostProcessor
from tests.helpers.analysis_nc import cell_block, make_analysis_ds
from tests.helpers.lma import write_flash_sorted_nc

pytestmark = [pytest.mark.unit, pytest.mark.pipeline]

pytest.importorskip("pyproj")

LAT0, LON0 = 40.0, -88.0


def _write(store_env, ds, scan_time: str):
    objects = ObjectStore(store_env.collection.objects_dir, store_env.collection.catalog)
    handle = objects.begin(suffix=".nc")
    ds.to_netcdf(handle.staging_path)
    objects.commit(
        handle,
        ArtifactMeta(
            artifact_type="segmentation2d",
            producer="test",
            run_id=store_env.run_id,
            scan_id=f"sid-{scan_time[-8:]}",
            observation_time=datetime.fromisoformat(scan_time).replace(tzinfo=UTC),
        ),
    )


def _build_moving_cell_repo(store_env):
    """Scans 19:00/19:03/19:06; the cell crosses the grid centre at minute 19:05.

    At 19:03 the cell is far west (cols 5-7), at 19:06 far east (cols 11-13);
    only the advected 19:05 mask (cols 9-11) covers the radar origin.
    """
    _write(
        store_env,
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
        store_env,
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


def _run_postprocess(store_env, make_config, tmp_path) -> None:
    store_env.registry.ensure_collection_location("TEST_RADAR", lat=LAT0, lon=LON0)
    lma_dir = tmp_path / "lma"
    lma_dir.mkdir(exist_ok=True)
    # two flashes in minute 19:05 at the radar origin — the cell's mid-gap position
    write_flash_sorted_nc(lma_dir / "LYLOUT_240518_190000_3600_map.nc", "2024-05-18T19:05:10", 2)
    (lma_dir / "LYLOUT_240518_190000.dat").write_text("raw ascii — ignored", encoding="utf-8")
    config = make_config(module_params={"xlma_stat": {"input_dir": str(lma_dir)}})
    PostProcessor(store_env.collection, store_env.registry, store_env.run_id, config).run(
        modules=["xlma_stat"]
    )


def test_postprocess_xlma_writes_both_extension_tables(store_env, make_config, tmp_path):
    _build_moving_cell_repo(store_env)
    _run_postprocess(store_env, make_config, tmp_path)

    conn = sqlite3.connect(store_env.collection.products_path)
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
        existing_tables = {
            r["name"] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
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
    assert all(r["run_id"] == store_env.run_id for r in [*minutes, *scans])
    # post-processing only adds extension tables; tracking core tables untouched
    assert not ({"cells_by_scan", "cell_events", "cell_tracks"} & existing_tables)


def test_postprocess_xlma_rerun_is_idempotent(store_env, make_config, tmp_path):
    _build_moving_cell_repo(store_env)
    _run_postprocess(store_env, make_config, tmp_path)
    _run_postprocess(store_env, make_config, tmp_path)

    conn = sqlite3.connect(store_env.collection.products_path)
    try:
        n_minutes = conn.execute("SELECT COUNT(*) FROM xlma_stat_minutes").fetchone()[0]
        n_scans = conn.execute("SELECT COUNT(*) FROM xlma_stat_scan").fetchone()[0]
    finally:
        conn.close()

    assert n_minutes == 1
    assert n_scans == 1
