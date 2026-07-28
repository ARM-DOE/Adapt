# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""OutputRouter persists module-declared outputs mechanically.

Synthetic modules and data only — the router must work without any knowledge
of real pipeline modules or their key names.
"""

import sqlite3
from dataclasses import dataclass, field
from datetime import UTC, datetime

import pandas as pd
import pytest
import xarray as xr

from adapt.contracts import (
    NetcdfArtifact,
    ParquetArtifact,
    PersistenceMeta,
    RegisterFileArtifact,
    SqliteTable,
    TrackTablesWrite,
)
from adapt.persistence.output_router import OutputRouter
from adapt.persistence.repository import DataRepository
from adapt.persistence.track_store import TrackStore

pytestmark = pytest.mark.unit

SCAN_TIME = datetime(2026, 6, 1, 12, 0, tzinfo=UTC)


@dataclass
class FakeModule:
    name: str = "fake"
    persistence: tuple = field(default_factory=tuple)


@pytest.fixture
def repo(tmp_path):
    return DataRepository(run_id="testrun1", base_dir=tmp_path, radar="KTST")


def _meta(repo):
    return PersistenceMeta(
        scan_time=SCAN_TIME,
        run_id=repo.run_id,
        source_file="KTST_20260601_120000_V06",
        dataset_id="KTST",
    )


def _tracking_frames():
    """Minimal consistent frames for a TrackStore.write_scan call."""
    cell_stats = pd.DataFrame({"cell_label": [1], "area_sqkm": [25.0]})
    tracked = pd.DataFrame({"cell_label": [1], "cell_uid": ["U1"]})
    events = pd.DataFrame(
        {
            "event_type": ["INITIATION"],
            "source_cell_uid": [None],
            "target_cell_uid": ["U1"],
            "cost": [0.0],
            "event_group_id": [None],
        }
    )
    adjacency = pd.DataFrame(
        {"cell_label_a": [], "cell_label_b": [], "touching_boundary_pixels": []}
    )
    return cell_stats, tracked, events, adjacency


def test_none_scan_time_raises_for_timed_artifact(repo):
    bad = PersistenceMeta(scan_time=None, run_id="r", source_file="s", dataset_id="KTST")
    mod = FakeModule(
        persistence=(ParquetArtifact(key="cells", product_type="analysis2d", producer="a"),)
    )
    with pytest.raises(ValueError, match="scan_time"):
        OutputRouter(repo).persist([mod], {"cells": pd.DataFrame({"x": [1]})}, bad)


def test_none_scan_time_allowed_for_sqlite_table(repo):
    """Run-level persists (postprocessor) carry no scan_time; rows bring their own."""
    bad = PersistenceMeta(scan_time=None, run_id="r", source_file="s", dataset_id="KTST")
    mod = FakeModule(
        persistence=(SqliteTable(key="rows", table="runlevel_ext", primary_key=("run_id",)),)
    )
    OutputRouter(repo).persist([mod], {"rows": pd.DataFrame({"run_id": ["r"], "v": [1.0]})}, bad)
    conn = sqlite3.connect(repo.catalog.db_path)
    try:
        assert conn.execute("SELECT COUNT(*) FROM runlevel_ext").fetchone()[0] == 1
    finally:
        conn.close()


def test_absent_key_skips(repo):
    mod = FakeModule(
        persistence=(ParquetArtifact(key="cells", product_type="analysis2d", producer="a"),)
    )
    OutputRouter(repo).persist([mod], {}, _meta(repo))
    assert repo.query(product_type="analysis2d") == []


def test_empty_dataframe_skips(repo):
    mod = FakeModule(
        persistence=(ParquetArtifact(key="cells", product_type="analysis2d", producer="a"),)
    )
    OutputRouter(repo).persist([mod], {"cells": pd.DataFrame()}, _meta(repo))
    assert repo.query(product_type="analysis2d") == []


def test_register_file_artifact_registers_existing_file(repo, tmp_path):
    f = tmp_path / "grid.nc"
    xr.Dataset({"reflectivity": ("x", [1.0])}).to_netcdf(f)
    mod = FakeModule(
        persistence=(
            RegisterFileArtifact(key="out_path", product_type="gridded3d", producer="ingest"),
        )
    )
    OutputRouter(repo).persist([mod], {"out_path": str(f)}, _meta(repo))
    assert len(repo.query(product_type="gridded3d")) == 1


def test_register_file_artifact_missing_file_raises(repo, tmp_path):
    mod = FakeModule(
        persistence=(
            RegisterFileArtifact(key="out_path", product_type="gridded3d", producer="ingest"),
        )
    )
    with pytest.raises(FileNotFoundError, match="out_path"):
        OutputRouter(repo).persist([mod], {"out_path": str(tmp_path / "gone.nc")}, _meta(repo))


def test_netcdf_artifact_written_with_metadata(repo):
    ds = xr.Dataset({"cell_labels": ("x", [0, 1])})
    mod = FakeModule(
        persistence=(
            NetcdfArtifact(
                key="ds_out",
                product_type="segmentation2d",
                producer="processor",
                description="synthetic analysis",
            ),
        )
    )
    OutputRouter(repo).persist([mod], {"ds_out": ds}, _meta(repo))
    artifacts = repo.query(product_type="segmentation2d")
    assert len(artifacts) == 1
    written = repo.open_dataset(artifacts[0]["artifact_id"])
    assert written.attrs["radar"] == "KTST"
    assert written.attrs["source"] == "KTST_20260601_120000_V06"
    assert written.attrs["description"] == "synthetic analysis"
    written.close()


def test_netcdf_write_failure_propagates(repo, monkeypatch):
    def boom(**kwargs):
        raise OSError("disk full")

    monkeypatch.setattr(repo, "write_netcdf", boom)
    mod = FakeModule(
        persistence=(
            NetcdfArtifact(key="d", product_type="segmentation2d", producer="p", description="x"),
        )
    )
    with pytest.raises(OSError, match="disk full"):
        OutputRouter(repo).persist([mod], {"d": xr.Dataset()}, _meta(repo))


def test_parquet_artifact_written(repo):
    df = pd.DataFrame({"cell_label": [1, 2], "area_sqkm": [10.0, 20.0]})
    mod = FakeModule(
        persistence=(ParquetArtifact(key="cells", product_type="analysis2d", producer="a"),)
    )
    OutputRouter(repo).persist([mod], {"cells": df}, _meta(repo))
    items = repo.query(product_type="analysis2d")
    assert len(items) == 1
    parquet_path = repo.catalog.radar_dir / items[0]["file_path"]
    assert pd.read_parquet(parquet_path)["cell_label"].tolist() == [1, 2]


def test_track_tables_write_missing_cokey_raises(repo):
    mod = FakeModule(
        persistence=(
            TrackTablesWrite(tracked_key="t", events_key="e", stats_key="s", adjacency_key="a"),
        )
    )
    result = {"t": pd.DataFrame({"cell_uid": ["U1"], "cell_label": [1]})}
    with pytest.raises(KeyError, match="alongside"):
        OutputRouter(repo).persist([mod], result, _meta(repo))


def test_track_tables_write_absent_trigger_skips(repo):
    mod = FakeModule(
        persistence=(
            TrackTablesWrite(tracked_key="t", events_key="e", stats_key="s", adjacency_key="a"),
        )
    )
    OutputRouter(repo).persist([mod], {}, _meta(repo))  # no raise


def test_track_tables_write_happy_path(repo):
    cell_stats, tracked, events, adjacency = _tracking_frames()
    mod = FakeModule(
        persistence=(
            TrackTablesWrite(tracked_key="t", events_key="e", stats_key="s", adjacency_key="a"),
        )
    )
    OutputRouter(repo).persist(
        [mod], {"t": tracked, "e": events, "s": cell_stats, "a": adjacency}, _meta(repo)
    )
    rows = TrackStore(repo.catalog.db_path).get_cells_by_scan(repo.run_id, SCAN_TIME)
    assert rows["cell_uid"].tolist() == ["U1"]


def test_sqlite_table_written_and_schema_registered(repo):
    df = pd.DataFrame({"run_id": ["r1"], "scan_time": [SCAN_TIME], "cell_uid": ["U1"]})
    mod = FakeModule(
        persistence=(
            SqliteTable(
                key="rows",
                table="synthetic_stats",
                primary_key=("run_id", "scan_time", "cell_uid"),
            ),
        )
    )
    OutputRouter(repo).persist([mod], {"rows": df}, _meta(repo))
    conn = sqlite3.connect(repo.catalog.db_path)
    try:
        assert conn.execute("SELECT COUNT(*) FROM synthetic_stats").fetchone()[0] == 1
        registered = conn.execute(
            "SELECT table_name FROM module_schemas WHERE table_name='synthetic_stats'"
        ).fetchall()
        assert registered == [("synthetic_stats",)]
    finally:
        conn.close()


def test_router_source_names_no_module_keys():
    import adapt.persistence.output_router as mod

    with open(mod.__file__, encoding="utf-8") as f:
        source = f.read()
    # Hardcoding means a quoted context-key literal (kwarg names of store APIs are fine).
    for key in ("grid_nc_path", "projected_ds", "cell_stats", "tracked_cells", "cell_events"):
        assert f'"{key}"' not in source and f"'{key}'" not in source, (
            f"router hardcodes module key '{key}'"
        )
