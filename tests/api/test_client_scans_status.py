# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Tests for RepositoryClient scan access, raw SQL, status, and streaming."""

import sqlite3
from datetime import UTC, datetime

import pandas as pd
import pytest
import xarray as xr

from adapt.api.client import RepositoryClient
from adapt.api.domain import Scan, ScanBundle
from adapt.contracts import ScanRecord
from adapt.persistence.catalog import RadarCatalog
from adapt.persistence.registry import RepositoryRegistry
from tests.api.synthetic_repo import _RADAR, _RUN_ID, _SID0, _UID_A

pytestmark = pytest.mark.unit

_SCAN_T = datetime(2024, 6, 1, 12, 0, 0, tzinfo=UTC)
_SID = _SID0  # matches the synthetic repo's cells_by_scan row


def _register_scan(repo_root, scan_id=_SID, scan_time=_SCAN_T):
    catalog = RadarCatalog(repo_root / _RADAR)
    catalog.register_scan(
        ScanRecord(
            run_id=_RUN_ID,
            scan_id=scan_id,
            scan_time=scan_time,
            source_file_name="KDIX_source_V06",
        )
    )
    catalog.close()


def _write_products(repo_root):
    """Write a tiny segmentation NetCDF + analysis Parquet and catalog them."""
    radar_dir = repo_root / _RADAR
    seg_path = radar_dir / "seg.nc"
    xr.Dataset({"cell_labels": (("y", "x"), [[0, 1], [1, 1]])}).to_netcdf(seg_path)
    cells_path = radar_dir / "cells.parquet"
    pd.DataFrame({"cell_label": [1], "area": [4.0]}).to_parquet(cells_path)

    conn = sqlite3.connect(str(radar_dir / "catalog.db"))
    for item_id, item_type, file_path in (
        ("seg-1", "segmentation2d", "seg.nc"),
        ("ana-1", "analysis2d", "cells.parquet"),
    ):
        conn.execute(
            "INSERT INTO items (item_id, run_id, item_type, scan_id, scan_time, file_path,"
            " status) VALUES (?, ?, ?, ?, ?, ?, 'complete')",
            (item_id, _RUN_ID, item_type, _SID, _SCAN_T.isoformat(), file_path),
        )
    conn.commit()
    conn.close()


class TestScans:
    def test_empty_repository_returns_no_scans(self, client):
        assert client.scans(_RADAR) == []

    def test_unknown_radar_raises(self, client):
        with pytest.raises(FileNotFoundError, match="KNOPE"):
            client.scans("KNOPE")

    def test_registered_scan_is_returned_with_metadata(self, repo_root):
        _register_scan(repo_root)

        client = RepositoryClient(repo_root)
        try:
            scans = client.scans(_RADAR, run_id=_RUN_ID)
        finally:
            client.close()

        assert len(scans) == 1
        assert isinstance(scans[0], Scan)
        assert scans[0].scan_id == _SID
        assert scans[0].run_id == _RUN_ID
        assert scans[0].scan_time == _SCAN_T
        assert scans[0].source_file_name == "KDIX_source_V06"
        assert scans[0].status == "complete"

    def test_time_window_excludes_scan(self, repo_root):
        _register_scan(repo_root)

        client = RepositoryClient(repo_root)
        try:
            scans = client.scans(_RADAR, start=datetime(2025, 1, 1, tzinfo=UTC))
        finally:
            client.close()

        assert scans == []


class TestScanBundle:
    def test_bundle_loads_products_by_identity(self, repo_root):
        _write_products(repo_root)
        _register_scan(repo_root)

        client = RepositoryClient(repo_root)
        try:
            bundle = client.scan_bundle(_RUN_ID, _SID, radar=_RADAR)
        finally:
            client.close()

        assert isinstance(bundle, ScanBundle)
        assert bundle.scan.scan_id == _SID
        assert bundle.scan.run_id == _RUN_ID
        assert isinstance(bundle.segmentation, xr.Dataset)
        assert bundle.cells["cell_label"].tolist() == [1]
        assert [t.cell_uid for t in bundle.tracks] == [_UID_A]

    def test_bundle_unregistered_scan_raises(self, client):
        with pytest.raises(KeyError, match="not registered"):
            client.scan_bundle(_RUN_ID, "sid-unknown", radar=_RADAR)

    def test_bundle_without_products_has_empty_fields(self, repo_root):
        # Registered scan, but no product items and no tracking rows at its time.
        other_t = datetime(2024, 6, 1, 18, 0, 0, tzinfo=UTC)
        _register_scan(repo_root, scan_id="sid-bare", scan_time=other_t)

        client = RepositoryClient(repo_root)
        try:
            bundle = client.scan_bundle(_RUN_ID, "sid-bare", radar=_RADAR)
        finally:
            client.close()

        assert bundle.segmentation is None
        assert bundle.cells is None
        assert bundle.tracks == []


class TestQuery:
    def test_select_query_runs_through_duckdb(self, client):
        df = client.query("SELECT 1 AS x", radar=_RADAR)

        assert df["x"].tolist() == [1]

    def test_non_select_query_is_rejected(self, client):
        with pytest.raises(ValueError, match="SELECT"):
            client.query("DROP TABLE cells_by_scan", radar=_RADAR)


class TestStatus:
    def test_running_run_reports_pipeline_running(self, repo_root, monkeypatch, tmp_path):
        monkeypatch.setattr("pathlib.Path.home", lambda: tmp_path / "home")

        client = RepositoryClient(repo_root)
        try:
            assert client.is_pipeline_running(radar=_RADAR) is True
        finally:
            client.close()

    def test_completed_run_reports_not_running(self, repo_root, monkeypatch, tmp_path):
        monkeypatch.setattr("pathlib.Path.home", lambda: tmp_path / "home")
        conn = sqlite3.connect(str(repo_root / "adapt_registry.db"))
        conn.execute("UPDATE runs SET status = 'complete'")
        conn.commit()
        conn.close()

        client = RepositoryClient(repo_root)
        try:
            assert client.is_pipeline_running(radar=_RADAR) is False
        finally:
            client.close()

    def test_pipeline_progress_reports_run(self, repo_root, monkeypatch, tmp_path):
        monkeypatch.setattr("pathlib.Path.home", lambda: tmp_path / "home")

        client = RepositoryClient(repo_root)
        try:
            progress = client.pipeline_progress(radar=_RADAR)
        finally:
            client.close()

        assert progress["run_id"] == _RUN_ID
        assert progress["radar"] == _RADAR
        assert "is_running" in progress

    def test_pipeline_progress_error_on_empty_repository(self, tmp_path):
        RepositoryRegistry._instance = None
        client = RepositoryClient(tmp_path)
        try:
            progress = client.pipeline_progress()
        finally:
            client.close()
            RepositoryRegistry._instance = None

        assert progress["is_running"] is False
        assert "error" in progress

    def test_repository_info_summarizes_repository(self, client, repo_root):
        info = client.repository_info()

        assert info["path"] == str(repo_root)
        assert info["is_initialized"] is True
        assert info["num_radars"] == 1
        assert info["radars"] == [_RADAR]
        assert info["num_runs"] == 1


class TestStream:
    def test_stream_yields_then_stops_on_interrupt(self, client, monkeypatch):
        def interrupt(_seconds):
            raise KeyboardInterrupt

        monkeypatch.setattr("adapt.api.client.time.sleep", interrupt)
        gen = client.stream("SELECT 1 AS scan_time", radar=_RADAR)

        first = next(gen)
        assert first["scan_time"].tolist() == [1]
        with pytest.raises(StopIteration):
            next(gen)

    def test_stream_swallows_query_errors_and_keeps_polling(self, client, monkeypatch):
        class _StopPolling(Exception):
            """Raised by the patched sleep to end the test's polling loop."""

        def interrupt(_seconds):
            raise _StopPolling

        monkeypatch.setattr("adapt.api.client.time.sleep", interrupt)
        gen = client.stream("SELECT * FROM table_that_does_not_exist", radar=_RADAR)

        # The bad query is logged and swallowed; the generator reaches the
        # error-branch sleep, proving it would keep polling.
        with pytest.raises(_StopPolling):
            next(gen)


class TestParseDt:
    def test_datetime_passes_through(self):
        assert RepositoryClient._parse_dt(_SCAN_T) is _SCAN_T

    def test_z_suffixed_string_is_parsed(self):
        parsed = RepositoryClient._parse_dt("2024-06-01T12:00:00Z")
        assert parsed == _SCAN_T

    def test_unparseable_value_returns_datetime_min(self):
        assert RepositoryClient._parse_dt(42) == datetime.min


def test_close_is_idempotent(repo_root):
    client = RepositoryClient(repo_root)
    client.query("SELECT 1 AS x", radar=_RADAR)  # opens duckdb connection
    client.close()
    client.close()
