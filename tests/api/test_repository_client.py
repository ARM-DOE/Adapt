# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Unit tests for RepositoryClient new domain-object methods.

Uses a minimal synthetic repository built on disk (tmp_path).
No network, no NEXRAD files, no pipeline needed.
"""

import sqlite3
from datetime import UTC, datetime

import pandas as pd
import pytest

from adapt.api.client import RepositoryClient
from adapt.api.domain import Run, Track
from adapt.api.selection import FilterSpec
from tests.api.synthetic_repo import _RADAR, _RUN_ID, _UID_A, _UID_B

pytestmark = pytest.mark.unit

# ---------------------------------------------------------------------------
# Tests: radars()
# ---------------------------------------------------------------------------


class TestRadars:
    def test_returns_registered_radar(self, client):
        assert _RADAR in client.radars()

    def test_returns_list_of_strings(self, client):
        result = client.radars()
        assert isinstance(result, list)
        assert all(isinstance(r, str) for r in result)


# ---------------------------------------------------------------------------
# Tests: runs()
# ---------------------------------------------------------------------------


class TestRuns:
    def test_returns_list_of_run_objects(self, client):
        result = client.runs()
        assert isinstance(result, list)
        assert len(result) >= 1
        assert all(isinstance(r, Run) for r in result)

    def test_run_has_correct_run_id(self, client):
        runs = client.runs()
        run_ids = [r.run_id for r in runs]
        assert _RUN_ID in run_ids

    def test_run_has_correct_radar_id(self, client):
        runs = client.runs(radar=_RADAR)
        assert all(r.radar_id == _RADAR for r in runs)

    def test_run_filtered_by_radar(self, client):
        runs = client.runs(radar=_RADAR)
        assert len(runs) >= 1


# ---------------------------------------------------------------------------
# Tests: run()
# ---------------------------------------------------------------------------


class TestRun:
    def test_returns_run_for_valid_run_id(self, client):
        run = client.run(_RUN_ID)
        assert isinstance(run, Run)
        assert run.run_id == _RUN_ID

    def test_raises_for_unknown_run_id(self, client):
        with pytest.raises(ValueError, match="not found"):
            client.run("nonexistent-run")


# ---------------------------------------------------------------------------
# Tests: tracks()
# ---------------------------------------------------------------------------


class TestTracks:
    def test_returns_dataframe(self, client):
        df = client.tracks(_RUN_ID, radar=_RADAR)
        assert isinstance(df, pd.DataFrame)

    def test_contains_both_tracks(self, client):
        df = client.tracks(_RUN_ID, radar=_RADAR)
        assert _UID_A in df["cell_uid"].values
        assert _UID_B in df["cell_uid"].values

    def test_track_count_matches_inserted_rows(self, client):
        df = client.tracks(_RUN_ID, radar=_RADAR)
        assert len(df) == 2


# ---------------------------------------------------------------------------
# Tests: track()
# ---------------------------------------------------------------------------


class TestTrack:
    def test_returns_track_object(self, client):
        track = client.track(_RUN_ID, _UID_A, radar=_RADAR)
        assert isinstance(track, Track)

    def test_track_has_correct_uid(self, client):
        track = client.track(_RUN_ID, _UID_A, radar=_RADAR)
        assert track.cell_uid == _UID_A

    def test_track_has_correct_max_area(self, client):
        track = client.track(_RUN_ID, _UID_A, radar=_RADAR)
        assert track.max_area_km2 == pytest.approx(500.0)

    def test_track_has_correct_max_reflectivity(self, client):
        track = client.track(_RUN_ID, _UID_A, radar=_RADAR)
        assert track.max_reflectivity_dbz == pytest.approx(62.3)

    def test_track_has_correct_n_scans(self, client):
        track = client.track(_RUN_ID, _UID_A, radar=_RADAR)
        assert track.n_scans == 24

    def test_track_has_correct_origin_type(self, client):
        track = client.track(_RUN_ID, _UID_A, radar=_RADAR)
        assert track.origin_type == "INITIATION"

    def test_raises_for_unknown_cell_uid(self, client):
        with pytest.raises(ValueError, match="not found"):
            client.track(_RUN_ID, "nonexistent-uid", radar=_RADAR)


# ---------------------------------------------------------------------------
# Tests: select()
# ---------------------------------------------------------------------------


class TestSelect:
    def test_empty_spec_returns_all_tracks(self, client):
        df = client.select(_RUN_ID, FilterSpec(), radar=_RADAR)
        assert len(df) == 2

    def test_n_scans_min_filters_short_tracks(self, client):
        # uid_a has 24 scans, uid_b has 6 — filter to n_scans >= 10
        df = client.select(_RUN_ID, FilterSpec(n_scans_min=10), radar=_RADAR)
        assert _UID_A in df["cell_uid"].values
        assert _UID_B not in df["cell_uid"].values

    def test_max_area_min_filters_small_tracks(self, client):
        # uid_a has area 500, uid_b has area 120 — filter to >= 200
        df = client.select(_RUN_ID, FilterSpec(max_area_min_km2=200.0), radar=_RADAR)
        assert _UID_A in df["cell_uid"].values
        assert _UID_B not in df["cell_uid"].values

    def test_max_refl_min_filters_weak_tracks(self, client):
        # uid_a has refl 62.3, uid_b has 48.1 — filter to >= 55
        df = client.select(_RUN_ID, FilterSpec(max_refl_min_dbz=55.0), radar=_RADAR)
        assert _UID_A in df["cell_uid"].values
        assert _UID_B not in df["cell_uid"].values

    def test_origin_type_filter(self, client):
        df = client.select(
            _RUN_ID,
            FilterSpec(origin_types=frozenset(["INITIATION"])),
            radar=_RADAR,
        )
        assert _UID_A in df["cell_uid"].values
        assert _UID_B not in df["cell_uid"].values

    def test_no_match_returns_empty_dataframe(self, client):
        df = client.select(_RUN_ID, FilterSpec(n_scans_min=9999), radar=_RADAR)
        assert len(df) == 0


# ---------------------------------------------------------------------------
# Tests: track_history() and track_events()
# ---------------------------------------------------------------------------


class TestTrackHistory:
    def test_track_history_returns_dataframe(self, client):
        df = client.track_history(_RUN_ID, _UID_A, radar=_RADAR)
        assert isinstance(df, pd.DataFrame)

    def test_track_history_contains_expected_row(self, client):
        df = client.track_history(_RUN_ID, _UID_A, radar=_RADAR)
        assert len(df) >= 1
        assert _UID_A in df["cell_uid"].values


class TestTrackEvents:
    def test_track_events_returns_dataframe(self, client):
        df = client.track_events(_RUN_ID, _UID_A, radar=_RADAR)
        assert isinstance(df, pd.DataFrame)


class TestCellsAtScan:
    def test_returns_rows_for_exact_scan(self, client):
        df = client.cells_at_scan(_RUN_ID, datetime(2024, 6, 1, 12, 0, 0, tzinfo=UTC), radar=_RADAR)
        assert list(df["cell_uid"]) == [_UID_A]

    def test_empty_for_scan_with_no_cells(self, client):
        df = client.cells_at_scan(_RUN_ID, datetime(2024, 6, 1, 13, 0, 0, tzinfo=UTC), radar=_RADAR)
        assert df.empty


class TestLightningViaGenericTable:
    """The lightning read path is the generic table API (no dedicated method)."""

    def _add_lma_table(self, repo_root):
        catalog_path = repo_root / _RADAR / "catalog.db"
        conn = sqlite3.connect(str(catalog_path))
        conn.execute(
            "CREATE TABLE xlma_stat_minutes (run_id TEXT, cell_uid TEXT, time TEXT, "
            "flash_count INTEGER, lightning_source_count INTEGER, PRIMARY KEY (cell_uid, time))"
        )
        conn.execute(
            "INSERT INTO xlma_stat_minutes VALUES (?,?,?,?,?)",
            (_RUN_ID, _UID_A, "2024-01-01T12:00:00Z", 5, 50),
        )
        # Schema registration normally done by ModuleOutputWriter on first write
        conn.execute(
            "CREATE TABLE IF NOT EXISTS module_schemas ("
            "table_name TEXT PRIMARY KEY, primary_key TEXT, index_columns TEXT, "
            "columns_json TEXT, updated_at TEXT)"
        )
        conn.execute(
            "INSERT INTO module_schemas VALUES (?,?,?,?,?)",
            ("xlma_stat_minutes", "run_id,time,cell_uid", "cell_uid,time", "[]", ""),
        )
        conn.commit()
        conn.close()

    def test_lightning_rows_readable_through_table(self, repo_root):
        self._add_lma_table(repo_root)
        client = RepositoryClient(repo_root)
        try:
            assert "xlma_stat_minutes" in set(client.tables(_RADAR)["table_name"])
            df = client.table(
                "xlma_stat_minutes", radar=_RADAR, run_id=_RUN_ID, filters={"cell_uid": _UID_A}
            )
        finally:
            client.close()
        assert list(df["flash_count"]) == [5]

    def test_lightning_table_absent_means_not_discovered(self, client):
        assert "xlma_stat_minutes" not in set(client.tables(_RADAR)["table_name"])
        with pytest.raises(ValueError, match="Unknown table"):
            client.table("xlma_stat_minutes", radar=_RADAR)


# ---------------------------------------------------------------------------
# Tests: annotate() and annotations()
# ---------------------------------------------------------------------------


class TestAnnotations:
    def test_annotate_then_retrieve(self, client):
        client.annotate(_RUN_ID, _UID_A, radar=_RADAR, tag="supercell")
        df = client.annotations(_RUN_ID, radar=_RADAR)
        assert not df.empty
        assert "supercell" in df["tag"].values

    def test_annotations_returns_empty_when_none(self, client):
        df = client.annotations(_RUN_ID, radar=_RADAR)
        assert isinstance(df, pd.DataFrame)

    def test_annotate_raises_when_both_none(self, client):
        with pytest.raises(ValueError):
            client.annotate(_RUN_ID, _UID_A, radar=_RADAR, tag=None, note=None)
