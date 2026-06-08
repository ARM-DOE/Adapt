# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Unit tests for RepositoryClient new domain-object methods.

Uses a minimal synthetic repository built on disk (tmp_path).
No network, no NEXRAD files, no pipeline needed.
"""

import sqlite3

import pandas as pd
import pytest

from adapt.api.client import RepositoryClient
from adapt.api.domain import Run, Track
from adapt.api.selection import FilterSpec
from adapt.persistence.registry import RepositoryRegistry

pytestmark = pytest.mark.unit

# ---------------------------------------------------------------------------
# Minimal catalog DDL — three-table schema used by TrackStore
# ---------------------------------------------------------------------------

_CATALOG_DDL = """
PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS cells_by_scan (
    run_id TEXT NOT NULL, scan_time TEXT NOT NULL, cell_label INTEGER NOT NULL,
    cell_uid TEXT NOT NULL, cell_area_sqkm REAL, cell_centroid_mass_lat REAL,
    cell_centroid_mass_lon REAL, cell_centroid_geom_x REAL,
    cell_centroid_geom_y REAL, radar_reflectivity_max REAL,
    radar_reflectivity_mean REAL, radar_differential_reflectivity_max REAL,
    area_40dbz_km2 REAL, age_seconds REAL NOT NULL DEFAULT 0,
    n_adjacent_cells INTEGER NOT NULL DEFAULT 0, adjacent_cell_uids_json TEXT,
    is_initiated_here INTEGER NOT NULL DEFAULT 0,
    is_split_target_here INTEGER NOT NULL DEFAULT 0,
    is_merge_target_here INTEGER NOT NULL DEFAULT 0,
    is_split_source_here INTEGER NOT NULL DEFAULT 0,
    is_merge_source_here INTEGER NOT NULL DEFAULT 0,
    is_terminated_after_here INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (run_id, scan_time, cell_uid),
    UNIQUE (run_id, scan_time, cell_label)
);

CREATE TABLE IF NOT EXISTS cell_events (
    event_id INTEGER PRIMARY KEY, run_id TEXT NOT NULL,
    source_scan_time TEXT, target_scan_time TEXT, event_type TEXT NOT NULL,
    source_cell_uid TEXT, target_cell_uid TEXT,
    source_cell_label INTEGER, target_cell_label INTEGER,
    cost REAL, is_dominant INTEGER NOT NULL DEFAULT 0,
    event_group_id TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS cell_tracks (
    run_id TEXT NOT NULL, cell_uid TEXT NOT NULL,
    first_seen_time TEXT NOT NULL, last_seen_time TEXT NOT NULL,
    n_scans INTEGER NOT NULL DEFAULT 0, origin_type TEXT NOT NULL,
    origin_event_group_id TEXT, origin_n_parents INTEGER NOT NULL DEFAULT 0,
    origin_primary_parent_cell_uid TEXT,
    termination_type TEXT NOT NULL DEFAULT 'ACTIVE_AT_END',
    termination_event_group_id TEXT, terminated_into_cell_uid TEXT,
    max_area_sqkm REAL, max_reflectivity REAL,
    PRIMARY KEY (run_id, cell_uid)
);

CREATE TABLE IF NOT EXISTS items (
    item_id TEXT PRIMARY KEY, run_id TEXT, item_type TEXT,
    scan_time TEXT, file_path TEXT, status TEXT DEFAULT 'pending'
);
"""

_RUN_ID = "2024JUN01-1200-KDIX"
_RADAR = "KDIX"
_T0 = "2024-06-01T12:00:00+00:00"
_T1 = "2024-06-01T14:00:00+00:00"
_UID_A = "uid_alpha"
_UID_B = "uid_beta"


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def repo_root(tmp_path):
    """Minimal synthetic repository with one radar and one run."""
    radar_dir = tmp_path / _RADAR
    radar_dir.mkdir()

    # Registry (adapt_registry.db)
    RepositoryRegistry._instance = None  # reset singleton
    registry = RepositoryRegistry.get_instance(tmp_path)
    registry.register_radar(_RADAR, str(radar_dir))
    registry.register_run(_RUN_ID, _RADAR, mode="historical")
    registry.close()
    RepositoryRegistry._instance = None  # reset for client usage

    # Catalog (catalog.db) with two tracks
    catalog_path = radar_dir / "catalog.db"
    conn = sqlite3.connect(str(catalog_path))
    conn.executescript(_CATALOG_DDL)
    conn.execute(
        "INSERT INTO cell_tracks VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (
            _RUN_ID,
            _UID_A,
            _T0,
            _T1,
            24,
            "INITIATION",
            None,
            0,
            None,
            "TERMINATION",
            None,
            None,
            500.0,
            62.3,
        ),
    )
    conn.execute(
        "INSERT INTO cell_tracks VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (
            _RUN_ID,
            _UID_B,
            _T0,
            _T1,
            6,
            "SPLIT",
            None,
            1,
            _UID_A,
            "ACTIVE_AT_END",
            None,
            None,
            120.0,
            48.1,
        ),
    )
    conn.execute(
        "INSERT INTO cells_by_scan VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (
            _RUN_ID,
            _T0,
            1,
            _UID_A,
            500.0,
            35.0,
            -97.0,
            0.0,
            0.0,
            62.3,
            55.0,
            3.0,
            200.0,
            0.0,
            0,
            None,
            1,
            0,
            0,
            0,
            0,
            0,
        ),
    )
    conn.execute(
        "INSERT INTO cell_events VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
        (1, _RUN_ID, None, _T0, "INITIATION", None, _UID_A, None, 1, 0.0, 1, "grp1"),
    )
    conn.commit()
    conn.close()

    return tmp_path


@pytest.fixture
def client(repo_root):
    c = RepositoryClient(repo_root)
    yield c
    c.close()


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


class TestTrackLightning:
    def _add_lma_table(self, repo_root):
        catalog_path = repo_root / _RADAR / "catalog.db"
        conn = sqlite3.connect(str(catalog_path))
        conn.execute(
            "CREATE TABLE lma_cell_stats (run_id TEXT, cell_uid TEXT, time_bin TEXT, "
            "flash_count INTEGER, source_count INTEGER, PRIMARY KEY (cell_uid, time_bin))"
        )
        conn.execute(
            "INSERT INTO lma_cell_stats VALUES (?,?,?,?,?)",
            (_RUN_ID, _UID_A, "2024-01-01T12:00:00Z", 5, 50),
        )
        conn.commit()
        conn.close()

    def test_track_lightning_returns_rows(self, repo_root):
        self._add_lma_table(repo_root)
        client = RepositoryClient(repo_root)
        try:
            df = client.track_lightning(_RUN_ID, _UID_A, radar=_RADAR)
        finally:
            client.close()
        assert list(df["flash_count"]) == [5]

    def test_track_lightning_empty_without_table(self, client):
        df = client.track_lightning(_RUN_ID, _UID_A, radar=_RADAR)
        assert df.empty


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
