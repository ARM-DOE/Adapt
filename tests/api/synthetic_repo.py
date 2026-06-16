# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Synthetic on-disk repository builder shared by the API client tests.

One radar, one run, two tracks — enough structure for every RepositoryClient
read path. No network, no NEXRAD files, no pipeline.
"""

import sqlite3

from adapt.persistence.registry import RepositoryRegistry

# Minimal catalog DDL — three-table schema used by TrackStore
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
    duration_seconds REAL NOT NULL DEFAULT 0,
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
# Canonical scan-time join-key format (adapt.utils.time.to_scan_iso)
_T0 = "2024-06-01T12:00:00Z"
_T1 = "2024-06-01T14:00:00Z"
_UID_A = "uid_alpha"
_UID_B = "uid_beta"


_TRACKS_INSERT = (
    "INSERT INTO cell_tracks (run_id, cell_uid, first_seen_time, last_seen_time, "
    "n_scans, origin_type, origin_event_group_id, origin_n_parents, "
    "origin_primary_parent_cell_uid, termination_type, termination_event_group_id, "
    "terminated_into_cell_uid, max_area_sqkm, max_reflectivity) "
    "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
)


def build_synthetic_repo(tmp_path):
    """Create the synthetic repository under tmp_path and return its root."""
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
        _TRACKS_INSERT,
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
        _TRACKS_INSERT,
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
