-- ====================================================================
-- Adapt Radar Catalog Database Schema (Radar Level)
-- File: catalog.db  
-- Location: {root_dir}/{radar}/catalog.db
-- 
-- Purpose: Detailed tracking of all data items for a specific radar
-- ====================================================================

PRAGMA journal_mode=WAL;  -- Enable Write-Ahead Logging for concurrency
PRAGMA foreign_keys=ON;   -- Enforce foreign key constraints

-- ====================================================================
-- Table: items
--
-- Core registry of all generated data objects for this radar
-- ====================================================================
CREATE TABLE IF NOT EXISTS items (
    item_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    item_type TEXT NOT NULL,
    scan_time TEXT NOT NULL,           -- ISO8601 UTC timestamp
    file_path TEXT NOT NULL,           -- Relative path from radar dir
    parent_ids TEXT,                   -- JSON array of parent item_ids for lineage
    processing_stage TEXT NOT NULL,    -- acquisition | gridding | segmentation | analysis
    status TEXT NOT NULL,              -- complete | failed | processing
    error_message TEXT,                -- Error if status=failed
    metadata TEXT,                     -- JSON metadata
    file_size_bytes INTEGER,           -- File size for monitoring
    file_hash TEXT,                    -- SHA256 for integrity
    created_at TEXT NOT NULL,          -- ISO8601 UTC timestamp
    updated_at TEXT NOT NULL           -- ISO8601 UTC timestamp
);

CREATE INDEX IF NOT EXISTS idx_items_run ON items(run_id);
CREATE INDEX IF NOT EXISTS idx_items_type ON items(item_type);
CREATE INDEX IF NOT EXISTS idx_items_scan_time ON items(scan_time DESC);
CREATE INDEX IF NOT EXISTS idx_items_type_time ON items(item_type, scan_time DESC);
CREATE INDEX IF NOT EXISTS idx_items_run_type_time ON items(run_id, item_type, scan_time DESC);
CREATE INDEX IF NOT EXISTS idx_items_status ON items(status);

-- ====================================================================
-- Table: progress
-- 
-- Real-time processing state tracking per run
-- ====================================================================
CREATE TABLE IF NOT EXISTS progress (
    run_id TEXT PRIMARY KEY,
    latest_downloaded_time TEXT,       -- Most recent scan downloaded
    latest_gridded_time TEXT,          -- Most recent scan gridded
    latest_segmented_time TEXT,        -- Most recent scan segmented
    latest_analyzed_time TEXT,         -- Most recent scan analyzed
    num_items_complete INTEGER DEFAULT 0,
    num_items_failed INTEGER DEFAULT 0,
    queue_depth INTEGER DEFAULT 0,     -- Items waiting to be processed
    last_updated TEXT NOT NULL         -- ISO8601 UTC timestamp
);

CREATE INDEX IF NOT EXISTS idx_progress_updated ON progress(last_updated DESC);

-- ====================================================================
-- Table: schemas
--
-- Schema definitions for Parquet tables (analysis2d, etc.)
-- Allows client to discover column types without reading files
-- ====================================================================
CREATE TABLE IF NOT EXISTS schemas (
    item_type TEXT PRIMARY KEY,
    columns_json TEXT NOT NULL,        -- JSON: [{"name": "refl", "dtype": "float32"}, ...]
    schema_version INTEGER DEFAULT 1,  -- For schema evolution
    updated_at TEXT NOT NULL           -- ISO8601 UTC timestamp
);

-- ====================================================================
-- Table: scans
--
-- Central scan index: one row per radar scan time
-- Provides efficient time-based lookup and cross-item relationships
-- ====================================================================
CREATE TABLE IF NOT EXISTS scans (
    scan_id TEXT PRIMARY KEY,
    scan_time TEXT NOT NULL,           -- ISO8601 UTC timestamp (indexed)
    scan_date TEXT NOT NULL,           -- YYYYMMDD for partitioning
    run_id TEXT NOT NULL,

    -- Item references (NULL if not yet produced)
    gridded3d_item_id TEXT,
    segmentation2d_item_id TEXT,
    projection2d_item_id TEXT,
    analysis2d_item_id TEXT,

    -- Quick-access metadata (denormalized for GUI speed)
    num_cells INTEGER DEFAULT 0,
    max_reflectivity REAL,
    has_tracks BOOLEAN DEFAULT FALSE,

    -- Provenance
    nexrad_file_name TEXT,             -- Original AWS filename
    processing_status TEXT NOT NULL DEFAULT 'pending',  -- pending | complete | partial | failed
    created_at TEXT NOT NULL,          -- ISO8601 UTC timestamp
    updated_at TEXT NOT NULL,          -- ISO8601 UTC timestamp

    FOREIGN KEY (gridded3d_item_id) REFERENCES items(item_id),
    FOREIGN KEY (segmentation2d_item_id) REFERENCES items(item_id),
    FOREIGN KEY (projection2d_item_id) REFERENCES items(item_id),
    FOREIGN KEY (analysis2d_item_id) REFERENCES items(item_id)
);

CREATE INDEX IF NOT EXISTS idx_scans_time ON scans(scan_time DESC);
CREATE INDEX IF NOT EXISTS idx_scans_date ON scans(scan_date);
CREATE INDEX IF NOT EXISTS idx_scans_run ON scans(run_id, scan_time DESC);
CREATE INDEX IF NOT EXISTS idx_scans_status ON scans(processing_status);

-- ====================================================================
-- Table: cells_by_scan
--
-- Wide canonical live table: one row per active tracked cell per scan.
-- Contains all per-cell/per-scan outputs (analysis + track identity).
-- Additional cell_stats columns are added dynamically via ALTER TABLE.
-- ====================================================================
CREATE TABLE IF NOT EXISTS cells_by_scan (
    run_id                  TEXT NOT NULL,
    scan_time               TEXT NOT NULL,       -- ISO8601 UTC
    cell_label              INTEGER NOT NULL,
    cell_uid                TEXT NOT NULL,
    track_index             INTEGER NOT NULL,

    -- Core cell stats (extended dynamically for all cell_stats columns)
    cell_area_sqkm          REAL,
    cell_centroid_mass_lat  REAL,
    cell_centroid_mass_lon  REAL,
    cell_centroid_geom_x    REAL,
    cell_centroid_geom_y    REAL,
    radar_reflectivity_max  REAL,
    radar_reflectivity_mean REAL,
    radar_differential_reflectivity_max REAL,
    area_40dbz_km2          REAL,

    -- Track adjacency summary
    n_adjacent_tracks        INTEGER NOT NULL DEFAULT 0,
    adjacent_cell_uids_json  TEXT,

    -- Forward-set convenience flags (known at write time)
    is_initiated_here       INTEGER NOT NULL DEFAULT 0,
    is_split_target_here    INTEGER NOT NULL DEFAULT 0,
    is_merge_target_here    INTEGER NOT NULL DEFAULT 0,

    -- Age since track birth
    age_seconds             REAL NOT NULL DEFAULT 0,

    -- Retroactively updated flags (canonical truth in track_events)
    is_split_source_here    INTEGER NOT NULL DEFAULT 0,
    is_merge_source_here    INTEGER NOT NULL DEFAULT 0,
    is_terminated_after_here INTEGER NOT NULL DEFAULT 0,

    PRIMARY KEY (run_id, scan_time, cell_uid),
    UNIQUE (run_id, scan_time, cell_label)
);

CREATE INDEX IF NOT EXISTS idx_cbs_track ON cells_by_scan(run_id, cell_uid, scan_time);
CREATE INDEX IF NOT EXISTS idx_cbs_scan  ON cells_by_scan(run_id, scan_time);
CREATE INDEX IF NOT EXISTS idx_cbs_label ON cells_by_scan(run_id, cell_label, scan_time);

-- ====================================================================
-- Table: track_events
--
-- Authoritative lineage table: one row per lineage edge or lifecycle event.
-- ====================================================================
CREATE TABLE IF NOT EXISTS track_events (
    event_id           INTEGER PRIMARY KEY,      -- autoincrement surrogate
    run_id             TEXT NOT NULL,
    source_scan_time   TEXT,                     -- ISO8601 UTC; NULL for INITIATION
    target_scan_time   TEXT,                     -- ISO8601 UTC; NULL for TERMINATION
    event_type         TEXT NOT NULL,            -- CONTINUE|SPLIT|MERGE|INITIATION|TERMINATION
    source_cell_uid    TEXT,
    target_cell_uid    TEXT,
    source_track_index INTEGER,
    target_track_index INTEGER,
    source_cell_label  INTEGER,
    target_cell_label  INTEGER,
    cost               REAL,
    is_dominant        INTEGER NOT NULL DEFAULT 0,
    event_group_id     TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_te_source ON track_events(run_id, source_cell_uid);
CREATE INDEX IF NOT EXISTS idx_te_target ON track_events(run_id, target_cell_uid);
CREATE INDEX IF NOT EXISTS idx_te_group  ON track_events(run_id, event_group_id);

-- ====================================================================
-- Table: tracks
--
-- Convenience summary index: one row per track lifecycle.
-- Not authoritative for lineage — use track_events for that.
-- ====================================================================
CREATE TABLE IF NOT EXISTS tracks (
    run_id                          TEXT NOT NULL,
    cell_uid                        TEXT NOT NULL,
    track_index                     INTEGER NOT NULL,
    first_seen_time                 TEXT NOT NULL,
    last_seen_time                  TEXT NOT NULL,
    n_scans                         INTEGER NOT NULL DEFAULT 0,
    origin_type                     TEXT NOT NULL,  -- INITIATION|SPLIT|MERGE|UNKNOWN
    origin_event_group_id           TEXT,
    origin_n_parents                INTEGER NOT NULL DEFAULT 0,
    origin_primary_parent_cell_uid  TEXT,
    termination_type                TEXT NOT NULL DEFAULT 'ACTIVE_AT_END',
                                                    -- TERMINATION|MERGED|ACTIVE_AT_END|UNKNOWN
    termination_event_group_id      TEXT,
    terminated_into_cell_uid        TEXT,
    max_area_sqkm                   REAL,
    max_reflectivity                REAL,

    PRIMARY KEY (run_id, cell_uid)
);

CREATE INDEX IF NOT EXISTS idx_tracks_run ON tracks(run_id);
