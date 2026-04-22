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
-- Table: tracks
--
-- First-class track entities with lifecycle metadata
-- One row per unique track (cell tracked across time)
-- ====================================================================
CREATE TABLE IF NOT EXISTS tracks (
    track_id TEXT PRIMARY KEY,
    track_signature TEXT NOT NULL,
    track_index INTEGER NOT NULL,      -- Human-readable index (starts at 1)
    run_id TEXT NOT NULL,

    -- Lifecycle
    start_time TEXT NOT NULL,          -- ISO8601 UTC when track first appeared
    end_time TEXT,                     -- ISO8601 UTC when track ended (NULL if active)
    lifetime_minutes REAL,
    birth_event TEXT NOT NULL DEFAULT 'NEW',  -- NEW | SPLIT
    death_event TEXT,                  -- DISSIPATED | MERGED | NULL (active)
    birth_track_id TEXT,               -- Parent track if SPLIT
    death_track_id TEXT,               -- Absorbing track if MERGED

    -- Peak statistics (updated as track progresses)
    max_area_sqkm REAL,
    max_reflectivity REAL,
    max_num_cells INTEGER DEFAULT 1,

    -- Spatial extent (bounding box in km)
    bbox_min_x REAL,
    bbox_min_y REAL,
    bbox_max_x REAL,
    bbox_max_y REAL,

    created_at TEXT NOT NULL,          -- ISO8601 UTC timestamp
    updated_at TEXT NOT NULL,          -- ISO8601 UTC timestamp

    UNIQUE(run_id, track_index),
    FOREIGN KEY (birth_track_id) REFERENCES tracks(track_id),
    FOREIGN KEY (death_track_id) REFERENCES tracks(track_id)
);

CREATE INDEX IF NOT EXISTS idx_tracks_run ON tracks(run_id);
CREATE INDEX IF NOT EXISTS idx_tracks_active ON tracks(end_time) WHERE end_time IS NULL;
CREATE INDEX IF NOT EXISTS idx_tracks_time_range ON tracks(start_time, end_time);

-- ====================================================================
-- Table: track_observations
--
-- Track observations: one row per (track, scan_time) pair
-- Contains position and properties at each time step
-- ====================================================================
CREATE TABLE IF NOT EXISTS track_observations (
    observation_id TEXT PRIMARY KEY,
    track_id TEXT NOT NULL,
    scan_time TEXT NOT NULL,           -- ISO8601 UTC timestamp
    cell_id INTEGER NOT NULL,          -- Segmentation label

    -- Position (in km from radar)
    centroid_x REAL NOT NULL,
    centroid_y REAL NOT NULL,
    centroid_lat REAL,                 -- Geographic latitude
    centroid_lon REAL,                 -- Geographic longitude

    -- Properties at this time
    area_sqkm REAL,
    mean_reflectivity REAL,
    max_reflectivity REAL,
    core_area_sqkm REAL,

    -- Motion (derived, km/min)
    vx REAL,
    vy REAL,
    speed REAL,

    -- Lifecycle phase
    lifecycle_phase TEXT,              -- GROWTH | MATURE | DECAY

    created_at TEXT NOT NULL,          -- ISO8601 UTC timestamp

    FOREIGN KEY (track_id) REFERENCES tracks(track_id),
    UNIQUE(track_id, scan_time)
);

CREATE INDEX IF NOT EXISTS idx_track_obs_track ON track_observations(track_id, scan_time);
CREATE INDEX IF NOT EXISTS idx_track_obs_time ON track_observations(scan_time);
