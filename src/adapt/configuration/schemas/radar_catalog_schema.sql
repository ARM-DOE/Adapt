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
