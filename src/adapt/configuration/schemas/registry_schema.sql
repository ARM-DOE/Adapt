-- ====================================================================
-- Adapt Registry Database Schema (Root Level)
-- File: adapt_registry.db
-- Location: {root_dir}/adapt_registry.db
-- 
-- Purpose: Global registry tracking all runs and radars
-- ====================================================================

PRAGMA journal_mode=WAL;  -- Enable Write-Ahead Logging for concurrency
PRAGMA foreign_keys=ON;   -- Enforce foreign key constraints

-- ====================================================================
-- Table: runs
-- 
-- Tracks all pipeline runs across all radars
-- ====================================================================
CREATE TABLE IF NOT EXISTS runs (
    run_id TEXT PRIMARY KEY,
    radar TEXT NOT NULL,
    start_time TEXT NOT NULL,         -- ISO8601 UTC timestamp
    end_time TEXT,                     -- NULL if running
    status TEXT NOT NULL,              -- running | complete | failed
    mode TEXT,                         -- realtime | historical | backfill
    config_path TEXT,                  -- Path to runtime config JSON
    repository_version TEXT NOT NULL,  -- Adapt version
    created_at TEXT NOT NULL           -- ISO8601 UTC timestamp
);

CREATE INDEX IF NOT EXISTS idx_runs_start_time ON runs(start_time DESC);
CREATE INDEX IF NOT EXISTS idx_runs_radar ON runs(radar);
CREATE INDEX IF NOT EXISTS idx_runs_status ON runs(status);

-- ====================================================================
-- Table: radars
-- 
-- Registry of radar stations processed by this repository
-- ====================================================================
CREATE TABLE IF NOT EXISTS radars (
    radar TEXT PRIMARY KEY,
    catalog_path TEXT NOT NULL,        -- Path to radar's catalog DB
    data_path TEXT NOT NULL,           -- Path to radar's data directory
    location_lat REAL,                 -- Radar latitude (optional metadata)
    location_lon REAL,                 -- Radar longitude (optional metadata)
    created_at TEXT NOT NULL,          -- ISO8601 UTC timestamp
    last_updated TEXT NOT NULL         -- ISO8601 UTC timestamp
);

CREATE INDEX IF NOT EXISTS idx_radars_updated ON radars(last_updated DESC);

-- ====================================================================
-- Table: item_types
-- 
-- Registry of product types that can be stored
-- Allows dynamic addition without code changes
-- ====================================================================
CREATE TABLE IF NOT EXISTS item_types (
    item_type TEXT PRIMARY KEY,
    description TEXT NOT NULL,
    storage_format TEXT NOT NULL,      -- parquet | netcdf | sqlite
    dimensionality TEXT NOT NULL,      -- 3d | 2d | table
    created_at TEXT NOT NULL           -- ISO8601 UTC timestamp
);

-- Prepopulate with known types
INSERT OR IGNORE INTO item_types (item_type, description, storage_format, dimensionality, created_at)
VALUES 
    ('gridded3d', 'Gridded reflectivity volume', 'netcdf', '3d', datetime('now')),
    ('segmentation2d', 'Cell segmentation masks', 'netcdf', '2d', datetime('now')),
    ('projection2d', 'Cell motion projections', 'netcdf', '2d', datetime('now')),
    ('analysis2d', 'Cell-level analysis metrics', 'parquet', 'table', datetime('now'));
