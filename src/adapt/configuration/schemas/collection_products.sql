-- ====================================================================
-- Adapt collection products — module-owned relational tables ONLY
-- File: products.db
-- Location: {root}/collections/{collection_id}/products.db
--
-- Module tables (cells_by_scan, cell_tracks, cell_stats, ...) are created
-- by the SchemaLedger at first-frame freeze — never static DDL, never
-- ALTER. Only the schema mirror and the user-owned annotations table are
-- static.
-- ====================================================================

PRAGMA journal_mode=WAL;

-- Mirror of catalog.db table_schemas; snapshot agreement is checked by
-- store.validate_collection at collection open.
CREATE TABLE IF NOT EXISTS table_schemas (
    table_name    TEXT PRIMARY KEY,
    owner_module  TEXT NOT NULL,
    granularity   TEXT NOT NULL CHECK (granularity IN ('scan', 'time', 'run')),
    primary_key   TEXT NOT NULL,   -- JSON array of column names
    index_columns TEXT NOT NULL,   -- JSON array of column names
    columns_json  TEXT NOT NULL,   -- frozen [{"name": ..., "type": ...}]
    frozen_at     TEXT NOT NULL
);

-- User-owned cell annotations (the one non-module table; written through
-- client.annotate, joined by tag in population selection).
CREATE TABLE IF NOT EXISTS annotations (
    run_id     TEXT NOT NULL,
    cell_uid   TEXT NOT NULL,
    tag        TEXT NOT NULL,
    note       TEXT,
    created_at TEXT NOT NULL,

    PRIMARY KEY (run_id, cell_uid, tag)
);
