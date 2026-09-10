-- ====================================================================
-- Adapt collection catalog — discovery + provenance ONLY
-- File: catalog.db
-- Location: {root}/collections/{collection_id}/catalog.db
--
-- Science rows live in products.db; run lifecycle lives in the store
-- registry. This database answers: which artifacts/scans exist, which
-- products each scan has, and where every object came from.
-- ====================================================================

PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;

-- One row per stored object. artifact_id is uuid4 hex allocated BEFORE the
-- bytes are written; the sha256 checksum is recorded at commit.
CREATE TABLE IF NOT EXISTS artifacts (
    artifact_id       TEXT PRIMARY KEY,
    artifact_type     TEXT NOT NULL,        -- e.g. raw_volume | gridded3d | segmentation2d
    producer          TEXT NOT NULL,
    run_id            TEXT NOT NULL,
    scan_id           TEXT,                 -- NULL for run-level artifacts
    observation_time  TEXT,                 -- ISO8601 UTC (ordering/display metadata)
    original_filename TEXT,
    source_uri        TEXT,
    object_name       TEXT NOT NULL UNIQUE, -- '{artifact_id}{suffix}' relative to objects/
    checksum_sha256   TEXT NOT NULL,
    size_bytes        INTEGER NOT NULL,
    created_at        TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_artifacts_run_scan_type
    ON artifacts(run_id, scan_id, artifact_type);
CREATE INDEX IF NOT EXISTS idx_artifacts_source_uri ON artifacts(source_uri);

-- Multi-parent provenance edges; no parent-count limit.
CREATE TABLE IF NOT EXISTS artifact_lineage (
    child_artifact_id  TEXT NOT NULL REFERENCES artifacts(artifact_id),
    parent_artifact_id TEXT NOT NULL REFERENCES artifacts(artifact_id),
    relationship       TEXT NOT NULL,
    PRIMARY KEY (child_artifact_id, parent_artifact_id, relationship)
);

-- One row per scan per run; keys verbatim from the scan-identity change.
-- A scan is 'complete' only when all REQUIRED_SCAN_PRODUCTS are linked.
CREATE TABLE IF NOT EXISTS scans (
    run_id           TEXT NOT NULL,
    scan_id          TEXT NOT NULL,   -- content-derived scan identity (join key)
    scan_time        TEXT NOT NULL,   -- ISO8601 UTC (ordering/display metadata)
    scan_date        TEXT NOT NULL,   -- YYYYMMDD (UTC), derived from scan_time
    start_time       TEXT,            -- per-source coverage metadata (nullable)
    end_time         TEXT,            -- per-source coverage metadata (nullable)
    source_file_name TEXT NOT NULL,   -- original source filename (metadata, not identity)
    status           TEXT NOT NULL DEFAULT 'pending'
                     CHECK (status IN ('pending', 'complete', 'failed')),
    created_at       TEXT NOT NULL,
    updated_at       TEXT NOT NULL,

    PRIMARY KEY (run_id, scan_id),
    -- Two DIFFERENT scans sharing one nominal second within a run is a data
    -- problem (e.g. a duplicate download); surface it at registration instead
    -- of letting time-ordered reads silently pick one.
    UNIQUE (run_id, scan_time)
);

CREATE INDEX IF NOT EXISTS idx_scans_time ON scans(run_id, scan_time DESC);

-- Which products a scan has, and where each lives (object or products.db
-- table). Completeness = all REQUIRED_SCAN_PRODUCTS linked; auxiliary links
-- (tracking, volume stats, ...) never complete a scan.
CREATE TABLE IF NOT EXISTS scan_products (
    run_id      TEXT NOT NULL,
    scan_id     TEXT NOT NULL,
    product     TEXT NOT NULL,
    kind        TEXT NOT NULL CHECK (kind IN ('artifact', 'table')),
    artifact_id TEXT REFERENCES artifacts(artifact_id),
    table_name  TEXT,
    created_at  TEXT NOT NULL,

    PRIMARY KEY (run_id, scan_id, product),
    FOREIGN KEY (run_id, scan_id) REFERENCES scans(run_id, scan_id)
);

-- Frozen first-frame schema per module table; mirrored in products.db and
-- compared by store.validate_collection.
CREATE TABLE IF NOT EXISTS table_schemas (
    table_name    TEXT PRIMARY KEY,
    owner_module  TEXT NOT NULL,
    granularity   TEXT NOT NULL CHECK (granularity IN ('scan', 'time', 'run')),
    primary_key   TEXT NOT NULL,   -- JSON array of column names
    index_columns TEXT NOT NULL,   -- JSON array of column names
    columns_json  TEXT NOT NULL,   -- frozen [{"name": ..., "type": ...}]
    frozen_at     TEXT NOT NULL
);
