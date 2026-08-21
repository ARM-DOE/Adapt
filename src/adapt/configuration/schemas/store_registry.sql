-- ====================================================================
-- Adapt store registry
-- File: registry.db
-- Location: {root}/registry.db
--
-- Purpose: collections and the single run lifecycle for the whole store.
-- Per-collection discovery/provenance lives in collections/{id}/catalog.db;
-- module-owned science tables live in collections/{id}/products.db.
-- ====================================================================

PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;

-- One row per collection: a radar/site data domain (e.g. KILX). Source kind
-- (nexrad | mrms | goes ...) is metadata, never directory structure.
CREATE TABLE IF NOT EXISTS collections (
    collection_id TEXT PRIMARY KEY,
    source_kind   TEXT NOT NULL,
    location_lat  REAL,
    location_lon  REAL,
    metadata_json TEXT,
    created_at    TEXT NOT NULL              -- ISO8601 UTC
);

-- One row per pipeline run; the ONLY run lifecycle in the store.
CREATE TABLE IF NOT EXISTS runs (
    run_id           TEXT PRIMARY KEY,
    collection_id    TEXT NOT NULL REFERENCES collections(collection_id),
    status           TEXT NOT NULL
                     CHECK (status IN ('running', 'completed', 'cancelled', 'failed')),
    config_hash      TEXT NOT NULL,
    config_json      TEXT NOT NULL,
    pipeline_version TEXT NOT NULL,
    environment_json TEXT NOT NULL,
    started_at       TEXT NOT NULL,          -- ISO8601 UTC
    ended_at         TEXT,                   -- NULL while running
    scans_processed  INTEGER NOT NULL DEFAULT 0,
    scans_failed     INTEGER NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_runs_collection ON runs(collection_id);
CREATE INDEX IF NOT EXISTS idx_runs_started ON runs(started_at DESC);
CREATE INDEX IF NOT EXISTS idx_runs_status ON runs(status);

-- One row per module execution within a run. Re-execution upserts: a module
-- re-run (including after-the-fact postprocess runs, which attach to the
-- original run_id) replaces its own outputs and this row.
CREATE TABLE IF NOT EXISTS run_modules (
    run_id           TEXT NOT NULL REFERENCES runs(run_id),
    module           TEXT NOT NULL,
    status           TEXT NOT NULL,
    duration_seconds REAL,
    detail_json      TEXT,
    PRIMARY KEY (run_id, module)
);

-- Warnings and errors surfaced during a run (replaces warning_history /
-- error_history).
CREATE TABLE IF NOT EXISTS run_events (
    event_id     INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id       TEXT NOT NULL REFERENCES runs(run_id),
    severity     TEXT NOT NULL CHECK (severity IN ('warning', 'error')),
    module       TEXT,
    message      TEXT NOT NULL,
    context_json TEXT,
    created_at   TEXT NOT NULL               -- ISO8601 UTC
);

CREATE INDEX IF NOT EXISTS idx_run_events_run ON run_events(run_id);
