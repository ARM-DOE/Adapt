-- Analysis-only decision log: collections/<RADAR>/decisions.db
--
-- Written by the output router from the tracking and detection modules'
-- decision records (adapt.contracts.decisions); never read by the pipeline or
-- the public API. Rows mirror the frozen record fields plus run/scan stamps.
-- Config thresholds are not repeated here: join run_id to the run registry.

-- user_version 3: tracker v2 (grown overlaps, curved gate, additive cost with a
-- no-link ceiling, crossing test, latent tracks, identity rule). Its records go
-- to tables with new names — tracking_scans, _cells, _pairs, _lineage,
-- _identity, _latent — so a decisions.db written by the previous tracker keeps
-- its tracking_frames/_candidates/_unmatched/_split_merge_tests rows untouched
-- and still takes new runs. CREATE TABLE IF NOT EXISTS does not migrate an
-- existing table: a changed column set needs a new table name or a new base_dir.
PRAGMA user_version = 3;

CREATE TABLE IF NOT EXISTS tracking_scans (
    run_id TEXT NOT NULL,
    scan_id TEXT NOT NULL,
    scan_time TEXT NOT NULL,
    dt_s REAL,
    reset_code TEXT,
    n_prev INTEGER NOT NULL,
    n_latent INTEGER NOT NULL,
    n_curr INTEGER NOT NULL,
    n_pairs INTEGER NOT NULL,
    n_pass_overlap INTEGER NOT NULL,
    n_pass_speed INTEGER NOT NULL,
    n_pass_cost INTEGER NOT NULL,
    n_components INTEGER NOT NULL,
    n_continue INTEGER NOT NULL,
    n_crossing_excluded INTEGER NOT NULL,
    n_split INTEGER NOT NULL,
    n_merge INTEGER NOT NULL,
    n_resumed INTEGER NOT NULL,
    n_identity_transfer INTEGER NOT NULL,
    n_initiation INTEGER NOT NULL,
    n_latent_created INTEGER NOT NULL,
    n_termination INTEGER NOT NULL,
    PRIMARY KEY (run_id, scan_id)
);

CREATE TABLE IF NOT EXISTS tracking_cells (
    run_id TEXT NOT NULL,
    scan_id TEXT NOT NULL,
    scan_time TEXT NOT NULL,
    cell_label INTEGER NOT NULL,
    cell_uid TEXT NOT NULL,
    area_km2 REAL NOT NULL,
    mass REAL NOT NULL,             -- Σ e, e = excess above the cell threshold
    mean_excess REAL NOT NULL,      -- ē = mass / area (per pixel)
    centre_x REAL NOT NULL,         -- e²-weighted centre, metres
    centre_y REAL NOT NULL,
    score REAL NOT NULL,            -- identity score S = log A + γ log ē (−inf: no excess)
    core_area_km2 REAL NOT NULL,    -- diagnostic, above tracker.core_field_threshold
    fate TEXT NOT NULL,             -- CONTINUE | RESUMED | SPLIT_CHILD | INITIATION
    PRIMARY KEY (run_id, scan_id, cell_label)
);
CREATE INDEX IF NOT EXISTS idx_tracking_cells_uid ON tracking_cells (run_id, cell_uid);

CREATE TABLE IF NOT EXISTS tracking_pairs (
    run_id TEXT NOT NULL,
    scan_id TEXT NOT NULL,
    scan_time TEXT NOT NULL,
    prev_cell_uid TEXT NOT NULL,
    prev_cell_label INTEGER NOT NULL,
    curr_cell_label INTEGER NOT NULL,
    source TEXT NOT NULL,           -- LIVE footprint or LATENT track
    steps INTEGER NOT NULL,         -- scan intervals spanned
    growth_radius_km REAL NOT NULL,
    footprint_area_px INTEGER NOT NULL,
    footprint_grown_px INTEGER NOT NULL,
    cell_grown_px INTEGER NOT NULL,
    intersection_px INTEGER NOT NULL,
    o_c REAL NOT NULL,              -- |H+ ∩ M+| / |M+|
    o_h REAL NOT NULL,              -- |H+ ∩ M+| / |H+|
    u REAL NOT NULL,                -- 1 − √(o_c o_h)
    predicted_x REAL NOT NULL,
    predicted_y REAL NOT NULL,
    curr_centre_x REAL NOT NULL,
    curr_centre_y REAL NOT NULL,
    d REAL NOT NULL,                -- shape-aware residual √(δᵀ Σ⁻¹ δ)
    speed_ms REAL NOT NULL,
    prev_speed_ms REAL,
    speed_limit_ms REAL,            -- v_prev + Δv_max; NULL on a track's first step
    heading_change_deg REAL,        -- NULL when a step is below two grid lengths
    h REAL NOT NULL,
    cost REAL NOT NULL,             -- u + w_d d + h
    gate TEXT NOT NULL,             -- PASS | OVERLAP | SPEED | SPEED_CHANGE | COST
    component_id INTEGER,
    cost_rank INTEGER,
    margin REAL,
    crossing_with_uid TEXT,
    outcome TEXT NOT NULL,          -- CONTINUE | RESUMED | LOST | CROSSING | REJECTED
    PRIMARY KEY (run_id, scan_id, prev_cell_uid, curr_cell_label)
);
CREATE INDEX IF NOT EXISTS idx_tracking_pairs_prev ON tracking_pairs (run_id, prev_cell_uid);

CREATE TABLE IF NOT EXISTS tracking_lineage (
    run_id TEXT NOT NULL,
    scan_id TEXT NOT NULL,
    scan_time TEXT NOT NULL,
    seq INTEGER NOT NULL,
    side TEXT NOT NULL,             -- curr orphan | prev unlinked cell
    cell_uid TEXT NOT NULL,
    cell_label INTEGER NOT NULL,
    hypothesis TEXT NOT NULL,       -- RESUME | SPLIT | INITIATION | MERGE | LATENT
    partner_uid TEXT,
    partner_label INTEGER,
    fraction REAL,
    cost REAL,
    threshold REAL,
    passed INTEGER NOT NULL,
    chosen INTEGER NOT NULL,
    reason TEXT,
    PRIMARY KEY (run_id, scan_id, seq)
);

CREATE TABLE IF NOT EXISTS tracking_identity (
    run_id TEXT NOT NULL,
    scan_id TEXT NOT NULL,
    scan_time TEXT NOT NULL,
    kind TEXT NOT NULL,             -- SPLIT | MERGE
    identity_uid TEXT NOT NULL,
    candidate_side TEXT NOT NULL,
    candidate_label INTEGER NOT NULL,
    score REAL NOT NULL,
    distance REAL NOT NULL,
    winner INTEGER NOT NULL,
    rule TEXT NOT NULL,             -- SCORE | NEAREST
    transferred INTEGER NOT NULL,
    PRIMARY KEY (run_id, scan_id, kind, identity_uid, candidate_side, candidate_label)
);

CREATE TABLE IF NOT EXISTS tracking_latent (
    run_id TEXT NOT NULL,
    scan_id TEXT NOT NULL,
    scan_time TEXT NOT NULL,
    cell_uid TEXT NOT NULL,
    last_label INTEGER NOT NULL,
    last_scan_id TEXT NOT NULL,
    age INTEGER NOT NULL,
    origin TEXT NOT NULL,           -- MERGED | TERMINATED
    merged_into_uid TEXT,
    status TEXT NOT NULL,           -- CREATED | CARRIED | RESUMED | EXPIRED
    footprint_px INTEGER NOT NULL,
    predicted_x REAL NOT NULL,
    predicted_y REAL NOT NULL,
    PRIMARY KEY (run_id, scan_id, cell_uid)
);

CREATE TABLE IF NOT EXISTS segmentation_frames (
    run_id TEXT NOT NULL,
    scan_id TEXT NOT NULL,
    scan_time TEXT NOT NULL,
    mask_px INTEGER NOT NULL,
    closed_mask_px INTEGER NOT NULL,
    n_components INTEGER NOT NULL,
    n_independent_seeds INTEGER NOT NULL,
    n_carried_claims INTEGER NOT NULL,
    n_carried_admitted INTEGER NOT NULL,
    n_basins INTEGER NOT NULL,
    n_dropped_small INTEGER NOT NULL,
    n_dropped_large INTEGER NOT NULL,
    n_cells INTEGER NOT NULL,
    n_size_exempt INTEGER,
    PRIMARY KEY (run_id, scan_id)
);

CREATE TABLE IF NOT EXISTS segmentation_seeds (
    run_id TEXT NOT NULL,
    scan_id TEXT NOT NULL,
    scan_time TEXT NOT NULL,
    seq INTEGER NOT NULL,
    marker_id INTEGER,
    origin TEXT NOT NULL,
    row INTEGER NOT NULL,
    col INTEGER NOT NULL,
    peak_value REAL,
    plateau_px INTEGER,
    component_id INTEGER,
    prior_label INTEGER,
    prior_age INTEGER,
    footprint_px INTEGER,
    clearance_px REAL,
    footprint_holds_seed INTEGER,
    component_claims INTEGER,
    component_seeds INTEGER,
    component_deficit INTEGER,
    decision TEXT NOT NULL,
    basin_px INTEGER,
    final_label INTEGER,
    PRIMARY KEY (run_id, scan_id, seq)
);

CREATE TABLE IF NOT EXISTS segmentation_components (
    run_id TEXT NOT NULL,
    scan_id TEXT NOT NULL,
    scan_time TEXT NOT NULL,
    component_id INTEGER NOT NULL,
    area_px INTEGER NOT NULL,
    field_max REAL NOT NULL,
    field_min REAL NOT NULL,
    n_seeds INTEGER NOT NULL,
    n_claims INTEGER NOT NULL,
    deficit INTEGER NOT NULL,
    n_admitted INTEGER NOT NULL,
    n_final_labels INTEGER NOT NULL,
    PRIMARY KEY (run_id, scan_id, component_id)
);
