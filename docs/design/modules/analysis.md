# Analysis Module

**Location**: `src/adapt/modules/analysis/module.py`

---

## Responsibility

The analysis module is the **feature measurement boundary**. For every cell identified in a scan, it computes a complete physical description: geometry, radar statistics, motion vectors, and projected future positions.

It transforms a label field (an integer mask) into a structured tabular record — one row per cell — that is the primary output of the processing pipeline.

---

## What It Is Not Responsible For

- Detecting or segmenting cells — that is the detection module's job
- Computing optical flow or projections — that is the projection module's job
- Linking cells across scans — that is the tracking module's job
- Deciding how analysis results are stored — that is the persistence layer's job
- Computing aggregate statistics across scans — that is the consumer's job (via StoreClient)

---

## Interface

```
AnalysisModule (BaseModule)
├── name:    "analysis"
├── inputs:  ["segmented_ds", "projected_ds", "grid_ds", "scan_time", "config"]
└── outputs: ["cell_stats"]
```

### Input: `segmented_ds`

The labelled field from detection. Analysis iterates over each unique non-zero label to compute per-cell measurements.

### Input: `projected_ds`

The motion and projection fields from projection. Analysis uses the per-pixel heading vectors and the per-step projected label arrays to compute projected centroid positions.

### Input: `grid_ds`

The full 3D grid from ingest. Analysis reads radar moments (reflectivity, velocity, spectrum width, etc.) that may not be present in the 2D slice.

### Input: `scan_time`

A `datetime` used to timestamp each row in the output DataFrame.

### Input: `config`

Analysis reads:
- `config.analyzer.radar_variables` — which radar fields to compute statistics on
- `config.analyzer.exclude_fields` — fields to skip
- `config.analyzer.adjacency_threshold` — minimum overlap fraction to record adjacency

### Output: `cell_stats`

A `pd.DataFrame` with one row per detected cell. Every row is self-contained — all information needed to understand, visualise, or query a cell is present in that row, with no foreign keys into other tables (within this scan).

```
cell_stats columns:

# Identity and time
cell_label          int      — label in segmented_ds (1..N, current scan only)
time                datetime — scan valid time

# Geometry
cell_area_sqkm      float    — cell area in km²
cell_centroid_geom_x float   — geometric centroid (grid pixels, x)
cell_centroid_geom_y float   — geometric centroid (grid pixels, y)
cell_centroid_mass_x float   — mass-weighted centroid (grid pixels, x)
cell_centroid_mass_y float   — mass-weighted centroid (grid pixels, y)
cell_centroid_max_x  float   — position of maximum reflectivity (grid pixels, x)
cell_centroid_max_y  float   — position of maximum reflectivity (grid pixels, y)
cell_centroid_mass_lat float — mass-weighted centroid latitude
cell_centroid_mass_lon float — mass-weighted centroid longitude

# Projected centroids (one pair per projection step)
cell_centroid_proj_0_x float — registered position (step 0)
cell_centroid_proj_0_y float
cell_centroid_proj_1_x float — one-step projection
cell_centroid_proj_1_y float
...
cell_centroid_proj_N_x float
cell_centroid_proj_N_y float

# Radar statistics (per configured variable)
radar_<var>_mean    float    — mean within cell
radar_<var>_std     float    — standard deviation
radar_<var>_min     float    — minimum
radar_<var>_max     float    — maximum
radar_<var>_median  float    — median
area_40dbz_km2      float    — area with reflectivity > 40 dBZ (or configured threshold)

# Motion (from optical flow within cell region)
heading_x           float    — mean u-component of motion (pixels/frame)
heading_y           float    — mean v-component of motion (pixels/frame)
```

---

## Per-Cell Computation Pattern

For each label `L` in `segmented_ds.cell_labels`:

1. Create binary mask `mask = (cell_labels == L)`
2. Extract reflectivity values under mask
3. Compute geometric centroid: `(mean_y, mean_x)` of mask pixels
4. Compute mass-weighted centroid: `(reflectivity-weighted mean_y, mean_x)`
5. Compute max-reflectivity position
6. Convert pixel centroids to lat/lon via grid coordinate arrays
7. For each projection step `k`: apply mask to `cell_projections[k]`, compute centroid of matching pixels
8. For each configured radar variable: compute statistics under mask
9. Extract heading vectors under mask, compute spatial mean

This pattern is stateless and embarrassingly parallel across cells.

---

## Output Contract (`analysis/contracts.py`)

`assert_analysis_output(cell_stats)` verifies:
- Required columns are present
- `cell_label` values are all > 0 (background excluded)
- `time` column is populated
- No required column is entirely NaN

---

## Connection to the Rest of the System

```
segmented_ds (from detection)
projected_ds (from projection)
grid_ds (from ingest)
scan_time (from ingest)
        ↓
  [ AnalysisModule ]
        ↓
cell_stats ─────────────→ [ TrackingModule ]
                           (tracking reads centroids, area, motion)
           ─────────────→ (stored as Parquet by persistence)
           ─────────────→ (queryable via StoreClient)
           ─────────────→ (displayed in GUI/dashboard)
```

---

## Design Notes

**Why is cell_stats a flat DataFrame rather than a nested structure?**

Flat tabular data is directly queryable via SQL (DuckDB, SQLite), serialisable to Parquet without loss, and compatible with every analysis tool in the scientific Python ecosystem. Nesting would require a custom serialisation format and make ad-hoc querying significantly harder.

**Why are projected centroids included here rather than computed on demand?**

Projected centroids are used immediately by the tracking module to assess cell overlap across the motion estimate. Deferring this computation would require either passing the full projected label arrays to tracking (expensive) or re-running projection (wasteful). Computing them once in analysis and storing them in the flat record is the simpler design.

**Why does analysis read from grid_ds (3D) and not just grid_ds_2d?**

Radar variables like velocity and spectrum width may be present in the full 3D volume but absent from the 2D reflectivity slice used for detection. Analysis needs the full observation to compute complete radar statistics. The 3D dataset is always available in context from ingest.
