# Ingest Module

**Location**: `src/adapt/modules/ingest/module.py`

---

## Responsibility

The ingest module is the **format translation boundary**. It takes a raw NEXRAD Level-II file — a polar, range-gated radar format — and produces a Cartesian grid dataset in the system's canonical internal representation.

After ingest, no other module ever sees a raw radar file, polar coordinates, or vendor-specific field names. The rest of the pipeline works exclusively with the normalized `grid_ds`.

---

## What It Is Not Responsible For

- Downloading or locating files — that is the acquisition module's job
- Detecting or analysing cells — that is the detection and analysis modules' jobs
- Choosing which variables to compute statistics on — that is the analysis module's job
- Writing the grid to disk — that is the persistence layer's job (called by runtime after ingest)
- Any computation that requires two frames — that is the projection module's job

---

## Interface

```
IngestModule (BaseModule)
├── name:    "ingest"
├── inputs:  ["nexrad_file", "config"]
└── outputs: ["grid_ds", "grid_ds_2d", "scan_time"]
```

### Input: `nexrad_file`

A `Path` object pointing to a downloaded NEXRAD Level-II file (`.gz` or uncompressed). The file must exist and be readable. If it does not exist, ingest raises immediately.

### Input: `config`

The resolved `InternalConfig`. Ingest reads:
- `config.regridder.*` — grid shape, spatial extent, interpolation parameters
- `config.global_.var_names` — canonical field name mappings

### Output: `grid_ds`

A 3-dimensional `xarray.Dataset` on a regular Cartesian grid `(z, y, x)`. All coordinates are in metres from the radar origin. Reflectivity and velocity fields are renamed to canonical names defined in `config.global_.var_names`.

```
grid_ds
├── dims: (z, y, x)
├── coords: x (m), y (m), z (m)
└── data_vars:
    ├── reflectivity    # float32, dBZ
    ├── velocity        # float32, m/s (if available)
    └── ...             # other radar moments, canonically named
```

### Output: `grid_ds_2d`

A 2-dimensional slice of `grid_ds` at the configured analysis altitude level. This is what detection, projection, and analysis operate on.

```
grid_ds_2d
├── dims: (y, x)
├── coords: x (m), y (m)
└── data_vars: same as grid_ds, at fixed z
```

### Output: `scan_time`

A `datetime` object representing the valid time of the scan, extracted from the file metadata.

---

## Internal Processing Steps

```
raw Level-II file
    │
    ▼ Py-ART reader (via adapters/)
Polar Radar object
    │
    ▼ Cressman regridder (via adapters/)
3D Cartesian grid
    │
    ▼ Field renaming (via config.global_.var_names)
Canonical grid_ds
    │
    ▼ z-slice at config.regridder.analysis_level
grid_ds_2d
```

The Py-ART and Cressman dependencies are accessed exclusively through `adapters/`. No module imports a third-party library directly.

---

## Output Contract (`ingest/contracts.py`)

`assert_gridded(grid_ds)` verifies:
- `x` and `y` coordinates exist with consistent shapes
- The canonical reflectivity variable is present
- `grid_ds_2d` has exactly 2 dimensions

A `ContractViolation` is raised immediately on failure, before any downstream module runs.

---

## Connection to the Rest of the System

```
nexrad_file (from acquisition)
        ↓
  [ IngestModule ]
        ↓
grid_ds ─────────────────→ (stored to disk by persistence)
grid_ds_2d ──────────────→ [ DetectionModule ]
                           [ ProjectionModule ]
                           [ AnalysisModule ]
scan_time ───────────────→ [ AnalysisModule ]
                           [ TrackingModule ]
                           [ persistence layer ]
```

---

## Design Notes

**Why does ingest own the coordinate system?**

The coordinate system (Cartesian, metres from radar origin) is chosen once at the ingest boundary. Every subsequent module — detection, projection, analysis, tracking — operates in this same space without needing to know anything about the original polar format, grid spacing, or projection. Coordinates travel with the data in the `xarray.Dataset` attributes and do not need to be passed as function arguments.

**Why Cressman regridding?**

The Cressman interpolation scheme is physically motivated (distance-weighted radius of influence) and produces well-behaved outputs near the radar origin where range gates are dense. This is a scientific default. Alternative regridding schemes can be supported by adding a new module that produces a `grid_ds`-compatible output and registering it in place of this one.
