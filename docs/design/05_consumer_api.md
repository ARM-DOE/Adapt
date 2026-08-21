# Consumer API — StoreClient

**Location**: `src/adapt/api/store_client.py`

---

## Responsibility

`StoreClient` is the **only** read surface for downstream users of pipeline
data — the dashboard, notebooks, the Target Selection Engine, and scripts.
It answers every consumer question from the store (see 03_persistence.md)
without exposing paths, connections, or schema internals. A fitness test
(`tests/test_architecture.py::test_consumers_read_only_through_the_store_api`)
enforces that no consumer opens files, globs directories, or connects to a
database itself.

## What It Is Not Responsible For

- Producing or computing data — it reads what the pipeline wrote
- Constructing file paths — objects are resolved through the catalog
- Writing — with one deliberate exception: `annotate()` (human notes)

---

## Interface

```python
from adapt.api import StoreClient

client = StoreClient("/path/to/store")           # a root made by `adapt init`

# Discovery
client.collections()                              # Collection dataclasses
client.runs("KILX")                               # newest first
client.latest_run("KILX")                         # the dashboard default
client.pipeline_progress("KILX")                  # status + complete-scan count

# Scans (identity-keyed; complete scans only)
client.scans("KILX", run_id=..., start=..., end=..., limit=...)
client.scan_timeline("KILX", run_id)              # ordered ScanRefs (the dashboard spine)
client.scans_since("KILX", run_id, after=ref)     # typed live-follow watermark

# Rasters — fully loaded in memory, zero retained file handles
with client.open_scan_raster("KILX", run_id, scan_id) as raster:
    raster.dataset                                # xr.Dataset with identity attrs

# Tracking
client.cells(run_id, "KILX")                      # cells_by_scan frame
client.cells_at_scan(run_id, scan_id, "KILX")
client.track_history(run_id, cell_uid, "KILX")    # per-scan time series
client.tracks("KILX", run_id=None)                # run_id=None unions across runs
client.track_graph(run_id, cell_uid, "KILX")      # split/merge connected component

# Any module's product table, with validated operator filters
client.tables("KILX")                             # frozen declarations
client.table("cell_stats", "KILX", run_id=run,
             filters={"cell_area_sqkm": {"op": "ge", "value": 50.0},
                      "scan_id": ["ab12...", "cd34..."]})

# The escape hatch: arbitrary read-only SQL (catalog ATTACHed as `catalog`)
client.sql("SELECT COUNT(DISTINCT cell_uid) FROM cells_by_scan "
           "WHERE scan_time BETWEEN ? AND ?", "KILX")

# Annotations — the one write
client.annotate(...); client.annotations(...)

client.close()                                    # releases every connection
```

Cross-run queries (`run_id=None`) always return rows carrying their `run_id`;
deduplication across reprocessed scans is the caller's decision.

---

## Design decisions

**Typed filters + read-only SQL, not an ORM.** `table()` validates every
filter column against the frozen schema and supports
`{eq, gt, ge, lt, le, in}`; anything more expressive goes through `sql()`,
which opens a per-call read-only connection (writes are rejected by SQLite
itself) with the catalog attached — full SELECT generality for scientists
without giving consumers a writable handle.

**Scan identity, never timestamps.** Every scan access keys on
`(run_id, scan_id)`; `scan_time` orders and labels. `scans_since` replaces
timestamp-window polling with a typed watermark.

**Complete-scans-only.** `scans()`/`scan_timeline()` return scans whose
required products all exist, so a consumer never observes a half-written scan.

**FD discipline.** One lazy connection per database per collection;
`open_scan_raster` uses `xr.load_dataset` so the returned `ScanRaster` holds
no file handle; movie/replay code closes each raster per frame. This is what
keeps long-running dashboards free of "too many open files".

---

## Connection to the Rest of the System

```
StoreClient (api/)
    ├── registry.db  → runs, progress
    ├── catalog.db   → scans, artifacts, lineage   (per collection)
    ├── products.db  → module tables, tracking, annotations
    └── objects/     → rasters, resolved via catalog rows only
```

`StoreClient` imports only from `adapt.persistence` errors/paths and the
standard scientific stack; never from `modules/`, `execution/`, or `runtime/`.

## Consumer Use Cases

- **Dashboard** (`consumers/live/`): `AppContext` wraps one cached
  `StoreClient`; the Latest Scan tab renders `scan_timeline` + rasters, the
  TSE tab replays snapshots, Save Movie streams frames with per-frame closes.
- **Target Selection** (`consumers/target_selection/`): `build_snapshot`
  reads cells/tracks for scoring.
- **Notebooks/scripts**: `table()` + `sql()` for ad-hoc science queries.
