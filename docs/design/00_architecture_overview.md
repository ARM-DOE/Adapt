# Adapt — Architecture Overview

## Vision

Adapt is a live and historical data analysis system for geospatial sensor networks — radar, satellite, and beyond. It ingests raw instrument data, extracts physically meaningful features, tracks them through time, and delivers a structured, queryable record that powers both real-time dashboards and autonomous analysis pipelines.

The system is designed to be explored interactively in a graphical environment (web-based or local), composed programmatically for batch operations, and extended with new sensors or algorithms without modifying existing code.

---

## Design Philosophy

### One Responsibility Per Layer

The system is divided into layers that never reach across each other. Science lives in modules. Orchestration lives in runtime. Storage lives in persistence. Each layer knows only what it needs to fulfil its one role.

### Contracts Over Conventions

Every module declares what data it needs and what data it guarantees to produce. These are enforced at runtime by the execution graph. If a module produces malformed output, the pipeline stops immediately with a clear error — not silently downstream.

### Fail Loud, Fail Early

There are no silent defaults, no fallback values, no optional paths. A missing file, a bad configuration, or a violated contract raises an exception at the point of failure. This makes bugs visible and reproducible.

### Determinism by Construction

Scientific reproducibility is a first-class constraint. Every pipeline stage decomposes into independent, stateless operations. The same input always produces the same output. Cell identities are stable hashes, not incrementing counters.

---

## System Layers

```
┌────────────────────────────────────────────────────────────────┐
│  Consumer Layer                                                │
│  api/client.py   gui/dashboard.py   visualization/plotter.py  │
│  Read-only. No write access to repository.                     │
├────────────────────────────────────────────────────────────────┤
│  Runtime Layer                                                 │
│  runtime/orchestrator.py   runtime/processor.py               │
│  Coordinates threads and data flow. Contains no science.       │
├────────────────────────────────────────────────────────────────┤
│  Execution Layer                                               │
│  execution/graph/   execution/module_registry.py              │
│  Wires modules into a DAG, runs them in dependency order.      │
├────────────────────────────────────────────────────────────────┤
│  Module Layer                                                  │
│  modules/ingest/  detection/  projection/  analysis/          │
│  modules/tracking/  acquisition/                              │
│  Science and signal processing only. No I/O, no threading.    │
├────────────────────────────────────────────────────────────────┤
│  Persistence Layer                                             │
│  persistence/repository.py   catalog.py   registry.py         │
│  Write and read artifacts. No science, no orchestration.       │
├────────────────────────────────────────────────────────────────┤
│  Configuration Layer                                           │
│  configuration/schemas/   configuration/defaults.yaml         │
│  Resolves and validates settings. Produces one frozen config.  │
└────────────────────────────────────────────────────────────────┘
```

---

## Data Flow

```
AWS S3 (NEXRAD Level-II)
        │
        ▼
  [ Acquisition ]  ──── downloads raw files ──→  nexrad/ on disk
        │
        ▼  (filepath enqueued)
  [ Ingest ]       ──── reads Level-II, regrids ──→  grid_ds (3D xarray)
        │
        ▼
  [ Detection ]    ──── segments reflectivity ──→  segmented_ds (+ cell_labels)
        │
        ▼  (requires previous frame)
  [ Projection ]   ──── optical flow + forecast ──→  projected_ds (+ heading, projections)
        │
        ▼
  [ Analysis ]     ──── per-cell statistics ──→  cell_stats (DataFrame)
        │
        ▼
  [ Tracking ]     ──── links cells across scans ──→  tracked_cells, cell_events
        │
        ▼
  [ Persistence ]  ──── writes NetCDF, Parquet, SQLite ──→  repository on disk
        │
        ▼
  [ API / GUI ]    ──── reads from repository ──→  dashboard, notebooks, exports
```

---

## Module Communication: The Context Dictionary

Modules do not call each other. They do not import each other. They communicate exclusively through a shared **context dictionary** passed and returned through the execution graph.

Each module declares:
- `inputs: list[str]` — keys it reads from context before running
- `outputs: list[str]` — keys it writes into context after running

The graph executor resolves dependencies automatically from these declarations. If module B needs `segmented_ds` and module A produces `segmented_ds`, A runs before B — no explicit wiring required.

This design means:
- Adding a new module requires zero changes to existing code
- Replacing a module (e.g., a new detection algorithm) requires only re-registration
- The DAG is self-documenting: the full dependency graph is derivable from module declarations

---

## Dependency Rules (Enforced Architecturally)

| Layer | May import from |
|-------|----------------|
| `modules/*` | `contracts/` only |
| `execution/` | `contracts/` and `modules/base.py` only |
| `runtime/` | `execution/`, `persistence/`, `contracts/` |
| `persistence/` | stdlib, SQLite, Arrow only |
| `adapters/` | third-party libraries (only layer permitted) |
| `utils/` | stdlib only |
| `api/` | `persistence/` only |
| `visualization/`, `gui/` | `api/`, `persistence/` only |

No layer may import from a layer above it. No module may import from another module.

---

## Extension Points

### Adding a New Processing Module

1. Implement `BaseModule` in `modules/<new_domain>/module.py`
2. Declare `name`, `inputs`, `outputs`
3. Add contracts in `modules/<new_domain>/contracts.py`
4. Register in `configuration/defaults.yaml` under `modules`
5. Done. No existing code changes.

### Adding a New Data Source (e.g., Satellite)

1. Create `modules/acquisition_satellite/` with a downloader thread
2. Create `modules/ingest_satellite/` that reads the new format
3. Produce a `grid_ds` compatible with the existing contract
4. All downstream modules (detection, tracking) work without modification

### Adding a New Consumer (e.g., Web Dashboard)

1. Use `StoreClient` from `adapt.api`
2. Query scans, rasters, tracks, and any product table via typed filters or read-only SQL
3. No coupling to any module or runtime component

---

## SOLID Principles Applied

**Single Responsibility**: Each module does one thing. `IngestModule` regrids. `DetectionModule` segments. `TrackingModule` links. The orchestrator coordinates. The repository stores.

**Open/Closed**: New modules extend the system without modifying the graph executor, orchestrator, or any existing module. The registry pattern enables open extension with zero modification.

**Liskov Substitution**: All modules implement `BaseModule`. Any module can be swapped for another that satisfies the same `inputs`/`outputs` contract. The executor does not care which specific class runs.

**Interface Segregation**: Consumers use `StoreClient` — a read-only view. They are never exposed to the catalog databases, the object store, or internal persistence details (enforced by a fitness test).

**Dependency Inversion**: The execution graph depends on `BaseModule` (abstraction), not on `IngestModule` or `DetectionModule` (concretions). The registry resolves concretions at startup, not at design time.
