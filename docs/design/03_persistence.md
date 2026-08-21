# Persistence Layer — the Data Store

**Location**: `src/adapt/persistence/`

---

## Responsibility

The persistence layer is the **data store**: the single source of truth for
every artifact and table the pipeline produces. It owns the on-disk layout,
the immutable object store, the catalogs that index it, and the frozen-schema
product tables. Consumers never touch it directly — they read through
`adapt.api.StoreClient` (see 05_consumer_api.md).

## What It Is Not Responsible For

- Computing anything — pure storage and retrieval
- Deciding what to store — the runtime composes; the store executes
- Exposing file paths to consumers — objects are resolved via the catalog only
- Science or business logic of any kind

---

## Layout

Created **only** by `adapt init` (`init_store`). Opening an uninitialized or
pre-store root fails loudly naming the fix; nothing is ever auto-created.

```
{root}/
├── registry.db                    ← runs, collections, module executions, events
├── logs/                          ← run log files (outside the data plane)
└── collections/
    └── {COLLECTION_ID}/           ← e.g. KILX; one instrument's data stream
        ├── catalog.db             ← artifacts, lineage, scans, table_schemas
        ├── products.db            ← all module tables + tracking + annotations
        └── objects/               ← immutable content-addressed files
            └── {artifact_id}{suffix}
```

Nothing else ever appears at any level; the integration gate asserts the
layout byte-for-byte. Legacy roots (`adapt_registry.db`) raise
`StoreError("obsolete pre-store layout …")`.

---

## Identity

- `scan_id = sha256(raw file bytes)[:16]` — minted once at the acquisition
  boundary, the join key across every table and artifact.
- `(run_id, scan_id)` is the composite key everywhere.
- Timestamps (`scan_time`, `scan_time_unix`) are **metadata** for ordering and
  display, never join keys.

## Components

### `store.py` — Store / Collection / init_store
`Store.open(root)` validates the root; `store.collection(id)` provisions
`catalog.db` + `products.db` + `objects/` from the checked-in schema SQL and
runs `validate_collection` (schema-fingerprint comparison, sha256).

### `store_registry.py` — StoreRegistry
Root-level `registry.db`; process-global cache (`get_instance` /
`close_all`). Tables: `collections` (id, source_kind, location),
`runs` (run_id, collection_id, status, config_json + config_hash,
pipeline_version, environment_json, scan counts), `run_modules`
(per-module aggregate execution rows, upserted), `run_events`
(warnings/errors). Continuation runs reload their config from
`runs.config_json` — config is data in the store, not a file beside it.

### `objects.py` — ObjectStore
Immutable content-addressed writes: `begin()` stages under
`objects/.staging/`, `commit(handle, meta, parents)` computes sha256,
`os.replace`s into place, registers the catalog row and lineage in one step.
Objects are never mutated or overwritten.

### `collection_catalog.py` — Catalog
`artifacts` (artifact_id, type, producer, run_id, scan_id, observation_time,
object_name, checksum, size), `artifact_lineage` (parent/child),
`scans` (PK (run_id, scan_id); status pending → complete recomputed on every
product link), `scan_products` (which required product each scan has),
`table_schemas` (frozen table declarations). A scan is **complete** when it
has all of `REQUIRED_SCAN_PRODUCTS = (gridded3d, segmentation2d, cell_stats)`;
auxiliary links (tracking, volume_stats, …) never complete a scan.

### `products.py` — SchemaLedger + TableWriter
Product tables freeze on first frame. `SchemaLedger` snapshots each
declaration into **both** `products.db` and `catalog.db` (`table_schemas`);
`validate_collection` cross-checks the fingerprints. After the freeze there is
no `ALTER` ever: novel columns are rejected; INTEGER↔REAL interchange is the
one dtype compatibility (pandas promotes int columns to float64 whenever a
NaN appears — SQLite affinity stores both losslessly).

**Temporal-column rule** (granularity derived from the primary key, recorded
in `table_schemas`):
- *scan-granular* (`scan_id` in PK): `run_id`/`scan_id`/`scan_time`/
  `scan_time_unix` are **stamped by the writer** from `PersistenceMeta`;
  module frames carrying identity columns are refused.
- *time-granular* (`valid_time` in PK): rows carry `valid_time` plus the basis
  `scan_id`.
- *run-granular*: rows carry per-row `scan_id` supplied by the module.

### `track_store.py` — TrackStore
Tracking's bespoke tables in `products.db` (`cells_by_scan`, `cell_events`,
`cell_tracks`), frozen through the same `SchemaLedger` (required constructor
argument). Row-oriented SQLite because tracking is queried per-uid, per-scan,
and by graph traversal.

### `output_router.py` — StoreOutputRouter
The one dispatch point from module outputs to the store:
`NetcdfArtifact` → object store (+ identity attrs stamped, lineage to the raw
volume — missing raw lineage raises), `ProductTableWrite` → `TableWriter`,
`TrackTablesWrite` → `TrackStore`; each write links `scan_products`.

### `execution_history.py` — StoreExecutionHistory
Aggregates per-module timings into `run_modules`, warnings/errors into
`run_events`, and finalizes the run row (completed/cancelled/failed + scan
counts).

### `scan_mask_reader.py`
Post-process input readers (minute masks, projection masks) that iterate the
catalog and open objects — the store-side seam post-process modules consume.

---

## Acquisition Gateway (`runtime/acquire.py`)

`StoreAcquirer` is the only entry for raw data: idempotent per `source_uri`,
reuses objects across runs, registers the scan for *this* run, and returns the
queue message `{artifact_id, scan_id, scan_time, queued_at}`. Sources and
downloaders never write into the store themselves.

---

## Lineage and Reproducibility

Every artifact records producer, run_id, checksum, and lineage edges; every
non-raw artifact traces to its raw volume. Every run records its full config
JSON, config hash, pipeline version, and environment. Given any product row
or object, the provenance chain back to raw bytes and configuration is a
catalog query.

---

## Read Path

```
RadarProcessor / PostProcessor (runtime)
    → StoreOutputRouter → ObjectStore + Catalog + TableWriter/TrackStore
    → StoreRegistry (run lifecycle)

StoreClient (api/) — consumers' only surface
    ├── catalog.db   (scans, artifacts, lineage) — lazy, one connection
    ├── products.db  (tables, tracking, annotations) — lazy, one connection
    ├── objects/     (rasters via xr.load_dataset: fully in-memory,
    │                 zero retained file handles)
    └── registry.db  (runs, progress)
```

FD discipline is a store-level guarantee: one lazy connection per database
per collection, `close()` releases everything, rasters never retain handles,
and `client.sql()` opens a per-call read-only connection (catalog ATTACHed).
