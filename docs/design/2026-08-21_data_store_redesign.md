# Decision record: the data store (registry + collections)

**Date**: 2026-08-21 (requirements settled 2026-08-20)
**Status**: Implemented and gated — commits `41b8230..d473c1e` on `wip`.
**Supersedes**: the pre-store persistence layer (`DataRepository`,
`RadarCatalog`, `RepositoryRegistry`, `RepositoryWriter`, `RepositoryClient`,
`FileProcessingTracker`) — all deleted, clean break, no compatibility code.
**Related**: [03_persistence.md](03_persistence.md) (as-built),
[05_consumer_api.md](05_consumer_api.md) (StoreClient),
[08_repository_evolution.md](08_repository_evolution.md) (the plan this
executes, phases 1–5).

---

## Context

The old layout was NEXRAD-shaped (date directories, filename conventions),
artifacts were discovered by globbing, schemas were inferred from DataFrames
with silent `ALTER TABLE`, per-file progress lived in a separate tracker that
could disagree with the catalog, and consumers opened files and databases
directly — the source of the dashboard's fd-exhaustion and identity-drift bug
classes. A requirements interview with the project owner settled the query
surface and store semantics before implementation.

## Decisions

1. **Layout**: `{root}/registry.db + logs/ + collections/{id}/{catalog.db,
   products.db, objects/}` — created **only** by `adapt init`. Opening an
   uninitialized or pre-store root fails loudly naming the fix. Nothing else
   ever appears at any level (asserted byte-for-byte by the integration gate).
2. **Identity**: `scan_id = sha256(raw bytes)[:16]`, `(run_id, scan_id)`
   composite keys everywhere. Timestamps are ordering/display metadata, never
   join keys.
3. **Query surface**: typed operator filters `{eq, gt, ge, lt, le, in}` on
   `table()`/`select()` **plus** `client.sql()` — arbitrary read-only SQL over
   products.db with the catalog ATTACHed. Rationale: scientists need full SQL
   generality (period counts, threshold AND-combinations, per-track series,
   split/merge graphs, duration filters) without a writable handle.
4. **Silver deferred entirely**: the live store is bronze; config hashes,
   checksums, and lineage are recorded so silver/gold can be materialised
   later. No `lake/` directories are created.
5. **Cross-run union**: `run_id` is an optional filter on listing reads; rows
   always carry their `run_id`; `latest_run()` and newest-first `runs()` are
   documented contracts. Dedup across reprocessed scans is the caller's call.
6. **Live-follow**: typed `scans_since(collection, run_id, after=ScanRef)`
   watermark replaces timestamp-window polling/streaming.
7. **Modules are uniform occupants**: acquisition, preprocessing, core, and
   post-processing modules are all the same kind of thing — they run in
   parallel once dependencies resolve, and may run **after the fact** into an
   existing store, attaching to the **original** `run_id` (upsert into
   `run_modules`, replace-own-output on re-run).
8. **Frozen first-frame schemas**: a table's schema freezes on its first
   non-empty frame; identical snapshots in products.db and catalog.db,
   validated by sha256 fingerprint. No `ALTER` ever; novel columns rejected;
   `INTEGER↔REAL` is the one dtype compatibility (see "What real data taught
   us" below).
9. **Temporal-column rule** (granularity derived from the primary key):
   scan-granular tables get `run_id/scan_id/scan_time/scan_time_unix` stamped
   by the writer — modules never pass identity; time-granular tables carry
   `valid_time` in the PK plus a basis `scan_id`; run-granular tables carry
   per-row `scan_id`.
10. **Completeness is the catalog's**: a scan is complete when
    `REQUIRED_SCAN_PRODUCTS` (gridded3d, segmentation2d, cell_stats) are all
    linked; auxiliary products never complete a scan. Consequence, by design:
    **the first scan of every run stays pending forever** (analysis needs a
    pair), so timelines show scans 2..N. There is no separate progress ledger.
11. **Acquisition gateway**: `StoreAcquirer` is the only entry for raw bytes —
    idempotent per `source_uri`, cross-run object reuse, queue contract
    `{artifact_id, scan_id, scan_time, queued_at}`.
12. **Annotations** are the one client write (products.db), keyed
    `(run_id, cell_uid, tag)`.
13. **Consumer boundary is enforced, not conventional**: an AST fitness test
    forbids consumers from importing sqlite3/duckdb, opening datasets/files,
    or naming store-internal paths. duckdb is removed from the project.
14. **FD discipline**: one lazy connection per database per collection;
    `open_scan_raster` returns a fully in-memory dataset (zero retained
    handles); movie/replay close one raster per frame; `sql()` opens a
    per-call read-only connection.

## What real data taught us (integration gate findings)

- `cell_stats` froze `cell_centroid_projection3_x` as INTEGER on the first
  frame; a later scan's unmatched cell produced NaN, pandas promoted the
  column to float64, and the writer refused the frame — a live-run killer that
  synthetic tests never hit. Hence decision 8's `INTEGER↔REAL` compatibility
  (SQLite affinity is lossless; NaN binds as NULL).
- pyart's `Xgrid` requires CF-encoded time on live `Grid.to_xarray()`
  datasets — ingest re-encodes (`_with_cf_time`) before gridding.
- `add_basemap` needed a **negative** cache: offline dashboards re-attempted
  the tile fetch on every 500 ms loop frame; failures are now cached per
  extent, retried on zoom.

## Verification

- 1274 unit tests; ruff/ruff-format/import-linter/mypy clean.
- `tests/integration/test_integration_store_pipeline.py` (CI-excluded) passes
  on real KILX Level-II volumes: exact layout, every object checksummed with
  lineage to its raw volume, completeness semantics, fd-stable raster loops,
  all dashboard workflows through the API, after-the-fact xlma postprocess,
  loud legacy-root failure.
- Headless dashboard verification on a real 5-volume store: fd count flat
  across 150 hot iterations and back to baseline on close; completes under
  `ulimit -n 64`; no segfault; concurrent clients clean; movies close exactly
  one raster per frame.

## Known gaps / deferred

- `xlma_stat_scan` / `xlma_stat_minutes` carry **no `scan_id`** (keyed on
  `scan_time` strings) — the one table family violating decision 2. The ISO
  `scan_time` string join works exactly; re-keying + `time → valid_time`
  rename is the top follow-up.
- 08 phases 6–10 (commit ledger with reconciliation, experiments/variants/
  derivation reuse, checkpoints, silver/gold materialisation) are not started.
- TSE default gate rules select nothing on weak storms — a defaults-vs-data
  observation, not a code defect (documented in the dashboard reference).
