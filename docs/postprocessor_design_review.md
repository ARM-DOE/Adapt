# PostProcessor framework — design review

Separate deliverable accompanying the V1 PostProcessor framework. It records
architectural smells and repository/persistence/scalability concerns hit while
building the framework, with proposed improvements that can be reviewed and
scheduled independently. **No redesign was done mid-task** — these are
recommendations, not changes already applied (except where noted as "shipped").

## What shipped in V1 (framework only)
- `PostProcessor` (`src/adapt/runtime/postprocessor.py`) — composes the existing
  `registry`, `GraphBuilder`/`GraphExecutor`, `resolve_module_configs`, and
  `ModuleOutputWriter`. No inheritance from / edits to `RadarProcessor`.
- `adapt postprocess` CLI subcommand (`src/adapt/cli.py`).
- Core-vs-extension table distinction: `src/adapt/persistence/tables.py`
  (`CORE_TABLES`, `is_core_table`) + a guard in `ModuleOutputWriter.__init__`
  that refuses any core-table write (binds *all* module writers, live and post).
- `POSTPROCESS_PHASE` marker (`adapt.modules.base`) + `postprocess_defaults.yaml`
  discovery; post-process modules are isolated from the live pipeline by
  registration alone.

## What shipped next (LMA module)
With PyXLMA installed and its API verified, the first real post-process module is
now implemented:
- Science (pure, no third-party): `src/adapt/modules/lma/{attribution,aggregate,
  geo,module}.py` — vectorized initiation-point attribution (500 m nearest-cell
  via `scipy.ndimage` distance transform in metres, `UNATTRIBUTED` sentinel),
  1-minute binning, per-`(cell_uid, time_bin)` aggregation, and aeqd projection.
- PyXLMA boundary (single file): `src/adapt/modules/lma/reader.py` — global
  flash-first `cluster_flashes` + `flash_stats`; nothing else imports pyxlma.
- Node: `src/adapt/execution/nodes/lma.py` (`LMACellStatisticsModule`,
  `pipeline_phase = POSTPROCESS_PHASE`) writing two extension tables
  (`lma_cell_stats`, `lma_flash_attribution`).
- Framework additions driven by LMA (one-time, reusable): `BaseModule.output_tables`
  (multi-table modules) and PostProcessor injection of `scan_masks` /
  `radar_origin` read via `src/adapt/persistence/scan_mask_reader.py`.

### LMA decisions & limitations (documented, not faked)
- **Dependencies:** requires `pyxlma` and `scikit-learn` (pyXLMA's DBSCAN). The
  reader/clustering and integration tests `importorskip("sklearn")`; the reader
  raises loudly if pyxlma is absent. Install both in the environment to run LMA.
- **Every requested column maps to a real PyXLMA field** (`flash_area`,
  `flash_energy`, `flash_duration`, `flash_init_altitude`, `event_power`,
  `event_chi2`, `event_stations`, …); none are fabricated. `source_density_km2`
  is defined as `source_count / total_flash_area_km2` (NaN when area is 0).
- **Representative time = flash initiation time** (consistent with attribution by
  initiation point); used both for the 1-minute `time_bin` and for **mask
  selection: the scan whose time is closest to that initiation time**.
- **`input_dir` is optional at config-build time** (defaults to `None`) so config
  resolution never fails for an unselected module; the node raises loudly at
  `run()` if it is missing — deferred validation, not a silent default.
- **Primary keys:** `lma_cell_stats` PK is `(cell_uid, time_bin)` and
  `lma_flash_attribution` PK is `(flash_id,)`, per the brief. A `run_id` column is
  added to both for provenance. **Recommendation:** include `run_id` in both PKs —
  the per-radar `catalog.db` is shared across runs and `cell_uid`/`flash_id` are
  only unique within a run, so cross-run collisions are possible.
- **Pre-existing `ModuleOutputWriter` edge case:** a table whose only column is
  its primary key produces an invalid empty `ON CONFLICT … DO UPDATE SET`. Not
  hit by LMA (both tables have value columns) but worth fixing in the writer.

---

## Smells & recommendations

### 1. Core vs extension not modeled in the schema
`radar_catalog_schema.sql` has no notion of table *class*; module output tables
are created ad-hoc by `ModuleOutputWriter` and discovered only via
`module_schemas`. V1 adds an application-level `CORE_TABLES` guard, but the DB
itself can't tell core from extension.
**Proposal:** add a `table_class` column to `module_schemas` (`'extension'`) and,
longer term, a catalog view enumerating core vs extension tables. Keep
`CORE_TABLES` as the single source feeding both the guard and the view.

### 2. Persistence format divergence (SQLite vs Parquet)
Today every module output is a SQLite table (`ModuleOutputWriter`). The LMA brief
wants `lma_cell_stats.parquet` + a catalog entry. These conflict.
**Proposal:** make the extension-table *writer* pluggable, selected by
`OutputTableSpec` (e.g. `storage="sqlite" | "parquet"`). A Parquet writer would
reuse `DataRepository.write_*_parquet` + catalog registration. **Decision taken:**
LMA ships on SQLite via `ModuleOutputWriter` (consistent with every other module
output and the `module_schemas` discovery registry), *not* the brief's
`lma_cell_stats.parquet`. Parquet remains the recommended option; the framework
routes all persistence through one call site (`PostProcessor._persist`) so adding
a parquet-backed `OutputTableSpec` later is localized.

### 3. `GraphBuilder` silently ignores unproduced inputs
`GraphBuilder.build()` wires a dependency only when an input has a registered
producer; an input with no producer is silently left unsatisfied and only fails
later inside `GraphExecutor` (or worse, the module reads a missing key). For
post-process modules whose inputs are repository-injected (not produced by
another node), this is ambiguous.
**Proposal:** distinguish *external* inputs (satisfied from the initial context)
from *produced* inputs, and have the builder validate that every non-produced
input is either declared external or present in the base context — failing fast
with a clear message.

### 4. Repository read-injection coupling
Per importlinter, `modules/` cannot import `persistence`, so all repository reads
must happen in the processor and be injected (e.g. live processor's
`_build_enrich_context` reads `grid_ds_3d`). The PostProcessor will need the same
for LMA: per-scan `cell_labels`, the `cell_uid` LUT, grid coords, and scan times,
with a "closest scan to flash time" selection rule.
**Proposal:** extract a small read facade (e.g. `ScanMaskReader`) over
`DataRepository` that both processors can use, so artifact-selection logic
(closest-scan-to-time, label→uid LUT extraction) lives in one tested place
instead of being re-implemented per consumer.

### 5. `resolve_module_configs` builds configs for *all* registered modules
`PostProcessor._build_context` calls `resolve_module_configs(config)`, which
iterates the whole global registry (including live pipeline modules if they were
imported in-process). It's harmless today but wasteful and couples the
post-process context to unrelated modules.
**Proposal:** scope config resolution to the selected modules.

### 6. CLI repository discovery is thin and partly untested
`_open_repository` discovers the latest run via the root registry and reloads the
saved config through `init_runtime_config`'s continuation fast-path. The
end-to-end CLI test monkeypatches it (the discovery path itself is exercised only
manually in V1). Config-file `postprocess.modules` is not yet a typed field —
`--module` is required for now.
**Proposal:** add a typed `postprocess` config section (modules + per-module
params) and cover `_open_repository` with an integration test once LMA provides a
real end-to-end target.

---

## Scalability notes for the LMA follow-up
- **Attribution must be vectorized.** With millions of sources, do not loop in
  Python per flash. Compute the 500 m nearest-cell rule on the projected grid by
  dilating the label mask by `ceil(500/dx)` px (`scipy.ndimage.grey_dilation` of
  labels, or a nearest-labeled-pixel transform via
  `scipy.ndimage.distance_transform_edt(..., return_indices=True)`), then index
  the dilated label array at each flash initiation pixel. Pixels still unlabeled
  → `cell_uid = UNATTRIBUTED` (never dropped).
- **Single temporal unit:** floor flash time to the minute (`time_bin`); store the
  mean flash time in the bin at 1 s accuracy. No scan-based window.
- **One flash → one cell** (by initiation point) → one denominator: all of a
  flash's sources and flash-level scalars roll up to that single `cell_uid`.
- **Mask selection:** use the scan whose time is closest to the flash's
  representative time (document the exact tie-break implemented).
- Verify every output column against the installed PyXLMA's actual flash/source
  fields; omit and document any that don't exist rather than fabricating them.
