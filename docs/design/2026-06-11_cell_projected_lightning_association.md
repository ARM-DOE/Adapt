# Cell-Projected Lightning Association — Design & Implementation Record (2026-06-11)

Status: **implemented** (this document records the agreed design and what shipped).

## Part 1 — Current architecture assessment

Assessed in the latest dated documents rather than re-derived here:

- `docs/postprocessor_design_review.md` (2026-06-11) — postprocess framework smells 1–6;
  smell #4 (repository read-injection coupling / read facade) is the direct ancestor of
  `read_minute_masks`.
- `docs/design/strategy/08_architecture_smells.md` — S1 (hardcoded output routing),
  S4 (context dict unowned), S5 (orchestration leaking into module declarations).
- `docs/code_review_2026_04.md` (2026-04-12) — cross-module contract imports; modules
  persisting their own outputs.

Code facts that shaped the design:

- `RadarCellProjector` computes Farneback optical flow in **pixels per scan interval**;
  `cell_projections[frame_offset=0]` is the registration (previous scan's labels advected
  to the current scan); real Δt was never used inside projection.
- Pipeline order is detection → projection → analysis → tracking; `cell_uid` exists only
  after tracking, attached to the analysis NetCDF as a LUT by the processor.
- `cell_tracks` had lifecycle fields but no duration column; no per-cell velocity exists.

## Part 2 — Design smells fixed

1. **Geometry decisions in a science module.** The first lightning module picked the
   nearest scan mask per flash — a time→geometry mapping made by lightning code. Radar
   scans are 5–10 min apart, lightning is 1-minute data; deciding where a cell *was* at
   19:03 is projection's job.
2. **Scan-interval frames pretending to be time.** `frame_offset` is unitless; nothing
   downstream could ask "where was this cell at 19:03?".
3. **Non-self-contained artifacts.** The analysis NetCDF carried only the current-scan
   label→uid LUT, while its registration masks use the *previous* scan's labels.
4. **Per-consumer re-implementation risk.** Every future external-association module
   (satellite, hail, soundings, NWP) would re-invent time→cell-geometry matching.

## Part 3 — Architecture (as shipped)

```
detection ──► projection ──► analysis ──► tracking ──► persistence (analysis NC + catalog)
                 │  owns: flow, registration (= cell_projections[0]),         ▲
                 │  registration_minutes (1-min interpolated masks)           │ uid LUTs attached
                 ▼                                                            │ post-tracking
        analysis NetCDF per scan  ◄───────────────────────────────────────────┘
                 │
                 ▼  (run completes — post-processing never runs inside the pipeline)
   adapt postprocess --module xlma_stat
                 │   PostProcessor injects minute_masks via
                 │   persistence read facade: read_minute_masks(repository)
                 ▼
   xlma_stat: lightning science only
     • exact minute-bin → minute-mask match (no nearest-scan logic, no projection)
     • outputs: xlma_stat_minutes (association), xlma_stat_scan (aggregation)
```

Key decisions (user-confirmed):

- **Registration interpolation, not forward extrapolation.** For scans 19:00 → 19:07 the
  minute masks 19:01…19:07 are the 19:00 labels advected by `flow × fraction`
  (fraction = minutes elapsed / Δt). Minute 19:00 itself is the real segmentation
  (fraction 0). Every minute between the first and last scan is produced exactly once —
  no overlap, no tie-breaking. Forward nowcast minutes (`projection_horizon_minutes`)
  remain a future projection-module extension.
- **No new `registration` variable** — `cell_projections[frame_offset=0]` already *is*
  the registration and stays the single source.
- **One post-process module** (`xlma_stat`). Post-process modules run only after the
  entire pipeline completes, as their own command; they never run per scan.
- **The analysis NetCDFs are the first-class geometry product** — no separate
  cell_projection dataset, no postproc geometry module.
- **Replace everything:** `lma_cell_stats` and `lma_flash_attribution` are gone; the new
  tables are named after the module.

## Part 4 — Dataset schemas

### Analysis NetCDF additions (per scan pair)

| Variable | Dims | Type | Notes |
|---|---|---|---|
| `registration_minutes` | (minute, y, x) | int32 | previous scan's labels at each whole minute in (t_prev, t_curr]; epoch-aligned grid, step `projector.registration_step_minutes` (default 1) |
| `minute` (coord) | (minute,) | datetime64 | absolute minute times |
| `interpolation_fraction` (coord) | (minute,) | float32 | minutes elapsed / Δt, in (0, 1] |
| `registration_cell_uid` | (registration_cell_label,) | str | previous-scan label → cell_uid LUT (absent for the first scan pair of a run — its previous scan was never tracked; those minutes are skipped by the reader) |
| attrs | | | `registration_source_scan_time`, `registration_target_scan_time` |

### `xlma_stat_minutes` (SQLite extension table; PK `(run_id, time, cell_uid)`)

`run_id, cell_uid, time` (minute, canonical scan-time format), provenance
(`source_scan_time, target_scan_time, interpolation_fraction`), and lightning stats:
`flash_count, lightning_source_count, flash_rate_per_min, mean_flash_time,
mean/max/total_flash_area_km2, mean_flash_init_alt_m, median/p95/max_source_alt_m,
mean/total_flash_energy, mean/max_source_power_dbw, mean/max_flash_duration_s,
mean_source_chi2, mean_station_count, source_density_km2,
mean/max_attribution_dist_m`. UNATTRIBUTED rows are kept (sentinel cell_uid); flashes
outside the run's minute coverage are excluded and logged.

### `xlma_stat_scan` (SQLite extension table; PK `(run_id, scan_time, cell_uid)`)

Derived **only** from `xlma_stat_minutes` (association happens once). A minute belongs to
the scan its mask was advected toward (`target_scan_time`). Columns: `n_minutes` (masks
the scan owns), `n_lightning_minutes`, `flash_count, lightning_source_count,
mean/max_flash_rate_per_min, first/last_lightning_minute, total_flash_energy,
max_source_alt_m, max_flash_area_km2, mean_interpolation_fraction` (+ auto
`scan_time_unix`).

### `cell_tracks` addition

`duration_seconds REAL NOT NULL DEFAULT 0` = `last_seen_time − first_seen_time`,
maintained on every scan update; final/total once the track terminates. Per-cell velocity
was deliberately **not** added (derivable from consecutive centroids; add when a consumer
needs it).

## Part 5 — Module changes shipped

| Component | Change |
|---|---|
| `modules/projection/module.py` | fractional advection (`_project_fractions`); emits `registration_minutes` with time coords; uses the frame pair's real Δt |
| `configuration` (param/user/internal + projection config) | `projector.registration_step_minutes: int = 1` |
| `contracts/projection.py` | `check_projected_ds` requires `registration_minutes` + valid fractions/monotone minutes whenever projections exist |
| `runtime/processor.py` | attaches `registration_cell_uid` from the previous scan's tracked cells (`_last_tracked_cells` state) |
| `persistence/scan_mask_reader.py` | `read_minute_masks()` facade (one record per covered minute; real segmentation wins at scan minutes); `read_scan_masks` deleted (no callers) |
| `runtime/postprocessor.py` | on-demand `minute_masks` injection |
| `modules/lma/` → `modules/xlma_stat/` | renamed; science rewritten: exact minute-bin matching, no time→geometry logic; `XlmaStatConfig` |
| `execution/nodes/xlma_stat.py` | `XlmaStatModule`, two run-keyed output tables, fails loudly when minute masks are missing ("reprocess the run") |
| `contracts/xlma_stat.py` | `check_xlma_stat_minutes`, `check_xlma_stat_scan` |
| `persistence/track_store.py` + `radar_catalog_schema.sql` | `cell_tracks.duration_seconds`; `get_track_lightning` reads `xlma_stat_minutes` |
| consumers/api | dashboard + `merge_lightning` + `RepositoryClient.track_lightning` read the new table (`time` column) |

## Part 6 — Migration order (executed, TDD red→green per step)

1. Projection fractional advection + `registration_minutes` (analytic uniform-flow tests).
2. Projection contract extension.
3. Processor `registration_cell_uid` attachment.
4. `read_minute_masks` persistence facade.
5. `cell_tracks.duration_seconds`.
6. xlma_stat rewrite + rename + e2e (moving-cell fixture: the flash sits at the cell's
   mid-gap projected position where **both** real scan masks are out of range — only the
   minute mask attributes it; idempotent rerun; core tables untouched).
7. Cleanup: defaults, CLI text, consumer read paths, docs.

## Part 7 — Testing strategy (implemented)

- **Unit (analytic):** uniform flow u px/scan over Δt min ⇒ minute k displaces k·u/Δt px;
  minute coverage exactly (t_prev, t_curr]; epoch-aligned step grid; fraction-1 frame ≡
  `cell_projections[0]`; duration arithmetic; scan totals ≡ Σ member minutes.
- **Contract tests:** accept/reject pairs for `check_xlma_stat_*` and the extended
  projection contract (required columns pinned in the tests, not imported).
- **E2E:** `PostProcessor.run(["xlma_stat"])` over a synthetic moving-cell repo
  (`tests/runtime/test_postprocess_xlma_e2e.py`).
- **Architecture fitness** (`tests/test_architecture.py`): heavy-dep containment pins
  `cv2` to `modules/projection/` and `pyxlma/sklearn/pyproj` to `modules/xlma_stat/`
  (+consumers for pyproj); module-independence auto-discovery forbids
  `modules/xlma_stat` importing `modules/projection`. A contributor cannot move
  projection logic into a science module without failing CI.
- **Determinism:** projection (incl. registration_minutes via `assert_identical`) and
  xlma_stat run-twice-identical tests under the `determinism` marker.

## Part 8 — Future-proofing: the external-association module pattern

The reusable asset is the pair **minute-resolution geometry product +
`read_minute_masks` facade**. Any future external-observation association module
(satellite, hail reports, surface networks, soundings, NWP) follows the same recipe:

1. Declare `inputs = ["<name>_config", "minute_masks", "radar_origin", "run_id"]`
   (PostProcessor injects the geometry; the module never reads the repository).
2. Associate observations to `cell_uid` by exact minute against `minute_masks`.
3. Write a `(run_id, time, cell_uid)` extension table (association) and derive a
   `(run_id, scan_time, cell_uid)` aggregate from it — association happens once.
4. Register the node in `postprocess_defaults.yaml`; run via
   `adapt postprocess --module <name>` after the pipeline completes.

Named future extensions with established owners: forward nowcast minutes
(`projection_horizon_minutes` — projection module), per-cell velocity columns
(tracking), entry-point packaging of external modules (strategy doc 02).

## Usage

```
adapt run-nexrad <config> --radar KHTX --mode historical ...   # pipeline completes
adapt postprocess --repository <repo> --module xlma_stat \
    --input-dir "<dir of xLMA flash-sorted .nc>"
```

Runs processed before this change have no `registration_minutes`; xlma_stat fails loudly
asking for a reprocess. pyxlma is not on conda and is not a dependency — convert raw
LYLOUT ASCII to flash-sorted NetCDF in xLMA first.
