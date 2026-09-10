# Target Selection Engine — Design & Implementation Record (2026-07-04)

Status: **implemented** (v0.1)

The Target Selection Engine (TSE) selects which tracked cell should be observed,
based on configurable science rules. It does **not** control the radar and holds
no radar-specific knowledge. It is a Ring 2 consumer: it reads exclusively
through the public `RepositoryClient` API, never writes into the Adapt
repository, and is removable by deleting `src/adapt/consumers/target_selection/`.

## Part 1 — Architecture

```
              Ring 0/1 (pipeline, persistence)              Ring 2 (this package)
  catalog.db ──► RepositoryClient (adapt.api) ──► repository_source.build_snapshot()
                                                       │  ONLY file importing adapt.api;
                                                       │  pandas → frozen records, ONCE
                                                       ▼
                                                 Snapshot (frozen)
                                                       │
  TSEConfig (YAML, frozen pydantic) ──► TargetSelectionEngine.select(snapshot)
                                                       │   pure rule functions (rules.py)
                                                       ▼
                                            TargetSelection | None
                                                       │
                                         writer.append_selection()   optional JSONL,
                                                                      OUTSIDE the repo
```

Design invariants:

- **Boundary conversion happens once.** `repository_source.py` is the only
  module that imports `adapt.api` or sees pandas. The engine consumes only
  frozen dataclasses.
- **Read-only consumer.** Selections are appended to a user-configured JSONL
  file outside the repository; `catalog.db` is never written. Enforced by
  import-linter contract 9 (`consumers_use_public_api_only`).
- **Determinism.** `selection_time` always equals the snapshot's `scan_time` —
  the wall clock is never read. Score ties break to the lexicographically
  smallest `cell_uid`. Identical snapshot sequences produce identical selections.
- **Fail loud.** Missing/unknown YAML keys, a missing quality column, and an
  empty run all raise. The only defined-absence semantics: NaN quality → not a
  candidate; fewer than two scans → growth rate 0.0.
- **Minimal state.** The engine's only state is `CurrentTarget(cell_uid,
  started_at)` — the minimum needed for the continuation and stop rules.

## Part 2 — Data model

```python
# snapshot.py (all @dataclass(frozen=True))
TrajectoryPoint(lat, lon, lead_seconds)
CellSnapshot(uid, lat, lon, area_sqkm, reflectivity_max, age_seconds,
             growth_rate_sqkm_per_min, trajectory: tuple[TrajectoryPoint, ...],
             values: Mapping[str, float])   # every numeric column of the merged
                                            # cells_by_scan + cell_tracks row
Snapshot(scan_time: datetime, cells: tuple[CellSnapshot, ...])

# selection.py
SelectionReason: NEW_TARGET | CONTINUATION | SWITCH
TargetSelection(cell_uid, reason, score, selection_time, trajectory,
                observation_window: tuple[datetime, datetime],
                predicted_hulls=None)       # always None in v0.1 — extension point

# engine.py
CurrentTarget(cell_uid, started_at)         # the engine's ONLY state
```

## Part 3 — Public API

```python
from adapt.consumers.target_selection import (
    TargetSelectionEngine, load_config, build_snapshot, append_selection)

cfg = load_config("tse.yaml")
engine = TargetSelectionEngine(cfg)
snap = build_snapshot(client, run_id, radar,
                      growth_window_scans=cfg.snapshot.growth_window_scans,
                      at=None)  # at=<datetime> replays the run as of that instant
selection = engine.select(snap)             # -> TargetSelection | None
if selection and cfg.output.jsonl_path:
    append_selection(cfg.output.jsonl_path, selection)
```

The rules themselves are pure functions in `rules.py` (`is_candidate`,
`priority_score`, `site_bonus`, `total_score`, `select_best`,
`continue_or_switch`, `should_stop`) — individually testable, no state.

## Part 4 — YAML schema

```yaml
candidate:
  gates:                              # ALL gates must pass; any numeric column
    - {field: n_scans, op: ge, value: 3}           # op ∈ {gt, ge, lt, le}
    - {field: radar_reflectivity_max, op: gt, value: 45}
  min_age_seconds: 600
priority:
  weights:                # raw weighted sum; weights carry units:
    reflectivity: 1.0     #   points per dBZ  (radar_reflectivity_max)
    area: 0.05            #   points per km²  (cell_area_sqkm)
    growth_rate: 2.0      #   points per km²/min (derived, OLS slope)
site_preference:
  projection_steps: 5     # use first N trajectory points
  sites:
    - {name: sgp_c1, lat: 36.607, lon: -97.488, radius_km: 20.0, bonus: 10.0}
selection:
  switch_margin: 5.0            # score units; strict: challenger > current + margin
  max_observation_seconds: 1800
snapshot:
  growth_window_scans: 4        # OLS slope over up to K most recent scans (>= 2)
output:                         # the only OPTIONAL section
  jsonl_path: /data/tse/selections.jsonl
```

All models are `ConfigDict(frozen=True, extra="forbid")` — a missing or unknown
key (including an unknown weight name) raises at load time.

**Why raw weighted sums instead of normalized scores?** A cell's score then
depends only on the cell itself plus fixed site geometry, so it is stable when
other cells appear or disappear — exactly the stability `switch_margin` exists
to protect. Per-snapshot normalization would make the current target's score
population-relative and reintroduce oscillation. The cost — weights carry
physical units — is documented in the schema above.

## Part 5 — Rule semantics

- **Candidate**: present in the snapshot (snapshots hold only latest-scan cells,
  so "active" is structural) AND every configured gate passes
  (`op(values[gate.field], gate.value)` for all gates, where `op` is one of
  gt/ge/lt/le — so a gate is a floor *or* a ceiling) AND
  `age_seconds >= min_age_seconds`. Missing gate column → `ValueError`
  naming the field and available columns. NaN gate value → not a candidate.
- **Priority**: `w_refl·reflectivity_max + w_area·area_sqkm + w_growth·growth_rate`.
- **Growth rate** (km²/min): OLS slope of `cell_area_sqkm` vs minutes over up to
  `growth_window_scans` most recent scans of the cell; fewer than two scans → 0.0.
- **Site bonus**: each site's bonus is added once if any of the first
  `projection_steps` trajectory points falls within `radius_km` (haversine);
  summed over sites.
- **Trajectory**: non-null `cell_centroid_projection{k}_{lat,lon}` columns in
  ascending k, stopping at the first null. Step indices are discovered from the
  table (the analysis module writes 1-indexed forward steps; index 0 is the
  registration centroid, stored separately). `lead_seconds = k·scan_interval`
  where the interval is the difference of the two most recent distinct scan
  times. At the first scan of a run no cadence exists yet, so the trajectory
  is empty even if projections are present (defined condition, not an error).
- **Selection**: highest total score; ties → smallest uid.
- **Continuation**: keep the current target unless a challenger's score exceeds
  it by strictly more than `switch_margin`. A switch resets `started_at`.
- **Stop**, checked in order: `TARGET_LOST` (uid absent from snapshot) →
  `QUALITY_DROPPED` (fails the candidate rule) → `MAX_DURATION`
  (`observed >= max_observation_seconds`, boundary inclusive). After a
  MAX_DURATION stop the stopped uid is excluded from re-selection within that
  same `select()` call only — otherwise it would immediately win again.
- **Observation window**: `(scan_time, started_at + max_observation_seconds)` —
  the end is fixed at selection/switch time, so the window shrinks during
  continuation.

## Part 6 — State machine

```
              select(): no candidates → None
             ┌────────┐◄──────────────────────┐
     ┌──────►│  IDLE  │───────────────────────┘
     │       └───┬────┘
     │           │ candidates → Candidate+Priority+Site+Selection rules
     │           ▼            → NEW_TARGET (started_at = scan_time)
     │       ┌───────────┐  should_stop == None:
     │       │ OBSERVING │   ├ challenger ≤ current+margin → CONTINUATION
     │       │ (uid, t0) │◄──┤ challenger > current+margin → SWITCH (reset t0)
     │       └───┬───────┘
     │           │ should_stop ∈ {TARGET_LOST, QUALITY_DROPPED, MAX_DURATION}
     │           ▼
     │   re-run selection on same snapshot (MAX_DURATION: stopped uid excluded)
     │           ├─ winner → OBSERVING (NEW_TARGET)
     └───────────┴─ none   → IDLE, return None
```

## Part 7 — Sequence

```
caller          repository_source        RepositoryClient      engine / rules      writer
  │ build_snapshot(client,run,radar)│                             │                  │
  │──────────────────────────────►  │ table("cells_by_scan")      │                  │
  │                                 │───────────────────────────► │                  │
  │                                 │ tracks(run_id)              │                  │
  │                                 │───────────────────────────► │                  │
  │ ◄── Snapshot (frozen) ───────── │                             │                  │
  │ select(snapshot) ─────────────────────────────────────────►   │                  │
  │                                 │       is_candidate → total_score → select_best │
  │                                 │       → continue_or_switch / should_stop       │
  │ ◄── TargetSelection | None ───────────────────────────────    │                  │
  │ append_selection(path, sel) ─────────────────────────────────────────────────►   │
```

## Part 8 — Package layout

```
src/adapt/consumers/target_selection/
    __init__.py            # public exports only
    config.py              # frozen pydantic models + load_config(path)
    snapshot.py            # Snapshot, CellSnapshot, TrajectoryPoint
    selection.py           # TargetSelection, SelectionReason
    rules.py               # pure rule functions + StopReason
    engine.py              # TargetSelectionEngine, CurrentTarget
    repository_source.py   # build_snapshot() — sole adapt.api import
    writer.py              # append_selection() JSONL
    _geo.py                # haversine_km() — stdlib math only
```

`haversine_km` stays package-local: one consumer needs it, it is ~10 lines of
stdlib math, and the repo convention is to promote shared helpers only on the
rule of three.

## Part 9 — Testing strategy

`tests/consumers/target_selection/` — one file per behaviour cluster, synthetic
inputs with analytically known outputs, no stored fixtures:

- Pure-rule tests (`unit`): candidate gate, weighted sum (exact 62.0), site
  bonus geometry, tie-breaking, switch-margin strictness, stop-rule ordering.
- Engine tests (`unit`): every state-machine transition, exact observation
  windows, plus a `determinism`-marked test running the same snapshot sequence
  through two fresh engines.
- `test_repository_source.py` (`integration`): builds the shared synthetic
  repository (`tests/api/synthetic_repo.py`) and extends it test-locally
  (ALTER TABLE for projection columns + extra rows) — the shared helper is
  never modified. Verifies latest-scan filtering, the growth slope, lead
  times, the tracks merge, and fail-loud on an unknown run.
- `factories.py` provides `make_cell()` / `make_config()` used across files.

## Part 10 — Not in v0.1 (deliberate) and extension points

Not implemented: radar API/control, networking, async execution, plugin
architecture, ML/RL scoring, projected cell properties, multi-target
scheduling, database writes beyond the JSONL selection log.

Extension points left clean:

- `TargetSelection.predicted_hulls` — always `None` until hull polygons are
  queryable from the repository.
- `CellSnapshot.values` — new repository columns become scoreable/gateable
  features with zero engine changes (candidate gates already read any column).
- `repository_source.py` — projected *properties* (not just positions) attach
  here when the projection module provides them.
- Stop-event persistence and a post-MAX_DURATION cooldown are v0.2 candidates
  (both add state or output schema).

Known v0.1 limitations: a MAX_DURATION-stopped cell may be re-selected at the
next scan; the latest scan is derived from `cells_by_scan`, so a zero-cell scan
delays TARGET_LOST by one scan; `lead_seconds` assumes a locally regular scan
cadence.
