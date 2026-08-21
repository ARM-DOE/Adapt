# Document 5 — Data Contract Architecture

Principle 8: contracts matter more than implementations. This document defines the
standards for validation, schemas, metadata, units, coordinates, and dimensions —
the surface that must stay stable for a decade while everything else churns.

## Current state, assessed

What exists in `src/adapt/contracts/` is a **validation library, not a contract
system**:

- Contracts are imperative check callables (`check_grid_ds_2d`, `check_segmented_ds`)
  attached per-module via `input_contracts`/`output_contracts` dicts and invoked by the
  executor at edges. The error taxonomy (`ContractViolation` = pipeline bug vs
  `ValueError` = user error) is genuinely good — keep it.
- **The contract is attached to the module, not the data.** Two modules consuming
  `grid_ds_2d` could attach different checks; a third could attach none. The *key*
  `"grid_ds_2d"` — the actual shared interface — has no authoritative definition
  anywhere. Its meaning lives in module docstrings and tribal knowledge.
- **Checks are shallow.** `check_grid_ds_2d` verifies x/y coords exist and
  reflectivity is 2-D. Nothing validates units, dtype, CRS, fill values, coordinate
  monotonicity, or attribute presence.
- **A flat contradiction:** `check_grid_ds_2d` hardcodes the variable name
  `"reflectivity"` (`contracts/grid.py:45`) while `InternalConfig.global_.var_names`
  makes that name *configurable* — so a user who renames the variable passes config
  validation and then trips a contract violation mid-pipeline. One of these two
  designs must die.
- Tables: `OutputTableSpec` declares name + primary key, columns *inferred from the
  DataFrame at write time*. Convenient, but the schema is whatever the code happened
  to produce — column drift between runs is silent (the writer even auto-`ALTER TABLE`s
  new columns in).

## Decision 1 — One canonical internal data model; kill name configurability

CLAUDE.md already mandates it: *input conversion happens once at the boundary; no
function accepts grid spacing, CRS, or coordinate names as arguments.* Follow through:

- **Fixed internal names**: dims/coords `(time, z, y, x)`; CF `standard_name`-anchored
  variables (`equivalent_reflectivity_factor` with internal short name `reflectivity`,
  `cell_labels`, …). Defined once in `contracts/`, used verbatim by every module.
- **Delete `var_names`/`coord_names` from config.** Renaming internals is flexibility
  nobody needs and everybody pays for: today every module threads `labels_var`/
  `reflectivity_var` parameters through configs to handle a rename that, if ever used,
  breaks the hardcoded contracts anyway. Ingest adapters map external names → canonical
  names at the boundary; that mapping is per-reader config, where it belongs.
- This is Principle 10 applied to data: one dialect, spoken everywhere, converted once.

## Decision 2 — Declarative DataSpecs, keyed by context key

The authoritative definition of every context key moves to one place:

```python
# contracts/specs.py  (illustrative shape — the only new abstraction this doc adds)
GRID_2D = DatasetSpec(
    key="grid_ds_2d",
    dims={"y": None, "x": None},                 # None = any length
    coords={"y": Coord(units="m"), "x": Coord(units="m")},
    variables={
        "reflectivity": Var(dtype="float32", dims=("y", "x"), units="dBZ",
                            standard_name="equivalent_reflectivity_factor"),
    },
    attrs_required=("radar_latitude", "radar_longitude"),
    invariants=(monotonic_coords, no_all_nan("reflectivity")),   # escape hatch: callables
)

CELL_STATS = TableSpec(
    key="cell_stats",
    columns={"scan_time": Col("datetime[utc]"), "cell_label": Col("int", ge=1),
             "area_km2": Col("float", units="km^2"), ...},
    primary_key=("scan_time", "cell_label"),
)
```

Consequences, in order of importance:

1. **Modules stop carrying contracts.** `inputs = ["grid_ds_2d"]` suffices — the
   executor looks the spec up by key and validates both sides of every edge uniformly.
   `input_contracts`/`output_contracts` ClassVars are deleted; one less thing for a
   contributor to learn (Principle 6). Custom one-off invariants remain possible as
   callables *inside* a spec, so no expressiveness is lost.
2. **Specs are data, so they generate things.** Reference docs (the module catalog
   shows each key's schema), synthetic test fixtures (`adapt.testing.make("grid_ds_2d")`
   builds a minimal valid instance — this powers the unit-test policy of synthetic
   inputs with no stored fixtures), and table DDL (replacing inferred-at-write columns:
   `ModuleOutputWriter` validates against the declared `TableSpec` and rejects drift
   instead of `ALTER TABLE`-ing it in).
3. **Specs are the versioned surface.** `CONTRACT_VERSION` (Doc 02) covers the spec
   set; adding an optional column/variable is minor, changing dtype/units/dims is
   major. Catalog records the version per run (Doc 04), keeping old data interpretable.

Build vs buy for the spec engine: **build minimal.** Evaluated pandera (tables) and
xarray-schema (datasets): pandera is heavy and pandas-centric; xarray-schema is
low-maintenance-risk. The needed validator is ~200 lines over two spec dataclasses,
sits in Ring 0 with zero new dependencies, and the spec format must outlive any
third-party library's API anyway (Principle 8).

## Decision 3 — Validation placement and cost

- **Boundary validation is total and always-on**: ingest output, persistence input.
- **Edge validation (between modules) is always-on by default**, per Principle 9.
  Measured against multi-second scan processing, dimension/dtype/attr checks are
  noise; only data-scanning invariants (NaN scans, monotonicity) cost anything. If
  profiling ever shows pain, the *sampling* knob (validate every Nth scan in
  operational mode, always in CI/reprocessing) already has a designed home — but it is
  not enabled until proven necessary.
- Validation failures remain `ContractViolation` → halt the run. A pipeline producing
  malformed science must not keep producing it.

## Decision 4 — File-format standards

| Surface | Standard | Action |
|---------|----------|--------|
| NetCDF outputs | **CF-1.10** + ACDD subset | Canonical names map directly to CF `standard_name`s; grid mapping variable carries the projection (azimuthal equidistant from radar lat/lon) so x/y are formally georeferenced — today CRS is implicit, a real interoperability defect |
| NetCDF compliance | cfchecker / IOOS compliance-checker | Run in integration tests (labelled, not default CI), not at runtime |
| Tables (SQLite/Parquet) | Declared `TableSpec`; `scan_time` ISO + `scan_time_unix` dual columns | Already enforced by the writer + fitness test — keep; add units to column specs so the API can expose them |
| In-memory | xarray Datasets + pandas DataFrames | Unchanged — the ecosystem default; cf-xarray optionally at ingest for name resolution, never required downstream |

## Units policy

Units live in **metadata, validated at edges** (`units="dBZ"` in specs; mismatch =
violation). Full unit *arithmetic* (pint/pint-xarray) inside the pipeline is rejected:
it taxes every operation in the hot path, infects every module's code with quantity
types, and the pipeline has exactly one unit system (SI + dBZ) by construction of the
canonical model. Converters belong in ingest adapters; a `to_units()` helper in the
read API can serve consumers. Revisit only if multi-domain distributions genuinely
mix unit systems.

## Coordinates and dimensions

- Canonical order `(time, z, y, x)`; specs enforce per-variable dims.
- x/y are radar-relative meters with an explicit CF grid mapping; lat/lon are derived
  2-D coordinates computed once at ingest (consumers like the dashboard currently
  re-derive km offsets — `centroid_to_km` — which the canonical CRS removes).
- `scan_time`: the existing single-source `to_scan_iso` rule is exemplary; the spec
  system absorbs it (every `TableSpec` datetime column is UTC, serialized by the one
  function).

## What not to build

- No runtime schema registry service (specs are code in `contracts/`, versioned by the
  package — a registry server is operational complexity with no consumer).
- No automatic schema migration framework for tables until there are external
  consumers of stored runs; until then, contract-version bump + reprocess is honest
  and simpler.
- No support for "bring your own coordinate names." That flexibility died in
  Decision 1, deliberately.

## Migration order

1. Resolve the contradiction now: hardcode canonical names, deprecate
   `var_names`/`coord_names` (config keys warn-and-ignore for one release, then
   removed).
2. Introduce `DatasetSpec`/`TableSpec` + validator; port the existing six check
   functions into specs (they become the `invariants` of the first spec set).
3. Move executor to key-based spec lookup; delete per-module contract ClassVars.
4. CF grid mapping + global attrs in the NetCDF writer (pairs with Doc 04 Layer 3).
5. Spec-driven fixture factory + doc generation.

## Scoring

| Criterion | Score | Rationale |
|-----------|-------|-----------|
| Contributor friendliness | High | Declare IO keys only; fixtures generated from specs; no contract plumbing |
| Extensibility | High | New domain = new spec set in its distribution; same validator |
| Testability | High | Specs generate synthetic valid/invalid cases mechanically |
| Reproducibility | High | Versioned, hashable schema surface; CRS/units explicit in artifacts |
| Operational complexity | Low | ~200 lines, zero new deps, no services |
| Long-term maintainability | High | One authoritative definition per key; drift becomes a hard error |
