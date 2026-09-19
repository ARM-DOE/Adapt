# Contracts and Module Boundaries

**Location**: `src/adapt/modules/*/contracts.py`, `src/adapt/modules/base.py`

---

## What a Contract Is

A contract is a structural assertion about a data object. It answers: *does this object have the shape and content that the consuming module expects?*

Contracts are enforced at stage **boundaries** — after a module runs, before the next module receives its output. They are not integrated into module logic.

---

## Why Contracts Are At Boundaries, Not Inside Modules

If a module validates its own output internally, the validation is coupled to the module's implementation. If the contract is wrong, you have to change the module.

When the executor enforces the contract externally, the contract becomes a shared specification between the producing module and all consuming modules. If the contract is wrong, you change the contract file. The module is untouched.

This also means: you can test contract compliance without running the full pipeline. Pass a synthetic dataset to `assert_segmented()` and it either passes or raises. No module wiring required.

---

## Contract Definitions

### `assert_gridded(grid_ds)` — Ingest output

Verifies:
- `grid_ds` is an `xarray.Dataset`
- Has `x` and `y` dimensions
- Has the canonical reflectivity variable (name from config)
- Shape is consistent (`y` and `x` have the same spatial extent)

`grid_ds_2d` verifies the above plus:
- Exactly 2 dimensions (no `z`)

### `assert_segmented(segmented_ds)` — Detection output

Verifies:
- `cell_labels` variable exists
- Dtype is integer (uint or int)
- Label 0 is the most common value (background dominates)
- Labels are contiguous from 0 to N
- Cells with label > 0 are ordered by decreasing area (label 1 is largest)

### `assert_projected(projected_ds)` — Projection output

Verifies:
- `heading_x` and `heading_y` fields exist with shape `(y, x)`
- `cell_projections` exists with shape `(step, y, x)`
- `step` dimension has `max_projection_steps + 1` levels (step 0 is registration)
- No NaN values in heading fields

### `assert_analysis_output(cell_stats)` — Analysis output

Verifies:
- `cell_stats` is a `pd.DataFrame`
- Required columns are present: `cell_label`, `time`, `cell_area_sqkm`, centroid columns, heading columns
- `cell_label` values are all positive (no background rows)
- `time` column is not null

### `assert_tracked_cells(tracked_cells)` — Tracking output

Verifies:
- `cell_uid` column exists
- No null UIDs
- `age_seconds` is non-negative
- Boolean event flag columns exist

### `assert_cell_events(cell_events)` — Tracking output (events)

Verifies:
- `event_type` is one of: `CONTINUE`, `SPLIT`, `MERGE`, `INITIATION`, `TERMINATION`
- `source_cell_uid` is null only for `INITIATION` events
- `target_cell_uid` is null only for `TERMINATION` events

---

## `ContractViolation` Exception

All contract failures raise `ContractViolation` (defined in `modules/base.py`). This is distinct from `ValueError` or `AssertionError` so that callers can catch specifically contract failures without accidentally swallowing other programming errors.

The exception message must identify:
1. Which contract failed (`assert_segmented`)
2. What was wrong (`cell_labels dtype is float32, expected integer`)
3. Where the value came from (module name, context key)

---

## What Contracts Do Not Validate

- Scientific correctness — whether a cell area of 500 km² is physically plausible
- Statistical properties — whether reflectivity values are within a normal range
- Completeness — whether the expected number of cells were detected
- Time ordering — whether scans are arriving in sequence

These are monitoring concerns, not interface contracts. Checking them in contracts would make the pipeline fragile against legitimate edge cases (unusual storm events, sparse scanning geometries, degraded radar modes).

---

## The `require()` Helper

`modules/base.py` exports a `require(condition, message)` helper for contract assertions:

```python
def require(condition: bool, message: str) -> None:
    if not condition:
        raise ContractViolation(message)
```

This is used inside contract functions to produce clean, readable assertions without `assert` (which can be disabled) or raw `raise ValueError` (which is too generic).

---

## Module Boundaries Summary

| From → To | Key transferred via context | Contract enforced |
|-----------|----------------------------|-------------------|
| Ingest → Detection | `grid_ds_2d` | `assert_gridded` |
| Detection → Projection | `segmented_ds` | `assert_segmented` |
| Projection → Analysis | `projected_ds` | `assert_projected` |
| Analysis → Tracking | `cell_stats` | `assert_analysis_output` |
| Tracking → Persistence | `tracked_cells`, `cell_events` | `assert_tracked_cells`, `assert_cell_events` |
| Persistence → API | Parquet/SQLite on disk | Schema validation at query time |

---

## Adding a New Contract

1. Add `assert_<name>(data_object)` to the relevant `contracts.py` file
2. In the module's `BaseModule` subclass, add to `output_contracts`:
   ```python
   output_contracts = {"my_output": assert_my_output}
   ```
3. The executor will call it automatically after `run()`

No changes to the executor or any other component required.
