# Storm Cell Tracking Module

**Author**: Adapt Development Team
**Status**: Production-ready
**Module Name**: `tracking`

## Overview

The tracking module performs tracking-only association of segmented radar cells across consecutive scans using projected mask overlap and a multi-term matching cost. It emits scan-local tracking observations, explicit lineage events, and same-scan adjacency translated into track identity space.

## Features

- **Registration-driven**: matches against projected (advected) cell hulls
- **Geometry-first, field-agnostic**: bidirectional overlap + `m + d/L` cost; the field is only a centroid weight
- **Constraint propagation before optimisation**: Hungarian only for ambiguous connected components
- **Graph-Based Lineage**: stores complete tracking history as a directed graph
- **Explicit Events**: emits CONTINUE, SPLIT, MERGE, INITIATION, TERMINATION rows per scan

## Architecture

The scientific layer is decomposed into focused, single-responsibility files under
`adapt/modules/tracking/`. `CellTracker` (in `module.py`) is orchestration only;
it delegates to:

The tracker is **geometry-first** and **field-agnostic**: the pixel field (reflectivity,
brightness temperature, vertical wind, …) is used only as a centroid weight, never
assumed to be reflectivity. Association is resolved as progressively stronger
constraints with optimisation as the last resort.

| File | Responsibility |
|------|----------------|
| `module.py` | `CellTracker` — per-scan flow, state, delegation |
| `graph.py` | `TrackingGraph` (the only `networkx` home) |
| `projection.py` | `select_registration_labels` — minute-resolution registration hull |
| `matching/geometry.py` | overlap (Opc/Ocp), hull dilation, length-scale strategies, `pair_cost`, centroids |
| `matching/candidate.py` | `CandidateGenerator` — buffered high-recall candidate pairs |
| `matching/validation.py` | `GeometricValidator` — bidirectional-overlap hard gate |
| `matching/assignment.py` | `ConstraintPropagator` + `AssignmentGraph` (connected components) |
| `matching/hungarian.py` | `HungarianMatcher` — per-component assignment (the only `scipy` optimisation home) |
| `motion.py` | `MotionValidator` (speed/accel caps) + heading-change helpers |
| `models.py` | `MatchMethod` / `TrackingError` enums, `MatchDiagnostics`, `TrackMotionState` |
| `identity.py` | stable `cell_uid` generation |
| `events.py` | lineage event-row builders + diagnostics assembly |
| `config.py` | frozen `TrackingConfig` |

The node layer (`adapt/execution/nodes/tracking.py`) keeps the `BaseModule` wrapper,
`registry.register`, `build_config`, contracts, and persistence specs — no engine
imports ever live under `modules/tracking/`.

### Matching hierarchy

Each frame pair is resolved in this order (registration-driven, optimisation last):

```
scan-gap classification (physical time; hard reset on excess gap / non-monotonic time)
        ↓
registration projected hulls (minute nearest the real gap)
        ↓
dilate hulls by projected_hull_buffer_km → liberal candidate pairs (high recall)
        ↓
hard gate: bidirectional overlap (Opc, Ocp) + kinematic speed/acceleration caps
        ↓
deterministic constraint propagation (mutually-unique matches → PROPAGATED)
        ↓
connected components → Hungarian inside ambiguous groups only (→ HUNGARIAN)
        ↓
split / merge detection
        ↓
initiation / termination
```

Bidirectional overlap — `Opc = |I|/|candidate|` and `Ocp = |I|/|projected hull|`, both
required above their thresholds — rejects tiny cells inside large hulls, merged cells
engulfing a prediction, and grazing contacts that a one-sided fraction or IoU would pass.
The buffer widens candidate *recall* only; the gate and cost use the un-buffered hull.

## Usage

### As Part of Pipeline

```python
# Automatic via pipeline DAG
# tracking module runs after projection module

# Context inputs:
#   - projected_ds: xr.Dataset (from ProjectionModule)
#   - cell_stats: pd.DataFrame (from AnalysisModule)
#   - cell_adjacency: pd.DataFrame (from AnalysisModule)
#   - config: InternalConfig
#
# Context outputs:
#   - tracked_cells: pd.DataFrame
#   - track_events: pd.DataFrame
#   - tracked_cell_adjacency: pd.DataFrame
```

### Standalone

```python
from adapt.modules.tracking.module import CellTracker
from adapt.schemas import init_runtime_config

# Initialize
config = init_runtime_config(user_config)
tracker = CellTracker(config)

# Track one scan at a time (scan-local outputs)
tracked_cells, cell_events = tracker.track(projected_ds, cell_stats)
```

## Configuration

Defaults live in `adapt.configuration.schemas.param.TrackerConfig`. Key knobs:

```python
projected_hull_buffer_km: float = 1.0    # dilation radius for candidate recall
minimum_candidate_overlap: float = 0.20  # Opc gate
minimum_projected_overlap: float = 0.20  # Ocp gate
length_scale: str = "hull_equiv_diameter"  # hull_equiv_diameter | sum_radii | fixed_km
geometry_length_scale_km: float = 5.0    # used only when length_scale == "fixed_km"
max_tracking_gap_minutes: float = 20.0   # hard reset when a scan gap exceeds this
max_speed_ms: float = 40.0               # kinematic velocity cap
max_speed_multiplier: float = 3.0        # kinematic acceleration cap
heading_change_penalty_weight: float = 0.0  # optional crossing-track penalty
split_overlap_threshold: float = 0.8     # SPLIT / MERGE overlap threshold
core_field_threshold: float = 40.0       # core-area output threshold (field units)
```

Override in user config:

```yaml
tracker:
  minimum_candidate_overlap: 0.25
  length_scale: sum_radii
  max_speed_ms: 35.0
```

## Data Outputs

### Tracked Cells

One row per tracked cell observation in the current scan:

| Column | Type | Description |
|--------|------|-------------|
| `time` | datetime64 | Observation timestamp |
| `track_index` | int | Deterministic track index (starts at 1) |
| `track_id` | str | Deterministic UUID (derived from run_id + track_index) |
| `cell_label` | int | Cell label from segmentation |
| `area` | float | Cell area (km²) |
| `centroid_x`, `centroid_y` | float | Cell center coordinates |
| `mean_reflectivity` | float | Average dBZ |
| `max_reflectivity` | float | Peak dBZ |
| `core_area` | float | Area above core threshold (km²) |
| `n_connected_cells` | int | Number of adjacent tracked neighbors in this scan |
| `connected_track_ids_json` | str | JSON list of adjacent `track_id` values |

### Track Events

Explicit lineage/event rows for the current scan:

| Column | Type | Description |
|--------|------|-------------|
| `time` | datetime64 | Scan timestamp |
| `event_type` | str | CONTINUE \| SPLIT \| MERGE \| INITIATION \| TERMINATION |
| `source_track_id` | str or None | Source track (if applicable) |
| `target_track_id` | str or None | Target track (if applicable) |
| `cost` | float or None | Matching cost (CONTINUE only in v1) |

### Tracked Cell Adjacency

Normalized adjacency pairs in track identity space:

| Column | Type | Description |
|--------|------|-------------|
| `time` | datetime64 | Scan timestamp |
| `track_id_a`, `track_id_b` | str | Adjacent tracks in this scan |
| `touching_boundary_pixels` | int | Boundary-touch count from analysis |

## Algorithm Details

### Cost Function (geometry-only, `matching/geometry.py`)

The cost is dimensionless and scale-invariant so the same tracker behaves
consistently across radar, satellite, and cloud-mask inputs:

```
cost = m + d / L + heading_penalty
m    = 1 − √Opc · √Ocp                     # overlap mismatch, in [0, 1]
d    = ‖ mass_centroid(candidate) − centroid(projected hull) ‖   # metres (prediction residual)
```

`√Opc·√Ocp` spreads near-threshold overlaps and compresses near-perfect ones. `d` is
normalised by a **configurable characteristic length** `L` (`length_scale`), so both
terms are O(1) with no invented weights:

| `length_scale` | L |
|----------------|---|
| `hull_equiv_diameter` (default) | `2·√(|hull|·pixel_area/π)` |
| `sum_radii` | `√(|hull|·pixel_area/π) + √(|cell|·pixel_area/π)` |
| `fixed_km` | `geometry_length_scale_km · 1000` |

When `heading_change_penalty_weight > 0`, a soft `weight · Δheading` (radians) term is
added for tracks with an established velocity (crossing-track prevention).

### Assignment

Most pairs are resolved by **constraint propagation** (mutually-unique matches, no
optimisation → `PROPAGATED`). Only genuinely ambiguous connected components reach
`scipy.optimize.linear_sum_assignment`, run **per component** — never one global matrix
(→ `HUNGARIAN`).

### Search Region

Candidates come from the registration projected hull — the minute-resolution
`registration_minutes` frame nearest the real scan gap (falling back to
`cell_projections[0]`) — dilated by `projected_hull_buffer_km` for recall
(`projection.select_registration_labels`, `matching/candidate.py`).

### Diagnostics

Every accepted match records a `MatchDiagnostics` row persisted to `cell_events`:
`candidate_opc`, `candidate_ocp`, `candidate_centroid_distance_m`,
`candidate_speed_ms`, `candidate_heading_change_deg`, `candidate_area_ratio`,
`candidate_final_cost`, and `match_method`
(`PROPAGATED` / `HUNGARIAN` / `SPLIT` / `MERGE`).

## Testing

Run behavior-driven tests:

```bash
pytest tests/modules/tracking/ -v
```

Tests cover:
- Linear motion tracking
- Cell growth and decay
- New cell birth and death
- Crossing tracks
- Graph structure
- Cost function
- DataFrame outputs

All tests use synthetic data for reproducibility.

## Dependencies

- `numpy`: Array operations
- `pandas`: DataFrame outputs
- `xarray`: Dataset handling
- `scipy`: Hungarian assignment
- `networkx`: Tracking graph storage

## Performance

Typical performance for 50 cells per scan:
- **Tracking time**: 10-50 ms per frame pair
- **Memory usage**: ~10 MB for 100 scans
- **Graph size**: Linear with total cell-observations

## Tracking-Assisted Segmentation Correction (design only — not implemented)

Projected hulls carry motion-coherence information that can flag segmentation
errors. This is a **designed extension point**, intentionally left unimplemented
(YAGNI) until a concrete use case exists. No hooks or dead code are present today.

**Motivating cases**

- *Over-split.* Segmentation fragments one storm into several cells, but a single
  continuing parent's projected hull covers all fragments coherently → the
  fragments should be re-merged into one tracked object.
- *Over-merge.* Segmentation fuses two storms into one cell, but two distinct
  parents project into separable sub-regions → the cell should be re-split.

**Where it would hook in.** A correction stage would sit between *registration
projected hulls* and *deterministic unique-overlap matching* in the hierarchy
above — i.e. it adjusts the current-frame label field *before* matching, so all
downstream stages operate on the corrected segmentation. It would consume the
same `select_registration_labels` hull plus the per-parent overlap structure
already computed by `OverlapMatcher`.

**Proposed shape when built.** A registered, swappable
`SegmentationCorrector` strategy (Open/Closed, like detection/tracking backends)
implemented under `modules/`, communicating via a `contracts/` interface, returning
a corrected label array + provenance describing each merge/split it applied. It
must be deterministic and produce no side effects. The correction provenance would
travel as additional diagnostic rows so every change is traceable.

## Future Enhancements

Potential improvements identified during development:

1. **Advanced Split/Merge Logic**: Implement temporary merge identity restoration
2. **Track Smoothing**: Apply Kalman filtering to motion vectors
3. **Motion-coherent segmentation correction**: see the design section above
4. **Parallel Processing**: Process multiple files concurrently
5. **Persistence**: Save/load tracking graph for resumable processing

## References

- **Hungarian Algorithm**: Kuhn, H. W. (1955). "The Hungarian method for the assignment problem"
- **Storm Tracking**: Dixon & Wiener (1993). "TITAN: Thunderstorm Identification, Tracking, Analysis, and Nowcasting"
- **Optical Flow**: Farnebäck, G. (2003). "Two-Frame Motion Estimation Based on Polynomial Expansion"

## Developer Notes

This module was implemented as the **first additional default module** in the Adapt system. For detailed developer experience and implementation patterns, see `MODULE_EXTENSION_GUIDE.md` in the repository root.

Key learnings:
- The two-layer pattern (scientific class + wrapper) works extremely well
- Pydantic config schemas eliminate runtime validation complexity
- Contract validators provide fail-fast guarantees
- Behavior-driven tests with synthetic data are robust and fast

## License

Same as Adapt main project.

## Contact

For questions or issues, open a GitHub issue in the Adapt repository.
