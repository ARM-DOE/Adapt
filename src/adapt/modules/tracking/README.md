# Storm Cell Tracking Module

**Author**: Adapt Development Team
**Status**: Production-ready
**Module Name**: `tracking`

## Overview

The tracking module performs tracking-only association of segmented radar cells across consecutive scans using projected mask overlap and a multi-term matching cost. It emits scan-local tracking observations, explicit lineage events, and same-scan adjacency translated into track identity space.

## Features

- **Optical Flow-Based Prediction**: Uses projected cell masks for robust matching
- **Multi-Term Cost Function**: Combines position, IoU, area, and reflectivity
- **Graph-Based Lineage**: Stores complete tracking history as a directed graph
- **Explicit Events**: Emits CONTINUE, SPLIT, MERGE, INITIATION, TERMINATION event rows per scan
- **Adjacency Plumbing**: Translates scan-local cell adjacency into track identity space

## Architecture

The scientific layer is decomposed into focused, single-responsibility files under
`adapt/modules/tracking/`. `RadarCellTracker` (in `module.py`) is orchestration only;
it delegates to:

| File | Responsibility |
|------|----------------|
| `module.py` | `RadarCellTracker` — per-scan flow, state, delegation |
| `graph.py` | `TrackingGraph` (the only `networkx` home) |
| `projection.py` | `select_registration_labels` — minute-resolution registration hull |
| `matching/overlap.py` | `OverlapMatcher` — deterministic unique-overlap matching |
| `matching/hungarian.py` | `MatchingEngine` — cost matrix + Hungarian (the only `scipy` home) |
| `motion.py` | `MotionValidator` + heading-change helpers |
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
hard physical-motion rejection (speed / acceleration caps — before matching)
        ↓
deterministic unique-overlap matching (skips Hungarian)
        ↓
Hungarian assignment (residual ambiguity only; soft heading-consistency penalty)
        ↓
split / merge detection
        ↓
initiation / termination
```

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
from adapt.modules.tracking.module import RadarCellTracker
from adapt.schemas import init_runtime_config

# Initialize
config = init_runtime_config(user_config)
tracker = RadarCellTracker(config)

# Track one scan at a time (scan-local outputs)
tracked_cells, track_events, tracked_cell_adjacency = tracker.track(ds, cell_stats, cell_adjacency)

# Access results
print(f"Tracked {len(tracks_df)} distinct storms")
print(f"Total {len(cells_df)} cell observations")
```

## Configuration

Default configuration in `adapt.schemas.param.TrackerConfig`:

```python
max_cost_threshold: float = 0.7         # Maximum cost for valid assignment
merge_memory_scans: int = 3             # Scans to remember for merge tracking
core_reflectivity_threshold: float = 40.0  # Core area threshold (dBZ)
```

Override in user config:

```yaml
tracker:
  max_cost_threshold: 0.65
  merge_memory_scans: 5
  core_reflectivity_threshold: 42.0
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

### Cost Function

The Hungarian matching cost (`matching/hungarian.py`) combines four terms:

```
cost = 0.4 * D_pos + 0.3 * (1 - IoU) + 0.15 * |log(A2/A1)| + 0.1 * |Z2 - Z1| / 50
```

Where `D_pos` is the centroid distance normalised by `expected_speed_ms * dt`,
`IoU` is the projected-hull/current-cell overlap, `A2/A1` is the area ratio, and
`Z2 - Z1` is the mean-reflectivity difference. When `heading_change_penalty_weight`
> 0, a soft `weight * heading_change` (radians) term is added for tracks with an
established velocity (crossing-track prevention).

### Assignment

Hungarian assignment (`scipy.optimize.linear_sum_assignment`) is applied **only to
residual ambiguity** — pairs left after deterministic unique-overlap matching and
hard physical-motion rejection. See the matching hierarchy above.

### Search Region

Candidates are filtered by non-zero overlap with the registration projected hull —
the minute-resolution `registration_minutes` frame nearest the real scan gap,
falling back to `cell_projections[0]` when minute frames are absent
(`projection.select_registration_labels`).

### Diagnostics

Every accepted match records a `MatchDiagnostics` row persisted to `cell_events`:
`candidate_overlap`, `candidate_iou`, `candidate_centroid_distance_m`,
`candidate_speed_ms`, `candidate_heading_change_deg`, `candidate_area_ratio`,
`candidate_reflectivity_difference`, `candidate_final_cost`, and `match_method`
(`OVERLAP` / `HUNGARIAN` / `SPLIT` / `MERGE`).

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
