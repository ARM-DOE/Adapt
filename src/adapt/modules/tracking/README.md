# Storm Cell Tracking Module

**Module Name**: `tracking`

Gives each segmented cell an identity that persists from scan to scan and records
where tracks begin, end, split, merge, pause and resume. Causal: a scan's decisions
use only that scan and earlier ones and are never revised. Field-agnostic: the field
enters only through the excess `e = polarity·(f − cell_threshold)₊`.

## Algorithm (per scan pair)

1. **Predict.** Footprint `H_i` = previous cell advected by the dense flow
   (`cell_projections[0]`). Predicted centre = last centre moved as the footprint moved.
2. **Grow** footprint and every cell by `r = r_max (1 − √A/ℓ0)₊` (distance transform,
   from the boundary). Candidates: grown shapes intersect.
3. **Overlap** on the grown shapes: `O^c = I/|M⁺|`, `O^h = I/|H⁺|`,
   `u = 1 − √(O^c O^h)`; curved gate `u ≤ u_max`.
4. **Position** = e²-weighted centre; residual `d = √(δᵀ Σ⁻¹ δ)` with `Σ` the
   footprint's second moment; heading `h = w_h (1 − cos Δθ)/2` when both steps exceed
   two grid lengths.
5. **Hard gates**: `v ≤ v_max`; `v ≤ v_prev + a_max Δt + 2√2 σ_x/Δt` (σ_x = grid length).
6. **Cost** `c = u + w_d d + h ≤ c_max`. Joint assignment per group of competing cells
   (Hungarian) with a no-link option at `c_max`; links that cross are never chosen
   (the costlier of a crossing pair is excluded), then a scan-wide crossing check.
7. **Lineage** of what is left: an orphan resumes a latent track (same gates and cost
   over the elapsed time, no crossing in any intervening interval), else is a split
   child (`O^c ≥ τ_s`), else an initiation. An unlinked track is a merge source
   (`O^h ≥ τ_m`) or not; either way it becomes **latent** for up to `N` scans, carried by
   the mean flow inside its footprint, and terminates when not resumed.
8. **Identity** at splits and merges: larger `S = log A + γ log ē`; within `δ_S`, the
   candidate nearer the predicted position.

## Files

| File | Responsibility |
|------|----------------|
| `module.py` | `CellTracker` — per-scan flow and state (live tracks, latent tracks, recent intervals) |
| `observations.py` | grid, cells, tracks, footprints; evidence and first failed gate of every pair |
| `matching/geometry.py` | growth, grown overlaps, excess/mass, e²-centre, second moment, Mahalanobis |
| `matching/gates.py` | speed gates, heading term, segment crossing, drop-the-costlier rule |
| `matching/assignment.py` | components, per-component assignment with crossing exclusion and margins |
| `matching/hungarian.py` | Hungarian with a no-link column (the only `scipy` optimisation home) |
| `lineage.py` | split/merge partner choice, identity score and rule |
| `latent.py` | `LatentTrack`, footprint carry by the flow |
| `events.py` | `cell_events` rows |
| `decision_log.py` | `ScanLog` → `TrackingDecisions` (decisions.db) |
| `identity.py`, `lut.py` | cell uids; label → uid lookup tables |
| `config.py` | frozen `TrackingConfig` |

## Outputs

- `tracked_cells`: one row per cell (`cell_uid`, `cell_label`, area, e²-centre, field stats, core area).
- `cell_events`: CONTINUE, SPLIT, MERGE, INITIATION, TERMINATION, **LATENT** (track kept
  for resumption) and **RESUMED**. `match_method` is ASSIGNED, IDENTITY_MERGE or
  IDENTITY_SPLIT (identity transferred by the score rule) or RESUMED;
  `source_scan_id` is the scan the source was last observed in.
- `tracking_decisions` → decisions.db: `tracking_scans`, `tracking_cells`,
  `tracking_pairs` (every pair, every number, first failed gate, component, rank,
  margin, crossing), `tracking_lineage` (every hypothesis), `tracking_identity`,
  `tracking_latent`.

## Configuration (`tracker:`)

`cell_threshold`, `polarity`, `growth_max_km` (r_max), `growth_size_km` (ℓ0),
`max_overlap_mismatch` (u_max), `max_link_cost` (c_max), `residual_weight` (w_d),
`heading_weight` (w_h), `max_speed_ms` (v_max), `max_acceleration_ms2` (a_max),
`split_overlap_threshold` (τ_s), `merge_overlap_threshold` (τ_m), `latent_scans` (N),
`identity_intensity_weight` (γ), `identity_score_margin` (δ_S), `core_field_threshold`
(diagnostic only). Keys of the previous tracker fail loudly, naming their replacement.

## References

Kuhn (1955); Jaqaman et al. (2008), *Nature Methods*; Dixon & Wiener (1993), TITAN;
Raut et al. (2021), *JAMC*.
