# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Track segmented geophysical objects across consecutive scans (geometry-first).

The tracker is **field-agnostic**: the pixel field (reflectivity, brightness temperature,
vertical wind, …) is used only as a centroid weight, never assumed to be reflectivity.
Association is resolved as progressively stronger constraints, with optimisation last:

1. Registration projected hulls (minute nearest the real gap)
2. Dilate hulls by ``projected_hull_buffer_km`` → liberal candidate pairs (high recall)
3. Hard gate: bidirectional overlap (Opc, Ocp) + kinematic speed/acceleration caps
4. Deterministic constraint propagation for mutually-unique matches (no optimisation)
5. Connected components → Hungarian assignment inside ambiguous groups only
6. Split / merge on the leftover born / dissipated cells
7. Initiation / termination

Each object gets a stable `cell_uid`; lineage is graph edges. State lives in
``graph.TrackingGraph``; matching in ``matching/``; uid generation in ``identity``; event
rows in ``events``. ``CellTracker`` here is orchestration only.

Scan outputs:
1. **tracked_cells**: per-observation rows for the current scan
2. **cell_events**: explicit lineage/event rows (CONTINUE, SPLIT, MERGE, INITIATION, TERMINATION)

Inspired by TINT (Raut et al., 2021) but mask/geometry-driven rather than centroid-only.

Author: Bhupendra Raut, ANL.

References: Raut, B. A., Jackson, R., Picel, M., Collis, S. M., Bergemann, M., & Jakob, C.
(2021). An adaptive tracking algorithm for convection in simulated and remote sensing data.
Journal of Applied Meteorology and Climatology, 60(4), 513-526.
"""

import logging
import math
from dataclasses import dataclass

import numpy as np
import pandas as pd
import xarray as xr

from adapt.modules.tracking.events import (
    build_cell_events_dataframe,
    event_continue,
    event_initiation,
    event_merge,
    event_split,
    event_termination,
)
from adapt.modules.tracking.graph import TrackingGraph
from adapt.modules.tracking.identity import (
    _cell_uid_from_signature,
    _track_signature_from_birth,
)
from adapt.modules.tracking.matching.assignment import AssignmentGraph, ConstraintPropagator
from adapt.modules.tracking.matching.candidate import CandidateGenerator
from adapt.modules.tracking.matching.geometry import (
    buffer_pixels_from_km,
    length_scale,
    mask_centroid,
    mass_weighted_centroid,
    pair_cost,
)
from adapt.modules.tracking.matching.hungarian import HungarianMatcher
from adapt.modules.tracking.matching.validation import GeometricValidator
from adapt.modules.tracking.models import (
    MatchDiagnostics,
    MatchMethod,
    TrackingError,
    TrackMotionState,
)
from adapt.modules.tracking.motion import (
    MotionValidator,
    heading_change_degrees,
    heading_change_radians,
)
from adapt.modules.tracking.projection import select_registration_labels
from adapt.utils.time import normalize_time_scalar

# Beyond this ratio between consecutive scan intervals the cadence is flagged
# irregular (diagnostic only — no track reset).
_CADENCE_IRREGULAR_RATIO = 2.0

# Re-exported for the public import surface (tests + tooling import these from here).
__all__ = [
    "CellTracker",
    "TrackingGraph",
    "_cell_uid_from_signature",
    "_track_signature_from_birth",
]

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class _EdgeCost:
    """Pre-computed geometry for a surviving candidate edge (prev_idx ↔ curr_idx)."""

    opc: float
    ocp: float
    displacement: float  # metres, projected-hull centroid → candidate mass centroid
    cost: float  # m + d/L (+ heading penalty)


class CellTracker:
    """Track segmented geophysical objects with a geometry-first pipeline.

    The tracker is field-agnostic (the field is used only as a centroid weight,
    never assumed to be reflectivity) and resolves association as progressively
    stronger constraints, using optimisation only as a last resort:

    1. Registration projected hulls (minute nearest the real gap)
    2. Dilate hulls by ``projected_hull_buffer_km`` and generate candidates liberally
    3. Hard gate: bidirectional overlap (Opc, Ocp) + kinematic speed/acceleration caps
    4. Deterministic constraint propagation (mutually-unique matches)
    5. Connected components → Hungarian assignment inside ambiguous groups only
    6. Split / merge on the leftover born/dissipated cells
    7. Initiation / termination

    Cost is ``m + d/L`` where ``m = 1 − √Opc·√Ocp``, ``d`` is the mass-weighted
    centroid's residual from the projected-hull centroid, and ``L`` is a configurable
    characteristic length. An optional heading-change penalty is added on top.
    """

    def __init__(self, config):
        self.split_overlap = config.split_overlap
        self.core_threshold = config.core_field_threshold
        self.field_var = config.field_var  # generic field; not assumed to be reflectivity
        self.labels_var = config.labels_var
        self.uid_time_step_s = config.uid_time_step_s
        self.uid_latlon_step_deg = config.uid_latlon_step_deg
        self.uid_area_step_km2 = config.uid_area_step_km2
        self.uid_width = config.uid_width

        self.buffer_km = config.projected_hull_buffer_km
        self.length_scale_name = config.length_scale
        self.geometry_length_scale_km = config.geometry_length_scale_km

        self.max_tracking_gap_minutes: float = config.max_tracking_gap_minutes
        self.max_tracking_gap_s: float = config.max_tracking_gap_minutes * 60.0

        self.graph = TrackingGraph()
        self.validator = GeometricValidator(
            config.minimum_candidate_overlap, config.minimum_projected_overlap
        )
        self.motion = MotionValidator(config.max_speed_ms, config.max_speed_multiplier)
        self.heading_penalty_weight = config.heading_change_penalty_weight
        self._previous_scan: tuple | None = None  # (time, node_ids)
        self._cell_identity: dict[int, tuple[str, str]] = {}
        self._track_motion: dict[int, TrackMotionState] = {}  # track_index → kinematics
        self._prev_dt_s: float | None = None  # last good scan interval (cadence check)

        logger.info(
            "CellTracker initialized: buffer=%.1fkm min_opc=%.2f min_ocp=%.2f"
            " length_scale=%s max_gap=%.1fmin max_speed=%.1fm/s",
            self.buffer_km,
            config.minimum_candidate_overlap,
            config.minimum_projected_overlap,
            self.length_scale_name,
            self.max_tracking_gap_minutes,
            config.max_speed_ms,
        )

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def track(
        self,
        ds_projected: xr.Dataset,
        cell_stats_df: pd.DataFrame,
    ) -> tuple[pd.DataFrame, pd.DataFrame]:
        """Process one scan.

        Returns scan-local outputs:
        - tracked_cells_df: one row per cell observation in this scan
        - cell_events_df: explicit lineage/events (continue/split/merge/initiation/termination)
        """
        current_time = normalize_time_scalar(ds_projected.time.values)
        cells_current = self._extract_cells_from_analyzer(ds_projected, cell_stats_df)

        events: list[dict] = []
        if self._previous_scan is None:
            node_ids = self._initialize_tracks(current_time, cells_current)
            self._previous_scan = (current_time, node_ids)
            for node_id in node_ids:
                events.append(
                    event_initiation(self.graph, self._cell_identity, current_time, node_id)
                )
        else:
            prev_time, prev_node_ids = self._previous_scan
            dt_s = self._to_epoch_seconds(current_time) - self._to_epoch_seconds(prev_time)
            if self._gap_forces_reset(dt_s, prev_time, current_time):
                events = self._reset_tracks(prev_node_ids, current_time, cells_current)
            else:
                self._prev_dt_s = dt_s
                events = self._track_frame_pair(
                    prev_node_ids, current_time, ds_projected, cells_current, dt_s
                )
            current_node_ids = self.graph.get_nodes_at_time(current_time)
            self._previous_scan = (current_time, current_node_ids)

        current_node_ids = self.graph.get_nodes_at_time(current_time)
        tracked_cells_df = self._build_tracked_cells_current(current_time, current_node_ids)
        cell_events_df = build_cell_events_dataframe(events)
        return tracked_cells_df, cell_events_df

    def get_cell_identity(self, track_index: int) -> tuple[str, str]:
        if track_index not in self._cell_identity:
            raise ValueError(f"Missing cell identity for track_index={track_index}")
        return self._cell_identity[track_index]

    # ------------------------------------------------------------------
    # Scan-gap classification (physical time)
    # ------------------------------------------------------------------

    def _gap_forces_reset(self, dt_s: float, prev_time, curr_time) -> bool:
        """Classify the inter-scan interval; log a structured code.

        Returns True when tracks must be terminated and restarted (non-monotonic
        time or a gap above the hard limit) — never raises, never matches across
        the gap. An irregular-but-monotonic cadence is a diagnostic warning only.
        """
        if dt_s <= 0:
            logger.error(
                "tracking_error code=%s prev=%s curr=%s dt_s=%.1f",
                TrackingError.NON_MONOTONIC_TIME.value,
                prev_time,
                curr_time,
                dt_s,
            )
            return True
        if dt_s > self.max_tracking_gap_s:
            logger.error(
                "tracking_error code=%s dt_minutes=%.1f limit_minutes=%.1f",
                TrackingError.TRACK_GAP_EXCEEDED.value,
                dt_s / 60.0,
                self.max_tracking_gap_minutes,
            )
            return True
        if self._prev_dt_s is not None and (
            dt_s > _CADENCE_IRREGULAR_RATIO * self._prev_dt_s
            or dt_s * _CADENCE_IRREGULAR_RATIO < self._prev_dt_s
        ):
            logger.warning(
                "tracking_error code=%s dt_s=%.1f prev_dt_s=%.1f",
                TrackingError.IRREGULAR_SCAN_CADENCE.value,
                dt_s,
                self._prev_dt_s,
            )
        return False

    def _reset_tracks(
        self, prev_node_ids: list[int], curr_time, cells_current: list[dict]
    ) -> list[dict]:
        """Terminate every active track, then start fresh tracks for the current cells."""
        events = [
            event_termination(self.graph, self._cell_identity, curr_time, node_id, None)
            for node_id in prev_node_ids
        ]
        self._initialize_tracks(curr_time, cells_current)
        for node_id in self.graph.get_nodes_at_time(curr_time):
            events.append(event_initiation(self.graph, self._cell_identity, curr_time, node_id))
        return events

    # ------------------------------------------------------------------
    # Cell extraction
    # ------------------------------------------------------------------

    @staticmethod
    def _to_epoch_seconds(time_val) -> float:
        ts = pd.Timestamp(time_val)
        if ts.tzinfo is None:
            ts = ts.tz_localize("UTC")
        return float(ts.timestamp())

    def _extract_cells_from_analyzer(
        self, ds: xr.Dataset, cell_stats_df: pd.DataFrame
    ) -> list[dict]:
        """Merge per-cell stats with masks; compute mass-weighted metric centroids.

        The tracker's own centroid is the **field-weighted** centroid of each mask
        (metres), so matching and motion are field-agnostic. Reflectivity-named stats
        are carried through only for identity and output columns.
        """
        labels = ds[self.labels_var].values
        field = ds[self.field_var].values

        cell_props_map: dict[int, dict] = {}
        for _, row in cell_stats_df.iterrows():
            lbl = int(row["cell_label"])
            cell_props_map[lbl] = {
                "area": float(row["cell_area_sqkm"]),
                "mean_reflectivity": float(row["radar_reflectivity_mean"]),
                "max_reflectivity": float(row["radar_reflectivity_max"]),
                "time_volume_start": row["time_volume_start"],
                "centroid_mass_lat": float(row["cell_centroid_mass_lat"]),
                "centroid_mass_lon": float(row["cell_centroid_mass_lon"]),
                "max_zdr": float(row["radar_differential_reflectivity_max"]),
                "area_40dbz_km2": float(row["area_40dbz_km2"]),
            }

        x_coords = np.asarray(ds.x.values, dtype=float)
        y_coords = np.asarray(ds.y.values, dtype=float)
        sx = float(x_coords[1] - x_coords[0])
        sy = float(y_coords[1] - y_coords[0])
        x0, y0 = float(x_coords[0]), float(y_coords[0])
        pixel_area_km2 = abs(sx * sy) / 1e6

        cells: list[dict] = []
        for cell_id in np.unique(labels):
            if cell_id == 0:
                continue
            if cell_id not in cell_props_map:
                logger.warning("Cell %d in labels but not in analyzer stats; skipping", cell_id)
                continue
            mask = labels == cell_id
            props = cell_props_map[cell_id]
            core_area_km2 = float(np.sum(mask & (field > self.core_threshold)) * pixel_area_km2)
            row_c, col_c = mass_weighted_centroid(field, mask)
            cells.append(
                {
                    "cell_id": int(cell_id),
                    "mask": mask,
                    "area": props["area"],
                    "area_px": float(np.count_nonzero(mask)),
                    "centroid_x": x0 + col_c * sx,  # mass-weighted, metres
                    "centroid_y": y0 + row_c * sy,
                    "mean_reflectivity": props["mean_reflectivity"],
                    "max_reflectivity": props["max_reflectivity"],
                    "core_area": core_area_km2,
                    "time_volume_start": props["time_volume_start"],
                    "centroid_mass_lat": props["centroid_mass_lat"],
                    "centroid_mass_lon": props["centroid_mass_lon"],
                    "max_zdr": props["max_zdr"],
                    "area_40dbz_km2": props["area_40dbz_km2"],
                }
            )
        return cells

    def _new_cell_identity(self, cell: dict) -> tuple[str, str]:
        max_zdr = float(cell["max_zdr"])
        if max_zdr < 0:
            max_zdr = 0.0
        signature = _track_signature_from_birth(
            scan_start_time_epoch_s=self._to_epoch_seconds(cell["time_volume_start"]),
            centroid_lat_deg=float(cell["centroid_mass_lat"]),
            centroid_lon_deg=float(cell["centroid_mass_lon"]),
            max_dbz=float(cell["max_reflectivity"]),
            max_zdr=max_zdr,
            area40_km2=float(cell["area_40dbz_km2"]),
            time_step_s=self.uid_time_step_s,
            latlon_step_deg=self.uid_latlon_step_deg,
            area_step_km2=self.uid_area_step_km2,
        )
        cell_uid = _cell_uid_from_signature(signature, width=self.uid_width)
        return cell_uid, signature

    # ------------------------------------------------------------------
    # Track initialisation helpers
    # ------------------------------------------------------------------

    def _initialize_tracks(self, time, cells: list[dict]) -> list[int]:
        node_ids = []
        for cell in cells:
            track_index = self.graph.get_new_track_index()
            cell_uid, track_signature = self._new_cell_identity(cell)
            self._cell_identity[track_index] = (cell_uid, track_signature)
            node_ids.append(self._add_cell_node(time, cell, track_index, cell_uid, track_signature))
        logger.debug("Initialized %d paths at time %s", len(cells), time)
        return node_ids

    def _add_cell_node(
        self,
        time,
        cell: dict,
        track_index: int,
        cell_uid: str | None = None,
        track_signature: str | None = None,
    ) -> int:
        if cell_uid is None or track_signature is None:
            cell_uid, track_signature = self.get_cell_identity(track_index)
        return self.graph.add_observation(
            time=time,
            cell_id=cell["cell_id"],
            track_index=track_index,
            area=cell["area"],
            centroid_x=cell["centroid_x"],
            centroid_y=cell["centroid_y"],
            mean_reflectivity=cell["mean_reflectivity"],
            max_reflectivity=cell["max_reflectivity"],
            core_area=cell["core_area"],
            cell_uid=cell_uid,
            track_signature=track_signature,
        )

    # ------------------------------------------------------------------
    # Frame-pair matching
    # ------------------------------------------------------------------

    def _update_motion(
        self, prev_node: int, curr_cell: dict, track_index: int, dt_s: float
    ) -> None:
        """Record a continuing track's kinematics for acceleration/heading checks."""
        prev_cx = float(self.graph.get_node_attr(prev_node, "centroid_x"))
        prev_cy = float(self.graph.get_node_attr(prev_node, "centroid_y"))
        vx = (float(curr_cell["centroid_x"]) - prev_cx) / dt_s
        vy = (float(curr_cell["centroid_y"]) - prev_cy) / dt_s
        self._track_motion[track_index] = TrackMotionState(
            speed=math.hypot(vx, vy),
            heading=math.atan2(vy, vx),
            has_velocity=True,
        )

    def _record_continue(
        self,
        i: int,
        c: int,
        all_prev_ids: list[int],
        curr_cells: list[dict],
        curr_time,
        dt_s: float,
        edge: "_EdgeCost",
        method: MatchMethod,
        matched_prev: dict[int, int],
        matched_curr: dict[int, int],
    ) -> dict:
        """Create the CONTINUE node/edge for prev row ``i`` ↔ curr col ``c``.

        Shared by the constraint-propagation and Hungarian paths. Records diagnostics
        from the pre-computed edge, updates the matched maps and the track's motion
        state; returns the event row.
        """
        prev_node = all_prev_ids[i]
        track_index = int(self.graph.get_node_attr(prev_node, "track_index") or 0)
        # Diagnostics use the track's prior motion — compute before _update_motion.
        diagnostics = self._match_diagnostics(prev_node, curr_cells[c], dt_s, edge, method)
        curr_node = self._add_cell_node(curr_time, curr_cells[c], track_index)
        self.graph.add_edge(prev_node, curr_node, edge_type="CONTINUE", cost=edge.cost)
        matched_prev[i] = curr_node
        matched_curr[c] = curr_node
        self._update_motion(prev_node, curr_cells[c], track_index, dt_s)
        return event_continue(
            self.graph, self._cell_identity, curr_time, prev_node, curr_node, edge.cost, diagnostics
        )

    def _match_diagnostics(
        self,
        prev_node: int,
        curr_cell: dict,
        dt_s: float,
        edge: "_EdgeCost",
        method: MatchMethod,
    ) -> MatchDiagnostics:
        """Assemble the per-match explainability record for an accepted CONTINUE."""
        prev_cx = float(self.graph.get_node_attr(prev_node, "centroid_x"))
        prev_cy = float(self.graph.get_node_attr(prev_node, "centroid_y"))
        prev_area = float(self.graph.get_node_attr(prev_node, "area"))
        track_index = int(self.graph.get_node_attr(prev_node, "track_index") or 0)

        heading_change_deg = None
        prev_state = self._track_motion.get(track_index)
        if prev_state is not None and prev_state.has_velocity:
            cand_heading = math.atan2(
                float(curr_cell["centroid_y"]) - prev_cy, float(curr_cell["centroid_x"]) - prev_cx
            )
            heading_change_deg = heading_change_degrees(prev_state.heading, cand_heading)

        curr_area = float(curr_cell["area"])
        return MatchDiagnostics(
            opc=edge.opc,
            ocp=edge.ocp,
            centroid_distance_m=edge.displacement,
            speed_ms=edge.displacement / dt_s,
            heading_change_deg=heading_change_deg,
            area_ratio=(curr_area / prev_area if prev_area > 0 else None),
            final_cost=edge.cost,
            match_method=method,
        )

    def _build_edges(
        self,
        all_prev_ids: list[int],
        curr_cells: list[dict],
        proj_labels: np.ndarray,
        ds: xr.Dataset,
        dt_s: float,
    ) -> dict[tuple[int, int], _EdgeCost]:
        """Generate candidates from buffered hulls and keep only validated edges.

        For every previous object the registration hull is dilated by the buffer,
        every intersecting current cell becomes a candidate, and a pair survives
        only if it clears the bidirectional-overlap gate and the kinematic gate.
        Returns ``{(prev_idx, curr_idx): _EdgeCost}`` with the geometry-first cost.
        """
        x_coords = np.asarray(ds.x.values, dtype=float)
        y_coords = np.asarray(ds.y.values, dtype=float)
        sx = float(x_coords[1] - x_coords[0])
        sy = float(y_coords[1] - y_coords[0])
        x0, y0 = float(x_coords[0]), float(y_coords[0])
        pixel_area_m2 = abs(sx * sy)
        buffer_pixels = buffer_pixels_from_km(self.buffer_km, min(abs(sx), abs(sy)))

        prev_hulls = [
            proj_labels == self.graph.get_node_attr(node_id, "cell_id") for node_id in all_prev_ids
        ]
        curr_masks = [cell["mask"] for cell in curr_cells]
        # Buffer only widens candidate recall; the overlap gate and cost are measured
        # against the true (un-buffered) projected hull so the buffer never dilutes them.
        _, pairs = CandidateGenerator(buffer_pixels).generate(prev_hulls, curr_masks)

        hull_area_px = [float(np.count_nonzero(h)) for h in prev_hulls]
        hull_centroid_m: list[tuple[float, float] | None] = []
        for hull in prev_hulls:
            if hull.any():
                row_c, col_c = mask_centroid(hull)
                hull_centroid_m.append((x0 + col_c * sx, y0 + row_c * sy))
            else:
                hull_centroid_m.append(None)

        edges: dict[tuple[int, int], _EdgeCost] = {}
        for i, j in pairs:
            result = self.validator.validate(prev_hulls[i], curr_masks[j])
            if not result.passed:
                continue

            prev_node = all_prev_ids[i]
            prev_cx = float(self.graph.get_node_attr(prev_node, "centroid_x"))
            prev_cy = float(self.graph.get_node_attr(prev_node, "centroid_y"))
            curr_cx = float(curr_cells[j]["centroid_x"])
            curr_cy = float(curr_cells[j]["centroid_y"])
            track_index = int(self.graph.get_node_attr(prev_node, "track_index") or 0)
            state = self._track_motion.get(track_index)
            previous_speed = state.speed if state and state.has_velocity else None

            decision = self.motion.check(prev_cx, prev_cy, curr_cx, curr_cy, dt_s, previous_speed)
            if not decision.ok:  # B3 kinematic gate
                logger.debug(
                    "tracking_error code=%s track=%d speed=%.1fm/s",
                    decision.code.value,
                    track_index,
                    decision.speed_ms,
                )
                continue

            hull_c = hull_centroid_m[i]
            displacement = math.hypot(curr_cx - hull_c[0], curr_cy - hull_c[1]) if hull_c else 0.0
            length_m = length_scale(
                self.length_scale_name,
                hull_area_px[i],
                curr_cells[j]["area_px"],
                pixel_area_m2,
                self.geometry_length_scale_km,
            )
            cost = pair_cost(result.opc, result.ocp, displacement, length_m)
            if self.heading_penalty_weight > 0.0 and state is not None and state.has_velocity:
                cand_heading = math.atan2(curr_cy - prev_cy, curr_cx - prev_cx)  # B5
                cost += self.heading_penalty_weight * heading_change_radians(
                    state.heading, cand_heading
                )
            edges[(i, j)] = _EdgeCost(result.opc, result.ocp, displacement, cost)
        return edges

    def _track_frame_pair(
        self,
        prev_node_ids: list[int],
        curr_time,
        ds_curr: xr.Dataset,
        curr_cells: list[dict],
        dt_s: float,
    ) -> list[dict]:
        # Without a registration frame we cannot match — reset (terminate + reinit).
        projections_missing = (
            "cell_projections" not in ds_curr.data_vars
            or ds_curr["cell_projections"].values.shape[0] < 1
        )
        if projections_missing:
            logger.warning("No cell_projections — resetting %d tracks", len(prev_node_ids))
            return self._reset_tracks(prev_node_ids, curr_time, curr_cells)

        # Registration hull at the minute nearest the real gap (falls back to
        # cell_projections[0] when minute frames are absent).
        proj_labels = select_registration_labels(ds_curr, dt_s)

        matched_prev: dict[int, int] = {}  # prev_idx → new curr node_id
        matched_curr: dict[int, int] = {}  # curr_idx → new curr node_id

        # 1. Candidate generation + hard gate → validated, costed edges.
        edge_costs = self._build_edges(prev_node_ids, curr_cells, proj_labels, ds_curr, dt_s)

        # 2. Deterministic propagation first; Hungarian only for ambiguous components.
        events = self._resolve_matches(
            edge_costs, prev_node_ids, curr_cells, curr_time, dt_s, matched_prev, matched_curr
        )

        dissipated = [prev_node_ids[i] for i in range(len(prev_node_ids)) if i not in matched_prev]
        born = [j for j in range(len(curr_cells)) if j not in matched_curr]

        # 3. Split / merge on the leftovers (behaviour preserved).
        split_born, split_events = self._detect_splits(
            prev_node_ids, curr_cells, proj_labels, matched_prev, born, curr_time
        )
        merged, merge_events = self._detect_merges(
            dissipated, curr_cells, proj_labels, matched_curr, curr_time
        )
        events += split_events + merge_events

        # 4. Births and terminations for everything still unmatched.
        events += self._emit_births(curr_cells, born, split_born, curr_time)
        events += self._emit_terminations(dissipated, merged, curr_time)

        logger.info(
            "Frame pair: prev=%d → continue=%d merged=%d dissipated=%d | born=%d split=%d",
            len(prev_node_ids),
            len(matched_prev),
            len(merged),
            len(dissipated) - len(merged),
            len(born) - len(split_born),
            len(split_born),
        )
        return events

    # ------------------------------------------------------------------
    # Matching, split/merge, births/terminations
    # ------------------------------------------------------------------

    def _resolve_matches(
        self,
        edge_costs: dict[tuple[int, int], _EdgeCost],
        prev_node_ids: list[int],
        curr_cells: list[dict],
        curr_time,
        dt_s: float,
        matched_prev: dict[int, int],
        matched_curr: dict[int, int],
    ) -> list[dict]:
        """Constraint propagation (PROPAGATED), then Hungarian per ambiguous component."""

        def record(i: int, c: int, method: MatchMethod) -> dict:
            return self._record_continue(
                i,
                c,
                prev_node_ids,
                curr_cells,
                curr_time,
                dt_s,
                edge_costs[(i, c)],
                method,
                matched_prev,
                matched_curr,
            )

        forced, remaining = ConstraintPropagator.resolve(list(edge_costs.keys()))
        events = [record(i, c, MatchMethod.PROPAGATED) for i, c in forced]

        component_costs = {edge: edge_costs[edge].cost for edge in remaining}
        for prevs, currs in AssignmentGraph(remaining).components():
            for i, c in HungarianMatcher.match(prevs, currs, component_costs):
                events.append(record(i, c, MatchMethod.HUNGARIAN))
        return events

    @staticmethod
    def _hull_overlap_fraction(proj_labels: np.ndarray, cell_id: int, mask: np.ndarray) -> float:
        """Fraction of a previous cell's projected hull covered by ``mask``."""
        proj_mask = proj_labels == cell_id
        denom = float(np.sum(proj_mask))
        return float(np.sum(mask & proj_mask)) / denom if denom else 0.0

    def _new_track_node(self, cell: dict, curr_time) -> int:
        """Allocate a new track index + identity and add the observation node."""
        new_index = self.graph.get_new_track_index()
        cell_uid, track_signature = self._new_cell_identity(cell)
        self._cell_identity[new_index] = (cell_uid, track_signature)
        return self._add_cell_node(curr_time, cell, new_index, cell_uid, track_signature)

    def _detect_splits(
        self,
        prev_node_ids: list[int],
        curr_cells: list[dict],
        proj_labels: np.ndarray,
        matched_prev: dict[int, int],
        born: list[int],
        curr_time,
    ) -> tuple[set[int], list[dict]]:
        """A born cell overlapping a continuing parent's projected hull is a SPLIT child."""
        split_born: set[int] = set()
        events: list[dict] = []
        for b_idx in born:
            b_mask = curr_cells[b_idx]["mask"]
            best_parent, best_overlap = None, 0.0
            for prev_idx, curr_node in matched_prev.items():
                cell_id = self.graph.get_node_attr(prev_node_ids[prev_idx], "cell_id")
                overlap = self._hull_overlap_fraction(proj_labels, cell_id, b_mask)
                if overlap >= self.split_overlap and overlap > best_overlap:
                    best_parent, best_overlap = curr_node, overlap
            if best_parent is not None:
                child_node = self._new_track_node(curr_cells[b_idx], curr_time)
                self.graph.add_edge(best_parent, child_node, edge_type="SPLIT", cost=0.0)
                split_born.add(b_idx)
                events.append(
                    event_split(self.graph, self._cell_identity, curr_time, best_parent, child_node)
                )
        return split_born, events

    def _detect_merges(
        self,
        dissipated: list[int],
        curr_cells: list[dict],
        proj_labels: np.ndarray,
        matched_curr: dict[int, int],
        curr_time,
    ) -> tuple[dict[int, int], list[dict]]:
        """A dissipated cell whose projected hull overlaps a continuing cell is a MERGE."""
        merged: dict[int, int] = {}
        events: list[dict] = []
        for d_node in dissipated:
            cell_id = self.graph.get_node_attr(d_node, "cell_id")
            best_target, best_overlap = None, 0.0
            for c_idx, curr_node in matched_curr.items():
                overlap = self._hull_overlap_fraction(
                    proj_labels, cell_id, curr_cells[c_idx]["mask"]
                )
                if overlap >= self.split_overlap and overlap > best_overlap:
                    best_target, best_overlap = curr_node, overlap
            if best_target is not None:
                self.graph.add_edge(d_node, best_target, edge_type="MERGE", cost=0.0)
                merged[d_node] = best_target
                events.append(
                    event_merge(self.graph, self._cell_identity, curr_time, d_node, best_target)
                )
        return merged, events

    def _emit_births(
        self, curr_cells: list[dict], born: list[int], split_born: set[int], curr_time
    ) -> list[dict]:
        """Initiate fresh tracks for born cells that were not claimed as split children."""
        events: list[dict] = []
        for b_idx in born:
            if b_idx in split_born:
                continue
            node_id = self._new_track_node(curr_cells[b_idx], curr_time)
            events.append(event_initiation(self.graph, self._cell_identity, curr_time, node_id))
        return events

    def _emit_terminations(
        self, dissipated: list[int], merged: dict[int, int], curr_time
    ) -> list[dict]:
        """Terminate every unmatched previous node (target = merge sink when merged)."""
        return [
            event_termination(
                self.graph, self._cell_identity, curr_time, d_node, merged.get(d_node)
            )
            for d_node in dissipated
        ]

    # ------------------------------------------------------------------
    # Scan-local builders (no per-track analytics)
    # ------------------------------------------------------------------

    def _build_tracked_cells_current(self, time, node_ids: list[int]) -> pd.DataFrame:
        rows: list[dict] = []
        for node_id in node_ids:
            node = self.graph.graph.nodes[node_id]
            time_val = normalize_time_scalar(node["time"])
            time_val = pd.Timestamp(time_val).to_datetime64()
            cell_uid = str(node["cell_uid"])
            rows.append(
                {
                    "time": time_val,
                    "cell_label": int(node["cell_id"]),
                    "cell_uid": cell_uid,
                    "area": float(node["area"]),
                    "centroid_x": float(node["centroid_x"]),
                    "centroid_y": float(node["centroid_y"]),
                    "mean_reflectivity": float(node["mean_reflectivity"]),
                    "max_reflectivity": float(node["max_reflectivity"]),
                    "core_area": float(node["core_area"]),
                }
            )
        df = pd.DataFrame(rows)
        if not df.empty:
            df["time"] = pd.to_datetime(df["time"])
            df = df.sort_values(["cell_uid", "cell_label"]).reset_index(drop=True)
        return df
