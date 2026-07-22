# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Track convective cells across consecutive radar scans using mask overlap and motion prediction.

This module implements a cell tracking algorithm inspired from TINT (Raut et al., 2021) with
following improvements.
- Motion-aware matching via projected label masks (no centroid-only matching)
- Projected mask overlap with current frame allows split and merge (registration frame)
- Split candidate: one cell → multiple cells (1 to N) in the projected area of a continuing parent
- Merge candidate: multiple cells → one cell (N to 1) in the projected area of a continuing child
- Explicit events (CONTINUE, SPLIT, MERGE, INITIATION, TERMINATION)

The tracker assigns a stable `cell_uid` to each tracked cell lifecycle (a connected chain of
cell observations across scans). This module does cell tracking only; any higher-level grouping
or aggregation is outside this module's scope.

Scan outputs:
1. **tracked_cells**: Per-observation rows for the current scan
2. **cell_events**: Explicit lineage/event rows for the current scan

Tracking state is stored in a directed graph structure (``graph.TrackingGraph``) with nodes
representing cell observations and edges representing temporal relationships. Cost-matrix
matching lives in ``matching.hungarian.MatchingEngine``; uid generation in ``identity``; event
rows in ``events``. ``RadarCellTracker`` here is orchestration only.

What is different from TINT:
- No centroid-only matching (uses full mask overlap + motion prediction)
- `cell_uid` values are hashes; lineage is represented by graph edges

Author: Bhupendra Raut, ANL.

References: Raut, B. A., Jackson, R., Picel, M., Collis, S. M., Bergemann, M., & Jakob, C.
(2021). An adaptive tracking algorithm for convection in simulated and remote sensing data.
Journal of Applied Meteorology and Climatology, 60(4), 513-526.
"""

import logging
import math

import numpy as np
import pandas as pd
import xarray as xr
from scipy.optimize import linear_sum_assignment

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
from adapt.modules.tracking.matching.hungarian import MatchingEngine
from adapt.modules.tracking.matching.overlap import OverlapMatcher, overlap_fraction
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
    "MatchingEngine",
    "RadarCellTracker",
    "TrackingGraph",
    "_cell_uid_from_signature",
    "_track_signature_from_birth",
]

logger = logging.getLogger(__name__)


class RadarCellTracker:
    """Track convective cells using projected masks, Hungarian matching, and
    area-overlap-based split/merge detection.

    Per-scan algorithm:
    1. Build cost matrix from cell_projections[0] (already projected hull)
    2. Pre-clamp: cost < match_cost → 0; cost > unmatch_cost → dummy_cost
    3. Pad to square; run Hungarian
    4. Post-filter at keep_cost: CONTINUE pairs vs dissipated/born
    5. Split: born cell overlaps CONTINUE parent hull >= split_overlap_threshold
    6. Merge: dissipated hull overlaps CONTINUE cell >= split_overlap_threshold
    7. True births: remaining unmatched born cells
    """

    def __init__(self, config):
        self.match_cost = config.match_cost
        self.keep_cost = config.keep_cost
        self.unmatch_cost = config.unmatch_cost
        self.split_overlap = config.split_overlap
        self.core_threshold = config.core_reflectivity_threshold
        self.refl_var = config.reflectivity_var
        self.labels_var = config.labels_var
        self.uid_time_step_s = config.uid_time_step_s
        self.uid_latlon_step_deg = config.uid_latlon_step_deg
        self.uid_area_step_km2 = config.uid_area_step_km2
        self.uid_width = config.uid_width

        self.max_gap_s: float = config.max_gap_minutes * 60.0
        self.max_tracking_gap_minutes: float = config.max_tracking_gap_minutes
        self.max_tracking_gap_s: float = config.max_tracking_gap_minutes * 60.0

        self.graph = TrackingGraph()
        self.matcher = MatchingEngine(config)
        self.overlap_matcher = OverlapMatcher(config.overlap_match_threshold)
        self.motion = MotionValidator(config.max_speed_ms, config.max_speed_multiplier)
        self.heading_penalty_weight = config.heading_change_penalty_weight
        self._previous_scan: tuple | None = None  # (time, ds, node_ids)
        self._cell_identity: dict[int, tuple[str, str]] = {}
        self._dormant_nodes: dict[int, float] = {}  # node_id → last_seen_epoch_s
        self._track_motion: dict[int, TrackMotionState] = {}  # track_index → kinematics
        self._prev_dt_s: float | None = None  # last good scan interval (cadence check)

        logger.info(
            "RadarCellTracker initialized: match=%.2f keep=%.2f unmatch=%.2f overlap=%.2f"
            " max_gap=%.1fmin expected_speed=%.1fm/s",
            self.match_cost,
            self.keep_cost,
            self.unmatch_cost,
            self.split_overlap,
            config.max_gap_minutes,
            config.expected_speed_ms,
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
        current_time = self._get_time(ds_projected)
        cells_current = self._extract_cells_from_analyzer(ds_projected, cell_stats_df)

        events: list[dict] = []
        if self._previous_scan is None:
            node_ids = self._initialize_tracks(current_time, cells_current)
            self._previous_scan = (current_time, ds_projected, node_ids)
            for node_id in node_ids:
                events.append(
                    event_initiation(self.graph, self._cell_identity, current_time, node_id)
                )
        else:
            prev_time, ds_prev, prev_node_ids = self._previous_scan
            prev_time_epoch = self._to_epoch_seconds(prev_time)
            curr_time_epoch = self._to_epoch_seconds(current_time)
            dt_s = curr_time_epoch - prev_time_epoch
            if self._gap_forces_reset(dt_s, prev_time, current_time):
                events = self._reset_tracks(prev_node_ids, current_time, cells_current)
            else:
                self._prev_dt_s = dt_s
                events = self._track_frame_pair(
                    prev_time,
                    ds_prev,
                    prev_node_ids,
                    current_time,
                    ds_projected,
                    cells_current,
                    dt_s,
                )
            current_node_ids = self.graph.get_nodes_at_time(current_time)
            self._previous_scan = (current_time, ds_projected, current_node_ids)

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
        """Terminate every active and dormant track, then start fresh tracks."""
        events: list[dict] = []
        for node_id in prev_node_ids:
            events.append(
                event_termination(self.graph, self._cell_identity, curr_time, node_id, None)
            )
        for node_id in list(self._dormant_nodes):
            events.append(
                event_termination(self.graph, self._cell_identity, curr_time, node_id, None)
            )
        self._dormant_nodes.clear()
        self._initialize_tracks(curr_time, cells_current)
        for node_id in self.graph.get_nodes_at_time(curr_time):
            events.append(event_initiation(self.graph, self._cell_identity, curr_time, node_id))
        return events

    # ------------------------------------------------------------------
    # Cell extraction
    # ------------------------------------------------------------------

    def _get_time(self, ds: xr.Dataset):
        return normalize_time_scalar(ds.time.values)

    @staticmethod
    def _to_epoch_seconds(time_val) -> float:
        ts = pd.Timestamp(time_val)
        if ts.tzinfo is None:
            ts = ts.tz_localize("UTC")
        return float(ts.timestamp())

    def _extract_cells_from_analyzer(
        self, ds: xr.Dataset, cell_stats_df: pd.DataFrame
    ) -> list[dict]:
        """Merge per-cell stats (from AnalysisModule) with segmentation masks."""
        labels = ds[self.labels_var].values

        cell_props_map: dict[int, dict] = {}
        for _, row in cell_stats_df.iterrows():
            lbl = int(row["cell_label"])
            cell_props_map[lbl] = {
                "area": float(row["cell_area_sqkm"]),
                "centroid_x": float(row["cell_centroid_geom_x"]),
                "centroid_y": float(row["cell_centroid_geom_y"]),
                "mean_reflectivity": float(row["radar_reflectivity_mean"]),
                "max_reflectivity": float(row["radar_reflectivity_max"]),
                "time_volume_start": row["time_volume_start"],
                "centroid_mass_lat": float(row["cell_centroid_mass_lat"]),
                "centroid_mass_lon": float(row["cell_centroid_mass_lon"]),
                "max_zdr": float(row["radar_differential_reflectivity_max"]),
                "area_40dbz_km2": float(row["area_40dbz_km2"]),
            }

        refl = ds[self.refl_var].values
        dx = float(np.abs(ds.x[1] - ds.x[0]))
        dy = float(np.abs(ds.y[1] - ds.y[0]))
        pixel_area_km2 = (dx * dy) / 1e6

        cells: list[dict] = []
        for cell_id in np.unique(labels):
            if cell_id == 0:
                continue
            if cell_id not in cell_props_map:
                logger.warning("Cell %d in labels but not in analyzer stats; skipping", cell_id)
                continue
            mask = labels == cell_id
            props = cell_props_map[cell_id]
            core_area_km2 = float(np.sum(mask & (refl > self.core_threshold)) * pixel_area_km2)
            cells.append(
                {
                    "cell_id": int(cell_id),
                    "mask": mask,
                    "area": props["area"],
                    "centroid_x": props["centroid_x"],
                    "centroid_y": props["centroid_y"],
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

    def _reject_unphysical(
        self,
        all_prev_ids: list[int],
        curr_cells: list[dict],
        raw: np.ndarray,
        dummy_cost: float,
        dt_s: float,
    ) -> None:
        """Set candidate pairs that violate hard kinematic limits to ``dummy_cost``.

        Mutates ``raw`` in place so rejected pairs cannot be assigned. Only pairs
        that actually overlap (cost below ``dummy_cost``) are checked.
        """
        for i, prev_node in enumerate(all_prev_ids):
            prev_cx = self.graph.get_node_attr(prev_node, "centroid_x")
            prev_cy = self.graph.get_node_attr(prev_node, "centroid_y")
            track_index = int(self.graph.get_node_attr(prev_node, "track_index") or 0)
            prev_state = self._track_motion.get(track_index)
            previous_speed = prev_state.speed if prev_state and prev_state.has_velocity else None
            for j, curr_cell in enumerate(curr_cells):
                if raw[i, j] >= dummy_cost:
                    continue  # no overlap — not a candidate
                decision = self.motion.check(
                    prev_cx,
                    prev_cy,
                    curr_cell["centroid_x"],
                    curr_cell["centroid_y"],
                    dt_s,
                    previous_speed,
                )
                if not decision.ok:
                    raw[i, j] = dummy_cost
                    logger.debug(
                        "tracking_error code=%s track=%d speed=%.1fm/s",
                        decision.code.value,
                        track_index,
                        decision.speed_ms,
                    )

    def _apply_motion_penalty(
        self,
        all_prev_ids: list[int],
        curr_cells: list[dict],
        raw: np.ndarray,
        dummy_cost: float,
    ) -> None:
        """Bias Hungarian away from heading-inconsistent matches (crossing tracks).

        Adds ``heading_change_penalty_weight × heading_change`` (radians) to each
        candidate pair whose track has an established velocity. Soft penalty — never
        a rejection — and a no-op when the weight is 0.
        """
        if self.heading_penalty_weight <= 0.0:
            return
        for i, prev_node in enumerate(all_prev_ids):
            track_index = int(self.graph.get_node_attr(prev_node, "track_index") or 0)
            state = self._track_motion.get(track_index)
            if state is None or not state.has_velocity:
                continue
            prev_cx = float(self.graph.get_node_attr(prev_node, "centroid_x"))
            prev_cy = float(self.graph.get_node_attr(prev_node, "centroid_y"))
            for j, curr_cell in enumerate(curr_cells):
                if raw[i, j] >= dummy_cost:
                    continue  # not a candidate
                cand_heading = math.atan2(
                    curr_cell["centroid_y"] - prev_cy, curr_cell["centroid_x"] - prev_cx
                )
                raw[i, j] += self.heading_penalty_weight * heading_change_radians(
                    state.heading, cand_heading
                )

    def _update_motion(
        self, prev_node: int, curr_cell: dict, track_index: int, dt_s: float
    ) -> None:
        """Record a continuing track's kinematics for prediction and accel checks."""
        prev_cx = float(self.graph.get_node_attr(prev_node, "centroid_x"))
        prev_cy = float(self.graph.get_node_attr(prev_node, "centroid_y"))
        curr_cx = float(curr_cell["centroid_x"])
        curr_cy = float(curr_cell["centroid_y"])
        vx = (curr_cx - prev_cx) / dt_s
        vy = (curr_cy - prev_cy) / dt_s
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
        cost: float,
        dt_s: float,
        proj_labels: np.ndarray,
        method: MatchMethod,
        matched_prev: dict[int, int],
        matched_curr: dict[int, int],
    ) -> dict:
        """Create the CONTINUE node/edge for prev row ``i`` ↔ curr col ``c``.

        Shared by the overlap-first and Hungarian paths. Records diagnostics,
        updates the matched maps, the track's motion state, and the dormant set;
        returns the event row.
        """
        prev_node = all_prev_ids[i]
        track_index = int(self.graph.get_node_attr(prev_node, "track_index") or 0)
        # Diagnostics use the track's prior motion — compute before _update_motion.
        diagnostics = self._match_diagnostics(
            prev_node, curr_cells[c], proj_labels, cost, dt_s, method
        )
        curr_node = self._add_cell_node(curr_time, curr_cells[c], track_index)
        self.graph.add_edge(prev_node, curr_node, edge_type="CONTINUE", cost=cost)
        matched_prev[i] = curr_node
        matched_curr[c] = curr_node
        self._update_motion(prev_node, curr_cells[c], track_index, dt_s)
        if prev_node in self._dormant_nodes:  # dormant node re-acquired
            del self._dormant_nodes[prev_node]
        return event_continue(
            self.graph, self._cell_identity, curr_time, prev_node, curr_node, cost, diagnostics
        )

    def _match_diagnostics(
        self,
        prev_node: int,
        curr_cell: dict,
        proj_labels: np.ndarray,
        cost: float,
        dt_s: float,
        method: MatchMethod,
    ) -> MatchDiagnostics:
        """Assemble the per-match explainability record for an accepted CONTINUE."""
        prev_cx = float(self.graph.get_node_attr(prev_node, "centroid_x"))
        prev_cy = float(self.graph.get_node_attr(prev_node, "centroid_y"))
        prev_area = float(self.graph.get_node_attr(prev_node, "area"))
        prev_refl = float(self.graph.get_node_attr(prev_node, "mean_reflectivity"))
        track_index = int(self.graph.get_node_attr(prev_node, "track_index") or 0)

        proj_mask = proj_labels == self.graph.get_node_attr(prev_node, "cell_id")
        curr_mask = curr_cell["mask"]
        union = float(np.sum(proj_mask | curr_mask))
        iou = float(np.sum(proj_mask & curr_mask)) / union if union > 0 else 0.0

        curr_cx = float(curr_cell["centroid_x"])
        curr_cy = float(curr_cell["centroid_y"])
        dist = math.hypot(curr_cx - prev_cx, curr_cy - prev_cy)

        heading_change_deg = None
        prev_state = self._track_motion.get(track_index)
        if prev_state is not None and prev_state.has_velocity:
            cand_heading = math.atan2(curr_cy - prev_cy, curr_cx - prev_cx)
            heading_change_deg = heading_change_degrees(prev_state.heading, cand_heading)

        curr_area = float(curr_cell["area"])
        return MatchDiagnostics(
            overlap=overlap_fraction(proj_mask, curr_mask),
            iou=iou,
            centroid_distance_m=dist,
            speed_ms=dist / dt_s,
            heading_change_deg=heading_change_deg,
            area_ratio=(curr_area / prev_area if prev_area > 0 else None),
            reflectivity_difference=abs(float(curr_cell["mean_reflectivity"]) - prev_refl),
            final_cost=cost,
            match_method=method,
        )

    def _track_frame_pair(
        self,
        prev_time,
        ds_prev: xr.Dataset,
        prev_node_ids: list[int],
        curr_time,
        ds_curr: xr.Dataset,
        curr_cells: list[dict],
        dt_s: float,
    ) -> list[dict]:
        events: list[dict] = []
        curr_time_epoch = self._to_epoch_seconds(curr_time)
        prev_time_epoch = self._to_epoch_seconds(prev_time)

        # ── Missing/empty projections — gap survival ──────────────────────
        projections_missing = (
            "cell_projections" not in ds_curr.data_vars
            or ds_curr["cell_projections"].values.shape[0] < 1
        )
        if projections_missing:
            logger.warning(
                "No cell_projections — deferring %d tracks (gap survival)", len(prev_node_ids)
            )
            # Expire dormant nodes that have already exceeded the gap limit
            for node_id, last_seen in list(self._dormant_nodes.items()):
                if curr_time_epoch - last_seen > self.max_gap_s:
                    events.append(
                        event_termination(self.graph, self._cell_identity, curr_time, node_id, None)
                    )
                    del self._dormant_nodes[node_id]
            # Move active prev nodes into dormant; preserve last_seen as prev scan time
            for node_id in prev_node_ids:
                if node_id not in self._dormant_nodes:
                    self._dormant_nodes[node_id] = prev_time_epoch
            # Current cells become new tracks
            self._initialize_tracks(curr_time, curr_cells)
            for node_id in self.graph.get_nodes_at_time(curr_time):
                events.append(event_initiation(self.graph, self._cell_identity, curr_time, node_id))
            return events

        # Registration hull at the minute nearest the real gap (falls back to the
        # whole-step cell_projections[0] when minute frames are absent).
        proj_labels = select_registration_labels(ds_curr, dt_s)

        # ── Expire dormant nodes beyond the gap limit ─────────────────────
        for node_id, last_seen in list(self._dormant_nodes.items()):
            if curr_time_epoch - last_seen > self.max_gap_s:
                events.append(
                    event_termination(self.graph, self._cell_identity, curr_time, node_id, None)
                )
                del self._dormant_nodes[node_id]

        # ── Include surviving dormant nodes in the matching pool ──────────
        dormant_ids = list(self._dormant_nodes.keys())
        all_prev_ids = prev_node_ids + dormant_ids
        n_all = len(all_prev_ids)
        n_curr = len(curr_cells)

        if n_all == 0:
            self._initialize_tracks(curr_time, curr_cells)
            for node_id in self.graph.get_nodes_at_time(curr_time):
                events.append(event_initiation(self.graph, self._cell_identity, curr_time, node_id))
            return events
        if n_curr == 0:
            # All cells dissipated — terminate every node including dormant
            for d_node in all_prev_ids:
                events.append(
                    event_termination(self.graph, self._cell_identity, curr_time, d_node, None)
                )
            self._dormant_nodes.clear()
            return events

        dummy_cost = self.unmatch_cost * 50.0

        # ── Step 1: raw cost matrix ────────────────────────────────────────
        raw = self.matcher.compute_cost_matrix(
            all_prev_ids,
            self.graph,
            proj_labels,
            curr_cells,
            dummy_cost,
            dt_s,
        )

        # ── Step 1b: hard physical-motion rejection (before matching) ─────
        self._reject_unphysical(all_prev_ids, curr_cells, raw, dummy_cost, dt_s)

        # ── Step 1b′: soft heading-consistency penalty (crossing prevention) ─
        self._apply_motion_penalty(all_prev_ids, curr_cells, raw, dummy_cost)

        matched_prev: dict[int, int] = {}  # row_idx → new curr node_id
        matched_curr: dict[int, int] = {}  # curr_idx → new curr node_id
        n_continue = 0

        # ── Step 1c: deterministic overlap-first matching ─────────────────
        # Mutually-unique parent↔child overlaps are matched directly and pulled
        # out of the Hungarian pool — uniqueness dominates the overlap threshold.
        prev_hulls = [
            proj_labels == self.graph.get_node_attr(node_id, "cell_id") for node_id in all_prev_ids
        ]
        curr_masks = [cell["mask"] for cell in curr_cells]
        allowed = raw < dummy_cost
        for i, c in self.overlap_matcher.unique_matches(prev_hulls, curr_masks, allowed):
            events.append(
                self._record_continue(
                    i,
                    c,
                    all_prev_ids,
                    curr_cells,
                    curr_time,
                    float(raw[i, c]),
                    dt_s,
                    proj_labels,
                    MatchMethod.OVERLAP,
                    matched_prev,
                    matched_curr,
                )
            )
            n_continue += 1
            raw[i, :] = dummy_cost  # remove this parent and child from the pool
            raw[:, c] = dummy_cost

        # ── Step 2: pre-clamp ─────────────────────────────────────────────
        raw[raw < self.match_cost] = 0.0
        raw[raw > self.unmatch_cost] = dummy_cost

        # ── Step 3: pad to square ─────────────────────────────────────────
        n = max(n_all, n_curr)
        square = np.full((n, n), dummy_cost, dtype=float)
        square[:n_all, :n_curr] = raw

        # ── Step 4: Hungarian (residual ambiguity only) ──────────────────
        row_ind, col_ind = linear_sum_assignment(square)

        # ── Step 5: post-filter → CONTINUE / dissipated / born ───────────
        for r, c in zip(row_ind, col_ind, strict=False):
            if r >= n_all or c >= n_curr:
                continue  # dummy slot
            if r in matched_prev or c in matched_curr:
                continue  # already resolved by overlap-first
            if square[r, c] <= self.keep_cost:
                events.append(
                    self._record_continue(
                        r,
                        c,
                        all_prev_ids,
                        curr_cells,
                        curr_time,
                        float(square[r, c]),
                        dt_s,
                        proj_labels,
                        MatchMethod.HUNGARIAN,
                        matched_prev,
                        matched_curr,
                    )
                )
                n_continue += 1

        # Dissipated = unmatched nodes; split by active vs dormant
        all_dissipated = [all_prev_ids[i] for i in range(n_all) if i not in matched_prev]
        dissipated_active = [nd for nd in all_dissipated if nd not in self._dormant_nodes]
        # Dormant nodes that failed to match stay in _dormant_nodes — no termination event yet

        born_indices = [i for i in range(n_curr) if i not in matched_curr]

        # ── Step 6: split detection ───────────────────────────────────────
        # Born cell overlaps a CONTINUE parent's projected hull >= threshold
        split_born: set[int] = set()
        for b_idx in born_indices:
            b_mask = curr_cells[b_idx]["mask"]
            best_parent = None
            best_overlap = 0.0
            for prev_idx, curr_node in matched_prev.items():
                prev_node = all_prev_ids[prev_idx]
                prev_cell_id = self.graph.get_node_attr(prev_node, "cell_id")
                proj_mask = proj_labels == prev_cell_id
                denom = float(np.sum(proj_mask))
                if denom == 0:
                    continue
                overlap_frac = float(np.sum(b_mask & proj_mask)) / denom
                if overlap_frac >= self.split_overlap and overlap_frac > best_overlap:
                    best_parent = curr_node  # current-frame node of the continuing parent
                    best_overlap = overlap_frac
            if best_parent is not None:
                parent_track_index = self.graph.get_node_attr(best_parent, "track_index")
                new_index = self.graph.get_new_track_index()
                cell_uid, track_signature = self._new_cell_identity(curr_cells[b_idx])
                self._cell_identity[new_index] = (cell_uid, track_signature)
                child_node = self._add_cell_node(
                    curr_time, curr_cells[b_idx], new_index, cell_uid, track_signature
                )
                self.graph.add_edge(best_parent, child_node, edge_type="SPLIT", cost=0.0)
                split_born.add(b_idx)
                events.append(
                    event_split(self.graph, self._cell_identity, curr_time, best_parent, child_node)
                )
                logger.debug(
                    "SPLIT: track %d → new track %d (overlap=%.2f)",
                    parent_track_index,
                    new_index,
                    best_overlap,
                )

        # ── Step 7: merge detection ───────────────────────────────────────
        # Dissipated active hull overlaps a CONTINUE cell >= threshold
        n_merge = 0
        merged_nodes: dict[int, int] = {}
        for d_node in dissipated_active:
            d_cell_id = self.graph.get_node_attr(d_node, "cell_id")
            proj_mask = proj_labels == d_cell_id
            denom = float(np.sum(proj_mask))
            if denom == 0:
                continue
            best_target = None
            best_overlap = 0.0
            for c_idx, curr_node in matched_curr.items():
                overlap_frac = float(np.sum(proj_mask & curr_cells[c_idx]["mask"])) / denom
                if overlap_frac >= self.split_overlap and overlap_frac > best_overlap:
                    best_target = curr_node
                    best_overlap = overlap_frac
            if best_target is not None:
                self.graph.add_edge(d_node, best_target, edge_type="MERGE", cost=0.0)
                n_merge += 1
                merged_nodes[d_node] = best_target
                events.append(
                    event_merge(self.graph, self._cell_identity, curr_time, d_node, best_target)
                )
                logger.debug(
                    "MERGE: track %d → track %d (overlap=%.2f)",
                    self.graph.get_node_attr(d_node, "track_index"),
                    self.graph.get_node_attr(best_target, "track_index"),
                    best_overlap,
                )

        # ── Step 8: true new births ───────────────────────────────────────
        n_births = 0
        for b_idx in born_indices:
            if b_idx not in split_born:
                new_index = self.graph.get_new_track_index()
                cell_uid, track_signature = self._new_cell_identity(curr_cells[b_idx])
                self._cell_identity[new_index] = (cell_uid, track_signature)
                node_id = self._add_cell_node(
                    curr_time, curr_cells[b_idx], new_index, cell_uid, track_signature
                )
                n_births += 1
                events.append(event_initiation(self.graph, self._cell_identity, curr_time, node_id))

        for d_node in dissipated_active:
            if d_node in merged_nodes:
                events.append(
                    event_termination(
                        self.graph, self._cell_identity, curr_time, d_node, merged_nodes[d_node]
                    )
                )
            else:
                events.append(
                    event_termination(self.graph, self._cell_identity, curr_time, d_node, None)
                )

        n_split = len(split_born)
        n_dissipated = len(dissipated_active) - n_merge
        n_alive = n_continue + n_births + n_split  # == n_curr
        logger.info(
            "Frame pair: prev=%d dormant_in=%d → continue=%d, dissipated=%d, merged=%d"
            " | born=%d, split=%d | alive=%d | still_dormant=%d",
            len(prev_node_ids),
            len(dormant_ids),
            n_continue,
            n_dissipated,
            n_merge,
            n_births,
            n_split,
            n_alive,
            len(self._dormant_nodes),
        )
        return events

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
