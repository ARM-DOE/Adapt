# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Track convective cells across consecutive radar scans using mask overlap and motion prediction.

This module implements a cell tracking algorithm inspired from TINT (Raut et al., 2021) with following improvements.
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

Tracking state is stored in a directed graph structure with nodes representing cell observations and edges representing temporal relationships.

What is different from TINT:
- No centroid-only matching (uses full mask overlap + motion prediction)
- `cell_uid` values are hashes; lineage is represented by graph edges

Author: Bhupendra Raut, ANL.

References: Raut, B. A., Jackson, R., Picel, M., Collis, S. M., Bergemann, M., & Jakob, C. (2021). An adaptive tracking algorithm for convection in simulated and remote sensing data. Journal of Applied Meteorology and Climatology, 60(4), 513-526.
"""

import logging
import hashlib
import string
import numpy as np
import pandas as pd
import xarray as xr
import networkx as nx
from typing import TYPE_CHECKING, Dict, List, Tuple, Optional
from scipy.optimize import linear_sum_assignment

if TYPE_CHECKING:
    from adapt.configuration.schemas import InternalConfig

__all__ = ['RadarCellTracker', 'TrackingModule']

logger = logging.getLogger(__name__)

BASE36_UPPER = string.digits + string.ascii_uppercase


def _quantize(value: float, step: float) -> int:
    # this is for creating stable hashes that are robust to small variations in the input values
    if step <= 0:
        raise ValueError("step must be positive")
    return int(round(value / step))


def _encode_base36(value: int) -> str:
    if value < 0:
        raise ValueError("value must be non-negative")
    if value == 0:
        return "0"
    chars: list[str] = []
    while value:
        value, remainder = divmod(value, 36)
        chars.append(BASE36_UPPER[remainder])
    return "".join(reversed(chars))


def _encode_base36_fixed(value: int, width: int) -> str:
    token = _encode_base36(value)
    return token.rjust(width, "0")


def _track_signature_from_birth(
    scan_start_time_epoch_s: float,
    centroid_lat_deg: float,
    centroid_lon_deg: float,
    max_dbz: float,
    max_zdr: float,
    area40_km2: float,
    *,
    time_step_s: int,
    latlon_step_deg: float,
    area_step_km2: float,
    signature_version: str = "v1",
) -> str:
    tq = _quantize(scan_start_time_epoch_s, time_step_s)
    latq = _quantize(centroid_lat_deg, latlon_step_deg)
    lonq = _quantize(centroid_lon_deg, latlon_step_deg)
    dbzq = int(round(max_dbz))
    zdrq = int(round(max_zdr * 10.0))
    a40q = _quantize(area40_km2, area_step_km2)
    return f"{signature_version}|{tq}|{latq}|{lonq}|{dbzq}|{zdrq}|{a40q}"


def _cell_uid_from_signature(signature: str, width: int) -> str:
    digest = hashlib.blake2b(signature.encode("utf-8"), digest_size=8).digest()
    value64 = int.from_bytes(digest, byteorder="big", signed=False)
    modulus = 36 ** width
    return _encode_base36_fixed(value64 % modulus, width=width)


# =============================================================================
# Tracking Graph Structure
# =============================================================================

class TrackingGraph:
    """Directed graph storing cell tracking history and lineage.

    Nodes represent cell observations at specific times.
    Edges represent temporal relationships (CONTINUE, SPLIT, MERGE).

    Node attributes:
        - node_id: unique identifier (int)
        - time: observation timestamp
        - cell_id: cell label from segmentation
        - track_index: tracking index this cell belongs to (starts at 1; 0 = background sentinel)
        - area: cell area in km²
        - centroid_x, centroid_y: cell center coordinates
        - mean_reflectivity: average dBZ
        - max_reflectivity: peak dBZ
        - core_area: area with Z > threshold dBZ

    Edge attributes:
        - edge_type: "CONTINUE", "SPLIT", "MERGE"
        - cost: assignment cost (for diagnostics)
    """

    def __init__(self):
        """Initialize empty tracking graph."""
        self.graph = nx.DiGraph()
        self._node_counter = 0
        self._track_counter = 0  # Will yield 1, 2, 3, ... (0 is background sentinel)

    def add_observation(
        self,
        time,
        cell_id: int,
        track_index: int,
        area: float,
        centroid_x: float,
        centroid_y: float,
        mean_reflectivity: float,
        max_reflectivity: float,
        core_area: float,
        cell_uid: str,
        track_signature: str,
    ) -> int:
        node_id = self._node_counter
        self._node_counter += 1

        self.graph.add_node(
            node_id,
            time=time,
            cell_id=cell_id,
            track_index=track_index,
            area=area,
            centroid_x=centroid_x,
            centroid_y=centroid_y,
            mean_reflectivity=mean_reflectivity,
            max_reflectivity=max_reflectivity,
            core_area=core_area,
            cell_uid=cell_uid,
            track_signature=track_signature,
        )
        return node_id

    def add_edge(self, from_node: int, to_node: int, edge_type: str, cost: float = 0.0):
        """Add a temporal relationship edge.

        Parameters
        ----------
        from_node : int
            Source node ID (earlier time)
        to_node : int
            Target node ID (later time)
        edge_type : str
            Edge type: "CONTINUE", "SPLIT", or "MERGE"
        cost : float, optional
            Assignment cost for diagnostics (default: 0.0)
        """
        self.graph.add_edge(from_node, to_node, edge_type=edge_type, cost=cost)

    def get_new_track_index(self) -> int:
        """Allocate a new unique track index (starts at 1; 0 is background sentinel)."""
        self._track_counter += 1
        return self._track_counter

    def get_node_attr(self, node_id: int, attr: str):
        """Get a node attribute value.

        Parameters
        ----------
        node_id : int
            Node identifier
        attr : str
            Attribute name

        Returns
        -------
        Any
            Attribute value, or None if not present
        """
        return self.graph.nodes[node_id].get(attr)

    def get_nodes_at_time(self, time) -> List[int]:
        """Get all node IDs for a given timestamp.

        Parameters
        ----------
        time : datetime-like
            Timestamp to query

        Returns
        -------
        List[int]
            List of node IDs at this time
        """
        return [n for n, d in self.graph.nodes(data=True) if d.get('time') == time]

    def get_track_nodes(self, track_index: int) -> List[int]:
        """Get all nodes belonging to a track, sorted by time."""
        nodes = [(n, d['time']) for n, d in self.graph.nodes(data=True)
                 if d.get('track_index') == track_index]
        nodes.sort(key=lambda x: x[1])
        return [n for n, _ in nodes]

    def get_predecessors(self, node_id: int) -> List[Tuple[int, str]]:
        """Get predecessor nodes with their edge types.

        Parameters
        ----------
        node_id : int
            Node identifier

        Returns
        -------
        List[Tuple[int, str]]
            List of (predecessor_node_id, edge_type) tuples
        """
        return [(pred, self.graph.edges[pred, node_id]['edge_type'])
                for pred in self.graph.predecessors(node_id)]

    def get_successors(self, node_id: int) -> List[Tuple[int, str]]:
        """Get successor nodes with their edge types.

        Parameters
        ----------
        node_id : int
            Node identifier

        Returns
        -------
        List[Tuple[int, str]]
            List of (successor_node_id, edge_type) tuples
        """
        return [(succ, self.graph.edges[node_id, succ]['edge_type'])
                for succ in self.graph.successors(node_id)]


# =============================================================================
# Matching Engine
# =============================================================================

class MatchingEngine:
    """Cost matrix builder using projected masks (cell_projections[0] is already the hull)."""

    def __init__(self, config: "InternalConfig"):
        self.core_threshold = config.tracker.core_reflectivity_threshold

    def compute_cost_matrix(
        self,
        prev_node_ids: List[int],
        graph: "TrackingGraph",
        proj_labels: np.ndarray,
        curr_cells: List[Dict],
        dummy_cost: float,
    ) -> np.ndarray:
        """Build (n_prev × n_curr) cost matrix.

        Uses cell_projections[0] directly as the projected hull — no recomputation.
        Pairs with no spatial overlap receive dummy_cost.
        """
        n_prev = len(prev_node_ids)
        n_curr = len(curr_cells)
        cost_matrix = np.full((n_prev, n_curr), dummy_cost, dtype=float)

        for prev_idx, prev_node in enumerate(prev_node_ids):
            prev_cell_id = graph.get_node_attr(prev_node, 'cell_id')
            proj_mask = (proj_labels == prev_cell_id)
            if not np.any(proj_mask):
                continue  # cell left the frame
            for curr_idx, curr_cell in enumerate(curr_cells):
                if np.any(proj_mask & curr_cell['mask']):
                    cost_matrix[prev_idx, curr_idx] = self._compute_cost(
                        prev_node, graph, proj_mask, curr_cell
                    )

        return cost_matrix

    def _compute_cost(
        self,
        prev_node: int,
        graph: "TrackingGraph",
        proj_mask: np.ndarray,
        curr_cell: Dict,
    ) -> float:
        """5-term cost: 0.4*Dpos + 0.3*(1-IoU) + 0.15*|log(A2/A1)| + 0.1*|Z2-Z1|/50"""
        prev_cx   = graph.get_node_attr(prev_node, 'centroid_x')
        prev_cy   = graph.get_node_attr(prev_node, 'centroid_y')
        prev_area = graph.get_node_attr(prev_node, 'area')
        prev_refl = graph.get_node_attr(prev_node, 'mean_reflectivity')

        curr_mask = curr_cell['mask']
        H, W      = proj_mask.shape
        diagonal  = np.sqrt(float(H**2 + W**2))
        dist      = np.sqrt(
            (curr_cell['centroid_x'] - prev_cx) ** 2 +
            (curr_cell['centroid_y'] - prev_cy) ** 2
        )
        D_pos = dist / diagonal

        union = np.sum(proj_mask | curr_mask)
        IoU   = float(np.sum(proj_mask & curr_mask)) / union if union > 0 else 0.0

        curr_area = curr_cell['area']
        area_diff = (
            float(np.abs(np.log(curr_area / prev_area)))
            if prev_area > 0 and curr_area > 0
            else 1.0
        )
        refl_diff = float(np.abs(curr_cell['mean_reflectivity'] - prev_refl)) / 50.0

        return 0.4 * D_pos + 0.3 * (1.0 - IoU) + 0.15 * area_diff + 0.1 * refl_diff


# =============================================================================
# Core Tracking Algorithm
# =============================================================================

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

    def __init__(self, config: "InternalConfig"):
        self.config        = config
        self.match_cost    = config.tracker.match_cost_threshold
        self.keep_cost     = config.tracker.keep_cost_threshold
        self.unmatch_cost  = config.tracker.unmatch_cost_threshold
        self.split_overlap = config.tracker.split_overlap_threshold
        self.core_threshold = config.tracker.core_reflectivity_threshold
        self.refl_var      = config.global_.var_names.reflectivity
        self.labels_var    = config.global_.var_names.cell_labels

        self.graph          = TrackingGraph()
        self.matcher        = MatchingEngine(config)
        self._previous_scan: Optional[Tuple] = None  # (time, ds, node_ids)
        self._cell_identity: dict[int, tuple[str, str]] = {}

        logger.info(
            "RadarCellTracker initialized: match=%.2f keep=%.2f unmatch=%.2f overlap=%.2f",
            self.match_cost, self.keep_cost, self.unmatch_cost, self.split_overlap,
        )

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def track(
        self,
        ds_projected: xr.Dataset,
        cell_stats_df: pd.DataFrame,
    ) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Process one scan.

        Returns scan-local outputs:
        - tracked_cells_df: one row per cell observation in this scan
        - cell_events_df: explicit lineage/events (continue/split/merge/initiation/termination)
        """
        current_time  = self._get_time(ds_projected)
        cells_current = self._extract_cells_from_analyzer(ds_projected, cell_stats_df)

        events: list[dict] = []
        if self._previous_scan is None:
            node_ids = self._initialize_tracks(current_time, cells_current)
            self._previous_scan = (current_time, ds_projected, node_ids)
            for node_id in node_ids:
                events.append(self._event_initiation(current_time, node_id))
        else:
            prev_time, ds_prev, prev_node_ids = self._previous_scan
            events = self._track_frame_pair(
                prev_time, ds_prev, prev_node_ids,
                current_time, ds_projected, cells_current,
            )
            current_node_ids    = self.graph.get_nodes_at_time(current_time)
            self._previous_scan = (current_time, ds_projected, current_node_ids)

        current_node_ids = self.graph.get_nodes_at_time(current_time)
        tracked_cells_df = self._build_tracked_cells_current(current_time, current_node_ids)
        cell_events_df = self._build_cell_events_dataframe(events)
        return tracked_cells_df, cell_events_df

    def get_cell_identity(self, track_index: int) -> tuple[str, str]:
        if track_index not in self._cell_identity:
            raise ValueError(f"Missing cell identity for track_index={track_index}")
        return self._cell_identity[track_index]

    # ------------------------------------------------------------------
    # Cell extraction
    # ------------------------------------------------------------------

    def _get_time(self, ds: xr.Dataset):
        return self._normalize_time_scalar(ds.time.values)

    @staticmethod
    def _to_epoch_seconds(time_val) -> float:
        ts = pd.Timestamp(time_val)
        if ts.tzinfo is None:
            ts = ts.tz_localize("UTC")
        return float(ts.timestamp())

    @staticmethod
    def _normalize_time_scalar(time_val):
        """Normalize xarray/cftime/numpy time representations to a scalar.

        Returns a scalar compatible with pandas.Timestamp:
        - np.datetime64
        - datetime.datetime
        - or a scalar string / timestamp-like object
        """
        tv = time_val
        while isinstance(tv, np.ndarray) and tv.size == 1:
            tv = tv.reshape(-1)[0]
        if isinstance(tv, np.ndarray):
            tv = tv.reshape(-1)[0]

        if hasattr(tv, "item"):
            try:
                tv = tv.item()
            except Exception:
                pass

        # Handle cftime.* objects (pandas cannot convert them directly)
        if getattr(type(tv), "__module__", "").startswith("cftime"):
            from datetime import datetime, timezone

            tv = datetime(
                int(tv.year),
                int(tv.month),
                int(tv.day),
                int(tv.hour),
                int(tv.minute),
                int(tv.second),
                int(getattr(tv, "microsecond", 0) or 0),
                tzinfo=timezone.utc,
            )

        return tv

    @staticmethod
    def _time_key(time_val) -> str:
        """Stable ISO8601 time key for event grouping."""
        tv = RadarCellTracker._normalize_time_scalar(time_val)
        return pd.Timestamp(tv).isoformat()

    def _extract_cells_from_analyzer(
        self, ds: xr.Dataset, cell_stats_df: pd.DataFrame
    ) -> List[Dict]:
        """Merge per-cell stats (from AnalysisModule) with segmentation masks."""
        labels = ds[self.labels_var].values

        cell_props_map: Dict[int, Dict] = {}
        for _, row in cell_stats_df.iterrows():
            lbl = int(row['cell_label'])
            cell_props_map[lbl] = {
                'area':              float(row['cell_area_sqkm']),
                'centroid_x':        float(row['cell_centroid_geom_x']),
                'centroid_y':        float(row['cell_centroid_geom_y']),
                'mean_reflectivity': float(row['radar_reflectivity_mean']),
                'max_reflectivity':  float(row['radar_reflectivity_max']),
                'time_volume_start': row['time_volume_start'],
                'centroid_mass_lat': float(row['cell_centroid_mass_lat']),
                'centroid_mass_lon': float(row['cell_centroid_mass_lon']),
                'max_zdr': float(row['radar_differential_reflectivity_max']),
                'area_40dbz_km2': float(row['area_40dbz_km2']),
            }

        refl           = ds[self.refl_var].values
        dx             = float(np.abs(ds.x[1] - ds.x[0]))
        dy             = float(np.abs(ds.y[1] - ds.y[0]))
        pixel_area_km2 = (dx * dy) / 1e6

        cells: List[Dict] = []
        for cell_id in np.unique(labels):
            if cell_id == 0:
                continue
            if cell_id not in cell_props_map:
                logger.warning("Cell %d in labels but not in analyzer stats; skipping", cell_id)
                continue
            mask          = labels == cell_id
            props         = cell_props_map[cell_id]
            core_area_km2 = float(np.sum(mask & (refl > self.core_threshold)) * pixel_area_km2)
            cells.append({
                'cell_id':           int(cell_id),
                'mask':              mask,
                'area':              props['area'],
                'centroid_x':        props['centroid_x'],
                'centroid_y':        props['centroid_y'],
                'mean_reflectivity': props['mean_reflectivity'],
                'max_reflectivity':  props['max_reflectivity'],
                'core_area':         core_area_km2,
                'time_volume_start': props['time_volume_start'],
                'centroid_mass_lat': props['centroid_mass_lat'],
                'centroid_mass_lon': props['centroid_mass_lon'],
                'max_zdr': props['max_zdr'],
                'area_40dbz_km2': props['area_40dbz_km2'],
            })
        return cells

    def _new_cell_identity(self, cell: Dict) -> tuple[str, str]:
        cfg = self.config.tracker.cell_uid
        max_zdr = float(cell['max_zdr'])
        if max_zdr < 0:
            max_zdr = 0.0
        signature = _track_signature_from_birth(
            scan_start_time_epoch_s=self._to_epoch_seconds(cell['time_volume_start']),
            centroid_lat_deg=float(cell['centroid_mass_lat']),
            centroid_lon_deg=float(cell['centroid_mass_lon']),
            max_dbz=float(cell['max_reflectivity']),
            max_zdr=max_zdr,
            area40_km2=float(cell['area_40dbz_km2']),
            time_step_s=int(cfg.time_step_s),
            latlon_step_deg=float(cfg.latlon_step_deg),
            area_step_km2=float(cfg.area_step_km2),
        )
        cell_uid = _cell_uid_from_signature(signature, width=int(cfg.width))
        return cell_uid, signature

    # ------------------------------------------------------------------
    # Track initialisation helpers
    # ------------------------------------------------------------------

    def _initialize_tracks(self, time, cells: List[Dict]) -> List[int]:
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
        cell: Dict,
        track_index: int,
        cell_uid: Optional[str] = None,
        track_signature: Optional[str] = None,
    ) -> int:
        if cell_uid is None or track_signature is None:
            cell_uid, track_signature = self.get_cell_identity(track_index)
        return self.graph.add_observation(
            time=time,
            cell_id=cell['cell_id'],
            track_index=track_index,
            area=cell['area'],
            centroid_x=cell['centroid_x'],
            centroid_y=cell['centroid_y'],
            mean_reflectivity=cell['mean_reflectivity'],
            max_reflectivity=cell['max_reflectivity'],
            core_area=cell['core_area'],
            cell_uid=cell_uid,
            track_signature=track_signature,
        )

    # ------------------------------------------------------------------
    # Frame-pair matching
    # ------------------------------------------------------------------

    def _track_frame_pair(
        self,
        prev_time,
        ds_prev: xr.Dataset,
        prev_node_ids: List[int],
        curr_time,
        ds_curr: xr.Dataset,
        curr_cells: List[Dict],
    ) -> list[dict]:
        events: list[dict] = []
        if "cell_projections" not in ds_curr.data_vars:
            logger.warning("No cell_projections — initializing new paths")
            self._initialize_tracks(curr_time, curr_cells)
            for node_id in self.graph.get_nodes_at_time(curr_time):
                events.append(self._event_initiation(curr_time, node_id))
            return events

        projections = ds_curr["cell_projections"].values
        if projections.shape[0] < 1:
            logger.warning("Empty cell_projections — initializing new paths")
            self._initialize_tracks(curr_time, curr_cells)
            for node_id in self.graph.get_nodes_at_time(curr_time):
                events.append(self._event_initiation(curr_time, node_id))
            return events

        proj_labels = projections[0]   # registration frame: prev cells → curr coords
        n_prev      = len(prev_node_ids)
        n_curr      = len(curr_cells)

        if n_prev == 0:
            self._initialize_tracks(curr_time, curr_cells)
            for node_id in self.graph.get_nodes_at_time(curr_time):
                events.append(self._event_initiation(curr_time, node_id))
            return events
        if n_curr == 0:
            for d_node in prev_node_ids:
                events.append(self._event_termination(curr_time, d_node, target_node_id=None))
            return events  # all prev cells dissipated — natural termination, no outgoing edges

        dummy_cost = self.unmatch_cost * 50.0

        # ── Step 1: raw cost matrix ────────────────────────────────────────
        raw = self.matcher.compute_cost_matrix(
            prev_node_ids, self.graph, proj_labels, curr_cells, dummy_cost,
        )

        # ── Step 2: pre-clamp ─────────────────────────────────────────────
        raw[raw < self.match_cost]   = 0.0
        raw[raw > self.unmatch_cost] = dummy_cost

        # ── Step 3: pad to square ─────────────────────────────────────────
        n      = max(n_prev, n_curr)
        square = np.full((n, n), dummy_cost, dtype=float)
        square[:n_prev, :n_curr] = raw

        # ── Step 4: Hungarian ─────────────────────────────────────────────
        row_ind, col_ind = linear_sum_assignment(square)

        # ── Step 5: post-filter → CONTINUE / dissipated / born ───────────
        matched_prev: Dict[int, int] = {}   # prev_idx → new curr node_id
        matched_curr: Dict[int, int] = {}   # curr_idx → new curr node_id
        n_continue = 0

        for r, c in zip(row_ind, col_ind):
            if r >= n_prev or c >= n_curr:
                continue  # dummy slot
            if square[r, c] <= self.keep_cost:
                prev_node   = prev_node_ids[r]
                track_index  = self.graph.get_node_attr(prev_node, 'track_index')
                curr_node   = self._add_cell_node(curr_time, curr_cells[c], int(track_index or 0))
                self.graph.add_edge(prev_node, curr_node, edge_type="CONTINUE", cost=float(square[r, c]))
                matched_prev[r] = curr_node
                matched_curr[c] = curr_node
                n_continue += 1
                events.append(self._event_continue(curr_time, prev_node, curr_node, float(square[r, c])))

        dissipated   = [prev_node_ids[i] for i in range(n_prev) if i not in matched_prev]
        born_indices = [i for i in range(n_curr)  if i not in matched_curr]

        # ── Step 6: split detection ───────────────────────────────────────
        # Born cell overlaps a CONTINUE parent's projected hull >= threshold
        split_born: set[int] = set()
        for b_idx in born_indices:
            b_mask       = curr_cells[b_idx]['mask']
            best_parent  = None
            best_overlap = 0.0
            for prev_idx, curr_node in matched_prev.items():
                prev_node    = prev_node_ids[prev_idx]
                prev_cell_id = self.graph.get_node_attr(prev_node, 'cell_id')
                proj_mask    = (proj_labels == prev_cell_id)
                denom        = float(np.sum(proj_mask))
                if denom == 0:
                    continue
                overlap_frac = float(np.sum(b_mask & proj_mask)) / denom
                if overlap_frac >= self.split_overlap and overlap_frac > best_overlap:
                    best_parent  = curr_node   # current-frame node of the continuing parent
                    best_overlap = overlap_frac
            if best_parent is not None:
                parent_track_index = self.graph.get_node_attr(best_parent, 'track_index')
                new_index    = self.graph.get_new_track_index()
                cell_uid, track_signature = self._new_cell_identity(curr_cells[b_idx])
                self._cell_identity[new_index] = (cell_uid, track_signature)
                child_node   = self._add_cell_node(curr_time, curr_cells[b_idx], new_index, cell_uid, track_signature)
                self.graph.add_edge(best_parent, child_node, edge_type="SPLIT", cost=0.0)
                split_born.add(b_idx)
                events.append(self._event_split(curr_time, best_parent, child_node))
                logger.debug(
                    "SPLIT: track %d → new track %d (overlap=%.2f)",
                    parent_track_index, new_index, best_overlap,
                )

        # ── Step 7: merge detection ───────────────────────────────────────
        # Dissipated hull overlaps a CONTINUE cell >= threshold
        n_merge = 0
        merged_nodes: dict[int, int] = {}
        for d_node in dissipated:
            d_cell_id = self.graph.get_node_attr(d_node, 'cell_id')
            proj_mask = (proj_labels == d_cell_id)
            denom     = float(np.sum(proj_mask))
            if denom == 0:
                continue
            best_target  = None
            best_overlap = 0.0
            for c_idx, curr_node in matched_curr.items():
                overlap_frac = float(np.sum(proj_mask & curr_cells[c_idx]['mask'])) / denom
                if overlap_frac >= self.split_overlap and overlap_frac > best_overlap:
                    best_target  = curr_node
                    best_overlap = overlap_frac
            if best_target is not None:
                self.graph.add_edge(d_node, best_target, edge_type="MERGE", cost=0.0)
                n_merge += 1
                merged_nodes[d_node] = best_target
                events.append(self._event_merge(curr_time, d_node, best_target))
                logger.debug(
                    "MERGE: track %d → track %d (overlap=%.2f)",
                    self.graph.get_node_attr(d_node, 'track_index'),
                    self.graph.get_node_attr(best_target, 'track_index'),
                    best_overlap,
                )

        # ── Step 8: true new births ───────────────────────────────────────
        n_births = 0
        for b_idx in born_indices:
            if b_idx not in split_born:
                new_index = self.graph.get_new_track_index()
                cell_uid, track_signature = self._new_cell_identity(curr_cells[b_idx])
                self._cell_identity[new_index] = (cell_uid, track_signature)
                node_id = self._add_cell_node(curr_time, curr_cells[b_idx], new_index, cell_uid, track_signature)
                n_births += 1
                events.append(self._event_initiation(curr_time, node_id))

        for d_node in dissipated:
            if d_node in merged_nodes:
                events.append(self._event_termination(curr_time, d_node, target_node_id=merged_nodes[d_node]))
            else:
                events.append(self._event_termination(curr_time, d_node, target_node_id=None))

        n_split      = len(split_born)
        n_dissipated = len(dissipated) - n_merge
        n_alive      = n_continue + n_births + n_split   # == n_curr
        logger.info(
            "Frame pair: prev=%d → continue=%d, dissipated=%d, merged=%d"
            " | born=%d, split=%d | alive=%d",
            n_prev, n_continue, n_dissipated, n_merge, n_births, n_split, n_alive,
        )
        return events

    # ------------------------------------------------------------------
    # Scan-local builders (no per-track analytics)
    # ------------------------------------------------------------------

    def _build_tracked_cells_current(self, time, node_ids: List[int]) -> pd.DataFrame:
        rows: list[dict] = []
        for node_id in node_ids:
            node = self.graph.graph.nodes[node_id]
            time_val = RadarCellTracker._normalize_time_scalar(node["time"])
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

    @staticmethod
    def _build_cell_events_dataframe(events: list[dict]) -> pd.DataFrame:
        cols = [
            "time",
            "event_type",
            "source_cell_uid",
            "target_cell_uid",
            "source_cell_label",
            "target_cell_label",
            "cost",
            "is_dominant",
            "event_group_id",
        ]
        if not events:
            return pd.DataFrame(columns=cols)
        df = pd.DataFrame(events)
        for col in cols:
            if col not in df.columns:
                df[col] = None
        df = df[cols]
        df["time"] = df["time"].apply(lambda t: pd.Timestamp(RadarCellTracker._normalize_time_scalar(t)))
        return df

    # ------------------------------------------------------------------
    # Event builders
    # ------------------------------------------------------------------

    def _event_continue(self, time, prev_node_id: int, curr_node_id: int, cost: float) -> dict:
        source_cell_uid = self.get_cell_identity(int(self.graph.get_node_attr(prev_node_id, "track_index")))[0]
        target_cell_uid = self.get_cell_identity(int(self.graph.get_node_attr(curr_node_id, "track_index")))[0]
        return {
            "time": time,
            "event_type": "CONTINUE",
            "source_cell_uid": source_cell_uid,
            "target_cell_uid": target_cell_uid,
            "source_cell_label": int(self.graph.get_node_attr(prev_node_id, "cell_id")),
            "target_cell_label": int(self.graph.get_node_attr(curr_node_id, "cell_id")),
            "cost": float(cost),
            "is_dominant": True,
            "event_group_id": f"{self._time_key(time)}:CONTINUE:{target_cell_uid}",
        }

    def _event_split(self, time, parent_node_id: int, child_node_id: int) -> dict:
        parent_uid = self.get_cell_identity(int(self.graph.get_node_attr(parent_node_id, "track_index")))[0]
        child_uid = self.get_cell_identity(int(self.graph.get_node_attr(child_node_id, "track_index")))[0]
        return {
            "time": time,
            "event_type": "SPLIT",
            "source_cell_uid": parent_uid,
            "target_cell_uid": child_uid,
            "source_cell_label": int(self.graph.get_node_attr(parent_node_id, "cell_id")),
            "target_cell_label": int(self.graph.get_node_attr(child_node_id, "cell_id")),
            "cost": None,
            "is_dominant": False,
            "event_group_id": f"{self._time_key(time)}:SPLIT:{parent_uid}",
        }

    def _event_merge(self, time, source_node_id: int, target_node_id: int) -> dict:
        source_path = int(self.graph.get_node_attr(source_node_id, "track_index"))
        target_path = int(self.graph.get_node_attr(target_node_id, "track_index"))
        target_uid = self.get_cell_identity(target_path)[0]
        return {
            "time": time,
            "event_type": "MERGE",
            "source_cell_uid": self.get_cell_identity(source_path)[0],
            "target_cell_uid": target_uid,
            "source_cell_label": int(self.graph.get_node_attr(source_node_id, "cell_id")),
            "target_cell_label": int(self.graph.get_node_attr(target_node_id, "cell_id")),
            "cost": None,
            "is_dominant": False,
            "event_group_id": f"{self._time_key(time)}:MERGE:{target_uid}",
        }

    def _event_initiation(self, time, node_id: int) -> dict:
        target_uid = self.get_cell_identity(int(self.graph.get_node_attr(node_id, "track_index")))[0]
        return {
            "time": time,
            "event_type": "INITIATION",
            "source_cell_uid": None,
            "target_cell_uid": target_uid,
            "source_cell_label": None,
            "target_cell_label": int(self.graph.get_node_attr(node_id, "cell_id")),
            "cost": None,
            "is_dominant": False,
            "event_group_id": f"{self._time_key(time)}:INITIATION:{target_uid}",
        }

    def _event_termination(self, time, source_node_id: int, target_node_id: Optional[int]) -> dict:
        source_path = int(self.graph.get_node_attr(source_node_id, "track_index"))
        target_path = int(self.graph.get_node_attr(target_node_id, "track_index")) if target_node_id is not None else None
        source_uid = self.get_cell_identity(source_path)[0]
        return {
            "time": time,
            "event_type": "TERMINATION",
            "source_cell_uid": source_uid,
            "target_cell_uid": self.get_cell_identity(target_path)[0] if target_path is not None else None,
            "source_cell_label": int(self.graph.get_node_attr(source_node_id, "cell_id")),
            "target_cell_label": int(self.graph.get_node_attr(target_node_id, "cell_id")) if target_node_id is not None else None,
            "cost": None,
            "is_dominant": False,
            "event_group_id": f"{self._time_key(time)}:TERMINATION:{source_uid}",
        }


# =============================================================================
# BaseModule wrapper (Phase 6 implementation placeholder)
# =============================================================================

from adapt.modules.base import BaseModule
from adapt.execution.module_registry import registry
from adapt.modules.projection.contracts import assert_projected
from .contracts import assert_tracked_cells, assert_cell_events


def _check_projected_ds(ds: xr.Dataset) -> None:
    assert_projected(ds)


def _check_tracked_cells(df: pd.DataFrame) -> None:
    if not df.empty:
        assert_tracked_cells(df)


def _check_cell_events(df: pd.DataFrame) -> None:
    if not df.empty:
        assert_cell_events(df)


class TrackingModule(BaseModule):
    """Assign stable `cell_uid` identities to convective cells across consecutive radar scans.

    Produces scan-local tracking outputs. Any higher-level grouping/aggregation
    is outside this module's scope.

    Context outputs
    ---------------
    tracked_cells : pd.DataFrame
        Per-cell observations for the current scan with cell_uid/cell_label.
    cell_events : pd.DataFrame
        Explicit event rows for CONTINUE, SPLIT, MERGE, INITIATION, TERMINATION.
    """

    name = "tracking"
    inputs = ["projected_ds", "cell_stats", "config", "scan_time"]
    outputs = ["tracked_cells", "cell_events"]
    input_contracts = {"projected_ds": _check_projected_ds}
    output_contracts = {
        "tracked_cells": _check_tracked_cells,
        "cell_events": _check_cell_events,
    }

    def __init__(self) -> None:
        self._tracker = None

    def run(self, context: dict) -> dict:
        config = context["config"]
        ds_2d = context["projected_ds"]
        cell_stats = context["cell_stats"]

        if self._tracker is None:
            self._tracker = RadarCellTracker(config)

        tracked_cells, cell_events = self._tracker.track(
            ds_projected=ds_2d,
            cell_stats_df=cell_stats,
        )

        return {
            "tracked_cells": tracked_cells,
            "cell_events": cell_events,
        }


# Register module
registry.register(TrackingModule)
