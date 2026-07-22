# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Hungarian (optimal-assignment) matcher for residual ambiguity.

Builds a (n_prev × n_curr) cost matrix from projected hulls and solves it with
``scipy.optimize.linear_sum_assignment``. This is the only home for ``scipy`` in
the package. Deterministic overlap resolution lives in ``overlap.py``; this
matcher is the fallback for genuinely contested cells.
"""

import numpy as np

__all__ = ["MatchingEngine"]


class MatchingEngine:
    """Cost matrix builder using projected masks (cell_projections[0] is already the hull)."""

    def __init__(self, config):
        self.core_threshold = config.core_reflectivity_threshold
        self.expected_speed_ms = config.expected_speed_ms

    def compute_cost_matrix(
        self,
        prev_node_ids: list[int],
        graph,
        proj_labels: np.ndarray,
        curr_cells: list[dict],
        dummy_cost: float,
        dt_s: float,
    ) -> np.ndarray:
        """Build (n_prev × n_curr) cost matrix.

        Uses cell_projections[0] directly as the projected hull — no recomputation.
        Pairs with no spatial overlap receive dummy_cost.
        D_pos is normalised by expected_speed_ms * dt_s so displacement cost scales
        correctly with scan interval.
        """
        n_prev = len(prev_node_ids)
        n_curr = len(curr_cells)
        cost_matrix = np.full((n_prev, n_curr), dummy_cost, dtype=float)

        for prev_idx, prev_node in enumerate(prev_node_ids):
            prev_cell_id = graph.get_node_attr(prev_node, "cell_id")
            proj_mask = proj_labels == prev_cell_id
            if not np.any(proj_mask):
                continue  # cell left the frame or is dormant (no projection)
            for curr_idx, curr_cell in enumerate(curr_cells):
                if np.any(proj_mask & curr_cell["mask"]):
                    cost_matrix[prev_idx, curr_idx] = self._compute_cost(
                        prev_node, graph, proj_mask, curr_cell, dt_s
                    )

        return cost_matrix

    def _compute_cost(
        self,
        prev_node: int,
        graph,
        proj_mask: np.ndarray,
        curr_cell: dict,
        dt_s: float,
    ) -> float:
        """4-term cost: 0.4*Dpos + 0.3*(1-IoU) + 0.15*|log(A2/A1)| + 0.1*|Z2-Z1|/50

        D_pos is normalised by max_displacement = expected_speed_ms * dt_s (metres),
        then capped at 1.0 so it stays in [0, 1] regardless of cadence.
        """
        prev_cx = graph.get_node_attr(prev_node, "centroid_x")
        prev_cy = graph.get_node_attr(prev_node, "centroid_y")
        prev_area = graph.get_node_attr(prev_node, "area")
        prev_refl = graph.get_node_attr(prev_node, "mean_reflectivity")

        curr_mask = curr_cell["mask"]
        dist = np.sqrt(
            (curr_cell["centroid_x"] - prev_cx) ** 2 + (curr_cell["centroid_y"] - prev_cy) ** 2
        )
        max_displacement = self.expected_speed_ms * dt_s  # metres
        D_pos = min(float(dist) / max_displacement, 1.0)

        union = np.sum(proj_mask | curr_mask)
        IoU = float(np.sum(proj_mask & curr_mask)) / union if union > 0 else 0.0

        curr_area = curr_cell["area"]
        area_diff = (
            float(np.abs(np.log(curr_area / prev_area))) if prev_area > 0 and curr_area > 0 else 1.0
        )
        refl_diff = float(np.abs(curr_cell["mean_reflectivity"] - prev_refl)) / 50.0

        return 0.4 * D_pos + 0.3 * (1.0 - IoU) + 0.15 * area_diff + 0.1 * refl_diff
