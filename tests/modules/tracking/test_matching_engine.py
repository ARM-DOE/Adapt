# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Unit tests for MatchingEngine cost matrix computation.

All assertions reference the 4-term formula defined in module.py:
    cost = 0.4*D_pos + 0.3*(1-IoU) + 0.15*|log(A2/A1)| + 0.1*|Z2-Z1|/50
"""

import math
from types import SimpleNamespace

import numpy as np
import pytest

from adapt.modules.tracking.module import MatchingEngine, TrackingGraph

pytestmark = pytest.mark.unit

DUMMY_COST = 9.0
DT_S = 300.0  # 5-minute scan interval


def _engine(expected_speed_ms: float = 30.0) -> MatchingEngine:
    cfg = SimpleNamespace(core_reflectivity_threshold=40.0, expected_speed_ms=expected_speed_ms)
    return MatchingEngine(cfg)


def _graph_with_node(
    centroid_x: float = 0.0,
    centroid_y: float = 0.0,
    area: float = 4.0,
    mean_reflectivity: float = 40.0,
    cell_id: int = 1,
) -> tuple[TrackingGraph, int]:
    g = TrackingGraph()
    node_id = g.add_observation(
        time=np.datetime64("2024-01-01T12:00:00"),
        cell_id=cell_id,
        track_index=1,
        area=area,
        centroid_x=centroid_x,
        centroid_y=centroid_y,
        mean_reflectivity=mean_reflectivity,
        max_reflectivity=mean_reflectivity + 5.0,
        core_area=1.0,
        cell_uid="TEST000001",
        track_signature="v1|test",
    )
    return g, node_id


def _curr_cell(
    mask: np.ndarray,
    centroid_x: float = 0.0,
    centroid_y: float = 0.0,
    area: float = 4.0,
    mean_reflectivity: float = 40.0,
) -> dict:
    return {
        "mask": mask,
        "centroid_x": centroid_x,
        "centroid_y": centroid_y,
        "area": area,
        "mean_reflectivity": mean_reflectivity,
    }


# ---------------------------------------------------------------------------
# Shape tests
# ---------------------------------------------------------------------------


def test_cost_matrix_shape():
    engine = _engine()
    H, W = 8, 8

    # Two previous nodes with disjoint projections
    g = TrackingGraph()
    t = np.datetime64("2024-01-01T12:00:00")
    n0 = g.add_observation(
        t,
        cell_id=1,
        track_index=1,
        area=4.0,
        centroid_x=1.0,
        centroid_y=1.0,
        mean_reflectivity=40.0,
        max_reflectivity=45.0,
        core_area=1.0,
        cell_uid="A",
        track_signature="s",
    )
    n1 = g.add_observation(
        t,
        cell_id=2,
        track_index=2,
        area=4.0,
        centroid_x=6.0,
        centroid_y=6.0,
        mean_reflectivity=40.0,
        max_reflectivity=45.0,
        core_area=1.0,
        cell_uid="B",
        track_signature="s",
    )

    proj = np.zeros((H, W), dtype=np.int32)
    curr_cells = [
        _curr_cell(np.zeros((H, W), dtype=bool)),
        _curr_cell(np.zeros((H, W), dtype=bool)),
        _curr_cell(np.zeros((H, W), dtype=bool)),
    ]

    mat = engine.compute_cost_matrix([n0, n1], g, proj, curr_cells, DUMMY_COST, DT_S)
    assert mat.shape == (2, 3)


def test_empty_prev_list_returns_zero_row_matrix():
    engine = _engine()
    H, W = 4, 4
    proj = np.zeros((H, W), dtype=np.int32)
    curr_cells = [_curr_cell(np.zeros((H, W), dtype=bool))]

    mat = engine.compute_cost_matrix([], TrackingGraph(), proj, curr_cells, DUMMY_COST, DT_S)
    assert mat.shape == (0, 1)


# ---------------------------------------------------------------------------
# No-overlap → dummy cost
# ---------------------------------------------------------------------------


def test_no_overlap_gets_dummy_cost():
    engine = _engine()
    H, W = 6, 6
    g, n0 = _graph_with_node(centroid_x=1.0, centroid_y=1.0, cell_id=1)

    proj = np.zeros((H, W), dtype=np.int32)
    proj[0:2, 0:2] = 1  # prev cell projected to top-left

    curr_mask = np.zeros((H, W), dtype=bool)
    curr_mask[4:6, 4:6] = True  # current cell at bottom-right: no overlap

    curr_cells = [_curr_cell(curr_mask, centroid_x=4.5, centroid_y=4.5)]
    mat = engine.compute_cost_matrix([n0], g, proj, curr_cells, DUMMY_COST, DT_S)

    assert mat[0, 0] == pytest.approx(DUMMY_COST)


# ---------------------------------------------------------------------------
# Cost formula correctness
# ---------------------------------------------------------------------------


def test_perfect_overlap_zero_displacement_identical_cell_has_low_cost():
    """All 4 terms near 0 → total cost near 0."""
    engine = _engine(expected_speed_ms=30.0)
    H, W = 6, 6
    # 2×2 cell at centre
    mask = np.zeros((H, W), dtype=bool)
    mask[2:4, 2:4] = True
    cx, cy = 2.5, 2.5
    area = 4.0
    refl = 40.0

    g, n0 = _graph_with_node(centroid_x=cx, centroid_y=cy, area=area, mean_reflectivity=refl)
    proj = np.zeros((H, W), dtype=np.int32)
    proj[2:4, 2:4] = 1  # perfect overlap

    curr_cells = [_curr_cell(mask, centroid_x=cx, centroid_y=cy, area=area, mean_reflectivity=refl)]
    mat = engine.compute_cost_matrix([n0], g, proj, curr_cells, DUMMY_COST, DT_S)

    # D_pos=0, IoU=1→(1-IoU)=0, area_diff=log(1)=0, refl_diff=0
    assert mat[0, 0] < 0.05


def test_dpos_term_saturates_at_one():
    """D_pos is capped at 1.0 regardless of displacement magnitude."""
    expected_speed_ms = 10.0
    engine = _engine(expected_speed_ms=expected_speed_ms)
    H, W = 20, 20

    prev_cx, prev_cy = 0.0, 0.0
    curr_cx = expected_speed_ms * DT_S * 100.0  # 100× max displacement
    curr_cy = 0.0

    mask = np.zeros((H, W), dtype=bool)
    mask[0, 0] = True
    proj = np.zeros((H, W), dtype=np.int32)
    proj[0, 0] = 1  # some overlap

    g, n0 = _graph_with_node(
        centroid_x=prev_cx, centroid_y=prev_cy, area=4.0, mean_reflectivity=40.0
    )
    curr_cells = [
        _curr_cell(mask, centroid_x=curr_cx, centroid_y=curr_cy, area=4.0, mean_reflectivity=40.0)
    ]
    mat = engine.compute_cost_matrix([n0], g, proj, curr_cells, DUMMY_COST, DT_S)

    # cost ≤ 0.4×1 + 0.3×1 + 0.15×1 + 0.1×1 = 1.0
    assert mat[0, 0] <= 1.0 + 1e-9


def test_iou_term_correct():
    """proj_mask 4px, curr_mask 4px, 2px overlap → IoU=2/6; (1-IoU)=4/6."""
    engine = _engine(expected_speed_ms=30.0)
    H, W = 8, 8

    # proj occupies columns 2-3; curr occupies columns 3-4 → 1-column overlap in row 2
    proj = np.zeros((H, W), dtype=np.int32)
    proj[2, 2] = 1
    proj[2, 3] = 1
    proj[3, 2] = 1
    proj[3, 3] = 1

    curr_mask = np.zeros((H, W), dtype=bool)
    curr_mask[2, 3] = True
    curr_mask[2, 4] = True
    curr_mask[3, 3] = True
    curr_mask[3, 4] = True
    # intersection: (2,3),(3,3) = 2px; union = 6px; IoU = 2/6

    cx = 3.5
    cy = 2.5
    g, n0 = _graph_with_node(centroid_x=cx, centroid_y=cy, area=4.0, mean_reflectivity=40.0)
    curr_cells = [
        _curr_cell(curr_mask, centroid_x=cx, centroid_y=cy, area=4.0, mean_reflectivity=40.0)
    ]
    mat = engine.compute_cost_matrix([n0], g, proj, curr_cells, DUMMY_COST, DT_S)

    iou = 2.0 / 6.0
    expected_iou_contribution = 0.3 * (1.0 - iou)
    # Other terms are near-zero (same centroid, same area, same refl)
    assert mat[0, 0] == pytest.approx(expected_iou_contribution, abs=0.02)


def test_area_ratio_term_zero_for_equal_area():
    """Equal areas → log(1) = 0 → area term = 0."""
    engine = _engine(expected_speed_ms=30.0)
    H, W = 6, 6
    mask = np.zeros((H, W), dtype=bool)
    mask[2:4, 2:4] = True
    proj = np.zeros((H, W), dtype=np.int32)
    proj[2:4, 2:4] = 1

    area = 4.0
    g, n0 = _graph_with_node(centroid_x=2.5, centroid_y=2.5, area=area, mean_reflectivity=40.0)
    curr_cells = [
        _curr_cell(mask, centroid_x=2.5, centroid_y=2.5, area=area, mean_reflectivity=40.0)
    ]
    mat = engine.compute_cost_matrix([n0], g, proj, curr_cells, DUMMY_COST, DT_S)

    # Area term = 0.15 * |log(1)| = 0
    # All other terms near 0 too
    assert mat[0, 0] < 0.05


def test_area_ratio_term_for_doubling():
    """curr_area = 2×prev_area → contribution ≈ 0.15×log(2)."""
    engine = _engine(expected_speed_ms=30.0)
    H, W = 6, 6
    mask = np.zeros((H, W), dtype=bool)
    mask[2:4, 2:4] = True
    proj = np.zeros((H, W), dtype=np.int32)
    proj[2:4, 2:4] = 1

    prev_area = 4.0
    curr_area = 8.0
    cx, cy = 2.5, 2.5

    g, n0 = _graph_with_node(centroid_x=cx, centroid_y=cy, area=prev_area, mean_reflectivity=40.0)
    curr_cells = [
        _curr_cell(mask, centroid_x=cx, centroid_y=cy, area=curr_area, mean_reflectivity=40.0)
    ]
    mat = engine.compute_cost_matrix([n0], g, proj, curr_cells, DUMMY_COST, DT_S)

    expected_area_contribution = 0.15 * math.log(2.0)
    # Other terms near 0 (same centroid, same refl, full overlap)
    assert mat[0, 0] == pytest.approx(expected_area_contribution, abs=0.02)


def test_no_proj_mask_for_cell_leaves_dummy_cost():
    """When cell_id does not appear in proj_labels, cost stays at dummy_cost."""
    engine = _engine()
    H, W = 6, 6
    g, n0 = _graph_with_node(cell_id=99)  # cell_id 99 not in proj_labels

    proj = np.zeros((H, W), dtype=np.int32)
    proj[0:2, 0:2] = 1  # only cell_id=1 projected, not 99

    curr_mask = np.zeros((H, W), dtype=bool)
    curr_mask[2:4, 2:4] = True
    curr_cells = [_curr_cell(curr_mask)]

    mat = engine.compute_cost_matrix([n0], g, proj, curr_cells, DUMMY_COST, DT_S)
    assert mat[0, 0] == pytest.approx(DUMMY_COST)
