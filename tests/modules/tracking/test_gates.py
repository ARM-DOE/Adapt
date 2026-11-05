# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Hard physical gates, the heading term, and the crossing test of tracker v2."""

import math

import pytest

from adapt.modules.tracking.matching.gates import (
    GATE_SPEED,
    GATE_SPEED_CHANGE,
    drop_crossing_links,
    heading_term,
    segments_cross,
    speed_change_limit,
    speed_gate,
)

pytestmark = pytest.mark.unit

A_MAX = 0.04
SIGMA = 1000.0


# ── speed and additive speed change ─────────────────────────────────────────


def test_speed_change_limit_is_acceleration_plus_centroid_noise():
    # a·Δt + 2√2·σ/Δt at Δt = 277 s: 11.08 + 10.21 ≈ 21.3 m/s (spec value)
    limit = speed_change_limit(0.0, 277.0, A_MAX, SIGMA)
    assert limit == pytest.approx(0.04 * 277 + 2 * math.sqrt(2) * 1000 / 277)
    assert limit == pytest.approx(21.3, abs=0.05)


def test_speed_change_limit_is_additive_to_the_previous_speed():
    base = speed_change_limit(0.0, 277.0, A_MAX, SIGMA)
    assert speed_change_limit(15.0, 277.0, A_MAX, SIGMA) == pytest.approx(15.0 + base)


def test_speed_change_noise_term_dominates_short_intervals():
    short = speed_change_limit(0.0, 60.0, A_MAX, SIGMA)
    assert short > speed_change_limit(0.0, 277.0, A_MAX, SIGMA)


def test_speed_above_the_cap_is_rejected():
    gate = speed_gate(45.0, v_prev=None, dt_s=277.0, v_max=40.0, a_max=A_MAX, sigma_x_m=SIGMA)
    assert gate.code == GATE_SPEED


def test_first_step_track_is_limited_only_by_the_cap():
    gate = speed_gate(39.0, v_prev=None, dt_s=277.0, v_max=40.0, a_max=A_MAX, sigma_x_m=SIGMA)
    assert gate.code is None
    assert gate.limit_ms is None


def test_speed_jump_beyond_the_additive_limit_is_rejected():
    # 5 + 21.3 = 26.3 m/s allowed; 30 m/s is not.
    gate = speed_gate(30.0, v_prev=5.0, dt_s=277.0, v_max=40.0, a_max=A_MAX, sigma_x_m=SIGMA)
    assert gate.code == GATE_SPEED_CHANGE
    assert gate.limit_ms == pytest.approx(5.0 + 21.3, abs=0.05)


def test_a_slow_cell_may_start_moving():
    # The former ratio rule (3 × 0.5 m/s) rejected this; the additive one does not.
    gate = speed_gate(12.0, v_prev=0.5, dt_s=277.0, v_max=40.0, a_max=A_MAX, sigma_x_m=SIGMA)
    assert gate.code is None


# ── heading term ────────────────────────────────────────────────────────────


def test_heading_term_is_zero_for_straight_continuation():
    h, turn = heading_term((5000.0, 0.0), (6000.0, 0.0), weight=1.0, min_step_m=2000.0)
    assert h == pytest.approx(0.0)
    assert turn == pytest.approx(0.0)


def test_heading_term_is_the_weight_for_a_full_reversal():
    h, turn = heading_term((5000.0, 0.0), (-5000.0, 0.0), weight=1.0, min_step_m=2000.0)
    assert h == pytest.approx(1.0)
    assert turn == pytest.approx(180.0)


def test_heading_term_is_half_the_weight_for_a_right_angle():
    h, _ = heading_term((5000.0, 0.0), (0.0, 5000.0), weight=0.8, min_step_m=2000.0)
    assert h == pytest.approx(0.4)


def test_heading_is_not_applied_below_two_grid_lengths():
    # A 1.5 km step is dominated by position noise: no term, no angle.
    assert heading_term((5000.0, 0.0), (-1500.0, 0.0), 1.0, 2000.0) == (0.0, None)
    assert heading_term((1500.0, 0.0), (-5000.0, 0.0), 1.0, 2000.0) == (0.0, None)


def test_heading_needs_a_previous_step():
    assert heading_term(None, (5000.0, 0.0), 1.0, 2000.0) == (0.0, None)


# ── crossing ────────────────────────────────────────────────────────────────


def test_exchanging_places_is_a_crossing():
    assert segments_cross((0, 0), (10, 10), (0, 10), (10, 0))


def test_parallel_motion_is_not_a_crossing():
    assert not segments_cross((0, 0), (10, 0), (0, 5), (10, 5))


def test_collinear_segments_are_not_a_crossing():
    assert not segments_cross((0, 0), (10, 0), (5, 0), (15, 0))


def test_shared_endpoints_are_not_a_crossing():
    # A split (shared start) and a merge (shared end).
    assert not segments_cross((0, 0), (10, 10), (0, 0), (10, -10))
    assert not segments_cross((0, 0), (10, 0), (5, 5), (10, 0))


def test_segments_that_stop_short_do_not_cross():
    assert not segments_cross((0, 0), (4, 4), (0, 10), (10, 0))


def test_the_costlier_of_two_crossing_links_is_dropped():
    links = {
        "a": ((0.0, 0.0), (10.0, 10.0), 0.3),
        "b": ((0.0, 10.0), (10.0, 0.0), 0.5),
        "c": ((20.0, 0.0), (30.0, 0.0), 0.9),
    }
    kept, dropped = drop_crossing_links(links)
    assert kept == ["a", "c"]
    assert dropped == {"b": "a"}


def test_crossing_resolution_is_independent_of_input_order():
    links = {
        "b": ((0.0, 10.0), (10.0, 0.0), 0.5),
        "a": ((0.0, 0.0), (10.0, 10.0), 0.3),
    }
    kept, dropped = drop_crossing_links(links)
    assert kept == ["a"] and dropped == {"b": "a"}


def test_crossing_accepts_numpy_coordinates():
    import numpy as np

    a0, a1, b0, b1 = (np.float64(v) for v in (0.0, 10.0, 10.0, 0.0))
    assert segments_cross((a0, a0), (a1, a1), (a0, b0), (a1, b1))
