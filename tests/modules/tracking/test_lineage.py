# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Lineage partner choice, identity score and identity rule; latent carry."""

import math

import numpy as np
import pytest

from adapt.modules.tracking.latent import carry_footprint
from adapt.modules.tracking.lineage import (
    RULE_NEAREST,
    RULE_SCORE,
    IdentityCandidate,
    best_partner,
    choose_identity,
    identity_score,
)

pytestmark = pytest.mark.unit


# ── split parent / merge target ─────────────────────────────────────────────


def test_partner_is_the_largest_fraction_above_threshold():
    assert best_partner({3: 0.7, 5: 0.9, 8: 0.66}, threshold=0.65) == 5


def test_no_partner_below_threshold():
    assert best_partner({3: 0.64}, threshold=0.65) is None


def test_partner_at_threshold_qualifies():
    assert best_partner({3: 0.65}, threshold=0.65) == 3


def test_partner_tie_goes_to_the_smaller_key():
    assert best_partner({7: 0.8, 2: 0.8}, threshold=0.5) == 2


# ── identity score S = log A + γ log ē ──────────────────────────────────────


def test_identity_score_with_gamma_one_ranks_by_intensity_mass():
    # A·ē is the mass: 10 km² at 5 dBZ excess outranks 20 km² at 2 dBZ.
    assert identity_score(10.0, 5.0, gamma=1.0) > identity_score(20.0, 2.0, gamma=1.0)


def test_identity_score_with_gamma_zero_is_the_area_rule():
    assert identity_score(20.0, 2.0, gamma=0.0) == pytest.approx(math.log(20.0))


def test_cell_without_excess_has_the_lowest_score():
    assert identity_score(50.0, 0.0, gamma=1.0) == -math.inf


# ── identity rule ───────────────────────────────────────────────────────────


def test_identity_follows_the_clearly_larger_score():
    winner, rule = choose_identity(
        [IdentityCandidate("near", 1.0, 0.1), IdentityCandidate("strong", 2.0, 3.0)],
        margin=0.33,
    )
    assert (winner, rule) == ("strong", RULE_SCORE)


def test_identity_within_noise_goes_to_the_nearer_candidate():
    winner, rule = choose_identity(
        [IdentityCandidate("near", 1.9, 0.1), IdentityCandidate("strong", 2.0, 3.0)],
        margin=0.33,
    )
    assert (winner, rule) == ("near", RULE_NEAREST)


def test_identity_between_two_cells_without_excess_goes_to_the_nearer():
    winner, _ = choose_identity(
        [IdentityCandidate("a", -math.inf, 2.0), IdentityCandidate("b", -math.inf, 1.0)],
        margin=0.33,
    )
    assert winner == "b"


# ── latent carry by the flow ────────────────────────────────────────────────


def test_latent_footprint_moves_with_the_mean_flow_inside_it():
    mask = np.zeros((10, 10), dtype=bool)
    mask[2:4, 2:4] = True
    flow_x = np.zeros((10, 10))
    flow_y = np.zeros((10, 10))
    flow_x[2:4, 2:4] = 3.0  # 3 px right
    flow_y[2:4, 2:4] = 1.0  # 1 px down
    carried, shift = carry_footprint(mask, flow_x, flow_y)
    assert shift == (1, 3)
    assert carried[3:5, 5:7].all() and carried.sum() == 4


def test_latent_footprint_leaving_the_domain_is_clipped():
    mask = np.zeros((5, 5), dtype=bool)
    mask[2, 3:5] = True
    flow_x = np.full((5, 5), 1.0)
    carried, _ = carry_footprint(mask, flow_x, np.zeros((5, 5)))
    assert carried.sum() == 1 and carried[2, 4]
