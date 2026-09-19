# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Priority rule — configurable weighted sum."""

import pytest

from adapt.consumers.target_selection.rules import priority_score
from tests.consumers.target_selection.factories import make_cell, make_config

pytestmark = pytest.mark.unit


def test_weighted_sum():
    cell = make_cell(refl=50.0, area=200.0, growth=1.0)
    weights = make_config(w_reflectivity=1.0, w_area=0.05, w_growth=2.0).priority.weights
    # 1.0*50 + 0.05*200 + 2.0*1.0 = 62.0
    assert priority_score(cell, weights) == 62.0


def test_priority_score_raises_on_nan_component():
    # A NULL/NaN statistic must never rank: 0*NaN is NaN, and min() over
    # NaN scores silently picks an arbitrary cell (silent wrong science).
    cell = make_cell(refl=float("nan"))
    weights = make_config().priority.weights
    with pytest.raises(ValueError, match="field_max"):
        priority_score(cell, weights)


def test_priority_score_names_every_nan_component():
    cell = make_cell(refl=float("nan"), growth=float("nan"))
    weights = make_config().priority.weights
    pattern = "field_max.*growth_rate|growth_rate.*field_max"
    with pytest.raises(ValueError, match=pattern):
        priority_score(cell, weights)
