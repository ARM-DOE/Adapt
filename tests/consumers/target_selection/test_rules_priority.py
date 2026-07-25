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
