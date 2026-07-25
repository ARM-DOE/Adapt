# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Haversine distance — analytically known outputs."""

import pytest

from adapt.consumers.target_selection._geo import haversine_km

pytestmark = pytest.mark.unit


def test_haversine_zero_distance():
    assert haversine_km(35.0, -97.0, 35.0, -97.0) == 0.0


def test_haversine_known_distance():
    # One degree of longitude at 35N: 111.32 km * cos(35 deg) ~= 91.2 km.
    assert haversine_km(35.0, -97.0, 35.0, -96.0) == pytest.approx(91.2, rel=0.01)
