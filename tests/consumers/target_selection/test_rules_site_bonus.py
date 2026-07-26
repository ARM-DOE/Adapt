# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Site preference rule — bonus when projected trajectory intersects a site."""

import pytest

from adapt.consumers.target_selection.rules import site_bonus
from adapt.consumers.target_selection.snapshot import TrajectoryPoint
from tests.consumers.target_selection.factories import make_config

pytestmark = pytest.mark.unit

SITE = {"name": "sgp_c1", "lat": 36.607, "lon": -97.488, "radius_km": 20.0, "bonus": 10.0}


def _point(lat, lon, lead=300.0):
    return TrajectoryPoint(lat=lat, lon=lon, lead_seconds=lead)


def test_point_at_site_center():
    cfg = make_config(sites=(SITE,)).site_preference
    assert site_bonus((_point(36.607, -97.488),), cfg) == 10.0


def test_outside_radius():
    cfg = make_config(sites=(SITE,)).site_preference
    # One degree of longitude (~89 km at 36.6N) is well outside 20 km.
    assert site_bonus((_point(36.607, -96.488),), cfg) == 0.0


def test_bonus_once_per_site():
    cfg = make_config(sites=(SITE,)).site_preference
    inside = tuple(_point(36.607, -97.488, lead=60.0 * k) for k in range(3))
    assert site_bonus(inside, cfg) == 10.0


def test_two_sites_sum():
    other = {"name": "aux", "lat": 36.8, "lon": -97.488, "radius_km": 30.0, "bonus": 5.0}
    cfg = make_config(sites=(SITE, other)).site_preference
    # 36.7N is ~10 km from SITE and ~11 km from aux: inside both radii.
    assert site_bonus((_point(36.7, -97.488),), cfg) == 15.0


def test_projection_steps_truncates():
    cfg = make_config(sites=(SITE,), projection_steps=2).site_preference
    trajectory = (_point(40.0, -100.0), _point(40.0, -100.0), _point(36.607, -97.488))
    assert site_bonus(trajectory, cfg) == 0.0


def test_empty_trajectory():
    cfg = make_config(sites=(SITE,)).site_preference
    assert site_bonus((), cfg) == 0.0
