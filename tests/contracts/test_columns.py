# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Canonical column/variable naming — the single authoritative convention."""

import pytest

from adapt.contracts import CELL_LABELS_VAR, stat_column

pytestmark = pytest.mark.unit


def test_stat_column_mints_the_radar_prefixed_convention():
    assert stat_column("reflectivity", "max") == "radar_reflectivity_max"
    assert stat_column("reflectivity", "mean") == "radar_reflectivity_mean"


def test_stat_column_is_field_generic():
    # The radar_ prefix is a cell-stat namespace, not a physics claim —
    # a pressure-tracked run mints radar_pressure_* columns.
    assert stat_column("pressure", "min") == "radar_pressure_min"


def test_cell_labels_constant():
    assert CELL_LABELS_VAR == "cell_labels"
