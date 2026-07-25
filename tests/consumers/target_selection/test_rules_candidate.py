# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Candidate rule — active, quality gate, age gate."""

import pytest

from adapt.consumers.target_selection.rules import is_candidate
from tests.consumers.target_selection.factories import make_cell, make_config

pytestmark = pytest.mark.unit

CFG = make_config(
    gates=({"field": "n_scans", "op": "ge", "value": 3.0},), min_age_seconds=600.0
).candidate


def test_passes():
    assert is_candidate(make_cell(age=700.0, values={"n_scans": 5.0}), CFG) is True


def test_fails_quality():
    assert is_candidate(make_cell(values={"n_scans": 2.0}), CFG) is False


def test_multiple_gates_all_must_pass():
    cfg = make_config(
        gates=(
            {"field": "cell_area_sqkm", "op": "gt", "value": 20.0},
            {"field": "radar_reflectivity_max", "op": "gt", "value": 45.0},
        )
    ).candidate
    strong = make_cell(values={"cell_area_sqkm": 30.0, "radar_reflectivity_max": 50.0})
    weak_refl = make_cell(values={"cell_area_sqkm": 30.0, "radar_reflectivity_max": 40.0})
    assert is_candidate(strong, cfg) is True
    assert is_candidate(weak_refl, cfg) is False


def test_strict_greater_than_excludes_equal():
    cfg = make_config(gates=({"field": "cell_area_sqkm", "op": "gt", "value": 20.0},)).candidate
    assert is_candidate(make_cell(values={"cell_area_sqkm": 20.0}), cfg) is False
    assert is_candidate(make_cell(values={"cell_area_sqkm": 20.1}), cfg) is True


def test_ge_includes_equal():
    cfg = make_config(gates=({"field": "cell_area_sqkm", "op": "ge", "value": 20.0},)).candidate
    assert is_candidate(make_cell(values={"cell_area_sqkm": 20.0}), cfg) is True


def test_less_than_gate():
    # "young cells only": age is on the cell, but any numeric column works as a
    # ceiling too — here n_adjacent_cells < 3 keeps isolated cells.
    cfg = make_config(gates=({"field": "n_adjacent_cells", "op": "lt", "value": 3.0},)).candidate
    assert is_candidate(make_cell(values={"n_adjacent_cells": 2.0}), cfg) is True
    assert is_candidate(make_cell(values={"n_adjacent_cells": 3.0}), cfg) is False


def test_less_equal_gate():
    cfg = make_config(gates=({"field": "n_adjacent_cells", "op": "le", "value": 3.0},)).candidate
    assert is_candidate(make_cell(values={"n_adjacent_cells": 3.0}), cfg) is True
    assert is_candidate(make_cell(values={"n_adjacent_cells": 4.0}), cfg) is False


def test_fails_age():
    assert is_candidate(make_cell(age=300.0), CFG) is False


def test_nan_quality_not_candidate():
    assert is_candidate(make_cell(values={"n_scans": float("nan")}), CFG) is False


def test_missing_quality_column_raises():
    cell = make_cell(values={"cell_area_sqkm": 100.0})
    with pytest.raises(ValueError, match=r"n_scans.*cell_area_sqkm"):
        is_candidate(cell, CFG)
