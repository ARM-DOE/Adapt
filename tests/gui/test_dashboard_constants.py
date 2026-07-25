# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Unit tests for dashboard module-level constants.

These tests verify structural consistency of _HV_KEYS and _BOX_DEFS without
creating any Tkinter windows. They import only the constant definitions.
"""

import pytest

pytestmark = pytest.mark.unit

tk = pytest.importorskip("tkinter", reason="tkinter not available")


def test_hv_keys_contains_sw_mean():
    """_HV_KEYS must contain 'sw_mean' — used in _on_plot_hover at line 2077."""
    from adapt.consumers.live._scan_view import _HV_KEYS

    assert "sw_mean" in _HV_KEYS


def test_hv_keys_covers_all_box_defs_keys():
    """Every key referenced in _BOX_DEFS must have a StringVar slot in _HV_KEYS."""
    from adapt.consumers.live._scan_view import _BOX_DEFS, _HV_KEYS

    hv_set = set(_HV_KEYS)
    for row in _BOX_DEFS:
        lbl1, key1, fg1, lbl2, key2, fg2 = row
        assert key1 in hv_set, f"_BOX_DEFS key '{key1}' missing from _HV_KEYS"
        assert key2 in hv_set, f"_BOX_DEFS key '{key2}' missing from _HV_KEYS"
