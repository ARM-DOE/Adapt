# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Unit tests for adapt.utils.cell_uid_lut.build_cell_uid_lut.

Synthetic label->uid mappings with analytically known output arrays. No IO.
"""

import numpy as np
import pytest

from adapt.utils.cell_uid_lut import build_cell_uid_lut

pytestmark = pytest.mark.unit


def test_maps_labels_to_uids_with_null_at_index_zero():
    lut = build_cell_uid_lut({1: "a", 2: "b"}, max_label=2)

    assert list(lut) == ["NONE", "a", "b"]


def test_index_zero_is_background_null_value():
    lut = build_cell_uid_lut({1: "a"}, max_label=1)

    assert lut[0] == "NONE"


def test_custom_null_value():
    lut = build_cell_uid_lut({1: "a"}, max_label=1, null_value="")

    assert lut[0] == ""


def test_indexable_by_every_label_value():
    lut = build_cell_uid_lut({1: "x7", 2: "y9", 3: "z1"}, max_label=3)

    assert lut[3] == "z1"
    assert lut.shape == (4,)


def test_raises_when_label_missing_from_mapping():
    with pytest.raises(ValueError, match="2"):
        build_cell_uid_lut({1: "a"}, max_label=2)


def test_empty_when_no_cells():
    lut = build_cell_uid_lut({}, max_label=0)

    assert list(lut) == ["NONE"]


def test_returns_string_dtype_array():
    lut = build_cell_uid_lut({1: "a"}, max_label=1)

    assert np.issubdtype(lut.dtype, np.str_) or lut.dtype == object
