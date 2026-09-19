# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""cell_uid v2: a pure function of scan identity + label.

scan_id is sha256 of the raw scan bytes and labels are unique within a
scan, so the uid is deterministic and collision-free by construction — and
independent of which physics fields the source provides (no ZDR needed,
no reflectivity needed, works for any tracked field).
"""

import pytest

from adapt.modules.tracking.identity import _cell_uid_from_signature, track_signature_v2

pytestmark = pytest.mark.unit

_SCAN = "abc123def4567890"  # 16-hex-char scan_id shape


def test_signature_is_scan_id_plus_label():
    assert track_signature_v2(_SCAN, 7) == f"v2|{_SCAN}|7"


def test_same_scan_and_label_give_same_uid():
    a = _cell_uid_from_signature(track_signature_v2(_SCAN, 7), width=10)
    b = _cell_uid_from_signature(track_signature_v2(_SCAN, 7), width=10)
    assert a == b


def test_different_label_gives_different_uid():
    a = _cell_uid_from_signature(track_signature_v2(_SCAN, 7), width=10)
    b = _cell_uid_from_signature(track_signature_v2(_SCAN, 8), width=10)
    assert a != b


def test_different_scan_gives_different_uid():
    a = _cell_uid_from_signature(track_signature_v2(_SCAN, 7), width=10)
    b = _cell_uid_from_signature(track_signature_v2("ffff23def4567890", 7), width=10)
    assert a != b


def test_empty_scan_id_raises():
    with pytest.raises(ValueError, match="scan_id"):
        track_signature_v2("", 1)


def test_uid_format_fixed_width_uppercase_base36():
    uid = _cell_uid_from_signature(track_signature_v2(_SCAN, 7), width=10)
    assert len(uid) == 10
    assert uid == uid.upper()
    assert all(c.isdigit() or c.isalpha() for c in uid)


def test_label_coerced_to_int():
    # numpy integer labels arrive from label grids; the signature must not
    # depend on the integer's type or formatting.
    import numpy as np

    assert track_signature_v2(_SCAN, np.int32(7)) == track_signature_v2(_SCAN, 7)
