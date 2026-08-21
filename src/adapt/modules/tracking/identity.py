# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Stable cell-uid generation.

A cell's birth identity — the scan it was born in (scan_id, sha256 of the
raw file bytes) and its label within that scan — is hashed into a short
base36 token. scan_id is a pure function of the input and labels are unique
within a scan, so the uid is deterministic and collision-free by
construction, independent of which physics fields the source provides.
Pure functions only — no I/O, no state.
"""

import hashlib
import string

BASE36_UPPER = string.digits + string.ascii_uppercase


def _encode_base36(value: int) -> str:
    if value < 0:
        raise ValueError("value must be non-negative")
    if value == 0:
        return "0"
    chars: list[str] = []
    while value:
        value, remainder = divmod(value, 36)
        chars.append(BASE36_UPPER[remainder])
    return "".join(reversed(chars))


def _encode_base36_fixed(value: int, width: int) -> str:
    token = _encode_base36(value)
    return token.rjust(width, "0")


def track_signature_v2(scan_id: str, cell_label: int) -> str:
    """Birth signature: pure function of scan identity + label.

    scan_id is sha256 of the raw scan bytes; labels are unique within a
    scan — so the signature is deterministic and collision-free by
    construction, and independent of which physics fields exist.
    """
    if not scan_id:
        raise ValueError("track_signature_v2: scan_id is required")
    return f"v2|{scan_id}|{int(cell_label)}"


def _cell_uid_from_signature(signature: str, width: int) -> str:
    digest = hashlib.blake2b(signature.encode("utf-8"), digest_size=8).digest()
    value64 = int.from_bytes(digest, byteorder="big", signed=False)
    modulus = 36**width
    return _encode_base36_fixed(value64 % modulus, width=width)
