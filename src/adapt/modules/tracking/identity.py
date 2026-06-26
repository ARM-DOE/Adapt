# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Stable cell-uid generation.

A cell's birth state (quantized time, location, intensity, area) is hashed into a
short base36 token. Quantization makes the token robust to small input variation;
the hash makes it stable and reproducible. Pure functions only — no I/O, no state.
"""

import hashlib
import string

BASE36_UPPER = string.digits + string.ascii_uppercase


def _quantize(value: float, step: float) -> int:
    # this is for creating stable hashes that are robust to small variations in the input values
    if step <= 0:
        raise ValueError("step must be positive")
    return int(round(value / step))


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


def _track_signature_from_birth(
    scan_start_time_epoch_s: float,
    centroid_lat_deg: float,
    centroid_lon_deg: float,
    max_dbz: float,
    max_zdr: float,
    area40_km2: float,
    *,
    time_step_s: int,
    latlon_step_deg: float,
    area_step_km2: float,
    signature_version: str = "v1",
) -> str:
    tq = _quantize(scan_start_time_epoch_s, time_step_s)
    latq = _quantize(centroid_lat_deg, latlon_step_deg)
    lonq = _quantize(centroid_lon_deg, latlon_step_deg)
    dbzq = int(round(max_dbz))
    zdrq = int(round(max_zdr * 10.0))
    a40q = _quantize(area40_km2, area_step_km2)
    return f"{signature_version}|{tq}|{latq}|{lonq}|{dbzq}|{zdrq}|{a40q}"


def _cell_uid_from_signature(signature: str, width: int) -> str:
    digest = hashlib.blake2b(signature.encode("utf-8"), digest_size=8).digest()
    value64 = int.from_bytes(digest, byteorder="big", signed=False)
    modulus = 36**width
    return _encode_base36_fixed(value64 % modulus, width=width)
