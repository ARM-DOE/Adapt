# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Time normalization helpers shared across Adapt modules."""

import contextlib
from datetime import UTC, datetime

import numpy as np

# The single authoritative scan-time string format. Rows join on scan_id (the
# content-derived scan identity); scan_time is ordering/display metadata — but
# every table must still store the identical string or time-ordered reads
# silently disagree. Defined exactly once; serialize with to_scan_iso, parse
# with from_scan_iso. Never hardcode this elsewhere — tests/test_architecture.py
# enforces that the literal appears only in this file.
_SCAN_ISO_FORMAT = "%Y-%m-%dT%H:%M:%SZ"


def normalize_time_scalar(time_val):
    """Normalize xarray/cftime/numpy time representations to a scalar."""
    tv = time_val
    while isinstance(tv, np.ndarray) and tv.size == 1:
        tv = tv.reshape(-1)[0]
    if isinstance(tv, np.ndarray):
        tv = tv.reshape(-1)[0]

    if hasattr(tv, "item"):
        with contextlib.suppress(TypeError, ValueError):
            tv = tv.item()

    if getattr(type(tv), "__module__", "").startswith("cftime"):
        tv = datetime(
            int(tv.year),
            int(tv.month),
            int(tv.day),
            int(tv.hour),
            int(tv.minute),
            int(tv.second),
            int(getattr(tv, "microsecond", 0) or 0),
            tzinfo=UTC,
        )

    return tv


def _to_utc_datetime(dt) -> datetime:
    """Normalize any datetime-like to a tz-aware UTC datetime (naive treated as UTC).

    Shared by to_scan_iso and to_scan_unix so the string and machine-readable
    representations always describe the identical instant. Accepts Python datetime,
    pandas Timestamp (a datetime subclass), and numpy datetime64.
    """
    if isinstance(dt, str):
        dt = datetime.fromisoformat(dt)  # accepts "...Z" and naive ISO (3.11+)
    elif isinstance(dt, np.datetime64):
        # via [us] to avoid numpy's datetime64[ns].astype(datetime) -> int gotcha
        dt = dt.astype("datetime64[us]").astype(datetime)
    if not isinstance(dt, datetime):
        dt = normalize_time_scalar(dt)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt


def to_scan_iso(dt) -> str:
    """Canonical scan-time string — ordering/display metadata, one format everywhere.

    Rows join on ``scan_id``; scan_time orders and labels them. Matches
    ``cells_by_scan`` (track_store ``_to_iso``) so every table's time strings
    compare consistently. Whole-second resolution.
    """
    return _to_utc_datetime(dt).strftime(_SCAN_ISO_FORMAT)


def to_scan_unix(dt) -> int:
    """Machine-readable scan time: UTC epoch seconds for the SAME instant as to_scan_iso.

    Whole seconds (sub-second dropped, matching the ISO format) so the string and
    integer representations are always consistent. Every derived table carries both.
    """
    return int(_to_utc_datetime(dt).timestamp())


def from_scan_iso(s: str) -> datetime:
    """Parse a canonical scan-time string (see ``to_scan_iso``) to a UTC datetime.

    The inverse of ``to_scan_iso``. Kept here so the scan-time format is defined in
    exactly one place — both serialization and parsing share ``_SCAN_ISO_FORMAT``.
    """
    return datetime.strptime(s, _SCAN_ISO_FORMAT).replace(tzinfo=UTC)
