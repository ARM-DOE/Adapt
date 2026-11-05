# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Structured tracking diagnostic codes (logged; not pipeline-fatal)."""

from enum import StrEnum

__all__ = ["TrackingError"]


class TrackingError(StrEnum):
    """Scan-cadence codes: why a scan pair restarted every track, or was flagged."""

    NON_MONOTONIC_TIME = "NON_MONOTONIC_TIME"
    TRACK_GAP_EXCEEDED = "TRACK_GAP_EXCEEDED"
    IRREGULAR_SCAN_CADENCE = "IRREGULAR_SCAN_CADENCE"
