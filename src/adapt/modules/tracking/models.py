# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Value types for the tracker: match methods, error/diagnostic codes, motion state.

Plain enums and frozen dataclasses — no logic, no I/O. Shared by the matching,
motion, and event layers so a decision and its explanation travel together.
"""

from dataclasses import dataclass
from enum import StrEnum

__all__ = [
    "MatchDiagnostics",
    "MatchMethod",
    "TrackMotionState",
    "TrackingError",
]


class MatchMethod(StrEnum):
    """How an accepted match was decided."""

    PROPAGATED = "PROPAGATED"  # deterministic constraint propagation (no optimisation)
    HUNGARIAN = "HUNGARIAN"  # optimal assignment inside an ambiguous component
    SPLIT = "SPLIT"
    MERGE = "MERGE"


class TrackingError(StrEnum):
    """Structured tracking diagnostic codes (logged; not pipeline-fatal)."""

    # Scan-cadence / gap problems
    NON_MONOTONIC_TIME = "NON_MONOTONIC_TIME"
    TRACK_GAP_EXCEEDED = "TRACK_GAP_EXCEEDED"
    IRREGULAR_SCAN_CADENCE = "IRREGULAR_SCAN_CADENCE"
    # Physical-constraint rejections
    VELOCITY_EXCEEDED = "VELOCITY_EXCEEDED"
    ACCELERATION_EXCEEDED = "ACCELERATION_EXCEEDED"


@dataclass(frozen=True)
class MatchDiagnostics:
    """Per-accepted-match explainability record (persisted with the event row).

    ``opc``/``ocp`` are the bidirectional overlap fractions; ``centroid_distance_m``
    is the projected-hull→candidate residual; ``final_cost`` is ``m + d/L`` (+ penalty).
    """

    opc: float | None = None
    ocp: float | None = None
    centroid_distance_m: float | None = None
    speed_ms: float | None = None
    heading_change_deg: float | None = None
    area_ratio: float | None = None
    final_cost: float | None = None
    match_method: str | None = None


@dataclass(frozen=True)
class TrackMotionState:
    """Per-track velocity carried forward for acceleration and heading checks.

    ``speed`` is in m/s; ``heading`` is in radians measured as ``atan2(vy, vx)``.
    ``has_velocity`` is False until a track has been observed across two scans.
    """

    speed: float = 0.0
    heading: float = 0.0
    has_velocity: bool = False
