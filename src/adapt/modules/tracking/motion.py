# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Physical motion checks for candidate matching.

``MotionValidator`` applies hard kinematic limits that reject impossible
candidate pairs *before* matching; the heading helpers quantify direction change
for the soft crossing-prevention penalty. Pure geometry — no graph, no I/O.
"""

import math
from dataclasses import dataclass

from adapt.modules.tracking.models import TrackingError

__all__ = [
    "MotionDecision",
    "MotionValidator",
    "heading_change_degrees",
    "heading_change_radians",
]


def heading_change_radians(prev_heading: float, curr_heading: float) -> float:
    """Smallest absolute heading change (radians, in [0, π]) between two directions."""
    delta = curr_heading - prev_heading
    return abs(math.atan2(math.sin(delta), math.cos(delta)))


def heading_change_degrees(prev_heading: float, curr_heading: float) -> float:
    """Smallest absolute heading change (degrees) between two directions (radians)."""
    return math.degrees(heading_change_radians(prev_heading, curr_heading))


@dataclass(frozen=True)
class MotionDecision:
    """Outcome of a physical-motion check for one candidate pair."""

    ok: bool
    speed_ms: float
    code: TrackingError | None = None


class MotionValidator:
    """Reject candidate pairs that violate hard kinematic limits.

    A pair is rejected when its implied speed exceeds ``max_speed_ms`` (absolute
    cap) or ``max_speed_multiplier * previous_speed`` (acceleration cap). Rejected
    pairs never reach overlap or Hungarian matching.
    """

    def __init__(self, max_speed_ms: float, max_speed_multiplier: float):
        self.max_speed_ms = max_speed_ms
        self.max_speed_multiplier = max_speed_multiplier

    @staticmethod
    def speed_ms(prev_x: float, prev_y: float, curr_x: float, curr_y: float, dt_s: float) -> float:
        if dt_s <= 0:
            raise ValueError("dt_s must be positive for a speed estimate")
        return math.hypot(curr_x - prev_x, curr_y - prev_y) / dt_s

    def check(
        self,
        prev_x: float,
        prev_y: float,
        curr_x: float,
        curr_y: float,
        dt_s: float,
        previous_speed: float | None = None,
    ) -> MotionDecision:
        speed = self.speed_ms(prev_x, prev_y, curr_x, curr_y, dt_s)
        if speed > self.max_speed_ms:
            return MotionDecision(False, speed, TrackingError.VELOCITY_EXCEEDED)
        if (
            previous_speed is not None
            and previous_speed > 0.0
            and speed > self.max_speed_multiplier * previous_speed
        ):
            return MotionDecision(False, speed, TrackingError.ACCELERATION_EXCEEDED)
        return MotionDecision(True, speed, None)
