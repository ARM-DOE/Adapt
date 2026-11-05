# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Physical plausibility of a link: hard speed gates, heading term, crossing.

A pair failing a speed gate is removed whatever its overlap. The heading term
is soft (part of the cost); the crossing test is hard and parameter-free.
Positions are metres, speeds m/s. Pure functions — no tracker state.
"""

import math
from collections.abc import Hashable
from dataclasses import dataclass

__all__ = [
    "GATE_SPEED",
    "GATE_SPEED_CHANGE",
    "SpeedGate",
    "drop_crossing_links",
    "heading_term",
    "segments_cross",
    "speed_change_limit",
    "speed_gate",
]

GATE_SPEED = "SPEED"
GATE_SPEED_CHANGE = "SPEED_CHANGE"

Point = tuple[float, float]


def speed_change_limit(v_prev: float, dt_s: float, a_max: float, sigma_x_m: float) -> float:
    """``v_prev + a_max Δt + 2√2 σ_x / Δt``.

    Acceleration over the interval plus the apparent speed change a position
    error σ_x at both ends of two consecutive steps produces; the second term
    dominates short intervals, so the limit adapts to scan cadence.
    """
    return v_prev + a_max * dt_s + 2.0 * math.sqrt(2.0) * sigma_x_m / dt_s


@dataclass(frozen=True)
class SpeedGate:
    """``code`` is None when the pair passes; ``limit_ms`` is the speed-change limit applied."""

    code: str | None
    limit_ms: float | None


def speed_gate(
    speed_ms: float,
    v_prev: float | None,
    dt_s: float,
    v_max: float,
    a_max: float,
    sigma_x_m: float,
) -> SpeedGate:
    """Absolute cap first; the additive speed-change limit once the track has a speed."""
    limit = None if v_prev is None else speed_change_limit(v_prev, dt_s, a_max, sigma_x_m)
    if speed_ms > v_max:
        return SpeedGate(GATE_SPEED, limit)
    if limit is not None and speed_ms > limit:
        return SpeedGate(GATE_SPEED_CHANGE, limit)
    return SpeedGate(None, limit)


def heading_term(
    prev_step: Point | None, step: Point, weight: float, min_step_m: float
) -> tuple[float, float | None]:
    """``(h, Δθ in degrees)`` with ``h = w_h (1 − cos Δθ) / 2``.

    Applied only when both displacements exceed ``min_step_m``; below it the
    direction is position noise and the term is (0, None).
    """
    if prev_step is None:
        return 0.0, None
    prev_len, step_len = math.hypot(*prev_step), math.hypot(*step)
    if prev_len <= min_step_m or step_len <= min_step_m:
        return 0.0, None
    cos_turn = (prev_step[0] * step[0] + prev_step[1] * step[1]) / (prev_len * step_len)
    cos_turn = min(1.0, max(-1.0, cos_turn))
    return weight * (1.0 - cos_turn) / 2.0, math.degrees(math.acos(cos_turn))


def _orientation(a: Point, b: Point, c: Point) -> int:
    cross = (b[0] - a[0]) * (c[1] - a[1]) - (b[1] - a[1]) * (c[0] - a[0])
    return int(cross > 0) - int(cross < 0)


def segments_cross(a0: Point, a1: Point, b0: Point, b1: Point) -> bool:
    """True when the two segments properly intersect.

    Each segment's endpoints lie strictly on opposite sides of the other's
    line. Shared endpoints (split, merge), touching and collinear segments are
    not crossings.
    """
    return (
        _orientation(a0, a1, b0) * _orientation(a0, a1, b1) < 0
        and _orientation(b0, b1, a0) * _orientation(b0, b1, a1) < 0
    )


def drop_crossing_links(
    links: dict[Hashable, tuple[Point, Point, float]],
) -> tuple[list, dict]:
    """Keep links cheapest first; a link crossing a kept one is dropped.

    ``links`` maps a key to ``(start, end, cost)``. Returns the kept keys
    (cheapest first) and ``{dropped key: key of the kept link it crossed}``.
    """
    kept: list = []
    dropped: dict = {}
    for key, (start, end, _) in sorted(links.items(), key=lambda kv: (kv[1][2], kv[0])):
        blocker = next((k for k in kept if segments_cross(start, end, *links[k][:2])), None)
        if blocker is None:
            kept.append(key)
        else:
            dropped[key] = blocker
    return kept, dropped
