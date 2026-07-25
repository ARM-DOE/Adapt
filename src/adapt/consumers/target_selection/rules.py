# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Science rules as pure functions.

Scores are raw weighted sums — a cell's score depends only on the cell
itself plus fixed site geometry, so it stays stable as other cells
appear and disappear (the stability switch_margin relies on).
"""

import math
import operator
from collections.abc import Callable, Mapping, Sequence
from enum import Enum

from adapt.consumers.target_selection._geo import haversine_km
from adapt.consumers.target_selection.config import (
    CandidateConfig,
    PriorityWeights,
    SitePreferenceConfig,
    TSEConfig,
)
from adapt.consumers.target_selection.selection import SelectionReason
from adapt.consumers.target_selection.snapshot import CellSnapshot, TrajectoryPoint

# Gate operator → comparison. Keyed by the validated config Literal, so an
# unknown operator can never reach here (config validation rejects it first).
_GATE_OPS: dict[str, Callable[[float, float], bool]] = {
    "gt": operator.gt,
    "ge": operator.ge,
    "lt": operator.lt,
    "le": operator.le,
}


class StopReason(Enum):
    TARGET_LOST = "TARGET_LOST"
    QUALITY_DROPPED = "QUALITY_DROPPED"
    MAX_DURATION = "MAX_DURATION"


def is_candidate(cell: CellSnapshot, cfg: CandidateConfig) -> bool:
    """Candidate rule: every quality gate plus the age gate must pass.

    Being in the snapshot at all is the "active" clause — snapshots hold
    only latest-scan cells. A NaN gate value (SQL NULL) is a defined
    data condition: not a candidate. A missing gate column is a
    configuration error: raise.
    """
    for gate in cfg.gates:
        if gate.field not in cell.values:
            raise ValueError(
                f"Gate field {gate.field!r} not found for cell {cell.uid!r}; "
                f"available columns: {sorted(cell.values)}"
            )
        value = cell.values[gate.field]
        if math.isnan(value) or not _GATE_OPS[gate.op](value, gate.value):
            return False
    return cell.age_seconds >= cfg.min_age_seconds


def priority_score(cell: CellSnapshot, weights: PriorityWeights) -> float:
    """Priority rule: weighted sum of reflectivity, area, growth rate."""
    return (
        weights.reflectivity * cell.reflectivity_max
        + weights.area * cell.area_sqkm
        + weights.growth_rate * cell.growth_rate_sqkm_per_min
    )


def site_bonus(trajectory: Sequence[TrajectoryPoint], cfg: SitePreferenceConfig) -> float:
    """Site preference rule: each site's bonus is added once if any of the
    first projection_steps trajectory points falls within its radius."""
    points = trajectory[: cfg.projection_steps]
    bonus = 0.0
    for site in cfg.sites:
        if any(haversine_km(p.lat, p.lon, site.lat, site.lon) <= site.radius_km for p in points):
            bonus += site.bonus
    return bonus


def total_score(cell: CellSnapshot, cfg: TSEConfig) -> float:
    """Full score used for selection: priority plus site bonus."""
    return priority_score(cell, cfg.priority.weights) + site_bonus(
        cell.trajectory, cfg.site_preference
    )


def select_best(scored: Mapping[str, float]) -> str | None:
    """Selection rule: highest score wins; ties break to smallest uid."""
    if not scored:
        return None
    return min(scored, key=lambda uid: (-scored[uid], uid))


def continue_or_switch(
    current_uid: str, scored: Mapping[str, float], margin: float
) -> tuple[str, SelectionReason]:
    """Continuation rule: keep the current target unless a challenger
    exceeds its score by strictly more than switch_margin."""
    challengers = {uid: s for uid, s in scored.items() if uid != current_uid}
    best = select_best(challengers)
    if best is not None and challengers[best] > scored[current_uid] + margin:
        return best, SelectionReason.SWITCH
    return current_uid, SelectionReason.CONTINUATION


def should_stop(
    cell: CellSnapshot | None, cfg: TSEConfig, observed_seconds: float
) -> StopReason | None:
    """Stop rule, checked in order: target gone from the snapshot, quality
    gate no longer passed (age cannot regress, so quality is the only
    failable clause of is_candidate), max observation duration reached."""
    if cell is None:
        return StopReason.TARGET_LOST
    if not is_candidate(cell, cfg.candidate):
        return StopReason.QUALITY_DROPPED
    if observed_seconds >= cfg.selection.max_observation_seconds:
        return StopReason.MAX_DURATION
    return None
