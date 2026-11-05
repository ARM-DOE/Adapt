# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Lineage partner choice and the identity rule at splits and merges.

A fragment is a split child of the continuing parent whose grown footprint
covers the largest share of it; a vanished cell is a merge source of the
continuing cell covering the largest share of its footprint. When a split or
merge leaves two candidates for one identity, the identity follows the larger
score ``S = log A + γ log ē``; within the noise margin it follows the candidate
nearer the predicted position. Pure functions.
"""

import math
from collections.abc import Hashable
from dataclasses import dataclass

__all__ = [
    "RULE_NEAREST",
    "RULE_SCORE",
    "IdentityCandidate",
    "best_partner",
    "choose_identity",
    "identity_score",
]

RULE_SCORE = "SCORE"
RULE_NEAREST = "NEAREST"


def best_partner(fractions: dict[int, float], threshold: float) -> int | None:
    """The key with the largest fraction ≥ ``threshold`` (smaller key on a tie), else None."""
    qualified = [(-f, k) for k, f in fractions.items() if f >= threshold]
    return min(qualified)[1] if qualified else None


def identity_score(area_km2: float, mean_excess: float, gamma: float) -> float:
    """``S = log A + γ log ē``; γ = 1 ranks by intensity mass, γ = 0 by area.

    A cell with no excess (ē = 0) has no core and scores −∞ for γ > 0.
    """
    if gamma == 0.0:
        return math.log(area_km2)
    if mean_excess == 0.0:
        return -math.inf
    return math.log(area_km2) + gamma * math.log(mean_excess)


@dataclass(frozen=True)
class IdentityCandidate:
    """One contender for an identity: its score and its shape-aware distance
    to the predicted position."""

    key: Hashable
    score: float
    distance: float


def choose_identity(candidates: list[IdentityCandidate], margin: float) -> tuple[Hashable, str]:
    """``(winner key, rule)``.

    Candidates scoring within ``margin`` of the best are indistinguishable by
    intensity; among them the nearest wins (RULE_NEAREST). A single clear
    leader wins on score (RULE_SCORE).
    """
    top = max(c.score for c in candidates)
    close = [c for c in candidates if c.score >= top - margin]
    if len(close) == 1:
        return close[0].key, RULE_SCORE
    nearest = min(close, key=lambda c: (c.distance, -c.score))
    return nearest.key, RULE_NEAREST
