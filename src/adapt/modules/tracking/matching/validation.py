# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Hard geometric validation gate.

Removes obviously impossible candidate pairs using *bidirectional* overlap: a pair
survives only when the candidate is sufficiently covered by the projected hull
(Opc) AND the hull is sufficiently covered by the candidate (Ocp). Bidirectionality
rejects tiny cells inside large hulls, merged cells engulfing a prediction, and
grazing contacts — cases a single IoU or one-sided fraction would let through.

The kinematic gate (speed/acceleration) is applied by the orchestrator, which owns
per-track velocity; this stage is pure geometry and returns the overlaps so callers
can reuse them for the cost.
"""

from dataclasses import dataclass

import numpy as np

from adapt.modules.tracking.matching.geometry import bidirectional_overlap

__all__ = ["GeometricValidator", "ValidationResult"]


@dataclass(frozen=True)
class ValidationResult:
    """Overlaps for a candidate pair plus whether it clears the bidirectional gate."""

    opc: float
    ocp: float
    passed: bool


class GeometricValidator:
    """Bidirectional-overlap hard gate."""

    def __init__(self, min_candidate_overlap: float, min_projected_overlap: float):
        self.min_candidate_overlap = min_candidate_overlap
        self.min_projected_overlap = min_projected_overlap

    def validate(self, hull: np.ndarray, cell: np.ndarray) -> ValidationResult:
        opc, ocp = bidirectional_overlap(hull, cell)
        passed = opc >= self.min_candidate_overlap and ocp >= self.min_projected_overlap
        return ValidationResult(opc=opc, ocp=ocp, passed=passed)
