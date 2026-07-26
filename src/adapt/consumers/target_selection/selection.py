# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Output data model of the target selection engine."""

from dataclasses import dataclass
from datetime import datetime
from enum import Enum

from adapt.consumers.target_selection.snapshot import TrajectoryPoint


class SelectionReason(Enum):
    NEW_TARGET = "NEW_TARGET"
    CONTINUATION = "CONTINUATION"
    SWITCH = "SWITCH"


@dataclass(frozen=True)
class TargetSelection:
    """The engine's decision for one snapshot. Radar-agnostic.

    predicted_hulls is always None in v0.1 — hull polygons are not yet
    queryable from the repository; the field is the extension point.
    """

    cell_uid: str
    reason: SelectionReason
    score: float
    selection_time: datetime
    trajectory: tuple[TrajectoryPoint, ...]
    observation_window: tuple[datetime, datetime]
    predicted_hulls: tuple | None = None
