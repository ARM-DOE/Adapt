# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Target Selection Engine — selects which tracked cell to observe.

A Ring 2 consumer: reads only through adapt.api.RepositoryClient, never
writes into the Adapt repository, and never controls the radar.
"""

from adapt.consumers.target_selection.config import TSEConfig, load_config
from adapt.consumers.target_selection.engine import CurrentTarget, TargetSelectionEngine
from adapt.consumers.target_selection.repository_source import build_snapshot
from adapt.consumers.target_selection.rules import is_candidate, total_score
from adapt.consumers.target_selection.selection import SelectionReason, TargetSelection
from adapt.consumers.target_selection.snapshot import (
    CellSnapshot,
    Snapshot,
    TrajectoryPoint,
)
from adapt.consumers.target_selection.writer import append_selection

__all__ = [
    "CellSnapshot",
    "CurrentTarget",
    "SelectionReason",
    "Snapshot",
    "TSEConfig",
    "TargetSelection",
    "TargetSelectionEngine",
    "TrajectoryPoint",
    "append_selection",
    "build_snapshot",
    "is_candidate",
    "load_config",
    "total_score",
]
