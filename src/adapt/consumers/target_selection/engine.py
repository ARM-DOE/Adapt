# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""The engine: a thin stateful shell around the pure rules.

State is exactly one field — the currently observed target. All time
comes from snapshots (never the wall clock), so identical snapshot
sequences produce identical selections.
"""

from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime, timedelta

from adapt.consumers.target_selection.config import TSEConfig
from adapt.consumers.target_selection.rules import (
    SelectionReason,
    StopReason,
    continue_or_switch,
    is_candidate,
    select_best,
    should_stop,
    total_score,
)
from adapt.consumers.target_selection.selection import TargetSelection
from adapt.consumers.target_selection.snapshot import CellSnapshot, Snapshot


@dataclass(frozen=True)
class CurrentTarget:
    """The engine's only state: which cell is being observed since when."""

    cell_uid: str
    started_at: datetime


class TargetSelectionEngine:
    def __init__(self, config: TSEConfig) -> None:
        self._config = config
        self._current: CurrentTarget | None = None

    @property
    def current_target(self) -> CurrentTarget | None:
        return self._current

    def select(self, snapshot: Snapshot) -> TargetSelection | None:
        """Apply the science rules to one snapshot and update state."""
        candidates = {c.uid: c for c in snapshot.cells if is_candidate(c, self._config.candidate)}
        scored = {uid: total_score(c, self._config) for uid, c in candidates.items()}

        if self._current is None:
            return self._start_new(snapshot, candidates, scored, exclude=())

        current = self._current
        current_cell = _find(snapshot.cells, current.cell_uid)
        observed = (snapshot.scan_time - current.started_at).total_seconds()
        stop = should_stop(current_cell, self._config, observed)
        if stop is not None:
            # A max-duration target would immediately win again on score;
            # exclude it from re-selection within this call only.
            exclude = (current.cell_uid,) if stop is StopReason.MAX_DURATION else ()
            self._current = None
            return self._start_new(snapshot, candidates, scored, exclude=exclude)

        uid, reason = continue_or_switch(
            current.cell_uid, scored, self._config.selection.switch_margin
        )
        if reason is SelectionReason.SWITCH:
            current = CurrentTarget(uid, snapshot.scan_time)
            self._current = current
        return self._selection(
            snapshot, candidates[uid], scored[uid], reason, started_at=current.started_at
        )

    def _start_new(
        self,
        snapshot: Snapshot,
        candidates: dict[str, CellSnapshot],
        scored: dict[str, float],
        *,
        exclude: tuple[str, ...],
    ) -> TargetSelection | None:
        eligible = {uid: s for uid, s in scored.items() if uid not in exclude}
        uid = select_best(eligible)
        if uid is None:
            return None
        self._current = CurrentTarget(uid, snapshot.scan_time)
        return self._selection(
            snapshot,
            candidates[uid],
            eligible[uid],
            SelectionReason.NEW_TARGET,
            started_at=self._current.started_at,
        )

    def _selection(
        self,
        snapshot: Snapshot,
        cell: CellSnapshot,
        score: float,
        reason: SelectionReason,
        *,
        started_at: datetime,
    ) -> TargetSelection:
        window_end = started_at + timedelta(seconds=self._config.selection.max_observation_seconds)
        return TargetSelection(
            cell_uid=cell.uid,
            reason=reason,
            score=score,
            selection_time=snapshot.scan_time,
            trajectory=cell.trajectory,
            observation_window=(snapshot.scan_time, window_end),
        )


def _find(cells: Iterable[CellSnapshot], uid: str) -> CellSnapshot | None:
    return next((c for c in cells if c.uid == uid), None)
