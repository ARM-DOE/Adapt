# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Tk ``after`` id bookkeeping for the dashboard — no Tk import here.

A self-rescheduling timer (auto-refresh, status tick, replay loop, pid poll)
fires forever; tracking its ids in a plain list makes that list grow without
bound over a long session. ``AfterHandles`` keeps a single live id per named
recurring timer (replacing the previous) and cancels everything on shutdown.

``schedule``/``cancel`` are injected (Tk's ``after``/``after_cancel``) so the
bookkeeping is unit-testable without a display.
"""

from collections.abc import Callable

__all__ = ["AfterHandles"]

Schedule = Callable[[int, Callable[[], None]], str]
Cancel = Callable[[str], None]


class AfterHandles:
    """Owns the after() ids for one dashboard component."""

    def __init__(self, schedule: Schedule, cancel: Cancel) -> None:
        self._schedule = schedule
        self._cancel = cancel
        self._recurring: dict[str, str] = {}
        self._oneshots: list[str] = []

    def recurring(self, name: str, delay_ms: int, callback: Callable[[], None]) -> str:
        """Schedule a self-rescheduling timer, cancelling any prior id for *name*
        so at most one id per recurring timer is ever outstanding."""
        prev = self._recurring.get(name)
        if prev is not None:
            self._cancel(prev)
        token = self._schedule(delay_ms, callback)
        self._recurring[name] = token
        return token

    def oneshot(self, delay_ms: int, callback: Callable[[], None]) -> str:
        """Schedule a one-shot timer (bounded by user actions, not by time)."""
        token = self._schedule(delay_ms, callback)
        self._oneshots.append(token)
        return token

    def cancel_all(self) -> None:
        """Cancel every outstanding timer and forget them."""
        for token in (*self._recurring.values(), *self._oneshots):
            self._cancel(token)
        self._recurring.clear()
        self._oneshots.clear()
