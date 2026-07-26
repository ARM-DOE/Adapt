# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Unit tests for the dashboard after-id manager.

``schedule``/``cancel`` are injected fakes tracking live timer tokens, so the
bounded-growth behaviour is verified with no Tk and no display.
"""

import pytest

from adapt.consumers.live._timers import AfterHandles

pytestmark = pytest.mark.unit


class _FakeClock:
    """Records which timer tokens are currently live (scheduled, not cancelled)."""

    def __init__(self):
        self.live: set[str] = set()
        self._n = 0

    def schedule(self, _delay_ms, _callback) -> str:
        self._n += 1
        token = f"t{self._n}"
        self.live.add(token)
        return token

    def cancel(self, token: str) -> None:
        self.live.discard(token)


def test_recurring_same_name_keeps_one_live_timer():
    """A self-rescheduling timer re-registered under the same name leaves exactly
    one live token — the previous is cancelled, so pending ids never accumulate."""
    clock = _FakeClock()
    handles = AfterHandles(clock.schedule, clock.cancel)

    for _ in range(50):
        handles.recurring("status", 1000, lambda: None)

    assert len(clock.live) == 1


def test_distinct_recurring_names_are_independent():
    """Different recurring names each keep their own single live timer."""
    clock = _FakeClock()
    handles = AfterHandles(clock.schedule, clock.cancel)

    handles.recurring("refresh", 10_000, lambda: None)
    handles.recurring("status", 1000, lambda: None)
    handles.recurring("refresh", 10_000, lambda: None)

    assert len(clock.live) == 2


def test_cancel_all_clears_every_live_timer():
    """cancel_all cancels recurring and one-shot timers alike."""
    clock = _FakeClock()
    handles = AfterHandles(clock.schedule, clock.cancel)

    handles.recurring("status", 1000, lambda: None)
    handles.oneshot(100, lambda: None)
    handles.oneshot(200, lambda: None)

    handles.cancel_all()

    assert clock.live == set()
