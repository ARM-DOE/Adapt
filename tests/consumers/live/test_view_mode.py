# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Behavioural tests for the Latest Scan tab's view-mode state machine.

Pure state transitions — no Tk, no display. Each test asserts what one user
action preserves or resets (mode, loop, camera), so the dashboard's buttons have
a deterministic, testable single source of truth.
"""

import pytest

from adapt.consumers.live._view_mode import Camera, ScanViewState, ViewMode

pytestmark = pytest.mark.unit


def test_starts_in_latest_scan_not_looping():
    state = ScanViewState()
    assert state.mode is ViewMode.LATEST_SCAN
    assert state.loop_running is False


def test_enter_loop_activates_live_loop_and_flips_button_to_stop():
    state = ScanViewState()
    state.enter_loop()
    assert state.mode is ViewMode.LIVE_LOOP
    assert state.loop_running is True
    assert state.loop_button_label() == "Stop Loop"


def test_latest_scan_takes_ownership_and_stops_the_loop():
    state = ScanViewState()
    state.enter_loop()
    state.enter_latest_scan()
    assert state.mode is ViewMode.LATEST_SCAN
    assert state.loop_running is False
    assert state.loop_button_label() == "Show Loop"


def test_selecting_a_scan_stops_the_loop():
    state = ScanViewState()
    state.enter_loop()
    state.enter_selected_scan()
    assert state.mode is ViewMode.SELECTED_SCAN
    assert state.loop_running is False


def test_camera_is_preserved_across_latest_scan():
    state = ScanViewState()
    state.save_camera((2.0, 6.0), (1.0, 7.0))
    state.enter_latest_scan()
    assert state.camera == Camera(xlim=(2.0, 6.0), ylim=(1.0, 7.0))


def test_camera_is_preserved_when_entering_the_loop():
    """Starting the loop must not reset zoom — only Reset/Home change the camera."""
    state = ScanViewState()
    state.save_camera((2.0, 6.0), (1.0, 7.0))
    state.enter_loop()
    assert state.camera == Camera(xlim=(2.0, 6.0), ylim=(1.0, 7.0))


def test_reset_returns_to_latest_scan_stops_loop_and_clears_camera():
    state = ScanViewState()
    state.enter_loop()
    state.save_camera((2.0, 6.0), (1.0, 7.0))
    state.reset()
    assert state.mode is ViewMode.LATEST_SCAN
    assert state.loop_running is False
    assert state.camera == Camera()
