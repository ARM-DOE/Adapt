# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Viewing-mode state machine for the dashboard's Latest Scan tab — no Tk here.

The tab's buttons used to poke independent flags (a loop bool, a saved-zoom
tuple), so actions interacted inconsistently — e.g. "Show Latest" left a running
loop firing. ``ScanViewState`` is the single source of truth: each user action
is one explicit transition that says exactly what it preserves or resets. Being
Tk-free, it is unit-testable (the tab applies the widget/side effects).
"""

from dataclasses import dataclass, field
from enum import Enum

__all__ = ["Camera", "ScanViewState", "ViewMode"]


class ViewMode(Enum):
    """Mutually exclusive viewing modes of the Latest Scan tab."""

    LATEST_SCAN = "latest_scan"
    LIVE_LOOP = "live_loop"
    SELECTED_SCAN = "selected_scan"


@dataclass(frozen=True)
class Camera:
    """Saved map camera (axes limits), preserved across redraws, reset on Reset."""

    xlim: tuple[float, float] | None = None
    ylim: tuple[float, float] | None = None


@dataclass
class ScanViewState:
    """Current view mode + loop + camera for the Latest Scan tab."""

    mode: ViewMode = ViewMode.LATEST_SCAN
    loop_running: bool = False
    camera: Camera = field(default_factory=Camera)

    def enter_loop(self) -> None:
        """Start the live loop."""
        self.mode = ViewMode.LIVE_LOOP
        self.loop_running = True

    def enter_latest_scan(self) -> None:
        """Take ownership of the display for the newest scan; stop any loop.
        Camera is preserved so the user keeps their zoom."""
        self.mode = ViewMode.LATEST_SCAN
        self.loop_running = False

    def enter_selected_scan(self) -> None:
        """Browse to one chosen scan; stop any loop. Camera preserved."""
        self.mode = ViewMode.SELECTED_SCAN
        self.loop_running = False

    def save_camera(self, xlim: tuple[float, float], ylim: tuple[float, float]) -> None:
        """Remember the current axes limits across redraws."""
        self.camera = Camera(xlim=xlim, ylim=ylim)

    def reset(self) -> None:
        """Reset the view: newest scan, no loop, camera cleared to full extent."""
        self.mode = ViewMode.LATEST_SCAN
        self.loop_running = False
        self.camera = Camera()

    def loop_button_label(self) -> str:
        """The label the loop button should show — the action it will perform."""
        return "Stop Loop" if self.loop_running else "Show Loop"
