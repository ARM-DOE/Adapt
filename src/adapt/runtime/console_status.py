# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""A single transient console status line that overwrites itself in place.

Shows what the pipeline is doing between the permanent per-scan log lines —
``⠹ processing <scan_id> · 12s`` / ``⠋ waiting for next scan · 1m4s`` — animated by
the orchestrator's monitor loop and erased before each permanent console line prints
(via StatusAwareStreamHandler in logging_setup). TTY only: on a non-tty stream, or
when disabled, every method is a no-op so redirected output and file logs stay clean.

Stdlib only — no tqdm/rich. The elapsed clock is injected (UI only; not science).
"""

from __future__ import annotations

import sys
import threading
from collections.abc import Callable
from typing import TextIO

from adapt.runtime.run_reporter import format_seconds

__all__ = ["ConsoleStatus"]

_SPINNER = "⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏"
_ERASE = "\r\x1b[K"  # carriage return + ANSI erase-to-end-of-line


class ConsoleStatus:
    """Thread-safe manager for one self-overwriting status line on a TTY stream."""

    def __init__(
        self, stream: TextIO | None = None, *, enabled: bool, clock: Callable[[], float]
    ) -> None:
        self.stream: TextIO = stream if stream is not None else sys.stderr
        self._clock = clock
        # Active only on a real terminal — otherwise the carriage returns would
        # corrupt piped output and log files.
        self._active = enabled and bool(getattr(self.stream, "isatty", lambda: False)())
        self.lock = threading.RLock()  # re-entrant: the log handler clears while holding it
        self._text = ""
        self._t0 = clock()
        self._frame = 0
        self._drawn = False

    def set(self, text: str) -> None:
        """Set the current activity text; reset the elapsed timer only if it changed."""
        if not self._active:
            return
        with self.lock:
            if text != self._text:
                self._text = text
                self._t0 = self._clock()

    def tick(self) -> None:
        """Redraw the line in place, advancing the spinner and the elapsed counter."""
        if not self._active:
            return
        with self.lock:
            frame = _SPINNER[self._frame % len(_SPINNER)]
            self._frame += 1
            elapsed = format_seconds(self._clock() - self._t0)
            self.stream.write(f"\r{frame} {self._text} · {elapsed}\x1b[K")
            self.stream.flush()
            self._drawn = True

    def clear(self) -> None:
        """Erase the transient line if one is drawn (before a permanent line, or on stop)."""
        if not self._active:
            return
        with self.lock:
            if self._drawn:
                self.stream.write(_ERASE)
                self.stream.flush()
                self._drawn = False
