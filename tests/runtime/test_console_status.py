# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""ConsoleStatus: a single transient TTY line that overwrites itself in place.

Behaviour under test: on a TTY it draws ``\\r<spinner> <text> · <elapsed>\\x1b[K``,
advances the spinner each tick, resets the elapsed timer only when the text changes,
and clears with ``\\r\\x1b[K``. On a non-TTY (or when disabled) it is completely inert,
so redirected output and file logs never see control characters.
"""

import io
import logging

import pytest

from adapt.runtime.console_status import ConsoleStatus
from adapt.runtime.logging_setup import StatusAwareStreamHandler

pytestmark = pytest.mark.unit


class _FakeTTY(io.StringIO):
    def isatty(self) -> bool:
        return True


def _status(stream, now):
    return ConsoleStatus(stream, enabled=True, clock=lambda: now[0])


def test_tick_draws_overwriting_line_with_spinner_text_and_elapsed():
    out = _FakeTTY()
    now = [10.0]
    cs = _status(out, now)

    cs.set("processing KOHX_1")
    now[0] = 13.0
    cs.tick()

    drawn = out.getvalue()
    assert drawn.startswith("\r")  # overwrites from line start
    assert "processing KOHX_1" in drawn
    assert "3.0s" in drawn  # elapsed since the text was set
    assert "\x1b[K" in drawn  # erase-to-end so shorter text doesn't leave residue


def test_tick_advances_spinner_frame():
    out = _FakeTTY()
    cs = _status(out, [0.0])
    cs.set("waiting for next scan")
    cs.tick()
    cs.tick()
    # Two ticks -> two different leading spinner glyphs.
    lines = [seg for seg in out.getvalue().split("\r") if seg]
    assert lines[0][0] != lines[1][0]


def test_elapsed_resets_only_when_text_changes():
    out = _FakeTTY()
    now = [10.0]
    cs = _status(out, now)

    cs.set("a")
    now[0] = 22.0
    cs.set("a")  # same text -> timer NOT reset
    cs.tick()
    assert "12.0s" in out.getvalue().rsplit("\r", 1)[-1]

    now[0] = 30.0
    cs.set("b")  # new text -> timer resets to now
    now[0] = 31.0
    cs.tick()
    assert "1.0s" in out.getvalue().rsplit("\r", 1)[-1]


def test_clear_erases_the_line_and_is_idempotent():
    out = _FakeTTY()
    cs = _status(out, [0.0])
    cs.set("x")
    cs.tick()
    out.truncate(0)
    out.seek(0)

    cs.clear()
    assert out.getvalue() == "\r\x1b[K"

    out.truncate(0)
    out.seek(0)
    cs.clear()  # nothing drawn now -> no-op
    assert out.getvalue() == ""


def test_non_tty_stream_is_completely_inert():
    out = io.StringIO()  # isatty() is False
    cs = ConsoleStatus(out, enabled=True, clock=lambda: 0.0)
    cs.set("x")
    cs.tick()
    cs.clear()
    assert out.getvalue() == ""


def test_disabled_is_inert_even_on_a_tty():
    out = _FakeTTY()
    cs = ConsoleStatus(out, enabled=False, clock=lambda: 0.0)
    cs.set("x")
    cs.tick()
    assert out.getvalue() == ""


def test_status_aware_handler_erases_transient_line_before_the_record():
    out = _FakeTTY()
    cs = _status(out, [0.0])
    cs.set("processing KOHX_1")
    cs.tick()  # a transient line is now drawn
    out.truncate(0)
    out.seek(0)

    handler = StatusAwareStreamHandler(cs)
    handler.setFormatter(logging.Formatter("%(message)s"))
    handler.emit(logging.LogRecord("adapt.run", logging.INFO, __file__, 1, "scan done", None, None))

    written = out.getvalue()
    assert written.startswith("\r\x1b[K")  # erase first
    assert "scan done" in written  # then the permanent line


def test_status_aware_handler_on_non_tty_just_writes_the_record():
    out = io.StringIO()
    cs = ConsoleStatus(out, enabled=True, clock=lambda: 0.0)
    handler = StatusAwareStreamHandler(cs)
    handler.setFormatter(logging.Formatter("%(message)s"))
    handler.emit(logging.LogRecord("adapt.run", logging.INFO, __file__, 1, "hello", None, None))

    assert "\x1b" not in out.getvalue()
    assert "hello" in out.getvalue()
