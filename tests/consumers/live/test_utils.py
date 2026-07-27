# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Unit tests for dashboard pure helpers."""

import logging
import subprocess
import sys
from datetime import UTC, datetime

import pytest

from adapt.api.domain import Run
from adapt.consumers.live._utils import adapt_cmd, format_run_labels, safe_close

pytestmark = pytest.mark.unit


def _run(run_id: str, start: datetime | None) -> Run:
    return Run(
        run_id=run_id,
        radar_id="KHGX",
        start_time=start,  # type: ignore[arg-type]
        end_time=None,
        status="running",
        mode="realtime",
    )


def test_format_run_labels_shows_run_id_and_month_day_time():
    labels = format_run_labels(
        [_run("2026JUL15-1540-KHGX", datetime(2026, 7, 15, 15, 40, tzinfo=UTC))]
    )
    assert labels == ["2026JUL15-1540-KHGX  (07-15 15:40)"]


def test_format_run_labels_marks_missing_start_time():
    assert format_run_labels([_run("R1", None)]) == ["R1  (?)"]


def test_adapt_cmd_is_runnable_on_this_interpreter():
    """The dashboard must launch the pipeline with a command the OS can execute.

    Resolving a bare, extensionless ``adapt`` next to the interpreter is a POSIX
    assumption: on Windows console scripts are ``Scripts\\adapt.exe``, and handing
    CreateProcess a non-executable file fails with WinError 11 (bad format).
    Running the module through the current interpreter is correct everywhere.
    """
    assert adapt_cmd() == [sys.executable, "-m", "adapt.cli"]
    assert subprocess.run([*adapt_cmd(), "--help"], capture_output=True).returncode == 0


def test_safe_close_closes_an_open_resource(tmp_path):
    """safe_close releases the resource so its file descriptor is freed."""
    handle = (tmp_path / "scratch.txt").open("w")

    safe_close(handle, "scratch file", logging.getLogger("adapt.test"))

    assert handle.closed


def test_safe_close_logs_but_does_not_raise_on_close_error(caplog):
    """A close that raises is logged (so the leak is visible) and swallowed, so a
    failing teardown never aborts the surrounding shutdown/redraw."""

    class _Boom:
        def close(self):
            raise OSError("device gone")

    with caplog.at_level(logging.WARNING):
        safe_close(_Boom(), "cache file", logging.getLogger("adapt.test"))

    assert "cache file" in caplog.text
