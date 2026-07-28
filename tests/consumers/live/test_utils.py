# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Unit tests for dashboard pure helpers."""

import logging
import os
import subprocess
import sys
from datetime import UTC, datetime

import pytest

from adapt.api.domain import Run
from adapt.consumers.live._utils import (
    _suppress_osx_stderr,
    adapt_cmd,
    format_run_labels,
    is_repository,
    safe_close,
    startup_repo,
)

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
    handle = (tmp_path / "scratch.txt").open("w", encoding="utf-8")

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


def test_suppress_osx_stderr_restores_the_stream_it_borrowed():
    """The macOS-only fd-2 redirect must leave stderr exactly as it found it.

    It swaps file descriptor 2 at the OS level, so a leaked or mis-restored
    descriptor silences every later error on the process. Off macOS it must do
    nothing at all: under a GUI launcher with no console (pythonw.exe) there is
    no fd 2 to dup, and os.dup(2) would raise.
    """
    before = os.dup(2)
    try:
        with _suppress_osx_stderr():
            pass
    finally:
        os.close(before)

    # Still usable afterwards — a botched restore shows up here.
    os.write(2, b"")
    assert sys.stderr is not None


def _repo(path):
    """A directory carrying the marker that makes it an Adapt repository root."""
    path.mkdir(parents=True, exist_ok=True)
    (path / "adapt_registry.db").touch()
    return path


def test_startup_repo_prefers_an_explicit_argument(tmp_path):
    other = _repo(tmp_path / "other")
    assert startup_repo(str(other), _repo(tmp_path / "cwd"), ["/recent"]) == str(other)


def test_startup_repo_prefers_the_working_directory_over_history(tmp_path):
    """`cd my_case && adapt dashboard` must show my_case.

    Falling back to the most recent repository would silently open a returning
    user's *previous* case while they sit in the new one.
    """
    cwd = _repo(tmp_path / "my_case")
    assert startup_repo(None, cwd, ["/somewhere/else"]) == str(cwd)


def test_startup_repo_falls_back_to_history_outside_a_repository(tmp_path):
    plain = tmp_path / "not_a_repo"
    plain.mkdir()
    assert startup_repo(None, plain, ["/recent", "/older"]) == "/recent"


def test_startup_repo_returns_none_with_nothing_to_open(tmp_path):
    plain = tmp_path / "not_a_repo"
    plain.mkdir()
    assert startup_repo(None, plain, []) is None


def test_a_plain_directory_is_not_a_repository(tmp_path):
    assert is_repository(tmp_path) is False
    assert is_repository(_repo(tmp_path / "repo")) is True
