"""Cross-platform process primitives.

Every test here runs the real OS primitive on whatever platform the suite is on.
That is the point: each of these functions has a platform branch, so a branch is
only ever exercised by the CI job for that OS, and a bug in one is invisible
everywhere else until it reaches a user.
"""

import os
import subprocess
import sys

import pytest

from adapt.utils.process import (
    detached_process_kwargs,
    process_alive,
    terminate_process_tree,
)

pytestmark = pytest.mark.unit

# Sleeps far longer than any assertion below, so the child is unambiguously alive
# until something stops it.
_LONG_LIVED = [sys.executable, "-c", "import time; time.sleep(300)"]


@pytest.fixture
def detached_child():
    """A live child in its own signal group, reaped however the test ends."""
    proc = subprocess.Popen(_LONG_LIVED, **detached_process_kwargs())
    yield proc
    if proc.poll() is None:
        proc.kill()
    proc.wait(timeout=10)


# ── Liveness ──────────────────────────────────────────────────────────────────


def test_own_process_is_alive():
    assert process_alive(os.getpid()) is True


def test_running_child_is_alive_and_survives_the_probe(detached_child):
    """The probe must report liveness without disturbing the process.

    On Windows os.kill(pid, 0) routes to TerminateProcess, so the naive probe
    kills what it was asked about. The child still running afterwards is the
    behaviour that matters.
    """
    assert process_alive(detached_child.pid) is True
    assert detached_child.poll() is None


def test_exited_process_is_not_alive():
    """An exited process reads as gone even while its parent still holds it.

    ``proc`` stays referenced on purpose: on Windows a process object survives as
    long as any handle to it is open, so the Popen handle keeps the exited child
    openable. A probe that only asks "can I open this PID?" answers True forever,
    and the dashboard would never let a new pipeline start.
    """
    proc = subprocess.Popen([sys.executable, "-c", ""])
    proc.wait(timeout=10)
    assert process_alive(proc.pid) is False


# ── Detached launch + termination ─────────────────────────────────────────────


def test_detached_kwargs_are_accepted_by_popen(detached_child):
    """The kwargs must be ones Popen accepts on *this* platform.

    start_new_session is POSIX-only and creationflags is Windows-only; naming the
    wrong one is a TypeError or a silently ignored request, and the dashboard
    would lose the ability to stop the pipeline without stopping itself.
    """
    assert detached_child.poll() is None


def test_terminate_stops_a_detached_child(detached_child):
    terminate_process_tree(detached_child, force=False)
    assert detached_child.wait(timeout=10) is not None
    assert process_alive(detached_child.pid) is False


def test_force_terminate_stops_a_detached_child(detached_child):
    terminate_process_tree(detached_child, force=True)
    assert detached_child.wait(timeout=10) is not None
    assert process_alive(detached_child.pid) is False


def test_terminate_is_safe_on_an_already_exited_child():
    """Shutdown paths call this after a race may already have reaped the child."""
    proc = subprocess.Popen([sys.executable, "-c", ""])
    proc.wait(timeout=10)

    terminate_process_tree(proc, force=False)  # must not raise
    terminate_process_tree(proc, force=True)
