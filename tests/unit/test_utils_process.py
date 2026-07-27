"""Tests for the portable process-liveness probe."""

import os
import subprocess
import sys

from adapt.utils.process import process_alive


def test_own_process_is_alive():
    assert process_alive(os.getpid()) is True


def test_running_child_is_alive():
    """A live child must be reported alive — and must survive being probed.

    On Windows os.kill(pid, 0) routes to TerminateProcess, so a naive probe
    kills the process it was asked about. The child staying alive across the
    probe is the behaviour that matters.
    """
    proc = subprocess.Popen(
        [sys.executable, "-c", "import sys; sys.stdin.read()"], stdin=subprocess.PIPE
    )
    try:
        assert process_alive(proc.pid) is True
        assert proc.poll() is None  # the probe must not have killed it
    finally:
        proc.stdin.close()
        proc.wait(timeout=10)


def test_exited_process_is_not_alive():
    proc = subprocess.Popen([sys.executable, "-c", ""])
    proc.wait(timeout=10)
    assert process_alive(proc.pid) is False
