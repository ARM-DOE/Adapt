# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Portable "is this PID still running?" probe.

``os.kill(pid, 0)`` is the POSIX idiom, but on Windows CPython routes every
signal other than ``CTRL_C_EVENT``/``CTRL_BREAK_EVENT`` to ``TerminateProcess``
— so the POSIX idiom *kills* the process it was asked to inspect. Callers that
guard against a second pipeline instance need a probe, not a weapon.
"""

import ctypes
import os
import sys

# Windows access right that grants nothing but the ability to wait on a handle.
_SYNCHRONIZE = 0x00100000


def process_alive(pid: int) -> bool:
    """Return True if a process with *pid* currently exists."""
    if sys.platform == "win32":
        return _process_alive_windows(pid)

    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True  # exists, we are just not allowed to signal it
    return True


def _process_alive_windows(pid: int) -> bool:
    kernel32 = ctypes.windll.kernel32  # type: ignore[attr-defined]
    handle = kernel32.OpenProcess(_SYNCHRONIZE, False, pid)
    if not handle:
        return False
    kernel32.CloseHandle(handle)
    return True
