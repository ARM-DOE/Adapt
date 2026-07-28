# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Cross-platform process primitives: liveness, detached launch, termination.

Every OS difference in how Adapt starts, inspects, and stops a child process is
resolved here, so callers never branch on the platform themselves. Each one is a
trap on Windows:

* ``os.kill(pid, 0)`` is the POSIX liveness idiom, but CPython routes every
  signal other than ``CTRL_C_EVENT``/``CTRL_BREAK_EVENT`` to ``TerminateProcess``
  — the POSIX idiom *kills* the process it was asked to inspect.
* ``start_new_session`` is silently ignored on Windows, so there is no process
  group to signal unless one is requested explicitly.
* ``os.killpg``/``os.getpgid`` do not exist on Windows at all, and raise
  ``AttributeError``, which an ``except OSError`` guard does not catch.
"""

import os
import subprocess
import sys

__all__ = ["process_alive", "detached_process_kwargs", "terminate_process_tree"]

if sys.platform == "win32":
    import ctypes

    _SYNCHRONIZE = 0x00100000  # grants nothing but the right to wait on the handle
    _ERROR_INVALID_PARAMETER = 87  # OpenProcess's "no such process"
    _ERROR_ACCESS_DENIED = 5
    _WAIT_TIMEOUT = 0x00000102  # not signalled — the process is still running

    _kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    # Explicit signatures are mandatory, not tidiness: HANDLE is pointer-sized and
    # ctypes defaults restype to c_int, which truncates a 64-bit handle.
    _kernel32.OpenProcess.restype = ctypes.c_void_p
    _kernel32.OpenProcess.argtypes = (ctypes.c_uint32, ctypes.c_int, ctypes.c_uint32)
    _kernel32.WaitForSingleObject.restype = ctypes.c_uint32
    _kernel32.WaitForSingleObject.argtypes = (ctypes.c_void_p, ctypes.c_uint32)
    _kernel32.CloseHandle.argtypes = (ctypes.c_void_p,)

    def _process_alive_windows(pid: int) -> bool:
        """Open the process object, then ask whether it is signalled.

        A successful ``OpenProcess`` is not enough. A Windows process object outlives
        the process itself for as long as anyone holds a handle to it — a parent's
        ``Popen`` object does exactly that — so an exited child stays openable. The
        handle becomes signalled on exit, which is the state that answers the question.
        """
        handle = _kernel32.OpenProcess(_SYNCHRONIZE, False, pid)
        if not handle:
            err = ctypes.get_last_error()
            if err == _ERROR_ACCESS_DENIED:
                return True  # exists; mirrors the POSIX PermissionError branch
            if err == _ERROR_INVALID_PARAMETER:
                return False  # no such process
            # Anything else is not a answer about liveness — say so rather than
            # reporting "dead" and letting a second pipeline start on top of a live one.
            raise OSError(
                0, f"OpenProcess failed for pid {pid}: {ctypes.WinError(err).strerror}", None, err
            )

        try:
            return _kernel32.WaitForSingleObject(handle, 0) == _WAIT_TIMEOUT
        finally:
            _kernel32.CloseHandle(handle)


def process_alive(pid: int) -> bool:
    """Return True if a process with *pid* currently exists.

    Never signals the process: this is a probe, not a weapon.
    """
    if sys.platform == "win32":
        return _process_alive_windows(pid)

    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True  # exists, we are just not allowed to signal it
    return True


def detached_process_kwargs() -> dict:
    """``Popen`` kwargs that put a child in its own signal group.

    Lets a parent stop the child without stopping itself. POSIX gets a new
    session; Windows has no sessions, so it gets a new process group instead.
    """
    if sys.platform == "win32":
        return {"creationflags": subprocess.CREATE_NEW_PROCESS_GROUP}
    return {"start_new_session": True}


def terminate_process_tree(proc: subprocess.Popen, *, force: bool = False) -> None:
    """Stop *proc* and its children; ``force`` escalates to an unblockable kill.

    Pairs with :func:`detached_process_kwargs` — the child must have been launched
    with those kwargs for the whole tree to be reached.
    """
    if sys.platform == "win32":
        proc.kill() if force else proc.terminate()
        return
    try:
        os.killpg(os.getpgid(proc.pid), 9 if force else 15)
    except OSError:
        # Already reaped, or never got its own group.
        proc.kill() if force else proc.terminate()
