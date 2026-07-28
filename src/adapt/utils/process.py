# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Portable "is this PID still running?" probe.

``os.kill(pid, 0)`` is the POSIX idiom, but on Windows CPython routes every
signal other than ``CTRL_C_EVENT``/``CTRL_BREAK_EVENT`` to ``TerminateProcess``
— so the POSIX idiom *kills* the process it was asked to inspect. Callers that
guard against a second pipeline instance need a probe, not a weapon.

The two implementations are selected at import time rather than per call, so
each platform's code is the only one the type checker and the reader ever see.
"""

import sys

if sys.platform == "win32":
    import ctypes

    _SYNCHRONIZE = 0x00100000  # grants nothing but the right to wait on the handle
    _ERROR_ACCESS_DENIED = 5
    _WAIT_TIMEOUT = 0x00000102  # not signalled — the process is still running

    _kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    _kernel32.OpenProcess.restype = ctypes.c_void_p  # HANDLE: a c_int truncates it
    _kernel32.OpenProcess.argtypes = (ctypes.c_uint32, ctypes.c_int, ctypes.c_uint32)
    _kernel32.WaitForSingleObject.restype = ctypes.c_uint32
    _kernel32.WaitForSingleObject.argtypes = (ctypes.c_void_p, ctypes.c_uint32)
    _kernel32.CloseHandle.argtypes = (ctypes.c_void_p,)

    def process_alive(pid: int) -> bool:
        """Return True if a process with *pid* currently exists.

        A successful ``OpenProcess`` is not enough. A Windows process object
        outlives the process itself for as long as anyone holds a handle to it
        — a parent's ``Popen`` object does exactly that — so an exited child
        stays openable. The handle becomes signalled on exit, which is the
        state that actually answers the question.
        """
        handle = _kernel32.OpenProcess(_SYNCHRONIZE, False, pid)
        if not handle:
            # Denied means it exists and is not ours to inspect — mirrors the
            # POSIX PermissionError branch. Anything else means no such process.
            return ctypes.get_last_error() == _ERROR_ACCESS_DENIED

        try:
            return _kernel32.WaitForSingleObject(handle, 0) == _WAIT_TIMEOUT
        finally:
            _kernel32.CloseHandle(handle)

else:
    import os

    def process_alive(pid: int) -> bool:
        """Return True if a process with *pid* currently exists."""
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            return False
        except PermissionError:
            return True  # exists, we are just not allowed to signal it
        return True
