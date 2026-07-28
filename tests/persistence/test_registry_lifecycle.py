# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Deterministic teardown for the process-global registry cache.

``RepositoryRegistry.get_instance`` caches one WAL connection per repository root
for the lifetime of the process. Without a release path that cache accumulates
open file descriptors (db + ``-wal`` + ``-shm`` per root) and exhausts a low
``ulimit -n`` — macOS defaults to 256, which is exactly where the suite fell over.

These tests pin the release primitive (``close_all``) behaviourally: they observe
that cached registries hold descriptors open, that ``close_all`` frees every one,
and that the module stays usable afterwards.
"""

import gc
import os
import sys

import pytest

from adapt.persistence.registry import RepositoryRegistry

pytestmark = pytest.mark.unit

# The leak this guards against is platform-independent, so the measurement has to
# be too. POSIX exposes descriptors as directory entries; Windows has no /dev/fd,
# so ask the kernel for the process handle count. Both are only ever compared as
# deltas around a known operation.
if sys.platform == "win32":
    import ctypes

    _kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    # Explicit signatures are mandatory, not tidiness: HANDLE is pointer-sized and
    # ctypes defaults restype to c_int, which truncates GetCurrentProcess's
    # (HANDLE)-1 pseudo-handle to 32 bits and yields ERROR_INVALID_HANDLE.
    _kernel32.GetCurrentProcess.restype = ctypes.c_void_p
    _kernel32.GetCurrentProcess.argtypes = ()
    _kernel32.GetProcessHandleCount.restype = ctypes.c_int
    _kernel32.GetProcessHandleCount.argtypes = (
        ctypes.c_void_p,
        ctypes.POINTER(ctypes.c_uint32),
    )

    def _open_fd_count() -> int:
        """Number of OS handles this process currently holds open."""
        count = ctypes.c_uint32()
        if not _kernel32.GetProcessHandleCount(_kernel32.GetCurrentProcess(), ctypes.byref(count)):
            raise OSError(ctypes.get_last_error(), "GetProcessHandleCount failed")
        return count.value

else:

    def _open_fd_count() -> int:
        """Number of OS handles this process currently holds open."""
        return len(os.listdir("/dev/fd"))


def test_close_all_releases_every_cached_registry_connection(tmp_path):
    """Each distinct repository root keeps a WAL connection open while cached;
    ``close_all`` must release them all so descriptors return to the baseline."""
    RepositoryRegistry.close_all()  # start from a clean cache, independent of other tests
    gc.collect()
    before = _open_fd_count()

    for i in range(20):
        root = tmp_path / f"repo{i}"
        root.mkdir()
        RepositoryRegistry.get_instance(root)
    assert _open_fd_count() >= before + 20  # 20 roots each hold a live connection

    RepositoryRegistry.close_all()
    gc.collect()
    assert _open_fd_count() <= before + 3  # every cached connection released


def test_get_instance_after_close_all_returns_a_working_registry(tmp_path):
    """``close_all`` is a teardown primitive, not a kill switch: a fresh
    ``get_instance`` afterwards opens a new, live registry."""
    RepositoryRegistry.get_instance(tmp_path)
    RepositoryRegistry.close_all()

    registry = RepositoryRegistry.get_instance(tmp_path)
    registry.register_radar("KTST")  # a real write proves the connection is live
    assert "KTST" in list(registry.list_radars()["radar"])
