# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Deterministic teardown for the process-global registry cache.

``RepositoryRegistry.get_instance`` caches one WAL connection per repository root
for the lifetime of the process. Without a release path that cache accumulates
open file descriptors (db + ``-wal`` + ``-shm`` per root) and exhausts a low
``ulimit -n`` — macOS defaults to 256, which is exactly where the suite fell over.

These tests pin the release primitive (``close_all``) behaviourally: they observe
that ``close_all`` frees every cached connection, and that the module stays usable
afterwards.

Each OS gets the probe that actually has teeth on it. POSIX counts open
descriptors, which is exact. Windows cannot: its only cheap counter,
``GetProcessHandleCount``, totals every kernel handle in the process — threads,
events, registry keys — so it neither drops reliably when a file closes nor holds
still between two reads. Windows instead gets a sharper probe that POSIX lacks:
it refuses to delete a file that is still open, so a surviving connection turns
into a ``PermissionError`` on cleanup.
"""

import gc
import os
import shutil
import sys

import pytest

from adapt.persistence.registry import RepositoryRegistry

pytestmark = pytest.mark.unit

_needs_posix_fds = pytest.mark.skipif(
    sys.platform == "win32",
    reason="descriptor counting is POSIX-only; Windows uses the deletion probe instead",
)


def _open_fd_count() -> int:
    """Number of file descriptors this process currently holds open."""
    return len(os.listdir("/dev/fd"))


def _cache_twenty_registries(tmp_path) -> list:
    """Cache one registry per root and return the roots."""
    roots = []
    for i in range(20):
        root = tmp_path / f"repo{i}"
        root.mkdir()
        RepositoryRegistry.get_instance(root)
        roots.append(root)
    return roots


@_needs_posix_fds
def test_open_fd_count_actually_tracks_open_handles(tmp_path):
    """Self-test of the measuring instrument, before anything relies on it.

    The assertions below are deltas from this counter, so a broken counter does
    not fail honestly — it fails somewhere else, as a confusing error inside an
    unrelated assertion.
    """
    baseline = _open_fd_count()
    assert baseline > 0

    handles = [(tmp_path / f"f{i}").open("w", encoding="utf-8") for i in range(10)]
    try:
        assert _open_fd_count() >= baseline + 10
    finally:
        for handle in handles:
            handle.close()

    assert _open_fd_count() <= baseline + 2


@_needs_posix_fds
def test_close_all_returns_the_descriptor_count_to_baseline(tmp_path):
    """Each cached root holds a live WAL connection; close_all releases them all."""
    RepositoryRegistry.close_all()  # start from a clean cache, independent of other tests
    gc.collect()
    before = _open_fd_count()

    _cache_twenty_registries(tmp_path)
    assert _open_fd_count() >= before + 20  # 20 roots each hold a live connection

    RepositoryRegistry.close_all()
    gc.collect()
    assert _open_fd_count() <= before + 3  # every cached connection released


def test_close_all_leaves_every_repository_deletable(tmp_path):
    """After close_all, nothing may still be holding a repository's files.

    This is the probe with teeth on Windows, where deleting an open file raises
    PermissionError — the failure mode that a leaked connection actually causes
    for users, whose repository directories become unremovable.
    """
    RepositoryRegistry.close_all()
    roots = _cache_twenty_registries(tmp_path)

    RepositoryRegistry.close_all()

    for root in roots:
        shutil.rmtree(root)  # raises on Windows if a connection survived
        assert not root.exists()


def test_get_instance_after_close_all_returns_a_working_registry(tmp_path):
    """``close_all`` is a teardown primitive, not a kill switch: a fresh
    ``get_instance`` afterwards opens a new, live registry."""
    RepositoryRegistry.get_instance(tmp_path)
    RepositoryRegistry.close_all()

    registry = RepositoryRegistry.get_instance(tmp_path)
    registry.register_radar("KTST")  # a real write proves the connection is live
    assert "KTST" in list(registry.list_radars()["radar"])
