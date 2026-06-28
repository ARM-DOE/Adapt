# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Quiet redundant compiled-library diagnostics through their supported controls.

These libraries write directly to the process stderr/stdout, bypassing Python
logging entirely, so they cannot be routed by the logging configuration. We use
each library's own supported mechanism — never stdout/stderr redirection — and
apply it on the thread that does the I/O (HDF5 error stacks are thread-local).
"""

from __future__ import annotations

import h5py

__all__ = ["silence_hdf5_errors"]


def silence_hdf5_errors() -> None:
    """Turn off libhdf5's automatic stderr error dump on the calling thread.

    libhdf5 prints multi-line ``HDF5-DIAG`` blocks to stderr when an operation
    fails — e.g. while probing a file's format — even though the real failure is
    already raised to Python as an exception. The dump is never actionable, only
    clutter. ``h5py._errors.silence_errors`` is h5py's wrapper over the official
    HDF5 ``H5Eset_auto`` control; because HDF5 error stacks are thread-local, this
    must run on each worker thread that touches HDF5, not once at import.
    """
    h5py._errors.silence_errors()
