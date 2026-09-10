# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""``Adapt`` - Automated Detection And Projection of storm cells using Tracking.

The layer stack and boundary rules live in ARCHITECTURE.md and are enforced
by lint-imports and tests/test_architecture.py. Key subpackages: ``modules``
(science), ``execution`` (graph + nodes), ``runtime`` (orchestration),
``persistence`` (store), ``api`` (StoreClient read facade), ``consumers``
(dashboard, target selection).

Authors: Bhupendra Raut and Sid Gupta
"""

import os as _os

# Quiet third-party import-time chatter before any submodule (and its transitive
# deps) load. Py-ART prints a citation banner on import unless PYART_QUIET is set,
# and the ingest module imports pyart at its own import time — earlier than any
# Adapt module could set this. The package root is the one place guaranteed to run
# first. setdefault preserves a user-provided override.
_os.environ.setdefault("PYART_QUIET", "1")

import importlib.metadata as _importlib_metadata

# Get the version
try:
    __version__ = _importlib_metadata.version("arm-adapt")
except _importlib_metadata.PackageNotFoundError:
    # package is not installed
    __version__ = "0.0.0"
