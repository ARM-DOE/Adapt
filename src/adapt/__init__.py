# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""``Adapt`` - Automated Detection And Projection of storm cells using Tracking.

Subpackages:
- radar: Data loading, segmentation, analysis
- pipeline: Orchestrator, processor, tracking
- visualization: Plotting

Authors: Bhupendra Raut and Sid Gupta
"""

import os as _os

# Quiet third-party import-time chatter before any submodule (and its transitive
# deps) load. Py-ART prints a citation banner on import unless PYART_QUIET is set,
# and it is pulled in transitively by nexradaws via the acquisition source — earlier
# than any Adapt module could set this. The package root is the one place guaranteed
# to run first. setdefault preserves a user-provided override.
_os.environ.setdefault("PYART_QUIET", "1")

import importlib.metadata as _importlib_metadata

# Get the version
try:
    __version__ = _importlib_metadata.version("arm-adapt")
except _importlib_metadata.PackageNotFoundError:
    # package is not installed
    __version__ = "0.0.0"
