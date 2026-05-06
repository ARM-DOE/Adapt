# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""``Adapt`` - Automated Detection And Projection of storm cells using Tracking.

Subpackages:
- radar: Data loading, segmentation, analysis
- pipeline: Orchestrator, processor, tracking
- visualization: Plotting

Authors: Bhupendra Raut and Sid Gupta
"""
import importlib.metadata as _importlib_metadata

# Get the version
try:
    __version__ = _importlib_metadata.version("arm-adapt")
except _importlib_metadata.PackageNotFoundError:
    # package is not installed
    __version__ = "0.0.0"
