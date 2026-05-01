# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Adapt Radar Dashboard GUI.

A simple, standalone GUI for visualizing radar pipeline outputs.

Usage::

    from adapt.gui import main
    main()

Or via CLI::

    adapt dashboard [--repo /path/to/repo]

Features
--------
- Browse and visualize segmentation NetCDF files
- Cell statistics table with filtering
- Pipeline start/stop control
- Loop animation through recent scans
- Hover cell info display
"""

from adapt.gui.dashboard import AdaptDashboard, main

__all__ = ['main', 'AdaptDashboard']
