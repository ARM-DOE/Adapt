# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Adapt Live — operational scan viewer (Tkinter dashboard).

Usage::

    from adapt.consumers.live import main
    main()

Or via CLI::

    adapt dashboard [--repo /path/to/repo]
"""

from adapt.consumers.live.dashboard import AdaptDashboard, main

__all__ = ["main", "AdaptDashboard"]
