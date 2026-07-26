# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Adapt Live — operational scan viewer (Tkinter dashboard).

Usage::

    from adapt.consumers.live import main
    main()

Or via CLI::

    adapt dashboard [--repo /path/to/repo]

``main`` and ``AdaptDashboard`` are resolved lazily (PEP 562): importing this
package must not import the ``dashboard`` module, which selects the interactive
``TkAgg`` backend at import time and cannot load on a headless CI runner. Keeping
the export lazy lets the Tk-free submodules (``_view_mode``, ``_timers``,
``_renderer``, …) stay importable without a display.
"""

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from adapt.consumers.live.dashboard import AdaptDashboard, main

__all__ = ["AdaptDashboard", "main"]


def __getattr__(name: str) -> Any:
    if name in __all__:
        from adapt.consumers.live import dashboard

        return getattr(dashboard, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
