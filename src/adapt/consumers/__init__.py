# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Adapt consumer applications.

Consumers are the GUI and analysis applications that sit above the
RepositoryClient API. They must not import from adapt.persistence,
adapt.runtime, or adapt.execution directly.

Available consumers
-------------------
adapt.consumers.live
    Operational scan viewer (Tkinter dashboard).
adapt.consumers.target_selection
    Target selection engine for adaptive scanning.
"""
