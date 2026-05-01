# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Visualization and plotting module for radar data."""

# OBSOLETE — RadarPlotter and PlotterThread are exported but never imported externally.
# Only PlotConsumer is used (imported directly in cli.py).
# Consider removing these exports or the classes themselves.
from .plotter import PlotterThread, RadarPlotter

__all__ = ['RadarPlotter', 'PlotterThread']
