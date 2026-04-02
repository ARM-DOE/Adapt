"""Visualization and plotting module for radar data."""

# OBSOLETE — RadarPlotter and PlotterThread are exported but never imported externally.
# Only PlotConsumer is used (imported directly in cli.py).
# Consider removing these exports or the classes themselves.
from .plotter import RadarPlotter, PlotterThread

__all__ = ['RadarPlotter', 'PlotterThread']
