# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Canonical column and variable naming — single authoritative source.

``stat_column`` is the ONLY place the per-cell statistics column convention
is spelled. The ``radar_`` prefix is a namespace for cell statistics, not a
physics claim: a pressure-tracked run mints ``radar_pressure_max``. Renaming
the prefix would orphan every frozen store for zero scientific gain.

``CELL_LABELS_VAR`` is the fixed internal name of the segmentation label
grid. It is pipeline-internal (produced by detection), never source data,
and therefore not configurable.
"""

CELL_LABELS_VAR = "cell_labels"


def stat_column(field: str, stat: str) -> str:
    """Name of the per-cell statistic column for a canonical field."""
    return f"radar_{field}_{stat}"
