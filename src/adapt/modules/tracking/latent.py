# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Latent tracks: cells that ended or merged away, carried by the flow.

A latent track keeps the last observed state of its cell and a footprint that
the flow moves on each scan, so a cell that vanishes for a scan — absorbed by
a neighbour, or briefly below the threshold — can resume its identity.
"""

from dataclasses import dataclass

import numpy as np

__all__ = ["ORIGIN_MERGED", "ORIGIN_TERMINATED", "LatentTrack", "carry_footprint"]

ORIGIN_MERGED = "MERGED"
ORIGIN_TERMINATED = "TERMINATED"


@dataclass(frozen=True, eq=False)
class LatentTrack:
    """A track without an observation since ``scan_id``, ``age`` scans ago.

    Position fields are metres; ``footprint`` and ``predicted`` are at the scan
    being tracked. ``merged_into_uid`` names the survivor for a merged-away track.
    """

    uid: str
    label: int
    scan_id: str
    scan_time: object
    time_s: float
    centre: tuple[float, float]
    step: tuple[float, float] | None
    speed: float | None
    score: float
    footprint: np.ndarray
    predicted: tuple[float, float]
    age: int
    origin: str
    merged_into_uid: str | None


def carry_footprint(
    footprint: np.ndarray, flow_x: np.ndarray, flow_y: np.ndarray
) -> tuple[np.ndarray, tuple[int, int]]:
    """Shift a footprint by the mean flow inside it, rounded to whole pixels.

    ``flow_x``/``flow_y`` are the displacement (pixels per scan) from the
    footprint's scan to the next. Returns the carried mask (clipped at the
    domain edge) and the ``(row, col)`` shift applied.
    """
    if not footprint.any():
        return footprint, (0, 0)
    d_row = int(round(float(flow_y[footprint].mean())))
    d_col = int(round(float(flow_x[footprint].mean())))
    rows, cols = np.nonzero(footprint)
    rows, cols = rows + d_row, cols + d_col
    inside = (rows >= 0) & (rows < footprint.shape[0]) & (cols >= 0) & (cols < footprint.shape[1])
    carried = np.zeros_like(footprint)
    carried[rows[inside], cols[inside]] = True
    return carried, (d_row, d_col)
