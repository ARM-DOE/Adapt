# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Attribute lightning flashes to storm cells by initiation point.

One flash, one cell: each flash's initiation point is located on the projected
metres grid. Inside a cell mask → that cell. Just outside → the nearest cell
within ``search_radius_m`` (positional uncertainty). Otherwise the sentinel
``UNATTRIBUTED`` — a flash is never silently dropped.

Vectorized: the nearest-cell distance for every background pixel is computed once
per scan with an exact Euclidean distance transform (metres via anisotropic
sampling); flashes are then indexed into it. No per-flash Python loop, no polygon
geometry.
"""

import numpy as np
from scipy import ndimage

UNATTRIBUTED = "UNATTRIBUTED"


def attribute_flashes(
    flash_x: np.ndarray,
    flash_y: np.ndarray,
    cell_labels: np.ndarray,
    x_coords: np.ndarray,
    y_coords: np.ndarray,
    cell_uid_lut: np.ndarray,
    search_radius_m: float = 500.0,
    unattributed: str = UNATTRIBUTED,
) -> tuple[np.ndarray, np.ndarray]:
    """Return ``(cell_uids, attribution_distance_m)`` for each flash.

    Parameters
    ----------
    flash_x, flash_y : np.ndarray
        Flash initiation coordinates in projected metres (same CRS as the grid).
    cell_labels : np.ndarray
        2D ``(ny, nx)`` segmentation labels (0 = background, 1..n = cells).
    x_coords, y_coords : np.ndarray
        1D grid coordinates in metres (uniform spacing).
    cell_uid_lut : np.ndarray
        1D string array; index ``L`` holds the global cell_uid of label ``L``.
    search_radius_m : float
        Maximum distance for nearest-cell attribution of a just-outside flash.

    Returns
    -------
    (np.ndarray, np.ndarray)
        ``cell_uids`` (str, ``unattributed`` where no cell within radius/in
        bounds) and ``attribution_distance_m`` (0.0 inside a cell, the nearest
        distance when attributed nearby, ``nan`` when unattributed).
    """
    n = flash_x.shape[0]
    uids = np.full(n, unattributed, dtype=object)
    dist = np.full(n, np.nan, dtype=float)

    ny, nx = cell_labels.shape
    dx = abs(float(x_coords[1] - x_coords[0]))
    dy = abs(float(y_coords[1] - y_coords[0]))

    ix = np.round((flash_x - x_coords[0]) / dx).astype(int)
    iy = np.round((flash_y - y_coords[0]) / dy).astype(int)
    in_bounds = (ix >= 0) & (ix < nx) & (iy >= 0) & (iy < ny)

    if cell_labels.max() == 0:
        return uids.astype(np.str_), dist

    # Exact nearest-cell distance (metres) and the nearest cell's label for every
    # background pixel; cell pixels have distance 0 and their own label.
    background = cell_labels == 0
    dist_grid, (iy_near, ix_near) = ndimage.distance_transform_edt(
        background, sampling=(dy, dx), return_indices=True
    )
    nearest_label = cell_labels[iy_near, ix_near]

    fi = np.where(in_bounds)[0]
    pix_iy, pix_ix = iy[fi], ix[fi]
    pix_dist = dist_grid[pix_iy, pix_ix]
    pix_label = nearest_label[pix_iy, pix_ix]

    attributed = pix_dist <= search_radius_m
    sel = fi[attributed]
    labels_sel = pix_label[attributed]
    uids[sel] = cell_uid_lut[labels_sel]
    dist[sel] = pix_dist[attributed]

    return uids.astype(np.str_), dist
