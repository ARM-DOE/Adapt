# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Pure geometry of tracker v2, in grid-index space.

Size-dependent growth of footprints and cells (measured from the shape
boundary), the two overlap fractions on the grown shapes and their combined
mismatch ``u``, the intensity excess above the cell threshold, the
e²-weighted core centre, and the shape-aware (Mahalanobis) residual. No graph,
no I/O; the field is never assumed to be reflectivity.
"""

import math
from dataclasses import dataclass

import numpy as np
from scipy.ndimage import distance_transform_edt

__all__ = [
    "PairOverlap",
    "cell_intensity",
    "core_centre",
    "footprint_pairs",
    "grow",
    "growth_radius_km",
    "mahalanobis",
    "second_moment",
]

# Variance of a uniform unit pixel: keeps a one-pixel shape's moment invertible.
_PIXEL_VARIANCE = 1.0 / 12.0


def growth_radius_km(area_km2: float, r_max_km: float, l0_km: float) -> float:
    """``r = r_max (1 − √A / ℓ0)₊``: small cells grow by up to r_max, cells of size ℓ0 not."""
    return r_max_km * max(0.0, 1.0 - math.sqrt(area_km2) / l0_km)


def grow(mask: np.ndarray, radius_px: float) -> np.ndarray:
    """Every pixel within ``radius_px`` of the shape (Euclidean, from the boundary)."""
    if radius_px <= 0.0 or not mask.any():
        return mask
    return distance_transform_edt(~mask) <= radius_px


@dataclass(frozen=True)
class PairOverlap:
    """Overlap of a grown footprint H⁺ with one grown current cell M⁺ (same radius)."""

    label: int
    intersection_px: int
    footprint_grown_px: int
    cell_grown_px: int

    @property
    def o_c(self) -> float:
        """Share of the grown cell covered: |H⁺∩M⁺| / |M⁺|."""
        return self.intersection_px / self.cell_grown_px

    @property
    def o_h(self) -> float:
        """Share of the grown footprint covered: |H⁺∩M⁺| / |H⁺|."""
        return self.intersection_px / self.footprint_grown_px

    @property
    def u(self) -> float:
        """Overlap mismatch ``1 − √(O^c O^h)``: 0 when the shapes coincide, 1 when they touch."""
        return 1.0 - math.sqrt(self.o_c * self.o_h)


def _box(mask: np.ndarray, margin: int) -> tuple[slice, slice]:
    rows, cols = np.nonzero(mask)
    return (
        slice(max(rows.min() - margin, 0), rows.max() + margin + 1),
        slice(max(cols.min() - margin, 0), cols.max() + margin + 1),
    )


def footprint_pairs(
    footprint: np.ndarray, labels: np.ndarray, radius_px: float
) -> list[PairOverlap]:
    """Every current cell whose grown shape meets the grown footprint.

    Footprint and cell are grown by the same radius; the fractions are taken
    on the grown shapes. Cells are grown whole, wherever they extend.
    """
    if not footprint.any():
        return []
    margin = math.ceil(radius_px) + 1
    search = _box(footprint, 2 * margin)
    near = grow(footprint[search], 2.0 * radius_px) & (labels[search] > 0)
    pairs: list[PairOverlap] = []
    for label in np.unique(labels[search][near]):
        cell = labels == label
        box = _box(footprint | cell, margin)
        h_grown = grow(footprint[box], radius_px)
        m_grown = grow(cell[box], radius_px)
        intersection = int(np.count_nonzero(h_grown & m_grown))
        if intersection:
            pairs.append(
                PairOverlap(
                    int(label),
                    intersection,
                    int(np.count_nonzero(h_grown)),
                    int(np.count_nonzero(m_grown)),
                )
            )
    return pairs


def _excess(field: np.ndarray, mask: np.ndarray, threshold: float, polarity: int) -> np.ndarray:
    values = np.nan_to_num(field[mask].astype(float), nan=threshold)
    return np.clip(polarity * (values - threshold), 0.0, None)


def cell_intensity(
    field: np.ndarray, mask: np.ndarray, threshold: float, polarity: int
) -> tuple[float, float]:
    """Intensity mass ``M = Σ e`` and mean excess ``ē = M / A`` of one cell.

    ``e = polarity · (f − threshold)₊``: positive above a reflectivity threshold
    (polarity +1) and below a brightness-temperature one (polarity −1), zero at
    the cell edge. Missing values carry no excess.
    """
    e = _excess(field, mask, threshold, polarity)
    return float(e.sum()), float(e.mean())


def core_centre(
    field: np.ndarray, mask: np.ndarray, threshold: float, polarity: int
) -> tuple[float, float]:
    """``(row, col)`` of the e²-weighted centre.

    The square pulls the centre towards the intense cores without jumping to
    one pixel. A cell with no excess anywhere has no core, so its centre is
    its geometric centre.
    """
    rows, cols = np.nonzero(mask)
    weight = _excess(field, mask, threshold, polarity) ** 2
    total = float(weight.sum())
    if total == 0.0:
        return float(rows.mean()), float(cols.mean())
    return float((weight * rows).sum() / total), float((weight * cols).sum() / total)


def second_moment(mask: np.ndarray) -> np.ndarray:
    """Second-moment (covariance) matrix of a shape's pixels, (row, col) order, px²."""
    points = np.stack(np.nonzero(mask)).astype(float)
    centred = points - points.mean(axis=1, keepdims=True)
    return centred @ centred.T / points.shape[1] + _PIXEL_VARIANCE * np.eye(2)


def mahalanobis(delta: np.ndarray, sigma: np.ndarray) -> float:
    """``√(δᵀ Σ⁻¹ δ)``: a displacement in units of the shape's extent in its direction."""
    return float(math.sqrt(delta @ np.linalg.solve(sigma, delta)))
