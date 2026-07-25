# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Pure geometry primitives for geometry-first matching.

Bidirectional overlap fractions, hull dilation (buffering), field-agnostic
centroids, characteristic-length strategies, and the dimensionless pair cost
``cost = m + d/L``. No graph, no I/O, no assumption that the field is reflectivity.
"""

import math

import numpy as np
from scipy.ndimage import binary_dilation

__all__ = [
    "LENGTH_SCALES",
    "bidirectional_overlap",
    "buffer_pixels_from_km",
    "dilate_hull",
    "geometric_mismatch",
    "length_scale",
    "mask_centroid",
    "mass_weighted_centroid",
    "pair_cost",
]

# 8-connected structuring element for isotropic dilation.
_STRUCT = np.ones((3, 3), dtype=bool)


def buffer_pixels_from_km(buffer_km: float, pixel_size_m: float) -> int:
    """Number of dilation iterations for a buffer distance, given metres-per-pixel."""
    if pixel_size_m <= 0:
        raise ValueError("pixel_size_m must be positive")
    return int(math.ceil(buffer_km * 1000.0 / pixel_size_m))


def dilate_hull(hull_mask: np.ndarray, buffer_pixels: int) -> np.ndarray:
    """Dilate a boolean hull mask by ``buffer_pixels`` (0 → unchanged)."""
    if buffer_pixels <= 0:
        return hull_mask
    return binary_dilation(hull_mask, structure=_STRUCT, iterations=buffer_pixels)


def bidirectional_overlap(hull: np.ndarray, cell: np.ndarray) -> tuple[float, float]:
    """Return (Opc, Ocp).

    Opc = intersection / candidate area (how much of the cell the hull explains).
    Ocp = intersection / hull area       (how much of the hull the cell covers).
    """
    intersection = float(np.count_nonzero(hull & cell))
    if intersection == 0.0:
        return 0.0, 0.0
    candidate_area = float(np.count_nonzero(cell))
    hull_area = float(np.count_nonzero(hull))
    opc = intersection / candidate_area if candidate_area > 0 else 0.0
    ocp = intersection / hull_area if hull_area > 0 else 0.0
    return opc, ocp


def mask_centroid(mask: np.ndarray) -> tuple[float, float]:
    """Geometric centroid ``(row, col)`` of a boolean mask (index space)."""
    ys, xs = np.nonzero(mask)
    return float(ys.mean()), float(xs.mean())


def mass_weighted_centroid(field: np.ndarray, mask: np.ndarray) -> tuple[float, float]:
    """Field-weighted centroid ``(row, col)`` in index space.

    Weights are ``field - min(field within mask)`` clamped to ≥ 0, so the result is
    well defined for any field (reflectivity, brightness temperature, vertical wind,
    …). Falls back to the geometric centroid when the total weight is zero.
    """
    ys, xs = np.nonzero(mask)
    values = field[ys, xs].astype(float)
    weights = values - values.min()
    total = float(weights.sum())
    if total <= 0.0:
        return float(ys.mean()), float(xs.mean())
    return float((weights * ys).sum() / total), float((weights * xs).sum() / total)


def _equiv_diameter(
    hull_area_px: float, cell_area_px: float, pixel_area_m2: float, fixed_km: float
) -> float:
    area_m2 = hull_area_px * pixel_area_m2
    return 2.0 * math.sqrt(area_m2 / math.pi) if area_m2 > 0 else 0.0


def _sum_radii(
    hull_area_px: float, cell_area_px: float, pixel_area_m2: float, fixed_km: float
) -> float:
    hull_r = math.sqrt(hull_area_px * pixel_area_m2 / math.pi) if hull_area_px > 0 else 0.0
    cell_r = math.sqrt(cell_area_px * pixel_area_m2 / math.pi) if cell_area_px > 0 else 0.0
    return hull_r + cell_r


def _fixed_km(
    hull_area_px: float, cell_area_px: float, pixel_area_m2: float, fixed_km: float
) -> float:
    return fixed_km * 1000.0


# Registered length-scale strategies (name → callable), all with a uniform signature.
LENGTH_SCALES = {
    "hull_equiv_diameter": _equiv_diameter,
    "sum_radii": _sum_radii,
    "fixed_km": _fixed_km,
}


def length_scale(
    name: str,
    hull_area_px: float,
    cell_area_px: float,
    pixel_area_m2: float,
    fixed_km: float,
) -> float:
    """Characteristic length L (metres) for the selected strategy."""
    try:
        fn = LENGTH_SCALES[name]
    except KeyError:
        raise ValueError(f"Unknown length_scale '{name}'; valid: {sorted(LENGTH_SCALES)}") from None
    return fn(hull_area_px, cell_area_px, pixel_area_m2, fixed_km)


def geometric_mismatch(opc: float, ocp: float) -> float:
    """Overlap mismatch ``m = 1 - sqrt(Opc)*sqrt(Ocp)`` in [0, 1]."""
    return 1.0 - math.sqrt(opc) * math.sqrt(ocp)


def pair_cost(opc: float, ocp: float, displacement_m: float, length_m: float) -> float:
    """Dimensionless geometry-first cost ``m + d/L``.

    Falls back to the overlap term alone when L is degenerate (zero-area hull).
    """
    m = geometric_mismatch(opc, ocp)
    if length_m <= 0.0:
        return m
    return m + displacement_m / length_m
