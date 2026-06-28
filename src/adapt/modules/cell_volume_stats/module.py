# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""3D cell statistics — pure scientific functions + CellVolumeStatsAlgorithm.

No I/O, no Adapt engine imports. Operates on numpy arrays extracted from the 3D
gridded volume. A cell's 3D volume is its 2D detection footprint extruded through
all altitude levels; every per-pixel column ("profile") is analysed for echo
structure, and aggregates are reduced over the footprint.
"""

import warnings

import numpy as np
from scipy import ndimage

from .config import CellVolumeStatsConfig


# ── level thickness ───────────────────────────────────────────────────────────
def _level_dz(z_coords: np.ndarray) -> np.ndarray:
    """Per-level vertical thickness (m). np.diff edge-padded to full length."""
    z = np.asarray(z_coords, dtype=float)
    if z.size < 2:
        return np.ones_like(z)
    dz = np.diff(z)
    return np.append(dz, dz[-1])


def _nan(*keys):
    return {k: float("nan") for k in keys}


def _safe(func, arr):
    """nan-aware reducer that returns NaN for all-NaN input without warnings."""
    a = np.asarray(arr, dtype=float)
    if a.size == 0 or np.all(np.isnan(a)):
        return float("nan")
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", RuntimeWarning)
        return float(func(a))


# ── per-profile (1D column) functions ─────────────────────────────────────────
def merge_vertical_gaps(mask_1d, z_coords, gap_tolerance_m: float) -> np.ndarray:
    """Bridge internal False gaps whose vertical span ≤ gap_tolerance_m.

    Gap span for a False run [i..j] bounded by True is ``z[j+1] - z[i]``.
    """
    mask = np.asarray(mask_1d, dtype=bool).copy()
    z = np.asarray(z_coords, dtype=float)
    n = mask.size
    i = 0
    while i < n:
        if mask[i]:
            i += 1
            continue
        j = i
        while j + 1 < n and not mask[j + 1]:
            j += 1
        # internal gap only: bounded by True on both sides
        if i - 1 >= 0 and j + 1 < n and mask[i - 1] and mask[j + 1]:
            span = z[j + 1] - z[i]
            if span <= gap_tolerance_m:
                mask[i : j + 1] = True
        i = j + 1
    return mask


def find_connected_regions(mask_1d) -> np.ndarray:
    """Label connected True runs in a 1D mask (0 = background)."""
    labels, _ = ndimage.label(np.asarray(mask_1d, dtype=bool))
    return labels


def compute_region_score(dbz_1d, region_mask, z_coords, threshold: float) -> float:
    """Σ max(DBZ - threshold, 0) * Δz over the region's levels."""
    dbz = np.asarray(dbz_1d, dtype=float)
    region = np.asarray(region_mask, dtype=bool)
    dz = _level_dz(z_coords)
    excess = np.where(region, np.nan_to_num(dbz - threshold, nan=0.0), 0.0)
    excess = np.clip(excess, 0.0, None)
    return float(np.sum(excess * dz))


def select_dominant_region(dbz_1d, labels_1d, z_coords, threshold: float) -> int:
    """Return the label of the highest-scoring region, or 0 if none."""
    labels = np.asarray(labels_1d)
    best_label, best_score = 0, -np.inf
    for lab in np.unique(labels):
        if lab == 0:
            continue
        score = compute_region_score(dbz_1d, labels == lab, z_coords, threshold)
        if score > best_score:
            best_label, best_score = int(lab), score
    return best_label


def analyze_profile(dbz_1d, z_coords, threshold: float, gap_tolerance_m: float) -> dict:
    """Echo structure of one column. NaN heights when no echo at threshold."""
    dbz = np.asarray(dbz_1d, dtype=float)
    z = np.asarray(z_coords, dtype=float)
    mask = dbz >= threshold  # NaN >= x is False
    mask = merge_vertical_gaps(mask, z, gap_tolerance_m)
    labels = find_connected_regions(mask)
    nlayers = int(labels.max())

    if nlayers == 0:
        out = _nan(
            "top_height", "bottom_height", "depth", "score", "max_dbz", "mean_dbz", "std_dbz"
        )
        out.update(nlayers=0, multilayer=False)
        return out

    dominant = select_dominant_region(dbz, labels, z, threshold)
    dom_levels = z[labels == dominant]
    echo_dbz = dbz[mask]
    return {
        "top_height": float(dom_levels.max()),
        "bottom_height": float(dom_levels.min()),
        "depth": float(dom_levels.max() - dom_levels.min()),
        "score": compute_region_score(dbz, labels == dominant, z, threshold),
        "max_dbz": _safe(np.nanmax, echo_dbz),
        "mean_dbz": _safe(np.nanmean, echo_dbz),
        "std_dbz": _safe(np.nanstd, echo_dbz),
        "nlayers": nlayers,
        "multilayer": nlayers > 1,
    }


# ── footprint aggregates (nz, npixels) ────────────────────────────────────────
def compute_threshold_features(
    dbz_volume, z_coords, threshold: float, gap_tolerance_m: float
) -> dict:
    """Aggregate per-profile echo features over the footprint, prefixed cell_ethXX_."""
    vol = np.asarray(dbz_volume, dtype=float)
    xx = int(threshold)
    p = f"cell_eth{xx}_"
    profiles = [
        analyze_profile(vol[:, c], z_coords, threshold, gap_tolerance_m)
        for c in range(vol.shape[1])
    ]
    tops = np.array([pr["top_height"] for pr in profiles], dtype=float)
    bots = np.array([pr["bottom_height"] for pr in profiles], dtype=float)
    deps = np.array([pr["depth"] for pr in profiles], dtype=float)
    scrs = np.array([pr["score"] for pr in profiles], dtype=float)
    nlay = np.array([pr["nlayers"] for pr in profiles], dtype=float)
    multi = np.array([pr["multilayer"] for pr in profiles], dtype=float)
    return {
        f"{p}top_max": _safe(np.nanmax, tops),
        f"{p}top_mean": _safe(np.nanmean, tops),
        f"{p}top_min": _safe(np.nanmin, tops),
        f"{p}bottom_max": _safe(np.nanmax, bots),
        f"{p}bottom_mean": _safe(np.nanmean, bots),
        f"{p}bottom_min": _safe(np.nanmin, bots),
        f"{p}depth_max": _safe(np.nanmax, deps),
        f"{p}depth_mean": _safe(np.nanmean, deps),
        f"{p}depth_min": _safe(np.nanmin, deps),
        f"{p}score_max": _safe(np.nanmax, scrs),
        f"{p}score_mean": _safe(np.nanmean, scrs),
        f"{p}score_min": _safe(np.nanmin, scrs),
        f"{p}multilayer_fraction": float(np.mean(multi)) if multi.size else float("nan"),
        f"{p}nlayers_mean": float(np.mean(nlay)) if nlay.size else float("nan"),
        f"{p}nlayers_max": float(np.max(nlay)) if nlay.size else float("nan"),
    }


def compute_volume_statistics(
    dbz_volume,
    z_coords,
    npixels: int,
    dx_m: float,
    dy_m: float,
    thresholds=(20.0, 30.0, 40.0, 50.0, 60.0),
) -> dict:
    """Geometry + volume-by-threshold for the extruded footprint."""
    vol = np.asarray(dbz_volume, dtype=float)
    pixel_area_km2 = dx_m * dy_m / 1e6
    dz_m = _level_dz(z_coords)  # (nz,)
    voxel_km3 = (pixel_area_km2 * dz_m / 1e3)[:, None]  # (nz, 1) km^3 per voxel

    defined = ~np.isnan(vol)
    out = {
        "cell_area_km2": float(npixels * pixel_area_km2),
        "cell_volume_km3": float(np.sum(np.where(defined, voxel_km3, 0.0))),
    }
    for thr in thresholds:
        above = defined & (vol >= thr)
        out[f"vol_{int(thr)}dbz_km3"] = float(np.sum(np.where(above, voxel_km3, 0.0)))

    z = np.asarray(z_coords, dtype=float)
    any_per_level = defined.any(axis=1)
    if any_per_level.any():
        zlev = z[any_per_level]
        out["cell_base_m"] = float(zlev.min())
        out["cell_top_m"] = float(zlev.max())
        out["cell_depth_m"] = float(zlev.max() - zlev.min())
    else:
        out.update(_nan("cell_base_m", "cell_top_m", "cell_depth_m"))
    return out


def compute_reflectivity_statistics(dbz_volume) -> dict:
    """dBZ stats + linear-Z stats over the volume (NaN-ignoring)."""
    vol = np.asarray(dbz_volume, dtype=float)
    z_lin = np.where(np.isnan(vol), np.nan, 10.0 ** (vol / 10.0))
    return {
        "dbz_max": _safe(np.nanmax, vol),
        "dbz_mean": _safe(np.nanmean, vol),
        "dbz_std": _safe(np.nanstd, vol),
        "dbz_min": _safe(np.nanmin, vol),
        "z_mean": _safe(np.nanmean, z_lin),
        "z_max": _safe(np.nanmax, z_lin),
        "z_std": _safe(np.nanstd, z_lin),
    }


def compute_polarimetric_statistics(volume, var_name: str) -> dict:
    """Generic {var}_{max,mean,std,min} over a polarimetric volume."""
    v = np.asarray(volume, dtype=float)
    return {
        f"{var_name}_max": _safe(np.nanmax, v),
        f"{var_name}_mean": _safe(np.nanmean, v),
        f"{var_name}_std": _safe(np.nanstd, v),
        f"{var_name}_min": _safe(np.nanmin, v),
    }


def compute_height_statistics(dbz_volume, z_coords) -> dict:
    """Height of max reflectivity and linear-Z-weighted center of mass."""
    vol = np.asarray(dbz_volume, dtype=float)
    z = np.asarray(z_coords, dtype=float)
    if vol.size == 0 or np.all(np.isnan(vol)):
        return _nan("dbz_max_height_m", "dbz_com_height_m")
    per_level = np.where(np.isnan(vol), -np.inf, vol).max(axis=1)
    max_h = float(z[int(np.argmax(per_level))])
    weights = np.where(np.isnan(vol), 0.0, 10.0 ** (vol / 10.0)).sum(axis=1)
    com = float(np.sum(z * weights) / np.sum(weights)) if weights.sum() > 0 else float("nan")
    return {"dbz_max_height_m": max_h, "dbz_com_height_m": com}


def compute_storm_structure(dbz_volume, z_coords, threshold: float, gap_tolerance_m: float) -> dict:
    """Layering summary across the footprint at the structure threshold."""
    vol = np.asarray(dbz_volume, dtype=float)
    profiles = [
        analyze_profile(vol[:, c], z_coords, threshold, gap_tolerance_m)
        for c in range(vol.shape[1])
    ]
    nlay = np.array([pr["nlayers"] for pr in profiles], dtype=float)
    multi = np.array([pr["multilayer"] for pr in profiles], dtype=float)
    if nlay.size == 0:
        return {
            "multilayer_fraction": float("nan"),
            "mean_nlayers": float("nan"),
            "max_nlayers": float("nan"),
        }
    return {
        "multilayer_fraction": float(np.mean(multi)),
        "mean_nlayers": float(np.mean(nlay)),
        "max_nlayers": float(np.max(nlay)),
    }


class CellVolumeStatsAlgorithm:
    """Compute one output row of 3D statistics for a single tracked cell."""

    def __init__(self, config: CellVolumeStatsConfig) -> None:
        self._c = config

    def _zyx(self, grid_ds, var):
        """Return a variable as a canonical (z, y, x) numpy array, by dim NAME.

        Robust to the real gridded NetCDF layout (time, z, y, x) and any transposed
        order: extra dims (e.g. time) are reduced to their first index and the
        remaining axes are transposed to (z, y, x) — never positional indexing.
        """
        c = self._c
        da = grid_ds[var]
        extra = [d for d in da.dims if d not in (c.z_coord, c.y_coord, c.x_coord)]
        if extra:
            da = da.isel({d: 0 for d in extra})
        return da.transpose(c.z_coord, c.y_coord, c.x_coord).values

    def compute_cell(
        self, grid_ds, cell_labels_2d, cell_label, run_id, scan_time, cell_uid
    ) -> dict:
        c = self._c
        mask_2d = np.asarray(cell_labels_2d) == cell_label
        npixels = int(mask_2d.sum())
        z = grid_ds[c.z_coord].values
        dbz_vol = self._zyx(grid_ds, c.reflectivity_var)[:, mask_2d]  # (nz, npixels)
        dx_m = float(np.diff(grid_ds.coords[c.x_coord].values).mean())
        dy_m = float(np.diff(grid_ds.coords[c.y_coord].values).mean())

        row = {
            "run_id": run_id,
            "scan_time": scan_time,
            "cell_uid": cell_uid,
            "cell_label": int(cell_label),
        }
        row.update(compute_volume_statistics(dbz_vol, z, npixels, dx_m, dy_m))
        row.update(compute_reflectivity_statistics(dbz_vol))
        row.update(compute_height_statistics(dbz_vol, z))
        for thr in c.thresholds:
            row.update(compute_threshold_features(dbz_vol, z, thr, c.gap_tolerance_m))
        for var, attr in ((c.zdr_var, "zdr"), (c.kdp_var, "kdp"), (c.rhohv_var, "rhohv")):
            if var in grid_ds:
                row.update(
                    compute_polarimetric_statistics(self._zyx(grid_ds, var)[:, mask_2d], attr)
                )
        row.update(compute_storm_structure(dbz_vol, z, c.structure_threshold, c.gap_tolerance_m))
        return row
