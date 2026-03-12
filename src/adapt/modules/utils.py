#!/usr/bin/env python3
"""Utility functions for radar cell analysis and geometry computation.

Centralized helper functions for:
- Grid spacing and coordinate conversion
- Time interval computation
- Cell centroid calculation (geometric and weighted)
- Pixel-to-geographic coordinate transformation
- Common array operations (masking, statistics)

These utilities support the analyzer and projector modules, reducing
code duplication and providing consistent definitions of operations
like "centroid" across the pipeline.

@TODO: Add more common utilities from other modules (e.g., spatial interpolation)
@TODO: Consider moving array operations here from cell_analyzer.py for reuse
"""

import numpy as np
import xarray as xr
import logging
from typing import Dict, Tuple, Optional

try:
    from scipy.ndimage import center_of_mass, label as scipy_label
    from skimage.morphology import convex_hull_image, binary_closing
except ImportError:
    scipy_label = None    
    center_of_mass = None
    convex_hull_image = None
    binary_closing = None

__all__ = ['compute_all_cell_centroids']

logger = logging.getLogger(__name__)


# ============================================================================
# GRID AND TIME UTILITIES
# ============================================================================

def compute_time_interval(ds1: xr.Dataset, ds2: xr.Dataset, units: str = 'seconds') -> float:
    """Calculate time difference between two xarray Datasets.
    
    Extracts the 'time' coordinate from each dataset and computes the
    absolute time difference. Useful for validating that consecutive
    radar frames are at expected intervals (e.g., ~5-10 minutes apart).
    
    Parameters
    ----------
    ds1, ds2 : xr.Dataset
        Datasets with 'time' coordinate. Time can be numpy datetime64,
        pandas Timestamp, or similar time-aware type.
    
    units : str, default 'seconds'
        Output time unit: 'seconds', 'minutes', 'hours', 'days'.
        The function converts the time difference to this unit.
    
    Returns
    -------
    float
        Absolute time interval between ds1 and ds2 in specified units.
    
    Raises
    ------
    KeyError
        If 'time' coordinate is missing from either dataset.
    ValueError
        If time difference cannot be converted to requested units.
    
    Notes
    -----
    - Returns absolute value; order of arguments doesn't matter
    - Typical usage: validate ~5-10 min gaps between consecutive frames
    - If gap exceeds threshold (e.g., 30 min), motion model is invalid
    
    Examples
    --------
    >>> interval_sec = compute_time_interval(ds_t1, ds_t0, units='seconds')
    >>> interval_min = interval_sec / 60
    >>> if interval_min > 30:
    ...     logger.warning("Large time gap; skipping projection")
    """


def compute_cell_centroid(label_array: np.ndarray, label_id: int, weighted_by: np.ndarray = None) -> Optional[Tuple[float, float]]:
    """Compute the centroid (y, x) of a single labeled cell.
    
    Calculates the center-of-mass of a cell region, optionally weighted
    by another field (e.g., reflectivity for intensity-weighted centroid).
    
    Parameters
    ----------
    label_array : np.ndarray
        2D array of integer labels where each unique value > 0 represents
        a distinct cell. Label 0 is background.
    
    label_id : int
        The cell ID to compute centroid for (must be > 0).
    
    weighted_by : np.ndarray, optional
        2D weight array same shape as label_array. If provided, centroid
        is weighted by this field (e.g., reflectivity values for mass-weighted
        centroid). If None, returns simple geometric centroid.
    
    Returns
    -------
    tuple of (float, float) or None
        Centroid coordinates (y_center, x_center) in pixel coordinates.
        Returns None if label_id is not found in label_array.
    
    Notes
    -----
    - Coordinates are center-of-mass, not rounded to nearest pixel
    - May return fractional pixel coordinates (e.g., 123.45, 456.78)
    - For empty regions (no pixels with label_id), returns None
    - Weighted centroid can be used to find "brightest" cell location
    
    Examples
    --------
    >>> label_array = ...  # (y, x) integer labels from segmentation
    >>> refl = ...  # (y, x) reflectivity values
    >>> 
    >>> # Geometric centroid
    >>> y_center, x_center = compute_cell_centroid(label_array, label_id=1)
    >>> 
    >>> # Reflectivity-weighted centroid
    >>> y_mass, x_mass = compute_cell_centroid(label_array, label_id=1, weighted_by=refl)
    """
    mask = (label_array == label_id)
    if not np.any(mask):
        return None
    if weighted_by is not None:
        centroid = center_of_mass(weighted_by, labels=mask, index=True)
    else:
        centroid = center_of_mass(mask)
    return centroid

def compute_all_cell_centroids(label_array: np.ndarray, weighted_by: np.ndarray = None) -> Dict[int, Tuple[float, float]]:
    """Compute centroids for all labeled cells in a 2D label array.
    
    Batch computation of centroids for all cells in a single operation.
    Useful for extracting multiple cell properties simultaneously without
    repeated iteration.
    
    Parameters
    ----------
    label_array : np.ndarray
        2D array of integer labels where each unique value > 0 is a cell ID.
        Label 0 is treated as background (ignored).
    
    weighted_by : np.ndarray, optional
        2D weight array same shape as label_array. If provided, all centroids
        are weighted by this field. If None, returns geometric centroids.
    
    Returns
    -------
    dict
        Mapping of cell_id -> (y_center, x_center). 
        - Keys: unique label_ids > 0 (background label 0 is excluded)
        - Values: centroid coordinates as (float, float) tuples
        - Empty dict if no cells found (all labels are 0)
    
    Notes
    -----
    - Returns geometric centroids if weighted_by is None
    - Returns intensity-weighted centroids if weighted_by is reflectivity
    - Fractional coordinates are preserved (not rounded to pixels)
    - Excludes background (label 0) automatically
    - Processing time: ~1-10 ms for typical cell counts (10-100 cells)
    
    Examples
    --------
    >>> label_array = ...  # Segmentation output
    >>> refl = ...  # Reflectivity field
    >>> 
    >>> # All geometric centroids
    >>> centroids = compute_all_cell_centroids(label_array)
    >>> for cell_id, (y, x) in centroids.items():
    ...     print(f"Cell {cell_id}: ({y:.1f}, {x:.1f})")
    >>> 
    >>> # Intensity-weighted centroids
    >>> mass_centroids = compute_all_cell_centroids(label_array, weighted_by=refl)
    """
    unique_labels = np.unique(label_array[label_array > 0])
    centroids = {}
    for label_id in unique_labels:
        centroid = compute_cell_centroid(label_array, int(label_id), weighted_by=weighted_by)
        if centroid is not None:
            centroids[int(label_id)] = centroid
    return centroids