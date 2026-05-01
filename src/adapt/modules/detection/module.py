# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Segment convective cells from gridded radar reflectivity.

This module detects convective cell boundaries using thresholding and
morphological operations. Input is a 2D reflectivity field (at fixed altitude);
output is a labeled image where each cell is assigned a unique integer ID.

Cell detection enables downstream analysis: motion tracking, intensity analysis,
cell-by-cell statistics extraction. The segmentation is configurable (threshold,
minimum/maximum cell size) for different storm morphologies.

Cell size ordering: cells are numbered 1, 2, 3, ... in decreasing order of
size (area in grid points). This ensures reproducible analysis and allows
size-based filtering in downstream steps.

Key capabilities:
- Morphological filtering (closing to fill small holes)
- Size-based filtering (min/max grid points per cell)
- Automatic relabeling by decreasing size for reproducibility
- Metadata preservation (threshold, z-level, configuration)
"""

import logging

import numpy as np
import xarray as xr
from scipy.ndimage import label
from skimage.morphology import h_maxima
from skimage.segmentation import watershed

__all__ = ['RadarCellSegmenter']

logger = logging.getLogger(__name__)


def _label_maxtree(binary: np.ndarray, field: np.ndarray, h: float = 5.0) -> np.ndarray:
    """Replace connected-component labeling: h-maxima seeding + watershed.

    Identifies individual cells within a binary convection mask by seeding
    each local intensity maximum (that rises at least `h` dBZ above its
    surroundings) and growing watershed regions from those seeds.

    Parameters
    ----------
    binary : np.ndarray (bool)
        Closed binary convection mask (output of morphological closing).
    field : np.ndarray (float)
        Reflectivity values aligned with `binary`.
    h : float
        Minimum intensity rise above surroundings for a peak to seed a cell.

    Returns
    -------
    np.ndarray (int32)
        0 = background, 1..N = individual cell IDs.
    """
    fp = np.where(binary, field, 0.0)
    peaks = h_maxima(fp, h=h)
    seeds, n_seeds = label(peaks)          # label from scipy.ndimage
    if n_seeds == 0:
        return np.zeros(binary.shape, dtype=np.int32)
    ws = watershed(-fp, seeds, mask=binary)
    return np.where(binary, ws, 0).astype(np.int32)


class RadarCellSegmenter:
    """Threshold-based cell detection and labeling for 2D radar reflectivity.
    
    This class applies a series of image processing steps to identify and label
    convective cells:
    
    1. **Binary thresholding**: Mark reflectivity > threshold as cell candidates
    2. **Morphological closing**: Fill small holes within cells (tunable)
    3. **Connected component labeling**: Assign unique IDs (1, 2, 3, ...)
    4. **Size filtering**: Remove cells smaller than min_gridpoints or larger
       than max_gridpoints (optional)
    5. **Relabeling by size**: Largest cell gets label 1, second-largest gets 2, etc.
    
    The output is a labeled image (xarray.DataArray) with integer cell IDs.
    Cell ID = 0 means no cell; cell ID > 0 means part of that cell.
    
    Configuration
    ==============
    Expects config dict with:
    
    - `method` : str, optional (default: "threshold")
        Segmentation algorithm. Currently only "threshold" is supported.
    
    - `threshold` : float, default 30
        Reflectivity threshold in dBZ. Cells have reflectivity > threshold.
        Typical: 30 dBZ for convection, 20 dBZ for weaker features.
    
    - `closing_kernel` : tuple of int, default (1, 1)
        Size of morphological closing footprint. (1, 1) means no closing.
        (3, 3) or (5, 5) fill small holes.
    
    - `filter_by_size` : bool, default True
        Whether to apply cell size filtering.
    
    - `min_cellsize_gridpoint` : int, default 5
        Minimum cell size in grid points. Smaller cells are removed.
        Typical: 5-20 points (1-4 km at 200 m spacing).
    
    - `max_cellsize_gridpoint` : int or None, default None
        Maximum cell size in grid points. Larger cells are removed.
        If None, no upper limit.
    
    - `global` : dict, optional
        Sub-dict with variable/coordinate naming:
        
        - `var_names` : dict, optional
            - `reflectivity` : str, name of reflectivity variable (default: "reflectivity")
            - `cell_labels` : str, name for output labels (default: "cell_labels")
    
    Notes
    -----
    - Input dataset must be 2D (already sliced at a fixed altitude by processor)
    - Not thread-safe; create separate instances for concurrent processing
    - Processing time: 50-200 ms per frame (depends on cell count)
    - Cell numbering is deterministic: largest cells always get lower IDs
    - Closing kernel of (1,1) means no morphological processing
    
    Examples
    --------
    >>> config = {
    ...     "method": "threshold",
    ...     "threshold": 30,
    ...     "closing_kernel": (3, 3),
    ...     "min_cellsize_gridpoint": 5,
    ...     "global": {"var_names": {"reflectivity": "reflectivity"}}
    ... }
    >>> segmenter = RadarCellSegmenter(config)
    >>> ds_labeled = segmenter.segment(ds_2d)
    >>> print(f"Found {ds_labeled['cell_labels'].max()} cells")
    """

    def __init__(self, config):
        """Initialize segmenter with validated configuration.
        
        Parameters
        ----------
        config : InternalConfig
            Fully validated runtime configuration.
        
        Notes
        -----
        All parameters are read directly from config - no defaults,
        no .get() calls, no validation. Configuration is already
        complete and validated by Pydantic.
        
        Examples
        --------
        >>> from adapt.configuration.schemas import resolve_config, ParamConfig
        >>> config = resolve_config(ParamConfig())
        >>> segmenter = RadarCellSegmenter(config)
        """
        self.method = config.method
        self.threshold = config.threshold
        self.kernel_size = config.closing_kernel
        self.filter_by_size = config.filter_by_size
        self.min_gridpoints = config.min_cellsize_gridpoint
        self.max_gridpoints = config.max_cellsize_gridpoint
        self.h_maxima = config.h_maxima
        self.refl_name = config.reflectivity_var
        self.labels_name = config.labels_var
        self.z_level = config.z_level

        logger.info("RadarCellSegmenter initialized: method=%s, threshold=%s", 
                    self.method, self.threshold)

    def segment(self, ds: xr.Dataset) -> xr.Dataset:
        """Segment 2D reflectivity and return dataset with cell labels.
        
        Dispatches to appropriate segmentation method (currently only threshold).
        The input dataset should be 2D (reflectivity at a fixed altitude).
        The output dataset is a copy of the input with an additional
        "cell_labels" variable containing integer cell IDs.
        
        Parameters
        ----------
        ds : xr.Dataset
            2D xarray.Dataset containing reflectivity field and coordinates.
            Expected dimensions: (y, x)
            Expected data variables: reflectivity (or custom name via config)
            Expected attributes: z_level_m (altitude in meters, set by processor)
        
        Returns
        -------
        xr.Dataset
            Copy of input dataset with added cell_labels variable.
            Dataset attributes are preserved; cell_labels attributes
            include segmentation metadata (threshold, z-level, method, etc.).
        

        Notes
        -----
        - Method is determined at initialization (config["method"])
        - Currently only "threshold" is implemented
        - Cell labels are stored as int32 (0 = background, 1+ = cell IDs)
        - Cells are numbered in decreasing order of size (largest = 1)
        - Processing time: ~50-200 ms per frame
        
        Examples
        --------
        >>> segmenter = RadarCellSegmenter(config)
        >>> ds_labeled = segmenter.segment(ds_2d)
        >>> num_cells = ds_labeled['cell_labels'].max().item()
        >>> print(f"Found {num_cells} cells in this scan")
        """
        return self._segment2D_threshold(ds)

    def _segment2D_threshold(self, ds: xr.Dataset) -> xr.Dataset:
        """Apply threshold and morphology to detect cells (internal method).
        
        Steps:
        1. Extract reflectivity field (must be 2D: already sliced by processor)
        2. Apply binary threshold (reflectivity > threshold)
        3. Apply morphological closing (fill small holes)
        4. Label connected components
        5. Filter cells by size (min/max grid points)
        6. Relabel by decreasing size (largest cell = 1)
        7. Attach labels to dataset and return
        
        Parameters
        ----------
        ds : xr.Dataset
            2D xarray.Dataset with reflectivity at fixed z-level.
            Expected to be pre-sliced at a single altitude by processor.
        
        Returns
        -------
        xr.Dataset
            Copy of input with new cell_labels variable. Attributes include:
            - long_name: "Cell segmentation labels"
            - units: "1"
            - method: segmentation method (e.g., "threshold")
            - threshold: threshold value used
            - z_level_m: altitude of this slice (from ds.attrs)
            - min_cellsize_gridpoint: minimum size filter
            - max_cellsize_gridpoint: maximum size filter (if set)
        
        Notes
        -----
        - Expects 2D input (if 3D, slicing is caller's responsibility)
        - Variable names (reflectivity, cell_labels) are read from config
        - Logging includes cell count, filtering results
        - Processing time: typically 50-200 ms
        
        Examples
        --------
        >>> # Not typically called directly; use segment() instead
        >>> ds_labeled = segmenter._segment2D_threshold(ds_2d)
        """
        # Extract reflectivity (already 2D)
        refl = ds[self.refl_name].values

        binary_mask = refl > self.threshold
        labels = self._binary_to_labels(
            binary_mask,
            refl,
            self.kernel_size,
            self.filter_by_size,
            self.min_gridpoints,
            self.max_gridpoints,
        )

        # Build attrs dict, excluding None values (NetCDF can't serialize None)
        attrs = {
            "long_name": "Cell segmentation labels",
            "units": "1",
            "method": self.method,
            "threshold": self.threshold,
            "z_level_m": self.z_level,
            "min_cellsize_gridpoint": self.min_gridpoints,
        }
        if self.max_gridpoints is not None:
            attrs["max_cellsize_gridpoint"] = self.max_gridpoints

        labels_da = xr.DataArray(
            labels,
            dims=("y", "x"),
            coords={"y": ds.y, "x": ds.x},
            attrs=attrs
        )

        # we attach labels to original dataset
        ds_out = ds.copy()
        ds_out[self.labels_name] = labels_da
        logger.debug(
            f"Labels attached: var={self.labels_name}, shape={labels.shape}, max={labels.max()}"
        )

        return ds_out

    def _binary_to_labels(
        self, binary_mask: np.ndarray, field: np.ndarray,
        kernel_size: tuple, filter_by_size: bool,
        min_gridpoints: int, max_gridpoints: int,
    ) -> np.ndarray:
        """Morphology, detect cells, filter."""
        from skimage.morphology import closing, footprint_rectangle

        closed_mask = closing(binary_mask, footprint_rectangle(kernel_size))

        labels = _label_maxtree(closed_mask, field, h=self.h_maxima)

        # if there are any cells, filter and/or renumber
        if labels.max() > 0:
            labels = self._filter_and_relabel(
                labels, filter_by_size, min_gridpoints, max_gridpoints
            )

        return labels.astype(np.int32)

    def _filter_and_relabel(self, labels: np.ndarray, filter_by_size: bool,
                             min_gridpoints: int, max_gridpoints: int) -> np.ndarray:
        """Filter, renumber by size."""
        labels_unique, counts = np.unique(labels, return_counts=True)
        keep_mask = labels_unique > 0

        if filter_by_size:
            if min_gridpoints > 1:
                keep_mask &= (counts >= min_gridpoints)
                num_small = np.sum((labels_unique > 0) & (counts < min_gridpoints))
                if num_small > 0:
                    logger.debug(f"Removed {num_small} small (< {min_gridpoints})")

            if max_gridpoints is not None:
                keep_mask &= (counts <= max_gridpoints)
                num_large = np.sum((labels_unique > 0) & (counts > max_gridpoints))
                if num_large > 0:
                    logger.debug(f"Removed {num_large} large (> {max_gridpoints})")

        labels_to_keep = labels_unique[keep_mask]
        labels_renumbered = self._relabel_by_size(labels, labels_to_keep, counts)

        num_kept = len(labels_to_keep)
        num_removed = len(labels_unique) - 1 - num_kept
        if filter_by_size and num_removed > 0:
            logger.debug(f"Kept {num_kept}, removed {num_removed}")

        return labels_renumbered

    def _relabel_by_size(
        self, labels: np.ndarray, labels_to_keep: np.ndarray, counts: np.ndarray
    ) -> np.ndarray:
        """Renumber: largest=1."""
        keep_indices = np.isin(np.arange(len(counts)), labels_to_keep)
        keep_counts = counts[keep_indices]

        sort_indices = np.argsort(-keep_counts)
        labels_sorted = labels_to_keep[sort_indices]

        old_to_new = np.zeros(labels.max() + 1, dtype=np.int32)
        old_to_new[labels_sorted] = np.arange(1, len(labels_sorted) + 1)

        return old_to_new[labels]


# ---------------------------------------------------------------------------
# BaseModule wrapper — Step 6
# ---------------------------------------------------------------------------

from adapt.contracts import assert_gridded, assert_segmented  # noqa: E402
from adapt.execution.module_registry import registry  # noqa: E402
from adapt.modules.base import BaseModule  # noqa: E402


def _check_grid_ds_2d(ds):
    assert_gridded(ds, "reflectivity")


def _check_segmented_ds(ds):
    assert_segmented(ds, "cell_labels")


class DetectModule(BaseModule):
    """BaseModule wrapper for RadarCellSegmenter.

    Segments convective cells from a 2D reflectivity field using
    threshold and morphological filtering.

    Context inputs
    --------------
    grid_ds_2d : xr.Dataset
        2D Cartesian dataset (output of LoadModule).
    config : InternalConfig
        Runtime configuration.

    Context outputs
    ---------------
    segmented_ds : xr.Dataset
        2D dataset with cell_labels variable added.
    num_cells : int
        Number of detected cells.
    """

    name = "detection"
    inputs = ["grid_ds_2d", "detection_config"]
    outputs = ["segmented_ds", "num_cells"]
    input_contracts  = {"grid_ds_2d": _check_grid_ds_2d}
    output_contracts = {"segmented_ds": _check_segmented_ds}

    def __init__(self) -> None:
        self._segmenter = None

    def run(self, context: dict) -> dict:
        config = context["detection_config"]
        ds_2d = context["grid_ds_2d"]

        if self._segmenter is None:
            self._segmenter = RadarCellSegmenter(config)

        segmented = self._segmenter.segment(ds_2d)
        num_cells = int(segmented[config.labels_var].max().item())

        return {"segmented_ds": segmented, "num_cells": num_cells}


registry.register(DetectModule)
