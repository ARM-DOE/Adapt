# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Segment convective cells from gridded radar reflectivity.

Detects convective cell boundaries from a 2D reflectivity field (at a fixed
altitude) and returns a labeled image where each cell is assigned a unique
integer ID.

Two families of methods build the *convective mask* that a shared watershed +
size-filtering backend then labels into cells:

- ``threshold`` — reflectivity above a fixed dBZ threshold (the original method).
- pyart convective/stratiform classifiers — ``conv_strat_raut`` (default),
  ``conv_strat_yuter``, ``feature_detection``, ``steiner_conv_strat`` — the
  convective class(es) of a per-pixel classification, computed over the full 3D
  grid via ``pyart.xradar.Xgrid``.

Only the mask differs between methods; downstream labeling, size filtering, and
size-ranked numbering are identical, so the output contract (integer,
non-negative, 2D ``cell_labels``) is unchanged and tracking is unaffected.

Cell size ordering: cells are numbered 1, 2, 3, ... in decreasing order of size
(area in grid points), so the largest cell is always ID 1. This ensures
reproducible analysis and allows size-based filtering in downstream steps.
"""

import logging

import numpy as np
import pyart
import xarray as xr
from scipy.ndimage import label
from skimage.morphology import h_maxima
from skimage.segmentation import watershed

__all__ = ["RadarCellSegmenter"]

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
    seeds, n_seeds = label(peaks)  # label from scipy.ndimage
    if n_seeds == 0:
        return np.zeros(binary.shape, dtype=np.int32)
    ws = watershed(-fp, seeds, mask=binary)
    return np.where(binary, ws, 0).astype(np.int32)


# ---------------------------------------------------------------------------
# pyart convective/stratiform mask builders
#
# Each maps a pyart classifier over the full 3D grid (wrapped as
# ``pyart.xradar.Xgrid``) to a 2D boolean convective mask at the analysis
# z-level. Registered by method name in ``_CONV_STRAT_MASKERS`` below; adding a
# classifier is one entry with no change to the segmenter (Open/Closed).
# ---------------------------------------------------------------------------


def _grid_spacing(xgrid) -> tuple[float, float]:
    """Horizontal grid spacing (dx, dy) in metres from an Xgrid's x/y axes."""
    x = np.asarray(xgrid.x["data"])
    y = np.asarray(xgrid.y["data"])
    return float(x[1] - x[0]), float(y[1] - y[0])


def _mask_conv_strat_raut(xgrid, refl_field: str, z_level: float, params: dict) -> np.ndarray:
    """Raut wavelet classification -> convective mask (cores + mixed: classes 2, 3)."""
    z = np.asarray(xgrid.z["data"])
    cappi_level = int(np.argmin(np.abs(z - z_level)))
    result = pyart.retrieve.conv_strat_raut(xgrid, refl_field, cappi_level=cappi_level, **params)
    classes = np.asarray(result["wt_reclass"]["data"]).squeeze()
    return np.isin(classes, (2, 3))


def _mask_conv_strat_yuter(xgrid, refl_field: str, z_level: float, params: dict) -> np.ndarray:
    """Yuter/Powell feature detection -> convective mask (class 2)."""
    dx, dy = _grid_spacing(xgrid)
    result = pyart.retrieve.conv_strat_yuter(
        xgrid, dx=dx, dy=dy, level_m=z_level, refl_field=refl_field, **params
    )
    classes = np.asarray(result["feature_detection"]["data"]).squeeze()
    return classes == 2


def _mask_feature_detection(xgrid, refl_field: str, z_level: float, params: dict) -> np.ndarray:
    """Tomkins generalized feature detection -> convective mask (class 2)."""
    dx, dy = _grid_spacing(xgrid)
    result = pyart.retrieve.feature_detection(
        xgrid, dx=dx, dy=dy, level_m=z_level, field=refl_field, **params
    )
    classes = np.asarray(result["feature_detection"]["data"]).squeeze()
    return classes == 2


def _mask_steiner_conv_strat(xgrid, refl_field: str, z_level: float, params: dict) -> np.ndarray:
    """Steiner et al. 1995 classification -> convective mask (class 2)."""
    dx, dy = _grid_spacing(xgrid)
    result = pyart.retrieve.steiner_conv_strat(
        xgrid, dx=dx, dy=dy, work_level=z_level, refl_field=refl_field, **params
    )
    classes = np.asarray(result["data"]).squeeze()
    return classes == 2


_CONV_STRAT_MASKERS = {
    "conv_strat_raut": _mask_conv_strat_raut,
    "conv_strat_yuter": _mask_conv_strat_yuter,
    "feature_detection": _mask_feature_detection,
    "steiner_conv_strat": _mask_steiner_conv_strat,
}


class RadarCellSegmenter:
    """Convective-cell detection and labeling for 2D radar reflectivity.

    Applies a series of image-processing steps to identify and label
    convective cells:

    1. **Convective mask**: mark convective grid points (``threshold`` uses
       reflectivity > threshold; the pyart methods use the convective class of a
       convective/stratiform classification)
    2. **Morphological closing**: fill small holes within cells (tunable)
    3. **Cell seeding + labeling**: h-maxima seeds grown by watershed
    4. **Size filtering**: remove cells smaller than min_gridpoints or larger
       than max_gridpoints (optional)
    5. **Relabeling by size**: largest cell gets label 1, second-largest 2, etc.

    The output is a labeled image (xarray.DataArray) with integer cell IDs.
    Cell ID = 0 means no cell; cell ID > 0 means part of that cell.

    Configuration
    =============
    Reads from a validated ``DetectionConfig``:

    - `method` : str
        ``conv_strat_raut`` (default), ``conv_strat_yuter``,
        ``feature_detection``, ``steiner_conv_strat`` or ``threshold``.
        The pyart methods require the 3D gridded NetCDF (``grid_nc_path``).
    - `method_params` : dict
        Resolved parameters for the selected method. For ``threshold`` this is
        ``{"threshold": <dBZ>}``; for the pyart methods it is the algorithm's
        keyword arguments (splatted into the pyart call, recorded in the output).
    - `closing_kernel` : tuple of int, default (1, 1)
        Morphological closing footprint. (1, 1) means no closing.
    - `filter_by_size` : bool, default True
        Whether to apply cell size filtering.
    - `min_cellsize_gridpoint` : int, default 5
        Minimum cell size in grid points. Smaller cells are removed.
    - `max_cellsize_gridpoint` : int or None, default None
        Maximum cell size in grid points. If None, no upper limit.

    Notes
    -----
    - Input dataset must be 2D (already sliced at a fixed altitude by processor)
    - Not thread-safe; create separate instances for concurrent processing
    - Cell numbering is deterministic: largest cells always get lower IDs
    """

    def __init__(self, config):
        """Initialize segmenter with validated configuration.

        Parameters
        ----------
        config : DetectionConfig
            Fully validated runtime configuration.

        Notes
        -----
        All parameters are read directly from config - no defaults,
        no .get() calls, no validation. Configuration is already
        complete and validated by Pydantic.
        """
        self.method = config.method
        self.method_params = config.method_params
        self.kernel_size = config.closing_kernel
        self.filter_by_size = config.filter_by_size
        self.min_gridpoints = config.min_cellsize_gridpoint
        self.max_gridpoints = config.max_cellsize_gridpoint
        self.h_maxima = config.h_maxima
        self.refl_name = config.reflectivity_var
        self.labels_name = config.labels_var
        self.z_level = config.z_level

        logger.info(
            "RadarCellSegmenter initialized: method=%s, params=%s",
            self.method,
            self.method_params,
        )

    def segment(self, ds: xr.Dataset, grid_nc_path: str | None = None) -> xr.Dataset:
        """Segment 2D reflectivity and return dataset with cell labels.

        The configured ``method`` determines how the convective mask is built;
        the mask is then labeled into size-ranked cells by a shared backend.
        The output dataset is a copy of the input with an added ``cell_labels``
        variable of integer cell IDs.

        Parameters
        ----------
        ds : xr.Dataset
            2D dataset (dims y, x) with the reflectivity field, sliced at the
            analysis z-level. Supplies the reflectivity, coordinates, and attrs
            for the output.
        grid_nc_path : str | None
            Path to the full 3D gridded NetCDF. Required by the pyart
            convective/stratiform methods (they classify the 3D grid via
            ``pyart.xradar.Xgrid``); unused by ``threshold``.

        Returns
        -------
        xr.Dataset
            Copy of ``ds`` with an int32 ``cell_labels`` variable
            (0 = background, 1..N = cells by decreasing size). Label attrs
            record method, threshold, z-level, and size-filter settings.
        """
        refl = ds[self.refl_name].values
        binary_mask = self._convective_mask(refl, grid_nc_path)

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
            "z_level_m": self.z_level,
            "min_cellsize_gridpoint": self.min_gridpoints,
        }
        if self.max_gridpoints is not None:
            attrs["max_cellsize_gridpoint"] = self.max_gridpoints
        # Record the parameters actually passed to the selected method, so the
        # output states exactly what drove the classification. Skip None and cast
        # bool -> int for NetCDF attribute serialization.
        for key, value in self.method_params.items():
            if value is None:
                continue
            attrs[key] = int(value) if isinstance(value, bool) else value

        labels_da = xr.DataArray(
            labels, dims=("y", "x"), coords={"y": ds.y, "x": ds.x}, attrs=attrs
        )

        ds_out = ds.copy()
        ds_out[self.labels_name] = labels_da
        logger.debug(
            f"Labels attached: var={self.labels_name}, shape={labels.shape}, max={labels.max()}"
        )
        return ds_out

    def _convective_mask(self, refl: np.ndarray, grid_nc_path: str | None) -> np.ndarray:
        """Build the 2D boolean convective mask for the configured method.

        ``threshold`` masks the 2D reflectivity directly; every other method
        delegates to a pyart convective/stratiform classifier and keeps the
        convective class(es).
        """
        if self.method == "threshold":
            return refl > self.method_params["threshold"]
        return self._pyart_conv_strat_mask(grid_nc_path)

    def _pyart_conv_strat_mask(self, grid_nc_path: str | None) -> np.ndarray:
        """Classify the 3D grid with the configured pyart method; return convective mask.

        Opens the gridded NetCDF as a fresh dataset and wraps it in
        ``pyart.xradar.Xgrid`` (the grid representation the pyart retrievals
        consume). The returned mask is 2D (y, x) at the analysis z-level,
        aligned with the 2D reflectivity used for labeling.
        """
        if grid_nc_path is None:
            raise RuntimeError(
                f"Segmentation method '{self.method}' requires the gridded 3D NetCDF "
                "(grid_nc_path), but none was produced. Enable regridder.save_netcdf."
            )
        masker = _CONV_STRAT_MASKERS[self.method]
        with xr.open_dataset(grid_nc_path, decode_times=False) as grid_ds:
            xgrid = pyart.xradar.Xgrid(grid_ds)
            return masker(xgrid, self.refl_name, self.z_level, self.method_params)

    def _binary_to_labels(
        self,
        binary_mask: np.ndarray,
        field: np.ndarray,
        kernel_size: tuple,
        filter_by_size: bool,
        min_gridpoints: int,
        max_gridpoints: int,
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

    def _filter_and_relabel(
        self,
        labels: np.ndarray,
        filter_by_size: bool,
        min_gridpoints: int,
        max_gridpoints: int,
    ) -> np.ndarray:
        """Filter, renumber by size."""
        labels_unique, counts = np.unique(labels, return_counts=True)
        keep_mask = labels_unique > 0

        if filter_by_size:
            if min_gridpoints > 1:
                keep_mask &= counts >= min_gridpoints
                num_small = np.sum((labels_unique > 0) & (counts < min_gridpoints))
                if num_small > 0:
                    logger.debug(f"Removed {num_small} small (< {min_gridpoints})")

            if max_gridpoints is not None:
                keep_mask &= counts <= max_gridpoints
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
