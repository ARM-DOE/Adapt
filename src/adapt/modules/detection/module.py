# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Segment convective cells from gridded radar reflectivity.

Detects convective cell boundaries from a 2D reflectivity field (at a fixed
altitude) and returns a labeled image where each cell is assigned a unique
integer ID.

Two families of methods build the *convective mask* that a shared watershed +
size-filtering backend then labels into cells:

- ``threshold`` (default) — reflectivity above a fixed dBZ threshold. Needs only
  the 2D CAPPI, so it works on any run out of the box.
- pyart convective/stratiform classifiers — ``conv_strat_raut``,
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
from dataclasses import dataclass, fields

import numpy as np
import pyart
import xarray as xr
from scipy.ndimage import center_of_mass, distance_transform_edt, label
from skimage.morphology import h_maxima
from skimage.segmentation import watershed

from adapt.contracts import (
    ContractViolation,
    SegmentationComponent,
    SegmentationDecisions,
    SegmentationFrame,
    SegmentationSeed,
    require,
)

__all__ = ["CarriedLabels", "RadarCellSegmenter", "SeedCandidate"]

logger = logging.getLogger(__name__)

# A watershed marker carried from the previous frame: (row, col, carry_age).
SeedCandidate = tuple[int, int, int]

# Mask components are 8-connected, matching h_maxima's default footprint.
_COMPONENT_CONNECTIVITY = np.ones((3, 3), dtype=bool)

_SEED_FIELDS = tuple(f.name for f in fields(SegmentationSeed))


def _seed_row(**values) -> dict:
    """A SegmentationSeed record under construction: every field, unset ones None."""
    row = dict.fromkeys(_SEED_FIELDS)
    row.update(values)
    return row


def _independent_seed(marker: int, plateau: np.ndarray, fp: np.ndarray, components) -> dict:
    """Decision row for an h-maxima marker: its peak pixel, plateau size and component."""
    rows, cols = np.nonzero(plateau)
    k = int(np.argmax(fp[rows, cols]))
    row, col = int(rows[k]), int(cols[k])
    return _seed_row(
        marker_id=marker,
        origin="HMAXIMA",
        row=row,
        col=col,
        peak_value=float(fp[row, col]),
        plateau_px=int(len(rows)),
        component_id=int(components[row, col]),
        decision="SEEDED",
    )


@dataclass(frozen=True)
class CarriedLabels:
    """The previous frame's cell labels advected to this frame, with carry ages.

    ``labels`` is an int raster (y, x), 0 = none; ``ages[i]`` is the carry age
    of label ``i + 1`` (0 = detected independently in the previous frame).
    """

    labels: np.ndarray
    ages: tuple[int, ...]


def _footprint_seed(footprint: np.ndarray) -> tuple[int, int]:
    """Centroid of a footprint, snapped to its nearest pixel when it falls outside."""
    row, col = (int(round(v)) for v in center_of_mass(footprint))
    if footprint[row, col]:
        return row, col
    rows, cols = np.nonzero(footprint)
    nearest = int(np.argmin((rows - row) ** 2 + (cols - col) ** 2))
    return int(rows[nearest]), int(cols[nearest])


# ---------------------------------------------------------------------------
# pyart convective/stratiform mask builders
#
# Each maps a pyart classifier over the full 3D grid (wrapped as
# ``pyart.xradar.Xgrid``) to a 2D boolean convective mask at the analysis
# z-level. Registered by method name in ``_CONV_STRAT_MASKERS`` below; adding a
# classifier is one entry with no change to the segmenter (Open/Closed).
# ---------------------------------------------------------------------------


def _with_cf_time(grid_ds: xr.Dataset) -> xr.Dataset:
    """Return the grid with CF-encoded (numeric + units) time for ``Xgrid``.

    ``pyart.xradar.Xgrid`` requires the un-decoded representation (numeric
    values with a ``units`` attr); the in-memory grid from ``Grid.to_xarray``
    carries decoded datetimes, so re-encode exactly what an un-decoded file
    open would present.
    """
    if "units" in grid_ds["time"].attrs:
        return grid_ds
    encoded = xr.conventions.encode_cf_variable(grid_ds["time"].variable)
    out = grid_ds.copy()
    out["time"] = xr.DataArray(encoded.data, dims=encoded.dims, attrs=dict(encoded.attrs))
    return out


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
    3. **Cell seeding + labeling**: h-maxima seeds — plus, with ``seed_carry``,
       seeds carried from the previous frame — grown by watershed
    4. **Size filtering**: remove cells smaller than min_gridpoints or larger
       than max_gridpoints (optional)
    5. **Relabeling by size**: largest cell gets label 1, second-largest 2, etc.

    The output is a labeled image (xarray.DataArray) with integer cell IDs.
    Cell ID = 0 means no cell; cell ID > 0 means part of that cell.

    Configuration
    =============
    Reads from a validated ``DetectionConfig``:

    - `method` : str
        ``threshold`` (default), ``conv_strat_raut``, ``conv_strat_yuter``,
        ``feature_detection`` or ``steiner_conv_strat``.
        The pyart methods require the 3D grid dataset (``grid_ds``).
    - `method_params` : dict
        Resolved parameters for the selected method. For ``threshold`` this is
        ``{"threshold": <dBZ>}``; for the pyart methods it is the algorithm's
        keyword arguments (splatted into the pyart call, recorded in the output).
    - `closing_radius` : int, default 2 (disk radius for mask closing; 0 = off)
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
        self.closing_radius = config.closing_radius
        self.filter_by_size = config.filter_by_size
        self.min_gridpoints = config.min_cellsize_gridpoint
        self.max_gridpoints = config.max_cellsize_gridpoint
        self.h_maxima = config.h_maxima
        self.seed_carry_frames = config.seed_carry_frames
        # One integer governs the carry: 0 is off, N allows N frames.
        self.seed_carry = config.seed_carry_frames > 0
        self.seed_carry_min_separation = config.seed_carry_min_separation
        self.carried_exempt_size_filter = config.carried_exempt_size_filter
        self.refl_name = config.reflectivity_var
        self.labels_name = config.labels_var
        self.z_level = config.z_level
        self._decisions: SegmentationDecisions | None = None

        logger.info(
            "RadarCellSegmenter initialized: method=%s, params=%s",
            self.method,
            self.method_params,
        )

    def segment(
        self,
        ds: xr.Dataset,
        grid_ds: xr.Dataset | None = None,
        carried: CarriedLabels | None = None,
    ) -> xr.Dataset:
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
        grid_ds : xr.Dataset | None
            Full 3D gridded dataset (ingest's in-memory output). Required by
            the pyart convective/stratiform methods (they classify the 3D grid
            via ``pyart.xradar.Xgrid``); unused by ``threshold``.
        carried : CarriedLabels | None
            The previous frame's labels advected to this frame, with carry
            ages. A connected component of the mask that holds fewer
            independent seeds than prior cells has lost a core; only that
            deficit is filled with extra watershed markers, so a cell's own
            mispositioned footprint never splits it. Requires ``seed_carry``.

        Returns
        -------
        xr.Dataset
            Copy of ``ds`` with an int32 ``cell_labels`` variable
            (0 = background, 1..N = cells by decreasing size). Label attrs
            record method, threshold, z-level, and size-filter settings and,
            with ``seed_carry``, ``carry_age`` per label.
        """
        require(
            self.seed_carry or carried is None,
            "Detection: carried labels offered while segmenter.seed_carry is off",
        )
        if self.refl_name not in ds.data_vars:
            raise ContractViolation(
                f"Detection: configured tracking field {self.refl_name!r} is "
                f"not in the grid (available: "
                f"{sorted(str(v) for v in ds.data_vars)}). Check "
                "global_.tracking_field and reader.field_map/fields."
            )
        refl = ds[self.refl_name].values
        binary_mask = self._convective_mask(refl, grid_ds)

        labels, ages = self._binary_to_labels(binary_mask, refl, carried)

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
        # Provenance of the carry: which cells this scan owes to a carried seed.
        # Only with the flag on (off must stay byte-identical) and only when
        # there are cells (NetCDF cannot hold a zero-length attribute).
        if self.seed_carry and ages:
            attrs["carry_age"] = np.asarray(ages, dtype=np.int32)
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

    def decisions(self) -> SegmentationDecisions:
        """The decision record of the most recent ``segment`` call (analysis only)."""
        if self._decisions is None:
            raise ValueError("RadarCellSegmenter.decisions: nothing has been segmented yet")
        return self._decisions

    def _convective_mask(self, refl: np.ndarray, grid_ds: xr.Dataset | None) -> np.ndarray:
        """Build the 2D boolean convective mask for the configured method.

        ``threshold`` masks the 2D reflectivity directly; every other method
        delegates to a pyart convective/stratiform classifier and keeps the
        convective class(es).
        """
        if self.method == "threshold":
            return refl > self.method_params["threshold"]
        return self._pyart_conv_strat_mask(grid_ds)

    def _pyart_conv_strat_mask(self, grid_ds: xr.Dataset | None) -> np.ndarray:
        """Classify the 3D grid with the configured pyart method; return convective mask.

        Wraps ingest's in-memory 3D grid in ``pyart.xradar.Xgrid`` (the grid
        representation the pyart retrievals consume). The returned mask is 2D
        (y, x) at the analysis z-level, aligned with the 2D reflectivity used
        for labeling.
        """
        if grid_ds is None:
            raise RuntimeError(
                f"Segmentation method '{self.method}' requires the 3D grid dataset "
                "(grid_ds), but none was provided."
            )
        masker = _CONV_STRAT_MASKERS[self.method]
        xgrid = pyart.xradar.Xgrid(_with_cf_time(grid_ds))
        return masker(xgrid, self.refl_name, self.z_level, self.method_params)

    def _binary_to_labels(
        self,
        binary_mask: np.ndarray,
        field: np.ndarray,
        carried: CarriedLabels | None,
    ) -> tuple[np.ndarray, tuple[int, ...]]:
        """Morphology, detect cells, filter; returns labels and carry age per label."""
        from skimage.morphology import closing, disk

        # Dilate then erode with the same disk: gaps and indentations narrower
        # than ~2r are filled and the boundary is rounded, while the outer extent
        # of anything larger than the disk is unchanged.
        closed_mask = (
            closing(binary_mask, disk(self.closing_radius))
            if self.closing_radius > 0
            else binary_mask
        )
        components, _ = label(closed_mask, structure=_COMPONENT_CONNECTIVITY)

        basins, admitted, seed_rows = self._label_maxtree(closed_mask, field, carried, components)

        # Basins whose marker came from a carried seed, so the size filter can
        # spare them (see _filter_and_relabel).
        exempt = (
            frozenset(
                int(r["marker_id"])
                for r in seed_rows
                if r["origin"] == "CARRIED" and r["decision"] == "SEEDED" and r["marker_id"]
            )
            if self.carried_exempt_size_filter
            else frozenset()
        )

        # if there are any cells, filter and/or renumber
        labels = self._filter_and_relabel(basins, exempt) if basins.max() > 0 else basins
        labels = labels.astype(np.int32)

        # A marker pixel keeps its own label through watershed and relabeling,
        # so an admitted candidate's final label is read at its pixel; 0 means
        # the size filter dropped that basin. Everything else is age 0.
        ages = [0] * int(labels.max())
        for row, col, age in admitted:
            final = int(labels[row, col])
            if final:
                ages[final - 1] = age + 1

        arrays = {
            "mask": binary_mask,
            "closed": closed_mask,
            "components": components,
            "field": field,
        }
        self._decisions = self._decide(arrays, basins, labels, seed_rows)
        return labels, tuple(ages)

    def _decide(
        self, arrays: dict, basins: np.ndarray, labels: np.ndarray, seed_rows: list[dict]
    ) -> SegmentationDecisions:
        """Assemble this scan's decision record from the stage arrays and seed rows."""
        components, field = arrays["components"], arrays["field"]
        for row in seed_rows:
            if row["marker_id"] is None:
                continue
            basin = basins == row["marker_id"]
            row["basin_px"] = int(basin.sum())
            row["final_label"] = int(labels[basin].max()) if row["basin_px"] else 0
        markers = [r for r in seed_rows if r["marker_id"] is not None]
        dropped = [r for r in markers if r["basin_px"] and r["final_label"] == 0]
        n_dropped_small = sum(r["basin_px"] < self.min_gridpoints for r in dropped)
        # Carried basins that survived only because the exemption spared them.
        n_size_exempt = (
            sum(
                r["origin"] == "CARRIED"
                and r["decision"] == "SEEDED"
                and r["final_label"] != 0
                and 0 < (r["basin_px"] or 0) < self.min_gridpoints
                for r in markers
            )
            if self.carried_exempt_size_filter and self.filter_by_size
            else 0
        )

        component_rows = []
        for cid in range(1, int(components.max()) + 1):
            inside = components == cid
            here = [r for r in seed_rows if r["component_id"] == cid]
            n_seeds = sum(r["origin"] == "HMAXIMA" for r in here)
            n_claims = sum(r["origin"] == "CARRIED" for r in here)
            component_rows.append(
                SegmentationComponent(
                    component_id=cid,
                    area_px=int(inside.sum()),
                    field_max=float(np.nanmax(field[inside])),
                    field_min=float(np.nanmin(field[inside])),
                    n_seeds=n_seeds,
                    n_claims=n_claims,
                    deficit=n_claims - n_seeds,
                    n_admitted=sum(
                        r["origin"] == "CARRIED" and r["decision"] == "SEEDED" for r in here
                    ),
                    n_final_labels=len(set(labels[inside].tolist()) - {0}),
                )
            )
        carried = [r for r in seed_rows if r["origin"] == "CARRIED"]
        frame = SegmentationFrame(
            mask_px=int(arrays["mask"].sum()),
            closed_mask_px=int(arrays["closed"].sum()),
            n_components=len(component_rows),
            n_independent_seeds=sum(r["origin"] == "HMAXIMA" for r in seed_rows),
            n_carried_claims=sum(r["component_id"] is not None for r in carried),
            n_carried_admitted=sum(r["decision"] == "SEEDED" for r in carried),
            n_basins=sum(bool(r["basin_px"]) for r in markers),
            n_dropped_small=n_dropped_small,
            n_dropped_large=len(dropped) - n_dropped_small,
            n_cells=int(labels.max()),
            n_size_exempt=n_size_exempt,
        )
        seeds = tuple(SegmentationSeed(**row) for row in seed_rows)
        return SegmentationDecisions(frame, seeds, tuple(component_rows))

    def _label_maxtree(
        self,
        binary: np.ndarray,
        field: np.ndarray,
        carried: CarriedLabels | None,
        components: np.ndarray,
    ) -> tuple[np.ndarray, tuple[SeedCandidate, ...], list[dict]]:
        """h-maxima seeding + watershed, with admitted carried seeds as extra markers.

        Identifies individual cells within a binary convection mask by seeding
        each local intensity maximum (that rises at least ``h_maxima`` dBZ
        above its surroundings) and growing watershed regions from those
        seeds. Admitted carried seeds are appended as markers after the
        h-maxima seeds — a union, never a replacement.

        Parameters
        ----------
        binary : np.ndarray (bool)
            Closed binary convection mask (output of morphological closing).
        field : np.ndarray (float)
            Reflectivity values aligned with `binary`.
        carried : CarriedLabels | None
            The previous frame's labels advected to this frame.
        components : np.ndarray (int)
            8-connected components of ``binary``.

        Returns
        -------
        basins : np.ndarray (int32)
            0 = background, 1..M = one basin per marker (before size filtering).
        admitted : tuple of (row, col, carry_age)
            The carried seeds used as markers.
        seed_rows : list of dict
            One decision row per marker or carried claim.
        """
        fp = np.where(binary, field, 0.0)
        peaks = h_maxima(fp, h=self.h_maxima)
        seeds, n_seeds = label(peaks)  # label from scipy.ndimage
        seed_rows = [
            _independent_seed(m, seeds == m, fp, components) for m in range(1, n_seeds + 1)
        ]
        admitted, carried_rows = self._admit(carried, binary, seeds, components)
        for marker, (row, col, _) in enumerate(admitted, start=n_seeds + 1):
            seeds[row, col] = marker
        for entry in carried_rows:
            if entry["decision"] == "SEEDED":
                row, col = entry["row"], entry["col"]
                entry.update(
                    marker_id=int(seeds[row, col]), peak_value=float(fp[row, col]), plateau_px=1
                )
        seed_rows += carried_rows
        if n_seeds + len(admitted) == 0:
            return np.zeros(binary.shape, dtype=np.int32), (), seed_rows
        ws = watershed(-fp, seeds, mask=binary)
        return np.where(binary, ws, 0).astype(np.int32), admitted, seed_rows

    def _admit(
        self,
        carried: CarriedLabels | None,
        binary: np.ndarray,
        seeds: np.ndarray,
        components: np.ndarray,
    ) -> tuple[tuple[SeedCandidate, ...], list[dict]]:
        """Fill each mask component's lost cores with carried seeds — nothing more.

        Per 8-connected component of the mask: every prior cell whose
        advected footprint lands in it counts as a claim; a footprint that
        already holds an independent seed is that component's survivor, not
        a candidate. With ``k`` seeds and ``n`` claims, at most ``n - k``
        markers are added, farthest from a seed first, each at least
        ``seed_carry_min_separation`` from any seed or marker. A cell's own
        mispositioned footprint (one claim, one seed) therefore never splits
        it; a second prior cell whose core dropped below ``h_maxima`` does
        get rescued. Prior cells past the carry bound make no claim.

        Returns the admitted markers and one decision row per prior cell.
        """
        if carried is None:
            return (), []
        min_sep = self.seed_carry_min_separation
        clearance = (
            distance_transform_edt(seeds == 0) if seeds.any() else np.full(seeds.shape, np.inf)
        )

        rows: list[dict] = []
        claims: dict[int, int] = {}
        candidates: dict[int, list[dict]] = {}
        for lab in np.unique(carried.labels[carried.labels > 0]):
            age = carried.ages[int(lab) - 1]
            footprint = carried.labels == lab
            row, col = _footprint_seed(footprint)
            entry = _seed_row(
                origin="CARRIED",
                row=row,
                col=col,
                prior_label=int(lab),
                prior_age=int(age),
                footprint_px=int(footprint.sum()),
            )
            rows.append(entry)
            if age + 1 > self.seed_carry_frames:
                entry["decision"] = "EXPIRED"
                continue
            if not binary[row, col]:
                entry["decision"] = "OUTSIDE_MASK"
                continue
            component = int(components[row, col])
            claims[component] = claims.get(component, 0) + 1
            entry.update(
                component_id=component,
                clearance_px=float(clearance[row, col]),
                footprint_holds_seed=bool((seeds[footprint] > 0).any()),
            )
            if entry["footprint_holds_seed"]:
                entry["decision"] = "SURVIVOR"
                continue
            candidates.setdefault(component, []).append(entry)

        seeds_in = {c: len(np.unique(seeds[(components == c) & (seeds > 0)])) for c in claims}
        admitted: list[SeedCandidate] = []
        for component, ranked in candidates.items():
            deficit = claims[component] - seeds_in[component]
            placed = 0
            for entry in sorted(ranked, key=lambda e: (-e["clearance_px"], e["row"], e["col"])):
                row, col = entry["row"], entry["col"]
                if placed >= deficit:
                    entry["decision"] = "NO_DEFICIT"
                elif entry["clearance_px"] < min_sep:
                    entry["decision"] = "TOO_CLOSE_TO_SEED"
                elif any(np.hypot(row - r, col - c) < min_sep for r, c, _ in admitted):
                    entry["decision"] = "TOO_CLOSE_TO_MARKER"
                else:
                    entry["decision"] = "SEEDED"
                    admitted.append((row, col, entry["prior_age"]))
                    placed += 1
        for entry in rows:
            component = entry["component_id"]
            if component is not None:
                entry.update(
                    component_claims=claims[component],
                    component_seeds=seeds_in[component],
                    component_deficit=claims[component] - seeds_in[component],
                )
        return tuple(admitted), rows

    def _filter_and_relabel(
        self, labels: np.ndarray, exempt_markers: frozenset[int] = frozenset()
    ) -> np.ndarray:
        """Filter, renumber by size.

        ``exempt_markers`` are basin labels the minimum-size filter must not
        drop. A carried seed is only ever admitted where a previous cell lost
        its core, so its basin is small by construction; deleting it undoes the
        rescue and leaves the previous cell with no successor, which the tracker
        then reports as a termination. Only the minimum-size test is waived —
        the maximum still applies, since a runaway basin is a different fault.
        """
        labels_unique, counts = np.unique(labels, return_counts=True)
        keep_mask = labels_unique > 0

        if self.filter_by_size:
            if self.min_gridpoints > 1:
                too_small = counts < self.min_gridpoints
                if exempt_markers:
                    too_small &= ~np.isin(labels_unique, list(exempt_markers))
                keep_mask &= ~too_small
                num_small = np.sum((labels_unique > 0) & too_small)
                if num_small > 0:
                    logger.debug(f"Removed {num_small} small (< {self.min_gridpoints})")

            if self.max_gridpoints is not None:
                keep_mask &= counts <= self.max_gridpoints
                num_large = np.sum((labels_unique > 0) & (counts > self.max_gridpoints))
                if num_large > 0:
                    logger.debug(f"Removed {num_large} large (> {self.max_gridpoints})")

        labels_to_keep = labels_unique[keep_mask]
        labels_renumbered = self._relabel_by_size(labels, labels_to_keep, counts[keep_mask])

        num_kept = len(labels_to_keep)
        num_removed = len(labels_unique) - 1 - num_kept
        if self.filter_by_size and num_removed > 0:
            logger.debug(f"Kept {num_kept}, removed {num_removed}")

        return labels_renumbered

    def _relabel_by_size(
        self, labels: np.ndarray, labels_to_keep: np.ndarray, keep_counts: np.ndarray
    ) -> np.ndarray:
        """Renumber: largest=1.

        ``keep_counts[i]`` is the pixel count of ``labels_to_keep[i]``; the caller
        slices both with the same mask so position is never read as label value.
        """
        sort_indices = np.argsort(-keep_counts)
        labels_sorted = labels_to_keep[sort_indices]

        old_to_new = np.zeros(labels.max() + 1, dtype=np.int32)
        old_to_new[labels_sorted] = np.arange(1, len(labels_sorted) + 1)

        return old_to_new[labels]
