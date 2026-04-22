# src/adapt/radar/cell_analyzer.py
"""Extract statistical properties from labeled convective cells.

This module computes per-cell statistics (area, intensity, motion, projection)
from segmented radar data. Output is a Pandas DataFrame with one row per cell,
containing geometric centroids, reflectivity/velocity statistics, and cell
motion information.

The analyzer handles:
- Multiple centroid types: geometric (center-of-mass), mass-weighted, max reflectivity, projected
- Multi-variable statistics: reflectivity, velocity, differential phase, spectrum width, etc.
- Geographic coordinate conversion: pixel (x,y) to latitude/longitude
- Cell motion: heading vectors and projection centroid evolution
- Database-ready output: DataFrame suitable for SQL insertion

All centroids are stored in both pixel coordinates (x, y) and geographic
coordinates (latitude, longitude) for flexibility in downstream analysis.

Author: Bhupendra Raut
"""

import logging
import json
import numpy as np
import pandas as pd
import xarray as xr
from typing import TYPE_CHECKING
from scipy.ndimage import center_of_mass
from skimage.measure import regionprops

if TYPE_CHECKING:
    from adapt.configuration.schemas import InternalConfig

__all__ = ['RadarCellAnalyzer']

logger = logging.getLogger(__name__)

# Suppress HDF5 diagnostic error messages
try:
    import h5py
    h5py._errors.silence_errors()
except (ImportError, AttributeError):
    pass


class RadarCellAnalyzer:
    """Extract geometric and statistical properties from segmented radar cells.
    
    This class computes per-cell statistics from segmented radar data to create
    a DataFrame suitable for machine learning, statistical analysis, or database
    storage. Input is a 2D xarray.Dataset with cell labels and radar fields;
    output is a Pandas DataFrame with one row per cell.
    
    Features computed per cell:
    
    1. **Geometric Centroids** (in both pixel and geographic coordinates):
       - Geometric: center-of-mass of binary cell mask
       - Mass-weighted: reflectivity-weighted centroid
       - Max reflectivity: location of highest reflectivity
       - Projection: forward-projected motion centroids (0-5 steps)
    
    2. **Area and Size**:
       - Cell area in km2 (computed from grid spacing)
       - Grid point count
    
    3. **Radar Statistics** (per variable):
       - Mean, std, min, max, median
       - 25th/75th percentiles
       - Variables: reflectivity, velocity, spectrum width, differential phase, etc.
    
    4. **Cell Motion**:
       - Heading vectors (from projector) within cell region
       - Heading direction and speed statistics
    
    5. **Metadata**:
       - Scan time, z-level (altitude)
       - Dataset attributes preservation
    
    Configuration
    ==============
    Config dict structure:
    
    - `global` : dict, optional
        - `var_names` : dict
            Variable naming mapping (reflectivity, cell_labels, etc.)
    
    - `radar_variables` : list, optional
        Whitelist of variables to analyze (default: common radar fields).
        Only variables in this list AND present in dataset are included.
    
    - `exclude_fields` : list, optional
        Variables to skip (metadata, projection, etc.). Takes precedence over
        whitelist.
    
    - `projector` : dict, optional
        - `max_projection_steps` : int, default 5
            Number of forward projections to extract
    
    Notes
    -----
    - Not thread-safe; create separate instances for concurrent processing
    - Requires pre-segmented input (cell_labels variable must exist)
    - Processing time: 100-500 ms per frame (depends on cell count)
    - Centroid naming is systematic: cell_centroid_<type>_{x,y,lat,lon}
    - Returns empty DataFrame if no cells found (all labels = 0)
    - All centroid coordinates are preserved in both coordinate systems
    
    Examples
    --------
    >>> config = {"radar_variables": ["reflectivity", "velocity"]}
    >>> analyzer = RadarCellAnalyzer(config)
    >>> df = analyzer.extract(ds_segmented)
    >>> print(df.columns)  # centroid locations, area, reflectivity stats, etc.
    >>> print(len(df))  # number of cells in this frame
    """
    
    def __init__(self, config: "InternalConfig"):
        """Initialize analyzer with validated configuration.
        
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
        >>> analyzer = RadarCellAnalyzer(config)
        """
        self.config = config
        self.reflectivity_field = config.global_.var_names.reflectivity
        self.radar_variables = config.analyzer.radar_variables
        self.exclude_fields = config.analyzer.exclude_fields
        self.max_projection_steps = config.projector.max_projection_steps
        self._adjacency_min_touching = config.analyzer.adjacency_min_touching_boundary_pixels

    def extract(self, ds: xr.Dataset, z_level: int = None) -> pd.DataFrame:
        """Extract geometric and statistical properties from all labeled cells.
        
        Computes per-cell statistics including centroids (geometric, mass-weighted,
        max reflectivity, projected), area, and multi-variable radar statistics.
        Output is a Pandas DataFrame suitable for machine learning, statistical
        analysis, or database insertion (one row = one cell).
        
        Parameters
        ----------
        ds : xr.Dataset
            2D segmented xarray.Dataset with:
            - Dimensions: (y, x)
            - Data variables: cell_labels, reflectivity, velocity (optional), etc.
            - Coordinates: x, y (pixel coordinates)
            - Optional coordinates: lat, lon (geographic)
            - Attributes: z_level_m (altitude), time, radar metadata
        
        z_level : int, optional
            Unused; kept for API compatibility with older versions.
        
        Returns
        -------
        pd.DataFrame
            One row per cell (cells with label > 0). Columns include:
            
            - **Cell Identity**:
              - `cell_label` : int, unique cell ID
            
            - **Geometric Centroids** (all in both pixel and geographic coords):
              - `cell_centroid_geom_{x,y,lat,lon}` : Geometric centroid
              - `cell_centroid_mass_{x,y,lat,lon}` : Reflectivity-weighted
              - `cell_centroid_maxdbz_{x,y,lat,lon}` : Max reflectivity location
              - `cell_centroid_projection_0_{x,y,lat,lon}` to `_4_...` : Projections
            
            - **Size**:
              - `cell_area_sqkm` : Cell area in square kilometers
              - `cell_area_npixels` : Number of grid points
            
            - **Radar Statistics** (per variable: reflectivity, velocity, etc.):
              - `radar_<variable>_mean`, `_std`, `_min`, `_max`, `_median` : Aggregate stats
              - `radar_<variable>_p25`, `_p75` : Percentiles
            
            - **Motion** (if heading vectors present):
              - `cell_heading_<stat>` : Direction/speed statistics
            
            - **Metadata**:
              - `time` : Timestamp of radar scan
              - `z_level_m` : Altitude of this slice
            
        Raises
        ------
        ValueError
            If cell_labels variable is not present in dataset.
        
        Notes
        -----
        - Processing time: 100-500 ms per frame (depends on cell count)
        - Returns empty DataFrame if no cells found (all labels = 0)
        - Only variables in radar_variables config AND present in dataset
          are analyzed (whitelist approach)
        - All centroid coordinates are in both pixel (x, y) and geographic
          (latitude, longitude) systems for flexibility
        - Suitable for direct insertion into SQL database via to_sql()
        
        Examples
        --------
        >>> analyzer = RadarCellAnalyzer(config)
        >>> df = analyzer.extract(ds_segmented)
        >>> print(f"Found {len(df)} cells")
        >>> print(df[['cell_label', 'cell_area_sqkm', 'radar_reflectivity_mean']])
        >>> df.to_sql('cells', conn, if_exists='append')  # Database storage
        """
        # Get labels variable name from config
        labels_name = self.config.global_.var_names.cell_labels

        # Extract reflectivity (already 2D)
        refl = ds[self.reflectivity_field].values
        label_array = ds[labels_name].values
        pixel_area_km2 = self._pixel_area_km2(ds)

        # Get lat/lon grids
        lat_grid, lon_grid = self._get_lat_lon_grids(ds)
        data_vars = self._get_valid_data_vars(ds)

        # Extract properties for each cell
        results = []
        for region in regionprops(label_array.astype(np.int32), intensity_image=refl):
            if region.label == 0:
                continue

            props = self._extract_region_props(
                region, label_array, refl, lat_grid, lon_grid,
                ds, data_vars, pixel_area_km2
            )
            results.append(props)

        df = pd.DataFrame(results)
        if df.empty:
            # Ensure required columns exist even if empty to satisfy contracts
            return pd.DataFrame(
                columns=[
                    "cell_label",
                    "cell_area_sqkm",
                    "time",
                    "time_volume_start",
                    "cell_centroid_mass_lat",
                    "cell_centroid_mass_lon",
                    "radar_reflectivity_max",
                    "radar_differential_reflectivity_max",
                    "area_40dbz_km2",
                ]
            )
        return df

    def extract_adjacency(self, ds: xr.Dataset) -> pd.DataFrame:
        """Extract direct same-scan cell adjacency pairs from the label grid.

        Adjacency definition: two positive labels are adjacent if they touch
        along a shared boundary with at least N touching boundary pixel-edges,
        where N is config-driven (`analyzer.adjacency_min_touching_boundary_pixels`).
        """
        labels_name = self.config.global_.var_names.cell_labels
        if labels_name not in ds.data_vars:
            raise ValueError(f"Missing required labels variable '{labels_name}' for adjacency extraction")
        if "time" not in ds.coords:
            raise ValueError("Missing required coordinate 'time' for adjacency extraction")

        labels = ds[labels_name].values
        if labels.ndim != 2:
            raise ValueError(f"Expected 2D labels array for adjacency extraction, got shape={labels.shape}")

        scan_time = str(ds.time.values)
        adjacency = self._compute_boundary_adjacency(
            labels=labels, min_touching_pixels=int(self._adjacency_min_touching),
        )

        if adjacency.empty:
            return pd.DataFrame(
                columns=["time", "cell_label_a", "cell_label_b", "touching_boundary_pixels"]
            )

        adjacency.insert(0, "time", scan_time)
        return adjacency

    @staticmethod
    def _compute_boundary_adjacency(labels: np.ndarray, min_touching_pixels: int) -> pd.DataFrame:
        """Compute direct boundary adjacency between positive labels.

        Counts touching boundary pixel-edges using 4-neighborhood comparisons
        (right and down). Stores each unordered pair once with canonical ordering
        (a < b).
        """
        if min_touching_pixels < 1:
            raise ValueError(f"min_touching_pixels must be >= 1, got {min_touching_pixels}")

        labels = labels.astype(np.int64, copy=False)
        counts: dict[tuple[int, int], int] = {}

        # Horizontal boundaries (x -> x+1)
        left = labels[:, :-1]
        right = labels[:, 1:]
        mask_h = (left > 0) & (right > 0) & (left != right)
        if np.any(mask_h):
            a = left[mask_h]
            b = right[mask_h]
            lo = np.minimum(a, b)
            hi = np.maximum(a, b)
            for aa, bb in zip(lo.tolist(), hi.tolist(), strict=True):
                key = (int(aa), int(bb))
                counts[key] = counts.get(key, 0) + 1

        # Vertical boundaries (y -> y+1)
        up = labels[:-1, :]
        down = labels[1:, :]
        mask_v = (up > 0) & (down > 0) & (up != down)
        if np.any(mask_v):
            a = up[mask_v]
            b = down[mask_v]
            lo = np.minimum(a, b)
            hi = np.maximum(a, b)
            for aa, bb in zip(lo.tolist(), hi.tolist(), strict=True):
                key = (int(aa), int(bb))
                counts[key] = counts.get(key, 0) + 1

        rows = [
            {
                "cell_label_a": k[0],
                "cell_label_b": k[1],
                "touching_boundary_pixels": v,
            }
            for k, v in counts.items()
            if v >= min_touching_pixels
        ]
        if not rows:
            return pd.DataFrame(columns=["cell_label_a", "cell_label_b", "touching_boundary_pixels"])

        df = pd.DataFrame(rows)
        df = df.sort_values(["cell_label_a", "cell_label_b"]).reset_index(drop=True)
        return df

    def _pixel_area_km2(self, ds):
        """Compute pixel area in km2."""
        dx = float(np.abs(ds.x[1] - ds.x[0]))
        dy = float(np.abs(ds.y[1] - ds.y[0]))
        return (dx * dy) / 1e6

    @staticmethod
    def _normalize_time_scalar(time_val):
        tv = time_val
        while isinstance(tv, np.ndarray) and tv.size == 1:
            tv = tv.reshape(-1)[0]
        if isinstance(tv, np.ndarray):
            tv = tv.reshape(-1)[0]
        if hasattr(tv, "item"):
            try:
                tv = tv.item()
            except Exception:
                pass
        if getattr(type(tv), "__module__", "").startswith("cftime"):
            from datetime import datetime, timezone
            tv = datetime(
                int(tv.year),
                int(tv.month),
                int(tv.day),
                int(tv.hour),
                int(tv.minute),
                int(tv.second),
                int(getattr(tv, "microsecond", 0) or 0),
                tzinfo=timezone.utc,
            )
        return tv

    def _get_lat_lon_grids(self, ds):
        """Get lat/lon grids from dataset.
        
        Returns lat/lon grids if available, otherwise returns placeholder grids
        of zeros (valid for in-memory analysis, invalid for geographic output).
        """
        if "lat" in ds.coords and "lon" in ds.coords:
            return ds["lat"].values, ds["lon"].values
        elif "lat" in ds.data_vars and "lon" in ds.data_vars:
            return ds["lat"].values, ds["lon"].values
        else:
            # No lat/lon available - use placeholder zeros
            # (This should only happen in testing; production data always has coords)
            lat_grid = np.zeros((len(ds.y), len(ds.x)))
            lon_grid = np.zeros((len(ds.y), len(ds.x)))
            return lat_grid, lon_grid

    def _get_valid_data_vars(self, ds):
        """Get list of radar variables suitable for statistics analysis.
        
        Uses whitelist approach: only variables in radar_variables config
        that are actually present in the dataset are analyzed.
        """
        available_vars = []
        for var in self.radar_variables:
            if var in ds.data_vars and var not in self.exclude_fields:
                # Check if it's 2D (y, x) or 3D (z, y, x)
                if ds[var].dims[-2:] == ("y", "x") or ds[var].dims[-3:] == ("z", "y", "x"):
                    available_vars.append(var)
        return available_vars

    def _compute_geometric_centroid(self, mask, lat_grid=None, lon_grid=None):
        """Compute geometric centroid (center of mass) of cell region.
        
        Parameters
        ----------
        mask : np.ndarray
            Boolean mask of cell region
        lat_grid : np.ndarray, optional
            Latitude grid for geographic coordinates
        lon_grid : np.ndarray, optional
            Longitude grid for geographic coordinates
            
        Returns
        -------
        dict
            Centroid coordinates: centroid_x, centroid_y, centroid_lat, centroid_lon
        """
        centroid_y, centroid_x = center_of_mass(mask.astype(float))
        centroid_x = int(np.round(centroid_x))
        centroid_y = int(np.round(centroid_y))
        
        result = {
            "centroid_x": centroid_x,
            "centroid_y": centroid_y,
        }
        
        if lat_grid is not None and lon_grid is not None:
            lat, lon = self.get_lat_lon(centroid_x, centroid_y, lat_grid, lon_grid)
            result["centroid_lat"] = float(lat)
            result["centroid_lon"] = float(lon)
        
        return result

    def _extract_field_values(self, ds, var, mask):
        """Extract field values at mask locations for 2D data."""
        data = ds[var].values
        return data[mask]

    def _extract_region_props(self, region, label_array, refl, lat_grid, lon_grid,
                              ds, data_vars, pixel_area_km2):
        """Extract properties for a single cell region.
        
        Naming convention - ALL centroids stored in both XY and lat/lon:
        - cell_centroid_<type>_x, cell_centroid_<type>_y: Pixel coordinates
        - cell_centroid_<type>_lat, cell_centroid_<type>_lon: Geographic coordinates
        
        Centroid types:
        - geom: Geometric centroid (center of mass of binary mask)
        - mass: Mass-weighted centroid (reflectivity weighted)
        - maxdbz: Maximum reflectivity centroid
        - registration_<idx>: Registration/projection centroids (index 0 = registration)
        - projection_<idx>: Forward projection centroids (indices 1+)
        
        Other naming:
        - cell_heading_<stat>: Heading vector statistics within cell
        - radar_<variable>_<stat>: Radar variable statistics
        """
        mask = label_array == region.label
        region_coords = region.coords
        region_values = refl[tuple(region_coords.T)]
        max_idx = np.argmax(region_values)
        max_coord = region_coords[max_idx]

        # Get scan start time
        scan_time = ""
        if "time" in ds.coords:
            tv = self._normalize_time_scalar(ds.time.values)
            scan_time = pd.Timestamp(tv).isoformat()

        # === GEOMETRIC CENTROID (center of mass of binary mask) ===
        geom_props = self._compute_geometric_centroid(mask, lat_grid, lon_grid)
        
        # === MAX REFLECTIVITY CENTROID ===
        centroid_maxdbz_y = int(np.round(max_coord[0]))
        centroid_maxdbz_x = int(np.round(max_coord[1]))
        lat_maxdbz = float(lat_grid[tuple(max_coord)])
        lon_maxdbz = float(lon_grid[tuple(max_coord)])

        # === MASS-WEIGHTED CENTROID (reflectivity weighted) ===
        refl_cell = refl[mask]
        if len(refl_cell) > 0 and np.any(np.isfinite(refl_cell)):
            y_indices, x_indices = np.where(mask)
            valid_mask = np.isfinite(refl_cell)
            if np.any(valid_mask):
                centroid_mass_y = int(np.round(np.average(y_indices[valid_mask], weights=refl_cell[valid_mask])))
                centroid_mass_x = int(np.round(np.average(x_indices[valid_mask], weights=refl_cell[valid_mask])))
            else:
                centroid_mass_y, centroid_mass_x = int(np.round(geom_props["centroid_y"])), int(np.round(geom_props["centroid_x"]))
        else:
            centroid_mass_y, centroid_mass_x = int(np.round(geom_props["centroid_y"])), int(np.round(geom_props["centroid_x"]))

        lat_mass, lon_mass = self.get_lat_lon(centroid_mass_x, centroid_mass_y, lat_grid, lon_grid)

        # Build properties dict with ALL centroids in both XY and lat/lon
        props = {
            "time_volume_start": scan_time,  # Start of radar volume scan
            "time": scan_time,  # Analysis time (same as time_volume_start)
            "time_volume_end": None,  # Will be populated when available
            "cell_label": int(region.label),
            "cell_area_sqkm": float(region.area * pixel_area_km2),
            "area_40dbz_km2": float(np.sum(refl[mask] > 40.0) * pixel_area_km2),
            # Geometric centroid - both XY and lat/lon
            "cell_centroid_geom_x": geom_props["centroid_x"],
            "cell_centroid_geom_y": geom_props["centroid_y"],
            "cell_centroid_geom_lat": geom_props["centroid_lat"],
            "cell_centroid_geom_lon": geom_props["centroid_lon"],
            # Max reflectivity centroid - both XY and lat/lon
            "cell_centroid_maxdbz_x": centroid_maxdbz_x,
            "cell_centroid_maxdbz_y": centroid_maxdbz_y,
            "cell_centroid_maxdbz_lat": lat_maxdbz,
            "cell_centroid_maxdbz_lon": lon_maxdbz,
            # Mass-weighted centroid - both XY and lat/lon
            "cell_centroid_mass_x": centroid_mass_x,
            "cell_centroid_mass_y": centroid_mass_y,
            "cell_centroid_mass_lat": float(lat_mass),
            "cell_centroid_mass_lon": float(lon_mass),
        }

        # === HEADING VECTOR STATISTICS ===
        if "heading_x" in ds.data_vars and "heading_y" in ds.data_vars:
            try:
                heading_x_vals = self._extract_field_values(ds, "heading_x", mask)
                heading_y_vals = self._extract_field_values(ds, "heading_y", mask)
                if heading_x_vals.size > 0 and heading_y_vals.size > 0:
                    props["cell_heading_x_mean"] = float(np.nanmean(heading_x_vals))
                    props["cell_heading_y_mean"] = float(np.nanmean(heading_y_vals))
            except Exception as e:
                logger.debug("Could not extract heading vectors: %s", e)

        # === PROJECTION CENTROIDS (registration + forward projections) ===
        # Store ALL in both XY and lat/lon coordinates
        if "cell_projections" in ds.data_vars:
            try:
                projections = ds["cell_projections"].values
                if projections.ndim == 3:  # (offset, y, x)
                    projection_centroids = []
                    
                    # Extract centroids for each projection step
                    for step_idx in range(min(projections.shape[0], self.max_projection_steps + 1)):
                        proj_mask = projections[step_idx] == region.label
                        if np.any(proj_mask):
                            # Use reusable centroid function (already has lat/lon)
                            proj_centroid = self._compute_geometric_centroid(proj_mask, lat_grid, lon_grid)
                            projection_centroids.append(proj_centroid)
                        else:
                            projection_centroids.append(None)
                    
                    # Store each centroid in both XY and lat/lon
                    if projection_centroids:
                        # Index 0 = Registration centroid (projection from previous to current frame)
                        if projection_centroids[0] is not None:
                            reg_cent = projection_centroids[0]
                            props["cell_centroid_registration_x"] = reg_cent["centroid_x"]
                            props["cell_centroid_registration_y"] = reg_cent["centroid_y"]
                            if "centroid_lat" in reg_cent:
                                props["cell_centroid_registration_lat"] = reg_cent["centroid_lat"]
                                props["cell_centroid_registration_lon"] = reg_cent["centroid_lon"]
                        
                        # Indices 1+ = Forward projection centroids
                        for proj_idx, proj_cent in enumerate(projection_centroids[1:], start=1):
                            if proj_cent is not None:
                                props[f"cell_centroid_projection{proj_idx}_x"] = proj_cent["centroid_x"]
                                props[f"cell_centroid_projection{proj_idx}_y"] = proj_cent["centroid_y"]
                                if "centroid_lat" in proj_cent:
                                    props[f"cell_centroid_projection{proj_idx}_lat"] = proj_cent["centroid_lat"]
                                    props[f"cell_centroid_projection{proj_idx}_lon"] = proj_cent["centroid_lon"]
                        
                        # Also store full projection centroids as JSON for compact storage
                        props["cell_projection_centroids_json"] = json.dumps([
                            {k: v for k, v in c.items() if c and not (isinstance(v, float) and np.isnan(v))} if c else None
                            for c in projection_centroids
                        ])
            except Exception as e:
                logger.debug("Could not extract projection centroids: %s", e)

        # Add statistics for each valid data variable (2D) with radar_ prefix
        for var in data_vars:
            try:
                vals = self._extract_field_values(ds, var, mask)
                if vals.size > 0:
                    props[f"radar_{var}_mean"] = float(np.nanmean(vals))
                    props[f"radar_{var}_min"] = float(np.nanmin(vals))
                    props[f"radar_{var}_max"] = float(np.nanmax(vals))
            except Exception as e:
                logger.warning("Skipped var '%s': %s", var, e)

        return props

    @staticmethod
    def get_lat_lon(ix, iy, lat_grid, lon_grid):
        """Extract lat–lon coordinates using integer pixel indices.
        
        Parameters
        ----------
        ix : int
            Integer x (column) index
        iy : int
            Integer y (row) index
        lat_grid : np.ndarray
            2D latitude array [y, x]
        lon_grid : np.ndarray
            2D longitude array [y, x]
            
        Returns
        -------
        tuple
            (lat, lon) as floats, or (np.nan, np.nan) if out of bounds or invalid
        """
        H, W = lat_grid.shape
        
        # Validate bounds
        if not (0 <= ix < W and 0 <= iy < H):
            return np.nan, np.nan
        
        # Extract lat/lon
        lat = lat_grid[iy, ix]
        lon = lon_grid[iy, ix]
        
        # Check for masked or fill values
        if not (np.isfinite(lat) and np.isfinite(lon)):
            return np.nan, np.nan
        
        return float(lat), float(lon)



# ---------------------------------------------------------------------------
# BaseModule wrapper — Step 6
# ---------------------------------------------------------------------------

from adapt.modules.base import BaseModule
from adapt.execution.module_registry import registry
from .contracts import assert_analysis_output, assert_cell_adjacency


def _check_cell_stats(df):
    assert_analysis_output(df)

def _check_cell_adjacency(df):
    assert_cell_adjacency(df)


class AnalysisModule(BaseModule):
    """BaseModule wrapper for RadarCellAnalyzer.

    Extracts per-cell statistics (area, reflectivity, motion, centroids)
    from a segmented/projected 2D dataset. Pure compute — no I/O.
    Persistence is the processor's responsibility.

    Context inputs
    --------------
    projected_ds : xr.Dataset
        2D dataset with projections (output of ProjectionModule).
    config : InternalConfig
        Runtime configuration.
    scan_time : datetime
        Radar scan timestamp (from LoadModule).

    Context outputs
    ---------------
    cell_stats : pd.DataFrame
        Per-cell statistics DataFrame.
    cell_adjacency : pd.DataFrame
        Touching-cell pairs DataFrame.
    """

    name = "analysis"
    inputs = ["projected_ds", "config", "scan_time"]
    outputs = ["cell_stats", "cell_adjacency"]
    output_contracts = {"cell_stats": _check_cell_stats, "cell_adjacency": _check_cell_adjacency}

    def __init__(self) -> None:
        self._analyzer = None

    def run(self, context: dict) -> dict:
        config = context["config"]
        ds_2d = context["projected_ds"]

        if self._analyzer is None:
            self._analyzer = RadarCellAnalyzer(config)

        z_level = config.global_.z_level
        df_cells = self._analyzer.extract(ds_2d, z_level=z_level)
        df_adjacency = self._analyzer.extract_adjacency(ds_2d)

        return {"cell_stats": df_cells, "cell_adjacency": df_adjacency}


registry.register(AnalysisModule)
