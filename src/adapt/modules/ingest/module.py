# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Read and regrid NEXRAD Level-II files to Cartesian xarray.Dataset.

This module handles loading raw NEXRAD archive files and transforming them into
regular Cartesian grids using Py-ART. The output is an xarray.Dataset containing
gridded reflectivity and other radar fields, enabling downstream analysis
(segmentation, projection, cell detection).

Key capabilities:
- Reads NEXRAD Level-II (.gz) files
- Regrids to configurable Cartesian grid (default: 41x201x201 m)
- Saves intermediate NetCDF files for data lineage
- Handles errors gracefully (logs and returns None)

Author: Bhupendra Raut
"""

import logging
from pathlib import Path
from typing import TYPE_CHECKING

import pyart
import xarray as xr

if TYPE_CHECKING:
    from adapt.configuration.schemas import InternalConfig

__all__ = ['RadarDataLoader']

logger = logging.getLogger(__name__)

class RadarDataLoader:
    """Load and regrid NEXRAD radar files to Cartesian grid xarray.Dataset.
    
    This class handles the two-stage process of transforming raw NEXRAD data:
    
    1. **Read**: Loads NEXRAD Level-II archive files using Py-ART
    2. **Regrid**: Transforms from polar to Cartesian grid using distance-weighted
       interpolation (configurable algorithm, default: Cressman weighting)
    
    The output is an xarray.Dataset with dimensions (z, y, x) and data variables
    including reflectivity and velocity. Radar metadata (lat/lon/alt) is preserved
    in dataset attributes.
    
    Configuration
    =============
    Expects config dict with two sub-dicts:
    
    - `reader` : dict
        - `file_format` : str, file type (default: "nexrad_archive")
    
    - `regridder` : dict
        - `grid_shape` : tuple, (nz, ny, nx) vertical/horizontal grid points
          (default: (41, 201, 201))
        - `grid_limits` : tuple, ((z_min, z_max), (y_min, y_max), (x_min, x_max))
          in meters from radar center (default: ((0, 20000), (-100000, 100000), (-100000, 100000)))
        - `roi_func` : str, influence radius function (default: "dist_beam" - distance
          from radar ray). Other options: "constant_distance", "dist"
        - `min_radius` : float, minimum radius for influence (meters, default: 1750)
        - `weighting_function` : str, interpolation weighting (default: "cressman")
          Other options: "linear", "nearest", "barnes"
    
    Notes
    -----
    - Not thread-safe: create separate loader instances for concurrent use
    - Regridding takes 5-15 seconds per file (CPU-bound)
    - NetCDF output is always compressed (zlib, level 9)
    - All methods return None on failure (logged, not raised)
    - Radar object is a temporary Py-ART object; only the xarray.Dataset is kept
    
    Examples
    --------
    >>> config = {
    ...     "reader": {"file_format": "nexrad_archive"},
    ...     "regridder": {"grid_shape": (41, 201, 201), "min_radius": 1750.0}
    ... }
    >>> loader = RadarDataLoader(config)
    >>> ds = loader.load_and_regrid("20250305_KLOT.gz", save_netcdf=True, 
    ...                              output_dir="./grids")
    >>> print(ds.data_vars)  # reflectivity, velocity, etc.
    """

    def __init__(self, config: "InternalConfig"):
        """Initialize loader with validated configuration.
        
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
        >>> loader = RadarDataLoader(config)
        """
        self.config = config
        self.file_format = config.reader.file_format
        self.grid_shape = config.regridder.grid_shape
        self.grid_limits = config.regridder.grid_limits
        self.roi_func = config.regridder.roi_func
        self.min_radius = config.regridder.min_radius
        self.weighting_function = config.regridder.weighting_function
        self.save_netcdf = config.regridder.save_netcdf

    def read(self, filepath: Path | str) -> object:
        """Read a NEXRAD archive file into a Py-ART Radar object.
        
        Loads the raw NEXRAD Level-II file (.gz format) and parses it into
        Py-ART's Radar object, which contains polar coordinate reflectivity,
        velocity, and other fields indexed by ray and gate.
        
        Parameters
        ----------
        filepath : Path or str
            Path to the NEXRAD Level-II file. Must exist on disk.
        
        Returns
        -------
        pyart.core.Radar or None
            Py-ART Radar object if successful, None if:
            - File does not exist
            - File format is unsupported
            - Read operation fails
        
        Notes
        -----
        - File existence is validated before attempting read
        - Errors are logged; exception stack trace is included
        - Radar object is large (~100-200 MB) and should be passed immediately
          to regrid() for processing and cleanup
        - Read time: 1-3 seconds per file (mostly decompression)
        
        Examples
        --------
        >>> loader = RadarDataLoader(config)
        >>> radar = loader.read("20250305_KLOT.gz")
        >>> if radar is not None:
        ...     print(f"Loaded {radar.nrays} rays x {radar.ngates} gates")
        """
        filepath = str(filepath)

        if not Path(filepath).exists():
            raise FileNotFoundError(f"Radar file not found: {filepath}")

        if self.file_format != "nexrad_archive":
            raise ValueError(f"Unsupported file format: {self.file_format!r}")

        # Let pyart exceptions (HDF5, IOError, etc.) propagate so the processor
        # can log the real error and skip the file cleanly.
        radar = pyart.io.read_nexrad_archive(filepath)
        logger.debug("Successfully read radar file: %s", filepath)
        return radar

    def regrid(self, radar: object, grid_kwargs: dict = None,
               output_dir: str = None, source_filepath: str = None) -> xr.Dataset | None:
        """Transform a Py-ART Radar object from polar to Cartesian grid.
        
        Performs distance-weighted interpolation to convert irregular polar
        coordinates (typical of radar) to a regular Cartesian grid suitable
        for machine learning and image processing tasks. Radar metadata
        (latitude, longitude, altitude) is preserved in dataset attributes.
        
        Parameters
        ----------
        radar : pyart.core.Radar
            Input Py-ART Radar object (from read() method).
        
        grid_kwargs : dict, optional
            Override regridding parameters. Merged with config defaults.
            Supported keys:
            
            - `grid_shape` : tuple of int
                (nz, ny, nx) grid dimensions
            
            - `grid_limits` : tuple of tuple
                ((z_min, z_max), (y_min, y_max), (x_min, x_max)) in meters
            
            - `roi_func` : str
                Radius of influence function
            
            - `min_radius` : float
                Minimum radius in meters
            
            - `weighting_function` : str
                Interpolation algorithm ("cressman", "barnes", "linear")
        
        output_dir : str, optional
            Directory to save intermediate NetCDF file. If None and
            save_netcdf=True in caller, uses current directory.
        
        source_filepath : str, optional
            Original file path for naming NetCDF output. If provided,
            output file is named {stem}.nc
        
        Returns
        -------
        xr.Dataset or None
            Cartesian grid xarray.Dataset with dimensions (z, y, x) and
            variables (reflectivity, velocity, etc.) if successful.
            Returns None if regridding or NetCDF save fails.
        
        Notes
        -----
        - Regridding is CPU-intensive: 5-15 seconds per file
        - Grid defaults from config are merged with grid_kwargs overrides
        - NetCDF files are compressed (zlib, level 9) for storage efficiency
        - Radar location attributes (latitude, longitude, altitude) are added
          to dataset.attrs for metadata tracking
        - Fails gracefully: logs exception and returns None
        
        Examples
        --------
        >>> loader = RadarDataLoader(config)
        >>> radar = loader.read("20250305_KLOT.gz")
        >>> ds = loader.regrid(
        ...     radar,
        ...     grid_kwargs={"grid_shape": (50, 250, 250)},
        ...     output_dir="./grids",
        ...     source_filepath="20250305_KLOT.gz"
        ... )
        >>> print(ds.reflectivity.shape)  # (50, 250, 250)
        """

        # Merge default regridder config with overrides
        final_grid_kwargs = {
            "grid_shape": self.grid_shape,
            "grid_limits": self.grid_limits,
            "roi_func": self.roi_func,
            "min_radius": self.min_radius,
            "weighting_function": self.weighting_function,
        }
        if grid_kwargs:
            final_grid_kwargs.update(grid_kwargs)

        # Let regridding failures propagate — the processor handles them.
        grid = pyart.map.grid_from_radars(radar, **final_grid_kwargs)
        ds = grid.to_xarray()
        logger.debug("Success: regrid to xarray.Dataset")

        ds.attrs['radar_latitude'] = float(radar.latitude['data'][0])
        ds.attrs['radar_longitude'] = float(radar.longitude['data'][0])
        ds.attrs['radar_altitude'] = float(radar.altitude['data'][0])

        # NetCDF save is best-effort: failure is logged but does not abort.
        self._write_netcdf(ds, output_dir, source_filepath)
        return ds


    def _write_netcdf(self, ds, output_dir, source_filepath):
        """Internal writer for netcdf output."""
        try:
            if output_dir is None:
                output_dir = "."

            output_dir_path = Path(output_dir)
            output_dir_path.mkdir(parents=True, exist_ok=True)

            nc_filename = Path(source_filepath).stem + ".nc"

            nc_path = output_dir_path / nc_filename

            encoding = {var: {"zlib": True, "complevel": 9} for var in ds.data_vars}
            ds.to_netcdf(nc_path, encoding=encoding, compute=True)

            logger.info("Saved regridded NetCDF: %s", nc_path)

        except Exception as e:
            logger.warning("Failed to save NetCDF: %s", e)



    def load_and_regrid(self, filepath: Path | str, grid_kwargs: dict = None,
                       save_netcdf: bool = True, output_dir: str = None) -> xr.Dataset | None:
        """Read and regrid a NEXRAD file in one call (convenience method).
        
        Combines read() and regrid() operations for simpler usage when
        both steps are needed in sequence. This is the primary entry point
        for most use cases.
        
        Parameters
        ----------
        filepath : Path or str
            Path to NEXRAD Level-II (.gz) file.
        
        grid_kwargs : dict, optional
            Regridding parameter overrides. Passed to regrid().
        
        save_netcdf : bool, default True
            Whether to save the intermediate regridded NetCDF file.
            If True, uses output_dir parameter.
        
        output_dir : str, optional
            Directory for NetCDF output. Only used if save_netcdf=True.
            If None, uses current directory.
        
        Returns
        -------
        xr.Dataset or None
            Cartesian grid xarray.Dataset if successful, None if:
            - File does not exist or cannot be read
            - Regridding fails
            - NetCDF save fails (if save_netcdf=True)
        
        Notes
        -----
        - Preferred method over separate read() + regrid() calls
        - Two failure points: read stage and regrid stage
        - NetCDF save is optional; set save_netcdf=False for memory-only
          processing (avoids disk I/O)
        - Returns same xarray.Dataset regardless of save_netcdf setting
        
        Examples
        --------
        >>> loader = RadarDataLoader(config)
        >>> ds = loader.load_and_regrid(
        ...     "20250305_KLOT.gz",
        ...     save_netcdf=True,
        ...     output_dir="./grids"
        ... )
        >>> if ds is not None:
        ...     print(f"Grid shape: {ds.reflectivity.shape}")
        ...     cells = segment_cells(ds)  # Downstream processing
        """
        radar = self.read(filepath)
        ds = self.regrid(
            radar,
            grid_kwargs=grid_kwargs,
            output_dir=output_dir if save_netcdf else None,
            source_filepath=filepath,
        )
        return ds


if __name__ == "__main__":
    print("RadarDataLoader loaded. Use: loader = RadarDataLoader(config)")



# ---------------------------------------------------------------------------
# BaseModule wrapper — Step 6
# ---------------------------------------------------------------------------

from datetime import UTC
from datetime import datetime as _dt

import numpy as np
import xarray as _xr

from adapt.configuration.schemas.directories import get_netcdf_path
from adapt.execution.module_registry import registry
from adapt.modules.base import BaseModule

from .contracts import assert_gridded


def _check_grid_ds_2d(ds):
    assert_gridded(ds, "reflectivity")


class LoadModule(BaseModule):
    """BaseModule wrapper for RadarDataLoader.

    Reads a NEXRAD Level-II file, regrids it to Cartesian coordinates,
    and extracts a 2D horizontal slice at the configured z-level.

    Context inputs
    --------------
    nexrad_file : str
        Path to the NEXRAD Level-II file.
    config : InternalConfig
        Runtime configuration (lazy-initialises the loader on first call).
    output_dirs : dict
        Output directory mapping (used for saving intermediate NetCDF).

    Context outputs
    ---------------
    grid_ds : xr.Dataset
        Full 3D Cartesian xarray Dataset.
    grid_ds_2d : xr.Dataset
        2D slice at configured z-level.
    scan_time : datetime
        Radar volume scan time parsed from the filename.
    """

    name = "ingest"
    inputs = ["nexrad_file", "config"]
    outputs = ["grid_ds", "grid_ds_2d", "scan_time"]
    output_contracts = {"grid_ds_2d": _check_grid_ds_2d}

    def __init__(self) -> None:
        self._loader = None

    def run(self, context: dict) -> dict:
        config = context["config"]
        filepath = context["nexrad_file"]
        output_dirs = context.get("output_dirs", {})

        if self._loader is None:
            self._loader = RadarDataLoader(config)

        radar = config.downloader.radar
        nc_filename = Path(filepath).stem
        scan_time = _dt.now(UTC)
        try:
            parts = nc_filename.split("_")
            dt_str = parts[0][-8:] + parts[1]
            scan_time = _dt.strptime(dt_str, "%Y%m%d%H%M%S")
        except Exception:
            pass

        nc_path = get_netcdf_path(output_dirs, radar, nc_filename, scan_time=scan_time)
        output_dir = str(nc_path.parent) if nc_path else None

        ds = self._loader.load_and_regrid(
            filepath,
            save_netcdf=config.regridder.save_netcdf,
            output_dir=output_dir,
        )

        if ds is None:
            raise RuntimeError(f"Ingest failed: load_and_regrid returned None for {filepath}")

        # Extract 2D slice at configured z-level
        z_level = config.global_.z_level
        z_name = config.global_.coord_names.z
        time_name = config.global_.coord_names.time
        z_idx = int(np.argmin(np.abs(ds[z_name].values - z_level)))

        ds_2d = _xr.Dataset()
        for var_name in ds.data_vars:
            var = ds[var_name]
            if time_name in var.dims and z_name in var.dims:
                ds_2d[var_name] = var.isel({time_name: 0, z_name: z_idx})
            else:
                ds_2d[var_name] = var
        for coord in ds.coords:
            if coord not in ds_2d.coords:
                ds_2d = ds_2d.assign_coords({coord: ds[coord]})
        ds_2d.attrs.update(ds.attrs)

        return {"grid_ds": ds, "grid_ds_2d": ds_2d, "scan_time": scan_time}


registry.register(LoadModule)
