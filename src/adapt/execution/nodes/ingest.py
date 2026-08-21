# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

import numpy as np
import xarray as _xr

from adapt.contracts import NetcdfArtifact, check_grid_ds_2d
from adapt.execution.module_registry import registry
from adapt.modules.base import BaseModule
from adapt.modules.ingest.config import IngestConfig
from adapt.modules.ingest.module import RadarDataLoader


class LoadModule(BaseModule):
    """BaseModule wrapper for RadarDataLoader.

    Reads a NEXRAD Level-II file, regrids it to Cartesian coordinates,
    and extracts a 2D horizontal slice at the configured z-level. The node
    builds no paths: the 3D grid is returned in-memory and persisted by the
    router as a ``gridded3d`` object.

    Context inputs
    --------------
    nexrad_file : str
        Path to the NEXRAD Level-II file.
    scan_time : datetime
        Scan time owned by the source boundary (acquisition parse or replay
        source); seeded into the context by the processor. Never re-derived
        here — a missing value raises instead of substituting a clock.
    config : InternalConfig
        Runtime configuration (lazy-initialises the loader on first call).

    Context outputs
    ---------------
    grid_ds : xr.Dataset
        Full 3D Cartesian xarray Dataset.
    grid_ds_2d : xr.Dataset
        2D slice at configured z-level.
    """

    name = "ingest"
    summary = "download + read + regrid radar volumes"
    required_history = 1
    pipeline_phase = 0
    inputs = ["nexrad_file", "ingest_config", "scan_time"]
    outputs = ["grid_ds", "grid_ds_2d"]
    output_contracts = {"grid_ds_2d": check_grid_ds_2d}
    config_class = IngestConfig
    persistence = (
        NetcdfArtifact(
            key="grid_ds",
            product_type="gridded3d",
            producer="ingest",
            description="Regridded 3D Cartesian radar volume",
        ),
    )

    @classmethod
    def build_config(cls, cfg) -> IngestConfig:
        return IngestConfig(
            file_format=cfg.reader.file_format,
            grid_shape=cfg.regridder.grid_shape,
            grid_limits=cfg.regridder.grid_limits,
            roi_func=cfg.regridder.roi_func,
            min_radius=cfg.regridder.min_radius,
            weighting_function=cfg.regridder.weighting_function,
            radar=cfg.downloader.radar,
            z_level=cfg.global_.z_level,
            z_coord=cfg.global_.coord_names.z,
            time_coord=cfg.global_.coord_names.time,
        )

    def __init__(self) -> None:
        self._loader: RadarDataLoader | None = None

    def run(self, context: dict) -> dict:
        config = context["ingest_config"]
        filepath = context["nexrad_file"]

        if self._loader is None:
            self._loader = RadarDataLoader(config)

        if context.get("scan_time") is None:
            raise ValueError(
                f"Ingest requires a scan_time for {filepath!r} and the source "
                "supplied none — refusing to substitute a clock or re-parse the "
                "filename (wall-clock substitution is forbidden)"
            )

        ds = self._loader.load_and_regrid(filepath)
        if ds is None:
            raise RuntimeError(f"Ingest failed: load_and_regrid returned None for {filepath}")

        z_level = config.z_level
        z_name = config.z_coord
        time_name = config.time_coord
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

        return {"grid_ds": ds, "grid_ds_2d": ds_2d}


registry.register(LoadModule)
