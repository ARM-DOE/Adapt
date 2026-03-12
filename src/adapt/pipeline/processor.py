"""Radar data processing pipeline.

Processes radar files through segmentation, projection, and analysis stages.
Reads Level-II data or gridded NetCDF, segments cells, projects motion,
computes statistics, and persists results to NetCDF and SQLite.
"""

import logging
from pathlib import Path
from typing import Optional, Dict, List, TYPE_CHECKING
import threading
import queue
from datetime import datetime, timezone


import pandas as pd
import numpy as np
import xarray as xr


from adapt.modules.load.module import RadarDataLoader
from adapt.modules.detect.module import RadarCellSegmenter
from adapt.modules.analysis.module import RadarCellAnalyzer
from adapt.modules.projection.module import RadarCellProjector
from adapt.modules.utils import compute_all_cell_centroids
from adapt.contracts import (
    ContractViolation,
    assert_gridded,
    assert_segmented,
    assert_projected,
    assert_analysis_output,
)
from adapt.core import DataRepository, ProductType

if TYPE_CHECKING:
    from adapt.schemas import InternalConfig

__all__ = ['RadarProcessor']

logger = logging.getLogger(__name__)


class RadarProcessor(threading.Thread):
    """Processes radar files through the complete scientific analysis pipeline.

    This worker thread runs in the background, receiving filepaths from the
    downloader queue and performing all scientific analysis: loading, regridding,
    segmentation, motion projection, and cell statistics extraction.

    **Processing Pipeline:**

    For each file, the processor performs (in order):

    1. **Load & Regrid**: Reads NEXRAD Level-II data or cached gridded NetCDF,
       converts to Cartesian grid using Cressman weighting and ARM Py-ART.

    2. **Segmentation**: Identifies storm cells using reflectivity threshold
       and morphological filtering. Outputs cell_labels array.

    3. **Motion Projection** (frame 2+): Computes optical flow between current
       and previous frame, projects cells forward by 1-5 timesteps. First frame
       has no projections (only segmentation).

    4. **Cell Analysis**: Extracts properties from each cell:
       - Geometry: area, centroid (mass-weighted and geometric)
       - Reflectivity: max, mean, 95th percentile
       - Dimensions: length, width, aspect ratio
       Stores in SQLite database for later analysis.

    5. **Persistence**: Writes segmentation + projections to NetCDF
       (analysis/{radar}_analysis_*_segmentation.nc) and queues file
       for visualization.

    **Output Files:**

    - **Gridded NetCDF**: gridded/{radar}_*_gridded.nc (regridded data)
    - **Analysis NetCDF**: analysis/{radar}_*_analysis_segmentation.nc
      (segmentation + projections + flow fields)
    - **SQLite Database**: analysis/{radar}_cells_statistics.db
      (cell properties for all files)

    **Database Schema:**

    SQLite table `cells` (one row per detected cell):
    - Temporal: time, time_volume_start, time_volume_end
    - Identification: cell_label, radar, file_id
    - Geometry: cell_area_sqkm, centroids (lat/lon and x/y)
    - Reflectivity: max_dbz, mean_dbz, percentile_95
    - Dimensions: length_km, width_km, aspect_ratio
    - Motion: heading_x, heading_y, projection_distances (1-5 steps)

    **Frame History:**

    Maintains a 2-frame rolling window to compute optical flow. This enables
    motion estimation between frame N and frame N+1. Frame 1 has only
    segmentation; projections begin at frame 2.

    **Thread Safety:**

    Uses SQLite WAL mode for concurrent read access. Main thread can query
    results while processor is still running.

    Example usage (typically called by orchestrator)::

        processor = RadarProcessor(
            input_queue=downloader_queue,
            config=config,
            output_queue=plotter_queue
        )
        processor.start()
        ...
        processor.stop()
        df = processor.get_results()
    """

    def __init__(self, input_queue: queue.Queue, config: "InternalConfig",
                 output_dirs: Dict[str, Path],
                 file_tracker=None,
                 repository: Optional[DataRepository] = None,
                 name: str = "RadarProcessor"):
        """Initialize processor with validated configuration.

        Parameters
        ----------
        input_queue : queue.Queue
            Queue of filepaths from downloader thread. Processor pops filepaths
            and processes them. None signals shutdown.

        config : InternalConfig
            Fully validated runtime configuration.

        output_dirs : dict
            Output directory paths from initialization:
            - nexrad: NEXRAD Level-II files
            - gridded: Regridded NetCDF files
            - analysis: Analysis NetCDF files
            - plots: Visualization PNG files
            - logs: Log files

        file_tracker : FileProcessingTracker, optional
            Optional file processing tracker to skip already completed files.

        repository : DataRepository
            Data repository for artifact management and database operations.
            **Required** - initialized by orchestrator.

        name : str, optional
            Thread name for logging (default: "RadarProcessor").

        Raises
        ------
        ValueError
            If DataRepository is not provided.
        """

        super().__init__(daemon=True, name=name)

        self.input_queue = input_queue
        self.config = config
        self.output_dirs = output_dirs
        self.file_tracker = file_tracker
        self.repository = repository
        self._stop_event = threading.Event()

        # Initialize processing modules
        self.loader = RadarDataLoader(self.config)
        self.segmenter = RadarCellSegmenter(self.config)
        self.analyzer = RadarCellAnalyzer(self.config)
        self.projector = RadarCellProjector(self.config)

        # Keep last N datasets so we can compute flow and projections
        # Uses max_history from config instead of hardcoding
        self.dataset_history = []  # List of (filepath, ds) tuples
        self.max_history = self.config.processor.max_history

        # DataRepository is required for data storage
        if not self.repository:
            raise ValueError(
                "DataRepository is required for processor initialization. "
                "Repository must be initialized by orchestrator."
            )
        
        self.output_lock = threading.Lock()
        self.cells_db_artifact_id: Optional[str] = None
        self.db_initialized = False
        logger.info("Processor initialized with DataRepository for data storage")

    def stop(self):
        """Signal processor to stop gracefully."""
        self._stop_event.set()

    def stopped(self):
        """Check if processor should stop."""
        return self._stop_event.is_set()


    def process_file(self, filepath: str) -> bool:
        """Process single file: load → regrid → segment → project → analyze → save."""
        try:
            # Step 0: Handle dict input and check tracker for completed/plotting
            if isinstance(filepath, dict):
                filepath = filepath["path"]
            
            # Validate file exists before attempting to process
            file_path = Path(filepath)
            
            file_id = Path(filepath).stem
            tracker = self.file_tracker
            if tracker and tracker.should_process(file_id, "analyzed") is False:
                logger.info("Skipping already analyzed: %s", Path(filepath).name)
                return True

            logger.info("Processing: %s", Path(filepath).name)

            # Step 1: Load and regrid
            ds, ds_2d, nc_full_path, scan_time = self._load_and_regrid(filepath)
            reflectivity_var = self.config.global_.var_names.reflectivity
            assert_gridded(ds_2d, reflectivity_var)

            # Step 2: Segment
            ds_2d = self.segmenter.segment(ds_2d)
            labels_name = self.config.global_.var_names.cell_labels
            assert_segmented(ds_2d, labels_name)
            num_cells = int(ds_2d[labels_name].max().item())
            logger.info("Segmented: %d cells", num_cells)

            # Step 3: PROJECT (before analyzer so it has projection info)
            ds_2d = self._compute_projections(ds_2d, filepath)
            
            # Contract: if 2+ frames, projections should exist
            if len(self.dataset_history) >= 2:
                assert_projected(ds_2d, self.config.projector.max_projection_steps)

            # Step 4: Save segmentation NetCDF (repository registers artifact for consumers)
            seg_nc_path = self._save_segmentation_netcdf(ds_2d, filepath, scan_time)

            # Step 5: Analyze and save cell statistics
            result = self._analyze_and_save(ds_2d, filepath, nc_full_path, num_cells, scan_time)
            return result

        except ContractViolation as e:
            logger.critical("💥 CRITICAL: Pipeline contract violated: %s", e)
            logger.critical("This indicates a bug in pipeline logic. Stopping pipeline.")
            self.stop()  # Signal processor to stop
            tracker = self.file_tracker
            if tracker:
                file_id = Path(filepath).stem
                tracker.mark_stage_complete(file_id, "analyzed", error=f"Contract violation: {e}")
            return False

        except Exception as e:
            logger.exception("Error processing %s", filepath)
            tracker = self.file_tracker
            if tracker:
                file_id = Path(filepath).stem
                tracker.mark_stage_complete(file_id, "analyzed", error=str(e))
            return False

    def _load_and_regrid(self, filepath):
        """Load and regrid radar file, return ds, ds_2d, nc_full_path, scan_time."""
        save_netcdf = self.config.regridder.save_netcdf
        nc_full_path = None
        scan_time = None
        
        from adapt.schemas.directories import get_netcdf_path
        from datetime import datetime, timezone
        radar = self.config.downloader.radar
        nc_filename = Path(filepath).stem
        try:
            parts = nc_filename.split('_')
            datetime_str = parts[0][-8:] + parts[1]
            scan_time = datetime.strptime(datetime_str, '%Y%m%d%H%M%S')
        except:
            scan_time = datetime.now(timezone.utc)
        nc_full_path = get_netcdf_path(self.output_dirs, radar, nc_filename, scan_time=scan_time)
        output_dir = str(nc_full_path.parent)
        
        ds = None
        if nc_full_path and nc_full_path.exists() and nc_full_path.stat().st_size > 1000:
            try:
                import pyart
                import warnings
                import os
                import sys

                # Suppress HDF5 diagnostic messages that appear on stderr
                # HDF5 C library prints directly to stderr when checking file accessibility
                # We need to redirect stderr at the file descriptor level
                sys.stderr.flush()
                old_stderr_fd = os.dup(2)
                devnull_fd = os.open(os.devnull, os.O_WRONLY)
                os.dup2(devnull_fd, 2)

                try:
                    with warnings.catch_warnings():
                        warnings.simplefilter("ignore")
                        grid = pyart.io.read_grid(str(nc_full_path))
                        ds = grid.to_xarray()
                finally:
                    # Restore stderr
                    sys.stderr.flush()
                    os.dup2(old_stderr_fd, 2)
                    os.close(devnull_fd)
                    os.close(old_stderr_fd)

                logger.debug("Loaded existing NetCDF: %s", nc_full_path.name)
            except Exception as e:
                logger.warning("Failed to load existing NetCDF %s: %s", nc_full_path.name, e)
                ds = None
        if ds is None:
            ds = self.loader.load_and_regrid(filepath, grid_kwargs=None,
                                            save_netcdf=save_netcdf, output_dir=output_dir)
        ds_2d = self._extract_2d_slice(ds)
        logger.debug(f"Extracted 2D slice at z-level, shape: {ds_2d.dims}")
        return ds, ds_2d, nc_full_path, scan_time

    def _compute_projections(self, ds_2d: xr.Dataset, filepath: str) -> xr.Dataset:
        """Compute optical flow projections if 2+ frames available.
        
        Parameters
        ----------
        ds_2d : xr.Dataset
            Segmented 2D dataset
        filepath : str
            Current file path (for history tracking)
            
        Returns
        -------
        xr.Dataset
            Dataset with projections added (flow_u, flow_v, cell_projections),
            or original ds_2d if insufficient frames
        """
        # Update history with current dataset
        self.dataset_history.append((filepath, ds_2d))
        if len(self.dataset_history) > self.max_history:
            self.dataset_history.pop(0)
        
        # Need 2+ frames for projection
        if len(self.dataset_history) < 2:
            logger.debug("First frame: optical flow not available (need 2+ datasets)")
            return ds_2d
        
        try:
            ds_prev_path, ds_prev = self.dataset_history[-2]
            ds_curr_path, ds_curr = self.dataset_history[-1]
            
            # Projector returns 2D ds with cell_projections, flow_u, flow_v added
            ds_with_proj = self.projector.project([ds_prev, ds_curr])
            if ds_with_proj is not None:
                logger.info(f"Projections computed: {list(ds_with_proj.data_vars)}")
                return ds_with_proj
        except Exception as e:
            logger.error(f"Optical flow projection failed: {e}", exc_info=True)
        
        return ds_2d


    def _analyze_and_save(self, ds_2d, filepath, nc_full_path, num_cells, scan_time):
        """Analyze cells (now has projection info), update tracker, insert into SQLite DB."""
        z_level = self.config.global_.z_level
        df_cells = self.analyzer.extract(ds_2d, z_level=z_level)

        # Contract: analysis output must meet requirements
        assert_analysis_output(df_cells)

        file_id = Path(filepath).stem
        if df_cells.empty:
            logger.debug("No cells detected in: %s", Path(filepath).name)
            return True

        # Ensure time columns are datetime for SQLite TIMESTAMP
        time_cols = ["time_volume_start", "time", "time_volume_end"]
        for col in time_cols:
            if col in df_cells.columns:
                df_cells[col] = pd.to_datetime(df_cells[col], errors='coerce')

        logger.debug("Analyzed: %d cells with projection info", len(df_cells))
        self._log_cell_statistics(df_cells)

        with self.output_lock:
            # NEW: Write to Parquet dataset via new catalog system
            try:
                item_id = self.repository.write_analysis2d_parquet(
                    df=df_cells,
                    scan_time=scan_time or datetime.now(timezone.utc),
                    producer="processor",
                    parent_ids=[],
                    metadata={"num_cells": len(df_cells)}
                )
                logger.debug(f"Registered analysis item: {item_id}")
            except Exception as e:
                logger.error(f"Failed to write analysis parquet: {e}")
                # Fallback to old SQLite method (deprecated)
                if self.cells_db_artifact_id is None:
                    self.cells_db_artifact_id = self.repository.get_or_create_cells_db(
                        scan_time=scan_time or datetime.now(timezone.utc),
                        producer="processor"
                    )
                    self.db_initialized = True

                self.repository.write_sqlite_table(
                    df=df_cells,
                    table_name='cells',
                    artifact_id=self.cells_db_artifact_id
                )
                logger.warning("Fell back to legacy SQLite storage")

        logger.info("Successfully processed: %s (saved %d cells)", Path(filepath).name, len(df_cells))
        tracker = self.file_tracker
        if tracker:
            tracker.mark_stage_complete(file_id, "analyzed", num_cells=len(df_cells))
        return True

    def run(self):
        """Main processor loop (runs in thread).

        Continuously reads filepaths from input_queue, processes each file through
        the complete scientific pipeline, and sends results to output_queue for
        visualization. Exits when None (sentinel) is received.

        Notes
        -----
        Called automatically by thread.start(). Do not call directly.
        """
        logger.info("Processor started, waiting for files...")

        while not self.stopped():
            try:
                # Get next file from queue (block for 1 second)
                try:
                    filepath = self.input_queue.get(timeout=1)
                except queue.Empty:
                    continue

                # Process file (always mark as done even if it fails)
                try:
                    success = self.process_file(filepath)
                except Exception as e:
                    logger.exception("Failed to process file: %s", filepath)
                finally:
                    # Always mark task as done to prevent queue from blocking
                    self.input_queue.task_done()

            except Exception as e:
                logger.exception("Processor error")

        logger.info("Processor stopped")


    def get_results(self) -> pd.DataFrame:
        """Return all processed cell statistics as a DataFrame.

        Thread-safe query from DataRepository containing all cells extracted
        from processed files. Can be called while processor is running.

        Returns
        -------
        pd.DataFrame
            One row per detected cell with columns for time, geometry, reflectivity,
            motion, and projections. Empty DataFrame if no processing completed yet.
        """
        with self.output_lock:
            # Don't query before schema is created
            if not self.db_initialized or not self.cells_db_artifact_id:
                return pd.DataFrame()

            try:
                return self.repository.open_table(self.cells_db_artifact_id, table_name='cells')
            except Exception as e:
                logger.warning("Failed to read from database: %s", e)
                return pd.DataFrame()

    def save_results(self, filepath: str = None):
        """Export all cell statistics to Parquet file via DataRepository.

        Creates a single Parquet file containing all cells from the database.
        Parquet format is efficient for subsequent analysis with Pandas,
        DuckDB, Polars, or other analytical tools.

        Parameters
        ----------
        filepath : str, optional
            Currently unused (for API compatibility). Parquet is stored via
            DataRepository.
        """
        try:
            with self.output_lock:
                if not self.cells_db_artifact_id:
                    logger.warning("No results to export (no artifact created)")
                    return

                df = self.repository.open_table(self.cells_db_artifact_id, table_name='cells')
                if len(df) > 0:
                    self.repository.write_parquet(
                        df=df,
                        product_type=ProductType.CELLS_PARQUET,
                        scan_time=datetime.now(timezone.utc),
                        producer="processor",
                        parent_ids=[self.cells_db_artifact_id],
                        metadata={"row_count": len(df)}
                    )
                    logger.info("Exported %d rows to Parquet via DataRepository", len(df))
                else:
                    logger.warning("No results to export")

        except Exception as e:
            logger.exception("Failed to export results")

    def _log_cell_statistics(self, df_cells: pd.DataFrame):
        """Log detailed cell statistics for each detected cell.

        Parameters
        ----------
        df_cells : pd.DataFrame
            DataFrame with cell properties from analyzer.
        """
        try:
            if df_cells.empty:
                return

            # Extract key statistics from DataFrame
            num_cells = len(df_cells)

            # Build statistics log line
            stats_parts = [f"Cells: {num_cells}"]

            # Area statistics
            if "area_km2" in df_cells.columns:
                area_vals = df_cells["area_km2"].dropna()
                if len(area_vals) > 0:
                    stats_parts.append(
                        f"Area [km2] - min={area_vals.min():.1f}, "
                        f"max={area_vals.max():.1f}, "
                        f"mean={area_vals.mean():.1f}"
                    )

            # Reflectivity statistics
            if "reflectivity_mean" in df_cells.columns:
                refl_mean_vals = df_cells["reflectivity_mean"].dropna()
                if len(refl_mean_vals) > 0:
                    stats_parts.append(
                        f"Refl-mean [dBZ] - min={refl_mean_vals.min():.1f}, "
                        f"max={refl_mean_vals.max():.1f}, "
                        f"mean={refl_mean_vals.mean():.1f}"
                    )

            if "reflectivity_max" in df_cells.columns:
                refl_max_vals = df_cells["reflectivity_max"].dropna()
                if len(refl_max_vals) > 0:
                    stats_parts.append(
                        f"Refl-max [dBZ] - min={refl_max_vals.min():.1f}, "
                        f"max={refl_max_vals.max():.1f}, "
                        f"mean={refl_max_vals.mean():.1f}"
                    )

            # Log summary
            summary_msg = " | ".join(stats_parts)
            logger.info("Cell Statistics: %s", summary_msg)

            # Log individual cell details for small number of cells
            if num_cells <= 5:
                for idx, row in df_cells.iterrows():
                    cell_label = row["cell_label"]
                    area = row["cell_area_sqkm"]
                    refl_mean = row["radar_reflectivity_mean"]
                    refl_max = row["radar_reflectivity_max"]

                    detail_msg = f"Cell #{cell_label}: area={area:.1f} km2, refl_mean={refl_mean:.1f} dBZ, refl_max={refl_max:.1f} dBZ"
                    logger.info("  %s", detail_msg)

        except Exception as e:
            logger.debug("Could not log cell statistics: %s", e)

    def _extract_2d_slice(self, ds: xr.Dataset) -> xr.Dataset:
        """Extract 2D slice at configured z-level.
        
        Parameters
        ----------
        ds : xr.Dataset
            Full 3D dataset from loader
            
        Returns
        -------
        xr.Dataset
            2D dataset with all variables sliced at z-level
        """
        z_level = self.config.global_.z_level
        time_name = self.config.global_.coord_names.time
        z_name = self.config.global_.coord_names.z
        
        # Find z-level index
        z_idx = int(np.argmin(np.abs(ds[z_name].values - z_level)))
        
        # Extract 2D slice for all 3D variables
        ds_2d = xr.Dataset()
        for var_name in ds.data_vars:
            var = ds[var_name]
            # Check if variable has time and z dimensions
            if time_name in var.dims and z_name in var.dims:
                # Slice to 2D
                ds_2d[var_name] = var.isel({time_name: 0, z_name: z_idx})
            else:
                # Keep as-is (already 2D or scalar)
                ds_2d[var_name] = var
        
        # Copy coordinates (only y, x for 2D)
        ds_2d = ds_2d.assign_coords({
            "y": ds.y,
            "x": ds.x,
        })
        
        # Copy attributes
        ds_2d.attrs.update(ds.attrs)
        ds_2d.attrs["z_level_m"] = float(ds[z_name].values[z_idx])
        
        logger.debug(f"Extracted 2D slice at z={ds[z_name].values[z_idx]:.0f}m (index {z_idx})")
        return ds_2d

    def _save_segmentation_netcdf(self, ds: xr.Dataset, filepath: str, scan_time) -> Optional[str]:
        """Save analysis results to NetCDF for visualization.
        """
        try:
            radar = self.config.downloader.radar
            filename_stem = Path(filepath).stem

            # Add metadata
            ds.attrs.update({
                "source": str(filepath),
                "radar": radar,
                "description": "Radar analysis with segmentation and projections"
            })

            if self.repository:
                # Use DataRepository for storage
                artifact_id = self.repository.write_netcdf(
                    ds=ds,
                    product_type=ProductType.ANALYSIS_NC,
                    scan_time=scan_time or datetime.now(timezone.utc),
                    producer="processor",
                    parent_ids=[],
                    metadata={"components": list(ds.data_vars.keys())},
                    filename_stem=filename_stem
                )
                artifact = self.repository.get_artifact(artifact_id)
                seg_nc_path = Path(artifact['file_path'])

                # Log what was saved
                components = list(ds.data_vars.keys())
                logger.info(f"Analysis saved: {seg_nc_path.name} [{', '.join(components)}]")
                return str(seg_nc_path)
            else:
                # Legacy mode: direct file access
                from adapt.schemas.directories import get_analysis_path

                seg_nc_filename = f"{filename_stem}_analysis.nc"
                seg_nc_path = get_analysis_path(
                    self.output_dirs,
                    radar=radar,
                    analysis_type="netcdf",
                    timestamp=scan_time,
                    filename=seg_nc_filename
                )
                seg_nc_path.parent.mkdir(parents=True, exist_ok=True)

                # Save to NetCDF
                ds.to_netcdf(seg_nc_path, mode='w', engine='netcdf4', format='NETCDF4', compute=True)
                ds.close()

                # Log what was saved
                components = list(ds.data_vars.keys())
                logger.info(f"Analysis saved: {seg_nc_path.name} [{', '.join(components)}]")
                return str(seg_nc_path)

        except Exception as e:
            logger.warning(f"Could not save segmentation NetCDF: {e}")
            return None


if __name__ == "__main__":
    # Logging configured by pipeline orchestrator
    pass  # Placeholder for demo code below
    print("Processor classes loaded. Use in pipeline orchestration.")
