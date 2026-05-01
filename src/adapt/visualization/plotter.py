# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Radar segmentation and projection visualization.

Renders reflectivity + cell segmentation + motion projections to PNG.
Supports threaded queue-based processing for pipeline integration.
"""

import logging
import queue
import threading
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING

import matplotlib
import numpy as np
import pandas as pd
import xarray as xr

matplotlib.use('Agg')
import contextlib

import matplotlib.pyplot as plt

try:
    import contextily as ctx
    from pyproj import Transformer
    CONTEXTILY_AVAILABLE = True
except ImportError:
    CONTEXTILY_AVAILABLE = False

if TYPE_CHECKING:
    from adapt.configuration.schemas import InternalConfig
    from adapt.persistence import DataRepository

__all__ = ['RadarPlotter', 'PlotterThread', 'PlotConsumer']

logger = logging.getLogger(__name__)


class RadarPlotter:
    """Generates segmentation and projection visualizations from radar data.

    Renders publication-quality PNG plots showing:

    - **Left Panel**: Reflectivity field with optical flow vectors (motion field)
    - **Right Panel**: Segmented storm cells with projected future positions

    **Reflectivity Display:**

    Uses dBZ color scale (ChaseSpectral colormap). Values below minimum threshold
    masked out. Optional basemap overlay shows geographic context (OpenStreetMap).

    **Cell Segmentation Overlay:**

    Thin black contours outline detected cells in right panel. Cell ID labeling
    can be configured. Cells shown only where reflectivity exceeds minimum threshold.

    **Motion Projections:**

    Colored contours show predicted cell positions at future timesteps (1-5 steps
    ahead). Line style and alpha transparency decrease with projection distance
    (recent projections more opaque, distant projections faint).

    **Configuration:**

    All appearance settings (DPI, figure size, colors, thresholds) controlled via
    `config["visualization"]` section. Enables consistent multi-radar visualization
    without code changes.

    **Output:**

    Saves PNG to `plots/{radar}_{scan_time}_{plot_type}.png` by default.
    Supports custom paths and output formats (PNG, PDF, etc).

    Example usage::

        plotter = RadarPlotter(config=config)
        plot_path = plotter.plot_from_netcdf(
            segmentation_nc="analysis/KDIX_20250305_000310_analysis_segmentation.nc",
            output_path="plots/KDIX_20250305_000310.png"
        )
        print(f"Plot saved to {plot_path}")
    """
    
    def __init__(self, config: "InternalConfig" = None, show_plots: bool = False):
        """Initialize plotter.

        Parameters
        ----------
        config : InternalConfig, optional
            Fully validated runtime configuration.
        show_plots : bool, optional
            If True, display plots (uses interactive backend). Default False
            (Agg backend for headless/file-only output).
        """

        self.config = config
        
        if config:
            # Plot configuration
            self.dpi = config.visualization.dpi
            self.figsize = config.visualization.figsize
            self.output_format = config.visualization.output_format
            
            # Basemap configuration
            self.use_basemap = config.visualization.use_basemap
            self.basemap_alpha = config.visualization.basemap_alpha
            
            # Style configuration
            self.seg_linewidth = config.visualization.seg_linewidth
            self.proj_linewidth = config.visualization.proj_linewidth
            self.proj_alpha = config.visualization.proj_alpha
            self.flow_scale = config.visualization.flow_scale
            self.flow_subsample = config.visualization.flow_subsample
            
            # Reflectivity thresholds
            self.min_refl = config.visualization.min_reflectivity
            self.vmin = config.visualization.refl_vmin
            self.vmax = config.visualization.refl_vmax
        else:
            # Defaults for backward compatibility
            self.dpi = 200
            self.figsize = (20, 10)
            self.output_format = "png"
            self.use_basemap = True
            self.basemap_alpha = 0.6
            self.seg_linewidth = 1.0
            self.proj_linewidth = 0.8
            self.proj_alpha = 0.6
            self.flow_scale = 1.0
            self.flow_subsample = 10
            self.min_refl = 0
            self.vmin = 10
            self.vmax = 50
        
        if self.use_basemap and not CONTEXTILY_AVAILABLE:
            logger.warning("Basemap requested but contextily not installed")
            self.use_basemap = False
        
        logger.info(f"RadarPlotter initialized (format={self.output_format}, dpi={self.dpi})")
    
    def _get_var_name(self, var_key: str, default: str) -> str:
        """Get variable name from config."""
        if self.config:
            if var_key == "reflectivity":
                return self.config.global_.var_names.reflectivity
            elif var_key == "cell_labels":
                return self.config.global_.var_names.cell_labels
        return default
    
    def _get_coord_name(self, coord_key: str, default: str) -> str:
        """Get coordinate name from config."""
        if self.config:
            if coord_key == "x":
                return self.config.global_.coord_names.x
            elif coord_key == "y":
                return self.config.global_.coord_names.y
            elif coord_key == "z":
                return self.config.global_.coord_names.z
            elif coord_key == "time":
                return self.config.global_.coord_names.time
        return default
    
    def _extract_timestamp(self, ds: xr.Dataset) -> datetime:
        """Extract timestamp from dataset."""
        if 'time' not in ds.coords:
            return datetime.now(UTC)
        
        try:
            time_val = ds.coords['time'].values
            if np.ndim(time_val) == 0:
                return pd.Timestamp(time_val).to_pydatetime()
            else:
                return pd.Timestamp(time_val[0]).to_pydatetime()
        except Exception:
            return datetime.now(UTC)
    
    def _get_coordinates_km(self, ds: xr.Dataset) -> tuple[np.ndarray, np.ndarray]:
        """Get x, y coordinates in km."""
        y_name = self._get_coord_name("y", "y")
        x_name = self._get_coord_name("x", "x")
        
        y_coords = ds[y_name].values / 1000  # Convert m to km
        x_coords = ds[x_name].values / 1000
        
        return x_coords, y_coords
    
    def _mask_reflectivity(self, refl: np.ndarray) -> np.ma.MaskedArray:
        """Apply thresholding to reflectivity."""
        refl_float = refl.astype(float)
        return np.ma.masked_where(
            (refl_float < self.min_refl) | np.isnan(refl_float),
            refl_float
        )
    
    def _setup_figure(self) -> tuple[plt.Figure, plt.Axes, plt.Axes]:
        """Create figure with two subplots."""
        fig, (ax1, ax2) = plt.subplots(
            1, 2,
            figsize=self.figsize,
            dpi=self.dpi
        )
        return fig, ax1, ax2
    
    def _get_radar_location(self, ds: xr.Dataset) -> tuple[float, float]:
        """Extract radar lat/lon from dataset."""
        def extract_float(val):
            """Convert various types to Python float scalar."""
            if val is None:
                return 0.0
            if isinstance(val, xr.DataArray):
                # Handle 0-dimensional DataArray
                v = val.values
                if np.ndim(v) == 0:
                    return float(v.item())
                elif len(v) > 0:
                    return float(v[0])
                return 0.0
            if isinstance(val, np.ndarray):
                if np.ndim(val) == 0:
                    return float(val.item())
                elif len(val) > 0:
                    return float(val[0])
                return 0.0
            if hasattr(val, 'item'):
                return float(val.item())
            try:
                return float(val)
            except (TypeError, ValueError):
                return 0.0

        # Try multiple attribute names for latitude
        lat_val = (ds.attrs.get('radar_latitude') or
                   ds.attrs.get('origin_latitude') or
                   ds.coords.get('radar_latitude'))
        lat = extract_float(lat_val)

        # Try multiple attribute names for longitude
        lon_val = (ds.attrs.get('radar_longitude') or
                   ds.attrs.get('origin_longitude') or
                   ds.coords.get('radar_longitude'))
        lon = extract_float(lon_val)

        return lat, lon
    
    def _add_basemap(self, ax: plt.Axes, ds: xr.Dataset, x_coords: np.ndarray, y_coords: np.ndarray) -> None:
        """Add OpenStreetMap basemap to axis."""
        if not self.use_basemap or not CONTEXTILY_AVAILABLE:
            return
        
        try:
            radar_lat, radar_lon = self._get_radar_location(ds)
            
            # Set CRS for azimuthal equidistant (km units)
            crs_str = f"+proj=aeqd +lat_0={radar_lat} +lon_0={radar_lon} +x_0=0 +y_0=0 +datum=WGS84 +units=km"
            
            ax.set_xlim(x_coords.min(), x_coords.max())
            ax.set_ylim(y_coords.min(), y_coords.max())
            
            ctx.add_basemap(
                ax,
                crs=crs_str,
                source=ctx.providers.OpenStreetMap.Mapnik,
                alpha=self.basemap_alpha,
                attribution=False,
                zoom='auto'
            )
        except Exception as e:
            logger.warning(f"Could not add basemap: {e}")
    
    def _plot_reflectivity_field(
        self,
        ax: plt.Axes,
        refl: np.ma.MaskedArray,
        x_coords: np.ndarray,
        y_coords: np.ndarray
    ) -> matplotlib.image.AxesImage:
        """Plot reflectivity pcolormesh."""
        return ax.pcolormesh(
            x_coords,
            y_coords,
            refl,
            cmap='ChaseSpectral',
            vmin=self.vmin,
            vmax=self.vmax,
            shading='auto',
            zorder=1
        )
    
    def _add_colorbar(self, ax: plt.Axes, im: matplotlib.image.AxesImage) -> None:
        """Add colorbar to axis."""
        plt.colorbar(im, ax=ax, label='Reflectivity (dBZ)', fraction=0.046, pad=0.04)
    
    def _plot_heading_yectors(
        self,
        ax: plt.Axes,
        ds: xr.Dataset,
        x_coords: np.ndarray,
        y_coords: np.ndarray
    ) -> bool:
        """Plot optical flow arrows on Panel 1."""
        heading_x_name = self._get_var_name("heading_x", "heading_x")
        heading_y_name = self._get_var_name("heading_y", "heading_y")
        
        if heading_x_name not in ds.data_vars or heading_y_name not in ds.data_vars:
            return False
        
        heading_x = ds[heading_x_name].values
        heading_y = ds[heading_y_name].values
        
        if np.all(np.isnan(heading_x)):
            logger.debug("Optical flow not plotted (all NaN - first frame)")
            return False
        
        # Subsample for clarity
        y_indices = np.arange(0, len(y_coords), self.flow_subsample)
        x_indices = np.arange(0, len(x_coords), self.flow_subsample)
        
        Y_sub = y_coords[y_indices]
        X_sub = x_coords[x_indices]
        U_sub = heading_x[np.ix_(y_indices, x_indices)]
        V_sub = heading_y[np.ix_(y_indices, x_indices)]
        
        X_mesh, Y_mesh = np.meshgrid(X_sub, Y_sub)
        
        ax.quiver(
            X_mesh, Y_mesh,
            U_sub, V_sub,
            color='#333333',
            alpha=0.7,
            scale=self.flow_scale,
            scale_units='xy',
            width=0.002,
            headwidth=3,
            headlength=4,
            zorder=45
        )
        
        logger.info(f"Plotted optical flow field ({len(y_indices)}x{len(x_indices)} vectors, scale={self.flow_scale})")
        return True
    
    def _plot_segmentation_contours(
        self,
        ax: plt.Axes,
        labels: xr.DataArray,
        x_coords: np.ndarray,
        y_coords: np.ndarray
    ) -> None:
        """Plot thin black contours for segmented cells."""
        labels_data = labels.values
        unique_labels = np.unique(labels_data)
        unique_labels = unique_labels[unique_labels > 0]
        
        if len(unique_labels) == 0:
            return
        
        y_grid, x_grid = np.meshgrid(y_coords, x_coords, indexing='ij')
        
        # Plot each cell individually with binary mask for clean contours
        for cell_id in unique_labels:
            cell_mask = (labels_data == cell_id).astype(float)
            ax.contour(
                x_grid, y_grid,
                cell_mask,
                levels=[0.5],
                colors='black',
                linewidths=self.seg_linewidth,
                alpha=0.9,
                zorder=50
            )
    
    def _plot_projection_contours(
        self,
        ax: plt.Axes,
        ds: xr.Dataset,
        x_coords: np.ndarray,
        y_coords: np.ndarray
    ) -> None:
        """Plot thin transparent gray contours for projections."""
        proj_name = self._get_var_name("cell_projections", "cell_projections")
        frame_offset_name = self._get_coord_name("frame_offset", "frame_offset")
        
        if proj_name not in ds.data_vars:
            return
        
        proj_da = ds[proj_name]
        if frame_offset_name not in proj_da.dims:
            return
        
        y_grid, x_grid = np.meshgrid(y_coords, x_coords, indexing='ij')
        
        linestyles = ['dashed', 'dashdot', 'dotted']
        base_width = self.proj_linewidth
        
        num_frames = len(proj_da[frame_offset_name])
        
        # Skip frame_offset=0 (registration), plot future projections
        for proj_idx in range(1, num_frames):
            labels_proj = proj_da.isel({frame_offset_name: proj_idx}).values
            
            if np.all(np.isnan(labels_proj)):
                continue
            
            unique_proj = np.unique(labels_proj)
            unique_proj = unique_proj[unique_proj > 0]
            
            if len(unique_proj) == 0:
                continue
            
            style_idx = (proj_idx - 1) % len(linestyles)
            linewidth = base_width * (1 - 0.1 * (proj_idx - 1))
            
            # Plot each cell individually to avoid matplotlib contour quirks
            for cell_id in unique_proj:
                # Create binary mask for this specific cell
                cell_mask = (labels_proj == cell_id).astype(float)
                
                # Plot single contour at 0.5 level (boundary of cell)
                ax.contour(
                    x_grid, y_grid,
                    cell_mask,
                    levels=[0.5],
                    colors='#555555',
                    linewidths=linewidth,
                    linestyles=linestyles[style_idx],
                    alpha=self.proj_alpha,
                    zorder=40
                )
    
    def _format_axis(
        self,
        ax: plt.Axes,
        title: str,
        timestamp: datetime,
        radar: str
    ) -> None:
        """Format axis with labels and title."""
        ax.set_xlabel('Distance from Radar - X (km)', fontsize=11)
        ax.set_ylabel('Distance from Radar - Y (km)', fontsize=11)
        ax.grid(True, alpha=0.2, linestyle=':', linewidth=0.5)
        
        time_str = timestamp.strftime('%Y-%m-%d %H:%M:%S UTC')
        ax.set_title(
            f'{radar} {title}\n{time_str}',
            fontsize=12,
            fontweight='bold',
            pad=10
        )
    
    def _add_flow_legend(self, ax: plt.Axes) -> None:
        """Add legend for optical flow vectors."""
        from matplotlib.lines import Line2D
        
        legend_elements = [
            Line2D([0], [0], marker='>', color='#333333',
                   linewidth=0, markersize=4, alpha=0.7,
                   label='Flow')
        ]
        ax.legend(
            handles=legend_elements,
            loc='upper right',
            fontsize=10,
            framealpha=0.9
        )
    
    def _save_figure(self, fig: plt.Figure, output_path: Path) -> str:
        """Save figure in configured format."""
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Ensure correct extension
        output_file = output_path.with_suffix(f'.{self.output_format}')
        
        fig.savefig(
            output_file,
            dpi=self.dpi,
            bbox_inches='tight',
            format=self.output_format
        )
        
        plt.close(fig)
        logger.info(f"Plot saved: {output_file}")
        
        return str(output_file)
    
    def plot_reflectivity_with_cells(
        self,
        ds: xr.Dataset,
        frame_offset: int = 0,
        output_path: Path | None = None,
    ) -> str:
        """Generate publication-quality two-panel radar visualization.

        Creates side-by-side plots: (left) reflectivity with optical flow vectors,
        (right) segmented cells with motion projections. Useful for understanding
        cell behavior, motion patterns, and validation of segmentation.

        **Left Panel:**
        - Background: Reflectivity field (dBZ scale, ChaseSpectral colormap)
        - Overlay: Optical flow vectors (motion estimation between frames)
        - Optional: OpenStreetMap basemap for geographic reference
        - Vectors scaled by flow_scale config, subsampled for clarity

        **Right Panel:**
        - Background: Reflectivity (masked to segmented cells only)
        - Overlay (black contours): Current segmentation (frame N)
        - Overlay (gray contours): Projections (frame N+1 through N+5)
        - Projection contours fade with distance (recent more opaque)

        **Data Requirements:**

        Input dataset must contain:
        - reflectivity: 2D array
        - cell_labels: 2D integer array (0 = background, 1+ = cell IDs)
        - heading_x, heading_y: 2D flow vectors (optional, frame 1 has no flow)
        - cell_projections: 3D array [frame_offset, y, x] (optional, frame 1 lacks)

        Parameters
        ----------
        ds : xr.Dataset
            Analysis dataset from RadarProcessor output. Must have reflectivity,
            cell_labels. Optionally has heading_x/heading_y and cell_projections.

        frame_offset : int, optional
            Frame offset for multi-frame plots (reserved for future enhancement).
            Currently unused (default: 0).

        output_path : Path, optional
            Output PNG path. If None, auto-generates in /tmp with timestamp.

        Returns
        -------
        str
            Path to saved PNG file (ready for web display or reports).

        Notes
        -----
        Processing time: 1-3 seconds per frame on typical hardware.
        File size: typically 200-500 KB per PNG at default DPI.
        """
        # Extract metadata
        radar = ds.attrs.get('radar', 'RADAR')
        timestamp = self._extract_timestamp(ds)
        
        # Get reflectivity
        refl_name = self._get_var_name("reflectivity", "reflectivity")
        refl = ds[refl_name].values
        refl_masked = self._mask_reflectivity(refl)
        
        # Get coordinates
        x_coords, y_coords = self._get_coordinates_km(ds)
        
        # Get labels
        labels_name = self._get_var_name("cell_labels", "cell_labels")
        labels = ds[labels_name]
        
        # Create figure
        fig, ax1, ax2 = self._setup_figure()
        
        # ============================================================
        # PANEL 1: Full Reflectivity + Flow Vectors
        # ============================================================
        im1 = self._plot_reflectivity_field(ax1, refl_masked, x_coords, y_coords)
        self._add_colorbar(ax1, im1)
        self._add_basemap(ax1, ds, x_coords, y_coords)
        
        flow_plotted = self._plot_heading_yectors(ax1, ds, x_coords, y_coords)
        
        self._format_axis(ax1, 'Reflectivity + Motion Vectors', timestamp, radar)
        
        if flow_plotted:
            self._add_flow_legend(ax1)
        
        # ============================================================
        # PANEL 2: Segmented Cells + Projections
        # ============================================================
        # Mask reflectivity to show only segmented cells
        labels_mask = labels.values > 0
        refl_segmented = np.ma.masked_where(~labels_mask, refl)
        refl_segmented = np.ma.masked_where(refl_segmented < self.min_refl, refl_segmented)
        
        im2 = self._plot_reflectivity_field(ax2, refl_segmented, x_coords, y_coords)
        self._add_colorbar(ax2, im2)
        self._add_basemap(ax2, ds, x_coords, y_coords)
        
        # Add segmentation contours (thin black lines)
        self._plot_segmentation_contours(ax2, labels, x_coords, y_coords)
        
        # Add projection contours (thin transparent gray lines)
        self._plot_projection_contours(ax2, ds, x_coords, y_coords)
        
        self._format_axis(ax2, 'Segmented Cells + Projections', timestamp, radar)
        
        # ============================================================
        # Save Figure
        # ============================================================
        plt.tight_layout()
        
        if output_path is None:
            output_path = Path(f"/tmp/radar_plot_{timestamp.strftime('%Y%m%d_%H%M%S')}.{self.output_format}")
        
        return self._save_figure(fig, Path(output_path))
    
    def plot_from_netcdf(
        self,
        segmentation_nc: Path,
        output_path: Path | None = None,
    ) -> str:
        """Load analysis NetCDF and generate visualization.

        Waits for file with retries (handles processor write delays).

        Parameters
        ----------
        segmentation_nc : Path
            Path to analysis NetCDF with segmentation and projections.
        output_path : Path, optional
            Output PNG path. Auto-generated if None.

        Returns
        -------
        str
            Path to saved PNG file.
        """
        import time
        
        max_retries = 5
        retry_delay = 0.1
        
        seg_path = Path(segmentation_nc)
        
        # Wait for file to exist
        for attempt in range(max_retries):
            if seg_path.exists() and seg_path.stat().st_size > 0:
                break
            if attempt < max_retries - 1:
                time.sleep(retry_delay * (attempt + 1))
            else:
                raise FileNotFoundError(f"File not found: {segmentation_nc}")
        
        # Try to open NetCDF
        seg_ds = None
        for attempt in range(max_retries):
            try:
                seg_ds = xr.open_dataset(segmentation_nc)
                break
            except (OSError, FileNotFoundError, RuntimeError) as e:
                if attempt < max_retries - 1:
                    time.sleep(retry_delay * (attempt + 1))
                else:
                    raise RuntimeError(f"Failed to open NetCDF: {segmentation_nc}") from e
        
        # Validate required variables
        labels_name = self._get_var_name("cell_labels", "cell_labels")
        refl_name = self._get_var_name("reflectivity", "reflectivity")
        
        if labels_name not in seg_ds.data_vars:
            raise ValueError(f"Missing variable: {labels_name}")
        if refl_name not in seg_ds.data_vars:
            raise ValueError(f"Missing variable: {refl_name}")
        
        try:
            plot_file = self.plot_reflectivity_with_cells(
                ds=seg_ds,
                frame_offset=0,
                output_path=output_path,
            )
        finally:
            seg_ds.close()
        
        return plot_file


class PlotterThread(threading.Thread):
    """Worker thread for generating radar visualizations in the pipeline.

    Monitors a queue of analysis files and generates PNG visualizations
    asynchronously. Decouples visualization (slow) from processor (critical path).
    Enables real-time monitoring of segmentation and projection quality.

    **Input Queue Format:**

    Each item is a dict with:
    - `segmentation_nc`: Path to analysis NetCDF (from processor)
    - `radar`: Radar identifier for output naming
    - `timestamp`: Scan datetime for plot annotation

    **Output:**

    Generates PNG files in output_dirs['plots']:
    `{radar}_{YYYYMMDD_HHMMSS}.png`

    **File Tracking:**

    Updates FileProcessingTracker with plot path and status on completion
    or error. Enables resumable plotting if pipeline restarts.

    **Threading:**

    Runs as daemon thread. Graceful shutdown via stop() signal. Waits for
    file writes to complete before acknowledging (handles slow disks).

    Example usage (typically called by orchestrator)::

        plotter = PlotterThread(
            input_queue=processor_output_queue,
            output_dirs=output_dirs,
            config=config,
            show_plots=False  # Headless mode
        )
        plotter.start()
        ...
        plotter.stop()
        plotter.join(timeout=5)
    """

    def __init__(
        self,
        input_queue: queue.Queue,
        output_dirs: dict,
        config: "InternalConfig" = None,
        file_tracker = None,
        show_plots: bool = False,
        name: str = 'RadarPlotter',
    ):
        """Initialize plotter thread.

        Parameters
        ----------
        input_queue : queue.Queue
            Queue of analysis file paths from processor.
        output_dirs : dict
            Output directory paths for saving plots.
        config : InternalConfig, optional
            Fully validated runtime configuration.
        file_tracker : FileProcessingTracker, optional
            Optional file processing tracker to record plot completion.
        show_plots : bool, optional
            Display plots (default False for headless mode).
        name : str
            Thread name (default: 'RadarPlotter').
        """
        super().__init__(name=name, daemon=True)
        
        self.input_queue = input_queue
        self.output_dirs = output_dirs
        self.config = config
        self.file_tracker = file_tracker
        self.show_plots = show_plots
        
        self.plotter = RadarPlotter(config=config, show_plots=show_plots)
        self.running = True
        
        logger.info(f"{name} initialized")
    
    def run(self):
        """Process files from queue until shutdown signal received.

        Monitors input queue for analysis file paths and generates visualizations.
        Logs errors but continues processing on per-file failures.
        """
        logger.info(f"{self.name} started")
        
        while self.running:
            try:
                item = self.input_queue.get(timeout=1.0)
                
                if item is None:
                    logger.info(f"{self.name} received shutdown signal")
                    break
                
                self._process_item(item)
                
            except queue.Empty:
                continue
            except Exception as e:
                logger.error(f"Error in {self.name}: {e}", exc_info=True)
        
        logger.info(f"{self.name} stopped")
    
    def _process_item(self, item: dict):
        """Process plot item from queue."""
        try:
            seg_nc = item.get('segmentation_nc')
            radar = item.get('radar', 'RADAR')
            timestamp = item.get('timestamp', datetime.now(UTC))
            
            if not seg_nc or not Path(seg_nc).exists():
                logger.warning(f"Segmentation file not found: {seg_nc}")
                return
            
            # Get file_id for tracker
            file_id = Path(seg_nc).stem.replace('_analysis', '').replace('_segmentation', '')
            
            # Use helper for consistent paths
            from adapt.configuration.schemas.directories import get_plot_path
            
            output_path = get_plot_path(
                output_dirs=self.output_dirs,
                radar=radar,
                plot_type='reflectivity',
                scan_time=timestamp
            )
            
            plot_file = self.plotter.plot_from_netcdf(
                segmentation_nc=seg_nc,
                output_path=output_path,
            )
            
            logger.info(f"{radar} plot saved: {plot_file}")
            
            # Update tracker
            tracker = self.file_tracker
            if tracker and plot_file:
                tracker.mark_stage_complete(file_id, "plotted", path=Path(plot_file))
            
        except Exception as e:
            logger.exception(f"Error processing plot item: {e}")
            
            tracker = self.file_tracker
            if tracker:
                file_id = Path(item.get('segmentation_nc', '')).stem.replace('_analysis', '').replace('_segmentation', '')
                if file_id:
                    tracker.mark_stage_complete(file_id, "plotted", error=str(e))
    
    def stop(self):
        """Signal thread to stop and join gracefully."""
        self.running = False
        self.input_queue.put(None)


class PlotConsumer(threading.Thread):
    """Repository-driven plot consumer thread.

    Polls the DataRepository for new analysis artifacts and generates
    visualizations. This consumer is completely decoupled from the processing
    pipeline - it only reads from the repository.

    **Architecture:**

    The PlotConsumer runs as an independent thread that:
    1. Polls repository.get_latest(ANALYSIS_NC) every poll_interval seconds
    2. When a new artifact is detected, loads the dataset via repository.open_dataset()
    3. Generates visualization using RadarPlotter
    4. Saves figure to disk and optionally displays live
    5. Prints table statistics from cells database

    **Thread Safety:**

    - Repository uses WAL mode SQLite for concurrent read/write
    - PlotConsumer only reads, never writes to repository
    - No shared state with processing threads

    **Graceful Shutdown:**

    Call stop() to signal shutdown. The thread will complete any in-progress
    plot and exit cleanly within one poll_interval.

    Example usage::

        from adapt.visualization.plotter import PlotConsumer
        from adapt.persistence import DataRepository

        repo = DataRepository(run_id="abc123", base_dir="/data", radar="KDIX")
        stop_event = threading.Event()

        consumer = PlotConsumer(
            repository=repo,
            stop_event=stop_event,
            output_dir=Path("/data/KDIX/plots"),
            poll_interval=2.0
        )
        consumer.start()

        # ... pipeline runs ...

        stop_event.set()
        consumer.join(timeout=10)
    """

    def __init__(
        self,
        repository: "DataRepository",
        stop_event: threading.Event,
        output_dir: Path,
        config: "InternalConfig" = None,
        poll_interval: float = 2.0,
        show_live: bool = False,
        name: str = "PlotConsumer"
    ):
        """Initialize plot consumer.

        Parameters
        ----------
        repository : DataRepository
            Repository to poll for new artifacts. Must be initialized
            and connected to the same catalog as the processor.
        stop_event : threading.Event
            Shared event to signal shutdown. Set this to stop the consumer.
        output_dir : Path
            Directory to save generated plots.
        config : InternalConfig, optional
            Configuration for plot styling and parameters.
        poll_interval : float
            Seconds between repository polls (default: 2.0).
        show_live : bool
            If True, display plots in a matplotlib window (default: False).
        name : str
            Thread name for logging.
        """
        super().__init__(name=name, daemon=True)

        self.repository = repository
        self.stop_event = stop_event
        self.output_dir = Path(output_dir)
        self.config = config
        self.poll_interval = poll_interval
        self.show_live = show_live

        # Initialize plotter
        self.plotter = RadarPlotter(config=config, show_plots=show_live)

        # Track last processed artifact to detect new ones
        self._last_seen_id: str | None = None
        self._processed_count = 0

        # Import ProductType here to avoid circular imports
        from adapt.persistence import ProductType
        self._product_type = ProductType.ANALYSIS_NC

        logger.info(f"{name} initialized (poll_interval={poll_interval}s, output_dir={output_dir})")

    def run(self):
        """Main consumer loop - poll repository and generate plots."""
        logger.info(f"{self.name} started, polling for new analysis artifacts...")

        # Setup matplotlib for live display if requested
        if self.show_live:
            try:
                plt.ion()  # Interactive mode
            except Exception as e:
                logger.warning(f"Could not enable interactive plotting: {e}")
                self.show_live = False

        while not self.stop_event.is_set():
            try:
                self._poll_and_process()
            except Exception as e:
                logger.error(f"Error in {self.name}: {e}", exc_info=True)

            # Wait for next poll (interruptible)
            self.stop_event.wait(timeout=self.poll_interval)

        logger.info(f"{self.name} stopped (processed {self._processed_count} plots)")

    def _poll_and_process(self):
        """Check for new artifacts and process them."""
        try:
            # Get latest analysis artifact
            latest = self.repository.get_latest(self._product_type)

            if latest is None:
                # No artifacts yet
                return

            artifact_id = latest['artifact_id']

            # Check if this is a new artifact
            if artifact_id == self._last_seen_id:
                return

            # Process new artifact
            logger.info(f"New analysis artifact detected: {artifact_id}")
            self._process_artifact(latest)
            self._last_seen_id = artifact_id

        except Exception as e:
            logger.error(f"Error polling repository: {e}", exc_info=True)

    def _process_artifact(self, artifact: dict):
        """Generate plot from artifact."""
        artifact_id = artifact['artifact_id']
        Path(artifact['file_path'])
        scan_time_str = artifact.get('scan_time')

        try:
            # Parse scan time
            if scan_time_str:
                scan_time = datetime.fromisoformat(scan_time_str)
            else:
                scan_time = datetime.now(UTC)

            # Load dataset from repository
            ds = self.repository.open_dataset(artifact_id)

            try:
                # Generate output path
                radar = artifact.get('radar', self.repository.radar)
                date_str = scan_time.strftime("%Y%m%d")
                time_str = scan_time.strftime("%H%M%S")

                plot_dir = self.output_dir / date_str
                plot_dir.mkdir(parents=True, exist_ok=True)

                output_path = plot_dir / f"{radar}_reflectivity_{time_str}.png"

                # Generate plot
                plot_file = self.plotter.plot_reflectivity_with_cells(
                    ds=ds,
                    output_path=output_path
                )

                self._processed_count += 1
                logger.info(f"Plot saved: {plot_file} (total: {self._processed_count})")

                # Show live if enabled
                if self.show_live:
                    with contextlib.suppress(Exception):
                        plt.pause(0.1)

                # Print table statistics
                self._print_table_stats()

            finally:
                ds.close()

        except FileNotFoundError as e:
            logger.warning(f"Artifact file not found: {e}")
        except Exception as e:
            logger.error(f"Error processing artifact {artifact_id}: {e}", exc_info=True)

    def _print_table_stats(self):
        """Print latest cell statistics from repository."""
        try:
            from adapt.persistence import ProductType

            # Get latest cells database
            cells_db = self.repository.get_latest(ProductType.CELLS_DB)
            if cells_db is None:
                return

            # Load table
            df = self.repository.open_table(cells_db['artifact_id'], table_name='cells')

            if df.empty:
                return

            # Get most recent cells (last scan)
            if 'time' in df.columns:
                df['time'] = pd.to_datetime(df['time'])
                latest_time = df['time'].max()
                recent = df[df['time'] == latest_time]
            else:
                recent = df.tail(10)

            # Print summary statistics
            num_cells = len(recent)
            if num_cells == 0:
                return

            # Build stats summary
            stats_parts = [f"Cells: {num_cells}"]

            if 'cell_area_sqkm' in recent.columns:
                area = recent['cell_area_sqkm'].dropna()
                if len(area) > 0:
                    stats_parts.append(f"Area: {area.mean():.1f} km2 (mean)")

            if 'radar_reflectivity_mean' in recent.columns:
                refl_mean = recent['radar_reflectivity_mean'].dropna()
                if len(refl_mean) > 0:
                    stats_parts.append(f"Refl: {refl_mean.mean():.1f} dBZ (mean)")

            if 'radar_reflectivity_max' in recent.columns:
                refl_max = recent['radar_reflectivity_max'].dropna()
                if len(refl_max) > 0:
                    stats_parts.append(f"Max: {refl_max.max():.1f} dBZ")

            summary = " | ".join(stats_parts)
            logger.info(f"Cell Stats: {summary}")

            # Print detailed table for small number of cells
            if num_cells <= 5:
                print("\n" + "=" * 60)
                print("Latest Cell Statistics:")
                print("-" * 60)

                cols_to_show = ['cell_label', 'cell_area_sqkm',
                               'radar_reflectivity_mean', 'radar_reflectivity_max']
                cols_available = [c for c in cols_to_show if c in recent.columns]

                if cols_available:
                    display_df = recent[cols_available].copy()
                    display_df.columns = ['Label', 'Area (km2)', 'Mean dBZ', 'Max dBZ'][:len(cols_available)]
                    print(display_df.to_string(index=False))

                print("=" * 60 + "\n")

        except Exception as e:
            logger.debug(f"Could not print table stats: {e}")

    def stop(self):
        """Signal consumer to stop."""
        self.stop_event.set()


if __name__ == "__main__":
    print("RadarPlotter loaded.")
