"""ParamConfig: Expert defaults for Adapt pipeline.

This module defines the complete, scientifically-validated default configuration.
ALL pipeline parameters must have defaults here. No runtime code should define
fallback values - this is the single source of truth for defaults.

Runtime code NEVER reads from ParamConfig directly - it only receives InternalConfig.
"""

from typing import Literal, Optional
from pydantic import Field, field_validator
from adapt.schemas.base import AdaptBaseModel


# =============================================================================
# Nested Configuration Models
# =============================================================================

class ReaderConfig(AdaptBaseModel):
    """Radar file reader configuration."""
    file_format: Literal["nexrad_archive"] = "nexrad_archive"


class DownloaderConfig(AdaptBaseModel):
    """NEXRAD data downloader configuration."""
    radar: Optional[str] = None
    output_dir: Optional[str] = None
    latest_files: int = Field(5, ge=1, description="Number of latest files to keep")
    latest_minutes: int = Field(60, ge=1, description="Time window in minutes")
    poll_interval_sec: int = Field(300, ge=1, description="Polling interval in seconds")
    start_time: Optional[str] = None
    end_time: Optional[str] = None
    min_file_size: int = Field(1024, ge=1, description="Minimum file size in bytes to consider valid")


class RegridderConfig(AdaptBaseModel):
    """PyART regridding configuration."""
    grid_shape: tuple[int, int, int] = (41, 301, 301)
    grid_limits: tuple[tuple[float, float], tuple[float, float], tuple[float, float]] = (
        (0.0, 20000.0),
        (-150000.0, 150000.0),
        (-150000.0, 150000.0),
    )
    roi_func: Literal["dist_beam", "dist"] = "dist_beam"
    min_radius: float = Field(1750.0, gt=0)
    weighting_function: Literal["cressman", "barnes", "nearest"] = "cressman"
    save_netcdf: bool = True


class SegmenterConfig(AdaptBaseModel):
    """Cell segmentation configuration."""
    method: Literal["threshold"] = "threshold"
    threshold: float = Field(30.0, description="Reflectivity threshold in dBZ")
    min_cellsize_gridpoint: int = Field(5, ge=1)
    max_cellsize_gridpoint: Optional[int] = Field(None, ge=1)
    closing_kernel: tuple[int, int] = (1, 1)
    filter_by_size: bool = True
    
    @field_validator("threshold", mode="before")
    @classmethod
    def coerce_threshold_to_float(cls, v):
        """Allow int or float for threshold."""
        return float(v)


class VarNamesConfig(AdaptBaseModel):
    """Variable name mappings."""
    reflectivity: str = "reflectivity"
    cell_labels: str = "cell_labels"


class CoordNamesConfig(AdaptBaseModel):
    """Coordinate name mappings."""
    time: str = "time"
    z: str = "z"
    y: str = "y"
    x: str = "x"


class GlobalConfig(AdaptBaseModel):
    """Global pipeline settings."""
    z_level: float = Field(2000.0, description="Analysis altitude in meters")
    var_names: VarNamesConfig = Field(default_factory=VarNamesConfig)
    coord_names: CoordNamesConfig = Field(default_factory=CoordNamesConfig)
    
    @field_validator("z_level", mode="before")
    @classmethod
    def coerce_z_level_to_float(cls, v):
        """Allow int or float for z_level."""
        return float(v)


class FlowParamsConfig(AdaptBaseModel):
    """OpenCV optical flow parameters."""
    pyr_scale: float = Field(0.5, gt=0, le=1.0)
    levels: int = Field(3, ge=1)
    winsize: int = Field(10, ge=1)
    iterations: int = Field(3, ge=1)
    poly_n: int = Field(7, ge=5)
    poly_sigma: float = Field(1.5, gt=0)
    flags: int = 0


class ProjectorConfig(AdaptBaseModel):
    """Cell projection configuration."""
    method: Literal["adapt_default"] = "adapt_default"
    max_time_interval_minutes: int = Field(30, ge=1)
    max_projection_steps: int = Field(1, ge=1, le=10)  # OLD DEFAULT: was 1 not 5
    nan_fill_value: float = 0.0
    flow_params: FlowParamsConfig = Field(default_factory=FlowParamsConfig)
    min_motion_threshold: float = Field(0.5, ge=0)
    
    @field_validator("method", mode="before")
    @classmethod
    def normalize_method_name(cls, v):
        """Normalize method names to lowercase."""
        if isinstance(v, str):
            return v.lower().strip()
        return v


class AnalyzerConfig(AdaptBaseModel):
    """Cell analysis configuration."""
    radar_variables: list[str] = Field(
        default_factory=lambda: [
            "reflectivity",
            "velocity",
            "differential_phase",
            "differential_reflectivity",
            "spectrum_width",
            "cross_correlation_ratio",
        ]
    )
    exclude_fields: list[str] = Field(
        default_factory=lambda: [
            "ROI",
            "labels",
            "cell_labels",
            "cell_projections",
            "clutter_filter_power_removed",
        ]
    )


class VisualizationConfig(AdaptBaseModel):
    """Visualization settings."""
    enabled: bool = True
    dpi: int = Field(200, ge=50)
    figsize: tuple[float, float] = (18.0, 8.0)
    output_format: Literal["png", "pdf", "jpeg"] = "png"
    use_basemap: bool = True
    basemap_alpha: float = Field(0.6, ge=0, le=1.0)
    seg_linewidth: float = Field(0.8, gt=0)
    proj_linewidth: float = Field(1.0, gt=0)
    proj_alpha: float = Field(0.8, ge=0, le=1.0)
    flow_scale: float = Field(0.5, gt=0)
    flow_subsample: int = Field(10, ge=1)
    min_reflectivity: float = 10.0
    refl_vmin: float = 10.0
    refl_vmax: float = 50.0


class OutputConfig(AdaptBaseModel):
    """Output file configuration."""
    compression: Literal["snappy", "gzip", "lz4", "none"] = "snappy"


class LoggingConfig(AdaptBaseModel):
    """Logging configuration."""
    level: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] = "INFO"


# =============================================================================
# Main ParamConfig
# =============================================================================

class ParamConfig(AdaptBaseModel):
    """Complete expert configuration with all defaults.
    
    This is the single source of truth for all pipeline parameters.
    Every tunable parameter MUST have a default here.
    
    Usage
    -----
    This config is NOT used directly by runtime code. It serves as the
    base layer in config resolution:
    
        internal_cfg = resolve_config(param_cfg, user_cfg, cli_cfg)
    
    Runtime code only sees InternalConfig.
    """
    
    mode: Literal["realtime", "historical"] = "realtime"
    reader: ReaderConfig = Field(default_factory=ReaderConfig)
    downloader: DownloaderConfig = Field(default_factory=DownloaderConfig)
    regridder: RegridderConfig = Field(default_factory=RegridderConfig)
    segmenter: SegmenterConfig = Field(default_factory=SegmenterConfig)
    global_: GlobalConfig = Field(default_factory=GlobalConfig, alias="global")
    projector: ProjectorConfig = Field(default_factory=ProjectorConfig)
    analyzer: AnalyzerConfig = Field(default_factory=AnalyzerConfig)
    visualization: VisualizationConfig = Field(default_factory=VisualizationConfig)
    output: OutputConfig = Field(default_factory=OutputConfig)
    logging: LoggingConfig = Field(default_factory=LoggingConfig)
    
    model_config = AdaptBaseModel.model_config.copy()
    model_config.update({"populate_by_name": True})  # Allow both 'global' and 'global_'
