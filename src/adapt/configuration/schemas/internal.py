"""InternalConfig: Authoritative runtime configuration.

This is the ONLY config schema that runtime code sees. It is fully validated,
normalized, and contains NO optional fields that processing code depends on.

All .get() calls, fallback defaults, and validation logic are FORBIDDEN in
runtime code - everything is explicit here.
"""

from typing import Literal, Optional
from pydantic import Field, ConfigDict
from adapt.configuration.schemas.base import AdaptBaseModel


# =============================================================================
# Nested Configuration Models (Runtime)
# =============================================================================

class InternalReaderConfig(AdaptBaseModel):
    """Runtime reader configuration."""
    file_format: Literal["nexrad_archive"]


class InternalDownloaderConfig(AdaptBaseModel):
    """Runtime downloader configuration."""
    mode: Literal["realtime", "historical"]
    radar: str
    output_dir: str
    latest_files: int
    latest_minutes: int
    poll_interval_sec: int
    start_time: Optional[str]
    end_time: Optional[str]
    min_file_size: int


class InternalRegridderConfig(AdaptBaseModel):
    """Runtime regridding configuration."""
    grid_shape: tuple[int, int, int]
    grid_limits: tuple[tuple[float, float], tuple[float, float], tuple[float, float]]
    roi_func: Literal["dist_beam", "dist"]
    min_radius: float
    weighting_function: Literal["cressman", "barnes", "nearest"]
    save_netcdf: bool


class InternalSegmenterConfig(AdaptBaseModel):
    """Runtime segmentation configuration."""
    method: Literal["threshold"]
    threshold: float
    min_cellsize_gridpoint: int
    max_cellsize_gridpoint: Optional[int]
    closing_kernel: tuple[int, int]
    filter_by_size: bool
    h_maxima: float


class InternalVarNamesConfig(AdaptBaseModel):
    """Runtime variable name mappings."""
    reflectivity: str
    cell_labels: str


class InternalCoordNamesConfig(AdaptBaseModel):
    """Runtime coordinate name mappings."""
    time: str
    z: str
    y: str
    x: str


class InternalGlobalConfig(AdaptBaseModel):
    """Runtime global settings."""
    z_level: float
    var_names: InternalVarNamesConfig
    coord_names: InternalCoordNamesConfig


class InternalFlowParamsConfig(AdaptBaseModel):
    """Runtime optical flow parameters."""
    pyr_scale: float
    levels: int
    winsize: int
    iterations: int
    poly_n: int
    poly_sigma: float
    flags: int


class InternalProjectorConfig(AdaptBaseModel):
    """Runtime projection configuration."""
    method: str
    max_time_interval_minutes: int
    max_projection_steps: int = Field(ge=1, le=10)  # Capped at 10
    nan_fill_value: float
    flow_params: InternalFlowParamsConfig
    min_motion_threshold: float
    max_flow_magnitude: float


class InternalAnalyzerConfig(AdaptBaseModel):
    """Runtime analysis configuration."""
    radar_variables: list[str]
    exclude_fields: list[str]
    adjacency_min_touching_boundary_pixels: int = Field(ge=1)


class InternalTrackerConfig(AdaptBaseModel):
    """Runtime tracking configuration."""
    class InternalCellUidConfig(AdaptBaseModel):
        """Runtime cell UID configuration."""
        time_step_s: int = Field(ge=1)
        latlon_step_deg: float = Field(gt=0.0)
        area_step_km2: float = Field(gt=0.0)
        width: int = Field(ge=1)
        alphabet: Literal["base36_upper"]

    match_cost_threshold: float = Field(default=0.15, ge=0.0)
    keep_cost_threshold: float = Field(default=1.0, ge=0.0)
    unmatch_cost_threshold: float = Field(default=2.0, ge=0.0)
    split_overlap_threshold: float = Field(default=0.8, ge=0.0, le=1.0)
    core_reflectivity_threshold: float = Field(default=40.0, ge=0.0)
    cell_uid: InternalCellUidConfig


class InternalVisualizationConfig(AdaptBaseModel):
    """Runtime visualization settings."""
    enabled: bool
    dpi: int
    figsize: tuple[float, float]
    output_format: Literal["png", "pdf", "jpeg"]
    use_basemap: bool
    basemap_alpha: float
    seg_linewidth: float
    proj_linewidth: float
    proj_alpha: float
    flow_scale: float
    flow_subsample: int
    min_reflectivity: float
    refl_vmin: float
    refl_vmax: float


class InternalOutputConfig(AdaptBaseModel):
    """Runtime output configuration."""
    compression: Literal["snappy", "gzip", "lz4", "none"]


class InternalLoggingConfig(AdaptBaseModel):
    """Runtime logging configuration."""
    level: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]


class InternalProcessorConfig(AdaptBaseModel):
    """Runtime processor configuration."""
    max_history: int = Field(default=2, ge=2, le=10)  # Frame history for optical flow
    min_file_size: int = Field(default=5000, ge=1000)  # Minimum file size in bytes
    db_filename_pattern: str = Field(default="{radar}_cells_statistics.db")  # Database filename pattern


# =============================================================================
# Main InternalConfig
# =============================================================================

class InternalConfig(AdaptBaseModel):
    """Authoritative runtime configuration.
    
    This is the ONLY configuration schema that processing code sees.
    It is fully validated, immutable, and contains explicit values for
    all parameters (no None for fields that runtime depends on).
    
    Usage
    -----
    Runtime modules receive InternalConfig and access fields directly:
    
        def __init__(self, config: InternalConfig):
            self.threshold = config.segmenter.threshold  # NOT .get()
            self.z_level = config.global_.z_level
    
    Rules
    -----
    - NO .get() calls
    - NO fallback defaults
    - NO type checking
    - NO validation
    
    All of that happens during config resolution, not in runtime code.
    """
    
    mode: Literal["realtime", "historical"]
    base_dir: str
    run_id: Optional[str] = Field(default=None, description="Unique run identifier generated during initialization")
    output_dirs: Optional[dict[str, str]] = Field(default=None, description="Output directory paths from initialization")
    reader: InternalReaderConfig
    downloader: InternalDownloaderConfig
    regridder: InternalRegridderConfig
    segmenter: InternalSegmenterConfig
    global_: InternalGlobalConfig = Field(alias="global")
    projector: InternalProjectorConfig
    analyzer: InternalAnalyzerConfig
    tracker: InternalTrackerConfig
    visualization: InternalVisualizationConfig
    output: InternalOutputConfig
    logging: InternalLoggingConfig
    processor: InternalProcessorConfig = Field(default_factory=InternalProcessorConfig)
    
    model_config = ConfigDict(
        extra='forbid',
        validate_assignment=True,
        use_enum_values=True,
        str_strip_whitespace=True,
        frozen=True,  # Immutable after construction
        populate_by_name=True,  # Allow both 'global' and 'global_'
    )
