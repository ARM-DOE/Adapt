# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""InternalConfig: Authoritative runtime configuration.

This is the ONLY config schema that runtime code sees. It is fully validated,
normalized, and contains NO optional fields that processing code depends on.

All .get() calls, fallback defaults, and validation logic are FORBIDDEN in
runtime code - everything is explicit here.
"""

from typing import Any, Literal

from pydantic import ConfigDict, Field, model_validator

from adapt.configuration.schemas.base import AdaptBaseModel
from adapt.configuration.schemas.segmenter_methods import (
    ConvStratRautParams,
    ConvStratYuterParams,
    FeatureDetectionParams,
    SteinerConvStratParams,
    ThresholdParams,
)

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
    max_fetch_retries: int
    start_time: str | None
    end_time: str | None
    min_file_size: int


class InternalRegridderConfig(AdaptBaseModel):
    """Runtime regridding configuration."""

    grid_shape: tuple[int, int, int]
    grid_limits: tuple[tuple[float, float], tuple[float, float], tuple[float, float]]
    roi_func: Literal["dist_beam", "dist"]
    min_radius: float
    weighting_function: Literal["cressman", "barnes", "nearest"]
    save_netcdf: bool
    netcdf_save_retries: int


class InternalSegmenterConfig(AdaptBaseModel):
    """Runtime segmentation configuration."""

    method: Literal[
        "conv_strat_raut",
        "conv_strat_yuter",
        "feature_detection",
        "steiner_conv_strat",
        "threshold",
    ]
    min_cellsize_gridpoint: int
    max_cellsize_gridpoint: int | None
    closing_kernel: tuple[int, int]
    filter_by_size: bool
    h_maxima: float
    threshold_params: ThresholdParams
    conv_strat_raut_params: ConvStratRautParams
    conv_strat_yuter_params: ConvStratYuterParams
    feature_detection_params: FeatureDetectionParams
    steiner_conv_strat_params: SteinerConvStratParams

    @model_validator(mode="before")
    @classmethod
    def _validate_only_selected_method_params(cls, data):
        """Only the selected method's params are validated; unused per-method
        blocks are reset to their defaults.

        The generated config carries a block for every method, but a run uses
        exactly one. A stale or renamed field in a method that is not selected
        must never block the pipeline, while a bad param in the selected method
        still fails loudly (its block is validated as-is).
        """
        if not isinstance(data, dict):
            return data
        keep = f"{data.get('method')}_params"
        param_fields = {name for name in cls.model_fields if name.endswith("_params")}
        if keep not in param_fields:
            return data  # unknown/missing method: let normal validation report it
        data = dict(data)
        for field in param_fields - {keep}:
            data[field] = {}  # unused block -> model defaults, ignoring any content
        return data


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
    registration_step_minutes: int = Field(ge=1)
    projection_horizon_minutes: int = Field(ge=1)


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

    split_overlap_threshold: float = Field(default=0.8, ge=0.0, le=1.0)
    core_field_threshold: float = Field(default=40.0, ge=0.0)
    max_tracking_gap_minutes: float = Field(default=20.0, gt=0.0)
    max_speed_ms: float = Field(default=40.0, gt=0.0)
    max_speed_multiplier: float = Field(default=3.0, gt=0.0)
    heading_change_penalty_weight: float = Field(default=0.0, ge=0.0)
    projected_hull_buffer_km: float = Field(default=1.0, gt=0.0)
    minimum_candidate_overlap: float = Field(default=0.20, ge=0.0, le=1.0)
    minimum_projected_overlap: float = Field(default=0.20, ge=0.0, le=1.0)
    length_scale: Literal["hull_equiv_diameter", "sum_radii", "fixed_km"] = "hull_equiv_diameter"
    geometry_length_scale_km: float = Field(default=5.0, gt=0.0)
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
    """Runtime logging + observability configuration.

    ``level`` governs the full file/JSON log; ``console_level`` keeps the console
    quiet independently. The remaining toggles enable/disable the observability
    subsystem and its pillars; the orchestrator translates these into an
    ``ObsSettings`` when it builds the provider.
    """

    level: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
    console_level: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] = "WARNING"
    enabled: bool = True
    traces: bool = True
    metrics: bool = True
    json_logs: bool = False
    console_logs: bool = True
    progress_every: float = Field(default=30.0, gt=0.0)


class InternalProcessorConfig(AdaptBaseModel):
    """Runtime processor configuration."""

    max_history: int = Field(default=2, ge=2, le=10)  # Frame history for optical flow
    min_file_size: int = Field(default=5000, ge=1000)  # Minimum file size in bytes
    # Database filename pattern
    db_filename_pattern: str = Field(default="{radar}_cells_statistics.db")


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
    source: str = Field(
        default="aws_nexrad",
        description="Name of the registered ingress source plugin that feeds the pipeline",
    )
    source_dir: str | None = Field(
        default=None,
        description="Directory a local source scans for already-present files",
    )
    run_id: str | None = Field(
        default=None,
        description="Unique run identifier generated during initialization",
    )
    output_dirs: dict[str, str] | None = Field(
        default=None, description="Output directory paths from initialization"
    )
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
    extensions: list[str] = Field(default_factory=list)
    module_params: dict[str, dict[str, Any]] = Field(default_factory=dict)
    # Module selection. ``modules`` is the file allowlist (None = run all
    # registered); ``only_modules`` / ``exclude_modules`` are the CLI --only / --not
    # overrides. The processor resolves these into the final enabled set.
    modules: list[str] | None = None
    only_modules: list[str] | None = None
    exclude_modules: list[str] | None = None

    model_config = ConfigDict(
        extra="forbid",
        validate_assignment=True,
        use_enum_values=True,
        str_strip_whitespace=True,
        frozen=True,  # Immutable after construction
        populate_by_name=True,  # Allow both 'global' and 'global_'
    )
