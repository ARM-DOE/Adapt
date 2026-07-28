# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""ParamConfig: Expert defaults for Adapt pipeline.

This module defines the complete, scientifically-validated default configuration.
ALL pipeline parameters must have defaults here. No runtime code should define
fallback values - this is the single source of truth for defaults.

Runtime code NEVER reads from ParamConfig directly - it only receives InternalConfig.
"""

from typing import Literal

from pydantic import Field, field_validator

from adapt.configuration.schemas.base import AdaptBaseModel
from adapt.configuration.schemas.segmenter_methods import (
    ConvStratRautParams,
    ConvStratYuterParams,
    FeatureDetectionParams,
    SteinerConvStratParams,
    ThresholdParams,
)

# =============================================================================
# Nested Configuration Models
# =============================================================================


class ReaderConfig(AdaptBaseModel):
    """Radar file reader configuration."""

    file_format: Literal["nexrad_archive"] = "nexrad_archive"


class DownloaderConfig(AdaptBaseModel):
    """NEXRAD data downloader configuration."""

    radar: str | None = None
    output_dir: str | None = None
    latest_files: int = Field(5, ge=1, description="Number of latest files to keep")
    latest_minutes: int = Field(60, ge=1, description="Time window in minutes")
    poll_interval_sec: int = Field(300, ge=1, description="Polling interval in seconds")
    max_fetch_retries: int = Field(
        3, ge=1, description="AWS scan-fetch attempts before giving up for this poll"
    )
    start_time: str | None = None
    end_time: str | None = None
    min_file_size: int = Field(
        1024, ge=1, description="Minimum file size in bytes to consider valid"
    )


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
    netcdf_save_retries: int = Field(
        3, ge=1, description="NetCDF write attempts before raising (when save_netcdf is set)"
    )


class SegmenterConfig(AdaptBaseModel):
    """Cell segmentation configuration.

    The user selects a `method`; the parameters for that method come from the
    matching nested block below (Adapt-owned defaults). The shared labeling
    parameters apply to every method's convective mask.
    """

    # threshold is the default because it needs only the 2D CAPPI the pipeline
    # always produces, so an out-of-the-box run works with no further setup. The
    # pyart convective/stratiform classifiers need a 3D grid; select one via
    # `segmenter.method` once that is available.
    method: Literal[
        "conv_strat_raut",
        "conv_strat_yuter",
        "feature_detection",
        "steiner_conv_strat",
        "threshold",
    ] = "threshold"
    # Shared labeling-backend parameters (applied to every method's mask)
    min_cellsize_gridpoint: int = Field(5, ge=1)
    max_cellsize_gridpoint: int | None = Field(None, ge=1)
    closing_kernel: tuple[int, int] = (1, 1)
    filter_by_size: bool = True
    h_maxima: float = Field(5.0, gt=0, description="h-maxima height for cell seeding (dBZ)")
    # Per-method scientific parameters (one block per method; defaults = algorithm defaults)
    threshold_params: ThresholdParams = Field(default_factory=ThresholdParams)  # type: ignore[arg-type]
    conv_strat_raut_params: ConvStratRautParams = Field(default_factory=ConvStratRautParams)  # type: ignore[arg-type]
    conv_strat_yuter_params: ConvStratYuterParams = Field(default_factory=ConvStratYuterParams)  # type: ignore[arg-type]
    feature_detection_params: FeatureDetectionParams = Field(default_factory=FeatureDetectionParams)  # type: ignore[arg-type]
    steiner_conv_strat_params: SteinerConvStratParams = Field(
        default_factory=SteinerConvStratParams  # type: ignore[arg-type]
    )


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
    var_names: VarNamesConfig = Field(default_factory=VarNamesConfig)  # type: ignore[arg-type]
    coord_names: CoordNamesConfig = Field(default_factory=CoordNamesConfig)  # type: ignore[arg-type]

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
    max_projection_steps: int = Field(3, ge=1, le=10)
    nan_fill_value: float = 0.0
    flow_params: FlowParamsConfig = Field(default_factory=FlowParamsConfig)  # type: ignore[arg-type]
    min_motion_threshold: float = Field(0.5, ge=0)
    max_flow_magnitude: float = Field(
        20.0,
        gt=0,
        description="Clip flow vectors exceeding this magnitude (pixels/frame)",
    )
    registration_step_minutes: int = Field(
        1,
        ge=1,
        description="Step of the minute-resolution registration masks between scans",
    )
    projection_horizon_minutes: int = Field(
        15,
        ge=1,
        le=60,
        description="Forward horizon (minutes) for the minute-resolution projection product",
    )

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
    adjacency_min_touching_boundary_pixels: int = Field(
        1,
        ge=1,
        description=(
            "Min number of touching boundary pixels to count two labels "
            "as adjacent in the same scan"
        ),
    )


class TrackerConfig(AdaptBaseModel):
    """Cell tracking configuration."""

    class CellUidConfig(AdaptBaseModel):
        """Track ID generation configuration."""

        time_step_s: int = Field(10, ge=1)
        latlon_step_deg: float = Field(0.1, gt=0.0)
        area_step_km2: float = Field(5.0, gt=0.0)
        width: int = Field(10, ge=1)
        alphabet: Literal["base36_upper"] = "base36_upper"

    split_overlap_threshold: float = Field(
        0.8,
        ge=0.0,
        le=1.0,
        description=(
            "Min fraction of projected hull area overlapping born/surviving cell "
            "to confirm SPLIT or MERGE"
        ),
    )
    core_field_threshold: float = Field(
        40.0, ge=0.0, description="Field threshold for the core-area output (e.g. dBZ for radar)"
    )
    max_tracking_gap_minutes: float = Field(
        20.0,
        gt=0.0,
        description="Hard limit: scan gaps above this terminate all tracks and restart "
        "(no matching attempted across the gap)",
    )
    max_speed_ms: float = Field(
        40.0,
        gt=0.0,
        description="Hard physical cap (m/s); candidate pairs above this are rejected pre-matching",
    )
    max_speed_multiplier: float = Field(
        3.0,
        gt=0.0,
        description="Hard acceleration cap: reject if candidate speed exceeds this times the "
        "track's previous speed",
    )
    heading_change_penalty_weight: float = Field(
        0.0,
        ge=0.0,
        description="Optional cost penalty per radian of heading change (0 = diagnostic only)",
    )
    projected_hull_buffer_km: float = Field(
        1.0,
        gt=0.0,
        description="Dilation radius (km) applied to projected hulls before candidate generation, "
        "to absorb segmentation and optical-flow uncertainty",
    )
    minimum_candidate_overlap: float = Field(
        0.20,
        ge=0.0,
        le=1.0,
        description="Hard gate: min fraction of the candidate cell covered by the projected hull "
        "(Opc = intersection / candidate area)",
    )
    minimum_projected_overlap: float = Field(
        0.20,
        ge=0.0,
        le=1.0,
        description="Hard gate: min fraction of the projected hull covered by the candidate cell "
        "(Ocp = intersection / projected hull area)",
    )
    length_scale: Literal["hull_equiv_diameter", "sum_radii", "fixed_km"] = Field(
        "hull_equiv_diameter",
        description="Characteristic length L that normalises centroid displacement in the "
        "geometry-first cost (cost = m + d/L). Swappable for experimentation",
    )
    geometry_length_scale_km: float = Field(
        5.0,
        gt=0.0,
        description="Fixed characteristic length (km) used only when length_scale='fixed_km'",
    )
    cell_uid: CellUidConfig = Field(default_factory=CellUidConfig)  # type: ignore[arg-type]


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
    source: str = "aws_nexrad"
    source_dir: str | None = None
    reader: ReaderConfig = Field(default_factory=ReaderConfig)  # type: ignore[arg-type]
    downloader: DownloaderConfig = Field(default_factory=DownloaderConfig)  # type: ignore[arg-type]
    regridder: RegridderConfig = Field(default_factory=RegridderConfig)  # type: ignore[arg-type]
    segmenter: SegmenterConfig = Field(default_factory=SegmenterConfig)  # type: ignore[arg-type]
    global_: GlobalConfig = Field(default_factory=GlobalConfig, alias="global")  # type: ignore[arg-type]
    projector: ProjectorConfig = Field(default_factory=ProjectorConfig)  # type: ignore[arg-type]
    analyzer: AnalyzerConfig = Field(default_factory=AnalyzerConfig)  # type: ignore[arg-type]
    tracker: TrackerConfig = Field(default_factory=TrackerConfig)  # type: ignore[arg-type]
    visualization: VisualizationConfig = Field(default_factory=VisualizationConfig)  # type: ignore[arg-type]
    output: OutputConfig = Field(default_factory=OutputConfig)  # type: ignore[arg-type]
    logging: LoggingConfig = Field(default_factory=LoggingConfig)  # type: ignore[arg-type]

    model_config = AdaptBaseModel.model_config.copy()
    model_config.update({"populate_by_name": True})  # Allow both 'global' and 'global_'
