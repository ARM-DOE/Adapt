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
    field_map: dict[str, str] = Field(
        default_factory=dict,
        description=(
            "Source-to-canonical variable renames applied once at ingest, "
            "e.g. {corrected_reflectivity: reflectivity}. Empty = no renames."
        ),
    )
    fields: list[str] = Field(
        default_factory=list,
        description=(
            "Canonical variable names to keep (post-rename). Empty = keep "
            "every field the source file provides."
        ),
    )


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
    max_queue_size: int = Field(
        100,
        ge=1,
        description=(
            "Scans the downloader may run ahead of the processor. Each queued item is a "
            "small message (the volume itself is already committed to the store), so this "
            "costs disk for the downloaded volumes only, ~10 MB each. When the queue "
            "reaches this many items the downloader pauses; processing never pauses."
        ),
    )
    queue_resume_fraction: float = Field(
        0.10,
        gt=0.0,
        lt=1.0,
        description=(
            "The downloader resumes once the queue has drained to this fraction of "
            "max_queue_size (at least 1 item). The gap between the two levels stops the "
            "downloader flapping on every consumed scan."
        ),
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
    closing_radius: int = Field(
        2,
        ge=0,
        description=(
            "Radius (grid points) of the disk used to close the convective mask: a "
            "dilation followed by an erosion with the same disk. Fills gaps and "
            "indentations narrower than ~2r and rounds the boundary without changing "
            "the extent of large features. 0 disables it."
        ),
    )
    filter_by_size: bool = True
    h_maxima: float = Field(5.0, gt=0, description="h-maxima height for cell seeding (dBZ)")
    # Previous-frame seeding: the projection module's one-step-ahead labels
    # become extra watershed markers (union with h-maxima seeds, never a
    # replacement). Off by default; off is byte-identical to no seeding.
    seed_carry_frames: int = Field(
        1,
        ge=0,
        description="How many consecutive frames a core may be carried without independent "
        "detection. 0 disables the carry entirely; 1 carries only from the immediately "
        "preceding frame (a core not re-detected next frame is dropped); N allows N frames",
    )
    carried_exempt_size_filter: bool = Field(
        False,
        description="Spare carried-seed cells from min_cellsize_gridpoint. Off by default: "
        "on KHTX 2021-05-04 the spared basins (1-4 px) sat wholly inside the parent's "
        "projected hull (Opc 1.0) but covered ~4% of it, so the (v1) tracker's "
        "projected-overlap gate rejected them anyway — 60 of 63 became one-scan "
        "orphan tracks and 3 continued. The rescue moved from the size filter to the "
        "tracking gate rather than succeeding. Kept as an option for study arms",
    )
    seed_carry_min_separation: int = Field(
        5,
        ge=1,
        description="Minimum pixel distance from any h-maxima seed for a carried core "
        "to be admitted",
    )
    # Per-method scientific parameters (one block per method; defaults = algorithm defaults)
    threshold_params: ThresholdParams = Field(default_factory=ThresholdParams)  # type: ignore[arg-type]
    conv_strat_raut_params: ConvStratRautParams = Field(default_factory=ConvStratRautParams)  # type: ignore[arg-type]
    conv_strat_yuter_params: ConvStratYuterParams = Field(default_factory=ConvStratYuterParams)  # type: ignore[arg-type]
    feature_detection_params: FeatureDetectionParams = Field(default_factory=FeatureDetectionParams)  # type: ignore[arg-type]
    steiner_conv_strat_params: SteinerConvStratParams = Field(
        default_factory=SteinerConvStratParams  # type: ignore[arg-type]
    )


class CoordNamesConfig(AdaptBaseModel):
    """Coordinate name mappings."""

    time: str = "time"
    z: str = "z"
    y: str = "y"
    x: str = "x"


class GlobalConfig(AdaptBaseModel):
    """Global pipeline settings."""

    z_level: float = Field(2000.0, description="Analysis altitude in meters")
    max_scan_gap_minutes: float = Field(
        10.0,
        gt=0.0,
        description=(
            "The one scan-gap bound, shared by every stage that pairs frames. Beyond it "
            "no seed is carried, no projection is computed and tracking resets. Set it to "
            "roughly twice the nominal scan cadence; a single value keeps the stages from "
            "disagreeing about whether a gap is crossable"
        ),
    )
    tracking_field: str = Field(
        "reflectivity",
        description=(
            "Canonical field that drives detection, projection, and "
            "tracking. Any canonical variable present after ingest "
            "(see reader.field_map/fields) is valid."
        ),
    )
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
    max_projection_steps: int = Field(3, ge=1, le=10)
    nan_fill_value: float = 0.0
    flow_params: FlowParamsConfig = Field(default_factory=FlowParamsConfig)  # type: ignore[arg-type]
    min_motion_threshold: float = Field(1.0, ge=0)
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
        """Track ID generation configuration.

        v2 uids hash (scan_id, cell_label) — deterministic and
        collision-free by construction; only the token width/alphabet
        remain configurable.
        """

        width: int = Field(10, ge=1)
        alphabet: Literal["base36_upper"] = "base36_upper"

    cell_threshold: float = Field(
        30.0,
        description="Intensity at the cell edge (field units). The excess e = polarity·(f − "
        "cell_threshold)₊ weights the cell centre (by e²) and the identity score",
    )
    polarity: Literal[1, -1] = Field(
        1,
        description="+1 when cells are above the threshold (reflectivity), −1 when below "
        "(brightness temperature)",
    )
    growth_max_km: float = Field(
        2.0,
        ge=0.0,
        description="Growth r_max (km) of the smallest footprints and cells: "
        "r = r_max·(1 − √A/ℓ0)₊ evens out the one-grid-cell registration error across cell sizes",
    )
    growth_size_km: float = Field(
        12.0, gt=0.0, description="Size ℓ0 (km, √area) at and above which cells are not grown"
    )
    max_overlap_mismatch: float = Field(
        0.6,
        gt=0.0,
        le=1.0,
        description="Curved overlap gate u_max: a pair is a candidate when u = 1 − √(O^c·O^h) "
        "on the grown shapes is at most this",
    )
    max_link_cost: float = Field(
        1.8,
        gt=0.0,
        description="Cost ceiling c_max: the cost of leaving a cell unlinked; a link is accepted "
        "only if its cost c = u + w_d·d + h is at most this",
    )
    residual_weight: float = Field(
        0.25,
        ge=0.0,
        description="w_d on the shape-aware centre residual d; 0.25 makes w_d·d the displacement "
        "over the diameter for a round cell",
    )
    heading_weight: float = Field(
        1.0,
        ge=0.0,
        description="w_h on the heading term w_h·(1 − cos Δθ)/2, applied when both steps exceed "
        "two grid lengths",
    )
    max_speed_ms: float = Field(40.0, gt=0.0, description="Hard speed cap v_max (m/s) of any link")
    max_acceleration_ms2: float = Field(
        0.04,
        ge=0.0,
        description="a_max (m/s²) in the additive speed-change gate "
        "v ≤ v_prev + a_max·Δt + 2√2·σ_x/Δt (σ_x = one grid length)",
    )
    split_overlap_threshold: float = Field(
        0.65,
        gt=0.0,
        le=1.0,
        description="τ_s: an orphan is a split child when a continuing parent's grown footprint "
        "covers at least this share of it (O^c)",
    )
    merge_overlap_threshold: float = Field(
        0.7,
        gt=0.0,
        le=1.0,
        description="τ_m: an unlinked cell merged into a continuing cell covering at least this "
        "share of its grown footprint (O^h)",
    )
    latent_scans: int = Field(
        2,
        ge=0,
        description="N: a track that ends or merges away is carried by the flow for up to N scans "
        "and may be resumed; 0 or 1 ends it at once",
    )
    identity_intensity_weight: float = Field(
        1.0,
        ge=0.0,
        description="γ in the identity score S = log A + γ·log ē (1: intensity mass, 0: area)",
    )
    identity_score_margin: float = Field(
        0.33,
        ge=0.0,
        description="δ_S: scores closer than this are within noise and the identity follows the "
        "candidate nearer the predicted position",
    )
    core_field_threshold: float = Field(
        40.0, description="Field threshold for the diagnostic core-area output (not a decision)"
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
