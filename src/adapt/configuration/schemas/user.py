# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""UserConfig: Forgiving, minimal user-facing configuration.

This schema accepts user inputs in a variety of formats, with aliases
for common naming patterns (e.g., RADAR_ID → radar, MODE → mode).

UserConfig is intentionally minimal - users only specify what they want
to override from the expert defaults. Validation is lenient to accept
both uppercase and lowercase keys, integers where floats are expected, etc.
"""

from typing import Any, Literal, cast

from pydantic import Field, field_validator, model_validator

from adapt.configuration.schemas.base import AdaptBaseModel

# InternalConfig sections that have no explicit UserConfig field and are routed
# straight through from undeclared (extra) input. Other sections (downloader,
# regridder, segmenter, global, analyzer, projector) are handled via typed fields.
_PASSTHROUGH_SECTIONS = frozenset(
    {"reader", "tracker", "visualization", "output", "logging", "processor"}
)


class _UserSection(AdaptBaseModel):
    """Base for typed nested user sections.

    Tolerates every field a fully-generated config.yaml emits (extra="allow") and
    passes the extras through model_dump so they reach InternalConfig, which makes
    the final validation decision.
    """

    model_config = AdaptBaseModel.model_config.copy()
    model_config.update({"extra": "allow", "populate_by_name": True})


class UserSegmenterConfig(_UserSection):
    """User-facing segmentation config with aliases."""

    method: str | None = None
    threshold: float | None = None
    min_cellsize_gridpoint: int | None = None
    max_cellsize_gridpoint: int | None = None
    closing_kernel: tuple[int, int] | None = None
    filter_by_size: bool | None = None
    h_maxima: float | None = None

    @field_validator("threshold", mode="before")
    @classmethod
    def coerce_threshold(cls, v):
        """Accept int or float for threshold."""
        if v is not None:
            return float(v)
        return v

    @field_validator("method", mode="before")
    @classmethod
    def normalize_method(cls, v):
        """Normalize method names to lowercase."""
        if isinstance(v, str):
            return v.lower().strip()
        return v


class UserGlobalConfig(_UserSection):
    """User-facing global config."""

    z_level: float | None = None
    var_names: dict[str, str] | None = None
    coord_names: dict[str, str] | None = None

    @field_validator("z_level", mode="before")
    @classmethod
    def coerce_z_level(cls, v):
        """Accept int or float for z_level."""
        if v is not None:
            return float(v)
        return v


class UserProjectorConfig(_UserSection):
    """User-facing projector config."""

    method: str | None = None
    max_time_interval_minutes: int | None = None
    max_projection_steps: int | None = None
    nan_fill_value: float | None = None
    flow_params: dict[str, Any] | None = None
    min_motion_threshold: float | None = None
    max_flow_magnitude: float | None = None
    registration_step_minutes: int | None = None

    @field_validator("method", mode="before")
    @classmethod
    def normalize_method(cls, v):
        """Normalize method names to lowercase."""
        if isinstance(v, str):
            return v.lower().strip()
        return v


class UserRegridderConfig(_UserSection):
    """User-facing regridder config."""

    grid_shape: tuple[int, int, int] | None = None
    grid_limits: tuple[tuple[float, float], tuple[float, float], tuple[float, float]] | None = None
    roi_func: str | None = None
    min_radius: float | None = None
    weighting_function: str | None = None
    save_netcdf: bool | None = None


class UserDownloaderConfig(_UserSection):
    """User-facing downloader config."""

    radar: str | None = None
    output_dir: str | None = None
    latest_files: int | None = None
    latest_minutes: int | None = None
    poll_interval_sec: int | None = None
    max_fetch_retries: int | None = None
    start_time: str | None = None
    end_time: str | None = None


class UserAnalyzerConfig(_UserSection):
    """User-facing analyzer config."""

    radar_variables: list[str] | None = None
    exclude_fields: list[str] | None = None


class UserConfig(AdaptBaseModel):
    """User-facing configuration schema.

    Minimal, forgiving, and uses common aliases. Users only specify
    what they want to override from ParamConfig defaults.

    This config is converted to internal overrides during resolution.

    Usage
    -----
        user_cfg = UserConfig(
            mode="historical",
            radar="KHTX",
            base_dir="/data/adapt",
            z_level=2000,
            threshold=35,
        )

        internal = resolve_config(param_cfg, user_cfg, cli_cfg)
    """

    # Top-level operational settings
    mode: Literal["realtime", "historical"] | None = Field(None, alias="MODE")
    source: str | None = None
    source_dir: str | None = None
    radar: str | None = Field(None, alias="RADAR_ID")
    base_dir: str | None = Field(None, alias="BASE_DIR")

    # Realtime settings
    latest_files: int | None = Field(None, alias="LATEST_FILES")
    latest_minutes: int | None = Field(None, alias="LATEST_MINUTES")
    poll_interval_sec: int | None = Field(None, alias="POLL_INTERVAL_SEC")
    max_fetch_retries: int | None = Field(None, alias="MAX_FETCH_RETRIES")

    # Historical settings
    start_time: str | None = Field(None, alias="START_TIME")
    end_time: str | None = Field(None, alias="END_TIME")

    # Grid settings (flat aliases)
    grid_shape: tuple[int, int, int] | None = Field(None, alias="GRID_SHAPE")
    grid_limits: tuple[tuple[float, float], tuple[float, float], tuple[float, float]] | None = (
        Field(None, alias="GRID_LIMITS")
    )

    # Segmentation settings (flat aliases)
    z_level: float | None = Field(None, alias="Z_LEVEL")
    reflectivity_var: str | None = Field(None, alias="REFLECTIVITY_VAR")
    segmentation_method: str | None = Field(None, alias="SEGMENTATION_METHOD")
    threshold: float | None = Field(None, alias="THRESHOLD_DBZ")
    min_cellsize_gridpoint: int | None = Field(None, alias="MIN_CELLSIZE_GRIDPOINT")
    max_cellsize_gridpoint: int | None = Field(None, alias="MAX_CELLSIZE_GRIDPOINT")

    # Projection settings (flat aliases)
    projection_method: str | None = Field(None, alias="PROJECTION_METHOD")
    max_projection_steps: int | None = Field(None, alias="MAX_PROJECTION_STEPS")

    # Analyzer settings (flat aliases)
    radar_variables: list[str] | None = None
    exclude_fields: list[str] | None = None

    # Extension modules (optional, user-selected)
    extensions: list[str] = Field(default_factory=list)

    # Pipeline module allowlist (node names). None/absent = run all registered
    # modules; a present list runs only the named ones (comment a line out to skip).
    modules: list[str] | None = None

    # Per-module config params, keyed by module name (for extension modules)
    module_params: dict[str, dict[str, Any]] | None = None

    # Nested overrides (advanced users)
    downloader: UserDownloaderConfig | None = None
    regridder: UserRegridderConfig | None = None
    segmenter: UserSegmenterConfig | None = None
    global_: UserGlobalConfig | None = Field(None, alias="global")
    projector: UserProjectorConfig | None = None
    analyzer: UserAnalyzerConfig | None = None

    model_config = AdaptBaseModel.model_config.copy()
    # Capture undeclared sections (extra="allow") so the full set of InternalConfig
    # sections can be overridden; unrouted legacy keys are filtered in
    # to_internal_overrides. Unknown keys *within* a routed section still raise
    # via InternalConfig (extra="forbid").
    model_config.update({"populate_by_name": True, "extra": "allow"})

    @model_validator(mode="after")
    def infer_historical_mode_from_times(self):
        """If times provided but mode not specified, set mode to historical.

        This is a schema responsibility: if user config indicates a time range,
        the mode should automatically be historical.
        """
        if self.mode is None and (
            (self.start_time and self.end_time)
            or (self.downloader and (self.downloader.start_time and self.downloader.end_time))
        ):
            self.mode = cast(Literal["realtime", "historical"], "historical")

        return self

    @field_validator("z_level", "threshold", mode="before")
    @classmethod
    def coerce_numeric_fields(cls, v):
        """Accept int or float for numeric fields."""
        if v is not None:
            return float(v)
        return v

    @field_validator("segmentation_method", "projection_method", mode="before")
    @classmethod
    def normalize_method_names(cls, v):
        """Normalize method names to lowercase."""
        if isinstance(v, str):
            return v.lower().strip()
        return v

    def to_internal_overrides(self) -> dict:
        """Convert flat UserConfig to nested InternalConfig structure.

        Returns
        -------
        dict
            Nested dictionary matching InternalConfig structure
        """
        overrides: dict[str, Any] = {}

        if self.mode is not None:
            overrides["mode"] = self.mode

        if self.source is not None:
            overrides["source"] = self.source

        if self.source_dir is not None:
            overrides["source_dir"] = self.source_dir

        if self.extensions:
            overrides["extensions"] = self.extensions

        if self.modules is not None:
            overrides["modules"] = self.modules

        if self.module_params:
            overrides["module_params"] = self.module_params

        if self.base_dir is not None:
            overrides["base_dir"] = str(self.base_dir)

        # Downloader section
        downloader: dict[str, Any] = {}
        if self.radar is not None:
            downloader["radar"] = self.radar
        if self.start_time is not None:
            downloader["start_time"] = self.start_time
        if self.end_time is not None:
            downloader["end_time"] = self.end_time
        if self.latest_files is not None:
            downloader["latest_files"] = self.latest_files
        if self.latest_minutes is not None:
            downloader["latest_minutes"] = self.latest_minutes
        if self.poll_interval_sec is not None:
            downloader["poll_interval_sec"] = self.poll_interval_sec
        if self.max_fetch_retries is not None:
            downloader["max_fetch_retries"] = self.max_fetch_retries

        # Map base_dir to downloader.output_dir for convenience
        if self.base_dir is not None:
            # Accept either a Path or string; keep as string for overrides
            downloader["output_dir"] = str(self.base_dir)

        # Merge with explicit downloader config
        if self.downloader is not None:
            downloader.update(self.downloader.model_dump(exclude_none=True))

        if downloader:
            overrides["downloader"] = downloader

        # Regridder section
        regridder: dict[str, Any] = {}
        if self.grid_shape is not None:
            regridder["grid_shape"] = self.grid_shape
        if self.grid_limits is not None:
            regridder["grid_limits"] = self.grid_limits

        # Merge with explicit regridder config
        if self.regridder is not None:
            regridder.update(self.regridder.model_dump(exclude_none=True))

        if regridder:
            overrides["regridder"] = regridder

        # Segmenter section
        segmenter: dict[str, Any] = {}
        if self.segmentation_method is not None:
            segmenter["method"] = self.segmentation_method
        if self.threshold is not None:
            segmenter["threshold"] = self.threshold
        if self.min_cellsize_gridpoint is not None:
            segmenter["min_cellsize_gridpoint"] = self.min_cellsize_gridpoint
        if self.max_cellsize_gridpoint is not None:
            segmenter["max_cellsize_gridpoint"] = self.max_cellsize_gridpoint

        # Merge with explicit segmenter config
        if self.segmenter is not None:
            segmenter.update(self.segmenter.model_dump(exclude_none=True))

        if segmenter:
            overrides["segmenter"] = segmenter

        # Global section
        global_cfg: dict[str, Any] = {}
        if self.z_level is not None:
            global_cfg["z_level"] = self.z_level

        if self.reflectivity_var is not None:
            var_names: dict[str, str] = global_cfg.get("var_names", {})
            var_names["reflectivity"] = self.reflectivity_var
            global_cfg["var_names"] = var_names

        # Merge with explicit global config
        if self.global_ is not None:
            global_cfg.update(self.global_.model_dump(exclude_none=True))

        if global_cfg:
            overrides["global"] = global_cfg

        # Projector section
        projector: dict[str, Any] = {}
        if self.projection_method is not None:
            projector["method"] = self.projection_method
        if self.max_projection_steps is not None:
            projector["max_projection_steps"] = self.max_projection_steps

        # Merge with explicit projector config
        if self.projector is not None:
            projector.update(self.projector.model_dump(exclude_none=True))

        if projector:
            overrides["projector"] = projector

        # Analyzer section
        analyzer: dict[str, Any] = {}
        if self.radar_variables is not None:
            analyzer["radar_variables"] = self.radar_variables
        if self.exclude_fields is not None:
            analyzer["exclude_fields"] = self.exclude_fields

        # Merge with explicit analyzer config
        if self.analyzer is not None:
            analyzer.update(self.analyzer.model_dump(exclude_none=True))

        if analyzer:
            overrides["analyzer"] = analyzer

        # Pass through whole sections that have no typed UserConfig field (tracker,
        # visualization, output, logging, processor, reader). They are deep-merged
        # onto ParamConfig by resolve_config; unknown sub-keys raise at InternalConfig.
        for key, value in (self.model_extra or {}).items():
            if key in _PASSTHROUGH_SECTIONS and isinstance(value, dict):
                overrides[key] = value

        return overrides
