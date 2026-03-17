"""UserConfig: Forgiving, minimal user-facing configuration.

This schema accepts user inputs in a variety of formats, with aliases
for common naming patterns (e.g., RADAR_ID → radar, MODE → mode).

UserConfig is intentionally minimal - users only specify what they want
to override from the expert defaults. Validation is lenient to accept
both uppercase and lowercase keys, integers where floats are expected, etc.
"""

from typing import Literal, Optional, Any
from pydantic import Field, field_validator, model_validator
from adapt.configuration.schemas.base import AdaptBaseModel


class UserSegmenterConfig(AdaptBaseModel):
    """User-facing segmentation config with aliases."""
    method: Optional[str] = None
    threshold: Optional[float] = None
    min_cellsize_gridpoint: Optional[int] = None
    max_cellsize_gridpoint: Optional[int] = None
    closing_kernel: Optional[tuple[int, int]] = None
    filter_by_size: Optional[bool] = None
    h_maxima: Optional[float] = None

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


class UserGlobalConfig(AdaptBaseModel):
    """User-facing global config."""
    z_level: Optional[float] = None
    var_names: Optional[dict[str, str]] = None
    coord_names: Optional[dict[str, str]] = None

    @field_validator("z_level", mode="before")
    @classmethod
    def coerce_z_level(cls, v):
        """Accept int or float for z_level."""
        if v is not None:
            return float(v)
        return v


class UserProjectorConfig(AdaptBaseModel):
    """User-facing projector config."""
    method: Optional[str] = None
    max_time_interval_minutes: Optional[int] = None
    max_projection_steps: Optional[int] = None
    nan_fill_value: Optional[float] = None
    flow_params: Optional[dict[str, Any]] = None
    min_motion_threshold: Optional[float] = None

    @field_validator("method", mode="before")
    @classmethod
    def normalize_method(cls, v):
        """Normalize method names to lowercase."""
        if isinstance(v, str):
            return v.lower().strip()
        return v


class UserRegridderConfig(AdaptBaseModel):
    """User-facing regridder config."""
    grid_shape: Optional[tuple[int, int, int]] = None
    grid_limits: Optional[tuple[tuple[float, float], tuple[float, float], tuple[float, float]]] = None
    roi_func: Optional[str] = None
    min_radius: Optional[float] = None
    weighting_function: Optional[str] = None
    save_netcdf: Optional[bool] = None


class UserDownloaderConfig(AdaptBaseModel):
    """User-facing downloader config."""
    radar: Optional[str] = None
    output_dir: Optional[str] = None
    latest_files: Optional[int] = None
    latest_minutes: Optional[int] = None
    poll_interval_sec: Optional[int] = None
    start_time: Optional[str] = None
    end_time: Optional[str] = None


class UserAnalyzerConfig(AdaptBaseModel):
    """User-facing analyzer config."""
    radar_variables: Optional[list[str]] = None
    exclude_fields: Optional[list[str]] = None


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
    mode: Optional[Literal["realtime", "historical"]] = Field(None, alias="MODE")
    radar: Optional[str] = Field(None, alias="RADAR_ID")
    base_dir: Optional[str] = Field(None, alias="BASE_DIR")

    # Realtime settings
    latest_files: Optional[int] = Field(None, alias="LATEST_FILES")
    latest_minutes: Optional[int] = Field(None, alias="LATEST_MINUTES")
    poll_interval_sec: Optional[int] = Field(None, alias="POLL_INTERVAL_SEC")

    # Historical settings
    start_time: Optional[str] = Field(None, alias="START_TIME")
    end_time: Optional[str] = Field(None, alias="END_TIME")

    # Grid settings (flat aliases)
    grid_shape: Optional[tuple[int, int, int]] = Field(None, alias="GRID_SHAPE")
    grid_limits: Optional[tuple[tuple[float, float], tuple[float, float], tuple[float, float]]] = Field(None, alias="GRID_LIMITS")

    # Segmentation settings (flat aliases)
    z_level: Optional[float] = Field(None, alias="Z_LEVEL")
    reflectivity_var: Optional[str] = Field(None, alias="REFLECTIVITY_VAR")
    segmentation_method: Optional[str] = Field(None, alias="SEGMENTATION_METHOD")
    threshold: Optional[float] = Field(None, alias="THRESHOLD_DBZ")
    min_cellsize_gridpoint: Optional[int] = Field(None, alias="MIN_CELLSIZE_GRIDPOINT")
    max_cellsize_gridpoint: Optional[int] = Field(None, alias="MAX_CELLSIZE_GRIDPOINT")

    # Projection settings (flat aliases)
    projection_method: Optional[str] = Field(None, alias="PROJECTION_METHOD")
    max_projection_steps: Optional[int] = Field(None, alias="MAX_PROJECTION_STEPS")

    # Analyzer settings (flat aliases)
    radar_variables: Optional[list[str]] = None
    exclude_fields: Optional[list[str]] = None

    # Nested overrides (advanced users)
    downloader: Optional[UserDownloaderConfig] = None
    regridder: Optional[UserRegridderConfig] = None
    segmenter: Optional[UserSegmenterConfig] = None
    global_: Optional[UserGlobalConfig] = Field(None, alias="global")
    projector: Optional[UserProjectorConfig] = None
    analyzer: Optional[UserAnalyzerConfig] = None

    model_config = AdaptBaseModel.model_config.copy()
    # Allow forgiving input dictionaries (ignore unknown legacy keys)
    model_config.update({"populate_by_name": True, "extra": "ignore"})

    @model_validator(mode="after")
    def infer_historical_mode_from_times(self):
        """If times provided but mode not specified, set mode to historical.

        This is a schema responsibility: if user config indicates a time range,
        the mode should automatically be historical.
        """
        if self.mode is None:
            # Check top-level times
            if self.start_time and self.end_time:
                self.mode = "historical"
            # Check nested downloader times
            elif self.downloader and (self.downloader.start_time and self.downloader.end_time):
                self.mode = "historical"

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
        overrides = {}

        if self.mode is not None:
            overrides["mode"] = self.mode

        if self.base_dir is not None:
            overrides["base_dir"] = str(self.base_dir)

        # Downloader section
        downloader = {}
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
        regridder = {}
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
        segmenter = {}
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
        global_cfg = {}
        if self.z_level is not None:
            global_cfg["z_level"] = self.z_level

        if self.reflectivity_var is not None:
            var_names = global_cfg.get("var_names", {})
            var_names["reflectivity"] = self.reflectivity_var
            global_cfg["var_names"] = var_names

        # Merge with explicit global config
        if self.global_ is not None:
            global_cfg.update(self.global_.model_dump(exclude_none=True))

        if global_cfg:
            overrides["global"] = global_cfg

        # Projector section
        projector = {}
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
        analyzer = {}
        if self.radar_variables is not None:
            analyzer["radar_variables"] = self.radar_variables
        if self.exclude_fields is not None:
            analyzer["exclude_fields"] = self.exclude_fields

        # Merge with explicit analyzer config
        if self.analyzer is not None:
            analyzer.update(self.analyzer.model_dump(exclude_none=True))

        if analyzer:
            overrides["analyzer"] = analyzer

        return overrides
