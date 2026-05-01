# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""CLIConfig: Command-line operational overrides.

Minimal configuration for operational parameters that commonly change
between runs: mode, radar ID, output paths, verbosity.

This schema handles command-line arguments parsed by argparse.
"""

from typing import Literal

from pydantic import Field, model_validator

from adapt.configuration.schemas.base import AdaptBaseModel


class CLIConfig(AdaptBaseModel):
    """Command-line configuration overrides.
    
    Operational-only settings that override user and param configs.
    Highest priority in config resolution.
    
    Notes
    -----
    If start_time or end_time are provided but mode is not, mode is
    automatically set to "historical" (schema responsibility, not runtime).
    
    Usage
    -----
        cli_cfg = CLIConfig(
            mode="historical",
            radar="KHTX",
            base_dir="/scratch/adapt_output",
        )
        
        # Or infer historical mode from times:
        cli_cfg = CLIConfig(
            start_time="2025-03-05T00:00:00Z",
            end_time="2025-03-05T23:59:59Z",
            radar="KHTX",
        )
        # mode automatically set to "historical"
        
        internal = resolve_config(param_cfg, user_cfg, cli_cfg)
    """
    
    mode: Literal["realtime", "historical"] | None = None
    radar: str | None = None
    base_dir: str | None = None
    start_time: str | None = None
    end_time: str | None = None
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] | None = None
    run_id: str | None = Field(
        default=None,
        description="Optional run ID for continuation (format: YYYYMONDD-HHMM-RADAR)"
    )
    
    @model_validator(mode="after")
    def infer_historical_mode_from_times(self):
        """If times provided but mode not specified, set mode to historical.
        
        This is a schema responsibility: if CLI args indicate a time range,
        the mode should automatically be historical. Runtime code should not
        make this decision.
        """
        if self.mode is None:
            # Check if either start_time or end_time are provided
            if self.start_time or self.end_time:
                self.mode = "historical"
        
        return self
    
    def to_internal_overrides(self) -> dict:
        """Convert CLI config to internal config structure.
        
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
        
        downloader_overrides = {}
        if self.radar is not None:
            downloader_overrides["radar"] = self.radar
        if self.start_time is not None:
            downloader_overrides["start_time"] = self.start_time
        if self.end_time is not None:
            downloader_overrides["end_time"] = self.end_time
        if self.base_dir is not None:
            downloader_overrides["output_dir"] = str(self.base_dir)
        
        if downloader_overrides:
            overrides["downloader"] = downloader_overrides
        
        if self.log_level is not None:
            overrides["logging"] = {"level": self.log_level}
        
        # base_dir handled separately by setup_output_directories
        
        return overrides
