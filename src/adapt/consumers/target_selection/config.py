# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Frozen YAML configuration for the target selection engine.

Every threshold and weight lives here. Missing or unknown keys raise;
the only optional section is ``output``.
"""

from pathlib import Path
from typing import Literal

import yaml
from pydantic import BaseModel, ConfigDict, Field

_FROZEN = ConfigDict(frozen=True, extra="forbid")

GateOp = Literal["gt", "ge", "lt", "le"]


class QualityGate(BaseModel):
    """Pluggable quality gate: any numeric snapshot column compared to a value.

    ``op`` selects the comparison — gt (>), ge (>=), lt (<), le (<=) — so a gate
    can express both a floor and a ceiling. There is no default: the operator is
    always explicit.
    """

    model_config = _FROZEN

    field: str
    op: GateOp
    value: float


class CandidateConfig(BaseModel):
    """A cell is a candidate only if every gate passes and it is old enough."""

    model_config = _FROZEN

    gates: tuple[QualityGate, ...]
    min_age_seconds: float


class PriorityWeights(BaseModel):
    """Weights carry units: points per dBZ, per km2, per km2/min."""

    model_config = _FROZEN

    reflectivity: float
    area: float
    growth_rate: float


class PriorityConfig(BaseModel):
    model_config = _FROZEN

    weights: PriorityWeights


class SiteConfig(BaseModel):
    model_config = _FROZEN

    name: str
    lat: float
    lon: float
    radius_km: float
    bonus: float


class SitePreferenceConfig(BaseModel):
    model_config = _FROZEN

    projection_steps: int = Field(ge=1)
    sites: tuple[SiteConfig, ...]


class SelectionConfig(BaseModel):
    model_config = _FROZEN

    switch_margin: float
    max_observation_seconds: float


class SnapshotConfig(BaseModel):
    model_config = _FROZEN

    growth_window_scans: int = Field(ge=2)


class OutputConfig(BaseModel):
    model_config = _FROZEN

    jsonl_path: str | None = None


class TSEConfig(BaseModel):
    model_config = _FROZEN

    candidate: CandidateConfig
    priority: PriorityConfig
    site_preference: SitePreferenceConfig
    selection: SelectionConfig
    snapshot: SnapshotConfig
    output: OutputConfig = OutputConfig()


def load_config(path: str | Path) -> TSEConfig:
    """Load and validate a TSE YAML config. Raises on any invalid content."""
    with open(path, encoding="utf-8") as f:
        raw = yaml.safe_load(f)
    if not isinstance(raw, dict):
        raise ValueError(f"Config file {path} is empty or not a mapping")
    return TSEConfig.model_validate(raw)
