# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Synthetic factories shared by the target-selection tests."""

from adapt.consumers.target_selection.config import TSEConfig
from adapt.consumers.target_selection.snapshot import CellSnapshot, TrajectoryPoint


def make_cell(
    uid: str = "a",
    *,
    lat: float = 35.0,
    lon: float = -97.0,
    area: float = 100.0,
    refl: float = 50.0,
    age: float = 700.0,
    growth: float = 0.0,
    trajectory: tuple[TrajectoryPoint, ...] = (),
    values: dict[str, float] | None = None,
) -> CellSnapshot:
    return CellSnapshot(
        uid=uid,
        lat=lat,
        lon=lon,
        area_sqkm=area,
        reflectivity_max=refl,
        age_seconds=age,
        growth_rate_sqkm_per_min=growth,
        trajectory=trajectory,
        values=values if values is not None else {"n_scans": 5.0},
    )


def make_config(
    *,
    gates: tuple[dict, ...] = ({"field": "n_scans", "op": "ge", "value": 3.0},),
    min_age_seconds: float = 600.0,
    w_reflectivity: float = 1.0,
    w_area: float = 0.05,
    w_growth: float = 2.0,
    projection_steps: int = 5,
    sites: tuple[dict, ...] = (),
    switch_margin: float = 5.0,
    max_observation_seconds: float = 1800.0,
    growth_window_scans: int = 4,
    jsonl_path: str | None = None,
) -> TSEConfig:
    return TSEConfig.model_validate(
        {
            "candidate": {
                "gates": list(gates),
                "min_age_seconds": min_age_seconds,
            },
            "priority": {
                "weights": {
                    "reflectivity": w_reflectivity,
                    "area": w_area,
                    "growth_rate": w_growth,
                }
            },
            "site_preference": {
                "projection_steps": projection_steps,
                "sites": list(sites),
            },
            "selection": {
                "switch_margin": switch_margin,
                "max_observation_seconds": max_observation_seconds,
            },
            "snapshot": {"growth_window_scans": growth_window_scans},
            "output": {"jsonl_path": jsonl_path},
        }
    )
