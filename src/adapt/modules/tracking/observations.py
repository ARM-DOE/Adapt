# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""What the tracker observes and predicts, and the evidence of each pair.

Cells of the current scan, tracks of the previous one, their footprints at the
current scan (live: the advected cell; latent: carried by the flow), and every
candidate pair's overlap, residual, speed, heading and cost with the first gate
it failed. Positions are metres ``(x, y)``; shapes are grid-index masks.
"""

import math
from dataclasses import dataclass

import numpy as np
import pandas as pd
import xarray as xr

from adapt.contracts import stat_column
from adapt.modules.tracking.config import TrackingConfig
from adapt.modules.tracking.latent import LatentTrack
from adapt.modules.tracking.lineage import identity_score
from adapt.modules.tracking.matching.gates import SpeedGate, heading_term, speed_gate
from adapt.modules.tracking.matching.geometry import (
    PairOverlap,
    cell_intensity,
    core_centre,
    footprint_pairs,
    growth_radius_km,
    mahalanobis,
    second_moment,
)

__all__ = [
    "GATE_COST",
    "GATE_OVERLAP",
    "GATE_PASS",
    "Cell",
    "Footprint",
    "Grid",
    "Pair",
    "Track",
    "evaluate_pairs",
    "extract_cells",
    "latent_footprint",
    "live_footprint",
]

GATE_PASS = "PASS"
GATE_OVERLAP = "OVERLAP"
GATE_COST = "COST"

Point = tuple[float, float]


@dataclass(frozen=True)
class Grid:
    """Regular grid: origin and spacing (metres) of columns (x) and rows (y)."""

    x0: float
    y0: float
    sx: float
    sy: float

    @classmethod
    def of(cls, ds: xr.Dataset) -> "Grid":
        x = np.asarray(ds.x.values, dtype=float)
        y = np.asarray(ds.y.values, dtype=float)
        return cls(float(x[0]), float(y[0]), float(x[1] - x[0]), float(y[1] - y[0]))

    @property
    def spacing_m(self) -> float:
        """One grid length σ_x: the position error the gates allow for."""
        return min(abs(self.sx), abs(self.sy))

    @property
    def pixel_km2(self) -> float:
        return abs(self.sx * self.sy) / 1e6

    def to_m(self, row: float, col: float) -> Point:
        return self.x0 + col * self.sx, self.y0 + row * self.sy

    def delta_px(self, start: Point, end: Point) -> np.ndarray:
        """``end − start`` in (row, col) pixels, the frame of a shape's moment."""
        return np.array([(end[1] - start[1]) / self.sy, (end[0] - start[0]) / self.sx])


def _geometric_centre(mask: np.ndarray, grid: Grid) -> Point:
    rows, cols = np.nonzero(mask)
    return grid.to_m(float(rows.mean()), float(cols.mean()))


@dataclass(frozen=True, eq=False)
class Cell:
    """A current cell and the quantities the decisions use."""

    label: int
    mask: np.ndarray
    area_km2: float
    mass: float
    mean_excess: float
    score: float
    centre: Point  # e²-weighted
    geom: Point  # geometric
    core_area_km2: float
    mean_field: float
    max_field: float


def extract_cells(
    ds: xr.Dataset, stats: pd.DataFrame, cfg: TrackingConfig, grid: Grid
) -> list[Cell]:
    """Cells of one scan, in label order, with the configured field's stats."""
    labels = ds[cfg.labels_var].values
    field = ds[cfg.field_var].values
    mean_col = stat_column(cfg.field_var, "mean")
    max_col = stat_column(cfg.field_var, "max")
    if not stats.empty and (mean_col not in stats.columns or max_col not in stats.columns):
        raise ValueError(
            f"Tracking: cell_stats is missing {mean_col!r}/{max_col!r} for the configured "
            f"tracking field {cfg.field_var!r}. The analyzer whitelist "
            "(analyzer.radar_variables) must include it."
        )
    by_label = {int(r["cell_label"]): r for _, r in stats.iterrows()}
    cells = []
    for label in np.unique(labels[labels > 0]):
        if int(label) not in by_label:
            raise ValueError(f"Tracking: cell {int(label)} is in the labels but not in cell_stats")
        mask = labels == label
        area = float(np.count_nonzero(mask)) * grid.pixel_km2
        mass, mean_excess = cell_intensity(field, mask, cfg.cell_threshold, cfg.polarity)
        row, col = core_centre(field, mask, cfg.cell_threshold, cfg.polarity)
        core = (
            cfg.polarity
            * (np.nan_to_num(field[mask], nan=cfg.core_field_threshold) - cfg.core_field_threshold)
            > 0
        )
        cells.append(
            Cell(
                label=int(label),
                mask=mask,
                area_km2=area,
                mass=mass,
                mean_excess=mean_excess,
                score=identity_score(area, mean_excess, cfg.identity_intensity_weight),
                centre=grid.to_m(row, col),
                geom=_geometric_centre(mask, grid),
                core_area_km2=float(np.count_nonzero(core)) * grid.pixel_km2,
                mean_field=float(by_label[int(label)][mean_col]),
                max_field=float(by_label[int(label)][max_col]),
            )
        )
    return cells


@dataclass(frozen=True)
class Track:
    """A live track as last observed (the previous scan)."""

    uid: str
    label: int
    scan_id: str
    scan_time: object
    time_s: float
    score: float
    centre: Point
    geom: Point
    step: Point | None  # last displacement per scan interval
    speed: float | None  # last speed


@dataclass(frozen=True, eq=False)
class Footprint:
    """Where a track is expected at the current scan, and its motion so far.

    ``origin`` is the last observed centre, ``predicted`` the expected one;
    ``sigma`` is the footprint's second moment (px²); ``steps``/``dt_s`` span
    the link from the last observation.
    """

    uid: str
    label: int
    source: str  # LIVE | LATENT
    steps: int
    dt_s: float
    mask: np.ndarray
    radius_km: float
    sigma: np.ndarray | None
    predicted: Point
    origin: Point
    step: Point | None
    speed: float | None


def _footprint(mask, grid, cfg, **kw) -> Footprint:
    area = float(np.count_nonzero(mask)) * grid.pixel_km2
    return Footprint(
        mask=mask,
        radius_km=growth_radius_km(area, cfg.growth_max_km, cfg.growth_size_km),
        sigma=second_moment(mask) if mask.any() else None,
        **kw,
    )


def live_footprint(
    track: Track, proj_labels: np.ndarray, grid: Grid, dt_s: float, cfg: TrackingConfig
) -> Footprint:
    """The previous cell advected by the flow; its centre moves as the shape did."""
    mask = proj_labels == track.label
    predicted = track.centre
    if mask.any():
        gx, gy = _geometric_centre(mask, grid)
        predicted = (track.centre[0] + gx - track.geom[0], track.centre[1] + gy - track.geom[1])
    return _footprint(
        mask,
        grid,
        cfg,
        uid=track.uid,
        label=track.label,
        source="LIVE",
        steps=1,
        dt_s=dt_s,
        predicted=predicted,
        origin=track.centre,
        step=track.step,
        speed=track.speed,
    )


def latent_footprint(
    latent: LatentTrack, time_s: float, grid: Grid, cfg: TrackingConfig
) -> Footprint:
    return _footprint(
        latent.footprint,
        grid,
        cfg,
        uid=latent.uid,
        label=latent.label,
        source="LATENT",
        steps=latent.age,
        dt_s=time_s - latent.time_s,
        predicted=latent.predicted,
        origin=latent.centre,
        step=latent.step,
        speed=latent.speed,
    )


@dataclass(frozen=True)
class Pair:
    """Evidence for one footprint–cell pair and the first gate it failed."""

    fp: Footprint
    cell: Cell
    overlap: PairOverlap
    residual_m: float
    d: float
    speed: float
    speed_gate: SpeedGate
    h: float
    turn_deg: float | None
    cost: float
    gate: str

    @property
    def displacement(self) -> Point:
        return (self.cell.centre[0] - self.fp.origin[0], self.cell.centre[1] - self.fp.origin[1])


def evaluate_pairs(
    fp: Footprint, cells: dict[int, Cell], labels: np.ndarray, grid: Grid, cfg: TrackingConfig
) -> list[Pair]:
    """Every cell whose grown shape meets the grown footprint, with its evidence.

    Gates in order: OVERLAP (u > u_max), SPEED / SPEED_CHANGE, COST (c > c_max).
    """
    if fp.sigma is None:  # the footprint left the domain: nothing to compare
        return []
    pairs = []
    radius_px = fp.radius_km * 1000.0 / grid.spacing_m
    for overlap in footprint_pairs(fp.mask, labels, radius_px):
        cell = cells[overlap.label]
        residual = grid.delta_px(fp.predicted, cell.centre)
        d = mahalanobis(residual, fp.sigma)
        disp = (cell.centre[0] - fp.origin[0], cell.centre[1] - fp.origin[1])
        speed = math.hypot(*disp) / fp.dt_s
        gate = speed_gate(
            speed, fp.speed, fp.dt_s, cfg.max_speed_ms, cfg.max_acceleration_ms2, grid.spacing_m
        )
        per_step = (disp[0] / fp.steps, disp[1] / fp.steps)
        h, turn = heading_term(fp.step, per_step, cfg.heading_weight, 2.0 * grid.spacing_m)
        cost = overlap.u + cfg.residual_weight * d + h
        if overlap.u > cfg.max_overlap_mismatch:
            code = GATE_OVERLAP
        elif gate.code is not None:
            code = gate.code
        elif cost > cfg.max_link_cost:
            code = GATE_COST
        else:
            code = GATE_PASS
        pairs.append(
            Pair(
                fp=fp,
                cell=cell,
                overlap=overlap,
                residual_m=math.hypot(
                    cell.centre[0] - fp.predicted[0], cell.centre[1] - fp.predicted[1]
                ),
                d=d,
                speed=speed,
                speed_gate=gate,
                h=h,
                turn_deg=turn,
                cost=cost,
                gate=code,
            )
        )
    return pairs
