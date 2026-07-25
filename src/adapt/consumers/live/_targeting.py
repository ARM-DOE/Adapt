# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Display-free helpers behind the dashboard's Target Selection tab.

Column discovery, config assembly, scan-time indexing, and matplotlib
drawing of the reflectivity backdrop + current-target overlay. No Tk here
(mirrors the ``_renderer`` / ``_timeseries`` split) so every function is
unit-testable without a display.
"""

import logging
import math
import re
from collections.abc import Iterable, Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path

import numpy as np
import pandas as pd
import xarray as xr
from matplotlib import colormaps
from matplotlib.lines import Line2D

from adapt.consumers.target_selection import TSEConfig, total_score

logger = logging.getLogger(__name__)

# Key / identifier columns that are never offered as a gate variable.
_NON_GATE_COLUMNS = frozenset({"run_id", "scan_time", "cell_uid", "cell_label", "scan_time_unix"})

# Default priority weights. Growth is kept small: on real data the area-slope
# estimate spikes when cells merge or split, and a large weight lets that noise
# dominate the score (see the KLOT replay notebook).
_DEFAULT_WEIGHTS = {"reflectivity": 1.0, "area": 0.05, "growth_rate": 0.2}
_DEFAULT_PROJECTION_STEPS = 3

# Semantic overlay colours — three distinct, colourblind-safe states.
_OTHER = "#b9b8b2"
_CANDIDATE = "#4e79a7"
_SELECTED = "#e15759"
_INK = "#1a1a1a"

_NC_STAMP = re.compile(r"(\d{8})_(\d{6})")
_PROJECTION_X = re.compile(r"^cell_centroid_projection(\d+)_x$")


def discover_numeric_columns(df: pd.DataFrame) -> list[str]:
    """Numeric ``cells_by_scan`` columns offered as gate variables, sorted.

    Drops keys/identifiers and non-numeric columns. An empty frame yields ``[]``.
    """
    if df is None or df.empty:
        return []
    numeric = df.select_dtypes(include="number").columns
    return sorted(c for c in numeric if c not in _NON_GATE_COLUMNS)


def build_tse_config(
    rules: Iterable[tuple[str, float | None, float | None]],
    *,
    min_age_s: float,
    max_obs_s: float,
    switch_margin: float,
    growth_window: int,
) -> TSEConfig:
    """Assemble a validated ``TSEConfig`` from the tab's rule rows + knobs.

    Each rule is ``(field, min, max)``. A non-null ``min`` becomes a ``>=`` gate
    and a non-null ``max`` a ``<=`` gate, so one row expresses a floor, a ceiling,
    or an inclusive range. Rows with a blank ``field`` (or no bound at all) are
    skipped. Priority weights and site preference use documented defaults (only
    the candidate gates and the listed knobs are user-driven in v0.1). Invalid
    input raises loud (``pydantic.ValidationError``, a ``ValueError``).
    """
    gates: list[dict] = []
    for field, lo, hi in rules:
        if not field:
            continue
        if lo is not None:
            gates.append({"field": field, "op": "ge", "value": float(lo)})
        if hi is not None:
            gates.append({"field": field, "op": "le", "value": float(hi)})
    return TSEConfig.model_validate(
        {
            "candidate": {"gates": gates, "min_age_seconds": float(min_age_s)},
            "priority": {"weights": dict(_DEFAULT_WEIGHTS)},
            "site_preference": {"projection_steps": _DEFAULT_PROJECTION_STEPS, "sites": []},
            "selection": {
                "switch_margin": float(switch_margin),
                "max_observation_seconds": float(max_obs_s),
            },
            "snapshot": {"growth_window_scans": int(growth_window)},
        }
    )


def filter_nc_paths_by_run(nc_paths: Iterable[Path], run_id: str | None) -> list[Path]:
    """Keep only analysis NetCDFs belonging to ``run_id``.

    The pipeline embeds the run id in every analysis filename
    (``…_V06_<run_id>_analysis.nc``), so run membership is a substring test —
    no catalog query. ``run_id`` of ``None`` returns the paths unchanged.
    """
    paths = list(nc_paths)
    if not run_id:
        return paths
    return [p for p in paths if run_id in Path(p).stem]


def nc_index_by_scan(nc_paths: Iterable[Path]) -> dict[datetime, Path]:
    """Map ``scan_time`` → analysis NetCDF path by parsing the filename stamp.

    Filenames look like ``RADAR_YYYYMMDD_HHMMSS_analysis.nc``. Paths without a
    parseable ``YYYYMMDD_HHMMSS`` stamp are ignored.
    """
    index: dict[datetime, Path] = {}
    for path in nc_paths:
        match = _NC_STAMP.search(Path(path).name)
        if not match:
            continue
        d, t = match.group(1), match.group(2)
        stamp = datetime(
            int(d[:4]), int(d[4:6]), int(d[6:8]), int(t[:2]), int(t[2:4]), int(t[4:6]), tzinfo=UTC
        )
        index[stamp] = Path(path)
    return index


def find_nc_for_scan(
    nc_index: Mapping[datetime, Path], scan_ts, tolerance_s: float = 90.0
) -> Path | None:
    """NC path whose filename stamp matches *scan_ts* within *tolerance_s*."""
    if not nc_index:
        return None
    target = pd.Timestamp(scan_ts)
    if target.tzinfo is None:
        target = target.tz_localize("UTC")
    best, best_diff = None, None
    for stamp, path in nc_index.items():
        diff = abs((pd.Timestamp(stamp) - target).total_seconds())
        if diff <= tolerance_s and (best_diff is None or diff < best_diff):
            best, best_diff = path, diff
    return best


def draw_reflectivity_backdrop(ax, ds, var: str = "reflectivity", *, alpha: float = 0.35) -> None:
    """Grayscale reflectivity backdrop in km, mirroring the Latest Scan map."""
    x_km = ds["x"].values / 1000.0
    y_km = ds["y"].values / 1000.0
    refl = ds[var].values.astype(float)
    bg = np.ma.masked_where(np.isnan(refl) | (refl < 10), refl)
    cmap = colormaps["gray_r"].copy()
    cmap.set_bad(alpha=0)
    ax.pcolormesh(
        x_km, y_km, bg, cmap=cmap, vmin=10, vmax=40, shading="auto", alpha=alpha, zorder=2
    )
    ax.set_aspect("equal")
    ax.set_xlabel("km east of radar")
    ax.set_ylabel("km north of radar")


def _pixel_to_km(
    col: float | None, row: float | None, x_metres: np.ndarray, y_metres: np.ndarray
) -> tuple[float, float] | None:
    """Grid pixel (col,row) → (x_km, y_km), matching the tracked-cell overlay."""
    if col is None or row is None or (isinstance(col, float) and math.isnan(col)):
        return None
    ci, ri = int(round(col)), int(round(row))
    if 0 <= ci < len(x_metres) and 0 <= ri < len(y_metres):
        return x_metres[ci] / 1000.0, y_metres[ri] / 1000.0
    return None


def _last_projection_pixel(values: dict[str, float]) -> tuple[float, float] | None:
    """Furthest-ahead projected centroid pixel (col,row), or None if absent."""
    steps = sorted(
        int(m.group(1))
        for key in values
        if (m := _PROJECTION_X.match(key)) and f"cell_centroid_projection{m.group(1)}_y" in values
    )
    for k in reversed(steps):
        px = values.get(f"cell_centroid_projection{k}_x")
        py = values.get(f"cell_centroid_projection{k}_y")
        if px is not None and py is not None and not (math.isnan(px) or math.isnan(py)):
            return px, py
    return None


def draw_target_overlay(ax, ds, snapshot, selection, candidate_uids: Sequence[str] | set) -> None:
    """Overlay every cell on the km grid, highlighting the current target.

    Gray = other, blue = candidate, red ring + label = selected. The selected
    target gets a motion arrow from its mass centroid to its furthest projected
    centroid. An empty trajectory (first scan of a run) draws no arrow.
    """
    x_metres = ds["x"].values
    y_metres = ds["y"].values
    candidate_uids = set(candidate_uids)
    sel_uid = selection.cell_uid if selection is not None else None

    others: list[tuple[float, float]] = []
    cands: list[tuple[float, float]] = []
    sel_pos: tuple[float, float] | None = None
    sel_values: dict[str, float] | None = None

    for cell in snapshot.cells:
        pos = _pixel_to_km(
            cell.values.get("cell_centroid_mass_x"),
            cell.values.get("cell_centroid_mass_y"),
            x_metres,
            y_metres,
        )
        if pos is None:
            continue
        if cell.uid == sel_uid:
            sel_pos, sel_values = pos, cell.values
        elif cell.uid in candidate_uids:
            cands.append(pos)
        else:
            others.append(pos)

    if others:
        ox, oy = zip(*others, strict=True)
        ax.scatter(ox, oy, s=18, color=_OTHER, alpha=0.55, zorder=4)
    if cands:
        cx, cy = zip(*cands, strict=True)
        ax.scatter(
            cx, cy, s=34, color=_CANDIDATE, alpha=0.9, edgecolors="white", linewidths=0.5, zorder=5
        )

    if sel_pos is not None:
        sx, sy = sel_pos
        ax.scatter([sx], [sy], s=110, color=_SELECTED, edgecolors=_INK, linewidths=1.6, zorder=7)
        ax.annotate(
            sel_uid,
            (sx, sy),
            xytext=(7, 6),
            textcoords="offset points",
            color=_INK,
            fontsize=9,
            fontweight="bold",
            zorder=8,
        )
        proj = _last_projection_pixel(sel_values or {})
        tip = _pixel_to_km(proj[0], proj[1], x_metres, y_metres) if proj else None
        if tip is not None:
            ax.annotate(
                "",
                xy=tip,
                xytext=(sx, sy),
                arrowprops={"arrowstyle": "-|>", "color": _SELECTED, "linewidth": 2.0},
                zorder=7,
            )

    # Identity legend — always the same three states, so the colours are named
    # even on a scan where one state happens to be absent.
    handles = [
        Line2D([], [], marker="o", ls="none", mfc=_OTHER, mec=_OTHER, ms=7, label="other cell"),
        Line2D([], [], marker="o", ls="none", mfc=_CANDIDATE, mec="white", ms=8, label="candidate"),
        Line2D(
            [], [], marker="o", ls="none", mfc=_SELECTED, mec=_INK, ms=9, label="selected target"
        ),
    ]
    ax.legend(handles=handles, loc="upper right", fontsize=8, framealpha=0.85)


def draw_tse_map(ax, scan_ts, nc_path: Path | None, snapshot, selection, candidate_uids) -> None:
    """Draw one Target Selection replay frame onto *ax* (clears it first).

    Shared by the live tab canvas and the movie exporter, so exported frames
    match the on-screen replay by construction.
    """
    ax.clear()
    title = pd.Timestamp(scan_ts).strftime("%Y-%m-%d %H:%M:%S UTC")
    if nc_path is not None:
        try:
            with xr.open_dataset(nc_path) as ds:
                draw_reflectivity_backdrop(ax, ds)
                draw_target_overlay(ax, ds, snapshot, selection, candidate_uids)
        except Exception:
            logger.exception("Failed to draw replay frame for %s", nc_path)
            ax.text(0.5, 0.5, "render error", ha="center", transform=ax.transAxes)
    else:
        title += "  (no scan raster)"
        ax.text(
            0.5,
            0.5,
            "no scan raster for this scan",
            ha="center",
            va="center",
            transform=ax.transAxes,
            color="#888",
        )
    ax.set_title(title, fontsize=10)


# ── Selection rationale ──────────────────────────────────────────────────────

_OP_SYMBOL = {"gt": ">", "ge": "≥", "lt": "<", "le": "≤"}


def _score_terms(cell, cfg: TSEConfig) -> list[tuple[str, float, float]]:
    """The (name, value, weight) triples that sum to the priority score."""
    w = cfg.priority.weights
    return [
        ("reflectivity_max", cell.reflectivity_max, w.reflectivity),
        ("area_sqkm", cell.area_sqkm, w.area),
        ("growth_km2/min", cell.growth_rate_sqkm_per_min, w.growth_rate),
    ]


def _gate_summary(cell, candidate_cfg) -> str:
    """Each candidate gate as ``field op value (=cell value)``."""
    parts = []
    for gate in candidate_cfg.gates:
        v = cell.values.get(gate.field)
        shown = f"{v:.1f}" if isinstance(v, (int, float)) and not isinstance(v, bool) else "n/a"
        parts.append(f"{gate.field} {_OP_SYMBOL[gate.op]} {gate.value:g} (={shown})")
    return ", ".join(parts) if parts else "no gates"


def _why_not(score: float, selected_score: float, margin: float) -> str:
    if score <= selected_score:
        return "lower score"
    if score <= selected_score + margin:
        return f"higher but within switch margin ({margin:g})"
    return "higher score"


def format_rationale(snapshot, cfg: TSEConfig, selection, candidate_uids) -> str:
    """Human-readable explanation of this scan's decision.

    For the selected cell: reason, score, the weighted score terms, and the gate
    values that qualified it. For every other qualified candidate (ranked by
    score): its score and why it was not chosen — lower, or higher but inside the
    switch margin. With no selection, the ranked qualified candidates are listed.
    """
    cells = {c.uid: c for c in snapshot.cells}
    ranked = sorted(
        (u for u in set(candidate_uids) if u in cells),
        key=lambda u: total_score(cells[u], cfg),
        reverse=True,
    )

    if selection is None:
        if not ranked:
            return "No target this scan — no qualified candidates."
        lines = [f"No target this scan. {len(ranked)} qualified candidate(s):"]
        lines += [f"  {u}   score {total_score(cells[u], cfg):.1f}" for u in ranked]
        return "\n".join(lines)

    lines = [
        f"SELECTED  {selection.cell_uid}   {selection.reason.value}   score {selection.score:.1f}"
    ]
    sel = cells.get(selection.cell_uid)
    if sel is not None:
        for name, value, weight in _score_terms(sel, cfg):
            lines.append(f"    {name:15s} {value:8.2f} × {weight:<5g} = {value * weight:7.2f}")
        lines.append(f"    gates: {_gate_summary(sel, cfg.candidate)}")

    others = [u for u in ranked if u != selection.cell_uid]
    lines.append("")
    lines.append(f"QUALIFIED, NOT SELECTED ({len(others)}):")
    if not others:
        lines.append("  (none)")
    margin = cfg.selection.switch_margin
    for u in others:
        score = total_score(cells[u], cfg)
        lines.append(f"  {u}   score {score:.1f}  —  {_why_not(score, selection.score, margin)}")
    return "\n".join(lines)
