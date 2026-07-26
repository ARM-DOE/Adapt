# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Radar scan-map rendering — pure matplotlib, no Tk, no pyplot.

The live canvas and the movie exporter draw every frame through
:func:`render_scan`, so an exported movie is pixel-identical to the on-screen
view for the same :class:`ViewState`.
"""

import logging
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import cmweather  # noqa: F401 — registers ChaseSpectral and other radar colormaps
import matplotlib.lines as mlines
import matplotlib.patches as mpatches
import numpy as np
import pandas as pd
import xarray as xr
from matplotlib import colormaps
from matplotlib.figure import Figure

from adapt.consumers.live._utils import _centroid_track_to_km, _visible_uids_in_scan

logger = logging.getLogger(__name__)

try:
    import contextily as ctx

    HAS_CTX = True
except ImportError:
    ctx = None
    HAS_CTX = False


# ── Variable selector defaults: (vmin, vmax, unit, cmap) ─────────────────────
_VAR_DEFAULTS = {
    "reflectivity": (10, 60, "dBZ", "ChaseSpectral"),
    "differential_reflectivity": (-2, 8, "dB", "RdYlBu_r"),
    "velocity": (-30, 30, "m/s", "RdBu_r"),
    "spectrum_width": (0, 15, "m/s", "plasma"),
}
_VAR_LABELS = {
    "reflectivity": "Reflectivity",
    "differential_reflectivity": "ZDR",
    "velocity": "Velocity",
    "spectrum_width": "Spec Width",
}


@dataclass(frozen=True)
class ViewState:
    """Frozen snapshot of every control shaping the Latest Scan map."""

    var_name: str
    vmin: float
    vmax: float
    bg_alpha: float
    max_proj_steps: int  # 0 = all projection frames
    show_flow: bool
    zoom: tuple[tuple[float, float], tuple[float, float]] | None
    selected_cells: Mapping[str, int]  # uid → color slot
    color_slots: tuple[str, ...]


@dataclass(frozen=True)
class OverlayData:
    """Per-run cell data behind the selected-cell overlays.

    Histories arrive as data, never I/O: the live tab fetches them per redraw,
    the movie exporter once per selected uid — the drawing is identical.
    """

    cell_df: pd.DataFrame | None
    track_histories: Mapping[str, pd.DataFrame]


@dataclass
class RenderResult:
    cell_contours: dict[int, Any]  # label → ContourSet, for click/hover hit-testing
    track_overlays: dict[str, list]  # uid → removable artists
    scan_ts: pd.Timestamp


def _scan_timestamp(ds: xr.Dataset) -> pd.Timestamp:
    if "time" not in ds.coords:
        raise ValueError("Dataset has no 'time' coordinate — not an analysis scan file")
    tv = ds.coords["time"].values
    return pd.Timestamp(tv.item() if np.ndim(tv) == 0 else tv[0])


def _masked_cmap(name: str):
    cmap = colormaps[name].copy()
    cmap.set_bad(alpha=0)
    return cmap


def data_extent_km(ds: xr.Dataset) -> tuple[tuple[float, float], tuple[float, float]]:
    """Full ``((xmin, xmax), (ymin, ymax))`` map extent in km from the grid.

    This is the authoritative "home" view for the toolbar Home button — the
    dataset's own bounds, never a matplotlib nav-stack snapshot (which can hold
    a zoomed view after the canvas was rebuilt while zoomed).
    """
    x_km = ds["x"].values / 1000.0
    y_km = ds["y"].values / 1000.0
    return (float(x_km.min()), float(x_km.max())), (float(y_km.min()), float(y_km.max()))


def render_scan(
    ax, cbar_ax, ds: xr.Dataset, view: ViewState, overlays: OverlayData
) -> RenderResult:
    """Draw one scan onto *ax* (map) and *cbar_ax* (colorbar). Clears both."""
    ax.clear()
    ax.set_facecolor("white")

    radar_id = ds.attrs.get("radar", ds.attrs.get("radar_id", ""))
    ts = _scan_timestamp(ds)
    tstr = ts.strftime("%Y-%m-%d %H:%M:%S UTC")

    x_km = ds["x"].values / 1000.0
    y_km = ds["y"].values / 1000.0
    y_grid, x_grid = np.meshgrid(y_km, x_km, indexing="ij")
    labels_data = ds["cell_labels"].values

    # ── Grayscale reflectivity background ────────────────────────────────
    refl = ds["reflectivity"].values.astype(float)
    refl_bg = np.ma.masked_where(np.isnan(refl) | (refl < 10), refl)
    # vmin=10 → light gray (~0.35 on gray_r), vmax=40 → black
    ax.pcolormesh(
        x_km,
        y_km,
        refl_bg,
        cmap=_masked_cmap("gray_r"),
        vmin=10,
        vmax=40,
        shading="auto",
        alpha=view.bg_alpha,
        zorder=2,
    )

    # ── Selected variable overlay (cells only) ────────────────────────────
    var_name = view.var_name if view.var_name in ds.data_vars else "reflectivity"
    vdef = _VAR_DEFAULTS.get(var_name, (10, 60, "dBZ", "viridis"))
    unit = vdef[2]
    var_lbl = _VAR_LABELS.get(var_name, var_name)

    raw = ds[var_name].values.astype(float)
    masked = np.ma.masked_where(np.isnan(raw) | (labels_data <= 0), raw)
    im_ov = ax.pcolormesh(
        x_km,
        y_km,
        masked,
        cmap=_masked_cmap(vdef[3]),
        vmin=view.vmin,
        vmax=view.vmax,
        shading="auto",
        alpha=0.90,
        zorder=3,
    )

    # Reset the axes locator before each colorbar creation. cla() leaves
    # _axes_locator intact; each new colorbar wraps the previous locator in
    # _ColorbarAxesLocator, building a chain that causes RecursionError
    # after ~1000 redraws.
    cbar_ax.set_axes_locator(None)
    ax.figure.colorbar(im_ov, cax=cbar_ax, label=unit)

    # ── Cell contours ─────────────────────────────────────────────────────
    cell_contours: dict[int, Any] = {}
    for cell_id in np.unique(labels_data[labels_data > 0]):
        cs = ax.contour(
            x_grid,
            y_grid,
            (labels_data == cell_id).astype(float),
            levels=[0.8],
            colors="#2C3539",
            linewidths=0.5,
            zorder=50,
        )
        cell_contours[int(cell_id)] = cs

    # ── Projection contours ───────────────────────────────────────────────
    if "cell_projections" in ds.data_vars:
        proj_da = ds["cell_projections"]
        fo = "frame_offset"
        if fo in proj_da.dims:
            n_frames = len(proj_da[fo])
            end_frame = (
                n_frames if view.max_proj_steps == 0 else min(n_frames, view.max_proj_steps + 1)
            )
            _ls_cycle = ["dashed", "dashdot", "dotted"]
            for i in range(1, end_frame):
                alpha = max(0.5, 1.0 - i / n_frames)
                lw = max(0.7, 1.6 - i * 0.2)
                ls = _ls_cycle[(i - 1) % len(_ls_cycle)]
                lp = proj_da.isel({fo: i}).values
                for cid in np.unique(lp[~np.isnan(lp) & (lp > 0)]):
                    ax.contour(
                        x_grid,
                        y_grid,
                        (lp == cid).astype(float),
                        levels=[0.5],
                        colors="#2C3539",
                        linewidths=lw,
                        linestyles=ls,
                        alpha=alpha,
                        zorder=40,
                    )

    # ── Optical flow vectors (toggle) ─────────────────────────────────────
    if view.show_flow and "heading_x" in ds.data_vars and "heading_y" in ds.data_vars:
        hx, hy = ds["heading_x"].values, ds["heading_y"].values
        if not np.all(np.isnan(hx)):
            s = 12
            yi_idx = np.arange(0, len(y_km), s)
            xi_idx = np.arange(0, len(x_km), s)
            xs, ys = np.meshgrid(x_km[xi_idx], y_km[yi_idx])
            ax.quiver(
                xs,
                ys,
                hx[np.ix_(yi_idx, xi_idx)],
                hy[np.ix_(yi_idx, xi_idx)],
                color="#5E7F94",
                alpha=0.7,
                scale=0.5,
                scale_units="xy",
                width=0.002,
                headwidth=4,
                zorder=45,
            )

    add_basemap(ax, ds, x_km, y_km)
    ax.set_xlabel("X (km)")
    ax.set_ylabel("Y (km)")
    ax.tick_params(reset=True)
    ax.grid(True, alpha=0.3, zorder=3)
    ax.set_title(f"{radar_id}  {var_lbl} [{tstr}]", fontsize=11, fontweight="bold")

    legend_handles = [
        mpatches.Patch(facecolor="gray", alpha=0.6, label="Stratiform"),
        mlines.Line2D([], [], color="#2C3539", linewidth=0.8, label="Cell boundary"),
        mlines.Line2D(
            [], [], color="#2C3539", linewidth=1.2, linestyle="dashed", label="Projection"
        ),
        mlines.Line2D([], [], color="cyan", linewidth=1.5, marker="o", markersize=4, label="Track"),
        mlines.Line2D(
            [],
            [],
            color="#8aff9c",
            marker="*",
            markersize=8,
            linestyle="None",
            label="Current Centroid",
        ),
    ]
    ax.legend(
        handles=legend_handles,
        loc="upper center",
        bbox_to_anchor=(0.5, -0.08),
        ncol=len(legend_handles),
        fontsize=10,
        framealpha=0.6,
        borderpad=0.4,
        columnspacing=1.0,
    )

    if view.zoom is not None:
        ax.set_xlim(view.zoom[0])
        ax.set_ylim(view.zoom[1])

    track_overlays = draw_track_overlays(ax, ds, view, overlays)
    return RenderResult(cell_contours=cell_contours, track_overlays=track_overlays, scan_ts=ts)


def _scan_window_rows(df: pd.DataFrame, scan_ts: pd.Timestamp) -> pd.DataFrame:
    """Rows of *df* whose scan_time is within 60 s of *scan_ts*."""
    if "scan_time" not in df.columns:
        return df
    st = pd.to_datetime(df["scan_time"], utc=True)
    ts = scan_ts if scan_ts.tzinfo is not None else scan_ts.tz_localize("UTC")
    return df[(st - ts).abs() < pd.Timedelta(seconds=60)]


def draw_track_overlays(
    ax, ds: xr.Dataset, view: ViewState, overlays: OverlayData
) -> dict[str, list]:
    """Draw track path + current-centroid star for every selected cell.

    Pure drawing — the caller removes any previous overlay artists.
    """
    result: dict[str, list] = {}
    if not view.selected_cells:
        return result

    cell_labels_da = ds.get("cell_labels", ds.get("labels", None))
    if cell_labels_da is None:
        return result
    labels_arr = cell_labels_da.values
    scan_ts = _scan_timestamp(ds)

    # label int → cell_uid for this scan
    uid_map: dict[int, str] = {}
    cell_df = overlays.cell_df
    if (
        cell_df is not None
        and not cell_df.empty
        and "cell_label" in cell_df.columns
        and "cell_uid" in cell_df.columns
    ):
        scan_df = _scan_window_rows(cell_df, scan_ts)
        uid_map = dict(zip(scan_df["cell_label"].astype(int), scan_df["cell_uid"], strict=False))
    visible = _visible_uids_in_scan(labels_arr, uid_map)

    x_metres = ds["x"].values
    y_metres = ds["y"].values

    for uid, slot in view.selected_cells.items():
        color = view.color_slots[slot % len(view.color_slots)]
        artists = []

        history_df = overlays.track_histories.get(uid)
        if (
            (history_df is None or history_df.empty)
            and cell_df is not None
            and "cell_uid" in cell_df.columns
        ):
            history_df = cell_df[cell_df["cell_uid"] == uid].copy()

        # Track path (line + dots)
        if (
            history_df is not None
            and not history_df.empty
            and "cell_centroid_mass_x" in history_df.columns
        ):
            track_df = history_df.dropna(
                subset=["cell_centroid_mass_x", "cell_centroid_mass_y"]
            ).sort_values("scan_time")
            if not track_df.empty:
                x_arr, y_arr = _centroid_track_to_km(track_df, x_metres, y_metres)
                (line,) = ax.plot(
                    x_arr, y_arr, "-", color=color, linewidth=1.5, alpha=0.85, zorder=10
                )
                dots = ax.scatter(x_arr, y_arr, s=14, color=color, zorder=11, alpha=0.7)
                artists.extend([line, dots])

        # Current-scan star only when the cell is present in this scan
        if uid in visible and cell_df is not None and "cell_uid" in cell_df.columns:
            scan_rows = _scan_window_rows(cell_df, scan_ts)
            scan_rows = scan_rows[scan_rows["cell_uid"] == uid]
            if not scan_rows.empty:
                cur = scan_rows.iloc[0]
                cx = cur.get("cell_centroid_mass_x")
                cy = cur.get("cell_centroid_mass_y")
                if cx is not None and cy is not None and pd.notna(cx) and pd.notna(cy):
                    col_i = int(cx)
                    row_i = int(cy)
                    if 0 <= col_i < len(x_metres) and 0 <= row_i < len(y_metres):
                        star = ax.scatter(
                            [x_metres[col_i] / 1000.0],
                            [y_metres[row_i] / 1000.0],
                            s=120,
                            color=color,
                            marker="*",
                            zorder=12,
                        )
                        artists.append(star)

        if artists:
            result[uid] = artists
    return result


def scan_frame_drawer(
    nc_paths: Sequence[Path], view: ViewState, overlays: OverlayData
) -> Callable[[Figure, int], None]:
    """Return a movie draw_frame callable rendering nc_paths[i] via render_scan."""

    def draw(fig: Figure, i: int) -> None:
        gs = fig.add_gridspec(1, 2, width_ratios=[1, 0.045])
        ax = fig.add_subplot(gs[0])
        cbar_ax = fig.add_subplot(gs[1])
        ds = xr.open_dataset(nc_paths[i])
        try:
            render_scan(ax, cbar_ax, ds, view, overlays)
        finally:
            ds.close()

    return draw


def _basemap_extent_key(x_km, y_km) -> tuple[float, float, float, float]:
    """Quantized (xmin, xmax, ymin, ymax) identifying a basemap extent.

    Two renders of the same view produce the same key, so cached tiles are
    reused instead of re-fetched; a zoom (new extent) yields a new key.
    """
    return (
        round(float(x_km.min()), 1),
        round(float(x_km.max()), 1),
        round(float(y_km.min()), 1),
        round(float(y_km.max()), 1),
    )


def add_basemap(ax, ds, x_km, y_km) -> None:
    """Add an OpenStreetMap basemap to *ax* using the dataset's radar location.

    Tiles are fetched at most once per extent and cached on the axes, then
    re-drawn from that cache on later frames: every ``render_scan`` clears the
    axes, so without the cache a redraw loop would re-hit the network (and open
    fresh sockets) on every frame. No-op when contextily is unavailable or the
    dataset has no lat/lon attrs.
    """
    if not HAS_CTX:
        return

    if ds is None:
        return

    lat = ds.attrs.get("radar_latitude", ds.attrs.get("origin_latitude"))
    lon = ds.attrs.get("radar_longitude", ds.attrs.get("origin_longitude"))

    if lat is None or lon is None:
        radar_id = ds.attrs.get("radar", ds.attrs.get("radar_id", ""))
        logger.debug("No radar location for %s — skipping basemap", radar_id)
        return

    lat, lon = float(lat), float(lon)
    ax.set_xlim(x_km.min(), x_km.max())
    ax.set_ylim(y_km.min(), y_km.max())

    key = _basemap_extent_key(x_km, y_km)
    cached = getattr(ax, "_adapt_basemap", None)
    if cached is not None and cached[0] == key:
        _, array, extent, origin = cached
        # aspect=ax.get_aspect() mirrors contextily (GH251): a bare imshow would
        # force aspect='equal' and squash the radar panel on every redraw.
        ax.imshow(array, extent=extent, origin=origin, aspect=ax.get_aspect(), alpha=0.6, zorder=0)
        return

    crs_str = f"+proj=aeqd +lat_0={lat} +lon_0={lon} +x_0=0 +y_0=0 +datum=WGS84 +units=km"
    before = list(ax.images)
    try:
        ctx.add_basemap(
            ax,
            crs=crs_str,
            source=ctx.providers.OpenStreetMap.Mapnik,
            alpha=0.6,
            attribution=False,
            zoom=8,
            zorder=0,
        )
    except Exception as e:
        logger.warning("Basemap unavailable: %s", e)
        return

    added = [im for im in ax.images if im not in before]
    if added:
        im = added[-1]
        ax._adapt_basemap = (key, im.get_array(), im.get_extent(), im.origin)
