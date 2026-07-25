# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Latest Scan tab — scan browser and loop, radar map canvas, cell selection,
hover stats, and the per-track time-series panels.

Rendering goes through the pure _renderer module (shared with the movie
exporter); repository reads go through the AppContext's RepositoryClient.
"""

import contextlib
import logging
import tkinter as tk
from pathlib import Path
from tkinter import messagebox, simpledialog, ttk

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import xarray as xr
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg

from adapt.consumers.live._context import AppContext
from adapt.consumers.live._lightning import merge_lightning as _merge_lightning_fn
from adapt.consumers.live._movie import MovieSource, MovieSpec
from adapt.consumers.live._renderer import (
    _VAR_DEFAULTS,
    OverlayData,
    ViewState,
    draw_track_overlays,
    render_scan,
    scan_frame_drawer,
)
from adapt.consumers.live._timeseries import (
    apply_time_axis as _apply_time_axis_fn,
)
from adapt.consumers.live._timeseries import (
    build_ts_title as _build_ts_title_fn,
)
from adapt.consumers.live._timeseries import (
    clear_time_series as _clear_time_series_fn,
)
from adapt.consumers.live._timeseries import (
    draw_scan_marker as _draw_scan_marker_fn,
)
from adapt.consumers.live._timeseries import (
    style_ts_ax as _style_ts_ax_fn,
)
from adapt.consumers.live._timeseries import (
    update_track_legend as _update_track_legend_fn,
)
from adapt.consumers.live._utils import (
    _apply_overflow_action,
    _cell_uid_disp,
    _next_free_color_slot,
)
from adapt.consumers.live._volume_stats import (
    load_track_volume_stats as _load_track_volume_stats_fn,
)
from adapt.consumers.live._volume_stats import (
    merge_volume_stats as _merge_volume_stats_fn,
)
from adapt.consumers.live._widgets import _CompactToolbar

logger = logging.getLogger(__name__)

# ── Stats strip theme ─────────────────────────────────────────────────────────
_STRIP_BG = "#252526"  # very dark gray — readable on any system theme
_BOX_BG = "#1e1e1e"  # slightly darker for individual boxes
_FONT_VAL = ("Courier", 15, "bold")
_FONT_LBL = ("Courier", 12)
# Each row: (top_label, hv_key_top, top_fg, bot_label, hv_key_bot, bot_fg)
# Lat(M)/Lon(M) removed — mouse coords are shown in toolbar coordinate bar
_BOX_DEFS = [
    ("Cell", "cell_uid", "#ffffff", "Area km²", "area", "#ffff44"),
    ("Lat(C)", "lat_mass", "#44ff88", "Lon(C)", "lon_mass", "#44ff88"),
    ("dBZ mean", "dbz_mean", "#ff8800", "dBZ max", "dbz_max", "#ffcc44"),
    ("ZDR mean", "zdr_mean", "#ff44ff", "ZDR max", "zdr_max", "#ff88ff"),
    ("Age", "age", "#aaffaa", "Vel mean", "vel_mean", "#44ffff"),
]
_HV_KEYS = (
    "cell_uid",
    "area",
    "lat_mass",
    "lon_mass",
    "dbz_mean",
    "dbz_max",
    "zdr_mean",
    "zdr_max",
    "age",
    "vel_mean",
    "sw_mean",
)

# Plot-group variables with these prefixes come from the cell_volume_stats
# enrichment table — empty unless that opt-in module ran (see _volume_stats).
_VOLUME_STATS_PREFIXES = ("cell_top", "cell_base", "cell_depth", "cell_volume", "cell_eth", "vol_")

# Plot-group variables with these prefixes come from the xlma_stat_minutes extension
# table — empty unless `adapt postprocess --module xlma_stat` ran (see _lightning).
_LIGHTNING_PREFIXES = ("flash_", "source_")


class ScanViewTab:
    """Latest Scan tab: scan browsing, map rendering, selection, time series."""

    def __init__(self, nb: ttk.Notebook, ctx: AppContext):
        self.ctx = ctx
        self.frame = ttk.Frame(nb)
        nb.add(self.frame, text="Latest Scan")

        # Render state
        self._canvas_refs = None  # (canvas, fig, toolbar, bottom)
        self._current_nc_ds: xr.Dataset | None = None
        self._current_cell_df = None  # cells_by_scan DataFrame or parquet fallback
        self._current_run_id = None  # run_id for the loaded cell data
        self._current_scan_ts = None  # pd.Timestamp of current displayed scan
        self._cell_contours: dict[int, object] = {}  # cell_id → contour set
        self._hover_canvas = None
        self._saved_zoom: tuple | None = None  # (xlim, ylim) preserved across redraws

        # Multi-cell selection: uid → color_slot_index; persists across scan changes
        self._selected_cells: dict[str, int] = {}
        self._track_overlay: dict[str, list] = {}  # uid → matplotlib artists

        # View controls shared with the shell's View menu
        self.show_flow_var = tk.BooleanVar(value=False)
        self._bg_alpha_var = tk.DoubleVar(value=0.85)
        self._max_proj_var = tk.IntVar(value=0)

        self._plot_settings_win: tk.Toplevel | None = None
        self._ts_axes: tuple | None = None  # (ax_area, ax_dbz, ax_reserved)
        self._cbar_ax: object | None = None  # pre-allocated colorbar axes

        # NC loop animation state
        self._nc_loop_running = False
        self._nc_loop_index = 0
        self._nc_loop_files: list = []

        self._last_rendered_nc = None  # path of last auto-rendered NC file
        self._all_nc_files: list = []  # full sorted NC list, updated every refresh
        self._after_ids: list[str] = []

        self._build_scan_tab(self.frame)

    @property
    def _color_slots(self) -> list[str]:
        return self.ctx.cfg()["colors"]

    # ── Shell hooks ───────────────────────────────────────────────────────────

    def refresh(self) -> int:
        """Re-scan the NC timeline, sync the scan selector, and auto-render newly
        arrived data. Returns the scan count for the shell status bar."""
        all_nc = self.ctx.nc_files()
        self._all_nc_files = all_nc
        labels = [self._nc_label(p) for p in all_nc]

        cur = self.scan_var.get()
        self.scan_cb["values"] = labels
        if labels and cur not in labels:
            self.scan_var.set(labels[-1])

        # Auto-update the live canvas when a new NC file appears
        if not self._nc_loop_running and all_nc:
            latest = all_nc[-1]
            repo = self.ctx.repo()
            radar = self.ctx.radar()
            if self._last_rendered_nc is not None and self._last_rendered_nc != latest:
                if self._canvas_refs is not None:
                    try:
                        self._load_cells_data(repo, radar)
                        _ds = xr.open_dataset(latest)
                        try:
                            self._redraw(_ds)
                        except Exception:
                            _ds.close()
                            raise
                        self._last_rendered_nc = latest
                        self.scan_var.set(labels[-1] if labels else "")
                        if self._selected_cells:
                            self._update_time_series_all()
                        else:
                            self._clear_time_series()
                    except Exception:
                        logger.exception("Failed to auto-refresh current NC canvas")
                else:
                    # Canvas was cleared externally; re-render
                    try:
                        self._load_cells_data(repo, radar)
                        self._render_nc(latest)
                        self._last_rendered_nc = latest
                        self.scan_var.set(labels[-1] if labels else "")
                    except Exception:
                        logger.exception("Failed to render latest NC file during auto-refresh")
        return len(all_nc)

    def on_run_changed(self) -> None:
        """Radar or run changed: drop the zoom so the next render shows full extent."""
        self._saved_zoom = None

    def show_latest_soon(self) -> None:
        """Render the newest scan shortly after the pending refresh settles."""
        if self._all_nc_files:
            self._after_ids.append(self.frame.after(100, self.show_latest))

    def on_close(self) -> None:
        """Stop the loop and cancel pending callbacks before teardown."""
        self._nc_loop_running = False
        for after_id in self._after_ids:
            with contextlib.suppress(Exception):
                self.frame.after_cancel(after_id)
        self._after_ids.clear()

    def _build_scan_tab(self, tab: ttk.Frame) -> None:

        # ── Row 1: variable selector + range ─────────────────────────────────
        ctrl1 = ttk.Frame(tab, padding=(4, 3, 4, 1))
        ctrl1.pack(side="top", fill="x")

        ttk.Label(ctrl1, text="Variable:", font=("", 10)).pack(side="left")
        self._plot_var = tk.StringVar(value="reflectivity")
        var_cb = ttk.Combobox(
            ctrl1,
            textvariable=self._plot_var,
            width=26,
            values=list(_VAR_DEFAULTS.keys()),
            state="readonly",
        )
        var_cb.pack(side="left", padx=2)
        var_cb.bind("<<ComboboxSelected>>", lambda _: self._on_var_changed())

        ttk.Label(ctrl1, text="Min:", font=("", 10)).pack(side="left", padx=(10, 0))
        self._plot_vmin = tk.StringVar(value="10")
        ttk.Entry(ctrl1, textvariable=self._plot_vmin, width=6, font=("Courier", 10)).pack(
            side="left", padx=2
        )
        ttk.Label(ctrl1, text="Max:", font=("", 10)).pack(side="left", padx=(4, 0))
        self._plot_vmax = tk.StringVar(value="60")
        ttk.Entry(ctrl1, textvariable=self._plot_vmax, width=6, font=("Courier", 10)).pack(
            side="left", padx=2
        )

        ttk.Separator(ctrl1, orient="vertical").pack(side="left", fill="y", padx=8)
        ttk.Button(ctrl1, text="Show Latest", command=self.show_latest).pack(side="left", padx=2)
        self.btn_loop = ttk.Button(ctrl1, text="Show Loop", command=self.toggle_loop)
        self.btn_loop.pack(side="left", padx=2)
        ttk.Button(ctrl1, text="Update", command=self._redraw).pack(side="left", padx=2)
        ttk.Button(ctrl1, text="Clear Tracks", command=self._clear_canvas).pack(side="left", padx=2)
        ttk.Button(ctrl1, text="⚙ Plot settings", command=self.open_plot_settings).pack(
            side="left", padx=(8, 2)
        )

        # ── Row 2: scan selector + loop controls ─────────────────────────────
        ctrl2 = ttk.Frame(tab, padding=(4, 1, 4, 3))
        ctrl2.pack(side="top", fill="x")

        ttk.Label(ctrl2, text="Scan:", font=("", 10)).pack(side="left")
        self.scan_var = tk.StringVar()
        self.scan_cb = ttk.Combobox(ctrl2, textvariable=self.scan_var, width=28, state="readonly")
        self.scan_cb.pack(side="left", padx=(2, 2))
        self.scan_cb.bind("<<ComboboxSelected>>", lambda _: self._inline_render())

        ttk.Label(ctrl2, text="Bundle:", font=("", 10)).pack(side="left", padx=(4, 0))
        self._bundle_var = tk.IntVar(value=1)
        ttk.Spinbox(
            ctrl2,
            from_=1,
            to=999,
            textvariable=self._bundle_var,
            width=3,
            font=("Courier", 10),
        ).pack(side="left", padx=(2, 2))
        ttk.Button(ctrl2, text="◄", width=2, command=self.prev_scan).pack(side="left", padx=1)
        ttk.Button(ctrl2, text="►", width=2, command=self.next_scan).pack(side="left", padx=(1, 10))

        ttk.Label(ctrl2, text="Loop N:", font=("", 10)).pack(side="left")
        self._loop_n_var = tk.IntVar(value=5)
        ttk.Spinbox(
            ctrl2,
            from_=2,
            to=20,
            textvariable=self._loop_n_var,
            width=3,
            font=("Courier", 10),
        ).pack(side="left")
        ttk.Label(ctrl2, text="dt(ms):", font=("", 10)).pack(side="left", padx=(4, 0))
        self._loop_dt_var = tk.IntVar(value=500)
        ttk.Spinbox(
            ctrl2,
            from_=100,
            to=5000,
            increment=100,
            textvariable=self._loop_dt_var,
            width=5,
            font=("Courier", 10),
        ).pack(side="left", padx=(2, 8))

        # Canvas area — toolbar + cell info embedded by _render_nc
        self.scan_container = ttk.Frame(tab)
        self.scan_container.pack(fill="both", expand=True)
        self.img_label = ttk.Label(self.scan_container)
        self.img_label.pack(fill="both", expand=True)

        # Hover stat StringVars — keys from _HV_KEYS, updated by _on_plot_hover
        self._hv = {k: tk.StringVar(value="\u2014") for k in _HV_KEYS}

    # ── Plot settings panel ───────────────────────────────────────────────────

    def open_plot_settings(self) -> None:
        if self._plot_settings_win is not None:
            try:
                self._plot_settings_win.lift()
                return
            except Exception:
                self._plot_settings_win = None

        win = tk.Toplevel(self.frame)
        win.title("Line-plot settings")
        win.resizable(False, False)
        self._plot_settings_win = win
        win.protocol("WM_DELETE_WINDOW", lambda: self._close_plot_settings(win))

        group_names = list(self.ctx.cfg()["plot_groups"].keys())
        slot_vars = []
        for i, label in enumerate(("Plot 1:", "Plot 2:", "Plot 3:")):
            ttk.Label(win, text=label).grid(row=i, column=0, padx=8, pady=4, sticky="w")
            var = tk.StringVar(value=self.ctx.cfg()["plot_assignments"][i])
            cb = ttk.Combobox(win, textvariable=var, values=group_names, state="readonly", width=16)
            cb.grid(row=i, column=1, padx=8, pady=4)
            slot_vars.append(var)

        def _apply():
            self.ctx.cfg()["plot_assignments"] = [v.get() for v in slot_vars]
            self._update_time_series_all()

        ttk.Button(win, text="Apply", command=_apply).grid(row=3, column=0, columnspan=2, pady=8)

    def _close_plot_settings(self, win) -> None:
        self._plot_settings_win = None
        win.destroy()

    def ask_bg_alpha(self) -> None:
        val = simpledialog.askfloat(
            "Background Opacity",
            "Enter opacity 0.0–1.0:",
            initialvalue=self._bg_alpha_var.get(),
            minvalue=0.0,
            maxvalue=1.0,
            parent=self.frame,
        )
        if val is not None:
            self._bg_alpha_var.set(val)

    def ask_proj_steps(self) -> None:
        val = simpledialog.askinteger(
            "Projection Steps",
            "Max steps to show (0 = all):",
            initialvalue=self._max_proj_var.get(),
            minvalue=0,
            maxvalue=20,
            parent=self.frame,
        )
        if val is not None:
            self._max_proj_var.set(val)

    # ── NC file helpers ───────────────────────────────────────────────────────

    @staticmethod
    def _nc_label(p):
        parts = p.stem.split("_")
        # filename: RADAR_YYYYMMDD_HHMMSS_analysis  or similar
        d = next((x for x in parts if len(x) == 8 and x.isdigit()), None)
        t = next((x for x in parts if len(x) == 6 and x.isdigit()), None)
        if d and t:
            return f"{d[4:6]}-{d[6:8]} {t[:2]}:{t[2:4]}:{t[4:6]}  ({p.stem})"
        if t:
            return f"{t[:2]}:{t[2:4]}:{t[4:6]} UTC  ({p.stem})"
        return p.stem

    def _on_var_changed(self):
        """Update vmin/vmax defaults when variable selector changes."""
        var = self._plot_var.get()
        if var in _VAR_DEFAULTS:
            vmin, vmax, _, _ = _VAR_DEFAULTS[var]
            self._plot_vmin.set(str(vmin))
            self._plot_vmax.set(str(vmax))

    def _current_scan_idx(self) -> int:
        """Return index of the currently selected scan in _all_nc_files, or -1."""
        cur_label = self.scan_var.get()
        stem = cur_label.split("(")[-1].rstrip(")") if "(" in cur_label else ""
        return next((i for i, p in enumerate(self._all_nc_files) if p.stem == stem), -1)

    def prev_scan(self):
        if not self._all_nc_files:
            return
        idx = self._current_scan_idx()
        step = max(1, self._bundle_var.get()) if self._bundle_var else 1
        new_idx = max(0, (idx if idx >= 0 else len(self._all_nc_files)) - step)
        if new_idx != idx:
            self.scan_var.set(self._nc_label(self._all_nc_files[new_idx]))
            self._inline_render()

    def next_scan(self):
        if not self._all_nc_files:
            return
        idx = self._current_scan_idx()
        step = max(1, self._bundle_var.get()) if self._bundle_var else 1
        last = len(self._all_nc_files) - 1
        new_idx = min(last, (idx if idx >= 0 else -1) + step)
        if new_idx != idx:
            self.scan_var.set(self._nc_label(self._all_nc_files[new_idx]))
            self._inline_render()

    # ── Show latest scan (single frame, auto-live) ────────────────────────────

    def show_latest(self):
        """Render the most recent NC file and enable live auto-refresh."""
        repo = self.ctx.repo()
        radar = self.ctx.radar()
        if not repo or not radar:
            return
        nc_files = self.ctx.nc_files()
        if not nc_files:
            messagebox.showinfo(
                "No data",
                f"No analysis files found in:\n{Path(repo) / radar / 'analysis'}",
                parent=self.frame,
            )
            return
        self._load_cells_data(repo, radar)
        # Sync scan selector
        labels = [self._nc_label(p) for p in nc_files]
        self.scan_cb["values"] = labels
        self.scan_var.set(labels[-1])
        self._last_rendered_nc = nc_files[-1]
        if self._canvas_refs is not None:
            # Reuse existing canvas — preserves zoom and cell selection
            _ds = xr.open_dataset(nc_files[-1])
            try:
                self._redraw(_ds)
            except Exception:
                _ds.close()
                raise
            if self._selected_cells:
                self._update_time_series_all()
        else:
            self._render_nc(nc_files[-1])

    # ── Live render (single frame) ────────────────────────────────────────────

    def _inline_render(self):
        repo = self.ctx.repo()
        radar = self.ctx.radar()
        if not repo or not radar:
            messagebox.showerror(
                "Missing input", "Set Radar ID and Repo path first.", parent=self.frame
            )
            return

        nc_files = self.ctx.nc_files()
        if not nc_files:
            messagebox.showinfo(
                "Not found",
                f"No analysis files found in:\n{Path(repo) / radar / 'analysis'}",
                parent=self.frame,
            )
            return

        # Match selected label to NC file
        sel = self.scan_var.get()
        stem = sel.split("(")[-1].rstrip(")") if "(" in sel else ""
        nc_path = next((p for p in nc_files if p.stem == stem), nc_files[-1])

        self._load_cells_data(repo, radar)
        if self._canvas_refs is not None:
            # Reuse existing canvas — preserves zoom and cell selection
            _ds = xr.open_dataset(nc_path)
            try:
                self._redraw(_ds)
            except Exception:
                _ds.close()
                raise
            if self._selected_cells:
                self._update_time_series_all()
            else:
                self._clear_time_series()
        else:
            self._render_nc(nc_path)

    def _load_cells_data(self, repo, radar):
        """Load per-cell data for the selected run into self._current_cell_df.

        Honours the Run selector: reads the chosen run's cells (falling back to
        the newest run only when nothing is selected). Reads through the public
        API. Falls back to parquet for legacy data.
        """
        self._current_cell_df = None
        self._current_run_id = None

        db_path = Path(repo) / radar / "catalog.db"
        if db_path.exists():
            try:
                run_id = self.ctx.run_id()
                client = self.ctx.client()
                if run_id is None:
                    df = client.table("cells_by_scan", radar=radar)
                    if not df.empty:
                        run_id = df.loc[df["scan_time"].idxmax(), "run_id"]
                        df = df[df["run_id"] == run_id]
                else:
                    df = client.table("cells_by_scan", radar=radar, run_id=run_id)
                if not df.empty:
                    self._current_cell_df = df.sort_values("scan_time", ignore_index=True)
                    self._current_run_id = run_id
                    return
            except Exception:
                logger.exception("Failed to load cells from repository client")

        # Fallback: parquet (may not contain cell_uid)
        pqs = sorted((Path(repo) / radar / "analysis").glob("analysis2d_*.parquet"))
        if pqs:
            try:
                dfs = [pd.read_parquet(p) for p in pqs]
                self._current_cell_df = pd.concat(dfs, ignore_index=True)
            except Exception:
                logger.exception("Failed to load fallback parquet cell data")

    # ── NC loop render (cycle through N frames) ───────────────────────────────

    def toggle_loop(self):
        if self._nc_loop_running:
            self._nc_loop_running = False
            self.btn_loop.config(text="Show Loop")
            return
        repo = self.ctx.repo()
        radar = self.ctx.radar()
        if not repo or not radar:
            return
        n = max(2, self._loop_n_var.get())
        nc_files = self.ctx.nc_files()[-n:]
        if not nc_files:
            messagebox.showinfo("No data", "No analysis NC files found.", parent=self.frame)
            return
        self._load_cells_data(repo, radar)
        self._nc_loop_files = nc_files
        self._nc_loop_index = 0
        self.btn_loop.config(text="Stop Loop")
        self._clear_canvas(clear_selection=False)  # keep selected cells so timeline stays populated
        self._nc_loop_running = True  # set AFTER clear so _clear_canvas doesn't kill it
        self._render_nc(nc_files[0])
        self._nc_loop_index = 1
        dt = max(100, self._loop_dt_var.get())
        self._after_ids.append(self.frame.after(dt, self._nc_loop_step))

    def _nc_loop_step(self):
        if not self._nc_loop_running or not self._nc_loop_files:
            return
        path = self._nc_loop_files[self._nc_loop_index % len(self._nc_loop_files)]
        self._nc_loop_index += 1
        if self._canvas_refs is not None:
            _ds = xr.open_dataset(path)
            try:
                self._redraw(_ds)
                self._update_time_series_all()
            except Exception:
                _ds.close()
                raise
        else:
            self._render_nc(path)
        dt = max(100, self._loop_dt_var.get())
        self._after_ids.append(self.frame.after(dt, self._nc_loop_step))

    # ── Core matplotlib rendering ─────────────────────────────────────────────

    def _render_nc(self, nc_path):
        """Create canvas + bottom strip, then render nc_path into a new figure."""
        ds_tmp = xr.open_dataset(nc_path)
        lat0 = ds_tmp.attrs.get("radar_latitude", ds_tmp.attrs.get("origin_latitude"))
        lon0 = ds_tmp.attrs.get("radar_longitude", ds_tmp.attrs.get("origin_longitude"))
        if lat0 is None or lon0 is None:
            lat0, lon0 = 0, 0
        else:
            lat0, lon0 = float(lat0), float(lon0)
        ds_tmp.close()

        # GridSpec: radar | cbar | time-series (3 columns, 3 rows)
        # cbar column is pre-allocated so colorbar never steals space from radar.
        fig = plt.figure(figsize=(18, 6.5), dpi=90)
        gs = fig.add_gridspec(
            3,
            3,
            width_ratios=[1.4, 0.05, 1.0],
            hspace=0.5,
            wspace=0.25,
            left=0.04,
            right=0.97,
            top=0.93,
            bottom=0.13,
        )
        ax_radar = fig.add_subplot(gs[:, 0])
        self._cbar_ax = fig.add_subplot(gs[:, 1])
        ax_area = fig.add_subplot(gs[0, 2])
        ax_dbz = fig.add_subplot(gs[1, 2], sharex=ax_area)
        ax_reserved = fig.add_subplot(gs[2, 2], sharex=ax_area)
        self._ts_axes = (ax_area, ax_dbz, ax_reserved)
        self._clear_time_series()

        self._draw_scan(xr.open_dataset(nc_path), fig, ax_radar)

        self.img_label.pack_forget()

        bottom = tk.Frame(self.scan_container, bg=_STRIP_BG)
        bottom.pack(side="bottom", fill="x")

        canvas = FigureCanvasTkAgg(fig, master=self.scan_container)
        canvas.get_tk_widget().pack(fill="both", expand=True)
        canvas.draw()

        toolbar = _CompactToolbar(canvas, bottom, pack_toolbar=False, lat0=lat0, lon0=lon0)
        toolbar.update()
        toolbar.pack(side="left")

        for var in self._hv.values():
            var.set("—")
        stat_frame = tk.Frame(bottom, bg=_STRIP_BG)
        stat_frame.pack(side="right", fill="y", padx=4, pady=2)
        for lbl1, key1, fg1, lbl2, key2, fg2 in _BOX_DEFS:
            box = tk.Frame(stat_frame, bg=_BOX_BG, padx=4, pady=2, relief="groove", bd=1)
            box.pack(side="left", fill="y", padx=2, pady=1)
            for lbl, key, fg in ((lbl1, key1, fg1), (lbl2, key2, fg2)):
                row = tk.Frame(box, bg=_BOX_BG)
                row.pack(fill="x")
                tk.Label(row, text=lbl + ":", font=_FONT_LBL, fg="#888888", bg=_BOX_BG).pack(
                    side="left"
                )
                tk.Label(
                    row,
                    textvariable=self._hv[key],
                    font=_FONT_VAL,
                    fg=fg,
                    bg=_BOX_BG,
                    anchor="w",
                    width=10,
                ).pack(side="left")

        self._canvas_refs = (canvas, fig, toolbar, bottom)
        self._hover_canvas = canvas
        canvas.mpl_connect("motion_notify_event", self._on_plot_hover)
        canvas.mpl_connect("button_press_event", self._on_cell_click)

    def _draw_scan(self, ds, fig, ax=None):
        """Render dataset into the radar axes via the pure renderer. Keeps ds open."""
        # Resolve ax — always the leftmost (index 0) in the GridSpec figure
        if ax is None:
            ax = fig.axes[0]

        # Save zoom before render_scan's ax.clear() wipes it
        if self._saved_zoom is not None or (ax.lines or ax.collections):
            xlim, ylim = ax.get_xlim(), ax.get_ylim()
            if xlim != (0.0, 1.0) or ylim != (0.0, 1.0):
                self._saved_zoom = (xlim, ylim)

        # Close previous dataset
        if self._current_nc_ds is not None and self._current_nc_ds is not ds:
            with contextlib.suppress(Exception):
                self._current_nc_ds.close()
        self._current_nc_ds = ds
        for var in self._hv.values():
            var.set("\u2014")

        res = render_scan(
            ax, self._cbar_ax, ds, self._current_view_state(), self._current_overlays()
        )
        self._cell_contours = res.cell_contours
        self._track_overlay = res.track_overlays
        self.ctx.report_scan_time(res.scan_ts.to_pydatetime())
        self._current_scan_ts = res.scan_ts  # for hover filtering

    def _current_view_state(self) -> ViewState:
        """Freeze the Latest Scan controls into a renderer ViewState."""
        var_name = self._plot_var.get() if self._plot_var is not None else "reflectivity"
        vdef = _VAR_DEFAULTS.get(var_name, (10, 60, "dBZ", "viridis"))
        try:
            vmin = float(self._plot_vmin.get() if self._plot_vmin else vdef[0])
        except (ValueError, AttributeError):
            vmin = float(vdef[0])
        try:
            vmax = float(self._plot_vmax.get() if self._plot_vmax else vdef[1])
        except (ValueError, AttributeError):
            vmax = float(vdef[1])
        return ViewState(
            var_name=var_name,
            vmin=vmin,
            vmax=vmax,
            bg_alpha=self._bg_alpha_var.get() if self._bg_alpha_var else 0.35,
            max_proj_steps=self._max_proj_var.get() if self._max_proj_var else 0,
            show_flow=bool(self.show_flow_var.get()) if self.show_flow_var else False,
            zoom=self._current_zoom(),
            selected_cells=dict(self._selected_cells),
            color_slots=tuple(self._color_slots),
        )

    def _current_zoom(self):
        """Live axes limits when a canvas exists (captures toolbar zooms that never
        triggered a redraw), else the zoom saved across redraws, else None."""
        if self._canvas_refs is not None:
            fig = self._canvas_refs[1]
            if fig.axes:
                xlim, ylim = fig.axes[0].get_xlim(), fig.axes[0].get_ylim()
                if xlim != (0.0, 1.0) or ylim != (0.0, 1.0):
                    return (xlim, ylim)
        return self._saved_zoom

    def _current_overlays(self) -> OverlayData:
        """Freeze the cell table + per-uid track histories for selected cells."""
        histories = {}
        for uid in self._selected_cells:
            hist = self._track_history(uid)
            if hist is not None:
                histories[uid] = hist
        return OverlayData(cell_df=self._current_cell_df, track_histories=histories)

    def _track_history(self, uid):
        """Full track history for one cell via the public API, or None."""
        repo = self.ctx.repo()
        radar = self.ctx.radar()
        if not self._current_run_id or not (Path(repo) / radar / "catalog.db").exists():
            return None
        try:
            return self.ctx.client().track_history(self._current_run_id, str(uid), radar=radar)
        except Exception:
            logger.exception("Failed to load track history for %s", uid)
            return None

    def _refresh_track_overlays(self, ax, ds) -> None:
        """Re-draw track paths and centroid markers for all selected cells."""
        for artists in self._track_overlay.values():
            for art in artists:
                with contextlib.suppress(Exception):
                    art.remove()
        self._track_overlay = draw_track_overlays(
            ax, ds, self._current_view_state(), self._current_overlays()
        )

    # ── Single update entry point ─────────────────────────────────────────────

    def _redraw(self, ds=None) -> None:
        """Re-render the current (or given) dataset with current control state.
        Called by Update button, loop step, and auto-refresh."""
        ds = ds or self._current_nc_ds
        if ds is None or self._canvas_refs is None:
            return
        _, fig, _, _ = self._canvas_refs
        self._draw_scan(ds, fig)
        fig.canvas.draw_idle()

    # ── Cell click → tracking history + time series ─────────────────────────

    def _on_cell_click(self, event) -> None:
        if self._canvas_refs is None:
            return
        if self._current_nc_ds is None:
            return
        _, fig, _, _ = self._canvas_refs
        if not fig.axes:
            return
        ax_radar = fig.axes[0]
        if event.inaxes is not ax_radar or event.button != 1:
            return
        ds = self._current_nc_ds
        x_m = event.xdata * 1000.0
        y_m = event.ydata * 1000.0
        xi = int(np.argmin(np.abs(ds["x"].values - x_m)))
        yi = int(np.argmin(np.abs(ds["y"].values - y_m)))
        cell_id = int(ds["cell_labels"].values[yi, xi])
        if cell_id <= 0:
            return
        repo = self.ctx.repo()
        radar = self.ctx.radar()
        db_path = Path(repo) / radar / "catalog.db"

        # Resolve cell_uid for clicked cell via the exact scan-time lookup
        # (avoids scan_time string-format mismatches in the loaded frame)
        cell_uid = None
        if self._current_run_id and db_path.exists() and self._current_scan_ts is not None:
            try:
                scan_time_dt = pd.Timestamp(self._current_scan_ts).to_pydatetime()
                scan_cells = self.ctx.client().cells_at_scan(
                    self._current_run_id, scan_time_dt, radar=radar
                )
                if not scan_cells.empty and "cell_label" in scan_cells.columns:
                    matched = scan_cells[scan_cells["cell_label"] == cell_id]
                    if not matched.empty:
                        r = matched.iloc[0]
                        cell_uid = r.get("cell_uid")
            except Exception:
                logger.exception("Failed to resolve cell UID via repository client")

        # Fallback: search loaded cell df with 60-s time window
        if cell_uid is None:
            df = self._current_cell_df
            if df is None or "cell_uid" not in df.columns:
                return
            if self._current_scan_ts is not None and "scan_time" in df.columns:
                df_t = df.copy()
                df_t["_st"] = pd.to_datetime(df_t["scan_time"], utc=True)
                scan_ts = pd.Timestamp(self._current_scan_ts)
                if scan_ts.tzinfo is None:
                    scan_ts = scan_ts.tz_localize("UTC")
                time_mask = (df_t["_st"] - scan_ts).abs() < pd.Timedelta(seconds=60)
                scan_rows = df_t[time_mask & (df_t["cell_label"] == cell_id)]
            else:
                scan_rows = df[df["cell_label"] == cell_id]
            if scan_rows.empty:
                return
            r = scan_rows.iloc[0]
            cell_uid = r.get("cell_uid")

        if cell_uid is not None and (isinstance(cell_uid, float) and pd.isna(cell_uid)):
            cell_uid = None

        uid_str = str(cell_uid) if cell_uid is not None else None
        if uid_str is None:
            return

        if uid_str in self._selected_cells:
            # Deselect: remove from selection and overlays
            self._selected_cells.pop(uid_str)
            for artist in self._track_overlay.pop(uid_str, []):
                with contextlib.suppress(Exception):
                    artist.remove()
        else:
            # Select: assign color slot
            slot = _next_free_color_slot(self._selected_cells)
            if slot is None:
                action = self.ctx.cfg().get("overflow_action", "ask")
                if action == "ask":
                    action = self._ask_overflow_action()
                slot = _apply_overflow_action(action, self._selected_cells)
                if slot is None:
                    return  # user chose ignore
            self._selected_cells[uid_str] = slot

        self._refresh_track_overlays(ax_radar, self._current_nc_ds)
        self._update_time_series_all()
        fig.canvas.draw_idle()

    def _clear_tracking_history(self) -> None:
        for artists in self._track_overlay.values():
            for artist in artists:
                with contextlib.suppress(Exception):
                    artist.remove()
        self._track_overlay = {}
        # _selected_cells intentionally NOT cleared; use Escape or empty-click to deselect

    # ── Time series panels ────────────────────────────────────────────────────

    @staticmethod
    def _style_ts_ax(ax, ylabel: str, title: str) -> None:
        _style_ts_ax_fn(ax, ylabel, title)

    @staticmethod
    def _apply_time_axis(ax_bottom, axes) -> None:
        _apply_time_axis_fn(ax_bottom, axes)

    def _ask_overflow_action(self) -> str:
        """Show popup and return 'ignore', 'replace_oldest', or 'wrap'."""
        win = tk.Toplevel(self.frame)
        win.title("Too many tracks selected")
        win.resizable(False, False)
        win.grab_set()
        result: list[str] = ["ignore"]

        ttk.Label(
            win,
            text="All 7 color slots are in use. What should happen?",
            padding=10,
        ).pack()

        def choose(action: str) -> None:
            result[0] = action
            win.destroy()

        ttk.Button(win, text="Ignore this click", command=lambda: choose("ignore")).pack(
            fill="x", padx=20, pady=4
        )
        ttk.Button(
            win, text="Replace oldest selection", command=lambda: choose("replace_oldest")
        ).pack(fill="x", padx=20, pady=4)
        ttk.Button(win, text="Wrap color (may be ambiguous)", command=lambda: choose("wrap")).pack(
            fill="x", padx=20, pady=(4, 12)
        )

        self.frame.wait_window(win)
        return result[0]

    def _update_time_series_all(self) -> None:
        """Re-draw all 3 time-series plots for all currently selected tracks."""
        if self._ts_axes is None:
            return
        ax1, ax2, ax3 = self._ts_axes
        for ax in (ax1, ax2, ax3):
            ax.clear()

        group_names = self.ctx.cfg().get("plot_assignments", ["Area", "Reflectivity", "ZDR"])
        axes = [ax1, ax2, ax3]

        # Variables referenced by the selected groups — used to decide whether the
        # cell_volume_stats table must be joined onto each track's history.
        needed_vars: set[str] = set()
        for gname in group_names:
            grp = self.ctx.cfg().get("plot_groups", {}).get(gname, {})
            needed_vars.update(grp.get("variables", []))

        repo = self.ctx.repo()
        radar = self.ctx.radar()
        db_path = Path(repo) / radar / "catalog.db"
        cur_t = (
            pd.Timestamp(self._current_scan_ts, tz="UTC")
            if self._current_scan_ts is not None
            else None
        )

        # Lightning columns come from the xlma_stat_minutes extension table —
        # read only when a selected group needs lightning.
        needs_lightning = any(
            str(v).startswith(_LIGHTNING_PREFIXES)
            for g in group_names
            for v in self.ctx.cfg().get("plot_groups", {}).get(g, {}).get("variables", [])
        )

        for uid, slot in self._selected_cells.items():
            color = self._color_slots[slot % len(self._color_slots)]
            history_df = self._track_history(uid)
            if history_df is None or history_df.empty:
                df = self._current_cell_df
                if df is not None and "cell_uid" in df.columns:
                    history_df = df[df["cell_uid"] == uid].copy()
            if history_df is None or history_df.empty:
                continue

            track_df = history_df.sort_values("scan_time")
            # Join 3D volume stats (e.g. cloud-top height) only when a selected
            # group needs columns that cells_by_scan does not carry.
            if needed_vars - set(track_df.columns) and self._current_run_id:
                vol_df = _load_track_volume_stats_fn(db_path, self._current_run_id, uid)
                track_df = _merge_volume_stats_fn(track_df, vol_df)
                if needs_lightning:
                    try:
                        client = self.ctx.client()
                        known = set(client.tables(radar)["table_name"])
                        if "xlma_stat_minutes" in known:
                            lma_df = client.table(
                                "xlma_stat_minutes",
                                radar=radar,
                                run_id=self._current_run_id,
                                filters={"cell_uid": uid},
                            )
                            track_df = _merge_lightning_fn(track_df, lma_df)
                    except Exception:
                        logger.exception("Failed to load lightning for %s", uid)
            t = pd.to_datetime(track_df["scan_time"], utc=True)

            for ax, group_name in zip(axes, group_names, strict=False):
                group = self.ctx.cfg().get("plot_groups", {}).get(group_name, {})
                for var, style, label in zip(
                    group.get("variables", []),
                    group.get("styles", []),
                    group.get("labels", []),
                    strict=False,
                ):
                    if var not in track_df.columns:
                        continue
                    ax.plot(
                        t,
                        track_df[var],
                        color=color,
                        linestyle=style,
                        linewidth=1.2,
                        label=f"{uid[:4]} {label}",
                    )

        for ax, group_name in zip(axes, group_names, strict=False):
            group = self.ctx.cfg().get("plot_groups", {}).get(group_name, {})
            rich_title = _build_ts_title_fn(group_name, group)
            self._style_ts_ax(ax, "", rich_title)
            if not ax.get_lines():
                group_vars = group.get("variables", [])
                if any(v.startswith(_LIGHTNING_PREFIXES) for v in group_vars):
                    msg = "no data — run xlma_stat postprocess"
                elif any(v.startswith(_VOLUME_STATS_PREFIXES) for v in group_vars):
                    msg = "no data — enable cell_volume_stats"
                else:
                    msg = "no data"
                ax.text(
                    0.5,
                    0.5,
                    msg,
                    transform=ax.transAxes,
                    ha="center",
                    va="center",
                    color="#888",
                    fontsize=7,
                )

        self._apply_time_axis(axes[-1], axes)
        _draw_scan_marker_fn(tuple(axes), cur_t)
        self._update_track_legend()

        if self._canvas_refs is not None:
            self._canvas_refs[0].draw_idle()

    def _update_track_legend(self) -> None:
        """Update the figure-level legend with colored patches for each selected track."""
        if self._canvas_refs is None:
            return
        _update_track_legend_fn(self._canvas_refs[1], self._selected_cells, self._color_slots)

    def _update_time_series(self, history_df: pd.DataFrame | None = None) -> None:
        if self._ts_axes is None:
            return
        ax_area, ax_dbz, ax_extra = self._ts_axes
        if history_df is not None and not history_df.empty:
            track_df = history_df.sort_values("scan_time")
            cell_uid = None
            if "cell_uid" in track_df.columns and track_df["cell_uid"].notna().any():
                cell_uid = str(track_df["cell_uid"].dropna().iloc[0])
        else:
            # Fall back to first selected cell if no history_df provided
            cell_uid = next(iter(self._selected_cells), None)
            if (
                not cell_uid
                or self._current_cell_df is None
                or "cell_uid" not in self._current_cell_df.columns
            ):
                return
            track_df = self._current_cell_df[
                self._current_cell_df["cell_uid"] == str(cell_uid)
            ].sort_values("scan_time")
            if track_df.empty:
                return

        for ax in (ax_area, ax_dbz, ax_extra):
            ax.cla()

        times = pd.to_datetime(track_df["scan_time"], utc=True)

        # ── Area panel ────────────────────────────────────────────────────────
        if "cell_area_sqkm" in track_df.columns:
            vals = track_df["cell_area_sqkm"].values
            ax_area.plot(times, vals, color="#7ec8e3", linewidth=1.5, label="total area")
            ax_area.fill_between(times, vals, alpha=0.15, color="#7ec8e3")
        if "area_40dbz_km2" in track_df.columns:
            ax_area.plot(
                times,
                track_df["area_40dbz_km2"].values,
                color="#ff9944",
                linewidth=1.0,
                linestyle="--",
                label="≥40 dBZ core",
            )
        self._style_ts_ax(ax_area, "km²", f"Cell {_cell_uid_disp(cell_uid)} — Area")
        if ax_area.get_lines():
            ax_area.legend(
                fontsize=6,
                labelcolor="#444",
                framealpha=0.5,
                loc="upper left",
                handlelength=1.2,
            )

        # ── Reflectivity panel ────────────────────────────────────────────────
        if "radar_reflectivity_mean" in track_df.columns:
            ax_dbz.plot(
                times,
                track_df["radar_reflectivity_mean"].values,
                color="#88cc44",
                linewidth=1.2,
                label="mean Z",
            )
        if "radar_reflectivity_max" in track_df.columns:
            ax_dbz.plot(
                times,
                track_df["radar_reflectivity_max"].values,
                color="#ff6644",
                linewidth=1.2,
                label="max Z",
            )
        self._style_ts_ax(ax_dbz, "dBZ", "Reflectivity")
        if ax_dbz.get_lines():
            ax_dbz.legend(
                fontsize=6,
                labelcolor="#444",
                framealpha=0.5,
                loc="upper left",
                handlelength=1.2,
            )

        # ── ZDR / extra panel ─────────────────────────────────────────────────
        has_extra = False
        if "radar_differential_reflectivity_max" in track_df.columns:
            zdr = track_df["radar_differential_reflectivity_max"]
            if zdr.notna().any():
                ax_extra.plot(times, zdr.values, color="#cc88ff", linewidth=1.2, label="max ZDR")
                has_extra = True
        self._style_ts_ax(ax_extra, "dB", "ZDR")
        if has_extra:
            ax_extra.legend(
                fontsize=6,
                labelcolor="#444",
                framealpha=0.5,
                loc="upper left",
                handlelength=1.2,
            )
        else:
            ax_extra.text(
                0.5,
                0.5,
                "no ZDR data",
                transform=ax_extra.transAxes,
                ha="center",
                va="center",
                color="#888",
                fontsize=7,
            )

        self._apply_time_axis(ax_extra, self._ts_axes)

    def _clear_time_series(self) -> None:
        if self._ts_axes is None:
            return
        _clear_time_series_fn(self._ts_axes)

    # ── Escape: clear overlay ─────────────────────────────────────────────────

    def clear_selection(self, _event=None) -> None:
        self._clear_tracking_history()
        self._clear_time_series()
        if self._canvas_refs:
            _, fig, _, _ = self._canvas_refs
            fig.canvas.draw_idle()

    def _clear_canvas(self, clear_selection: bool = True):
        self._nc_loop_running = False
        self._last_rendered_nc = None
        if hasattr(self, "btn_loop"):
            self.btn_loop.config(text="Show Loop")

        if clear_selection:
            self._selected_cells = {}
        self._clear_tracking_history()
        self._ts_axes = None
        self._cbar_ax = None

        if self._canvas_refs:
            canvas, fig, toolbar, bottom = self._canvas_refs
            plt.close(fig)
            toolbar.destroy()
            canvas.get_tk_widget().destroy()
            bottom.destroy()
            self._canvas_refs = None
            self._hover_canvas = None
        if self._current_nc_ds is not None:
            with contextlib.suppress(Exception):
                self._current_nc_ds.close()
            self._current_nc_ds = None
        self._cell_contours = {}
        for var in self._hv.values():
            var.set("\u2014")
        self.img_label.config(image="", text="")
        self.img_label.pack(fill="both", expand=True)

    # ── Hover interaction ─────────────────────────────────────────────────────

    def _on_plot_hover(self, event):
        if self._current_nc_ds is None:
            return

        _em = "\u2014"
        ds = self._current_nc_ds

        if event.inaxes is None or event.xdata is None:
            for var in self._hv.values():
                var.set(_em)
            return

        # Only process hover on the radar panel (axes[0])
        if self._canvas_refs is not None:
            _, fig, _, _ = self._canvas_refs
            if len(fig.axes) > 0 and event.inaxes is not fig.axes[0]:
                return

        x_m = event.xdata * 1000.0
        y_m = event.ydata * 1000.0

        try:
            # ── Cell under cursor ─────────────────────────────────────────────
            x_vals = ds["x"].values
            y_vals = ds["y"].values
            xi = int(np.argmin(np.abs(x_vals - x_m)))
            yi = int(np.argmin(np.abs(y_vals - y_m)))
            cell_id = int(ds["cell_labels"].values[yi, xi])

            if cell_id <= 0:
                for k in _HV_KEYS:
                    self._hv[k].set(_em)
                return

            # ── Cell stats from cells_by_scan (filter by scan time AND cell_id) ─
            df = self._current_cell_df
            if df is not None and "cell_label" in df.columns:
                if self._current_scan_ts is not None and "scan_time" in df.columns:
                    df_time = df.copy()
                    df_time["scan_time"] = pd.to_datetime(df_time["scan_time"], utc=True)
                    scan_ts = (
                        self._current_scan_ts.tz_localize("UTC")
                        if self._current_scan_ts.tzinfo is None
                        else self._current_scan_ts
                    )
                    valid_mask = df_time["scan_time"].notna()
                    time_diff = abs(df_time.loc[valid_mask, "scan_time"] - scan_ts)
                    time_mask = pd.Series(False, index=df_time.index)
                    time_mask.loc[valid_mask] = time_diff < pd.Timedelta(minutes=1)
                    rows = df_time[time_mask & (df_time["cell_label"] == cell_id)]
                else:
                    rows = df[df["cell_label"] == cell_id]
                if not rows.empty:
                    r = rows.iloc[0]

                    def _f(key, fmt=".1f", suffix=""):
                        if key in r and r[key] == r[key]:
                            return f"{r[key]:{fmt}}{suffix}"
                        return _em

                    pid = r.get("cell_uid")
                    if pid and pid == pid:
                        self._hv["cell_uid"].set(_cell_uid_disp(pid))
                    else:
                        self._hv["cell_uid"].set(_em)
                    self._hv["area"].set(_f("cell_area_sqkm"))

                    # Age: prefer age_seconds; fallback = count unique scans for tracking history
                    age_raw = r.get("age_seconds")
                    if age_raw is not None and age_raw == age_raw:
                        age_s = float(age_raw)
                        if age_s < 60:
                            age_str = f"{int(age_s)}s"
                        elif age_s < 3600:
                            age_str = f"{int(age_s / 60)}m{int(age_s % 60):02d}s"
                        else:
                            age_str = f"{int(age_s / 3600)}h{int((age_s % 3600) / 60):02d}m"
                        self._hv["age"].set(age_str)
                    elif self._current_cell_df is not None:
                        cdf = self._current_cell_df
                        if pid and "cell_uid" in cdf.columns:
                            mask = cdf["cell_uid"] == str(pid)
                        else:
                            mask = None
                        if mask is not None:
                            n_scans = (
                                int(mask.groupby(cdf["scan_time"]).any().sum())
                                if "scan_time" in cdf.columns
                                else int(mask.sum())
                            )
                            self._hv["age"].set(f"{n_scans} scans")
                    else:
                        self._hv["age"].set(_em)

                    self._hv["lat_mass"].set(_f("cell_centroid_mass_lat", ".4f", "\u00b0"))
                    self._hv["lon_mass"].set(_f("cell_centroid_mass_lon", ".4f", "\u00b0"))
                    self._hv["dbz_mean"].set(_f("radar_reflectivity_mean"))
                    self._hv["dbz_max"].set(_f("radar_reflectivity_max"))
                    self._hv["zdr_mean"].set(_f("radar_differential_reflectivity_mean", ".2f"))
                    self._hv["zdr_max"].set(_f("radar_differential_reflectivity_max", ".2f"))
                    self._hv["vel_mean"].set(_f("radar_velocity_mean"))
                    self._hv["sw_mean"].set(_f("radar_spectrum_width_mean"))
                    return

            for k in _HV_KEYS:
                self._hv[k].set(_em)

        except Exception:
            logger.exception("Failed to update hover stats values")

    def movie_source(self) -> MovieSource | None:
        """Movie source reproducing the Latest Scan view, or None while idle."""
        if self._canvas_refs is None or not self._all_nc_files:
            return None
        paths = list(self._all_nc_files)
        # Freeze the view now: later control changes cannot leak into the movie.
        view = self._current_view_state()
        overlays = self._current_overlays()  # full-track histories, fetched once
        radar = self.ctx.radar()

        def make_spec(i0: int, i1: int) -> MovieSpec:
            frame_paths = paths[i0 : i1 + 1]
            return MovieSpec(
                n_frames=len(frame_paths),
                draw_frame=scan_frame_drawer(frame_paths, view, overlays),
                figsize=(9.5, 8.0),
                dpi=100,
            )

        return MovieSource(
            labels=[self._nc_label(p) for p in paths],
            make_spec=make_spec,
            default_stem=f"{radar}_{view.var_name}",
        )
