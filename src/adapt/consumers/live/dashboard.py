# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Adapt Radar Dashboard — A Very basic Tkinter GUI for exploring pipeline outputs in realtime.

Entry point: adapt dashboard [--repo /path/to/repo]

Layout
------
- Toolbar: repo browser, radar/run selection, refresh, pipeline start/stop
- Tab 0 "Latest Scan": matplotlib canvas (left) + cell-info panel (right)
                        + quick-filter strip (bottom)
- Tab 1 "Cell Statistics": filtered table (existing design)
- Tab 2 "Log": pipeline stdout

Single-instance note
--------------------
Only one `adapt run-nexrad` is allowed at a time (enforced by PID file).
The dashboard is a pure consumer — it reads from the repository and does
not need a running pipeline.  The Start/Stop buttons are provided for
convenience.
"""

import contextlib
import copy
import logging
import os
import subprocess
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


# ── Tkinter ───────────────────────────────────────────────────────────────────
import tkinter as tk  # noqa: E402
from tkinter import filedialog, messagebox, scrolledtext, simpledialog, ttk  # noqa: E402

from adapt.consumers.live._utils import (  # noqa: E402
    _PID_FILE,
    _apply_overflow_action,
    _cell_uid_disp,
    _centroid_track_to_km,
    _find_adapt_exe,
    _list_radars,
    _list_runs,
    _next_free_color_slot,
    _pipeline_pid_from_file,
    _pipeline_running,
    _suppress_osx_stderr,
    _visible_uids_in_scan,
)

# ── Optional deps ─────────────────────────────────────────────────────────────
try:
    import PIL  # noqa: F401

    HAS_PIL = True
except ImportError:
    HAS_PIL = False

try:
    import matplotlib

    matplotlib.use("TkAgg")
    import cmweather  # noqa: F401 — registers ChaseSpectral and other radar colormaps — must follow use()
    import matplotlib.lines as mlines
    import matplotlib.patches as mpatches
    import matplotlib.pyplot as plt
    from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg

    HAS_MPL = True
except ImportError:
    HAS_MPL = False

REFL_CMAP = "ChaseSpectral"

try:
    import numpy as np
    import pandas as pd
    import xarray as xr

    HAS_DATA = True
except ImportError:
    HAS_DATA = False

# ── Constants ─────────────────────────────────────────────────────────────────
POLL_MS = 10_000  # auto-refresh every 10 s
LOG_MAX = 500
LOG_FILE = Path.home() / ".adapt" / "pipeline.log"
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

# Plot-group variables with these prefixes come from the cell_volume_stats
# enrichment table — empty unless that opt-in module ran (see _volume_stats).
_VOLUME_STATS_PREFIXES = ("cell_top", "cell_base", "cell_depth", "cell_volume", "cell_eth", "vol_")

# Plot-group variables with these prefixes come from the xlma_stat_minutes extension
# table — empty unless `adapt postprocess --module xlma_stat` ran (see _lightning).
_LIGHTNING_PREFIXES = ("flash_", "source_")


from adapt.consumers.live._config import (  # noqa: E402, I001
    _list_user_configs,
    _load_default_config,
    _load_recent_repos,
    _load_user_config,
    _save_recent_repos,
    _save_user_config,
)
from adapt.consumers.live._renderer import add_basemap as _add_basemap_fn  # noqa: E402
from adapt.consumers.live._lightning import merge_lightning as _merge_lightning_fn  # noqa: E402
from adapt.consumers.live._volume_stats import (  # noqa: E402
    load_track_volume_stats as _load_track_volume_stats_fn,
    merge_volume_stats as _merge_volume_stats_fn,
)
from adapt.consumers.live._timeseries import (  # noqa: E402
    apply_time_axis as _apply_time_axis_fn,
    build_ts_title as _build_ts_title_fn,
    clear_time_series as _clear_time_series_fn,
    draw_scan_marker as _draw_scan_marker_fn,
    style_ts_ax as _style_ts_ax_fn,
    update_track_legend as _update_track_legend_fn,
)
from adapt.consumers.live._widgets import _CompactToolbar, _RangeSlider  # noqa: E402

# ── Main dashboard window ─────────────────────────────────────────────────────


class AdaptDashboard(tk.Tk):
    def __init__(self, repo: str | None = None):
        super().__init__()
        self.title("Adapt Radar Dashboard")
        self.geometry("1400x900")
        self.minsize(1000, 680)

        self._repo_root = tk.StringVar(value=repo or "")
        self._radar = tk.StringVar(value="")
        self._run_sel = tk.StringVar(value="")
        self._proc: subprocess.Popen | None = None
        self._log_lines: list[str] = []
        self._today = datetime.now().strftime("%Y%m%d")
        self._last_n_plots = -1
        self._canvas_refs = None  # (canvas, fig, toolbar, bottom)
        self._refresh_active = True

        # Inline render state
        self._current_nc_ds: xr.Dataset | None = None  # loaded xarray Dataset
        self._current_cell_df = None  # cells_by_scan DataFrame (SQLite) or parquet fallback
        self._current_run_id = None  # run_id for the loaded cell data
        self._current_scan_ts = None  # pd.Timestamp of current displayed scan
        self._cell_contours: dict[int, object] = {}  # cell_id -> contour set on radar ax
        self._hover_canvas = None  # ref to mpl canvas for hover

        # Config — loaded from bundled JSON, optionally overridden by user-saved config
        self._cfg: dict = _load_default_config()
        self._color_slots: list[str] = self._cfg["colors"]

        # Multi-cell selection: uid → color_slot_index; persists across scan changes
        self._selected_cells: dict[str, int] = {}
        self._track_overlay: dict[str, list] = {}  # uid → matplotlib artists
        self._saved_zoom: tuple | None = None  # (xlim, ylim) preserved across redraws

        # Plot settings dialog reference (prevent duplicate windows)
        self._plot_settings_win: tk.Toplevel | None = None

        # Pipeline subprocess — single reference for both toolbar and wizard launches
        self._log_file_handle: object | None = None  # open file handle for pipeline stdout

        self._ts_axes: tuple | None = None  # (ax_area, ax_dbz, ax_reserved)
        self._show_flow_var: tk.BooleanVar = tk.BooleanVar(value=False)
        self._colorbar: object | None = None  # active colorbar reference
        self._cbar_ax: object | None = None  # pre-allocated colorbar axes
        self._bg_alpha_var: tk.DoubleVar = tk.DoubleVar(value=0.85)
        self._max_proj_var: tk.IntVar = tk.IntVar(value=0)
        self._auto_refresh_var: tk.BooleanVar = tk.BooleanVar(value=True)

        # Recent repos: loaded from user_dashboard.json["recent_repos"]
        self._recent_repos: list[str] = _load_recent_repos()
        self._pipeline_badge: tk.Label | None = None

        # NC loop animation state (replaces PNG loop)
        self._nc_loop_running = False
        self._nc_loop_index = 0
        self._nc_loop_files: list[str] = []

        # Pending after() IDs — cancelled on close to prevent post-destroy callbacks
        self._after_ids: list[str] = []

        # Auto-refresh live tracking
        self._last_rendered_nc = None  # path of last auto-rendered NC file

        # Status bar state
        self._status_base = "Idle"
        self._last_scan_dt = None  # datetime of last rendered scan
        self._next_refresh_at = time.time() + POLL_MS / 1000

        # Full sorted NC file list — updated every refresh cycle
        self._all_nc_files: list = []
        self._bundle_var: tk.IntVar | None = None  # set in _build_scan_tab

        # Plot variable controls (set by _build_scan_tab)
        self._plot_var = None  # tk.StringVar set in _build_scan_tab
        self._plot_vmin = None
        self._plot_vmax = None

        self._build_ui()
        self.protocol("WM_DELETE_WINDOW", self._on_close)
        self.bind("<Escape>", self._on_escape)
        self.bind("<space>", lambda _: self._show_latest())
        self.bind("<Left>", lambda _: self._prev_scan())
        self.bind("<Right>", lambda _: self._next_scan())
        self.bind("l", lambda _: self._toggle_nc_loop())
        self.bind("<Control-r>", lambda _: self._refresh_all())
        self.bind("<Control-o>", lambda _: self._browse_repo())

        # Start auto-refresh and status countdown ticker
        self._after_ids.append(self.after(500, self._schedule_refresh))
        self._after_ids.append(self.after(1000, self._status_tick))

        if repo:
            self.after(200, self._on_repo_changed)
        elif self._recent_repos:
            # Auto-load most recent repo so panels show on startup without --repo arg
            self._repo_root.set(self._recent_repos[0])
            self.after(200, self._on_repo_changed)
        else:
            self.after(150, self._show_first_run_dialog)

        # Offer reconnect if a pipeline is already running externally
        if _pipeline_running() and self._proc is None:
            self.after(600, self._check_reconnect)

    # ── Build UI ──────────────────────────────────────────────────────────────

    def _build_ui(self):
        # ── Menubar ───────────────────────────────────────────────────────────
        self._build_menubar()

        # ── Top toolbar (single row) ──────────────────────────────────────────
        toolbar = ttk.Frame(self, padding=(6, 4))
        toolbar.pack(side="top", fill="x")

        ttk.Label(toolbar, text="Radar:").pack(side="left")
        self.radar_cb = ttk.Combobox(toolbar, textvariable=self._radar, width=8, state="readonly")
        self.radar_cb.pack(side="left", padx=(2, 10))
        self.radar_cb.bind("<<ComboboxSelected>>", lambda _: self._on_radar_changed())

        ttk.Label(toolbar, text="Run:").pack(side="left")
        self.run_cb = ttk.Combobox(toolbar, textvariable=self._run_sel, width=30, state="readonly")
        self.run_cb.pack(side="left", padx=(2, 10))

        ttk.Button(toolbar, text="Refresh", command=self._refresh_all).pack(side="left", padx=2)

        ttk.Separator(toolbar, orient="vertical").pack(side="left", fill="y", padx=8)
        self._pipeline_badge = tk.Label(toolbar, text="○ Idle", fg="#888888", font=("", 10))
        self._pipeline_badge.pack(side="left", padx=4)

        # Repo indicator — right-aligned, click opens browse dialog
        ttk.Separator(toolbar, orient="vertical").pack(side="right", fill="y", padx=4)
        self._repo_label = tk.Label(
            toolbar,
            textvariable=self._repo_root,
            fg="#555555",
            font=("", 9),
            cursor="hand2",
            anchor="e",
        )
        self._repo_label.pack(side="right", padx=(0, 4))
        self._repo_label.bind("<Button-1>", lambda _: self._browse_repo())
        ttk.Label(toolbar, text="Repo:", font=("", 9), foreground="#777").pack(
            side="right", padx=(8, 2)
        )

        # ── Status bar ────────────────────────────────────────────────────────
        self.status_var = tk.StringVar(value="Idle — set Output repo and click Refresh")
        ttk.Label(
            self,
            textvariable=self.status_var,
            relief="sunken",
            anchor="w",
            padding=(6, 2),
        ).pack(side="bottom", fill="x")
        ttk.Separator(self, orient="horizontal").pack(side="bottom", fill="x")

        # ── Notebook ──────────────────────────────────────────────────────────
        self._nb = ttk.Notebook(self)
        self._nb.pack(fill="both", expand=True, padx=6, pady=(2, 0))

        self._build_scan_tab()
        self._build_stats_tab()
        self._build_log_tab()

        self._nb.bind("<<NotebookTabChanged>>", self._on_tab_change)

    # ── Tab 0: Latest Scan ────────────────────────────────────────────────────

    def _build_scan_tab(self):
        tab = ttk.Frame(self._nb)
        self._nb.add(tab, text="Latest Scan")

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
        ttk.Button(ctrl1, text="Show Latest", command=self._show_latest).pack(side="left", padx=2)
        self.btn_loop = ttk.Button(ctrl1, text="Show Loop", command=self._toggle_nc_loop)
        self.btn_loop.pack(side="left", padx=2)
        ttk.Button(ctrl1, text="Update", command=self._redraw).pack(side="left", padx=2)
        ttk.Button(ctrl1, text="Clear Tracks", command=self._clear_canvas).pack(side="left", padx=2)
        ttk.Button(ctrl1, text="⚙ Plot settings", command=self._open_plot_settings).pack(
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
        ttk.Button(ctrl2, text="◄", width=2, command=self._prev_scan).pack(side="left", padx=1)
        ttk.Button(ctrl2, text="►", width=2, command=self._next_scan).pack(
            side="left", padx=(1, 10)
        )

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

    # ── Tab 1: Cell Statistics ────────────────────────────────────────────────

    def _build_stats_tab(self):
        tab = ttk.Frame(self._nb)
        self._nb.add(tab, text="Cell Statistics")

        left = ttk.Frame(tab, padding=(6, 4), width=300)
        left.pack(side="left", fill="y")
        left.pack_propagate(False)

        ttk.Label(left, text="Filter cells", font=("", 10, "bold")).pack(anchor="w", pady=(0, 6))

        # Cell UID prefix search
        pid_row = ttk.Frame(left)
        pid_row.pack(fill="x", pady=(0, 8))
        ttk.Label(pid_row, text="Cell UID prefix:", width=14, anchor="w").pack(side="left")
        self._cell_uid_filter = tk.StringVar()
        ttk.Entry(pid_row, textvariable=self._cell_uid_filter, width=12).pack(side="left", padx=2)
        self._cell_uid_filter.trace_add("write", lambda *_: self._refresh_table())

        self._flt = {}
        self._flt_sliders = {}

        filter_defs = [
            ("Area  km\u00b2", "cell_area_sqkm", 0, 2000, ".0f"),
            ("Mean dBZ", "radar_reflectivity_mean", 10, 80, ".1f"),
            ("ZDR  mean", "radar_differential_reflectivity_mean", -2, 8, ".2f"),
            ("Vel  mean", "radar_velocity_mean", -30, 30, ".1f"),
        ]

        for label, key, lo, hi, fmt in filter_defs:
            lo_var = tk.DoubleVar(value=lo)
            hi_var = tk.DoubleVar(value=hi)

            grp = ttk.Frame(left)
            grp.pack(fill="x", pady=4)

            hdr = ttk.Frame(grp)
            hdr.pack(fill="x")
            ttk.Label(hdr, text=label, width=12, anchor="w").pack(side="left")
            lo_lbl = ttk.Label(hdr, width=7, anchor="e", foreground="#555")
            lo_lbl.pack(side="left")
            ttk.Label(hdr, text="\u2013").pack(side="left")
            hi_lbl = ttk.Label(hdr, width=7, anchor="w", foreground="#555")
            hi_lbl.pack(side="left")

            def _update(*_, lv=lo_var, hv=hi_var, ll=lo_lbl, hl=hi_lbl, f=fmt):
                ll.config(text=f"{lv.get():{f}}")
                hl.config(text=f"{hv.get():{f}}")

            lo_var.trace_add("write", _update)
            hi_var.trace_add("write", _update)
            _update()

            slider = _RangeSlider(grp, lo, hi, lo_var, hi_var, fmt=fmt)
            slider.pack(fill="x", padx=2)

            self._flt[key] = (lo_var, hi_var)
            self._flt_sliders[key] = slider

        ttk.Button(left, text="Apply filters", command=self._refresh_table).pack(
            fill="x", pady=(10, 2)
        )

        right = ttk.Frame(tab, padding=(4, 4))
        right.pack(side="left", fill="both", expand=True)

        self.stats_lbl = ttk.Label(right, text="")
        self.stats_lbl.pack(anchor="w", pady=(0, 4))

        tv_frame = ttk.Frame(right)
        tv_frame.pack(fill="both", expand=True)

        self._tv_cols = [
            "time_label",
            "cell_label",
            "cell_area_sqkm",
            "radar_reflectivity_max",
            "radar_reflectivity_mean",
            "radar_differential_reflectivity_mean",
            "radar_velocity_mean",
            "cell_centroid_mass_lat",
            "cell_centroid_mass_lon",
        ]
        self.tv = ttk.Treeview(tv_frame, columns=self._tv_cols, show="headings", height=24)
        widths = [70, 60, 75, 80, 80, 85, 75, 90, 90]
        for c, w in zip(self._tv_cols, widths, strict=False):
            hdr = (
                c.replace("radar_differential_reflectivity_mean", "ZDR mean")
                .replace("radar_", "")
                .replace("cell_", "")
                .replace("_", " ")
            )
            self.tv.heading(c, text=hdr)
            self.tv.column(c, width=w, anchor="center")

        vsb = ttk.Scrollbar(tv_frame, orient="vertical", command=self.tv.yview)
        hsb = ttk.Scrollbar(tv_frame, orient="horizontal", command=self.tv.xview)
        self.tv.configure(yscrollcommand=vsb.set, xscrollcommand=hsb.set)
        self.tv.grid(row=0, column=0, sticky="nsew")
        vsb.grid(row=0, column=1, sticky="ns")
        hsb.grid(row=1, column=0, sticky="ew")
        tv_frame.rowconfigure(0, weight=1)
        tv_frame.columnconfigure(0, weight=1)

    # ── Tab 2: Pipeline Log ───────────────────────────────────────────────────

    def _build_log_tab(self):
        tab = ttk.Frame(self._nb)
        self._nb.add(tab, text="Log")

        ctrl = ttk.Frame(tab, padding=4)
        ctrl.pack(side="top", fill="x")
        ttk.Button(ctrl, text="Refresh", command=self._flush_log).pack(side="left")
        ttk.Button(ctrl, text="Clear", command=self._clear_log).pack(side="left", padx=4)
        ttk.Separator(ctrl, orient="vertical").pack(side="left", fill="y", padx=8)
        ttk.Button(ctrl, text="■ Stop Pipeline", command=self._stop).pack(side="left")

        self.log_text = scrolledtext.ScrolledText(
            tab,
            state="disabled",
            wrap="none",
            font=("Courier", 11),
            background="#1e1e1e",
            foreground="#d4d4d4",
        )
        self.log_text.pack(fill="both", expand=True)
        self.log_text.tag_config("error", foreground="#f44747")
        self.log_text.tag_config("warning", foreground="#dcdcaa")
        self.log_text.tag_config("info", foreground="#9cdcfe")

    # ── Menubar ───────────────────────────────────────────────────────────────

    def _build_menubar(self) -> None:
        mb = tk.Menu(self)
        self.config(menu=mb)
        self._build_file_menu(mb)
        self._build_pipeline_menu(mb)
        self._build_config_menu(mb)
        self._build_view_menu(mb)

    def _build_file_menu(self, mb: tk.Menu) -> None:
        m = tk.Menu(mb, tearoff=False)
        mb.add_cascade(label="File", menu=m)
        m.add_command(label="Open Repository…", command=self._browse_repo, accelerator="Ctrl+O")
        self._recent_menu = tk.Menu(m, tearoff=False)
        m.add_cascade(label="Open Recent", menu=self._recent_menu)
        m.add_separator()
        m.add_command(label="Exit", command=self._on_close)
        self._refresh_recent_menu()

    def _build_pipeline_menu(self, mb: tk.Menu) -> None:
        m = tk.Menu(mb, tearoff=False)
        mb.add_cascade(label="Pipeline", menu=m)
        m.add_command(label="Start New…", command=self._open_run_wizard)
        m.add_command(label="■ Stop", command=self._stop_any)
        m.add_separator()
        m.add_checkbutton(label="Auto-refresh", variable=self._auto_refresh_var)
        m.add_command(label="Refresh Now", command=self._refresh_all, accelerator="Ctrl+R")

    def _build_config_menu(self, mb: tk.Menu) -> None:
        cfg_menu = tk.Menu(mb, tearoff=False)
        mb.add_cascade(label="Config", menu=cfg_menu)
        self._load_cfg_menu = tk.Menu(cfg_menu, tearoff=False)
        cfg_menu.add_cascade(label="Load Config", menu=self._load_cfg_menu)
        cfg_menu.add_command(label="Save Config As…", command=self._save_config_as)
        cfg_menu.add_separator()
        cfg_menu.add_command(label="Reset to Defaults", command=self._reset_config)
        self._refresh_load_cfg_menu()

    def _build_view_menu(self, mb: tk.Menu) -> None:
        m = tk.Menu(mb, tearoff=False)
        mb.add_cascade(label="View", menu=m)
        m.add_command(label="⚙ Plot Settings…", command=self._open_plot_settings)
        m.add_separator()
        m.add_checkbutton(label="Show Optical Flow", variable=self._show_flow_var)
        m.add_command(label="Background Opacity…", command=self._ask_bg_alpha)
        m.add_command(label="Projection Steps…", command=self._ask_proj_steps)
        m.add_separator()
        m.add_command(label="Keyboard Shortcuts…", command=self._show_shortcuts)

    def _refresh_load_cfg_menu(self) -> None:
        self._load_cfg_menu.delete(0, "end")
        names = _list_user_configs()
        if not names:
            self._load_cfg_menu.add_command(label="(no saved configs)", state="disabled")
            return
        for name in names:
            self._load_cfg_menu.add_command(
                label=name,
                command=lambda n=name: self._load_config(n),  # type: ignore[misc]
            )

    def _load_config(self, name: str) -> None:
        self._cfg = _load_user_config(name)
        self._color_slots = self._cfg["colors"]
        messagebox.showinfo("Config loaded", f"Loaded config: {name}", parent=self)

    def _save_config_as(self) -> None:
        name = simpledialog.askstring("Save Config", "Config name:", parent=self)
        if not name:
            return
        _save_user_config(name.strip(), self._cfg)
        self._refresh_load_cfg_menu()
        messagebox.showinfo("Saved", f"Config saved as: {name.strip()}", parent=self)

    def _reset_config(self) -> None:
        self._cfg = _load_default_config()
        self._color_slots = self._cfg["colors"]
        messagebox.showinfo("Reset", "Dashboard config reset to defaults.", parent=self)

    # ── Plot settings panel ───────────────────────────────────────────────────

    def _open_plot_settings(self) -> None:
        if self._plot_settings_win is not None:
            try:
                self._plot_settings_win.lift()
                return
            except Exception:
                self._plot_settings_win = None

        win = tk.Toplevel(self)
        win.title("Line-plot settings")
        win.resizable(False, False)
        self._plot_settings_win = win
        win.protocol("WM_DELETE_WINDOW", lambda: self._close_plot_settings(win))

        group_names = list(self._cfg["plot_groups"].keys())
        slot_vars = []
        for i, label in enumerate(("Plot 1:", "Plot 2:", "Plot 3:")):
            ttk.Label(win, text=label).grid(row=i, column=0, padx=8, pady=4, sticky="w")
            var = tk.StringVar(value=self._cfg["plot_assignments"][i])
            cb = ttk.Combobox(win, textvariable=var, values=group_names, state="readonly", width=16)
            cb.grid(row=i, column=1, padx=8, pady=4)
            slot_vars.append(var)

        def _apply():
            self._cfg["plot_assignments"] = [v.get() for v in slot_vars]
            self._update_time_series_all()

        ttk.Button(win, text="Apply", command=_apply).grid(row=3, column=0, columnspan=2, pady=8)

    def _close_plot_settings(self, win) -> None:
        self._plot_settings_win = None
        win.destroy()

    # ── Run Adapt wizard ──────────────────────────────────────────────────────

    def _open_run_wizard(self) -> None:
        import webbrowser

        win = tk.Toplevel(self)
        win.title("Start New Pipeline")
        win.resizable(False, False)

        path_var = tk.StringVar(value=self._repo_root.get())
        radar_var = tk.StringVar(value=self._radar.get())
        mode_var = tk.StringVar(value="realtime")
        start_var = tk.StringVar(value="")
        end_var = tk.StringVar(value="")
        config_mode_var = tk.StringVar(value="use")  # "use" | "create"
        info_var = tk.StringVar(value="")

        # ── Config mode radio ─────────────────────────────────────────────────
        radio_f = ttk.Frame(win)
        radio_f.grid(row=0, column=0, columnspan=3, padx=8, pady=(12, 4), sticky="w")
        ttk.Radiobutton(
            radio_f,
            text="I have config file",
            variable=config_mode_var,
            value="use",
            command=lambda: _on_mode_change(),
        ).pack(side="left")
        ttk.Radiobutton(
            radio_f,
            text="Create config in directory",
            variable=config_mode_var,
            value="create",
            command=lambda: _on_mode_change(),
        ).pack(side="left", padx=16)

        # ── Path entry + Browse ───────────────────────────────────────────────
        ttk.Label(win, text="Path:").grid(row=1, column=0, padx=8, pady=(4, 4), sticky="w")
        ttk.Entry(win, textvariable=path_var, width=42).grid(row=1, column=1, padx=4, pady=(4, 4))

        def _browse():
            if config_mode_var.get() == "create":
                chosen = filedialog.askdirectory(title="Select repository directory", parent=win)
            else:
                chosen = filedialog.askopenfilename(
                    title="Select config.yaml",
                    filetypes=[("YAML", "*.yaml *.yml"), ("All", "*.*")],
                    parent=win,
                )
            if chosen:
                path_var.set(chosen)

        ttk.Button(win, text="Browse…", command=_browse).grid(
            row=1, column=2, padx=(2, 8), pady=(4, 4)
        )

        # ── Radar ID ──────────────────────────────────────────────────────────
        ttk.Label(win, text="Radar ID:").grid(row=2, column=0, padx=8, pady=(8, 4), sticky="w")
        ttk.Entry(win, textvariable=radar_var, width=10).grid(
            row=2, column=1, padx=4, pady=(8, 4), sticky="w"
        )
        ttk.Label(win, text="(optional if set in config)", font=("", 8), foreground="gray").grid(
            row=2, column=2, padx=(0, 8), sticky="w"
        )

        # ── Mode ──────────────────────────────────────────────────────────────
        ttk.Label(win, text="Mode:").grid(row=3, column=0, padx=8, pady=4, sticky="w")
        mode_f = ttk.Frame(win)
        mode_f.grid(row=3, column=1, pady=4, sticky="w")
        ttk.Radiobutton(
            mode_f,
            text="Realtime",
            variable=mode_var,
            value="realtime",
            command=lambda: _toggle_time(),
        ).pack(side="left")
        ttk.Radiobutton(
            mode_f,
            text="Historical",
            variable=mode_var,
            value="historical",
            command=lambda: _toggle_time(),
        ).pack(side="left", padx=8)

        # ── Historical time range (hidden unless historical) ──────────────────
        time_frame = ttk.Frame(win)
        time_frame.grid(row=4, column=0, columnspan=3, padx=8, pady=(0, 4), sticky="ew")
        ttk.Label(time_frame, text="Start (UTC):").grid(row=0, column=0, padx=(0, 4), sticky="w")
        ttk.Entry(time_frame, textvariable=start_var, width=20).grid(row=0, column=1, padx=(0, 12))
        ttk.Label(time_frame, text="End (UTC):").grid(row=0, column=2, padx=(0, 4), sticky="w")
        ttk.Entry(time_frame, textvariable=end_var, width=20).grid(row=0, column=3)

        # ── Docs link ─────────────────────────────────────────────────────────
        docs = ttk.Label(
            win,
            text="More settings in config.yaml — see Adapt config docs",
            foreground="#4a9fd4",
            cursor="hand2",
        )
        docs.grid(row=5, column=0, columnspan=3, padx=8, pady=4)
        docs.bind(
            "<Button-1>",
            lambda _: webbrowser.open("https://arm-doe.github.io/Adapt/api/config.html"),
        )

        # ── Inline info label (shown after config creation) ───────────────────
        info_label = ttk.Label(
            win,
            textvariable=info_var,
            foreground="#e09a10",
            wraplength=400,
            justify="left",
        )
        info_label.grid(row=6, column=0, columnspan=3, padx=12, pady=(2, 4), sticky="w")

        # ── Buttons ───────────────────────────────────────────────────────────
        btn_frame = ttk.Frame(win)
        btn_frame.grid(row=7, column=0, columnspan=3, pady=(4, 12))
        ttk.Button(btn_frame, text="Cancel", command=win.destroy).pack(side="left", padx=8)

        create_btn = ttk.Button(
            btn_frame,
            text="Create Config",
            command=lambda: self._create_config_from_wizard(
                path_var.get().strip(),
                win,
                info_var,
            ),
        )
        create_btn.pack(side="left", padx=8)

        ttk.Button(
            btn_frame,
            text="Launch Pipeline",
            command=lambda: self._launch_pipeline_from_wizard(
                path_var.get().strip(),
                radar_var.get().strip(),
                mode_var.get(),
                start_var.get() or None,
                end_var.get() or None,
                win,
                config_mode_var.get(),
                info_var,
            ),
        ).pack(side="left", padx=8)

        def _toggle_time():
            if mode_var.get() == "historical":
                time_frame.grid()
            else:
                time_frame.grid_remove()

        def _on_mode_change():
            info_var.set("")
            if config_mode_var.get() == "create":
                create_btn.state(["!disabled"])
            else:
                create_btn.state(["disabled"])

        _on_mode_change()  # set initial button state
        _toggle_time()

    def _create_config_from_wizard(self, path: str, wizard_win, info_var) -> None:
        """Run 'adapt config' in the given directory and show an inline advisory."""
        if not path:
            messagebox.showerror("Missing input", "Enter a directory first.", parent=wizard_win)
            return
        p = Path(path)
        if not p.is_dir():
            messagebox.showerror(
                "Not a directory", f"Expected a directory:\n{path}", parent=wizard_win
            )
            return
        config_file = p / "config.yaml"
        if config_file.exists():
            info_var.set(f"ℹ config.yaml already exists at {config_file}.")
            return
        try:
            result = subprocess.run(
                [*_find_adapt_exe(), "config", str(config_file)],
                capture_output=True,
                text=True,
                timeout=15,
                check=False,
            )
            if result.returncode != 0:
                messagebox.showerror(
                    "Config creation failed",
                    f"adapt config failed:\n{result.stderr}",
                    parent=wizard_win,
                )
                return
        except Exception as exc:
            messagebox.showerror("Config creation failed", str(exc), parent=wizard_win)
            return
        info_var.set(
            f"ℹ config.yaml created at {config_file}. "
            "Check config before running or click Launch Pipeline."
        )

    def _launch_pipeline_from_wizard(
        self,
        path: str,
        radar: str,
        mode: str,
        start: str | None,
        end: str | None,
        wizard_win,
        config_mode: str = "use",
        info_var=None,
    ) -> None:
        """Resolve config path and launch the pipeline."""
        if not path:
            messagebox.showerror("Missing input", "Enter a path first.", parent=wizard_win)
            return

        # ── Check for any already-running pipeline ────────────────────────────
        running_pid, running_proc = self._find_running_pipeline()
        if running_pid is not None:
            kill = messagebox.askyesno(
                "Pipeline already running",
                f"A pipeline is already running (PID {running_pid}).\n\nKill it and continue?",
                parent=wizard_win,
            )
            if not kill:
                return
            if running_proc is not None:
                with contextlib.suppress(Exception):
                    running_proc.terminate()
                    running_proc.wait(timeout=5)
            else:
                with contextlib.suppress(OSError):
                    os.kill(running_pid, 15)
            return  # user clicks Launch Pipeline again once old process is gone

        p = Path(path)

        if config_mode == "use":
            # ── User has an existing config.yaml ──────────────────────────────
            if p.is_dir():
                # Directory given — auto-resolve to config.yaml inside it.
                config_file = p / "config.yaml"
                if not config_file.exists():
                    messagebox.showerror(
                        "No config.yaml",
                        f"No config.yaml found in:\n{p}\n\n"
                        "Select the config.yaml file directly, or click "
                        "'Create Config' to generate one.",
                        parent=wizard_win,
                    )
                    return
            elif p.is_file():
                config_file = p
            else:
                messagebox.showerror(
                    "Not found", f"Path does not exist:\n{path}", parent=wizard_win
                )
                return
            cmd = [*_find_adapt_exe(), "run-nexrad", str(config_file)]
            if radar:
                cmd += ["--radar", radar]
            if mode == "historical":
                if start:
                    cmd += ["--start-time", start]
                if end:
                    cmd += ["--end-time", end]

        else:
            # ── User created (or will use) config in a directory ──────────────
            if not p.is_dir():
                messagebox.showerror(
                    "Not a directory", f"Expected a directory:\n{path}", parent=wizard_win
                )
                return
            config_file = p / "config.yaml"
            if not config_file.exists():
                messagebox.showerror(
                    "No config.yaml",
                    f"No config.yaml found in:\n{p}\n\nClick 'Create Config' first.",
                    parent=wizard_win,
                )
                return
            cmd = [
                *_find_adapt_exe(),
                "run-nexrad",
                str(config_file),
                "--base-dir",
                str(p),
                "--mode",
                mode,
            ]
            if radar:
                cmd += ["--radar", radar]
            if mode == "historical":
                if start:
                    cmd += ["--start-time", start]
                if end:
                    cmd += ["--end-time", end]

        # Auto-select the repo in the dashboard so panels load from this run
        repo_dir = str(p) if p.is_dir() else str(p.parent)
        self._repo_root.set(repo_dir)
        self._record_recent_repo(repo_dir)
        # adapt_registry.db is created by the pipeline on first run, so retry
        # until it appears (3 s, 8 s, 15 s, 25 s after launch).
        for delay_ms in (3000, 5000, 7000, 10000):
            self._after_ids.append(self.after(delay_ms, self._on_repo_changed))

        wizard_win.destroy()
        self._launch_pipeline(cmd)
        if self._proc is not None:
            messagebox.showinfo(
                "Pipeline started",
                f"Adapt pipeline running (PID {self._proc.pid}).\n"
                f"Output is streamed to the Log tab and saved to:\n{LOG_FILE}",
                parent=self,
            )

    # ── Browse / selection ────────────────────────────────────────────────────

    def _browse_repo(self):
        with _suppress_osx_stderr():
            path = filedialog.askdirectory(title="Select Adapt output repository", parent=self)
        if path:
            self._repo_root.set(path)
            self._record_recent_repo(path)
            self._on_repo_changed()

    def _record_recent_repo(self, path: str) -> None:
        """Prepend path to recent list (dedup, cap at 5) and persist."""
        repos = [r for r in self._recent_repos if r != path]
        repos.insert(0, path)
        self._recent_repos = repos[:5]
        _save_recent_repos(self._recent_repos)
        self._refresh_recent_menu()

    def _refresh_recent_menu(self) -> None:
        self._recent_menu.delete(0, "end")
        if not self._recent_repos:
            self._recent_menu.add_command(label="(none)", state="disabled")
            return
        for repo in self._recent_repos:
            self._recent_menu.add_command(
                label=repo,
                command=lambda r=repo: self._open_recent_repo(r),  # type: ignore[misc]
            )

    def _open_recent_repo(self, path: str) -> None:
        self._repo_root.set(path)
        self._record_recent_repo(path)
        self._on_repo_changed()

    def _on_repo_changed(self):
        repo = Path(self._repo_root.get().strip())
        radars = _list_radars(repo)
        self.radar_cb["values"] = radars

        # Select radar with most recent run activity
        latest_radar = None
        if radars and repo.exists():
            from adapt.api.client import RepositoryClient

            runs_all = RepositoryClient(repo).runs()
            if runs_all:
                latest = max(runs_all, key=lambda r: r.start_time or datetime.min)
                if latest.radar_id in radars:
                    latest_radar = latest.radar_id

        if latest_radar:
            self._radar.set(latest_radar)
        elif radars:
            self._radar.set(radars[0])
        else:
            self._radar.set("")

        self._on_radar_changed()

    def _on_radar_changed(self):
        self._saved_zoom = None  # reset zoom when radar/run changes
        repo = Path(self._repo_root.get().strip())
        radar = self._radar.get().strip().upper()
        # Pass radar to filter runs by the selected radar
        runs = _list_runs(repo, radar=radar if radar else None)
        self.run_cb["values"] = runs
        if runs:
            self._run_sel.set(runs[0])  # Select most recent run (first in list)
        else:
            self._run_sel.set("")
        self._today = datetime.now().strftime("%Y%m%d")
        self._last_n_plots = -1
        self._refresh_all()
        # Auto-show the latest scan so panels appear immediately on repo load
        if self._all_nc_files and HAS_MPL and HAS_DATA:
            self.after(100, self._show_latest)

    # ── Pipeline control ──────────────────────────────────────────────────────

    def _start(self):
        radar = self._radar.get().strip().upper()
        repo = self._repo_root.get().strip()
        if not radar:
            messagebox.showerror("Missing input", "Select a Radar ID first", parent=self)
            return
        if not repo:
            messagebox.showerror("Missing input", "Set the Output repo path first", parent=self)
            return
        if self._proc and self._proc.poll() is None:
            messagebox.showerror(
                "Already running",
                "Stop the current pipeline before starting a new one.",
                parent=self,
            )
            return
        if _pipeline_running():
            pid = _pipeline_pid_from_file()
            messagebox.showerror(
                "Already running",
                f"A pipeline is already running (PID {pid}).\nStop it first or delete {_PID_FILE}.",
                parent=self,
            )
            return

        self._radar.set(radar)
        self._today = datetime.now().strftime("%Y%m%d")
        self._last_n_plots = -1

        cmd = [
            *_find_adapt_exe(),
            "run-nexrad",
            "--radar",
            radar,
            "--base-dir",
            repo,
            "--mode",
            "realtime",
        ]
        self.status_var.set(f"Running  |  {radar}  ->  {repo}")
        self._launch_pipeline(cmd)
        self._append_log(f"  Command: {' '.join(cmd)}", "info")

    def _launch_pipeline(self, cmd: list) -> None:
        """Launch adapt pipeline, redirect all output to LOG_FILE, start watcher threads."""
        self._log_lines = []
        LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
        log_handle = LOG_FILE.open("w", buffering=1)
        self._log_file_handle = log_handle
        try:
            self._proc = subprocess.Popen(
                cmd,
                stdout=log_handle,
                stderr=subprocess.STDOUT,
                start_new_session=True,
            )
        except Exception as exc:
            logger.exception("Failed to launch pipeline: %s", cmd)
            log_handle.close()
            self._log_file_handle = None
            messagebox.showerror("Launch failed", str(exc), parent=self)
            return
        self._append_log(
            f"[{datetime.now():%H:%M:%S}] Pipeline started (PID {self._proc.pid})", "info"
        )
        self._append_log(f"  Log: {LOG_FILE}", "info")
        self._start_log_tail(LOG_FILE)
        self._start_proc_watcher(self._proc)
        self._update_pipeline_badge()

    def _start_log_tail(self, log_path: Path) -> None:
        """Daemon thread: tail log_path and append new lines to the log display."""

        def _tail():
            try:
                with log_path.open("r") as f:
                    f.seek(0, 2)  # start at end — don't replay old content
                    while self._refresh_active:
                        line = f.readline()
                        if line:
                            line = line.rstrip()
                            self._log_lines.append(line)
                            if len(self._log_lines) > LOG_MAX:
                                self._log_lines.pop(0)
                            tag = (
                                "error"
                                if "ERROR" in line
                                else "warning"
                                if "WARNING" in line
                                else ""
                            )
                            self.after(0, self._append_log, line, tag)
                        else:
                            if self._proc is None or self._proc.poll() is not None:
                                break
                            time.sleep(0.15)
            except Exception:
                logger.exception("Log tail thread failed")

        threading.Thread(target=_tail, daemon=True, name="LogTail").start()

    def _start_log_tail_from_end(self, log_path: Path, last_n: int = 200) -> None:
        """Tail log_path starting from the last *last_n* lines (for reconnect)."""

        def _tail_reconnect():
            try:
                with log_path.open("r") as f:
                    lines = f.readlines()
                    for ln in lines[-last_n:]:
                        ln = ln.rstrip()
                        self._log_lines.append(ln)
                        tag = "error" if "ERROR" in ln else "warning" if "WARNING" in ln else ""
                        self.after(0, self._append_log, ln, tag)
                    # Continue tailing from current position
                    while self._refresh_active and _pipeline_running():
                        line = f.readline()
                        if line:
                            line = line.rstrip()
                            self._log_lines.append(line)
                            if len(self._log_lines) > LOG_MAX:
                                self._log_lines.pop(0)
                            tag = (
                                "error"
                                if "ERROR" in line
                                else "warning"
                                if "WARNING" in line
                                else ""
                            )
                            self.after(0, self._append_log, line, tag)
                        else:
                            time.sleep(0.2)
            except Exception:
                logger.exception("Reconnect log tail thread failed")

        threading.Thread(target=_tail_reconnect, daemon=True, name="LogTailReconnect").start()

    def _start_proc_watcher(self, proc: subprocess.Popen) -> None:
        """Daemon thread: block on proc.wait(), then fire _on_proc_ended on the main thread."""

        def _watch():
            proc.wait()
            if self._log_file_handle is not None:
                with contextlib.suppress(Exception):
                    self._log_file_handle.close()
                self._log_file_handle = None
            self.after(0, self._on_proc_ended)

        threading.Thread(target=_watch, daemon=True, name="ProcWatcher").start()

    def _stop(self) -> None:
        """Send SIGTERM to the pipeline process group; SIGKILL after 5 s timeout."""
        if self._proc is None or self._proc.poll() is not None:
            # No owned process — try PID-file-only external process
            pid = _pipeline_pid_from_file()
            if pid is not None:
                with contextlib.suppress(OSError):
                    os.kill(pid, 15)
            self._on_proc_ended()
            return
        self.status_var.set("Stopping pipeline…")
        proc = self._proc

        def _do_kill():
            try:
                os.killpg(os.getpgid(proc.pid), 15)
            except OSError:
                proc.terminate()
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                try:
                    os.killpg(os.getpgid(proc.pid), 9)
                except OSError:
                    proc.kill()

        threading.Thread(target=_do_kill, daemon=True).start()

    def _on_proc_ended(self) -> None:
        rc = self._proc.returncode if self._proc else None
        self._proc = None
        rc_str = f"exit {rc}" if rc is not None else "unknown"
        self.status_var.set(f"Stopped  |  {self._radar.get()}")
        self._append_log(f"[{datetime.now():%H:%M:%S}] Pipeline ended ({rc_str})", "info")
        self._update_pipeline_badge()
        self._flush_log()

    def _stop_any(self) -> None:
        self._stop()

    def _find_running_pipeline(self) -> tuple[int | None, subprocess.Popen | None]:
        """Return (pid, proc) for the active pipeline, or (None, None) if idle."""
        if self._proc is not None and self._proc.poll() is None:
            return self._proc.pid, self._proc
        pid = _pipeline_pid_from_file()
        if pid is not None and _pipeline_running():
            return pid, None
        return None, None

    def _update_pipeline_badge(self) -> None:
        if self._pipeline_badge is None:
            return
        running = (self._proc is not None and self._proc.poll() is None) or _pipeline_running()
        if running:
            self._pipeline_badge.config(text="● Pipeline running", fg="#4daf4a")
        else:
            self._pipeline_badge.config(text="○ Idle", fg="#888888")

    def _check_reconnect(self) -> None:
        """Offer to reconnect to an externally running pipeline on startup."""
        if not _pipeline_running() or self._proc is not None:
            return
        pid = _pipeline_pid_from_file()
        if pid is None:
            return
        ans = messagebox.askyesno(
            "Pipeline already running",
            f"Adapt pipeline (PID {pid}) is already running.\n\nReconnect to its log output?",
            parent=self,
        )
        if ans:
            self._reconnect_pipeline(pid)

    def _reconnect_pipeline(self, pid: int) -> None:
        """Attach log tail to a pipeline started outside this GUI session."""
        self._append_log(f"[{datetime.now():%H:%M:%S}] Reconnected to pipeline PID {pid}", "info")
        self._update_pipeline_badge()
        if LOG_FILE.exists():
            self._start_log_tail_from_end(LOG_FILE, last_n=200)
        self._after_ids.append(self.after(2000, lambda: self._poll_external_pid(pid)))

    def _poll_external_pid(self, pid: int) -> None:
        """Poll every 2 s for death of an external (PID-file-only) pipeline."""
        if not _pipeline_running():
            self._on_proc_ended()
            return
        self._after_ids.append(self.after(2000, lambda: self._poll_external_pid(pid)))

    def _ask_bg_alpha(self) -> None:
        val = simpledialog.askfloat(
            "Background Opacity",
            "Enter opacity 0.0–1.0:",
            initialvalue=self._bg_alpha_var.get(),
            minvalue=0.0,
            maxvalue=1.0,
            parent=self,
        )
        if val is not None:
            self._bg_alpha_var.set(val)

    def _ask_proj_steps(self) -> None:
        val = simpledialog.askinteger(
            "Projection Steps",
            "Max steps to show (0 = all):",
            initialvalue=self._max_proj_var.get(),
            minvalue=0,
            maxvalue=20,
            parent=self,
        )
        if val is not None:
            self._max_proj_var.set(val)

    def _show_shortcuts(self) -> None:
        win = tk.Toplevel(self)
        win.title("Keyboard Shortcuts")
        win.resizable(False, False)
        shortcuts = [
            ("Space", "Show Latest scan"),
            ("← / →", "Previous / Next scan"),
            ("l", "Toggle loop"),
            ("Ctrl+R", "Refresh"),
            ("Ctrl+O", "Open Repository"),
            ("Escape", "Stop loop"),
        ]
        for i, (key, desc) in enumerate(shortcuts):
            ttk.Label(win, text=key, font=("Courier", 10, "bold"), width=12, anchor="e").grid(
                row=i, column=0, padx=(12, 4), pady=3
            )
            ttk.Label(win, text=desc, font=("", 10)).grid(
                row=i, column=1, padx=(4, 12), pady=3, sticky="w"
            )
        ttk.Button(win, text="Close", command=win.destroy).grid(
            row=len(shortcuts), column=0, columnspan=2, pady=8
        )

    def _show_first_run_dialog(self) -> None:
        win = tk.Toplevel(self)
        win.title("Welcome to Adapt Dashboard")
        win.resizable(False, False)
        win.grab_set()

        pad: dict[str, Any] = {"padx": 20, "pady": 6}

        ttk.Label(win, text="Welcome to Adapt Dashboard", font=("", 13, "bold")).pack(**pad)
        ttk.Label(
            win,
            text=(
                "Adapt Dashboard is a read-only viewer for radar pipeline output.\n"
                "Choose one of the options below to get started."
            ),
            justify="center",
        ).pack(padx=20, pady=(0, 10))

        ttk.Separator(win, orient="horizontal").pack(fill="x", padx=16, pady=4)

        # ── Option A: open existing repository ───────────────────────────────
        ttk.Label(win, text="Open an existing repository", font=("", 10, "bold")).pack(**pad)
        ttk.Label(
            win,
            text=(
                "Select the output folder from a previous or currently running\n"
                "Adapt pipeline run (must contain adapt_registry.db)."
            ),
            justify="center",
            foreground="#555555",
        ).pack(padx=20, pady=(0, 6))

        repo_var = tk.StringVar()
        row = ttk.Frame(win)
        row.pack(padx=20, pady=(0, 6))
        ttk.Entry(row, textvariable=repo_var, width=42).pack(side="left", padx=(0, 4))
        ttk.Button(
            row,
            text="Browse…",
            command=lambda: repo_var.set(
                filedialog.askdirectory(title="Select repository folder", parent=win)
                or repo_var.get()
            ),
        ).pack(side="left")

        def _open():
            path = repo_var.get().strip()
            if not path:
                return
            win.destroy()
            self._repo_root.set(path)
            self._record_recent_repo(path)
            self._on_repo_changed()

        ttk.Button(win, text="Open Repository", command=_open).pack(pady=(2, 10))

        ttk.Separator(win, orient="horizontal").pack(fill="x", padx=16, pady=4)

        # ── Option B: start a new pipeline ───────────────────────────────────
        ttk.Label(win, text="Run a new pipeline", font=("", 10, "bold")).pack(**pad)
        ttk.Label(
            win,
            text=(
                "Launch a new Adapt pipeline from a config file.\n"
                "The dashboard will connect to it automatically."
            ),
            justify="center",
            foreground="#555555",
        ).pack(padx=20, pady=(0, 6))

        def _start_new():
            win.destroy()
            self._open_run_wizard()

        ttk.Button(win, text="Start New Pipeline…", command=_start_new).pack(pady=(2, 16))

    def _on_close(self):
        # Cancel all pending after() callbacks before destroying.
        self._nc_loop_running = False
        self._refresh_active = False
        for after_id in self._after_ids:
            with contextlib.suppress(Exception):
                self.after_cancel(after_id)
        self._after_ids.clear()

        plt.close("all")

        # Close log file handle so tail threads exit cleanly.
        if self._log_file_handle is not None:
            with contextlib.suppress(Exception):
                self._log_file_handle.close()
            self._log_file_handle = None

        if self._proc and self._proc.poll() is None:
            pid = self._proc.pid
            choice = messagebox.askyesnocancel(
                "Pipeline running",
                f"Adapt pipeline (PID {pid}) is still running.\n\n"
                "Yes → Kill it now\n"
                f"No  → Keep it running in the background\n"
                "Cancel → Stay in dashboard",
                parent=self,
            )
            if choice is None:
                # User cancelled — restore refresh loop and stay open.
                self._refresh_active = True
                self._after_ids.append(self.after(POLL_MS, self._schedule_refresh))
                self._after_ids.append(self.after(1000, self._status_tick))
                return
            if choice:
                try:
                    os.killpg(os.getpgid(self._proc.pid), 15)
                except OSError:
                    self._proc.terminate()
                with contextlib.suppress(subprocess.TimeoutExpired):
                    self._proc.wait(timeout=3)

        self.destroy()

    # ── Auto-refresh ──────────────────────────────────────────────────────────

    def _schedule_refresh(self):
        if self._auto_refresh_var.get():
            self._refresh_all()
        self._after_ids.append(self.after(POLL_MS, self._schedule_refresh))

    def _status_tick(self):
        """Update status bar every second: scan time + countdown to next check."""
        if not self._refresh_active:
            return
        secs = max(0, int(self._next_refresh_at - time.time()))
        scan_str = self._last_scan_dt.strftime("%H:%M:%S UTC") if self._last_scan_dt else "—"
        self.status_var.set(
            f"{self._status_base}  |  Last scan: {scan_str}  |  Next check: {secs}s"
        )
        self._update_pipeline_badge()
        self._after_ids.append(self.after(1000, self._status_tick))

    def _refresh_all(self):
        repo = self._repo_root.get().strip()
        radar = self._radar.get().strip().upper()
        if not repo or not radar:
            return

        all_nc = self._get_nc_files(repo, radar)
        self._all_nc_files = all_nc
        nc_files = all_nc
        labels = [self._nc_label(p) for p in nc_files]

        cur = self.scan_var.get()
        self.scan_cb["values"] = labels
        if labels and cur not in labels:
            self.scan_var.set(labels[-1])

        if len(all_nc) > self._last_n_plots and all_nc:
            self._last_n_plots = len(all_nc)

        running = _pipeline_running() or (self._proc and self._proc.poll() is None)
        state = "Running" if running else ("Idle" if not all_nc else "Done")
        self._status_base = f"{state}  |  Radar: {radar}  |  Scans: {len(all_nc)}"
        self._next_refresh_at = time.time() + POLL_MS / 1000

        # ── Auto-update live canvas when a new NC file appears ────────────────
        if HAS_DATA and not self._nc_loop_running and all_nc:
            latest = all_nc[-1]
            if self._last_rendered_nc is not None and self._last_rendered_nc != latest:
                # New file appeared — update existing canvas in place or re-open
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

        self._refresh_table()
        if self._nb.index("current") == 2:
            self._flush_log()

    # ── NC file helpers ───────────────────────────────────────────────────────

    def _get_nc_files(self, repo, radar):
        """Get all analysis NC files across all date directories."""
        analysis_dir = Path(repo) / radar / "analysis"
        if not analysis_dir.exists():
            return []

        # Collect NC files from all date subdirectories
        all_nc = []
        for date_dir in list(analysis_dir.iterdir()):  # eager: release FD immediately
            if date_dir.is_dir() and len(date_dir.name) == 8 and date_dir.name.isdigit():
                all_nc.extend(list(date_dir.glob("*_analysis.nc")))  # eager

        # Sort by filename (contains timestamp)
        return sorted(all_nc, key=lambda p: p.name)

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

    def _prev_scan(self):
        if not self._all_nc_files:
            return
        idx = self._current_scan_idx()
        step = max(1, self._bundle_var.get()) if self._bundle_var else 1
        new_idx = max(0, (idx if idx >= 0 else len(self._all_nc_files)) - step)
        if new_idx != idx:
            self.scan_var.set(self._nc_label(self._all_nc_files[new_idx]))
            self._inline_render()

    def _next_scan(self):
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

    def _show_latest(self):
        """Render the most recent NC file and enable live auto-refresh."""
        repo = self._repo_root.get().strip()
        radar = self._radar.get().strip().upper()
        if not repo or not radar:
            return
        nc_files = self._get_nc_files(repo, radar)
        if not nc_files:
            messagebox.showinfo(
                "No data",
                f"No analysis files found in:\n{Path(repo) / radar / 'analysis'}",
                parent=self,
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
        if not HAS_MPL or not HAS_DATA:
            messagebox.showerror(
                "Missing dependencies",
                "matplotlib, numpy, pandas, xarray required.",
                parent=self,
            )
            return
        repo = self._repo_root.get().strip()
        radar = self._radar.get().strip().upper()
        if not repo or not radar:
            messagebox.showerror("Missing input", "Set Radar ID and Repo path first.", parent=self)
            return

        nc_files = self._get_nc_files(repo, radar)
        if not nc_files:
            messagebox.showinfo(
                "Not found",
                f"No analysis files found in:\n{Path(repo) / radar / 'analysis'}",
                parent=self,
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
        """Load per-cell data for the current run into self._current_cell_df.

        Tries cells_by_scan (SQLite) first — contains cell_uid, cell_label, and
        all cell_stats columns. Falls back to parquet for legacy data.
        """
        self._current_cell_df = None
        self._current_run_id = None

        db_path = Path(repo) / radar / "catalog.db"
        if db_path.exists():
            try:
                import sqlite3

                from adapt.persistence.track_store import TrackStore

                conn = sqlite3.connect(str(db_path))
                conn.row_factory = sqlite3.Row
                run_row = conn.execute(
                    "SELECT run_id FROM cells_by_scan ORDER BY scan_time DESC LIMIT 1"
                ).fetchone()
                if run_row:
                    run_id = run_row["run_id"]
                    conn.close()
                    with contextlib.closing(TrackStore(db_path, readonly=True)) as ts_obj:
                        rows = (
                            ts_obj._connect()
                            .execute(
                                "SELECT * FROM cells_by_scan WHERE run_id=? ORDER BY scan_time",
                                (run_id,),
                            )
                            .fetchall()
                        )
                    if rows:
                        self._current_cell_df = pd.DataFrame([dict(r) for r in rows])
                        self._current_run_id = run_id
                        return
                conn.close()
            except Exception:
                logger.exception("Failed to load cells from SQLite catalog")

        # Fallback: parquet (may not contain cell_uid)
        pqs = sorted((Path(repo) / radar / "analysis").glob("analysis2d_*.parquet"))
        if pqs:
            try:
                dfs = [pd.read_parquet(p) for p in pqs]
                self._current_cell_df = pd.concat(dfs, ignore_index=True)
            except Exception:
                logger.exception("Failed to load fallback parquet cell data")

    # ── NC loop render (cycle through N frames) ───────────────────────────────

    def _toggle_nc_loop(self):
        if self._nc_loop_running:
            self._nc_loop_running = False
            self.btn_loop.config(text="Show Loop")
            return
        repo = self._repo_root.get().strip()
        radar = self._radar.get().strip().upper()
        if not repo or not radar:
            return
        n = max(2, self._loop_n_var.get())
        nc_files = self._get_nc_files(repo, radar)[-n:]
        if not nc_files:
            messagebox.showinfo("No data", "No analysis NC files found.", parent=self)
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
        self._after_ids.append(self.after(dt, self._nc_loop_step))

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
        self._after_ids.append(self.after(dt, self._nc_loop_step))

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
        """Render dataset into the radar axes. Keeps ds open."""
        # Resolve ax — always the leftmost (index 0) in the GridSpec figure
        if ax is None:
            ax = fig.axes[0]

        # Save zoom before ax.clear() wipes it
        if self._saved_zoom is not None or (ax.lines or ax.collections):
            try:
                xlim, ylim = ax.get_xlim(), ax.get_ylim()
                if xlim != (0.0, 1.0) or ylim != (0.0, 1.0):
                    self._saved_zoom = (xlim, ylim)
            except Exception:
                pass

        ax.clear()
        ax.set_facecolor("white")
        # Track overlay artists were removed by ax.clear(); reset references
        self._track_overlay = {}
        # NOTE: _selected_cells is intentionally NOT cleared here

        # Close previous dataset
        if self._current_nc_ds is not None and self._current_nc_ds is not ds:
            with contextlib.suppress(Exception):
                self._current_nc_ds.close()
        self._current_nc_ds = ds
        self._cell_contours = {}
        for var in self._hv.values():
            var.set("\u2014")

        radar_id = ds.attrs.get("radar", ds.attrs.get("radar_id", ""))
        tv = ds.coords["time"].values if "time" in ds.coords else None
        ts = pd.Timestamp(
            tv.item()
            if tv is not None and np.ndim(tv) == 0
            else tv[0]
            if tv is not None
            else pd.Timestamp.now()
        )
        tstr = ts.strftime("%Y-%m-%d %H:%M:%S UTC")
        self._last_scan_dt = ts.to_pydatetime()
        self._current_scan_ts = ts  # Store for hover filtering

        x_km = ds["x"].values / 1000.0
        y_km = ds["y"].values / 1000.0
        y_grid, x_grid = np.meshgrid(y_km, x_km, indexing="ij")
        labels_data = ds["cell_labels"].values

        # ── Grayscale reflectivity background ────────────────────────────────
        refl = ds["reflectivity"].values.astype(float)
        refl_bg = np.ma.masked_where(np.isnan(refl) | (refl < 10), refl)
        cmap_gray = copy.copy(plt.get_cmap("gray_r"))
        cmap_gray.set_bad(alpha=0)
        # vmin=10 → light gray (~0.35 on gray_r), vmax=50 → black
        bg_alpha = self._bg_alpha_var.get() if self._bg_alpha_var else 0.35
        ax.pcolormesh(
            x_km,
            y_km,
            refl_bg,
            cmap=cmap_gray,
            vmin=10,
            vmax=40,
            shading="auto",
            alpha=bg_alpha,
            zorder=2,
        )

        # ── User-selected variable overlay (cells only) ───────────────────────
        var_name = self._plot_var.get() if self._plot_var is not None else "reflectivity"
        if var_name not in ds.data_vars:
            var_name = "reflectivity"
        vdef = _VAR_DEFAULTS.get(var_name, (10, 60, "dBZ", "viridis"))
        try:
            vmin = float(self._plot_vmin.get() if self._plot_vmin else vdef[0])
        except (ValueError, AttributeError):
            vmin = vdef[0]
        try:
            vmax = float(self._plot_vmax.get() if self._plot_vmax else vdef[1])
        except (ValueError, AttributeError):
            vmax = vdef[1]
        unit = vdef[2]
        cmap_str = vdef[3]
        var_lbl = _VAR_LABELS.get(var_name, var_name)

        raw = ds[var_name].values.astype(float)
        masked = np.ma.masked_where(np.isnan(raw) | (labels_data <= 0), raw)
        cmap_ov = copy.copy(plt.get_cmap(cmap_str))
        cmap_ov.set_bad(alpha=0)
        im_ov = ax.pcolormesh(
            x_km,
            y_km,
            masked,
            cmap=cmap_ov,
            vmin=vmin,
            vmax=vmax,
            shading="auto",
            alpha=0.90,
            zorder=3,
        )

        if self._cbar_ax is not None:
            # Reset the axes locator before each colorbar creation. cla() leaves
            # _axes_locator intact; each new colorbar wraps the previous locator
            # in _ColorbarAxesLocator, building a chain that causes RecursionError
            # after ~1000 redraws.
            self._cbar_ax.set_axes_locator(None)
            self._colorbar = fig.colorbar(im_ov, cax=self._cbar_ax, label=unit)
        else:
            self._colorbar = fig.colorbar(im_ov, ax=ax, label=unit, fraction=0.046, pad=0.04)

        # ── Cell contours ─────────────────────────────────────────────────────
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
            self._cell_contours[int(cell_id)] = cs

        # ── Projection contours ───────────────────────────────────────────────
        if "cell_projections" in ds.data_vars:
            proj_da = ds["cell_projections"]
            fo = "frame_offset"
            if fo in proj_da.dims:
                n_frames = len(proj_da[fo])
                max_proj = self._max_proj_var.get() if self._max_proj_var else 0
                end_frame = n_frames if max_proj == 0 else min(n_frames, max_proj + 1)
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
        if (
            self._show_flow_var is not None
            and self._show_flow_var.get()
            and "heading_x" in ds.data_vars
            and "heading_y" in ds.data_vars
        ):
            hx, hy = ds["heading_x"].values, ds["heading_y"].values
            if not np.all(np.isnan(hx)):
                s = 12
                yi_idx = np.arange(0, len(y_km), s)
                xi_idx = np.arange(0, len(x_km), s)
                Xs, Ys = np.meshgrid(x_km[xi_idx], y_km[yi_idx])
                q = ax.quiver(
                    Xs,
                    Ys,
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
                q._adapt_flow = True

        self._add_basemap(ax, ds, x_km, y_km)
        ax.set_xlabel("X (km)")
        ax.set_ylabel("Y (km)")
        ax.tick_params(reset=True)
        ax.grid(True, alpha=0.3, zorder=3)
        ax.set_title(f"{radar_id}  {var_lbl} [{tstr}]", fontsize=11, fontweight="bold")

        # ── Legend ────────────────────────────────────────────────────────────
        legend_handles = [
            mpatches.Patch(facecolor="gray", alpha=0.6, label="Stratiform"),
            mlines.Line2D([], [], color="#2C3539", linewidth=0.8, label="Cell boundary"),
            mlines.Line2D(
                [],
                [],
                color="#2C3539",
                linewidth=1.2,
                linestyle="dashed",
                label="Projection",
            ),
            mlines.Line2D(
                [],
                [],
                color="cyan",
                linewidth=1.5,
                marker="o",
                markersize=4,
                label="Track",
            ),
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

        # Restore user zoom/pan if saved
        if self._saved_zoom is not None:
            ax.set_xlim(self._saved_zoom[0])
            ax.set_ylim(self._saved_zoom[1])

        # Re-draw selected-cell overlays for cells present in this scan
        self._redraw_overlays_on(ax, ds)

    def _redraw_overlays_on(self, ax, ds) -> None:
        """Re-draw track paths and centroid markers for all selected cells."""
        # Always clear old overlay artists first (ax.clear() invalidates them)
        for artists in self._track_overlay.values():
            for art in artists:
                with contextlib.suppress(Exception):
                    art.remove()
        self._track_overlay = {}

        if not self._selected_cells or not HAS_DATA:
            return

        cell_labels_da = ds.get("cell_labels", ds.get("labels", None))
        if cell_labels_da is None:
            return
        labels_arr = cell_labels_da.values

        # Build uid_map for this scan (label int → cell_uid)
        uid_map: dict[int, str] = {}
        if self._current_cell_df is not None and not self._current_cell_df.empty:
            df = self._current_cell_df
            if "cell_label" in df.columns and "cell_uid" in df.columns:
                if self._current_scan_ts is not None and "scan_time" in df.columns:
                    st = pd.to_datetime(df["scan_time"], utc=True)
                    scan_ts = pd.Timestamp(self._current_scan_ts)
                    if scan_ts.tzinfo is None:
                        scan_ts = scan_ts.tz_localize("UTC")
                    scan_df = df[(st - scan_ts).abs() < pd.Timedelta(seconds=60)]
                else:
                    scan_df = df
                uid_map = dict(
                    zip(scan_df["cell_label"].astype(int), scan_df["cell_uid"], strict=False)
                )
        visible = _visible_uids_in_scan(labels_arr, uid_map)

        repo = self._repo_root.get().strip()
        radar = self._radar.get().strip().upper()
        db_path = Path(repo) / radar / "catalog.db"
        x_metres = ds["x"].values
        y_metres = ds["y"].values

        for uid, slot in self._selected_cells.items():
            color = self._color_slots[slot % len(self._color_slots)]
            artists = []

            # Load full track history for path drawing
            history_df = None
            if self._current_run_id and db_path.exists():
                try:
                    from adapt.persistence.track_store import TrackStore

                    with contextlib.closing(TrackStore(db_path, readonly=True)) as _ts:
                        history_df = _ts.get_track_history(self._current_run_id, uid)
                except Exception:
                    logger.exception("Failed to load track history for %s", uid)
            if (history_df is None or history_df.empty) and self._current_cell_df is not None:
                df = self._current_cell_df
                if "cell_uid" in df.columns:
                    history_df = df[df["cell_uid"] == uid].copy()

            # Draw track path (line + dots)
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

            # Draw current-scan star only when cell is present in this scan
            if uid in visible and self._current_cell_df is not None:
                df = self._current_cell_df
                if "cell_uid" in df.columns:
                    if self._current_scan_ts is not None and "scan_time" in df.columns:
                        st = pd.to_datetime(df["scan_time"], utc=True)
                        scan_ts = pd.Timestamp(self._current_scan_ts)
                        if scan_ts.tzinfo is None:
                            scan_ts = scan_ts.tz_localize("UTC")
                        scan_rows = df[
                            ((st - scan_ts).abs() < pd.Timedelta(seconds=60))
                            & (df["cell_uid"] == uid)
                        ]
                    else:
                        scan_rows = df[df["cell_uid"] == uid]
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
                self._track_overlay[uid] = artists

    @staticmethod
    def _add_basemap(ax, ds, x_km, y_km):
        _add_basemap_fn(ax, ds, x_km, y_km)

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
        if not HAS_MPL or not HAS_DATA or self._canvas_refs is None:
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
        repo = self._repo_root.get().strip()
        radar = self._radar.get().strip().upper()
        db_path = Path(repo) / radar / "catalog.db"

        # Resolve cell_uid for clicked cell via SQLite (avoids scan_time format issues)
        cell_uid = None
        if self._current_run_id and db_path.exists() and self._current_scan_ts is not None:
            try:
                from adapt.persistence.track_store import TrackStore

                scan_time_dt = pd.Timestamp(self._current_scan_ts).to_pydatetime()
                with contextlib.closing(TrackStore(db_path, readonly=True)) as _ts:
                    scan_cells = _ts.get_cells_by_scan(self._current_run_id, scan_time_dt)
                if not scan_cells.empty and "cell_label" in scan_cells.columns:
                    matched = scan_cells[scan_cells["cell_label"] == cell_id]
                    if not matched.empty:
                        r = matched.iloc[0]
                        cell_uid = r.get("cell_uid")
            except Exception:
                logger.exception("Failed to resolve cell UID from track store")

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

        # Load full tracking history from birth to current scan
        history_df = None
        if self._current_run_id and db_path.exists():
            try:
                from adapt.persistence.track_store import TrackStore

                with contextlib.closing(TrackStore(db_path, readonly=True)) as _ts:
                    history_df = _ts.get_track_history(self._current_run_id, str(cell_uid))
            except Exception:
                logger.exception("Failed to load tracking history from track store")

        if history_df is None or history_df.empty:
            df = self._current_cell_df
            if df is not None and cell_uid is not None and "cell_uid" in df.columns:
                history_df = df[df["cell_uid"] == cell_uid].copy()

        uid_str = str(cell_uid) if cell_uid is not None else None
        if uid_str is None:
            return

        if uid_str in self._selected_cells:
            # Deselect: remove from selection and overlays
            slot = self._selected_cells.pop(uid_str)
            for artist in self._track_overlay.pop(uid_str, []):
                with contextlib.suppress(Exception):
                    artist.remove()
        else:
            # Select: assign color slot
            slot = _next_free_color_slot(self._selected_cells)
            if slot is None:
                action = self._cfg.get("overflow_action", "ask")
                if action == "ask":
                    action = self._ask_overflow_action()
                slot = _apply_overflow_action(action, self._selected_cells)
                if slot is None:
                    return  # user chose ignore
            self._selected_cells[uid_str] = slot

        self._redraw_overlays_on(ax_radar, self._current_nc_ds)
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
        win = tk.Toplevel(self)
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

        self.wait_window(win)
        return result[0]

    def _update_time_series_all(self) -> None:
        """Re-draw all 3 time-series plots for all currently selected tracks."""
        if self._ts_axes is None:
            return
        ax1, ax2, ax3 = self._ts_axes
        for ax in (ax1, ax2, ax3):
            ax.clear()

        group_names = self._cfg.get("plot_assignments", ["Area", "Reflectivity", "ZDR"])
        axes = [ax1, ax2, ax3]

        # Variables referenced by the selected groups — used to decide whether the
        # cell_volume_stats table must be joined onto each track's history.
        needed_vars: set[str] = set()
        for gname in group_names:
            grp = self._cfg.get("plot_groups", {}).get(gname, {})
            needed_vars.update(grp.get("variables", []))

        repo = self._repo_root.get().strip()
        radar = self._radar.get().strip().upper()
        db_path = Path(repo) / radar / "catalog.db"
        cur_t = (
            pd.Timestamp(self._current_scan_ts, tz="UTC")
            if self._current_scan_ts is not None
            else None
        )

        # Lightning columns come from the xlma_stat_minutes extension table, read
        # only through the public API (RepositoryClient). Build the client once
        # per refresh and only when a selected group needs lightning.
        needs_lightning = any(
            str(v).startswith(_LIGHTNING_PREFIXES)
            for g in group_names
            for v in self._cfg.get("plot_groups", {}).get(g, {}).get("variables", [])
        )
        lightning_client = None

        for uid, slot in self._selected_cells.items():
            color = self._color_slots[slot % len(self._color_slots)]
            history_df = None
            if self._current_run_id and db_path.exists():
                try:
                    from adapt.persistence.track_store import TrackStore

                    with contextlib.closing(TrackStore(db_path, readonly=True)) as _ts:
                        history_df = _ts.get_track_history(self._current_run_id, uid)
                except Exception:
                    logger.exception("Failed to load track history for %s", uid)
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
                        if lightning_client is None:
                            from adapt.api.client import RepositoryClient

                            lightning_client = RepositoryClient(repo)
                        known = set(lightning_client.tables(radar)["table_name"])
                        if "xlma_stat_minutes" in known:
                            lma_df = lightning_client.table(
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
                group = self._cfg.get("plot_groups", {}).get(group_name, {})
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

        if lightning_client is not None:
            with contextlib.suppress(Exception):
                lightning_client.close()

        for ax, group_name in zip(axes, group_names, strict=False):
            group = self._cfg.get("plot_groups", {}).get(group_name, {})
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

    def _on_escape(self, _event=None) -> None:
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
        self._colorbar = None
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
        if not HAS_DATA or self._current_nc_ds is None:
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

    # ── Cell statistics ───────────────────────────────────────────────────────

    def _refresh_table(self):
        if not HAS_DATA:
            return
        repo = self._repo_root.get().strip()
        radar = self._radar.get().strip().upper()
        if not repo or not radar:
            return

        df = None
        # Try SQLite cells_by_scan first
        db_path = Path(repo) / radar / "catalog.db"
        if db_path.exists() and self._current_run_id:
            try:
                from adapt.persistence.track_store import TrackStore

                with contextlib.closing(TrackStore(db_path, readonly=True)) as ts_obj:
                    rows = (
                        ts_obj._connect()
                        .execute(
                            "SELECT * FROM cells_by_scan WHERE run_id=? "
                            "ORDER BY scan_time, cell_uid",
                            (self._current_run_id,),
                        )
                        .fetchall()
                    )
                if rows:
                    df = pd.DataFrame([dict(r) for r in rows])
            except Exception:
                logger.exception("DB stats query failed; falling back to parquet")
                df = None

        # Fallback: parquet
        if df is None or df.empty:
            pqs = sorted((Path(repo) / radar / "analysis").glob("analysis2d_*.parquet"))
            if not pqs:
                self.stats_lbl.config(text="No data yet.")
                return
            try:
                dfs = [pd.read_parquet(p) for p in pqs]
                df = pd.concat(dfs, ignore_index=True)
            except Exception as e:
                logger.exception("Failed to load parquet files for stats table")
                self.stats_lbl.config(text=f"Error: {e}")
                return

        if df is None or df.empty:
            self.stats_lbl.config(text="No data yet.")
            return

        try:
            df["scan_time"] = pd.to_datetime(df["scan_time"], utc=True)
            df["time_label"] = df["scan_time"].dt.strftime("%H:%M:%S")
        except Exception:
            logger.exception("Failed to parse scan_time column for table display")

        # Update slider range bounds from data
        for col, (lo_v, hi_v) in self._flt.items():
            if col not in df.columns:
                continue
            col_min = float(df[col].min(skipna=True))
            col_max = float(df[col].max(skipna=True))
            if lo_v.get() < col_min:
                lo_v.set(col_min)
            if hi_v.get() > col_max:
                hi_v.set(col_max)

        mask = pd.Series(True, index=df.index)
        for col, (lo_v, hi_v) in self._flt.items():
            if col in df.columns:
                with contextlib.suppress(Exception):
                    mask &= df[col].between(float(lo_v.get()), float(hi_v.get()))

        # Cell UID prefix filter
        pid_prefix = self._cell_uid_filter.get().strip().upper() if self._cell_uid_filter else ""
        if pid_prefix and "cell_uid" in df.columns:
            mask &= df["cell_uid"].astype(str).str.upper().str.startswith(pid_prefix)

        filt = df[mask]

        def _avg(col, fmt=".1f"):
            return (
                f"{filt[col].mean():{fmt}}" if col in filt.columns and not filt.empty else "\u2014"
            )

        self.stats_lbl.config(
            text=(
                f"Showing {len(filt)} / {len(df)} cells"
                f"  |  Avg dBZ: {_avg('radar_reflectivity_mean')}"
                f"  |  Avg area: {_avg('cell_area_sqkm')} km\u00b2"
                f"  |  Avg ZDR: {_avg('radar_differential_reflectivity_mean', '.2f')}"
            )
        )

        # Build column list dynamically from available data
        preferred = [
            "time_label",
            "cell_uid",
            "cell_label",
            "cell_area_sqkm",
            "area_40dbz_km2",
            "radar_reflectivity_max",
            "radar_reflectivity_mean",
            "radar_differential_reflectivity_max",
            "radar_differential_reflectivity_mean",
            "cell_centroid_mass_lat",
            "cell_centroid_mass_lon",
            "n_adjacent_cells",
        ]
        show_cols = [c for c in preferred if c in filt.columns]
        # Rebuild treeview columns if they changed
        if list(self._tv_cols) != show_cols:
            self._tv_cols = show_cols
            self.tv["columns"] = show_cols
            col_widths = {
                "time_label": 65,
                "cell_uid": 160,
                "cell_label": 55,
                "cell_area_sqkm": 70,
                "area_40dbz_km2": 70,
                "radar_reflectivity_max": 75,
                "radar_reflectivity_mean": 75,
                "radar_differential_reflectivity_max": 75,
                "radar_differential_reflectivity_mean": 75,
                "cell_centroid_mass_lat": 80,
                "cell_centroid_mass_lon": 80,
                "n_adjacent_cells": 65,
            }
            for c in show_cols:
                hdr = (
                    c.replace("radar_differential_reflectivity_", "ZDR ")
                    .replace("radar_reflectivity_", "Z ")
                    .replace("cell_", "")
                    .replace("_", " ")
                )
                self.tv.heading(c, text=hdr)
                self.tv.column(c, width=col_widths.get(c, 70), anchor="center")

        self.tv.delete(*self.tv.get_children())
        for _, row in filt[show_cols].iterrows():
            vals = []
            for c in show_cols:
                v = row.get(c, "")
                if isinstance(v, float):
                    vals.append(f"{v:.2f}" if not pd.isna(v) else "\u2014")
                else:
                    if c == "cell_uid":
                        vals.append(_cell_uid_disp(v))
                    else:
                        vals.append(str(v) if not pd.isna(v) else "\u2014")
            self.tv.insert("", "end", values=vals)

    # ── Log ───────────────────────────────────────────────────────────────────

    def _append_log(self, line, tag=""):
        self.log_text.config(state="normal")
        self.log_text.insert("end", line + "\n", tag)
        self.log_text.see("end")
        self.log_text.config(state="disabled")

    def _flush_log(self):
        self.log_text.config(state="normal")
        self.log_text.delete("1.0", "end")
        for ln in self._log_lines[-200:]:
            tag = "error" if "ERROR" in ln else ("warning" if "WARNING" in ln else "")
            self.log_text.insert("end", ln + "\n", tag)
        self.log_text.see("end")
        self.log_text.config(state="disabled")

    def _clear_log(self):
        self._log_lines.clear()
        self.log_text.config(state="normal")
        self.log_text.delete("1.0", "end")
        self.log_text.config(state="disabled")

    def _on_tab_change(self, _):
        idx = self._nb.index("current")
        if idx == 2:
            self._flush_log()


# ── Entry point ───────────────────────────────────────────────────────────────


def main(repo: str | None = None):
    """Launch the Adapt Dashboard.

    Parameters
    ----------
    repo : str, optional
        Repository path to preload
    """
    app = AdaptDashboard(repo=repo)
    app.mainloop()


if __name__ == "__main__":
    main()
