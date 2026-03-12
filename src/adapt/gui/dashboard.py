"""Adapt Radar Dashboard — Tkinter GUI for exploring pipeline outputs.

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

import copy
import os
import shutil
import subprocess
import sys
import threading
import time
from datetime import datetime
from pathlib import Path

# ── PROJ data path fix (must be before contextily/rasterio) ──────────────────
# Force-set PROJ paths to the active environment's proj.db.
# Cannot use setdefault: PROJ_DATA may already point to a different conda env.
try:
    import pyproj as _pyproj
    _pd = _pyproj.datadir.get_data_dir()
    os.environ['PROJ_DATA'] = _pd
    os.environ['PROJ_LIB']  = _pd
except Exception:
    pass

# ── Tkinter ───────────────────────────────────────────────────────────────────
import tkinter as tk
from tkinter import ttk, filedialog, scrolledtext, messagebox

# ── Optional deps ─────────────────────────────────────────────────────────────
try:
    from PIL import Image, ImageTk
    HAS_PIL = True
except ImportError:
    HAS_PIL = False

try:
    import matplotlib
    matplotlib.use('TkAgg')
    import matplotlib.pyplot as plt
    from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg, NavigationToolbar2Tk
    HAS_MPL = True
except ImportError:
    HAS_MPL = False

try:
    import contextily as ctx
    HAS_CTX = True
except ImportError:
    ctx = None
    HAS_CTX = False

import cmweather.cm  # registers ChaseSpectral and other radar colormaps
if HAS_MPL:
    matplotlib.use('TkAgg')
REFL_CMAP = 'ChaseSpectral'

try:
    import numpy as np
    import pandas as pd
    import xarray as xr
    HAS_DATA = True
except ImportError:
    HAS_DATA = False

try:
    from pyproj import Transformer
    HAS_PROJ = True
except ImportError:
    HAS_PROJ = False

# ── Constants ─────────────────────────────────────────────────────────────────
POLL_MS   = 10_000  # auto-refresh every 10 s
LOG_MAX   = 500
_PID_FILE = Path.home() / '.adapt' / 'pipeline.pid'

# ── Stats strip theme ─────────────────────────────────────────────────────────
_STRIP_BG = '#252526'   # very dark gray — readable on any system theme
_BOX_BG   = '#1e1e1e'   # slightly darker for individual boxes
_FONT_VAL = ('Courier', 15, 'bold')
_FONT_LBL = ('Courier', 12)
# Each row: (top_label, hv_key_top, top_fg, bot_label, hv_key_bot, bot_fg)
# Lat(M)/Lon(M) removed — mouse coords are shown in toolbar coordinate bar
_BOX_DEFS = [
    ('Cell',   'cell_id',  '#ffffff', 'Area km²', 'area',     '#ffff44'),
    ('Lat(C)', 'lat_mass', '#44ff88', 'Lon(C)',   'lon_mass', '#44ff88'),
    ('dBZ mean', 'dbz_mean', '#ff8800', 'dBZ max',  'dbz_max',  '#ffcc44'),
    ('ZDR mean', 'zdr_mean', '#ff44ff', 'ZDR max',  'zdr_max',  '#ff88ff'),
    ('Vel mean', 'vel_mean', '#44ffff', 'SpW mean',  'sw_mean',  '#ff5555'),
]
_HV_KEYS = ('cell_id', 'area',
            'lat_mass', 'lon_mass',
            'dbz_mean', 'dbz_max', 'zdr_mean', 'zdr_max',
            'vel_mean', 'sw_mean')

# ── Variable selector defaults: (vmin, vmax, unit, cmap) ─────────────────────
_VAR_DEFAULTS = {
    'reflectivity':              (10,  65,  'dBZ', 'ChaseSpectral'),
    'differential_reflectivity': (-2,  8,   'dB',  'RdYlBu_r'),
    'velocity':                  (-30, 30,  'm/s', 'RdBu_r'),
    'spectrum_width':            (0,   15,  'm/s', 'plasma'),
}
_VAR_LABELS = {
    'reflectivity':              'Reflectivity',
    'differential_reflectivity': 'ZDR',
    'velocity':                  'Velocity',
    'spectrum_width':            'Spec Width',
}


# ── Compact toolbar: no Back/Forward; shows x y lat lon in coordinate bar ────
if HAS_MPL:
    class _CompactToolbar(NavigationToolbar2Tk):
        toolitems = [t for t in NavigationToolbar2Tk.toolitems
                     if t[0] not in ('Back', 'Forward')]

        def __init__(self, canvas, window, *, pack_toolbar=True,
                     lat0=0.0, lon0=0.0):
            self._ltrans = None
            if HAS_PROJ and (lat0 or lon0):
                try:
                    self._ltrans = Transformer.from_crs(
                        f'+proj=aeqd +lat_0={lat0} +lon_0={lon0} +units=m',
                        'EPSG:4326', always_xy=True)
                except Exception:
                    pass
            super().__init__(canvas, window, pack_toolbar=pack_toolbar)

        def set_message(self, s):
            if self._ltrans is not None and s and 'x=' in s:
                try:
                    toks = {t.split('=')[0]: float(t.split('=')[1])
                            for t in s.split() if '=' in t and len(t.split('=')) == 2}
                    x_km = toks.get('x', 0.0)
                    y_km = toks.get('y', 0.0)
                    lon_v, lat_v = self._ltrans.transform(
                        x_km * 1000.0, y_km * 1000.0)
                    s = (f'x={x_km:.2f}  y={y_km:.2f}'
                         f'    {lat_v:.4f}\u00b0  {lon_v:.4f}\u00b0')
                except Exception:
                    pass
            super().set_message(s)
else:
    _CompactToolbar = None


# ── Range slider widget ───────────────────────────────────────────────────────

class _RangeSlider(tk.Canvas):
    """Single-bar dual-handle range slider."""
    _PAD = 10
    _R   = 7
    _CY  = 14

    def __init__(self, parent, from_, to, lo_var, hi_var, fmt='.1f', **kw):
        kw.setdefault('height', 28)
        kw.setdefault('highlightthickness', 0)
        super().__init__(parent, **kw)
        self._from, self._to = from_, to
        self._lo, self._hi   = lo_var, hi_var
        self._fmt            = fmt
        self._drag           = None
        self.bind('<Configure>',       lambda _: self._draw())
        self.bind('<ButtonPress-1>',   self._on_press)
        self.bind('<B1-Motion>',       self._on_drag)
        self.bind('<ButtonRelease-1>', lambda _: setattr(self, '_drag', None))
        lo_var.trace_add('write', lambda *_: self._draw())
        hi_var.trace_add('write', lambda *_: self._draw())

    def _tw(self):
        return max(self.winfo_width(), 160) - 2 * self._PAD

    def _v2x(self, v):
        ratio = (v - self._from) / (self._to - self._from)
        return self._PAD + max(0.0, min(1.0, ratio)) * self._tw()

    def _x2v(self, x):
        ratio = (x - self._PAD) / self._tw()
        return self._from + max(0.0, min(1.0, ratio)) * (self._to - self._from)

    def _draw(self):
        self.delete('all')
        w  = self._PAD + self._tw() + self._PAD
        cy = self._CY
        lx = self._v2x(self._lo.get())
        hx = self._v2x(self._hi.get())
        r  = self._R
        self.create_line(self._PAD, cy, w - self._PAD, cy,
                         fill='#cccccc', width=4, capstyle='round')
        self.create_line(lx, cy, hx, cy,
                         fill='#4a9eca', width=4, capstyle='round')
        for x, tag in ((lx, 'lo'), (hx, 'hi')):
            self.create_oval(x - r, cy - r, x + r, cy + r,
                             fill='#2980b9', outline='#1a5276', width=1,
                             tags=tag)

    def _on_press(self, event):
        lx = self._v2x(self._lo.get())
        hx = self._v2x(self._hi.get())
        self._drag = 'lo' if abs(event.x - lx) <= abs(event.x - hx) else 'hi'

    def _on_drag(self, event):
        val = self._x2v(event.x)
        if self._drag == 'lo':
            self._lo.set(min(val, self._hi.get()))
        else:
            self._hi.set(max(val, self._lo.get()))
        self.event_generate('<<RangeChanged>>')


# ── Helpers ───────────────────────────────────────────────────────────────────

def _find_adapt_exe() -> list:
    """Return command list for adapt run-nexrad."""
    candidate = Path(sys.executable).parent / 'adapt'
    if candidate.exists():
        return [str(candidate)]
    found = shutil.which('adapt')
    if found:
        return [found]
    return [sys.executable, '-m', 'adapt.cli']


def _pipeline_running() -> bool:
    """Return True if a pipeline PID file exists and the process is alive."""
    if not _PID_FILE.exists():
        return False
    try:
        pid = int(_PID_FILE.read_text().strip())
        os.kill(pid, 0)
        return True
    except Exception:
        return False


def _list_radars(repo: Path) -> list:
    """Return radar subdirs that look like NEXRAD sites (4 uppercase letters)."""
    if not repo.exists():
        return []
    return sorted(
        d.name for d in repo.iterdir()
        if d.is_dir() and len(d.name) == 4 and d.name.isupper()
        and (d / 'nexrad').exists()
    )


def _list_runs(repo: Path) -> list:
    """Return (run_id, mtime_str) pairs from runtime_config_*.json files."""
    configs = sorted(repo.glob('runtime_config_*.json'), reverse=True)
    runs = []
    for c in configs:
        rid = c.stem.replace('runtime_config_', '')
        mtime = datetime.fromtimestamp(c.stat().st_mtime).strftime('%m-%d %H:%M')
        runs.append(f'{rid}  ({mtime})')
    return runs


# ── Main dashboard window ─────────────────────────────────────────────────────

class AdaptDashboard(tk.Tk):

    def __init__(self, repo: str = None):
        super().__init__()
        self.title('Adapt Radar Dashboard')
        self.geometry('1400x900')
        self.minsize(1000, 680)

        self._repo_root      = tk.StringVar(value=repo or '')
        self._radar          = tk.StringVar(value='')
        self._run_sel        = tk.StringVar(value='')
        self._proc           = None
        self._log_lines      = []
        self._today          = datetime.now().strftime('%Y%m%d')
        self._last_n_plots   = -1
        self._canvas_refs    = None   # (canvas, fig, toolbar, bottom)
        self._refresh_active = True

        # Inline render state
        self._current_nc_ds   = None   # loaded xarray Dataset
        self._current_cell_df = None   # loaded parquet DataFrame
        self._cell_contours   = {}     # cell_id -> contour set (ax2)
        self._hover_canvas    = None   # ref to mpl canvas for hover

        # NC loop animation state (replaces PNG loop)
        self._nc_loop_running = False
        self._nc_loop_index   = 0
        self._nc_loop_files   = []

        # Auto-refresh live tracking
        self._last_rendered_nc = None   # path of last auto-rendered NC file

        # Status bar state
        self._status_base      = 'Idle'
        self._last_scan_dt     = None   # datetime of last rendered scan
        self._next_refresh_at  = time.time() + POLL_MS / 1000

        # Plot variable controls (set by _build_scan_tab)
        self._plot_var    = None   # tk.StringVar set in _build_scan_tab
        self._plot_vmin   = None
        self._plot_vmax   = None
        self._max_proj_var = None  # tk.IntVar: 0 = all available proj steps

        self._build_ui()
        self.protocol('WM_DELETE_WINDOW', self._on_close)

        # Start auto-refresh and status countdown ticker
        self.after(500, self._schedule_refresh)
        self.after(1000, self._status_tick)

        if repo:
            self.after(200, self._on_repo_changed)

    # ── Build UI ──────────────────────────────────────────────────────────────

    def _build_ui(self):
        # ── Top toolbar ──────────────────────────────────────────────────────
        toolbar = ttk.Frame(self, padding=(6, 5))
        toolbar.pack(side='top', fill='x')

        # Row 1: Repo
        row1 = ttk.Frame(toolbar)
        row1.pack(fill='x')

        ttk.Label(row1, text='Output repo:').pack(side='left')
        repo_entry = ttk.Entry(row1, textvariable=self._repo_root, width=50)
        repo_entry.pack(side='left', padx=2)
        repo_entry.bind('<Return>', lambda _: self._on_repo_changed())
        ttk.Button(row1, text='Browse',
                   command=self._browse_repo).pack(side='left', padx=(2, 10))

        ttk.Separator(row1, orient='vertical').pack(side='left', fill='y', padx=4)
        ttk.Button(row1, text='Refresh',
                   command=self._refresh_all).pack(side='left', padx=2)

        # Row 2: Radar + Run + Pipeline control
        row2 = ttk.Frame(toolbar)
        row2.pack(fill='x', pady=(3, 0))

        ttk.Label(row2, text='Radar:').pack(side='left')
        self.radar_cb = ttk.Combobox(row2, textvariable=self._radar,
                                     width=8, state='readonly')
        self.radar_cb.pack(side='left', padx=(2, 10))
        self.radar_cb.bind('<<ComboboxSelected>>', lambda _: self._on_radar_changed())

        ttk.Label(row2, text='Run:').pack(side='left')
        self.run_cb = ttk.Combobox(row2, textvariable=self._run_sel,
                                   width=30, state='readonly')
        self.run_cb.pack(side='left', padx=(2, 14))

        ttk.Separator(row2, orient='vertical').pack(side='left', fill='y', padx=4)
        self.btn_start = ttk.Button(row2, text='Start Pipeline',
                                    command=self._start)
        self.btn_start.pack(side='left', padx=2)
        self.btn_stop = ttk.Button(row2, text='Stop',
                                   command=self._stop, state='disabled')
        self.btn_stop.pack(side='left', padx=2)

        # ── Status bar ────────────────────────────────────────────────────────
        self.status_var = tk.StringVar(value='Idle — set Output repo and click Refresh')
        ttk.Label(self, textvariable=self.status_var,
                  relief='sunken', anchor='w', padding=(6, 2)
                  ).pack(side='bottom', fill='x')
        ttk.Separator(self, orient='horizontal').pack(side='bottom', fill='x')

        # ── Notebook ──────────────────────────────────────────────────────────
        self._nb = ttk.Notebook(self)
        self._nb.pack(fill='both', expand=True, padx=6, pady=(2, 0))

        self._build_scan_tab()
        self._build_stats_tab()
        self._build_log_tab()

        self._nb.bind('<<NotebookTabChanged>>', self._on_tab_change)

    # ── Tab 0: Latest Scan ────────────────────────────────────────────────────

    def _build_scan_tab(self):
        tab = ttk.Frame(self._nb)
        self._nb.add(tab, text='Latest Scan')

        # ── Row 1: variable selector + range ─────────────────────────────────
        ctrl1 = ttk.Frame(tab, padding=(4, 3, 4, 1))
        ctrl1.pack(side='top', fill='x')

        ttk.Label(ctrl1, text='Variable:', font=('', 10)).pack(side='left')
        self._plot_var = tk.StringVar(value='reflectivity')
        var_cb = ttk.Combobox(ctrl1, textvariable=self._plot_var, width=26,
                              values=list(_VAR_DEFAULTS.keys()), state='readonly')
        var_cb.pack(side='left', padx=2)
        var_cb.bind('<<ComboboxSelected>>', lambda _: self._on_var_changed())

        ttk.Label(ctrl1, text='Min:', font=('', 10)).pack(side='left', padx=(10, 0))
        self._plot_vmin = tk.StringVar(value='10')
        ttk.Entry(ctrl1, textvariable=self._plot_vmin, width=6,
                  font=('Courier', 10)).pack(side='left', padx=2)
        ttk.Label(ctrl1, text='Max:', font=('', 10)).pack(side='left', padx=(4, 0))
        self._plot_vmax = tk.StringVar(value='65')
        ttk.Entry(ctrl1, textvariable=self._plot_vmax, width=6,
                  font=('Courier', 10)).pack(side='left', padx=2)
        ttk.Label(ctrl1,
                  text='  (change variable/range then click Show Latest or Show Loop)',
                  font=('', 9), foreground='gray').pack(side='left', padx=4)

        # ── Row 2: scan selector + loop controls + render buttons ─────────────
        ctrl2 = ttk.Frame(tab, padding=(4, 1, 4, 3))
        ctrl2.pack(side='top', fill='x')

        ttk.Label(ctrl2, text='Scan:', font=('', 10)).pack(side='left')
        self.scan_var = tk.StringVar()
        self.scan_cb  = ttk.Combobox(ctrl2, textvariable=self.scan_var,
                                     width=28, state='readonly')
        self.scan_cb.pack(side='left', padx=(2, 2))
        self.scan_cb.bind('<<ComboboxSelected>>', lambda _: self._inline_render())
        ttk.Button(ctrl2, text='◄', width=2,
                   command=self._prev_scan).pack(side='left', padx=1)
        ttk.Button(ctrl2, text='►', width=2,
                   command=self._next_scan).pack(side='left', padx=(1, 10))

        ttk.Label(ctrl2, text='N:', font=('', 10)).pack(side='left')
        self._loop_n_var = tk.IntVar(value=5)
        ttk.Spinbox(ctrl2, from_=2, to=20, textvariable=self._loop_n_var,
                    width=3, font=('Courier', 10)).pack(side='left')
        ttk.Label(ctrl2, text='dt(ms):', font=('', 10)).pack(side='left', padx=(4, 0))
        self._loop_dt_var = tk.IntVar(value=500)
        ttk.Spinbox(ctrl2, from_=100, to=5000, increment=100,
                    textvariable=self._loop_dt_var,
                    width=5, font=('Courier', 10)).pack(side='left', padx=(2, 8))

        ttk.Label(ctrl2, text='Proj steps:', font=('', 10)).pack(side='left', padx=(8, 0))
        self._max_proj_var = tk.IntVar(value=0)
        ttk.Spinbox(ctrl2, from_=0, to=20, textvariable=self._max_proj_var,
                    width=3, font=('Courier', 10)).pack(side='left', padx=(2, 4))
        ttk.Label(ctrl2, text='(0=all)', font=('', 9),
                  foreground='gray').pack(side='left', padx=(0, 8))

        ttk.Button(ctrl2, text='Show Latest',
                   command=self._show_latest).pack(side='left', padx=2)
        self.btn_loop = ttk.Button(ctrl2, text='Show Loop',
                                   command=self._toggle_nc_loop)
        self.btn_loop.pack(side='left', padx=2)
        ttk.Button(ctrl2, text='Clear',
                   command=self._clear_canvas).pack(side='left', padx=2)

        # Canvas area — toolbar + cell info embedded by _render_nc
        self.scan_container = ttk.Frame(tab)
        self.scan_container.pack(fill='both', expand=True)
        self.img_label = ttk.Label(self.scan_container)
        self.img_label.pack(fill='both', expand=True)

        # Hover stat StringVars — keys from _HV_KEYS, updated by _on_plot_hover
        self._hv = {k: tk.StringVar(value='\u2014') for k in _HV_KEYS}

    # ── Tab 1: Cell Statistics ────────────────────────────────────────────────

    def _build_stats_tab(self):
        tab = ttk.Frame(self._nb)
        self._nb.add(tab, text='Cell Statistics')

        left = ttk.Frame(tab, padding=(6, 4), width=300)
        left.pack(side='left', fill='y')
        left.pack_propagate(False)

        ttk.Label(left, text='Filter cells', font=('', 10, 'bold')).pack(anchor='w', pady=(0, 6))

        self._flt      = {}
        self._flt_sliders = {}

        filter_defs = [
            ('Area  km\u00b2', 'cell_area_sqkm',                      0,    2000,  '.0f'),
            ('Mean dBZ',        'radar_reflectivity_mean',              10,   80,    '.1f'),
            ('ZDR  mean',       'radar_differential_reflectivity_mean', -2,   8,     '.2f'),
            ('Vel  mean',       'radar_velocity_mean',                  -30,  30,    '.1f'),
        ]

        for label, key, lo, hi, fmt in filter_defs:
            lo_var = tk.DoubleVar(value=lo)
            hi_var = tk.DoubleVar(value=hi)

            grp = ttk.Frame(left)
            grp.pack(fill='x', pady=4)

            hdr = ttk.Frame(grp)
            hdr.pack(fill='x')
            ttk.Label(hdr, text=label, width=12, anchor='w').pack(side='left')
            lo_lbl = ttk.Label(hdr, width=7, anchor='e', foreground='#555')
            lo_lbl.pack(side='left')
            ttk.Label(hdr, text='\u2013').pack(side='left')
            hi_lbl = ttk.Label(hdr, width=7, anchor='w', foreground='#555')
            hi_lbl.pack(side='left')

            def _update(*_, lv=lo_var, hv=hi_var, ll=lo_lbl, hl=hi_lbl, f=fmt):
                ll.config(text=f'{lv.get():{f}}')
                hl.config(text=f'{hv.get():{f}}')
            lo_var.trace_add('write', _update)
            hi_var.trace_add('write', _update)
            _update()

            slider = _RangeSlider(grp, lo, hi, lo_var, hi_var, fmt=fmt)
            slider.pack(fill='x', padx=2)

            self._flt[key]         = (lo_var, hi_var)
            self._flt_sliders[key] = slider

        ttk.Button(left, text='Apply filters',
                   command=self._refresh_table).pack(fill='x', pady=(10, 2))

        right = ttk.Frame(tab, padding=(4, 4))
        right.pack(side='left', fill='both', expand=True)

        self.stats_lbl = ttk.Label(right, text='')
        self.stats_lbl.pack(anchor='w', pady=(0, 4))

        tv_frame = ttk.Frame(right)
        tv_frame.pack(fill='both', expand=True)

        self._tv_cols = [
            'time_label', 'cell_label', 'cell_area_sqkm',
            'radar_reflectivity_max', 'radar_reflectivity_mean',
            'radar_differential_reflectivity_mean',
            'radar_velocity_mean',
            'cell_centroid_mass_lat', 'cell_centroid_mass_lon',
        ]
        self.tv = ttk.Treeview(tv_frame, columns=self._tv_cols,
                               show='headings', height=24)
        widths = [70, 60, 75, 80, 80, 85, 75, 90, 90]
        for c, w in zip(self._tv_cols, widths):
            hdr = (c.replace('radar_differential_reflectivity_mean', 'ZDR mean')
                    .replace('radar_', '').replace('cell_', '')
                    .replace('_', ' '))
            self.tv.heading(c, text=hdr)
            self.tv.column(c, width=w, anchor='center')

        vsb = ttk.Scrollbar(tv_frame, orient='vertical',   command=self.tv.yview)
        hsb = ttk.Scrollbar(tv_frame, orient='horizontal', command=self.tv.xview)
        self.tv.configure(yscrollcommand=vsb.set, xscrollcommand=hsb.set)
        self.tv.grid(row=0, column=0, sticky='nsew')
        vsb.grid(row=0, column=1, sticky='ns')
        hsb.grid(row=1, column=0, sticky='ew')
        tv_frame.rowconfigure(0, weight=1)
        tv_frame.columnconfigure(0, weight=1)

    # ── Tab 2: Pipeline Log ───────────────────────────────────────────────────

    def _build_log_tab(self):
        tab = ttk.Frame(self._nb)
        self._nb.add(tab, text='Log')

        ctrl = ttk.Frame(tab, padding=4)
        ctrl.pack(side='top', fill='x')
        ttk.Button(ctrl, text='Refresh', command=self._flush_log).pack(side='left')
        ttk.Button(ctrl, text='Clear',   command=self._clear_log).pack(side='left', padx=4)

        self.log_text = scrolledtext.ScrolledText(
            tab, state='disabled', wrap='none',
            font=('Courier', 11), background='#1e1e1e', foreground='#d4d4d4')
        self.log_text.pack(fill='both', expand=True)
        self.log_text.tag_config('error',   foreground='#f44747')
        self.log_text.tag_config('warning', foreground='#dcdcaa')
        self.log_text.tag_config('info',    foreground='#9cdcfe')

    # ── Browse / selection ────────────────────────────────────────────────────

    def _browse_repo(self):
        path = filedialog.askdirectory(title='Select Adapt output repository', parent=self)
        if path:
            self._repo_root.set(path)
            self._on_repo_changed()

    def _on_repo_changed(self):
        repo = Path(self._repo_root.get().strip())
        radars = _list_radars(repo)
        self.radar_cb['values'] = radars
        if radars:
            self._radar.set(radars[0])
        else:
            self._radar.set('')
        self._on_radar_changed()

    def _on_radar_changed(self):
        repo = Path(self._repo_root.get().strip())
        runs = _list_runs(repo)
        self.run_cb['values'] = runs
        if runs:
            self._run_sel.set(runs[0])
        else:
            self._run_sel.set('')
        self._today = datetime.now().strftime('%Y%m%d')
        self._last_n_plots = -1
        self._refresh_all()

    # ── Pipeline control ──────────────────────────────────────────────────────

    def _start(self):
        radar = self._radar.get().strip().upper()
        repo  = self._repo_root.get().strip()
        if not radar:
            messagebox.showerror('Missing input', 'Select a Radar ID first', parent=self)
            return
        if not repo:
            messagebox.showerror('Missing input', 'Set the Output repo path first', parent=self)
            return
        if _pipeline_running():
            pid = _PID_FILE.read_text().strip()
            messagebox.showerror(
                'Already running',
                f'A pipeline is already running (PID {pid}).\n'
                f'Stop it first or delete {_PID_FILE}.', parent=self)
            return

        self._radar.set(radar)
        self._today        = datetime.now().strftime('%Y%m%d')
        self._last_n_plots = -1
        self._log_lines    = []

        cmd = [*_find_adapt_exe(), 'run-nexrad',
               '--radar', radar, '--base-dir', repo, '--mode', 'realtime']
        try:
            self._proc = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                text=True,
                start_new_session=True,
            )
        except Exception as e:
            messagebox.showerror('Launch failed', str(e), parent=self)
            return

        self.btn_start.config(state='disabled')
        self.btn_stop.config(state='normal')
        self.status_var.set(f'Running  |  {radar}  ->  {repo}')

        def _read():
            for line in self._proc.stdout:
                self._log_lines.append(line.rstrip())
                if len(self._log_lines) > LOG_MAX:
                    self._log_lines.pop(0)
            self.after(0, self._on_proc_ended)

        threading.Thread(target=_read, daemon=True).start()
        self._append_log(f'[{datetime.now():%H:%M:%S}] Pipeline started: {radar}', 'info')
        self._append_log(f'  Output: {repo}/{radar}', 'info')

    def _stop(self):
        if not (self._proc and self._proc.poll() is None):
            self._on_proc_ended()
            return
        self.status_var.set('Stopping pipeline...')
        self.btn_stop.config(state='disabled')
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
            self.after(0, self._on_proc_ended)

        threading.Thread(target=_do_kill, daemon=True).start()

    def _on_proc_ended(self):
        self.btn_start.config(state='normal')
        self.btn_stop.config(state='disabled')
        self.status_var.set(f'Stopped  |  {self._radar.get()}')

    def _on_close(self):
        if self._proc and self._proc.poll() is None:
            try:
                os.killpg(os.getpgid(self._proc.pid), 15)
            except OSError:
                self._proc.terminate()
            try:
                self._proc.wait(timeout=3)
            except subprocess.TimeoutExpired:
                try:
                    os.killpg(os.getpgid(self._proc.pid), 9)
                except OSError:
                    self._proc.kill()
        self.destroy()

    # ── Auto-refresh ──────────────────────────────────────────────────────────

    def _schedule_refresh(self):
        self._refresh_all()
        self.after(POLL_MS, self._schedule_refresh)

    def _status_tick(self):
        """Update status bar every second: scan time + countdown to next check."""
        if not self._refresh_active:
            return
        secs = max(0, int(self._next_refresh_at - time.time()))
        scan_str = (self._last_scan_dt.strftime('%H:%M:%S UTC')
                    if self._last_scan_dt else '—')
        self.status_var.set(
            f'{self._status_base}  |  Last scan: {scan_str}  |  Next check: {secs}s')
        self.after(1000, self._status_tick)

    def _refresh_all(self):
        repo  = self._repo_root.get().strip()
        radar = self._radar.get().strip().upper()
        if not repo or not radar:
            return

        all_nc   = self._get_nc_files(repo, radar)
        nc_files = all_nc[-5:]   # last 5 for dropdown
        labels   = [self._nc_label(p) for p in nc_files]

        cur = self.scan_var.get()
        self.scan_cb['values'] = labels
        if labels and cur not in labels:
            self.scan_var.set(labels[-1])

        if len(all_nc) > self._last_n_plots and all_nc:
            self._last_n_plots = len(all_nc)

        running = _pipeline_running() or (self._proc and self._proc.poll() is None)
        state = 'Running' if running else ('Idle' if not all_nc else 'Done')
        self._status_base     = f'{state}  |  Radar: {radar}  |  Scans: {len(all_nc)}'
        self._next_refresh_at = time.time() + POLL_MS / 1000

        # ── Auto-update live canvas when a new NC file appears ────────────────
        if HAS_DATA and not self._nc_loop_running and all_nc:
            latest = all_nc[-1]
            if self._last_rendered_nc is not None and self._last_rendered_nc != latest:
                # New file appeared — update existing canvas in place or re-open
                if self._canvas_refs is not None:
                    canvas, fig, _tb, _bot = self._canvas_refs
                    try:
                        self._load_parquet(repo, radar)
                        self._draw_scan(xr.open_dataset(latest), fig)
                        canvas.draw_idle()
                        self._last_rendered_nc = latest
                        self.scan_var.set(labels[-1] if labels else '')
                    except Exception:
                        pass
                else:
                    # Canvas was cleared externally; re-render
                    try:
                        self._load_parquet(repo, radar)
                        self._render_nc(latest)
                        self._last_rendered_nc = latest
                        self.scan_var.set(labels[-1] if labels else '')
                    except Exception:
                        pass

        self._refresh_table()
        if self._nb.index('current') == 2:
            self._flush_log()

    # ── NC file helpers ───────────────────────────────────────────────────────

    def _get_nc_files(self, repo, radar):
        d = Path(repo) / radar / 'analysis' / self._today
        return sorted(d.glob('*_analysis.nc')) if d.exists() else []

    @staticmethod
    def _nc_label(p):
        parts = p.stem.split('_')
        # filename: RADAR_YYYYMMDD_HHMMSS_analysis  or similar
        t = next((x for x in parts if len(x) == 6 and x.isdigit()), None)
        if t:
            return f'{t[:2]}:{t[2:4]}:{t[4:6]} UTC  ({p.stem})'
        return p.stem

    def _on_var_changed(self):
        """Update vmin/vmax defaults when variable selector changes."""
        var = self._plot_var.get()
        if var in _VAR_DEFAULTS:
            vmin, vmax, _, _ = _VAR_DEFAULTS[var]
            self._plot_vmin.set(str(vmin))
            self._plot_vmax.set(str(vmax))

    def _prev_scan(self):
        vals = list(self.scan_cb['values'])
        if not vals:
            return
        cur = self.scan_var.get()
        idx = vals.index(cur) if cur in vals else len(vals)
        if idx > 0:
            self.scan_var.set(vals[idx - 1])
            self._inline_render()

    def _next_scan(self):
        vals = list(self.scan_cb['values'])
        if not vals:
            return
        cur = self.scan_var.get()
        idx = vals.index(cur) if cur in vals else -1
        if idx < len(vals) - 1:
            self.scan_var.set(vals[idx + 1])
            self._inline_render()

    # ── Show latest scan (single frame, auto-live) ────────────────────────────

    def _show_latest(self):
        """Render the most recent NC file and enable live auto-refresh."""
        repo  = self._repo_root.get().strip()
        radar = self._radar.get().strip().upper()
        if not repo or not radar:
            return
        nc_files = self._get_nc_files(repo, radar)
        if not nc_files:
            messagebox.showinfo('No data',
                                f'No *_analysis.nc for today in:\n'
                                f'{Path(repo) / radar / "analysis" / self._today}',
                                parent=self)
            return
        self._load_parquet(repo, radar)
        self._clear_canvas()
        self._render_nc(nc_files[-1])
        self._last_rendered_nc = nc_files[-1]
        # Sync scan selector
        labels = [self._nc_label(p) for p in nc_files[-5:]]
        self.scan_cb['values'] = labels
        self.scan_var.set(labels[-1])

    # ── Live render (single frame) ────────────────────────────────────────────

    def _inline_render(self):
        if not HAS_MPL or not HAS_DATA:
            messagebox.showerror('Missing dependencies',
                                 'matplotlib, numpy, pandas, xarray required.',
                                 parent=self)
            return
        repo  = self._repo_root.get().strip()
        radar = self._radar.get().strip().upper()
        if not repo or not radar:
            messagebox.showerror('Missing input',
                                 'Set Radar ID and Repo path first.', parent=self)
            return

        nc_files = self._get_nc_files(repo, radar)
        if not nc_files:
            messagebox.showinfo('Not found',
                                f'No *_analysis.nc for today in:\n'
                                f'{Path(repo) / radar / "analysis" / self._today}',
                                parent=self)
            return

        # Match selected label to NC file
        sel  = self.scan_var.get()
        stem = sel.split('(')[-1].rstrip(')') if '(' in sel else ''
        nc_path = next((p for p in nc_files if p.stem == stem), nc_files[-1])

        self._load_parquet(repo, radar)
        self._clear_canvas()
        self._render_nc(nc_path)

    def _load_parquet(self, repo, radar):
        """Load cell stats parquet (most recent) into self._current_cell_df."""
        self._current_cell_df = None
        pqs = sorted((Path(repo) / radar / 'analysis').glob('analysis2d_*.parquet'))
        if pqs:
            try:
                self._current_cell_df = pd.read_parquet(pqs[-1])
            except Exception:
                pass

    # ── NC loop render (cycle through N frames) ───────────────────────────────

    def _toggle_nc_loop(self):
        if self._nc_loop_running:
            self._nc_loop_running = False
            self.btn_loop.config(text='Show Loop')
            return
        repo  = self._repo_root.get().strip()
        radar = self._radar.get().strip().upper()
        if not repo or not radar:
            return
        n = max(2, self._loop_n_var.get())
        nc_files = self._get_nc_files(repo, radar)[-n:]
        if not nc_files:
            messagebox.showinfo('No data',
                                'No analysis NC files found for today.', parent=self)
            return
        self._load_parquet(repo, radar)
        self._nc_loop_files   = nc_files
        self._nc_loop_index   = 0
        self.btn_loop.config(text='Stop Loop')
        self._clear_canvas()
        self._nc_loop_running = True   # set AFTER clear so _clear_canvas doesn't kill it
        self._render_nc(nc_files[0])
        self._nc_loop_index = 1
        dt = max(100, self._loop_dt_var.get())
        self.after(dt, self._nc_loop_step)

    def _nc_loop_step(self):
        if not self._nc_loop_running or not self._nc_loop_files:
            return
        path = self._nc_loop_files[self._nc_loop_index % len(self._nc_loop_files)]
        self._nc_loop_index += 1
        if self._canvas_refs is not None:
            canvas, fig, toolbar, bottom = self._canvas_refs
            self._draw_scan(xr.open_dataset(path), fig)
            canvas.draw_idle()
        else:
            self._render_nc(path)
        dt = max(100, self._loop_dt_var.get())
        self.after(dt, self._nc_loop_step)

    # ── Core matplotlib rendering ─────────────────────────────────────────────

    def _render_nc(self, nc_path):
        """Create canvas + bottom strip, then render nc_path into a new figure."""
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 6.5), dpi=90)

        # Extract lat0/lon0 for toolbar coordinate transform
        ds_tmp = xr.open_dataset(nc_path)
        lat0 = float(ds_tmp.attrs.get('radar_latitude',
                                      ds_tmp.attrs.get('origin_latitude', 0)) or 0)
        lon0 = float(ds_tmp.attrs.get('radar_longitude',
                                      ds_tmp.attrs.get('origin_longitude', 0)) or 0)
        ds_tmp.close()

        self._draw_scan(xr.open_dataset(nc_path), fig)

        self.img_label.pack_forget()

        # Bottom strip: toolbar (left) + stat boxes (right)
        bottom = tk.Frame(self.scan_container, bg=_STRIP_BG)
        bottom.pack(side='bottom', fill='x')

        canvas = FigureCanvasTkAgg(fig, master=self.scan_container)
        canvas.get_tk_widget().pack(fill='both', expand=True)
        canvas.draw()

        toolbar = _CompactToolbar(canvas, bottom, pack_toolbar=False,
                                  lat0=lat0, lon0=lon0)
        toolbar.update()
        toolbar.pack(side='left')

        # Dark stat boxes
        for var in self._hv.values():
            var.set('\u2014')
        stat_frame = tk.Frame(bottom, bg=_STRIP_BG)
        stat_frame.pack(side='right', fill='y', padx=4, pady=2)
        for lbl1, key1, fg1, lbl2, key2, fg2 in _BOX_DEFS:
            box = tk.Frame(stat_frame, bg=_BOX_BG, padx=4, pady=2,
                           relief='groove', bd=1)
            box.pack(side='left', fill='y', padx=2, pady=1)
            for lbl, key, fg in ((lbl1, key1, fg1), (lbl2, key2, fg2)):
                row = tk.Frame(box, bg=_BOX_BG)
                row.pack(fill='x')
                tk.Label(row, text=lbl + ':', font=_FONT_LBL,
                         fg='#888888', bg=_BOX_BG).pack(side='left')
                tk.Label(row, textvariable=self._hv[key], font=_FONT_VAL,
                         fg=fg, bg=_BOX_BG, anchor='w',
                         width=10).pack(side='left')

        self._canvas_refs = (canvas, fig, toolbar, bottom)
        self._hover_canvas = canvas
        canvas.mpl_connect('motion_notify_event', self._on_plot_hover)

    def _draw_scan(self, ds, fig):
        """Render dataset into fig (clears figure first). Keeps ds open."""
        fig.clear()
        ax1, ax2 = fig.subplots(1, 2)
        ax1.set_facecolor('#cccccc')
        ax2.set_facecolor('#cccccc')

        # Close previous dataset
        if self._current_nc_ds is not None and self._current_nc_ds is not ds:
            try:
                self._current_nc_ds.close()
            except Exception:
                pass
        self._current_nc_ds = ds
        self._cell_contours = {}
        for var in self._hv.values():
            var.set('\u2014')

        radar_id = ds.attrs.get('radar', ds.attrs.get('radar_id', ''))
        tv  = ds.coords['time'].values if 'time' in ds.coords else None
        ts  = pd.Timestamp(
            tv.item() if tv is not None and np.ndim(tv) == 0
            else tv[0]  if tv is not None
            else pd.Timestamp.now())
        tstr = ts.strftime('%Y-%m-%d %H:%M:%S UTC')
        self._last_scan_dt = ts.to_pydatetime()

        x_km = ds['x'].values / 1000.0
        y_km = ds['y'].values / 1000.0
        y_grid, x_grid = np.meshgrid(y_km, x_km, indexing='ij')
        labels_data = ds['cell_labels'].values

        # ── Left panel: user-selected variable ───────────────────────────────
        var_name = (self._plot_var.get()
                    if self._plot_var is not None else 'reflectivity')
        if var_name not in ds.data_vars:
            var_name = 'reflectivity'
        vdef     = _VAR_DEFAULTS.get(var_name, (10, 65, 'dBZ', 'viridis'))
        try:
            vmin = float(self._plot_vmin.get() if self._plot_vmin else vdef[0])
        except (ValueError, AttributeError):
            vmin = vdef[0]
        try:
            vmax = float(self._plot_vmax.get() if self._plot_vmax else vdef[1])
        except (ValueError, AttributeError):
            vmax = vdef[1]
        unit     = vdef[2]
        cmap_str = vdef[3]
        var_lbl  = _VAR_LABELS.get(var_name, var_name)

        raw = ds[var_name].values.astype(float)
        masked = np.ma.masked_where(np.isnan(raw) | (raw < vmin) | (raw > vmax), raw)
        cmap1 = copy.copy(plt.get_cmap(cmap_str))
        cmap1.set_bad(alpha=0)

        im1 = ax1.pcolormesh(x_km, y_km, masked,
                             cmap=cmap1, vmin=vmin, vmax=vmax,
                             shading='auto', zorder=2)
        plt.colorbar(im1, ax=ax1, label=unit, fraction=0.046, pad=0.04)
        self._add_basemap(ax1, ds, x_km, y_km)

        # Motion vectors (if available)
        if 'heading_x' in ds.data_vars and 'heading_y' in ds.data_vars:
            hx, hy = ds['heading_x'].values, ds['heading_y'].values
            if not np.all(np.isnan(hx)):
                s  = 10
                yi_idx = np.arange(0, len(y_km), s)
                xi_idx = np.arange(0, len(x_km), s)
                Xs, Ys = np.meshgrid(x_km[xi_idx], y_km[yi_idx])
                ax1.quiver(Xs, Ys,
                           hx[np.ix_(yi_idx, xi_idx)],
                           hy[np.ix_(yi_idx, xi_idx)],
                           color='#222', alpha=0.7, scale=1.0, scale_units='xy',
                           width=0.002, headwidth=3, zorder=45)

        ax1.set_xlabel('X (km)'); ax1.set_ylabel('Y (km)')
        ax1.grid(True, alpha=0.3, zorder=3)
        ax1.set_title(f'{radar_id}  {var_lbl} + Motion\n{tstr}',
                      fontsize=11, fontweight='bold')

        # ── Right panel: cells + ALL projections ──────────────────────────────
        refl = ds['reflectivity'].values.astype(float)
        cmap2 = copy.copy(plt.get_cmap(REFL_CMAP))
        cmap2.set_bad(alpha=0)
        refl_seg = np.ma.masked_where(~(labels_data > 0) | (refl < 0), refl)
        im2 = ax2.pcolormesh(x_km, y_km, refl_seg,
                             cmap=cmap2, vmin=10, vmax=65,
                             shading='auto', zorder=2)
        plt.colorbar(im2, ax=ax2, label='dBZ', fraction=0.046, pad=0.04)
        self._add_basemap(ax2, ds, x_km, y_km)

        for cell_id in np.unique(labels_data[labels_data > 0]):
            cs = ax2.contour(x_grid, y_grid,
                             (labels_data == cell_id).astype(float),
                             levels=[0.5], colors='black', linewidths=1.5, zorder=50)
            self._cell_contours[int(cell_id)] = cs

        if 'cell_projections' in ds.data_vars:
            proj_da = ds['cell_projections']
            fo      = 'frame_offset'
            if fo in proj_da.dims:
                n_frames   = len(proj_da[fo])
                max_proj   = self._max_proj_var.get()
                end_frame  = n_frames if max_proj == 0 else min(n_frames, max_proj + 1)
                _ls_cycle  = ['dashed', 'dashdot', 'dotted']
                for i in range(1, end_frame):
                    alpha = max(0.3, 1.0 - i / n_frames)
                    lw    = max(0.8, 1.8 - i * 0.2)
                    ls    = _ls_cycle[(i - 1) % len(_ls_cycle)]
                    lp = proj_da.isel({fo: i}).values
                    for cid in np.unique(lp[~np.isnan(lp) & (lp > 0)]):
                        ax2.contour(x_grid, y_grid, (lp == cid).astype(float),
                                    levels=[0.5], colors='#555555',
                                    linewidths=lw, linestyles=ls,
                                    alpha=alpha, zorder=40)

        ax2.set_xlabel('X (km)'); ax2.set_ylabel('Y (km)')
        ax2.grid(True, alpha=0.3, zorder=3)
        ax2.set_title(f'{radar_id}  Cells + All Projections\n{tstr}',
                      fontsize=11, fontweight='bold')

        fig.tight_layout()

    @staticmethod
    def _add_basemap(ax, ds, x_km, y_km):
        if not HAS_CTX:
            return
        lat = ds.attrs.get('radar_latitude', ds.attrs.get('origin_latitude'))
        lon = ds.attrs.get('radar_longitude', ds.attrs.get('origin_longitude'))
        if lat is None or lon is None:
            return
        lat, lon = float(lat), float(lon)
        crs_str = (f'+proj=aeqd +lat_0={lat} +lon_0={lon} '
                   f'+x_0=0 +y_0=0 +datum=WGS84 +units=km')
        ax.set_xlim(x_km.min(), x_km.max())
        ax.set_ylim(y_km.min(), y_km.max())
        try:
            ctx.add_basemap(ax, crs=crs_str,
                            source=ctx.providers.OpenStreetMap.Mapnik,
                            alpha=0.5, attribution=False, zoom='auto')
        except Exception as e:
            print(f'Basemap error: {e}')

    def _clear_canvas(self):
        self._nc_loop_running = False
        self._last_rendered_nc = None
        if hasattr(self, 'btn_loop'):
            self.btn_loop.config(text='Show Loop')

        if self._canvas_refs:
            canvas, fig, toolbar, bottom = self._canvas_refs
            plt.close(fig)
            toolbar.destroy()
            canvas.get_tk_widget().destroy()
            bottom.destroy()
            self._canvas_refs = None
            self._hover_canvas = None
        if self._current_nc_ds is not None:
            try:
                self._current_nc_ds.close()
            except Exception:
                pass
            self._current_nc_ds = None
        self._cell_contours = {}
        for var in self._hv.values():
            var.set('\u2014')
        self.img_label.config(image='', text='')
        self.img_label.pack(fill='both', expand=True)

    # ── Hover interaction ─────────────────────────────────────────────────────

    def _on_plot_hover(self, event):
        if not HAS_DATA or self._current_nc_ds is None:
            return

        _em = '\u2014'
        ds  = self._current_nc_ds

        if event.inaxes is None or event.xdata is None:
            for var in self._hv.values():
                var.set(_em)
            return

        x_m = event.xdata * 1000.0
        y_m = event.ydata * 1000.0

        try:
            # ── Cell under cursor ─────────────────────────────────────────────
            x_vals = ds['x'].values
            y_vals = ds['y'].values
            xi = int(np.argmin(np.abs(x_vals - x_m)))
            yi = int(np.argmin(np.abs(y_vals - y_m)))
            cell_id = int(ds['cell_labels'].values[yi, xi])

            if cell_id <= 0:
                for k in _HV_KEYS:
                    self._hv[k].set(_em)
                return

            self._hv['cell_id'].set(str(cell_id))

            # ── Cell stats from parquet ───────────────────────────────────────
            df = self._current_cell_df
            if df is not None and 'cell_label' in df.columns:
                rows = df[df['cell_label'] == cell_id]
                if not rows.empty:
                    r = rows.iloc[0]

                    def _f(key, fmt='.1f', suffix=''):
                        if key in r and r[key] == r[key]:
                            return f'{r[key]:{fmt}}{suffix}'
                        return _em

                    self._hv['area'].set(_f('cell_area_sqkm'))
                    self._hv['lat_mass'].set(
                        _f('cell_centroid_mass_lat', '.4f', '\u00b0'))
                    self._hv['lon_mass'].set(
                        _f('cell_centroid_mass_lon', '.4f', '\u00b0'))
                    self._hv['dbz_mean'].set(_f('radar_reflectivity_mean'))
                    self._hv['dbz_max'].set(_f('radar_reflectivity_max'))
                    self._hv['zdr_mean'].set(
                        _f('radar_differential_reflectivity_mean', '.2f'))
                    self._hv['zdr_max'].set(
                        _f('radar_differential_reflectivity_max', '.2f'))
                    self._hv['vel_mean'].set(_f('radar_velocity_mean'))
                    self._hv['sw_mean'].set(_f('radar_spectrum_width_mean'))
                    return

            for k in ('area', 'lat_mass', 'lon_mass',
                      'dbz_mean', 'dbz_max', 'zdr_mean', 'zdr_max',
                      'vel_mean', 'sw_mean'):
                self._hv[k].set(_em)

        except Exception:
            pass

    # ── Cell statistics ───────────────────────────────────────────────────────

    def _refresh_table(self):
        if not HAS_DATA:
            return
        repo  = self._repo_root.get().strip()
        radar = self._radar.get().strip().upper()
        if not repo or not radar:
            return

        pqs = sorted((Path(repo) / radar / 'analysis').glob('analysis2d_*.parquet'))
        if not pqs:
            self.stats_lbl.config(text='No parquet data yet.')
            return

        try:
            df = pd.read_parquet(pqs[-1])
            df['scan_time']  = pd.to_datetime(df['scan_time'])
            df['time_label'] = df['scan_time'].dt.strftime('%H:%M:%S')
        except Exception as e:
            self.stats_lbl.config(text=f'Error reading parquet: {e}')
            return

        for col, (lo_v, hi_v) in self._flt.items():
            if col not in df.columns:
                continue
            if lo_v.get() < float(df[col].min()):
                lo_v.set(float(df[col].min()))
            if hi_v.get() > float(df[col].max()):
                hi_v.set(float(df[col].max()))

        mask = pd.Series(True, index=df.index)
        for col, (lo_v, hi_v) in self._flt.items():
            if col in df.columns:
                try:
                    mask &= df[col].between(float(lo_v.get()), float(hi_v.get()))
                except Exception:
                    pass

        filt = df[mask]

        def _avg(col, fmt='.1f'):
            return (f'{filt[col].mean():{fmt}}'
                    if col in filt.columns and not filt.empty else '\u2014')

        self.stats_lbl.config(
            text=(f'Showing {len(filt)} / {len(df)} cells'
                  f'  |  Avg dBZ: {_avg("radar_reflectivity_mean")}'
                  f'  |  Avg area: {_avg("cell_area_sqkm")} km\u00b2'
                  f'  |  Avg ZDR: {_avg("radar_differential_reflectivity_mean", ".2f")}'))

        self.tv.delete(*self.tv.get_children())
        show = [c for c in self._tv_cols if c in filt.columns]
        for _, row in filt[show].iterrows():
            vals = []
            for c in self._tv_cols:
                v = row.get(c, '')
                vals.append(f'{v:.2f}' if isinstance(v, float) else str(v))
            self.tv.insert('', 'end', values=vals)

    # ── Log ───────────────────────────────────────────────────────────────────

    def _append_log(self, line, tag=''):
        self.log_text.config(state='normal')
        self.log_text.insert('end', line + '\n', tag)
        self.log_text.see('end')
        self.log_text.config(state='disabled')

    def _flush_log(self):
        self.log_text.config(state='normal')
        self.log_text.delete('1.0', 'end')
        for ln in self._log_lines[-200:]:
            tag = 'error' if 'ERROR' in ln else ('warning' if 'WARNING' in ln else '')
            self.log_text.insert('end', ln + '\n', tag)
        self.log_text.see('end')
        self.log_text.config(state='disabled')

    def _clear_log(self):
        self._log_lines.clear()
        self.log_text.config(state='normal')
        self.log_text.delete('1.0', 'end')
        self.log_text.config(state='disabled')

    def _on_tab_change(self, _):
        idx = self._nb.index('current')
        if idx == 2:
            self._flush_log()


# ── Entry point ───────────────────────────────────────────────────────────────

def main():
    app = AdaptDashboard()
    app.mainloop()


if __name__ == '__main__':
    main()
