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
import os
import shutil
import subprocess
import sys
import threading
import time
from datetime import datetime
from pathlib import Path


@contextlib.contextmanager
def _suppress_osx_stderr():
    """Redirect fd 2 to /dev/null for the duration of the block.

    macOS ObjC runtime prints NSOpenPanel/NSWindow warnings directly to
    file-descriptor 2, bypassing Python's sys.stderr.  Only an OS-level
    dup2 can suppress them.
    """
    devnull = os.open(os.devnull, os.O_WRONLY)
    saved   = os.dup(2)
    os.dup2(devnull, 2)
    os.close(devnull)
    try:
        yield
    finally:
        os.dup2(saved, 2)
        os.close(saved)

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
import tkinter as tk  # noqa: E402
from tkinter import filedialog, messagebox, scrolledtext, ttk  # noqa: E402

# ── Optional deps ─────────────────────────────────────────────────────────────
try:
    import PIL  # noqa: F401
    HAS_PIL = True
except ImportError:
    HAS_PIL = False

try:
    import matplotlib
    matplotlib.use('TkAgg')
    import cmweather  # noqa: F401 — registers ChaseSpectral and other radar colormaps — must follow use()
    import matplotlib.dates as mdates
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
    ('Cell',    'cell_uid', '#ffffff', 'Area km²', 'area',     '#ffff44'),
    ('Lat(C)',   'lat_mass', '#44ff88', 'Lon(C)',   'lon_mass', '#44ff88'),
    ('dBZ mean', 'dbz_mean', '#ff8800', 'dBZ max',  'dbz_max',  '#ffcc44'),
    ('ZDR mean', 'zdr_mean', '#ff44ff', 'ZDR max',  'zdr_max',  '#ff88ff'),
    ('Age',      'age',      '#aaffaa', 'Vel mean', 'vel_mean', '#44ffff'),
]
_HV_KEYS = ('cell_uid', 'area',
            'lat_mass', 'lon_mass',
            'dbz_mean', 'dbz_max', 'zdr_mean', 'zdr_max',
            'age', 'vel_mean')

def _cell_uid_disp(uid) -> str:
    try:
        import pandas as _pd
        if _pd.isna(uid):
            return '\u2014'
    except Exception:
        pass
    if uid is None:
        return '\u2014'
    return str(uid)[:4]

# ── Variable selector defaults: (vmin, vmax, unit, cmap) ─────────────────────
_VAR_DEFAULTS = {
    'reflectivity':              (10,  60,  'dBZ', 'ChaseSpectral'),
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
                with contextlib.suppress(Exception):
                    self._ltrans = Transformer.from_crs(
                        f'+proj=aeqd +lat_0={lat0} +lon_0={lon0} +units=m',
                        'EPSG:4326', always_xy=True)
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
    """Return radars from database registry.

    Uses DataClient API to query adapt_registry.db for registered radars.
    Falls back to filesystem scan if database not available.
    """
    if not repo.exists():
        return []

    # Try database-based discovery via DataClient
    try:
        from adapt.api.client import DataClient
        client = DataClient(repo)
        radars = client.list_radars()
        if radars:
            return sorted(radars)
    except Exception:
        pass

    # Fallback: filesystem scan for NEXRAD-style directories
    return sorted(
        d.name for d in repo.iterdir()
        if d.is_dir() and len(d.name) == 4 and d.name.isupper()
        and (d / 'nexrad').exists()
    )


def _list_runs(repo: Path, radar: str = None) -> list:
    """Return runs from database registry.

    Uses DataClient API to query adapt_registry.db for runs.
    Falls back to runtime_config_*.json scan if database not available.

    Parameters
    ----------
    repo : Path
        Repository root path
    radar : str, optional
        Filter runs by radar ID

    Returns
    -------
    list
        List of formatted run strings: "run_id  (MM-DD HH:MM)"
    """
    if not repo.exists():
        return []

    # Try database-based discovery via DataClient
    try:
        from adapt.api.client import DataClient
        client = DataClient(repo)
        runs_df = client.list_runs(radar=radar)
        if not runs_df.empty:
            runs = []
            for _, row in runs_df.iterrows():
                run_id = row['run_id']
                start_time = row.get('start_time', '')
                # Parse ISO timestamp and format
                try:
                    dt = datetime.fromisoformat(start_time.replace('Z', '+00:00'))
                    mtime = dt.strftime('%m-%d %H:%M')
                except (ValueError, AttributeError):
                    mtime = str(start_time)[:16] if start_time else '?'
                runs.append(f'{run_id}  ({mtime})')
            return runs
    except Exception:
        pass

    # Fallback: filesystem scan for runtime_config_*.json
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
        self._current_cell_df = None   # cells_by_scan DataFrame (SQLite) or parquet fallback
        self._current_run_id  = None   # run_id for the loaded cell data
        self._current_scan_ts = None   # pd.Timestamp of current displayed scan
        self._cell_contours   = {}     # cell_id -> contour set on radar ax
        self._hover_canvas    = None   # ref to mpl canvas for hover

        # Track click overlay state
        self._selected_cell_uid: str | None = None
        self._track_overlay: list | None = None    # matplotlib artists for tracking overlay
        self._ts_axes: tuple | None = None         # (ax_area, ax_dbz, ax_reserved)
        self._show_flow_var: tk.BooleanVar | None = None  # set in _build_scan_tab
        self._colorbar: object | None = None               # active colorbar reference
        self._cbar_ax: object | None = None               # pre-allocated colorbar axes
        self._bg_alpha_var: tk.DoubleVar | None = None    # grayscale background alpha

        # NC loop animation state (replaces PNG loop)
        self._nc_loop_running = False
        self._nc_loop_index   = 0
        self._nc_loop_files   = []

        # Pending after() IDs — cancelled on close to prevent post-destroy callbacks
        self._after_ids: list[str] = []

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
        self.bind('<Escape>', self._on_escape)

        # Start auto-refresh and status countdown ticker
        self._after_ids.append(self.after(500, self._schedule_refresh))
        self._after_ids.append(self.after(1000, self._status_tick))

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
        self._plot_vmax = tk.StringVar(value='60')
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

        ttk.Separator(ctrl2, orient='vertical').pack(side='left', fill='y', padx=8)
        self._show_flow_var = tk.BooleanVar(value=False)
        ttk.Checkbutton(ctrl2, text='Show flow',
                        variable=self._show_flow_var).pack(side='left', padx=2)

        ttk.Label(ctrl2, text='BG α:').pack(side='left', padx=(6, 0))
        self._bg_alpha_var = tk.DoubleVar(value=0.85)
        ttk.Spinbox(ctrl2, from_=0.0, to=1.0, increment=0.05,
                    format='%.2f', textvariable=self._bg_alpha_var,
                    width=5).pack(side='left', padx=2)

        ttk.Separator(ctrl2, orient='vertical').pack(side='left', fill='y', padx=6)
        ttk.Button(ctrl2, text='Update',
                   command=self._redraw).pack(side='left', padx=2)

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

        # Cell UID prefix search
        pid_row = ttk.Frame(left)
        pid_row.pack(fill='x', pady=(0, 8))
        ttk.Label(pid_row, text='Cell UID prefix:', width=14, anchor='w').pack(side='left')
        self._cell_uid_filter = tk.StringVar()
        ttk.Entry(pid_row, textvariable=self._cell_uid_filter, width=12).pack(side='left', padx=2)
        self._cell_uid_filter.trace_add('write', lambda *_: self._refresh_table())

        self._flt         = {}
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
        for c, w in zip(self._tv_cols, widths, strict=False):
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
        with _suppress_osx_stderr():
            path = filedialog.askdirectory(title='Select Adapt output repository', parent=self)
        if path:
            self._repo_root.set(path)
            self._on_repo_changed()

    def _on_repo_changed(self):
        repo = Path(self._repo_root.get().strip())
        radars = _list_radars(repo)
        self.radar_cb['values'] = radars

        # Select radar with most recent run activity
        latest_radar = None
        if radars:
            try:
                from adapt.api.client import DataClient
                client = DataClient(repo)
                latest_run = client.registry.get_latest_run()
                if latest_run and latest_run.get('radar') in radars:
                    latest_radar = latest_run['radar']
            except Exception:
                pass

        if latest_radar:
            self._radar.set(latest_radar)
        elif radars:
            self._radar.set(radars[0])
        else:
            self._radar.set('')

        self._on_radar_changed()

    def _on_radar_changed(self):
        repo = Path(self._repo_root.get().strip())
        radar = self._radar.get().strip().upper()
        # Pass radar to filter runs by the selected radar
        runs = _list_runs(repo, radar=radar if radar else None)
        self.run_cb['values'] = runs
        if runs:
            self._run_sel.set(runs[0])  # Select most recent run (first in list)
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
        # Stop all pending after() callbacks before destroying to avoid
        # "invalid command name" errors from orphaned scheduled calls.
        self._nc_loop_running = False
        for after_id in self._after_ids:
            with contextlib.suppress(Exception):
                self.after_cancel(after_id)
        self._after_ids.clear()

        # Close matplotlib figures
        plt.close('all')

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
        self._after_ids.append(self.after(POLL_MS, self._schedule_refresh))

    def _status_tick(self):
        """Update status bar every second: scan time + countdown to next check."""
        if not self._refresh_active:
            return
        secs = max(0, int(self._next_refresh_at - time.time()))
        scan_str = (self._last_scan_dt.strftime('%H:%M:%S UTC')
                    if self._last_scan_dt else '—')
        self.status_var.set(
            f'{self._status_base}  |  Last scan: {scan_str}  |  Next check: {secs}s')
        self._after_ids.append(self.after(1000, self._status_tick))

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
                    try:
                        self._load_cells_data(repo, radar)
                        self._redraw(xr.open_dataset(latest))
                        self._last_rendered_nc = latest
                        self.scan_var.set(labels[-1] if labels else '')
                        db_path_r = Path(repo) / radar / 'catalog.db'
                        if self._selected_cell_uid and self._current_run_id and db_path_r.exists():
                            try:
                                from adapt.persistence.track_store import TrackStore
                                hist = TrackStore(db_path_r).get_track_history(
                                    self._current_run_id, self._selected_cell_uid)
                                if not hist.empty:
                                    self._update_time_series(hist)
                                else:
                                    self._clear_time_series()
                            except Exception:
                                self._clear_time_series()
                        else:
                            self._clear_time_series()
                    except Exception:
                        pass
                else:
                    # Canvas was cleared externally; re-render
                    try:
                        self._load_cells_data(repo, radar)
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
        """Get all analysis NC files across all date directories."""
        analysis_dir = Path(repo) / radar / 'analysis'
        if not analysis_dir.exists():
            return []

        # Collect NC files from all date subdirectories
        all_nc = []
        for date_dir in analysis_dir.iterdir():
            if date_dir.is_dir() and len(date_dir.name) == 8 and date_dir.name.isdigit():
                all_nc.extend(date_dir.glob('*_analysis.nc'))

        # Sort by filename (contains timestamp)
        return sorted(all_nc, key=lambda p: p.name)

    @staticmethod
    def _nc_label(p):
        parts = p.stem.split('_')
        # filename: RADAR_YYYYMMDD_HHMMSS_analysis  or similar
        d = next((x for x in parts if len(x) == 8 and x.isdigit()), None)
        t = next((x for x in parts if len(x) == 6 and x.isdigit()), None)
        if d and t:
            return f'{d[4:6]}-{d[6:8]} {t[:2]}:{t[2:4]}:{t[4:6]}  ({p.stem})'
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
                                f'No analysis files found in:\n'
                                f'{Path(repo) / radar / "analysis"}',
                                parent=self)
            return
        self._load_cells_data(repo, radar)
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
                                f'No analysis files found in:\n'
                                f'{Path(repo) / radar / "analysis"}',
                                parent=self)
            return

        # Match selected label to NC file
        sel  = self.scan_var.get()
        stem = sel.split('(')[-1].rstrip(')') if '(' in sel else ''
        nc_path = next((p for p in nc_files if p.stem == stem), nc_files[-1])

        self._load_cells_data(repo, radar)
        self._clear_canvas()
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
                    ts_obj = TrackStore(db_path)
                    rows = ts_obj._connect().execute(
                        "SELECT * FROM cells_by_scan WHERE run_id=? ORDER BY scan_time",
                        (run_id,),
                    ).fetchall()
                    if rows:
                        self._current_cell_df = pd.DataFrame([dict(r) for r in rows])
                        self._current_run_id = run_id
                        return
                conn.close()
            except Exception:
                pass

        # Fallback: parquet (may not contain cell_uid)
        pqs = sorted((Path(repo) / radar / 'analysis').glob('analysis2d_*.parquet'))
        if pqs:
            try:
                dfs = [pd.read_parquet(p) for p in pqs]
                self._current_cell_df = pd.concat(dfs, ignore_index=True)
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
                                'No analysis NC files found.', parent=self)
            return
        self._load_cells_data(repo, radar)
        self._nc_loop_files   = nc_files
        self._nc_loop_index   = 0
        self.btn_loop.config(text='Stop Loop')
        self._clear_canvas()
        self._nc_loop_running = True   # set AFTER clear so _clear_canvas doesn't kill it
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
            self._clear_time_series()
            self._redraw(xr.open_dataset(path))
        else:
            self._render_nc(path)
        dt = max(100, self._loop_dt_var.get())
        self._after_ids.append(self.after(dt, self._nc_loop_step))

    # ── Core matplotlib rendering ─────────────────────────────────────────────

    def _render_nc(self, nc_path):
        """Create canvas + bottom strip, then render nc_path into a new figure."""
        ds_tmp = xr.open_dataset(nc_path)
        lat0 = ds_tmp.attrs.get('radar_latitude', ds_tmp.attrs.get('origin_latitude'))
        lon0 = ds_tmp.attrs.get('radar_longitude', ds_tmp.attrs.get('origin_longitude'))
        if lat0 is None or lon0 is None:
            lat0, lon0 = 0, 0
        else:
            lat0, lon0 = float(lat0), float(lon0)
        ds_tmp.close()

        # GridSpec: radar | cbar | time-series (3 columns, 3 rows)
        # cbar column is pre-allocated so colorbar never steals space from radar.
        fig = plt.figure(figsize=(18, 6.5), dpi=90)
        gs  = fig.add_gridspec(
            3, 3,
            width_ratios=[1.4, 0.05, 1.0],
            hspace=0.5, wspace=0.25,
            left=0.04, right=0.97, top=0.93, bottom=0.09,
        )
        ax_radar    = fig.add_subplot(gs[:, 0])
        self._cbar_ax = fig.add_subplot(gs[:, 1])
        ax_area     = fig.add_subplot(gs[0, 2])
        ax_dbz      = fig.add_subplot(gs[1, 2], sharex=ax_area)
        ax_reserved = fig.add_subplot(gs[2, 2], sharex=ax_area)
        self._ts_axes = (ax_area, ax_dbz, ax_reserved)
        self._clear_time_series()

        self._draw_scan(xr.open_dataset(nc_path), fig, ax_radar)

        self.img_label.pack_forget()

        bottom = tk.Frame(self.scan_container, bg=_STRIP_BG)
        bottom.pack(side='bottom', fill='x')

        canvas = FigureCanvasTkAgg(fig, master=self.scan_container)
        canvas.get_tk_widget().pack(fill='both', expand=True)
        canvas.draw()

        toolbar = _CompactToolbar(canvas, bottom, pack_toolbar=False,
                                  lat0=lat0, lon0=lon0)
        toolbar.update()
        toolbar.pack(side='left')

        for var in self._hv.values():
            var.set('—')
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
        canvas.mpl_connect('button_press_event', self._on_cell_click)

    def _draw_scan(self, ds, fig, ax=None):
        """Render dataset into the radar axes. Keeps ds open."""
        # Resolve ax — always the leftmost (index 0) in the GridSpec figure
        if ax is None:
            ax = fig.axes[0]

        ax.clear()
        ax.set_facecolor('white')
        # Track overlay artists were removed by ax.clear(); reset references
        self._track_overlay = None
        self._selected_cell_uid = None

        # Close previous dataset
        if self._current_nc_ds is not None and self._current_nc_ds is not ds:
            with contextlib.suppress(Exception):
                self._current_nc_ds.close()
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
        self._current_scan_ts = ts  # Store for hover filtering

        x_km = ds['x'].values / 1000.0
        y_km = ds['y'].values / 1000.0
        y_grid, x_grid = np.meshgrid(y_km, x_km, indexing='ij')
        labels_data = ds['cell_labels'].values

        # ── Grayscale reflectivity background ────────────────────────────────
        refl = ds['reflectivity'].values.astype(float)
        refl_bg = np.ma.masked_where(np.isnan(refl) | (refl < 10), refl)
        cmap_gray = copy.copy(plt.get_cmap('gray_r'))
        cmap_gray.set_bad(alpha=0)
        # vmin=10 → light gray (~0.35 on gray_r), vmax=50 → black
        bg_alpha = self._bg_alpha_var.get() if self._bg_alpha_var else 0.35
        ax.pcolormesh(x_km, y_km, refl_bg,
                      cmap=cmap_gray, vmin=10, vmax=40,
                      shading='auto', alpha=bg_alpha, zorder=2)

        # ── User-selected variable overlay (cells only) ───────────────────────
        var_name = (self._plot_var.get()
                    if self._plot_var is not None else 'reflectivity')
        if var_name not in ds.data_vars:
            var_name = 'reflectivity'
        vdef     = _VAR_DEFAULTS.get(var_name, (10, 60, 'dBZ', 'viridis'))
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

        raw    = ds[var_name].values.astype(float)
        masked = np.ma.masked_where(np.isnan(raw) | (labels_data <= 0), raw)
        cmap_ov = copy.copy(plt.get_cmap(cmap_str))
        cmap_ov.set_bad(alpha=0)
        im_ov = ax.pcolormesh(x_km, y_km, masked,
                              cmap=cmap_ov, vmin=vmin, vmax=vmax,
                              shading='auto', alpha=0.90, zorder=3)

        if self._cbar_ax is not None:
            self._cbar_ax.cla()
            self._colorbar = fig.colorbar(im_ov, cax=self._cbar_ax, label=unit)
        else:
            self._colorbar = fig.colorbar(im_ov, ax=ax, label=unit,
                                          fraction=0.046, pad=0.04)

        # ── Cell contours ─────────────────────────────────────────────────────
        # for cell_id in np.unique(labels_data[labels_data > 0]):
        #     cs = ax.contour(x_grid, y_grid,
        #                     (labels_data == cell_id).astype(float),
        #                     levels=[0.5], colors='#2C3539', linewidths=0.5, zorder=50)
        #     self._cell_contours[int(cell_id)] = cs

        # ── Projection contours ───────────────────────────────────────────────
        if 'cell_projections' in ds.data_vars:
            proj_da = ds['cell_projections']
            fo      = 'frame_offset'
            if fo in proj_da.dims:
                n_frames  = len(proj_da[fo])
                max_proj  = self._max_proj_var.get() if self._max_proj_var else 0
                end_frame = n_frames if max_proj == 0 else min(n_frames, max_proj + 1)
                _ls_cycle = ['dashed', 'dashdot', 'dotted']
                for i in range(1, end_frame):
                    alpha = max(0.5, 1.0 - i / n_frames)
                    lw    = max(0.5, 1.5 - i * 0.2)
                    ls    = _ls_cycle[(i - 1) % len(_ls_cycle)]
                    lp = proj_da.isel({fo: i}).values
                    for cid in np.unique(lp[~np.isnan(lp) & (lp > 0)]):
                        ax.contour(x_grid, y_grid, (lp == cid).astype(float),
                                   levels=[0.5], colors='#2C3539',
                                   linewidths=lw, linestyles=ls,
                                   alpha=alpha, zorder=40)

        # ── Optical flow vectors (toggle) ─────────────────────────────────────
        if (self._show_flow_var is not None and self._show_flow_var.get()
                and 'heading_x' in ds.data_vars and 'heading_y' in ds.data_vars):
            hx, hy = ds['heading_x'].values, ds['heading_y'].values
            if not np.all(np.isnan(hx)):
                s      = 12
                yi_idx = np.arange(0, len(y_km), s)
                xi_idx = np.arange(0, len(x_km), s)
                Xs, Ys = np.meshgrid(x_km[xi_idx], y_km[yi_idx])
                q = ax.quiver(Xs, Ys,
                              hx[np.ix_(yi_idx, xi_idx)],
                              hy[np.ix_(yi_idx, xi_idx)],
                              color='#5E7F94', alpha=0.7, scale=0.5, scale_units='xy',
                              width=0.002, headwidth=4, zorder=45)
                q._adapt_flow = True

        self._add_basemap(ax, ds, x_km, y_km)
        ax.set_xlabel('X (km)')
        ax.set_ylabel('Y (km)')
        ax.tick_params(reset=True)
        ax.grid(True, alpha=0.3, zorder=3)
        ax.set_title(f'{radar_id}  {var_lbl}\n{tstr}',
                     fontsize=11, fontweight='bold')

    @staticmethod
    def _add_basemap(ax, ds, x_km, y_km):
        if not HAS_CTX:
            print('contextily not available for basemap')
            return

        # Try to get lat/lon from dataset attrs first
        lat = ds.attrs.get('radar_latitude', ds.attrs.get('origin_latitude'))
        lon = ds.attrs.get('radar_longitude', ds.attrs.get('origin_longitude'))

        if lat is None or lon is None:
            radar_id = ds.attrs.get('radar', ds.attrs.get('radar_id', ''))
            print(f'No radar location for {radar_id}')
            return

        lat, lon = float(lat), float(lon)
        crs_str = (f'+proj=aeqd +lat_0={lat} +lon_0={lon} '
                   f'+x_0=0 +y_0=0 +datum=WGS84 +units=km')
        ax.set_xlim(x_km.min(), x_km.max())
        ax.set_ylim(y_km.min(), y_km.max())
        try:
            ctx.add_basemap(ax, crs=crs_str,
                            source=ctx.providers.OpenStreetMap.Mapnik,
                            alpha=0.6, attribution=False, zoom=8, zorder=0)
        except Exception as e:
            print(f'Basemap error: {e}')


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
        ds  = self._current_nc_ds
        x_m = event.xdata * 1000.0
        y_m = event.ydata * 1000.0
        xi  = int(np.argmin(np.abs(ds['x'].values - x_m)))
        yi  = int(np.argmin(np.abs(ds['y'].values - y_m)))
        cell_id = int(ds['cell_labels'].values[yi, xi])
        if cell_id <= 0:
            self._clear_tracking_history()
            self._clear_time_series()
            fig.canvas.draw_idle()
            return
        repo  = self._repo_root.get().strip()
        radar = self._radar.get().strip().upper()
        db_path = Path(repo) / radar / "catalog.db"

        # Resolve cell_uid for clicked cell via SQLite (avoids scan_time format issues)
        cell_uid = None
        if self._current_run_id and db_path.exists() and self._current_scan_ts is not None:
            try:
                from adapt.persistence.track_store import TrackStore
                scan_time_dt = pd.Timestamp(self._current_scan_ts).to_pydatetime()
                ts_obj = TrackStore(db_path)
                scan_cells = ts_obj.get_cells_by_scan(self._current_run_id, scan_time_dt)
                if not scan_cells.empty and 'cell_label' in scan_cells.columns:
                    matched = scan_cells[scan_cells['cell_label'] == cell_id]
                    if not matched.empty:
                        r = matched.iloc[0]
                        cell_uid = r.get('cell_uid')
            except Exception:
                pass

        # Fallback: search loaded cell df with 60-s time window
        if cell_uid is None:
            df = self._current_cell_df
            if df is None or 'cell_uid' not in df.columns:
                return
            if self._current_scan_ts is not None and 'scan_time' in df.columns:
                df_t = df.copy()
                df_t['_st'] = pd.to_datetime(df_t['scan_time'], utc=True)
                scan_ts = pd.Timestamp(self._current_scan_ts)
                if scan_ts.tzinfo is None:
                    scan_ts = scan_ts.tz_localize('UTC')
                time_mask = (df_t['_st'] - scan_ts).abs() < pd.Timedelta(seconds=60)
                scan_rows = df_t[time_mask & (df_t['cell_label'] == cell_id)]
            else:
                scan_rows = df[df['cell_label'] == cell_id]
            if scan_rows.empty:
                return
            r = scan_rows.iloc[0]
            cell_uid = r.get('cell_uid')

        if cell_uid is not None and (isinstance(cell_uid, float) and pd.isna(cell_uid)):
            cell_uid = None

        # Load full tracking history from birth to current scan
        history_df = None
        if self._current_run_id and db_path.exists():
            try:
                from adapt.persistence.track_store import TrackStore
                ts_obj = TrackStore(db_path)
                history_df = ts_obj.get_track_history(self._current_run_id, str(cell_uid))
            except Exception:
                pass

        if history_df is None or history_df.empty:
            df = self._current_cell_df
            if df is not None and cell_uid is not None and 'cell_uid' in df.columns:
                history_df = df[df['cell_uid'] == cell_uid].copy()

        self._clear_tracking_history()
        self._selected_cell_uid = str(cell_uid) if cell_uid is not None else None
        self._draw_tracking_history(ax_radar, history_df)
        self._update_time_series(history_df)
        fig.canvas.draw_idle()

    def _draw_tracking_history(self, ax, history_df: pd.DataFrame | None = None) -> None:
        df = history_df if history_df is not None else self._current_cell_df
        if df is None:
            return
        lat_col, lon_col = 'cell_centroid_mass_lat', 'cell_centroid_mass_lon'
        if lat_col not in df.columns:
            return
        track_df = (
            df.dropna(subset=[lat_col, lon_col])
            .sort_values('scan_time')
        )
        if track_df.empty:
            return
        ds   = self._current_nc_ds
        lat0 = float(ds.attrs.get('radar_latitude', 0.0))
        lon0 = float(ds.attrs.get('radar_longitude', 0.0))
        R    = 6371.0
        lats = track_df[lat_col].values
        lons = track_df[lon_col].values
        y_km = (lats - lat0) * (np.pi / 180.0) * R
        x_km = (lons - lon0) * (np.pi / 180.0) * R * np.cos(np.radians(lat0))
        line, = ax.plot(x_km, y_km, '-', color='cyan',
                        linewidth=1.5, alpha=0.85, zorder=10)
        dots  = ax.scatter(x_km, y_km, s=18, color='cyan', 
                        zorder=11, alpha=0.9)
        cur   = track_df.iloc[-1]
        cy    = float((cur[lat_col] - lat0) * np.pi / 180.0 * R)
        cx    = float((cur[lon_col] - lon0) * np.pi / 180.0 * R
                       * np.cos(np.radians(lat0)))
        star  = ax.scatter([cx], [cy], s=60, color='#8aff9c', marker='*', zorder=12)
        self._track_overlay = [line, dots, star]

    def _clear_tracking_history(self) -> None:
        if self._track_overlay:
            for artist in self._track_overlay:
                with contextlib.suppress(Exception):
                    artist.remove()
            self._track_overlay = None
        self._selected_cell_uid = None

    # ── Time series panels ────────────────────────────────────────────────────

    @staticmethod
    def _style_ts_ax(ax, ylabel: str, title: str) -> None:
        """Apply light-panel styling to a time-series axis (no x-axis work — handled centrally)."""
        ax.set_facecolor('#f5f5f5')
        ax.set_title(title, fontsize=8, color='#222222', pad=3)
        ax.set_ylabel(ylabel, fontsize=7, color='#444444')
        ax.yaxis.label.set_color('#444444')
        ax.tick_params(axis='y', colors='#333333', labelsize=7, which='both')
        for sp in ax.spines.values():
            sp.set_color('#aaaaaa')

    @staticmethod
    def _apply_time_axis(ax_bottom, axes) -> None:
        """Apply shared time-axis formatting. Call after plotting, using bottom axis."""
        ax_bottom.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
        ax_bottom.xaxis.set_major_locator(mdates.AutoDateLocator(minticks=3, maxticks=10))
        ax_bottom.tick_params(axis='x', colors='#333333', labelsize=8, rotation=30)
        ax_bottom.set_xlabel('UTC', fontsize=8, color='#444444')
        ax_bottom.xaxis.label.set_color('#444444')
        for ax in axes[:-1]:
            plt.setp(ax.get_xticklabels(), visible=False)
            ax.tick_params(axis='x', colors='#aaaaaa', which='both')

    def _update_time_series(self, history_df: pd.DataFrame | None = None) -> None:
        if self._ts_axes is None:
            return
        ax_area, ax_dbz, ax_extra = self._ts_axes
        if history_df is not None and not history_df.empty:
            track_df = history_df.sort_values('scan_time')
            cell_uid = None
            if 'cell_uid' in track_df.columns and track_df['cell_uid'].notna().any():
                cell_uid = str(track_df['cell_uid'].dropna().iloc[0])
        else:
            cell_uid = self._selected_cell_uid
            if (not cell_uid or self._current_cell_df is None
                    or 'cell_uid' not in self._current_cell_df.columns):
                return
            track_df = (
                self._current_cell_df[self._current_cell_df['cell_uid'] == str(cell_uid)]
                .sort_values('scan_time')
            )
            if track_df.empty:
                return

        for ax in (ax_area, ax_dbz, ax_extra):
            ax.cla()

        times = pd.to_datetime(track_df['scan_time'], utc=True)

        # ── Area panel ────────────────────────────────────────────────────────
        if 'cell_area_sqkm' in track_df.columns:
            vals = track_df['cell_area_sqkm'].values
            ax_area.plot(times, vals, color='#7ec8e3', linewidth=1.5, label='total area')
            ax_area.fill_between(times, vals, alpha=0.15, color='#7ec8e3')
        if 'area_40dbz_km2' in track_df.columns:
            ax_area.plot(times, track_df['area_40dbz_km2'].values,
                         color='#ff9944', linewidth=1.0, linestyle='--', label='≥40 dBZ core')
        self._style_ts_ax(ax_area, 'km²', f'Cell {_cell_uid_disp(cell_uid)} — Area')
        if ax_area.get_lines():
            ax_area.legend(fontsize=6, labelcolor='#444', framealpha=0.5,
                           loc='upper left', handlelength=1.2)

        # ── Reflectivity panel ────────────────────────────────────────────────
        if 'radar_reflectivity_mean' in track_df.columns:
            ax_dbz.plot(times, track_df['radar_reflectivity_mean'].values,
                        color='#88cc44', linewidth=1.2, label='mean Z')
        if 'radar_reflectivity_max' in track_df.columns:
            ax_dbz.plot(times, track_df['radar_reflectivity_max'].values,
                        color='#ff6644', linewidth=1.2, label='max Z')
        self._style_ts_ax(ax_dbz, 'dBZ', 'Reflectivity')
        if ax_dbz.get_lines():
            ax_dbz.legend(fontsize=6, labelcolor='#444', framealpha=0.5,
                          loc='upper left', handlelength=1.2)

        # ── ZDR / extra panel ─────────────────────────────────────────────────
        has_extra = False
        if 'radar_differential_reflectivity_max' in track_df.columns:
            zdr = track_df['radar_differential_reflectivity_max']
            if zdr.notna().any():
                ax_extra.plot(times, zdr.values, color='#cc88ff', linewidth=1.2, label='max ZDR')
                has_extra = True
        self._style_ts_ax(ax_extra, 'dB', 'ZDR')
        if has_extra:
            ax_extra.legend(fontsize=6, labelcolor='#444', framealpha=0.5,
                            loc='upper left', handlelength=1.2)
        else:
            ax_extra.text(0.5, 0.5, 'no ZDR data', transform=ax_extra.transAxes,
                          ha='center', va='center', color='#888', fontsize=7)

        self._apply_time_axis(ax_extra, self._ts_axes)

    def _clear_time_series(self) -> None:
        if self._ts_axes is None:
            return
        for ax, (ylabel, title) in zip(
            self._ts_axes,
            [('km²', 'Area'), ('dBZ', 'Reflectivity'), ('dB', 'ZDR')],
            strict=False,
        ):
            ax.cla()
            self._style_ts_ax(ax, ylabel, title)
            ax.text(0.5, 0.5, 'click a cell', transform=ax.transAxes,
                    ha='center', va='center', color='#888', fontsize=8)
        ax_extra = self._ts_axes[-1]
        ax_extra.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
        ax_extra.set_xlabel('UTC', fontsize=7, color='#444444')
        ax_extra.tick_params(axis='x', colors='#333333', labelsize=7, rotation=30)
        for ax in self._ts_axes[:-1]:
            plt.setp(ax.get_xticklabels(), visible=False)

    # ── Escape: clear overlay ─────────────────────────────────────────────────

    def _on_escape(self, _event=None) -> None:
        self._clear_tracking_history()
        self._clear_time_series()
        if self._canvas_refs:
            _, fig, _, _ = self._canvas_refs
            fig.canvas.draw_idle()

    def _clear_canvas(self):
        self._nc_loop_running = False
        self._last_rendered_nc = None
        if hasattr(self, 'btn_loop'):
            self.btn_loop.config(text='Show Loop')

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

        # Only process hover on the radar panel (axes[0])
        if self._canvas_refs is not None:
            _, fig, _, _ = self._canvas_refs
            if len(fig.axes) > 0 and event.inaxes is not fig.axes[0]:
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

            # ── Cell stats from cells_by_scan (filter by scan time AND cell_id) ─
            df = self._current_cell_df
            if df is not None and 'cell_label' in df.columns:
                if self._current_scan_ts is not None and 'scan_time' in df.columns:
                    df_time = df.copy()
                    df_time['scan_time'] = pd.to_datetime(df_time['scan_time'], utc=True)
                    scan_ts = (self._current_scan_ts.tz_localize('UTC')
                               if self._current_scan_ts.tzinfo is None
                               else self._current_scan_ts)
                    valid_mask = df_time['scan_time'].notna()
                    time_diff = abs(df_time.loc[valid_mask, 'scan_time'] - scan_ts)
                    time_mask = pd.Series(False, index=df_time.index)
                    time_mask.loc[valid_mask] = time_diff < pd.Timedelta(minutes=1)
                    rows = df_time[time_mask & (df_time['cell_label'] == cell_id)]
                else:
                    rows = df[df['cell_label'] == cell_id]
                if not rows.empty:
                    r = rows.iloc[0]

                    def _f(key, fmt='.1f', suffix=''):
                        if key in r and r[key] == r[key]:
                            return f'{r[key]:{fmt}}{suffix}'
                        return _em

                    pid = r.get('cell_uid')
                    if pid and pid == pid:
                        self._hv['cell_uid'].set(_cell_uid_disp(pid))
                    else:
                        self._hv['cell_uid'].set(_em)
                    self._hv['area'].set(_f('cell_area_sqkm'))

                    # Age: prefer age_seconds; fallback = count unique scans for tracking history
                    age_raw = r.get('age_seconds')
                    if age_raw is not None and age_raw == age_raw:
                        age_s = float(age_raw)
                        if age_s < 60:
                            age_str = f'{int(age_s)}s'
                        elif age_s < 3600:
                            age_str = f'{int(age_s / 60)}m{int(age_s % 60):02d}s'
                        else:
                            age_str = (f'{int(age_s / 3600)}h'
                                       f'{int((age_s % 3600) / 60):02d}m')
                        self._hv['age'].set(age_str)
                    elif self._current_cell_df is not None:
                        cdf = self._current_cell_df
                        if pid and 'cell_uid' in cdf.columns:
                            mask = cdf['cell_uid'] == str(pid)
                        else:
                            mask = None
                        if mask is not None:
                            n_scans = int(
                                mask.groupby(cdf['scan_time']).any().sum()
                            ) if 'scan_time' in cdf.columns else int(mask.sum())
                            self._hv['age'].set(f'{n_scans} scans')
                    else:
                        self._hv['age'].set(_em)

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

            for k in _HV_KEYS:
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

        df = None
        # Try SQLite cells_by_scan first
        db_path = Path(repo) / radar / "catalog.db"
        if db_path.exists() and self._current_run_id:
            try:
                from adapt.persistence.track_store import TrackStore
                ts_obj = TrackStore(db_path)
                conn = ts_obj._connect()
                rows = conn.execute(
                    "SELECT * FROM cells_by_scan WHERE run_id=? ORDER BY scan_time, cell_uid",
                    (self._current_run_id,),
                ).fetchall()
                if rows:
                    df = pd.DataFrame([dict(r) for r in rows])
            except Exception:
                df = None

        # Fallback: parquet
        if df is None or df.empty:
            pqs = sorted((Path(repo) / radar / 'analysis').glob('analysis2d_*.parquet'))
            if not pqs:
                self.stats_lbl.config(text='No data yet.')
                return
            try:
                dfs = [pd.read_parquet(p) for p in pqs]
                df = pd.concat(dfs, ignore_index=True)
            except Exception as e:
                self.stats_lbl.config(text=f'Error: {e}')
                return

        if df is None or df.empty:
            self.stats_lbl.config(text='No data yet.')
            return

        try:
            df['scan_time']  = pd.to_datetime(df['scan_time'], utc=True)
            df['time_label'] = df['scan_time'].dt.strftime('%H:%M:%S')
        except Exception:
            pass

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
        pid_prefix = self._cell_uid_filter.get().strip().upper() if self._cell_uid_filter else ''
        if pid_prefix and 'cell_uid' in df.columns:
            mask &= df['cell_uid'].astype(str).str.upper().str.startswith(pid_prefix)

        filt = df[mask]

        def _avg(col, fmt='.1f'):
            return (f'{filt[col].mean():{fmt}}'
                    if col in filt.columns and not filt.empty else '\u2014')

        self.stats_lbl.config(
            text=(f'Showing {len(filt)} / {len(df)} cells'
                  f'  |  Avg dBZ: {_avg("radar_reflectivity_mean")}'
                  f'  |  Avg area: {_avg("cell_area_sqkm")} km\u00b2'
                  f'  |  Avg ZDR: {_avg("radar_differential_reflectivity_mean", ".2f")}'))

        # Build column list dynamically from available data
        preferred = [
            'time_label', 'cell_uid', 'cell_label',
            'cell_area_sqkm', 'area_40dbz_km2',
            'radar_reflectivity_max', 'radar_reflectivity_mean',
            'radar_differential_reflectivity_max', 'radar_differential_reflectivity_mean',
            'cell_centroid_mass_lat', 'cell_centroid_mass_lon',
            'n_adjacent_cells',
        ]
        show_cols = [c for c in preferred if c in filt.columns]
        # Rebuild treeview columns if they changed
        if list(self._tv_cols) != show_cols:
            self._tv_cols = show_cols
            self.tv['columns'] = show_cols
            col_widths = {
                'time_label': 65, 'cell_uid': 160, 'cell_label': 55,
                'cell_area_sqkm': 70, 'area_40dbz_km2': 70,
                'radar_reflectivity_max': 75, 'radar_reflectivity_mean': 75,
                'radar_differential_reflectivity_max': 75,
                'radar_differential_reflectivity_mean': 75,
                'cell_centroid_mass_lat': 80, 'cell_centroid_mass_lon': 80,
                'n_adjacent_cells': 65,
            }
            for c in show_cols:
                hdr = (c.replace('radar_differential_reflectivity_', 'ZDR ')
                         .replace('radar_reflectivity_', 'Z ')
                         .replace('cell_', '').replace('_', ' '))
                self.tv.heading(c, text=hdr)
                self.tv.column(c, width=col_widths.get(c, 70), anchor='center')

        self.tv.delete(*self.tv.get_children())
        for _, row in filt[show_cols].iterrows():
            vals = []
            for c in show_cols:
                v = row.get(c, '')
                if isinstance(v, float):
                    vals.append(f'{v:.2f}' if not pd.isna(v) else '\u2014')
                else:
                    if c == 'cell_uid':
                        vals.append(_cell_uid_disp(v))
                    else:
                        vals.append(str(v) if not pd.isna(v) else '\u2014')
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

def main(repo: str = None):
    """Launch the Adapt Dashboard.

    Parameters
    ----------
    repo : str, optional
        Repository path to preload
    """
    app = AdaptDashboard(repo=repo)
    app.mainloop()


if __name__ == '__main__':
    main()
