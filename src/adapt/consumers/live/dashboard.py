# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Adapt Radar Dashboard — A Very basic Tkinter GUI for exploring pipeline outputs in realtime.

Entry point: adapt dashboard [--repo /path/to/repo]

Layout
------
- Toolbar: repo browser, radar/run selection, refresh, pipeline start/stop
- Tab 0 "Latest Scan": matplotlib canvas (left) + cell-info panel (right)
                        + quick-filter strip (bottom)
- Tab 1 "Target Selection": rule builder + live engine replay on a reflectivity map
- Tab 2 "Log": pipeline stdout

Single-instance note
--------------------
Only one `adapt run-nexrad` is allowed at a time (enforced by PID file).
The dashboard is a pure consumer — it reads from the repository and does
not need a running pipeline.  The Start/Stop buttons are provided for
convenience.
"""

import contextlib
import logging
import time
from datetime import datetime
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


# ── Tkinter ───────────────────────────────────────────────────────────────────
import tkinter as tk  # noqa: E402
from tkinter import filedialog, messagebox, simpledialog, ttk  # noqa: E402

# Matplotlib state is configured lazily to avoid importing the TkAgg backend at
# module import time. That keeps headless CI and non-GUI code paths importable.
plt: Any = None
ScanViewTab: Any = None
TargetSelectionTab: Any = None


def _ensure_tkagg_backend() -> None:
    global plt, ScanViewTab, TargetSelectionTab

    if plt is not None:
        return

    import matplotlib  # noqa: E402

    matplotlib.use("TkAgg")
    import cmweather  # noqa: E402, F401 — registers ChaseSpectral and other radar colormaps — must follow use()
    import matplotlib.pyplot as _plt  # noqa: E402

    from adapt.consumers.live._scan_view import ScanViewTab as _ScanViewTab  # noqa: E402
    from adapt.consumers.live._tse_view import (
        TargetSelectionTab as _TargetSelectionTab,  # noqa: E402
    )

    plt = _plt
    ScanViewTab = _ScanViewTab
    TargetSelectionTab = _TargetSelectionTab


# ── Repository access (Ring 2 consumer via the public API) ───────────────────
from adapt.api.client import RepositoryClient  # noqa: E402
from adapt.consumers.live._config import (  # noqa: E402, I001
    POLL_MS,
    _list_user_configs,
    _load_default_config,
    _load_recent_repos,
    _load_user_config,
    _save_recent_repos,
    _save_user_config,
)
from adapt.consumers.live._context import AppContext  # noqa: E402
from adapt.consumers.live._movie_dialog import MovieDialog  # noqa: E402
from adapt.consumers.live._pipeline import PipelineController  # noqa: E402
from adapt.consumers.live._timers import AfterHandles  # noqa: E402
from adapt.consumers.live._utils import (  # noqa: E402
    _list_radars,
    _list_runs,
    _pipeline_running,
    _suppress_osx_stderr,
    startup_repo,
)

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
        self._refresh_active = True

        # Config — loaded from bundled JSON, optionally overridden by user-saved config
        self._cfg: dict = _load_default_config()

        _ensure_tkagg_backend()

        # Session facts shared with tabs — tabs depend on this, never on the shell
        self._ctx = AppContext(
            get_repo=self._repo_root.get,
            get_radar=self._radar.get,
            get_run_sel=self._run_sel.get,
            get_cfg=lambda: self._cfg,
            report_scan_time=self._report_scan_time,
        )

        self._auto_refresh_var = tk.BooleanVar(value=True)

        # Recent repos: loaded from user_dashboard.json["recent_repos"]
        self._recent_repos: list[str] = _load_recent_repos()
        self._pipeline_badge: tk.Label | None = None

        # after() ids — recurring timers keep one live id each (never accumulate);
        # cancelled on close to prevent post-destroy callbacks.
        self._timers = AfterHandles(self.after, self.after_cancel)

        # Status bar state
        self._status_base = "Idle"
        self._last_scan_dt = None  # datetime of last rendered scan
        self._next_refresh_at = time.time() + POLL_MS / 1000

        self._build_ui()
        self.protocol("WM_DELETE_WINDOW", self._on_close)
        self.bind("<Escape>", lambda _: self._scan_view.clear_selection())
        self.bind("<space>", lambda _: self._scan_view.show_latest())
        self.bind("<Left>", lambda _: self._scan_view.prev_scan())
        self.bind("<Right>", lambda _: self._scan_view.next_scan())
        self.bind("l", lambda _: self._scan_view.toggle_loop())
        self.bind("<Control-r>", lambda _: self._refresh_all())
        self.bind("<Control-o>", lambda _: self._browse_repo())

        # Start auto-refresh and status countdown ticker
        self._timers.recurring("refresh", 500, self._schedule_refresh)
        self._timers.recurring("status", 1000, self._status_tick)

        # Open the working directory's repository when there is one, else the most
        # recent — so panels show on startup without a --repo argument either way.
        opening = startup_repo(repo, Path.cwd(), self._recent_repos)
        if opening:
            self._repo_root.set(opening)
            self.after(200, self._on_repo_changed)
        else:
            self.after(150, self._show_first_run_dialog)

        # Offer reconnect if a pipeline is already running externally
        if _pipeline_running():
            self.after(600, self._pipeline.check_reconnect)

    # ── Build UI ──────────────────────────────────────────────────────────────

    def _build_ui(self):
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
        self.run_cb.bind("<<ComboboxSelected>>", lambda _: self._on_run_changed())

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

        self._scan_view = ScanViewTab(self._nb, self._ctx)
        self._tse_view = TargetSelectionTab(self._nb, self._ctx)
        self._pipeline = PipelineController(
            self,
            self._nb,
            self._ctx,
            badge=self._pipeline_badge,
            status_var=self.status_var,
            adopt_repo=self._adopt_repo,
        )

        self._nb.bind("<<NotebookTabChanged>>", self._on_tab_change)

        # Menubar last — its View items reference scan-view state
        self._build_menubar()

    # ── Tab 0: Latest Scan ────────────────────────────────────────────────────

    def _on_run_changed(self):
        """User picked a run: reset both tabs, reload, and show its latest scan."""
        self._scan_view.on_run_changed()
        self._tse_view.on_run_changed()
        self._refresh_all()
        self._scan_view.show_latest_soon()

    # ── Save Movie (File menu) ────────────────────────────────────────────────

    def _save_movie(self):
        """Record the active tab's plot panel over a scan range to .mp4/.gif."""
        sources = {0: self._scan_view.movie_source, 1: self._tse_view.movie_source}
        source = sources.get(self._nb.index("current"), lambda: None)()
        if source is None:
            messagebox.showinfo(
                "Save Movie",
                "Nothing to record here — render a scan or start a Target Selection session first.",
                parent=self,
            )
            return
        MovieDialog(self, source)

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
        m.add_command(label="Save Movie…", command=self._save_movie)
        m.add_separator()
        m.add_command(label="Exit", command=self._on_close)
        self._refresh_recent_menu()

    def _build_pipeline_menu(self, mb: tk.Menu) -> None:
        m = tk.Menu(mb, tearoff=False)
        mb.add_cascade(label="Pipeline", menu=m)
        m.add_command(label="Start New…", command=lambda: self._pipeline.open_wizard())
        m.add_command(label="■ Stop", command=lambda: self._pipeline.stop())
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
        m.add_command(
            label="⚙ Plot Settings…", command=lambda: self._scan_view.open_plot_settings()
        )
        m.add_separator()
        m.add_checkbutton(label="Show Optical Flow", variable=self._scan_view.show_flow_var)
        m.add_command(label="Background Opacity…", command=lambda: self._scan_view.ask_bg_alpha())
        m.add_command(label="Projection Steps…", command=lambda: self._scan_view.ask_proj_steps())
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
        messagebox.showinfo("Reset", "Dashboard config reset to defaults.", parent=self)

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
            with contextlib.closing(RepositoryClient(repo)) as client:
                runs_all = client.runs()
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
        self._scan_view.on_run_changed()  # reset zoom when radar/run changes
        repo = Path(self._repo_root.get().strip())
        radar = self._radar.get().strip().upper()
        # Pass radar to filter runs by the selected radar
        runs = _list_runs(repo, radar=radar if radar else None)
        self.run_cb["values"] = runs
        if runs:
            self._run_sel.set(runs[0])  # Select most recent run (first in list)
        else:
            self._run_sel.set("")
        self._refresh_all()
        # Auto-show the latest scan so panels appear immediately on repo load
        self._scan_view.show_latest_soon()

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
            self._pipeline.open_wizard()

        ttk.Button(win, text="Start New Pipeline…", command=_start_new).pack(pady=(2, 16))

    def _adopt_repo(self, repo_dir: str) -> None:
        """Select repo_dir in the toolbar and re-scan it as the registry appears."""
        self._repo_root.set(repo_dir)
        self._record_recent_repo(repo_dir)
        # adapt_registry.db is created by the pipeline on first run, so retry
        # until it appears (3 s, 8 s, 15 s, 25 s after launch).
        for delay_ms in (3000, 5000, 7000, 10000):
            self._timers.oneshot(delay_ms, self._on_repo_changed)

    def _report_scan_time(self, dt) -> None:
        """Status-bar hook: tabs report the timestamp of the scan they rendered."""
        self._last_scan_dt = dt

    def _on_close(self):
        # Cancel all pending after() callbacks before destroying.
        self._scan_view.on_close()
        self._tse_view.on_close()
        self._refresh_active = False
        self._timers.cancel_all()

        plt.close("all")

        if not self._pipeline.confirm_close():
            # User cancelled — restore refresh loop and stay open.
            self._refresh_active = True
            self._timers.recurring("refresh", POLL_MS, self._schedule_refresh)
            self._timers.recurring("status", 1000, self._status_tick)
            return

        self._pipeline.shutdown()
        self._ctx.close()
        self.destroy()

    # ── Auto-refresh ──────────────────────────────────────────────────────────

    def _schedule_refresh(self):
        if self._auto_refresh_var.get():
            self._refresh_all()
        self._timers.recurring("refresh", POLL_MS, self._schedule_refresh)

    def _status_tick(self):
        """Update status bar every second: scan time + countdown to next check."""
        if not self._refresh_active:
            return
        secs = max(0, int(self._next_refresh_at - time.time()))
        scan_str = self._last_scan_dt.strftime("%H:%M:%S UTC") if self._last_scan_dt else "—"
        self.status_var.set(
            f"{self._status_base}  |  Last scan: {scan_str}  |  Next check: {secs}s"
        )
        self._pipeline.update_badge()
        self._timers.recurring("status", 1000, self._status_tick)

    def _refresh_all(self):
        repo = self._ctx.repo()
        radar = self._ctx.radar()
        if not repo or not radar:
            return

        n_scans = self._scan_view.refresh()
        state = "Running" if self._pipeline.is_running() else ("Idle" if not n_scans else "Done")
        self._status_base = f"{state}  |  Radar: {radar}  |  Scans: {n_scans}"
        self._next_refresh_at = time.time() + POLL_MS / 1000

        self._tse_view.refresh()
        if self._nb.index("current") == 2:
            self._pipeline.flush_log()

    def _on_tab_change(self, _):
        idx = self._nb.index("current")
        if idx == 2:
            self._pipeline.flush_log()


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
