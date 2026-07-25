# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Target Selection tab — rule builder + engine replay map + rationale panel.

Replays the Target Selection Engine over the selected run scan-by-scan on a
reflectivity map (after()-driven; never blocks the UI), then goes live. All
repository access goes through the AppContext's RepositoryClient.
"""

import contextlib
import logging
import tkinter as tk
from tkinter import messagebox, scrolledtext, ttk

from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
from matplotlib.figure import Figure

from adapt.consumers.live._config import POLL_MS
from adapt.consumers.live._context import AppContext
from adapt.consumers.live._movie import MovieSource, MovieSpec
from adapt.consumers.live._targeting import (
    build_tse_config,
    discover_numeric_columns,
    draw_tse_map,
    find_nc_for_scan,
    format_rationale,
    nc_index_by_scan,
)
from adapt.consumers.live._widgets import _CompactToolbar
from adapt.consumers.target_selection import (
    TargetSelectionEngine,
    build_snapshot,
    is_candidate,
)
from adapt.utils.time import from_scan_iso

logger = logging.getLogger(__name__)

# Rules pre-loaded into the tab: (field, min, max); "" = unbounded on that side.
_DEFAULT_RULES = (
    ("cell_area_sqkm", "20", "200"),
    ("radar_reflectivity_mean", "40", ""),
    ("radar_differential_reflectivity_mean", "2", ""),
    ("age_seconds", "300", ""),
    ("", "", ""),
    ("", "", ""),
)

_IDLE_TITLE = "Press Play to run the engine over the selected run"


class TargetSelectionTab:
    """Rule builder + target-selection engine replay map + rationale."""

    def __init__(self, nb: ttk.Notebook, ctx: AppContext):
        self.ctx = ctx
        self.frame = ttk.Frame(nb)
        nb.add(self.frame, text="Target Selection")

        # Replay session state
        self._engine: TargetSelectionEngine | None = None
        self._cfg = None  # TSEConfig | None
        self._run_id: str = ""
        self._scan_times: list = []  # sorted scan datetimes of the run
        self._nc_index: dict = {}  # scan_time → analysis NC path
        self._index = 0  # count of scans processed/shown so far
        self._running = False
        self._step_after_id = None  # single in-flight replay step (never two)
        self._columns_run: str = ""  # run whose columns fill the dropdowns

        self._build_ui(self.frame)

    # ── UI construction ───────────────────────────────────────────────────────

    def _build_ui(self, tab: ttk.Frame) -> None:
        left = ttk.Frame(tab, padding=(8, 6), width=360)
        left.pack(side="left", fill="y")
        left.pack_propagate(False)

        ttk.Label(left, text="Candidate rules", font=("", 11, "bold")).pack(anchor="w")
        ttk.Label(
            left,
            text="A cell qualifies only if it is inside every set range.",
            foreground="#666",
            font=("", 8),
        ).pack(anchor="w", pady=(0, 4))

        hdr = ttk.Frame(left)
        hdr.pack(fill="x")
        ttk.Label(hdr, text="Variable", width=20, anchor="w", font=("", 8)).pack(side="left")
        ttk.Label(hdr, text="Min", width=8, anchor="w", font=("", 8)).pack(side="left", padx=2)
        ttk.Label(hdr, text="Max", width=8, anchor="w", font=("", 8)).pack(side="left")

        self._rule_rows = []  # (field_var, min_var, max_var)
        self._field_cbs = []
        for field, lo, hi in _DEFAULT_RULES:
            row = ttk.Frame(left)
            row.pack(fill="x", pady=1)
            field_var = tk.StringVar(value=field)
            min_var = tk.StringVar(value=lo)
            max_var = tk.StringVar(value=hi)
            field_cb = ttk.Combobox(
                row,
                textvariable=field_var,
                values=[field] if field else [""],
                state="readonly",
                width=19,
            )
            field_cb.pack(side="left")
            ttk.Spinbox(row, textvariable=min_var, from_=-1e6, to=1e6, increment=1, width=7).pack(
                side="left", padx=2
            )
            ttk.Spinbox(row, textvariable=max_var, from_=-1e6, to=1e6, increment=1, width=7).pack(
                side="left"
            )
            self._rule_rows.append((field_var, min_var, max_var))
            self._field_cbs.append(field_cb)

        # ── Engine settings ───────────────────────────────────────────────────
        ttk.Separator(left, orient="horizontal").pack(fill="x", pady=(8, 6))
        ttk.Label(left, text="Engine settings", font=("", 10, "bold")).pack(anchor="w")
        self._min_age = tk.StringVar(value="0")
        self._max_obs = tk.StringVar(value="20")
        self._switch = tk.StringVar(value="15")

        def setting_row(label: str, var: tk.StringVar, lo: float, hi: float, inc: float) -> None:
            r = ttk.Frame(left)
            r.pack(fill="x", pady=1)
            ttk.Label(r, text=label, width=20, anchor="w").pack(side="left")
            ttk.Spinbox(r, textvariable=var, from_=lo, to=hi, increment=inc, width=8).pack(
                side="left"
            )

        setting_row("Min age (s)", self._min_age, 0, 7200, 60)
        setting_row("Max observation (min)", self._max_obs, 1, 240, 5)
        setting_row("Switch margin", self._switch, 0, 500, 5)

        # ── Replay controls ───────────────────────────────────────────────────
        ttk.Separator(left, orient="horizontal").pack(fill="x", pady=(8, 6))
        ctrl = ttk.Frame(left)
        ctrl.pack(fill="x")
        self._play_btn = ttk.Button(ctrl, text="▶ Play", command=self._toggle_play)
        self._play_btn.pack(side="left")
        ttk.Button(ctrl, text="◄", width=3, command=lambda: self._step_manual(-1)).pack(
            side="left", padx=(6, 1)
        )
        ttk.Button(ctrl, text="►", width=3, command=lambda: self._step_manual(1)).pack(
            side="left", padx=1
        )
        ttk.Button(ctrl, text="Reset", command=self._reset).pack(side="left", padx=6)

        speed_row = ttk.Frame(left)
        speed_row.pack(fill="x", pady=(6, 0))
        ttk.Label(speed_row, text="Speed (ms/scan)", width=16, anchor="w").pack(side="left")
        self._speed = tk.StringVar(value="500")
        ttk.Spinbox(
            speed_row, textvariable=self._speed, from_=100, to=5000, increment=100, width=8
        ).pack(side="left")

        self._status = ttk.Label(
            left, text="Select a run, set rules, then Play.", foreground="#333", wraplength=340
        )
        self._status.pack(anchor="w", pady=(10, 0), fill="x")

        # ── Right: embedded replay map (top) + rationale panel (bottom) ───────
        right = ttk.Frame(tab, padding=(4, 4))
        right.pack(side="left", fill="both", expand=True)

        rat_frame = ttk.Frame(right)
        rat_frame.pack(side="bottom", fill="x", pady=(4, 0))
        ttk.Label(rat_frame, text="Rationale", font=("", 9, "bold")).pack(anchor="w")
        self._rationale = scrolledtext.ScrolledText(
            rat_frame, height=11, wrap="word", font=("Courier", 9), state="disabled"
        )
        self._rationale.pack(fill="x")

        map_area = ttk.Frame(right)
        map_area.pack(side="top", fill="both", expand=True)
        # A standalone Figure (NOT plt.figure()) — pyplot's figure manager would
        # open a competing Tk window for this second canvas, a known segfault
        # source on macOS TkAgg. This figure is owned solely by the embedded canvas.
        fig = Figure(figsize=(6.5, 6.0), dpi=90)
        ax = fig.add_subplot(111)
        ax.set_title(_IDLE_TITLE)
        canvas = FigureCanvasTkAgg(fig, master=map_area)
        assert _CompactToolbar is not None  # matplotlib import above already succeeded
        toolbar = _CompactToolbar(canvas, map_area, pack_toolbar=False)
        toolbar.pack(side="bottom", fill="x")
        canvas.get_tk_widget().pack(side="top", fill="both", expand=True)
        self._canvas_refs = (canvas, fig, ax, toolbar)
        canvas.draw_idle()

    def _set_rationale(self, text):
        """Replace the rationale panel's contents (read-only widget)."""
        self._rationale.config(state="normal")
        self._rationale.delete("1.0", "end")
        self._rationale.insert("end", text)
        self._rationale.config(state="disabled")

    # ── Run resolution, columns, config ───────────────────────────────────────

    def _populate_columns(self):
        """Fill the six rule dropdowns from the selected run's numeric columns."""
        run_id = self.ctx.run_id()
        if not (run_id and self.ctx.repo() and self.ctx.radar()) or run_id == self._columns_run:
            return
        try:
            df = self.ctx.client().table("cells_by_scan", radar=self.ctx.radar(), run_id=run_id)
        except Exception:
            logger.exception("Failed to read columns for run %s", run_id)
            return
        columns = discover_numeric_columns(df)
        if not columns:
            return
        for cb in self._field_cbs:
            cb["values"] = ["", *columns]
        self._columns_run = run_id

    def _rules(self):
        """Current rule rows as (field, min, max) tuples; blank bound → None,
        blank field → dropped."""

        def num(text):
            text = text.strip()
            return float(text) if text else None

        rules = []
        for field_var, min_var, max_var in self._rule_rows:
            field = field_var.get().strip()
            if not field:
                continue
            rules.append((field, num(min_var.get()), num(max_var.get())))
        return rules

    def _build_config(self):
        return build_tse_config(
            self._rules(),
            min_age_s=float(self._min_age.get()),
            max_obs_s=float(self._max_obs.get()) * 60.0,
            switch_margin=float(self._switch.get()),
            growth_window=4,
        )

    # ── Replay loop (after()-driven; never blocks the UI) ─────────────────────

    def _toggle_play(self):
        if self._running:
            self._pause()
            return
        if self._engine is None and not self._ensure_session():
            return
        self._running = True
        self._play_btn.config(text="❚❚ Pause")
        self._schedule(10)

    def _schedule(self, delay):
        """Schedule the next replay step, cancelling any already pending — so the
        step chain can never fork into two after a pause/resume across a live wait."""
        if self._step_after_id is not None:
            with contextlib.suppress(Exception):
                self.frame.after_cancel(self._step_after_id)
        self._step_after_id = self.frame.after(delay, self._step)

    def _pause(self):
        self._running = False
        if self._step_after_id is not None:
            with contextlib.suppress(Exception):
                self.frame.after_cancel(self._step_after_id)
            self._step_after_id = None
        self._play_btn.config(text="▶ Play")

    def _run_scan_times(self, radar, run_id):
        """Distinct scan datetimes of a run, from cells_by_scan (the authoritative
        per-scan table build_snapshot itself reads — so the timeline always
        matches, unlike the optional scans registry)."""
        hist = self.ctx.client().table("cells_by_scan", radar=radar, run_id=run_id)
        if hist.empty or "scan_time" not in hist.columns:
            return []
        return sorted({from_scan_iso(str(s)) for s in hist["scan_time"].unique()})

    def _ensure_session(self) -> bool:
        """Build cfg/client/engine + the run's scan list. False (with a dialog)
        on any missing selection or invalid rule."""
        radar = self.ctx.radar()
        run_id = self.ctx.run_id()
        if not (self.ctx.repo() and radar and run_id):
            messagebox.showinfo(
                "Target Selection", "Select a repository, radar, and run first.", parent=self.frame
            )
            return False
        try:
            cfg = self._build_config()
        except Exception as exc:
            messagebox.showerror("Invalid rules", str(exc), parent=self.frame)
            return False
        try:
            scan_times = self._run_scan_times(radar, run_id)
        except Exception as exc:
            logger.exception("Failed to list scans for run %s", run_id)
            messagebox.showerror(
                "Target Selection", f"Could not list scans: {exc}", parent=self.frame
            )
            return False
        if not scan_times:
            messagebox.showinfo(
                "Target Selection", f"Run {run_id} has no scans yet.", parent=self.frame
            )
            return False
        self._cfg = cfg
        self._run_id = run_id
        self._scan_times = scan_times
        self._nc_index = nc_index_by_scan(self.ctx.nc_files())
        self._engine = TargetSelectionEngine(cfg)
        self._index = 0
        return True

    def _step(self):
        if not self._running:
            return
        if self._index >= len(self._scan_times):
            self._go_live()
            return
        result = self._process(self._index)
        if result is None:
            self._pause()
            return
        self._index += 1
        self._render(*result)
        try:
            dt = max(100, int(float(self._speed.get())))
        except (TypeError, ValueError):
            dt = 500
        self._schedule(dt)

    def _process(self, i):
        """Run the engine on scan i; return (scan_ts, snap, selection, candidates)."""
        scan_ts = self._scan_times[i]
        try:
            snap = build_snapshot(
                self.ctx.client(),
                self._run_id,
                self.ctx.radar(),
                growth_window_scans=self._cfg.snapshot.growth_window_scans,
                at=scan_ts,
            )
            selection = self._engine.select(snap)
            candidates = [c.uid for c in snap.cells if is_candidate(c, self._cfg.candidate)]
        except Exception:
            logger.exception("Target selection failed at scan %s", scan_ts)
            return None
        return scan_ts, snap, selection, candidates

    def _go_live(self):
        """Reached the newest scan: pick up new scans, or wait while live."""
        radar = self.ctx.radar()
        try:
            times = self._run_scan_times(radar, self._run_id)
        except Exception:
            logger.exception("go_live scan re-query failed")
            times = self._scan_times
        if len(times) > len(self._scan_times):
            self._scan_times = times
            self._nc_index = nc_index_by_scan(self.ctx.nc_files())
            self._schedule(200)
            return
        last = self._scan_times[-1] if self._scan_times else None
        stamp = last.strftime("%H:%M:%S") if last else "—"
        live = False
        try:
            live = self.ctx.client().is_pipeline_running(radar)
        except Exception:
            logger.exception("is_pipeline_running failed")
        if live:
            self._status.config(text=f"● LIVE — caught up at {stamp} UTC; waiting for new scans")
            self._schedule(POLL_MS)
        else:
            self._status.config(
                text=f"■ Done — replayed {len(self._scan_times)} scans (last {stamp} UTC)"
            )
            self._pause()

    def _step_manual(self, delta):
        self._pause()
        if self._engine is None and not self._ensure_session():
            return
        self._seek(self._index + delta)

    def _seek(self, target):
        """Replay [0, target) with a fresh engine and render the last scan shown."""
        target = max(0, min(target, len(self._scan_times)))
        self._engine = TargetSelectionEngine(self._cfg)
        last = None
        for i in range(target):
            last = self._process(i)
            if last is None:
                break
        self._index = target
        if last is not None:
            self._render(*last)
        else:
            self._clear_map(_IDLE_TITLE)

    # ── Rendering ─────────────────────────────────────────────────────────────

    def _render(self, scan_ts, snap, selection, candidates):
        canvas, _fig, ax, _tb = self._canvas_refs
        nc = find_nc_for_scan(self._nc_index, scan_ts)
        draw_tse_map(ax, scan_ts, nc, snap, selection, candidates)
        canvas.draw_idle()
        self._update_status(scan_ts, snap, selection, candidates)
        self._set_rationale(format_rationale(snap, self._cfg, selection, candidates))

    def _update_status(self, scan_ts, snap, selection, candidates):
        stamp = scan_ts.strftime("%H:%M:%S")
        n = len(snap.cells)
        c = len(candidates)
        prefix = f"{self._index}/{len(self._scan_times)}  {stamp} UTC"
        if selection is None:
            self._status.config(text=f"{prefix}  ·  no target  ·  {c}/{n} candidates")
        else:
            self._status.config(
                text=(
                    f"{prefix}  ·  {selection.cell_uid}  ·  {selection.reason.value}"
                    f"  ·  score {selection.score:.0f}  ·  {c}/{n} candidates"
                )
            )

    def _clear_map(self, message):
        canvas, _fig, ax, _tb = self._canvas_refs
        ax.clear()
        ax.set_title(message, fontsize=10)
        canvas.draw_idle()
        self._set_rationale("")

    def _reset(self):
        self._pause()
        self._engine = None
        self._cfg = None
        self._scan_times = []
        self._index = 0
        self._clear_map(_IDLE_TITLE)
        self._status.config(text="Select a run, set rules, then Play.")

    # ── Shell hooks ───────────────────────────────────────────────────────────

    def refresh(self):
        """Auto-refresh hook: keep the rule dropdowns populated for the selected
        run. Live continuation is driven by the running step chain itself
        (``_step`` → ``_go_live``), so nothing else is scheduled here."""
        with contextlib.suppress(Exception):
            self._populate_columns()

    def on_run_changed(self):
        """User picked a different run: reset the replay and refill the dropdowns."""
        self._reset()
        self._columns_run = ""
        with contextlib.suppress(Exception):
            self._populate_columns()

    def on_close(self):
        """Stop the replay loop and cancel its pending step before teardown."""
        self._pause()

    # ── Save Movie source ─────────────────────────────────────────────────────

    def movie_source(self) -> MovieSource | None:
        """Movie source replaying the Target Selection map, or None if no session."""
        if self._cfg is None or not self._scan_times:
            return None
        cfg = self._cfg
        times = list(self._scan_times)
        nc_index = dict(self._nc_index)
        run_id = self._run_id
        radar = self.ctx.radar()
        client = self.ctx.client()

        def make_spec(i0: int, i1: int) -> MovieSpec:
            # Engine state is path-dependent (switch margin, observation dwell):
            # replay from the run's first scan, writing frames only for [i0, i1].
            engine = TargetSelectionEngine(cfg)
            cursor = 0
            latest = None

            def process(i):
                snap = build_snapshot(
                    client,
                    run_id,
                    radar,
                    growth_window_scans=cfg.snapshot.growth_window_scans,
                    at=times[i],
                )
                selection = engine.select(snap)
                cands = [c.uid for c in snap.cells if is_candidate(c, cfg.candidate)]
                return snap, selection, cands

            def draw(fig, k):
                nonlocal cursor, latest
                target = i0 + k
                while cursor <= target:  # each scan processed exactly once, in order
                    latest = process(cursor)
                    cursor += 1
                snap, selection, cands = latest
                ax = fig.add_subplot(111)
                ts = times[target]
                draw_tse_map(ax, ts, find_nc_for_scan(nc_index, ts), snap, selection, cands)

            return MovieSpec(n_frames=i1 - i0 + 1, draw_frame=draw, figsize=(7.5, 7.0), dpi=100)

        return MovieSource(
            labels=[t.strftime("%Y-%m-%d %H:%M:%S") for t in times],
            make_spec=make_spec,
            default_stem=f"{radar}_target_selection",
        )
