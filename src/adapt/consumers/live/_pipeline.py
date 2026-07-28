# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Pipeline subprocess control — start, stop, observe, reconnect — and the
Log tab that displays its output. The dashboard shell delegates every
pipeline concern here; this controller never touches the map tabs.
"""

import contextlib
import logging
import os
import platform
import subprocess
import threading
import time
import tkinter as tk
from collections.abc import Callable
from datetime import datetime
from pathlib import Path
from tkinter import filedialog, messagebox, scrolledtext, ttk
from typing import IO

from adapt.consumers.live._context import AppContext
from adapt.consumers.live._timers import AfterHandles
from adapt.consumers.live._utils import (
    _pipeline_pid_from_file,
    _pipeline_running,
    adapt_cmd,
    safe_close,
)
from adapt.utils.process import detached_process_kwargs, terminate_process_tree

logger = logging.getLogger(__name__)

LOG_MAX = 500
LOG_FILE = Path.home() / ".adapt" / "pipeline.log"


class PipelineController:
    """Start, stop, observe, and reconnect the adapt pipeline subprocess."""

    def __init__(
        self,
        app: tk.Tk,
        nb: ttk.Notebook,
        ctx: AppContext,
        *,
        badge: tk.Label,
        status_var: tk.StringVar,
        adopt_repo: Callable[[str], None],
    ):
        self.app = app
        self.ctx = ctx
        self._badge = badge
        self._status_var = status_var
        self._adopt_repo = adopt_repo  # select a repo in the toolbar + re-scan it
        self._proc: subprocess.Popen | None = None
        self._log_lines: list[str] = []
        self._log_file_handle: IO[str] | None = None
        self._active = True  # tail threads exit when this goes False
        self._timers = AfterHandles(self.app.after, self.app.after_cancel)
        self._build_log_tab(nb)

    # ── Log tab ───────────────────────────────────────────────────────────────

    def _build_log_tab(self, nb: ttk.Notebook) -> None:
        tab = ttk.Frame(nb)
        nb.add(tab, text="Log")

        ctrl = ttk.Frame(tab, padding=4)
        ctrl.pack(side="top", fill="x")
        ttk.Button(ctrl, text="Refresh", command=self.flush_log).pack(side="left")
        ttk.Button(ctrl, text="Clear", command=self._clear_log).pack(side="left", padx=4)
        ttk.Separator(ctrl, orient="vertical").pack(side="left", fill="y", padx=8)
        ttk.Button(ctrl, text="■ Stop Pipeline", command=self.stop).pack(side="left")

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

    def _append_log(self, line, tag=""):
        self.log_text.config(state="normal")
        self.log_text.insert("end", line + "\n", tag)
        self.log_text.see("end")
        self.log_text.config(state="disabled")

    def flush_log(self):
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

    # ── Run Adapt wizard ──────────────────────────────────────────────────────

    def open_wizard(self) -> None:
        import webbrowser

        win = tk.Toplevel(self.app)
        win.title("Start New Pipeline")
        win.resizable(False, False)

        path_var = tk.StringVar(value=self.ctx.repo())
        radar_var = tk.StringVar(value=self.ctx.radar())
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
                [*adapt_cmd(), "config", str(config_file)],
                capture_output=True,
                encoding="utf-8",
                errors="replace",
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
            cmd = [*adapt_cmd(), "run-nexrad", str(config_file)]
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
                *adapt_cmd(),
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
        self._adopt_repo(repo_dir)

        wizard_win.destroy()
        self._launch_pipeline(cmd)
        if self._proc is not None:
            messagebox.showinfo(
                "Pipeline started",
                f"Adapt pipeline running (PID {self._proc.pid}).\n"
                f"Output is streamed to the Log tab and saved to:\n{LOG_FILE}",
                parent=self.app,
            )

    # ── Launch / observe / stop ───────────────────────────────────────────────

    def _launch_pipeline(self, cmd: list) -> None:
        """Launch adapt pipeline, redirect all output to LOG_FILE, start watcher threads."""
        self._log_lines = []
        LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
        # errors="replace": this handle receives a subprocess's raw output, which
        # we do not control — an undecodable byte must not abort the launch.
        log_handle = LOG_FILE.open("w", buffering=1, encoding="utf-8", errors="replace")
        self._log_file_handle = log_handle
        try:
            self._proc = subprocess.Popen(
                cmd,
                stdout=log_handle,
                stderr=subprocess.STDOUT,
                **detached_process_kwargs(),
            )
        except Exception as exc:
            logger.exception("Failed to launch pipeline: %s", cmd)
            log_handle.close()
            self._log_file_handle = None
            # Show the command and the environment, not just the exception: this
            # is where an OS-level launch failure surfaces (on Windows a bad
            # interpreter or dependency reads as a bare "[WinError N]"), and the
            # command plus platform is what makes such a report actionable.
            messagebox.showerror(
                "Launch failed",
                f"{type(exc).__name__}: {exc}\n\n"
                f"Command: {' '.join(str(part) for part in cmd)}\n"
                f"Platform: {platform.platform()} · Python {platform.python_version()}\n\n"
                f"Full traceback in {LOG_FILE}",
                parent=self.app,
            )
            return
        self._append_log(
            f"[{datetime.now():%H:%M:%S}] Pipeline started (PID {self._proc.pid})", "info"
        )
        self._append_log(f"  Log: {LOG_FILE}", "info")
        self._start_log_tail(LOG_FILE)
        self._start_proc_watcher(self._proc)
        self.update_badge()

    def _start_log_tail(self, log_path: Path) -> None:
        """Daemon thread: tail log_path and append new lines to the log display."""

        def _tail():
            try:
                with log_path.open("r", encoding="utf-8", errors="replace") as f:
                    f.seek(0, 2)  # start at end — don't replay old content
                    while self._active:
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
                            self.app.after(0, self._append_log, line, tag)
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
                with log_path.open("r", encoding="utf-8", errors="replace") as f:
                    lines = f.readlines()
                    for ln in lines[-last_n:]:
                        ln = ln.rstrip()
                        self._log_lines.append(ln)
                        tag = "error" if "ERROR" in ln else "warning" if "WARNING" in ln else ""
                        self.app.after(0, self._append_log, ln, tag)
                    # Continue tailing from current position
                    while self._active and _pipeline_running():
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
                            self.app.after(0, self._append_log, line, tag)
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
                safe_close(self._log_file_handle, "pipeline log file", logger)
                self._log_file_handle = None
            self.app.after(0, self._on_proc_ended)

        threading.Thread(target=_watch, daemon=True, name="ProcWatcher").start()

    def stop(self) -> None:
        """Terminate the pipeline process group; escalate to a hard kill after 5 s."""
        if self._proc is None or self._proc.poll() is not None:
            # No owned process — try PID-file-only external process
            pid = _pipeline_pid_from_file()
            if pid is not None:
                with contextlib.suppress(OSError):
                    os.kill(pid, 15)
            self._on_proc_ended()
            return
        self._status_var.set("Stopping pipeline…")
        proc = self._proc

        def _do_kill():
            terminate_process_tree(proc, force=False)
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                terminate_process_tree(proc, force=True)

        threading.Thread(target=_do_kill, daemon=True).start()

    def _on_proc_ended(self) -> None:
        rc = self._proc.returncode if self._proc else None
        self._proc = None
        rc_str = f"exit {rc}" if rc is not None else "unknown"
        self._status_var.set(f"Stopped  |  {self.ctx.radar()}")
        self._append_log(f"[{datetime.now():%H:%M:%S}] Pipeline ended ({rc_str})", "info")
        self.update_badge()
        self.flush_log()

    def _find_running_pipeline(self) -> tuple[int | None, subprocess.Popen | None]:
        """Return (pid, proc) for the active pipeline, or (None, None) if idle."""
        if self._proc is not None and self._proc.poll() is None:
            return self._proc.pid, self._proc
        pid = _pipeline_pid_from_file()
        if pid is not None and _pipeline_running():
            return pid, None
        return None, None

    def is_running(self) -> bool:
        """True when this session's pipeline or a PID-file pipeline is alive."""
        return (self._proc is not None and self._proc.poll() is None) or _pipeline_running()

    def update_badge(self) -> None:
        if self.is_running():
            self._badge.config(text="● Pipeline running", fg="#4daf4a")
        else:
            self._badge.config(text="○ Idle", fg="#888888")

    # ── Reconnect to an external pipeline ─────────────────────────────────────

    def check_reconnect(self) -> None:
        """Offer to reconnect to an externally running pipeline on startup."""
        if not _pipeline_running() or self._proc is not None:
            return
        pid = _pipeline_pid_from_file()
        if pid is None:
            return
        ans = messagebox.askyesno(
            "Pipeline already running",
            f"Adapt pipeline (PID {pid}) is already running.\n\nReconnect to its log output?",
            parent=self.app,
        )
        if ans:
            self._reconnect_pipeline(pid)

    def _reconnect_pipeline(self, pid: int) -> None:
        """Attach log tail to a pipeline started outside this GUI session."""
        self._append_log(f"[{datetime.now():%H:%M:%S}] Reconnected to pipeline PID {pid}", "info")
        self.update_badge()
        if LOG_FILE.exists():
            self._start_log_tail_from_end(LOG_FILE, last_n=200)
        self._timers.recurring("poll", 2000, lambda: self._poll_external_pid(pid))

    def _poll_external_pid(self, pid: int) -> None:
        """Poll every 2 s for death of an external (PID-file-only) pipeline."""
        if not _pipeline_running():
            self._on_proc_ended()
            return
        self._timers.recurring("poll", 2000, lambda: self._poll_external_pid(pid))

    # ── Shutdown ──────────────────────────────────────────────────────────────

    def confirm_close(self) -> bool:
        """If the owned pipeline is still running, ask what to do.

        Returns False when the user chooses to stay in the dashboard.
        """
        if self._proc and self._proc.poll() is None:
            pid = self._proc.pid
            choice = messagebox.askyesnocancel(
                "Pipeline running",
                f"Adapt pipeline (PID {pid}) is still running.\n\n"
                "Yes → Kill it now\n"
                "No  → Keep it running in the background\n"
                "Cancel → Stay in dashboard",
                parent=self.app,
            )
            if choice is None:
                return False
            if choice:
                terminate_process_tree(self._proc, force=False)
                with contextlib.suppress(subprocess.TimeoutExpired):
                    self._proc.wait(timeout=3)
        return True

    def shutdown(self) -> None:
        """Stop tail threads, cancel polls, release the log handle."""
        self._active = False
        self._timers.cancel_all()
        if self._log_file_handle is not None:
            safe_close(self._log_file_handle, "pipeline log file", logger)
            self._log_file_handle = None
