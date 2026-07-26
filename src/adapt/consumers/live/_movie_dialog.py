# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Save Movie dialog — scan range / fps / output path, one frame per Tk tick.

Frames are produced strictly sequentially on the main thread via ``after``
(the dashboard's scheduling idiom), so the UI stays responsive and Cancel
works mid-export. A file exists on disk iff the export completed.
"""

import logging
import tkinter as tk
from collections.abc import Generator
from pathlib import Path
from tkinter import filedialog, messagebox, ttk

from adapt.consumers.live._movie import MovieSource, make_writer, write_movie_frames

logger = logging.getLogger(__name__)


class MovieDialog(tk.Toplevel):
    """Modal dialog that exports the active view's scan range to .mp4/.gif."""

    def __init__(self, parent: tk.Tk, source: MovieSource):
        super().__init__(parent)
        self._source = source
        self._gen: Generator[int, None, None] | None = None
        self._after_id: str | None = None
        self._path: Path | None = None
        self._n_frames = 0

        self.title("Save Movie")
        self.resizable(False, False)
        self.transient(parent)

        body = ttk.Frame(self, padding=10)
        body.pack(fill="both", expand=True)

        labels = source.labels
        self._start_var = tk.StringVar(value=labels[0])
        self._end_var = tk.StringVar(value=labels[-1])
        self._fps_var = tk.IntVar(value=5)

        ttk.Label(body, text="Start scan").grid(row=0, column=0, sticky="w")
        ttk.Combobox(
            body, textvariable=self._start_var, values=labels, state="readonly", width=36
        ).grid(row=0, column=1, padx=6, pady=2)
        ttk.Label(body, text="End scan").grid(row=1, column=0, sticky="w")
        ttk.Combobox(
            body, textvariable=self._end_var, values=labels, state="readonly", width=36
        ).grid(row=1, column=1, padx=6, pady=2)
        ttk.Label(body, text="Frames/s").grid(row=2, column=0, sticky="w")
        ttk.Spinbox(body, from_=1, to=30, textvariable=self._fps_var, width=6).grid(
            row=2, column=1, sticky="w", padx=6, pady=2
        )

        self._progress = ttk.Progressbar(body, length=300, mode="determinate")
        self._progress.grid(row=3, column=0, columnspan=2, pady=(10, 2), sticky="we")
        self._status = ttk.Label(body, text="")
        self._status.grid(row=4, column=0, columnspan=2, sticky="w")

        btns = ttk.Frame(body)
        btns.grid(row=5, column=0, columnspan=2, pady=(8, 0), sticky="e")
        self._save_btn = ttk.Button(btns, text="Save As…", command=self._start)
        self._save_btn.pack(side="left", padx=4)
        ttk.Button(btns, text="Cancel", command=self._cancel).pack(side="left")

        self.protocol("WM_DELETE_WINDOW", self._cancel)
        self.grab_set()

    def _start(self) -> None:
        labels = self._source.labels
        i0 = labels.index(self._start_var.get())
        i1 = labels.index(self._end_var.get())
        if i1 < i0:
            messagebox.showerror("Save Movie", "End scan is before start scan.", parent=self)
            return
        filename = filedialog.asksaveasfilename(
            parent=self,
            title="Save movie as",
            initialfile=f"{self._source.default_stem}.mp4",
            defaultextension=".mp4",
            filetypes=[("MP4 video", "*.mp4"), ("GIF animation", "*.gif")],
        )
        if not filename:
            return
        self._path = Path(filename)
        fps = max(1, int(self._fps_var.get()))
        try:
            make_writer(self._path, fps)  # fail fast, e.g. .mp4 without ffmpeg
            spec = self._source.make_spec(i0, i1)
        except Exception as exc:
            logger.exception("Could not start movie export")
            messagebox.showerror("Save Movie", str(exc), parent=self)
            return
        self._gen = write_movie_frames(spec, self._path, fps)
        self._n_frames = spec.n_frames
        self._progress.configure(maximum=spec.n_frames)
        self._save_btn.state(["disabled"])
        self._tick()

    def _tick(self) -> None:
        self._after_id = None
        if not self.winfo_exists() or self._gen is None:
            return
        try:
            i = next(self._gen)
        except StopIteration:
            self._gen = None
            messagebox.showinfo("Save Movie", f"Saved {self._path}", parent=self)
            self.destroy()
            return
        except Exception as exc:
            logger.exception("Movie export failed")
            self._gen = None
            if self._path is not None:
                self._path.unlink(missing_ok=True)
            messagebox.showerror("Save Movie", str(exc), parent=self)
            self.destroy()
            return
        self._progress.configure(value=i + 1)
        self._status.configure(text=f"frame {i + 1}/{self._n_frames}")
        self._after_id = self.after(1, self._tick)

    def _cancel(self) -> None:
        if self._after_id is not None:
            self.after_cancel(self._after_id)
            self._after_id = None
        if self._gen is not None:
            self._gen.close()  # finalizes the writer cleanly
            self._gen = None
            if self._path is not None:
                self._path.unlink(missing_ok=True)  # cancelled → no partial file
        self.destroy()
