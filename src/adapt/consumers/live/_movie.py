# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Movie writing for the dashboard — pure matplotlib animation writers, no Tk.

Frames are rendered off-screen (bare Figure + Agg canvas, never pyplot — a
second pyplot-managed figure would open a Tk window and can segfault on macOS).
"""

from collections.abc import Callable, Generator
from dataclasses import dataclass
from pathlib import Path

from matplotlib.animation import AbstractMovieWriter, FFMpegWriter, PillowWriter
from matplotlib.backends.backend_agg import FigureCanvasAgg
from matplotlib.figure import Figure


@dataclass(frozen=True)
class MovieSpec:
    """A fully frozen movie: frame count, how to draw each frame, geometry."""

    n_frames: int
    draw_frame: Callable[[Figure, int], None]  # draw frame i into a cleared Figure
    figsize: tuple[float, float]
    dpi: int


@dataclass(frozen=True)
class MovieSource:
    """What a dashboard tab offers the Save Movie dialog.

    ``make_spec(start_idx, end_idx)`` (inclusive range into ``labels``) must
    freeze all state it needs at call time, so live-view mutation cannot leak
    into the exported movie.
    """

    labels: list[str]
    make_spec: Callable[[int, int], MovieSpec]
    default_stem: str


def make_writer(path: Path, fps: int) -> AbstractMovieWriter:
    """Writer for *path* by extension: .mp4 → ffmpeg, .gif → Pillow."""
    suffix = path.suffix.lower()
    if suffix == ".mp4":
        if not FFMpegWriter.isAvailable():
            raise RuntimeError(
                "Cannot write .mp4 — the ffmpeg binary is not available. "
                "Install ffmpeg or save as .gif instead."
            )
        return FFMpegWriter(fps=fps)
    if suffix == ".gif":
        return PillowWriter(fps=fps)
    raise ValueError(f"Unsupported movie format: '{path.suffix}' (use .mp4 or .gif)")


def write_movie_frames(spec: MovieSpec, path: Path, fps: int) -> Generator[int, None, None]:
    """Write the movie one frame per iteration, yielding each frame index.

    Closing the generator finalizes the file with the frames written so far;
    the caller decides whether a partial file is kept or unlinked.
    """
    fig = Figure(figsize=spec.figsize, dpi=spec.dpi)
    FigureCanvasAgg(fig)
    writer = make_writer(path, fps)
    with writer.saving(fig, str(path), spec.dpi):
        for i in range(spec.n_frames):
            fig.clear()
            spec.draw_frame(fig, i)
            writer.grab_frame()
            yield i
