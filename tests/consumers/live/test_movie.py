# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Movie writing for the dashboard — pure matplotlib writers, no Tk.

Synthetic frames, headless Agg figures, files written to tmp_path only.
"""

from pathlib import Path

import numpy as np
import pandas as pd
import pytest
import xarray as xr
from matplotlib.animation import FFMpegWriter, PillowWriter
from PIL import Image

from adapt.consumers.live._movie import MovieSpec, make_writer, write_movie_frames
from adapt.consumers.live._renderer import OverlayData, ViewState, scan_frame_drawer

pytestmark = pytest.mark.unit

_HAS_FFMPEG = FFMpegWriter.isAvailable()


def _text_spec(n_frames=4):
    def draw(fig, i):
        ax = fig.add_subplot(111)
        ax.text(0.5, 0.5, f"frame {i}", ha="center")

    return MovieSpec(n_frames=n_frames, draw_frame=draw, figsize=(2.0, 2.0), dpi=50)


# ── make_writer ─────────────────────────────────────────────────────────────


def test_writer_for_gif_is_pillow():
    assert isinstance(make_writer(Path("out.gif"), fps=5), PillowWriter)


@pytest.mark.skipif(not _HAS_FFMPEG, reason="ffmpeg not available")
def test_writer_for_mp4_is_ffmpeg():
    assert isinstance(make_writer(Path("out.mp4"), fps=5), FFMpegWriter)


def test_mp4_without_ffmpeg_raises(monkeypatch):
    monkeypatch.setattr(FFMpegWriter, "isAvailable", classmethod(lambda cls: False))
    with pytest.raises(RuntimeError, match="ffmpeg"):
        make_writer(Path("out.mp4"), fps=5)


def test_unknown_extension_raises():
    with pytest.raises(ValueError, match=r"\.avi"):
        make_writer(Path("out.avi"), fps=5)


# ── write_movie_frames ──────────────────────────────────────────────────────


def test_gif_written_with_one_image_per_frame(tmp_path):
    path = tmp_path / "out.gif"
    written = list(write_movie_frames(_text_spec(4), path, fps=5))
    assert written == [0, 1, 2, 3]
    with Image.open(path) as im:
        assert im.n_frames == 4


def test_closing_generator_finalizes_partial_file(tmp_path):
    path = tmp_path / "out.gif"
    gen = write_movie_frames(_text_spec(5), path, fps=5)
    next(gen)
    next(gen)
    gen.close()
    with Image.open(path) as im:
        assert im.n_frames == 2


@pytest.mark.skipif(not _HAS_FFMPEG, reason="ffmpeg not available")
def test_mp4_smoke(tmp_path):
    path = tmp_path / "out.mp4"
    written = list(write_movie_frames(_text_spec(2), path, fps=5))
    assert written == [0, 1]
    assert path.stat().st_size > 0


# ── scan_frame_drawer + writer integration ──────────────────────────────────


def _write_nc(path, hour):
    n = 10
    refl = np.full((n, n), np.nan)
    labels = np.zeros((n, n), dtype=int)
    refl[3:7, 3:7] = 45.0
    labels[4:6, 4:6] = 1
    ds = xr.Dataset(
        {
            "reflectivity": (("y", "x"), refl),
            "cell_labels": (("y", "x"), labels),
        },
        coords={
            "x": np.arange(n) * 1000.0,
            "y": np.arange(n) * 1000.0,
            "time": pd.Timestamp(f"2024-01-01T{hour:02d}:00:00").to_numpy(),
        },
        attrs={"radar": "TEST"},
    )
    ds.to_netcdf(path)


def _view(zoom=None):
    return ViewState(
        var_name="reflectivity",
        vmin=10.0,
        vmax=60.0,
        bg_alpha=0.35,
        max_proj_steps=0,
        show_flow=False,
        zoom=zoom,
        selected_cells={},
        color_slots=("#e15759",),
    )


def test_scan_frame_drawer_one_frame_per_nc(tmp_path):
    paths = []
    for h in (10, 11, 12):
        p = tmp_path / f"scan_{h}.nc"
        _write_nc(p, h)
        paths.append(p)
    draw = scan_frame_drawer(paths, _view(), OverlayData(cell_df=None, track_histories={}))
    spec = MovieSpec(n_frames=3, draw_frame=draw, figsize=(4.0, 3.0), dpi=50)
    out = tmp_path / "scan.gif"
    list(write_movie_frames(spec, out, fps=2))
    with Image.open(out) as im:
        assert im.n_frames == 3


def test_scan_frame_drawer_applies_frozen_zoom(tmp_path):
    from matplotlib.backends.backend_agg import FigureCanvasAgg
    from matplotlib.figure import Figure

    p = tmp_path / "scan.nc"
    _write_nc(p, 10)
    zoom = ((2.0, 6.0), (1.0, 7.0))
    draw = scan_frame_drawer([p], _view(zoom=zoom), OverlayData(cell_df=None, track_histories={}))
    fig = Figure(figsize=(4.0, 3.0), dpi=50)
    FigureCanvasAgg(fig)
    draw(fig, 0)
    assert fig.axes[0].get_xlim() == zoom[0]
