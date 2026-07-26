# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Pure scan-map rendering behind the dashboard's Latest Scan tab.

Synthetic datasets, headless Agg figures — no Tk, no repository, no pyplot.
"""

import numpy as np
import pandas as pd
import pytest
import xarray as xr
from matplotlib.backends.backend_agg import FigureCanvasAgg
from matplotlib.figure import Figure
from matplotlib.quiver import Quiver

from adapt.consumers.live._renderer import (
    OverlayData,
    ViewState,
    data_extent_km,
    render_scan,
)
from adapt.consumers.live._renderer import (
    draw_track_overlays as draw_overlays_fn,
)

pytestmark = pytest.mark.unit

_SCAN_TS = pd.Timestamp("2024-01-01T12:00:00")


def _make_ds(*, with_projections=False, with_flow=False):
    """10×10 grid, 1 km spacing, one square cell (label 1) in the middle."""
    n = 10
    refl = np.full((n, n), np.nan)
    labels = np.zeros((n, n), dtype=int)
    refl[3:7, 3:7] = 45.0
    labels[4:6, 4:6] = 1
    data = {
        "reflectivity": (("y", "x"), refl),
        "cell_labels": (("y", "x"), labels),
    }
    if with_projections:
        proj = np.full((3, n, n), np.nan)
        proj[0, 4:6, 4:6] = 1.0  # frame 0 = registration
        proj[1, 5:7, 5:7] = 1.0
        proj[2, 6:8, 6:8] = 1.0
        data["cell_projections"] = (("frame_offset", "y", "x"), proj)
    if with_flow:
        data["heading_x"] = (("y", "x"), np.ones((n, n)))
        data["heading_y"] = (("y", "x"), np.ones((n, n)))
    return xr.Dataset(
        data,
        coords={
            "x": np.arange(n) * 1000.0,
            "y": np.arange(n) * 1000.0,
            "time": _SCAN_TS.to_numpy(),
        },
        attrs={"radar": "TEST"},
    )


def _view(**overrides):
    base = {
        "var_name": "reflectivity",
        "vmin": 10.0,
        "vmax": 60.0,
        "bg_alpha": 0.35,
        "max_proj_steps": 0,
        "show_flow": False,
        "zoom": None,
        "selected_cells": {},
        "color_slots": ("#e15759", "#4e79a7"),
    }
    base.update(overrides)
    return ViewState(**base)


def _fig_axes():
    fig = Figure(figsize=(6, 5))
    FigureCanvasAgg(fig)
    gs = fig.add_gridspec(1, 2, width_ratios=[20, 1])
    return fig, fig.add_subplot(gs[0]), fig.add_subplot(gs[1])


def _cell_df(uid="u1"):
    return pd.DataFrame(
        {
            "run_id": ["r"] * 3,
            "scan_time": ["2024-01-01T11:50:00Z", "2024-01-01T11:55:00Z", "2024-01-01T12:00:00Z"],
            "cell_uid": [uid] * 3,
            "cell_label": [1, 1, 1],
            "cell_centroid_mass_x": [2.0, 3.0, 4.0],
            "cell_centroid_mass_y": [2.0, 3.0, 4.0],
        }
    )


_NO_OVERLAYS = OverlayData(cell_df=None, track_histories={})


# ── ViewState ───────────────────────────────────────────────────────────────


def test_view_state_is_frozen():
    view = _view()
    with pytest.raises(AttributeError):
        view.vmin = 0.0


# ── render_scan ─────────────────────────────────────────────────────────────


def test_render_scan_draws_backdrop_overlay_and_title():
    _, ax, cbar_ax = _fig_axes()
    render_scan(ax, cbar_ax, _make_ds(), _view(), _NO_OVERLAYS)
    meshes = [c for c in ax.collections if type(c).__name__ == "QuadMesh"]
    assert len(meshes) >= 2  # gray backdrop + variable overlay
    title = ax.get_title()
    assert "TEST" in title and "Reflectivity" in title and "2024-01-01 12:00:00" in title


def test_render_scan_returns_contour_per_cell():
    _, ax, cbar_ax = _fig_axes()
    res = render_scan(ax, cbar_ax, _make_ds(), _view(), _NO_OVERLAYS)
    assert set(res.cell_contours) == {1}


def test_render_scan_reports_scan_timestamp():
    _, ax, cbar_ax = _fig_axes()
    res = render_scan(ax, cbar_ax, _make_ds(), _view(), _NO_OVERLAYS)
    assert res.scan_ts == _SCAN_TS


def test_render_scan_applies_zoom():
    _, ax, cbar_ax = _fig_axes()
    zoom = ((2.0, 6.0), (1.0, 7.0))
    render_scan(ax, cbar_ax, _make_ds(), _view(zoom=zoom), _NO_OVERLAYS)
    assert ax.get_xlim() == zoom[0]
    assert ax.get_ylim() == zoom[1]


def test_render_scan_full_extent_without_zoom():
    _, ax, cbar_ax = _fig_axes()
    render_scan(ax, cbar_ax, _make_ds(), _view(zoom=None), _NO_OVERLAYS)
    x0, x1 = ax.get_xlim()
    assert x0 <= 0.0 and x1 >= 9.0  # spans the 0–9 km grid


def test_render_scan_flow_toggle():
    for show, expect in ((False, 0), (True, 1)):
        _, ax, cbar_ax = _fig_axes()
        render_scan(ax, cbar_ax, _make_ds(with_flow=True), _view(show_flow=show), _NO_OVERLAYS)
        quivers = [c for c in ax.collections if isinstance(c, Quiver)]
        assert len(quivers) == expect


def test_render_scan_projection_steps_limit():
    counts = {}
    for steps in (0, 1):
        _, ax, cbar_ax = _fig_axes()
        render_scan(
            ax,
            cbar_ax,
            _make_ds(with_projections=True),
            _view(max_proj_steps=steps),
            _NO_OVERLAYS,
        )
        counts[steps] = len(ax.collections)
    assert counts[1] < counts[0]  # limiting steps draws fewer projection contours


def test_render_scan_draws_selected_track_and_star():
    _, ax, cbar_ax = _fig_axes()
    overlays = OverlayData(cell_df=_cell_df(), track_histories={"u1": _cell_df()})
    res = render_scan(ax, cbar_ax, _make_ds(), _view(selected_cells={"u1": 0}), overlays)
    artists = res.track_overlays["u1"]
    assert len(artists) >= 2  # track line + dots (+ star when visible)
    line = next(a for a in artists if type(a).__name__ == "Line2D")
    assert line.get_color() == "#e15759"


def test_draw_track_overlays_uses_cell_df_when_history_empty():
    _, ax, _ = _fig_axes()
    overlays = OverlayData(cell_df=_cell_df(), track_histories={})
    result = draw_overlays_fn(ax, _make_ds(), _view(selected_cells={"u1": 0}), overlays)
    assert result["u1"]  # track drawn from cell_df rows


# ── data_extent_km (toolbar Home → full extent) ──────────────────────────────


def test_data_extent_km_returns_full_grid_bounds():
    """The Home reset uses the dataset's own grid bounds (km) for full extent."""
    ds = _make_ds()  # x, y = arange(10) * 1000 metres
    (x0, x1), (y0, y1) = data_extent_km(ds)
    assert (x0, x1) == (0.0, 9.0)
    assert (y0, y1) == (0.0, 9.0)


# ── add_basemap ─────────────────────────────────────────────────────────────


def test_add_basemap_is_no_op_when_contextily_unavailable(monkeypatch):
    import adapt.consumers.live._renderer as renderer_mod

    monkeypatch.setattr(renderer_mod, "HAS_CTX", False)
    _, ax, _ = _fig_axes()
    renderer_mod.add_basemap(ax, ds=None, x_km=None, y_km=None)  # must not raise


def _located_ds():
    ds = _make_ds()
    ds.attrs["radar_latitude"] = 35.0
    ds.attrs["radar_longitude"] = -97.0
    return ds


def test_add_basemap_reuses_cached_tiles_for_unchanged_extent(monkeypatch):
    """A redraw at the same extent must not re-fetch tiles (the loop hammered the
    network every frame → socket exhaustion + UI hangs), yet the basemap must
    still be drawn from cache after the axes is cleared."""
    import adapt.consumers.live._renderer as renderer_mod

    class _OneShotCtx:
        class providers:
            class OpenStreetMap:
                Mapnik = "osm"

        used = False

        @classmethod
        def add_basemap(cls, ax, **kw):
            if cls.used:
                raise AssertionError("basemap re-fetched for an unchanged extent")
            cls.used = True
            ax.imshow(np.zeros((2, 2, 4)), extent=(0.0, 9.0, 0.0, 9.0), zorder=0)

    monkeypatch.setattr(renderer_mod, "HAS_CTX", True)
    monkeypatch.setattr(renderer_mod, "ctx", _OneShotCtx)

    _, ax, _ = _fig_axes()
    x_km = np.arange(10.0)
    y_km = np.arange(10.0)
    ds = _located_ds()

    renderer_mod.add_basemap(ax, ds, x_km, y_km)
    ax.clear()  # every render_scan frame clears the axes, wiping all artists
    renderer_mod.add_basemap(ax, ds, x_km, y_km)  # must reuse cache, never re-fetch

    assert len(ax.images) == 1  # basemap still present on the redrawn frame


def test_add_basemap_refetches_when_extent_changes(monkeypatch):
    """A zoom (new extent) must fetch fresh tiles — the cache is keyed on extent,
    so a changed view is never served stale tiles."""
    import adapt.consumers.live._renderer as renderer_mod

    fetched_extents = []

    class _RecordingCtx:
        class providers:
            class OpenStreetMap:
                Mapnik = "osm"

        @staticmethod
        def add_basemap(ax, **kw):
            fetched_extents.append((ax.get_xlim(), ax.get_ylim()))
            ax.imshow(np.zeros((2, 2, 4)), extent=(0.0, 1.0, 0.0, 1.0), zorder=0)

    monkeypatch.setattr(renderer_mod, "HAS_CTX", True)
    monkeypatch.setattr(renderer_mod, "ctx", _RecordingCtx)

    _, ax, _ = _fig_axes()
    ds = _located_ds()

    renderer_mod.add_basemap(ax, ds, np.arange(10.0), np.arange(10.0))
    renderer_mod.add_basemap(ax, ds, np.arange(10.0) * 2.0, np.arange(10.0))

    assert len(fetched_extents) == 2  # each distinct extent fetched its own tiles


def test_cached_basemap_replay_preserves_axes_aspect(monkeypatch):
    """The map panel must not resize on redraw (loop / back / zoom). The cached
    replay must keep the axes aspect exactly as the initial contextily fetch left
    it — a bare imshow would force aspect='equal' and squash the radar panel."""
    import adapt.consumers.live._renderer as renderer_mod

    class _AspectPreservingCtx:  # mimics contextily add_basemap (GH251)
        class providers:
            class OpenStreetMap:
                Mapnik = "osm"

        @staticmethod
        def add_basemap(ax, **kw):
            ax.imshow(
                np.zeros((2, 2, 4)),
                extent=(0.0, 9.0, 0.0, 9.0),
                aspect=ax.get_aspect(),  # contextily preserves the axes aspect
                zorder=0,
            )

    monkeypatch.setattr(renderer_mod, "HAS_CTX", True)
    monkeypatch.setattr(renderer_mod, "ctx", _AspectPreservingCtx)

    _, ax, _ = _fig_axes()
    ax.set_aspect("auto")  # the radar panel is auto (fills its GridSpec cell)
    ds = _located_ds()
    x_km = np.arange(10.0)
    y_km = np.arange(10.0)

    renderer_mod.add_basemap(ax, ds, x_km, y_km)  # initial fetch
    aspect_after_fetch = ax.get_aspect()
    ax.clear()  # a redraw (loop step / back / zoom) clears the axes first
    renderer_mod.add_basemap(ax, ds, x_km, y_km)  # cached replay

    assert ax.get_aspect() == aspect_after_fetch
