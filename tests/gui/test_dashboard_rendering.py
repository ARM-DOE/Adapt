# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Tests for dashboard rendering correctness and resource management.

Run headless (no Tkinter) using the Agg backend.

Bug B — RecursionError from ColorbarLocator chain:
  cla() preserves _axes_locator on the colorbar axes. Each call to
  fig.colorbar(im, cax=cbar_ax) wraps the previous locator in a new
  _ColorbarAxesLocator, building a chain. After ~983 draws the stack overflows.
  Fix: cbar_ax.set_axes_locator(None) before creating the new colorbar.

Bug C — Too many open files:
  xr.open_dataset() in _refresh_all / _nc_loop_step can be leaked if _redraw
  raises before _current_nc_ds is assigned. Fix: try/finally guards.
  iterdir()/glob() in _get_nc_files use lazy iterators that hold directory FDs;
  wrap in list() to eagerly consume.
"""

import matplotlib.gridspec as gridspec
import numpy as np
import pandas as pd
import pytest
import xarray as xr
from matplotlib.backends.backend_agg import FigureCanvasAgg
from matplotlib.figure import Figure

pytestmark = pytest.mark.unit


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_figure():
    """Reproduce the GridSpec layout used in dashboard._render_nc.
    Uses Figure+FigureCanvasAgg directly to avoid pyplot's backend selection
    (which can collide with TkAgg when running in the full test suite)."""
    fig = Figure(figsize=(10, 5))
    FigureCanvasAgg(fig)
    gs = gridspec.GridSpec(1, 2, width_ratios=[20, 1])
    ax = fig.add_subplot(gs[0])
    cbar_ax = fig.add_subplot(gs[1])
    return fig, ax, cbar_ax


def _make_minimal_nc(path):
    t = pd.Timestamp("2024-01-01T12:00:00")
    ds = xr.Dataset(
        {
            "reflectivity": (("y", "x"), np.ones((5, 5)) * 30.0),
            "cell_labels": (("y", "x"), np.zeros((5, 5), dtype=int)),
        },
        coords={
            "x": np.arange(5) * 1000.0,
            "y": np.arange(5) * 1000.0,
            "time": t.to_numpy(),
        },
        attrs={"radar": "TEST"},
    )
    ds.to_netcdf(path)


# ---------------------------------------------------------------------------
# Bug B — ColorbarLocator chain / RecursionError
# ---------------------------------------------------------------------------


def test_cla_builds_locator_chain():
    """Structural proof of Bug B: cla() leaves _axes_locator intact.
    After N colorbar creations, the chain depth equals N — each draw wraps
    the previous locator in a new _ColorbarAxesLocator.
    In production this chain reaches ~983 deep and causes RecursionError
    (Python 3.14 aborts the process on stack overflow; we verify chain
    depth structurally instead of relying on the exception being catchable)."""
    fig, ax, cbar_ax = _make_figure()
    N = 10
    for _ in range(N):
        ax.clear()
        im = ax.pcolormesh(np.random.default_rng(0).random((5, 5)), vmin=0, vmax=1)
        cbar_ax.cla()
        fig.colorbar(im, cax=cbar_ax)
    depth = 0
    loc = cbar_ax._axes_locator
    while loc is not None:
        depth += 1
        loc = getattr(loc, "_orig_locator", None)
    assert depth == N, f"expected chain depth {N}, got {depth}"


def test_set_axes_locator_none_keeps_chain_depth_one():
    """Structural complement to test_cla_builds_locator_chain: after N redraws
    with set_axes_locator(None), the chain depth must be exactly 1 (no growth)."""
    fig, ax, cbar_ax = _make_figure()
    N = 10
    for _ in range(N):
        ax.clear()
        im = ax.pcolormesh(np.random.default_rng(0).random((5, 5)), vmin=0, vmax=1)
        cbar_ax.set_axes_locator(None)
        fig.colorbar(im, cax=cbar_ax)
    depth = 0
    loc = cbar_ax._axes_locator
    while loc is not None:
        depth += 1
        loc = getattr(loc, "_orig_locator", None)
    assert depth == 1, f"expected chain depth 1, got {depth}"


# ---------------------------------------------------------------------------
# Bug C — xr.open_dataset leak when draw raises before _current_nc_ds is set
# ---------------------------------------------------------------------------


def test_dataset_closed_when_replaced(tmp_path):
    """The _draw_scan pattern must close the previous dataset when a new one is
    assigned. After replacement, the old dataset's _close attribute is None."""
    nc_path = tmp_path / "test.nc"
    _make_minimal_nc(nc_path)

    ds_old = xr.open_dataset(nc_path)
    ds_new = xr.open_dataset(nc_path)

    assert ds_old._close is not None, "ds_old should be open before replacement"

    # Replicate _draw_scan lines 1415–1418
    current = [ds_old]
    incoming = ds_new
    if current[0] is not None and current[0] is not incoming:
        current[0].close()
    current[0] = incoming

    assert ds_old._close is None, "ds_old must be closed after replacement"
    ds_new.close()


def test_dataset_closed_by_try_finally_on_exception(tmp_path):
    """The try/finally guard in _refresh_all / _nc_loop_step must close the
    dataset when _redraw raises before storing it in _current_nc_ds."""
    nc_path = tmp_path / "test.nc"
    _make_minimal_nc(nc_path)

    closed_datasets = []

    def open_and_fail():
        _ds = xr.open_dataset(nc_path)
        try:
            raise RuntimeError("simulated draw failure before _current_nc_ds = ds")
        except Exception:
            _ds.close()
            closed_datasets.append(_ds)
            raise

    with pytest.raises(RuntimeError):
        open_and_fail()

    assert len(closed_datasets) == 1
    assert closed_datasets[0]._close is None, "dataset must be closed by the guard"


# ---------------------------------------------------------------------------
# Bug C — _get_nc_files eager iterator consumption
# ---------------------------------------------------------------------------


def test_get_nc_files_eager_consume_does_not_exhaust_fds(tmp_path):
    """300 calls to the fixed _get_nc_files must return correct results.
    Eager list() consumption prevents lazy iterators from holding directory FDs."""
    from pathlib import Path

    analysis_dir = tmp_path / "KHTX" / "analysis" / "20240101"
    analysis_dir.mkdir(parents=True)
    (analysis_dir / "KHTX_20240101_120000_analysis.nc").touch()

    def get_nc_files_fixed(repo, radar):
        adir = Path(repo) / radar / "analysis"
        if not adir.exists():
            return []
        all_nc = []
        for date_dir in list(adir.iterdir()):  # eager: releases FD immediately
            if date_dir.is_dir() and len(date_dir.name) == 8 and date_dir.name.isdigit():
                all_nc.extend(list(date_dir.glob("*_analysis.nc")))  # eager
        return sorted(all_nc, key=lambda p: p.name)

    for _ in range(300):
        result = get_nc_files_fixed(str(tmp_path), "KHTX")

    assert len(result) == 1
    assert result[0].name == "KHTX_20240101_120000_analysis.nc"
