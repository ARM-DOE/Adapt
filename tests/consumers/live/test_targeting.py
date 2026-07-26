# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Pure helpers behind the dashboard's Target Selection tab.

Synthetic inputs, analytically known outputs — no Tk, no repository, no
stored fixtures. Matplotlib runs on the Agg backend (no display).
"""

from datetime import UTC, datetime, timedelta
from pathlib import Path

import matplotlib
import numpy as np
import pandas as pd
import pytest
import xarray as xr

from adapt.consumers.live._targeting import (
    build_tse_config,
    discover_numeric_columns,
    draw_reflectivity_backdrop,
    draw_target_overlay,
    draw_tse_map,
    filter_nc_paths_by_run,
    find_nc_for_scan,
    format_rationale,
    nc_index_by_scan,
)
from adapt.consumers.target_selection import (
    CellSnapshot,
    SelectionReason,
    Snapshot,
    TargetSelection,
)

# Importing the live package pulls in the Tkinter dashboard, which sets the
# TkAgg backend at import time. Force the headless Agg backend afterwards so the
# drawing tests never open a window.
matplotlib.use("Agg", force=True)
import matplotlib.pyplot as plt  # noqa: E402

pytestmark = pytest.mark.unit


# ── discover_numeric_columns ────────────────────────────────────────────────


def test_discover_drops_keys_and_non_numeric():
    df = pd.DataFrame(
        {
            "run_id": ["r"],
            "scan_time": ["2024-01-01T00:00:00Z"],
            "cell_uid": ["a"],
            "cell_label": [1],
            "adjacent_cell_uids_json": ["[]"],
            "cell_area_sqkm": [12.0],
            "radar_reflectivity_max": [50.0],
        }
    )
    cols = discover_numeric_columns(df)
    assert cols == ["cell_area_sqkm", "radar_reflectivity_max"]


def test_discover_empty_dataframe():
    assert discover_numeric_columns(pd.DataFrame()) == []


# ── build_tse_config ────────────────────────────────────────────────────────


def test_build_config_range_makes_min_and_max_gates_and_skips_blanks():
    rules = [
        ("cell_area_sqkm", 20.0, 200.0),  # range → two gates
        ("", None, None),  # blank field → skipped
        ("radar_reflectivity_mean", 40.0, None),  # floor only → one gate
        ("age_seconds", None, None),  # field but no bound → skipped
    ]
    cfg = build_tse_config(
        rules, min_age_s=0.0, max_obs_s=1200.0, switch_margin=15.0, growth_window=4
    )
    assert [(g.field, g.op, g.value) for g in cfg.candidate.gates] == [
        ("cell_area_sqkm", "ge", 20.0),
        ("cell_area_sqkm", "le", 200.0),
        ("radar_reflectivity_mean", "ge", 40.0),
    ]
    assert cfg.selection.max_observation_seconds == 1200.0
    assert cfg.selection.switch_margin == 15.0
    assert cfg.snapshot.growth_window_scans == 4


def test_build_config_max_only_makes_ceiling_gate():
    cfg = build_tse_config(
        [("n_adjacent_cells", None, 3.0)],
        min_age_s=0.0,
        max_obs_s=600.0,
        switch_margin=5.0,
        growth_window=2,
    )
    assert [(g.field, g.op, g.value) for g in cfg.candidate.gates] == [
        ("n_adjacent_cells", "le", 3.0)
    ]


def test_build_config_no_rules_gives_empty_gates():
    cfg = build_tse_config([], min_age_s=0.0, max_obs_s=600.0, switch_margin=5.0, growth_window=2)
    assert cfg.candidate.gates == ()


# ── nc_index_by_scan ────────────────────────────────────────────────────────


# Real pipeline filenames: RADAR<YYYYMMDD>_<HHMMSS>_V06_<run_id>_analysis.nc
_P1 = Path("/repo/KLOT/analysis/20260705/KLOT20260705_182600_V06_2026JUL04-1454-KLOT_analysis.nc")
_P2 = Path("/repo/KLOT/analysis/20260705/KLOT20260705_183130_V06_2026JUL04-1454-KLOT_analysis.nc")
_P_OLD = Path(
    "/repo/KLOT/analysis/20260703/KLOT20260703_040116_V06_2026JUL02-2319-KLOT_analysis.nc"
)


def test_nc_index_parses_timestamps():
    index = nc_index_by_scan([_P1, _P2])
    assert set(index) == {
        datetime(2026, 7, 5, 18, 26, 0, tzinfo=UTC),
        datetime(2026, 7, 5, 18, 31, 30, tzinfo=UTC),
    }
    assert index[datetime(2026, 7, 5, 18, 26, 0, tzinfo=UTC)] == _P1


def test_nc_index_ignores_unparseable():
    assert nc_index_by_scan([Path("/repo/KLOT/analysis/notes.nc")]) == {}


def test_filter_nc_paths_by_run():
    kept = filter_nc_paths_by_run([_P1, _P2, _P_OLD], "2026JUL04-1454-KLOT")
    assert kept == [_P1, _P2]


def test_filter_nc_paths_by_run_none_keeps_all():
    paths = [_P1, _P_OLD]
    assert filter_nc_paths_by_run(paths, None) == paths


# ── drawing helpers (Agg; assert artists, not pixels) ───────────────────────


def _grid():
    # 101×101 grid, 1 km spacing, radar at pixel (50, 50) → (0 km, 0 km).
    x = np.arange(101) * 1000.0 - 50_000.0
    y = np.arange(101) * 1000.0 - 50_000.0
    return x, y


def _ds():
    x, y = _grid()
    refl = np.zeros((y.size, x.size))
    return xr.Dataset({"reflectivity": (("y", "x"), refl)}, coords={"x": x, "y": y})


def _cell(uid, *, mass=(50, 50), proj=None, area=30.0, refl=50.0, growth=0.0):
    values = {
        "cell_centroid_mass_x": float(mass[0]),
        "cell_centroid_mass_y": float(mass[1]),
        "cell_area_sqkm": float(area),
        "radar_reflectivity_max": float(refl),
    }
    if proj:
        for k, (px, py) in proj.items():
            values[f"cell_centroid_projection{k}_x"] = float(px)
            values[f"cell_centroid_projection{k}_y"] = float(py)
    return CellSnapshot(
        uid=uid,
        lat=41.0,
        lon=-88.0,
        area_sqkm=area,
        reflectivity_max=refl,
        age_seconds=600.0,
        growth_rate_sqkm_per_min=growth,
        trajectory=(),
        values=values,
    )


def _selection(uid, reason=SelectionReason.NEW_TARGET, score=100.0):
    t = datetime(2026, 7, 4, 18, 26, tzinfo=UTC)
    return TargetSelection(
        cell_uid=uid,
        reason=reason,
        score=score,
        selection_time=t,
        trajectory=(),
        observation_window=(t, t),
    )


def test_backdrop_adds_a_mesh():
    fig, ax = plt.subplots()
    draw_reflectivity_backdrop(ax, _ds(), "reflectivity", alpha=0.8)
    assert len(ax.collections) >= 1
    plt.close(fig)


def test_overlay_draws_arrow_for_selected_with_projection():
    fig, ax = plt.subplots()
    snap = Snapshot(
        scan_time=datetime(2026, 7, 4, 18, 26, tzinfo=UTC),
        cells=(
            _cell("sel", mass=(50, 50), proj={1: (60, 55), 2: (70, 60)}),
            _cell("other", mass=(40, 40)),
        ),
    )
    draw_target_overlay(ax, _ds(), snap, _selection("sel"), candidate_uids={"sel"})
    # An arrow annotation exists (Annotation with a non-None arrow_patch).
    arrows = [t for t in ax.texts if getattr(t, "arrow_patch", None) is not None]
    assert len(arrows) == 1
    plt.close(fig)


def test_overlay_no_arrow_when_selected_has_no_projection():
    fig, ax = plt.subplots()
    snap = Snapshot(
        scan_time=datetime(2026, 7, 4, 18, 26, tzinfo=UTC),
        cells=(_cell("sel", mass=(50, 50)),),  # no projection columns
    )
    draw_target_overlay(ax, _ds(), snap, _selection("sel"), candidate_uids={"sel"})
    arrows = [t for t in ax.texts if getattr(t, "arrow_patch", None) is not None]
    assert arrows == []
    plt.close(fig)


def test_overlay_no_selection_draws_no_arrow():
    fig, ax = plt.subplots()
    snap = Snapshot(
        scan_time=datetime(2026, 7, 4, 18, 26, tzinfo=UTC),
        cells=(_cell("a", mass=(50, 50), proj={1: (60, 55)}),),
    )
    draw_target_overlay(ax, _ds(), snap, None, candidate_uids=set())
    arrows = [t for t in ax.texts if getattr(t, "arrow_patch", None) is not None]
    assert arrows == []
    # every cell still plotted as a marker
    assert len(ax.collections) >= 1
    plt.close(fig)


def test_overlay_has_three_entry_legend():
    fig, ax = plt.subplots()
    snap = Snapshot(
        scan_time=datetime(2026, 7, 4, 18, 26, tzinfo=UTC),
        cells=(_cell("sel", mass=(50, 50)),),
    )
    draw_target_overlay(ax, _ds(), snap, _selection("sel"), candidate_uids={"sel"})
    legend = ax.get_legend()
    assert legend is not None
    labels = [t.get_text() for t in legend.get_texts()]
    assert labels == ["other cell", "candidate", "selected target"]
    plt.close(fig)


# ── find_nc_for_scan ────────────────────────────────────────────────────────

_T26 = datetime(2026, 7, 5, 18, 26, 0, tzinfo=UTC)
_T31 = datetime(2026, 7, 5, 18, 31, 30, tzinfo=UTC)


def test_find_nc_for_scan_picks_nearest_within_tolerance():
    index = {_T26: _P1, _T31: _P2}
    assert find_nc_for_scan(index, _T26 + timedelta(seconds=30)) == _P1
    assert find_nc_for_scan(index, _T31 - timedelta(seconds=60)) == _P2


def test_find_nc_for_scan_none_beyond_tolerance():
    index = {_T26: _P1}
    assert find_nc_for_scan(index, _T26 + timedelta(minutes=10)) is None
    assert find_nc_for_scan({}, _T26) is None


# ── draw_tse_map (shared by live tab and movie frames) ──────────────────────


def test_draw_tse_map_draws_backdrop_overlay_and_title(tmp_path):
    nc_path = tmp_path / "scan.nc"
    _ds().to_netcdf(nc_path)
    snap = Snapshot(scan_time=_T26, cells=(_cell("sel", mass=(50, 50)),))
    fig, ax = plt.subplots()
    draw_tse_map(ax, _T26, nc_path, snap, _selection("sel"), {"sel"})
    meshes = [c for c in ax.collections if type(c).__name__ == "QuadMesh"]
    assert meshes  # reflectivity backdrop
    assert ax.get_legend() is not None
    assert "2026-07-05 18:26:00" in ax.get_title()
    plt.close(fig)


def test_draw_tse_map_without_raster_shows_placeholder():
    snap = Snapshot(scan_time=_T26, cells=())
    fig, ax = plt.subplots()
    draw_tse_map(ax, _T26, None, snap, None, set())
    assert ax.get_title().endswith("(no scan raster)")
    assert any("no scan raster" in t.get_text() for t in ax.texts)
    meshes = [c for c in ax.collections if type(c).__name__ == "QuadMesh"]
    assert not meshes
    plt.close(fig)


# ── format_rationale ────────────────────────────────────────────────────────


def _rationale_setup():
    # Default weights: reflectivity 1.0, area 0.05, growth 0.2.
    cfg = build_tse_config(
        [("cell_area_sqkm", 20.0, 200.0)],
        min_age_s=0.0,
        max_obs_s=1200.0,
        switch_margin=15.0,
        growth_window=4,
    )
    cur = _cell("cur", area=100.0, refl=50.0)  # score 50 + 5 = 55
    chal = _cell("chal", area=100.0, refl=55.0)  # score 55 + 5 = 60 (within margin)
    low = _cell("low", area=100.0, refl=30.0)  # score 30 + 5 = 35 (lower)
    snap = Snapshot(scan_time=datetime(2026, 7, 4, 18, 26, tzinfo=UTC), cells=(cur, chal, low))
    return cfg, snap


def test_rationale_selected_block_has_reason_score_and_terms():
    cfg, snap = _rationale_setup()
    sel = _selection("cur", reason=SelectionReason.CONTINUATION, score=55.0)
    text = format_rationale(snap, cfg, sel, {"cur", "chal", "low"})
    assert "SELECTED" in text and "cur" in text
    assert "CONTINUATION" in text
    assert "55" in text
    # score terms and the gate summary
    assert "reflectivity_max" in text and "area_sqkm" in text and "growth" in text
    assert "cell_area_sqkm" in text  # gate summary field


def test_rationale_ranks_candidates_and_explains_why_not():
    cfg, snap = _rationale_setup()
    sel = _selection("cur", reason=SelectionReason.CONTINUATION, score=55.0)
    text = format_rationale(snap, cfg, sel, {"cur", "chal", "low"})
    # higher-scoring challenger (60) is listed before the lower one (35)
    assert text.index("chal") < text.index("low")
    # chal scored above the current target but within switch margin
    assert "within switch margin" in text
    # low scored below → lower score
    assert "lower score" in text.lower()


def test_rationale_no_selection_lists_candidates():
    cfg, snap = _rationale_setup()
    text = format_rationale(snap, cfg, None, {"chal", "low"})
    assert "No target" in text
    assert "chal" in text and "low" in text


def test_rationale_no_candidates_message():
    cfg, snap = _rationale_setup()
    text = format_rationale(snap, cfg, None, set())
    assert "no qualified candidate" in text.lower()
