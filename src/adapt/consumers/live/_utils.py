# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Dashboard pure helper functions — no Tk, no matplotlib."""

from __future__ import annotations

import contextlib
import logging
import os
import sys
from collections.abc import Iterable
from pathlib import Path
from typing import TYPE_CHECKING

from adapt.utils.process import process_alive

if TYPE_CHECKING:
    import numpy as np
    import pandas as pd

logger = logging.getLogger(__name__)

_PID_FILE = Path.home() / ".adapt" / "pipeline.pid"
_N_COLOR_SLOTS = 7


def safe_close(resource, description: str, log: logging.Logger) -> None:
    """Close *resource*, logging (not swallowing silently) any failure.

    A close that raises must never abort the surrounding teardown/redraw, but a
    silently-swallowed failure hides a leaked file/connection — the exact class
    of bug behind the dashboard's "Too many open files". Log it so it is visible.
    """
    try:
        resource.close()
    except Exception:
        log.warning("Failed to close %s", description, exc_info=True)


@contextlib.contextmanager
def _suppress_osx_stderr():
    """Redirect fd 2 to /dev/null for the duration of the block.

    macOS ObjC runtime prints NSOpenPanel/NSWindow warnings directly to
    file-descriptor 2, bypassing Python's sys.stderr.  Only an OS-level
    dup2 can suppress them.

    A no-op elsewhere: there is nothing to suppress, and under a GUI launcher
    with no console (pythonw.exe) fd 2 may not exist, so os.dup(2) would raise.
    """
    if sys.platform != "darwin":
        yield
        return

    devnull = os.open(os.devnull, os.O_WRONLY)
    saved = os.dup(2)
    os.dup2(devnull, 2)
    os.close(devnull)
    try:
        yield
    finally:
        os.dup2(saved, 2)
        os.close(saved)


def _centroid_track_to_km(
    history_df: pd.DataFrame,
    x_metres: np.ndarray,
    y_metres: np.ndarray,
) -> tuple[np.ndarray, np.ndarray]:
    """Convert pixel centroid history to (x_km, y_km) using dataset grid coordinates."""
    cols = history_df["cell_centroid_mass_x"].values.astype(int)
    rows = history_df["cell_centroid_mass_y"].values.astype(int)
    return x_metres[cols] / 1000.0, y_metres[rows] / 1000.0


def _cell_uid_disp(uid) -> str:
    try:
        import pandas as _pd

        if _pd.isna(uid):
            return "—"
    except Exception:
        logger.exception("Failed to normalize cell UID display value")
    if uid is None:
        return "—"
    return str(uid)[:4]


def adapt_cmd() -> list[str]:
    """Return the command prefix that runs the Adapt CLI.

    Always the current interpreter plus ``-m``. Locating the console script on
    disk is platform-specific (``bin/adapt`` vs ``Scripts\\adapt.exe``) and can
    hand the OS a file it refuses to execute; ``-m`` is unambiguous everywhere
    and guarantees the pipeline runs in the same environment as the dashboard.
    """
    return [sys.executable, "-m", "adapt.cli"]


def _pipeline_pid_from_file() -> int | None:
    """Return the PID from the PID file, or None if absent/unreadable/empty."""
    if not _PID_FILE.exists():
        return None
    try:
        text = _PID_FILE.read_text(encoding="utf-8").strip()
        return int(text) if text else None
    except (ValueError, OSError):
        return None


def _pipeline_running() -> bool:
    """Return True if a pipeline PID file exists and the process is alive."""
    pid = _pipeline_pid_from_file()
    if pid is None:
        if _PID_FILE.exists():
            with contextlib.suppress(OSError):
                _PID_FILE.unlink()  # empty or malformed — stale
        return False

    if process_alive(pid):
        return True

    with contextlib.suppress(OSError):
        _PID_FILE.unlink()
    return False


def _next_free_color_slot(selected: dict[str, int]) -> int | None:
    """Return the first unused color slot index, or None if all 7 are taken."""
    used = set(selected.values())
    for i in range(_N_COLOR_SLOTS):
        if i not in used:
            return i
    return None


def _apply_overflow_action(action: str, selected: dict[str, int]) -> int | None:
    """Handle adding an 8th+ cell given the chosen overflow action.

    Parameters
    ----------
    action : str
        One of ``"ignore"``, ``"replace_oldest"``, or ``"wrap"``.
    selected : dict
        Mapping cell_uid → color_slot_index; modified in-place for
        ``"replace_oldest"``.

    Returns
    -------
    int | None
        The color slot to assign to the new cell, or None if the click
        should be discarded.
    """
    if action == "ignore":
        return None
    if action == "replace_oldest":
        oldest_uid = next(iter(selected))
        freed_slot = selected.pop(oldest_uid)
        return freed_slot
    # "wrap": reuse slot modulo 7 (color becomes ambiguous)
    return len(selected) % _N_COLOR_SLOTS


def _visible_uids_in_scan(
    cell_labels,  # numpy int array
    uid_map: dict[int, str],
) -> set[str]:
    """Return the set of cell_uids present in the current scan's label array.

    Parameters
    ----------
    cell_labels : np.ndarray
        Integer label array from the analysis NetCDF (0 = background).
    uid_map : dict[int, str]
        Maps integer label value → cell_uid string.
    """
    import numpy as np

    unique = set(np.unique(cell_labels).tolist()) - {0}
    return {uid_map[lbl] for lbl in unique if lbl in uid_map}


def format_run_labels(runs: Iterable) -> list[str]:
    """Format run records as ``"run_id  (MM-DD HH:MM)"`` toolbar labels."""
    labels = []
    for run in runs:
        mtime = run.start_time.strftime("%m-%d %H:%M") if run.start_time else "?"
        labels.append(f"{run.run_id}  ({mtime})")
    return labels


def _list_radars(repo: Path) -> list:
    """Return all registered radar IDs from the repository registry."""
    if not (repo / "adapt_registry.db").exists():
        return []
    from adapt.api.client import RepositoryClient

    with contextlib.closing(RepositoryClient(repo)) as client:
        return sorted(client.radars())


def _list_runs(repo: Path, radar: str | None = None) -> list:
    """Return formatted run strings from the repository registry.

    Returns
    -------
    list
        List of strings: "run_id  (MM-DD HH:MM)"
    """
    if not (repo / "adapt_registry.db").exists():
        return []
    from adapt.api.client import RepositoryClient

    with contextlib.closing(RepositoryClient(repo)) as client:
        return format_run_labels(client.runs(radar=radar))
