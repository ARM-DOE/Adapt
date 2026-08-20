# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Read per-cell 3D volume statistics for a track and join them to its history.

The ``cell_volume_stats`` enrichment table lives in the same ``catalog.db`` as
the track tables, keyed on (run_id, scan_id, cell_uid). The live dashboard's
time-series panels read ``cells_by_scan`` only, so volume columns (e.g.
cloud-top height) must be joined on demand when a volume plot group is
selected. This module owns that read + join — no Tk, no matplotlib.
"""

import sqlite3
from pathlib import Path

import pandas as pd

from adapt.consumers.live._utils import require_scan_identity


def load_track_volume_stats(db_path, run_id: str, cell_uid: str) -> pd.DataFrame:
    """Return ``cell_volume_stats`` rows for one track, ordered by scan_time.

    Empty DataFrame when the db, the table, or the track's rows are absent.
    Opens the db read-only (immutable) so the dashboard never needs write access.
    """
    path = Path(db_path)
    if not path.exists():
        return pd.DataFrame()
    uri = f"file:{path}?mode=ro&immutable=1"
    conn = sqlite3.connect(uri, uri=True, check_same_thread=False, isolation_level=None)
    try:
        tables = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        if "cell_volume_stats" not in tables:
            return pd.DataFrame()
        return pd.read_sql_query(
            "SELECT * FROM cell_volume_stats WHERE run_id=? AND cell_uid=? ORDER BY scan_time",
            conn,
            params=(run_id, cell_uid),
        )
    finally:
        conn.close()


def merge_volume_stats(track_df: pd.DataFrame, vol_df: pd.DataFrame) -> pd.DataFrame:
    """Left-join volume columns onto ``track_df`` on ``scan_id``.

    Returns ``track_df`` unchanged when there is nothing to add. Columns already
    present in ``track_df`` (other than the join key) are kept from ``track_df``.
    Both tables carry the pipeline-stamped scan identity, so the join is exact —
    an identity join between two per-scan tables, never a timestamp comparison.
    """
    if vol_df is None or vol_df.empty:
        return track_df
    require_scan_identity(vol_df, table="cell_volume_stats")
    drop = [c for c in vol_df.columns if c != "scan_id" and c in track_df.columns]
    return track_df.merge(vol_df.drop(columns=drop), on="scan_id", how="left")
