# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Read per-cell 3D volume statistics for a track and join them to its history.

The ``cell_volume_stats`` enrichment table is a module-owned products table,
keyed on (run_id, scan_id, cell_uid). The live dashboard's time-series panels
read ``cells_by_scan`` only, so volume columns (e.g. cloud-top height) must be
joined on demand when a volume plot group is selected. This module owns that
read + join — API only, no Tk, no matplotlib, no raw SQL.
"""

import pandas as pd

from adapt.consumers.live._utils import require_scan_identity


def load_track_volume_stats(client, collection: str, run_id: str, cell_uid: str) -> pd.DataFrame:
    """Return ``cell_volume_stats`` rows for one track, ordered by scan_time.

    Empty DataFrame when the enrichment module never ran (table not frozen)
    or the track has no rows.
    """
    known = set(client.tables(collection)["table_name"])
    if "cell_volume_stats" not in known:
        return pd.DataFrame()
    df = client.table(
        "cell_volume_stats", collection, run_id=run_id, filters={"cell_uid": cell_uid}
    )
    if df.empty:
        return df
    return df.sort_values("scan_time", ignore_index=True)


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
