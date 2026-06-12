# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Join xLMA lightning stats (xlma_stat_minutes) onto a track's scan timeline.

Lightning is accumulated in 1-minute bins (``time_bin``); a track's time-series
is indexed by ``scan_time``. This aligns them by flooring each scan time to the
minute and left-joining the lightning value columns. The dashboard loads the
lightning rows through the API (RepositoryClient.track_lightning); this module
only does the pure, Tk/matplotlib-free join. Mirrors ``_volume_stats``.
"""

import pandas as pd


def merge_lightning(track_df: pd.DataFrame, lma_df: pd.DataFrame) -> pd.DataFrame:
    """Left-join lightning value columns onto ``track_df`` by 1-minute bin.

    Returns ``track_df`` unchanged when there is no lightning to add. The join
    key (the floored scan minute / ``time_bin``) is internal and not kept.
    """
    if lma_df is None or lma_df.empty or "time" not in lma_df.columns:
        return track_df

    track = track_df.copy()
    track["_bin"] = pd.to_datetime(track["scan_time"], utc=True).dt.floor("min")

    lma = lma_df.copy()
    lma["_bin"] = pd.to_datetime(lma["time"], utc=True)
    drop = [c for c in lma.columns if c in track.columns and c != "_bin"]
    lma = lma.drop(columns=drop)

    return track.merge(lma, on="_bin", how="left").drop(columns="_bin")
