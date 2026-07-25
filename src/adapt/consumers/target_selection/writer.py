# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Append-only JSONL log of selections.

Lives at a user-configured path outside the Adapt repository — consumers
never write into catalog.db. The parent directory must exist.
"""

import json
from pathlib import Path

from adapt.consumers.target_selection.selection import TargetSelection
from adapt.utils.time import to_scan_iso


def append_selection(path: str | Path, selection: TargetSelection) -> None:
    """Append one selection as a single JSON line."""
    record = {
        "cell_uid": selection.cell_uid,
        "reason": selection.reason.value,
        "score": selection.score,
        "selection_time": to_scan_iso(selection.selection_time),
        "trajectory": [
            {"lat": p.lat, "lon": p.lon, "lead_seconds": p.lead_seconds}
            for p in selection.trajectory
        ],
        "observation_window": [to_scan_iso(t) for t in selection.observation_window],
        "predicted_hulls": selection.predicted_hulls,
    }
    with open(path, "a") as f:
        f.write(json.dumps(record) + "\n")
