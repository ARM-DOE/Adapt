# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Lineage event rows persisted as ``cell_events``.

One row per event: CONTINUE, SPLIT, MERGE, INITIATION, TERMINATION, LATENT (a
track kept for possible resumption) and RESUMED. ``source_scan_id`` /
``source_scan_time`` name the scan the source cell was last observed in — the
previous scan, except for a resumed or an expiring latent track.
"""

from dataclasses import dataclass

import pandas as pd

from adapt.utils.time import normalize_time_scalar

__all__ = [
    "DIAGNOSTIC_COLUMNS",
    "EVENT_COLUMNS",
    "EndPoint",
    "LinkDiagnostics",
    "build_cell_events_dataframe",
    "event_row",
]

DIAGNOSTIC_COLUMNS = [
    "candidate_opc",
    "candidate_ocp",
    "candidate_centroid_distance_m",
    "candidate_speed_ms",
    "candidate_heading_change_deg",
    "candidate_area_ratio",
    "candidate_final_cost",
    "match_method",
]

EVENT_COLUMNS = [
    "time",
    "event_type",
    "source_cell_uid",
    "target_cell_uid",
    "source_cell_label",
    "target_cell_label",
    "source_scan_id",
    "source_scan_time",
    "cost",
    "is_dominant",
    "event_group_id",
    *DIAGNOSTIC_COLUMNS,
]


@dataclass(frozen=True)
class EndPoint:
    """A cell at one end of an event."""

    uid: str
    label: int | None  # None: an expiring merge's survivor, observed scans ago


@dataclass(frozen=True)
class LinkDiagnostics:
    """Evidence of an accepted link: grown overlaps (O^c, O^h), centre residual
    to the prediction (m), speed, heading change, area ratio, cost, and how the
    link was decided."""

    o_c: float
    o_h: float
    residual_m: float
    speed_ms: float
    heading_change_deg: float | None
    area_ratio: float
    cost: float
    method: str


def _time_key(time_val) -> str:
    return pd.Timestamp(normalize_time_scalar(time_val)).isoformat()


def event_row(
    time,
    event_type: str,
    source: EndPoint | None,
    target: EndPoint | None,
    *,
    source_scan: tuple[str, object] | None = None,
    diagnostics: LinkDiagnostics | None = None,
    is_dominant: bool = False,
) -> dict:
    """One ``cell_events`` row; the group id is keyed by the cell the event is about."""
    about = target if event_type in {"CONTINUE", "MERGE", "INITIATION", "RESUMED"} else source
    if about is None:
        raise ValueError(f"{event_type} event without the cell it is about")
    diag = dict.fromkeys(DIAGNOSTIC_COLUMNS)
    if diagnostics is not None:
        diag = {
            "candidate_opc": diagnostics.o_c,
            "candidate_ocp": diagnostics.o_h,
            "candidate_centroid_distance_m": diagnostics.residual_m,
            "candidate_speed_ms": diagnostics.speed_ms,
            "candidate_heading_change_deg": diagnostics.heading_change_deg,
            "candidate_area_ratio": diagnostics.area_ratio,
            "candidate_final_cost": diagnostics.cost,
            "match_method": diagnostics.method,
        }
    return {
        "time": time,
        "event_type": event_type,
        "source_cell_uid": source.uid if source else None,
        "target_cell_uid": target.uid if target else None,
        "source_cell_label": source.label if source else None,
        "target_cell_label": target.label if target else None,
        "source_scan_id": source_scan[0] if source_scan else None,
        "source_scan_time": source_scan[1] if source_scan else None,
        "cost": diagnostics.cost if diagnostics else None,
        "is_dominant": is_dominant,
        "event_group_id": f"{_time_key(time)}:{event_type}:{about.uid}",
        **diag,
    }


def build_cell_events_dataframe(events: list[dict]) -> pd.DataFrame:
    if not events:
        return pd.DataFrame(columns=EVENT_COLUMNS)
    df = pd.DataFrame(events)[EVENT_COLUMNS]
    df["time"] = df["time"].apply(lambda t: pd.Timestamp(normalize_time_scalar(t)))
    return df
