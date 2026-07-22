# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Lineage event-row builders.

Pure functions that turn graph nodes into the explicit event dicts persisted as
``cell_events`` (CONTINUE / SPLIT / MERGE / INITIATION / TERMINATION). Each takes
the graph and the track_index→(cell_uid, signature) identity map; no tracker state.
"""

import pandas as pd

from adapt.modules.tracking.graph import TrackingGraph
from adapt.modules.tracking.models import MatchDiagnostics, MatchMethod
from adapt.utils.time import normalize_time_scalar

__all__ = [
    "DIAGNOSTIC_COLUMNS",
    "EVENT_COLUMNS",
    "build_cell_events_dataframe",
    "event_continue",
    "event_initiation",
    "event_merge",
    "event_split",
    "event_termination",
]

# Per-accepted-match explainability columns (null for INITIATION / TERMINATION).
DIAGNOSTIC_COLUMNS = [
    "candidate_overlap",
    "candidate_iou",
    "candidate_centroid_distance_m",
    "candidate_speed_ms",
    "candidate_heading_change_deg",
    "candidate_area_ratio",
    "candidate_reflectivity_difference",
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
    "cost",
    "is_dominant",
    "event_group_id",
    *DIAGNOSTIC_COLUMNS,
]

Identity = dict[int, tuple[str, str]]


def _diagnostic_fields(diag: MatchDiagnostics | None) -> dict:
    """Map a MatchDiagnostics into the ``candidate_*`` / ``match_method`` columns."""
    if diag is None:
        return dict.fromkeys(DIAGNOSTIC_COLUMNS)
    return {
        "candidate_overlap": diag.overlap,
        "candidate_iou": diag.iou,
        "candidate_centroid_distance_m": diag.centroid_distance_m,
        "candidate_speed_ms": diag.speed_ms,
        "candidate_heading_change_deg": diag.heading_change_deg,
        "candidate_area_ratio": diag.area_ratio,
        "candidate_reflectivity_difference": diag.reflectivity_difference,
        "candidate_final_cost": diag.final_cost,
        "match_method": diag.match_method,
    }


def _uid(graph: TrackingGraph, identity: Identity, node_id: int) -> str:
    track_index = int(graph.get_node_attr(node_id, "track_index"))
    if track_index not in identity:
        raise ValueError(f"Missing cell identity for track_index={track_index}")
    return identity[track_index][0]


def _time_key(time_val) -> str:
    """Stable ISO8601 time key for event grouping."""
    tv = normalize_time_scalar(time_val)
    return pd.Timestamp(tv).isoformat()


def event_continue(
    graph: TrackingGraph,
    identity: Identity,
    time,
    prev_node_id: int,
    curr_node_id: int,
    cost: float,
    diagnostics: MatchDiagnostics | None = None,
) -> dict:
    target_cell_uid = _uid(graph, identity, curr_node_id)
    return {
        "time": time,
        "event_type": "CONTINUE",
        "source_cell_uid": _uid(graph, identity, prev_node_id),
        "target_cell_uid": target_cell_uid,
        "source_cell_label": int(graph.get_node_attr(prev_node_id, "cell_id")),
        "target_cell_label": int(graph.get_node_attr(curr_node_id, "cell_id")),
        "cost": float(cost),
        "is_dominant": True,
        "event_group_id": f"{_time_key(time)}:CONTINUE:{target_cell_uid}",
        **_diagnostic_fields(diagnostics),
    }


def event_split(
    graph: TrackingGraph,
    identity: Identity,
    time,
    parent_node_id: int,
    child_node_id: int,
) -> dict:
    parent_uid = _uid(graph, identity, parent_node_id)
    return {
        "time": time,
        "event_type": "SPLIT",
        "source_cell_uid": parent_uid,
        "target_cell_uid": _uid(graph, identity, child_node_id),
        "source_cell_label": int(graph.get_node_attr(parent_node_id, "cell_id")),
        "target_cell_label": int(graph.get_node_attr(child_node_id, "cell_id")),
        "cost": None,
        "is_dominant": False,
        "event_group_id": f"{_time_key(time)}:SPLIT:{parent_uid}",
        **_diagnostic_fields(MatchDiagnostics(match_method=MatchMethod.SPLIT)),
    }


def event_merge(
    graph: TrackingGraph,
    identity: Identity,
    time,
    source_node_id: int,
    target_node_id: int,
) -> dict:
    target_uid = _uid(graph, identity, target_node_id)
    return {
        "time": time,
        "event_type": "MERGE",
        "source_cell_uid": _uid(graph, identity, source_node_id),
        "target_cell_uid": target_uid,
        "source_cell_label": int(graph.get_node_attr(source_node_id, "cell_id")),
        "target_cell_label": int(graph.get_node_attr(target_node_id, "cell_id")),
        "cost": None,
        "is_dominant": False,
        "event_group_id": f"{_time_key(time)}:MERGE:{target_uid}",
        **_diagnostic_fields(MatchDiagnostics(match_method=MatchMethod.MERGE)),
    }


def event_initiation(graph: TrackingGraph, identity: Identity, time, node_id: int) -> dict:
    target_uid = _uid(graph, identity, node_id)
    return {
        "time": time,
        "event_type": "INITIATION",
        "source_cell_uid": None,
        "target_cell_uid": target_uid,
        "source_cell_label": None,
        "target_cell_label": int(graph.get_node_attr(node_id, "cell_id")),
        "cost": None,
        "is_dominant": False,
        "event_group_id": f"{_time_key(time)}:INITIATION:{target_uid}",
    }


def event_termination(
    graph: TrackingGraph,
    identity: Identity,
    time,
    source_node_id: int,
    target_node_id: int | None,
) -> dict:
    source_uid = _uid(graph, identity, source_node_id)
    target_uid = _uid(graph, identity, target_node_id) if target_node_id is not None else None
    return {
        "time": time,
        "event_type": "TERMINATION",
        "source_cell_uid": source_uid,
        "target_cell_uid": target_uid,
        "source_cell_label": int(graph.get_node_attr(source_node_id, "cell_id")),
        "target_cell_label": (
            int(graph.get_node_attr(target_node_id, "cell_id"))
            if target_node_id is not None
            else None
        ),
        "cost": None,
        "is_dominant": False,
        "event_group_id": f"{_time_key(time)}:TERMINATION:{source_uid}",
    }


def build_cell_events_dataframe(events: list[dict]) -> pd.DataFrame:
    if not events:
        return pd.DataFrame(columns=EVENT_COLUMNS)
    df = pd.DataFrame(events)
    for col in EVENT_COLUMNS:
        if col not in df.columns:
            df[col] = None
    df = df[EVENT_COLUMNS]
    df["time"] = df["time"].apply(lambda t: pd.Timestamp(normalize_time_scalar(t)))
    return df
