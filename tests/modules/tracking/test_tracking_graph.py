# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

import numpy as np
import pytest

from adapt.modules.tracking.module import TrackingGraph

pytestmark = pytest.mark.unit


def _add_node(
    graph: TrackingGraph,
    time,
    track_index: int = 1,
    cell_id: int = 1,
    area: float = 4.0,
) -> int:
    return graph.add_observation(
        time=time,
        cell_id=cell_id,
        track_index=track_index,
        area=area,
        centroid_x=10.0,
        centroid_y=10.0,
        mean_reflectivity=40.0,
        max_reflectivity=50.0,
        core_area=2.0,
        cell_uid="TESTUIDA0",
        track_signature="v1|sig",
    )


T1 = np.datetime64("2024-01-01T12:00:00")
T2 = np.datetime64("2024-01-01T12:05:00")
T3 = np.datetime64("2024-01-01T12:10:00")


def test_add_observation_returns_sequential_ids():
    g = TrackingGraph()
    n0 = _add_node(g, T1)
    n1 = _add_node(g, T2)
    assert n0 == 0
    assert n1 == 1


def test_get_new_track_index_starts_at_one():
    g = TrackingGraph()
    assert g.get_new_track_index() == 1
    assert g.get_new_track_index() == 2


def test_get_nodes_at_time_returns_nodes_for_that_time():
    g = TrackingGraph()
    n0 = _add_node(g, T1, cell_id=1)
    n1 = _add_node(g, T1, cell_id=2)
    n2 = _add_node(g, T2, cell_id=1)

    at_t1 = g.get_nodes_at_time(T1)
    at_t2 = g.get_nodes_at_time(T2)

    assert sorted(at_t1) == sorted([n0, n1])
    assert at_t2 == [n2]


def test_get_nodes_at_time_unknown_time_returns_empty():
    g = TrackingGraph()
    _add_node(g, T1)
    assert g.get_nodes_at_time(T3) == []


def test_get_track_nodes_returns_sorted_by_time():
    g = TrackingGraph()
    # Add in non-chronological order
    n2 = _add_node(g, T3, track_index=1)
    n0 = _add_node(g, T1, track_index=1)
    n1 = _add_node(g, T2, track_index=1)

    result = g.get_track_nodes(1)
    assert result == [n0, n1, n2]


def test_get_track_nodes_excludes_other_tracks():
    g = TrackingGraph()
    n_track1_a = _add_node(g, T1, track_index=1, cell_id=1)
    n_track1_b = _add_node(g, T2, track_index=1, cell_id=1)
    _add_node(g, T1, track_index=2, cell_id=2)
    _add_node(g, T2, track_index=2, cell_id=2)

    result = g.get_track_nodes(1)
    assert sorted(result) == sorted([n_track1_a, n_track1_b])


def test_add_edge_and_get_successors():
    g = TrackingGraph()
    a = _add_node(g, T1)
    b = _add_node(g, T2)
    g.add_edge(a, b, edge_type="CONTINUE", cost=0.05)

    succs = g.get_successors(a)
    assert len(succs) == 1
    assert succs[0] == (b, "CONTINUE")


def test_add_edge_and_get_predecessors():
    g = TrackingGraph()
    a = _add_node(g, T1)
    b = _add_node(g, T2)
    g.add_edge(a, b, edge_type="SPLIT", cost=0.2)

    preds = g.get_predecessors(b)
    assert len(preds) == 1
    assert preds[0] == (a, "SPLIT")


def test_get_node_attr_returns_stored_value():
    g = TrackingGraph()
    n = g.add_observation(
        time=T1,
        cell_id=7,
        track_index=3,
        area=12.5,
        centroid_x=5.0,
        centroid_y=6.0,
        mean_reflectivity=38.0,
        max_reflectivity=52.0,
        core_area=3.1,
        cell_uid="ABCDE12345",
        track_signature="v1|test",
    )
    assert g.get_node_attr(n, "area") == pytest.approx(12.5)
    assert g.get_node_attr(n, "cell_id") == 7
    assert g.get_node_attr(n, "cell_uid") == "ABCDE12345"


def test_node_with_no_predecessor_has_empty_predecessors():
    g = TrackingGraph()
    n = _add_node(g, T1)
    assert g.get_predecessors(n) == []


def test_node_with_no_successors_has_empty_successors():
    g = TrackingGraph()
    n = _add_node(g, T1)
    assert g.get_successors(n) == []


def test_merge_edge_type_recorded():
    g = TrackingGraph()
    a = _add_node(g, T1, cell_id=1)
    b = _add_node(g, T1, cell_id=2)
    c = _add_node(g, T2, cell_id=1)
    g.add_edge(a, c, edge_type="MERGE")
    g.add_edge(b, c, edge_type="MERGE")

    preds = g.get_predecessors(c)
    edge_types = {etype for _, etype in preds}
    assert edge_types == {"MERGE"}
    assert len(preds) == 2
