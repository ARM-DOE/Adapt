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
