# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Directed graph storing cell tracking history and lineage.

Nodes are cell observations at a time; edges are temporal relationships
(CONTINUE, SPLIT, MERGE). This is the only home for ``networkx`` in the package.
"""

import networkx as nx

__all__ = ["TrackingGraph"]


class TrackingGraph:
    """Directed graph storing cell tracking history and lineage.

    Nodes represent cell observations at specific times.
    Edges represent temporal relationships (CONTINUE, SPLIT, MERGE).

    Node attributes:
        - node_id: unique identifier (int)
        - time: observation timestamp
        - cell_id: cell label from segmentation
        - track_index: tracking index this cell belongs to (starts at 1; 0 = background sentinel)
        - area: cell area in km²
        - centroid_x, centroid_y: cell center coordinates
        - mean_reflectivity: average dBZ
        - max_reflectivity: peak dBZ
        - core_area: area with Z > threshold dBZ

    Edge attributes:
        - edge_type: "CONTINUE", "SPLIT", "MERGE"
        - cost: assignment cost (for diagnostics)
    """

    def __init__(self):
        """Initialize empty tracking graph."""
        self.graph = nx.DiGraph()
        self._node_counter = 0
        self._track_counter = 0  # Will yield 1, 2, 3, ... (0 is background sentinel)

    def add_observation(
        self,
        time,
        cell_id: int,
        track_index: int,
        area: float,
        centroid_x: float,
        centroid_y: float,
        mean_reflectivity: float,
        max_reflectivity: float,
        core_area: float,
        cell_uid: str,
        track_signature: str,
    ) -> int:
        node_id = self._node_counter
        self._node_counter += 1

        self.graph.add_node(
            node_id,
            time=time,
            cell_id=cell_id,
            track_index=track_index,
            area=area,
            centroid_x=centroid_x,
            centroid_y=centroid_y,
            mean_reflectivity=mean_reflectivity,
            max_reflectivity=max_reflectivity,
            core_area=core_area,
            cell_uid=cell_uid,
            track_signature=track_signature,
        )
        return node_id

    def add_edge(self, from_node: int, to_node: int, edge_type: str, cost: float = 0.0):
        """Add a temporal relationship edge.

        Parameters
        ----------
        from_node : int
            Source node ID (earlier time)
        to_node : int
            Target node ID (later time)
        edge_type : str
            Edge type: "CONTINUE", "SPLIT", or "MERGE"
        cost : float, optional
            Assignment cost for diagnostics (default: 0.0)
        """
        self.graph.add_edge(from_node, to_node, edge_type=edge_type, cost=cost)

    def get_new_track_index(self) -> int:
        """Allocate a new unique track index (starts at 1; 0 is background sentinel)."""
        self._track_counter += 1
        return self._track_counter

    def get_node_attr(self, node_id: int, attr: str):
        """Get a node attribute value.

        Parameters
        ----------
        node_id : int
            Node identifier
        attr : str
            Attribute name

        Returns
        -------
        Any
            Attribute value, or None if not present
        """
        return self.graph.nodes[node_id].get(attr)

    def get_nodes_at_time(self, time) -> list[int]:
        """Get all node IDs for a given timestamp.

        Parameters
        ----------
        time : datetime-like
            Timestamp to query

        Returns
        -------
        List[int]
            List of node IDs at this time
        """
        return [n for n, d in self.graph.nodes(data=True) if d.get("time") == time]

    def get_track_nodes(self, track_index: int) -> list[int]:
        """Get all nodes belonging to a track, sorted by time."""
        nodes = [
            (n, d["time"])
            for n, d in self.graph.nodes(data=True)
            if d.get("track_index") == track_index
        ]
        nodes.sort(key=lambda x: x[1])
        return [n for n, _ in nodes]

    def get_predecessors(self, node_id: int) -> list[tuple[int, str]]:
        """Get predecessor nodes with their edge types.

        Parameters
        ----------
        node_id : int
            Node identifier

        Returns
        -------
        List[Tuple[int, str]]
            List of (predecessor_node_id, edge_type) tuples
        """
        return [
            (pred, self.graph.edges[pred, node_id]["edge_type"])
            for pred in self.graph.predecessors(node_id)
        ]

    def get_successors(self, node_id: int) -> list[tuple[int, str]]:
        """Get successor nodes with their edge types.

        Parameters
        ----------
        node_id : int
            Node identifier

        Returns
        -------
        List[Tuple[int, str]]
            List of (successor_node_id, edge_type) tuples
        """
        return [
            (succ, self.graph.edges[node_id, succ]["edge_type"])
            for succ in self.graph.successors(node_id)
        ]
