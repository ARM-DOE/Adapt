# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Constraint propagation and ambiguity isolation.

After validation the surviving pairs form a bipartite graph (previous objects ↔
current cells). ``ConstraintPropagator`` deterministically resolves every pair that
is unique on *both* sides (degree-1 ↔ degree-1), iterating until convergence — this
handles the great majority of ordinary tracks with no optimisation. ``AssignmentGraph``
then splits whatever remains into independent connected components, so Hungarian
assignment runs on small, locally-ambiguous groups instead of one global matrix.
"""

import networkx as nx

__all__ = ["AssignmentGraph", "ConstraintPropagator"]

Edge = tuple[int, int]  # (prev_idx, curr_idx)


class ConstraintPropagator:
    """Iteratively peel deterministic (mutually-unique) matches to convergence."""

    @staticmethod
    def resolve(edges: list[Edge]) -> tuple[list[Edge], list[Edge]]:
        """Return (forced_matches, remaining_edges).

        A pair (i, j) is forced when previous i has exactly one surviving candidate
        and current j has exactly one surviving predecessor. Removing forced nodes
        can make neighbours unique, so the peel repeats until nothing new resolves.
        """
        remaining: set[Edge] = set(edges)
        forced: list[Edge] = []
        while True:
            prev_deg: dict[int, int] = {}
            curr_deg: dict[int, int] = {}
            for i, j in remaining:
                prev_deg[i] = prev_deg.get(i, 0) + 1
                curr_deg[j] = curr_deg.get(j, 0) + 1
            new = [(i, j) for (i, j) in remaining if prev_deg[i] == 1 and curr_deg[j] == 1]
            if not new:
                break
            forced.extend(new)
            matched_prev = {i for i, _ in new}
            matched_curr = {j for _, j in new}
            remaining = {
                (i, j) for (i, j) in remaining if i not in matched_prev and j not in matched_curr
            }
        return sorted(forced), sorted(remaining)


class AssignmentGraph:
    """Bipartite graph over candidate edges; yields connected components."""

    def __init__(self, edges: list[Edge]):
        self.edges = list(edges)

    def components(self) -> list[tuple[list[int], list[int]]]:
        """Connected components as ``(prev_indices, curr_indices)`` sorted lists.

        Only nodes that appear in an edge are included; isolated previous/current
        objects (no surviving candidate) are handled by the caller as dissipated/born.
        """
        graph = nx.Graph()
        for i, j in self.edges:
            graph.add_edge(("p", i), ("c", j))
        components: list[tuple[list[int], list[int]]] = []
        for nodes in nx.connected_components(graph):
            prevs = sorted(n[1] for n in nodes if n[0] == "p")
            currs = sorted(n[1] for n in nodes if n[0] == "c")
            components.append((prevs, currs))
        return components
