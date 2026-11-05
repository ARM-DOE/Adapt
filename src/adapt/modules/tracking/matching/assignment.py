# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Joint assignment inside each group of competing cells.

Admissible links form a bipartite graph (previous cells ↔ current cells);
each connected component is decided on its own, so a clear association in one
part of the domain is never altered by an ambiguous one elsewhere. Inside a
component the minimum-cost set of links is found with a no-link option at the
cost ceiling, subject to the crossing test: while the chosen links contain a
crossing pair, the one of the two whose exclusion leaves the cheaper
assignment is excluded (the costlier on a tie) and the component is solved
again.
"""

from dataclasses import dataclass
from itertools import combinations

import networkx as nx

from adapt.modules.tracking.matching.gates import Point, segments_cross
from adapt.modules.tracking.matching.hungarian import HungarianMatcher

__all__ = ["ComponentSolution", "Link", "components", "solve_component"]

Edge = tuple[int, int]  # (prev_idx, curr_idx)


@dataclass(frozen=True)
class Link:
    """An admissible link with its cost and its segment (metres)."""

    prev: int
    curr: int
    cost: float
    start: Point
    end: Point


@dataclass(frozen=True)
class ComponentSolution:
    """Chosen links, links excluded for crossing (→ the chosen-set link they crossed),
    and each chosen link's margin: the extra total cost of the best assignment without it.
    """

    chosen: list[Edge]
    crossing_excluded: dict[Edge, Edge]
    margins: dict[Edge, float]


def components(edges: list[Edge]) -> list[tuple[list[int], list[int]]]:
    """Connected components as sorted ``(prev_indices, curr_indices)``."""
    graph = nx.Graph()
    graph.add_edges_from((("p", i), ("c", j)) for i, j in edges)
    groups = []
    for nodes in nx.connected_components(graph):
        groups.append(
            (sorted(n[1] for n in nodes if n[0] == "p"), sorted(n[1] for n in nodes if n[0] == "c"))
        )
    return sorted(groups)


def solve_component(links: list[Link], no_link_cost: float) -> ComponentSolution:
    """Minimum-cost non-crossing links of one component, with no-link at ``no_link_cost``."""
    by_edge = {(lk.prev, lk.curr): lk for lk in links}
    prevs = sorted({lk.prev for lk in links})
    currs = sorted({lk.curr for lk in links})

    def solve(excluded) -> tuple[list[Edge], float]:
        costs = {e: lk.cost for e, lk in by_edge.items() if e not in excluded}
        chosen = HungarianMatcher.match(prevs, currs, costs, no_link_cost)
        total = sum(costs[e] for e in chosen) + no_link_cost * (len(prevs) - len(chosen))
        return chosen, total

    def first_crossing(chosen: list[Edge]) -> tuple[Edge, Edge] | None:
        for a, b in combinations(chosen, 2):
            la, lb = by_edge[a], by_edge[b]
            if segments_cross(la.start, la.end, lb.start, lb.end):
                return a, b
        return None

    excluded: dict[Edge, Edge] = {}
    chosen, total = solve(excluded)
    while (pair := first_crossing(chosen)) is not None:
        a, b = sorted(pair, key=lambda e: (by_edge[e].cost, e))  # b is the costlier
        total_without_a = solve({**excluded, a: b})[1]
        total_without_b = solve({**excluded, b: a})[1]
        if total_without_a < total_without_b:
            excluded[a] = b
        else:
            excluded[b] = a
        chosen, total = solve(excluded)

    margins = {e: solve({**excluded, e: e})[1] - total for e in chosen}
    return ComponentSolution(chosen, excluded, margins)
