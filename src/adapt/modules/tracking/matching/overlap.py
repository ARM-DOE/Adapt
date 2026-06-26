# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Deterministic overlap-first matcher.

Resolves the unambiguous cases before any optimisation: when a projected parent
hull and a current child overlap *mutually uniquely* above a moderate threshold,
they are a direct match and never enter Hungarian assignment. Uniqueness — no
competing candidate on either side — matters more than the exact overlap value.
Pure geometry over boolean masks; no graph, no state.
"""

import numpy as np

__all__ = ["OverlapMatcher", "overlap_fraction"]


def overlap_fraction(hull: np.ndarray, mask: np.ndarray) -> float:
    """Fraction of a projected hull covered by a current cell mask."""
    denom = float(np.sum(hull))
    if denom == 0.0:
        return 0.0
    return float(np.sum(hull & mask)) / denom


class OverlapMatcher:
    """Find mutually-unique parent↔child overlaps above a threshold."""

    def __init__(self, overlap_threshold: float):
        self.overlap_threshold = overlap_threshold

    def unique_matches(
        self,
        prev_hulls: list[np.ndarray],
        curr_masks: list[np.ndarray],
        allowed: np.ndarray,
    ) -> list[tuple[int, int]]:
        """Return ``(prev_idx, curr_idx)`` pairs that overlap mutually uniquely.

        ``allowed[i, j]`` gates a pair (e.g. it overlaps at all and is physically
        plausible). A pair is returned only when prev ``i`` links to exactly one
        curr and that curr links back to exactly one prev — both above threshold.
        """
        n_prev = len(prev_hulls)
        n_curr = len(curr_masks)
        links_per_prev: list[list[int]] = [[] for _ in range(n_prev)]
        links_per_curr: list[list[int]] = [[] for _ in range(n_curr)]

        for i, hull in enumerate(prev_hulls):
            if not hull.any():
                continue
            for j, mask in enumerate(curr_masks):
                if not allowed[i, j]:
                    continue
                if overlap_fraction(hull, mask) >= self.overlap_threshold:
                    links_per_prev[i].append(j)
                    links_per_curr[j].append(i)

        matches: list[tuple[int, int]] = []
        for i, js in enumerate(links_per_prev):
            if len(js) != 1:
                continue
            j = js[0]
            if len(links_per_curr[j]) == 1:  # j links back only to i
                matches.append((i, j))
        return matches
