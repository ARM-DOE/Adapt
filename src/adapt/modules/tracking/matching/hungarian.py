# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Per-component Hungarian assignment with a "no link" option.

Runs ``scipy.optimize.linear_sum_assignment`` over the admissible links of one
group of competing cells. Every previous cell also has its own "no link"
column at the cost ceiling, so a link is never forced when every candidate is
poor (Jaqaman et al. 2008 structure). The only home for ``scipy`` optimisation
in the tracker.
"""

import numpy as np
from scipy.optimize import linear_sum_assignment

__all__ = ["HungarianMatcher"]

# Larger than any admissible cost: marks a non-edge, never chosen over no-link.
_SENTINEL = 1.0e9


class HungarianMatcher:
    """Optimal assignment restricted to one group of competing cells."""

    @staticmethod
    def match(
        prev_indices: list[int],
        curr_indices: list[int],
        costs: dict[tuple[int, int], float],
        no_link_cost: float,
    ) -> list[tuple[int, int]]:
        """Return the minimum-cost ``(prev_idx, curr_idx)`` links of the group.

        ``costs`` holds admissible links only. A previous cell left unlinked
        costs ``no_link_cost``; a link at exactly that cost is still preferred.
        """
        if not prev_indices or not curr_indices:
            return []
        n, m = len(prev_indices), len(curr_indices)
        matrix = np.full((n, m + n), _SENTINEL, dtype=float)
        for a, i in enumerate(prev_indices):
            for b, j in enumerate(curr_indices):
                if (i, j) in costs:
                    matrix[a, b] = costs[(i, j)]
            matrix[a, m + a] = np.nextafter(no_link_cost, np.inf)
        rows, cols = linear_sum_assignment(matrix)
        return sorted(
            (prev_indices[a], curr_indices[b])
            for a, b in zip(rows, cols, strict=True)
            if b < m and (prev_indices[a], curr_indices[b]) in costs
        )
