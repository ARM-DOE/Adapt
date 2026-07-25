# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Per-component Hungarian assignment (last resort).

Runs ``scipy.optimize.linear_sum_assignment`` over the validated edges *inside one
connected component only* — never a global matrix. Non-edge cells in the padded
matrix are dropped, so a component with unequal sides naturally leaves the surplus
objects unmatched (they become births / dissipations upstream). This is the only
home for ``scipy`` optimisation in the tracker.
"""

import numpy as np
from scipy.optimize import linear_sum_assignment

__all__ = ["HungarianMatcher"]

# Larger than any real geometric cost; padding cells are dropped after assignment.
_SENTINEL = 1.0e9


class HungarianMatcher:
    """Optimal assignment restricted to one ambiguous connected component."""

    @staticmethod
    def match(
        prev_indices: list[int],
        curr_indices: list[int],
        costs: dict[tuple[int, int], float],
    ) -> list[tuple[int, int]]:
        """Return the minimum-cost ``(prev_idx, curr_idx)`` matches for the component.

        ``costs`` holds only validated edges; assignments landing on a non-edge
        (padding) are discarded so unequal-sized components leave a surplus unmatched.
        """
        if not prev_indices or not curr_indices:
            return []
        matrix = np.full((len(prev_indices), len(curr_indices)), _SENTINEL, dtype=float)
        for a, i in enumerate(prev_indices):
            for b, j in enumerate(curr_indices):
                if (i, j) in costs:
                    matrix[a, b] = costs[(i, j)]
        rows, cols = linear_sum_assignment(matrix)
        matched: list[tuple[int, int]] = []
        for a, b in zip(rows, cols, strict=False):
            pair = (prev_indices[a], curr_indices[b])
            if pair in costs:  # real validated edge only
                matched.append(pair)
        return matched
