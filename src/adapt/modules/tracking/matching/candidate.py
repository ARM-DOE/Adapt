# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Liberal candidate generation.

Each projected hull is dilated by the configured buffer, then every current cell
that touches the buffered hull becomes a candidate pair. High recall by design —
ranking and rejection happen in later stages. Pure geometry over boolean masks.
"""

import numpy as np

from adapt.modules.tracking.matching.geometry import dilate_hull

__all__ = ["CandidateGenerator"]


class CandidateGenerator:
    """Dilate projected hulls and emit every intersecting cell as a candidate."""

    def __init__(self, buffer_pixels: int):
        self.buffer_pixels = buffer_pixels

    def generate(
        self,
        prev_hulls: list[np.ndarray],
        curr_masks: list[np.ndarray],
    ) -> tuple[list[np.ndarray], list[tuple[int, int]]]:
        """Return (buffered_hulls, candidate_pairs).

        ``buffered_hulls[i]`` is the dilated hull for previous object ``i`` (reused
        by downstream overlap/cost). ``candidate_pairs`` are ``(prev_idx, curr_idx)``
        for every current cell intersecting a buffered hull.
        """
        buffered = [dilate_hull(h, self.buffer_pixels) for h in prev_hulls]
        pairs: list[tuple[int, int]] = []
        for i, hull in enumerate(buffered):
            if not hull.any():
                continue
            for j, cell in enumerate(curr_masks):
                if np.any(hull & cell):
                    pairs.append((i, j))
        return buffered, pairs
