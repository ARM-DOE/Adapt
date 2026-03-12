"""Pipeline contracts — fail-fast enforcement of stage invariants.

This package enforces semantic guarantees between pipeline stages.
Contracts fail immediately and loudly when pipeline stages don't produce
their promised invariants. This is not defensive programming — it is
architecture enforcement.

Key principle:
- Pydantic validates config correctness
- Contracts validate pipeline correctness
- Algorithms handle science edge cases
"""

from adapt.contracts.base import ContractViolation, require
from adapt.contracts.grid import assert_gridded
from adapt.contracts.segmentation import assert_segmented
from adapt.contracts.projection import assert_projected
from adapt.contracts.analysis import assert_analysis_output

__all__ = [
    "ContractViolation",
    "require",
    "assert_gridded",
    "assert_segmented",
    "assert_projected",
    "assert_analysis_output",
]
