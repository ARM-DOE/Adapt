# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Central contract definitions for the Adapt pipeline.

All pipeline stage validators and the ContractViolation exception live here.
Import from this package — never from individual contract submodules.

Naming convention
-----------------
assert_*  : primitive validators that may take extra arguments (e.g. variable
            names). Call these from other validators or tests.
check_*   : bound, zero-extra-arg wrappers. Register these directly in a
            module's ``input_contracts`` or ``output_contracts`` dict.
"""

from adapt.contracts.analysis import (
    assert_analysis_output,
    assert_cell_adjacency,
    check_cell_adjacency,
    check_cell_stats,
)
from adapt.contracts.cell_volume_stats import (
    assert_cell_volume_stats,
    check_cell_volume_stats,
)
from adapt.contracts.grid import assert_gridded, check_grid_ds_2d
from adapt.contracts.history import check_scan_history
from adapt.contracts.lma import (
    assert_lma_cell_stats,
    assert_lma_flash_attribution,
    check_lma_cell_stats,
    check_lma_flash_attribution,
)
from adapt.contracts.pipeline import ContractViolation, require
from adapt.contracts.projection import assert_projected, check_projected_ds
from adapt.contracts.segmentation import assert_segmented, check_segmented_ds
from adapt.contracts.time import assert_time_normalized, check_time_normalized
from adapt.contracts.tracking import (
    assert_cell_events,
    assert_tracked_cells,
    check_cell_events,
    check_tracked_cells,
)

__all__ = [
    # primitives
    "ContractViolation",
    "require",
    "assert_gridded",
    "assert_segmented",
    "assert_projected",
    "assert_analysis_output",
    "assert_cell_adjacency",
    "assert_cell_volume_stats",
    "assert_tracked_cells",
    "assert_cell_events",
    "assert_time_normalized",
    "assert_lma_cell_stats",
    "assert_lma_flash_attribution",
    # bound checks — register these in input_contracts / output_contracts
    "check_grid_ds_2d",
    "check_scan_history",
    "check_segmented_ds",
    "check_projected_ds",
    "check_cell_stats",
    "check_cell_adjacency",
    "check_cell_volume_stats",
    "check_tracked_cells",
    "check_cell_events",
    "check_time_normalized",
    "check_lma_cell_stats",
    "check_lma_flash_attribution",
]
