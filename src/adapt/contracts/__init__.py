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
from adapt.contracts.columns import CELL_LABELS_VAR, stat_column
from adapt.contracts.decisions import (
    SegmentationComponent,
    SegmentationDecisions,
    SegmentationFrame,
    SegmentationSeed,
    TrackingCell,
    TrackingDecisions,
    TrackingIdentity,
    TrackingLatent,
    TrackingLineage,
    TrackingPair,
    TrackingScan,
    check_segmentation_decisions,
    check_tracking_decisions,
)
from adapt.contracts.grid import assert_gridded, check_grid_ds_2d
from adapt.contracts.history import check_prior_scan, check_scan_history
from adapt.contracts.persistence import (
    DecisionTableWrite,
    NetcdfArtifact,
    PersistenceMeta,
    PersistenceSpec,
    ProductTableWrite,
    ScanRecord,
    TrackTablesWrite,
)
from adapt.contracts.pipeline import ContractViolation, require
from adapt.contracts.projection import assert_projected, check_projected_ds
from adapt.contracts.segmentation import assert_segmented, check_seed_carry, check_segmented_ds
from adapt.contracts.time import assert_time_normalized, check_time_normalized
from adapt.contracts.tracking import (
    assert_cell_events,
    assert_tracked_cells,
    check_cell_events,
    check_tracked_cells,
)
from adapt.contracts.xlma_stat import (
    assert_xlma_stat_minutes,
    assert_xlma_stat_scan,
    check_xlma_stat_minutes,
    check_xlma_stat_scan,
)

__all__ = [
    # primitives
    "ContractViolation",
    "require",
    # canonical naming
    "CELL_LABELS_VAR",
    "stat_column",
    # persistence specs — modules declare these in their ``persistence`` ClassVar
    "ProductTableWrite",
    "NetcdfArtifact",
    "TrackTablesWrite",
    "DecisionTableWrite",
    "PersistenceSpec",
    "PersistenceMeta",
    "ScanRecord",
    # decision records — analysis-only, written to decisions.db
    "TrackingScan",
    "TrackingCell",
    "TrackingPair",
    "TrackingLineage",
    "TrackingIdentity",
    "TrackingLatent",
    "TrackingDecisions",
    "SegmentationFrame",
    "SegmentationSeed",
    "SegmentationComponent",
    "SegmentationDecisions",
    "assert_gridded",
    "assert_segmented",
    "assert_projected",
    "assert_analysis_output",
    "assert_cell_adjacency",
    "assert_cell_volume_stats",
    "assert_tracked_cells",
    "assert_cell_events",
    "assert_time_normalized",
    "assert_xlma_stat_minutes",
    "assert_xlma_stat_scan",
    # bound checks — register these in input_contracts / output_contracts
    "check_grid_ds_2d",
    "check_scan_history",
    "check_prior_scan",
    "check_segmented_ds",
    "check_seed_carry",
    "check_projected_ds",
    "check_cell_stats",
    "check_cell_adjacency",
    "check_cell_volume_stats",
    "check_tracked_cells",
    "check_cell_events",
    "check_time_normalized",
    "check_xlma_stat_minutes",
    "check_xlma_stat_scan",
    "check_tracking_decisions",
    "check_segmentation_decisions",
]
